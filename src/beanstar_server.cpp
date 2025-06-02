#include <seastar/core/app-template.hh>
#include <seastar/core/distributed.hh>
#include <seastar/core/future.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/timer.hh>
#include <seastar/core/sleep.hh>
#include <seastar/net/api.hh>
#include <seastar/util/log.hh>

#include <iostream>
#include <string>
#include <vector>
#include <queue>
#include <unordered_map>
#include <unordered_set>
#include <deque>
#include <atomic>
#include <chrono>
#include <optional>
#include <functional> // For std::function
#include <algorithm>  // For std::find



#define STALKD_SERVER 11300

// Global logger
seastar::logger applog("beanstar_daemon");

// Forward declaration
class Tube; 

enum class JobState {
    READY,
    RESERVED,
    DELAYED,
    BURIED,
    INVALID // after deletion or if not found
};

struct Job {
    uint64_t id;
    seastar::sstring body;
    unsigned priority; // Lower value means higher priority
    std::chrono::microseconds delay_us; // Original delay
    std::chrono::microseconds ttr_us;   // Time To Run

    JobState state;
    seastar::sstring tube_name;
    std::chrono::steady_clock::time_point creation_time;
    std::chrono::steady_clock::time_point deadline_time; // For TTR or delay activation
    seastar::timer<> ttr_timer{}; // Default constructor

    Job(uint64_t i, seastar::sstring b, unsigned prio, std::chrono::microseconds delay, std::chrono::microseconds ttr, seastar::sstring t_name)
        : id(i), body(std::move(b)), priority(prio), delay_us(delay), ttr_us(ttr),
          state(JobState::INVALID), tube_name(std::move(t_name)),
          creation_time(std::chrono::steady_clock::now()) {
    }
};

// Global storage for all jobs, sharded by job_id
seastar::sharded<std::unordered_map<uint64_t, Job>> jobs_storage;
// Atomic counter for generating unique job IDs
std::atomic<uint64_t> next_job_id{1};


// Reference to a job, used in Tube's priority queues to avoid cross-shard access for sorting
struct JobReference {
    uint64_t id;
    unsigned priority; 
    std::chrono::steady_clock::time_point sort_key_ts; // creation_time for ready, deadline_time for delayed

    struct ReadyComparator { // Higher actual priority (lower value) first, then FIFO
        bool operator()(const JobReference& a, const JobReference& b) const {
            if (a.priority != b.priority) {
                return a.priority > b.priority;  // Lower prio value is higher actual prio
            }
            return a.sort_key_ts > b.sort_key_ts; // Earlier creation_time first
        }
    };

    struct DelayedComparator { // Earlier deadline first
        bool operator()(const JobReference& a, const JobReference& b) const {
            return a.sort_key_ts > b.sort_key_ts; // Earlier deadline_time first
        }
    };
};

// Per-tube data structure
struct Tube {
    seastar::sstring name;
    // Jobs ready to be reserved, sorted by priority then creation time
    std::priority_queue<JobReference, std::vector<JobReference>, JobReference::ReadyComparator> ready_queue;
    // Jobs delayed for future execution, sorted by their activation time
    std::priority_queue<JobReference, std::vector<JobReference>, JobReference::DelayedComparator> delayed_queue;
    // Job IDs of jobs currently reserved by clients
    std::unordered_set<uint64_t> reserved_jobs;
    // Job IDs of jobs buried by clients
    std::deque<uint64_t> buried_queue;
    // Promises from clients waiting for a job from this tube
    std::deque<seastar::promise<std::optional<uint64_t>>> waiting_clients;
    // Timer to periodically check the delayed_queue
    seastar::timer<> delayed_job_poller{};

    Tube(seastar::sstring n) : name(std::move(n)) {}

    // Method to check delayed jobs and move them to ready queue if their time has come
    // This method runs on the tube's shard.
    void check_delayed_jobs() {
        while (!delayed_queue.empty()) {
            JobReference job_ref = delayed_queue.top(); // Peek at the top job

            if (job_ref.sort_key_ts <= std::chrono::steady_clock::now()) {
                delayed_queue.pop(); // Remove from delayed queue

                unsigned job_actual_shard = job_ref.id % seastar::smp::count;
                (void)jobs_storage.invoke_on(job_actual_shard, 
                    [job_id = job_ref.id](std::unordered_map<uint64_t, Job>& local_job_map) 
                    -> std::optional<std::pair<unsigned, std::chrono::steady_clock::time_point>> {
                    auto it = local_job_map.find(job_id);
                    if (it != local_job_map.end() && it->second.state == JobState::DELAYED) {
                        it->second.state = JobState::READY;
                        // Return the priority and creation_time needed for the ready queue JobReference
                        return std::make_pair(it->second.priority, it->second.creation_time);
                    }
                    return std::nullopt; // Job not found, or not in expected state
                }).then_wrapped([this, job_id = job_ref.id]
                    (seastar::future<std::optional<std::pair<unsigned, std::chrono::steady_clock::time_point>>> f_job_details) {
                    // This callback runs on the original tube's shard.
                    try {
                        std::optional<std::pair<unsigned, std::chrono::steady_clock::time_point>> opt_details = f_job_details.get();
                        if (opt_details) {
                            // Job state updated on its home shard, now add to local ready_queue
                            ready_queue.push({job_id, opt_details->first, opt_details->second});
                            applog.trace("Moved job {} from delayed to ready in tube {}", job_id, name);
                            notify_a_waiting_client(); // Notify a client that a job is available
                        } else {
                            applog.trace("Job {} not moved from delayed (not found or state changed) in tube {}", job_id, name);
                        }
                    } catch (const std::exception& e) {
                        applog.error("Exception processing delayed job {} for tube {}: {}", job_id, name, e.what());
                    }
                });
            } else {
                // Top job not ready yet, re-arm timer for its specific deadline
                delayed_job_poller.rearm(job_ref.sort_key_ts);
                return; // Stop checking, next check will be when timer fires
            }
        }
        // If loop finishes, means delayed_queue is empty or all processed jobs were invalid
        if (delayed_queue.empty()) {
            delayed_job_poller.cancel(); // No more delayed jobs, cancel poller
            applog.trace("Delayed queue for tube {} is empty, poller cancelled.", name);
        } else if (!delayed_job_poller.armed()) {
            // This case should ideally be covered by rearming for the next job's specific time.
            // If somehow it's not armed, arm it for the new top.
            delayed_job_poller.rearm(delayed_queue.top().sort_key_ts);
        }
    }

    // Arms the poller if there are delayed jobs and it's not already armed.
    void arm_delayed_job_poller_if_needed() {
        if (!delayed_queue.empty() && !delayed_job_poller.armed()) {
            applog.trace("Arming delayed job poller for tube {} for time: {}", name, delayed_queue.top().sort_key_ts.time_since_epoch().count());
            delayed_job_poller.arm(delayed_queue.top().sort_key_ts);
        }
    }

    // Notifies one waiting client (if any) that a job might be available.
    void notify_a_waiting_client() {
        if (!waiting_clients.empty()) {
            seastar::promise<std::optional<uint64_t>> promise = std::move(waiting_clients.front());
            waiting_clients.pop_front();
            // Fulfill promise with nullopt, signaling client to retry reservation.
            // The client's reserve logic will then try to pick a job from ready_queue.
            promise.set_value(std::nullopt); 
            applog.trace("Notified a waiting client for tube {}", name);
        }
    }

    ~Tube() {
        delayed_job_poller.cancel();
        // Clear waiting clients with an error or specific signal if necessary
        for (auto&& promise : waiting_clients) {
            promise.set_exception(std::runtime_error("Tube being destroyed"));
        }
        waiting_clients.clear();
    }
};

// Global collection of all tubes, sharded by tube name
seastar::sharded<std::unordered_map<seastar::sstring, Tube>> all_tubes;

// Helper to determine the shard ID for a given tube name
unsigned get_tube_shard_id(const seastar::sstring& tube_name) {
    return std::hash<seastar::sstring>{}(tube_name) % seastar::smp::count;
}

// Helper function to get or create a tube on the current shard (used within invoke_on)
// This static version is for use inside lambdas passed to invoke_on for all_tubes.
static Tube& get_or_create_tube_on_current_shard(
    std::unordered_map<seastar::sstring, Tube>& local_tubes_map, 
    const seastar::sstring& name) {
    auto it = local_tubes_map.find(name);
    if (it == local_tubes_map.end()) {
        it = local_tubes_map.emplace(std::piecewise_construct,
                                     std::forward_as_tuple(name),
                                     std::forward_as_tuple(name)).first;
        applog.debug("Created tube '{}' on shard {}", name, seastar::this_shard_id());
        
        Tube* this_tube_ptr = &(it->second);
        this_tube_ptr->delayed_job_poller.set_callback([this_tube_ptr] {
            this_tube_ptr->check_delayed_jobs();
        });
    }
    return it->second;
}


class client_connection : public seastar::enable_shared_from_this<client_connection> {
public:
    seastar::input_stream<char> _in;
    seastar::output_stream<char> _out;
    seastar::sstring _current_put_tube = "default"; // Tube for 'put'
    std::unordered_set<seastar::sstring> _watched_tubes; // Tubes for 'reserve'

    // State for multi-stage reserve operation
    seastar::promise<std::optional<uint64_t>> _reserve_promise;
    seastar::timer<> _reserve_timeout_timer{};
    bool _is_reserving = false;


    client_connection(seastar::input_stream<char>&& in, seastar::output_stream<char>&& out)
        : _in(std::move(in)), _out(std::move(out)) {
        _watched_tubes.insert("default"); // Watch "default" tube by default
    }

    // Main processing loop for a client connection
    seastar::future<> process() {
        return seastar::repeat([this] {
            // Read a line from the input stream (up to \n)
            return _in.read_until('\n').then([this](seastar::temporary_buffer<char> line_buf) {
                if (!line_buf || line_buf.empty()) { // EOF or empty line indicates connection closed by client
                    applog.debug("Client {} closed connection (EOF or empty line)", _out.socket_address());
                    return seastar::make_ready_future<seastar::stop_iteration>(seastar::stop_iteration::yes);
                }
                seastar::sstring line(line_buf.get(), line_buf.size());
                // Trim \r if present from \r\n
                if (!line.empty() && line.back() == '\r') {
                    line.pop_back();
                }
                
                applog.trace("Client {}: Received command line: '{}'", _out.socket_address(), line);
                return handle_command(line).then([this](bool continue_processing) {
                    if (continue_processing) {
                        return seastar::make_ready_future<seastar::stop_iteration>(seastar::stop_iteration::no);
                    } else { // e.g. quit command
                        applog.debug("Client {}: Stopping command processing.", _out.socket_address());
                        return seastar::make_ready_future<seastar::stop_iteration>(seastar::stop_iteration::yes);
                    }
                });
            });
        }).finally([this] {
            // Cleanup resources when connection is fully closed
            applog.debug("Client {} connection processing finished. Closing output stream.", _out.socket_address());
            _reserve_timeout_timer.cancel(); // Cancel any pending reserve timer
            if (_is_reserving) { // If a reserve was active and promise not fulfilled
                _reserve_promise.set_exception(std::runtime_error("Client connection closed during reserve"));
                _is_reserving = false;
            }
            return _out.close(); // Ensure output stream is closed
        });
    }

    // Parses and handles a single command line
    seastar::future<bool> handle_command(seastar::sstring line) {
        std::vector<seastar::sstring> parts;
        size_t start = 0;
        while(start < line.length()) {
            size_t end = line.find(' ', start);
            if (end == seastar::sstring::npos) {
                parts.push_back(line.substr(start));
                break;
            }
            parts.push_back(line.substr(start, end - start));
            start = end + 1;
        }

        if (parts.empty()) { // Should not happen if line_buf was not empty
            return client_error_response("INTERNAL_SERVER_ERROR_EMPTY_CMD"); 
        }

        const seastar::sstring& command = parts[0];
        applog.trace("Client {}: Parsed command: '{}'", _out.socket_address(), command);

        // Command dispatching
        if (command == "put") return handle_put(parts);
        if (command == "reserve") return handle_reserve_base(parts, std::nullopt);
        if (command == "reserve-with-timeout") return handle_reserve_base(parts, true);
        if (command == "delete") return handle_delete(parts);
        if (command == "release") return handle_release(parts);
        if (command == "bury") return handle_bury(parts);
        if (command == "touch") return handle_touch(parts);
        if (command == "use") return handle_use(parts);
        if (command == "watch") return handle_watch(parts);
        if (command == "ignore") return handle_ignore(parts);
        if (command == "quit") return seastar::make_ready_future<bool>(false); // Signal to stop processing

        applog.warn("Client {}: Unknown command: {}", _out.socket_address(), command);
        return client_error_response("UNKNOWN_COMMAND");
    }
    
    // Sends an error response to the client
    seastar::future<bool> client_error_response(const seastar::sstring& err_msg) {
        return _out.write(err_msg + "\r\n").then([this] { 
            return _out.flush(); // Ensure error message is sent
        }).then([] {
            return true; // Continue processing commands
        });
    }

    // Handles "put <priority> <delay_sec> <ttr_sec> <bytes>\r\n<data>\r\n"
    seastar::future<bool> handle_put(const std::vector<seastar::sstring>& parts) {
        if (parts.size() != 5) return client_error_response("BAD_FORMAT (put args)");
        try {
            unsigned priority = std::stoul(parts[1].c_str());
            unsigned delay_sec = std::stoul(parts[2].c_str());
            unsigned ttr_sec = std::stoul(parts[3].c_str());
            size_t bytes = std::stoul(parts[4].c_str());

            if (bytes > 1024 * 64) { // Max job size (64KB, as in original beanstalkd)
                // Drain the body first before sending error
                return _in.read_exactly(bytes + 2).then_wrapped([this](seastar::future<seastar::temporary_buffer<char>> f_drain) {
                    // We don't care about the drained data or if draining failed.
                    // Just ensure we attempt to consume it.
                    return client_error_response("JOB_TOO_BIG");
                });
            }

            return _in.read_exactly(bytes + 2).then([this, priority, delay_sec, ttr_sec, bytes](seastar::temporary_buffer<char> data_buf) {
                if (data_buf.size() != bytes + 2 || data_buf[bytes] != '\r' || data_buf[bytes+1] != '\n') {
                    // This indicates a framing error. The client sent incorrect byte count or missed CRLF.
                    // It's hard to recover gracefully. We might have consumed part of next command.
                    // Best to close connection or send error and hope client resets.
                    applog.warn("Client {}: Put command framing error. Expected {} bytes + CRLF, got {} bytes.", _out.socket_address(), bytes, data_buf.size());
                    // For simplicity, send error and continue. A robust server might close.
                    return client_error_response("BAD_FORMAT (put data framing)");
                }
                seastar::sstring data(data_buf.get(), bytes);
                return do_put(priority, delay_sec, ttr_sec, std::move(data));
            });
        } catch (const std::exception& e) {
            applog.warn("Client {}: Bad format in put command parameters: {}", _out.socket_address(), e.what());
            return client_error_response("BAD_FORMAT (put params parse)");
        }
    }

    seastar::future<bool> do_put(unsigned priority, unsigned delay_sec, unsigned ttr_sec, seastar::sstring data) {
        uint64_t job_id = next_job_id.fetch_add(1, std::memory_order_relaxed);
        unsigned target_tube_shard = get_tube_shard_id(_current_put_tube);
        unsigned job_storage_shard = job_id % seastar::smp::count;

        auto creation_tp = std::chrono::steady_clock::now();
        auto deadline_tp = creation_tp + std::chrono::seconds(delay_sec); // For delayed jobs

        // Step 1: Store the job data on its designated shard
        return jobs_storage.invoke_on(job_storage_shard, 
            [job_id, data = std::move(data), priority, delay_sec, ttr_sec, tube_name = _current_put_tube, creation_tp, deadline_tp]
            (std::unordered_map<uint64_t, Job>& local_job_map) mutable {
            
            Job new_job(job_id, std::move(data), priority,
                        std::chrono::seconds(delay_sec),
                        std::chrono::seconds(ttr_sec),
                        tube_name);
            new_job.creation_time = creation_tp; // Ensure consistent creation time

            if (delay_sec > 0) {
                new_job.state = JobState::DELAYED;
                new_job.deadline_time = deadline_tp;
            } else {
                new_job.state = JobState::READY;
            }
            local_job_map.emplace(job_id, std::move(new_job));
            applog.trace("Stored job {} on shard {}", job_id, seastar::this_shard_id());
        }).then([this, job_id, delay_sec, target_tube_shard, priority, creation_tp, deadline_tp] {
            // Step 2: Add job reference to the tube on its shard
            return all_tubes.invoke_on(target_tube_shard, 
                [job_id, delay_sec, tube_name = _current_put_tube, priority, creation_tp, deadline_tp]
                (std::unordered_map<seastar::sstring, Tube>& local_tubes_map) {
                
                Tube& tube = get_or_create_tube_on_current_shard(local_tubes_map, tube_name);
                if (delay_sec > 0) {
                    tube.delayed_queue.push({job_id, priority, deadline_tp});
                    tube.arm_delayed_job_poller_if_needed();
                    applog.trace("Added job {} to delayed queue of tube '{}'", job_id, tube_name);
                } else {
                    tube.ready_queue.push({job_id, priority, creation_tp}); // sort_key_ts is creation_time for ready
                    tube.notify_a_waiting_client();
                    applog.trace("Added job {} to ready queue of tube '{}'", job_id, tube_name);
                }
            });
        }).then([this, job_id] {
            // Step 3: Send response to client
            return _out.write(seastar::format("INSERTED {}\r\n", job_id))
                       .then([this]{ return _out.flush(); })
                       .then([]{ return true; });
        }).handle_exception_type([this](const std::exception& e) {
            applog.error("Client {}: Exception during 'put' operation: {}", _out.socket_address(), e.what());
            return client_error_response("INTERNAL_SERVER_ERROR (put)");
        });
    }
    
    // Base handler for "reserve" and "reserve-with-timeout"
    seastar::future<bool> handle_reserve_base(const std::vector<seastar::sstring>& parts, std::optional<bool> with_timeout_flag) {
        if (_is_reserving) { // Client issued a new reserve while one is pending
            // This is a protocol violation by client or a server logic issue.
            // Cancel previous, log, and send error.
            applog.warn("Client {}: New reserve command while previous one active. Cancelling old.", _out.socket_address());
            _reserve_timeout_timer.cancel();
            _reserve_promise.set_exception(std::runtime_error("Superseded by new reserve command"));
            _is_reserving = false; // Reset state
        }

        std::optional<std::chrono::seconds> timeout_duration;
        if (with_timeout_flag.has_value()) { // reserve-with-timeout
            if (parts.size() != 2) return client_error_response("BAD_FORMAT (reserve-with-timeout args)");
            try {
                timeout_duration = std::chrono::seconds(std::stoul(parts[1].c_str()));
            } catch (const std::exception& e) {
                return client_error_response("BAD_FORMAT (reserve-with-timeout parse)");
            }
        } else { // plain reserve (wait indefinitely)
            if (parts.size() != 1) return client_error_response("BAD_FORMAT (reserve args)");
            // timeout_duration remains std::nullopt for indefinite wait
        }
        
        _is_reserving = true;
        _reserve_promise = seastar::promise<std::optional<uint64_t>>(); // Fresh promise

        // Start the reservation attempt
        do_try_reserve_round(); 

        seastar::future<std::optional<uint64_t>> future_to_wait_on = _reserve_promise.get_future();

        if (timeout_duration) {
            if (*timeout_duration == std::chrono::seconds(0)) { // Special case: deadline soon
                // If do_try_reserve_round didn't find a job immediately, it will fulfill promise with nullopt.
                // The .then block below handles this.
            } else {
                _reserve_timeout_timer.set_callback([this] {
                    if (_is_reserving) { // Check if reserve is still active
                        applog.trace("Client {}: reserve timed out.", _out.socket_address());
                        _reserve_promise.set_value(std::nullopt); // Fulfill with nullopt for timeout
                        _is_reserving = false; // Mark as no longer reserving due to timeout
                    }
                });
                _reserve_timeout_timer.arm(*timeout_duration);
            }
        }

        // Handle the result of the reservation attempt (either immediate, after waiting, or timeout)
        return future_to_wait_on.then_wrapped([this, timeout_val = timeout_duration] // Capture timeout_val for DEADLINE_SOON logic
            (seastar::future<std::optional<uint64_t>> f_job_id_opt) {
            _is_reserving = false; // Reservation attempt concluded (successfully or not)
            _reserve_timeout_timer.cancel(); // Ensure timer is cancelled

            try {
                std::optional<uint64_t> job_id_opt = f_job_id_opt.get();
                if (job_id_opt) { // Job was successfully reserved
                    uint64_t job_id = *job_id_opt;
                    unsigned job_storage_shard = job_id % seastar::smp::count;
                    // Fetch job body to send to client
                    return jobs_storage.invoke_on(job_storage_shard, 
                        [job_id](const std::unordered_map<uint64_t, Job>& local_job_map) -> std::optional<seastar::sstring> {
                        auto it = local_job_map.find(job_id);
                        if (it != local_job_map.end()) return it->second.body;
                        return std::nullopt;
                    }).then([this, job_id](std::optional<seastar::sstring> body_opt) {
                        if (body_opt) {
                            return _out.write(seastar::format("RESERVED {} {}\r\n{}\r\n", job_id, body_opt->length(), *body_opt))
                                       .then([this]{ return _out.flush(); }).then([]{ return true; });
                        }
                        applog.error("Client {}: Job {} reserved but not found in storage for body fetch.", _out.socket_address(), job_id);
                        return client_error_response("INTERNAL_SERVER_ERROR (reserve body fetch)");
                    });
                } else { // No job reserved (could be timeout, deadline_soon, or explicit wake-up with no job)
                    if (timeout_val && *timeout_val == std::chrono::seconds(0)) {
                        return _out.write("DEADLINE_SOON\r\n").then([this]{ return _out.flush(); }).then([]{ return true; });
                    }
                    return _out.write("TIMED_OUT\r\n").then([this]{ return _out.flush(); }).then([]{ return true; });
                }
            } catch (const std::exception& e) {
                applog.error("Client {}: Exception while finalizing reserve: {}", _out.socket_address(), e.what());
                return client_error_response("INTERNAL_SERVER_ERROR (reserve finalization)");
            }
        });
    }

    // Tries to reserve a job from watched tubes. If none, and not timed out, registers for notification.
    void do_try_reserve_round() {
        if (!_is_reserving) return; // Guard against calls if reserve was cancelled/finished

        seastar::shared_ptr<std::vector<seastar::sstring>> watched_tubes_copy = 
            seastar::make_shared<std::vector<seastar::sstring>>(_watched_tubes.begin(), _watched_tubes.end());

        if (watched_tubes_copy->empty()) { // Should not happen (default tube)
            _reserve_promise.set_value(std::nullopt); // No tubes to watch, effectively a timeout/fail
            _is_reserving = false;
            return;
        }
        
        // Asynchronously iterate through watched tubes to find a job
        // This uses a manual loop with futures. seastar::do_for_each might be an alternative.
        seastar::shared_ptr<size_t> current_tube_idx = seastar::make_shared<size_t>(0);

        seastar::future<> loop_fut = seastar::do_until(
            [this, watched_tubes_copy, current_tube_idx] { // Stop condition for loop
                return *current_tube_idx >= watched_tubes_copy->size() || !_is_reserving; // Stop if all tubes checked or reserve cancelled
            },
            [this, watched_tubes_copy, current_tube_idx] { // Loop body
                const seastar::sstring& tube_name = (*watched_tubes_copy)[*current_tube_idx];
                (*current_tube_idx)++; // Move to next tube for subsequent iteration

                unsigned target_tube_shard = get_tube_shard_id(tube_name);
                return all_tubes.invoke_on(target_tube_shard, 
                    [this, tube_name](std::unordered_map<seastar::sstring, Tube>& local_tubes_map) -> seastar::future<std::optional<uint64_t>> {
                    // This lambda runs on the tube's shard
                    auto tube_it = local_tubes_map.find(tube_name);
                    if (tube_it == local_tubes_map.end() || tube_it->second.ready_queue.empty()) {
                        return seastar::make_ready_future<std::optional<uint64_t>>(std::nullopt); // No job in this tube
                    }
                    Tube& tube = tube_it->second;
                    JobReference job_ref = tube.ready_queue.top(); // Peek

                    uint64_t job_id = job_ref.id;
                    unsigned job_storage_shard = job_id % seastar::smp::count;

                    // Attempt to mark job as RESERVED on its home shard
                    return jobs_storage.invoke_on(job_storage_shard, 
                        [job_id, tube_name_copy = tube.name /*copy tube name for TTR callback*/]
                        (std::unordered_map<uint64_t, Job>& local_job_map) -> bool {
                        auto it = local_job_map.find(job_id);
                        if (it != local_job_map.end() && it->second.state == JobState::READY) {
                            Job& job = it->second;
                            job.state = JobState::RESERVED;
                            job.deadline_time = std::chrono::steady_clock::now() + job.ttr_us;
                            
                            // Setup TTR timer for the job
                            job.ttr_timer.set_callback([job_id_cb = job_id, tube_name_cb = tube_name_copy, 
                                                        prio_cb = job.priority, creation_time_cb = job.creation_time] {
                                applog.debug("TTR expired for job {}", job_id_cb);
                                unsigned job_shard_cb = job_id_cb % seastar::smp::count;
                                unsigned original_tube_shard_cb = get_tube_shard_id(tube_name_cb);

                                // Step 1: On job's shard, mark as READY (if still RESERVED)
                                (void)jobs_storage.invoke_on(job_shard_cb, [job_id_cb](std::unordered_map<uint64_t, Job>& j_map) {
                                    auto j_it = j_map.find(job_id_cb);
                                    if (j_it != j_map.end() && j_it->second.state == JobState::RESERVED) {
                                        j_it->second.state = JobState::READY;
                                        // TTR timer is one-shot, no need to cancel explicitly after it fires
                                    } else {
                                        // Job no longer reserved or doesn't exist, TTR effectively void
                                    }
                                }).then([original_tube_shard_cb, job_id_cb, tube_name_cb, prio_cb, creation_time_cb] {
                                    // Step 2: On tube's shard, remove from reserved_jobs and add to ready_queue
                                    (void)all_tubes.invoke_on(original_tube_shard_cb, 
                                        [job_id_cb, tube_name_cb, prio_cb, creation_time_cb]
                                        (std::unordered_map<seastar::sstring, Tube>& tubes_map_cb){
                                        auto tube_it_cb = tubes_map_cb.find(tube_name_cb);
                                        if (tube_it_cb != tubes_map_cb.end()) {
                                            Tube& t = tube_it_cb->second;
                                            if (t.reserved_jobs.erase(job_id_cb) > 0) { // If it was in reserved set
                                                t.ready_queue.push({job_id_cb, prio_cb, creation_time_cb});
                                                applog.debug("Job {} (TTR expired) moved back to ready in tube {}", job_id_cb, tube_name_cb);
                                                t.notify_a_waiting_client();
                                            }
                                        }
                                    });
                                });
                            }); // End TTR callback setup
                            job.ttr_timer.arm(job.deadline_time);
                            return true; // Successfully reserved on job's shard
                        }
                        return false; // Job not found or not in READY state on its shard
                    }).then_wrapped([this, job_id, &tube] // Back on tube's shard
                        (seastar::future<bool> f_reserved_on_job_shard) {
                        try {
                            if (f_reserved_on_job_shard.get()) { // Successfully reserved on job's shard
                                tube.ready_queue.pop(); // Now actually remove from ready queue
                                tube.reserved_jobs.insert(job_id);
                                applog.trace("Client {} reserved job {} from tube {}", _out.socket_address(), job_id, tube.name);
                                
                                if (_is_reserving) _reserve_promise.set_value(job_id); // Fulfill the main promise
                                _is_reserving = false; // Stop further attempts for this reserve call
                                return seastar::make_ready_future<std::optional<uint64_t>>(job_id);
                            }
                        } catch (const std::exception& e) {
                             applog.error("Client {}: Exception checking reservation status for job {} from tube {}: {}", _out.socket_address(), job_id, tube.name, e.what());
                        }
                        // If reservation failed on job's shard, or exception.
                        return seastar::make_ready_future<std::optional<uint64_t>>(std::nullopt);
                    });
                }).then_wrapped([this](seastar::future<std::optional<uint64_t>> f_job_opt) {
                    if (f_job_opt.failed()) {
                        applog.error("Client {}: Failure in reserve loop invoke_on: {}", _out.socket_address(), f_job_opt.get_exception());
                    }
                }); // End of loop body's future chain
            }); // End of seastar::do_until

        // After the loop (all tubes checked or job found)
        (void)loop_fut.then_wrapped([this, watched_tubes_copy](seastar::future<> f_loop_done) {
            if (f_loop_done.failed()) {
                 applog.error("Client {}: Reserve loop itself failed: {}", _out.socket_address(), f_loop_done.get_exception());
                 if(_is_reserving) _reserve_promise.set_exception(f_loop_done.get_exception());
                 _is_reserving = false;
                 return;
            }

            if (_is_reserving) { // Loop finished, no job found, and reserve is still active
                bool has_timeout = _reserve_timeout_timer.armed(); // Approximation: if timer armed, there's a timeout.
                
                if (!has_timeout) { // Indefinite wait: register with tubes
                    applog.trace("Client {}: No job found, will wait indefinitely on watched tubes.", _out.socket_address());
                    for (const auto& tube_name : *watched_tubes_copy) {
                        unsigned target_tube_shard = get_tube_shard_id(tube_name);

                        // Let's add client to waiting list of all watched tubes.
                        // The promise is fulfilled once. First tube to fulfill "wins".
                        for (const auto& tube_name : *watched_tubes_copy) {
                             unsigned target_tube_shard = get_tube_shard_id(tube_name);
                             (void)all_tubes.invoke_on(target_tube_shard, [this, tube_name](std::unordered_map<seastar::sstring, Tube>& local_tubes_map){
                                if (!_is_reserving) return; // Check again, might have been fulfilled by another tube
                                Tube& tube = get_or_create_tube_on_current_shard(local_tubes_map, tube_name);
                                // Only add if the promise is not already fulfilled
                                if (_is_reserving && !_reserve_promise.get_future().available()) {
                                   tube.waiting_clients.push_back(std::move(_reserve_promise));
                                   applog.trace("Client {} added to waiting list for tube {} (indefinite wait, actual wait depends on tube activity)", _out.socket_address(), tube_name);
                                   break; 
                                }
                             });
                             if (!_is_reserving || _reserve_promise.get_future().available()) break; // Stop if reserve ended or promise moved
                        }
                    }
                }
            }
        });
    }


    // Handles "delete <id>\r\n"
    seastar::future<bool> handle_delete(const std::vector<seastar::sstring>& parts) {
        if (parts.size() != 2) return client_error_response("BAD_FORMAT (delete args)");
        try {
            uint64_t job_id = std::stoull(parts[1].c_str());
            return do_delete(job_id);
        } catch (const std::exception& e) {
            return client_error_response("BAD_FORMAT (delete id parse)");
        }
    }

    seastar::future<bool> do_delete(uint64_t job_id) {
        unsigned job_storage_shard = job_id % seastar::smp::count;
        // Step 1: Remove job from its storage shard and get its tube name
        return jobs_storage.invoke_on(job_storage_shard, 
            [job_id](std::unordered_map<uint64_t, Job>& local_job_map) -> std::optional<seastar::sstring> {
            auto it = local_job_map.find(job_id);
            if (it == local_job_map.end()) {
                return std::nullopt; // Not found
            }
            seastar::sstring tube_name = it->second.tube_name;
            it->second.ttr_timer.cancel(); // Cancel TTR timer if active
            local_job_map.erase(it);
            applog.trace("Deleted job {} from storage on shard {}", job_id, seastar::this_shard_id());
            return tube_name;
        }).then([this, job_id](std::optional<seastar::sstring> tube_name_opt) {
            if (!tube_name_opt) {
                return _out.write("NOT_FOUND\r\n").then([this]{ return _out.flush(); }).then([]{ return true; });
            }
            // Step 2: If job was in a tube's reserved_jobs set, remove it.
            // Other queues (ready, delayed) handle missing jobs implicitly when they try to access them.
            unsigned target_tube_shard = get_tube_shard_id(*tube_name_opt);
            return all_tubes.invoke_on(target_tube_shard, 
                [job_id, tube_name = *tube_name_opt](std::unordered_map<seastar::sstring, Tube>& local_tubes_map){
                auto tube_it = local_tubes_map.find(tube_name);
                if (tube_it != local_tubes_map.end()) {
                    if (tube_it->second.reserved_jobs.erase(job_id) > 0) {
                         applog.trace("Removed job {} from reserved set of tube '{}'", job_id, tube_name);
                    }
                    // Buried queue might also need explicit cleanup if we implement kick thoroughly.
                    // For now, this is a simplification.
                }
            }).then([this] {
                return _out.write("DELETED\r\n").then([this]{ return _out.flush(); }).then([]{ return true; });
            });
        });
    }
    
    // Handles "release <id> <priority> <delay_sec>\r\n"
    seastar::future<bool> handle_release(const std::vector<seastar::sstring>& parts) {
        if (parts.size() != 4) return client_error_response("BAD_FORMAT (release args)");
        try {
            uint64_t job_id = std::stoull(parts[1].c_str());
            unsigned priority = std::stoul(parts[2].c_str());
            unsigned delay_sec = std::stoul(parts[3].c_str());
            return do_release(job_id, priority, delay_sec);
        } catch (const std::exception& e) {
            return client_error_response("BAD_FORMAT (release params parse)");
        }
    }

    seastar::future<bool> do_release(uint64_t job_id, unsigned new_priority, unsigned delay_sec) {
        unsigned job_storage_shard = job_id % seastar::smp::count;
        auto new_deadline_tp = std::chrono::steady_clock::now() + std::chrono::seconds(delay_sec);

        // Step 1: Update job details on its shard. Check if it was reserved.
        return jobs_storage.invoke_on(job_storage_shard, 
            [job_id, new_priority, delay_sec, new_deadline_tp] 
            (std::unordered_map<uint64_t, Job>& local_job_map) 
            -> std::tuple<bool, JobState, seastar::sstring, std::chrono::steady_clock::time_point, unsigned> { 
            // success, old_state, tube_name, sort_key_ts_for_queue, final_priority_for_queue
            auto it = local_job_map.find(job_id);
            if (it == local_job_map.end()) {
                return {false, JobState::INVALID, "", {}, 0}; // Not found
            }
            if (it->second.state != JobState::RESERVED) {
                return {false, it->second.state, "", {}, 0}; // Not reserved
            }
            
            Job& job = it->second;
            job.ttr_timer.cancel();
            job.priority = new_priority;
            seastar::sstring tube_name = job.tube_name;
            std::chrono::steady_clock::time_point sort_key_ts;

            if (delay_sec > 0) {
                job.state = JobState::DELAYED;
                job.deadline_time = new_deadline_tp;
                sort_key_ts = new_deadline_tp; // For delayed_queue
            } else {
                job.state = JobState::READY;
                sort_key_ts = job.creation_time; // For ready_queue
            }
            applog.trace("Releasing job {} on shard {}, new state: {}, new prio: {}", job_id, seastar::this_shard_id(), (int)job.state, new_priority);
            return {true, JobState::RESERVED /*old_state*/, tube_name, sort_key_ts, job.priority};
        }).then([this, job_id, delay_sec](std::tuple<bool, JobState, seastar::sstring, std::chrono::steady_clock::time_point, unsigned> result) {
            bool success = std::get<0>(result);
            JobState old_job_state = std::get<1>(result);
            seastar::sstring tube_name = std::get<2>(result);
            std::chrono::steady_clock::time_point sort_key_ts = std::get<3>(result);
            unsigned final_priority = std::get<4>(result);

            if (!success) {
                return _out.write((old_job_state == JobState::INVALID ? "NOT_FOUND\r\n" : "NOT_FOUND\r\n")) // Beanstalkd sends NOT_FOUND if not reserved.
                           .then([this]{ return _out.flush(); }).then([]{ return true; });
            }

            // Step 2: Update tube state on its shard (remove from reserved, add to ready/delayed)
            unsigned target_tube_shard = get_tube_shard_id(tube_name);
            return all_tubes.invoke_on(target_tube_shard, 
                [job_id, tube_name, delay_sec, sort_key_ts, final_priority]
                (std::unordered_map<seastar::sstring, Tube>& local_tubes_map){
                auto tube_it = local_tubes_map.find(tube_name);
                if (tube_it == local_tubes_map.end()) return; // Should not happen if job existed
                
                Tube& tube = tube_it->second;
                if (tube.reserved_jobs.erase(job_id) == 0) {
                    // Job was not in this tube's reserved set, though job's state said RESERVED.
                    // This indicates a potential inconsistency. Log it.
                    applog.warn("Job {} was marked RESERVED but not found in tube {}'s reserved set during release.", job_id, tube_name);
                    // Proceed to add to ready/delayed anyway, as job's state is updated.
                }

                if (delay_sec > 0) {
                    tube.delayed_queue.push({job_id, final_priority, sort_key_ts});
                    tube.arm_delayed_job_poller_if_needed();
                    applog.trace("Released job {} to delayed queue of tube '{}'", job_id, tube_name);
                } else {
                    tube.ready_queue.push({job_id, final_priority, sort_key_ts});
                    tube.notify_a_waiting_client();
                    applog.trace("Released job {} to ready queue of tube '{}'", job_id, tube_name);
                }
            }).then([this] {
                return _out.write("RELEASED\r\n").then([this]{ return _out.flush(); }).then([]{ return true; });
            });
        });
    }

    // Handles "bury <id> <priority>\r\n"
    seastar::future<bool> handle_bury(const std::vector<seastar::sstring>& parts) {
        if (parts.size() != 3) return client_error_response("BAD_FORMAT (bury args)");
        try {
            uint64_t job_id = std::stoull(parts[1].c_str());
            unsigned priority = std::stoul(parts[2].c_str()); // Priority for future kick
            return do_bury(job_id, priority);
        } catch (const std::exception& e) {
            return client_error_response("BAD_FORMAT (bury params parse)");
        }
    }

    seastar::future<bool> do_bury(uint64_t job_id, unsigned new_priority) {
        unsigned job_storage_shard = job_id % seastar::smp::count;
        // Step 1: Update job state to BURIED and set new priority on its shard.
        return jobs_storage.invoke_on(job_storage_shard, 
            [job_id, new_priority] (std::unordered_map<uint64_t, Job>& local_job_map) 
            -> std::tuple<bool, JobState, seastar::sstring> { // success, old_state, tube_name
            auto it = local_job_map.find(job_id);
            if (it == local_job_map.end()) {
                return {false, JobState::INVALID, ""};
            }
            if (it->second.state != JobState::RESERVED) {
                // Beanstalkd allows burying READY jobs too, but spec says "reserved job".
                // For now, strict: only reserved jobs. Or check if it's READY.
                // Let's stick to RESERVED as per common flow.
                return {false, it->second.state, ""}; 
            }
            Job& job = it->second;
            job.ttr_timer.cancel();
            job.state = JobState::BURIED;
            job.priority = new_priority; // Store new priority
            applog.trace("Burying job {} on shard {}, new prio: {}", job_id, seastar::this_shard_id(), new_priority);
            return {true, JobState::RESERVED, job.tube_name};
        }).then([this, job_id](std::tuple<bool, JobState, seastar::sstring> result) {
            bool success = std::get<0>(result);
            JobState old_job_state = std::get<1>(result);
            seastar::sstring tube_name = std::get<2>(result);

            if (!success) {
                 return _out.write("NOT_FOUND\r\n").then([this]{ return _out.flush(); }).then([]{ return true; }); // Or NOT_FOUND if not reserved
            }
            // Step 2: Update tube: remove from reserved, add to buried.
            unsigned target_tube_shard = get_tube_shard_id(tube_name);
            return all_tubes.invoke_on(target_tube_shard, 
                [job_id, tube_name](std::unordered_map<seastar::sstring, Tube>& local_tubes_map){
                auto tube_it = local_tubes_map.find(tube_name);
                if (tube_it == local_tubes_map.end()) return;
                Tube& tube = tube_it->second;
                tube.reserved_jobs.erase(job_id);
                tube.buried_queue.push_back(job_id); // Add to buried queue
                applog.trace("Moved job {} to buried queue of tube '{}'", job_id, tube_name);
            }).then([this] {
                return _out.write("BURIED\r\n").then([this]{ return _out.flush(); }).then([]{ return true; });
            });
        });
    }
    
    // Handles "touch <id>\r\n"
    seastar::future<bool> handle_touch(const std::vector<seastar::sstring>& parts) {
        if (parts.size() != 2) return client_error_response("BAD_FORMAT (touch args)");
        try {
            uint64_t job_id = std::stoull(parts[1].c_str());
            return do_touch(job_id);
        } catch (const std::exception& e) {
            return client_error_response("BAD_FORMAT (touch id parse)");
        }
    }

    seastar::future<bool> do_touch(uint64_t job_id) {
        unsigned job_storage_shard = job_id % seastar::smp::count;
        return jobs_storage.invoke_on(job_storage_shard, 
            [job_id](std::unordered_map<uint64_t, Job>& local_job_map) -> bool {
            auto it = local_job_map.find(job_id);
            if (it == local_job_map.end() || it->second.state != JobState::RESERVED) {
                return false; // Not found or not reserved
            }
            Job& job = it->second;
            job.deadline_time = std::chrono::steady_clock::now() + job.ttr_us;
            job.ttr_timer.rearm(job.deadline_time); // Re-arm TTR timer
            applog.trace("Touched job {}, new TTR deadline on shard {}", job_id, seastar::this_shard_id());
            return true;
        }).then([this, job_id](bool success) {
            if (!success) {
                // Beanstalkd: "NOT_FOUND\r\n" if the job does not exist or is not reserved by the client.
                // We don't track which client reserved it, so just "NOT_FOUND" if not reservable.
                return _out.write("NOT_FOUND\r\n").then([this]{ return _out.flush(); }).then([]{ return true; });
            }
            return _out.write("TOUCHED\r\n").then([this]{ return _out.flush(); }).then([]{ return true; });
        });
    }

    // Handles "use <tube>\r\n"
    seastar::future<bool> handle_use(const std::vector<seastar::sstring>& parts) {
        if (parts.size() != 2) return client_error_response("BAD_FORMAT (use args)");
        _current_put_tube = parts[1];
        // Tube names have length limit, etc. Not validated here for brevity.
        return _out.write(seastar::format("USING {}\r\n", _current_put_tube))
                   .then([this]{ return _out.flush(); }).then([]{ return true; });
    }

    // Handles "watch <tube>\r\n"
    seastar::future<bool> handle_watch(const std::vector<seastar::sstring>& parts) {
        if (parts.size() != 2) return client_error_response("BAD_FORMAT (watch args)");
        _watched_tubes.insert(parts[1]);
        return _out.write(seastar::format("WATCHING {}\r\n", _watched_tubes.size()))
                   .then([this]{ return _out.flush(); }).then([]{ return true; });
    }

    // Handles "ignore <tube>\r\n"
    seastar::future<bool> handle_ignore(const std::vector<seastar::sstring>& parts) {
        if (parts.size() != 2) return client_error_response("BAD_FORMAT (ignore args)");
        if (_watched_tubes.size() > 1) { // Cannot ignore the last tube
            _watched_tubes.erase(parts[1]);
        } else if (_watched_tubes.count(parts[1])) { // Trying to ignore the last tube
             return _out.write("NOT_IGNORED\r\n").then([this]{ return _out.flush(); }).then([]{ return true; });
        }
        // If tube was not watched, count remains same. Beanstalkd sends WATCHING <count>.
        return _out.write(seastar::format("WATCHING {}\r\n", _watched_tubes.size()))
                   .then([this]{ return _out.flush(); }).then([]{ return true; });
    }
};


// Main application entry point
int main(int argc, char** argv) {
    seastar::app_template app;
    app.add_options()
        ("port", boost::program_options::value<uint16_t>()->default_value(STALKD_PORT), "Beanstalkd port");

    return app.run(argc, argv, [&app]() -> seastar::future<> {
        auto& config = app.configuration();
        uint16_t port = config["port"].as<uint16_t>();

        // Start sharded services
        co_await jobs_storage.start();
        co_await all_tubes.start();
        // Initialize next_job_id (already done globally)

        // Any per-core initialization for sharded services if needed
        co_await all_tubes.invoke_on_all([](std::unordered_map<seastar::sstring, Tube>& local_tubes_map){
            // Example: could pre-create 'default' tube or other initial setup.
            // get_or_create_tube_on_current_shard handles individual tube timer setup upon first use.
            applog.info("Tube service initialized on shard {}", seastar::this_shard_id());
        });

        seastar::listen_options lo;
        lo.reuse_address = true;
        
        applog.info("Starting Beanstalkd clone (in-memory) on port {}...", port);

        // Listen on all cores for incoming connections
        co_await seastar::smp::invoke_on_all([port, lo] { // Listen on all cores
            return seastar::listen(seastar::make_ipv4_address({port}), lo, 
                [](seastar::connected_socket fd, seastar::socket_address addr) {
                applog.info("Accepted connection from {} on shard {}", addr, seastar::this_shard_id());
                auto conn = seastar::make_shared<client_connection>(fd.input(), fd.output());
                // Start processing commands for this connection.
                // Run in background, don't wait for it to complete here.
                (void)conn->process().handle_exception_type([conn, addr](const std::exception& e) {
                    // Log exceptions from connection processing
                    applog.error("Exception from connection {} ({}): {}", addr, conn->_out.socket_address(), e.what());
                    // Connection processing future will also close output stream in its finally().
                });
            });
        });
        
        applog.info("All cores are listening. Server is up.");
        // The application will keep running. app.run manages the main event loop.
        // To stop the server, one would typically use a signal handler to call seastar::engine().exit(0);
        co_return; 
    }).finally([] { // This finally executes when the application is shutting down
        applog.info("Shutting down server...");
        return jobs_storage.stop().then([]{
            return all_tubes.stop();
        }).then([]{
            applog.info("All sharded services stopped. Shutdown complete.");
        });
    });
}

