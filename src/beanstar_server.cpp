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
#include <seastar/util/std-compat.hh> // For seastar::compat::optional
#include <seastar/core/map_reduce.hh>


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
#include <sstream>    // For YAML formatting


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
    seastar::timer<> ttr_timer{}; 

    Job(uint64_t i, seastar::sstring b, unsigned prio, std::chrono::microseconds delay, std::chrono::microseconds ttr, seastar::sstring t_name)
        : id(i), body(std::move(b)), priority(prio), delay_us(delay), ttr_us(ttr),
          state(JobState::INVALID), tube_name(std::move(t_name)),
          creation_time(std::chrono::steady_clock::now()) {
    }
};

seastar::sharded<std::unordered_map<uint64_t, Job>> jobs_storage;
std::atomic<uint64_t> next_job_id{1};


struct JobReference {
    uint64_t id;
    unsigned priority; 
    std::chrono::steady_clock::time_point sort_key_ts; 

    struct ReadyComparator { 
        bool operator()(const JobReference& a, const JobReference& b) const {
            if (a.priority != b.priority) {
                return a.priority > b.priority;  
            }
            return a.sort_key_ts > b.sort_key_ts; 
        }
    };

    struct DelayedComparator { 
        bool operator()(const JobReference& a, const JobReference& b) const {
            return a.sort_key_ts > b.sort_key_ts; 
        }
    };
};

struct Tube {
    seastar::sstring name;
    std::priority_queue<JobReference, std::vector<JobReference>, JobReference::ReadyComparator> ready_queue;
    std::priority_queue<JobReference, std::vector<JobReference>, JobReference::DelayedComparator> delayed_queue;
    std::unordered_set<uint64_t> reserved_jobs;
    std::deque<uint64_t> buried_queue;
    std::deque<seastar::promise<std::optional<uint64_t>>> waiting_clients;
    seastar::timer<> delayed_job_poller{};

    std::atomic<uint64_t> total_jobs_ever{0}; 


    Tube(seastar::sstring n) : name(std::move(n)) {}

    void check_delayed_jobs() {
        while (!delayed_queue.empty()) {
            JobReference job_ref = delayed_queue.top(); 

            if (job_ref.sort_key_ts <= std::chrono::steady_clock::now()) {
                delayed_queue.pop(); 

                unsigned job_actual_shard = job_ref.id % seastar::smp::count;
                (void)jobs_storage.invoke_on(job_actual_shard, 
                    [job_id = job_ref.id](std::unordered_map<uint64_t, Job>& local_job_map) 
                    -> seastar::compat::optional<std::pair<unsigned, std::chrono::steady_clock::time_point>> { 
                    auto it = local_job_map.find(job_id);
                    if (it != local_job_map.end() && it->second.state == JobState::DELAYED) {
                        it->second.state = JobState::READY;
                        return std::make_pair(it->second.priority, it->second.creation_time);
                    }
                    return seastar::compat::nullopt; 
                }).then_wrapped([this, job_id = job_ref.id]
                    (seastar::future<seastar::compat::optional<std::pair<unsigned, std::chrono::steady_clock::time_point>>> f_job_details) { 
                    try {
                        seastar::compat::optional<std::pair<unsigned, std::chrono::steady_clock::time_point>> opt_details = f_job_details.get(); 
                        if (opt_details) {
                            ready_queue.push({job_id, opt_details->first, opt_details->second});
                            applog.trace("Moved job {} from delayed to ready in tube {}", job_id, name);
                            notify_a_waiting_client(); 
                        } else {
                            applog.trace("Job {} not moved from delayed (not found or state changed) in tube {}", job_id, name);
                        }
                    } catch (const std::exception& e) {
                        applog.error("Exception processing delayed job {} for tube {}: {}", job_id, name, e.what());
                    }
                });
            } else {
                delayed_job_poller.rearm(job_ref.sort_key_ts);
                return; 
            }
        }
        if (delayed_queue.empty()) {
            delayed_job_poller.cancel(); 
            applog.trace("Delayed queue for tube {} is empty, poller cancelled.", name);
        } else if (!delayed_job_poller.armed()) {
            delayed_job_poller.rearm(delayed_queue.top().sort_key_ts);
        }
    }

    void arm_delayed_job_poller_if_needed() {
        if (!delayed_queue.empty() && !delayed_job_poller.armed()) {
            applog.trace("Arming delayed job poller for tube {} for time: {}", name, delayed_queue.top().sort_key_ts.time_since_epoch().count());
            delayed_job_poller.arm(delayed_queue.top().sort_key_ts);
        }
    }

    void notify_a_waiting_client() {
        if (!waiting_clients.empty()) {
            seastar::promise<std::optional<uint64_t>> promise = std::move(waiting_clients.front());
            waiting_clients.pop_front();
            promise.set_value(std::nullopt); 
            applog.trace("Notified a waiting client for tube {}", name);
        }
    }

    ~Tube() {
        delayed_job_poller.cancel();
        for (auto&& promise : waiting_clients) {
            // Check if promise is not already fulfilled or broken before setting exception
            if (promise.get_future().available() == false) {
                 try {
                    promise.set_exception(std::runtime_error("Tube being destroyed"));
                } catch (const seastar::broken_promise& bp) {
                    applog.trace("Promise for waiting client in tube {} was already broken: {}", name, bp.what());
                } catch (...) {
                    applog.warn("Unknown exception setting broken promise for waiting client in tube {}", name);
                }
            }
        }
        waiting_clients.clear();
    }
};

seastar::sharded<std::unordered_map<seastar::sstring, Tube>> all_tubes;

unsigned get_tube_shard_id(const seastar::sstring& tube_name) {
    return std::hash<seastar::sstring>{}(tube_name) % seastar::smp::count;
}

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

seastar::sstring to_yaml_string_map(const std::unordered_map<seastar::sstring, seastar::sstring>& data) {
    std::ostringstream oss;
    oss << "---\r\n";
    for (const auto& pair : data) {
        oss << pair.first << ": " << pair.second << "\r\n";
    }
    return seastar::sstring(oss.str());
}

seastar::sstring to_yaml_string_list(const std::vector<seastar::sstring>& data) {
    std::ostringstream oss;
    oss << "---\r\n";
    for (const auto& item : data) {
        oss << "- " << item << "\r\n";
    }
    return seastar::sstring(oss.str());
}


class client_connection : public seastar::enable_shared_from_this<client_connection> {
public:
    seastar::input_stream<char> _in;
    seastar::output_stream<char> _out;
    seastar::sstring _current_put_tube = "default"; 
    std::unordered_set<seastar::sstring> _watched_tubes; 

    seastar::promise<std::optional<uint64_t>> _reserve_promise;
    seastar::timer<> _reserve_timeout_timer{};
    bool _is_reserving = false;


    client_connection(seastar::input_stream<char>&& in, seastar::output_stream<char>&& out)
        : _in(std::move(in)), _out(std::move(out)) {
        _watched_tubes.insert("default"); 
    }

    seastar::future<> process() {
        return seastar::repeat([this] {
            return _in.read_until('\n').then([this](seastar::temporary_buffer<char> line_buf) {
                if (!line_buf || line_buf.empty()) { 
                    applog.debug("Client {} closed connection (EOF or empty line)", _out.socket_address());
                    return seastar::make_ready_future<seastar::stop_iteration>(seastar::stop_iteration::yes);
                }
                seastar::sstring line(line_buf.get(), line_buf.size());
                if (!line.empty() && line.back() == '\r') {
                    line.pop_back();
                }
                
                applog.trace("Client {}: Received command line: '{}'", _out.socket_address(), line);
                return handle_command(line).then([this](bool continue_processing) {
                    if (continue_processing) {
                        return seastar::make_ready_future<seastar::stop_iteration>(seastar::stop_iteration::no);
                    } else { 
                        applog.debug("Client {}: Stopping command processing.", _out.socket_address());
                        return seastar::make_ready_future<seastar::stop_iteration>(seastar::stop_iteration::yes);
                    }
                });
            });
        }).finally([this] {
            applog.debug("Client {} connection processing finished. Closing output stream.", _out.socket_address());
            _reserve_timeout_timer.cancel(); 
            if (_is_reserving) { 
                if (_reserve_promise.get_future().available() == false) { 
                   try {
                       _reserve_promise.set_exception(std::runtime_error("Client connection closed during reserve"));
                   } catch (const seastar::broken_promise& bp) {
                       applog.trace("Reserve promise for client {} was already broken: {}", _out.socket_address(), bp.what());
                   } catch (...) {
                       applog.warn("Unknown exception setting broken promise for client {} during connection close", _out.socket_address());
                   }
                }
                _is_reserving = false;
            }
            return _out.close(); 
        });
    }

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

        if (parts.empty()) { 
            return client_error_response("INTERNAL_SERVER_ERROR_EMPTY_CMD"); 
        }

        const seastar::sstring& command = parts[0];
        applog.trace("Client {}: Parsed command: '{}'", _out.socket_address(), command);

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
        if (command == "quit") return seastar::make_ready_future<bool>(false); 

        if (command == "stats") return handle_stats(parts);
        if (command == "stats-tube") return handle_stats_tube(parts);
        if (command == "list-tubes") return handle_list_tubes(parts);
        if (command == "peek-ready") return handle_peek_ready(parts);
        if (command == "kick") return handle_kick(parts);

        if (command == "stats-job" || command == "peek" || command == "peek-delayed" ||
            command == "peek-buried" || command == "kick-job" || command == "list-tube-used" ||
            command == "list-tubes-watched" || command == "pause-tube") {
            applog.warn("Client {}: Command {} not yet implemented.", _out.socket_address(), command);
            return client_error_response("COMMAND_NOT_IMPLEMENTED");
        }


        applog.warn("Client {}: Unknown command: {}", _out.socket_address(), command);
        return client_error_response("UNKNOWN_COMMAND");
    }
    
    seastar::future<bool> client_error_response(const seastar::sstring& err_msg) {
        return _out.write(err_msg + "\r\n").then([this] { 
            return _out.flush(); 
        }).then([] {
            return true; 
        });
    }

    seastar::future<bool> handle_put(const std::vector<seastar::sstring>& parts) {
        if (parts.size() != 5) return client_error_response("BAD_FORMAT (put args)");
        try {
            unsigned priority = std::stoul(parts[1].c_str());
            unsigned delay_sec = std::stoul(parts[2].c_str());
            unsigned ttr_sec = std::stoul(parts[3].c_str());
            size_t bytes = std::stoul(parts[4].c_str());

            if (bytes > 1024 * 64) { 
                return _in.read_exactly(bytes + 2).then_wrapped([this](seastar::future<seastar::temporary_buffer<char>> f_drain) {
                    return client_error_response("JOB_TOO_BIG");
                });
            }

            return _in.read_exactly(bytes + 2).then([this, priority, delay_sec, ttr_sec, bytes](seastar::temporary_buffer<char> data_buf) {
                if (data_buf.size() != bytes + 2 || data_buf[bytes] != '\r' || data_buf[bytes+1] != '\n') {
                    applog.warn("Client {}: Put command framing error. Expected {} bytes + CRLF, got {} bytes.", _out.socket_address(), bytes, data_buf.size());
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
        auto deadline_tp = creation_tp + std::chrono::seconds(delay_sec); 

        return jobs_storage.invoke_on(job_storage_shard, 
            [job_id, data = std::move(data), priority, delay_sec, ttr_sec, tube_name = _current_put_tube, creation_tp, deadline_tp]
            (std::unordered_map<uint64_t, Job>& local_job_map) mutable {
            
            Job new_job(job_id, std::move(data), priority,
                        std::chrono::seconds(delay_sec),
                        std::chrono::seconds(ttr_sec),
                        tube_name);
            new_job.creation_time = creation_tp; 

            if (delay_sec > 0) {
                new_job.state = JobState::DELAYED;
                new_job.deadline_time = deadline_tp;
            } else {
                new_job.state = JobState::READY;
            }
            local_job_map.emplace(job_id, std::move(new_job));
            applog.trace("Stored job {} on shard {}", job_id, seastar::this_shard_id());
        }).then([this, job_id, delay_sec, target_tube_shard, priority, creation_tp, deadline_tp] {
            return all_tubes.invoke_on(target_tube_shard, 
                [job_id, delay_sec, tube_name = _current_put_tube, priority, creation_tp, deadline_tp]
                (std::unordered_map<seastar::sstring, Tube>& local_tubes_map) {
                
                Tube& tube = get_or_create_tube_on_current_shard(local_tubes_map, tube_name);
                tube.total_jobs_ever.fetch_add(1, std::memory_order_relaxed); 
                if (delay_sec > 0) {
                    tube.delayed_queue.push({job_id, priority, deadline_tp});
                    tube.arm_delayed_job_poller_if_needed();
                    applog.trace("Added job {} to delayed queue of tube '{}'", job_id, tube_name);
                } else {
                    tube.ready_queue.push({job_id, priority, creation_tp}); 
                    tube.notify_a_waiting_client();
                    applog.trace("Added job {} to ready queue of tube '{}'", job_id, tube_name);
                }
            });
        }).then([this, job_id] {
            return _out.write(seastar::format("INSERTED {}\r\n", job_id))
                       .then([this]{ return _out.flush(); })
                       .then([]{ return true; });
        }).handle_exception_type([this](const std::exception& e) {
            applog.error("Client {}: Exception during 'put' operation: {}", _out.socket_address(), e.what());
            return client_error_response("INTERNAL_SERVER_ERROR (put)");
        });
    }
    
    seastar::future<bool> handle_reserve_base(const std::vector<seastar::sstring>& parts, std::optional<bool> with_timeout_flag) {
        if (_is_reserving) { 
            applog.warn("Client {}: New reserve command while previous one active. Cancelling old.", _out.socket_address());
            _reserve_timeout_timer.cancel();
             if (_reserve_promise.get_future().available() == false) {
                try {
                    _reserve_promise.set_exception(std::runtime_error("Superseded by new reserve command"));
                } catch (const seastar::broken_promise& bp) {
                     applog.trace("Superseded reserve promise for client {} was already broken: {}", _out.socket_address(), bp.what());
                } catch (...) {
                    applog.warn("Unknown exception setting broken promise for superseded reserve on client {}", _out.socket_address());
                }
            }
            _is_reserving = false; 
        }

        std::optional<std::chrono::seconds> timeout_duration;
        if (with_timeout_flag.has_value()) { 
            if (parts.size() != 2) return client_error_response("BAD_FORMAT (reserve-with-timeout args)");
            try {
                timeout_duration = std::chrono::seconds(std::stoul(parts[1].c_str()));
            } catch (const std::exception& e) {
                return client_error_response("BAD_FORMAT (reserve-with-timeout parse)");
            }
        } else { 
            if (parts.size() != 1) return client_error_response("BAD_FORMAT (reserve args)");
        }
        
        _is_reserving = true;
        _reserve_promise = seastar::promise<std::optional<uint64_t>>(); 

        do_try_reserve_round(); 

        seastar::future<std::optional<uint64_t>> future_to_wait_on = _reserve_promise.get_future();

        if (timeout_duration) {
            if (*timeout_duration == std::chrono::seconds(0)) { 
            } else {
                _reserve_timeout_timer.set_callback([this] {
                    if (_is_reserving) { 
                        applog.trace("Client {}: reserve timed out.", _out.socket_address());
                         if (_reserve_promise.get_future().available() == false) {
                            try {
                                _reserve_promise.set_value(std::nullopt); 
                            } catch (const seastar::broken_promise& bp) {
                                applog.trace("Reserve promise (timeout) for client {} was already broken: {}", _out.socket_address(), bp.what());
                            } catch (...) {
                                applog.warn("Unknown exception setting reserve promise (timeout) for client {}", _out.socket_address());
                            }
                        }
                        _is_reserving = false; 
                    }
                });
                _reserve_timeout_timer.arm(*timeout_duration);
            }
        }

        return future_to_wait_on.then_wrapped([this, timeout_val = timeout_duration] 
            (seastar::future<std::optional<uint64_t>> f_job_id_opt) {
            _is_reserving = false; 
            _reserve_timeout_timer.cancel(); 

            try {
                std::optional<uint64_t> job_id_opt = f_job_id_opt.get();
                if (job_id_opt) { 
                    uint64_t job_id = *job_id_opt;
                    unsigned job_storage_shard = job_id % seastar::smp::count;
                    return jobs_storage.invoke_on(job_storage_shard, 
                        [job_id](const std::unordered_map<uint64_t, Job>& local_job_map) -> seastar::compat::optional<seastar::sstring> { 
                        auto it = local_job_map.find(job_id);
                        if (it != local_job_map.end()) return it->second.body;
                        return seastar::compat::nullopt; 
                    }).then([this, job_id](seastar::compat::optional<seastar::sstring> body_opt) { 
                        if (body_opt) {
                            return _out.write(seastar::format("RESERVED {} {}\r\n{}\r\n", job_id, body_opt->length(), *body_opt))
                                       .then([this]{ return _out.flush(); }).then([]{ return true; });
                        }
                        applog.error("Client {}: Job {} reserved but not found in storage for body fetch.", _out.socket_address(), job_id);
                        return client_error_response("INTERNAL_SERVER_ERROR (reserve body fetch)");
                    });
                } else { 
                    if (timeout_val && *timeout_val == std::chrono::seconds(0)) {
                        return _out.write("DEADLINE_SOON\r\n").then([this]{ return _out.flush(); }).then([]{ return true; });
                    }
                    return _out.write("TIMED_OUT\r\n").then([this]{ return _out.flush(); }).then([]{ return true; });
                }
            } catch (const std::exception& e) {
                applog.error("Client {}: Exception while finalizing reserve: {}", _out.socket_address(), e.what());
                // Check if the promise was already broken, e.g., by client disconnect
                if (dynamic_cast<const seastar::broken_promise*>(&e)) {
                     applog.info("Client {}: Reserve finalization failed because promise was broken (likely client disconnect).", _out.socket_address());
                     // No need to send error response if client is gone.
                     return seastar::make_ready_future<bool>(true); // Indicate processing should continue (or stop if quit)
                }
                return client_error_response("INTERNAL_SERVER_ERROR (reserve finalization)");
            }
        });
    }

    // MODIFIED: Corrected handling of _reserve_promise for multiple watched tubes
    void do_try_reserve_round() {
        if (!_is_reserving) return; 

        seastar::shared_ptr<std::vector<seastar::sstring>> watched_tubes_copy = 
            seastar::make_shared<std::vector<seastar::sstring>>(_watched_tubes.begin(), _watched_tubes.end());

        if (watched_tubes_copy->empty()) { 
             if (_is_reserving && _reserve_promise.get_future().available() == false) {
                try {
                    _reserve_promise.set_value(std::nullopt); 
                } catch (const seastar::broken_promise&) {/*ignore*/}
            }
            _is_reserving = false;
            return;
        }
        
        seastar::shared_ptr<size_t> current_tube_idx = seastar::make_shared<size_t>(0);
        seastar::shared_ptr<std::optional<uint64_t>> found_job_id = seastar::make_shared<std::optional<uint64_t>>(std::nullopt);

        // This flag ensures the _reserve_promise is moved to a waiting_clients list at most once.
        auto promise_registered_for_waiting_flag = seastar::make_shared<std::atomic_bool>(false);

        seastar::future<> loop_fut = seastar::do_until(
            [this, watched_tubes_copy, current_tube_idx, found_job_id] { 
                return *current_tube_idx >= watched_tubes_copy->size() || !_is_reserving || found_job_id->has_value(); 
            },
            [this, watched_tubes_copy, current_tube_idx, found_job_id] { 
                const seastar::sstring& tube_name = (*watched_tubes_copy)[*current_tube_idx];
                (*current_tube_idx)++; 

                unsigned target_tube_shard = get_tube_shard_id(tube_name);
                return all_tubes.invoke_on(target_tube_shard, 
                    [this, tube_name, found_job_id](std::unordered_map<seastar::sstring, Tube>& local_tubes_map) -> seastar::future<> {
                    if (!_is_reserving || found_job_id->has_value()) return seastar::make_ready_future<>();

                    auto tube_it = local_tubes_map.find(tube_name);
                    if (tube_it == local_tubes_map.end() || tube_it->second.ready_queue.empty()) {
                        return seastar::make_ready_future<>(); 
                    }
                    Tube& tube = tube_it->second;
                    JobReference job_ref = tube.ready_queue.top(); 

                    uint64_t job_id = job_ref.id;
                    unsigned job_storage_shard = job_id % seastar::smp::count;

                    return jobs_storage.invoke_on(job_storage_shard, 
                        [job_id, tube_name_copy = tube.name ]
                        (std::unordered_map<uint64_t, Job>& local_job_map) -> bool {
                        auto it = local_job_map.find(job_id);
                        if (it != local_job_map.end() && it->second.state == JobState::READY) {
                            Job& job = it->second;
                            job.state = JobState::RESERVED;
                            job.deadline_time = std::chrono::steady_clock::now() + job.ttr_us;
                            
                            job.ttr_timer.set_callback([job_id_cb = job_id, tube_name_cb = tube_name_copy, 
                                                        prio_cb = job.priority, creation_time_cb = job.creation_time] {
                                applog.debug("TTR expired for job {}", job_id_cb);
                                unsigned job_shard_cb = job_id_cb % seastar::smp::count;
                                unsigned original_tube_shard_cb = get_tube_shard_id(tube_name_cb);

                                (void)jobs_storage.invoke_on(job_shard_cb, [job_id_cb](std::unordered_map<uint64_t, Job>& j_map) {
                                    auto j_it = j_map.find(job_id_cb);
                                    if (j_it != j_map.end() && j_it->second.state == JobState::RESERVED) {
                                        j_it->second.state = JobState::READY;
                                    } 
                                }).then([original_tube_shard_cb, job_id_cb, tube_name_cb, prio_cb, creation_time_cb] {
                                    (void)all_tubes.invoke_on(original_tube_shard_cb, 
                                        [job_id_cb, tube_name_cb, prio_cb, creation_time_cb]
                                        (std::unordered_map<seastar::sstring, Tube>& tubes_map_cb){
                                        auto tube_it_cb = tubes_map_cb.find(tube_name_cb);
                                        if (tube_it_cb != tubes_map_cb.end()) {
                                            Tube& t = tube_it_cb->second;
                                            if (t.reserved_jobs.erase(job_id_cb) > 0) { 
                                                t.ready_queue.push({job_id_cb, prio_cb, creation_time_cb});
                                                applog.debug("Job {} (TTR expired) moved back to ready in tube {}", job_id_cb, tube_name_cb);
                                                t.notify_a_waiting_client();
                                            }
                                        }
                                    });
                                });
                            }); 
                            job.ttr_timer.arm(job.deadline_time);
                            return true; 
                        }
                        return false; 
                    }).then_wrapped([this, job_id, &tube, found_job_id] 
                        (seastar::future<bool> f_reserved_on_job_shard) {
                        if (!_is_reserving || found_job_id->has_value()) return seastar::make_ready_future<>();
                        try {
                            if (f_reserved_on_job_shard.get()) { 
                                tube.ready_queue.pop(); 
                                tube.reserved_jobs.insert(job_id);
                                applog.trace("Client {} reserved job {} from tube {}", _out.socket_address(), job_id, tube.name);
                                
                                *found_job_id = job_id; 
                                if (_is_reserving && _reserve_promise.get_future().available() == false) {
                                     try {
                                        _reserve_promise.set_value(job_id); 
                                     } catch (const seastar::broken_promise&) {/*ignore*/}
                                }
                                return seastar::make_ready_future<>();
                            }
                        } catch (const std::exception& e) {
                             applog.error("Client {}: Exception checking reservation status for job {} from tube {}: {}", _out.socket_address(), job_id, tube.name, e.what());
                        }
                        return seastar::make_ready_future<>();
                    });
                }).then_wrapped([this](seastar::future<> f_job_opt) {
                    if (f_job_opt.failed()) {
                        applog.error("Client {}: Failure in reserve loop invoke_on: {}", _out.socket_address(), f_job_opt.get_exception());
                    }
                }); 
            }); 

        (void)loop_fut.then_wrapped([this, watched_tubes_copy, found_job_id, promise_registered_for_waiting_flag](seastar::future<> f_loop_done) {
            if (f_loop_done.failed()) {
                 applog.error("Client {}: Reserve loop itself failed: {}", _out.socket_address(), f_loop_done.get_exception());
                 if(_is_reserving && _reserve_promise.get_future().available() == false) {
                    try {
                         _reserve_promise.set_exception(f_loop_done.get_exception());
                    } catch (const seastar::broken_promise&) {/*ignore*/}
                 }
                 _is_reserving = false;
                 return;
            }

            if (_is_reserving && !found_job_id->has_value()) { 
                bool has_timeout = _reserve_timeout_timer.armed() || 
                                   (_reserve_timeout_timer.get_timeout() == std::chrono::steady_clock::duration::zero() && 
                                    !_reserve_timeout_timer.armed()); // Specifically for timeout 0 that wasn't armed
                
                if (!has_timeout) { 
                    applog.trace("Client {}: No job found, will wait indefinitely on watched tubes.", _out.socket_address());
                    
                    for (const auto& tube_name : *watched_tubes_copy) {
                        if (!_is_reserving) break; 
                        // If promise already registered by another tube for this reserve op, stop.
                        if (promise_registered_for_waiting_flag->load(std::memory_order_acquire)) break;

                        unsigned target_tube_shard = get_tube_shard_id(tube_name);
                        (void)all_tubes.invoke_on(target_tube_shard, 
                            [this, tube_name, promise_registered_for_waiting_flag](std::unordered_map<seastar::sstring, Tube>& local_tubes_map){
                            if (!_is_reserving) return; 
                            
                            bool expected_flag_val = false;
                            // Attempt to be the one to register the promise
                            if (promise_registered_for_waiting_flag->compare_exchange_strong(expected_flag_val, true, std::memory_order_acq_rel)) {
                                Tube& tube = get_or_create_tube_on_current_shard(local_tubes_map, tube_name);
                                if (_reserve_promise.get_future().available() == false) {
                                   try {
                                       tube.waiting_clients.push_back(std::move(_reserve_promise)); 
                                       applog.trace("Client {} added to waiting list for tube {}", _out.socket_address(), tube_name);
                                   } catch (const seastar::broken_promise&) {
                                       // Should not happen if available() is false, but defensive.
                                       applog.warn("Client {}: Tried to move an already broken promise to waiting list of tube {}", _out.socket_address(), tube_name);
                                       promise_registered_for_waiting_flag->store(false, std::memory_order_release); // Allow retry by another tube if this failed.
                                   }
                                } else {
                                     // Promise already fulfilled/broken, reset flag to allow another tube if needed (though unlikely path)
                                     promise_registered_for_waiting_flag->store(false, std::memory_order_release);
                                }
                            }
                         });
                         // Small yield to allow other shards/fibers to run, potentially processing the invoke_on
                         // This might help the atomic flag be seen by other iterations sooner.
                         // seastar::yield().get(); // Consider if this is beneficial or adds too much overhead.
                    }
                     // After attempting to register with all tubes, if still not registered and promise is pending,
                     // it means all invoke_on calls either found the flag already set by another, or failed to set it.
                     // This could happen if all tubes were new and the get_or_create didn't complete in time for the flag check.
                     // This is a fallback to prevent hanging.
                     if (_is_reserving && !promise_registered_for_waiting_flag->load(std::memory_order_acquire) && _reserve_promise.get_future().available() == false) {
                        try {
                            _reserve_promise.set_value(std::nullopt); 
                        } catch (const seastar::broken_promise&) {/*ignore*/}
                        _is_reserving = false; // No longer actively reserving if we fulfill it here
                        applog.warn("Client {}: Reserve promise not moved to any waiting list after loop, fulfilling with no job.", _out.socket_address());
                    }

                } else if (_is_reserving && _reserve_promise.get_future().available() == false) {
                    try {
                        _reserve_promise.set_value(std::nullopt);
                    } catch (const seastar::broken_promise&) {/*ignore*/}
                }
            } 
        });
    }


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
        return jobs_storage.invoke_on(job_storage_shard, 
            [job_id](std::unordered_map<uint64_t, Job>& local_job_map) -> seastar::compat::optional<seastar::sstring> { 
            auto it = local_job_map.find(job_id);
            if (it == local_job_map.end()) {
                return seastar::compat::nullopt; 
            }
            seastar::sstring tube_name = it->second.tube_name;
            it->second.ttr_timer.cancel(); 
            local_job_map.erase(it);
            applog.trace("Deleted job {} from storage on shard {}", job_id, seastar::this_shard_id());
            return tube_name;
        }).then([this, job_id](seastar::compat::optional<seastar::sstring> tube_name_opt) { 
            if (!tube_name_opt) {
                return _out.write("NOT_FOUND\r\n").then([this]{ return _out.flush(); }).then([]{ return true; });
            }
            unsigned target_tube_shard = get_tube_shard_id(*tube_name_opt);
            return all_tubes.invoke_on(target_tube_shard, 
                [job_id, tube_name = *tube_name_opt](std::unordered_map<seastar::sstring, Tube>& local_tubes_map){
                auto tube_it = local_tubes_map.find(tube_name);
                if (tube_it != local_tubes_map.end()) {
                    if (tube_it->second.reserved_jobs.erase(job_id) > 0) {
                         applog.trace("Removed job {} from reserved set of tube '{}'", job_id, tube_name);
                    }
                    auto& bq = tube_it->second.buried_queue;
                    bq.erase(std::remove(bq.begin(), bq.end(), job_id), bq.end());
                }
            }).then([this] {
                return _out.write("DELETED\r\n").then([this]{ return _out.flush(); }).then([]{ return true; });
            });
        });
    }
    
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

        return jobs_storage.invoke_on(job_storage_shard, 
            [job_id, new_priority, delay_sec, new_deadline_tp] 
            (std::unordered_map<uint64_t, Job>& local_job_map) 
            -> std::tuple<bool, JobState, seastar::sstring, std::chrono::steady_clock::time_point, unsigned> { 
            auto it = local_job_map.find(job_id);
            if (it == local_job_map.end()) {
                return {false, JobState::INVALID, "", {}, 0}; 
            }
            if (it->second.state != JobState::RESERVED) {
                return {false, it->second.state, "", {}, 0}; 
            }
            
            Job& job = it->second;
            job.ttr_timer.cancel();
            job.priority = new_priority;
            seastar::sstring tube_name = job.tube_name;
            std::chrono::steady_clock::time_point sort_key_ts;

            if (delay_sec > 0) {
                job.state = JobState::DELAYED;
                job.deadline_time = new_deadline_tp;
                sort_key_ts = new_deadline_tp; 
            } else {
                job.state = JobState::READY;
                sort_key_ts = job.creation_time; 
            }
            applog.trace("Releasing job {} on shard {}, new state: {}, new prio: {}", job_id, seastar::this_shard_id(), (int)job.state, new_priority);
            return {true, JobState::RESERVED , tube_name, sort_key_ts, job.priority};
        }).then([this, job_id, delay_sec](std::tuple<bool, JobState, seastar::sstring, std::chrono::steady_clock::time_point, unsigned> result) {
            bool success = std::get<0>(result);
            JobState old_job_state = std::get<1>(result);
            seastar::sstring tube_name = std::get<2>(result);
            std::chrono::steady_clock::time_point sort_key_ts = std::get<3>(result);
            unsigned final_priority = std::get<4>(result);

            if (!success) {
                return _out.write((old_job_state == JobState::INVALID ? "NOT_FOUND\r\n" : "NOT_FOUND\r\n")) 
                           .then([this]{ return _out.flush(); }).then([]{ return true; });
            }

            unsigned target_tube_shard = get_tube_shard_id(tube_name);
            return all_tubes.invoke_on(target_tube_shard, 
                [job_id, tube_name, delay_sec, sort_key_ts, final_priority]
                (std::unordered_map<seastar::sstring, Tube>& local_tubes_map){
                auto tube_it = local_tubes_map.find(tube_name);
                if (tube_it == local_tubes_map.end()) return; 
                
                Tube& tube = tube_it->second;
                if (tube.reserved_jobs.erase(job_id) == 0) {
                    applog.warn("Job {} was marked RESERVED but not found in tube {}'s reserved set during release.", job_id, tube_name);
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

    seastar::future<bool> handle_bury(const std::vector<seastar::sstring>& parts) {
        if (parts.size() != 3) return client_error_response("BAD_FORMAT (bury args)");
        try {
            uint64_t job_id = std::stoull(parts[1].c_str());
            unsigned priority = std::stoul(parts[2].c_str()); 
            return do_bury(job_id, priority);
        } catch (const std::exception& e) {
            return client_error_response("BAD_FORMAT (bury params parse)");
        }
    }

    seastar::future<bool> do_bury(uint64_t job_id, unsigned new_priority) {
        unsigned job_storage_shard = job_id % seastar::smp::count;
        return jobs_storage.invoke_on(job_storage_shard, 
            [job_id, new_priority] (std::unordered_map<uint64_t, Job>& local_job_map) 
            -> std::tuple<bool, JobState, seastar::sstring> { 
            auto it = local_job_map.find(job_id);
            if (it == local_job_map.end()) {
                return {false, JobState::INVALID, ""};
            }
            if (it->second.state != JobState::RESERVED) {
                return {false, it->second.state, ""}; 
            }
            Job& job = it->second;
            job.ttr_timer.cancel();
            job.state = JobState::BURIED;
            job.priority = new_priority; 
            applog.trace("Burying job {} on shard {}, new prio: {}", job_id, seastar::this_shard_id(), new_priority);
            return {true, JobState::RESERVED, job.tube_name};
        }).then([this, job_id](std::tuple<bool, JobState, seastar::sstring> result) {
            bool success = std::get<0>(result);
            JobState old_job_state = std::get<1>(result);
            seastar::sstring tube_name = std::get<2>(result);

            if (!success) {
                 return _out.write("NOT_FOUND\r\n").then([this]{ return _out.flush(); }).then([]{ return true; }); 
            }
            unsigned target_tube_shard = get_tube_shard_id(tube_name);
            return all_tubes.invoke_on(target_tube_shard, 
                [job_id, tube_name](std::unordered_map<seastar::sstring, Tube>& local_tubes_map){
                auto tube_it = local_tubes_map.find(tube_name);
                if (tube_it == local_tubes_map.end()) return;
                Tube& tube = tube_it->second;
                tube.reserved_jobs.erase(job_id);
                tube.buried_queue.push_back(job_id); 
                applog.trace("Moved job {} to buried queue of tube '{}'", job_id, tube_name);
            }).then([this] {
                return _out.write("BURIED\r\n").then([this]{ return _out.flush(); }).then([]{ return true; });
            });
        });
    }
    
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
                return false; 
            }
            Job& job = it->second;
            job.deadline_time = std::chrono::steady_clock::now() + job.ttr_us;
            job.ttr_timer.rearm(job.deadline_time); 
            applog.trace("Touched job {}, new TTR deadline on shard {}", job_id, seastar::this_shard_id());
            return true;
        }).then([this, job_id](bool success) {
            if (!success) {
                return _out.write("NOT_FOUND\r\n").then([this]{ return _out.flush(); }).then([]{ return true; });
            }
            return _out.write("TOUCHED\r\n").then([this]{ return _out.flush(); }).then([]{ return true; });
        });
    }

    seastar::future<bool> handle_use(const std::vector<seastar::sstring>& parts) {
        if (parts.size() != 2) return client_error_response("BAD_FORMAT (use args)");
        _current_put_tube = parts[1];
        return _out.write(seastar::format("USING {}\r\n", _current_put_tube))
                   .then([this]{ return _out.flush(); }).then([]{ return true; });
    }

    seastar::future<bool> handle_watch(const std::vector<seastar::sstring>& parts) {
        if (parts.size() != 2) return client_error_response("BAD_FORMAT (watch args)");
        _watched_tubes.insert(parts[1]);
        return _out.write(seastar::format("WATCHING {}\r\n", _watched_tubes.size()))
                   .then([this]{ return _out.flush(); }).then([]{ return true; });
    }

    seastar::future<bool> handle_ignore(const std::vector<seastar::sstring>& parts) {
        if (parts.size() != 2) return client_error_response("BAD_FORMAT (ignore args)");
        if (_watched_tubes.size() > 1) { 
            _watched_tubes.erase(parts[1]);
        } else if (_watched_tubes.count(parts[1])) { 
             return _out.write("NOT_IGNORED\r\n").then([this]{ return _out.flush(); }).then([]{ return true; });
        }
        return _out.write(seastar::format("WATCHING {}\r\n", _watched_tubes.size()))
                   .then([this]{ return _out.flush(); }).then([]{ return true; });
    }

    seastar::future<bool> handle_stats(const std::vector<seastar::sstring>& parts) {
        if (parts.size() != 1) return client_error_response("BAD_FORMAT (stats args)");
        return do_stats();
    }

    seastar::future<bool> do_stats() {
        struct GlobalStats {
            uint64_t current_jobs_ready = 0;
            uint64_t current_jobs_reserved = 0;
            uint64_t current_jobs_delayed = 0;
            uint64_t current_jobs_buried = 0;
            uint64_t current_tubes = 0;
            uint64_t total_jobs_ever = 0; 
        };

        return seastar::map_reduce(all_tubes.begin(), all_tubes.end(),
            [](const std::unordered_map<seastar::sstring, Tube>& local_tubes_map) {
                GlobalStats local_stats;
                local_stats.current_tubes = local_tubes_map.size();
                for(const auto& pair : local_tubes_map) {
                    const Tube& tube = pair.second;
                    local_stats.current_jobs_ready += tube.ready_queue.size();
                    local_stats.current_jobs_reserved += tube.reserved_jobs.size();
                    local_stats.current_jobs_delayed += tube.delayed_queue.size();
                    local_stats.current_jobs_buried += tube.buried_queue.size();
                }
                return local_stats;
            },
            GlobalStats{}, 
            [](GlobalStats acc, GlobalStats shard_stats) {
                acc.current_jobs_ready += shard_stats.current_jobs_ready;
                acc.current_jobs_reserved += shard_stats.current_jobs_reserved;
                acc.current_jobs_delayed += shard_stats.current_jobs_delayed;
                acc.current_jobs_buried += shard_stats.current_jobs_buried;
                acc.current_tubes += shard_stats.current_tubes;
                return acc;
            }
        ).then([this](GlobalStats stats) {
            stats.total_jobs_ever = next_job_id.load(std::memory_order_relaxed) -1;
            if (stats.total_jobs_ever < 0) stats.total_jobs_ever = 0;


            std::unordered_map<seastar::sstring, seastar::sstring> data;
            data["current-jobs-urgent"] = "0"; 
            data["current-jobs-ready"] = std::to_string(stats.current_jobs_ready);
            data["current-jobs-reserved"] = std::to_string(stats.current_jobs_reserved);
            data["current-jobs-delayed"] = std::to_string(stats.current_jobs_delayed);
            data["current-jobs-buried"] = std::to_string(stats.current_jobs_buried);
            data["cmd-put"] = "0"; 
            data["current-tubes"] = std::to_string(stats.current_tubes);
            data["current-connections"] = "1"; 
            data["current-producers"] = "0"; 
            data["current-workers"] = "0";   
            data["current-waiting"] = "0";   
            data["total-jobs"] = std::to_string(stats.total_jobs_ever);
            data["uptime"] = "0"; 

            seastar::sstring yaml_data = to_yaml_string_map(data);
            return _out.write(seastar::format("OK {}\r\n{}", yaml_data.length(), yaml_data))
                       .then([this]{ return _out.flush(); })
                       .then([]{ return true; });
        });
    }

    seastar::future<bool> handle_stats_tube(const std::vector<seastar::sstring>& parts) {
        if (parts.size() != 2) return client_error_response("BAD_FORMAT (stats-tube args)");
        return do_stats_tube(parts[1]);
    }

    seastar::future<bool> do_stats_tube(const seastar::sstring& tube_name) {
        unsigned target_tube_shard = get_tube_shard_id(tube_name);
        return all_tubes.invoke_on(target_tube_shard, 
            [tube_name](const std::unordered_map<seastar::sstring, Tube>& local_tubes_map) 
            -> seastar::compat::optional<std::unordered_map<seastar::sstring, seastar::sstring>> { 
            auto it = local_tubes_map.find(tube_name);
            if (it == local_tubes_map.end()) {
                return seastar::compat::nullopt; 
            }
            const Tube& tube = it->second;
            std::unordered_map<seastar::sstring, seastar::sstring> data;
            data["name"] = tube.name;
            data["current-jobs-urgent"] = "0"; 
            data["current-jobs-ready"] = std::to_string(tube.ready_queue.size());
            data["current-jobs-reserved"] = std::to_string(tube.reserved_jobs.size());
            data["current-jobs-delayed"] = std::to_string(tube.delayed_queue.size());
            data["current-jobs-buried"] = std::to_string(tube.buried_queue.size());
            data["total-jobs"] = std::to_string(tube.total_jobs_ever.load(std::memory_order_relaxed)); 
            data["current-using"] = "0"; 
            data["current-watching"] = "0"; 
            data["current-waiting"] = std::to_string(tube.waiting_clients.size());
            data["cmd-pause-tube"] = "0";
            data["pause"] = "0";
            data["pause-time-left"] = "0";

            return data;
        }).then([this](seastar::compat::optional<std::unordered_map<seastar::sstring, seastar::sstring>> opt_data) { 
            if (!opt_data) {
                return _out.write("NOT_FOUND\r\n").then([this]{ return _out.flush(); }).then([]{ return true; });
            }
            seastar::sstring yaml_data = to_yaml_string_map(*opt_data);
            return _out.write(seastar::format("OK {}\r\n{}", yaml_data.length(), yaml_data))
                       .then([this]{ return _out.flush(); })
                       .then([]{ return true; });
        });
    }

    seastar::future<bool> handle_list_tubes(const std::vector<seastar::sstring>& parts) {
        if (parts.size() != 1) return client_error_response("BAD_FORMAT (list-tubes args)");
        return do_list_tubes();
    }

    seastar::future<bool> do_list_tubes() {
        return seastar::map_reduce(all_tubes.begin(), all_tubes.end(),
            [](const std::unordered_map<seastar::sstring, Tube>& local_tubes_map) {
                std::vector<seastar::sstring> names;
                for(const auto& pair : local_tubes_map) {
                    names.push_back(pair.first);
                }
                return names;
            },
            std::vector<seastar::sstring>{},
            [](std::vector<seastar::sstring> acc, const std::vector<seastar::sstring>& shard_names) {
                acc.insert(acc.end(), shard_names.begin(), shard_names.end());
                return acc;
            }
        ).then([this](std::vector<seastar::sstring> all_tube_names) {
            std::sort(all_tube_names.begin(), all_tube_names.end());
            seastar::sstring yaml_data = to_yaml_string_list(all_tube_names);
            return _out.write(seastar::format("OK {}\r\n{}", yaml_data.length(), yaml_data))
                       .then([this]{ return _out.flush(); })
                       .then([]{ return true; });
        });
    }

    seastar::future<bool> handle_peek_ready(const std::vector<seastar::sstring>& parts) {
        if (parts.size() != 1) return client_error_response("BAD_FORMAT (peek-ready args)");
        return do_peek_ready();
    }

    seastar::future<bool> do_peek_ready() {
        seastar::shared_ptr<std::vector<seastar::sstring>> watched_tubes_copy = 
            seastar::make_shared<std::vector<seastar::sstring>>(_watched_tubes.begin(), _watched_tubes.end());

        if (watched_tubes_copy->empty()) {
            return _out.write("NOT_FOUND\r\n").then([this]{ return _out.flush(); }).then([]{ return true; });
        }

        struct CandidateJob {
            std::optional<JobReference> ref;
            seastar::sstring tube_name; 

            bool has_value() const { return ref.has_value(); }
            
            void update_if_better(const JobReference& new_ref, const seastar::sstring& current_tube_name) {
                if (!ref || JobReference::ReadyComparator()(new_ref, *ref)) { 
                    ref = new_ref;
                    tube_name = current_tube_name;
                }
            }
        };
        
        return seastar::map_reduce(all_tubes.begin(), all_tubes.end(),
            [watched_tubes_copy](const std::unordered_map<seastar::sstring, Tube>& local_tubes_map) {
                CandidateJob local_best_candidate;
                for (const auto& tube_name : *watched_tubes_copy) {
                    auto tube_it = local_tubes_map.find(tube_name);
                    if (tube_it != local_tubes_map.end() && !tube_it->second.ready_queue.empty()) {
                        local_best_candidate.update_if_better(tube_it->second.ready_queue.top(), tube_name);
                    }
                }
                return local_best_candidate;
            },
            CandidateJob{}, 
            [](CandidateJob acc, const CandidateJob& shard_best) {
                if (shard_best.has_value()) {
                    acc.update_if_better(*shard_best.ref, shard_best.tube_name);
                }
                return acc;
            }
        ).then([this](CandidateJob best_candidate) {
            if (!best_candidate.has_value()) {
                return _out.write("NOT_FOUND\r\n").then([this]{ return _out.flush(); }).then([]{ return true; });
            }
            uint64_t job_id = best_candidate.ref->id;
            unsigned job_storage_shard = job_id % seastar::smp::count;
            return jobs_storage.invoke_on(job_storage_shard, 
                [job_id](const std::unordered_map<uint64_t, Job>& local_job_map) -> seastar::compat::optional<seastar::sstring> { 
                auto it = local_job_map.find(job_id);
                if (it != local_job_map.end() && it->second.state == JobState::READY) return it->second.body;
                return seastar::compat::nullopt; 
            }).then([this, job_id](seastar::compat::optional<seastar::sstring> body_opt) { 
                if (body_opt) {
                    return _out.write(seastar::format("FOUND {} {}\r\n{}\r\n", job_id, body_opt->length(), *body_opt))
                               .then([this]{ return _out.flush(); }).then([]{ return true; });
                }
                return _out.write("NOT_FOUND\r\n").then([this]{ return _out.flush(); }).then([]{ return true; });
            });
        });
    }

    seastar::future<bool> handle_kick(const std::vector<seastar::sstring>& parts) {
        if (parts.size() != 2) return client_error_response("BAD_FORMAT (kick args)");
        try {
            unsigned bound = std::stoul(parts[1].c_str());
            return do_kick(bound);
        } catch (const std::exception& e) {
            return client_error_response("BAD_FORMAT (kick bound parse)");
        }
    }

    seastar::future<bool> do_kick(unsigned bound) {
        if (bound == 0) { 
            return _out.write("KICKED 0\r\n").then([this]{ return _out.flush(); }).then([]{ return true; });
        }

        unsigned target_tube_shard = get_tube_shard_id(_current_put_tube); 
        
        return all_tubes.invoke_on(target_tube_shard,
            [bound, tube_name = _current_put_tube]
            (std::unordered_map<seastar::sstring, Tube>& local_tubes_map) 
            -> std::vector<uint64_t> { 
                auto tube_it = local_tubes_map.find(tube_name);
                if (tube_it == local_tubes_map.end() || tube_it->second.buried_queue.empty()) {
                    return {};
                }
                Tube& tube = tube_it->second;
                std::vector<uint64_t> ids_to_kick;
                for (unsigned i = 0; i < bound && !tube.buried_queue.empty(); ++i) {
                    ids_to_kick.push_back(tube.buried_queue.front());
                    tube.buried_queue.pop_front(); 
                }
                return ids_to_kick;
        }).then([this, target_tube_shard, tube_name = _current_put_tube] (std::vector<uint64_t> job_ids_to_kick) {
            if (job_ids_to_kick.empty()) {
                return _out.write("KICKED 0\r\n").then([this]{ return _out.flush(); }).then([]{ return true; });
            }

            std::vector<seastar::future<>> kick_futures;
            seastar::shared_ptr<std::atomic<unsigned>> kicked_count = seastar::make_shared<std::atomic<unsigned>>(0);

            for (uint64_t job_id : job_ids_to_kick) {
                unsigned job_storage_shard = job_id % seastar::smp::count;
                kick_futures.push_back(
                    jobs_storage.invoke_on(job_storage_shard, 
                        [job_id](std::unordered_map<uint64_t, Job>& local_job_map) 
                        -> seastar::compat::optional<std::pair<unsigned, std::chrono::steady_clock::time_point>> { 
                        auto it = local_job_map.find(job_id);
                        if (it != local_job_map.end() && it->second.state == JobState::BURIED) {
                            it->second.state = JobState::READY;
                            return std::make_pair(it->second.priority, it->second.creation_time);
                        }
                        return seastar::compat::nullopt;
                    }).then([this, job_id, target_tube_shard, tube_name, kicked_count]
                           (seastar::compat::optional<std::pair<unsigned, std::chrono::steady_clock::time_point>> opt_details) {
                        if (opt_details) {
                            return all_tubes.invoke_on(target_tube_shard,
                                [job_id, tube_name, details = *opt_details, kicked_count]
                                (std::unordered_map<seastar::sstring, Tube>& local_tubes_map) {
                                auto tube_it = local_tubes_map.find(tube_name);
                                if (tube_it != local_tubes_map.end()) {
                                    Tube& tube = tube_it->second;
                                    tube.ready_queue.push({job_id, details.first, details.second});
                                    tube.notify_a_waiting_client();
                                    kicked_count->fetch_add(1, std::memory_order_relaxed);
                                    applog.trace("Kicked job {} to ready in tube {}", job_id, tube_name);
                                }
                            });
                        }
                        return seastar::make_ready_future<>(); 
                    })
                );
            }

            return seastar::when_all_succeed(kick_futures.begin(), kick_futures.end())
                .then_wrapped([this, kicked_count](seastar::future<> result_fut) {
                    if (result_fut.failed()) {
                        applog.warn("Client {}: Some kick operations failed: {}", _out.socket_address(), result_fut.get_exception());
                    }
                    unsigned count = kicked_count->load(std::memory_order_relaxed);
                    return _out.write(seastar::format("KICKED {}\r\n", count))
                               .then([this]{ return _out.flush(); })
                               .then([]{ return true; });
                });
        });
    }

};


int main(int argc, char** argv) {
    seastar::app_template app;
    app.add_options()
        ("port", boost::program_options::value<uint16_t>()->default_value(STALKD_SERVER), "Beanstalkd port"); 

    return app.run(argc, argv, [&app]() -> seastar::future<> {
        auto& config = app.configuration();
        uint16_t port = config["port"].as<uint16_t>();

        co_await jobs_storage.start();
        co_await all_tubes.start();

        co_await all_tubes.invoke_on_all([](std::unordered_map<seastar::sstring, Tube>& local_tubes_map){
            applog.info("Tube service initialized on shard {}", seastar::this_shard_id());
        });

        seastar::listen_options lo;
        lo.reuse_address = true;
        
        applog.info("Starting Beanstalkd clone (in-memory) on port {}...", port);

        co_await seastar::smp::invoke_on_all([port, lo] { 
            return seastar::listen(seastar::make_ipv4_address({port}), lo, 
                [](seastar::connected_socket fd, seastar::socket_address addr) {
                applog.info("Accepted connection from {} on shard {}", addr, seastar::this_shard_id());
                auto conn = seastar::make_shared<client_connection>(fd.input(), fd.output());
                (void)conn->process().handle_exception_type([conn, addr](const std::exception& e) {
                    applog.error("Exception from connection {} ({}): {}", addr, conn->_out.socket_address(), e.what());
                });
            });
        });
        
        applog.info("All cores are listening. Server is up.");
        co_return; 
    }).finally([] { 
        applog.info("Shutting down server...");
        return jobs_storage.stop().then([]{
            return all_tubes.stop();
        }).then([]{
            applog.info("All sharded services stopped. Shutdown complete.");
        });
    });
}
