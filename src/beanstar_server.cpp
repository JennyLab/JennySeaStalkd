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
#include <seastar/util/std-compat.hh>
#include <seastar/core/map_reduce.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/do_for_each.hh>


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
#include <functional>
#include <algorithm>
#include <sstream> // Kept for std::stoul/stoull, but not for string building


#define STALKD_SERVER 11300

// Global logger
seastar::logger applog("beanstar_daemon");

// Forward declaration
class Tube;

// Represents the state of a job in the system.
enum class JobState {
    READY,
    RESERVED,
    DELAYED,
    BURIED,
    INVALID // after deletion or if not found
};

// Represents a single job.
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

// Sharded storage for all jobs, distributed by job ID.
seastar::sharded<std::unordered_map<uint64_t, Job>> jobs_storage;
std::atomic<uint64_t> next_job_id{1};

// A lightweight reference to a job, used in priority queues to avoid storing full jobs.
struct JobReference {
    uint64_t id;
    unsigned priority;
    std::chrono::steady_clock::time_point sort_key_ts;

    // Comparator for the ready queue: sorts by priority (ascending), then by creation time (ascending).
    struct ReadyComparator {
        bool operator()(const JobReference& a, const JobReference& b) const {
            if (a.priority != b.priority) {
                return a.priority > b.priority;
            }
            return a.sort_key_ts > b.sort_key_ts;
        }
    };

    // Comparator for the delayed queue: sorts by activation time (ascending).
    struct DelayedComparator {
        bool operator()(const JobReference& a, const JobReference& b) const {
            return a.sort_key_ts > b.sort_key_ts;
        }
    };
};

// Represents a single message queue (tube).
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

    void check_delayed_jobs();
    void arm_delayed_job_poller_if_needed();
    void notify_a_waiting_client();

    ~Tube() {
        delayed_job_poller.cancel();
        for (auto&& promise : waiting_clients) {
            if (!promise.available()) {
               try {
                   promise.set_exception(std::runtime_error("Tube being destroyed"));
               } catch (const seastar::broken_promise&) {
                    // Ignore, client likely disconnected.
               }
           }
        }
        waiting_clients.clear();
    }
};

// Sharded storage for all tubes, distributed by tube name hash.
seastar::sharded<std::unordered_map<seastar::sstring, Tube>> all_tubes;

// Utility to safely set an exception on a promise, ignoring broken_promise errors.
template<typename T>
void try_set_exception(seastar::promise<T>& p, std::exception_ptr e, const seastar::sstring& context = "") {
    if (!p.available()) {
        try {
            p.set_exception(e);
        } catch (const seastar::broken_promise& bp) {
            applog.trace("Promise was already broken ({}): {}", context, bp.what());
        }
    }
}


// Determines which shard a tube belongs to based on its name.
unsigned get_tube_shard_id(const seastar::sstring& tube_name) {
    return std::hash<seastar::sstring>{}(tube_name) % seastar::smp::count;
}

// Retrieves a tube from the local shard's map, creating it if it doesn't exist.
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

// ### Tube method implementations ###

void Tube::check_delayed_jobs() {
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
    delayed_job_poller.cancel();
}

void Tube::arm_delayed_job_poller_if_needed() {
    if (!delayed_queue.empty() && !delayed_job_poller.armed()) {
        delayed_job_poller.arm(delayed_queue.top().sort_key_ts);
    }
}

void Tube::notify_a_waiting_client() {
    if (!waiting_clients.empty()) {
        seastar::promise<std::optional<uint64_t>> promise = std::move(waiting_clients.front());
        waiting_clients.pop_front();
        // Set value to nullopt to signal the reserve loop to try again.
        promise.set_value(std::nullopt);
        applog.trace("Notified a waiting client for tube {}", name);
    }
}


// Efficiently builds a YAML-formatted map using sstring_builder.
seastar::sstring to_yaml_string_map(const std::unordered_map<seastar::sstring, seastar::sstring>& data) {
    seastar::sstring_builder builder;
    builder << "---\r\n";
    for (const auto& pair : data) {
        builder << pair.first << ": " << pair.second << "\r\n";
    }
    return builder.build();
}

// Efficiently builds a YAML-formatted list using sstring_builder.
seastar::sstring to_yaml_string_list(const std::vector<seastar::sstring>& data) {
    seastar::sstring_builder builder;
    builder << "---\r\n";
    for (const auto& item : data) {
        builder << "- " << item << "\r\n";
    }
    return builder.build();
}


class client_connection : public seastar::enable_shared_from_this<client_connection> {
private:
    // Protocol responses are defined as constants for correctness and maintainability.
    static constexpr const char* RESPONSE_OK_FMT = "OK {}\r\n{}";
    static constexpr const char* RESPONSE_INSERTED_FMT = "INSERTED {}\r\n";
    static constexpr const char* RESPONSE_DELETED = "DELETED\r\n";
    static constexpr const char* RESPONSE_RELEASED = "RELEASED\r\n";
    static constexpr const char* RESPONSE_BURIED = "BURIED\r\n";
    static constexpr const char* RESPONSE_TOUCHED = "TOUCHED\r\n";
    static constexpr const char* RESPONSE_WATCHING_FMT = "WATCHING {}\r\n";
    static constexpr const char* RESPONSE_USING_FMT = "USING {}\r\n";
    static constexpr const char* RESPONSE_KICKED_FMT = "KICKED {}\r\n";
    static constexpr const char* RESPONSE_FOUND_FMT = "FOUND {} {}\r\n{}\r\n";
    static constexpr const char* RESPONSE_RESERVED_FMT = "RESERVED {} {}\r\n{}\r\n";

    static constexpr const char* ERROR_UNKNOWN_COMMAND = "UNKNOWN_COMMAND\r\n";
    static constexpr const char* ERROR_BAD_FORMAT = "BAD_FORMAT\r\n";
    static constexpr const char* ERROR_NOT_FOUND = "NOT_FOUND\r\n";
    static constexpr const char* ERROR_JOB_TOO_BIG = "JOB_TOO_BIG\r\n";
    static constexpr const char* ERROR_TIMED_OUT = "TIMED_OUT\r\n";
    static constexpr const char* ERROR_DEADLINE_SOON = "DEADLINE_SOON\r\n";
    static constexpr const char* ERROR_NOT_IGNORED = "NOT_IGNORED\r\n";
    static constexpr const char* ERROR_INTERNAL = "INTERNAL_SERVER_ERROR\r\n";

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
                    applog.debug("Client {} closed connection", _out.socket_address());
                    return seastar::make_ready_future<seastar::stop_iteration>(seastar::stop_iteration::yes);
                }
                seastar::sstring line(line_buf.get(), line_buf.size());
                if (!line.empty() && line.back() == '\r') {
                    line.pop_back();
                }

                return handle_command(line).then([](bool continue_processing) {
                    return continue_processing ? seastar::stop_iteration::no : seastar::stop_iteration::yes;
                });
            });
        }).finally([this] {
            _reserve_timeout_timer.cancel();
            if (_is_reserving) {
                try_set_exception(_reserve_promise, std::make_exception_ptr(std::runtime_error("Client connection closed")), "process finally");
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

        if (parts.empty()) { return client_error_response(ERROR_INTERNAL); }

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

        applog.warn("Client {}: Unknown command: {}", _out.socket_address(), command);
        return client_error_response(ERROR_UNKNOWN_COMMAND);
    }

    seastar::future<bool> client_error_response(const seastar::sstring& err_msg) {
        return _out.write(err_msg).then([this] {
            return _out.flush();
        }).then([] { return true; });
    }

    // Command handlers (put, delete, etc.) are largely the same as the original,
    // but are updated to use the new response constants.
    // ... (handle_put, handle_delete, handle_release, etc.)

    // *********************************************************************************
    // REVISED `reserve` and `kick` IMPLEMENTATIONS
    // *********************************************************************************

    seastar::future<bool> handle_reserve_base(const std::vector<seastar::sstring>& parts, std::optional<bool> with_timeout_flag) {
        if (_is_reserving) {
            applog.warn("Client {}: New reserve command while previous one active. Cancelling old.", _out.socket_address());
            _reserve_timeout_timer.cancel();
            try_set_exception(_reserve_promise, std::make_exception_ptr(std::runtime_error("Superseded by new reserve command")), "handle_reserve_base");
            _is_reserving = false;
        }

        std::optional<std::chrono::seconds> timeout_duration;
        if (with_timeout_flag.has_value()) {
            if (parts.size() != 2) return client_error_response(ERROR_BAD_FORMAT);
            try {
                timeout_duration = std::chrono::seconds(std::stoul(parts[1].c_str()));
            } catch (const std::exception&) {
                return client_error_response(ERROR_BAD_FORMAT);
            }
        } else {
            if (parts.size() != 1) return client_error_response(ERROR_BAD_FORMAT);
        }

        _is_reserving = true;
        _reserve_promise = seastar::promise<std::optional<uint64_t>>();

        if (timeout_duration) {
            if (*timeout_duration > std::chrono::seconds(0)) {
                 _reserve_timeout_timer.set_callback([this] {
                    if (_is_reserving && !_reserve_promise.available()) {
                         _reserve_promise.set_value(std::nullopt);
                    }
                    _is_reserving = false;
                });
                _reserve_timeout_timer.arm(*timeout_duration);
            }
        }

        // Start the reservation attempt loop.
        do_try_reserve_round();

        return _reserve_promise.get_future().then_wrapped([this, timeout_val = timeout_duration]
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
                        return (it != local_job_map.end()) ? seastar::compat::make_optional(it->second.body) : seastar::compat::nullopt;
                    }).then([this, job_id](seastar::compat::optional<seastar::sstring> body_opt) {
                        if (body_opt) {
                            return _out.write(seastar::format(RESPONSE_RESERVED_FMT, job_id, body_opt->length(), *body_opt))
                                   .then([this] { return _out.flush(); }).then([] { return true; });
                        }
                        return client_error_response(ERROR_INTERNAL); // Reserved but couldn't find body
                    });
                } else {
                    // This means timeout occurred or a client was notified to re-try.
                    if (timeout_val && *timeout_val == std::chrono::seconds(0)) {
                         return client_error_response(ERROR_DEADLINE_SOON);
                    }
                    return client_error_response(ERROR_TIMED_OUT);
                }
            } catch (const std::exception& e) {
                applog.error("Client {}: Exception while finalizing reserve: {}", _out.socket_address(), e.what());
                return client_error_response(ERROR_INTERNAL);
            }
        });
    }

    // SAFER AND SIMPLER `reserve` LOGIC
    void do_try_reserve_round() {
        if (!_is_reserving) return;

        auto watched_tubes_copy = seastar::make_shared<std::vector<seastar::sstring>>(_watched_tubes.begin(), _watched_tubes.end());

        if (watched_tubes_copy->empty()) {
            if (_is_reserving && !_reserve_promise.available()) _reserve_promise.set_value(std::nullopt);
            _is_reserving = false;
            return;
        }

        // Sequentially check each watched tube for a job.
        (void)seastar::do_for_each(watched_tubes_copy->begin(), watched_tubes_copy->end(), [this](const seastar::sstring& tube_name) {
            if (_reserve_promise.available()) {
                return seastar::make_ready_future<>(); // A job was found in a previous iteration.
            }

            unsigned target_tube_shard = get_tube_shard_id(tube_name);
            return all_tubes.invoke_on(target_tube_shard, [this, tube_name](std::unordered_map<seastar::sstring, Tube>& local_tubes_map) -> seastar::future<bool> {
                auto tube_it = local_tubes_map.find(tube_name);
                if (tube_it == local_tubes_map.end() || tube_it->second.ready_queue.empty()) {
                    return seastar::make_ready_future<bool>(false); // No job here.
                }

                Tube& tube = tube_it->second;
                uint64_t job_id = tube.ready_queue.top().id;
                unsigned job_storage_shard = job_id % seastar::smp::count;

                return jobs_storage.invoke_on(job_storage_shard, [job_id, tube_name](std::unordered_map<uint64_t, Job>& j_map) -> bool {
                    auto it = j_map.find(job_id);
                    if (it != j_map.end() && it->second.state == JobState::READY) {
                        Job& job = it->second;
                        job.state = JobState::RESERVED;
                        job.deadline_time = std::chrono::steady_clock::now() + job.ttr_us;
                        job.ttr_timer.arm(job.deadline_time);
                        return true;
                    }
                    return false;
                }).then([this, &tube, job_id](bool success) {
                    if (success) {
                        tube.ready_queue.pop();
                        tube.reserved_jobs.insert(job_id);
                        if (!_reserve_promise.available()) _reserve_promise.set_value(job_id);
                    }
                    return success;
                });
            });
        }).then([this, watched_tubes_copy] {
            // After checking all tubes, if no job was found...
            if (_is_reserving && !_reserve_promise.available()) {
                if (_reserve_timeout_timer.armed()) return; // Timeout is set, let it fire.
                if (_reserve_timeout_timer.get_timeout() == std::chrono::seconds::zero()) {
                    _reserve_promise.set_value(std::nullopt); // Immediate timeout.
                    return;
                }

                // No timeout, so wait indefinitely on the first watched tube.
                const seastar::sstring& first_tube = watched_tubes_copy->front();
                unsigned target_shard = get_tube_shard_id(first_tube);
                (void)all_tubes.invoke_on(target_shard, [this, first_tube](auto& local_map) {
                    if (_is_reserving && !_reserve_promise.available()) {
                        Tube& tube = get_or_create_tube_on_current_shard(local_map, first_tube);
                        tube.waiting_clients.push_back(std::move(_reserve_promise));
                    }
                });
            }
        }).handle_exception([this](std::exception_ptr e) {
            applog.error("Exception in reservation loop: {}", e);
            try_set_exception(_reserve_promise, e, "do_try_reserve_round exception");
            _is_reserving = false;
        });
    }

    seastar::future<bool> handle_kick(const std::vector<seastar::sstring>& parts) {
        if (parts.size() != 2) return client_error_response(ERROR_BAD_FORMAT);
        try {
            return do_kick(std::stoul(parts[1].c_str()));
        } catch (const std::exception&) {
            return client_error_response(ERROR_BAD_FORMAT);
        }
    }

    // PERFORMANT `kick` LOGIC WITH BATCHING
    seastar::future<bool> do_kick(unsigned bound) {
        if (bound == 0) {
            return _out.write(seastar::format(RESPONSE_KICKED_FMT, 0)).then([this] { return _out.flush(); }).then([]{ return true; });
        }

        unsigned target_tube_shard = get_tube_shard_id(_current_put_tube);

        // 1. Get IDs to kick from the tube's shard.
        return all_tubes.invoke_on(target_tube_shard, [bound, tube_name = _current_put_tube]
            (std::unordered_map<seastar::sstring, Tube>& local_tubes_map) -> std::vector<uint64_t> {
            auto tube_it = local_tubes_map.find(tube_name);
            if (tube_it == local_tubes_map.end() || tube_it->second.buried_queue.empty()) return {};
            Tube& tube = tube_it->second;
            std::vector<uint64_t> ids_to_kick;
            for (unsigned i = 0; i < bound && !tube.buried_queue.empty(); ++i) {
                ids_to_kick.push_back(tube.buried_queue.front());
                tube.buried_queue.pop_front();
            }
            return ids_to_kick;
        }).then([this, target_tube_shard, tube_name = _current_put_tube] (std::vector<uint64_t> job_ids_to_kick) {
            if (job_ids_to_kick.empty()) {
                return _out.write(seastar::format(RESPONSE_KICKED_FMT, 0)).then([this]{ return _out.flush(); }).then([]{ return true; });
            }

            // 2. Batch job IDs by their storage shard.
            std::unordered_map<unsigned, std::vector<uint64_t>> jobs_by_shard;
            for (uint64_t id : job_ids_to_kick) {
                jobs_by_shard[id % seastar::smp::count].push_back(id);
            }

            // 3. Send one batch request per shard.
            std::vector<seastar::future<std::vector<JobReference>>> kick_futures;
            for (auto const& [shard_id, ids] : jobs_by_shard) {
                kick_futures.push_back(jobs_storage.invoke_on(shard_id, [ids](auto& local_job_map) {
                    std::vector<JobReference> kicked_refs;
                    for (uint64_t id : ids) {
                        auto it = local_job_map.find(id);
                        if (it != local_job_map.end() && it->second.state == JobState::BURIED) {
                            it->second.state = JobState::READY;
                            kicked_refs.push_back({id, it->second.priority, it->second.creation_time});
                        }
                    }
                    return kicked_refs;
                }));
            }

            // 4. Collect results and update the tube.
            return seastar::when_all_succeed(kick_futures.begin(), kick_futures.end())
            .then([this, target_tube_shard, tube_name] (std::vector<std::vector<JobReference>> all_kicked_refs) {
                auto refs_to_enqueue = seastar::make_shared<std::vector<JobReference>>();
                for (const auto& vec : all_kicked_refs) {
                    refs_to_enqueue->insert(refs_to_enqueue->end(), vec.begin(), vec.end());
                }
                return all_tubes.invoke_on(target_tube_shard, [tube_name, refs_to_enqueue](auto& local_tubes_map) {
                    auto tube_it = local_tubes_map.find(tube_name);
                    if (tube_it != local_tubes_map.end()) {
                        Tube& tube = tube_it->second;
                        for (const auto& ref : *refs_to_enqueue) {
                            tube.ready_queue.push(ref);
                        }
                        if (!refs_to_enqueue->empty()) {
                            tube.notify_a_waiting_client();
                        }
                    }
                    return refs_to_enqueue->size();
                });
            }).then([this](size_t kicked_count) {
                return _out.write(seastar::format(RESPONSE_KICKED_FMT, kicked_count))
                       .then([this]{ return _out.flush(); }).then([]{ return true; });
            });
        });
    }

    // ... (other handlers like handle_stats, handle_peek_ready, etc.)
    // These should be updated to use sstring_builder and response constants,
    // but their core logic remains the same.
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

        applog.info("Starting Beanstalkd clone (in-memory) on port {}...", port);
        seastar::listen_options lo;
        lo.reuse_address = true;

        co_await seastar::smp::invoke_on_all([port, lo] {
            return seastar::listen(seastar::make_ipv4_address({port}), lo,
                [](seastar::connected_socket fd, seastar::socket_address addr) {
                applog.info("Accepted connection from {} on shard {}", addr, seastar::this_shard_id());
                auto conn = seastar::make_shared<client_connection>(fd.input(), fd.output());
                (void)conn->process().handle_exception([conn, addr](std::exception_ptr e) {
                    applog.error("Exception from connection {}: {}", addr, e);
                });
            });
        });

        applog.info("All cores are listening. Server is up.");
    }).finally([] {
        applog.info("Shutting down server...");
        return jobs_storage.stop().then([]{
            return all_tubes.stop();
        }).then([]{
            applog.info("All sharded services stopped. Shutdown complete.");
        });
    });
}
A