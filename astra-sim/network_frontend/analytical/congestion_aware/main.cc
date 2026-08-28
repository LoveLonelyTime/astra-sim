/******************************************************************************
This source code is licensed under the MIT license found in the
LICENSE file in the root directory of this source tree.
*******************************************************************************/

#include "astra-sim/common/ChromeTracer.hh"
#include "astra-sim/common/Logging.hh"
#include "common/CmdLineParser.hh"
#include "congestion_aware/CongestionAwareNetworkApi.hh"
#include <algorithm>
#include <astra-network-analytical/common/EventQueue.h>
#include <astra-network-analytical/common/NetworkParser.h>
#include <astra-network-analytical/congestion_aware/Helper.h>
#include <cstdlib>
#include <iostream>
#include <json/json.hpp>
#include <memory_backend/analytical/AnalyticalMemory.hh>
#include <unistd.h>
#include <vector>
#include <zmq.hpp>

using namespace AstraSim;
using namespace Analytical;
using namespace AstraSimAnalytical;
using namespace AstraSimAnalyticalCongestionAware;
using namespace NetworkAnalytical;
using namespace NetworkAnalyticalCongestionAware;
using namespace std;
using json = nlohmann::json;

static std::string save_json_to_tmp(const json& j, const std::string& name) {
    const char* dir = "tmp_mem";
    if (::mkdir(dir, 0755) == -1) {
        if (errno != EEXIST) {
            AstraSim::LoggerFactory::get_logger("sim")->critical(
                "mkdir tmp_mem");
            std::exit(1);
        }
    }
    std::string path = std::string(dir) + "/" + name + ".json";
    std::ofstream ofs(path);
    if (!ofs) {
        AstraSim::LoggerFactory::get_logger("sim")->critical(
            "Unable to write tmp file: {}", path);
        std::exit(1);
    }
    ofs << j.dump(2);
    return path;
}

static void report_to_zmq(zmq::socket_t& socket, AstraSim::Sys* sys) {
    Tick curr_tick = Sys::boostedTick();

    std::stringstream ss;
    ss << "Waiting" << " " << sys->id << " " << sys->workload->iteration << " "
       << curr_tick << " "
       << (curr_tick - sys->workload->hw_resource->tics_gpu_ops);

    socket.send(zmq::buffer(ss.str()), zmq::send_flags::none);
}

static void parse_cmd(const std::string& str, vector<std::string>& comp) {
    std::stringstream ss(str);
    std::string token;

    while (ss >> token) {
        comp.push_back(token);
    }
}

int main(int argc, char* argv[]) {
    // Parse command line arguments
    auto cmd_line_parser = CmdLineParser(argv[0]);
    cmd_line_parser.parse(argc, argv);

    // Get command line arguments
    const auto workload_configuration =
        cmd_line_parser.get<std::string>("workload-configuration");
    const auto comm_group_configuration =
        cmd_line_parser.get<std::string>("comm-group-configuration");
    const auto system_configuration =
        cmd_line_parser.get<std::string>("system-configuration");
    const auto memory_configuration =
        cmd_line_parser.get<std::string>("memory-configuration");
    const auto network_configuration =
        cmd_line_parser.get<std::string>("network-configuration");
    const auto logging_configuration =
        cmd_line_parser.get<std::string>("logging-configuration");
    const auto num_queues_per_dim =
        cmd_line_parser.get<int>("num-queues-per-dim");
    const auto comm_scale = cmd_line_parser.get<double>("comm-scale");
    const auto injection_scale = cmd_line_parser.get<double>("injection-scale");
    const auto rendezvous_protocol =
        cmd_line_parser.get<bool>("rendezvous-protocol");
    const auto zmq_addr = cmd_line_parser.get<std::string>("zmq-addr");

    auto start_npu_ids = cmd_line_parser.get<std::vector<int>>("start-npu-ids");
    auto end_npu_ids = cmd_line_parser.get<std::vector<int>>("end-npu-ids");
    const auto node_npu_ids =
        cmd_line_parser.get<std::vector<int>>("node-npu-ids");
    const auto instance_npu_ids =
        cmd_line_parser.get<std::vector<int>>("instance-npu-ids");
    const auto inner_npu_ids =
        cmd_line_parser.get<std::vector<int>>("inner-npu-ids");

    // clear vector if default value is used
    if (start_npu_ids.size() == 1 && start_npu_ids[0] == -1) {
        start_npu_ids.clear();
    }
    if (end_npu_ids.size() == 1 && end_npu_ids[0] == -1) {
        end_npu_ids.clear();
    }

    AstraSim::LoggerFactory::init(logging_configuration);

    // Instantiate event queue
    const auto event_queue = std::make_shared<EventQueue>();
    Topology::set_event_queue(event_queue);

    // Generate topology
    const auto network_parser = NetworkParser(network_configuration);
    const auto topology = construct_topology(network_parser);

    // Get topology information
    const auto npus_count = topology->get_npus_count();
    const auto npus_count_per_dim = topology->get_npus_count_per_dim();
    const auto dims_count = topology->get_dims_count();

    // Set up Network API
    CongestionAwareNetworkApi::set_event_queue(event_queue);
    CongestionAwareNetworkApi::set_topology(topology);

    // Create ASTRA-sim related resources
    auto network_apis =
        std::vector<std::unique_ptr<CongestionAwareNetworkApi>>();

    json mem_json;
    std::ifstream rm_ifs(memory_configuration);
    rm_ifs >> mem_json;

    std::vector<std::unique_ptr<AnalyticalMemory>> memory_levels;

    // Check if the configuration is for a single memory type
    const bool is_single =
        mem_json.is_object() && mem_json.contains("memory-type") &&
        mem_json.contains("mem-latency") && mem_json.contains("mem-bw");

    if (is_single) {
        memory_levels.push_back(
            std::make_unique<AnalyticalMemory>(memory_configuration));
    } else {
        // local memory
        if (mem_json.contains("local_mem") &&
            mem_json["local_mem"].is_object()) {
            json j = mem_json["local_mem"];
            j["memory-location"] = "LOCAL_MEMORY";
            auto path = save_json_to_tmp(j, "local_mem");
            memory_levels.push_back(std::make_unique<AnalyticalMemory>(path));
            std::remove(path.c_str());
        }

        // remote memory
        if (mem_json.contains("remote_mem") &&
            mem_json["remote_mem"].is_object()) {
            json j = mem_json["remote_mem"];
            j["memory-location"] = "REMOTE_MEMORY";
            auto path = save_json_to_tmp(j, "remote_mem");
            memory_levels.push_back(std::make_unique<AnalyticalMemory>(path));
            std::remove(path.c_str());
        }

        // cxl memory
        if (mem_json.contains("cxl_mem") && mem_json["cxl_mem"].is_object()) {
            json j = mem_json["cxl_mem"];
            j["memory-location"] = "CXL_MEMORY";
            auto path = save_json_to_tmp(j, "cxl_mem");
            memory_levels.push_back(std::make_unique<AnalyticalMemory>(path));
            std::remove(path.c_str());
        }

        ::rmdir("tmp_mem");
    }

    auto memory_apis = std::vector<AstraMemoryAPI*>();
    for (auto& mem_api : memory_levels) {
        memory_apis.push_back(mem_api.get());
    }

    auto systems = std::vector<Sys*>();
    vector<set<uint32_t>> wait_list(npus_count);

    auto queues_per_dim = std::vector<int>();
    for (auto i = 0; i < dims_count; i++) {
        queues_per_dim.push_back(num_queues_per_dim);
    }

    for (int i = 0; i < npus_count; i++) {
        // create network and system
        auto network_api = std::make_unique<CongestionAwareNetworkApi>(i);
        auto* const system =
            new Sys(i, workload_configuration, comm_group_configuration,
                    system_configuration, memory_apis, network_api.get(),
                    npus_count_per_dim, queues_per_dim, injection_scale,
                    comm_scale, rendezvous_protocol);

        // Config id
        system->node_id = node_npu_ids[i];
        system->instance_id = instance_npu_ids[i];
        system->inner_id = inner_npu_ids[i];

        // push back network and system
        network_apis.push_back(std::move(network_api));
        systems.push_back(system);
    }

    // Map instance NPU IDs for proper workload management
    // Precompute the systems handled by each controller NPU
    std::vector<std::vector<Sys*>> managed_systems(start_npu_ids.size());

    for (std::size_t idx = 0; idx < start_npu_ids.size(); ++idx) {
        int npu_id = start_npu_ids[idx];

        // Determine the upper bound for this controller:
        // - If there's a next controller, stop before it
        // - Otherwise, go until npus_count
        int upper_bound_id;
        if (idx + 1 < start_npu_ids.size()) {
            upper_bound_id = start_npu_ids[idx + 1];
        } else {
            upper_bound_id =
                npus_count;  // last controller handles until the end
        }

        // Collect systems in the range (npu_id+1 .. upper_bound_id-1)
        for (int sid = npu_id + 1; sid < upper_bound_id; ++sid) {
            if (sid < 0 || sid >= npus_count) {
                AstraSim::LoggerFactory::get_logger("workload")
                    ->critical("Skipping invalid system id {} while building "
                               "managed_systems",
                               sid);
            }
            if (std::find(end_npu_ids.begin(), end_npu_ids.end(), sid) !=
                end_npu_ids.end()) {
                continue;
            }
            managed_systems[idx].push_back(systems[sid]);
        }
    }

    // Initiate simulation
    for (int i = 0; i < npus_count; i++) {
        systems[i]->workload->fire();
        // For debugging
        // systems[i]->workload->et_feeder->printGraph();
    }

    // Open ZMQ
    zmq::context_t context(1);
    zmq::socket_t socket(context, zmq::socket_type::req);
    socket.connect(zmq_addr);
    AstraSim::LoggerFactory::get_logger("sim")->info("Connected to {}",
                                                     zmq_addr);

    auto release_wait = [&](int npu_id) {
        bool next_poll = false;
        for (auto waiting_npu_id : wait_list[npu_id]) {
            systems[waiting_npu_id]->workload->is_sleep = false;
            next_poll = true;
        }
        wait_list[npu_id].clear();
        return next_poll;
    };

    // Run simulation
    bool exit = false;
    while (!exit) {
        if (!event_queue->finished()) {
            event_queue->proceed();
        } else {
            // Check deadlock
            bool finish = true;
            for (std::size_t idx = 0; idx < npus_count; idx++) {
                if (!systems[idx]->workload->is_finished) {
                    finish = false;
                    // systems[idx]->workload->et_feeder->printGraph();
                }
            }
            if (!finish) {
                AstraSim::LoggerFactory::get_logger("sim")->critical(
                    "Deadlock is detected");
                std::exit(1);
            } else {
                AstraSim::LoggerFactory::get_logger("sim")->critical(
                    "Expected exit cmd");
                std::exit(1);
            }
        }

        bool next_poll;
        do {
            // Poll
            next_poll = false;

            for (std::size_t idx = 0; idx < end_npu_ids.size(); ++idx) {
                int npu_id = end_npu_ids[idx];
                // Only proceed if the workload has finished its iteration

                if (!systems[npu_id]->workload->is_sleep &&
                    systems[npu_id]->workload->is_finished) {

                    next_poll = next_poll | release_wait(npu_id);

                    report_to_zmq(socket, systems[npu_id]);

                    zmq::message_t reply;
                    auto result = socket.recv(reply, zmq::recv_flags::none);

                    if (result) {
                        std::string cmd_msg(static_cast<char*>(reply.data()),
                                            reply.size());
                        std::vector<std::string> cmd_comp;
                        parse_cmd(cmd_msg, cmd_comp);

                        if (cmd_comp[0].compare("pass") == 0) {  // Pass
                            continue;
                        } else if (cmd_comp[0].compare("exit") == 0) {  // Exit
                            exit = true;
                            break;
                        } else if (cmd_comp[0].compare("add_workload") ==
                                   0) {  // Add workload
                            systems[npu_id]->workload->add_workload(cmd_comp[1],
                                                                    {});
                        } else if (cmd_comp[0].compare("sleep") ==
                                   0) {  // Sleep
                            systems[npu_id]->workload->is_sleep = true;

                            event_queue->schedule_event(
                                std::stoul(cmd_comp[1]),
                                [&systems, npu_id](void* arg) {
                                    systems[npu_id]->workload->is_sleep = false;
                                },
                                nullptr);
                        } else if (cmd_comp[0].compare("done") == 0) {  // Done
                            systems[npu_id]->workload->is_sleep = true;
                        } else if (cmd_comp[0].compare("wait") == 0) {  // Wait
                            systems[npu_id]->workload->is_sleep = true;
                            wait_list[std::stoul(cmd_comp[1])].insert(npu_id);
                        }
                    } else {
                        AstraSim::LoggerFactory::get_logger("sim")->critical(
                            "Error on cmd recv");
                    }
                }
            }

            if (exit) {
                break;
            }

            for (std::size_t idx = 0; idx < start_npu_ids.size(); ++idx) {
                int npu_id = start_npu_ids[idx];
                // Only proceed if the workload has finished its iteration
                if (!systems[npu_id]->workload->is_sleep &&
                    systems[npu_id]->workload->is_finished) {
                    next_poll = next_poll | release_wait(npu_id);

                    report_to_zmq(socket, systems[npu_id]);

                    zmq::message_t reply;
                    auto result = socket.recv(reply, zmq::recv_flags::none);

                    if (result) {
                        std::string cmd_msg(static_cast<char*>(reply.data()),
                                            reply.size());
                        std::vector<std::string> cmd_comp;
                        parse_cmd(cmd_msg, cmd_comp);

                        if (cmd_comp[0].compare("pass") == 0) {  // Pass
                            continue;
                        } else if (cmd_comp[0].compare("exit") == 0) {  // Exit
                            exit = true;
                            break;
                        } else if (cmd_comp[0].compare("add_workload") ==
                                   0) {  // Add workload
                            systems[npu_id]->workload->add_workload(
                                cmd_comp[1], managed_systems[idx]);
                        } else if (cmd_comp[0].compare("sleep") ==
                                   0) {  // Sleep
                            systems[npu_id]->workload->is_sleep = true;

                            event_queue->schedule_event(
                                std::stoul(cmd_comp[1]),
                                [&systems, npu_id](void* arg) {
                                    systems[npu_id]->workload->is_sleep = false;
                                },
                                nullptr);
                        } else if (cmd_comp[0].compare("done") == 0) {  // Done
                            systems[npu_id]->workload->is_sleep = true;
                        } else if (cmd_comp[0].compare("wait") == 0) {  // Wait
                            systems[npu_id]->workload->is_sleep = true;
                            wait_list[std::stoul(cmd_comp[1])].insert(npu_id);
                        }
                    } else {
                        AstraSim::LoggerFactory::get_logger("sim")->critical(
                            "Error on cmd recv");
                    }
                }
            }
        } while (next_poll && !exit);
    }

    // check non exited system
    AstraSim::LoggerFactory::get_logger("sim")->info(
        "Checking Non-Exited Systems ...");
    bool done = true;
    for (int npu_id = 0; npu_id < npus_count; npu_id++) {
        if (!systems[npu_id]->workload->is_finished) {
            done = false;
        }
    }
    if (done) {
        AstraSim::LoggerFactory::get_logger("sim")->info(
            "All Request Has Been Exited");
    } else {
        AstraSim::LoggerFactory::get_logger("sim")->critical(
            "Some Requests Remain");
    }

    // Dump trace
    ChromeTracer::GetInstance().Dump("log/log_trace.json");
    AstraSim::LoggerFactory::get_logger("workload")
        ->info("ChromeTracer dumped to log/log_trace.json");

    // terminate simulation
    AstraSim::LoggerFactory::shutdown();
    return 0;
}
