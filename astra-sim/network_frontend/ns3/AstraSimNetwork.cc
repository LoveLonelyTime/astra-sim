#include "astra-sim/common/AstraNetworkAPI.hh"
#include "astra-sim/common/ChromeTracer.hh"
#include "astra-sim/system/Sys.hh"
#include "extern/memory_backend/analytical/AnalyticalMemory.hh"
#include <json/json.hpp>

#include <zmq.hpp>
#include "entry.h"
#include "ns3/applications-module.h"
#include "ns3/core-module.h"
#include "ns3/csma-module.h"
#include "ns3/internet-module.h"
#include "ns3/network-module.h"
#include <execinfo.h>
#include <fstream>
#include <iostream>
#include <queue>
#include <stdio.h>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>
#include <json/json.hpp>
#include "astra-sim/common/Logging.hh"

using namespace AstraSim;
using namespace Analytical;
using namespace std;
using namespace ns3;
using json = nlohmann::json;

static std::string save_json_to_tmp(const json& j, const std::string& name) {
  const char* dir = "tmp_mem";
  if (::mkdir(dir, 0755) == -1) {
    if (errno != EEXIST) {
      std::perror("mkdir tmp_mem");
      std::exit(1);
    }
  }
  std::string path = std::string(dir) + "/" + name + ".json";
  std::ofstream ofs(path);
  if (!ofs) {
    std::cerr << "Unable to write tmp file: " << path << "\n";
    std::exit(1);
  }
  ofs << j.dump(2);
  return path;
}


/**
 * @class NS3BackendCompletionTracker
 * @brief Tracks the completion status of ranks in the NS3 backend.
 *
 * This is a hacky approach to track which ranks have completed their workload.
 * The purpose of this class is to end the ns3 simulation once all ranks have completed. 
 * The hacky approach is necessary because each ASTRASimNetwork instance only corresponds to one rank.
 * That is, someone needs to keep track of the completion status of all of the ranks.
 * Because there is no exit point once the ns3 simulator has started, 
 * we cannot implement such tracker in the main function.
 */
class NS3BackendCompletionTracker {
    public: 
        NS3BackendCompletionTracker(int num_ranks) {
            num_unfinished_ranks_ = num_ranks;
            completion_tracker_ = vector<int>(num_ranks, 0);
        }

        void mark_rank_as_finished(int rank) {
            if (completion_tracker_[rank] == 0) {
                completion_tracker_[rank] = 1;
                num_unfinished_ranks_--;
            }
            if (num_unfinished_ranks_ == 0) {
                // AstraSim::LoggerFactory::get_logger("network")
                //     ->debug("All ranks have finished. Exiting simulation.");
                // Simulator::Stop();
                // Simulator::Destroy();
                // exit(0);

                // Stop astra-sim + ns3 simulation exit. Enter idle/wait mode.
                AstraSim::LoggerFactory::get_logger("network")
                ->debug("All ranks have finished. Entering idle/wait mode.");
            }
        }

        void check_all_ranks_finished() {
            // check non exited system
            cout << "Checking Non-Exited Systems ..." << endl;
            if (num_unfinished_ranks_ == 0){
                cout << "---------------------------" << endl;
                cout << "All Request Has Been Exited" << endl;
                cout << "---------------------------" << endl;
            }
            else{
                cout << "---------------------------" << endl;
                cout << "ERROR: Some Requests Remain" << endl;
                cout << "---------------------------" << endl;
            }
            Simulator::Stop();
            Simulator::Destroy();
        }

    private:
        int num_unfinished_ranks_;
        vector<int> completion_tracker_;
};

class ASTRASimNetwork : public AstraSim::AstraNetworkAPI {
  public:
    ASTRASimNetwork(int rank, NS3BackendCompletionTracker *completion_tracker) : AstraNetworkAPI(rank) {
        completion_tracker_ = completion_tracker;
    }

    ~ASTRASimNetwork() {}

    void sim_notify_finished() {
        // Output to file instead of stdout
        /*
        for (auto it = node_to_bytes_sent_map.begin();
             it != node_to_bytes_sent_map.end(); it++) {
            pair<int, int> p = it->first;
            if (p.second == 0) {
                cout << "All data sent from node " << p.first << " is "
                     << it->second << "\n";
            } else {
                cout << "All data received by node " << p.first << " is "
                     << it->second << "\n";
            }
        }
        */
        completion_tracker_->mark_rank_as_finished(rank);
        return;
    }

    double sim_time_resolution() {
        return 0;
    }

    void handleEvent(int dst, int cnt) {}

    AstraSim::timespec_t sim_get_time() {
        AstraSim::timespec_t timeSpec;
        timeSpec.time_res = AstraSim::NS;
        timeSpec.time_val = Simulator::Now().GetNanoSeconds();
        return timeSpec;
    }

    virtual void sim_schedule(AstraSim::timespec_t delta,
                              void (*fun_ptr)(void* fun_arg),
                              void* fun_arg) {
        Simulator::Schedule(NanoSeconds(delta.time_val), fun_ptr, fun_arg);
        return;
    }

    virtual int sim_send(void* buffer,
                         uint64_t message_size,
                         int type,
                         int dst_id,
                         int tag,
                         AstraSim::sim_request* request,
                         void (*msg_handler)(void* fun_arg),
                         void* fun_arg) {
        int src_id = rank;

        // Trigger ns3 to schedule RDMA QP event.
        send_flow(src_id, dst_id, message_size, msg_handler, fun_arg, tag);
        return 0;
    }

    virtual int sim_recv(void* buffer,
                         uint64_t message_size,
                         int type,
                         int src_id,
                         int tag,
                         AstraSim::sim_request* request,
                         void (*msg_handler)(void* fun_arg),
                         void* fun_arg) {
        int dst_id = rank;
        MsgEvent recv_event =
            MsgEvent(src_id, dst_id, 1, message_size, fun_arg, msg_handler);
        MsgEventKey recv_event_key =
            make_pair(tag, make_pair(recv_event.src_id, recv_event.dst_id));

        if (received_msg_standby_hash.find(recv_event_key) !=
            received_msg_standby_hash.end()) {
            // 1) ns3 has already received some message before sim_recv is
            // called.
            int received_msg_bytes = received_msg_standby_hash[recv_event_key];
            if (received_msg_bytes == message_size) {
                // 1-1) The received message size is same as what we expect.
                // Exit.
                received_msg_standby_hash.erase(recv_event_key);
                recv_event.callHandler();
            } else if (received_msg_bytes > message_size) {
                // 1-2) The node received more than expected.
                // Do trigger the callback handler for this message, but wait
                // for Sys layer to call sim_recv for more messages.
                received_msg_standby_hash[recv_event_key] =
                    received_msg_bytes - message_size;
                recv_event.callHandler();
            } else {
                // 1-3) The node received less than what we expected.
                // Reduce the number of bytes we are waiting to receive.
                received_msg_standby_hash.erase(recv_event_key);
                recv_event.remaining_msg_bytes -= received_msg_bytes;
                sim_recv_waiting_hash[recv_event_key] = recv_event;
            }
        } else {
            // 2) ns3 has not yet received anything.
            if (sim_recv_waiting_hash.find(recv_event_key) ==
                sim_recv_waiting_hash.end()) {
                // 2-1) We have not been expecting anything.
                sim_recv_waiting_hash[recv_event_key] = recv_event;
            } else {
                // 2-2) We have already been expecting something.
                // Increment the number of bytes we are waiting to receive.
                int expecting_msg_bytes =
                    sim_recv_waiting_hash[recv_event_key].remaining_msg_bytes;
                recv_event.remaining_msg_bytes += expecting_msg_bytes;
                sim_recv_waiting_hash[recv_event_key] = recv_event;
            }
        }
        return 0;
    }

    private:
    NS3BackendCompletionTracker* completion_tracker_;
};

// Command line arguments and default values.
string workload_configuration;
string system_configuration;
string network_configuration;
string memory_configuration;
string comm_group_configuration = "empty";
string logical_topology_configuration;
string logging_configuration = "empty";
string zmq_addr;
std::vector<uint32_t> start_npu_ids;
std::vector<uint32_t> end_npu_ids;
std::vector<uint32_t> node_npu_ids;
std::vector<uint32_t> instance_npu_ids;
std::vector<uint32_t> inner_npu_ids;
int num_queues_per_dim = 1;
double comm_scale = 1;
double injection_scale = 1;
bool rendezvous_protocol = false;
auto logical_dims = vector<int>();
int num_npus = 1;
auto queues_per_dim = vector<int>();
static constexpr uint64_t idle_ticks = 100000000;

// TODO: Migrate to yaml
void read_logical_topo_config(string network_configuration,
                              vector<int>& logical_dims) {
    ifstream inFile;
    inFile.open(network_configuration);
    if (!inFile) {
        cerr << "Unable to open file: " << network_configuration << endl;
        exit(1);
    }

    // Find the size of each dimension.
    json j;
    inFile >> j;
    if (j.contains("logical-dims")) {
        vector<string> logical_dims_str_vec = j["logical-dims"];
        for (auto logical_dims_str : logical_dims_str_vec) {
            logical_dims.push_back(stoi(logical_dims_str));
        }
    }

    // Find the number of all npus.
    stringstream dimstr;
    for (auto num_npus_per_dim : logical_dims) {
        num_npus *= num_npus_per_dim;
        dimstr << num_npus_per_dim << ",";
    }
    cout << "There are " << num_npus << " npus: " << dimstr.str() << "\n";

    queues_per_dim = vector<int>(logical_dims.size(), num_queues_per_dim);
}

void parse_vec(const string& str, vector<uint32_t>& vec) {
    std::stringstream ss(str);
    std::string token;
    
    while (std::getline(ss, token, ',')) {
        if (!token.empty()) {
            vec.push_back(static_cast<uint32_t>(std::stoul(token)));
        }
    }
}

void parse_cmd(const std::string& str, vector<std::string>& comp) {
    std::stringstream ss(str);
    std::string token;
    
    while (ss >> token) {
        comp.push_back(token);
    }
}

// Read command line arguments.
void parse_args(int argc, char* argv[]) {
    std::string start_npu_ids_str;
    std::string end_npu_ids_str;
    std::string node_npu_ids_str;
    std::string instance_npu_ids_str;
    std::string inner_npu_ids_str;

    CommandLine cmd;
    cmd.AddValue("workload-configuration", "Workload configuration file.",
                 workload_configuration);
    cmd.AddValue("system-configuration", "System configuration file",
                 system_configuration);
    cmd.AddValue("network-configuration", "Network configuration file",
                 network_configuration);
    cmd.AddValue("memory-configuration", "Memory configuration file",
                 memory_configuration);
    cmd.AddValue("comm-group-configuration",
                 "Communicator group configuration file",
                 comm_group_configuration);
    cmd.AddValue("logical-topology-configuration",
                 "Logical topology configuration file",
                 logical_topology_configuration);
    cmd.AddValue("logging-configuration",
                 "Logging configuration file", 
                 logging_configuration);

    cmd.AddValue("zmq-addr", "Address of ZMQ",
                 zmq_addr);
    cmd.AddValue("start-npu-ids", "IDs of start npu of instances.",
                 start_npu_ids_str);
    cmd.AddValue("end-npu-ids", "IDs of end npu of instances.",
                 end_npu_ids_str);
    cmd.AddValue("node-npu-ids", "Node IDs of npus.",
                 node_npu_ids_str);
    cmd.AddValue("instance-npu-ids", "Instance IDs of npus.",
                 instance_npu_ids_str);
    cmd.AddValue("inner-npu-ids", "Inner IDs of npus.",
                 inner_npu_ids_str);
    cmd.AddValue("num-queues-per-dim", "Number of queues per each dimension",
                 num_queues_per_dim);
    cmd.AddValue("comm-scale", "Communication scale", comm_scale);
    cmd.AddValue("injection-scale", "Injection scale", injection_scale);
    cmd.AddValue("rendezvous-protocol", "Whether to enable rendezvous protocol",
                 rendezvous_protocol);

    cmd.Parse(argc, argv);

    parse_vec(start_npu_ids_str, start_npu_ids);
    parse_vec(end_npu_ids_str, end_npu_ids);
    parse_vec(node_npu_ids_str, node_npu_ids);
    parse_vec(instance_npu_ids_str, instance_npu_ids);
    parse_vec(inner_npu_ids_str, inner_npu_ids);
}

void report_to_zmq(zmq::socket_t& socket, AstraSim::Sys* sys) {
    Tick curr_tick = Sys::boostedTick();

    std::stringstream ss;
    ss << "Waiting" << " " << sys->id << " " << sys->workload->iteration << " " << curr_tick << " " << (curr_tick - sys->workload->hw_resource->tics_gpu_ops);

    socket.send(zmq::buffer(ss.str()), zmq::send_flags::none);
}

int main(int argc, char* argv[]) {
    LogComponentEnable("OnOffApplication", LOG_LEVEL_INFO);
    LogComponentEnable("PacketSink", LOG_LEVEL_INFO);

    // Read network config and find logical dims.
    parse_args(argc, argv);
    AstraSim::LoggerFactory::init(logging_configuration);

    AstraSim::LoggerFactory::get_logger("sim")->info("ASTRA-Sim NS3");

    read_logical_topo_config(logical_topology_configuration, logical_dims);

    // Setup network & System layer.
    vector<ASTRASimNetwork*> networks(num_npus, nullptr);
    vector<AstraSim::Sys*> systems(num_npus, nullptr);
    vector<set<uint32_t>> wait_list(num_npus);
    json mem_json;
    std::ifstream rm_ifs(memory_configuration);
    rm_ifs >> mem_json;

    std::vector<std::unique_ptr<AnalyticalMemory>> memory_levels;

    // Check if the configuration is for a single memory type
    const bool is_single =
      mem_json.is_object() &&
      mem_json.contains("memory-type") &&
      mem_json.contains("mem-latency") &&
      mem_json.contains("mem-bw");
    
    if (is_single) {
      AstraSim::LoggerFactory::get_logger("sim")->info("Single Memory Configuration Detected");
      memory_levels.push_back(std::make_unique<AnalyticalMemory>(memory_configuration));
    } else {
      // local memory
      if (mem_json.contains("local_mem") && mem_json["local_mem"].is_object()) {
        json j = mem_json["local_mem"];
        j["memory-location"] = "LOCAL_MEMORY";
        auto path = save_json_to_tmp(j, "local_mem");
        memory_levels.push_back(std::make_unique<AnalyticalMemory>(path));
        std::remove(path.c_str()); 
      }

      // remote memory
      if (mem_json.contains("remote_mem") && mem_json["remote_mem"].is_object()) {
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

    NS3BackendCompletionTracker* completion_tracker = new NS3BackendCompletionTracker(num_npus);

    for (int npu_id = 0; npu_id < num_npus; npu_id++) {
        networks[npu_id] = new ASTRASimNetwork(npu_id, completion_tracker);
        systems[npu_id] = new AstraSim::Sys(
            npu_id, workload_configuration, comm_group_configuration,
            system_configuration, memory_apis, networks[npu_id], logical_dims,
            queues_per_dim, injection_scale, comm_scale, rendezvous_protocol);
        
        // Config id
        systems[npu_id]->node_id = node_npu_ids[npu_id];
        systems[npu_id]->instance_id = instance_npu_ids[npu_id];
        systems[npu_id]->inner_id = inner_npu_ids[npu_id];
    }

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
        upper_bound_id = num_npus;  // last controller handles until the end
      }

      // Collect systems in the range (npu_id+1 .. upper_bound_id-1)
      for (int sid = npu_id + 1; sid < upper_bound_id; ++sid) {
        if (sid < 0 || sid >= num_npus) {
            AstraSim::LoggerFactory::get_logger("workload")
                ->critical("Skipping invalid system id {} while building managed_systems", sid);
        }
        if (std::find(end_npu_ids.begin(), end_npu_ids.end(), sid) != end_npu_ids.end()) {
          continue;
        }
        managed_systems[idx].push_back(systems[sid]);
      }
    }

    // Initialize ns3 simulation.
    if (auto ok = setup_ns3_simulation(network_configuration); ok == -1) {
        std::cerr << "Fail to setup ns3 simulation." << std::endl;
        return -1;
    }

    // Tell workload layer to schedule first events.
    for (int i = 0; i < num_npus; i++) {
        systems[i]->workload->fire();
    }

    // Open ZMQ
    zmq::context_t context(1);
    zmq::socket_t socket(context, zmq::socket_type::req);
    socket.connect(zmq_addr);
    AstraSim::LoggerFactory::get_logger("sim")->info("Connected to {}", zmq_addr);

    // Run the simulation by triggering the ns3 event queue.
    // Simulator::Run();
    Simulator::PreRun();
    bool exit = false;
    while (!exit) {
        // if(Simulator::IsFinished()){
        //     Simulator::Schedule(NanoSeconds(idle_ticks), [] {});
        //     bool is_fin = true;
        //     for (std::size_t idx = 0; idx < end_npu_ids.size(); ++idx) {
        //         int npu_id = end_npu_ids[idx];
        //         if(!systems[npu_id]->workload->is_finished){
        //             is_fin = false;
        //         }
        //     }
        //     for (std::size_t idx = 0; idx < start_npu_ids.size(); ++idx) {
        //         int npu_id = start_npu_ids[idx];
        //         if(!systems[npu_id]->workload->is_finished){
        //             is_fin = false;
        //         }
        //     }
        //     if (!is_fin){
        //         AstraSim::LoggerFactory::get_logger("workload")->info("Deadlock");
        //         return -1;
        //     }
        // }

        // Schedule one event
        Simulator::RunOneEvent();
        bool next_poll = false;

        do {
            // Poll
            next_poll = false;
            
            for (std::size_t idx = 0; idx < end_npu_ids.size(); ++idx) {
                int npu_id = end_npu_ids[idx];

                if(systems[npu_id]->workload->is_finished && !systems[npu_id]->workload->is_sleep){
                    for (auto waiting_npu_id : wait_list[npu_id]) {
                        systems[waiting_npu_id]->workload->is_sleep = false;
                        next_poll = true;
                    }
                    wait_list[npu_id].clear();

                    report_to_zmq(socket, systems[npu_id]);

                    zmq::message_t reply;
                    auto result = socket.recv(reply, zmq::recv_flags::none);

                    if (result) {
                        std::string cmd_msg(static_cast<char*>(reply.data()), reply.size());
                        std::vector<std::string> cmd_comp;
                        parse_cmd(cmd_msg, cmd_comp);

                        if (cmd_comp[0].compare("pass") == 0){ // Pass
                            continue;
                        } else if (cmd_comp[0].compare("exit") == 0) { // Exit
                            exit = true;
                            break;
                        } else if (cmd_comp[0].compare("add_workload") == 0) { // Add workload
                            systems[npu_id]->workload->add_workload(cmd_comp[1], {});
                        } else if(cmd_comp[0].compare("sleep") == 0) { // Sleep
                            systems[npu_id]->workload->is_sleep = true;
                            Simulator::Schedule(NanoSeconds(std::stoul(cmd_comp[1])), [&systems, npu_id] {
                                systems[npu_id]->workload->is_sleep = false;
                            });
                        } else if(cmd_comp[0].compare("done") == 0) { // Done
                            systems[npu_id]->workload->is_sleep = true;
                        } else if(cmd_comp[0].compare("wait") == 0) { // Wait
                            systems[npu_id]->workload->is_sleep = true;
                            wait_list[std::stoul(cmd_comp[1])].insert(npu_id);
                        }
                    } else {
                        AstraSim::LoggerFactory::get_logger("sim")
                            ->critical("Error on cmd recv");
                    }
                }
            }

            if (exit) {
                break;
            }

            for (std::size_t idx = 0; idx < start_npu_ids.size(); ++idx) {
                int npu_id = start_npu_ids[idx];

                if(systems[npu_id]->workload->is_finished && !systems[npu_id]->workload->is_sleep){
                    for (auto waiting_npu_id : wait_list[npu_id]) {
                        systems[waiting_npu_id]->workload->is_sleep = false;
                        next_poll = true;
                    }
                    wait_list[npu_id].clear();

                    report_to_zmq(socket, systems[npu_id]);

                    zmq::message_t reply;
                    auto result = socket.recv(reply, zmq::recv_flags::none);

                    if (result) {
                        std::string cmd_msg(static_cast<char*>(reply.data()), reply.size());
                        std::vector<std::string> cmd_comp;
                        parse_cmd(cmd_msg, cmd_comp);

                        if (cmd_comp[0].compare("pass") == 0){ // Pass
                            continue;
                        } else if (cmd_comp[0].compare("exit") == 0) { // Exit
                            exit = true;
                            break;
                        } else if (cmd_comp[0].compare("add_workload") == 0) { // Add workload
                            systems[npu_id]->workload->add_workload(cmd_comp[1], managed_systems[idx]);
                        } else if(cmd_comp[0].compare("sleep") == 0) { // Sleep
                            systems[npu_id]->workload->is_sleep = true;
                            Simulator::Schedule(NanoSeconds(std::stoul(cmd_comp[1])), [&systems, npu_id] {
                                systems[npu_id]->workload->is_sleep = false;
                            });
                        } else if(cmd_comp[0].compare("done") == 0) { // Done
                            systems[npu_id]->workload->is_sleep = true;
                        } else if(cmd_comp[0].compare("wait") == 0) { // Wait
                            systems[npu_id]->workload->is_sleep = true;
                            wait_list[std::stoul(cmd_comp[1])].insert(npu_id);
                        }
                    } else {
                        AstraSim::LoggerFactory::get_logger("sim")
                            ->critical("Error on cmd recv");
                    }
                }
            }
        } while(next_poll && !exit);
    }

    completion_tracker->check_all_ranks_finished();

    ChromeTracer::GetInstance().Dump("log/log_trace.json");
    AstraSim::LoggerFactory::get_logger("workload")->info("ChromeTracer dumped to log/log_trace.json");
    return 0;
}
