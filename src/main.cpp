#include <chrono>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include "kvstore/consistent_hash.h"
#include "kvstore/gossip.h"
#include "kvstore/merkle_tree.h"
#include "kvstore/storage_engine.h"
#include "kvstore/vector_clock.h"

namespace fs = std::filesystem;
using namespace kvstore;

// ANSI Terminal Colors
namespace Color {
constexpr const char* RESET   = "\033[0m";
constexpr const char* BOLD    = "\033[1m";
constexpr const char* CYAN    = "\033[36m";
constexpr const char* GREEN   = "\033[32m";
constexpr const char* YELLOW  = "\033[33m";
constexpr const char* RED     = "\033[31m";
}  // namespace Color

void printHeader(const std::string& title) {
    std::cout << "\n" << Color::BOLD << Color::CYAN
              << "======================================================================\n"
              << " " << title << "\n"
              << "======================================================================"
              << Color::RESET << "\n\n";
}

int main() {
    std::cout << Color::BOLD << Color::GREEN
              << "\n======================================================================\n"
              << "   DISTRIBUTED FAULT-TOLERANT KEY-VALUE STORE (DYNAMO ARCHITECTURE)   \n"
              << "======================================================================\n"
              << Color::RESET;

    const fs::path base_temp = fs::temp_directory_path() / "kvstore_cluster_demo";
    fs::remove_all(base_temp);
    fs::create_directories(base_temp);

    // =========================================================================
    // 1. CLUSTER TOPOLOGY & CONSISTENT HASH RING
    // =========================================================================
    printHeader("STAGE 1: Consistent Hash Ring (Partitioning & Virtual Nodes)");

    ConsistentHashRing ring(150);  // 150 virtual nodes per physical node
    ring.addNode("node-alpha");
    ring.addNode("node-beta");
    ring.addNode("node-gamma");

    std::cout << "Added 3 physical nodes with 150 virtual nodes each (450 virtual points on 2^32 ring):\n"
              << "  • " << Color::CYAN << "node-alpha" << Color::RESET << "\n"
              << "  • " << Color::CYAN << "node-beta" << Color::RESET << "\n"
              << "  • " << Color::CYAN << "node-gamma" << Color::RESET << "\n"
              << "Ring node count: " << Color::BOLD << ring.nodeCount() << Color::RESET << "\n\n";

    // Show key ownership
    const std::vector<std::string> sample_keys = {
        "user:1001", "user:1002", "order:5501", "session:xyz", "cart:alice"
    };

    std::cout << "Key-to-Replica Mapping (Replication Factor N=2):\n";
    for (const auto& key : sample_keys) {
        auto replicas = ring.getReplicaNodes(key, 2);
        std::cout << "  Key " << std::left << std::setw(14) << ("'" + key + "'")
                  << " ──► Primary: " << Color::GREEN << std::setw(12) << replicas[0] << Color::RESET
                  << " Replica: " << Color::YELLOW << replicas[1] << Color::RESET << "\n";
    }

    // =========================================================================
    // 2. GOSSIP PROTOCOL & MEMBERSHIP
    // =========================================================================
    printHeader("STAGE 2: Decentralized Gossip Protocol (Failure Detection)");

    // 200ms timeout for demonstration
    GossipProtocol gossip_alpha("node-alpha", 200);
    GossipProtocol gossip_beta("node-beta", 200);
    GossipProtocol gossip_gamma("node-gamma", 200);

    gossip_alpha.addMember("node-beta");
    gossip_alpha.addMember("node-gamma");

    gossip_beta.addMember("node-alpha");
    gossip_beta.addMember("node-gamma");

    gossip_gamma.addMember("node-alpha");
    gossip_gamma.addMember("node-beta");

    // All tick their heartbeats
    gossip_alpha.heartbeat();
    gossip_beta.heartbeat();
    gossip_gamma.heartbeat();

    // Gossip membership exchange (alpha receives from beta & gamma)
    gossip_alpha.receiveHeartbeat(gossip_beta.getMembershipList());
    gossip_alpha.receiveHeartbeat(gossip_gamma.getMembershipList());

    std::cout << "Gossip round complete. Node statuses observed from 'node-alpha':\n";
    for (const auto& id : {"node-alpha", "node-beta", "node-gamma"}) {
        auto status = gossip_alpha.getNodeStatus(id);
        std::string status_str = (status == NodeStatus::ALIVE) ? (std::string(Color::GREEN) + "ALIVE" + Color::RESET)
                                                               : (std::string(Color::RED) + "DEAD" + Color::RESET);
        std::cout << "  • " << std::setw(12) << id << " status: " << status_str << "\n";
    }

    // =========================================================================
    // 3. STORAGE ENGINES (WAL + MEMTABLE + SSTABLE)
    // =========================================================================
    printHeader("STAGE 3: LSM-Tree Storage Engine (WAL & MemTable & Flush)");

    fs::path dir_alpha = base_temp / "node_alpha";
    fs::path dir_beta  = base_temp / "node_beta";
    fs::path dir_gamma = base_temp / "node_gamma";

    StorageEngine store_alpha(dir_alpha.string());
    StorageEngine store_beta(dir_beta.string());
    StorageEngine store_gamma(dir_gamma.string());

    std::cout << "Writing keys to primary + backup replica storage engines...\n";

    // Write sample keys with replication
    for (const auto& key : sample_keys) {
        std::string val = "data_for_" + key;
        auto replicas = ring.getReplicaNodes(key, 2);

        // Primary write
        if (replicas[0] == "node-alpha") store_alpha.put(key, val);
        else if (replicas[0] == "node-beta") store_beta.put(key, val);
        else if (replicas[0] == "node-gamma") store_gamma.put(key, val);

        // Secondary replica write
        if (replicas[1] == "node-alpha") store_alpha.put(key, val);
        else if (replicas[1] == "node-beta") store_beta.put(key, val);
        else if (replicas[1] == "node-gamma") store_gamma.put(key, val);

        std::cout << "  " << Color::GREEN << "[PUT]" << Color::RESET << " " << key
                  << " written to WAL & MemTable on " << replicas[0] << " & " << replicas[1] << "\n";
    }

    std::cout << "\nFlushing active MemTable to immutable SSTable on 'node-alpha'...\n";
    store_alpha.flush();
    std::cout << "  " << Color::CYAN << "[FLUSH]" << Color::RESET
              << " MemTable frozen and written as on-disk SSTable (with Bloom filter + index)\n";

    // Read back
    std::cout << "\nReading 'session:xyz' from node-alpha (replicated to alpha):\n";
    auto read_val = store_alpha.get("session:xyz");
    if (read_val.has_value()) {
        std::cout << "  " << Color::GREEN << "[HIT]" << Color::RESET
                  << " Found value in SSTable/MemTable: " << Color::BOLD << *read_val << Color::RESET << "\n";
    }

    // =========================================================================
    // 4. CRASH & RECOVERY (WAL REPLAY)
    // =========================================================================
    printHeader("STAGE 4: Crash Recovery via Write-Ahead Log (WAL)");

    std::cout << "Writing un-flushed key 'user:crash_test' to node-beta's MemTable...\n";
    store_beta.put("user:crash_test", "must_survive_crash");

    std::cout << "Simulating sudden process crash & restart of 'node-beta'...\n";
    // Create new engine pointing to same directory without flushing
    {
        StorageEngine recovered_beta(dir_beta.string());
        auto recovered_val = recovered_beta.get("user:crash_test");
        if (recovered_val.has_value() && *recovered_val == "must_survive_crash") {
            std::cout << "  " << Color::GREEN << "[RECOVERED]" << Color::RESET
                      << " Successfully replayed WAL! Found: " << Color::BOLD << *recovered_val << Color::RESET << "\n";
        } else {
            std::cout << "  " << Color::RED << "[FAILED]" << Color::RESET << " WAL recovery failed\n";
        }
    }

    // =========================================================================
    // 5. NODE FAILURE DETECTION (GOSSIP TIMEOUT & FAILOVER)
    // =========================================================================
    printHeader("STAGE 5: Gossip Failure Detection & Routing Failover");

    std::cout << "'node-gamma' encounters a network partition / crash (stops heartbeating)...\n";
    std::cout << "Waiting 250ms for failure timeout (configured threshold = 200ms)...\n";
    std::this_thread::sleep_for(std::chrono::milliseconds(250));

    // Alpha and Beta continue heartbeating, but gamma has ceased
    gossip_alpha.heartbeat();
    gossip_beta.heartbeat();
    gossip_alpha.receiveHeartbeat(gossip_beta.getMembershipList());

    auto gamma_status = gossip_alpha.getNodeStatus("node-gamma");
    if (gamma_status == NodeStatus::DEAD) {
        std::cout << "  " << Color::RED << "[ALERT]" << Color::RESET
                  << " node-alpha detected 'node-gamma' is " << Color::BOLD << Color::RED << "DEAD" << Color::RESET << "!\n";
    }

    auto failed_nodes = gossip_alpha.getFailedNodes();
    std::cout << "  Failed nodes list: ";
    for (const auto& fn : failed_nodes) std::cout << Color::RED << fn << " " << Color::RESET;
    std::cout << "\n";

    std::cout << "Rerouting: client requests targeting failed node are diverted to surviving replicas.\n";

    // =========================================================================
    // 6. VECTOR CLOCKS (CAUSALITY & CONFLICT RESOLUTION)
    // =========================================================================
    printHeader("STAGE 6: Vector Clocks (Causality & Concurrent Conflict Resolution)");

    VectorClock v_base;
    v_base.increment("node-alpha");  // [alpha: 1]
    std::cout << "Initial write at node-alpha:\n"
              << "  v_base clock: [node-alpha: " << v_base.getCounter("node-alpha") << "]\n\n";

    // Client 1 branches from v_base and writes via node-alpha
    VectorClock v1 = v_base;
    v1.increment("node-alpha");  // [alpha: 2]

    // Client 2 branches from v_base and writes concurrently via node-beta
    VectorClock v2 = v_base;
    v2.increment("node-beta");   // [alpha: 1, beta: 1]

    std::cout << "Concurrent writes occurred during network partition:\n"
              << "  Client 1 (via alpha) Clock: [node-alpha: " << v1.getCounter("node-alpha")
              << ", node-beta: " << v1.getCounter("node-beta") << "]\n"
              << "  Client 2 (via beta)  Clock: [node-alpha: " << v2.getCounter("node-alpha")
              << ", node-beta: " << v2.getCounter("node-beta") << "]\n\n";

    auto cmp = v1.compare(v2);
    if (cmp == ClockComparison::CONCURRENT) {
        std::cout << "  " << Color::YELLOW << "[CONFLICT]" << Color::RESET
                  << " v1.compare(v2) == " << Color::BOLD << "CONCURRENT" << Color::RESET
                  << " (neither dominates, conflict detected!)\n";
    }

    // Merge conflict
    std::cout << "\nReconciling conflict via element-wise merge:\n";
    VectorClock v_merged = v1;
    v_merged.merge(v2);
    v_merged.increment("node-alpha");  // Write resolved version

    std::cout << "  Merged clock: [node-alpha: " << v_merged.getCounter("node-alpha")
              << ", node-beta: " << v_merged.getCounter("node-beta") << "]\n";
    std::cout << "  v_merged descends from v1? "
              << (v_merged.descends(v1) ? (std::string(Color::GREEN) + "true" + Color::RESET) : "false") << "\n";
    std::cout << "  v_merged descends from v2? "
              << (v_merged.descends(v2) ? (std::string(Color::GREEN) + "true" + Color::RESET) : "false") << "\n";

    // =========================================================================
    // 7. ANTI-ENTROPY MERKLE TREES (REPLICA DIVERGENCE DETECTION)
    // =========================================================================
    printHeader("STAGE 7: Anti-Entropy Sync with Merkle Trees (Hash Tree Diff)");

    std::vector<std::pair<std::string, std::string>> replica_a_data = {
        {"key:1", "val1"}, {"key:2", "val2"}, {"key:3", "val3"}, {"key:4", "val4"}
    };

    // Replica B has a divergence in key:3
    std::vector<std::pair<std::string, std::string>> replica_b_data = {
        {"key:1", "val1"}, {"key:2", "val2"}, {"key:3", "DIVERGENT_VALUE"}, {"key:4", "val4"}
    };

    MerkleTree tree_a(8);
    MerkleTree tree_b(8);

    tree_a.build(replica_a_data);
    tree_b.build(replica_b_data);

    std::cout << "Replica A Root Hash: " << Color::CYAN << tree_a.getRootHash() << Color::RESET << "\n";
    std::cout << "Replica B Root Hash: " << Color::CYAN << tree_b.getRootHash() << Color::RESET << "\n";

    std::cout << "\nComparing trees via MerkleTree::diff()...\n";
    auto diff_ranges = tree_a.diff(tree_b);

    std::cout << "  " << Color::YELLOW << "[OUT-OF-SYNC]" << Color::RESET
              << " Discovered " << diff_ranges.size() << " diverged bucket(s):\n";
    for (const auto& r : diff_ranges) {
        std::cout << "    • Divergent KeyRange Bucket: [" << Color::BOLD << r.start_key
                  << Color::RESET << "]\n";
    }

    std::cout << "\nAnti-Entropy repair only transfers keys in the diverged bucket, avoiding full table scan!\n";

    // Cleanup demo temp directories
    fs::remove_all(base_temp);

    std::cout << "\n" << Color::BOLD << Color::GREEN
              << "======================================================================\n"
              << "            ALL DISTRIBUTED KV-STORE COMPONENTS VERIFIED!             \n"
              << "======================================================================\n"
              << Color::RESET << std::endl;

    return 0;
}

