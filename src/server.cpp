/**
 * @file server.cpp
 * @brief Unified HTTP server for the distributed C++ KV-Store.
 *
 * This server manages a simulated multi-node cluster backed by real C++ library
 * components: a ConsistentHashRing, per-node GossipProtocol instances, per-node
 * StorageEngine (WAL + MemTable + SSTables), MerkleTree anti-entropy, and
 * VectorClock causality tracking.
 *
 * Architecture overview:
 *  ┌─────────────────────────────────────────────┐
 *  │                  ClusterState               │
 *  │  ┌──────────────┐  ┌──────────────────────┐ │
 *  │  │ ConsistentHash│  │ GossipProtocol[node] │ │
 *  │  │     Ring      │  │ (failure detection)  │ │
 *  │  └──────────────┘  └──────────────────────┘ │
 *  │  ┌──────────────────────────────────────┐   │
 *  │  │  StorageEngine[node]  (WAL+LSM)      │   │
 *  │  └──────────────────────────────────────┘   │
 *  │  ┌──────────────┐  ┌──────────────────────┐ │
 *  │  │ VectorClock  │  │  MerkleTree (diff)   │ │
 *  │  │  per key     │  │  anti-entropy repair │ │
 *  │  └──────────────┘  └──────────────────────┘ │
 *  └─────────────────────────────────────────────┘
 *
 * Request flow:
 *  accept() → handleClient() → handleApiRequest() → handleXxxApi()
 *
 * Concurrency:
 *  A single std::mutex (ClusterState::mtx) protects all shared cluster data.
 *  Every API handler and the background gossip thread both acquire this lock.
 */

// ─── POSIX / Socket headers ────────────────────────────────────────────────
#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

// ─── Standard library ──────────────────────────────────────────────────────
#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

// ─── KV-Store C++ library headers ─────────────────────────────────────────
#include "kvstore/consistent_hash.h"
#include "kvstore/gossip.h"
#include "kvstore/merkle_tree.h"
#include "kvstore/storage_engine.h"
#include "kvstore/vector_clock.h"

namespace fs = std::filesystem;
using namespace kvstore;

// ============================================================================
// Lightweight JSON Builder
//
// Eliminates raw escaped-string JSON construction throughout the codebase.
// Usage:
//   Json::obj obj;
//   obj.str("key", value).num("count", 42).boolean("ok", true);
//   obj.arr("items", Json::arr().str("a").str("b"));
//   std::string result = obj.build();
//
// Json::arr can hold strings, numbers, booleans, or nested objects.
// ============================================================================

/**
 * @brief Minimal JSON array builder.
 *
 * Appends strongly-typed values without any manual quote escaping.
 * Call build() to get the final JSON array string (including brackets).
 */
struct JsonArr {
    std::ostringstream buf_;
    bool               first_ = true;

    /// Append a raw (already-serialised) JSON fragment.
    JsonArr& raw(const std::string& fragment) {
        if (!first_) buf_ << ',';
        buf_ << fragment;
        first_ = false;
        return *this;
    }

    /// Append a quoted string element.
    JsonArr& str(const std::string& s) { return raw('"' + s + '"'); }

    /// Append an integer element.
    JsonArr& num(long long n) { return raw(std::to_string(n)); }

    /// Append a boolean element.
    JsonArr& boolean(bool b) { return raw(b ? "true" : "false"); }

    /// Append a nested object/array (pass in the already-built JSON string).
    JsonArr& obj(const std::string& json) { return raw(json); }

    std::string build() const { return '[' + buf_.str() + ']'; }
};

/**
 * @brief Minimal JSON object builder.
 *
 * Appends key/value pairs without any manual quote escaping.
 * Call build() to get the final JSON object string (including braces).
 */
struct JsonObj {
    std::ostringstream buf_;
    bool               first_ = true;

    /// Append a raw (already-serialised) value under a quoted key.
    JsonObj& raw(const std::string& key, const std::string& value) {
        if (!first_) buf_ << ',';
        buf_ << '"' << key << "\":" << value;
        first_ = false;
        return *this;
    }

    /// Append a quoted string field.
    JsonObj& str(const std::string& key, const std::string& value) {
        return raw(key, '"' + value + '"');
    }

    /// Append an integer field.
    JsonObj& num(const std::string& key, long long value) {
        return raw(key, std::to_string(value));
    }

    /// Append a boolean field.
    JsonObj& boolean(const std::string& key, bool value) {
        return raw(key, value ? "true" : "false");
    }

    /// Append a nested object or array (pass in the already-built JSON string).
    JsonObj& nested(const std::string& key, const std::string& json) {
        return raw(key, json);
    }

    std::string build() const { return '{' + buf_.str() + '}'; }
};

// ============================================================================
// ClusterState — Single Source of Truth for the Entire Cluster
// ============================================================================

/**
 * @brief Global cluster state shared by all request-handling threads.
 *
 * Holds every cluster-wide data structure: the consistent hash ring, one
 * GossipProtocol instance per node, one StorageEngine per node (WAL + LSM),
 * in-memory key-value mirrors used for Merkle diff computation, WAL entry
 * logs for the UI inspector, and per-key VectorClocks for causal versioning.
 *
 * All fields are protected by ClusterState::mtx.
 */
struct ClusterState {
    std::mutex mtx;

    // ── Consistent Hash Ring ──────────────────────────────────────────────
    /// Token ring with virtual nodes (50 vnodes per physical node).
    std::unique_ptr<ConsistentHashRing> ring;

    /// Ordered list of node IDs as they were added (preserves insertion order).
    std::vector<std::string> node_order;

    // ── Gossip / Failure Detection ────────────────────────────────────────
    /// One gossip instance per node; drives heartbeats and ALIVE/DEAD detection.
    std::unordered_map<std::string, std::unique_ptr<GossipProtocol>> gossip_nodes;

    /// Nodes that have been explicitly "killed" via the chaos-testing API.
    std::unordered_set<std::string> disabled_nodes;

    // ── Persistent Storage (WAL + MemTable + SSTables) ───────────────────
    /// Base directory under which each node gets its own sub-directory.
    std::string base_dir;

    /// One StorageEngine per node; manages WAL, MemTable, and SSTable files.
    std::unordered_map<std::string, std::unique_ptr<StorageEngine>> storage_nodes;

    // ── In-memory mirrors (used by Merkle diff & UI inspector) ───────────
    /// Mirror of every key→value stored on each node.  Updated on every PUT.
    std::unordered_map<std::string, std::map<std::string, std::string>> node_data;

    /// Human-readable WAL log entries per node, shown in the UI storage panel.
    std::unordered_map<std::string, std::vector<std::string>> node_wal_entries;

    /// Running count of SSTable flushes per node.
    std::unordered_map<std::string, int> node_sstable_counts;

    // ── Vector Clocks ─────────────────────────────────────────────────────
    /// Tracks causal version history for each key across coordinator nodes.
    std::unordered_map<std::string, VectorClock> key_clocks;

    /**
     * @brief Constructs the cluster and bootstraps 3 default nodes.
     */
    ClusterState() {
        ring     = std::make_unique<ConsistentHashRing>(50);
        base_dir = (fs::temp_directory_path() / "kvstore_cluster_storage").string();
        fs::remove_all(base_dir);
        fs::create_directories(base_dir);

        for (const auto& id : {"node-alpha", "node-beta", "node-gamma"}) {
            addNodeInternal(id);
        }
    }

    /**
     * @brief Registers a new node across all cluster sub-systems.
     *
     * Adds the node to the hash ring, creates a GossipProtocol instance,
     * cross-introduces it to all peers, and provisions a StorageEngine on disk.
     *
     * @param id  Unique node identifier (e.g. "node-alpha").
     */
    void addNodeInternal(const std::string& id) {
        if (ring->hasNode(id)) return;  // idempotent

        ring->addNode(id);
        node_order.push_back(id);

        gossip_nodes[id] = std::make_unique<GossipProtocol>(id, 2500);
        for (const auto& peer : node_order) {
            if (peer != id) {
                gossip_nodes[peer]->addMember(id);
                gossip_nodes[id]->addMember(peer);
            }
        }

        fs::path node_dir = fs::path(base_dir) / id;
        fs::create_directories(node_dir);
        storage_nodes[id] = std::make_unique<StorageEngine>(node_dir.string());

        node_data[id]           = {};
        node_wal_entries[id]    = {};
        node_sstable_counts[id] = 0;
    }

    /**
     * @brief Permanently removes a node from all cluster sub-systems.
     *
     * Removes the node from the ring, erases its gossip membership from all
     * peers, deletes the StorageEngine and all on-disk files, and clears
     * the in-memory mirrors.
     *
     * @param id  Unique node identifier to remove.
     */
    void removeNodeInternal(const std::string& id) {
        if (!ring->hasNode(id)) return;

        ring->removeNode(id);
        node_order.erase(std::remove(node_order.begin(), node_order.end(), id),
                         node_order.end());
        disabled_nodes.erase(id);

        for (const auto& peer : node_order) {
            if (gossip_nodes.count(peer)) {
                gossip_nodes[peer]->removeMember(id);
            }
        }

        gossip_nodes.erase(id);
        storage_nodes.erase(id);
        node_data.erase(id);
        node_wal_entries.erase(id);
        node_sstable_counts.erase(id);

        fs::remove_all(fs::path(base_dir) / id);
    }
};

/// Global singleton cluster state (accessed by all threads under mtx).
static ClusterState g_cluster;

// ============================================================================
// JSON & HTTP Utility Helpers
// ============================================================================

/**
 * @brief Extracts a string value from a minimal JSON object by key name.
 *
 * Performs a simple substring search; sufficient for the small, well-known
 * JSON payloads sent by the UI.
 *
 * @param json  Raw JSON text.
 * @param key   The field name to look up (without quotes).
 * @return      The string value, or an empty string if the key is absent.
 */
static std::string extractJsonString(const std::string& json, const std::string& key) {
    std::string search = '"' + key + '"';
    std::size_t pos    = json.find(search);
    if (pos == std::string::npos) return "";
    pos = json.find(':', pos);
    if (pos == std::string::npos) return "";
    pos = json.find('"', pos);
    if (pos == std::string::npos) return "";
    std::size_t end = json.find('"', pos + 1);
    if (end == std::string::npos) return "";
    return json.substr(pos + 1, end - pos - 1);
}

/**
 * @brief Extracts an integer value from a minimal JSON object by key name.
 *
 * @param json         Raw JSON text.
 * @param key          The field name to look up.
 * @param default_val  Returned when the key is absent or unparseable.
 * @return             The integer value, or default_val on failure.
 */
static int extractJsonInt(const std::string& json, const std::string& key, int default_val = 0) {
    std::string search = '"' + key + '"';
    std::size_t pos    = json.find(search);
    if (pos == std::string::npos) return default_val;
    pos = json.find(':', pos);
    if (pos == std::string::npos) return default_val;
    while (pos < json.size() && (json[pos] == ':' || json[pos] == ' ')) pos++;
    std::size_t end = pos;
    while (end < json.size() && json[end] >= '0' && json[end] <= '9') end++;
    if (end == pos) return default_val;
    return std::stoi(json.substr(pos, end - pos));
}

/**
 * @brief Percent-decodes a URL-encoded string (e.g. "%3A" → ":").
 *
 * Also converts '+' to space, matching standard form-encoded behaviour.
 *
 * @param in  URL-encoded input.
 * @return    Decoded output string.
 */
static std::string urlDecode(const std::string& in) {
    std::string out;
    out.reserve(in.size());
    for (std::size_t i = 0; i < in.size(); ++i) {
        if (in[i] == '%' && i + 2 < in.size()) {
            int value = 0;
            std::istringstream is(in.substr(i + 1, 2));
            if (is >> std::hex >> value) {
                out += static_cast<char>(value);
                i += 2;
            } else {
                out += in[i];
            }
        } else if (in[i] == '+') {
            out += ' ';
        } else {
            out += in[i];
        }
    }
    return out;
}

/**
 * @brief Builds a complete HTTP/1.1 response string.
 *
 * Always adds CORS headers so the browser-based UI can make cross-origin
 * requests freely.
 *
 * @param status        HTTP status code (200, 404, 500, …).
 * @param content_type  MIME type for the Content-Type header.
 * @param body          Response body text.
 * @return              Full HTTP response ready to write() to a socket.
 */
static std::string makeResponse(int status,
                                const std::string& content_type,
                                const std::string& body) {
    std::ostringstream ss;
    ss << "HTTP/1.1 " << status << ' ';
    if      (status == 200) ss << "OK";
    else if (status == 404) ss << "Not Found";
    else if (status == 500) ss << "Internal Server Error";
    else                    ss << "Status";
    ss << "\r\n"
       << "Content-Type: "   << content_type    << "\r\n"
       << "Content-Length: " << body.size()      << "\r\n"
       << "Access-Control-Allow-Origin: *\r\n"
       << "Access-Control-Allow-Methods: GET, POST, OPTIONS\r\n"
       << "Access-Control-Allow-Headers: Content-Type\r\n"
       << "Connection: close\r\n\r\n"
       << body;
    return ss.str();
}

/** @brief Convenience: HTTP 200 JSON response. */
static std::string jsonOk(const std::string& json) {
    return makeResponse(200, "application/json", json);
}

/** @brief Convenience: HTTP 404 JSON error response. */
static std::string jsonNotFound(const std::string& message) {
    return makeResponse(404, "application/json",
                        JsonObj().boolean("success", false).str("error", message).build());
}

/** @brief Convenience: HTTP 400 JSON error response. */
static std::string jsonBadRequest(const std::string& message) {
    return makeResponse(400, "application/json",
                        JsonObj().boolean("success", false).str("error", message).build());
}

/** @brief Convenience: HTTP 500 JSON error response. */
static std::string jsonServerError(const std::string& message) {
    return makeResponse(500, "application/json",
                        JsonObj().boolean("success", false).str("error", message).build());
}

/**
 * @brief Serialises a VectorClock to a human-readable string.
 *
 * Format: "[node1: N, node2: M, …]"
 */
static std::string clockToString(const VectorClock& vc) {
    std::ostringstream ss;
    ss << '[';
    std::size_t idx = 0;
    for (const auto& [server, count] : vc.clocks()) {
        ss << server << ": " << count;
        if (++idx < vc.clocks().size()) ss << ", ";
    }
    ss << ']';
    return ss.str();
}

// ============================================================================
// Individual API Handler Functions
//
// All callers must already hold g_cluster.mtx before invoking these functions.
// ============================================================================

// ─── 1. Cluster State Snapshot ────────────────────────────────────────────

/**
 * @brief  GET /api/cluster/state
 *
 * Returns a JSON snapshot of the entire cluster: each node's liveness status,
 * gossip heartbeat counter, key count, WAL entry count, and SSTable count.
 *
 * @return HTTP 200 with JSON body.
 */
static std::string handleClusterState() {
    JsonArr nodes;

    for (const auto& id : g_cluster.node_order) {
        bool     is_disabled = (g_cluster.disabled_nodes.count(id) > 0);
        uint64_t heartbeat   = 0;
        std::string status_str = "DEAD";

        // Query gossip status only for non-disabled nodes
        if (!is_disabled && g_cluster.gossip_nodes.count(id)) {
            auto gossip_status = g_cluster.gossip_nodes[id]->getNodeStatus(id);
            status_str = (gossip_status == NodeStatus::ALIVE) ? "ALIVE" : "DEAD";

            // Retrieve the latest heartbeat counter from this node's own view
            for (const auto& m : g_cluster.gossip_nodes[id]->getMembershipList()) {
                if (m.node_id == id) heartbeat = m.heartbeat_counter;
            }
        }

        int key_count  = static_cast<int>(g_cluster.node_data.count(id)
                                           ? g_cluster.node_data.at(id).size() : 0);
        int sstables   = g_cluster.node_sstable_counts[id];
        int wal_count  = static_cast<int>(g_cluster.node_wal_entries[id].size());

        nodes.obj(JsonObj()
            .str("id",            id)
            .str("status",        status_str)
            .boolean("is_alive",  !is_disabled)
            .num("heartbeat",     static_cast<long long>(heartbeat))
            .num("key_count",     key_count)
            .num("sstable_count", sstables)
            .num("wal_count",     wal_count)
            .build());
    }

    return jsonOk(JsonObj()
        .boolean("success", true)
        .nested("nodes", nodes.build())
        .num("ring_node_count", static_cast<long long>(g_cluster.ring->nodeCount()))
        .build());
}

// ─── 2. Node Lifecycle (Add / Remove) ────────────────────────────────────

/**
 * @brief  POST /api/cluster/add_node  |  POST /api/hash/add_node
 *
 * Registers a new node simultaneously in the consistent hash ring, gossip
 * mesh, and storage layer.
 *
 * @param body  Request body containing {"node_id": "…"}.
 */
static std::string handleAddNode(const std::string& body) {
    std::string node_id = extractJsonString(body, "node_id");
    if (!node_id.empty()) g_cluster.addNodeInternal(node_id);

    return jsonOk(JsonObj()
        .boolean("success", true)
        .str("message", "Node added to Ring, Gossip, and Storage")
        .build());
}

/**
 * @brief  POST /api/cluster/remove_node  |  POST /api/hash/remove_node
 *
 * Permanently removes a node from all cluster sub-systems.
 *
 * @param body  Request body containing {"node_id": "…"}.
 */
static std::string handleRemoveNode(const std::string& body) {
    std::string node_id = extractJsonString(body, "node_id");
    if (!node_id.empty()) g_cluster.removeNodeInternal(node_id);

    return jsonOk(JsonObj()
        .boolean("success", true)
        .str("message", "Node removed from all cluster components")
        .build());
}

// ─── 3. Chaos Engineering (Kill / Revive) ────────────────────────────────

/**
 * @brief  POST /api/cluster/kill_node
 *
 * Simulates a node crash by adding it to the disabled_nodes set.  Killed
 * nodes stop emitting gossip heartbeats; the gossip protocol will eventually
 * mark them as DEAD via timeout.
 *
 * @param body  Request body containing {"node_id": "…"}.
 */
static std::string handleKillNode(const std::string& body) {
    std::string node_id = extractJsonString(body, "node_id");
    if (!node_id.empty()) g_cluster.disabled_nodes.insert(node_id);

    return jsonOk(JsonObj()
        .boolean("success", true)
        .str("message", "Node killed; Gossip heartbeats stopped")
        .build());
}

/**
 * @brief  POST /api/cluster/revive_node
 *
 * Brings a previously killed node back online and immediately fires one
 * gossip heartbeat so peers learn about the revival quickly.
 *
 * @param body  Request body containing {"node_id": "…"}.
 */
static std::string handleReviveNode(const std::string& body) {
    std::string node_id = extractJsonString(body, "node_id");
    if (!node_id.empty()) {
        g_cluster.disabled_nodes.erase(node_id);
        if (g_cluster.gossip_nodes.count(node_id)) {
            g_cluster.gossip_nodes[node_id]->heartbeat();
        }
    }

    return jsonOk(JsonObj()
        .boolean("success", true)
        .str("message", "Node revived; Gossip heartbeats resumed")
        .build());
}

// ─── 4. Gossip Protocol ───────────────────────────────────────────────────

/**
 * @brief  POST /api/gossip/tick  |  POST /api/cluster/gossip_tick
 *
 * Manually triggers one full gossip round: all alive nodes increment their
 * heartbeat counters, then exchange membership lists pairwise.
 *
 * The background thread also runs this automatically every 1 second.
 */
static std::string handleGossipTick() {
    // Step 1: Increment heartbeat on every alive node
    for (const auto& id : g_cluster.node_order) {
        if (!g_cluster.disabled_nodes.count(id)) {
            g_cluster.gossip_nodes[id]->heartbeat();
        }
    }

    // Step 2: Pairwise membership list exchange between alive nodes
    for (const auto& id1 : g_cluster.node_order) {
        if (g_cluster.disabled_nodes.count(id1)) continue;
        for (const auto& id2 : g_cluster.node_order) {
            if (id1 != id2 && !g_cluster.disabled_nodes.count(id2)) {
                g_cluster.gossip_nodes[id1]->receiveHeartbeat(
                    g_cluster.gossip_nodes[id2]->getMembershipList());
            }
        }
    }

    return jsonOk(JsonObj()
        .boolean("success", true)
        .str("message", "Gossip propagation complete")
        .build());
}

/**
 * @brief  GET/POST /api/gossip/membership
 *
 * Returns the membership table as seen by a specific node.  Each entry
 * includes the peer's node_id, latest heartbeat counter, timestamp, and
 * liveness status (ALIVE/DEAD).
 *
 * node_id can be in the JSON body or as a URL query parameter (?node_id=…).
 * Defaults to the first node in the cluster if omitted.
 *
 * @param path  Full request path (used to parse the query string).
 * @param body  Raw request body (used to parse JSON).
 */
static std::string handleGossipMembership(const std::string& path, const std::string& body) {
    // Resolve node_id: JSON body → query string → first node default
    std::string node_id = extractJsonString(body, "node_id");
    if (node_id.empty()) {
        std::size_t q = path.find("node_id=");
        if (q != std::string::npos) {
            node_id = path.substr(q + 8);
            std::size_t amp = node_id.find('&');
            if (amp != std::string::npos) node_id = node_id.substr(0, amp);
            node_id = urlDecode(node_id);
        }
    }
    if (node_id.empty() && !g_cluster.node_order.empty()) {
        node_id = g_cluster.node_order[0];
    }

    JsonArr members;
    if (g_cluster.gossip_nodes.count(node_id)) {
        for (const auto& m : g_cluster.gossip_nodes[node_id]->getMembershipList()) {
            auto   node_status = g_cluster.gossip_nodes[node_id]->getNodeStatus(m.node_id);
            bool   is_disabled = (g_cluster.disabled_nodes.count(m.node_id) > 0);
            std::string status = (!is_disabled && node_status == NodeStatus::ALIVE)
                                     ? "ALIVE" : "DEAD";

            members.obj(JsonObj()
                .str("node_id",       m.node_id)
                .num("heartbeat",     static_cast<long long>(m.heartbeat_counter))
                .num("timestamp_ms",  static_cast<long long>(m.timestamp_ms))
                .str("status",        status)
                .build());
        }
    }

    return jsonOk(JsonObj()
        .boolean("success",      true)
        .str("viewing_node",     node_id)
        .nested("members",       members.build())
        .build());
}

// ─── 5. Consistent Hash Ring — Key Routing ───────────────────────────────

/**
 * @brief  POST /api/hash/route  |  POST /api/cluster/route
 *
 * Resolves which ring nodes own a given key, returning the primary node and
 * the requested number of replica nodes along with their current liveness.
 *
 * @param body  Request body containing {"key": "…", "count": N}.
 */
static std::string handleKeyRoute(const std::string& body) {
    std::string key = extractJsonString(body, "key");
    if (key.empty()) key = "user:alice";

    int count = extractJsonInt(body, "count", 2);
    if (count < 1) count = 1;

    std::size_t req_count = std::min(static_cast<std::size_t>(count),
                                     g_cluster.ring->nodeCount());

    std::string              primary  = g_cluster.ring->empty() ? "" : g_cluster.ring->getNode(key);
    std::vector<std::string> replicas = (req_count > 0)
                                            ? g_cluster.ring->getReplicaNodes(key, req_count)
                                            : std::vector<std::string>{};

    JsonArr replica_arr;
    for (const auto& rep : replicas) {
        bool is_dead = (g_cluster.disabled_nodes.count(rep) > 0);
        replica_arr.obj(JsonObj()
            .str("id",     rep)
            .str("status", is_dead ? "DEAD" : "ALIVE")
            .build());
    }

    return jsonOk(JsonObj()
        .boolean("success", true)
        .str("key",          key)
        .str("primary",      primary)
        .nested("replicas",  replica_arr.build())
        .num("ring_node_count", static_cast<long long>(g_cluster.ring->nodeCount()))
        .build());
}

// ─── 6. Distributed PUT (Sloppy Quorum + Hinted Handoff) ─────────────────

/**
 * @brief  POST /api/cluster/put  |  POST /api/storage/put
 *
 * Coordinated write with Dynamo-style sloppy quorum:
 *
 *  1. Resolve the coordinator (defaults to the primary ring owner).
 *  2. Identify N=2 planned replica nodes from the ring.
 *  3. Increment the key's VectorClock at the coordinator.
 *  4. Write to every alive planned replica via its StorageEngine.
 *  5. If any planned replica is down, walk the ring for the next alive node
 *     and write there as a hinted handoff (noted in the WAL entry).
 *
 * @param body  Request body: {"key":"…","val":"…","coordinator":"…"}.
 */
static std::string handleDistributedPut(const std::string& body) {
    std::string key         = extractJsonString(body, "key");
    std::string val         = extractJsonString(body, "val");
    std::string coordinator = extractJsonString(body, "coordinator");

    if (key.empty())            return jsonBadRequest("Key cannot be empty");
    if (g_cluster.ring->empty()) return jsonServerError("Cluster ring is empty");

    // Default coordinator: primary ring owner of this key
    if (coordinator.empty() || !g_cluster.ring->hasNode(coordinator)) {
        coordinator = g_cluster.ring->getNode(key);
    }

    // ── Replica selection ──────────────────────────────────────────────────
    std::size_t replication_factor =
        std::min<std::size_t>(2, g_cluster.ring->nodeCount());
    std::vector<std::string> planned_replicas =
        g_cluster.ring->getReplicaNodes(key, replication_factor);

    std::vector<std::string> written_nodes;
    std::vector<std::string> failed_nodes;

    // ── Vector clock update ───────────────────────────────────────────────
    VectorClock& clock = g_cluster.key_clocks[key];
    clock.increment(coordinator);
    std::string clock_str = clockToString(clock);

    // ── Write to alive planned replicas ───────────────────────────────────
    for (const auto& replica_id : planned_replicas) {
        if (g_cluster.disabled_nodes.count(replica_id)) {
            failed_nodes.push_back(replica_id);
            continue;
        }
        auto it = g_cluster.storage_nodes.find(replica_id);
        if (it != g_cluster.storage_nodes.end()) {
            it->second->put(key, val);  // Real C++ WAL + MemTable write
            g_cluster.node_data[replica_id][key] = val;
            g_cluster.node_wal_entries[replica_id].push_back("PUT " + key + "=" + val);
            written_nodes.push_back(replica_id);
        }
    }

    // ── Sloppy quorum: hinted handoff to next alive ring node ─────────────
    if (!failed_nodes.empty() && written_nodes.size() < replication_factor) {
        auto ring_walk = g_cluster.ring->getReplicaNodes(key, g_cluster.ring->nodeCount());
        for (const auto& candidate : ring_walk) {
            if (written_nodes.size() >= replication_factor) break;

            bool already_written = std::find(written_nodes.begin(),
                                             written_nodes.end(),
                                             candidate) != written_nodes.end();
            if (!g_cluster.disabled_nodes.count(candidate) && !already_written) {
                auto it = g_cluster.storage_nodes.find(candidate);
                if (it != g_cluster.storage_nodes.end()) {
                    it->second->put(key, val);
                    g_cluster.node_data[candidate][key] = val;
                    g_cluster.node_wal_entries[candidate].push_back(
                        "PUT " + key + "=" + val + " (Handoff)");
                    written_nodes.push_back(candidate);
                }
            }
        }
    }

    // ── Build response ────────────────────────────────────────────────────
    JsonArr written_arr, failed_arr;
    for (const auto& n : written_nodes) written_arr.str(n);
    for (const auto& n : failed_nodes)  failed_arr.str(n);

    return jsonOk(JsonObj()
        .boolean("success",           true)
        .str("key",                   key)
        .str("value",                 val)
        .str("coordinator",           coordinator)
        .str("vector_clock",          clock_str)
        .nested("written_to",         written_arr.build())
        .nested("failed_replicas",    failed_arr.build())
        .build());
}

// ─── 7. Distributed GET (Replica Failover Read) ───────────────────────────

/**
 * @brief  GET|POST /api/cluster/get  |  /api/storage/get
 *
 * Read with automatic replica failover.
 *
 * Candidate priority order:
 *  1. Explicitly requested node (if alive).
 *  2. Primary + secondary ring replicas (consistent hash order).
 *  3. Any other alive cluster node (covers hinted handoff scenarios).
 *
 * The response includes a "failover" flag so the UI can highlight when the
 * read came from a secondary.
 *
 * Supports POST {"key":…, "node_id":…} and GET ?key=… query param.
 *
 * @param path    Full request path (for GET query param parsing).
 * @param method  HTTP method string.
 * @param body    Raw request body (for POST JSON parsing).
 */
static std::string handleDistributedGet(const std::string& path,
                                        const std::string& method,
                                        const std::string& body) {
    std::string key            = "";
    std::string requested_node = "";

    if (method == "POST") {
        key            = extractJsonString(body, "key");
        requested_node = extractJsonString(body, "node_id");
    } else {
        // Parse key from GET query string: …?key=<value>
        std::size_t q = path.find("key=");
        if (q != std::string::npos) {
            key = path.substr(q + 4);
            std::size_t amp = key.find('&');
            if (amp != std::string::npos) key = key.substr(0, amp);
            key = urlDecode(key);
        }
    }

    if (key.empty()) return jsonBadRequest("Key required");

    // ── Build preference list (deduplicated, priority-ordered) ────────────
    std::size_t ring_size = g_cluster.ring->nodeCount();
    std::vector<std::string> ring_replicas = (ring_size > 0)
        ? g_cluster.ring->getReplicaNodes(key, ring_size)
        : std::vector<std::string>{};

    std::vector<std::string> candidates;
    auto push_unique = [&](const std::string& n) {
        if (std::find(candidates.begin(), candidates.end(), n) == candidates.end()) {
            candidates.push_back(n);
        }
    };

    if (!requested_node.empty() && !g_cluster.disabled_nodes.count(requested_node)) {
        push_unique(requested_node);
    }
    for (const auto& rep : ring_replicas)        push_unique(rep);
    for (const auto& nid : g_cluster.node_order) push_unique(nid);

    // ── Read from first alive candidate that holds the key ─────────────────
    for (const auto& replica_id : candidates) {
        if (g_cluster.disabled_nodes.count(replica_id)) continue;

        auto it = g_cluster.storage_nodes.find(replica_id);
        if (it != g_cluster.storage_nodes.end()) {
            auto val = it->second->get(key);  // Real C++ MemTable + SSTable read
            if (val.has_value()) {
                bool is_failover = (!requested_node.empty() && requested_node != replica_id);

                return jsonOk(JsonObj()
                    .boolean("success",         true)
                    .boolean("found",           true)
                    .str("key",                 key)
                    .str("value",               *val)
                    .str("from_node",           replica_id)
                    .boolean("failover",        is_failover)
                    .str("requested_node",      requested_node)
                    .str("vector_clock",        clockToString(g_cluster.key_clocks[key]))
                    .build());
            }
        }
    }

    // Key not found on any alive replica
    return jsonOk(JsonObj()
        .boolean("success", true)
        .boolean("found",   false)
        .str("key",         key)
        .build());
}

// ─── 8. Storage Engine Operations (Flush / Recover) ──────────────────────

/**
 * @brief  POST /api/cluster/flush  |  POST /api/storage/flush
 *
 * Forces the MemTable for a given node to be written out to a new SSTable
 * file on disk (LSM compaction trigger).  Clears the UI WAL log and
 * increments the SSTable count.
 *
 * @param body  Request body containing {"node_id": "…"}.
 */
static std::string handleStorageFlush(const std::string& body) {
    std::string node_id = extractJsonString(body, "node_id");
    if (node_id.empty() && !g_cluster.node_order.empty()) {
        node_id = g_cluster.node_order[0];
    }

    auto it = g_cluster.storage_nodes.find(node_id);
    if (it == g_cluster.storage_nodes.end()) return jsonNotFound("Node not found");

    it->second->flush();
    g_cluster.node_sstable_counts[node_id] += 1;
    g_cluster.node_wal_entries[node_id].clear();

    return jsonOk(JsonObj()
        .boolean("success", true)
        .str("message", "C++ MemTable flushed to SSTable on disk")
        .build());
}

/**
 * @brief  POST /api/cluster/recover  |  POST /api/storage/recover
 *
 * Replays the WAL log for the given node's StorageEngine, restoring its
 * MemTable state as if the server had just restarted after a crash.
 *
 * @param body  Request body containing {"node_id": "…"}.
 */
static std::string handleStorageRecover(const std::string& body) {
    std::string node_id = extractJsonString(body, "node_id");
    if (node_id.empty() && !g_cluster.node_order.empty()) {
        node_id = g_cluster.node_order[0];
    }

    auto it = g_cluster.storage_nodes.find(node_id);
    if (it == g_cluster.storage_nodes.end()) return jsonNotFound("Node not found");

    it->second->recover();

    return jsonOk(JsonObj()
        .boolean("success", true)
        .str("message", "C++ WAL log replayed successfully")
        .build());
}

// ─── 9. Merkle Tree Anti-Entropy ─────────────────────────────────────────

/**
 * @brief  POST /api/cluster/merkle_diff
 *
 * Computes Merkle trees for two nodes and returns the list of key-range
 * buckets that differ.  Defaults to node_order[0] vs node_order[1].
 *
 * @param body  Request body: {"node_a":"…","node_b":"…"}.
 */
static std::string handleMerkleDiff(const std::string& body) {
    std::string node_a = extractJsonString(body, "node_a");
    std::string node_b = extractJsonString(body, "node_b");

    if (node_a.empty() || node_b.empty()) {
        if (g_cluster.node_order.size() >= 2) {
            node_a = g_cluster.node_order[0];
            node_b = g_cluster.node_order[1];
        } else {
            return jsonBadRequest("Requires 2 nodes");
        }
    }

    // Build Merkle inputs from in-memory node_data mirrors
    std::vector<std::pair<std::string, std::string>> entries_a, entries_b;
    for (const auto& [k, v] : g_cluster.node_data[node_a]) entries_a.emplace_back(k, v);
    for (const auto& [k, v] : g_cluster.node_data[node_b]) entries_b.emplace_back(k, v);

    MerkleTree tree_a(8), tree_b(8);
    tree_a.build(entries_a);
    tree_b.build(entries_b);

    JsonArr diff_arr;
    for (const auto& range : tree_a.diff(tree_b)) diff_arr.str(range.start_key);

    return jsonOk(JsonObj()
        .boolean("success", true)
        .str("node_a",      node_a)
        .str("node_b",      node_b)
        .str("rootA",       tree_a.getRootHash())
        .str("rootB",       tree_b.getRootHash())
        .nested("diff_buckets", diff_arr.build())
        .build());
}

/**
 * @brief  POST /api/cluster/merkle_repair
 *
 * Performs a full bidirectional key synchronisation between two nodes.
 * After this call, both nodes will hold the union of their key-value pairs.
 *
 * @param body  Request body: {"node_a":"…","node_b":"…"}.
 */
static std::string handleMerkleRepair(const std::string& body) {
    std::string node_a = extractJsonString(body, "node_a");
    std::string node_b = extractJsonString(body, "node_b");

    if ((node_a.empty() || node_b.empty()) && g_cluster.node_order.size() >= 2) {
        node_a = g_cluster.node_order[0];
        node_b = g_cluster.node_order[1];
    }

    // Sync A → B and B → A (full bidirectional anti-entropy repair)
    for (const auto& [k, v] : g_cluster.node_data[node_a]) {
        g_cluster.storage_nodes[node_b]->put(k, v);
        g_cluster.node_data[node_b][k] = v;
    }
    for (const auto& [k, v] : g_cluster.node_data[node_b]) {
        g_cluster.storage_nodes[node_a]->put(k, v);
        g_cluster.node_data[node_a][k] = v;
    }

    return jsonOk(JsonObj()
        .boolean("success", true)
        .str("message", "Replicas synchronized via anti-entropy")
        .build());
}

// ─── 10. Vector Clock Causality Comparison ───────────────────────────────

/**
 * @brief  POST /api/vector_clock/compare
 *
 * Accepts two manually-specified vector clock values and returns the causal
 * relationship: BEFORE, AFTER, CONCURRENT, or EQUAL.
 *
 * Expected body fields:  a_sx, a_sy, a_sz, b_sx, b_sy, b_sz  (tick counts).
 *
 * @param body  Raw JSON request body.
 */
static std::string handleVectorClockCompare(const std::string& body) {
    VectorClock clockA, clockB;

    // Build clock A from individual server tick counts
    for (int i = 0; i < extractJsonInt(body, "a_sx"); ++i) clockA.increment("Sx");
    for (int i = 0; i < extractJsonInt(body, "a_sy"); ++i) clockA.increment("Sy");
    for (int i = 0; i < extractJsonInt(body, "a_sz"); ++i) clockA.increment("Sz");

    // Build clock B
    for (int i = 0; i < extractJsonInt(body, "b_sx"); ++i) clockB.increment("Sx");
    for (int i = 0; i < extractJsonInt(body, "b_sy"); ++i) clockB.increment("Sy");
    for (int i = 0; i < extractJsonInt(body, "b_sz"); ++i) clockB.increment("Sz");

    ClockComparison cmp     = clockA.compare(clockB);
    std::string     verdict = (cmp == ClockComparison::BEFORE)     ? "BEFORE"     :
                              (cmp == ClockComparison::AFTER)      ? "AFTER"      :
                              (cmp == ClockComparison::CONCURRENT) ? "CONCURRENT" : "EQUAL";

    return jsonOk(JsonObj()
        .boolean("success", true)
        .str("verdict",     verdict)
        .build());
}

// ─── 11. Cluster-Wide Key Inventory ─────────────────────────────────────

/**
 * @brief  GET /api/cluster/keys
 *
 * Returns the full set of known keys across all nodes, enriched with per-key
 * replication metadata: primary node, expected targets, which targets are
 * holding the key, which are missing it, and the vector clock string.
 */
static std::string handleClusterKeys() {
    // Union of all keys across all nodes and all tracked vector clocks
    std::set<std::string> all_keys;
    for (const auto& [nid, kmap] : g_cluster.node_data)
        for (const auto& [k, v] : kmap)
            all_keys.insert(k);
    for (const auto& [k, vc] : g_cluster.key_clocks)
        all_keys.insert(k);

    JsonArr keys_arr;
    for (const auto& key : all_keys) {
        std::string primary = g_cluster.ring->empty() ? "" : g_cluster.ring->getNode(key);
        std::size_t rep_factor =
            std::min<std::size_t>(2, g_cluster.ring->nodeCount());
        std::vector<std::string> targets =
            g_cluster.ring->getReplicaNodes(key, rep_factor);

        std::vector<std::string> holding, missing;
        for (const auto& target : targets) {
            if (g_cluster.node_data.count(target) && g_cluster.node_data[target].count(key)) {
                holding.push_back(target);
            } else {
                missing.push_back(target);
            }
        }

        std::string clock_str;
        if (g_cluster.key_clocks.count(key)) {
            clock_str = clockToString(g_cluster.key_clocks[key]);
        }

        JsonArr targets_arr, holding_arr, missing_arr;
        for (const auto& t : targets) targets_arr.str(t);
        for (const auto& h : holding) holding_arr.str(h);
        for (const auto& m : missing) missing_arr.str(m);

        keys_arr.obj(JsonObj()
            .str("key",           key)
            .str("primary",       primary)
            .str("vector_clock",  clock_str)
            .nested("targets",    targets_arr.build())
            .nested("holding",    holding_arr.build())
            .nested("missing",    missing_arr.build())
            .build());
    }

    return jsonOk(JsonObj()
        .boolean("success", true)
        .nested("keys",     keys_arr.build())
        .build());
}

// ─── 12. Per-Node Storage Inspector ──────────────────────────────────────

/**
 * @brief  GET|POST /api/cluster/node_storage
 *
 * Returns the in-memory key-value entries, WAL log, and SSTable count for
 * a specific node.  Used by the UI's "Storage Inspector" panel.
 *
 * @param path  Full request path (for GET query param parsing).
 * @param body  Raw request body (for POST JSON parsing).
 */
static std::string handleNodeStorage(const std::string& path, const std::string& body) {
    std::string node_id = extractJsonString(body, "node_id");
    if (node_id.empty()) {
        std::size_t q = path.find("node_id=");
        if (q != std::string::npos) {
            node_id = path.substr(q + 8);
            std::size_t amp = node_id.find('&');
            if (amp != std::string::npos) node_id = node_id.substr(0, amp);
            node_id = urlDecode(node_id);
        }
    }
    if (node_id.empty() && !g_cluster.node_order.empty()) {
        node_id = g_cluster.node_order[0];
    }

    JsonArr entries_arr;
    if (g_cluster.node_data.count(node_id)) {
        for (const auto& [k, v] : g_cluster.node_data[node_id]) {
            entries_arr.obj(JsonObj().str("key", k).str("value", v).build());
        }
    }

    JsonArr wal_arr;
    if (g_cluster.node_wal_entries.count(node_id)) {
        for (const auto& entry : g_cluster.node_wal_entries[node_id]) {
            wal_arr.str(entry);
        }
    }

    return jsonOk(JsonObj()
        .boolean("success",   true)
        .str("node_id",       node_id)
        .nested("entries",    entries_arr.build())
        .nested("wal_entries", wal_arr.build())
        .num("sstable_count", g_cluster.node_sstable_counts[node_id])
        .build());
}

// ─── 13. Delete / Tombstone ───────────────────────────────────────────────

/**
 * @brief  POST /api/storage/del  |  POST /api/cluster/del
 *
 * Writes a tombstone marker for a key on a specific node.  Removes the key
 * from the in-memory mirror and appends a DELETE entry to the WAL log.
 *
 * @param body  Request body: {"key":"…","node_id":"…"}.
 */
static std::string handleDeleteKey(const std::string& body) {
    std::string key     = extractJsonString(body, "key");
    std::string node_id = extractJsonString(body, "node_id");
    if (node_id.empty() && !g_cluster.node_order.empty()) {
        node_id = g_cluster.node_order[0];
    }

    if (node_id.empty() || !g_cluster.storage_nodes.count(node_id)) {
        return jsonBadRequest("Node not found");
    }

    g_cluster.node_data[node_id].erase(key);
    g_cluster.node_wal_entries[node_id].push_back("DELETE " + key + " (TOMBSTONE)");

    return jsonOk(JsonObj()
        .boolean("success", true)
        .str("message", "Tombstone written on node")
        .build());
}

// ─── 14. Demo Seed ────────────────────────────────────────────────────────

/**
 * @brief  POST /api/cluster/seed_demo
 *
 * Populates the cluster with a fixed set of sample keys across the 3 default
 * nodes so the UI shows a realistic cluster view immediately.
 */
static std::string handleSeedDemo() {
    // Ensure all 3 default nodes exist and are alive
    for (const auto& id : {"node-alpha", "node-beta", "node-gamma"}) {
        g_cluster.addNodeInternal(id);
        g_cluster.disabled_nodes.erase(id);
    }

    struct SampleKey { std::string key, val, coord; };
    const std::vector<SampleKey> samples = {
        {"user:alice",       "Alice Smith (ID: 101)",          "node-alpha"},
        {"user:bob",         "Bob Jones (ID: 102)",            "node-beta"},
        {"order:9901",       "MacBook Pro M3 + USB-C Hub",     "node-alpha"},
        {"session:prod_xyz", "authenticated_session_token_99", "node-gamma"},
    };

    for (const auto& s : samples) {
        std::size_t req_count =
            std::min<std::size_t>(2, g_cluster.ring->nodeCount());
        auto replicas = g_cluster.ring->getReplicaNodes(s.key, req_count);

        g_cluster.key_clocks[s.key].increment(s.coord);

        for (const auto& rep : replicas) {
            g_cluster.storage_nodes[rep]->put(s.key, s.val);
            g_cluster.node_data[rep][s.key] = s.val;
            g_cluster.node_wal_entries[rep].push_back("PUT " + s.key + "=" + s.val);
        }
    }

    return jsonOk(JsonObj()
        .boolean("success", true)
        .str("message", "Demo cluster seeded with 3 nodes and 4 replicated keys")
        .build());
}

// ============================================================================
// Primary API Router
// ============================================================================

/**
 * @brief Dispatches an incoming API request to the appropriate handler.
 *
 * Acquires the global cluster mutex and matches the (method, path) pair
 * against known routes, delegating to the appropriate handleXxx() function.
 * Returns a 404 JSON error for unknown routes.
 *
 * All route handlers must not re-acquire the lock — it is already held.
 *
 * @param method  HTTP method string (e.g. "GET", "POST").
 * @param path    Request path, possibly including a query string.
 * @param body    Raw request body (may be empty for GET requests).
 * @return        Complete HTTP response string ready to send to the client.
 */
static std::string handleApiRequest(const std::string& method,
                                    const std::string& path,
                                    const std::string& body) {
    std::lock_guard<std::mutex> lock(g_cluster.mtx);

    // ── Cluster topology ──────────────────────────────────────────────────
    if (path == "/api/cluster/state" && method == "GET")
        return handleClusterState();

    if ((path == "/api/cluster/add_node" || path == "/api/hash/add_node") && method == "POST")
        return handleAddNode(body);

    if ((path == "/api/cluster/remove_node" || path == "/api/hash/remove_node") && method == "POST")
        return handleRemoveNode(body);

    // ── Chaos engineering ─────────────────────────────────────────────────
    if (path == "/api/cluster/kill_node" && method == "POST")
        return handleKillNode(body);

    if (path == "/api/cluster/revive_node" && method == "POST")
        return handleReviveNode(body);

    // ── Gossip protocol ───────────────────────────────────────────────────
    if ((path == "/api/gossip/tick" || path == "/api/cluster/gossip_tick") && method == "POST")
        return handleGossipTick();

    if (path.rfind("/api/gossip/membership", 0) == 0)
        return handleGossipMembership(path, body);

    // ── Consistent hash ring ──────────────────────────────────────────────
    if ((path == "/api/hash/route" || path == "/api/cluster/route") && method == "POST")
        return handleKeyRoute(body);

    // ── Distributed KV operations ─────────────────────────────────────────
    if ((path == "/api/cluster/put" || path == "/api/storage/put") && method == "POST")
        return handleDistributedPut(body);

    if (path.rfind("/api/cluster/get", 0) == 0 || path.rfind("/api/storage/get", 0) == 0)
        return handleDistributedGet(path, method, body);

    if ((path == "/api/storage/del" || path == "/api/cluster/del") && method == "POST")
        return handleDeleteKey(body);

    // ── Storage engine operations ─────────────────────────────────────────
    if ((path == "/api/cluster/flush" || path == "/api/storage/flush") && method == "POST")
        return handleStorageFlush(body);

    if ((path == "/api/cluster/recover" || path == "/api/storage/recover") && method == "POST")
        return handleStorageRecover(body);

    // ── Merkle tree anti-entropy ──────────────────────────────────────────
    if (path == "/api/cluster/merkle_diff" && method == "POST")
        return handleMerkleDiff(body);

    if (path == "/api/cluster/merkle_repair" && method == "POST")
        return handleMerkleRepair(body);

    // ── Vector clock causality ────────────────────────────────────────────
    if (path == "/api/vector_clock/compare" && method == "POST")
        return handleVectorClockCompare(body);

    // ── Cluster inspection ────────────────────────────────────────────────
    if (path == "/api/cluster/keys" && method == "GET")
        return handleClusterKeys();

    if (path.rfind("/api/cluster/node_storage", 0) == 0)
        return handleNodeStorage(path, body);

    // ── Demo seeding ──────────────────────────────────────────────────────
    if (path == "/api/cluster/seed_demo" && method == "POST")
        return handleSeedDemo();

    return makeResponse(404, "application/json",
                        JsonObj().str("error", "Unknown API endpoint").build());
}

// ============================================================================
// HTTP Request Parser & Static File Server
// ============================================================================

/**
 * @brief Handles a single TCP client connection end-to-end.
 *
 * Reads the HTTP request, extracts method/path/body, then either dispatches
 * to the API router or serves a static file from the web root directory.
 *
 * @param client_fd  Accepted socket file descriptor for this connection.
 * @param web_root   Filesystem path to the directory containing static files.
 */
static void handleClient(int client_fd, const std::string& web_root) {
    char buffer[8192];
    std::memset(buffer, 0, sizeof(buffer));

    ssize_t bytes_read = read(client_fd, buffer, sizeof(buffer) - 1);
    if (bytes_read <= 0) {
        close(client_fd);
        return;
    }

    std::string        request(buffer, static_cast<std::size_t>(bytes_read));
    std::istringstream req_stream(request);
    std::string        method, path, protocol;
    req_stream >> method >> path >> protocol;

    // Respond to CORS preflight immediately
    if (method == "OPTIONS") {
        std::string resp = makeResponse(200, "text/plain", "");
        write(client_fd, resp.c_str(), resp.size());
        close(client_fd);
        return;
    }

    // Extract body (everything after the blank header line)
    std::string body;
    std::size_t header_end = request.find("\r\n\r\n");
    if (header_end != std::string::npos) {
        body = request.substr(header_end + 4);
    }

    // Route /api/ requests to the API dispatcher
    if (path.rfind("/api/", 0) == 0) {
        std::string response = handleApiRequest(method, path, body);
        write(client_fd, response.c_str(), response.size());
        close(client_fd);
        return;
    }

    // Serve static files from web_root
    if (path == "/") path = "/index.html";

    std::string rel_path = path;
    if (!rel_path.empty() && rel_path[0] == '/') rel_path = rel_path.substr(1);

    fs::path file_path = fs::path(web_root) / rel_path;
    if (fs::exists(file_path) && fs::is_regular_file(file_path)) {
        std::ifstream      file(file_path, std::ios::binary);
        std::ostringstream ss;
        ss << file.rdbuf();
        std::string content = ss.str();

        // Derive MIME type from file extension
        std::string content_type = "text/plain";
        if      (file_path.extension() == ".html") content_type = "text/html";
        else if (file_path.extension() == ".css")  content_type = "text/css";
        else if (file_path.extension() == ".js")   content_type = "application/javascript";
        else if (file_path.extension() == ".json") content_type = "application/json";
        else if (file_path.extension() == ".svg")  content_type = "image/svg+xml";

        std::string response = makeResponse(200, content_type, content);
        write(client_fd, response.c_str(), response.size());
    } else {
        std::string response = makeResponse(404, "text/plain", "File Not Found: " + path);
        write(client_fd, response.c_str(), response.size());
    }

    close(client_fd);
}

// ============================================================================
// Background Gossip Thread
// ============================================================================

/**
 * @brief Runs a continuous 1 Hz gossip heartbeat loop in a detached thread.
 *
 * Every second:
 *  1. Increments the heartbeat counter on every alive node.
 *  2. Performs pairwise membership list exchange between all alive peers.
 */
static void runGossipThread() {
    while (true) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        std::lock_guard<std::mutex> lock(g_cluster.mtx);

        // Step 1: Monotonic heartbeats for every alive node
        for (const auto& id : g_cluster.node_order) {
            if (!g_cluster.disabled_nodes.count(id) && g_cluster.gossip_nodes.count(id)) {
                g_cluster.gossip_nodes[id]->heartbeat();
            }
        }

        // Step 2: Pairwise gossip exchange between all alive peers
        for (const auto& id1 : g_cluster.node_order) {
            if (g_cluster.disabled_nodes.count(id1)) continue;
            for (const auto& id2 : g_cluster.node_order) {
                if (id1 != id2 && !g_cluster.disabled_nodes.count(id2)) {
                    if (g_cluster.gossip_nodes.count(id1) && g_cluster.gossip_nodes.count(id2)) {
                        g_cluster.gossip_nodes[id1]->receiveHeartbeat(
                            g_cluster.gossip_nodes[id2]->getMembershipList());
                    }
                }
            }
        }
    }
}

// ============================================================================
// main — Server Entry Point
// ============================================================================

/**
 * @brief Initialises the TCP server and starts the accept loop.
 *
 * Usage:  ./kvstore_server [port]    (default port: 8080)
 *
 * Startup sequence:
 *  1. Parse port from argv[1] (optional).
 *  2. Locate the web root directory relative to cwd.
 *  3. Create + bind + listen on a TCP socket.
 *  4. Detach the background gossip thread.
 *  5. Accept client connections and spawn a detached handler thread per client.
 *
 * @param argc  Argument count.
 * @param argv  Argument values; argv[1] may be a port number.
 * @return 0 on clean exit, 1 on fatal socket error.
 */
int main(int argc, char* argv[]) {
    int port = 8080;
    if (argc > 1) {
        port = std::atoi(argv[1]);
        if (port <= 0) port = 8080;
    }

    fs::path exe_dir  = fs::current_path();
    fs::path web_root = exe_dir / "web";
    if (!fs::exists(web_root)) web_root = exe_dir.parent_path() / "web";

    // ── Socket setup ──────────────────────────────────────────────────────
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        std::cerr << "Failed to create socket\n";
        return 1;
    }

    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in address{};
    address.sin_family      = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port        = htons(static_cast<uint16_t>(port));

    if (bind(server_fd, reinterpret_cast<struct sockaddr*>(&address), sizeof(address)) < 0) {
        std::cerr << "Failed to bind to port " << port << "\n";
        close(server_fd);
        return 1;
    }

    if (listen(server_fd, 32) < 0) {
        std::cerr << "Failed to listen on socket\n";
        close(server_fd);
        return 1;
    }

    std::cout << "\n======================================================================\n"
              << " 🚀 Unified C++ KV-Store Server with Linked C++ Library is running!\n"
              << " Serving Web UI from: " << web_root << "\n"
              << " URL: http://localhost:" << port << "\n"
              << "======================================================================\n\n";

    // ── Background gossip thread (1 Hz heartbeat + peer exchange) ─────────
    std::thread(runGossipThread).detach();

    // ── Main accept loop (one detached thread per client) ─────────────────
    while (true) {
        sockaddr_in client_addr{};
        socklen_t   client_len = sizeof(client_addr);
        int         client_fd  = accept(server_fd,
                                        reinterpret_cast<struct sockaddr*>(&client_addr),
                                        &client_len);
        if (client_fd < 0) continue;

        std::thread([client_fd, web_root_str = web_root.string()]() {
            handleClient(client_fd, web_root_str);
        }).detach();
    }

    close(server_fd);
    return 0;
}
