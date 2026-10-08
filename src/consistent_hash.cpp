#include "kvstore/consistent_hash.h"

#include <stdexcept>
#include <unordered_set>

namespace kvstore {

// ── Constructor ─────────────────────────────────────────────────────────────
//
// The consistent hash ring is a virtual circle of numbers from 0 to 2^32.
// Both nodes and keys are hashed onto this circle. To find which node
// owns a key, you hash the key and walk clockwise until you hit a node.
//
/*
         0
        / \
    [N1#0]  [N2#2]       ← virtual nodes placed on the ring
    /            \
  key-X → walks clockwise → lands on N2#2 → owned by node-2
    \            /
    [N2#0]  [N1#1]
        \ /
        2^32
*/
//
// num_virtual_nodes_ controls how many positions each physical node gets
// on the ring. More virtual nodes = more even key distribution.

ConsistentHashRing::ConsistentHashRing(std::size_t num_virtual_nodes)
    : num_virtual_nodes_(num_virtual_nodes) {}

// ── addNode() — Place a physical node on the ring ───────────────────────────
//
// A single physical node (e.g., "node-1") gets many positions on the ring
// called "virtual nodes." This improves distribution because:
//   - 1 position → all keys between this node and the previous one go here
//   - 150 positions → keys are spread across 150 small arcs, balancing load
//
// Virtual node naming: "node-1#0", "node-1#1", ..., "node-1#149"
// Each is hashed to a different position on the ring, but all map back
// to the same physical node "node-1".

void ConsistentHashRing::addNode(const std::string& node_id) {
    // Track this physical node (std::set ignores duplicates automatically).
    physical_nodes_.insert(node_id);

    // Create num_virtual_nodes_ positions on the ring for this node.
    for (size_t i = 0; i < num_virtual_nodes_; ++i) {
        // Append "#i" to create a unique string for each virtual node.
        std::string virtual_node_id = node_id + "#" + std::to_string(i);

        // Hash the virtual node ID to get a position on the ring (0 to 2^32),
        // then map that position back to the physical node ID.
        // ring_ is a std::map<uint32_t, string> — sorted by hash value,
        // which naturally represents positions on the ring in clockwise order.
        ring_[hash(virtual_node_id)] = node_id;
    }
}

// ── removeNode() — Remove a physical node from the ring ─────────────────────
//
// The exact reverse of addNode(): we regenerate the same virtual node keys
// (same naming scheme), hash them to get the same ring positions, then erase
// those positions from the ring map.
//
// After removal, keys that were assigned to this node will now "fall through"
// clockwise to the next node on the ring — this is what makes consistent
// hashing efficient (only neighboring keys move, not all of them).

void ConsistentHashRing::removeNode(const std::string& node_id) {
    // Remove from the set of known physical nodes.
    physical_nodes_.erase(node_id);

    // Remove every virtual node position from the ring.
    for (size_t i = 0; i < num_virtual_nodes_; ++i) {
        // Must use the SAME naming scheme as addNode() so we get the same hashes.
        std::string virtual_node_id = node_id + "#" + std::to_string(i);
        ring_.erase(hash(virtual_node_id));
    }
}

// ── getNode() — Find which node owns a key ──────────────────────────────────
//
// The core operation of consistent hashing:
//   1. Hash the key to get a position on the ring.
//   2. Walk clockwise (increasing hash values) to find the first node.
//   3. That node is the "owner" of this key.
//
// Why lower_bound?
//   ring_ is a sorted map (by hash value = position on ring).
//   lower_bound(h) returns the first entry with hash >= h — that's the
//   first node you'd encounter walking clockwise from position h.
//
// Why wrap around?
//   If h is larger than all positions in the ring (past the "12 o'clock"),
//   lower_bound returns end(). We wrap to begin() because the ring is
//   circular — the next node clockwise is the one with the smallest hash.

std::string ConsistentHashRing::getNode(const std::string& key) const {
    if (ring_.empty()) {
        throw std::runtime_error("Cannot get node from empty ring");
    }

    // Hash the key to a position on the ring.
    uint32_t h = hash(key);

    // Find the first node at or after this position (clockwise walk).
    auto it = ring_.lower_bound(h);

    // Wrap around: if we went past the end, circle back to the beginning.
    if (it == ring_.end()) {
        it = ring_.begin();
    }

    // it->second is the physical node ID that this virtual node maps to.
    return it->second;
}

// ── getReplicaNodes() — Find N nodes for replication ────────────────────────
//
// For fault tolerance, each key is stored on multiple nodes (replicas).
// This function walks clockwise from the key's position, collecting
// N *distinct physical* nodes.
//
// Why "distinct physical"?
//   As we walk clockwise, we'll hit many virtual nodes, but several of
//   them might belong to the SAME physical node. We skip duplicates
//   because storing two replicas on the same machine gives no redundancy.
//
// Example with 3 physical nodes, each with 3 virtual nodes:
//
//   Ring positions: [N1#0] [N2#0] [N1#1] [N3#0] [N2#1] [N1#2] ...
//   Key hashes to here: ^
//   Walk clockwise collecting distinct nodes:
//     N1 ✓ (first time seeing node-1)
//     N2 ✓ (first time seeing node-2)
//     N1 ✗ (already seen node-1, skip)
//     N3 ✓ (first time seeing node-3)
//   Result: [N1, N2, N3] — the first one (N1) is the primary owner.

std::vector<std::string> ConsistentHashRing::getReplicaNodes(const std::string& key,
                                                             std::size_t n) const {
    // Guard: impossible to find N distinct nodes if fewer exist.
    if (physical_nodes_.size() < n) {
        throw std::runtime_error("Not enough distinct nodes for replication");
    }
    if (ring_.empty() || n == 0) {
        return {};
    }

    uint32_t h = hash(key);

    std::vector<std::string> replicas;
    std::unordered_set<std::string> seen;  // tracks which physical nodes we've collected

    // Start at the key's position and walk clockwise.
    auto it = ring_.lower_bound(h);
    while (replicas.size() < n) {
        // Wrap around the ring (circular).
        if (it == ring_.end()) {
            it = ring_.begin();
        }

        // it->second is the physical node ID for this virtual node position.
        const std::string& node_id = it->second;

        // Only collect this node if we haven't seen it yet (distinct physical nodes).
        if (seen.find(node_id) == seen.end()) {
            replicas.push_back(node_id);
            seen.insert(node_id);
        }

        ++it;  // continue clockwise
    }

    return replicas;
}

// ── Accessors ───────────────────────────────────────────────────────────────

std::size_t ConsistentHashRing::nodeCount() const {
    return physical_nodes_.size();
}

bool ConsistentHashRing::empty() const {
    return physical_nodes_.empty();
}

bool ConsistentHashRing::hasNode(const std::string& node_id) const {
    return physical_nodes_.count(node_id) > 0;
}

// ── hash() — Map a string to a position on the ring ─────────────────────────
//
// Converts any string into a 32-bit number (0 to ~4 billion), which
// represents a position on the hash ring.
//
// We use std::hash and truncate to 32 bits. In production systems,
// you'd use something like MurmurHash3 or xxHash for better distribution
// and consistency across platforms (std::hash is implementation-defined).

uint32_t ConsistentHashRing::hash(const std::string& key) {
    std::hash<std::string> hasher;
    return static_cast<uint32_t>(hasher(key));
}

}  // namespace kvstore
