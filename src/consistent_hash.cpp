#include "kvstore/consistent_hash.h"

namespace kvstore {

ConsistentHashRing::ConsistentHashRing(std::size_t num_virtual_nodes)
    : num_virtual_nodes_(num_virtual_nodes) {}

// TODO: Implement — create virtual nodes and insert into the ring map.
void ConsistentHashRing::addNode(const std::string& /*node_id*/) {
    // Stub: num_virtual_nodes_ controls how many virtual nodes to create.
    (void)num_virtual_nodes_;
}

// TODO: Implement — remove all virtual nodes for this physical node.
void ConsistentHashRing::removeNode(const std::string& /*node_id*/) {}

// TODO: Implement — hash the key, walk clockwise to find the owning node.
std::string ConsistentHashRing::getNode(const std::string& /*key*/) const {
    return "";
}

// TODO: Implement — walk clockwise collecting N distinct physical nodes.
std::vector<std::string> ConsistentHashRing::getReplicaNodes(
    const std::string& /*key*/, std::size_t /*n*/) const {
    return {};
}

std::size_t ConsistentHashRing::nodeCount() const {
    return physical_nodes_.size();
}

bool ConsistentHashRing::empty() const { return physical_nodes_.empty(); }

bool ConsistentHashRing::hasNode(const std::string& node_id) const {
    return physical_nodes_.count(node_id) > 0;
}

// TODO: Implement — use a good hash function (e.g. MurmurHash, FNV, or std::hash).
uint32_t ConsistentHashRing::hash(const std::string& /*key*/) { return 0; }

}  // namespace kvstore
