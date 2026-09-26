#ifndef KVSTORE_CONSISTENT_HASH_H
#define KVSTORE_CONSISTENT_HASH_H

#include <cstddef>
#include <cstdint>
#include <map>
#include <set>
#include <string>
#include <vector>

namespace kvstore {

/// Consistent hash ring with virtual nodes for data partitioning.
///
/// Physical nodes are mapped to multiple positions (virtual nodes) on a
/// 32-bit hash ring to achieve more even key distribution.  Keys are
/// assigned to the first node encountered while walking clockwise.
class ConsistentHashRing {
public:
    /// @param num_virtual_nodes  Number of virtual nodes per physical node.
    explicit ConsistentHashRing(std::size_t num_virtual_nodes = 150);

    /// Add a physical node (and its virtual nodes) to the ring.
    void addNode(const std::string& node_id);

    /// Remove a physical node (and its virtual nodes) from the ring.
    void removeNode(const std::string& node_id);

    /// Determine which physical node owns the given key.
    /// @throws std::runtime_error if the ring is empty.
    std::string getNode(const std::string& key) const;

    /// Return the N *distinct* physical nodes responsible for replicating `key`.
    /// Walks clockwise from the key's position, skipping duplicate physical nodes.
    /// @throws std::runtime_error if the ring has fewer than n distinct nodes.
    std::vector<std::string> getReplicaNodes(const std::string& key,
                                             std::size_t n) const;

    /// Number of distinct physical nodes on the ring.
    std::size_t nodeCount() const;

    /// True if the ring contains no nodes.
    bool empty() const;

    /// True if the given physical node is on the ring.
    bool hasNode(const std::string& node_id) const;

private:
    std::size_t num_virtual_nodes_;
    std::map<uint32_t, std::string> ring_;       // hash → physical node id
    std::set<std::string> physical_nodes_;

    /// Compute a 32-bit hash for consistent hashing.
    static uint32_t hash(const std::string& key);
};

}  // namespace kvstore

#endif  // KVSTORE_CONSISTENT_HASH_H
