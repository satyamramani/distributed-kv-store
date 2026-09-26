#ifndef KVSTORE_MERKLE_TREE_H
#define KVSTORE_MERKLE_TREE_H

#include <cstddef>
#include <string>
#include <utility>
#include <vector>

namespace kvstore {

/// A key range that differs between two replicas.
struct KeyRange {
    std::string start_key;
    std::string end_key;
};

/// Hash tree for anti-entropy replica synchronization.
///
/// Keys are bucketed, each bucket gets a leaf hash, and parent hashes are
/// computed bottom-up.  Two replicas compare root hashes first; if they
/// differ, they walk down the tree to find exactly which buckets diverge.
class MerkleTree {
public:
    /// @param num_buckets  Number of leaf buckets to partition keys into.
    explicit MerkleTree(std::size_t num_buckets = 16);

    /// Build (or rebuild) the tree from a set of key-value pairs.
    void build(const std::vector<std::pair<std::string, std::string>>& entries);

    /// Root hash of the tree (empty string if tree is empty).
    std::string getRootHash() const;

    /// Hash of a specific leaf bucket.
    std::string getBucketHash(std::size_t bucket_id) const;

    /// Compare with another MerkleTree and return the key ranges that differ.
    std::vector<KeyRange> diff(const MerkleTree& other) const;

    /// Number of leaf buckets.
    std::size_t numBuckets() const;

    /// True if the tree has not been built yet (or was built with zero entries).
    bool empty() const;

private:
    std::size_t num_buckets_;
    std::vector<std::string> bucket_hashes_;  // leaf hashes
    std::vector<std::string> tree_;           // full binary tree stored as array
    bool built_{false};

    /// Assign a key to a bucket index.
    std::size_t bucketFor(const std::string& key) const;

    /// Rebuild internal tree nodes from leaf hashes.
    void buildTree();
};

}  // namespace kvstore

#endif  // KVSTORE_MERKLE_TREE_H
