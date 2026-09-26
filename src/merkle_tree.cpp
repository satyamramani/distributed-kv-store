#include "kvstore/merkle_tree.h"

namespace kvstore {

MerkleTree::MerkleTree(std::size_t num_buckets)
    : num_buckets_(num_buckets),
      bucket_hashes_(num_buckets) {}

// TODO: Implement — bucket each key, hash bucket contents, then buildTree().
void MerkleTree::build(
    const std::vector<std::pair<std::string, std::string>>& /*entries*/) {}

std::string MerkleTree::getRootHash() const {
    if (tree_.empty()) return "";
    return tree_[0];
}

// TODO: Implement — return the hash for the given bucket index.
std::string MerkleTree::getBucketHash(std::size_t /*bucket_id*/) const {
    return "";
}

// TODO: Implement — walk both trees and collect differing key ranges.
std::vector<KeyRange> MerkleTree::diff(const MerkleTree& /*other*/) const {
    return {};
}

std::size_t MerkleTree::numBuckets() const { return num_buckets_; }

bool MerkleTree::empty() const { return !built_; }

// TODO: Implement — hash(key) % num_buckets_.
std::size_t MerkleTree::bucketFor(const std::string& /*key*/) const {
    return 0;
}

// TODO: Implement — build the full binary tree array from leaf hashes.
void MerkleTree::buildTree() {}

}  // namespace kvstore
