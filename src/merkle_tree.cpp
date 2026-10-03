#include "kvstore/merkle_tree.h"

#include <algorithm>
#include <functional>

namespace kvstore {

MerkleTree::MerkleTree(std::size_t num_buckets)
    : num_buckets_(num_buckets),
      bucket_hashes_(num_buckets) {}

// build: Partitions key-value entries into buckets, hashes each bucket,
// and constructs the binary Merkle tree bottom-up.
void MerkleTree::build(
    const std::vector<std::pair<std::string, std::string>>& entries) {
    if (entries.empty()) {
        bucket_hashes_.assign(num_buckets_, "");
        tree_.clear();
        built_ = false;
        return;
    }

    // Reset leaf bucket hashes
    bucket_hashes_.assign(num_buckets_, "");

    // 1. Group entries by bucket index
    std::vector<std::vector<std::pair<std::string, std::string>>> buckets(num_buckets_);
    for (const auto& entry : entries) {
        std::size_t b = bucketFor(entry.first);
        buckets[b].push_back(entry);
    }

    // 2. Hash contents of each bucket deterministically
    std::hash<std::string> hasher;
    for (std::size_t i = 0; i < num_buckets_; ++i) {
        if (buckets[i].empty()) {
            bucket_hashes_[i] = "";
            continue;
        }

        // Sort entries within the bucket to guarantee consistent hashing
        std::sort(buckets[i].begin(), buckets[i].end());

        std::string serialized;
        for (const auto& [k, v] : buckets[i]) {
            serialized += k;
            serialized += '=';
            serialized += v;
            serialized += ';';
        }
        bucket_hashes_[i] = std::to_string(hasher(serialized));
    }

    // 3. Build internal nodes up to the root
    buildTree();
    built_ = true;
}

std::string MerkleTree::getRootHash() const {
    if (tree_.empty()) return "";
    return tree_[0];
}

// getBucketHash: Returns the hash of a specific leaf bucket.
std::string MerkleTree::getBucketHash(std::size_t bucket_id) const {
    if (bucket_id < bucket_hashes_.size()) {
        return bucket_hashes_[bucket_id];
    }
    return "";
}

// diff: Compares this tree with another to find diverging key ranges.
// Prunes subtrees where parent hashes match, only traversing down branches
// that differ until differing leaf buckets are identified.
std::vector<KeyRange> MerkleTree::diff(const MerkleTree& other) const {
    // If root hashes match (or both are empty), replicas are identical
    if (getRootHash() == other.getRootHash()) {
        return {};
    }

    std::vector<KeyRange> diffs;

    // Fallback if tree sizes or bucket configurations differ
    if (num_buckets_ != other.num_buckets_ || tree_.size() != other.tree_.size()) {
        std::size_t max_b = std::max(num_buckets_, other.num_buckets_);
        for (std::size_t i = 0; i < max_b; ++i) {
            std::string h1 = (i < bucket_hashes_.size()) ? bucket_hashes_[i] : "";
            std::string h2 = (i < other.bucket_hashes_.size()) ? other.bucket_hashes_[i] : "";
            if (h1 != h2) {
                diffs.push_back({std::to_string(i), std::to_string(i)});
            }
        }
        return diffs;
    }

    std::size_t leaf_count = 1;
    while (leaf_count < num_buckets_) {
        leaf_count *= 2;
    }
    std::size_t leaf_offset = leaf_count - 1;

    // DFS traversal starting from the root (index 0)
    std::vector<std::size_t> stack;
    stack.push_back(0);

    while (!stack.empty()) {
        std::size_t node = stack.back();
        stack.pop_back();

        if (node >= tree_.size() || node >= other.tree_.size()) {
            continue;
        }

        // Subtrees match; prune this branch
        if (tree_[node] == other.tree_[node]) {
            continue;
        }

        std::size_t left = 2 * node + 1;
        std::size_t right = 2 * node + 2;

        if (left < tree_.size()) {
            // Internal node: explore children (push right first so left is popped first)
            if (right < tree_.size()) {
                stack.push_back(right);
            }
            stack.push_back(left);
        } else {
            // Leaf node: compute corresponding bucket id
            std::size_t bucket_id = node - leaf_offset;
            if (bucket_id < num_buckets_) {
                diffs.push_back({std::to_string(bucket_id), std::to_string(bucket_id)});
            }
        }
    }

    return diffs;
}

std::size_t MerkleTree::numBuckets() const { return num_buckets_; }

bool MerkleTree::empty() const { return !built_; }

// bucketFor: Maps key to a bucket index using consistent hashing modulo num_buckets_.
std::size_t MerkleTree::bucketFor(const std::string& key) const {
    if (num_buckets_ == 0) return 0;
    return std::hash<std::string>{}(key) % num_buckets_;
}

// buildTree: Constructs the complete binary tree array from leaf bucket hashes.
// Leaves are positioned at the bottom layer; parent nodes hash their two children.
void MerkleTree::buildTree() {
    if (num_buckets_ == 0) {
        tree_.clear();
        return;
    }

    std::size_t leaf_count = 1;
    while (leaf_count < num_buckets_) {
        leaf_count *= 2;
    }

    std::size_t total_nodes = 2 * leaf_count - 1;
    std::size_t leaf_offset = leaf_count - 1;

    tree_.assign(total_nodes, "");

    // Populate leaf layer
    for (std::size_t i = 0; i < num_buckets_; ++i) {
        tree_[leaf_offset + i] = bucket_hashes_[i];
    }

    // Compute parent hashes bottom-up
    std::hash<std::string> hasher;
    for (std::size_t k = leaf_offset; k > 0; --k) {
        std::size_t parent = k - 1;
        std::size_t left_idx = 2 * parent + 1;
        std::size_t right_idx = 2 * parent + 2;

        const std::string& left = tree_[left_idx];
        const std::string& right = tree_[right_idx];

        if (left.empty() && right.empty()) {
            tree_[parent] = "";
        } else {
            tree_[parent] = std::to_string(hasher(left + ":" + right));
        }
    }
}

}  // namespace kvstore

