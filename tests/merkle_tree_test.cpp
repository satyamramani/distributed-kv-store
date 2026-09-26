#include <gtest/gtest.h>

#include <string>
#include <utility>
#include <vector>

#include "kvstore/merkle_tree.h"

class MerkleTreeTest : public ::testing::Test {
protected:
    using KV = std::pair<std::string, std::string>;

    std::vector<KV> sampleEntries() {
        return {
            {"key1", "val1"},
            {"key2", "val2"},
            {"key3", "val3"},
            {"key4", "val4"},
            {"key5", "val5"},
            {"key6", "val6"},
        };
    }
};

// ── Build & Root Hash ───────────────────────────────────────────────────────

TEST_F(MerkleTreeTest, EmptyTreeHasEmptyRootHash) {
    kvstore::MerkleTree tree(4);
    EXPECT_TRUE(tree.empty());
    EXPECT_EQ(tree.getRootHash(), "");
}

TEST_F(MerkleTreeTest, BuildProducesNonEmptyRootHash) {
    kvstore::MerkleTree tree(4);
    tree.build(sampleEntries());

    EXPECT_FALSE(tree.empty());
    EXPECT_FALSE(tree.getRootHash().empty());
}

TEST_F(MerkleTreeTest, SameDataProducesSameRootHash) {
    kvstore::MerkleTree t1(4), t2(4);
    t1.build(sampleEntries());
    t2.build(sampleEntries());

    EXPECT_EQ(t1.getRootHash(), t2.getRootHash());
}

TEST_F(MerkleTreeTest, DifferentDataProducesDifferentRootHash) {
    kvstore::MerkleTree t1(4), t2(4);
    t1.build(sampleEntries());

    auto modified = sampleEntries();
    modified[2].second = "CHANGED";
    t2.build(modified);

    EXPECT_NE(t1.getRootHash(), t2.getRootHash());
}

// ── Bucket hashes ───────────────────────────────────────────────────────────

TEST_F(MerkleTreeTest, GetBucketHashReturnsNonEmpty) {
    kvstore::MerkleTree tree(4);
    tree.build(sampleEntries());

    // At least one bucket should have a non-empty hash
    bool found_non_empty = false;
    for (std::size_t i = 0; i < tree.numBuckets(); ++i) {
        if (!tree.getBucketHash(i).empty()) {
            found_non_empty = true;
            break;
        }
    }
    EXPECT_TRUE(found_non_empty);
}

TEST_F(MerkleTreeTest, NumBucketsMatchesConfiguration) {
    kvstore::MerkleTree tree(8);
    EXPECT_EQ(tree.numBuckets(), 8u);
}

// ── Diff (anti-entropy) ─────────────────────────────────────────────────────

TEST_F(MerkleTreeTest, NoDiffForIdenticalTrees) {
    kvstore::MerkleTree t1(4), t2(4);
    t1.build(sampleEntries());
    t2.build(sampleEntries());

    auto diffs = t1.diff(t2);
    EXPECT_TRUE(diffs.empty());
}

TEST_F(MerkleTreeTest, DiffDetectsDivergence) {
    kvstore::MerkleTree t1(4), t2(4);
    t1.build(sampleEntries());

    auto modified = sampleEntries();
    modified[0].second = "DIFFERENT";
    t2.build(modified);

    auto diffs = t1.diff(t2);
    EXPECT_FALSE(diffs.empty());
}

TEST_F(MerkleTreeTest, DiffReturnsKeyRanges) {
    kvstore::MerkleTree t1(4), t2(4);
    t1.build(sampleEntries());

    auto modified = sampleEntries();
    modified.back().second = "CHANGED";
    t2.build(modified);

    auto diffs = t1.diff(t2);
    for (const auto& range : diffs) {
        // Key ranges should be non-empty strings (or at least defined).
        // We just verify the structure is valid.
        EXPECT_FALSE(range.start_key.empty() && range.end_key.empty())
            << "KeyRange should have at least one non-empty bound";
    }
}

// ── Edge cases ──────────────────────────────────────────────────────────────

TEST_F(MerkleTreeTest, BuildWithEmptyEntries) {
    kvstore::MerkleTree tree(4);
    std::vector<KV> empty;
    tree.build(empty);
    // An empty build should still result in a valid (but empty) tree.
    EXPECT_TRUE(tree.empty() || tree.getRootHash().empty());
}

TEST_F(MerkleTreeTest, BuildWithSingleEntry) {
    kvstore::MerkleTree tree(4);
    tree.build({{"only-key", "only-val"}});
    EXPECT_FALSE(tree.getRootHash().empty());
}

TEST_F(MerkleTreeTest, RebuildReplacesOldTree) {
    kvstore::MerkleTree tree(4);
    tree.build(sampleEntries());
    auto hash1 = tree.getRootHash();

    auto modified = sampleEntries();
    modified[0].second = "NEW";
    tree.build(modified);
    auto hash2 = tree.getRootHash();

    EXPECT_NE(hash1, hash2);
}
