#include <gtest/gtest.h>

#include <algorithm>
#include <cmath>
#include <set>
#include <string>
#include <unordered_map>

#include "kvstore/consistent_hash.h"

class ConsistentHashTest : public ::testing::Test {
protected:
    kvstore::ConsistentHashRing ring{100};  // 100 virtual nodes per physical node
};

// ── Add / Remove nodes ──────────────────────────────────────────────────────

TEST_F(ConsistentHashTest, AddNodeIncreasesCount) {
    ring.addNode("node-1");
    EXPECT_EQ(ring.nodeCount(), 1u);
    EXPECT_TRUE(ring.hasNode("node-1"));
    EXPECT_FALSE(ring.empty());
}

TEST_F(ConsistentHashTest, AddMultipleNodes) {
    ring.addNode("node-1");
    ring.addNode("node-2");
    ring.addNode("node-3");
    EXPECT_EQ(ring.nodeCount(), 3u);
}

TEST_F(ConsistentHashTest, AddDuplicateNodeIsIdempotent) {
    ring.addNode("node-1");
    ring.addNode("node-1");
    EXPECT_EQ(ring.nodeCount(), 1u);
}

TEST_F(ConsistentHashTest, RemoveNode) {
    ring.addNode("node-1");
    ring.addNode("node-2");
    ring.removeNode("node-1");

    EXPECT_EQ(ring.nodeCount(), 1u);
    EXPECT_FALSE(ring.hasNode("node-1"));
    EXPECT_TRUE(ring.hasNode("node-2"));
}

TEST_F(ConsistentHashTest, RemoveNonexistentNodeIsNoop) {
    ring.addNode("node-1");
    ring.removeNode("ghost");
    EXPECT_EQ(ring.nodeCount(), 1u);
}

// ── getNode ─────────────────────────────────────────────────────────────────

TEST_F(ConsistentHashTest, GetNodeReturnsConsistentResult) {
    ring.addNode("node-1");
    ring.addNode("node-2");

    auto result1 = ring.getNode("my-key");
    auto result2 = ring.getNode("my-key");
    EXPECT_EQ(result1, result2);
}

TEST_F(ConsistentHashTest, GetNodeReturnsValidNode) {
    ring.addNode("node-1");
    ring.addNode("node-2");

    auto result = ring.getNode("test-key");
    EXPECT_TRUE(result == "node-1" || result == "node-2");
}

TEST_F(ConsistentHashTest, GetNodeThrowsOnEmptyRing) {
    EXPECT_THROW(ring.getNode("key"), std::runtime_error);
}

TEST_F(ConsistentHashTest, MinimalKeyMovementOnNodeAddition) {
    // Record assignments with 3 nodes.
    ring.addNode("node-1");
    ring.addNode("node-2");
    ring.addNode("node-3");

    std::unordered_map<std::string, std::string> before;
    for (int i = 0; i < 1000; ++i) {
        std::string key = "key-" + std::to_string(i);
        before[key] = ring.getNode(key);
    }

    // Add a 4th node.
    ring.addNode("node-4");

    int moved = 0;
    for (const auto& [key, old_node] : before) {
        if (ring.getNode(key) != old_node) ++moved;
    }

    // With consistent hashing, roughly 1/4 of keys should move.
    // Allow generous tolerance: no more than 40%.
    double move_ratio = static_cast<double>(moved) / 1000.0;
    EXPECT_LT(move_ratio, 0.40)
        << "Too many keys moved (" << moved << "/1000)";
}

// ── getReplicaNodes ─────────────────────────────────────────────────────────

TEST_F(ConsistentHashTest, GetReplicaNodesReturnsDistinctNodes) {
    ring.addNode("node-1");
    ring.addNode("node-2");
    ring.addNode("node-3");

    auto replicas = ring.getReplicaNodes("my-key", 3);
    ASSERT_EQ(replicas.size(), 3u);

    std::set<std::string> unique(replicas.begin(), replicas.end());
    EXPECT_EQ(unique.size(), 3u);
}

TEST_F(ConsistentHashTest, GetReplicaNodesFirstIsPrimary) {
    ring.addNode("node-1");
    ring.addNode("node-2");
    ring.addNode("node-3");

    auto primary = ring.getNode("my-key");
    auto replicas = ring.getReplicaNodes("my-key", 3);
    EXPECT_EQ(replicas[0], primary);
}

TEST_F(ConsistentHashTest, GetReplicaNodesThrowsWhenNotEnoughNodes) {
    ring.addNode("node-1");
    ring.addNode("node-2");

    EXPECT_THROW(ring.getReplicaNodes("key", 5), std::runtime_error);
}

// ── Distribution balance ────────────────────────────────────────────────────

TEST_F(ConsistentHashTest, KeysAreReasonablyDistributed) {
    ring.addNode("node-1");
    ring.addNode("node-2");
    ring.addNode("node-3");

    std::unordered_map<std::string, int> counts;
    for (int i = 0; i < 3000; ++i) {
        counts[ring.getNode("key-" + std::to_string(i))]++;
    }

    // Each node should get roughly 1000 keys (1/3).
    // Allow range [500, 1500] — generous for educational code.
    for (const auto& [node, count] : counts) {
        EXPECT_GT(count, 500)
            << node << " got too few keys: " << count;
        EXPECT_LT(count, 1500)
            << node << " got too many keys: " << count;
    }
}
