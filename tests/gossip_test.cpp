#include <gtest/gtest.h>

#include <string>
#include <thread>
#include <vector>

#include "kvstore/gossip.h"

class GossipTest : public ::testing::Test {
protected:
    // Use a very large timeout so nodes don't accidentally appear dead
    // during normal tests (except the failure-detection tests).
    kvstore::GossipProtocol gossip{"self-node", /*failure_timeout_ms=*/60000};
};

// ── Construction ────────────────────────────────────────────────────────────

TEST_F(GossipTest, SelfIsRegisteredOnConstruction) {
    EXPECT_EQ(gossip.memberCount(), 1u);
    EXPECT_EQ(gossip.getNodeStatus("self-node"), kvstore::NodeStatus::ALIVE);
}

// ── Heartbeat ───────────────────────────────────────────────────────────────

TEST_F(GossipTest, HeartbeatIncrementsSelfCounter) {
    gossip.heartbeat();
    gossip.heartbeat();

    auto members = gossip.getMembershipList();
    ASSERT_EQ(members.size(), 1u);
    EXPECT_EQ(members[0].node_id, "self-node");
    EXPECT_GE(members[0].heartbeat_counter, 2u);
}

// ── AddMember / RemoveMember ────────────────────────────────────────────────

TEST_F(GossipTest, AddMember) {
    gossip.addMember("node-2");
    EXPECT_EQ(gossip.memberCount(), 2u);
}

TEST_F(GossipTest, RemoveMember) {
    gossip.addMember("node-2");
    gossip.removeMember("node-2");
    EXPECT_EQ(gossip.memberCount(), 1u);
}

TEST_F(GossipTest, CannotRemoveSelf) {
    gossip.removeMember("self-node");
    // Self should persist even after removeMember is called on it
    // (implementation choice — at minimum, memberCount should stay ≥ 1).
    EXPECT_GE(gossip.memberCount(), 1u);
}

// ── GetMembershipList ───────────────────────────────────────────────────────

TEST_F(GossipTest, GetMembershipListContainsAllMembers) {
    gossip.addMember("node-2");
    gossip.addMember("node-3");

    auto members = gossip.getMembershipList();
    EXPECT_EQ(members.size(), 3u);

    std::set<std::string> ids;
    for (const auto& m : members) ids.insert(m.node_id);

    EXPECT_TRUE(ids.count("self-node"));
    EXPECT_TRUE(ids.count("node-2"));
    EXPECT_TRUE(ids.count("node-3"));
}

// ── ReceiveHeartbeat (gossip merge) ─────────────────────────────────────────

TEST_F(GossipTest, ReceiveHeartbeatAddsNewMembers) {
    std::vector<kvstore::MemberInfo> incoming = {
        {"remote-1", 5, 1000},
        {"remote-2", 3, 1000},
    };
    gossip.receiveHeartbeat(incoming);

    EXPECT_EQ(gossip.memberCount(), 3u);  // self + 2 remotes
}

TEST_F(GossipTest, ReceiveHeartbeatUpdatesHigherCounter) {
    gossip.addMember("node-2");

    // Simulate receiving a newer heartbeat from node-2.
    std::vector<kvstore::MemberInfo> incoming = {
        {"node-2", 10, 99999},
    };
    gossip.receiveHeartbeat(incoming);

    auto members = gossip.getMembershipList();
    for (const auto& m : members) {
        if (m.node_id == "node-2") {
            EXPECT_GE(m.heartbeat_counter, 10u);
        }
    }
}

TEST_F(GossipTest, ReceiveHeartbeatIgnoresLowerCounter) {
    gossip.addMember("node-2");

    // First receive a high counter…
    gossip.receiveHeartbeat({{"node-2", 100, 99999}});
    // …then a lower one (stale).
    gossip.receiveHeartbeat({{"node-2", 50, 99999}});

    auto members = gossip.getMembershipList();
    for (const auto& m : members) {
        if (m.node_id == "node-2") {
            EXPECT_GE(m.heartbeat_counter, 100u);
        }
    }
}

// ── Failure detection ───────────────────────────────────────────────────────

TEST_F(GossipTest, NodeDetectedAsFailedAfterTimeout) {
    // Create a gossip instance with a very short timeout.
    kvstore::GossipProtocol fast_gossip("self", /*failure_timeout_ms=*/50);
    fast_gossip.addMember("slow-node");

    // Wait longer than the timeout.
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    auto failed = fast_gossip.getFailedNodes();
    // "slow-node" never sent a heartbeat, so it should be failed.
    bool found = false;
    for (const auto& id : failed) {
        if (id == "slow-node") found = true;
    }
    EXPECT_TRUE(found) << "slow-node should be detected as failed";
}

TEST_F(GossipTest, SelfNeverAppearsInFailedNodes) {
    kvstore::GossipProtocol fast_gossip("self", 10);
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    // Even if timeout elapses, self should never be "failed".
    // (heartbeat() should keep self alive, but even without it,
    //  a well-behaved impl should exclude self.)
    fast_gossip.heartbeat();
    auto failed = fast_gossip.getFailedNodes();
    for (const auto& id : failed) {
        EXPECT_NE(id, "self") << "Self should never appear in failed nodes";
    }
}

// ── GetNodeStatus ───────────────────────────────────────────────────────────

TEST_F(GossipTest, AliveNodeStatus) {
    gossip.heartbeat();
    EXPECT_EQ(gossip.getNodeStatus("self-node"), kvstore::NodeStatus::ALIVE);
}

TEST_F(GossipTest, UnknownNodeStatusIsDead) {
    // A node we've never heard of should be considered DEAD.
    EXPECT_EQ(gossip.getNodeStatus("never-seen"),
              kvstore::NodeStatus::DEAD);
}
