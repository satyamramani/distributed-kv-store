#include "kvstore/gossip.h"

#include <chrono>

namespace kvstore {

// Constructor: Initializes the gossip protocol instance for this node.
// Every node immediately registers itself as ALIVE with a starting heartbeat counter of 0
// and sets its last updated timestamp to the current local time.
GossipProtocol::GossipProtocol(const std::string& self_id, uint64_t failure_timeout_ms)
    : self_id_(self_id), failure_timeout_ms_(failure_timeout_ms) {
    // Register this node (self) in the local membership list.
    members_[self_id_] = {self_id_, 0, nowMs()};
}

// heartbeat: Called periodically by the local node to signal that it is alive.
// Increments this node's own monotonic heartbeat counter and refreshes its timestamp.
void GossipProtocol::heartbeat() {
    auto it = members_.find(self_id_);
    if (it != members_.end()) {
        ++it->second.heartbeat_counter;
        it->second.timestamp_ms = nowMs();
    }
}

// receiveHeartbeat: Merges a gossiped membership list received from a peer node.
// For each incoming member:
// 1. If the node is newly discovered, add it to our membership map with local nowMs().
// 2. If the node already exists, only update if the incoming heartbeat counter is strictly greater
//    than our current record. This ensures monotonic progress and ignores stale gossip messages.
// Note: We use local nowMs() instead of the sender's timestamp to prevent clock drift/skew
// between different physical machines from corrupting failure detection.
void GossipProtocol::receiveHeartbeat(const std::vector<MemberInfo>& membership_list) {
    for (const auto& member : membership_list) {
        auto it = members_.find(member.node_id);
        if (it == members_.end()) {
            // First time hearing about this node; register it with our local reception time.
            members_[member.node_id] = {member.node_id, member.heartbeat_counter, nowMs()};
        } else {
            // Only accept updates if the incoming heartbeat counter has progressed.
            if (member.heartbeat_counter > it->second.heartbeat_counter) {
                it->second.heartbeat_counter = member.heartbeat_counter;
                // Update our local time of last contact.
                it->second.timestamp_ms = nowMs();
            }
        }
    }
}

// getMembershipList: Returns a snapshot of all known cluster members.
// This list is serialized and sent to randomly selected peer nodes during gossip rounds.
std::vector<MemberInfo> GossipProtocol::getMembershipList() const {
    std::vector<MemberInfo> list;
    list.reserve(members_.size());
    for (const auto& [node_id, member] : members_) {
        list.push_back(member);
    }
    return list;
}

// getFailedNodes: Detects which nodes appear to have failed or become unreachable.
// A node is considered failed if the elapsed time since its last heartbeat update exceeds
// failure_timeout_ms_.
std::vector<std::string> GossipProtocol::getFailedNodes() const {
    std::vector<std::string> failed;
    uint64_t now = nowMs();
    for (const auto& [node_id, member] : members_) {
        if (now - member.timestamp_ms > failure_timeout_ms_) {
            failed.push_back(node_id);
        }
    }
    return failed;
}

// getNodeStatus: Queries the liveness status of a specific node.
// - Unknown node or timed-out node -> NodeStatus::DEAD
// - Known node with recent heartbeat -> NodeStatus::ALIVE
NodeStatus GossipProtocol::getNodeStatus(const std::string& node_id) const {
    auto it = members_.find(node_id);
    if (it == members_.end()) {
        // Node is not in the membership list at all.
        return NodeStatus::DEAD;
    }
    // Check if the node's heartbeat has timed out based on our local clock.
    if (nowMs() - it->second.timestamp_ms > failure_timeout_ms_) {
        return NodeStatus::DEAD;
    }
    return NodeStatus::ALIVE;
}

// addMember: Manually registers a known node / seed peer into the membership list.
// If the node is already present, this is a no-op so existing state is preserved.
void GossipProtocol::addMember(const std::string& node_id) {
    if (members_.find(node_id) == members_.end()) {
        members_[node_id] = {node_id, 0, nowMs()};
    }
}

// removeMember: Removes a decommissioned or permanently dead node from tracking.
// Guard: A node must never remove itself from its own membership list.
void GossipProtocol::removeMember(const std::string& node_id) {
    if (node_id == self_id_) return;  // Protect self from being removed
    members_.erase(node_id);
}

// memberCount: Returns total number of members currently tracked (including self).
std::size_t GossipProtocol::memberCount() const {
    return members_.size();
}

// nowMs: Returns the current monotonic time in milliseconds using steady_clock.
// Using steady_clock ensures elapsed time calculations are immune to system clock jumps.
uint64_t GossipProtocol::nowMs() {
    using namespace std::chrono;
    return static_cast<uint64_t>(
        duration_cast<milliseconds>(steady_clock::now().time_since_epoch()).count());
}

}  // namespace kvstore

