#include "kvstore/gossip.h"

#include <chrono>

namespace kvstore {

GossipProtocol::GossipProtocol(const std::string& self_id,
                               uint64_t failure_timeout_ms)
    : self_id_(self_id), failure_timeout_ms_(failure_timeout_ms) {
    // Register self in the membership list.
    members_[self_id_] = {self_id_, 0, nowMs()};
}

// TODO: Implement — increment own heartbeat counter and update timestamp.
void GossipProtocol::heartbeat() {}

// TODO: Implement — merge incoming list: for each member, keep the higher counter.
void GossipProtocol::receiveHeartbeat(
    const std::vector<MemberInfo>& /*membership_list*/) {}

// TODO: Implement — return a snapshot of all members.
std::vector<MemberInfo> GossipProtocol::getMembershipList() const { return {}; }

// TODO: Implement — return node IDs whose timestamp is older than failure_timeout_ms_.
std::vector<std::string> GossipProtocol::getFailedNodes() const {
    (void)failure_timeout_ms_;
    return {};
}

// TODO: Implement — check member's timestamp against failure_timeout_ms_.
NodeStatus GossipProtocol::getNodeStatus(
    const std::string& /*node_id*/) const {
    return NodeStatus::DEAD;
}

// TODO: Implement — add a new member entry.
void GossipProtocol::addMember(const std::string& /*node_id*/) {}

// TODO: Implement — erase a member from the list.
void GossipProtocol::removeMember(const std::string& /*node_id*/) {}

std::size_t GossipProtocol::memberCount() const { return members_.size(); }

uint64_t GossipProtocol::nowMs() {
    using namespace std::chrono;
    return static_cast<uint64_t>(
        duration_cast<milliseconds>(
            steady_clock::now().time_since_epoch())
            .count());
}

}  // namespace kvstore
