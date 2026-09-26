#ifndef KVSTORE_GOSSIP_H
#define KVSTORE_GOSSIP_H

#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

namespace kvstore {

/// Information about a single cluster member.
struct MemberInfo {
    std::string node_id;
    uint64_t heartbeat_counter;
    uint64_t timestamp_ms;  // local wall-clock time of last update
};

/// Liveness status of a node.
enum class NodeStatus {
    ALIVE,
    SUSPECT,
    DEAD,
};

/// Decentralized failure detection using the gossip protocol.
///
/// Each node periodically increments its own heartbeat counter, then
/// sends its membership list to a random subset of peers.  If a peer's
/// heartbeat has not advanced within `failure_timeout_ms` it is marked
/// as suspect / dead.
class GossipProtocol {
public:
    /// @param self_id            This node's identifier.
    /// @param failure_timeout_ms Milliseconds before a peer is suspected.
    explicit GossipProtocol(const std::string& self_id,
                            uint64_t failure_timeout_ms = 5000);

    /// Increment this node's own heartbeat counter.
    void heartbeat();

    /// Merge an incoming membership list from a peer.
    void receiveHeartbeat(const std::vector<MemberInfo>& membership_list);

    /// Snapshot of the full membership list (for sending to a peer).
    std::vector<MemberInfo> getMembershipList() const;

    /// Return node IDs whose heartbeat has not advanced past the timeout.
    std::vector<std::string> getFailedNodes() const;

    /// Current status of a specific node.
    NodeStatus getNodeStatus(const std::string& node_id) const;

    /// Manually register a new member.
    void addMember(const std::string& node_id);

    /// Permanently remove a member from the list.
    void removeMember(const std::string& node_id);

    /// Number of members (including self).
    std::size_t memberCount() const;

private:
    std::string self_id_;
    uint64_t failure_timeout_ms_;
    std::unordered_map<std::string, MemberInfo> members_;

    /// Current wall-clock time in milliseconds (overridable for testing).
    static uint64_t nowMs();
};

}  // namespace kvstore

#endif  // KVSTORE_GOSSIP_H
