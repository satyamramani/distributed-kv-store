#include "kvstore/vector_clock.h"

namespace kvstore {

// TODO: Implement — increment counter for server_id (create with 1 if absent).
void VectorClock::increment(const std::string& /*server_id*/) {}

// TODO: Implement — element-wise max of this clock and other.
void VectorClock::merge(const VectorClock& /*other*/) {}

// TODO: Implement — compare element-wise to determine causal ordering.
ClockComparison VectorClock::compare(const VectorClock& /*other*/) const {
    return ClockComparison::EQUAL;
}

// TODO: Implement — true if every entry in other is <= corresponding entry here.
bool VectorClock::descends(const VectorClock& /*other*/) const {
    return false;
}

uint64_t VectorClock::getCounter(const std::string& server_id) const {
    auto it = clocks_.find(server_id);
    return it != clocks_.end() ? it->second : 0;
}

std::size_t VectorClock::size() const { return clocks_.size(); }

bool VectorClock::empty() const { return clocks_.empty(); }

// TODO: Implement — keep only the max_entries with highest counters.
void VectorClock::prune(std::size_t /*max_entries*/) {}

const std::unordered_map<std::string, uint64_t>& VectorClock::clocks() const {
    return clocks_;
}

}  // namespace kvstore
