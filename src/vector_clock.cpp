#include "kvstore/vector_clock.h"

#include <algorithm>
#include <vector>

namespace kvstore {

// increment: Increments the logical counter for a given server.
// If the server does not yet exist in this vector clock, it is inserted with counter 1.
void VectorClock::increment(const std::string& server_id) {
    ++clocks_[server_id];
}

// merge: Computes the element-wise maximum of this vector clock and another.
// For every server present in either clock, the merged clock retains the highest counter seen.
// This is used to reconcile conflicting or concurrent versions during client reads or repairs.
void VectorClock::merge(const VectorClock& other) {
    for (const auto& [server, count] : other.clocks_) {
        auto& my_count = clocks_[server];
        my_count = std::max(my_count, count);
    }
}

// compare: Determines the causal relationship between two vector clocks.
// Let V1 be this clock and V2 be other:
// - EQUAL: V1[s] == V2[s] for all servers s
// - BEFORE: V1[s] <= V2[s] for all s, and V1[s] < V2[s] for at least one s (V1 happened-before V2)
// - AFTER: V1[s] >= V2[s] for all s, and V1[s] > V2[s] for at least one s (V1 happened-after V2)
// - CONCURRENT: V1 dominates on some server(s) while V2 dominates on others (conflict!)
ClockComparison VectorClock::compare(const VectorClock& other) const {
    bool has_greater = false;
    bool has_less = false;

    // Check all servers recorded in this clock against other
    for (const auto& [server, count] : clocks_) {
        uint64_t other_count = other.getCounter(server);
        if (count > other_count) {
            has_greater = true;
        } else if (count < other_count) {
            has_less = true;
        }
    }

    // Check servers that only exist in the other clock (this has 0 for them)
    for (const auto& [server, other_count] : other.clocks_) {
        if (clocks_.find(server) == clocks_.end()) {
            if (other_count > 0) {
                has_less = true;
            }
        }
    }

    if (has_greater && has_less) {
        return ClockComparison::CONCURRENT;
    }
    if (has_greater) {
        return ClockComparison::AFTER;
    }
    if (has_less) {
        return ClockComparison::BEFORE;
    }
    return ClockComparison::EQUAL;
}

// descends: Returns true if this clock causally descends from (subsumes) other.
// Specifically, every server's counter in other must be <= the corresponding counter here.
bool VectorClock::descends(const VectorClock& other) const {
    for (const auto& [server, other_count] : other.clocks_) {
        if (getCounter(server) < other_count) {
            return false;
        }
    }
    return true;
}

uint64_t VectorClock::getCounter(const std::string& server_id) const {
    auto it = clocks_.find(server_id);
    return it != clocks_.end() ? it->second : 0;
}

std::size_t VectorClock::size() const { return clocks_.size(); }

bool VectorClock::empty() const { return clocks_.empty(); }

// prune: Caps the vector clock size to avoid unbounded growth over time.
// Keeps only the max_entries with the highest counter values, discarding older entries.
void VectorClock::prune(std::size_t max_entries) {
    if (clocks_.size() <= max_entries) {
        return;
    }

    if (max_entries == 0) {
        clocks_.clear();
        return;
    }

    // Sort entries by counter in descending order
    std::vector<std::pair<std::string, uint64_t>> entries(clocks_.begin(), clocks_.end());
    std::sort(entries.begin(), entries.end(),
              [](const auto& a, const auto& b) {
                  return a.second > b.second;
              });

    // Retain only the top max_entries
    clocks_.clear();
    for (std::size_t i = 0; i < max_entries; ++i) {
        clocks_.insert(entries[i]);
    }
}

const std::unordered_map<std::string, uint64_t>& VectorClock::clocks() const {
    return clocks_;
}

}  // namespace kvstore

