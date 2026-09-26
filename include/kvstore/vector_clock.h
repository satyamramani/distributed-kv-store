#ifndef KVSTORE_VECTOR_CLOCK_H
#define KVSTORE_VECTOR_CLOCK_H

#include <cstdint>
#include <string>
#include <unordered_map>

namespace kvstore {

/// Result of comparing two vector clocks.
enum class ClockComparison {
    BEFORE,      // this happened-before other
    AFTER,       // this happened-after  other
    CONCURRENT,  // neither dominates — conflict!
    EQUAL,       // identical clocks
};

/// A vector clock for detecting causal ordering and conflicts.
///
/// Each entry is a (server_id → counter) pair.  When server Si handles a
/// write it increments its own counter.  Two clocks are *concurrent* (i.e.
/// conflicting) when neither dominates the other element-wise.
class VectorClock {
public:
    VectorClock() = default;

    /// Increment the counter for the given server (creates entry if absent).
    void increment(const std::string& server_id);

    /// Merge with another clock (element-wise max).
    void merge(const VectorClock& other);

    /// Compare this clock to `other`.
    ClockComparison compare(const VectorClock& other) const;

    /// True if this clock causally descends from (happened after) `other`.
    bool descends(const VectorClock& other) const;

    /// Get the counter for a specific server (0 if absent).
    uint64_t getCounter(const std::string& server_id) const;

    /// Number of (server, counter) entries.
    std::size_t size() const;

    /// True if no entries.
    bool empty() const;

    /// Remove the oldest entries so that at most `max_entries` remain.
    /// "Oldest" = lowest counter value.
    void prune(std::size_t max_entries);

    /// Access the internal clock map (useful for serialization / tests).
    const std::unordered_map<std::string, uint64_t>& clocks() const;

private:
    std::unordered_map<std::string, uint64_t> clocks_;
};

}  // namespace kvstore

#endif  // KVSTORE_VECTOR_CLOCK_H
