#ifndef KVSTORE_MEMTABLE_H
#define KVSTORE_MEMTABLE_H

#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <vector>

namespace kvstore {

/// A single entry in the MemTable.
struct MemTableEntry {
    std::string key;
    std::string value;
    bool is_tombstone;   // true → key has been deleted
    uint64_t timestamp;
};

/// In-memory sorted key-value store (backed by std::map for ordered iteration).
///
/// Supports tombstones: a delete marks the key as deleted rather than removing
/// it, so that SSTables downstream can record the deletion.  When the MemTable
/// exceeds a configurable size threshold it should be flushed to an SSTable.
class MemTable {
public:
    /// @param max_size_bytes  Approximate memory budget before flush is needed.
    explicit MemTable(std::size_t max_size_bytes = 4UL * 1024 * 1024);

    /// Insert or update a key.
    void put(const std::string& key, const std::string& value);

    /// Look up a key.  Returns the entry (including tombstones).
    std::optional<MemTableEntry> get(const std::string& key) const;

    /// Mark a key as deleted (tombstone).
    void remove(const std::string& key);

    /// True when approximate memory usage exceeds the configured threshold.
    bool shouldFlush() const;

    /// Return all entries sorted by key (for SSTable flush).
    std::vector<MemTableEntry> getEntries() const;

    /// Drop all entries and reset byte counter.
    void clear();

    /// Number of entries (including tombstones).
    std::size_t size() const;

    /// Approximate in-memory byte usage.
    std::size_t sizeBytes() const;

    /// True if there are no entries.
    bool empty() const;

    /// True if the key exists (even as a tombstone).
    bool contains(const std::string& key) const;

private:
    std::size_t max_size_bytes_;
    std::size_t current_size_bytes_{0};
    std::map<std::string, MemTableEntry> entries_;
};

}  // namespace kvstore

#endif  // KVSTORE_MEMTABLE_H
