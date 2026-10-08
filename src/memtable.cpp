#include "kvstore/memtable.h"

#include <chrono>

namespace kvstore {

// ── Helper ──────────────────────────────────────────────────────────────────
// Returns the current system time in milliseconds since the UNIX epoch.
// Used to timestamp every entry so we can track the chronological order of writes.
static uint64_t now_ms() {
    return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
                                     std::chrono::system_clock::now().time_since_epoch())
                                     .count());
}

// ── Constructor ─────────────────────────────────────────────────────────────
// Stores the maximum byte threshold. Once the MemTable exceeds this size,
// shouldFlush() returns true, signaling the StorageEngine to dump it to an SSTable.
MemTable::MemTable(std::size_t max_size_bytes) : max_size_bytes_(max_size_bytes) {}

// ── put() ───────────────────────────────────────────────────────────────────
// Inserts or updates a key-value pair in the in-memory sorted map.
//
// Why entries_[key] = ... instead of entries_.insert()?
//   insert() silently does NOTHING if the key already exists.
//   operator[] overwrites the value if it already exists — which is what we need
//   for an "upsert" (update-or-insert) operation.
void MemTable::put(const std::string& key, const std::string& value) {
    // 1. Check if this key already exists in the map.
    auto it = entries_.find(key);
    if (it != entries_.end()) {
        // 2a. Key exists — subtract the OLD entry's size from the byte counter
        //     before we overwrite it, so our size tracking stays accurate.
        current_size_bytes_ -=
            it->second.key.size() + it->second.value.size() + sizeof(MemTableEntry);
    }

    // 3. Insert or overwrite the entry in the map.
    //    - is_tombstone = false (this is a live value, not a deletion marker)
    //    - now_ms() captures the exact time this write happened
    entries_[key] = {key, value, false, now_ms()};

    // 4. Add the NEW entry's size to the byte counter.
    current_size_bytes_ += key.size() + value.size() + sizeof(MemTableEntry);
}

// ── get() ───────────────────────────────────────────────────────────────────
// Looks up a key in the map. Returns the full MemTableEntry (including tombstones)
// wrapped in std::optional. Returns std::nullopt if the key was never written.
//
// Note: Even if the entry is a tombstone, we still return it.
// The caller (StorageEngine) needs to see tombstones to know the key was deleted
// and should NOT fall through to searching older SSTables.
std::optional<MemTableEntry> MemTable::get(const std::string& key) const {
    // entries_.find() returns an iterator pointing to the key-value pair,
    // or entries_.end() if the key doesn't exist.
    auto it = entries_.find(key);
    if (it == entries_.end()) {
        return std::nullopt;
    }
    // it->second gives us the MemTableEntry (the value in the map).
    // it->first would give us the key string.
    return it->second;
}

// ── remove() ────────────────────────────────────────────────────────────────
// Marks a key as deleted by inserting a "tombstone" entry.
//
// Why not just erase the key from the map?
//   Because older versions of this key might exist in SSTables on disk.
//   If we simply erased it here, a get() would miss the MemTable, fall through
//   to the SSTable, and find the OLD value — making it look like the key was
//   never deleted! The tombstone acts as a "this key is dead" marker that
//   overrides any older values on disk.
void MemTable::remove(const std::string& key) {
    // Insert a tombstone: value is empty, is_tombstone = true.
    entries_[key] = {key, "", true, now_ms()};
    current_size_bytes_ += key.size() + sizeof(MemTableEntry);
}

// ── shouldFlush() ───────────────────────────────────────────────────────────
// Returns true when the MemTable has accumulated enough data to be worth
// flushing to disk as an SSTable. The StorageEngine checks this after every put().
bool MemTable::shouldFlush() const {
    return current_size_bytes_ >= max_size_bytes_;
}

// ── getEntries() ────────────────────────────────────────────────────────────
// Returns ALL entries (including tombstones) sorted by key.
// This is called when flushing the MemTable to an SSTable on disk.
//
// Because entries_ is a std::map (Red-Black Tree), iterating over it
// automatically gives us keys in sorted alphabetical order — no sorting needed!
std::vector<MemTableEntry> MemTable::getEntries() const {
    std::vector<MemTableEntry> result;
    // Structured binding: [key, entry] unpacks each pair in the map.
    // key = the map key (std::string), entry = the map value (MemTableEntry).
    for (const auto& [key, entry] : entries_) {
        result.push_back(entry);
    }
    return result;
}

// ── clear() ─────────────────────────────────────────────────────────────────
// Wipes the MemTable clean. Called after a successful flush to an SSTable,
// since all the data is now safely persisted on disk.
void MemTable::clear() {
    entries_.clear();        // Remove all entries from the map
    current_size_bytes_ = 0; // Reset the byte counter
}

// ── size() ──────────────────────────────────────────────────────────────────
// Returns the number of entries (including tombstones) currently in the map.
std::size_t MemTable::size() const {
    return entries_.size();
}

// ── sizeBytes() ─────────────────────────────────────────────────────────────
// Returns the approximate memory usage in bytes. This is used by shouldFlush()
// to decide when the MemTable is "full".
std::size_t MemTable::sizeBytes() const {
    return current_size_bytes_;
}

// ── empty() ─────────────────────────────────────────────────────────────────
// Returns true if the map has zero entries.
bool MemTable::empty() const {
    return entries_.empty();
}

// ── contains() ──────────────────────────────────────────────────────────────
// Checks if a key exists in the map (even if it's a tombstone).
// entries_.count(key) returns 1 if the key exists, 0 if it doesn't.
bool MemTable::contains(const std::string& key) const {
    return entries_.count(key) > 0;
}

}  // namespace kvstore
