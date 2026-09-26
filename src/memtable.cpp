#include "kvstore/memtable.h"

namespace kvstore {

MemTable::MemTable(std::size_t max_size_bytes)
    : max_size_bytes_(max_size_bytes) {}

// TODO: Implement — insert key/value into the sorted map, update byte counter.
void MemTable::put(const std::string& /*key*/, const std::string& /*value*/) {}

// TODO: Implement — look up key in the map, return entry if found.
std::optional<MemTableEntry> MemTable::get(const std::string& /*key*/) const {
    return std::nullopt;
}

// TODO: Implement — insert a tombstone entry for the key.
void MemTable::remove(const std::string& /*key*/) {}

// TODO: Implement — return true if current_size_bytes_ >= max_size_bytes_.
bool MemTable::shouldFlush() const {
    return current_size_bytes_ >= max_size_bytes_;
}

// TODO: Implement — collect all entries from the map in sorted order.
std::vector<MemTableEntry> MemTable::getEntries() const { return {}; }

// TODO: Implement — clear the map and reset byte counter.
void MemTable::clear() {}

std::size_t MemTable::size() const { return entries_.size(); }

std::size_t MemTable::sizeBytes() const { return current_size_bytes_; }

bool MemTable::empty() const { return entries_.empty(); }

// TODO: Implement — check if key exists in the map.
bool MemTable::contains(const std::string& /*key*/) const { return false; }

}  // namespace kvstore
