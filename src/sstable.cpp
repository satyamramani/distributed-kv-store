#include "kvstore/sstable.h"

#include <algorithm>
#include <fstream>

namespace kvstore {

// ── SSTableWriter ───────────────────────────────────────────────────────────
//
// Writes a sorted list of SSTableEntry records to a binary file on disk.
//
// On-disk layout (three sections written sequentially):
//
//   1. DATA SECTION — the actual key-value entries (variable size per entry):
//        [key_len (8B) | key | val_len (8B) | value | is_tombstone (1B)]
//
//   2. INDEX SECTION — one index record per entry (maps key → data offset):
//        [key_len (8B) | key | data_offset (8B)]
//
//   3. FOOTER — fixed 16 bytes at the very end of the file:
//        [index_offset (8B) | entry_count (8B)]
//
// The footer lets the reader locate the index section and know how many
// entries to expect.  The index is sorted (since entries are sorted by key),
// enabling O(log n) binary search on read.

bool SSTableWriter::write(const std::vector<SSTableEntry>& entries, const std::string& filepath) {
    // Open output file in binary mode (truncates any existing content).
    std::ofstream ofs(filepath, std::ios::binary);

    if (!ofs.is_open()) {
        return false;
    }

    // ── 1. Write the DATA SECTION ──────────────────────────────────────────
    // While writing, collect each entry's {key, file_offset} for the index.
    std::vector<IndexEntry> index;
    index.reserve(entries.size());

    for (const auto& entry : entries) {
        // Record the file offset *before* writing this entry's data.
        auto offset = static_cast<uint64_t>(ofs.tellp());
        index.push_back({entry.key, offset});

        size_t key_len = entry.key.size();
        size_t val_len = entry.value.size();

        // write key length + key
        ofs.write(reinterpret_cast<const char*>(&key_len), sizeof(key_len));
        ofs.write(entry.key.data(), static_cast<std::streamsize>(key_len));

        // write value length + value
        ofs.write(reinterpret_cast<const char*>(&val_len), sizeof(val_len));
        ofs.write(entry.value.data(), static_cast<std::streamsize>(val_len));

        // write tombstone flag (1 byte)
        ofs.write(reinterpret_cast<const char*>(&entry.is_tombstone), sizeof(entry.is_tombstone));
    }

    // ── 2. Write the INDEX SECTION ─────────────────────────────────────────
    // Record where the index starts — this goes into the footer.
    auto index_offset = static_cast<uint64_t>(ofs.tellp());

    for (const auto& idx : index) {
        size_t key_len = idx.key.size();

        // write key length + key
        ofs.write(reinterpret_cast<const char*>(&key_len), sizeof(key_len));
        ofs.write(idx.key.data(), static_cast<std::streamsize>(key_len));

        // write the data offset this key maps to
        ofs.write(reinterpret_cast<const char*>(&idx.offset), sizeof(idx.offset));
    }

    // ── 3. Write the FOOTER (16 bytes) ─────────────────────────────────────
    // The reader reads these last 16 bytes to locate the index section.
    auto entry_count = static_cast<uint64_t>(entries.size());
    ofs.write(reinterpret_cast<const char*>(&index_offset), sizeof(index_offset));
    ofs.write(reinterpret_cast<const char*>(&entry_count), sizeof(entry_count));

    // Flush the internal buffer to ensure all data reaches the OS.
    ofs.flush();

    // ofs.good() returns true if no I/O errors occurred during writing.
    return ofs.good();
}

// ── SSTableReader ───────────────────────────────────────────────────────────

SSTableReader::SSTableReader(const std::string& filepath) : filepath_(filepath) {
    // Load the in-memory index from the file's index section + footer.
    // After this, get() and scan() can use binary search — O(log n).
    loadIndex();
}

// ── loadIndex() — Read footer + index section into memory ───────────────────
//
// Steps:
//   1. Open file, seek to (end - 16) to read the footer.
//   2. Footer gives us index_offset and entry_count.
//   3. Seek to index_offset, read entry_count × {key_len, key, offset}.
//   4. Store them in index_ (already sorted since entries were written sorted).

void SSTableReader::loadIndex() {
    std::ifstream ifs(filepath_, std::ios::binary);
    if (!ifs.is_open()) {
        return;  // file doesn't exist yet (e.g., empty SSTable path)
    }

    // Check if file is empty (no footer to read).
    ifs.seekg(0, std::ios::end);
    auto file_size = ifs.tellg();
    if (file_size < 16) {
        return;  // file too small to contain a valid footer
    }

    // 1. Read the footer: last 16 bytes of the file.
    ifs.seekg(-16, std::ios::end);
    uint64_t index_offset = 0;
    uint64_t entry_count = 0;
    ifs.read(reinterpret_cast<char*>(&index_offset), sizeof(index_offset));
    ifs.read(reinterpret_cast<char*>(&entry_count), sizeof(entry_count));

    if (entry_count == 0) {
        return;  // empty SSTable, nothing to index
    }

    // 2. Seek to the index section and read all {key, offset} pairs.
    ifs.seekg(static_cast<std::streamoff>(index_offset));
    index_.reserve(entry_count);

    for (uint64_t i = 0; i < entry_count; ++i) {
        size_t key_len = 0;
        uint64_t offset = 0;

        ifs.read(reinterpret_cast<char*>(&key_len), sizeof(key_len));
        std::string key(key_len, '\0');
        ifs.read(key.data(), static_cast<std::streamsize>(key_len));
        ifs.read(reinterpret_cast<char*>(&offset), sizeof(offset));

        index_.push_back({std::move(key), offset});
    }
}

// ── readEntryAt() — Read one entry at a given byte offset ───────────────────
//
// Seeks to `offset` in the already-open stream, then deserializes one
// complete entry (key_len, key, val_len, value, tombstone).

SSTableEntry SSTableReader::readEntryAt(std::ifstream& ifs, uint64_t offset) const {
    ifs.seekg(static_cast<std::streamoff>(offset));

    SSTableEntry entry;
    size_t key_len = 0;
    size_t val_len = 0;

    // Read key
    ifs.read(reinterpret_cast<char*>(&key_len), sizeof(key_len));
    entry.key.resize(key_len);
    ifs.read(entry.key.data(), static_cast<std::streamsize>(key_len));

    // Read value
    ifs.read(reinterpret_cast<char*>(&val_len), sizeof(val_len));
    entry.value.resize(val_len);
    ifs.read(entry.value.data(), static_cast<std::streamsize>(val_len));

    // Read tombstone flag
    bool is_tombstone = false;
    ifs.read(reinterpret_cast<char*>(&is_tombstone), sizeof(is_tombstone));
    entry.is_tombstone = is_tombstone;

    return entry;
}

// ── get() — O(log n) point lookup ───────────────────────────────────────────
//
// Binary searches the in-memory index for the key.
// If found, seeks directly to the entry's file offset and reads just that
// one record — no scanning needed.

std::optional<SSTableEntry> SSTableReader::get(const std::string& key) const {
    if (index_.empty()) {
        return std::nullopt;
    }

    // Binary search: find the index entry whose key matches.
    // std::lower_bound returns an iterator to the first element with key >= target.
    auto it = std::lower_bound(
        index_.begin(), index_.end(), key,
        [](const IndexEntry& entry, const std::string& target) {
            return entry.key < target;
        });

    // Check if we actually found an exact match (lower_bound finds >=, not ==).
    if (it == index_.end() || it->key != key) {
        return std::nullopt;
    }

    // Found it — open file, seek to offset, read the single entry.
    std::ifstream ifs(filepath_, std::ios::binary);
    if (!ifs.is_open()) {
        return std::nullopt;
    }

    return readEntryAt(ifs, it->offset);
}

// ── scan() — O(log n + k) range query ──────────────────────────────────────
//
// Uses lower_bound to jump to the first key >= start_key in the index,
// then reads entries sequentially from that offset until key > end_key.
// Since entries are sorted, we can stop early once we pass end_key.

std::vector<SSTableEntry> SSTableReader::scan(const std::string& start_key,
                                              const std::string& end_key) const {
    if (index_.empty()) {
        return {};
    }

    // Binary search: find first index entry with key >= start_key.
    auto it = std::lower_bound(
        index_.begin(), index_.end(), start_key,
        [](const IndexEntry& entry, const std::string& target) {
            return entry.key < target;
        });

    // If all keys are before start_key, no results.
    if (it == index_.end()) {
        return {};
    }

    std::ifstream ifs(filepath_, std::ios::binary);
    if (!ifs.is_open()) {
        return {};
    }

    std::vector<SSTableEntry> result;

    // Read entries from the found position until we pass end_key.
    for (; it != index_.end() && it->key <= end_key; ++it) {
        result.push_back(readEntryAt(ifs, it->offset));
    }

    return result;
}

// ── readAll() — Full table dump ─────────────────────────────────────────────
//
// Reads every entry using the index offsets.
// Useful for compaction, debugging, or merging SSTables.

std::vector<SSTableEntry> SSTableReader::readAll() const {
    if (index_.empty()) {
        return {};
    }

    std::ifstream ifs(filepath_, std::ios::binary);
    if (!ifs.is_open()) {
        return {};
    }

    std::vector<SSTableEntry> result;
    result.reserve(index_.size());

    // Read every entry using the stored offsets.
    for (const auto& idx : index_) {
        result.push_back(readEntryAt(ifs, idx.offset));
    }

    return result;
}

// ── getFilePath() — Accessor ────────────────────────────────────────────────
//
// Returns the filesystem path of the SSTable file this reader is bound to.

std::string SSTableReader::getFilePath() const {
    return filepath_;
}

}  // namespace kvstore
