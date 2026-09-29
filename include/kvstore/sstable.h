#ifndef KVSTORE_SSTABLE_H
#define KVSTORE_SSTABLE_H

#include <cstdint>
#include <fstream>
#include <optional>
#include <string>
#include <vector>

namespace kvstore {

/// A single key-value record stored in an SSTable file.
struct SSTableEntry {
    std::string key;
    std::string value;
    bool is_tombstone;  // true → this entry represents a deletion
};

/// Index entry: maps a key to its byte offset in the data section.
/// Used by SSTableReader to perform O(log n) binary search lookups.
struct IndexEntry {
    std::string key;
    uint64_t offset;  // byte offset of this entry's data in the file
};

/// Writes a sorted sequence of entries to an immutable SSTable file on disk.
///
/// File format:
///   ┌──────────────────────────────────────────────────┐
///   │  DATA SECTION  (per entry, back-to-back):        │
///   │  [8B key_len | key | 8B val_len | value | 1B ts] │
///   ├──────────────────────────────────────────────────┤
///   │  INDEX SECTION (per entry):                      │
///   │  [8B key_len | key | 8B data_offset]             │
///   ├──────────────────────────────────────────────────┤
///   │  FOOTER (fixed 16 bytes):                        │
///   │  [8B index_offset | 8B entry_count]              │
///   └──────────────────────────────────────────────────┘
class SSTableWriter {
public:
    /// Write all entries to the given file path.  Entries MUST be sorted by key.
    /// Returns true on success.
    static bool write(const std::vector<SSTableEntry>& entries,
                      const std::string& filepath);
};

/// Reads from an existing SSTable file.
///
/// On construction, loads the index section into memory so that point
/// lookups (get) and range scans (scan) can use binary search — O(log n).
class SSTableReader {
public:
    /// Open an SSTable file for reading.  Loads the in-memory index.
    explicit SSTableReader(const std::string& filepath);

    /// Point lookup — returns the entry for `key`, or nullopt.
    std::optional<SSTableEntry> get(const std::string& key) const;

    /// Range scan — returns entries with keys in [start_key, end_key].
    std::vector<SSTableEntry> scan(const std::string& start_key,
                                   const std::string& end_key) const;

    /// Read every entry in the file.
    std::vector<SSTableEntry> readAll() const;

    /// Path to the backing file.
    std::string getFilePath() const;

private:
    std::string filepath_;
    std::vector<IndexEntry> index_;  // in-memory index, loaded from footer

    /// Load the index from the file's index section + footer.
    void loadIndex();

    /// Read a single SSTableEntry starting at the given byte offset.
    SSTableEntry readEntryAt(std::ifstream& ifs, uint64_t offset) const;
};

}  // namespace kvstore

#endif  // KVSTORE_SSTABLE_H
