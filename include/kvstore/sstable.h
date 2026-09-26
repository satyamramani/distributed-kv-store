#ifndef KVSTORE_SSTABLE_H
#define KVSTORE_SSTABLE_H

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

/// Writes a sorted sequence of entries to an immutable SSTable file on disk.
///
/// File format (per entry):
///   [4B key_len | key_bytes | 4B val_len | val_bytes | 1B is_tombstone]
class SSTableWriter {
public:
    /// Write all entries to the given file path.  Entries MUST be sorted by key.
    /// Returns true on success.
    static bool write(const std::vector<SSTableEntry>& entries,
                      const std::string& filepath);
};

/// Reads from an existing SSTable file.
class SSTableReader {
public:
    /// Open an SSTable file for reading.
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
};

}  // namespace kvstore

#endif  // KVSTORE_SSTABLE_H
