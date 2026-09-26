#ifndef KVSTORE_STORAGE_ENGINE_H
#define KVSTORE_STORAGE_ENGINE_H

#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace kvstore {

// Forward declarations — keeps this header lightweight.
class WAL;
class MemTable;
class SSTableReader;

/// LSM-Tree based storage engine.
///
/// **Write path:**  Client → WAL::append → MemTable::put → (if full) flush to SSTable
/// **Read  path:**  MemTable → SSTables (newest first) → not found
///
/// On construction the engine replays the WAL to recover any unflushed writes.
class StorageEngine {
public:
    /// @param data_dir  Directory for WAL and SSTable files.
    explicit StorageEngine(const std::string& data_dir);
    ~StorageEngine();

    // Non-copyable, movable
    StorageEngine(const StorageEngine&) = delete;
    StorageEngine& operator=(const StorageEngine&) = delete;
    StorageEngine(StorageEngine&&) noexcept;
    StorageEngine& operator=(StorageEngine&&) noexcept;

    /// Insert or update a key-value pair.
    void put(const std::string& key, const std::string& value);

    /// Retrieve a value.  Returns nullopt if key does not exist or is deleted.
    std::optional<std::string> get(const std::string& key) const;

    /// Delete a key.  Returns true if the key existed.
    bool del(const std::string& key);

    /// Force-flush the active MemTable to a new SSTable on disk.
    void flush();

    /// Replay the WAL to rebuild in-memory state (called by constructor).
    void recover();

private:
    std::string data_dir_;
    std::unique_ptr<WAL> wal_;
    std::unique_ptr<MemTable> memtable_;
    std::vector<std::unique_ptr<SSTableReader>> sstables_;  // newest first
    uint64_t sstable_seq_{0};  // monotonic counter for SSTable filenames
};

}  // namespace kvstore

#endif  // KVSTORE_STORAGE_ENGINE_H
