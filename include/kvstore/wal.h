#ifndef KVSTORE_WAL_H
#define KVSTORE_WAL_H

#include <cstdint>
#include <string>
#include <vector>

namespace kvstore {

/// Operation types for WAL entries.
enum class OpType : uint8_t {
    PUT = 0,
    DELETE = 1,
};

/// A single entry in the write-ahead log.
struct LogEntry {
    uint64_t timestamp;
    OpType op_type;
    std::string key;
    std::string value;  // empty for DELETE operations
};

/// Write-Ahead Log (Commit Log) for durability.
///
/// Every write is appended to disk before modifying in-memory state.
/// On crash recovery, the log is replayed to restore the MemTable.
///
/// Log entry binary format:
///   [8B timestamp | 1B op_type | 4B key_len | key | 4B val_len | value]
class WAL {
public:
    /// Open (or create) a WAL file at the given path.
    explicit WAL(const std::string& filepath);
    ~WAL();

    // Non-copyable, non-movable (holds file handle)
    WAL(const WAL&) = delete;
    WAL& operator=(const WAL&) = delete;
    WAL(WAL&&) = delete;
    WAL& operator=(WAL&&) = delete;

    /// Append a write operation to the log.
    void append(const std::string& key, const std::string& value, OpType op_type);

    /// Replay the entire log from disk.  Returns entries in order.
    std::vector<LogEntry> replay() const;

    /// Truncate the log file (called after successful SSTable flush).
    void clear();

    /// Number of entries written since last clear (tracked in memory).
    std::size_t entryCount() const;

    /// Path to the underlying log file.
    std::string getFilePath() const;

private:
    std::string filepath_;
    std::size_t entry_count_{0};
};

}  // namespace kvstore

#endif  // KVSTORE_WAL_H
