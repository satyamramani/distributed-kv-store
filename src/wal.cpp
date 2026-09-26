#include "kvstore/wal.h"

#include <chrono>
#include <fstream>

namespace kvstore {

// TODO: Implement — open/create the WAL file for appending.
WAL::WAL(const std::string& filepath) : filepath_(filepath) {
    // If the file doesn't exist, this will create it.
    // If it does, we don't truncate it (we might want to replay it later).
    std::ofstream ofs(filepath_, std::ios::app | std::ios::binary);
}

// TODO: Implement — close file handle if open.
WAL::~WAL() = default;

// ── append() ────────────────────────────────────────────────────────────────
// Safely writes a new operation (like PUT or DELETE) to the end of the file.
void WAL::append(const std::string& key, const std::string& value, OpType op_type) {
    // 1. Open the file in append (app) and binary mode.
    // Append mode ensures we never overwrite existing data.
    // Binary mode prevents C++ from altering bytes (like newline characters).
    std::ofstream ofs(filepath_, std::ios::app | std::ios::binary);
    if (!ofs.is_open()) {
        throw std::runtime_error("Failed to open WAL file for appending");
    }

    // 2. Grab the current system time in milliseconds since the UNIX epoch.
    // This allows us to know the exact chronological order of operations.
    uint64_t timestamp =
        static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
                                  std::chrono::system_clock::now().time_since_epoch())
                                  .count());

    // 3. Pre-calculate the lengths of the strings.
    // We need to write these lengths to the file so the reader knows how many bytes to read.
    auto key_len = static_cast<uint32_t>(key.size());
    auto val_len = static_cast<uint32_t>(value.size());

    // 4. Write all data out as raw bytes.
    // reinterpret_cast tells C++ to treat the memory addresses as raw byte arrays.
    ofs.write(reinterpret_cast<const char*>(&timestamp), sizeof(timestamp));
    ofs.write(reinterpret_cast<const char*>(&op_type), sizeof(op_type));
    ofs.write(reinterpret_cast<const char*>(&key_len), sizeof(key_len));
    ofs.write(key.data(), key_len);  // key.data() is already a byte array
    ofs.write(reinterpret_cast<const char*>(&val_len), sizeof(val_len));
    ofs.write(value.data(), val_len);

    // 5. Force the OS to push data to the physical disk (guarantees durability!)
    ofs.flush();

    entry_count_++;
    if (!ofs.good()) {
        throw std::runtime_error("Failed to write to WAL file");
    }
}

// ── replay() ────────────────────────────────────────────────────────────────
// Called on system startup. Reads the entire WAL to recover data in case of a crash.
std::vector<LogEntry> WAL::replay() const {
    std::vector<LogEntry> entries;
    // Open an input stream (ifs) in binary mode to read raw bytes
    std::ifstream ifs(filepath_, std::ios::binary);

    if (!ifs)
        return entries;  // File might not exist or be unreadable

    // Keep reading until we hit the End Of File (EOF)
    while (ifs.peek() != EOF) {
        LogEntry entry;

        // Read Timestamp (8 bytes). If this fails, the file ended unexpectedly, so we break out.
        if (!ifs.read(reinterpret_cast<char*>(&entry.timestamp), sizeof(entry.timestamp)))
            break;

        // Read OpType (1 byte)
        ifs.read(reinterpret_cast<char*>(&entry.op_type), sizeof(entry.op_type));

        // Read Key Length (4 bytes), then resize the string to hold the exact number of characters.
        // Finally, read those characters directly into the string.
        uint32_t key_len = 0;
        ifs.read(reinterpret_cast<char*>(&key_len), sizeof(key_len));
        entry.key.resize(key_len);
        ifs.read(entry.key.data(), key_len);

        // Do the same for the Value Length (4 bytes) and the Value itself.
        uint32_t val_len = 0;
        ifs.read(reinterpret_cast<char*>(&val_len), sizeof(val_len));
        entry.value.resize(val_len);
        ifs.read(entry.value.data(), val_len);

        // std::move transfers the object into the vector efficiently without copying
        entries.push_back(std::move(entry));
    }

    // Note: We don't update entry_count_ here because replay() is const,
    // but the WAL is typically re-created or clear() is called after a flush.
    return entries;
}

// TODO: Implement — truncate/clear the file and reset counter.
void WAL::clear() {
    // Truncate the file by opening it with std::ios::trunc
    std::ofstream ofs(filepath_, std::ios::trunc | std::ios::binary);
    entry_count_ = 0;
}

std::size_t WAL::entryCount() const {
    return entry_count_;
}

std::string WAL::getFilePath() const {
    return filepath_;
}

}  // namespace kvstore
