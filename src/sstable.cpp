#include "kvstore/sstable.h"

namespace kvstore {

// ── SSTableWriter ───────────────────────────────────────────────────────────

// TODO: Implement — open file, write header, serialize each entry, close file.
bool SSTableWriter::write(const std::vector<SSTableEntry>& /*entries*/,
                          const std::string& /*filepath*/) {
    return false;
}

// ── SSTableReader ───────────────────────────────────────────────────────────

SSTableReader::SSTableReader(const std::string& filepath)
    : filepath_(filepath) {}

// TODO: Implement — scan the file for the given key.
std::optional<SSTableEntry> SSTableReader::get(
    const std::string& /*key*/) const {
    return std::nullopt;
}

// TODO: Implement — read entries with keys in [start_key, end_key].
std::vector<SSTableEntry> SSTableReader::scan(
    const std::string& /*start_key*/,
    const std::string& /*end_key*/) const {
    return {};
}

// TODO: Implement — read every entry from the file.
std::vector<SSTableEntry> SSTableReader::readAll() const { return {}; }

std::string SSTableReader::getFilePath() const { return filepath_; }

}  // namespace kvstore
