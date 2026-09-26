#include "kvstore/storage_engine.h"

#include "kvstore/memtable.h"
#include "kvstore/sstable.h"
#include "kvstore/wal.h"

namespace kvstore {

// TODO: Implement — create data_dir if needed, open WAL, create MemTable, call recover().
StorageEngine::StorageEngine(const std::string& data_dir)
    : data_dir_(data_dir) {}

StorageEngine::~StorageEngine() = default;

StorageEngine::StorageEngine(StorageEngine&&) noexcept = default;
StorageEngine& StorageEngine::operator=(StorageEngine&&) noexcept = default;

// TODO: Implement — WAL::append then MemTable::put, flush if needed.
void StorageEngine::put(const std::string& /*key*/,
                        const std::string& /*value*/) {}

// TODO: Implement — check MemTable first, then SSTables newest→oldest.
std::optional<std::string> StorageEngine::get(
    const std::string& /*key*/) const {
    return std::nullopt;
}

// TODO: Implement — WAL::append(DELETE), MemTable::remove.
bool StorageEngine::del(const std::string& /*key*/) { return false; }

// TODO: Implement — flush MemTable entries to a new SSTable, clear WAL.
void StorageEngine::flush() {
    (void)sstable_seq_;
}

// TODO: Implement — replay WAL entries into MemTable.
void StorageEngine::recover() {}

}  // namespace kvstore
