#include "kvstore/storage_engine.h"

#include <filesystem>

#include "kvstore/memtable.h"
#include "kvstore/sstable.h"
#include "kvstore/wal.h"

namespace kvstore {

// TODO: Implement — create data_dir if needed, open WAL, create MemTable, call recover().
StorageEngine::StorageEngine(const std::string& data_dir) : data_dir_(data_dir) {
    std::filesystem::create_directories(data_dir_);
    wal_ = std::make_unique<WAL>(data_dir_ + "/wal.log");
    memtable_ = std::make_unique<MemTable>();
    recover();
}

StorageEngine::~StorageEngine() = default;

StorageEngine::StorageEngine(StorageEngine&&) noexcept = default;
StorageEngine& StorageEngine::operator=(StorageEngine&&) noexcept = default;

// TODO: Implement — WAL::append then MemTable::put, flush if needed.
void StorageEngine::put(const std::string& key, const std::string& value) {
    wal_->append(key, value, OpType::PUT);
    memtable_->put(key, value);
    if (memtable_->shouldFlush())
        flush();
}

// TODO: Implement — check MemTable first, then SSTables newest→oldest.
std::optional<std::string> StorageEngine::get(const std::string& key) const {
    if (memtable_) {
        auto val = memtable_->get(key);
        if (val && val->is_tombstone)
            return std::nullopt;
        if (val)
            return val->value;
    }
    for (const auto& sstable : sstables_) {
        auto val = sstable->get(key);
        if (val && val->is_tombstone)
            return std::nullopt;
        if (val)
            return val->value;
    }
    return std::nullopt;
}

// TODO: Implement — WAL::append(DELETE), MemTable::remove.
bool StorageEngine::del(const std::string& key) {
    auto val = get(key);
    if (!val)
        return false;
    if (wal_) {
        wal_->append(key, "", OpType::DELETE);
    }
    if (memtable_) {
        memtable_->remove(key);
    }
    return true;
}

// TODO: Implement — flush MemTable entries to a new SSTable, clear WAL.
void StorageEngine::flush() {
    if (!memtable_ || memtable_->empty())
        return;
    auto memEntries = memtable_->getEntries();
    std::vector<SSTableEntry> entries;
    entries.reserve(memEntries.size());
    for (auto& me : memEntries) {
        entries.push_back({std::move(me.key), std::move(me.value), me.is_tombstone});
    }
    std::string filepath = data_dir_ + "/sst_" + std::to_string(sstable_seq_++) + ".sst";
    SSTableWriter::write(entries, filepath);
    sstables_.insert(sstables_.begin(), std::make_unique<SSTableReader>(filepath));
    memtable_->clear();
    wal_->clear();
}

// TODO: Implement — replay WAL entries into MemTable.
void StorageEngine::recover() {
    auto entries = wal_->replay();
    for (const auto& entry : entries) {
        if (entry.op_type == OpType::PUT)
            memtable_->put(entry.key, entry.value);
        else if (entry.op_type == OpType::DELETE)
            memtable_->remove(entry.key);
    }
}

}  // namespace kvstore
