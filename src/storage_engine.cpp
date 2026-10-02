#include "kvstore/storage_engine.h"

#include <filesystem>

#include "kvstore/memtable.h"
#include "kvstore/sstable.h"
#include "kvstore/wal.h"

namespace kvstore {

// ── Constructor ─────────────────────────────────────────────────────────────
//
// Sets up the LSM-tree storage engine. The three core components are:
//   - WAL (Write-Ahead Log): durability — survives crashes
//   - MemTable: fast in-memory writes (sorted std::map)
//   - SSTables: immutable sorted files on disk
//
// Initialization order matters:
//   1. Create directory  → so WAL and SSTable files have somewhere to live
//   2. Open WAL          → so we can replay it in step 4
//   3. Create MemTable   → empty, ready to receive writes
//   4. recover()         → replay WAL entries into the MemTable
//                          (restores any writes from before a crash)

StorageEngine::StorageEngine(const std::string& data_dir) : data_dir_(data_dir) {
    // create_directories is safe to call even if the dir already exists.
    std::filesystem::create_directories(data_dir_);

    // Open (or create) the WAL file. It lives inside the data directory.
    wal_ = std::make_unique<WAL>(data_dir_ + "/wal.log");

    // Create a fresh MemTable with the default 4MB size threshold.
    memtable_ = std::make_unique<MemTable>();

    // Replay the WAL to restore any unflushed writes (crash recovery).
    recover();
}

StorageEngine::~StorageEngine() = default;

StorageEngine::StorageEngine(StorageEngine&&) noexcept = default;
StorageEngine& StorageEngine::operator=(StorageEngine&&) noexcept = default;

// ── put() — Write path ──────────────────────────────────────────────────────
//
// Every write follows this sequence:
//   1. WAL first   → guarantees we can recover this write after a crash
//   2. MemTable    → fast O(log n) insert into the in-memory sorted map
//   3. Auto-flush  → if the MemTable exceeds its byte threshold, dump it
//                    to a new SSTable so memory doesn't grow unbounded
//
// Why WAL before MemTable?
//   If we crash after WAL but before MemTable, recover() will replay it.
//   If we wrote to MemTable first and crashed before WAL, the write is lost.

void StorageEngine::put(const std::string& key, const std::string& value) {
    // 1. Append to WAL for durability (persisted to disk immediately).
    wal_->append(key, value, OpType::PUT);

    // 2. Write to the in-memory MemTable (fast, sorted insert).
    memtable_->put(key, value);

    // 3. If MemTable has exceeded its size limit, flush to an SSTable.
    //    shouldFlush() checks byte size against the configured threshold (default 4MB).
    if (memtable_->shouldFlush())
        flush();
}

// ── get() — Read path ───────────────────────────────────────────────────────
//
// Lookups follow "newest data wins" — check the most recent sources first:
//   1. MemTable (most recent writes, not yet flushed)
//   2. SSTables newest → oldest (sstables_[0] is the most recently flushed)
//
// Why this order?
//   A key might exist in both an old SSTable AND the MemTable (if it was
//   overwritten). The MemTable version is newer, so it must take priority.
//
// Tombstone handling:
//   If we find the key but it's marked as a tombstone (deleted), we return
//   nullopt immediately — we do NOT continue searching older SSTables,
//   because the tombstone means "this key was intentionally deleted."

std::optional<std::string> StorageEngine::get(const std::string& key) const {
    // 1. Check MemTable first — it has the most recent writes.
    if (memtable_) {
        auto val = memtable_->get(key);

        // Found a tombstone → key was deleted → stop searching.
        if (val && val->is_tombstone)
            return std::nullopt;

        // Found a live value → return it.
        if (val)
            return val->value;
    }

    // 2. Not in MemTable → search SSTables from newest to oldest.
    //    sstables_ is ordered newest-first, so the first match is
    //    the most recent version of the key.
    for (const auto& sstable : sstables_) {
        auto val = sstable->get(key);

        // Tombstone in SSTable → key was deleted at this point in time.
        if (val && val->is_tombstone)
            return std::nullopt;

        // Live value found → return it.
        if (val)
            return val->value;
    }

    // 3. Key not found anywhere.
    return std::nullopt;
}

// ── del() — Delete path ────────────────────────────────────────────────────
//
// Deletion in an LSM-tree does NOT erase data. Instead, it writes a
// "tombstone" — a marker that says "this key is dead." The tombstone
// shadows any older values in SSTables during get().
//
// Steps:
//   1. Check if the key actually exists (and isn't already deleted).
//   2. Log the DELETE to the WAL for crash recovery.
//   3. Write a tombstone to the MemTable to shadow older SSTable values.

bool StorageEngine::del(const std::string& key) {
    // 1. Check if the key exists. If not (or already deleted), return false.
    auto val = get(key);
    if (!val)
        return false;

    // 2. Log DELETE to WAL (value is empty for deletes).
    wal_->append(key, "", OpType::DELETE);

    // 3. Write a tombstone to MemTable — this will shadow any older
    //    versions of this key in SSTables during future get() calls.
    memtable_->remove(key);

    return true;
}

// ── flush() — MemTable → SSTable ────────────────────────────────────────────
//
// Dumps all MemTable entries to a new immutable SSTable file on disk.
// This is triggered automatically when the MemTable exceeds its size
// threshold, or manually via engine.flush().
//
// Steps:
//   1. Get sorted entries from MemTable.
//   2. Convert MemTableEntry → SSTableEntry (drop the timestamp field).
//   3. Write to a new .sst file with a unique sequential name.
//   4. Create an SSTableReader and add it to the FRONT of sstables_
//      (newest first — so get() searches it before older SSTables).
//   5. Clear the MemTable (data is now on disk).
//   6. Clear the WAL (all logged ops are now persisted in the SSTable).

void StorageEngine::flush() {
    // Guard: nothing to flush if MemTable is empty.
    if (!memtable_ || memtable_->empty())
        return;

    // 1. Get all entries sorted by key (std::map guarantees sorted order).
    auto memEntries = memtable_->getEntries();

    // 2. Convert MemTableEntry → SSTableEntry.
    //    We drop the timestamp — SSTables don't need it because ordering
    //    is determined by which SSTable is newer (sequence number).
    std::vector<SSTableEntry> entries;
    entries.reserve(memEntries.size());
    for (auto& me : memEntries) {
        entries.push_back({std::move(me.key), std::move(me.value), me.is_tombstone});
    }

    // 3. Generate a unique filename using the monotonic sequence counter.
    //    e.g., "sst_0.sst", "sst_1.sst", "sst_2.sst", ...
    //    sstable_seq_++ increments AFTER using the current value.
    std::string filepath = data_dir_ + "/sst_" + std::to_string(sstable_seq_++) + ".sst";

    // 4. Write the sorted entries to disk as an SSTable.
    SSTableWriter::write(entries, filepath);

    // 5. Create a reader for the new SSTable and insert at the FRONT.
    //    Front = newest, so get() will search this SSTable before older ones.
    sstables_.insert(sstables_.begin(), std::make_unique<SSTableReader>(filepath));

    // 6. Clear MemTable — all its data is now safely in the SSTable.
    memtable_->clear();

    // 7. Clear WAL — all logged operations are now persisted in the SSTable.
    //    On next crash, recover() won't replay these (they're already on disk).
    wal_->clear();
}

// ── recover() — WAL replay (crash recovery) ────────────────────────────────
//
// Called during construction to restore unflushed writes after a crash.
//
// Scenario: The engine was running, received writes (logged to WAL +
// stored in MemTable), but crashed BEFORE flush(). On restart:
//   - The MemTable is gone (it was in-memory, lost on crash).
//   - The WAL is still on disk (it was persisted).
//   - replay() reads the WAL and returns all logged operations.
//   - We re-apply each operation to the fresh MemTable.
//
// After this, the MemTable is in the same state as before the crash.

void StorageEngine::recover() {
    // Read all entries from the WAL file on disk.
    auto entries = wal_->replay();

    // Re-apply each operation to the MemTable.
    for (const auto& entry : entries) {
        if (entry.op_type == OpType::PUT)
            memtable_->put(entry.key, entry.value);
        else if (entry.op_type == OpType::DELETE)
            memtable_->remove(entry.key);
    }
}

}  // namespace kvstore
