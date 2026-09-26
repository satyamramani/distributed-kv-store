#include <gtest/gtest.h>

#include <cstdio>
#include <filesystem>
#include <string>

#include "kvstore/wal.h"

namespace fs = std::filesystem;

class WALTest : public ::testing::Test {
protected:
    std::string test_dir;
    std::string wal_path;

    void SetUp() override {
        test_dir = fs::temp_directory_path() / "kvstore_wal_test";
        fs::create_directories(test_dir);
        wal_path = test_dir + "/test.wal";
        // Remove any leftover file from a previous run.
        std::remove(wal_path.c_str());
    }

    void TearDown() override { fs::remove_all(test_dir); }
};

// ── Append & Replay ─────────────────────────────────────────────────────────

TEST_F(WALTest, AppendAndReplaySinglePut) {
    kvstore::WAL wal(wal_path);
    wal.append("key1", "value1", kvstore::OpType::PUT);

    auto entries = wal.replay();
    ASSERT_EQ(entries.size(), 1u);
    EXPECT_EQ(entries[0].key, "key1");
    EXPECT_EQ(entries[0].value, "value1");
    EXPECT_EQ(entries[0].op_type, kvstore::OpType::PUT);
    EXPECT_GT(entries[0].timestamp, 0u);
}

TEST_F(WALTest, AppendAndReplayMultipleEntries) {
    kvstore::WAL wal(wal_path);
    wal.append("a", "1", kvstore::OpType::PUT);
    wal.append("b", "2", kvstore::OpType::PUT);
    wal.append("a", "",  kvstore::OpType::DELETE);

    auto entries = wal.replay();
    ASSERT_EQ(entries.size(), 3u);

    EXPECT_EQ(entries[0].key, "a");
    EXPECT_EQ(entries[0].op_type, kvstore::OpType::PUT);

    EXPECT_EQ(entries[1].key, "b");
    EXPECT_EQ(entries[1].op_type, kvstore::OpType::PUT);

    EXPECT_EQ(entries[2].key, "a");
    EXPECT_EQ(entries[2].op_type, kvstore::OpType::DELETE);
}

TEST_F(WALTest, ReplayEmptyLog) {
    kvstore::WAL wal(wal_path);
    auto entries = wal.replay();
    EXPECT_TRUE(entries.empty());
}

// ── Durability across instances ─────────────────────────────────────────────

TEST_F(WALTest, DurabilityAcrossReopen) {
    // Write with one WAL instance…
    {
        kvstore::WAL wal(wal_path);
        wal.append("persist", "me", kvstore::OpType::PUT);
    }
    // …replay with a fresh instance.
    {
        kvstore::WAL wal(wal_path);
        auto entries = wal.replay();
        ASSERT_EQ(entries.size(), 1u);
        EXPECT_EQ(entries[0].key, "persist");
        EXPECT_EQ(entries[0].value, "me");
    }
}

// ── Clear ───────────────────────────────────────────────────────────────────

TEST_F(WALTest, ClearTruncatesLog) {
    kvstore::WAL wal(wal_path);
    wal.append("x", "y", kvstore::OpType::PUT);
    wal.clear();

    auto entries = wal.replay();
    EXPECT_TRUE(entries.empty());
}

TEST_F(WALTest, AppendAfterClear) {
    kvstore::WAL wal(wal_path);
    wal.append("old", "data", kvstore::OpType::PUT);
    wal.clear();
    wal.append("new", "data", kvstore::OpType::PUT);

    auto entries = wal.replay();
    ASSERT_EQ(entries.size(), 1u);
    EXPECT_EQ(entries[0].key, "new");
}

// ── Entry count ─────────────────────────────────────────────────────────────

TEST_F(WALTest, EntryCountTracksAppends) {
    kvstore::WAL wal(wal_path);
    EXPECT_EQ(wal.entryCount(), 0u);

    wal.append("a", "1", kvstore::OpType::PUT);
    EXPECT_EQ(wal.entryCount(), 1u);

    wal.append("b", "2", kvstore::OpType::PUT);
    EXPECT_EQ(wal.entryCount(), 2u);
}

// ── File path ───────────────────────────────────────────────────────────────

TEST_F(WALTest, GetFilePathReturnsConfiguredPath) {
    kvstore::WAL wal(wal_path);
    EXPECT_EQ(wal.getFilePath(), wal_path);
}

// ── Edge cases ──────────────────────────────────────────────────────────────

TEST_F(WALTest, EmptyKeyAndValue) {
    kvstore::WAL wal(wal_path);
    wal.append("", "", kvstore::OpType::PUT);

    auto entries = wal.replay();
    ASSERT_EQ(entries.size(), 1u);
    EXPECT_EQ(entries[0].key, "");
    EXPECT_EQ(entries[0].value, "");
}

TEST_F(WALTest, LargeValues) {
    kvstore::WAL wal(wal_path);
    std::string big_value(8192, 'X');
    wal.append("big", big_value, kvstore::OpType::PUT);

    auto entries = wal.replay();
    ASSERT_EQ(entries.size(), 1u);
    EXPECT_EQ(entries[0].value, big_value);
}

TEST_F(WALTest, TimestampsAreMonotonicallyIncreasing) {
    kvstore::WAL wal(wal_path);
    wal.append("a", "1", kvstore::OpType::PUT);
    wal.append("b", "2", kvstore::OpType::PUT);

    auto entries = wal.replay();
    ASSERT_EQ(entries.size(), 2u);
    EXPECT_LE(entries[0].timestamp, entries[1].timestamp);
}
