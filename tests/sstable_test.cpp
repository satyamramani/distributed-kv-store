#include <gtest/gtest.h>

#include <filesystem>
#include <string>
#include <vector>

#include "kvstore/sstable.h"

namespace fs = std::filesystem;

class SSTableTest : public ::testing::Test {
protected:
    std::string test_dir;
    std::string sst_path;

    void SetUp() override {
        test_dir = fs::temp_directory_path() / "kvstore_sst_test";
        fs::create_directories(test_dir);
        sst_path = test_dir + "/test.sst";
    }

    void TearDown() override { fs::remove_all(test_dir); }

    std::vector<kvstore::SSTableEntry> makeSortedEntries() {
        return {
            {"apple",  "red",    false},
            {"banana", "yellow", false},
            {"cherry", "red",    false},
            {"date",   "brown",  false},
        };
    }
};

// ── Write & Read All ────────────────────────────────────────────────────────

TEST_F(SSTableTest, WriteAndReadAll) {
    auto entries = makeSortedEntries();
    ASSERT_TRUE(kvstore::SSTableWriter::write(entries, sst_path));

    kvstore::SSTableReader reader(sst_path);
    auto result = reader.readAll();

    ASSERT_EQ(result.size(), entries.size());
    for (std::size_t i = 0; i < entries.size(); ++i) {
        EXPECT_EQ(result[i].key, entries[i].key);
        EXPECT_EQ(result[i].value, entries[i].value);
        EXPECT_EQ(result[i].is_tombstone, entries[i].is_tombstone);
    }
}

// ── Point Lookup ────────────────────────────────────────────────────────────

TEST_F(SSTableTest, GetExistingKey) {
    ASSERT_TRUE(kvstore::SSTableWriter::write(makeSortedEntries(), sst_path));
    kvstore::SSTableReader reader(sst_path);

    auto entry = reader.get("banana");
    ASSERT_TRUE(entry.has_value());
    EXPECT_EQ(entry->key, "banana");
    EXPECT_EQ(entry->value, "yellow");
    EXPECT_FALSE(entry->is_tombstone);
}

TEST_F(SSTableTest, GetMissingKey) {
    ASSERT_TRUE(kvstore::SSTableWriter::write(makeSortedEntries(), sst_path));
    kvstore::SSTableReader reader(sst_path);

    EXPECT_FALSE(reader.get("zebra").has_value());
}

TEST_F(SSTableTest, GetFirstKey) {
    ASSERT_TRUE(kvstore::SSTableWriter::write(makeSortedEntries(), sst_path));
    kvstore::SSTableReader reader(sst_path);

    auto entry = reader.get("apple");
    ASSERT_TRUE(entry.has_value());
    EXPECT_EQ(entry->value, "red");
}

TEST_F(SSTableTest, GetLastKey) {
    ASSERT_TRUE(kvstore::SSTableWriter::write(makeSortedEntries(), sst_path));
    kvstore::SSTableReader reader(sst_path);

    auto entry = reader.get("date");
    ASSERT_TRUE(entry.has_value());
    EXPECT_EQ(entry->value, "brown");
}

// ── Tombstones ──────────────────────────────────────────────────────────────

TEST_F(SSTableTest, WritesAndReadsTombstones) {
    std::vector<kvstore::SSTableEntry> entries = {
        {"alive", "yes", false},
        {"dead",  "",    true},
    };
    ASSERT_TRUE(kvstore::SSTableWriter::write(entries, sst_path));
    kvstore::SSTableReader reader(sst_path);

    auto dead = reader.get("dead");
    ASSERT_TRUE(dead.has_value());
    EXPECT_TRUE(dead->is_tombstone);

    auto alive = reader.get("alive");
    ASSERT_TRUE(alive.has_value());
    EXPECT_FALSE(alive->is_tombstone);
}

// ── Range Scan ──────────────────────────────────────────────────────────────

TEST_F(SSTableTest, ScanRange) {
    ASSERT_TRUE(kvstore::SSTableWriter::write(makeSortedEntries(), sst_path));
    kvstore::SSTableReader reader(sst_path);

    auto result = reader.scan("banana", "cherry");
    ASSERT_EQ(result.size(), 2u);
    EXPECT_EQ(result[0].key, "banana");
    EXPECT_EQ(result[1].key, "cherry");
}

TEST_F(SSTableTest, ScanFullRange) {
    auto entries = makeSortedEntries();
    ASSERT_TRUE(kvstore::SSTableWriter::write(entries, sst_path));
    kvstore::SSTableReader reader(sst_path);

    auto result = reader.scan("apple", "date");
    EXPECT_EQ(result.size(), entries.size());
}

TEST_F(SSTableTest, ScanNoMatchingKeys) {
    ASSERT_TRUE(kvstore::SSTableWriter::write(makeSortedEntries(), sst_path));
    kvstore::SSTableReader reader(sst_path);

    auto result = reader.scan("zzz", "zzzz");
    EXPECT_TRUE(result.empty());
}

// ── Empty SSTable ───────────────────────────────────────────────────────────

TEST_F(SSTableTest, WriteEmptyEntries) {
    std::vector<kvstore::SSTableEntry> empty;
    ASSERT_TRUE(kvstore::SSTableWriter::write(empty, sst_path));

    kvstore::SSTableReader reader(sst_path);
    EXPECT_TRUE(reader.readAll().empty());
}

// ── File path ───────────────────────────────────────────────────────────────

TEST_F(SSTableTest, GetFilePathReturnsCorrectPath) {
    kvstore::SSTableReader reader(sst_path);
    EXPECT_EQ(reader.getFilePath(), sst_path);
}
