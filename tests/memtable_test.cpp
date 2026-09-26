#include <gtest/gtest.h>

#include "kvstore/memtable.h"

class MemTableTest : public ::testing::Test {
protected:
    kvstore::MemTable table{1024};  // 1 KB threshold for easier testing
};

// ── Put & Get ───────────────────────────────────────────────────────────────

TEST_F(MemTableTest, PutAndGetBasic) {
    table.put("key1", "value1");
    auto entry = table.get("key1");
    ASSERT_TRUE(entry.has_value());
    EXPECT_EQ(entry->key, "key1");
    EXPECT_EQ(entry->value, "value1");
    EXPECT_FALSE(entry->is_tombstone);
}

TEST_F(MemTableTest, PutOverwritesValue) {
    table.put("key1", "old");
    table.put("key1", "new");
    auto entry = table.get("key1");
    ASSERT_TRUE(entry.has_value());
    EXPECT_EQ(entry->value, "new");
}

TEST_F(MemTableTest, GetMissingKeyReturnsNullopt) {
    EXPECT_FALSE(table.get("nope").has_value());
}

TEST_F(MemTableTest, PutMultipleKeys) {
    table.put("c", "3");
    table.put("a", "1");
    table.put("b", "2");

    EXPECT_EQ(table.get("a")->value, "1");
    EXPECT_EQ(table.get("b")->value, "2");
    EXPECT_EQ(table.get("c")->value, "3");
}

// ── Remove (Tombstones) ─────────────────────────────────────────────────────

TEST_F(MemTableTest, RemoveMarksTombstone) {
    table.put("key1", "value1");
    table.remove("key1");

    auto entry = table.get("key1");
    ASSERT_TRUE(entry.has_value());
    EXPECT_TRUE(entry->is_tombstone);
}

TEST_F(MemTableTest, RemoveNonexistentKeyCreatesTombstone) {
    table.remove("ghost");
    auto entry = table.get("ghost");
    ASSERT_TRUE(entry.has_value());
    EXPECT_TRUE(entry->is_tombstone);
}

TEST_F(MemTableTest, PutAfterRemoveOverwritesTombstone) {
    table.put("key1", "v1");
    table.remove("key1");
    table.put("key1", "v2");

    auto entry = table.get("key1");
    ASSERT_TRUE(entry.has_value());
    EXPECT_FALSE(entry->is_tombstone);
    EXPECT_EQ(entry->value, "v2");
}

// ── Size & Empty ────────────────────────────────────────────────────────────

TEST_F(MemTableTest, EmptyOnConstruction) {
    kvstore::MemTable fresh;
    EXPECT_TRUE(fresh.empty());
    EXPECT_EQ(fresh.size(), 0u);
    EXPECT_EQ(fresh.sizeBytes(), 0u);
}

TEST_F(MemTableTest, SizeReflectsEntries) {
    table.put("a", "1");
    table.put("b", "2");
    EXPECT_EQ(table.size(), 2u);
    EXPECT_FALSE(table.empty());
}

TEST_F(MemTableTest, SizeBytesGrowsWithData) {
    table.put("key", "value");
    EXPECT_GT(table.sizeBytes(), 0u);
}

// ── Contains ────────────────────────────────────────────────────────────────

TEST_F(MemTableTest, ContainsExistingKey) {
    table.put("key1", "v");
    EXPECT_TRUE(table.contains("key1"));
}

TEST_F(MemTableTest, ContainsMissingKey) {
    EXPECT_FALSE(table.contains("missing"));
}

TEST_F(MemTableTest, ContainsTombstone) {
    table.put("key1", "v");
    table.remove("key1");
    // Tombstones still "exist" in the MemTable
    EXPECT_TRUE(table.contains("key1"));
}

// ── Flush threshold ─────────────────────────────────────────────────────────

TEST_F(MemTableTest, ShouldFlushWhenThresholdExceeded) {
    // table threshold is 1024 bytes
    std::string big_value(512, 'X');
    table.put("key1", big_value);
    table.put("key2", big_value);
    table.put("key3", big_value);  // should push past 1 KB

    EXPECT_TRUE(table.shouldFlush());
}

TEST_F(MemTableTest, ShouldNotFlushBelowThreshold) {
    table.put("a", "1");
    EXPECT_FALSE(table.shouldFlush());
}

// ── getEntries (sorted output) ──────────────────────────────────────────────

TEST_F(MemTableTest, GetEntriesReturnsSorted) {
    table.put("c", "3");
    table.put("a", "1");
    table.put("b", "2");

    auto entries = table.getEntries();
    ASSERT_EQ(entries.size(), 3u);
    EXPECT_EQ(entries[0].key, "a");
    EXPECT_EQ(entries[1].key, "b");
    EXPECT_EQ(entries[2].key, "c");
}

TEST_F(MemTableTest, GetEntriesIncludesTombstones) {
    table.put("a", "1");
    table.remove("b");

    auto entries = table.getEntries();
    ASSERT_EQ(entries.size(), 2u);

    // Find the tombstone
    bool found_tombstone = false;
    for (const auto& e : entries) {
        if (e.key == "b") {
            EXPECT_TRUE(e.is_tombstone);
            found_tombstone = true;
        }
    }
    EXPECT_TRUE(found_tombstone);
}

// ── Clear ───────────────────────────────────────────────────────────────────

TEST_F(MemTableTest, ClearResetsEverything) {
    table.put("a", "1");
    table.put("b", "2");
    table.clear();

    EXPECT_TRUE(table.empty());
    EXPECT_EQ(table.size(), 0u);
    EXPECT_EQ(table.sizeBytes(), 0u);
    EXPECT_FALSE(table.contains("a"));
}

// ── Timestamps ──────────────────────────────────────────────────────────────

TEST_F(MemTableTest, EntriesHaveTimestamps) {
    table.put("key1", "value1");
    auto entry = table.get("key1");
    ASSERT_TRUE(entry.has_value());
    EXPECT_GT(entry->timestamp, 0u);
}
