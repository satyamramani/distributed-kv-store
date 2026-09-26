#include <gtest/gtest.h>
#include "kvstore/kvstore.h"

class KVStoreTest : public ::testing::Test {
protected:
    kvstore::KVStore store;
};

// ── Put / Get ────────────────────────────────────────────────────────────────

TEST_F(KVStoreTest, PutAndGetBasic) {
    EXPECT_TRUE(store.put("key1", "value1"));
    auto result = store.get("key1");
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(*result, "value1");
}

TEST_F(KVStoreTest, PutOverwriteReturnsFalse) {
    store.put("key1", "value1");
    EXPECT_FALSE(store.put("key1", "value2"));

    auto result = store.get("key1");
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(*result, "value2");
}

TEST_F(KVStoreTest, GetMissingKeyReturnsNullopt) {
    EXPECT_FALSE(store.get("nonexistent").has_value());
}

// ── Delete ───────────────────────────────────────────────────────────────────

TEST_F(KVStoreTest, DeleteExistingKey) {
    store.put("key1", "value1");
    EXPECT_TRUE(store.del("key1"));
    EXPECT_FALSE(store.get("key1").has_value());
}

TEST_F(KVStoreTest, DeleteMissingKeyReturnsFalse) {
    EXPECT_FALSE(store.del("nonexistent"));
}

// ── Size / Empty ─────────────────────────────────────────────────────────────

TEST_F(KVStoreTest, EmptyStoreHasZeroSize) {
    EXPECT_EQ(store.size(), 0u);
    EXPECT_TRUE(store.empty());
}

TEST_F(KVStoreTest, SizeReflectsInsertions) {
    store.put("a", "1");
    store.put("b", "2");
    EXPECT_EQ(store.size(), 2u);
    EXPECT_FALSE(store.empty());
}

// ── Contains ─────────────────────────────────────────────────────────────────

TEST_F(KVStoreTest, ContainsExistingKey) {
    store.put("key1", "value1");
    EXPECT_TRUE(store.contains("key1"));
}

TEST_F(KVStoreTest, ContainsMissingKey) {
    EXPECT_FALSE(store.contains("nonexistent"));
}

// ── Clear ────────────────────────────────────────────────────────────────────

TEST_F(KVStoreTest, ClearRemovesAllEntries) {
    store.put("a", "1");
    store.put("b", "2");
    store.clear();
    EXPECT_TRUE(store.empty());
    EXPECT_EQ(store.size(), 0u);
    EXPECT_FALSE(store.contains("a"));
}
