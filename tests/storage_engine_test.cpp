#include <gtest/gtest.h>

#include <filesystem>
#include <string>

#include "kvstore/storage_engine.h"

namespace fs = std::filesystem;

class StorageEngineTest : public ::testing::Test {
protected:
    std::string test_dir;

    void SetUp() override {
        test_dir = fs::temp_directory_path() / "kvstore_engine_test";
        fs::remove_all(test_dir);
        fs::create_directories(test_dir);
    }

    void TearDown() override { fs::remove_all(test_dir); }
};

// ── Basic put / get ─────────────────────────────────────────────────────────

TEST_F(StorageEngineTest, PutAndGet) {
    kvstore::StorageEngine engine(test_dir);
    engine.put("key1", "value1");

    auto result = engine.get("key1");
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(*result, "value1");
}

TEST_F(StorageEngineTest, GetMissingKey) {
    kvstore::StorageEngine engine(test_dir);
    EXPECT_FALSE(engine.get("nope").has_value());
}

TEST_F(StorageEngineTest, PutOverwrite) {
    kvstore::StorageEngine engine(test_dir);
    engine.put("key1", "old");
    engine.put("key1", "new");

    EXPECT_EQ(*engine.get("key1"), "new");
}

// ── Delete ──────────────────────────────────────────────────────────────────

TEST_F(StorageEngineTest, DeleteExistingKey) {
    kvstore::StorageEngine engine(test_dir);
    engine.put("key1", "value1");
    EXPECT_TRUE(engine.del("key1"));
    EXPECT_FALSE(engine.get("key1").has_value());
}

TEST_F(StorageEngineTest, DeleteMissingKey) {
    kvstore::StorageEngine engine(test_dir);
    EXPECT_FALSE(engine.del("nope"));
}

// ── Write path: MemTable → SSTable flush ────────────────────────────────────

TEST_F(StorageEngineTest, FlushWritesDataToSSTable) {
    kvstore::StorageEngine engine(test_dir);
    engine.put("key1", "value1");
    engine.put("key2", "value2");

    engine.flush();

    // Data should still be readable after flush.
    EXPECT_EQ(*engine.get("key1"), "value1");
    EXPECT_EQ(*engine.get("key2"), "value2");
}

TEST_F(StorageEngineTest, DataSurvivesMultipleFlushes) {
    kvstore::StorageEngine engine(test_dir);
    engine.put("a", "1");
    engine.flush();
    engine.put("b", "2");
    engine.flush();

    EXPECT_EQ(*engine.get("a"), "1");
    EXPECT_EQ(*engine.get("b"), "2");
}

// ── Read path: MemTable first, then SSTables ────────────────────────────────

TEST_F(StorageEngineTest, MemTableOverridesSSTable) {
    kvstore::StorageEngine engine(test_dir);
    engine.put("key1", "old");
    engine.flush();
    engine.put("key1", "new");  // still in MemTable

    EXPECT_EQ(*engine.get("key1"), "new");
}

TEST_F(StorageEngineTest, DeleteInMemTableOverridesSSTable) {
    kvstore::StorageEngine engine(test_dir);
    engine.put("key1", "value1");
    engine.flush();
    engine.del("key1");  // tombstone in MemTable

    EXPECT_FALSE(engine.get("key1").has_value());
}

// ── Crash recovery (WAL replay) ─────────────────────────────────────────────

TEST_F(StorageEngineTest, RecoveryFromWAL) {
    // Write data and let the engine go out of scope (simulated crash).
    {
        kvstore::StorageEngine engine(test_dir);
        engine.put("survive", "crash");
        // NOT flushing — data is only in WAL + MemTable.
    }
    // New engine should replay the WAL.
    {
        kvstore::StorageEngine engine(test_dir);
        auto result = engine.get("survive");
        ASSERT_TRUE(result.has_value());
        EXPECT_EQ(*result, "crash");
    }
}

TEST_F(StorageEngineTest, RecoveryIncludesDeletes) {
    {
        kvstore::StorageEngine engine(test_dir);
        engine.put("key1", "value1");
        engine.del("key1");
    }
    {
        kvstore::StorageEngine engine(test_dir);
        EXPECT_FALSE(engine.get("key1").has_value());
    }
}

// ── Multiple keys ───────────────────────────────────────────────────────────

TEST_F(StorageEngineTest, MultipleKeys) {
    kvstore::StorageEngine engine(test_dir);
    for (int i = 0; i < 100; ++i) {
        engine.put("key" + std::to_string(i), "val" + std::to_string(i));
    }
    for (int i = 0; i < 100; ++i) {
        auto val = engine.get("key" + std::to_string(i));
        ASSERT_TRUE(val.has_value()) << "Missing key" << i;
        EXPECT_EQ(*val, "val" + std::to_string(i));
    }
}
