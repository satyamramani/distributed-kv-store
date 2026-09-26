# ── Fetch GoogleTest ──────────────────────────────────────────────────────────
include(FetchContent)

FetchContent_Declare(
    googletest
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG        v1.14.0
)

# Prevent GoogleTest from overriding our compiler/linker settings on Windows
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

include(GoogleTest)

# ── Helper function to add tests ─────────────────────────────────────────────
function(add_kvstore_test TEST_NAME TEST_SOURCE)
    add_executable(${TEST_NAME} ${TEST_SOURCE})
    target_link_libraries(${TEST_NAME}
        PRIVATE
            kvstore_lib
            GTest::gtest
            GTest::gtest_main
    )
    target_compile_options(${TEST_NAME} PRIVATE ${KVSTORE_WARNINGS})
    gtest_discover_tests(${TEST_NAME})
endfunction()

# ── Register test executables ─────────────────────────────────────────────────
add_kvstore_test(kvstore_test kvstore_test.cpp)
add_kvstore_test(wal_test wal_test.cpp)
add_kvstore_test(memtable_test memtable_test.cpp)
add_kvstore_test(sstable_test sstable_test.cpp)
add_kvstore_test(storage_engine_test storage_engine_test.cpp)
add_kvstore_test(consistent_hash_test consistent_hash_test.cpp)
add_kvstore_test(vector_clock_test vector_clock_test.cpp)
add_kvstore_test(merkle_tree_test merkle_tree_test.cpp)
add_kvstore_test(gossip_test gossip_test.cpp)

