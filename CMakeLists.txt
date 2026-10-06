cmake_minimum_required(VERSION 3.16)
project(kv-store
    VERSION 0.1.0
    DESCRIPTION "A key-value store implementation"
    LANGUAGES CXX
)

# ── C++ Standard ─────────────────────────────────────────────────────────────
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# ── Compiler Warnings (scoped to our targets, not third-party) ────────────────
set(KVSTORE_WARNINGS
    -Wall
    -Wextra
    -Wpedantic
    -Werror
    -Wshadow
    -Wconversion
    -Wsign-conversion
    -Wnull-dereference
    -Wdouble-promotion
    -Wformat=2
)

# ── Build Type Defaults ──────────────────────────────────────────────────────
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Debug CACHE STRING "Build type" FORCE)
endif()

# ── Output Directories ───────────────────────────────────────────────────────
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)

# ── Export compile_commands.json for editors/LSP ─────────────────────────────
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# ── Source Library ────────────────────────────────────────────────────────────
add_library(kvstore_lib
    src/kvstore.cpp
    src/wal.cpp
    src/memtable.cpp
    src/sstable.cpp
    src/storage_engine.cpp
    src/consistent_hash.cpp
    src/vector_clock.cpp
    src/merkle_tree.cpp
    src/gossip.cpp
)

target_include_directories(kvstore_lib
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/src
)

target_compile_options(kvstore_lib PRIVATE ${KVSTORE_WARNINGS})

# ── Main Executable ──────────────────────────────────────────────────────────
add_executable(kvstore src/main.cpp)
target_link_libraries(kvstore PRIVATE kvstore_lib)
target_compile_options(kvstore PRIVATE ${KVSTORE_WARNINGS})

# ── Native HTTP API Server ───────────────────────────────────────────────────
add_executable(kvstore_server src/server.cpp)
target_link_libraries(kvstore_server PRIVATE kvstore_lib)
target_compile_options(kvstore_server PRIVATE ${KVSTORE_WARNINGS})

# ── Testing ──────────────────────────────────────────────────────────────────
option(BUILD_TESTS "Build unit tests" ON)

if(BUILD_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()
