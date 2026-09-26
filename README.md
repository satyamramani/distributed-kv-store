# kv-store

A simple in-memory key-value store implemented in C++17.

## Project Structure

```
kv-store/
├── CMakeLists.txt          # Root build configuration
├── include/
│   └── kvstore/
│       └── kvstore.h       # Public headers
├── src/
│   ├── kvstore.cpp         # Library implementation
│   └── main.cpp            # Main executable
├── tests/
│   ├── CMakeLists.txt      # Test build configuration
│   └── kvstore_test.cpp    # Unit tests (GoogleTest)
├── .clang-format           # Code formatting rules
├── .clang-tidy             # Static analysis rules
└── .gitignore
```

## Prerequisites

- **CMake** ≥ 3.16
- **C++17** compatible compiler (GCC ≥ 7, Clang ≥ 5, MSVC ≥ 19.14)
- **Internet access** (for automatic GoogleTest download on first build)

## Build

```bash
# Configure
cmake -B build -DCMAKE_BUILD_TYPE=Debug

# Build
cmake --build build

# Run
./build/bin/kvstore
```

## Tests

```bash
# Build & run all tests
cmake --build build
ctest --test-dir build --output-on-failure

# Or run the test binary directly
./build/bin/kvstore_test
```

## Adding New Tests

1. Create a new test file in `tests/`, e.g. `tests/my_feature_test.cpp`
2. Add to `tests/CMakeLists.txt`:
   ```cmake
   add_kvstore_test(my_feature_test my_feature_test.cpp)
   ```
3. Rebuild — CTest will automatically discover the new tests.

## Code Style

```bash
# Format all source files
find src include tests -name '*.cpp' -o -name '*.h' | xargs clang-format -i

# Run static analysis
clang-tidy src/*.cpp -- -Iinclude -std=c++17
```
