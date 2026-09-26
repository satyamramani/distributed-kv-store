#include <iostream>
#include "kvstore/kvstore.h"

int main() {
    kvstore::KVStore store;

    store.put("hello", "world");
    store.put("foo", "bar");

    std::cout << "Store size: " << store.size() << "\n";

    if (auto val = store.get("hello"); val.has_value()) {
        std::cout << "hello => " << *val << "\n";
    }

    if (auto val = store.get("missing"); !val.has_value()) {
        std::cout << "Key 'missing' not found (expected)\n";
    }

    store.del("foo");
    std::cout << "After deleting 'foo', size: " << store.size() << "\n";

    return 0;
}
