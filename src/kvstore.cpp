#include "kvstore/kvstore.h"

namespace kvstore {

bool KVStore::put(const std::string& key, const std::string& value) {
    auto [it, inserted] = store_.insert_or_assign(key, value);
    return inserted;
}

std::optional<std::string> KVStore::get(const std::string& key) const {
    auto it = store_.find(key);
    if (it == store_.end()) {
        return std::nullopt;
    }
    return it->second;
}

bool KVStore::del(const std::string& key) {
    return store_.erase(key) > 0;
}

std::size_t KVStore::size() const {
    return store_.size();
}

bool KVStore::empty() const {
    return store_.empty();
}

void KVStore::clear() {
    store_.clear();
}

bool KVStore::contains(const std::string& key) const {
    return store_.find(key) != store_.end();
}

}  // namespace kvstore
