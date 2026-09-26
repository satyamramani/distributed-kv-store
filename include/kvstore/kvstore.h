#ifndef KVSTORE_KVSTORE_H
#define KVSTORE_KVSTORE_H

#include <optional>
#include <string>
#include <unordered_map>

namespace kvstore {

/// A simple in-memory key-value store.
class KVStore {
public:
    KVStore() = default;
    ~KVStore() = default;

    // Non-copyable, movable
    KVStore(const KVStore&) = delete;
    KVStore& operator=(const KVStore&) = delete;
    KVStore(KVStore&&) = default;
    KVStore& operator=(KVStore&&) = default;

    /// Insert or update a key-value pair. Returns true if the key was newly inserted.
    bool put(const std::string& key, const std::string& value);

    /// Retrieve the value for a given key. Returns std::nullopt if not found.
    std::optional<std::string> get(const std::string& key) const;

    /// Delete a key-value pair. Returns true if the key existed.
    bool del(const std::string& key);

    /// Returns the number of stored key-value pairs.
    std::size_t size() const;

    /// Returns true if the store contains no entries.
    bool empty() const;

    /// Removes all key-value pairs.
    void clear();

    /// Returns true if the key exists in the store.
    bool contains(const std::string& key) const;

private:
    std::unordered_map<std::string, std::string> store_;
};

}  // namespace kvstore

#endif  // KVSTORE_KVSTORE_H
