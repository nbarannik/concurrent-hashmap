#pragma once

#include <mutex>
#include <functional>
#include <list>
#include <stdexcept>
#include <atomic>

template <class K, class V, class Hash = std::hash<K>>
class ConcurrentHashMap {
    using ListIterator = typename std::list<std::pair<K, V>>::iterator;
    using ConstListIterator = typename std::list<std::pair<K, V>>::const_iterator;
    static constexpr double kLoadFactor = 0.9;
    static constexpr size_t kMutexesCount = 63;

public:
    ConcurrentHashMap(const Hash& hasher = Hash()) : ConcurrentHashMap(kUndefinedSize, hasher) {
    }

    explicit ConcurrentHashMap(int expected_size, const Hash& hasher = Hash())
        : ConcurrentHashMap(expected_size, kDefaultConcurrencyLevel, hasher) {
    }

    ConcurrentHashMap(int, int, const Hash& hasher = Hash())
        : mutexes_(kMutexesCount),
          data_(kMutexesCount),
          mutexes_count_(kMutexesCount),
          size_(0),
          capacity_(kMutexesCount),
          hasher_(hasher) {
    }

    bool Insert(const K& key, const V& value) {
        if (static_cast<double>(capacity_.load()) * kLoadFactor <=
            static_cast<double>(size_.load())) {
            rehash_mutex_.lock();
            if (static_cast<double>(capacity_.load()) * kLoadFactor <=
                static_cast<double>(size_.load())) {
                Rehash();
            }
            rehash_mutex_.unlock();
        }

        size_t mutex_id = hasher_(key) % mutexes_count_;
        std::lock_guard<std::mutex> lock(mutexes_[mutex_id]);
        size_t data_id = hasher_(key) % capacity_.load();

        if (ListIterator it = FindInList(data_id, key); it != data_[data_id].end()) {
            return false;
        }
        data_[data_id].push_back({key, value});
        ++size_;
        return true;
    }

    bool Erase(const K& key) {
        size_t mutex_id = hasher_(key) % mutexes_count_;
        std::lock_guard<std::mutex> lock(mutexes_[mutex_id]);
        size_t data_id = hasher_(key) % capacity_.load();
        if (ListIterator it = FindInList(data_id, key); it != data_[data_id].end()) {
            data_[data_id].erase(it);
            --size_;
            return true;
        }
        return false;
    }

    void Clear() {
        rehash_mutex_.lock();
        for (size_t i = 0; i < mutexes_count_; ++i) {
            mutexes_[i].lock();
        }
        data_.assign(kMutexesCount, {});
        size_ = 0;
        capacity_ = kMutexesCount;
        for (size_t i = mutexes_count_ - 1; i > 0; --i) {
            mutexes_[i].unlock();
        }
        mutexes_[0].unlock();
        rehash_mutex_.unlock();
    }

    std::pair<bool, V> Find(const K& key) const {
        size_t mutex_id = hasher_(key) % mutexes_count_;
        std::lock_guard<std::mutex> lock(mutexes_[mutex_id]);
        size_t data_id = hasher_(key) % capacity_.load();
        if (ConstListIterator it = FindInList(data_id, key); it != data_[data_id].end()) {
            return {true, it->second};
        }
        return {false, V{}};
    }

    const V At(const K& key) const {
        size_t mutex_id = hasher_(key) % mutexes_count_;
        std::lock_guard<std::mutex> lock(mutexes_[mutex_id]);
        size_t data_id = hasher_(key) % capacity_.load();
        if (ConstListIterator it = FindInList(data_id, key); it != data_[data_id].end()) {
            return it->second;
        }
        throw std::out_of_range("");
    }

    size_t Size() const {
        return size_;
    }

    static const int kDefaultConcurrencyLevel;
    static const int kUndefinedSize;

private:
    void Rehash() {
        for (size_t i = 0; i < mutexes_count_; ++i) {
            mutexes_[i].lock();
        }

        capacity_.store(capacity_ << 2);
        std::vector<std::list<std::pair<K, V>>> new_data(capacity_);

        for (const auto& list : data_) {
            for (const auto& i : list) {
                size_t id = hasher_(i.first) % capacity_;
                new_data[id].push_back(i);
            }
        }
        data_ = new_data;

        for (size_t i = mutexes_count_ - 1; i > 0; --i) {
            mutexes_[i].unlock();
        }
        mutexes_[0].unlock();
    }

    ListIterator FindInList(size_t id, const K& key) {
        for (auto it = data_[id].begin(); it != data_[id].end(); ++it) {
            if (it->first == key) {
                return it;
            }
        }
        return data_[id].end();
    }

    ConstListIterator FindInList(size_t id, const K& key) const {
        for (auto it = data_[id].begin(); it != data_[id].end(); ++it) {
            if (it->first == key) {
                return it;
            }
        }
        return data_[id].end();
    }

    mutable std::vector<std::mutex> mutexes_;
    std::vector<std::list<std::pair<K, V>>> data_;
    const size_t mutexes_count_;
    std::atomic<size_t> size_;
    std::atomic<size_t> capacity_;
    std::mutex rehash_mutex_;
    const Hash hasher_;
};

template <class K, class V, class Hash>
const int ConcurrentHashMap<K, V, Hash>::kDefaultConcurrencyLevel = 8;

template <class K, class V, class Hash>
const int ConcurrentHashMap<K, V, Hash>::kUndefinedSize = -1;
