#pragma once
#include <string>
#include <vector>
#include <atomic>

struct StubKVClient {
    std::atomic<int> setCount{0};
    std::atomic<int> getCount{0};
    std::atomic<int> delCount{0};
    std::atomic<int> createCount{0};
    std::atomic<bool> failSets{false};

    bool Set(const std::string &key, const std::string &data) {
        if (failSets) return false;
        setCount++;
        return true;
    }

    bool Get(const std::string &key, std::string &out) {
        getCount++;
        out = "data";
        return true;
    }

    bool CreateAndSet(const std::string &key, uint64_t size, const std::string &data) {
        if (failSets) return false;
        createCount++;
        setCount++;
        return true;
    }

    bool Del(const std::vector<std::string> &keys) {
        delCount += static_cast<int>(keys.size());
        return true;
    }

    void Reset() {
        setCount = 0;
        getCount = 0;
        delCount = 0;
        createCount = 0;
    }
};
