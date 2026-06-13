#pragma once
#include <string>
#include <vector>
#include <atomic>

struct StubKVClient {
    std::atomic<int> setCount{0};
    std::atomic<int> getCount{0};
    std::atomic<int> delCount{0};
    std::atomic<int> createCount{0};
    std::atomic<int> msetCount{0};
    std::atomic<int> mgetCount{0};
    std::atomic<bool> failSets{false};
    std::atomic<bool> failGets{false};

    bool Set(const std::string &key, const std::string &data) {
        if (failSets) return false;
        setCount++;
        return true;
    }

    bool Get(const std::string &key, std::string &out) {
        if (failGets) return false;
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

    bool CreateAndSetRaw(const std::string &key, uint64_t size, const std::string &data) {
        if (failSets) return false;
        createCount++;
        setCount++;
        return true;
    }

    bool Del(const std::vector<std::string> &keys) {
        delCount += static_cast<int>(keys.size());
        return true;
    }

    bool MSet(const std::vector<std::string> &keys, const std::string &data) {
        if (failSets) return false;
        msetCount += static_cast<int>(keys.size());
        return true;
    }

    bool MGet(const std::vector<std::string> &keys, std::vector<std::string> &out) {
        if (failGets) return false;
        mgetCount += static_cast<int>(keys.size());
        out.clear();
        out.resize(keys.size(), "data");
        return true;
    }

    void Reset() {
        setCount = 0;
        getCount = 0;
        delCount = 0;
        createCount = 0;
        msetCount = 0;
        mgetCount = 0;
    }
};
