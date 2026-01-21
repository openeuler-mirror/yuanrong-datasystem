#pragma once
#include <etcd/SyncClient.hpp>
#include <chrono>
#include <memory>

namespace datasystem {
namespace master{
class EtcdLock {
public:
    explicit EtcdLock(const std::string& key,
                      int ttl = 10,
                      const std::string& endpoints = "http://127.0.0.1:2379")
        : key_(key), ttl_(ttl) {
        auto& c = getClient(endpoints);
        lock_key_ = c.lock(key_, ttl_).lock_key();
    }

    ~EtcdLock() {
        getClient().unlock(lock_key_);
    }

    EtcdLock(const EtcdLock&)            = delete;
    EtcdLock& operator=(const EtcdLock&) = delete;

private:
    static etcd::SyncClient& getClient(const std::string& endpoints = "http://127.0.0.1:2379") {
        static etcd::SyncClient client(endpoints);
        return client;
    }

    std::string key_;
    std::string lock_key_;
    int ttl_;
};
}
}