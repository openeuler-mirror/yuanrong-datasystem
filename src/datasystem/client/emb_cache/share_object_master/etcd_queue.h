#pragma once
#include <etcd/Client.hpp>
#include <etcd/v3/Transaction.hpp>
#include <etcd/Watcher.hpp>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <queue>
#include <atomic>
#include <optional>
#include "datasystem/common/util/status_helper.h"
 
class EtcdMPMCQueue
{
public:
    struct Message
    {
        std::string key;
        std::string value;
 
        Message(const std::string& key, const std::string& value) : key(key), value(value){}
        Message(Message &msg) : key(msg.key), value(msg.value){}
    };
 
    EtcdMPMCQueue(const std::string &endpoints,
                  const std::string &queue_prefix = "/queue",
                  size_t prefetch = 128);
 
    ~EtcdMPMCQueue();
 
    // 生产者：线程安全，阻塞直到 etcd 返回 OK
    datasystem::Status push(const std::string &key, const std::string &value);
 
    datasystem::Status pushEOF(const std::string &key);

    // 消费者：阻塞直到拿到一条消息；返回空代表永久失败
    std::unique_ptr<Message> pop();
 
private:
    etcd::Client etcd_;
    const std::string prefix_;
    const size_t prefetch_;

    std::mutex mu_;
    std::condition_variable cv_;
    std::queue<Message> local_queue_;
    std::unique_ptr<etcd::Watcher> watcherPtr_;
    int64_t from_index_ = 0;
    const std::string EOFMSG = "eof";
};