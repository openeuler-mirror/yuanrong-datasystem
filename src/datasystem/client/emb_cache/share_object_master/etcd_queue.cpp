#include "etcd_queue.h"
#include <chrono>
 
using namespace std::chrono_literals;
 
EtcdMPMCQueue::EtcdMPMCQueue(const std::string &endpoints,
                               const std::string &queue_prefix,
                               size_t prefetch)
    : etcd_(endpoints), prefix_(queue_prefix), prefetch_(prefetch)
{
    watcherPtr_ = std::make_unique<etcd::Watcher>(etcd_, prefix_, from_index_,
                [&](etcd::Response resp) {
                    if (resp.error_code() || resp.events().empty())
                        return;
                    for (auto const &ev : resp.events()) {
                        if (ev.event_type() != etcd::Event::EventType::PUT)
                            return;
                        std::string key = ev.kv().key();
                        std::string owner_key = key;
                        etcdv3::Transaction txn;
                        bool tresp = true;
                        if (ev.kv().as_string() != EOFMSG) {
                            txn.setup_compare_or_delete(owner_key, 0, owner_key);
                            tresp = etcd_.txn(txn).get().is_ok();
                        }
                        if (tresp) {
                            std::lock_guard<std::mutex> lk(mu_);
                            local_queue_.emplace(key, ev.kv().as_string());
                            cv_.notify_one();
                        }
                    }
                    from_index_ = resp.events().back().kv().modified_index() + 1;
                },
                true);
}
 
EtcdMPMCQueue::~EtcdMPMCQueue()
{
    cv_.notify_all();
}
 
datasystem::Status EtcdMPMCQueue::push(const std::string &key, const std::string &value)
{
    etcdv3::Transaction txn;
    txn.setup_compare_and_swap(prefix_ + "/" + key, 0, value);

    auto resp = etcd_.txn(txn).get();
    if (!resp.is_ok())
        return datasystem::Status(datasystem::StatusCode::K_IO_ERROR, "etcd push failed: " + resp.error_message());
    return datasystem::Status::OK();
}

datasystem::Status EtcdMPMCQueue::pushEOF()
{
    auto resp = etcd_.put(prefix_, EOFMSG).get();
    if (!resp.is_ok())
        return datasystem::Status(datasystem::StatusCode::K_IO_ERROR, "etcd push failed: " + resp.error_message());
    return datasystem::Status::OK();
}

std::unique_ptr<EtcdMPMCQueue::Message> EtcdMPMCQueue::pop()
{
    std::unique_lock<std::mutex> lk(mu_);
    cv_.wait(lk, [this] { return !local_queue_.empty();});
    if (local_queue_.front().value == EOFMSG) {
        local_queue_.pop();
        return nullptr;
    }

    auto msg = std::make_unique<Message>(local_queue_.front());
    local_queue_.pop();
    return msg;
}