/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2022. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Description: Message mailbox.
 */
#ifndef DATASYSTEM_COMMON_RPC_ZMQ_MSG_QUE_H
#define DATASYSTEM_COMMON_RPC_ZMQ_MSG_QUE_H

#include <sys/eventfd.h>

#include <thread>
#include <utility>

#include "datasystem/common/rpc/rpc_options.h"
#include "datasystem/common/rpc/zmq/zmq_common.h"
#include "datasystem/common/util/locks.h"
#include "datasystem/common/util/queue/queue.h"
#include "datasystem/common/util/thread.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/common/util/uuid_generator.h"
#include "datasystem/common/util/wait_post.h"

namespace datasystem {
static constexpr int MAX_MSG_QUE_BUCKETS = 13;  // Any prime number will do
static constexpr int ZMQ_PERF_LOG_LEVEL = 10;
/**
 * Hash a message queue to a bucket
 * @param qID
 * @return bucket id
 */
inline size_t QueueId2Bucket(const std::string &qID)
{
    auto hval = std::hash<std::string>{}(qID);
    return hval % datasystem::MAX_MSG_QUE_BUCKETS;
}

template <typename W, typename R>
class MsgQueMgr;

template <typename W, typename R>
class MsgQueRef;

/**
 * @brief MsgQue is a message queue for passing messages, mainly rpc messages.
 * It consists of one outbound Queue<W> and one inbound Queue<R>.
 * @note It is managed by MsgQueMgr. Caller sends and receives messages
 * using MsgQueRef rather than MsgQue directly.
 */
template <typename W, typename R>
class MsgQue {
public:
    typedef std::pair<std::string, R> InBoundMsg;
    typedef std::pair<std::string, W> OutBoundMsg;
    explicit MsgQue(const std::shared_ptr<MsgQueMgr<W, R>> &mgr, const RpcOptions &opts, const std::string &suffix = "")
        : outBoundQueId_(0),
          mgr_(mgr),
          opts_(opts),
          id_(GetStringUuid().substr(0, ZMQ_SHORT_UUID) + suffix),
          outBound_(nullptr),
          inBound_(std::make_unique<Queue<InBoundMsg>>(opts_.hwm_)),
          next_(nullptr),
          prev_(nullptr)
    {
    }
    ~MsgQue() = default;

    /**
     * @brief Reset the message queue.
     * @param[in] opts Reset options.
     * @param[in] suffix Set id of message queue with suffix.
     * @return Status of the call.
     */
    Status Reset(const RpcOptions &opts)
    {
        if (opts_.hwm_ != opts.hwm_) {
            inBound_->Reset(opts.hwm_);
        }
        opts_ = opts;
        return Status::OK();
    }

    /**
     * @brief Return the ID of a message queue.
     * @return Id of the message queue.
     */
    std::string GetId() const
    {
        return id_;
    }

    /**
     * @brief Poll if the inbound queue has any message.
     * @param[in] timeout The timeout for polling the receiving queue.
     * @return Status of the call.
     */
    Status PollRecv(int timeout) const
    {
        return inBound_->PollRecv(timeout);
    }

    /**
     * @brief Send a message.
     * @param[in/out] ele Element to be moved onto que.
     * @param[in] timeout (in millisecond).
     * @return Status of the call.
     */
    Status SendMsg(W &ele, int timeout)
    {
        CHECK_FAIL_RETURN_STATUS(outBound_ != nullptr, K_RUNTIME_ERROR, "Outbound queue not set up");
        Queue<OutBoundMsg> *que = outBound_.get();
        auto p = std::make_pair(id_, std::move(ele));
        RETURN_IF_NOT_OK(que->Send(std::move(p), timeout));
        auto mgr = mgr_.lock();
        mgr->Ping(outBoundQueId_);
        return Status::OK();
    }

    /**
     * @brief Send a message using the default timeout value of the message queue.
     * @param[in] ele The message to be sent.
     * @return Status of the call.
     */
    Status SendMsg(W &ele)
    {
        return SendMsg(ele, opts_.GetTimeout());
    }

    /**
     * @brief Receive a message.
     * @param[out] ele Element to receive the message in.
     * @param[in] timeout (in millisecond).
     * @return Status of the call.
     */
    Status ReceiveMsg(R &ele, int timeout)
    {
        Queue<InBoundMsg> *que = inBound_.get();
        do {
            InBoundMsg p;
            RETURN_IF_NOT_OK(que->Recv(p, timeout));
            // Please see the comment of MsgQueMgr::SendMsg. We need to
            // discard previous old message if this msg queue is reused.
            if (p.first != id_) {
                continue;
            } else {
                ele = std::move(p.second);
                break;
            }
        } while (true);
        return Status::OK();
    }

    /**
     * @brief Receive a message using the default timeout value of the message queue.
     * @param[out] ele Element to receive the message.
     * @return Status of the call.
     */
    Status ReceiveMsg(R &ele)
    {
        return ReceiveMsg(ele, opts_.GetTimeout());
    }

    /**
     * @brief Get the Que Mgr object.
     * @return std::shared_ptr<MsgQueMgr<W, R>> The que manager.
     */
    std::shared_ptr<MsgQueMgr<W, R>> GetQueMgr()
    {
        if (mgr_.expired()) {
            return nullptr;
        } else {
            return mgr_.lock();
        }
    }

    /**
     * @brief Return the options the current message queue.
     */
    RpcOptions GetOpts() const
    {
        return opts_;
    }

    /**
     * @brief Update the options of the current message queue.
     * @param[in] opts The options to set.
     */
    void UpdateOpts(const RpcOptions &opts)
    {
        // Only refresh those we are allowed to change.
        opts_.timeout_ = opts.timeout_;
    }

    auto GetOutBoundQueId() const
    {
        return outBoundQueId_;
    }

private:
    friend class MsgQueMgr<W, R>;
    int outBoundQueId_;
    std::weak_ptr<MsgQueMgr<W, R>> mgr_;
    RpcOptions opts_;
    std::string id_;
    std::shared_ptr<Queue<OutBoundMsg>> outBound_;
    std::unique_ptr<Queue<InBoundMsg>> inBound_;
    MsgQue<W, R> *next_;
    MsgQue<W, R> *prev_;
    mutable WriterPrefRWLock inUse_;  // sync with Close() and SendMsg()
    void AssignOutBoundQueue()
    {
        auto queMgr = GetQueMgr();
        if (queMgr) {
            outBoundQueId_ = static_cast<int>(queMgr->GetNextOutBoundQueId());
            outBound_ = queMgr->GetOutBoundQueue(outBoundQueId_);
        }
    }
};
}  // namespace datasystem
/**
 * Support for std::hash<datasystem::MsgQue<W,R>>
 */
namespace std {
template <typename W, typename R>
struct hash<datasystem::MsgQue<W, R>> {
    size_t operator()(const datasystem::MsgQue<W, R> &mQue) const
    {
        return datasystem::QueueId2Bucket(mQue.GetId());
    }
};
}  // namespace std
namespace datasystem {
/**
 * @brief MsgQueMgr manage a set of MsgQue.
 * User should create a MsgQueMgr object for sending/receiving messages.
 */
template <typename W, typename R>
class MsgQueMgr : public std::enable_shared_from_this<MsgQueMgr<W, R>> {
public:
    typedef typename MsgQue<W, R>::InBoundMsg InBoundMsg;
    typedef typename MsgQue<W, R>::OutBoundMsg OutBoundMsg;
    const static int maxCache = 16;
    typedef std::function<Status(const std::string &, W &&)> MsgRoutingFn;
    explicit MsgQueMgr(MsgRoutingFn fn = nullptr, int numOutBoundQue = RPC_NUM_BACKEND)
        : outBoundArraySize(numOutBoundQue),
          interrupt_(false),
          ind_(0),
          maxCacheSz_(maxCache),
          efd_(ZMQ_NO_FILE_FD),
          curInx_(0),
          nextOutBoundQueId_(0),
          outBounds_(outBoundArraySize),
          routeFn_(fn),
          parts_(MAX_MSG_QUE_BUCKETS)
    {
        cache_.reserve(maxCacheSz_);
        for (auto i = 0; i < MAX_MSG_QUE_BUCKETS; ++i) {
            parts_[i] = std::make_shared<PartMsgQ>();
        }
        for (auto i = 0; i < outBoundArraySize; ++i) {
            outBounds_[i].outBound_ = std::make_unique<Queue<OutBoundMsg>>(RPC_HWM_QUEUE);
        }
    }

    ~MsgQueMgr()
    {
        Stop();
    }

    Status Init()
    {
        if (routeFn_ == nullptr) {
            efd_ = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
            CHECK_FAIL_RETURN_STATUS(efd_ > 0, K_RUNTIME_ERROR,
                                     FormatString("MsgQueMgr initialization failed with errno %d", errno));
            const int outBoundQueSz = 2048;
            outBound_ = std::make_shared<Queue<OutBoundMsg>>(outBoundQueSz);
            routeFn_ = [this](const std::string &sender, W &&ele) {
                OutBoundMsg msg = std::make_pair(sender, std::move(ele));
                Status rc;
                do {
                    rc = SendDirect(std::move(msg), RPC_POLL_TIME);
                } while (rc.GetCode() == K_TRY_AGAIN && !interrupt_);
                return rc;
            };
        }
        prefetcher_ = Thread([this]() {
            while (!interrupt_) {
                const auto n = Wait(RPC_POLL_TIME, ind_.load() == 0);
                for (auto i = 0; i < n && !interrupt_; ++i) {
                    PrefetchMsg();
                }
            }
        });
        return Status::OK();
    }

    float GetHWMRatio() const
    {
        float maxHWM = 0.0;
        for (auto &part : parts_) {
            ReadLock plock(&part->mux_);
            if (part->head_ == nullptr) {
                continue;
            }
            auto mQ = part->head_;
            do {
                maxHWM = std::max(maxHWM, mQ->inBound_->GetHWMRatio());
                mQ = mQ->next_;
            } while (mQ != part->head_);
        }
        return maxHWM;
    }

    void Stop()
    {
        interrupt_ = true;
        wp_.Set();
        if (prefetcher_.joinable()) {
            prefetcher_.join();
        }
        WriteLock lock(&mux_);
        allMsgQ_.clear();
        cache_.clear();
        if (efd_ > 0) {
            RETRY_ON_EINTR(close(efd_));
            efd_ = ZMQ_NO_FILE_FD;
        }
    }

    void InterruptAll()
    {
        interrupt_ = true;
        if (prefetcher_.joinable()) {
            prefetcher_.join();
        }
    }

    /**
     * @brief Return an event file descriptor to be used by epoll.
     * If the event file descriptor is readable, user can call GetMsg
     * to retrieve the message.
     */
    int GetEventFd() const
    {
        return efd_;
    }

    /**
     * @brief Create a message queue reference.
     * @note A message queue reference instead of a direct message queue is returned to the caller.
     * @param[out] msgQue The message queue reference to store the created message queue.
     * @param[in] opts The options for the message queue.
     * @param[in] suffix The suffix added to the id.
     * @return Status of the call.
     */
    Status CreateMsgQ(MsgQueRef<W, R> &msgQue, const RpcOptions &opts = RpcOptions(), const std::string &suffix = "")
    {
        std::string altName = GetStringUuid().substr(0, ZMQ_SHORT_UUID) + suffix;
        return CreateMsgQWithName(msgQue, altName, opts);
    }

    Status CreateMsgQWithName(MsgQueRef<W, R> &msgQue, const std::string &altName,
                              const RpcOptions &opts = RpcOptions())
    {
        // Get a cache or create a new one.
        auto msgQ = GetOrCreateMsgQ();
        RETURN_IF_NOT_OK(msgQ->Reset(opts));
        msgQ->id_ = altName;
        // Insert into the repository. It is done by holding the mux_ lock
        // and will be released as soon as it is inserted into the repository.
        {
            WriteLock lock(&mux_);
            auto id = msgQ->GetId();
            auto it = allMsgQ_.insert(std::make_pair(id, msgQ));
            CHECK_FAIL_RETURN_STATUS(it.second, K_DUPLICATED,
                                     FormatString("msgQ with name %s already exists", altName));
        }
        // Next we will chain to one of the partitions. But before that, assign an outbound queue to it.
        msgQ->AssignOutBoundQueue();
        auto idx = std::hash<MsgQue<W, R>>{}(*msgQ);
        parts_[idx]->AppendMsgQ(msgQ.get());
        VLOG(ZMQ_PERF_LOG_LEVEL) << FormatString("msgQ %s is assigned to bucket %d with outbound id %d", msgQ->GetId(),
                                                 idx, msgQ->GetOutBoundQueId());
        msgQue = std::move(MsgQueRef<W, R>(msgQ));
        return Status::OK();
    }

    /**
     * @brief Remove a message queue.
     * @param[out] msgQue Reference to pointer of message que.
     */
    void UnchainMsgQ(MsgQue<W, R> *msgQue)
    {
        if (msgQue == nullptr) {
            return;
        }
        auto idx = std::hash<MsgQue<W, R>>{}(*msgQue);
        parts_[idx]->RemoveMsgQ(msgQue);
    }

    void RemoveMsgQ(MsgQue<W, R> *msgQue, bool cacheMsgQ = true)
    {
        if (msgQue == nullptr) {
            return;
        }
        std::shared_ptr<MsgQue<W, R>> mQ;
        // Step (1). Remove the msg Q from repository
        {
            WriteLock lock(&mux_);
            auto id = msgQue->GetId();
            auto it = allMsgQ_.find(id);
            if (it == allMsgQ_.end()) {
                return;
            }
            // Save a copy first
            mQ = std::move(it->second);
            allMsgQ_.erase(it);
        }
        // Step (2). Take it out from the partition chain
        {
            UnchainMsgQ(mQ.get());
            // Step (3). Synchronize with SendMsg() if it has a reference.
            // Must wait until it is done.
            WriteLock lock(&mux_);
            if (!interrupt_ && cacheMsgQ && cache_.size() < maxCacheSz_) {
                // Save the MsgQue for reuse, and no need to worry about
                // the memory will be deallocated.
                cache_.push_back(std::move(mQ));
            } else {
                lock.UnlockIfLocked();
                mQ->inUse_.WriteLock();
            }
        }
    }

    /**
     * @brief Retrieve a message from one of the MsgQue(s).
     * @note The sender of the message will be stored in qID
     * @param[out] rq The message reference to get the message.
     * @param[out] qID The sender id reference to get the sender id.
     * @param[in] timeout (in millisecond).
     * @return Status of the call.
     */
    Status GetMsg(W &rq, std::string &qID, int timeout)
    {
        OutBoundMsg ele;
        RETURN_IF_NOT_OK(outBound_->Recv(ele, timeout));
        eventfd_t n;
        eventfd_read(efd_, &n);
        if (n > 1) {
            eventfd_write(efd_, n - 1);
        }
        rq = std::move(ele.second);
        qID = std::move(ele.first);
        return Status::OK();
    }

    /**
     * @brief Send a msg to the queue matching the qID
     *      No error is returned if qID is not found.
     * @param[in] qID The id of the queue to send to.
     * @param[in/out] reply The reply or message to send.
     * @param[in] timeout The time out for sending the message.
     * If ZMQ_NOT_TIMEOUT is used calls blocking call to put.
     * @return Status of the call.
     */
    Status SendMsg(const std::string &qID, R &reply, int timeout)
    {
        auto idx = QueueId2Bucket(qID);
        auto &part = parts_[idx];
        VLOG(ZMQ_PERF_LOG_LEVEL) << FormatString("Sending data to msgQ %s in bucket %d", qID, idx);
        ReadLock plock(&part->mux_);
        RETURN_OK_IF_TRUE(part->head_ == nullptr);
        bool found = false;
        auto mQ = part->head_;
        do {
            if (mQ->GetId() == qID) {
                found = true;
                break;
            } else {
                mQ = mQ->next_;
            }
        } while (mQ != part->head_);
        RETURN_OK_IF_TRUE(!found);
        // See the comment in RemoveMsgQ. We must ensure the queue we are accessing
        // is not being deallocated. RemoveMsgQ will take out queue from the
        // partition. So if we can still find the queue in this partition, RemoveMsgQ
        // hasn't started Step (2) yet, and we can lock safely without being blocked.
        ReadLock rlock(&mQ->inUse_);
        // Now we can let go of the partition lock
        plock.UnlockIfLocked();
        // We are not holding partition lock at this point, so we will
        // not be blocked. This can create a timing hole that this
        // msgQ will be closed and recycled for reuse while we
        // are pushing messages for the previous owner. To solve
        // this problem, we need to tag what this reply is for.
        // Consumer must drain the queue.
        Queue<InBoundMsg> *que = mQ->inBound_.get();
        auto p = std::make_pair(qID, std::move(reply));
        RETURN_IF_NOT_OK(que->Send(std::move(p), timeout));
        return Status::OK();
    }

    /**
     * @brief Ping the prefetch thread a message is ready.
     */
    void Ping(int idx)
    {
        if (idx >= outBoundArraySize) {
            return;
        }
        outBounds_[idx].pending_++;
        if (ind_.fetch_add(1) == 0) {
            wp_.Set();
        }
    }

    /**
     * @brief Get the outbound queue given an index i
     * @param[in] index i
     * @return outbound queue
     */
    std::shared_ptr<Queue<OutBoundMsg>> GetOutBoundQueue(int i)
    {
        return (i < outBoundArraySize) ? outBounds_[i].outBound_ : nullptr;
    }

    uint64_t GetNextOutBoundQueId()
    {
        uint64_t val = nextOutBoundQueId_.fetch_add(1);
        return val % outBoundArraySize;
    }

private:
    const int outBoundArraySize;
    std::atomic<bool> interrupt_;
    WaitPost wp_;
    std::atomic<int64_t> ind_;
    WriterPrefRWLock mux_;
    size_t maxCacheSz_;
    int efd_;
    int curInx_;
    std::atomic<uint64_t> nextOutBoundQueId_;
    std::unordered_map<std::string, std::shared_ptr<MsgQue<W, R>>> allMsgQ_;
    std::vector<std::shared_ptr<MsgQue<W, R>>> cache_;
    std::shared_ptr<Queue<OutBoundMsg>> outBound_{ nullptr };
    struct OutBoundQueue {
        OutBoundQueue() : pending_(0)
        {
        }
        ~OutBoundQueue() = default;
        std::atomic<size_t> pending_;
        std::shared_ptr<Queue<OutBoundMsg>> outBound_;
    };
    std::vector<OutBoundQueue> outBounds_;
    MsgRoutingFn routeFn_;
    Thread prefetcher_;
    // All MsgQue is partitioned to reduce lock contention
    struct PartMsgQ {
        PartMsgQ() : pending_(0), head_(nullptr), cur_(nullptr), count_(0)
        {
        }
        ~PartMsgQ() = default;
        std::atomic<size_t> pending_;
        mutable WriterPrefRWLock mux_;
        MsgQue<W, R> *head_;
        MsgQue<W, R> *cur_;
        size_t count_;

        /**
         * @brief All message queues formed a doubly circular linked list.
         * @param[in/out] ptr The pointer to link the head and tail to.
         */
        void AppendMsgQ(MsgQue<W, R> *ptr)
        {
            WriteLock lock(&mux_);
            if (count_ == 0) {
                head_ = ptr;
                ptr->next_ = ptr;
                ptr->prev_ = ptr;
            }
            auto tail = head_->prev_;
            ptr->next_ = head_;
            ptr->prev_ = tail;
            tail->next_ = ptr;
            head_->prev_ = ptr;
            ++count_;
        }

        /**
         * @brief Undo the circular linked list.
         * @param[in/out] ptr The pointer to start unlinking the head and tail.
         */
        void RemoveMsgQ(MsgQue<W, R> *ptr)
        {
            WriteLock lock(&mux_);
            if (count_ == 1) {
                head_ = nullptr;
                cur_ = nullptr;
            } else {
                auto *p = ptr->prev_;
                auto *q = ptr->next_;
                if (cur_ == ptr) {
                    cur_ = p;
                }
                p->next_ = q;
                q->prev_ = p;
                if (head_ == ptr) {
                    head_ = q;
                }
            }
            --count_;
            ptr->next_ = nullptr;
            ptr->prev_ = nullptr;
        }
    };
    std::vector<std::shared_ptr<PartMsgQ>> parts_;

    /**
     * @brief Use internal cv to wait for timeout.
     * @param[in] timeout The time to wait for.
     * @return Number of outstanding requests.
     */
    int64_t Wait(int64_t timeout, bool waitOnCv)
    {
        if (waitOnCv) {
            auto result = wp_.WaitFor(timeout);
            if (interrupt_ || !result) {
                return 0;
            }
            wp_.Clear();
        }
        int64_t ret = ind_.load();
        while (!ind_.compare_exchange_strong(ret, 0)) {
            ret = ind_.load();
        }
        return ret;
    }

    /**
     * @brief A separate thread which do a round robin poll of all message queues
     * and move new message to the final outbound queue for GetMsg to retrieve.
     */
    void PrefetchMsg()
    {
        Timer t;
        for (auto i = 0; i < outBoundArraySize; ++i) {
            auto idx = (curInx_ + i) % outBoundArraySize;
            if (outBounds_[idx].pending_ == 0) {
                continue;
            }
            auto &que = outBounds_[idx].outBound_;
            OutBoundMsg rq;
            Status rc = que->Remove(&rq);
            if (rc.GetCode() == K_TRY_AGAIN) {
                continue;
            }
            --outBounds_[idx].pending_;
            VLOG(ZMQ_PERF_LOG_LEVEL) << FormatString("PrefetchMsg: iteration %d [%.6lf]s", i + 1, t.ElapsedSecond());
            // Next iteration will start from the next bucket
            curInx_ = (idx + 1) % outBoundArraySize;
            routeFn_(rq.first, std::move(rq.second));
            return;
        }
    }

    /**
     * @brief Create a MsgQue
     * @return MsgQue
     */
    std::shared_ptr<MsgQue<W, R>> GetOrCreateMsgQ()
    {
        // If we have cached a MsgQue, reuse it.
        // First check (without getting the lock) if the cache is empty or not.
        // The logic here is to bet most of the time the cache is empty, and
        // we proceed to call make_unique quickly.
        if (!cache_.empty()) {
            WriteLock lock(&mux_);
            // Now we have the lock, check again.
            if (!cache_.empty()) {
                auto msgQ = std::move(cache_.back());
                cache_.pop_back();
                return msgQ;
            }
        }
        return std::make_shared<MsgQue<W, R>>(this->shared_from_this(), RpcOptions());
    }

    /**
     * @brief Push a message directly onto the output queue
     * @note Lock conflict with the PrefetchMsg thread
     * @param[in/out] ele Element to be moved onto que.
     * @param[in] timeout (in millisecond).
     * @return Status of the call.
     */
    Status SendDirect(OutBoundMsg &&p, int timeout)
    {
        RETURN_IF_NOT_OK(outBound_->Send(std::move(p), timeout));
        eventfd_write(efd_, 1);
        return Status::OK();
    }
};

/**
 * @brief A MsgQueRef is like a MsgQue class except the destructor
 * where the underlying resources is properly cleaned up.
 */
template <typename W, typename R>
class MsgQueRef {
public:
    explicit MsgQueRef(std::shared_ptr<MsgQue<W, R>> mQue) : msgQue_(mQue)
    {
    }

    MsgQueRef()
    {
        msgQue_.reset();
    }

    // Copy constructors are disabled because the destructor
    // of this class will close the underlying msgQue_.
    // If we have two identical copies, undesirable behavior
    // will happen if one of them goes out of scope.
    MsgQueRef<W, R>(const MsgQueRef<W, R> &other) = delete;
    MsgQueRef<W, R> &operator=(const MsgQueRef<W, R> &other) = delete;

    // Move constructors are supported. But mainly if one
    // of them is null to begin with.
    MsgQueRef<W, R>(MsgQueRef<W, R> &&other) noexcept : msgQue_(other.msgQue_)
    {
        other.msgQue_.reset();
    }

    MsgQueRef<W, R> &operator=(MsgQueRef<W, R> &&other) noexcept
    {
        if (&other != this) {
            msgQue_ = other.msgQue_;
            other.msgQue_.reset();
        }
        return *this;
    }

    /**
     * @brief Close the message queue.
     */
    void Close(bool cacheMsgQ = true)
    {
        auto ptr = msgQue_.lock();
        if (ptr) {
            auto mgr = ptr->GetQueMgr();
            if (mgr) {
                mgr->RemoveMsgQ(ptr.get(), cacheMsgQ);
            }
            msgQue_.reset();
        }
    }

    virtual ~MsgQueRef()
    {
        Close();
    }

    /**
     * @brief Get the Id of message queue.
     * @return std::string Id of message queue.
     */
    std::string GetId()
    {
        auto ptr = msgQue_.lock();
        if (ptr) {
            return ptr->GetId();
        } else {
            return "";
        }
    }

    /**
     * @brief Send a message.
     * @param[in/out] ele Message to be sent.
     * @param[in] timeout (in millisecond) The time out for sending the message.
     * @return Status of the call.
     */
    Status SendMsg(W &ele, int timeout)
    {
        auto ptr = msgQue_.lock();
        CHECK_FAIL_RETURN_STATUS(ptr != nullptr, K_INVALID, "Not connected to MsgQueMgr");
        return ptr->SendMsg(ele, timeout);
    }

    /**
     * @brief Send a message using the default timeout value of the message queue.
     * @param[in/out] ele The message to send.
     * @return Status of the call.
     */
    Status SendMsg(W &ele)
    {
        auto ptr = msgQue_.lock();
        CHECK_FAIL_RETURN_STATUS(ptr != nullptr, K_INVALID, "Not connected to MsgQueMgr");
        return ptr->SendMsg(ele);
    }

    /**
     * @brief Send a message using zmq send flag.
     * @param[in/out] ele Message to be sent.
     * @param[in] flags The send flags, as of now only ZmqSendFlags::DONTWAIT is supported.
     * @return Status of the call.
     */
    Status SendMsg(W &ele, ZmqSendFlags flags)
    {
        auto ptr = msgQue_.lock();
        CHECK_FAIL_RETURN_STATUS(ptr != nullptr, K_INVALID, "Not connected to MsgQueMgr");
        if (flags == ZmqSendFlags::DONTWAIT) {
            return SendMsg(ele, 0);
        }
        return SendMsg(ele);
    }

    /**
     * @brief Receive a message.
     * @param[out] ele Receive message reference.
     * @param[in] timeout (in millisecond) The timeout for receiving message.
     * @return Status of the call.
     */
    Status ReceiveMsg(R &ele, int timeout)
    {
        auto ptr = msgQue_.lock();
        CHECK_FAIL_RETURN_STATUS(ptr != nullptr, K_INVALID, "Not connected to MsgQueMgr");
        return ptr->ReceiveMsg(ele, timeout);
    }

    /**
     * @brief Receive a message using the default timeout value of the message queue.
     * @param[out] ele Receive message reference.
     * @return Status of the call.
     */
    Status ReceiveMsg(R &ele)
    {
        auto ptr = msgQue_.lock();
        CHECK_FAIL_RETURN_STATUS(ptr != nullptr, K_INVALID, "Not connected to MsgQueMgr");
        return ptr->ReceiveMsg(ele);
    }

    /**
     * @brief Receive a message using zmq receive flag.
     * @param[out] ele Receive message reference.
     * @param[in] flags The receiving flags, as of now only ZmqRecvFlags::DONTWAIT is implemented.
     * @return Status of the call.
     */
    Status ReceiveMsg(R &ele, ZmqRecvFlags flags)
    {
        auto ptr = msgQue_.lock();
        CHECK_FAIL_RETURN_STATUS(ptr != nullptr, K_INVALID, "Not connected to MsgQueMgr");
        if (flags == ZmqRecvFlags::DONTWAIT) {
            return ReceiveMsg(ele, 0);
        }
        return ReceiveMsg(ele);
    }

    /**
     * @brief Same as ReceiveMsg but mainly for client to map TRY_AGAIN to a clear message
     * @param[out] ele Receive message reference.
     * @param[in] flags The receiving flags, as of now only ZmqRecvFlags::DONTWAIT is implemented.
     * @return Status of the call.
     */
    Status ClientReceiveMsg(R &ele, ZmqRecvFlags flags)
    {
        Status rc = ReceiveMsg(ele, flags);
        if (rc.GetCode() == K_TRY_AGAIN) {
            // Do nothing if flags for non-blocking call.
            if (flags == ZmqRecvFlags::DONTWAIT) {
                return rc;
            }
            rc = Status(StatusCode::K_RPC_UNAVAILABLE,
                        FormatString("Rpc service for client %s has not responded within the allowed time. Detail: %s",
                                     GetId(), rc.ToString()));
        }
        return rc;
    }

    /**
     * @brief Get the message queue's zmq options.
     * @return Message queue's options.
     */
    auto GetOpts() const
    {
        auto ptr = msgQue_.lock();
        CHECK_FAIL_RETURN_STATUS(ptr != nullptr, K_INVALID, "Not connected to MsgQueMgr");
        return ptr->GetOpts();
    }

    /**
     * @brief Update the message queue's options.
     * @param[in] opts The options to set.
     */
    void UpdateOpts(const RpcOptions &opts)
    {
        auto ptr = msgQue_.lock();
        // Only refresh those we are allowed to change.
        if (ptr) {
            ptr->UpdateOpts(opts);
        }
    }

    /**
     * @brief Poll message reception with timeout.
     * @param[in] timeout The reception timeout.
     * @return Status of the call.
     */
    auto PollRecv(int timeout) const
    {
        auto ptr = msgQue_.lock();
        CHECK_FAIL_RETURN_STATUS(ptr != nullptr, K_INVALID, "Not connected to MsgQueMgr");
        return ptr->PollRecv(timeout);
    };

private:
    friend class MsgQueMgr<W, R>;
    std::weak_ptr<MsgQue<W, R>> msgQue_;
};

/**
 * On the client side the outbound message queue is the pair {MetaPb, std::deque<zmq:message_t>}
 * and inbound queue is std::deque<ZmqMessage>.
 */
typedef MsgQue<ZmqMetaMsgFrames, ZmqMsgFrames> ZmqMsgQue;
typedef MsgQueMgr<ZmqMetaMsgFrames, ZmqMetaMsgFrames> ZmqMsgMgr;
typedef MsgQueRef<ZmqMetaMsgFrames, ZmqMetaMsgFrames> ZmqMsgQueRef;
}  // namespace datasystem
#endif  // DATASYSTEM_COMMON_RPC_ZMQ_MSG_QUE_H
