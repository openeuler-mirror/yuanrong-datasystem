/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
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
#ifndef P2PCOMMMANAGER_H
#define P2PCOMMMANAGER_H

#include <atomic>
#include <condition_variable>
#include <cstring>
#include <mutex>
#include <memory>
#include <queue>
#include <string>
#include <unordered_map>
#include <vector>
#include "communicator/P2PCommunicator.h"
#include "npu/P2PMem.h"
#include "tools/hccl-convert.h"

constexpr uint32_t MAX_NUM_PINGPONG_BUFF = 5;

class PingpongBufferPool {
public:
    using Buffer = std::vector<std::shared_ptr<P2PMem>>;

    PingpongBufferPool(size_t maxPoolSize) : maxPoolSize_(maxPoolSize), totalCount_(0), curChunk_(0), curBuffer_(0)
    {
    }

    class BufferHandle {
    public:
        BufferHandle(BufferHandle &&other) noexcept : pool_(other.pool_), buf_(std::move(other.buf_))
        {
            other.pool_ = nullptr;
        }

        BufferHandle &operator=(BufferHandle &&other) noexcept
        {
            if (this != &other) {
                Release();
                pool_ = other.pool_;
                buf_ = std::move(other.buf_);
                other.pool_ = nullptr;
            }
            return *this;
        }

        BufferHandle(const BufferHandle &) = delete;
        BufferHandle &operator=(const BufferHandle &) = delete;

        ~BufferHandle()
        {
            Release();
        }

        Buffer *operator->()
        {
            return buf_.get();
        }
        Buffer &operator*()
        {
            return *buf_;
        }
        Buffer *get()
        {
            return buf_.get();
        }
        explicit operator bool() const
        {
            return buf_ != nullptr;
        }

    private:
        friend class PingpongBufferPool;

        BufferHandle(PingpongBufferPool *pool, std::shared_ptr<Buffer> buf) : pool_(pool), buf_(std::move(buf))
        {
        }

        void Release()
        {
            if (pool_ && buf_) {
                pool_->Release(std::move(buf_));
                pool_ = nullptr;
            }
        }

        PingpongBufferPool *pool_;
        std::shared_ptr<Buffer> buf_;
    };

    std::optional<BufferHandle> Acquire()
    {
        std::unique_lock<std::mutex> lock(mtx_);

        if (!pool_.empty()) {
            auto buf = pool_.front();
            pool_.pop();
            return BufferHandle(this, std::move(buf));
        }

        if (totalCount_ < maxPoolSize_) {
            ++totalCount_;
            lock.unlock();
            auto buf = std::make_shared<Buffer>();
            for (size_t i = 0; i < P2P_NUM_PINGPONG_BUFF; i++) {
                auto mem = std::make_unique<P2PMem>();
                auto status = mem->alloc(P2P_BLOCK_SIZE_BYTES, ACL_MEM_MALLOC_HUGE_FIRST);
                if (!status.IsSuccess()) {
                    totalCount_--;
                    std::cerr << "Failed to allocate pingpong buffer, status: " << status.ToString() << std::endl;
                    return std::nullopt;
                }
                buf->push_back(std::move(mem));
            }
            return BufferHandle(this, std::move(buf));
        }

        cv_.wait(lock, [&] { return !pool_.empty(); });
        auto buf = pool_.front();
        pool_.pop();
        return BufferHandle(this, std::move(buf));
    }

    uint32_t GetCurBuffer()
    {
        return curBuffer_;
    }

    void SetCurBuffer(uint32_t bufferId)
    {
        curBuffer_ = bufferId;
    }

    uint32_t GetCurChunk()
    {
        return curChunk_;
    }

    void SetCurChunk(uint32_t chunkId)
    {
        curChunk_ = chunkId;
    }

private:
    void Release(std::shared_ptr<Buffer> buf)
    {
        std::lock_guard<std::mutex> lock(mtx_);
        pool_.push(std::move(buf));
        cv_.notify_one();
    }

private:
    size_t bufferSize_;
    size_t maxPoolSize_;
    uint32_t curChunk_;
    uint32_t curBuffer_;

    std::queue<std::shared_ptr<Buffer>> pool_;
    std::atomic<size_t> totalCount_;
    std::mutex mtx_;
    std::condition_variable cv_;
};

class P2PCommunicatorManager {
public:
    // Store root communicator which has not yet been associated with a client communicator
    void AddUnboundRootComm(std::string &identifier, std::shared_ptr<P2PCommunicator> p2pComm)
    {
        {
            std::lock_guard<std::mutex> lock(unboundRootCommsMut);
            unboundRootComms.insert(std::make_pair(identifier, p2pComm));
        }
    }

    // Retreive and remove root communicator which has not yet been associated with a client communicator
    std::shared_ptr<P2PCommunicator> GetAndRemoveUnboundCommunicator(const std::string &identifier)
    {
        std::unique_lock<std::mutex> lock(unboundRootCommsMut);
        auto pos = unboundRootComms.find(identifier);
        if (pos == unboundRootComms.end()) {
            return std::shared_ptr<P2PCommunicator>();
        }
        auto res = pos->second;
        unboundRootComms.erase(identifier);
        return res;
    }

    // Retrieve communicator for which a connection has been established
    std::shared_ptr<P2PCommunicator> GetCommunicator(const P2PComm &comm)
    {
        std::unique_lock<std::mutex> lock(commsMut);
        auto pos = comms.find(comm);
        if (pos == comms.end()) {
            return std::shared_ptr<P2PCommunicator>();
        }
        return pos->second;
    }

    // Add communicator for which a connection has been established
    void AddCommunicator(P2PComm &resComm, std::shared_ptr<P2PCommunicator> p2pComm)
    {
        {
            std::lock_guard<std::mutex> lock(commsMut);
            comms.insert(std::make_pair(resComm, p2pComm));
        }
    }

    // Remove communicator for which a connection has been established
    bool RemoveCommunicator(const P2PComm &comm)
    {
        std::unique_lock<std::mutex> lock(commsMut);
        auto pos = comms.find(comm);
        if (pos == comms.end()) {
            return false;
        }
        comms.erase(comm);
        return true;
    }

    PingpongBufferPool *GetBufferPool()
    {
        return &bufferPool;
    }

    void AddFailedIdentifier(const std::string &identifier)
    {
        std::lock_guard<std::mutex> lock(failedIdentifiersMut);
        failedIdentifiers.insert(identifier);
    }

    bool IsFailedIdentifier(const std::string &identifier)
    {
        std::lock_guard<std::mutex> lock(failedIdentifiersMut);
        return failedIdentifiers.find(identifier) != failedIdentifiers.end();
    }

private:
    // Port -> Communicator
    std::unordered_map<std::string, std::shared_ptr<P2PCommunicator>> unboundRootComms;
    std::mutex unboundRootCommsMut;
    std::unordered_map<P2PComm, std::shared_ptr<P2PCommunicator>> comms;
    std::mutex commsMut;
    PingpongBufferPool bufferPool{ MAX_NUM_PINGPONG_BUFF };
    std::unordered_set<std::string> failedIdentifiers;
    std::mutex failedIdentifiersMut;
};

#endif
