/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
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
 * Description: brpc async RPC context for AsyncWrite/AsyncRead support.
 * Maps tag IDs to in-flight async RPCs. AsyncWrite issues a brpc CallMethod
 * with a Done closure; AsyncRead waits for completion and retrieves the result.
 *
 * Threading: The Done closure runs on a brpc bthread when the RPC completes.
 * AsyncRead may be called from any thread. All shared state is protected by
 * per-entry mutex+cv to avoid global contention.
 */
#ifndef DATASYSTEM_COMMON_RPC_BRPC_ASYNC_CONTEXT_H
#define DATASYSTEM_COMMON_RPC_BRPC_ASYNC_CONTEXT_H

#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <map>
#include <memory>
#include <mutex>
#include <string>

#include <brpc/controller.h>
#include <google/protobuf/message.h>

#include "datasystem/common/log/log.h"

namespace datasystem {

/**
 * Tracks a single in-flight async RPC.
 * Owned by BrpcAsyncContext and self-deleted via shared_ptr when both the
 * Done closure and AsyncRead have released their references.
 */
struct BrpcAsyncCall {
    BrpcAsyncCall() : completed(false), failed(false), createdAt(std::chrono::steady_clock::now()) {}
    ~BrpcAsyncCall() = default;

    std::mutex mtx;
    std::condition_variable cv;
    bool completed;
    bool failed;
    std::chrono::steady_clock::time_point createdAt;
    int errorCode = 0;
    std::string errorText;
    brpc::Controller cntl;
    // Owned by BrpcAsyncCall: allocated in generated AsyncWrite, consumed in AsyncRead.
    std::unique_ptr<google::protobuf::Message> response;
    // Heap-allocated copy of the request protobuf, passed to brpc::Channel::CallMethod.
    // brpc serializes the request asynchronously on a bthread; the pointer must outlive
    // AsyncWrite's stack frame. Owned by BrpcAsyncCall.
    std::unique_ptr<google::protobuf::Message> request;
    // Snapshot of cntl.response_attachment() taken in OnRpcDone under lock, so that
    // AsyncRead can read it safely without accessing cntl after Done returns.
    butil::IOBuf responseAttachment;
};

/**
 * Per-stub async context. Each BrpcStub instance owns one.
 * Thread-safe: pending calls are sharded into 32 buckets, each with its own
 * mutex, keyed by tagId. OnRpcDone runs on a brpc bthread and only locks
 * the per-call mutex (no shard-mutex contention from the callback path).
 */
class BrpcAsyncContext {
public:
    static constexpr size_t kShardCount = 32;
    static_assert((kShardCount & (kShardCount - 1)) == 0, "kShardCount must be a power of 2");

    BrpcAsyncContext() = default;
    ~BrpcAsyncContext() = default;

    /**
     * Allocate a new tag and store the async call state.
     * @return tag ID for later AsyncRead lookup
     */
    int64_t AllocateTag()
    {
        int64_t tagId = nextTagId_.fetch_add(1, std::memory_order_relaxed);
        auto call = std::make_shared<BrpcAsyncCall>();
        auto& shard = shards_[GetShardIndex(tagId)];
        std::lock_guard<std::mutex> lock(shard.mutex);
        shard.pendingCalls[tagId] = call;
        return tagId;
    }

    /**
     * Get the BrpcAsyncCall for a given tag (must exist).
     * Returns nullptr if tag not found.
     */
    std::shared_ptr<BrpcAsyncCall> GetCall(int64_t tagId)
    {
        auto& shard = shards_[GetShardIndex(tagId)];
        std::lock_guard<std::mutex> lock(shard.mutex);
        auto it = shard.pendingCalls.find(tagId);
        if (it == shard.pendingCalls.end()) {
            return nullptr;
        }
        return it->second;
    }

    /**
     * Create a Done closure that signals completion on the async call.
     * The closure decrements the shared_ptr refcount when Run() completes.
     */
    google::protobuf::Closure *MakeDone(std::shared_ptr<BrpcAsyncCall> call)
    {
        return brpc::NewCallback(&OnRpcDone, call);
    }

    /**
     * Remove and return the call for a tag. Caller takes ownership.
     */
    std::shared_ptr<BrpcAsyncCall> TakeCall(int64_t tagId)
    {
        auto& shard = shards_[GetShardIndex(tagId)];
        std::lock_guard<std::mutex> lock(shard.mutex);
        auto it = shard.pendingCalls.find(tagId);
        if (it == shard.pendingCalls.end()) {
            return nullptr;
        }
        auto call = std::move(it->second);
        shard.pendingCalls.erase(it);
        return call;
    }

    /**
     * Drop a pending call without reading (ForgetRequest).
     * The shared_ptr destructor chain will clean up the BrpcAsyncCall
     * (and its response) once the in-flight RPC's Done closure also releases
     * its reference.
     */
    void ForgetCall(int64_t tagId)
    {
        size_t idx = GetShardIndex(tagId);
        {
            auto& shard = shards_[idx];
            std::lock_guard<std::mutex> lock(shard.mutex);
            shard.pendingCalls.erase(tagId);
            CleanupExpiredInShard(shard);
        }
        // Round-robin: each ForgetCall also reaps one other shard,
        // so 32 consecutive ForgetCalls cover the whole table.
        size_t cleanIdx = nextCleanupShard_.fetch_add(1, std::memory_order_relaxed) % kShardCount;
        if (cleanIdx != idx) {
            auto& s = shards_[cleanIdx];
            std::lock_guard<std::mutex> lock(s.mutex);
            CleanupExpiredInShard(s);
        }
    }

private:
    struct Shard {
        std::mutex mutex;
        std::map<int64_t, std::shared_ptr<BrpcAsyncCall>> pendingCalls;
    };

    size_t GetShardIndex(int64_t tagId) const
    {
        return static_cast<size_t>(tagId) & (kShardCount - 1);
    }

    std::array<Shard, kShardCount> shards_;
    std::atomic<int64_t> nextTagId_{1};
    std::atomic<size_t> nextCleanupShard_{0};

    static constexpr auto kPendingCallTtl = std::chrono::minutes(5);
    static constexpr size_t kCleanupLogInterval = 100;

    // Scan a single shard for expired entries. Old code scanned the global
    // map on every ForgetCall; now each ForgetCall only cleans its own shard.
    // Worst case: a shard with no ForgetCall activity may retain orphaned
    // entries up to 32× the typical ForgetCall interval before cleanup.
    // This is acceptable because (a) orphaned calls are rare in practice
    // (every AsyncWrite is paired with AsyncRead or ForgetRequest), and
    // (b) the stub destructor cleans up all remaining entries on shutdown.
    void CleanupExpiredInShard(Shard& shard)
    {
        auto now = std::chrono::steady_clock::now();
        for (auto it = shard.pendingCalls.begin(); it != shard.pendingCalls.end();) {
            if (now - it->second->createdAt > kPendingCallTtl) {
                LOG_EVERY_N(WARNING, kCleanupLogInterval) << "BrpcAsyncContext: expiring stale pending call tag="
                                          << it->first << " (age="
                                          << std::chrono::duration_cast<std::chrono::seconds>(
                                                 now - it->second->createdAt).count()
                                          << "s) — AsyncRead never called";
                it = shard.pendingCalls.erase(it);
            } else {
                ++it;
            }
        }
    }

    /**
     * Called by brpc when the async RPC completes (runs on bthread).
     * Signals the condition variable so AsyncRead can proceed.
     */
    static void OnRpcDone(std::shared_ptr<BrpcAsyncCall> call)
    {
        std::lock_guard<std::mutex> lock(call->mtx);
        call->failed = call->cntl.Failed();
        if (call->failed) {
            call->errorCode = call->cntl.ErrorCode();
            call->errorText = call->cntl.ErrorText();
        }
        // Snapshot response_attachment under lock so AsyncRead does not access
        // cntl after OnRpcDone returns (lock-free access safety).
        call->responseAttachment = call->cntl.response_attachment();
        call->completed = true;
        call->cv.notify_one();
        // The shared_ptr<BrpcAsyncCall> is also held by pendingCalls_.
        // When this closure self-deletes after Run(), it drops its ref.
        // Orphaned calls (AsyncRead never invoked) are cleaned up by
        // ForgetCall via round-robin CleanupExpiredInShard (5-minute TTL).
    }
};

}  // namespace datasystem
#endif  // DATASYSTEM_COMMON_RPC_BRPC_ASYNC_CONTEXT_H
