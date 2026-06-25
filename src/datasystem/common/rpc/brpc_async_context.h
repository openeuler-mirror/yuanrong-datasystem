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
    // Owned by BrpcAsyncCall: allocated by generated AsyncWrite,
    // consumed (moved-from) by AsyncRead. unique_ptr guarantees cleanup
    // for ForgetRequest or stub destruction while calls are in-flight.
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
 * Thread-safe: the internal map is protected by a mutex.
 *
 * V10 note: contention on mapMtx_ is bounded per-stub (each stub has its
 * own BrpcAsyncContext), not global. A single stub's async QPS is limited
 * by the calling service's concurrency (typically <1000 QPS per stub).
 * If profiling shows this mutex hot, consider tbb::concurrent_hash_map or
 * a sharded map keyed by tagId % N. Tracked as low-priority follow-up.
 */
class BrpcAsyncContext {
public:
    BrpcAsyncContext() : nextTagId_(1) {}
    ~BrpcAsyncContext() = default;

    /**
     * Allocate a new tag and store the async call state.
     * @return tag ID for later AsyncRead lookup
     */
    int64_t AllocateTag()
    {
        std::lock_guard<std::mutex> lock(mapMtx_);
        int64_t tagId = nextTagId_++;
        auto call = std::make_shared<BrpcAsyncCall>();
        pendingCalls_[tagId] = call;
        return tagId;
    }

    /**
     * Get the BrpcAsyncCall for a given tag (must exist).
     * Returns nullptr if tag not found.
     */
    std::shared_ptr<BrpcAsyncCall> GetCall(int64_t tagId)
    {
        std::lock_guard<std::mutex> lock(mapMtx_);
        auto it = pendingCalls_.find(tagId);
        if (it == pendingCalls_.end()) {
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
        // brpc::NewCallback allocates; closure is self-deleted after Run().
        return brpc::NewCallback(&OnRpcDone, call);
    }

    /**
     * Remove and return the call for a tag. Caller takes ownership.
     */
    std::shared_ptr<BrpcAsyncCall> TakeCall(int64_t tagId)
    {
        std::lock_guard<std::mutex> lock(mapMtx_);
        auto it = pendingCalls_.find(tagId);
        if (it == pendingCalls_.end()) {
            return nullptr;
        }
        auto call = std::move(it->second);
        pendingCalls_.erase(it);
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
        std::lock_guard<std::mutex> lock(mapMtx_);
        pendingCalls_.erase(tagId);
        CleanupExpiredLocked();
    }

private:
    static constexpr auto kPendingCallTtl = std::chrono::minutes(5);
    static constexpr size_t kCleanupLogInterval = 100;

    /// Remove entries older than kPendingCallTtl (called under mapMtx_).
    void CleanupExpiredLocked()
    {
        auto now = std::chrono::steady_clock::now();
        for (auto it = pendingCalls_.begin(); it != pendingCalls_.end();) {
            if (now - it->second->createdAt > kPendingCallTtl) {
                LOG_EVERY_N(WARNING, kCleanupLogInterval) << "BrpcAsyncContext: expiring stale pending call tag="
                                          << it->first << " (age="
                                          << std::chrono::duration_cast<std::chrono::seconds>(
                                                 now - it->second->createdAt).count()
                                          << "s) — AsyncRead never called";
                it = pendingCalls_.erase(it);
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
        {
            std::lock_guard<std::mutex> lock(call->mtx);
            call->failed = call->cntl.Failed();
            if (call->failed) {
                call->errorCode = call->cntl.ErrorCode();
                call->errorText = call->cntl.ErrorText();
            }
            call->responseAttachment = call->cntl.response_attachment();
            call->completed = true;
            call->cv.notify_one();
        }
        // The shared_ptr<BrpcAsyncCall> is also held by pendingCalls_.
        // When this closure self-deletes after Run(), it drops its ref.
        // Orphaned calls (AsyncRead never invoked) are cleaned up by
        // CleanupExpiredLocked (5-minute TTL, piggybacked on ForgetCall).
    }

    std::mutex mapMtx_;
    std::map<int64_t, std::shared_ptr<BrpcAsyncCall>> pendingCalls_;
    int64_t nextTagId_;
};

}  // namespace datasystem
#endif  // DATASYSTEM_COMMON_RPC_BRPC_ASYNC_CONTEXT_H
