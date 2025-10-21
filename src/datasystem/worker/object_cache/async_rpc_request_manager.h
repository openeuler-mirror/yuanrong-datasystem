
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

/**
 * Description: Code to manage device object.
 */

#ifndef DATASYSTEM_ASYNC_RPC_REQUEST_MANAGER_H
#define DATASYSTEM_ASYNC_RPC_REQUEST_MANAGER_H

#include <chrono>
#include <functional>
#include <future>
#include <memory>
#include <utility>

#include "datasystem/common/util/format.h"
#include "datasystem/common/rpc/rpc_server_stream_base.h"
#include "datasystem/common/util/queue/blocking_queue.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/thread_pool.h"
#include "datasystem/common/util/wait_post.h"
#include "datasystem/protos/p2p_subscribe.pb.h"
#include "datasystem/utils/optional.h"
#include "datasystem/utils/status.h"

namespace datasystem {
constexpr int32_t TEN_SECONDS_MS = 10000;
class AsyncRpcRequest {
public:
    explicit AsyncRpcRequest(uint32_t timeoutMs) : timeoutMs_(timeoutMs)
    {
    }

    virtual ~AsyncRpcRequest(){};

    /**
     * @brief Async write rpc function.
     * @return Status of the call.
     */
    virtual Status AsyncWrite() = 0;

    /**
     * @brief Check Async write rpc is time out.
     * @return True is timeout.
     */
    virtual bool HasTimeout()
    {
        if (asyncWriteTimestamp_ == std::chrono::steady_clock::time_point::min()) {
            return false;
        }
        auto duration = std::chrono::steady_clock::now() - asyncWriteTimestamp_;
        auto timeMs = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
        return timeMs > timeoutMs_;
    }

    /**
     * @brief Check Async write rpc is reply.
     * @param[in] timeoutMs timeout of check, default is not wait.
     * @return True is reply.
     */
    virtual bool HasReply(uint32_t timeoutMs = 0) = 0;

    /**
     * @brief Handler async write timeout callback.
     */
    virtual void HandleTimeout() = 0;

    /**
     * @brief Handler response callback and send back to client.
     */
    virtual void HandleReturn() = 0;

protected:
    std::chrono::steady_clock::time_point asyncWriteTimestamp_{ std::chrono::steady_clock::time_point::min() };
    int32_t timeoutMs_;
};

template <typename ReqPb, typename RespPb>
class LocalAsyncRpcRequest : public AsyncRpcRequest {
public:
    using DerivedApi = LocalServerUnaryWriterReader<RespPb, ReqPb>;
    using BaseApiPtr = std::shared_ptr<ServerUnaryWriterReader<RespPb, ReqPb>>;
    using Callback = std::function<Status(LocalAsyncRpcRequest &)>;

    LocalAsyncRpcRequest(ReqPb &req, uint32_t timeoutMs) : AsyncRpcRequest(timeoutMs)
    {
        std::promise<std::pair<RespPb, Status>> promise;
        future_ = promise.get_future();
        localServerApi_ = std::make_shared<DerivedApi>(req, std::move(promise));
    }

    ~LocalAsyncRpcRequest()
    {
    }

    static Status EmptyCallback(LocalAsyncRpcRequest &request)
    {
        (void)request;
        return Status::OK();
    }

    Status AsyncWrite() override
    {
        Status rc = asyncWriteCallback_(*this);
        asyncWriteTimestamp_ = std::chrono::steady_clock::now();
        return rc;
    }

    void HandleTimeout() override
    {
        LOG_IF_ERROR(timeoutCallback_(*this), "HandleTimeout failed.");
    }

    void HandleReturn() override
    {
        LOG_IF_ERROR(returnCallback_(*this), "HandleReturn failed.");
    }

    bool HasReply(uint32_t timeoutMs = 0) override
    {
        return future_.wait_for(std::chrono::seconds(timeoutMs)) == std::future_status::ready;
    }

    std::pair<RespPb, Status> GetReply(uint32_t timeoutMs = 0)
    {
        std::pair<RespPb, Status> result;
        if (!HasReply(timeoutMs)) {
            result.second = { K_TRY_AGAIN, "Reply is not ready" };
            return result;
        }

        try {
            result = future_.get();
        } catch (const std::exception &e) {
            result.second = { K_RUNTIME_ERROR, FormatString("Exception when calling future.get(): %s ", e.what()) };
        }
        return result;
    }

    /**
     * @brief Get local stub api.
     * @return BaseApiPtr is rpc stub.
     */
    BaseApiPtr GetServerApi()
    {
        return localServerApi_;
    }

    /**
     * @brief Reply response to client .
     * @param[in] result The result of master call back.
     * @param[in] clientServerApi The stub send to client.
     * @return Status of the call.
     */
    Status ReplyToClient(const std::pair<RespPb, Status> &result, const BaseApiPtr clientServerApi)
    {
        // reply to client
        if (result.second.IsError()) {
            return clientServerApi->SendStatus(result.second);
        }
        return clientServerApi->Write(result.first);
    }

    /**
     * @brief Set async rpc handler.
     * @param[in] asyncWrite The async write call back function.
     * @param[in] returnCallback The return call back function.
     * @param[in] timeoutCallback The timeout call back function.
     */
    void SetCallback(Callback asyncWrite, Callback returnCallback, Callback timeoutCallback)
    {
        asyncWriteCallback_ = asyncWrite;
        returnCallback_ = returnCallback;
        timeoutCallback_ = timeoutCallback;
    }

private:
    Callback asyncWriteCallback_;
    Callback returnCallback_;
    Callback timeoutCallback_;

    BaseApiPtr localServerApi_;
    std::shared_future<std::pair<RespPb, Status>> future_;
};

template <typename ReqPb, typename RespPb>
class RemoteAsyncRpcRequest : public AsyncRpcRequest {
public:
    using BaseApiPtr = std::shared_ptr<ServerUnaryWriterReader<RespPb, ReqPb>>;
    using Callback = std::function<Status(RemoteAsyncRpcRequest &)>;
    using RegisterRpcFunc = std::function<Status(RemoteAsyncRpcRequest &, int64_t, RespPb &, RpcRecvFlags)>;

    RemoteAsyncRpcRequest(std::shared_ptr<master::MasterOCService_Stub> rpcSession, uint32_t timeoutMs)
        : AsyncRpcRequest(timeoutMs)
    {
        rpcSession_ = rpcSession;
    }

    ~RemoteAsyncRpcRequest()
    {
    }

    static Status EmptyCallback(RemoteAsyncRpcRequest &request)
    {
        (void)request;
        return Status::OK();
    }

    Status AsyncWrite() override
    {
        Status rc = asyncWriteCallback_(*this);
        asyncWriteTimestamp_ = std::chrono::steady_clock::now();
        return rc;
    }

    void HandleTimeout() override
    {
        LOG_IF_ERROR(timeoutCallback_(*this), "HandleTimeout failed.");
    }

    void HandleReturn() override
    {
        LOG_IF_ERROR(returnCallback_(*this), "HandleReturn failed.");
    }

    bool HasReply(uint32_t timeoutMs = 0) override
    {
        (void)timeoutMs;
        replyStatus_ = rpcRespFunc_(*this, tagId_, rsp_, RpcRecvFlags::DONTWAIT);
        if (replyStatus_.GetCode() == StatusCode::K_TRY_AGAIN) {
            return replyTag_;  // default is not ready
        }
        replyTag_ = true;
        if (replyStatus_.IsError()) {
            LOG(ERROR) << "Got unexpected error while AsyncRead : " << replyStatus_.ToString();
        }
        return replyTag_;
    }

    /**
     * @brief Get reply status from master.
     * @param[in] timeoutMs The read rpc timeout, default is not wait.
     * @return Result of master return.
     */
    std::pair<RespPb, Status> GetReply(uint32_t timeoutMs = 0)
    {
        std::pair<RespPb, Status> result;

        if (replyTag_) {
            result.first = std::move(rsp_);
            result.second = std::move(replyStatus_);
        } else if (timeoutMs != 0) {
            // Blocking read, zmq Async read does not support setting a unique timeout for each rpc
            result.second = rpcRespFunc_(*this, tagId_, result.first, RpcRecvFlags::NONE);
        } else {
            result.second = { K_TRY_AGAIN, "Reply is not ready" };
        }

        return result;
    }

    /**
     * @brief Reply response to client .
     * @param[in] result The result of master call back.
     * @param[in] clientServerApi The stub send to client.
     * @return Status of the call.
     */
    Status ReplyToClient(const std::pair<RespPb, Status> &result, const BaseApiPtr clientServerApi)
    {
        // reply to client
        if (result.second.IsError()) {
            return clientServerApi->SendStatus(result.second);
        }
        return clientServerApi->Write(result.first);
    }

    /**
     * @brief Set async rpc handler.
     * @param[in] asyncWrite The async write call back function.
     * @param[in] returnCallback The return call back function.
     * @param[in] timeoutCallback The timeout call back function.
     * @param[in] rpcRespFunc The async read function.
     */
    void SetCallback(Callback asyncWrite, Callback returnCallback, Callback timeoutCallback,
                     RegisterRpcFunc rpcRespFunc)
    {
        asyncWriteCallback_ = asyncWrite;
        returnCallback_ = returnCallback;
        timeoutCallback_ = timeoutCallback;
        rpcRespFunc_ = rpcRespFunc;
    }

    /**
     * @brief Get remote master stub api.
     * @return BaseApiPtr is rpc stub.
     */
    std::shared_ptr<master::MasterOCService_Stub> GetServerApi()
    {
        return rpcSession_;
    }

    /**
     * @brief Store async write tag.
     * @param[in] tagId The tag id of async write.
     * @return Status of the call.
     */
    void SetResponseTag(int64_t tagId)
    {
        tagId_ = tagId;
    }

    /**
     * @brief Get write tag.
     * @return The tag id of async write.
     */
    int64_t GetResponseTag()
    {
        return tagId_;
    }

private:
    Callback asyncWriteCallback_;
    Callback returnCallback_;
    Callback timeoutCallback_;
    RegisterRpcFunc rpcRespFunc_;

    std::shared_ptr<master::MasterOCService_Stub> rpcSession_{ nullptr };
    Status replyStatus_;
    bool replyTag_ = false;
    RespPb rsp_;
    int64_t tagId_ = 0;
};

class AsyncRpcRequestManager {
public:
    explicit AsyncRpcRequestManager(int threadNum)
    {
        if (threadNum <= 0) {
            return;
        }
        pool_ = std::make_unique<ThreadPool>(threadNum, threadNum, "AsyncRequestPool");
        for (int i = 0; i < threadNum; i++) {
            pool_->Execute([this]() { ReadLoop(); });
        }
    }

    void AddRequest(std::shared_ptr<AsyncRpcRequest> request)
    {
        requestQueue_.Push(std::move(request));
        waitQueueNotEmpty_.Set();
    }

    ~AsyncRpcRequestManager()
    {
        interruptFlag_ = true;
        waitQueueNotEmpty_.Set();
        requestQueue_.Abort();
        if (pool_) {
            pool_.reset();
        }
    }

private:
    void ReadLoop()
    {
        // later will check and findout if it is need to add internal
        while (!interruptFlag_) {
            waitQueueNotEmpty_.WaitFor(TEN_SECONDS_MS);

            std::shared_ptr<AsyncRpcRequest> request;
            if (requestQueue_.Pop(request).IsError() || request == nullptr) {
                continue;
            }

            if (request->HasTimeout()) {
                request->HandleTimeout();
            } else if (request->HasReply()) {
                request->HandleReturn();
            } else {
                requestQueue_.Push(std::move(request));
            }

            if (requestQueue_.Empty()) {
                waitQueueNotEmpty_.Clear();
            }
        }
    }

    WaitPost waitQueueNotEmpty_;
    std::atomic<bool> interruptFlag_{ false };
    BlockingQueue<std::shared_ptr<AsyncRpcRequest>> requestQueue_;
    std::unique_ptr<ThreadPool> pool_;
};

}  // namespace datasystem
#endif