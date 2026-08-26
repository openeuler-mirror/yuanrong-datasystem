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
 * Description: Transport-neutral base class for generated RPC services.
 */
#ifndef DATASYSTEM_COMMON_RPC_RPC_SERVICE_BASE_H
#define DATASYSTEM_COMMON_RPC_RPC_SERVICE_BASE_H

#include <memory>
#include <string>
#include <utility>

#include "datasystem/common/rpc/rpc_message.h"
#include "datasystem/common/rpc/rpc_service_cfg.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/thread_pool.h"
#include "datasystem/protos/meta_zmq.pb.h"

namespace datasystem {
/**
 * @brief Abstract base for RPC services emitted by the protoc generator.
 * The generator emits a subclass per service; brpc adapters dispatch onto it.
 * serviceName_ is retained for generated-code compatibility (generator emits a
 * constructor that assigns it); ServiceName() overrides return the literal
 * name, so the member is dead under the brpc-only transport.
 */
class RpcServiceBase {
public:
    RpcServiceBase() = default;

    virtual ~RpcServiceBase() = default;

    RpcServiceBase(const RpcServiceBase &) = delete;

    RpcServiceBase &operator=(const RpcServiceBase &) = delete;

    virtual std::string FullServiceName() const = 0;

    virtual std::string ServiceName() const = 0;

    virtual Status CallMethod(MetaPb meta, RpcMsgFrames &&inMsg, int64_t seqNo) = 0;

    virtual Status DirectCallMethod(MetaPb meta, RpcMsgFrames &&inMsg, int64_t seqNo, RpcMsgFrames &outMsg) = 0;

    Status Init(RpcServiceCfg cfg)
    {
        cfg_ = std::move(cfg);
        return InitThreadPool();
    }

    const RpcServiceCfg &GetCfg() const
    {
        return cfg_;
    }

    int32_t NumRegularSockets() const
    {
        return cfg_.numRegularSockets_;
    }

    ThreadPool::ThreadPoolUsage GetThreadPoolSnapshot() const
    {
        static ThreadPool::ThreadPoolUsage empty;
        return thrdPool_ != nullptr ? thrdPool_->GetThreadPoolUsage() : empty;
    }

    ThreadPool::ThreadPoolUsage GetThreadPoolUsage() const
    {
        static ThreadPool::ThreadPoolUsage empty;
        return thrdPool_ != nullptr ? thrdPool_->GetAndResetIntervalStats() : empty;
    }

protected:
    Status InitThreadPool()
    {
        auto numThreads = cfg_.numRegularSockets_ + cfg_.numStreamSockets_;
        const int minThread = 1 + cfg_.numStreamSockets_;
        const int initMaxThread = 16;
        auto minThreads = numThreads / minThread;
        minThreads = minThreads > minThread ? minThreads : minThread;
        minThreads = minThreads > initMaxThread ? initMaxThread : minThreads;
        RETURN_IF_EXCEPTION_OCCURS(thrdPool_ = std::make_unique<ThreadPool>(minThreads, numThreads, ServiceName()));
        return Status::OK();
    }

    RpcServiceCfg cfg_{};
    std::unique_ptr<ThreadPool> thrdPool_{ nullptr };
    std::string serviceName_;
};
}  // namespace datasystem
#endif  // DATASYSTEM_COMMON_RPC_RPC_SERVICE_BASE_H
