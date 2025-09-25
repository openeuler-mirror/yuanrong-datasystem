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
 * Description: Zmq Stub.
 */
#ifndef DATASYSTEM_COMMON_RPC_ZMQ_STUB_H
#define DATASYSTEM_COMMON_RPC_ZMQ_STUB_H

#include <memory>
#include <string>

#include "datasystem/common/rpc/rpc_message.h"
#include "datasystem/common/rpc/rpc_options.h"
#include "datasystem/common/rpc/rpc_stub.h"
#include "datasystem/protos/meta_zmq.pb.h"
#include "datasystem/utils/status.h"

namespace datasystem {
class ZmqStubImpl;
class RpcChannel;
class RpcServiceMethod;

/**
 * @brief A stub provides methods to send/receive rpc service to the server.
 * The zmq plugin will generate a subclass to inherit from this class.
 * In general there is no need to call the methods in this class directly.
 */
class ZmqStub {
public:
    explicit ZmqStub(const std::shared_ptr<RpcChannel> &channel, int32_t timeoutMs = -1);

    virtual ~ZmqStub();

    virtual std::string FullServiceName() const = 0;

    virtual std::string ServiceName() const = 0;

    /**
     * @brief Forget a previous AsyncWrite request.
     * @details The underlying dealer socket will disconnect from backend and
     * free for reuse.
     * @param[in] tag Tag to identify a request.
     */
    void ForgetRequest(int64_t tag);

    /**
     * @brief Check if the peer is alive.
     * @param[in] threshold The threshold time to determine if the peer is alive.
     * @return true if the peer lost contact for less than the threshold seconds.
     */
    bool IsPeerAlive(uint32_t threshold);

    /**
     * @brief Get the tcp/ip channel number
     * @return integer
     */
    auto GetChannelNumber() const
    {
        return channelNo_;
    }

    void CacheSession(bool);

    Status GetInitStatus();

    /**
     * @brief Set exclusive connection by assigning required fields
     * @param[in] exclusiveId The exclusive id
     * @param[in] exclusiveSockPath The socket path for exclusive sock connect
     */
    void SetExclusiveConnInfo(const std::optional<int32_t> &exclusiveId, const std::string &sockPath)
    {
        exclusiveSockPath_ = sockPath;
        exclusiveId_ = exclusiveId;
    }

protected:
    /**
     * @brief Initialization. If requesting uds connection, the connection will be established asynchronously.
     * @return Status of call.
     */
    Status InitConn();

    std::map<int32_t, std::shared_ptr<RpcServiceMethod>> methodMap_;
    std::string serviceName_;
    std::string exclusiveSockPath_;
    std::optional<int32_t> exclusiveId_;
    int channelNo_;
    std::unique_ptr<ZmqStubImpl> pimpl_;
};
}  // namespace datasystem
#endif  // DATASYSTEM_COMMON_RPC_ZMQ_STUB_H
