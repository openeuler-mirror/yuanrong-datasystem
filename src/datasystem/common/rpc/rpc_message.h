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
 * Description: Message classes encapsulating the ZmqMessage/AddPayloadFrames.
 */
#ifndef DATASYSTEM_COMMON_RPC_RPC_MESSAGE_H
#define DATASYSTEM_COMMON_RPC_RPC_MESSAGE_H

#include <string>
#include <utility>
#include <memory>

#include "datasystem/common/rpc/zmq/zmq_message.h"
#include "datasystem/common/rpc/mem_view.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {
enum class RpcSendFlags : int { NONE = 0, DONTWAIT = ZMQ_DONTWAIT, SNDMORE = ZMQ_SNDMORE };
enum class RpcRecvFlags : int { NONE = 0, DONTWAIT = ZMQ_DONTWAIT };
typedef void(MsgFreeFn)(void *data, void *hint);

class RpcMessage {
public:
    /**
     * @brief The most basic form of constructor, sets everything to empty.
     */
    RpcMessage() = default;

    RpcMessage(RpcMessage &&msg) noexcept;
    RpcMessage &operator=(RpcMessage &&msg) noexcept;

    virtual ~RpcMessage();

    void *Data() const;

    size_t Size() const;

    // helper functions for zmq lower layer or FC purposes
    explicit RpcMessage(ZmqMessage msg);

    void MoveToMsg(ZmqMessage &msg);

    bool operator==(const RpcMessage &other) const;

    bool operator!=(const RpcMessage &other) const;

    std::string ToString();

    bool Empty() const;

    void Clear();

    Status Resize(size_t len);

    Status TransferOwnership(void *data, size_t size, MsgFreeFn *ffn, void *hint = nullptr);

    Status CopyString(const std::string &str);

    /**
     * @brief Pre-allocate memory buffer size
     * @param size
     * @return Status object
     */
    Status AllocMem(size_t size);

    /**
     * @brief A wrapper for referencing a buffer at address data, size bytes long. The caller still owns the buffer and
     * is responsible for freeing the buffer.
     * @param[in] data Source of the buffer
     * @param[in] size Size of the buffer
     * @return Status object
     */
    Status ZeroCopyBuffer(void *data, size_t size);

    /**
     * @brief Copy a buffer into ZmqMessage
     * @param[in] data Source of the buffer
     * @param[in] size Size of the buffer
     * @return Status object
     */
    Status CopyBuffer(const void *data, size_t size);

protected:
    ZmqMessage &GetMsg();

    ZmqMessage msg_;
};
}  // namespace datasystem
#endif  // DATASYSTEM_COMMON_RPC_RPC_MESSAGE_H
