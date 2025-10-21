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
 * Description: Wrapper for zmq message
 */

#ifndef DATASYSTEM_COMMON_RPC_ZMQ_MESSAGE_H
#define DATASYSTEM_COMMON_RPC_ZMQ_MESSAGE_H

#include <cctype>
#include <string>
#include <zmq.h>
#include <iomanip>
#include "datasystem/utils/status.h"

namespace datasystem {
#define ZmqRecvFlags RpcRecvFlags
#define ZmqSendFlags RpcSendFlags
class ZmqMessage {
public:
    enum class ZmqMsgType : uint16_t { NONE = 0, PAYLOAD_SZ, DECODER };
    ZmqMessage();
    ~ZmqMessage();
    // Disable copy constructors
    ZmqMessage(const ZmqMessage &rhs) = delete;
    ZmqMessage &operator=(const ZmqMessage &rhs) = delete;
    // Move constructors
    ZmqMessage(ZmqMessage &&rhs) noexcept : flag_(rhs.flag_), msg_(rhs.msg_)
    {
        int rc = zmq_msg_init(&rhs.msg_);
        if (rc != 0) {
            std::terminate();
        }
    }

    ZmqMessage &operator=(ZmqMessage &&rhs) noexcept
    {
        if (this != &rhs) {
            std::swap(flag_, rhs.flag_);
            std::swap(msg_, rhs.msg_);
        }
        return *this;
    }

    /**
     * @brief Move one ZmqMessage into another ZmqMessage
     * @param src
     * @return Status
     */
    Status Move(ZmqMessage &src);

    /**
     * @brief Copy one ZmqMessage into another ZmqMessage
     * @param src
     * @return
     */
    Status Copy(ZmqMessage &src);

    const void *Data() const
    {
        return zmq_msg_data(const_cast<zmq_msg_t *>(&msg_));
    }

    void *Data()
    {
        return zmq_msg_data(&msg_);
    }

    size_t Size() const
    {
        return zmq_msg_size(const_cast<zmq_msg_t *>(&msg_));
    }

    auto *GetHandle()
    {
        return &msg_;
    }

    bool More() const
    {
        return zmq_msg_more(&msg_) > 0;
    }

    bool Empty() const
    {
        return zmq_msg_size(&msg_) == 0u;
    }

    /**
     * @brief Pre-allocate memory buffer size
     * @param size
     * @return Status object
     */
    Status AllocMem(size_t size);

    /**
     * @brief Initialise the message object to represent the content
     * referenced by the buffer located at address data, size bytes long.
     * No copy of data shall be performed and ØMQ shall take ownership of the supplied buffer.
     * @param[in] data Source of the buffer
     * @param[in] size Size of the buffer
     * @param[in] ffn A thread safe function for releasing the buffer
     * @param[in] hint Additional parameter passed to ffn
     * @return Status object
     */
    Status TransferOwnership(void *data, size_t size, zmq_free_fn *ffn, void *hint = nullptr);

    /**
     * @brief A wrapper for referencing a buffer at address data, size bytes long. The caller still owns the buffer and
     * is responsible for freeing the buffer.
     * @param[in] data Source of the buffer
     * @param[in] size Size of the buffer
     * @return Status object
     */
    Status ZeroCopyBuffer(void *data, size_t size)
    {
        return TransferOwnership(data, size, nullptr, nullptr);
    }

    /**
     * @brief Copy a buffer into ZmqMessage
     * @param[in] data Source of the buffer
     * @param[in] size Size of the buffer
     * @return Status object
     */
    Status CopyBuffer(const void *data, size_t size);

    /**
     * @brief Copy a string into ZmqMessage
     * @param str
     * @return Status object
     */
    Status CopyString(const std::string &str)
    {
        return CopyBuffer(str.data(), str.size());
    }

    auto GetMetaProperty(const std::string &property)
    {
        return zmq_msg_gets(&msg_, property.data());
    }

    auto ToString() const
    {
        return std::string(reinterpret_cast<const char *>(Data()), Size());
    }

    friend std::ostream &operator<<(std::ostream &out, const ZmqMessage &msg)
    {
#ifdef WITH_TESTS
        // we don't know whether msg contain sensitive info or not, so can't output the msg in product environment
        out << msg.DebugString();
#else
        (void)msg;
        out << "***";
#endif
        return out;
    }

    bool operator==(const ZmqMessage &other) const;

    bool operator!=(const ZmqMessage &other) const
    {
        return !(*this == other);
    }

    void SetType(ZmqMsgType type)
    {
        flag_ = type;
    }

    ZmqMsgType GetType() const
    {
        return flag_;
    }

private:
    ZmqMsgType flag_;
    zmq_msg_t msg_{};
    Status Close();
    std::string DebugString() const;
};
}  // namespace datasystem
#endif  // DATASYSTEM_COMMON_RPC_ZMQ_MESSAGE_H
