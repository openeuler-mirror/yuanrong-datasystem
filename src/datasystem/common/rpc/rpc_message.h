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
 * Description: Transport-neutral RPC message buffer.
 */
#ifndef DATASYSTEM_COMMON_RPC_RPC_MESSAGE_H
#define DATASYSTEM_COMMON_RPC_RPC_MESSAGE_H

#include <cstddef>
#include <cstdint>
#include <deque>
#include <ostream>
#include <string>
#include <utility>

#include "datasystem/common/rpc/mem_view.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/protos/utils.pb.h"

namespace datasystem {
// Wire flags for payload framing: DONTWAIT=1 (non-blocking), SNDMORE=2 (multi-frame).
enum class RpcSendFlags : int { NONE = 0, DONTWAIT = 1, SNDMORE = 2 };
enum class RpcRecvFlags : int { NONE = 0, DONTWAIT = 1 };
typedef void(MsgFreeFn)(void *data, void *hint);
class RpcMessage;
using RpcMsgFrames = std::deque<RpcMessage>;
using RpcMsgFramesRef = std::deque<RpcMessage> &;

class RpcMessage {
public:
    enum class MsgType : uint16_t { NONE = 0, PAYLOAD_SZ, DECODER };

    RpcMessage() = default;

    RpcMessage(const RpcMessage &) = delete;
    RpcMessage &operator=(const RpcMessage &) = delete;
    RpcMessage(RpcMessage &&msg) noexcept;

    RpcMessage &operator=(RpcMessage &&msg) noexcept;

    virtual ~RpcMessage();

    void *Data() const;

    size_t Size() const;

    bool operator==(const RpcMessage &other) const;

    bool operator!=(const RpcMessage &other) const;

    std::string ToString();

    bool Empty() const;

    void Clear();

    // Frame metadata: payload-size / decoder markers and multi-frame continuation.
    void SetType(MsgType type)
    {
        type_ = type;
    }

    MsgType GetType() const
    {
        return type_;
    }

    void SetMore(bool more)
    {
        more_ = more;
    }

    bool More() const
    {
        return more_;
    }

    Status Resize(size_t len);

    // Takes ownership of data: ffn(data, hint) is called on destruction (ffn may be null
    // for a caller-owned buffer that is only referenced).
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
     * @brief Copy a buffer into this message
     * @param[in] data Source of the buffer
     * @param[in] size Size of the buffer
     * @return Status object
     */
    Status CopyBuffer(const void *data, size_t size);

protected:
    void FreeBuffer();

    uint8_t *data_ = nullptr;
    size_t size_ = 0;
    // Owning free function; null means the buffer is referenced (caller-owned) or malloc'ed
    // via AllocMem/CopyBuffer (freed with free()).
    MsgFreeFn *freeFn_ = nullptr;
    void *hint_ = nullptr;
    bool owned_ = false;
    MsgType type_ = MsgType::NONE;
    bool more_ = false;
};

template <typename T>
inline Status SerializeToRpcMessage(const T &pb, RpcMessage &dest)
{
    auto sz = pb.ByteSizeLong();
    RETURN_IF_NOT_OK(dest.AllocMem(sz));
    bool rc = pb.SerializeToArray(dest.Data(), sz);
    CHECK_FAIL_RETURN_STATUS(rc, K_RUNTIME_ERROR, "Serialization error");
    return Status::OK();
}

inline RpcMessage StatusToRpcMessage(const Status &st)
{
    RpcMessage errorMsg;
    ErrorInfoPb err;
    err.set_error_code(st.GetCode());
    err.set_error_msg(st.GetMsg());
    Status tmpRc = SerializeToRpcMessage<ErrorInfoPb>(err, errorMsg);
    if (tmpRc.IsError()) {
        LOG(ERROR) << "SerializeToRpcMessage Fail, status: " << tmpRc.ToString();
    }
    return errorMsg;
}

inline std::ostream &operator<<(std::ostream &out, const RpcMessage &msg)
{
    (void)msg;
    out << "***";
    return out;
}
}  // namespace datasystem
#endif  // DATASYSTEM_COMMON_RPC_RPC_MESSAGE_H
