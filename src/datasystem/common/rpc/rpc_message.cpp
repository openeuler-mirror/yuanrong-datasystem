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
#include "datasystem/common/rpc/rpc_message.h"

#include <securec.h>

#include <cstdlib>
#include <cstring>
#include <limits>

namespace datasystem {
void RpcMessage::FreeBuffer()
{
    if (data_ != nullptr) {
        if (freeFn_ != nullptr) {
            freeFn_(data_, hint_);
        } else if (owned_) {
            free(data_);
        }
    }
    data_ = nullptr;
    size_ = 0;
    freeFn_ = nullptr;
    hint_ = nullptr;
    owned_ = false;
}

RpcMessage::RpcMessage(RpcMessage &&msg) noexcept
{
    *this = std::move(msg);
}

RpcMessage &RpcMessage::operator=(RpcMessage &&msg) noexcept
{
    if (this != &msg) {
        FreeBuffer();
        data_ = msg.data_;
        size_ = msg.size_;
        freeFn_ = msg.freeFn_;
        hint_ = msg.hint_;
        owned_ = msg.owned_;
        type_ = msg.type_;
        more_ = msg.more_;
        msg.data_ = nullptr;
        msg.size_ = 0;
        msg.freeFn_ = nullptr;
        msg.hint_ = nullptr;
        msg.owned_ = false;
        msg.type_ = MsgType::NONE;
        msg.more_ = false;
    }
    return *this;
}

RpcMessage::~RpcMessage()
{
    FreeBuffer();
}

bool RpcMessage::operator==(const RpcMessage &other) const
{
    if (size_ != other.size_) {
        return false;
    }
    return size_ == 0 || std::memcmp(data_, other.data_, size_) == 0;
}

bool RpcMessage::operator!=(const RpcMessage &other) const
{
    return !(*this == other);
}

std::string RpcMessage::ToString()
{
    if (data_ == nullptr || size_ == 0) {
        return std::string();
    }
    return std::string(reinterpret_cast<const char *>(data_), size_);
}

bool RpcMessage::Empty() const
{
    return size_ == 0u;
}

void RpcMessage::Clear()
{
    FreeBuffer();
    type_ = MsgType::NONE;
    more_ = false;
}

Status RpcMessage::Resize(size_t len)
{
    Clear();
    return AllocMem(len);
}

Status RpcMessage::TransferOwnership(void *data, size_t size, MsgFreeFn *ffn, void *hint)
{
    FreeBuffer();
    if (data == nullptr && size != 0) {
        RETURN_STATUS_LOG_ERROR(K_INVALID, "data is nullptr, but size is not zero");
    }
    data_ = static_cast<uint8_t *>(data);
    size_ = size;
    freeFn_ = ffn;
    hint_ = hint;
    owned_ = (ffn != nullptr);
    return Status::OK();
}

Status RpcMessage::CopyString(const std::string &str)
{
    return CopyBuffer(str.data(), str.size());
}

Status RpcMessage::AllocMem(size_t size)
{
    FreeBuffer();
    if (size == 0) {
        return Status::OK();
    }
    CHECK_FAIL_RETURN_STATUS(size <= std::numeric_limits<size_t>::max(), K_OUT_OF_MEMORY,
                             "Alloc size exceeds the addressable range");
    data_ = static_cast<uint8_t *>(malloc(size));
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(data_ != nullptr, K_OUT_OF_MEMORY,
                                         FormatString("Malloc %zu bytes failed", size));
    size_ = size;
    owned_ = true;
    return Status::OK();
}

Status RpcMessage::ZeroCopyBuffer(void *data, size_t size)
{
    // Reference-only: the caller keeps ownership and is responsible for freeing.
    CHECK_FAIL_RETURN_STATUS(data != nullptr || size == 0, K_INVALID, "data is nullptr, but size is not zero");
    FreeBuffer();
    data_ = static_cast<uint8_t *>(data);
    size_ = size;
    freeFn_ = nullptr;
    hint_ = nullptr;
    owned_ = false;
    return Status::OK();
}

Status RpcMessage::CopyBuffer(const void *data, size_t size)
{
    FreeBuffer();
    if (size == 0) {
        return Status::OK();
    }
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(data != nullptr, K_INVALID, "data is nullptr but size is not zero");
    RETURN_IF_NOT_OK(AllocMem(size));
    errno_t rc = memcpy_s(data_, size, data, size);
    if (rc != EOK) {
        FreeBuffer();
        RETURN_STATUS_LOG_ERROR(K_RUNTIME_ERROR, "Failed to copy memory to message");
    }
    return Status::OK();
}

void *RpcMessage::Data() const
{
    return data_;
}

size_t RpcMessage::Size() const
{
    return size_;
}
}  // namespace datasystem
