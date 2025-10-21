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
 * Description: A message class encapsulating the zmq::message_t rawdata.
 * This file does not have any connection.
 */
#include "datasystem/common/rpc/rpc_message.h"

#include "datasystem/common/perf/perf_manager.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {
RpcMessage::RpcMessage(RpcMessage &&msg) noexcept
{
    *this = std::move(msg);
}

RpcMessage &RpcMessage::operator=(RpcMessage &&msg) noexcept
{
    // ZMQ message handling
    msg_ = std::move(msg.GetMsg());
    return *this;
}

RpcMessage::RpcMessage(ZmqMessage msg) : msg_(std::move(msg))
{
}

ZmqMessage &RpcMessage::GetMsg()
{
    return msg_;
}

void RpcMessage::MoveToMsg(ZmqMessage &msg)
{
    msg = std::move(msg_);
}

bool RpcMessage::operator==(const RpcMessage &other) const
{
    // ZMQ message handling
    return msg_ == const_cast<RpcMessage &>(other).msg_;
}

bool RpcMessage::operator!=(const RpcMessage &other) const
{
    // ZMQ message handling
    return msg_ != const_cast<RpcMessage &>(other).msg_;
}

std::string RpcMessage::ToString()
{
    // ZMQ message handling
    return msg_.ToString();
}

bool RpcMessage::Empty() const
{
    // ZMQ message handling
    return msg_.Empty();
}

void RpcMessage::Clear()
{
    ZmqMessage msg;
    msg_ = std::move(msg);
}

Status RpcMessage::Resize(size_t len)
{
    // ZMQ message handling
    ZmqMessage msg;
    RETURN_IF_NOT_OK(msg.AllocMem(len));
    msg_ = std::move(msg);
    return Status::OK();
}

Status RpcMessage::TransferOwnership(void *data, size_t size, MsgFreeFn *ffn, void *hint)
{
    // ZMQ message handling
    return msg_.TransferOwnership(data, size, ffn, hint);
}

Status RpcMessage::CopyString(const std::string &str)
{
    // ZMQ message handling
    return msg_.CopyString(str);
}

RpcMessage::~RpcMessage()
{
}

void *RpcMessage::Data() const
{
    // ZMQ message handling
    return const_cast<void *>(msg_.Data());
}

size_t RpcMessage::Size() const
{
    // ZMQ message handling
    return msg_.Size();
}

Status RpcMessage::AllocMem(size_t size)
{
    // ZMQ message handling
    return msg_.AllocMem(size);
}

Status RpcMessage::ZeroCopyBuffer(void *data, size_t size)
{
    // ZMQ message handling
    return msg_.ZeroCopyBuffer(data, size);
}

Status RpcMessage::CopyBuffer(const void *data, size_t size)
{
    // ZMQ message handling
    return msg_.CopyBuffer(data, size);
}
}  // namespace datasystem
