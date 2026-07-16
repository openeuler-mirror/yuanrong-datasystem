/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2023. All rights reserved.
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
 * Description: RPC client streaming common code
 * It is in order to reduce code duplication
 *
 * Note: ClientWriterReader methods are now defined inline in rpc_stub.h
 * using virtual dispatch through ClientWriterReaderBase.
 */
#ifndef DATASYSTEM_COMMON_RPC_RPC_CLIENT_STREAM_BASE_COMMON_H
#define DATASYSTEM_COMMON_RPC_RPC_CLIENT_STREAM_BASE_COMMON_H

#include "datasystem/utils/status.h"
#include "datasystem/common/rpc/rpc_message.h"
#include "datasystem/common/rpc/rpc_stub.h"

namespace datasystem {
template <typename W>
ClientWriter<W>::ClientWriter(std::unique_ptr<ClientWriterImpl<W>> &&impl)
{
    pimpl_ = std::move(impl);
}

template <typename W>
ClientWriter<W>::ClientWriter(std::unique_ptr<BrpcClientWriterImpl<W>> &&impl)
{
    pimpl_ = std::move(impl);
}

template <typename W>
ClientWriter<W>::~ClientWriter() = default;

template <typename W>
Status ClientWriter<W>::Write(const W &pb)
{
    return std::visit([&pb](auto &p) { return p->Write(pb); }, pimpl_);
}

template <typename W>
template <typename R>
Status ClientWriter<W>::Read(R &pb)
{
    return std::visit([&pb](auto &p) { return p->Read(pb); }, pimpl_);
}

template <typename W>
Status ClientWriter<W>::Finish()
{
    return std::visit([](auto &p) { return p->Finish(); }, pimpl_);
}

template <typename W>
Status ClientWriter<W>::SendPayload(const std::vector<MemView> &payload)
{
    return std::visit([&payload](auto &p) { return p->SendPayload(payload); }, pimpl_);
}

template <typename W>
Status ClientWriter<W>::ReceivePayload(std::vector<RpcMessage> &recvBuffer)
{
    return std::visit([&recvBuffer](auto &p) { return p->ReceivePayload(recvBuffer); }, pimpl_);
}

template <typename R>
ClientReader<R>::ClientReader(std::unique_ptr<ClientReaderImpl<R>> &&impl)
{
    pimpl_ = std::move(impl);
}

template <typename R>
ClientReader<R>::ClientReader(std::shared_ptr<BrpcClientReaderImpl<R>> &&impl)
{
    pimpl_ = std::move(impl);
}

template <typename R>
ClientReader<R>::~ClientReader()
{
    // Trigger non-blocking Close() so brpc fires on_closed and the brpc handler's
    // self-keepalive can release (ZMQ Close() is a no-op). Without this, dropping
    // a brpc stream without Finish() would never close it -> on_closed never fires
    // -> keepalive leak.
    std::visit([](auto &p) { if (p) { p->Close(); } }, pimpl_);
}

template <typename R>
Status ClientReader<R>::Read(R &pb)
{
    return std::visit([&pb](auto &p) { return p->Read(pb); }, pimpl_);
}

template <typename R>
template <typename W>
Status ClientReader<R>::Write(const W &pb)
{
    return std::visit([&pb](auto &p) { return p->Write(pb); }, pimpl_);
}

template <typename R>
Status ClientReader<R>::SendPayload(const std::vector<MemView> &payload)
{
    return std::visit([&payload](auto &p) { return p->SendPayload(payload); }, pimpl_);
}

template <typename R>
Status ClientReader<R>::ReceivePayload(std::vector<RpcMessage> &recvBuffer)
{
    return std::visit([&recvBuffer](auto &p) { return p->ReceivePayload(recvBuffer); }, pimpl_);
}

template <typename R>
Status ClientReader<R>::Finish()
{
    return std::visit([](auto &p) { return p->Finish(); }, pimpl_);
}
}  // namespace datasystem
#endif  // DATASYSTEM_COMMON_RPC_RPC_CLIENT_STREAM_BASE_COMMON_H
