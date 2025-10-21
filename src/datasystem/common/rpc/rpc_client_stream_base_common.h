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
 */
#ifndef DATASYSTEM_COMMON_RPC_RPC_CLIENT_STREAM_BASE_COMMON_H
#define DATASYSTEM_COMMON_RPC_RPC_CLIENT_STREAM_BASE_COMMON_H

#include "datasystem/utils/status.h"
#include "datasystem/common/rpc/rpc_message.h"
#include "datasystem/common/rpc/rpc_stub.h"

namespace datasystem {
template <typename W, typename R>
ClientWriterReader<W, R>::ClientWriterReader(std::unique_ptr<ClientWriterReaderImpl<W, R>> &&impl)
{
    pimpl_ = std::move(impl);
}

template <typename W, typename R>
ClientWriterReader<W, R>::~ClientWriterReader() = default;

template <typename W, typename R>
Status ClientWriterReader<W, R>::Write(const W &pb)
{
    return pimpl_->Write(pb);
}

template <typename W, typename R>
Status ClientWriterReader<W, R>::Read(R &pb)
{
    return pimpl_->Read(pb);
}

template <typename W, typename R>
Status ClientWriterReader<W, R>::Finish()
{
    return pimpl_->Finish();
}

template <typename W, typename R>
Status ClientWriterReader<W, R>::SendPayload(const std::vector<MemView> &payload)
{
    return pimpl_->SendPayload(payload);
}

template <typename W, typename R>
Status ClientWriterReader<W, R>::ReceivePayload(std::vector<RpcMessage> &recvBuffer)
{
    return pimpl_->ReceivePayload(recvBuffer);
}

template <typename W>
ClientWriter<W>::ClientWriter(std::unique_ptr<ClientWriterImpl<W>> &&impl)
{
    pimpl_ = std::move(impl);
}

template <typename W>
ClientWriter<W>::~ClientWriter() = default;

template <typename W>
Status ClientWriter<W>::Write(const W &pb)
{
    return pimpl_->Write(pb);
}

template <typename W>
template <typename R>
Status ClientWriter<W>::Read(R &pb)
{
    return pimpl_->Read(pb);
}

template <typename W>
Status ClientWriter<W>::Finish()
{
    return pimpl_->Finish();
}

template <typename W>
Status ClientWriter<W>::SendPayload(const std::vector<MemView> &payload)
{
    return pimpl_->SendPayload(payload);
}

template <typename W>
Status ClientWriter<W>::ReceivePayload(std::vector<RpcMessage> &recvBuffer)
{
    return pimpl_->ReceivePayload(recvBuffer);
}

template <typename R>
ClientReader<R>::ClientReader(std::unique_ptr<ClientReaderImpl<R>> &&impl)
{
    pimpl_ = std::move(impl);
}

template <typename R>
ClientReader<R>::~ClientReader() = default;

template <typename R>
Status ClientReader<R>::Read(R &pb)
{
    return pimpl_->Read(pb);
}

template <typename R>
template <typename W>
Status ClientReader<R>::Write(const W &pb)
{
    return pimpl_->Write(pb);
}

template <typename R>
Status ClientReader<R>::SendPayload(const std::vector<MemView> &payload)
{
    return pimpl_->SendPayload(payload);
}

template <typename R>
Status ClientReader<R>::ReceivePayload(std::vector<RpcMessage> &recvBuffer)
{
    return pimpl_->ReceivePayload(recvBuffer);
}

template <typename R>
Status ClientReader<R>::Finish()
{
    return pimpl_->Finish();
}
}  // namespace datasystem
#endif  // DATASYSTEM_COMMON_RPC_RPC_CLIENT_STREAM_BASE_COMMON_H
