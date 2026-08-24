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
 * Description: Rpc generic Stub
 */
#ifndef DATASYSTEM_COMMON_RPC_STUB_H
#define DATASYSTEM_COMMON_RPC_STUB_H

#include <memory>
#include <vector>

#include "datasystem/common/rpc/brpc_client_stream_impl.h"
#include "datasystem/common/rpc/client_writer_reader_base.h"
#include "datasystem/common/rpc/rpc_message.h"
#include "datasystem/common/rpc/rpc_options.h"
#include "datasystem/protos/meta_zmq.pb.h"
#include "datasystem/utils/status.h"

namespace datasystem {

template <typename W, typename R>
class ClientWriterReader {
public:
    explicit ClientWriterReader(std::shared_ptr<ClientWriterReaderBase<W, R>> &&impl)
        : pimpl_(std::move(impl))
    {
    }

    ~ClientWriterReader()
    {
        if (pimpl_) {
            pimpl_->Close();
        }
    }

    Status Write(const W &pb)
    {
        return pimpl_->Write(pb);
    }

    Status Read(R &pb)
    {
        return pimpl_->Read(pb);
    }

    Status Finish()
    {
        return pimpl_->Finish();
    }

    Status SendPayload(const std::vector<MemView> &payload)
    {
        return pimpl_->SendPayload(payload);
    }

    Status ReceivePayload(std::vector<RpcMessage> &recvBuffer)
    {
        return pimpl_->ReceivePayload(recvBuffer);
    }

private:
    std::shared_ptr<ClientWriterReaderBase<W, R>> pimpl_;
};

template <typename W>
class ClientWriter {
public:
    explicit ClientWriter(std::unique_ptr<BrpcClientWriterImpl<W>> &&impl)
        : pimpl_(std::move(impl))
    {
    }

    ~ClientWriter() = default;

    Status Write(const W &pb)
    {
        return pimpl_->Write(pb);
    }

    template <typename R>
    Status Read(R &pb)
    {
        return pimpl_->Read(pb);
    }

    Status Finish()
    {
        return pimpl_->Finish();
    }

    Status SendPayload(const std::vector<MemView> &payload)
    {
        return pimpl_->SendPayload(payload);
    }

    Status ReceivePayload(std::vector<RpcMessage> &recvBuffer)
    {
        return pimpl_->ReceivePayload(recvBuffer);
    }

private:
    std::unique_ptr<BrpcClientWriterImpl<W>> pimpl_;
};

template <typename R>
class ClientReader {
public:
    explicit ClientReader(std::shared_ptr<BrpcClientReaderImpl<R>> &&impl)
        : pimpl_(std::move(impl))
    {
    }

    ~ClientReader()
    {
        if (pimpl_) {
            pimpl_->Close();
        }
    }

    Status Read(R &pb)
    {
        return pimpl_->Read(pb);
    }

    template <typename W>
    Status Write(const W &pb)
    {
        return pimpl_->Write(pb);
    }

    Status Finish()
    {
        return pimpl_->Finish();
    }

    Status SendPayload(const std::vector<MemView> &payload)
    {
        return pimpl_->SendPayload(payload);
    }

    Status ReceivePayload(std::vector<RpcMessage> &recvBuffer)
    {
        return pimpl_->ReceivePayload(recvBuffer);
    }

private:
    std::shared_ptr<BrpcClientReaderImpl<R>> pimpl_;
};
}  // namespace datasystem
#endif  // DATASYSTEM_COMMON_RPC_STUB_H
