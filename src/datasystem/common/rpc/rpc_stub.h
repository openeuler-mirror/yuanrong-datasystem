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

#include <variant>

#include "datasystem/common/rpc/brpc_client_stream_impl.h"
#include "datasystem/common/rpc/client_writer_reader_base.h"
#include "datasystem/common/rpc/rpc_message.h"
#include "datasystem/common/rpc/rpc_options.h"
#include "datasystem/protos/meta_zmq.pb.h"
#include "datasystem/utils/status.h"

namespace datasystem {

template <typename W, typename R>
class ClientUnaryWriterReaderImpl;
template <typename W, typename R>
class ClientUnaryWriterReader;
template <typename W, typename R>
class ClientWriterReaderImpl;
template <typename W>
class ClientWriterImpl;
template <typename R>
class ClientReaderImpl;

template <typename W, typename R>
class ClientWriterReader {
public:
    explicit ClientWriterReader(std::shared_ptr<ClientWriterReaderBase<W, R>> &&impl)
        : pimpl_(std::move(impl))
    {
    }

    // Trigger non-blocking Close() so the brpc handler fires on_closed and its
    // self-keepalive can release (ZMQ Close() is a no-op). shared_ptr so the brpc
    // handler can hold a self-keepalive keeping itself alive past this drop.
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

    /**
     * @brief Done sending sequence of protobuf to the server.
     * @return Status of call.
     */
    Status Finish()
    {
        return pimpl_->Finish();
    }

    /**
     * @brief Send a payload after sending the request protobuf.
     * @note The option send_payload_option must be set in the proto. Must be called after Write().
     * @param[in] payload Sending payload buffers.
     * @return Status of call.
     */
    Status SendPayload(const std::vector<MemView> &payload)
    {
        return pimpl_->SendPayload(payload);
    }

    /**
     * @brief Receive a payload after receiving response protobuf.
     * @note The option recv_payload_option must be set in the proto. Must be called after Read().
     * @param[out] recvBuffer receiving payload buffers.
     * @return Status of call.
     */
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
    explicit ClientWriter(std::unique_ptr<ClientWriterImpl<W>> &&impl);

    explicit ClientWriter(std::unique_ptr<BrpcClientWriterImpl<W>> &&impl);

    ~ClientWriter();

    Status Write(const W &pb);

    template <typename R>
    Status Read(R &pb);

    /**
     * @brief Done sending sequence of protobuf to the server.
     * @return Status of call.
     */
    Status Finish();

    /**
     * @brief Send a payload after sending the request protobuf.
     * @note The option send_payload_option must be set in the proto. Must be called after Write().
     * @param[in] payload Sending payload buffers.
     * @return Status of call.
     */
    Status SendPayload(const std::vector<MemView> &payload);

    /**
     * @brief Receive a payload after receiving response protobuf.
     * @note The option recv_payload_option must be set in the proto. Must be called after Read().
     * @param[out] recvBuffer receiving payload buffers.
     * @return Status of call.
     */
    Status ReceivePayload(std::vector<RpcMessage> &recvBuffer);

private:
    std::variant<std::unique_ptr<ClientWriterImpl<W>>,
                 std::unique_ptr<BrpcClientWriterImpl<W>>> pimpl_;
};

template <typename R>
class ClientReader {
public:
    explicit ClientReader(std::unique_ptr<ClientReaderImpl<R>> &&impl);

    explicit ClientReader(std::shared_ptr<BrpcClientReaderImpl<R>> &&impl);

    ~ClientReader();

    Status Read(R &pb);

    template <typename W>
    Status Write(const W &pb);

    /**
     * @brief Done sending sequence of protobuf to the server.
     * @return Status of call.
     */
    Status Finish();

    /**
     * @brief Send a payload after sending the request protobuf.
     * @note The option send_payload_option must be set in the proto. Must be called after Write().
     * @param[in] payload Sending payload buffers.
     * @return Status of call.
     */
    Status SendPayload(const std::vector<MemView> &payload);

    /**
     * @brief Receive a payload after receiving response protobuf.
     * @note The option recv_payload_option must be set in the proto. Must be called after Read().
     * @param[out] recvBuffer receiving payload buffers.
     * @return Status of call.
     */
    Status ReceivePayload(std::vector<RpcMessage> &recvBuffer);

private:
    // Brpc branch is shared_ptr: the brpc handler keeps itself alive via a
    // self-keepalive until on_closed fires (see BrpcClientReaderImpl), so its
    // lifetime must be shared, not uniquely owned. ZMQ stays unique_ptr.
    std::variant<std::unique_ptr<ClientReaderImpl<R>>,
                 std::shared_ptr<BrpcClientReaderImpl<R>>> pimpl_;
};
}  // namespace datasystem
#endif  // DATASYSTEM_COMMON_RPC_STUB_H
