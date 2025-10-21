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
    explicit ClientWriterReader(std::unique_ptr<ClientWriterReaderImpl<W, R>> &&impl);

    ~ClientWriterReader();

    Status Write(const W &pb);

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
    std::unique_ptr<ClientWriterReaderImpl<W, R>> pimpl_;
};

template <typename W>
class ClientWriter {
public:
    explicit ClientWriter(std::unique_ptr<ClientWriterImpl<W>> &&impl);

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
    std::unique_ptr<ClientWriterImpl<W>> pimpl_;
};

template <typename R>
class ClientReader {
public:
    explicit ClientReader(std::unique_ptr<ClientReaderImpl<R>> &&impl);

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
    std::unique_ptr<ClientReaderImpl<R>> pimpl_;
};
}  // namespace datasystem
#endif  // DATASYSTEM_COMMON_RPC_STUB_H
