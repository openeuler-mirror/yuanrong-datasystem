/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
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
 * Description: Abstract base class for client-side bidi-streaming WriterReader.
 * Both ZMQ and brpc implementations inherit from this interface.
 */
#ifndef DATASYSTEM_COMMON_RPC_CLIENT_WRITER_READER_BASE_H
#define DATASYSTEM_COMMON_RPC_CLIENT_WRITER_READER_BASE_H

#include <vector>

#include "datasystem/common/rpc/mem_view.h"
#include "datasystem/common/rpc/rpc_message.h"
#include "datasystem/utils/status.h"

namespace datasystem {

/**
 * @brief Pure virtual interface for client-side bidi-streaming.
 *
 * Defines the contract that both ClientWriterReaderImpl (ZMQ) and
 * BrpcClientStreamWriterReader (brpc) must implement.
 *
 * @tparam W Write (request) protobuf type.
 * @tparam R Read (response) protobuf type.
 */
template <typename W, typename R>
class ClientWriterReaderBase {
public:
    virtual ~ClientWriterReaderBase() = default;

    /**
     * @brief Write a request protobuf to the server.
     * @param[in] pb The request protobuf.
     * @return Status of the call.
     */
    virtual Status Write(const W &pb) = 0;

    /**
     * @brief Read a response protobuf from the server.
     * @param[out] pb The response protobuf.
     * @return Status of the call.
     */
    virtual Status Read(R &pb) = 0;

    /**
     * @brief Done sending sequence of protobuf to the server.
     * @return Status of the call.
     */
    virtual Status Finish() = 0;

    /**
     * @brief Send a payload after sending the request protobuf.
     * @param[in] payload Sending payload buffers.
     * @return Status of the call.
     */
    virtual Status SendPayload(const std::vector<MemView> &payload) = 0;

    /**
     * @brief Receive a payload after receiving response protobuf.
     * @param[out] recvBuffer Receiving payload buffers.
     * @return Status of the call.
     */
    virtual Status ReceivePayload(std::vector<RpcMessage> &recvBuffer) = 0;

    /**
     * @brief Non-blocking close. Called by the wrapper destructor so the brpc
     * handler can trigger async on_closed (which clears its self-keepalive).
     * Default no-op for ZMQ; brpc overrides.
     */
    virtual void Close() {}
};

}  // namespace datasystem
#endif  // DATASYSTEM_COMMON_RPC_CLIENT_WRITER_READER_BASE_H
