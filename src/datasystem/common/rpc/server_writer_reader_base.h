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
 * Description: Abstract base class for server-side bidi-streaming WriterReader.
 * Both ZMQ and brpc implementations inherit from this interface.
 */
#ifndef DATASYSTEM_COMMON_RPC_SERVER_WRITER_READER_BASE_H
#define DATASYSTEM_COMMON_RPC_SERVER_WRITER_READER_BASE_H

#include <vector>

#include "datasystem/common/rpc/mem_view.h"
#include "datasystem/common/rpc/rpc_message.h"
#include "datasystem/utils/status.h"

namespace datasystem {

/**
 * @brief Pure virtual interface for server-side bidi-streaming.
 *
 * Defines the contract that both ServerWriterReaderImpl (ZMQ) and
 * BrpcServerWriterReaderImpl (brpc) must implement.
 *
 * @tparam W Write (response) protobuf type.
 * @tparam R Read (request) protobuf type.
 */
template <typename W, typename R>
class ServerWriterReaderBase {
public:
    virtual ~ServerWriterReaderBase() = default;

    /**
     * @brief Send a status to the client.
     * @param[in] rc The status to send.
     * @return Status of the call.
     */
    virtual Status SendStatus(const Status &rc) = 0;

    /**
     * @brief Read a request protobuf from the client.
     * @param[out] pb The request protobuf.
     * @return Status of the call.
     */
    virtual Status Read(R &pb) = 0;

    /**
     * @brief Write a response protobuf to the client.
     * @param[in] pb The response protobuf.
     * @return Status of the call.
     */
    virtual Status Write(const W &pb) = 0;

    /**
     * @brief Done sending sequence of protobuf to the client.
     * @return Status of the call.
     */
    virtual Status Finish() = 0;

    /**
     * @brief Send a payload (RpcMessage vector) to the client.
     * @param[in] buffer Payload buffers.
     * @return Status of the call.
     */
    virtual Status SendPayload(std::vector<RpcMessage> &buffer) = 0;

    /**
     * @brief Send a payload (MemView vector) to the client.
     * @param[in] payload Payload buffers.
     * @return Status of the call.
     */
    virtual Status SendPayload(const std::vector<MemView> &payload) = 0;

    /**
     * @brief Receive a payload from the client.
     * @param[out] payload Receiving payload buffers.
     * @return Status of the call.
     */
    virtual Status ReceivePayload(std::vector<RpcMessage> &payload) = 0;
};

}  // namespace datasystem
#endif  // DATASYSTEM_COMMON_RPC_SERVER_WRITER_READER_BASE_H
