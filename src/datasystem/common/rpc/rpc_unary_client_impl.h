/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2024. All rights reserved.
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
 * Description: Unary client headers.
 */
#ifndef DATASYSTEM_COMMON_RPC_UNARY_CLIENT_IMPL_H
#define DATASYSTEM_COMMON_RPC_UNARY_CLIENT_IMPL_H

#include <variant>

#include "datasystem/common/rpc/zmq/zmq_unary_client_impl.h"

namespace datasystem {
template <typename W, typename R>
class ClientUnaryWriterReader {
public:
    explicit ClientUnaryWriterReader(std::unique_ptr<ClientUnaryWriterReaderImpl<W, R>> &&impl)
        : pimpl_(std::move(impl))
    {
    }

    ~ClientUnaryWriterReader() = default;

    Status Write(const W &pb)
    {
        return std::visit([&pb](auto &pimpl) { return pimpl->Write(pb); }, pimpl_);
    }

    /**
     * @brief Send a payload after sending the request protobuf.
     * @note The option send_payload_option must be set in the proto. Must be called after Write().
     * @param[in] payload Sending payload buffers.
     * @return Status of call.
     */
    Status SendPayload(const std::vector<MemView> &payload)
    {
        return std::visit([&payload](auto &pimpl) { return pimpl->SendPayload(payload); }, pimpl_);
    }

    /**
     * @brief Split a payload into small pieces and send in parallel
     * @param payload
     * @return Status object
     */
    Status AsyncSendPayload(const std::vector<MemView> &payload)
    {
        return std::visit([&payload](auto &pimpl) { return pimpl->AsyncSendPayload(payload); }, pimpl_);
    }

    Status Read(R &pb)
    {
        return std::visit([&pb](auto &pimpl) { return pimpl->Read(pb); }, pimpl_);
    }

    /**
     * @brief Receive a payload after receiving response protobuf.
     * @note The option recv_payload_option must be set in the proto. Must be called after Read().
     * @param[out] recvBuffer receiving payload buffers.
     * @return Status of call.
     */
    Status ReceivePayload(std::vector<RpcMessage> &recvBuffer)
    {
        return std::visit([&recvBuffer](auto &pimpl) { return pimpl->ReceivePayload(recvBuffer); }, pimpl_);
    }

    /**
     * @brief Receive a payload and write directly into user provided memory.
     * @param dest Address of user provided buffer
     * @param sz Size of the destination
     * @return Status of call
     * @note Only V2 MTP can support this form of direct write without memory copy
     */
    Status ReceivePayload(void *dest, size_t sz)
    {
        return std::visit([&dest, &sz](auto &pimpl) { return pimpl->ReceivePayload(dest, sz); }, pimpl_);
    }

    void CleanupOnError(const Status &rc)
    {
        std::visit([&rc](auto &pimpl) { pimpl->CleanupOnError(rc); }, pimpl_);
    }

    bool IsV2Client() const
    {
        return std::visit([](auto &pimpl) { return pimpl->IsV2Client(); }, pimpl_);
    }

private:
    std::variant<std::unique_ptr<ClientUnaryWriterReaderImpl<W, R>>> pimpl_;
};
}  // namespace datasystem
#endif  // DATASYSTEM_COMMON_RPC_UNARY_CLIENT_IMPL_H
