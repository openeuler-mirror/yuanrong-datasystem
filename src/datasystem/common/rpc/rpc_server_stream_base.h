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
 * Description: RPC generic server streaming api.
 * Including stream RPC and non-blocking unary RPC.
 * In stream RPC, we have three combinations of reader and writer streaming mode for client and server, respectively.
 * In non-blocking unary RPC, we have ServerUnaryWriterReader.
 */
#ifndef DATASYSTEM_COMMON_RPC_SERVER_STREAM_BASE_H
#define DATASYSTEM_COMMON_RPC_SERVER_STREAM_BASE_H

#include <variant>

#include "datasystem/common/rpc/rpc_message.h"
#include "datasystem/common/rpc/zmq/zmq_server_stream_base.h"
#include "datasystem/common/log/log_helper.h"

namespace datasystem {
template <class... Ts>
struct overloaded : Ts... {
    using Ts::operator()...;
};
template <class... Ts>
overloaded(Ts...) -> overloaded<Ts...>;
/**
 * Only server side is streaming.
 * @tparam W Stream RPC mode, WritePb type.
 */
template <typename W>
class ServerWriter {
public:
    explicit ServerWriter(std::unique_ptr<ServerWriterImpl<W>> &&impl) : pimpl_(std::move(impl))
    {
    }

    ~ServerWriter() = default;

    Status SendStatus(const Status &rc)
    {
        return std::visit([&rc](auto &pimpl) { return pimpl->SendStatus(rc); }, pimpl_);
    }

    template <typename R>
    Status ReadPb(R &pb)
    {
        return std::visit(
            overloaded{ [&pb](std::unique_ptr<ServerWriterImpl<W>> &pimpl) { return pimpl->ReadPb(pb); },
                        [](auto &pimpl) { return Status(K_RUNTIME_ERROR, "Unexpected ReadPb function call."); }
            },
            pimpl_);
    }

    Status Write(const W &pb)
    {
        return std::visit([&pb](auto &pimpl) { return pimpl->Write(pb); }, pimpl_);
    }

    Status Finish()
    {
        return std::visit([](auto &pimpl) { return pimpl->Finish(); }, pimpl_);
    }

    Status SendPayload(std::vector<RpcMessage> &buffer)
    {
        return std::visit([&buffer](auto &pimpl) { return pimpl->SendPayload(buffer); }, pimpl_);
    }

    Status SendPayload(const std::vector<MemView> &payload)
    {
        return std::visit([&payload](auto &pimpl) { return pimpl->SendPayload(payload); }, pimpl_);
    }

    Status ReceivePayload(std::vector<RpcMessage> &payload)
    {
        return std::visit([&payload](auto &pimpl) { return pimpl->ReceivePayload(payload); }, pimpl_);
    }

private:
    std::variant<std::unique_ptr<ServerWriterImpl<W>>> pimpl_;
};

/**
 * Only the client side is streaming.
 */
template <typename R>
class ServerReader {
public:
    explicit ServerReader(std::unique_ptr<ServerReaderImpl<R>> &&impl) : pimpl_(std::move(impl))
    {
    }

    ~ServerReader() = default;

    Status SendStatus(const Status &rc)
    {
        return std::visit(
            overloaded{ [&rc](std::unique_ptr<ServerReaderImpl<R>> &pimpl) { return pimpl->SendStatus(rc); },
                        [](auto &pimpl) { return Status(K_RUNTIME_ERROR, "Unexpected SendStatus function call."); }
            },
            pimpl_);
    }

    Status Read(R &pb)
    {
        return std::visit([&pb](auto &pimpl) { return pimpl->Read(pb); }, pimpl_);
    }

    Status ReceivePayload(std::vector<RpcMessage> &payload)
    {
        return std::visit([&payload](auto &pimpl) { return pimpl->ReceivePayload(payload); }, pimpl_);
    }

    template <typename W>
    Status WritePb(const W &pb)
    {
        return std::visit(
            overloaded{ [&pb](std::unique_ptr<ServerReaderImpl<R>> &pimpl) { return pimpl->WritePb(pb); },
                        [](auto &pimpl) { return Status(K_RUNTIME_ERROR, "Unexpected WritePb function call."); }
            },
            pimpl_);
    }

    Status SendPayload(std::vector<RpcMessage> &buffer)
    {
        return std::visit(
            overloaded{ [&buffer](std::unique_ptr<ServerReaderImpl<R>> &pimpl) { return pimpl->SendPayload(buffer); },
                        [](auto &pimpl) { return Status(K_RUNTIME_ERROR, "Unexpected SendPayload function call."); }
            },
            pimpl_);
    }

    Status SendPayload(const std::vector<MemView> &payload)
    {
        return std::visit([&payload](auto &pimpl) { return pimpl->SendPayload(payload); }, pimpl_);
    }

    Status Finish()
    {
        return std::visit([](auto &pimpl) { return pimpl->Finish(); }, pimpl_);
    }

private:
    std::variant<std::unique_ptr<ServerReaderImpl<R>>> pimpl_;
};

/**
 * Both sides are streaming.
 * @tparam W Stream RPC mode, WritePb type.
 * @tparam R Stream RPC mode, ReadPb type.
 */
template <typename W, typename R>
class ServerWriterReader {
public:
    explicit ServerWriterReader(std::unique_ptr<ServerWriterReaderImpl<W, R>> &&impl) : pimpl_(std::move(impl))
    {
    }

    ~ServerWriterReader() = default;

    Status SendStatus(const Status &rc)
    {
        return std::visit([&rc](auto &pimpl) { return pimpl->SendStatus(rc); }, pimpl_);
    }

    Status Read(R &pb)
    {
        return std::visit([&pb](auto &pimpl) { return pimpl->Read(pb); }, pimpl_);
    }

    Status Write(const W &pb)
    {
        return std::visit([&pb](auto &pimpl) { return pimpl->Write(pb); }, pimpl_);
    }

    Status Finish()
    {
        return std::visit([](auto &pimpl) { return pimpl->Finish(); }, pimpl_);
    }

    Status SendPayload(std::vector<RpcMessage> &buffer)
    {
        return std::visit([&buffer](auto &pimpl) { return pimpl->SendPayload(buffer); }, pimpl_);
    }

    Status SendPayload(const std::vector<MemView> &payload)
    {
        return std::visit([&payload](auto &pimpl) { return pimpl->SendPayload(payload); }, pimpl_);
    }

    Status ReceivePayload(std::vector<RpcMessage> &payload)
    {
        return std::visit([&payload](auto &pimpl) { return pimpl->ReceivePayload(payload); }, pimpl_);
    }

private:
    std::variant<std::unique_ptr<ServerWriterReaderImpl<W, R>>> pimpl_;
};

template <typename W, typename R>
class ServerUnaryWriterReader {
public:
    explicit ServerUnaryWriterReader(std::unique_ptr<ServerUnaryWriterReaderImpl<W, R>> &&impl)
        : pimpl_(std::move(impl))
    {
    }

    virtual ~ServerUnaryWriterReader() = default;

    virtual Status SendStatus(const Status &rc)
    {
        return std::visit([&rc](auto &pimpl) { return pimpl->SendStatus(rc); }, pimpl_);
    }

    virtual Status Read(R &pb)
    {
        return std::visit([&pb](auto &pimpl) { return pimpl->Read(pb); }, pimpl_);
    }

    virtual Status Write(const W &pb)
    {
        return std::visit([&pb](auto &pimpl) { return pimpl->Write(pb); }, pimpl_);
    }

    virtual Status Finish()
    {
        return std::visit([](auto &pimpl) { return pimpl->Finish(); }, pimpl_);
    }

    virtual Status ReceivePayload(std::vector<RpcMessage> &payload)
    {
        return std::visit([&payload](auto &pimpl) { return pimpl->ReceivePayload(payload); }, pimpl_);
    }

    virtual Status SendAndTagPayload(std::vector<datasystem::RpcMessage> &buffer, bool tagPayloadFrame)
    {
        return std::visit(
            [&buffer, &tagPayloadFrame](auto &pimpl) { return pimpl->SendAndTagPayload(buffer, tagPayloadFrame); },
            pimpl_);
    }

    virtual Status SendPayload(std::vector<datasystem::RpcMessage> &buffer)
    {
        return std::visit([&buffer](auto &pimpl) { return pimpl->SendPayload(buffer); }, pimpl_);
    }

    virtual Status SendAndTagPayload(const std::vector<MemView> &payload, bool tagPayloadFrame)
    {
        return std::visit(
            [&payload, &tagPayloadFrame](auto &pimpl) { return pimpl->SendAndTagPayload(payload, tagPayloadFrame); },
            pimpl_);
    }

    virtual Status SendPayload(const std::vector<MemView> &payload)
    {
        return std::visit([&payload](auto &pimpl) { return pimpl->SendPayload(payload); }, pimpl_);
    }

    virtual Status GetOutMsg(ZmqMsgFrames &outMsg)
    {
        return std::visit([&outMsg](auto &pimpl) { return pimpl->GetOutMsg(outMsg); }, pimpl_);
    }

    virtual bool EnableMsgQ()
    {
        return std::visit([](auto &pimpl) { return pimpl->EnableMsgQ(); }, pimpl_);
    }

    virtual void SetRequestInProgress()
    {
        return std::visit([](auto &pimpl) { return pimpl->SetRequestInProgress(); }, pimpl_);
    }

    virtual void SetRequestComplete()
    {
        return std::visit([](auto &pimpl) { return pimpl->SetRequestComplete(); }, pimpl_);
    }

private:
    std::variant<std::unique_ptr<ServerUnaryWriterReaderImpl<W, R>>> pimpl_;
};

template <typename W, typename R>
class LocalServerUnaryWriterReader : public ServerUnaryWriterReader<W, R> {
public:
    explicit LocalServerUnaryWriterReader(R &pb, std::promise<std::pair<W, Status>> promise)
        : ServerUnaryWriterReader<W, R>(std::unique_ptr<ServerUnaryWriterReaderImpl<W, R>>(nullptr))
    {
        pb_ = std::move(pb);
        promise_ = std::move(promise);
        writeOnce_ = false;
        readOnce_ = false;
    }

    ~LocalServerUnaryWriterReader() override = default;

    /**
     * @brief Read message into proto buffer.
     * @param[out] pb Buffer to read into.
     * @return Status of the call.
     */
    Status Read(R &pb) override
    {
        bool expected = false;
        if (readOnce_.compare_exchange_strong(expected, true)) {
            VLOG(RPC_LOG_LEVEL) << "Server uses unary socket reading" << std::endl;
            pb = std::move(pb_);
        } else {
            RETURN_STATUS(StatusCode::K_RUNTIME_ERROR,
                "The Read method of LocalServerUnaryWriterReader is only supposed to be used once!");
        }
        return Status::OK();
    }

    /**
     * @brief Write message into proto buffer.
     * @param[in] pb Buffer to write into.
     * @return Status of the call.
     */
    Status Write(const W &pb) override
    {
        bool expected = false;
        if (writeOnce_.compare_exchange_strong(expected, true)) {
            VLOG(RPC_LOG_LEVEL) << "Server uses unary socket sending rc " << Status::OK() << " message "
                                << LogHelper::IgnoreSensitive(pb) << " back to client" << std::endl;
            promise_.set_value(std::make_pair(pb, Status::OK()));
        } else {
            RETURN_STATUS(StatusCode::K_RUNTIME_ERROR,
                "The Write method of LocalServerUnaryWriterReader is only supposed to be used once!");
        }
        return Status::OK();
    }

    /**
     * @brief Send a status through the connection.
     * @param[in] rc the status to be sent.
     * @return The status of this send action
     */
    Status SendStatus(const Status &rc) override
    {
        bool expected = false;
        if (writeOnce_.compare_exchange_strong(expected, true)) {
            VLOG(RPC_LOG_LEVEL) << "Server uses unary socket sending rc " << Status::OK();
            promise_.set_value(std::make_pair(W(), rc));
        } else {
            RETURN_STATUS(StatusCode::K_RUNTIME_ERROR,
                "The SendStatus method of LocalServerUnaryWriterReader is only supposed to be used once!");
        }
        return Status::OK();
    }

    Status SendAndTagPayload(std::vector<datasystem::RpcMessage> &buffer, bool tagPayloadFrame)
    {
        (void)buffer;
        (void)tagPayloadFrame;
        return {StatusCode::K_INVALID, "LocalServerUnaryWriterReader doesn't support SendAndTagPayload()!"};
    }

    Status SendPayload(std::vector<RpcMessage> &buffer) override
    {
        payloads_ = std::move(buffer);
        return Status::OK();
    }

    Status SendAndTagPayload(const std::vector<MemView> &payload, bool tagPayloadFrame)
    {
        (void)payload;
        (void)tagPayloadFrame;
        return {StatusCode::K_INVALID, "LocalServerUnaryWriterReader doesn't support SendAndTagPayload()!"};
    }

    Status SendPayload(const std::vector<MemView> &payload) override
    {
        (void)payload;
        return {StatusCode::K_INVALID, "LocalServerUnaryWriterReader doesn't support SendPayload()!"};
    }

    Status ReceivePayload(std::vector<RpcMessage> &payload)
    {
        payload = std::move(payloads_);
        return Status::OK();
    }

    Status Finish() override
    {
        return {StatusCode::K_INVALID, "LocalServerUnaryWriterReader doesn't support Finish()!"};
    }

    void SetRequestInProgress() override
    {
        return;
    }

    void SetRequestComplete() override
    {
        return;
    }

    bool EnableMsgQ() override
    {
        return false;
    }

private:
    R pb_;
    std::promise<std::pair<W, Status>> promise_;
    std::atomic<bool> writeOnce_;
    std::atomic<bool> readOnce_;
    std::vector<RpcMessage> payloads_;
};
}  // namespace datasystem
#endif  // DATASYSTEM_COMMON_RPC_SERVER_STREAM_BASE_H
