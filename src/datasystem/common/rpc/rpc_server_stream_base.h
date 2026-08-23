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

#include <future>

#include "datasystem/common/rpc/brpc_server_stream_impl.h"
#include "datasystem/common/rpc/rpc_message.h"
#include "datasystem/common/rpc/server_writer_reader_base.h"
#include "datasystem/common/log/log_helper.h"

namespace datasystem {
/**
 * Only server side is streaming.
 * @tparam W Stream RPC mode, WritePb type.
 */
template <typename W>
class ServerWriter {
public:
    explicit ServerWriter(std::unique_ptr<BrpcServerWriterImpl<W>> &&impl) : pimpl_(std::move(impl))
    {
    }

    ~ServerWriter() = default;

    Status SendStatus(const Status &rc)
    {
        return pimpl_->SendStatus(rc);
    }

    template <typename R>
    Status ReadPb(R &pb)
    {
        return pimpl_->ReadPb(pb);
    }

    Status Write(const W &pb)
    {
        return pimpl_->Write(pb);
    }

    Status Finish()
    {
        return pimpl_->Finish();
    }

    Status SendPayload(std::vector<RpcMessage> &buffer)
    {
        return pimpl_->SendPayload(buffer);
    }

    Status SendPayload(const std::vector<MemView> &payload)
    {
        return pimpl_->SendPayload(payload);
    }

    Status ReceivePayload(std::vector<RpcMessage> &payload)
    {
        return pimpl_->ReceivePayload(payload);
    }

private:
    std::unique_ptr<BrpcServerWriterImpl<W>> pimpl_;
};

/**
 * Only the client side is streaming.
 */
template <typename R>
class ServerReader {
public:
    explicit ServerReader(std::shared_ptr<BrpcServerReaderImpl<R>> &&impl) : pimpl_(std::move(impl))
    {
    }

    // Trigger non-blocking Close() so the brpc handler fires on_closed and its
    // self-keepalive can release.
    ~ServerReader()
    {
        if (pimpl_) {
            pimpl_->Close();
        }
    }

    Status SendStatus(const Status &rc)
    {
        return pimpl_->SendStatus(rc);
    }

    Status Read(R &pb)
    {
        return pimpl_->Read(pb);
    }

    Status ReceivePayload(std::vector<RpcMessage> &payload)
    {
        return pimpl_->ReceivePayload(payload);
    }

    template <typename W>
    Status WritePb(const W &pb)
    {
        return pimpl_->WritePb(pb);
    }

    Status SendPayload(std::vector<RpcMessage> &buffer)
    {
        return pimpl_->SendPayload(buffer);
    }

    Status SendPayload(const std::vector<MemView> &payload)
    {
        return pimpl_->SendPayload(payload);
    }

    Status Finish()
    {
        return pimpl_->Finish();
    }

private:
    std::shared_ptr<BrpcServerReaderImpl<R>> pimpl_;
};

/**
 * Both sides are streaming.
 * Uses ServerWriterReaderBase for virtual dispatch instead of std::variant.
 * @tparam W Stream RPC mode, WritePb type.
 * @tparam R Stream RPC mode, ReadPb type.
 */
template <typename W, typename R>
class ServerWriterReader {
public:
    explicit ServerWriterReader(std::unique_ptr<ServerWriterReaderBase<W, R>> &&impl) : pimpl_(std::move(impl))
    {
    }

    ~ServerWriterReader() = default;

    Status SendStatus(const Status &rc)
    {
        return pimpl_->SendStatus(rc);
    }

    Status Read(R &pb)
    {
        return pimpl_->Read(pb);
    }

    Status Write(const W &pb)
    {
        return pimpl_->Write(pb);
    }

    Status Finish()
    {
        return pimpl_->Finish();
    }

    Status SendPayload(std::vector<RpcMessage> &buffer)
    {
        return pimpl_->SendPayload(buffer);
    }

    Status SendPayload(const std::vector<MemView> &payload)
    {
        return pimpl_->SendPayload(payload);
    }

    Status ReceivePayload(std::vector<RpcMessage> &payload)
    {
        return pimpl_->ReceivePayload(payload);
    }

private:
    std::unique_ptr<ServerWriterReaderBase<W, R>> pimpl_;
};

template <typename W, typename R>
class ServerUnaryWriterReader {
public:
    virtual ~ServerUnaryWriterReader() = default;

    virtual Status SendStatus(const Status &rc) = 0;

    virtual Status Read(R &pb) = 0;

    virtual Status Write(const W &pb) = 0;

    virtual Status Finish() = 0;

    virtual Status ReceivePayload(std::vector<RpcMessage> &payload) = 0;

    virtual Status SendAndTagPayload(std::vector<datasystem::RpcMessage> &buffer, bool tagPayloadFrame) = 0;

    virtual Status SendPayload(std::vector<datasystem::RpcMessage> &buffer) = 0;

    virtual Status SendAndTagPayload(const std::vector<MemView> &payload, bool tagPayloadFrame) = 0;

    virtual Status SendPayload(const std::vector<MemView> &payload) = 0;

    virtual Status GetOutMsg(RpcMsgFrames &outMsg) = 0;

    virtual bool EnableMsgQ() = 0;

    virtual void SetRequestInProgress() = 0;

    virtual void SetRequestComplete() = 0;
};

template <typename W, typename R>
class LocalServerUnaryWriterReader : public ServerUnaryWriterReader<W, R> {
public:
    explicit LocalServerUnaryWriterReader(R &pb, std::promise<std::pair<W, Status>> promise)
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

    Status SendAndTagPayload(std::vector<datasystem::RpcMessage> &buffer, bool tagPayloadFrame) override
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

    Status SendAndTagPayload(const std::vector<MemView> &payload, bool tagPayloadFrame) override
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

    Status ReceivePayload(std::vector<RpcMessage> &payload) override
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

    Status GetOutMsg(RpcMsgFrames &outMsg) override
    {
        (void)outMsg;
        return Status::OK();
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
