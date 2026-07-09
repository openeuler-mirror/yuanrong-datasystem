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
 * Description: brpc client-side streaming adapters for single-streaming modes.
 *
 * BrpcClientWriterImpl<W> — client-streaming (stream→unary):
 *   Client writes N messages via brpc::StreamWrite on a StreamCreate'd stream,
 *   closes the stream, then reads a single response from the RPC.
 *   Comparable to ZMQ ClientWriterImpl<W>.
 *
 * BrpcClientReaderImpl<R> — server-streaming (unary→stream):
 *   Client sends a single request via CallMethod, receives N response
 *   messages via the stream's on_received_messages callback.
 *   Comparable to ZMQ ClientReaderImpl<R>.
 */
#ifndef DATASYSTEM_COMMON_RPC_BRPC_CLIENT_STREAM_IMPL_H
#define DATASYSTEM_COMMON_RPC_BRPC_CLIENT_STREAM_IMPL_H

#include <atomic>
#include <condition_variable>
#include <deque>
#include <mutex>

#include <brpc/channel.h>
#include <brpc/controller.h>
#include <brpc/stream.h>
#include <butil/iobuf.h>
#include <google/protobuf/message.h>
#include "datasystem/common/log/log.h"
#include "datasystem/common/rpc/trace_attachment.h"
#include "datasystem/common/rpc/brpc_status_util.h"
#include "datasystem/common/rpc/brpc_stream_close_helper.h"
#include "datasystem/common/rpc/rpc_message.h"
#include "datasystem/common/rpc/mem_view.h"
#include "datasystem/utils/status.h"

namespace datasystem {

/**
 * @brief brpc adapter for client-streaming (stream→unary) on the client side.
 *
 * The client creates a brpc stream, writes N request messages via
 * StreamWrite, closes the stream, and reads a single response.
 *
 * @tparam W Write (request) protobuf type that the client streams.
 */
template <typename W>
class BrpcClientWriterImpl {
public:
    BrpcClientWriterImpl(brpc::Channel *channel,
                         const google::protobuf::MethodDescriptor *method,
                         int32_t timeoutMs)
        : channel_(channel),
          method_(method),
          timeoutMs_(timeoutMs),
          streamId_(brpc::INVALID_STREAM_ID),
          streamCreated_(false),
          rpcDone_(false),
          rpcFailed_(false),
          finished_(false)
    {
    }

    ~BrpcClientWriterImpl()
    {
        if (streamId_ != brpc::INVALID_STREAM_ID) {
            brpc::StreamClose(streamId_);
            streamId_ = brpc::INVALID_STREAM_ID;
        }
        if (cntl_) {
            brpc::Join(cntl_->call_id());
        }
    }

    /**
     * @brief Write a request protobuf to the stream.
     * The first call creates the stream and issues the RPC.
     * @param[in] pb Request protobuf to stream to the server.
     * @return Status of the call.
     */
    Status Write(const W &pb)
    {
        if (finished_.load()) {
            RETURN_STATUS(StatusCode::K_RUNTIME_ERROR,
                "Write called after Finish");
        }

        // Serialize to string first, then to IOBuf
        std::string serialized;
        if (!pb.SerializeToString(&serialized)) {
            RETURN_STATUS(StatusCode::K_RUNTIME_ERROR,
                "Failed to serialize protobuf for stream write");
        }

        if (!streamCreated_) {
            // First Write: create the stream and issue the RPC.
            cntl_ = std::make_unique<brpc::Controller>();
            if (timeoutMs_ > 0) {
                cntl_->set_timeout_ms(timeoutMs_);
            }

            brpc::StreamOptions options;
            options.handler = nullptr;  // Client does not read from this stream
            int rc = brpc::StreamCreate(&streamId_, *cntl_, &options);
            if (rc != 0) {
                cntl_.reset();
                RETURN_STATUS(StatusCode::K_RUNTIME_ERROR,
                    "brpc::StreamCreate failed for client-streaming");
            }
            streamCreated_ = true;

            // Issue the RPC.  For client-streaming (stream→unary), the real data
            // goes via the stream; the RPC body carries the first message as a
            // marker.  The response is a placeholder created from the method
            // descriptor so CallMethod has a correctly-typed target.
            auto *respProto = method_->output_type();
            response_.reset(
                google::protobuf::MessageFactory::generated_factory()
                    ->GetPrototype(respProto)->New());
            AttachTraceIDToAttachment(cntl_->request_attachment());
            channel_->CallMethod(method_, cntl_.get(),
                                 static_cast<const google::protobuf::Message *>(&pb),
                                 response_.get(), nullptr);
            if (cntl_->Failed()) {
                auto errText = cntl_->ErrorText();
                // Wrap with RETURN_STATUS to retain this adapter's call-site
                // (file/line) for on-call; the helper's Status carries the
                // brpc errno/name + ErrorText diagnostics in its message.
                auto st = TryExtractStatusFromControllerError(errText, cntl_->ErrorCode());
                RETURN_STATUS(st.GetCode(), st.GetMsg());
            }
        }

        // Write the serialized message to the stream
        butil::IOBuf buf;
        buf.append(serialized);
        int rc = brpc::StreamWrite(streamId_, buf);
        if (rc != 0) {
            RETURN_STATUS(StatusCode::K_RPC_CANCELLED,
                "brpc::StreamWrite failed for client-streaming");
        }
        return Status::OK();
    }

    /**
     * @brief Read the single response from the RPC.
     * Must be called after Finish().
     * @tparam R Response protobuf type.
     * @param[out] pb Response protobuf to fill.
     * @return Status of the call.
     */
    template <typename R>
    Status Read(R &pb)
    {
        if (!finished_.load()) {
            RETURN_STATUS(StatusCode::K_RUNTIME_ERROR,
                "Read called before Finish");
        }
        if (cntl_ == nullptr) {
            RETURN_STATUS(StatusCode::K_RUNTIME_ERROR,
                "No RPC in flight");
        }

        // Wait for the RPC to complete
        brpc::Join(cntl_->call_id());

        if (cntl_->Failed()) {
            auto errText = cntl_->ErrorText();
            return TryExtractStatusFromControllerError(errText, cntl_->ErrorCode());
        }

        // Parse response from response attachment
        butil::IOBuf &rspBuf = cntl_->response_attachment();
        if (!rspBuf.empty()) {
            std::string data = rspBuf.to_string();
            if (pb.ParseFromString(data)) {
                return Status::OK();
            }
            // Fallback: try parsing from the response Message itself
        }

        // Try the response Message if attachment is empty
        auto *typedRsp = dynamic_cast<R *>(response_.get());
        if (typedRsp != nullptr) {
            pb.CopyFrom(*typedRsp);
            return Status::OK();
        }

        RETURN_STATUS(StatusCode::K_RUNTIME_ERROR,
            "Failed to extract response from client-streaming RPC");
    }

    /**
     * @brief Signal that writing is done. Closes the stream.
     * @return Status of the call.
     */
    Status Finish()
    {
        if (finished_.exchange(true)) {
            return Status::OK();
        }
        if (streamId_ != brpc::INVALID_STREAM_ID) {
            brpc::StreamClose(streamId_);
            streamId_ = brpc::INVALID_STREAM_ID;
        }
        return Status::OK();
    }

    Status SendPayload(const std::vector<MemView> &payload)
    {
        (void)payload;
        RETURN_STATUS(StatusCode::K_NOT_SUPPORTED,
            "SendPayload not supported in BrpcClientWriterImpl");
    }

    Status ReceivePayload(std::vector<RpcMessage> &recvBuffer)
    {
        (void)recvBuffer;
        RETURN_STATUS(StatusCode::K_NOT_SUPPORTED,
            "ReceivePayload not supported in BrpcClientWriterImpl");
    }

private:
    brpc::Channel *channel_;
    const google::protobuf::MethodDescriptor *method_;
    int32_t timeoutMs_;
    std::unique_ptr<brpc::Controller> cntl_;
    std::unique_ptr<google::protobuf::Message> response_;
    brpc::StreamId streamId_;
    bool streamCreated_;
    bool rpcDone_;
    bool rpcFailed_;
    std::atomic<bool> finished_;
};

/**
 * @brief brpc adapter for server-streaming (unary→stream) on the client side.
 *
 * The client sends a single request via CallMethod, then receives N
 * response messages through the stream's on_received_messages callback.
 *
 * @tparam R Read (response) protobuf type that the client reads from the stream.
 */
template <typename R>
class BrpcClientReaderImpl : public brpc::StreamInputHandler {
public:
    BrpcClientReaderImpl(brpc::Channel *channel,
                         const google::protobuf::MethodDescriptor *method,
                         int32_t timeoutMs)
        : channel_(channel),
          method_(method),
          timeoutMs_(timeoutMs),
          streamId_(brpc::INVALID_STREAM_ID),
          writeOnce_(false),
          finished_(false),
          readReady_(false),
          streamEnd_(false),
          readError_(false)
    {
    }

    ~BrpcClientReaderImpl() override
    {
        StreamCloseAndDrain(
            {streamId_, readMtx_, readCond_, streamEnd_, readError_, closeNotifier_, kDefaultDeferredWaitSec},
            cntl_,
            "~BrpcClientReaderImpl");
    }

    // --- brpc::StreamInputHandler callbacks ---

    int on_received_messages(brpc::StreamId id, butil::IOBuf *const messages[], size_t size) override
    {
        (void)id;
        std::lock_guard<std::mutex> lock(readMtx_);
        for (size_t i = 0; i < size; ++i) {
            if (messages[i] != nullptr) {
                pendingMessages_.push_back(std::move(*messages[i]));
            }
        }
        readReady_ = true;
        readCond_.notify_one();
        return 0;
    }

    void on_idle_timeout(brpc::StreamId id) override
    {
        (void)id;
        std::lock_guard<std::mutex> lock(readMtx_);
        readError_ = true;
        readCond_.notify_one();
    }

    void on_closed(brpc::StreamId id) override
    {
        (void)id;
        if (closeNotifier_) closeNotifier_->store(true, std::memory_order_relaxed);
        std::lock_guard<std::mutex> lock(readMtx_);
        streamEnd_ = true;
        readReady_ = true;
        readCond_.notify_one();
    }

    void on_failed(brpc::StreamId id, int error_code, const std::string &error_text) override
    {
        (void)id;
        (void)error_code;
        LOG(WARNING) << "BrpcClientReaderImpl stream failed: " << error_text;
        if (closeNotifier_) closeNotifier_->store(true, std::memory_order_relaxed);
        std::lock_guard<std::mutex> lock(readMtx_);
        readError_ = true;
        readCond_.notify_one();
    }

    /**
     * @brief Send the request and set up the stream for reading responses.
     * @tparam W Request protobuf type.
     * @param[in] pb Request protobuf to send.
     * @return Status of the call.
     */
    template <typename W>
    Status Write(const W &pb)
    {
        bool expected = false;
        if (!writeOnce_.compare_exchange_strong(expected, true)) {
            RETURN_STATUS(StatusCode::K_RUNTIME_ERROR,
                "BrpcClientReaderImpl::Write called more than once");
        }

        cntl_ = std::make_unique<brpc::Controller>();
        if (timeoutMs_ > 0) {
            cntl_->set_timeout_ms(timeoutMs_);
        }

        brpc::StreamOptions options;
        options.handler = this;
        int rc = brpc::StreamCreate(&streamId_, *cntl_, &options);
        if (rc != 0) {
            cntl_.reset();
            RETURN_STATUS(StatusCode::K_RUNTIME_ERROR,
                "brpc::StreamCreate failed for server-streaming reader");
        }

        // Send the RPC with the request in the body.
        // The response is a placeholder — real responses arrive via the stream.
        R dummyResponse;
        AttachTraceIDToAttachment(cntl_->request_attachment());
        channel_->CallMethod(method_, cntl_.get(), &pb, &dummyResponse, nullptr);
        if (cntl_->Failed()) {
            Status embedded = TryExtractStatusFromResponse(dummyResponse);
            if (embedded.IsError()) {
                StreamCloseAndDrain(
                    {streamId_, readMtx_, readCond_, streamEnd_, readError_, closeNotifier_},
                    cntl_,
                    "BrpcClientReaderImpl::Write (embedded error)");
                return embedded;
            }
            auto errText = cntl_->ErrorText();
            StreamCloseAndDrain(
                {streamId_, readMtx_, readCond_, streamEnd_, readError_, closeNotifier_},
                cntl_,
                "BrpcClientReaderImpl::Write (errText)");
            // Wrap with RETURN_STATUS to retain this adapter's call-site
            // (file/line) for on-call; the helper's Status carries the brpc
            // errno/name + ErrorText diagnostics in its message.
            auto st = TryExtractStatusFromControllerError(errText, cntl_->ErrorCode());
            RETURN_STATUS(st.GetCode(), st.GetMsg());
        }

        return Status::OK();
    }

    /**
     * @brief Read one response from the stream.
     * @param[out] pb Response protobuf to fill.
     * @return Status of the call; K_RPC_STREAM_END when the server has finished.
     */
    Status Read(R &pb)
    {
        if (finished_.load()) {
            RETURN_STATUS(StatusCode::K_RPC_STREAM_END, "Stream already finished");
        }

        std::unique_lock<std::mutex> lock(readMtx_);
        readCond_.wait(lock, [this] { return readReady_ || readError_ || streamEnd_; });

        if (readError_) {
            RETURN_STATUS(StatusCode::K_RPC_CANCELLED, "Stream error during read");
        }

        // Check for pending messages
        if (!pendingMessages_.empty()) {
            butil::IOBuf msg = std::move(pendingMessages_.front());
            pendingMessages_.pop_front();
            if (pendingMessages_.empty() && !streamEnd_) {
                readReady_ = false;
            }
            lock.unlock();

            std::string serialized = msg.to_string();
            if (!pb.ParseFromString(serialized)) {
                RETURN_STATUS(StatusCode::K_RUNTIME_ERROR,
                    "Failed to parse protobuf from stream message");
            }
            return Status::OK();
        }

        // No pending messages and stream ended
        if (streamEnd_) {
            RETURN_STATUS(StatusCode::K_RPC_STREAM_END, "Stream ended");
        }

        RETURN_STATUS(StatusCode::K_RPC_CANCELLED, "Unexpected read state");
    }

    /**
     * @brief Signal reading is done. Closes the stream.
     * @return Status of the call.
     */
    Status Finish()
    {
        if (finished_.exchange(true)) {
            return Status::OK();
        }
        StreamCloseAndDrain(
            {streamId_, readMtx_, readCond_, streamEnd_, readError_, closeNotifier_, kDefaultDeferredWaitSec},
            cntl_,
            "BrpcClientReaderImpl::Finish");
        return Status::OK();
    }

    Status SendPayload(const std::vector<MemView> &payload)
    {
        (void)payload;
        RETURN_STATUS(StatusCode::K_NOT_SUPPORTED,
            "SendPayload not supported in BrpcClientReaderImpl");
    }

    Status ReceivePayload(std::vector<RpcMessage> &recvBuffer)
    {
        (void)recvBuffer;
        RETURN_STATUS(StatusCode::K_NOT_SUPPORTED,
            "ReceivePayload not supported in BrpcClientReaderImpl");
    }

private:
    brpc::Channel *channel_;
    const google::protobuf::MethodDescriptor *method_;
    int32_t timeoutMs_;
    std::unique_ptr<brpc::Controller> cntl_;
    brpc::StreamId streamId_;
    std::atomic<bool> writeOnce_;
    std::atomic<bool> finished_;

    // Read synchronization
    std::mutex readMtx_;
    std::condition_variable readCond_;
    std::deque<butil::IOBuf> pendingMessages_;
    bool readReady_;
    bool streamEnd_;
    bool readError_;
    std::shared_ptr<std::atomic<bool>> closeNotifier_ =
        std::make_shared<std::atomic<bool>>(false);
};

}  // namespace datasystem
#endif  // DATASYSTEM_COMMON_RPC_BRPC_CLIENT_STREAM_IMPL_H
