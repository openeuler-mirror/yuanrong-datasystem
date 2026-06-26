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
 * Description: brpc server-side unary writer/reader adapter.
 * Wraps brpc::Controller + request/response pointers to provide the same
 * ServerUnaryWriterReader interface that service implementations expect.
 * Binary payloads are transferred via brpc IOBuf attachments.
 *
 * Async completion model:
 *   In ZMQ mode, ServerUnaryWriterReader owns the response lifetime and
 *   sends it directly. In brpc, the response is owned by CallMethod and
 *   sent when CallMethod returns (or done->Run() is called).
 *
 *   Service methods that dispatch to a thread pool (e.g., CreateProducer)
 *   return immediately from CallMethod, but the thread pool worker calls
 *   Write()/SendStatus() later. To bridge this, BrpcServerUnaryWriterReader
 *   holds the brpc `done` closure. When Write() or SendStatus() is called
 *   (from any thread), it copies into brpc's response and calls done->Run()
 *   to trigger the RPC completion.
 *
 *   For methods with recv_payload_option, Write() defers done->Run() so that
 *   SendPayload()/SendAndTagPayload() can write the payload to
 *   response_attachment first. The payload-send method then calls
 *   TryCompleteDeferred() to finalize the RPC.
 */
#ifndef DATASYSTEM_COMMON_RPC_BRPC_SERVER_UNARY_WRITER_READER_H
#define DATASYSTEM_COMMON_RPC_BRPC_SERVER_UNARY_WRITER_READER_H

#include <atomic>
#include <vector>

#include <brpc/controller.h>
#include <sys/time.h>
#include "datasystem/common/rpc/rpc_server_stream_base.h"
#include "datasystem/common/rpc/rpc_message.h"
#include "datasystem/common/rpc/mem_view.h"
#include "datasystem/common/rpc/timeout_duration.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {

namespace {
constexpr int kTagPayloadFrameWarnInterval = 100;
}  // namespace

/**
 * @brief brpc adapter that implements ServerUnaryWriterReader for unary RPCs.
 *
 * @tparam W Output (response) protobuf type.
 * @tparam R Input (request) protobuf type.
 *
 * Service code calls Read() to get the request, Write() to set the response,
 * and SendPayload()/ReceivePayload() for binary data via brpc IOBuf attachments.
 *
 * The class is not thread-safe beyond the once-only semantics of Read/Write/SendStatus.
 */
template <typename W, typename R>
class BrpcServerUnaryWriterReader : public ServerUnaryWriterReader<W, R> {
public:
    /**
     * @brief Construct from brpc call parameters.
     * @param cntl      brpc controller (owns request/response attachments).
     * @param request   Pointer to the deserialized request protobuf.
     * @param response  Pointer to the response protobuf to be filled.
     */
    BrpcServerUnaryWriterReader(brpc::Controller *cntl, const google::protobuf::Message *request,
                                google::protobuf::Message *response, google::protobuf::Closure *done = nullptr)
        : ServerUnaryWriterReader<W, R>(std::unique_ptr<ServerUnaryWriterReaderImpl<W, R>>(nullptr)),
          cntl_(cntl),
          request_(nullptr),
          response_(nullptr),
          done_(done),
          readOnce_(false),
          writeOnce_(false),
          doneConsumed_(false),
          recvPayloadOp_(false)
    {
        // Cast the generic Message pointers to typed pointers.
        // The brpc adapter generator guarantees the types match.
        request_ = dynamic_cast<const R *>(request);
        response_ = dynamic_cast<W *>(response);

        // P3: Store scTimeoutDuration per-adapter so it survives bthread M:N
        // migration.  Under brpc, a bthread may yield and resume on a different
        // pthread whose thread_local scTimeoutDuration copy is stale.  Service
        // implementations should use GetScTimeoutDuration() instead of the
        // thread_local after yield points (sub-RPC calls).
        if (cntl_ != nullptr) {
            int64_t deadlineUs = cntl_->deadline_us();
            if (deadlineUs > 0) {
                struct timeval tv;
                gettimeofday(&tv, nullptr);
                int64_t nowUs = static_cast<int64_t>(tv.tv_sec) * 1000000L + tv.tv_usec;
                int64_t remainingMs = (deadlineUs - nowUs + 999) / 1000;
                if (remainingMs > 0) {
                    scTimeoutDuration_.Init(remainingMs);
                } else {
                    scTimeoutDuration_.Init();
                }
            } else {
                scTimeoutDuration_.Init();
            }
        }
    }

    ~BrpcServerUnaryWriterReader() override
    {
        // If Write()/SendStatus() was never called and done was not marked
        // consumed, run it now to prevent RPC leak. brpc requires done->Run()
        // exactly once; the closure created by NewCallback is self-deleting
        // and remains valid until Run(). The previous "only log, do not Run"
        // behavior assumed brpc might reclaim the closure, but that is wrong:
        // brpc does not touch the closure until we Run it. Failing to Run
        // leaks the closure and leaves brpc waiting for a response that never
        // comes.
        //
        // CAS on doneConsumed_ ensures exactly one thread enters the Run path:
        // Write/SendStatus/TryCompleteDeferred all do the same CAS before touching
        // done_, so losers see doneConsumed_=true and skip. The raw `done_` write
        // (`done_ = nullptr` before Run) is intentionally not atomic — only the
        // CAS-winning thread can reach Run(), and concurrent readers of `done_`
        // are already gated by the CAS-failed branch above. NewCallback closures
        // self-delete after Run(); we never `delete done_`.
        bool expected = false;
        if (doneConsumed_.compare_exchange_strong(expected, true, std::memory_order_acq_rel)
            && done_ != nullptr) {
            LOG(ERROR) << "BrpcServerUnaryWriterReader destroyed without completing RPC — "
                          "service did not call Write() or SendStatus(); running done to prevent leak";
            auto *done = done_;
            done_ = nullptr;
            done->Run();
        }
    }

    // --- Core API ---

    /**
     * @brief Read the request protobuf. Can only be called once.
     * @param[out] pb Buffer to read the request into.
     * @return Status of the call.
     */
    Status Read(R &pb) override
    {
        bool expected = false;
        if (readOnce_.compare_exchange_strong(expected, true)) {
            if (request_ == nullptr) {
                RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, "Request protobuf is null (type mismatch?)");
            }
            pb.CopyFrom(*request_);
        } else {
            RETURN_STATUS(StatusCode::K_RUNTIME_ERROR,
                "The Read method of BrpcServerUnaryWriterReader is only supposed to be used once!");
        }
        return Status::OK();
    }

    /**
     * @brief Write the response protobuf. Can only be called once.
     * @param[in] pb Response protobuf to send.
     * @return Status of the call.
     */
    Status Write(const W &pb) override
    {
        bool expected = false;
        if (writeOnce_.compare_exchange_strong(expected, true)) {
            if (response_ != nullptr) {
                response_->CopyFrom(pb);
            }
            if (!recvPayloadOp_) {
                doneConsumed_.store(true, std::memory_order_release);
                if (done_ != nullptr) {
                    done_->Run();
                    done_ = nullptr;
                }
            }
        } else {
            RETURN_STATUS(StatusCode::K_RUNTIME_ERROR,
                "The Write method of BrpcServerUnaryWriterReader is only supposed to be used once!");
        }
        return Status::OK();
    }

    /**
     * @brief Send an error status to the client by failing the brpc controller.
     * @param[in] rc Status to send.
     * @return Status::OK (brpc SetFailed does not return errors).
     */
    Status SendStatus(const Status &rc) override
    {
        bool expected = false;
        if (writeOnce_.compare_exchange_strong(expected, true)) {
            if (cntl_ != nullptr && rc.IsError()) {
                cntl_->SetFailed(rc.GetMsg() + "\x01DS_ERR:"
                    + std::to_string(static_cast<int>(rc.GetCode())) + "\x02");
            }
            // Mark done as consumed and run done closure immediately — same as
            // Write() and TryCompleteDeferred(). Without this, the RPC hangs until
            // the destructor fires done->Run() + spurious LOG(ERROR).
            doneConsumed_.store(true, std::memory_order_release);
            if (done_ != nullptr) {
                done_->Run();
                done_ = nullptr;
            }
        } else {
            LOG(WARNING) << "BrpcServerUnaryWriterReader::SendStatus called after Write/SendStatus";
        }
        return Status::OK();
    }

    // --- Payload API ---

    /**
     * @brief Send binary payload via brpc response attachment.
     *
     * Framing format: [int64_t count][int64_t size1][data1][int64_t size2][data2]...
     *
     * @param[in] buffer Payload messages to send.
     * @return Status of the call.
     */
    Status SendPayload(std::vector<RpcMessage> &buffer) override
    {
        // If the RPC was already completed (e.g. SendStatus in error path),
        // writing to response_attachment would touch memory brpc may have freed.
        if (doneConsumed_.load(std::memory_order_acquire)) {
            return Status::OK();
        }
        auto st = AppendToResponseAttachment(buffer);
        if (st.IsOk()) {
            TryCompleteDeferred();
        }
        return st;
    }

    /**
     * @brief Send MemView payload via brpc response attachment.
     * @param[in] payload MemView segments to send.
     * @return Status of the call.
     */
    Status SendPayload(const std::vector<MemView> &payload) override
    {
        if (doneConsumed_.load(std::memory_order_acquire)) {
            return Status::OK();
        }
        auto st = AppendMemViewToResponseAttachment(payload);
        if (st.IsOk()) {
            TryCompleteDeferred();
        }
        return st;
    }

    Status SendAndTagPayload(std::vector<RpcMessage> &buffer, bool tagPayloadFrame) override
    {
        if (doneConsumed_.load(std::memory_order_acquire)) {
            return Status::OK();
        }
        // tagPayloadFrame is a ZMQ concept: when true, the size frame's message
        // type is set to PAYLOAD_SZ for ZMQ routing. In brpc, all payload is
        // sent via Controller::response_attachment() with a uniform [count][size][data]
        // framing -- there is no equivalent ZMQ message-type flag.
        if (tagPayloadFrame) {
            LOG_EVERY_N(WARNING, kTagPayloadFrameWarnInterval) << "SendAndTagPayload tagPayloadFrame ignored -- "
                                         "ZMQ frame tags not applicable to brpc attachment path";
        }
        auto st = AppendToResponseAttachment(buffer);
        if (st.IsOk()) {
            TryCompleteDeferred();
        }
        return st;
    }

    Status SendAndTagPayload(const std::vector<MemView> &payload, bool tagPayloadFrame) override
    {
        if (doneConsumed_.load(std::memory_order_acquire)) {
            return Status::OK();
        }
        if (tagPayloadFrame) {
            LOG_EVERY_N(WARNING, kTagPayloadFrameWarnInterval) << "SendAndTagPayload tagPayloadFrame ignored -- "
                                         "ZMQ frame tags not applicable to brpc attachment path";
        }
        auto st = AppendMemViewToResponseAttachment(payload);
        if (st.IsOk()) {
            TryCompleteDeferred();
        }
        return st;
    }

    /**
     * @brief Receive binary payload from brpc request attachment.
     *
     * Parses int64_t length-prefixed frames from the request IOBuf attachment:
     *   [int64_t count][int64_t size1][data1][int64_t size2][data2]...
     *
     * @param[out] payload Received payload messages.
     * @return Status of the call.
     */
    Status ReceivePayload(std::vector<RpcMessage> &payload) override
    {
        if (cntl_ == nullptr) {
            RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, "Controller is null");
        }
        return ParseFromRequestAttachment(payload);
    }

    // --- Unsupported / no-op methods ---

    Status Finish() override
    {
        // brpc handles RPC completion automatically when CallMethod returns.
        return Status::OK();
    }

    Status GetOutMsg(ZmqMsgFrames &outMsg) override
    {
        (void)outMsg;
        // Not applicable to brpc. Return OK to avoid breaking generated code paths.
        return Status::OK();
    }

    bool EnableMsgQ() override
    {
        // brpc uses its own internal messaging, not ZMQ message queues.
        return false;
    }

    void SetRequestInProgress() override
    {
        // No-op: brpc handles request lifecycle internally.
    }

    void SetRequestComplete() override
    {
        // No-op: brpc handles request lifecycle internally.
    }

    /**
     * @brief Mark done as consumed so the destructor will not call it.
     * Used by the generated CallMethod error path to prevent double-close.
     */
    void MarkDoneConsumed()
    {
        doneConsumed_.store(true, std::memory_order_release);
    }

    /**
     * @brief Set whether this method expects the service to send payload
     * (recv_payload_option in proto). When true, Write() defers done_->Run()
     * so SendPayload()/SendAndTagPayload() can write to response_attachment
     * before brpc serializes the response.
     */
    void SetHasRecvPayload(bool v) { recvPayloadOp_ = v; }

    /**
     * @brief Get per-adapter scTimeoutDuration (survives bthread M:N migration).
     * Preferred over thread_local scTimeoutDuration after yield points.
     */
    const TimeoutDuration& GetScTimeoutDuration() const { return scTimeoutDuration_; }

private:
    /**
     * @brief Complete the deferred RPC by running done_->Run().
     * Called by SendPayload()/SendAndTagPayload() when Write() deferred
     * completion (i.e., recvPayloadOp_ == true).
     * Uses CAS on doneConsumed_ to ensure only one path completes.
     */
    void TryCompleteDeferred()
    {
        bool expected = false;
        if (doneConsumed_.compare_exchange_strong(expected, true) && done_ != nullptr) {
            done_->Run();
            done_ = nullptr;
        }
    }

    /**
     * @brief Append RpcMessage frames using int64_t framing:
     * [int64_t count][int64_t size1][data1][int64_t size2][data2]...
     */
    Status AppendToResponseAttachment(const std::vector<RpcMessage> &buffer)
    {
        if (cntl_ == nullptr) {
            RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, "Controller is null");
        }
        butil::IOBuf &attach = cntl_->response_attachment();
        int64_t count = static_cast<int64_t>(buffer.size());
        attach.append(reinterpret_cast<const char *>(&count), sizeof(count));
        for (const auto &msg : buffer) {
            int64_t sz = static_cast<int64_t>(msg.Size());
            attach.append(reinterpret_cast<const char *>(&sz), sizeof(sz));
            attach.append(static_cast<const char *>(msg.Data()), msg.Size());
        }
        return Status::OK();
    }

    /**
     * @brief Append MemView segments using int64_t framing.
     */
    Status AppendMemViewToResponseAttachment(const std::vector<MemView> &payload)
    {
        if (cntl_ == nullptr) {
            RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, "Controller is null");
        }
        butil::IOBuf &attach = cntl_->response_attachment();
        int64_t count = static_cast<int64_t>(payload.size());
        attach.append(reinterpret_cast<const char *>(&count), sizeof(count));
        for (const auto &view : payload) {
            int64_t sz = static_cast<int64_t>(view.Size());
            attach.append(reinterpret_cast<const char *>(&sz), sizeof(sz));
            attach.append(static_cast<const char *>(view.Data()), view.Size());
        }
        return Status::OK();
    }

    /**
     * @brief Parse int64_t-framed payload from request attachment.
     * Format: [int64_t count][int64_t size1][data1]...
     */
    Status ParseFromRequestAttachment(std::vector<RpcMessage> &payload)
    {
        const butil::IOBuf &attach = cntl_->request_attachment();
        if (attach.empty()) {
            return Status::OK();
        }

        size_t remaining = attach.size();
        size_t offset = 0;

        CHECK_FAIL_RETURN_STATUS(offset + sizeof(int64_t) <= remaining, StatusCode::K_RUNTIME_ERROR,
            "Malformed payload: missing count header");
        int64_t count = 0;
        attach.copy_to(reinterpret_cast<char *>(&count), sizeof(count), offset);
        offset += sizeof(count);

        for (int64_t i = 0; i < count; ++i) {
            CHECK_FAIL_RETURN_STATUS(offset + sizeof(int64_t) <= remaining, StatusCode::K_RUNTIME_ERROR,
                "Malformed payload: truncated size header");
            int64_t sz = 0;
            attach.copy_to(reinterpret_cast<char *>(&sz), sizeof(sz), offset);
            offset += sizeof(sz);

            CHECK_FAIL_RETURN_STATUS(sz >= 0, StatusCode::K_RUNTIME_ERROR,
                "Malformed payload: negative frame size");
            CHECK_FAIL_RETURN_STATUS(offset + static_cast<size_t>(sz) <= remaining, StatusCode::K_RUNTIME_ERROR,
                "Malformed payload: frame data exceeds attachment size");

            RpcMessage msg;
            RETURN_IF_NOT_OK(msg.AllocMem(static_cast<size_t>(sz)));
            attach.copy_to(static_cast<char *>(msg.Data()), static_cast<size_t>(sz), offset);
            payload.emplace_back(std::move(msg));
            offset += static_cast<size_t>(sz);
        }
        return Status::OK();
    }

    brpc::Controller *cntl_;
    const R *request_;
    W *response_;
    google::protobuf::Closure *done_;
    std::atomic<bool> readOnce_;
    std::atomic<bool> writeOnce_;
    std::atomic<bool> doneConsumed_;
    bool recvPayloadOp_;
    TimeoutDuration scTimeoutDuration_;
};

}  // namespace datasystem
#endif  // DATASYSTEM_COMMON_RPC_BRPC_SERVER_UNARY_WRITER_READER_H
