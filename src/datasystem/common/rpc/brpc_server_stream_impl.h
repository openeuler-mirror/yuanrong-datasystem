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
 * Description: brpc server-side streaming adapter implementations.
 *
 * BrpcServerWriterImpl<W> (server-streaming / unary-to-stream):
 *   Server accepts a brpc stream, reads the initial request from cntl->request
 *   via ReadPb<R>(), writes N response messages via Write() using
 *   brpc::StreamWrite, then closes the stream via Finish().
 *
 * BrpcServerReaderImpl<R> (client-streaming / stream-to-unary):
 *   Server accepts a brpc stream, reads N request messages via
 *   StreamInputHandler::on_received_messages, then the service returns a
 *   single response written to cntl->response_attachment() via WritePb<W>().
 */
#ifndef DATASYSTEM_COMMON_RPC_BRPC_SERVER_STREAM_IMPL_H
#define DATASYSTEM_COMMON_RPC_BRPC_SERVER_STREAM_IMPL_H

#include <sys/time.h>
#include <atomic>
#include <condition_variable>
#include <bthread/mutex.h>
#include <bthread/condition_variable.h>
#include <deque>
#include <memory>
#include <mutex>
#include <string>

#include <brpc/controller.h>
#include <brpc/stream.h>

#include "datasystem/common/log/log.h"
#include "datasystem/common/log/trace.h"
#include "datasystem/common/rpc/brpc_perf_trace.h"
#include "datasystem/common/rpc/rpc_message.h"
#include "datasystem/common/rpc/timeout_duration.h"
#include "datasystem/utils/status.h"

namespace datasystem {

// ============================================================================
// BrpcServerWriterImpl<W> — server-streaming (unary→stream)
// ============================================================================
template <typename W>
class BrpcServerWriterImpl {
public:
    BrpcServerWriterImpl(brpc::Controller *cntl, const google::protobuf::Message *request,
                         std::string methodName)
        : cntl_(cntl),
          request_(request),
          streamId_(brpc::INVALID_STREAM_ID),
          finished_(false),
          traceRecorded_(false),
          trace_(Trace::Instance().GetTraceID(), std::move(methodName))
    {
        trace_.MarkServerRecv();
        // P3: Store scTimeoutDuration per-adapter so it survives bthread M:N migration.
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

        brpc::StreamOptions options;
        options.handler = nullptr;
        int rc = brpc::StreamAccept(&streamId_, *cntl, &options);
        if (rc != 0) {
            LOG(ERROR) << "BrpcServerWriterImpl: StreamAccept failed (rc=" << rc << ")";
            cntl->SetFailed("BrpcServerWriterImpl: StreamAccept failed");
        }
    }

    ~BrpcServerWriterImpl()
    {
        if (streamId_ != brpc::INVALID_STREAM_ID) {
            brpc::StreamClose(streamId_);
            streamId_ = brpc::INVALID_STREAM_ID;
        }
        RecordTraceOnce();
    }

    Status SendStatus(const Status &rc)
    {
        if (rc.IsError() && streamId_ != brpc::INVALID_STREAM_ID) {
            // Send error status via a control frame, then close
            butil::IOBuf buf;
            std::string errMsg = std::string("\x01" "DS_ERR:") +
                std::to_string(static_cast<int>(rc.GetCode())) + "\x02" + rc.GetMsg();
            buf.append(errMsg);
            (void)brpc::StreamWrite(streamId_, buf);
        }
        return Status::OK();
    }

    /**
     * Read the initial request protobuf from the RPC body.
     * In server-streaming mode, the client sends one request in the RPC body
     * and the server streams responses back.
     */
    template <typename R>
    Status ReadPb(R &pb)
    {
        if (request_ == nullptr) {
            RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, "No request proto");
        }
        const R *typedRequest = dynamic_cast<const R *>(request_);
        if (typedRequest == nullptr) {
            RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, "Request type mismatch");
        }
        trace_.MarkServerExecStart();
        pb.CopyFrom(*typedRequest);
        return Status::OK();
    }

    Status Write(const W &pb)
    {
        if (streamId_ == brpc::INVALID_STREAM_ID) {
            RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, "Stream not initialized");
        }
        std::string serialized;
        if (!pb.SerializeToString(&serialized)) {
            RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, "Failed to serialize Write protobuf");
        }
        butil::IOBuf buf;
        buf.append(serialized);
        int rc = brpc::StreamWrite(streamId_, buf);
        if (rc != 0) {
            RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, "brpc::StreamWrite failed");
        }
        return Status::OK();
    }

    Status Finish()
    {
        if (finished_.exchange(true)) {
            return Status::OK();
        }
        if (streamId_ != brpc::INVALID_STREAM_ID) {
            trace_.MarkServerExecEnd();
            brpc::StreamClose(streamId_);
            streamId_ = brpc::INVALID_STREAM_ID;
        }
        RecordTraceOnce();
        return Status::OK();
    }

    Status SendPayload(std::vector<RpcMessage> &buffer)
    {
        (void)buffer;
        RETURN_STATUS(StatusCode::K_NOT_SUPPORTED, "SendPayload(RpcMessage) not supported");
    }

    Status SendPayload(const std::vector<MemView> &payload)
    {
        (void)payload;
        RETURN_STATUS(StatusCode::K_NOT_SUPPORTED, "SendPayload(MemView) not supported in brpc streaming");
    }

    Status ReceivePayload(std::vector<RpcMessage> &payload)
    {
        (void)payload;
        RETURN_STATUS(StatusCode::K_NOT_SUPPORTED, "ReceivePayload not supported");
    }

    /**
     * @brief Get per-adapter scTimeoutDuration (survives bthread M:N migration).
     */
    const TimeoutDuration& GetScTimeoutDuration() const { return scTimeoutDuration_; }

private:
    brpc::Controller *cntl_;
    const google::protobuf::Message *request_;
    brpc::StreamId streamId_;
    std::atomic<bool> finished_;
    TimeoutDuration scTimeoutDuration_;
    std::atomic<bool> traceRecorded_;
    BrpcPerfTrace trace_;

    void RecordTraceOnce()
    {
        if (!traceRecorded_.exchange(true, std::memory_order_acq_rel)) {
            // Stream RPC tracing is one summary sample per stream, not one sample per message.
            trace_.MarkServerSend();
            // StreamWrite responses cannot carry the unary trace trailer.
            RecordBrpcRpcTrace(trace_);
        }
    }
};

// ============================================================================
// BrpcServerReaderImpl<R> — client-streaming (stream→unary)
// ============================================================================
template <typename R>
class BrpcServerReaderImpl : public brpc::StreamInputHandler,
                            public std::enable_shared_from_this<BrpcServerReaderImpl<R>> {
public:
    explicit BrpcServerReaderImpl(brpc::Controller *cntl, std::string methodName)
        : cntl_(cntl),
          streamId_(brpc::INVALID_STREAM_ID),
          finished_(false),
          readReady_(false),
          streamEnd_(false),
          readError_(false),
          execStarted_(false),
          traceRecorded_(false),
          trace_(Trace::Instance().GetTraceID(), std::move(methodName))
    {
        trace_.MarkServerRecv();
        // P3: Store scTimeoutDuration per-adapter so it survives bthread M:N migration.
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

        brpc::StreamOptions options;
        options.handler = this;
        int rc = brpc::StreamAccept(&streamId_, *cntl, &options);
        if (rc != 0) {
            LOG(ERROR) << "BrpcServerReaderImpl: StreamAccept failed (rc=" << rc << ")";
            cntl->SetFailed("BrpcServerReaderImpl: StreamAccept failed");
        }
    }

    // Arm self-keepalive. MUST be called once after the object is managed by a
    // shared_ptr (shared_from_this() is UB inside the constructor). The generated
    // stub calls this right after make_shared<BrpcServerReaderImpl>.
    void Init()
    {
        keepalive_ = this->shared_from_this();
    }

    // Non-blocking close: triggers brpc to fire on_closed asynchronously, which
    // drops the self-keepalive so this object can be destroyed safely once brpc
    // is done with the raw handler pointer. Called by the ServerReader wrapper dtor.
    void Close()
    {
        brpc::StreamId id = brpc::INVALID_STREAM_ID;
        {
            std::lock_guard<bthread::Mutex> lock(readMtx_);
            id = streamId_;
            streamId_ = brpc::INVALID_STREAM_ID;
        }
        if (id != brpc::INVALID_STREAM_ID) {
            brpc::StreamClose(id);
        }
    }

    ~BrpcServerReaderImpl() override
    {
        // Under keepalive this runs only after on_closed has fired (keepalive_
        // cleared) — or when Init() was never called. The stream is already closed
        // by then. Just record the trace.
        RecordTraceOnce();
    }

    // brpc::StreamInputHandler callbacks
    int on_received_messages(brpc::StreamId id, butil::IOBuf *const messages[], size_t size) override
    {
        (void)id;
        if (destroyed_->load(std::memory_order_acquire)) {
            return 0;  // stream logically closed; ignore racing late data
        }
        {
            std::lock_guard<bthread::Mutex> lock(readMtx_);
            for (size_t i = 0; i < size; ++i) {
                pendingMessages_.push_back(*messages[i]);
            }
            readReady_ = true;
        }
        readCond_.notify_one();
        return 0;
    }

    void on_idle_timeout(brpc::StreamId id) override
    {
        (void)id;
        if (destroyed_->load(std::memory_order_acquire)) {
            return;
        }
        std::lock_guard<bthread::Mutex> lock(readMtx_);
        readError_ = true;
        readCond_.notify_one();
    }

    void on_closed(brpc::StreamId id) override
    {
        (void)id;
        if (closeNotifier_) closeNotifier_->store(true, std::memory_order_relaxed);
        destroyed_->store(true, std::memory_order_release);
        {
            std::lock_guard<bthread::Mutex> lock(readMtx_);
            streamEnd_ = true;
            readCond_.notify_one();
        }
        // Drop self-keepalive LAST (on_closed is brpc's final callback). If this
        // was the last ref, this object is destroyed here, after the lock is
        // released and all member access is done.
        keepalive_.reset();
    }

    Status SendStatus(const Status &rc)
    {
        if (rc.IsError()) {
            cntl_->SetFailed(rc.GetMsg() + "\x01" "DS_ERR:" +
                std::to_string(static_cast<int>(rc.GetCode())) + "\x02");
        }
        return Status::OK();
    }

    Status Read(R &pb)
    {
        if (finished_.load()) {
            RETURN_STATUS(StatusCode::K_RPC_STREAM_END, "Stream already finished");
        }

        std::unique_lock<bthread::Mutex> lock(readMtx_);
        if (streamEnd_ && pendingMessages_.empty()) {
            RETURN_STATUS(StatusCode::K_RPC_STREAM_END, "Stream ended");
        }

        while (!readReady_ && !readError_ && !streamEnd_) {
            readCond_.wait(lock);
        }

        if (readError_) {
            RETURN_STATUS(StatusCode::K_RPC_CANCELLED, "Stream error during read");
        }

        if (pendingMessages_.empty()) {
            if (streamEnd_) {
                RETURN_STATUS(StatusCode::K_RPC_STREAM_END, "Stream ended");
            }
            RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, "Unexpected empty message queue");
        }

        butil::IOBuf msg = std::move(pendingMessages_.front());
        pendingMessages_.pop_front();
        if (pendingMessages_.empty() && !streamEnd_) {
            readReady_ = false;
        }
        lock.unlock();

        std::string serialized = msg.to_string();
        if (!pb.ParseFromString(serialized)) {
            RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, "Failed to parse protobuf from stream message");
        }
        bool expected = false;
        if (execStarted_.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
            trace_.MarkServerExecStart();
        }
        return Status::OK();
    }

    template <typename W>
    Status WritePb(const W &pb)
    {
        std::string serialized;
        if (!pb.SerializeToString(&serialized)) {
            RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, "Failed to serialize final response");
        }
        butil::IOBuf buf;
        buf.append(serialized);
        trace_.MarkServerExecEnd();
        cntl_->response_attachment().append(buf);
        RecordTraceOnce();
        return Status::OK();
    }

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

    Status SendPayload(std::vector<RpcMessage> &buffer)
    {
        (void)buffer;
        RETURN_STATUS(StatusCode::K_NOT_SUPPORTED, "SendPayload not supported");
    }

    Status SendPayload(const std::vector<MemView> &payload)
    {
        (void)payload;
        RETURN_STATUS(StatusCode::K_NOT_SUPPORTED, "SendPayload not supported");
    }

    Status ReceivePayload(std::vector<RpcMessage> &payload)
    {
        (void)payload;
        RETURN_STATUS(StatusCode::K_NOT_SUPPORTED, "ReceivePayload not supported");
    }

    /**
     * @brief Get per-adapter scTimeoutDuration (survives bthread M:N migration).
     */
    const TimeoutDuration& GetScTimeoutDuration() const { return scTimeoutDuration_; }

private:
    brpc::Controller *cntl_;
    brpc::StreamId streamId_;
    std::atomic<bool> finished_;

    bthread::Mutex readMtx_;
    bthread::ConditionVariable readCond_;
    std::deque<butil::IOBuf> pendingMessages_;
    bool readReady_;
    bool streamEnd_;
    bool readError_;
    // See BrpcClientReaderImpl for the keepalive/destroyed/closeNotifier rationale
    // (handler must outlive brpc's final on_closed callback).
    std::shared_ptr<std::atomic<bool>> closeNotifier_ =
        std::make_shared<std::atomic<bool>>(false);
    std::shared_ptr<std::atomic<bool>> destroyed_ =
        std::make_shared<std::atomic<bool>>(false);
    std::shared_ptr<BrpcServerReaderImpl<R>> keepalive_;
    TimeoutDuration scTimeoutDuration_;
    std::atomic<bool> execStarted_;
    std::atomic<bool> traceRecorded_;
    BrpcPerfTrace trace_;

    void RecordTraceOnce()
    {
        if (!traceRecorded_.exchange(true, std::memory_order_acq_rel)) {
            // Stream RPC tracing is one summary sample per stream, not one sample per message.
            trace_.MarkServerSend();
            // The client-streaming adapter has no trailer merge path yet.
            RecordBrpcRpcTrace(trace_);
        }
    }
};

}  // namespace datasystem
#endif  // DATASYSTEM_COMMON_RPC_BRPC_SERVER_STREAM_IMPL_H
