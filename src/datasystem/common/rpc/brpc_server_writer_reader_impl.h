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
 * Description: brpc server-side streaming WriterReader adapter.
 * Bridges brpc::StreamWrite/StreamAccept to the ServerWriterReaderBase
 * interface used by service implementations (e.g., QueryMetadata).
 *
 * Architecture:
 *   - The RPC request is carried in the normal protobuf body (not the stream).
 *     Read() copies from the request protobuf passed by CallMethod.
 *   - Responses are sent via brpc::StreamWrite on a stream accepted in the
 *     constructor via StreamAccept. The client must create the stream with
 *     StreamCreate before issuing the RPC.
 *   - Finish() closes the stream.
 *
 * CallMethod flow:
 *   1. Code-generated CallMethod creates BrpcServerWriterReaderImpl(cntl, request)
 *   2. Passes it as ServerWriterReader to the service implementation
 *   3. Service calls Read(req)  -> copies request proto
 *   4. Service calls Write(rsp) N times -> StreamWrite each response
 *   5. Service calls Finish() -> StreamClose
 *   6. CallMethod returns, brpc sends the RPC response
 */
#ifndef DATASYSTEM_COMMON_RPC_BRPC_SERVER_WRITER_READER_IMPL_H
#define DATASYSTEM_COMMON_RPC_BRPC_SERVER_WRITER_READER_IMPL_H

#include <sys/time.h>
#include <atomic>
#include <string>
#include <vector>

#include <brpc/controller.h>
#include <brpc/stream.h>
#include <butil/iobuf.h>
#include "datasystem/common/log/trace.h"
#include "datasystem/common/rpc/brpc_perf_trace.h"
#include "datasystem/common/rpc/server_writer_reader_base.h"
#include "datasystem/common/rpc/rpc_message.h"
#include "datasystem/common/rpc/mem_view.h"
#include "datasystem/common/rpc/timeout_duration.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {

/**
 * @brief brpc adapter that provides the ServerWriterReaderBase streaming interface.
 *
 * Read() reads from the RPC request protobuf (already deserialized by CallMethod).
 * Write() serializes a protobuf and sends it via brpc::StreamWrite.
 * Finish() closes the stream.
 *
 * @tparam W Output (response) protobuf type.
 * @tparam R Input (request) protobuf type.
 */
template <typename W, typename R>
class BrpcServerWriterReaderImpl : public ServerWriterReaderBase<W, R> {
public:
    /**
     * @brief Construct, accept a brpc stream, and capture request.
     * @param cntl    brpc controller.
     * @param request Pointer to the deserialized request protobuf.
     */
    BrpcServerWriterReaderImpl(brpc::Controller *cntl, const google::protobuf::Message *request,
                               std::string methodName = "unknown")
        : cntl_(cntl),
          request_(nullptr),
          streamId_(brpc::INVALID_STREAM_ID),
          acceptFailed_(false),
          readOnce_(false),
          finished_(false),
          traceRecorded_(false),
          trace_(Trace::Instance().GetTraceID(), std::move(methodName))
    {
        trace_.MarkServerRecv();
        request_ = dynamic_cast<const R *>(request);

        // Accept the stream that the client created with StreamCreate.
        brpc::StreamOptions options;
        // No handler needed -- server does not read from the stream.
        // The request arrives in the RPC body; responses go out via StreamWrite.
        options.handler = nullptr;
        int rc = brpc::StreamAccept(&streamId_, *cntl, &options);
        if (rc != 0) {
            // Fail fast: without a stream, downstream Write/SendStatus would hang
            // waiting on streamId_. Mark the Controller failed so the client
            // sees the error immediately rather than timing out.
            acceptFailed_ = true;
            LOG(ERROR) << "BrpcServerWriterReaderImpl: StreamAccept failed (rc=" << rc
                       << "), marking Controller failed to prevent client hang";
            cntl->SetFailed("BrpcServerWriterReaderImpl: StreamAccept failed");
        }

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
    }

    ~BrpcServerWriterReaderImpl() override
    {
        if (streamId_ != brpc::INVALID_STREAM_ID) {
            brpc::StreamClose(streamId_);
            streamId_ = brpc::INVALID_STREAM_ID;
        }
        RecordTraceOnce();
    }

    // --- ServerWriterReaderBase interface ---

    Status Read(R &pb) override
    {
        bool expected = false;
        if (!readOnce_.compare_exchange_strong(expected, true)) {
            RETURN_STATUS(StatusCode::K_RUNTIME_ERROR,
                "BrpcServerWriterReaderImpl::Read called more than once");
        }
        if (request_ == nullptr) {
            RETURN_STATUS(StatusCode::K_RUNTIME_ERROR,
                "Request protobuf is null (type mismatch?)");
        }
        trace_.MarkServerExecStart();
        pb.CopyFrom(*request_);
        return Status::OK();
    }

    Status Write(const W &pb) override
    {
        if (acceptFailed_) {
            RETURN_STATUS(StatusCode::K_RUNTIME_ERROR,
                "Stream not available: StreamAccept failed during construction");
        }
        if (finished_.load()) {
            RETURN_STATUS(StatusCode::K_RUNTIME_ERROR,
                "Write called after Finish");
        }
        if (streamId_ == brpc::INVALID_STREAM_ID) {
            RETURN_STATUS(StatusCode::K_RUNTIME_ERROR,
                "Stream not initialized (StreamAccept failed?)");
        }

        // Serialize protobuf to string, then to IOBuf
        std::string serialized;
        if (!pb.SerializeToString(&serialized)) {
            RETURN_STATUS(StatusCode::K_RUNTIME_ERROR,
                "Failed to serialize protobuf for stream write");
        }

        butil::IOBuf buf;
        buf.append(serialized);
        int rc = brpc::StreamWrite(streamId_, buf);
        if (rc != 0) {
            RETURN_STATUS(StatusCode::K_RPC_CANCELLED,
                "brpc::StreamWrite failed");
        }
        return Status::OK();
    }

    Status Finish() override
    {
        if (finished_.exchange(true)) {
            return Status::OK();  // Already finished
        }
        if (streamId_ != brpc::INVALID_STREAM_ID) {
            trace_.MarkServerExecEnd();
            brpc::StreamClose(streamId_);
            streamId_ = brpc::INVALID_STREAM_ID;
        }
        RecordTraceOnce();
        return Status::OK();
    }

    Status SendStatus(const Status &rc) override
    {
        if (acceptFailed_) {
            // StreamAccept failed: cannot send error frame to client,
            // but the Controller is already marked Failed so the client
            // will see the error regardless.
            return Finish();
        }
        if (rc.IsError()) {
            LOG(WARNING) << "BrpcServerWriterReaderImpl::SendStatus: " << rc.ToString();
            // Write an error status frame so the client can recover the Status
            // code instead of treating stream close as normal end-of-stream.
            if (streamId_ != brpc::INVALID_STREAM_ID) {
                std::string errFrame = "\x01" "DS_ERR:"
                    + std::to_string(static_cast<int>(rc.GetCode())) + "\x02" + rc.GetMsg();
                butil::IOBuf buf;
                buf.append(errFrame);
                (void)brpc::StreamWrite(streamId_, buf);
            }
        }
        return Finish();
    }

    Status SendPayload(std::vector<RpcMessage> &buffer) override
    {
        if (finished_.load() || streamId_ == brpc::INVALID_STREAM_ID) {
            RETURN_STATUS(StatusCode::K_RUNTIME_ERROR,
                "SendPayload called in invalid state");
        }
        // Serialize payload frames: [int64_t count][int64_t size1][data1]...
        butil::IOBuf buf;
        int64_t count = static_cast<int64_t>(buffer.size());
        buf.append(reinterpret_cast<const char *>(&count), sizeof(count));
        for (const auto &msg : buffer) {
            int64_t sz = static_cast<int64_t>(msg.Size());
            buf.append(reinterpret_cast<const char *>(&sz), sizeof(sz));
            buf.append(static_cast<const char *>(msg.Data()), msg.Size());
        }
        int rc = brpc::StreamWrite(streamId_, buf);
        if (rc != 0) {
            RETURN_STATUS(StatusCode::K_RPC_CANCELLED,
                "brpc::StreamWrite (payload) failed");
        }
        return Status::OK();
    }

    Status SendPayload(const std::vector<MemView> &payload) override
    {
        if (finished_.load() || streamId_ == brpc::INVALID_STREAM_ID) {
            RETURN_STATUS(StatusCode::K_RUNTIME_ERROR,
                "SendPayload called in invalid state");
        }
        butil::IOBuf buf;
        int64_t count = static_cast<int64_t>(payload.size());
        buf.append(reinterpret_cast<const char *>(&count), sizeof(count));
        for (const auto &view : payload) {
            int64_t sz = static_cast<int64_t>(view.Size());
            buf.append(reinterpret_cast<const char *>(&sz), sizeof(sz));
            buf.append(static_cast<const char *>(view.Data()), view.Size());
        }
        int rc = brpc::StreamWrite(streamId_, buf);
        if (rc != 0) {
            RETURN_STATUS(StatusCode::K_RPC_CANCELLED,
                "brpc::StreamWrite (payload) failed");
        }
        return Status::OK();
    }

    Status ReceivePayload(std::vector<RpcMessage> &payload) override
    {
        (void)payload;
        RETURN_STATUS(StatusCode::K_NOT_SUPPORTED,
            "ReceivePayload not supported in brpc streaming adapter");
    }

    /**
     * @brief Get per-adapter scTimeoutDuration (survives bthread M:N migration).
     */
    const TimeoutDuration& GetScTimeoutDuration() const { return scTimeoutDuration_; }

private:
    brpc::Controller *cntl_;
    const R *request_;
    brpc::StreamId streamId_;
    bool acceptFailed_;
    std::atomic<bool> readOnce_;
    std::atomic<bool> finished_;
    TimeoutDuration scTimeoutDuration_;
    std::atomic<bool> traceRecorded_;
    BrpcPerfTrace trace_;

    void RecordTraceOnce()
    {
        if (!traceRecorded_.exchange(true, std::memory_order_acq_rel)) {
            // Stream RPC tracing is one summary sample per stream, not one sample per message.
            trace_.MarkServerSend();
            RecordBrpcRpcTrace(trace_);
        }
    }
};

}  // namespace datasystem
#endif  // DATASYSTEM_COMMON_RPC_BRPC_SERVER_WRITER_READER_IMPL_H
