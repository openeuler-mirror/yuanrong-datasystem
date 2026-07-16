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
 * Description: brpc client-side streaming WriterReader adapter.
 * Bridges brpc::StreamCreate/StreamWrite to the ClientWriterReaderBase
 * interface used by client code (e.g., MasterRemoteWorkerSCApi::QueryMetadata).
 *
 * Architecture:
 *   - Write(req) sends the request in the RPC body and establishes the stream.
 *   - Read(rsp) receives responses via the stream's StreamInputHandler callback.
 *   - The stream is created with StreamCreate before the RPC, so the server
 *     can accept it during CallMethod.
 *
 * Flow:
 *   1. Write(req) -> StreamCreate + CallMethod(req) -> server gets req + stream
 *   2. Read(rsp) N times -> on_received_messages delivers each response
 *   3. Stream closes when server calls Finish/StreamClose -> on_closed
 */
#ifndef DATASYSTEM_COMMON_RPC_BRPC_CLIENT_STREAM_WRITER_READER_H
#define DATASYSTEM_COMMON_RPC_BRPC_CLIENT_STREAM_WRITER_READER_H

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <mutex>
#include <vector>

#include <brpc/channel.h>
#include <brpc/controller.h>
#include <brpc/stream.h>
#include <butil/iobuf.h>
#include "datasystem/common/rpc/brpc_perf_trace.h"
#include "datasystem/common/rpc/brpc_status_util.h"
#include "datasystem/common/rpc/brpc_stream_close_helper.h"
#include "datasystem/common/rpc/client_writer_reader_base.h"
#include "datasystem/common/rpc/rpc_message.h"
#include "datasystem/common/rpc/mem_view.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/rpc/trace_attachment.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {

/**
 * @brief brpc client-side streaming adapter implementing ClientWriterReaderBase.
 *
 * @tparam W Write (request) protobuf type.
 * @tparam R Read (response) protobuf type.
 */
template <typename W, typename R>
class BrpcClientStreamWriterReader : public ClientWriterReaderBase<W, R>,
                                     public brpc::StreamInputHandler,
                                     public std::enable_shared_from_this<BrpcClientStreamWriterReader<W, R>> {
public:
    BrpcClientStreamWriterReader(brpc::Channel *channel,
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
          readError_(false),
          traceRecorded_(false),
          trace_(Trace::Instance().GetTraceID(), method == nullptr ? "unknown" : method->full_name())
    {
    }

    ~BrpcClientStreamWriterReader() override
    {
        // Under the keepalive model this runs only after on_closed has fired (or
        // Write was never called). Nothing to close; just record the trace.
        if (ShouldRecordBrpcTraceOnDestroy(trace_)) {
            RecordTraceOnce();
        }
    }

    // --- StreamInputHandler callbacks (called by brpc IO threads) ---

    int on_received_messages(brpc::StreamId id, butil::IOBuf *const messages[], size_t size) override
    {
        (void)id;
        if (destroyed_->load(std::memory_order_acquire)) {
            return 0;  // stream logically closed; ignore racing late data
        }
        if (firstResponseTs_.exchange(BrpcTraceNowNs()) == 0) {
            trace_.MarkClientRecv(firstResponseTs_.load());
        }
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
        if (destroyed_->load(std::memory_order_acquire)) {
            return;
        }
        std::lock_guard<std::mutex> lock(readMtx_);
        readError_ = true;
        readCond_.notify_one();
    }

    void on_closed(brpc::StreamId id) override
    {
        (void)id;
        if (closeNotifier_) closeNotifier_->store(true, std::memory_order_relaxed);
        destroyed_->store(true, std::memory_order_release);
        {
            std::lock_guard<std::mutex> lock(readMtx_);
            streamEnd_ = true;
            readReady_ = true;
            readCond_.notify_one();
        }
        // Drop self-keepalive LAST (on_closed is brpc's final callback). If this
        // was the last ref, this object is destroyed here, after the lock is
        // released and all member access is done.
        keepalive_.reset();
    }

    void on_failed(brpc::StreamId id, int error_code, const std::string &error_text) override
    {
        (void)id;
        (void)error_code;
        LOG(WARNING) << "BrpcClientStreamWriterReader stream failed: " << error_text;
        if (destroyed_->load(std::memory_order_acquire)) {
            return;
        }
        // Do NOT set closeNotifier_ here: brpc always fires on_closed after on_failed
        // (stream.cpp:594 -> :596), and the Controller/handler are only safe to release
        // once on_closed (the final callback) has run. Signalling on on_failed would let
        // deferred cleanup free state that on_closed still touches.
        std::lock_guard<std::mutex> lock(readMtx_);
        readError_ = true;
        readCond_.notify_one();
    }

    // --- ClientWriterReaderBase interface ---

    Status Write(const W &pb) override
    {
        bool expected = false;
        if (!writeOnce_.compare_exchange_strong(expected, true)) {
            RETURN_STATUS(StatusCode::K_RUNTIME_ERROR,
                "BrpcClientStreamWriterReader::Write called more than once");
        }

        // Create the stream before the RPC so the server can accept it.
        // Heap-allocate cntl: brpc StreamCreate docs require the Controller
        // to survive until stream close. A stack cntl would go out of scope
        // when Write() returns, leaving streamId_ with a dangling reference.
        stream_cntl_ = std::make_unique<brpc::Controller>();
        if (timeoutMs_ > 0) {
            stream_cntl_->set_timeout_ms(timeoutMs_);
        }

        brpc::StreamOptions options;
        options.handler = this;  // This object receives stream messages
        int rc = brpc::StreamCreate(&streamId_, *stream_cntl_, &options);
        if (rc != 0) {
            stream_cntl_.reset();
            RETURN_STATUS(StatusCode::K_RUNTIME_ERROR,
                "brpc::StreamCreate failed");
        }
        // Arm self-keepalive so this object outlives brpc's final on_closed.
        keepalive_ = this->shared_from_this();

        AttachTraceIDToAttachment(stream_cntl_->request_attachment());
        // Send the RPC with the request protobuf in the body.
        R dummyResponse;
        trace_.MarkClientStart();
        trace_.MarkClientSend();
        channel_->CallMethod(method_, stream_cntl_.get(), &pb, &dummyResponse, nullptr);
        if (stream_cntl_->Failed()) {
            trace_.MarkClientRecv();
            RecordTraceOnce();
            // The server refused the stream (or RPC failed). Close the stream
            // immediately to prevent brpc from calling Stream::Consume() on the
            // bthread execution queue, which would SIGSEGV on the unconnected state.
            Status embedded = TryExtractStatusFromResponse(dummyResponse);
            if (embedded.IsError()) {
                Close();  // trigger async on_closed so the armed keepalive_ can release
                return embedded;
            }
            // Capture errText AND errCode up front (f29c788c2): Close() itself does
            // not touch stream_cntl_, but capturing before any close-path keeps the
            // reads stable and preserves the SEGV-fix shape.
            auto errText = stream_cntl_->ErrorText();
            auto errCode = stream_cntl_->ErrorCode();
            Close();  // trigger async on_closed so the armed keepalive_ can release
            // Wrap with RETURN_STATUS to retain this adapter's call-site
            // (file/line) for on-call; the helper's Status carries the brpc
            // errno/name + ErrorText diagnostics in its message.
            auto st = TryExtractStatusFromControllerError(errText, errCode);
            RETURN_STATUS(st.GetCode(), st.GetMsg());
        }

        return Status::OK();
    }

    Status Read(R &pb) override
    {
        if (finished_.load()) {
            RETURN_STATUS(StatusCode::K_RPC_STREAM_END, "Stream already finished");
        }

        std::unique_lock<std::mutex> lock(readMtx_);
        // Bound the wait so a peer crash / network partition cannot block the calling
        // bthread forever (which would exhaust the bthread pool under nested RPC load).
        // 30s > kStreamCloseTimeoutSec(5s), well below typical client upper-layer deadlines.
        if (!readCond_.wait_for(lock, std::chrono::seconds(kStreamReadTimeoutSec),
            [this] { return readReady_ || readError_ || streamEnd_; })) {
            LOG(ERROR) << "Stream read timed out after " << kStreamReadTimeoutSec
                       << "s, streamId=" << streamId_;
            RETURN_STATUS(StatusCode::K_RPC_DEADLINE_EXCEEDED, "Stream read timeout");
        }

        if (readError_) {
            RETURN_STATUS(StatusCode::K_RPC_CANCELLED, "Stream error during read");
        }

        // Check if we have pending messages
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
            RecordTraceOnce();
            RETURN_STATUS(StatusCode::K_RPC_STREAM_END, "Stream ended");
        }

        RETURN_STATUS(StatusCode::K_RPC_CANCELLED, "Unexpected read state");
    }

    Status SendPayload(const std::vector<MemView> &payload) override
    {
        (void)payload;
        RETURN_STATUS(StatusCode::K_NOT_SUPPORTED,
            "SendPayload not supported in brpc streaming client adapter");
    }

    Status ReceivePayload(std::vector<RpcMessage> &payload) override
    {
        (void)payload;
        RETURN_STATUS(StatusCode::K_NOT_SUPPORTED,
            "ReceivePayload not supported in brpc streaming client adapter");
    }

    /**
     * @brief Signal that writing is done. For brpc streaming, this closes
     * the client-side stream to signal completion.
     * @return Status of the call.
     */
    // Non-blocking close: triggers brpc to fire on_closed asynchronously, which
    // drops the self-keepalive and lets this object be destroyed safely once brpc
    // is finished with the raw handler pointer. Called by the wrapper destructor.
    void Close() override
    {
        brpc::StreamId id = brpc::INVALID_STREAM_ID;
        {
            std::lock_guard<std::mutex> lock(readMtx_);
            id = streamId_;
            streamId_ = brpc::INVALID_STREAM_ID;
        }
        if (id != brpc::INVALID_STREAM_ID) {
            brpc::StreamClose(id);
        }
    }

    /**
     * @brief Signal that writing is done. For brpc streaming, this closes
     * the client-side stream to signal completion.
     * @return Status of the call.
     */
    Status Finish() override
    {
        if (finished_.exchange(true)) {
            return Status::OK();
        }
        // Bounded wait for on_closed. On timeout the object stays alive via
        // keepalive_ until the (possibly late) on_closed fires — safe, no UAF.
        (void)StreamCloseAndWait(
            {streamId_, readMtx_, readCond_, streamEnd_, readError_, closeNotifier_, kDefaultDeferredWaitSec});
        RecordTraceOnce();
        return Status::OK();
    }

private:
    brpc::Channel *channel_;
    const google::protobuf::MethodDescriptor *method_;
    int32_t timeoutMs_;
    std::unique_ptr<brpc::Controller> stream_cntl_;
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
    std::shared_ptr<std::atomic<bool>> destroyed_ =
        std::make_shared<std::atomic<bool>>(false);
    // Self-keepalive: armed after StreamCreate, cleared at the tail of on_closed
    // so this object outlives brpc's final callback. Closes the UAF window.
    std::shared_ptr<BrpcClientStreamWriterReader<W, R>> keepalive_;
    std::atomic<uint64_t> firstResponseTs_ { 0 };
    std::atomic<bool> traceRecorded_;
    BrpcPerfTrace trace_;

    void RecordTraceOnce()
    {
        if (!traceRecorded_.exchange(true, std::memory_order_acq_rel)) {
            // Stream RPC tracing is one summary sample per stream, not one sample per message.
            trace_.MarkClientEnd();
            RecordBrpcRpcTrace(trace_);
        }
    }
};

}  // namespace datasystem
#endif  // DATASYSTEM_COMMON_RPC_BRPC_CLIENT_STREAM_WRITER_READER_H
