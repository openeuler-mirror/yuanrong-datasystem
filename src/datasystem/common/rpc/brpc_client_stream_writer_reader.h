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
#include <condition_variable>
#include <deque>
#include <mutex>
#include <vector>

#include <brpc/channel.h>
#include <brpc/controller.h>
#include <brpc/stream.h>
#include <butil/iobuf.h>
#include "datasystem/common/rpc/brpc_status_util.h"
#include "datasystem/common/rpc/client_writer_reader_base.h"
#include "datasystem/common/rpc/rpc_message.h"
#include "datasystem/common/rpc/mem_view.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {

/**
 * @brief brpc client-side streaming adapter implementing ClientWriterReaderBase.
 *
 * @tparam W Write (request) protobuf type.
 * @tparam R Read (response) protobuf type.
 */
template <typename W, typename R>
class BrpcClientStreamWriterReader : public ClientWriterReaderBase<W, R>, public brpc::StreamInputHandler {
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
          readError_(false)
    {
    }

    ~BrpcClientStreamWriterReader() override
    {
        if (streamId_ != brpc::INVALID_STREAM_ID) {
            brpc::StreamClose(streamId_);
            // StreamClose is async — brpc's ExecutionQueue may still have
            // pending Stream::Consume calls on a bthread. Must wait for
            // on_closed/on_failed to fire before destroying the Controller,
            // otherwise Consume dereferences a dangling Controller → SIGSEGV.
            // Timeout bounds the wait in case the peer is unresponsive.
            std::unique_lock<std::mutex> lk(readMtx_);
            bool closed = readCond_.wait_for(lk, std::chrono::seconds(kStreamCloseTimeoutSec),
                [this] { return streamEnd_ || readError_; });
            streamId_ = brpc::INVALID_STREAM_ID;
            if (!closed) {
                // Peer did not close within 5s. Resetting stream_cntl_ now could
                // UAF (brpc IO bthread may still dereference Controller). Intentionally
                // leak the Controller by releasing ownership; the memory is reachable
                // but never freed. This trades a small leak for crash safety.
                LOG(ERROR) << "BrpcClientStreamWriterReader destructor: stream close "
                              "timed out after 5s; intentionally leaking stream_cntl_ "
                              "to avoid UAF on in-flight brpc IO bthread";
                (void)stream_cntl_.release();
                return;
            }
        }
        stream_cntl_.reset();
    }

    // --- StreamInputHandler callbacks (called by brpc IO threads) ---

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
        std::lock_guard<std::mutex> lock(readMtx_);
        streamEnd_ = true;
        readReady_ = true;
        readCond_.notify_one();
    }

    void on_failed(brpc::StreamId id, int error_code, const std::string &error_text) override
    {
        (void)id;
        (void)error_code;
        LOG(WARNING) << "BrpcClientStreamWriterReader stream failed: " << error_text;
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

        // Send the RPC with the request protobuf in the body.
        R dummyResponse;
        channel_->CallMethod(method_, stream_cntl_.get(), &pb, &dummyResponse, nullptr);
        if (stream_cntl_->Failed()) {
            // The server refused the stream (or RPC failed). Close the stream
            // immediately to prevent brpc from calling Stream::Consume() on the
            // bthread execution queue, which would SIGSEGV on the unconnected state.
            Status embedded = TryExtractStatusFromResponse(dummyResponse);
            if (embedded.IsError()) {
                if (streamId_ != brpc::INVALID_STREAM_ID) {
                    brpc::StreamClose(streamId_);
                    // StreamClose is async. Must wait for on_closed/on_failed before
                    // resetting Controller, otherwise brpc IO bthread may still
                    // dereference it via Stream::Consume → UAF.
                    std::unique_lock<std::mutex> lk(readMtx_);
                    readCond_.wait_for(lk, std::chrono::seconds(kStreamCloseTimeoutSec),
                        [this] { return streamEnd_ || readError_; });
                    streamId_ = brpc::INVALID_STREAM_ID;
                }
                stream_cntl_.reset();
                return embedded;
            }
            if (streamId_ != brpc::INVALID_STREAM_ID) {
                brpc::StreamClose(streamId_);
                // StreamClose is async. Must wait for on_closed/on_failed before
                // resetting Controller, otherwise brpc IO bthread may still
                // dereference it via Stream::Consume → UAF.
                std::unique_lock<std::mutex> lk(readMtx_);
                readCond_.wait_for(lk, std::chrono::seconds(kStreamCloseTimeoutSec),
                    [this] { return streamEnd_ || readError_; });
                streamId_ = brpc::INVALID_STREAM_ID;
            }
            auto errText = stream_cntl_->ErrorText();
            stream_cntl_.reset();
            RETURN_STATUS(TryExtractStatusFromControllerError(errText).GetCode(),
                          errText.c_str());
        }

        return Status::OK();
    }

    Status Read(R &pb) override
    {
        if (finished_.load()) {
            RETURN_STATUS(StatusCode::K_RPC_STREAM_END, "Stream already finished");
        }

        std::unique_lock<std::mutex> lock(readMtx_);
        readCond_.wait(lock, [this] { return readReady_ || readError_ || streamEnd_; });

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
    Status Finish() override
    {
        if (finished_.exchange(true)) {
            return Status::OK();
        }
        if (streamId_ != brpc::INVALID_STREAM_ID) {
            brpc::StreamClose(streamId_);
            std::unique_lock<std::mutex> lk(readMtx_);
            bool closed = readCond_.wait_for(lk, std::chrono::seconds(kStreamCloseTimeoutSec),
                [this] { return streamEnd_ || readError_; });
            streamId_ = brpc::INVALID_STREAM_ID;
            if (closed) {
                stream_cntl_.reset();
            } else {
                (void)stream_cntl_.release();
            }
        } else {
            stream_cntl_.reset();
        }
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
};

}  // namespace datasystem
#endif  // DATASYSTEM_COMMON_RPC_BRPC_CLIENT_STREAM_WRITER_READER_H
