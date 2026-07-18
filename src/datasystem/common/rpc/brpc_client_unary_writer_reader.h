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
 * Description: brpc client-side unary writer/reader adapter.
 * Provides the same interface as ClientUnaryWriterReaderImpl but uses
 * brpc::Channel for synchronous unary RPC calls with IOBuf attachment payloads.
 */
#ifndef DATASYSTEM_COMMON_RPC_BRPC_CLIENT_UNARY_WRITER_READER_H
#define DATASYSTEM_COMMON_RPC_BRPC_CLIENT_UNARY_WRITER_READER_H

#include <atomic>
#include <vector>

#include <brpc/channel.h>
#include <brpc/controller.h>
#include <butil/iobuf.h>
#include "datasystem/common/rpc/brpc_status_util.h"
#include "datasystem/common/rpc/brpc_perf_trace.h"
#include "datasystem/common/rpc/rpc_message.h"
#include "datasystem/common/rpc/mem_view.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/rpc/trace_attachment.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"

namespace datasystem {

template <typename W, typename R>
class BrpcClientUnaryWriterReader {
public:
    BrpcClientUnaryWriterReader(brpc::Channel *channel,
                                const google::protobuf::MethodDescriptor *method,
                                int32_t timeoutMs)
        : channel_(channel),
          method_(method),
          timeoutMs_(timeoutMs),
          writeOnce_(false),
          readOnce_(false)
    {
    }

    ~BrpcClientUnaryWriterReader() = default;

    Status Write(const W &pb)
    {
        bool expected = false;
        if (writeOnce_.compare_exchange_strong(expected, true)) {
            request_.CopyFrom(pb);
        } else {
            RETURN_STATUS(StatusCode::K_RUNTIME_ERROR,
                "BrpcClientUnaryWriterReader::Write called more than once");
        }
        return Status::OK();
    }

    Status SendPayload(const std::vector<MemView> &payload)
    {
        AppendMemViewToRequestAttachment(payload);
        return Status::OK();
    }

    Status AsyncSendPayload(const std::vector<MemView> &payload)
    {
        return SendPayload(payload);
    }

    Status Read(R &pb)
    {
        bool expected = false;
        if (!readOnce_.compare_exchange_strong(expected, true)) {
            RETURN_STATUS(StatusCode::K_RUNTIME_ERROR,
                "BrpcClientUnaryWriterReader::Read called more than once");
        }
        return DoRead(pb);
    }

    Status ReceivePayload(std::vector<RpcMessage> &payload)
    {
        if (responseAttachment_.empty()) {
            return Status::OK();
        }
        size_t remaining = responseAttachment_.size();
        size_t offset = 0;

        CHECK_FAIL_RETURN_STATUS(offset + sizeof(int64_t) <= remaining, StatusCode::K_RUNTIME_ERROR,
            "Malformed payload: missing count header");
        int64_t count = 0;
        responseAttachment_.copy_to(reinterpret_cast<char *>(&count), sizeof(count), offset);
        offset += sizeof(count);

        for (int64_t i = 0; i < count; ++i) {
            CHECK_FAIL_RETURN_STATUS(offset + sizeof(int64_t) <= remaining, StatusCode::K_RUNTIME_ERROR,
                "Malformed payload: truncated size header");
            int64_t sz = 0;
            responseAttachment_.copy_to(reinterpret_cast<char *>(&sz), sizeof(sz), offset);
            offset += sizeof(sz);

            CHECK_FAIL_RETURN_STATUS(sz >= 0, StatusCode::K_RUNTIME_ERROR,
                "Malformed payload: negative frame size");
            CHECK_FAIL_RETURN_STATUS(offset + static_cast<size_t>(sz) <= remaining, StatusCode::K_RUNTIME_ERROR,
                "Malformed payload: frame data exceeds attachment size");

            RpcMessage msg;
            RETURN_IF_NOT_OK(msg.AllocMem(static_cast<size_t>(sz)));
            responseAttachment_.copy_to(static_cast<char *>(msg.Data()), static_cast<size_t>(sz), offset);
            payload.emplace_back(std::move(msg));
            offset += static_cast<size_t>(sz);
        }
        return Status::OK();
    }

    Status ReceivePayload(void *dest, size_t sz)
    {
        CHECK_FAIL_RETURN_STATUS(!responseAttachment_.empty(), StatusCode::K_RUNTIME_ERROR,
            "No response attachment data");
        size_t remaining = responseAttachment_.size();
        size_t offset = 0;

        CHECK_FAIL_RETURN_STATUS(offset + sizeof(int64_t) <= remaining, StatusCode::K_RUNTIME_ERROR,
            "Malformed payload: missing count header");
        int64_t count = 0;
        responseAttachment_.copy_to(reinterpret_cast<char *>(&count), sizeof(count), offset);
        offset += sizeof(count);

        CHECK_FAIL_RETURN_STATUS(count == 1, StatusCode::K_RUNTIME_ERROR,
            "Direct ReceivePayload expects exactly one frame");

        CHECK_FAIL_RETURN_STATUS(offset + sizeof(int64_t) <= remaining, StatusCode::K_RUNTIME_ERROR,
            "Malformed payload: truncated size header");
        int64_t frameSz = 0;
        responseAttachment_.copy_to(reinterpret_cast<char *>(&frameSz), sizeof(frameSz), offset);
        offset += sizeof(frameSz);

        CHECK_FAIL_RETURN_STATUS(frameSz >= 0, StatusCode::K_RUNTIME_ERROR,
            "Malformed payload: negative frame size");
        CHECK_FAIL_RETURN_STATUS(static_cast<size_t>(frameSz) == sz, StatusCode::K_RUNTIME_ERROR,
            "Payload size mismatch");
        CHECK_FAIL_RETURN_STATUS(offset + sz <= remaining, StatusCode::K_RUNTIME_ERROR,
            "Malformed payload: frame data exceeds attachment size");
        responseAttachment_.copy_to(dest, sz, offset);
        return Status::OK();
    }

    void CleanupOnError(const Status &rc)
    {
        (void)rc;
    }

    bool IsV2Client() const
    {
        return false;
    }

private:
    Status DoRead(R &pb)
    {
        BrpcPerfTrace trace(Trace::Instance().GetTraceID(), method_ == nullptr ? "unknown" : method_->full_name());
        trace.MarkClientStart();
        brpc::Controller cntl;
        if (timeoutMs_ > 0) {
            cntl.set_timeout_ms(timeoutMs_);
        }
        AttachTraceIDToAttachment(cntl.request_attachment());
        if (!payloadBuf_.empty()) {
            cntl.request_attachment().append(payloadBuf_);
            payloadBuf_.clear();
        }
        trace.MarkClientSend();
        channel_->CallMethod(method_, &cntl, &request_, &response_, nullptr);
        trace.MarkClientRecv();
        if (cntl.Failed()) {
            return HandleReadFailure(cntl, trace);
        }
        responseAttachment_ = std::move(cntl.response_attachment());
        MergeBrpcServerTraceTrailer(responseAttachment_, trace);
        pb.CopyFrom(response_);
        trace.MarkClientEnd();
        RecordBrpcRpcTrace(trace);
        return Status::OK();
    }

    Status HandleReadFailure(brpc::Controller &cntl, BrpcPerfTrace &trace)
    {
        butil::IOBuf errorAttachment = cntl.response_attachment();
        MergeBrpcServerTraceTrailer(errorAttachment, trace);
        trace.MarkClientEnd();
        RecordBrpcRpcTrace(trace);
        Status embedded = TryExtractStatusFromResponse(response_);
        const auto &errorText = cntl.ErrorText();
        VLOG(1) << "[BRPC_UNARY_READ_FAILED] method="
                << (method_ == nullptr ? "UNKNOWN" : method_->full_name())
                << ", errorCode=" << cntl.ErrorCode()
                << ", containsDsErr=" << (errorText.find("\x01" "DS_ERR:") != std::string::npos)
                << ", embeddedStatus=" << embedded.ToString()
                << ", localSide=" << cntl.local_side() << ", remoteSide=" << cntl.remote_side()
                << ", errorTextLen=" << errorText.size()
                << ", errorText=" << FormatStringForLog(errorText);
        VLOG(kBrpcDetailLogLevel) << "[BRPC_UNARY_READ_FAILED_DETAIL] method="
                                  << (method_ == nullptr ? "UNKNOWN" : method_->full_name())
                                  << ", timeoutMs=" << timeoutMs_
                                  << ", responseDebug=" << response_.ShortDebugString();
        return embedded.IsError() ? embedded : TryExtractStatusFromControllerError(errorText, cntl.ErrorCode());
    }

    void AppendMemViewToRequestAttachment(const std::vector<MemView> &payload)
    {
        int64_t count = static_cast<int64_t>(payload.size());
        payloadBuf_.append(reinterpret_cast<const char *>(&count), sizeof(count));
        for (const auto &view : payload) {
            int64_t sz = static_cast<int64_t>(view.Size());
            payloadBuf_.append(reinterpret_cast<const char *>(&sz), sizeof(sz));
            payloadBuf_.append(static_cast<const char *>(view.Data()), view.Size());
        }
    }

    brpc::Channel *channel_;
    const google::protobuf::MethodDescriptor *method_;
    int32_t timeoutMs_;
    W request_;
    R response_;
    butil::IOBuf payloadBuf_;
    butil::IOBuf responseAttachment_;
    std::atomic<bool> writeOnce_;
    std::atomic<bool> readOnce_;
};

}  // namespace datasystem
#endif  // DATASYSTEM_COMMON_RPC_BRPC_CLIENT_UNARY_WRITER_READER_H
