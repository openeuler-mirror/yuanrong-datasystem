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
#include <securec.h>
#include "datasystem/common/rpc/brpc_status_util.h"
#include "datasystem/common/rpc/rpc_message.h"
#include "datasystem/common/rpc/mem_view.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/util/status_helper.h"

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
        if (readOnce_.compare_exchange_strong(expected, true)) {
            brpc::Controller cntl;
            if (timeoutMs_ > 0) {
                cntl.set_timeout_ms(timeoutMs_);
            }
            if (!payloadBuf_.empty()) {
                cntl.request_attachment().append(payloadBuf_);
                payloadBuf_.clear();
            }
            channel_->CallMethod(method_, &cntl, &request_, &response_, nullptr);
            if (cntl.Failed()) {
                Status embedded = TryExtractStatusFromResponse(response_);
                if (embedded.IsError()) {
                    return embedded;
                }
                return TryExtractStatusFromControllerError(cntl.ErrorText());
            }
            responseAttachment_ = std::move(cntl.response_attachment());
            pb.CopyFrom(response_);
        } else {
            RETURN_STATUS(StatusCode::K_RUNTIME_ERROR,
                "BrpcClientUnaryWriterReader::Read called more than once");
        }
        return Status::OK();
    }

    Status ReceivePayload(std::vector<RpcMessage> &payload)
    {
        if (responseAttachment_.empty()) {
            return Status::OK();
        }
        std::string buf = responseAttachment_.to_string();
        const char *data = buf.data();
        size_t remaining = buf.size();
        size_t offset = 0;

        CHECK_FAIL_RETURN_STATUS(offset + sizeof(int64_t) <= remaining, StatusCode::K_RUNTIME_ERROR,
            "Malformed payload: missing count header");
        int64_t count = 0;
        errno_t rc = memcpy_s(&count, sizeof(count), data + offset, sizeof(count));
        CHECK_FAIL_RETURN_STATUS(rc == 0, StatusCode::K_RUNTIME_ERROR, "memcpy_s failed for count header");
        offset += sizeof(count);

        for (int64_t i = 0; i < count; ++i) {
            CHECK_FAIL_RETURN_STATUS(offset + sizeof(int64_t) <= remaining, StatusCode::K_RUNTIME_ERROR,
                "Malformed payload: truncated size header");
            int64_t sz = 0;
            rc = memcpy_s(&sz, sizeof(sz), data + offset, sizeof(sz));
            CHECK_FAIL_RETURN_STATUS(rc == 0, StatusCode::K_RUNTIME_ERROR, "memcpy_s failed for size header");
            offset += sizeof(sz);

            CHECK_FAIL_RETURN_STATUS(offset + static_cast<size_t>(sz) <= remaining, StatusCode::K_RUNTIME_ERROR,
                "Malformed payload: frame data exceeds attachment size");

            RpcMessage msg;
            RETURN_IF_NOT_OK(msg.CopyBuffer(data + offset, static_cast<size_t>(sz)));
            payload.emplace_back(std::move(msg));
            offset += static_cast<size_t>(sz);
        }
        return Status::OK();
    }

    Status ReceivePayload(void *dest, size_t sz)
    {
        CHECK_FAIL_RETURN_STATUS(!responseAttachment_.empty(), StatusCode::K_RUNTIME_ERROR,
            "No response attachment data");
        std::string buf = responseAttachment_.to_string();
        const char *data = buf.data();
        size_t remaining = buf.size();
        size_t offset = 0;

        CHECK_FAIL_RETURN_STATUS(offset + sizeof(int64_t) <= remaining, StatusCode::K_RUNTIME_ERROR,
            "Malformed payload: missing count header");
        int64_t count = 0;
        errno_t rc = memcpy_s(&count, sizeof(count), data + offset, sizeof(count));
        CHECK_FAIL_RETURN_STATUS(rc == 0, StatusCode::K_RUNTIME_ERROR, "memcpy_s failed for count header");
        offset += sizeof(count);

        CHECK_FAIL_RETURN_STATUS(count == 1, StatusCode::K_RUNTIME_ERROR,
            "Direct ReceivePayload expects exactly one frame");

        CHECK_FAIL_RETURN_STATUS(offset + sizeof(int64_t) <= remaining, StatusCode::K_RUNTIME_ERROR,
            "Malformed payload: truncated size header");
        int64_t frameSz = 0;
        rc = memcpy_s(&frameSz, sizeof(frameSz), data + offset, sizeof(frameSz));
        CHECK_FAIL_RETURN_STATUS(rc == 0, StatusCode::K_RUNTIME_ERROR, "memcpy_s failed for size header");
        offset += sizeof(frameSz);

        CHECK_FAIL_RETURN_STATUS(static_cast<size_t>(frameSz) == sz, StatusCode::K_RUNTIME_ERROR,
            "Payload size mismatch");
        CHECK_FAIL_RETURN_STATUS(offset + sz <= remaining, StatusCode::K_RUNTIME_ERROR,
            "Malformed payload: frame data exceeds attachment size");
        rc = memcpy_s(dest, sz, data + offset, sz);
        CHECK_FAIL_RETURN_STATUS(rc == 0, StatusCode::K_RUNTIME_ERROR, "memcpy_s failed for payload data");
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
