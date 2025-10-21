/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2022. All rights reserved.
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
 * Description: Zmq unary client apis.
 * Including stream RPC and non-blocking unary RPC.
 * In stream RPC, we have three combinations of reader and writer streaming mode for client and server, respectively.
 * In non-blocking unary RPC, we have ServerUnaryWriterReader.
 */
#ifndef DATASYSTEM_COMMON_RPC_ZMQ_STREAM_H
#define DATASYSTEM_COMMON_RPC_ZMQ_STREAM_H

#include <deque>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "datasystem/common/rpc/zmq/rpc_service_method.h"
#include "datasystem/common/rpc/zmq/zmq_msg_decoder.h"
#include "datasystem/common/rpc/zmq/zmq_msg_queue.h"
#include "datasystem/common/rpc/zmq/zmq_stream_base.h"
#include "datasystem/common/rpc/zmq/zmq_stub.h"
#include "datasystem/common/log/log_helper.h"
#include "datasystem/common/util/gflag/common_gflags.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {
template <typename W, typename R>
class ClientUnaryWriterReaderImpl : public StreamBase {
public:
    explicit ClientUnaryWriterReaderImpl(std::shared_ptr<ZmqMsgQueRef> mQue, const std::string &svcName,
                                         int32_t methodIndex, bool sendPayload, bool recvPayload)
        : StreamBase::StreamBase(sendPayload, recvPayload),
          mQue_(std::move(mQue)),
          writeOnce_(false),
          readOnce_(false),
          v2Server_(false),
          payloadId_(-1),
          payloadSz_(0)
    {
        meta_ = CreateMetaData(svcName, methodIndex, sendPayload ? ZMQ_EMBEDDED_PAYLOAD_INX : ZMQ_INVALID_PAYLOAD_INX,
                               mQue_->GetId());
    }
    ~ClientUnaryWriterReaderImpl() override
    {
        mQue_->Close();
    }

    Status Write(const W &pb)
    {
        Status status = WriteImpl(pb);
        return status;
    }

    /**
     * @brief Send a payload after sending the request protobuf.
     * @note The option send_payload_option must be set in the proto. Must be called after Write().
     * @param[in] payload Sending payload buffers.
     * @return Status of call.
     */
    Status SendPayload(const std::vector<MemView> &payload)
    {
        Status status = SendPayloadImpl(payload);
        return status;
    }

    Status AsyncSendPayload(const std::vector<MemView> &payload)
    {
        Status status = AsyncSendPayloadImpl(payload);
        return status;
    }

    Status Read(R &pb)
    {
        Status status = ReadImpl(pb);
        return status;
    }

    /**
     * @brief Receive a payload after receiving response protobuf.
     * @note The option recv_payload_option must be set in the proto. Must be called after Read().
     * @param[out] recvBuffer receiving payload buffers.
     * @return Status of call.
     */
    Status ReceivePayload(std::vector<RpcMessage> &recvBuffer)
    {
        CHECK_FAIL_RETURN_STATUS(HasRecvPayloadOp(), StatusCode::K_INVALID,
                                 "rev_payload_option is not specified in the proto");
        // We are supposed to call the other form with buffer allocated to get the payload.
        // But if the caller is a downlevel client, it may just call this function directly.
        // Send a special rc to the server to do it the old way.
        ZmqMetaMsgFrames reply;
        if (payloadId_ >= 0) {
            PayloadDirectGetReqPb rq;
            rq.set_sz(payloadSz_);
            rq.set_id(payloadId_);
            rq.set_error_code(K_NOT_READY);
            RETURN_IF_NOT_OK(RequestPayload(rq));
            RETURN_IF_NOT_OK(mQue_->ClientReceiveMsg(reply, ZmqRecvFlags::NONE));
            // Just like other reply, the first one is a Status rc
            auto &frames = reply.second;
            auto rcMsg = std::move(frames.front());
            frames.pop_front();
            RETURN_IF_NOT_OK(ZmqMessageToStatus(rcMsg));
        } else if (payloadId_ == ZMQ_OFFLINE_PAYLOAD_INX) {
            // This is a continuation of the original rpc but sent separately.
            RETURN_IF_NOT_OK(mQue_->ClientReceiveMsg(reply, ZmqRecvFlags::NONE));
        }
        auto &frames = reply.second;
        while (!frames.empty()) {
            auto msg = std::move(frames.front());
            frames.pop_front();
            inMsg_.push_back(std::move(msg));
        }

        std::unique_ptr<ZmqPayloadEntry> entry;
        RETURN_IF_NOT_OK(ZmqPayload::ProcessEmbeddedPayload(inMsg_, entry));
        recvBuffer = std::move(entry->recvBuf);
        VLOG(RPC_LOG_LEVEL) << FormatString(
            "Client %s use unary socket to receive %zu payload bytes from Service %s"
            " Method %d",
            meta_.client_id(), entry->len, meta_.svc_name(), meta_.method_index());
        return Status::OK();
    }

    /**
     * @brief Receive a payload and write directly into user provided memory.
     * @param dest Address of user provided buffer
     * @param sz Size of the destination
     * @return Status of call
     * @note Only V2 MTP can support this form of direct write without memory copy
     */
    Status ReceivePayload(void *dest, size_t sz)
    {
        CHECK_FAIL_RETURN_STATUS(HasRecvPayloadOp(), StatusCode::K_INVALID,
                                 "rev_payload_option is not specified in the proto");
        CHECK_FAIL_RETURN_STATUS(payloadId_ >= 0, StatusCode::K_INVALID,
                                 "This form of ReceivePayload is not supported");
        CHECK_FAIL_RETURN_STATUS(payloadSz_ == sz, K_INVALID,
                                 FormatString("Unexpected payload size %zu. Expected %zu", sz, payloadSz_));
        ZmqMsgDecoder::RegisterAllocation(dest, sz);
        Raii unreg([dest]() { ZmqMsgDecoder::DeregisterAllocation(dest); });
        PayloadDirectGetReqPb rq;
        rq.set_addr(reinterpret_cast<intptr_t>(dest));
        rq.set_sz(sz);
        rq.set_id(payloadId_);
        rq.set_error_code(0);
        RETURN_IF_NOT_OK(RequestPayload(rq));
        // Now we wait for underlying framework to write the payload directly into the memory provided.
        ZmqMetaMsgFrames reply;
        RETURN_IF_NOT_OK(mQue_->ClientReceiveMsg(reply, ZmqRecvFlags::NONE));
        auto &meta = reply.first;
        GetLapTime(meta, "ZMQ_PAYLOAD_TRANSFER");
        auto elapsed = GetTotalTime(meta);
        PerfPoint::RecordElapsed(PerfKey::ZMQ_PAYLOAD_TRANSFER, elapsed);
        // Verify the response
        auto &frames = reply.second;
        ZmqMessage msg;
        RETURN_IF_NOT_OK(AckRequest(frames, msg));
        PayloadDirectGetRspPb rsp;
        RETURN_IF_NOT_OK(ParseFromZmqMessage(msg, rsp));
        CHECK_FAIL_RETURN_STATUS(
            rq.sz() == rsp.sz() && rq.addr() == rsp.addr(), K_RUNTIME_ERROR,
            FormatString("Request and reply do not match. %d %d %d %d", rq.sz(), rq.addr(), rsp.sz(), rsp.addr()));
        CHECK_FAIL_RETURN_STATUS(frames.empty(), K_RUNTIME_ERROR,
                                 FormatString("Expect empty frames but there are %d frames. First frame size %d",
                                              frames.size(), frames.front().Size()));
        VLOG(RPC_LOG_LEVEL) << FormatString(
            "Client %s use unary socket to receive %d payload bytes from Service %s"
            " Method %d.",
            meta_.client_id(), sz, meta_.svc_name(), meta_.method_index());
        const int NANO_TO_MS = 1'000'000;
        VLOG(RPC_KEY_LOG_LEVEL) << FormatString("Time to transfer payload size %d : %6lf milliseconds", sz,
                                                (float)elapsed / (float)NANO_TO_MS);
        return Status::OK();
    }

    /**
     * @brief Done sending sequence of protobuf to the server.
     * @return Status of call.
     */
    Status Finish() override
    {
        return { StatusCode::K_INVALID, "ClientUnaryWriterReader doesn't support Finish()!" };
    }

    Status SendAll(ZmqSendFlags flags) override
    {
        // Send metadata first.
        StartTheClock(meta_);
        ZmqMetaMsgFrames p(meta_, std::move(outMsg_));
        return mQue_->SendMsg(p, flags);
    }

    Status ReadAll(ZmqRecvFlags flags) override
    {
        inMsg_.clear();
        ZmqMetaMsgFrames reply;
        RETURN_IF_NOT_OK(mQue_->ClientReceiveMsg(reply, flags));
        PerfPoint::RecordElapsed(PerfKey::ZMQ_STUB_FRONT_TO_BACK, GetLapTime(reply.first, "ZMQ_STUB_FRONT_TO_BACK"));
        inMsg_ = std::move(reply.second);
        payloadId_ = reply.first.payload_index();
        v2Server_ = payloadId_ >= 0;
        return Status::OK();
    }

    void CleanupOnError(const Status &status)
    {
        auto f = [this](const Status &rc) {
            PayloadDirectGetReqPb rq;
            rq.set_addr(0);
            rq.set_sz(payloadSz_);
            rq.set_id(payloadId_);
            rq.set_error_code(rc.GetCode());
            rq.set_error_msg(rc.ToString());
            RETURN_IF_NOT_OK(RequestPayload(rq));
            return Status::OK();
        };
        (void)f(status);
    }

    bool IsV2Client() const
    {
        return v2Server_;
    }

protected:
    std::shared_ptr<ZmqMsgQueRef> mQue_;

private:
    Status WriteImpl(const W &pb)
    {
        bool expected = false;
        if (writeOnce_.compare_exchange_strong(expected, true)) {
            VLOG(RPC_LOG_LEVEL) << FormatString("Client %s use unary socket writing to Service %s Method %d",
                                                meta_.client_id(), meta_.svc_name(), meta_.method_index());
            SetMetaAuthInfo();
            RETURN_IF_NOT_OK(PushBackProtobufToFrames(pb, outMsg_));
            RETURN_OK_IF_TRUE(HasSendPayloadOp());
            return SendAll(ZmqSendFlags::NONE);
        } else {
            RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, "ClientUnaryWriterReader is only supposed to be used once!");
        }
    }

    Status SendPayloadImpl(const std::vector<MemView> &payload)
    {
        CHECK_FAIL_RETURN_STATUS(HasSendPayloadOp(), StatusCode::K_INVALID,
                                 "send_payload_option is not specified in the proto");
        size_t bufSz = 0;
        RETURN_IF_NOT_OK(ZmqPayload::AddPayloadFrames(payload, outMsg_, bufSz, false));
        VLOG(RPC_LOG_LEVEL) << FormatString(
            "Client %s use unary socket to send %zu payload bytes to Service %s Method"
            "%d",
            meta_.client_id(), bufSz, meta_.svc_name(), meta_.method_index());
        return SendAll(ZmqSendFlags::NONE);
    }

    Status AsyncSendPayloadImpl(const std::vector<MemView> &payload)
    {
        CHECK_FAIL_RETURN_STATUS(HasSendPayloadOp(), StatusCode::K_INVALID,
                                 "send_payload_option is not specified in the proto");
        if (payload.empty()) {
            return SendPayloadImpl(payload);
        }
        VLOG(RPC_LOG_LEVEL) << "Parallel send payload";
        RETURN_IF_NOT_OK(ZmqPayload::SendAsync(mQue_, meta_, FLAGS_zmq_chunk_sz, outMsg_, payload));
        return Status::OK();
    }

    Status ReadImpl(R &pb)
    {
        bool expected = false;
        if (readOnce_.compare_exchange_strong(expected, true)) {
            VLOG(RPC_LOG_LEVEL) << "Client " << meta_.client_id() << " unary socket reading" << std::endl;
            RETURN_IF_NOT_OK(ReadAll(ZmqRecvFlags::NONE));
            ZmqMessage msg;
            RETURN_IF_NOT_OK(AckRequest(inMsg_, msg));
            RETURN_IF_NOT_OK(ParseFromZmqMessage(msg, pb));
            VLOG(RPC_LOG_LEVEL) << "Client " << meta_.client_id() << " got message\n"
                                << LogHelper::IgnoreSensitive(pb) << std::endl;
            // If we receive payload, check if the payload is banked at the server or come separately.
            if (HasRecvPayloadOp() && (v2Server_ || meta_.payload_index() == ZMQ_OFFLINE_PAYLOAD_INX)) {
                CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(inMsg_.size() == 1, StatusCode::K_INVALID,
                                                     "Expect a payload size frame only");
                int64_t sz = 0;
                RETURN_IF_NOT_OK(ZmqMessageToInt64(inMsg_.front(), sz));
                payloadSz_ = static_cast<size_t>(sz);
                // We will leave the payload size frame in the queue. No need to pop it.
                VLOG(RPC_KEY_LOG_LEVEL) << FormatString("Banked payload frame detected with id %d sz %zu", payloadId_,
                                                        payloadSz_);
            }
            return Status::OK();
        } else {
            RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, "ClientUnaryWriterReader is only supposed to be used once!");
        }
    }

    Status RequestPayload(const PayloadDirectGetReqPb &rq)
    {
        // Use the same msgQ and MetaPb to send back the ack
        MetaPb meta = meta_;
        meta.set_method_index(ZMQ_PAYLOAD_GET_METHOD);
        StartTheClock(meta);
        ZmqMetaMsgFrames p;
        p.first = std::move(meta);
        RETURN_IF_NOT_OK(PushBackProtobufToFrames(rq, p.second));
        RETURN_IF_NOT_OK(mQue_->SendMsg(p));
        return Status::OK();
    }

    std::atomic<bool> writeOnce_;
    std::atomic<bool> readOnce_;
    bool v2Server_;
    int64_t payloadId_;
    size_t payloadSz_;
};
}  // namespace datasystem
#endif  // DATASYSTEM_COMMON_RPC_ZMQ_STREAM_H
