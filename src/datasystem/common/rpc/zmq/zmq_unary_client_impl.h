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
#include "datasystem/common/rpc/zmq/zmq_constants.h"
#include "datasystem/common/rpc/zmq/zmq_stream_base.h"
#include "datasystem/common/rpc/zmq/zmq_stub.h"
#include "datasystem/common/log/log_helper.h"
#include "datasystem/common/util/gflag/common_gflags.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/rpc/zmq/exclusive_conn_mgr.h"

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

    // Alternate constructor for exclusive connection mode
    ClientUnaryWriterReaderImpl(int32_t exclusiveId, const std::string &svcName, int32_t methodIndex, bool sendPayload,
                                bool recvPayload)
        : StreamBase::StreamBase(sendPayload, recvPayload),
          mQue_(nullptr),
          writeOnce_(false),
          readOnce_(false),
          v2Server_(false),
          payloadId_(-1),
          payloadSz_(0),
          exclusiveId_(exclusiveId)
    {
        // In non-exclusive connection mode, the mQue_->GetId() is used for the clientId metadata. This is not the
        // actual client id, but the ZmqMsgQueue id. This field is not used in exclusive connection mode, but we can
        // populate it with a name for diagnostic purposes.
        std::string clientId = gExclusiveConnMgr.GetExclusiveConnMgrName();
        meta_ = CreateMetaData(svcName, methodIndex, sendPayload ? ZMQ_EMBEDDED_PAYLOAD_INX : ZMQ_INVALID_PAYLOAD_INX,
                               clientId);
    }

    Status InitExclusiveConnection(const std::string &exclusiveSockPath, int64_t timeoutMs)
    {
        RETURN_IF_NOT_OK(
            gExclusiveConnMgr.CreateExclusiveConnection(exclusiveId_.value(), timeoutMs, exclusiveSockPath));
        return Status::OK();
    }

    ~ClientUnaryWriterReaderImpl() override
    {
        if (mQue_) {
            mQue_->Close();
        }
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
            RETURN_IF_NOT_OK(RecvConnReply(reply, ZmqRecvFlags::NONE, true));
            // Just like other reply, the first one is a Status rc
            auto &frames = reply.second;
            auto rcMsg = std::move(frames.front());
            frames.pop_front();
            RETURN_IF_NOT_OK(ZmqMessageToStatus(rcMsg));
        } else if (payloadId_ == ZMQ_OFFLINE_PAYLOAD_INX) {
            RETURN_IF_NOT_OK(RecvConnReply(reply, ZmqRecvFlags::NONE, true));
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
        RETURN_IF_NOT_OK(RecvConnReply(reply, ZmqRecvFlags::NONE, true));
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
        // Align with AsyncWriteImpl: enqueue then send; outbound adds TICK_CLIENT_TO_STUB on the queued copy.
        StartTheClock(meta_);
        RecordTick(meta_, TICK_CLIENT_ENQUEUE);
        ZmqMsgFrames frames = std::move(outMsg_);
        RETURN_IF_NOT_OK(SendConnMsg(meta_, frames, flags));
        return Status::OK();
    }

    Status ReadAll(ZmqRecvFlags flags) override
    {
        inMsg_.clear();
        ZmqMetaMsgFrames reply;
        RETURN_IF_NOT_OK(RecvConnReply(reply, flags, false));
        // Response MetaPb carries SERVER_* ticks + TICK_SERVER_RPC_WINDOW_NS; unary used to discard it.
        lastConnReplyMeta_ = std::move(reply.first);
        inMsg_ = std::move(reply.second);
        payloadId_ = lastConnReplyMeta_.payload_index();
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

    bool IsExclusiveConnection()
    {
        return exclusiveId_.has_value();
    }

protected:
    std::shared_ptr<ZmqMsgQueRef> mQue_;

    /** Response MetaPb from last ReadAll(); holds ticks for unary queue-flow histograms */
    MetaPb lastConnReplyMeta_;

private:
    Status WriteImpl(const W &pb)
    {
        bool expected = false;
        if (writeOnce_.compare_exchange_strong(expected, true)) {
            VLOG(RPC_LOG_LEVEL) << FormatString("Client %s use unary socket writing to Service %s Method %d",
                                                meta_.client_id(), meta_.svc_name(), meta_.method_index());
            SetMetaAuthInfo();
            PerfPoint point(PerfKey::ZMQ_REQUEST_PROTO_TO_MSG);
            RETURN_IF_NOT_OK(PushBackProtobufToFrames(pb, outMsg_));
            point.Record();
            PerfPoint::RecordElapsed(PerfKey::ZMQ_REQUEST_SIZE_AFTER_SERIALIZE, outMsg_.back().Size());
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
            // Response MetaPb may omit request-path ticks copied into Outbound earlier; mirror Async merge.
            for (int i = 0; i < meta_.ticks_size(); i++) {
                const auto &t = meta_.ticks(i);
                if (!MetaHasNamedTick(lastConnReplyMeta_, t.tick_name().c_str())) {
                    *lastConnReplyMeta_.mutable_ticks()->Add() = t;
                }
            }
            // Align with ZmqStubImpl::AsyncReadImpl: recv tick + queue-flow histograms before AckRequest().
            RecordTick(lastConnReplyMeta_, TICK_CLIENT_RECV);
            RecordRpcLatencyMetrics(lastConnReplyMeta_);
            ZmqMessage msg;
            RETURN_IF_NOT_OK(AckRequest(inMsg_, msg));
            PerfPoint point(PerfKey::ZMQ_RESPONSE_MSG_TO_PROTO);
            RETURN_IF_NOT_OK(ParseFromZmqMessage(msg, pb));
            point.Record();
            PerfPoint::RecordElapsed(PerfKey::ZMQ_RESPONSE_SIZE_BEFORE_DESERIALIZE, msg.Size());
            VLOG(RPC_LOG_LEVEL) << "Client " << meta_.client_id() << " got message\n"
                                << LogHelper::IgnoreSensitive(pb) << std::endl;
            // If we receive payload, check if the payload is banked at the server or come separately.
            if (HasRecvPayloadOp() &&
                (v2Server_ || lastConnReplyMeta_.payload_index() == ZMQ_OFFLINE_PAYLOAD_INX)) {
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

        ZmqMsgFrames frames;
        RETURN_IF_NOT_OK(PushBackProtobufToFrames(rq, frames));
        RETURN_IF_NOT_OK(SendConnMsg(meta, frames, ZmqSendFlags::NONE));
        return Status::OK();
    }

    Status SendConnMsg(MetaPb &meta, ZmqMsgFrames &frames, ZmqSendFlags flags)
    {
        if (!IsExclusiveConnection()) {
            ZmqMetaMsgFrames p(meta, std::move(frames));
            RETURN_IF_NOT_OK(mQue_->SendMsg(p, flags));
        } else {
            ZmqMsgEncoder *encoder;
            RETURN_IF_NOT_OK(gExclusiveConnMgr.GetExclusiveConnEncoder(exclusiveId_.value(), encoder));

            // This is the last perf point before sending data to worker. In non-exclusive mode, this would be
            // ZMQ_STUB_TO_BACK_TO_FRONT that gets recorded in the frontend sending codepath.
            // Here in exclusive conn mode, it directly sends right here because there is no frontend.
            // Thus, record a perf point here so that the next point, the server-side ZMQ_NETWORK_TRANSFER, will more
            // accurately show the transfer time.
            TraceGuard traceGuard = SetTraceContextFromMeta(meta);
            PerfPoint::RecordElapsed(PerfKey::ZMQ_STUB_TO_EXCL_CONN, GetLapTime(meta, "ZMQ_STUB_TO_EXCL_CONN"));

            // Add the meta to the frames
            RETURN_IF_NOT_OK(PushFrontProtobufToFrames(meta, frames));

            // The gatewayId field is part of the protocol for non-exclusive connections.
            // In exclusive connection mode, this field is not functional, however it does provide a debugging handle
            // for aligning the client request with server response when viewing logs, so it will continue to be added
            // to the send frames.
            std::string gatewayId = gExclusiveConnMgr.GetExclusiveConnMgrName();
            RETURN_IF_NOT_OK(PushFrontStringToFrames(gatewayId, frames));
            Status rc = encoder->SendMsgFrames(EventType::V1MTP, frames);
            if (rc.IsError()) {
                // If any error happens from sending the data, we shall close the exclusive connection so that future
                // rpcs are forced to create a new connection. This avoids subsequent connection re-use againt a
                // connection that is in error state.
                // In the future, we may consider more intelligent error handling, since all errors may not require
                // full cleanup.
                LOG_IF_ERROR(gExclusiveConnMgr.CloseExclusiveConn(exclusiveId_.value()),
                             "Error closing exclusive conn during error path");
                return rc;
            }
        }
        // Stub Outbound stamps CLIENT_TO_STUB on the queued MetaPb copy; unary keeps a separate meta_ object.
        // ReadImpl merges meta_ into reply meta for RecordRpcLatencyMetrics; without this, clientToStubTs is 0
        // (no queuing / wrong network residual). Avoid double-stamp if caller already set the tick.
        if (!MetaHasNamedTick(meta, TICK_CLIENT_TO_STUB)) {
            RecordTick(meta, TICK_CLIENT_TO_STUB);
        }
        return Status::OK();
    }

    Status RecvConnReply(ZmqMetaMsgFrames &reply, ZmqRecvFlags flags, bool isPayload)
    {
        if (!IsExclusiveConnection()) {
            RETURN_IF_NOT_OK(mQue_->ClientReceiveMsg(reply, flags));
            // Regular receive counts the perf point using ZMQ_STUB_FRONT_TO_BACK. Special case for the payload version
            // of a receive
            if (isPayload) {
                PerfPoint::RecordElapsed(PerfKey::ZMQ_PAYLOAD_TRANSFER,
                                         GetLapTime(reply.first, "ZMQ_PAYLOAD_TRANSFER"));
            } else {
                PerfPoint::RecordElapsed(PerfKey::ZMQ_STUB_FRONT_TO_BACK,
                                         GetLapTime(reply.first, "ZMQ_STUB_FRONT_TO_BACK"));
            }
        } else {
            ZmqMsgDecoder *decoder;
            RETURN_IF_NOT_OK(gExclusiveConnMgr.GetExclusiveConnDecoder(exclusiveId_.value(), decoder));

            ZmqMsgFrames replyFrames;
            // v2 decode supports v1 format. Use the v2 version of the decode.
            Status rc = decoder->ReceiveMsgFramesV2(replyFrames);
            if (rc.IsError()) {
                // If any error happens from receiving the data, we shall close the exclusive connection so that future
                // rpcs are forced to create a new connection. This avoids subsequent connection re-use againt a
                // connection that is in error state.
                // In the future, we may consider more intelligent error handling, since all errors may not require
                // full cleanup.
                LOG_IF_ERROR(gExclusiveConnMgr.CloseExclusiveConn(exclusiveId_.value()),
                             "Error closing exclusive conn during error path");
                return rc;
            }

            const size_t msgFrameMinSize = 2;
            CHECK_FAIL_RETURN_STATUS(replyFrames.size() >= msgFrameMinSize, StatusCode::K_INVALID,
                                     "Invalid msg: frames.size() = " + std::to_string(replyFrames.size()));
            std::string receiver = ZmqMessageToString(replyFrames.front());
            replyFrames.pop_front();

            ZmqMessage metaHdr = std::move(replyFrames.front());
            replyFrames.pop_front();
            MetaPb meta;
            RETURN_IF_NOT_OK(ParseFromZmqMessage(metaHdr, meta));
            // In exclusive mode, there is no front/backend. There is only a receive. Do not record any stub perf
            // points, and just record the network transfer here.
            // Note that in non-exclusive mode, the ZMQ_NETWORK_TRANSFER_STUB_UDS was recorded in the frontend codepath,
            // but that code did not run here so this is the appropriate time to capture this stat.
            TraceGuard traceGuard = SetTraceContextFromMeta(meta, true);
            PerfPoint::RecordElapsed(PerfKey::ZMQ_NETWORK_TRANSFER_STUB_UDS,
                                     GetLapTime(meta, "ZMQ_NETWORK_TRANSFER (SOCKET)"));
            reply = std::make_pair(meta, std::move(replyFrames));
        }
        return Status::OK();
    }

    std::atomic<bool> writeOnce_;
    std::atomic<bool> readOnce_;
    bool v2Server_;
    int64_t payloadId_;
    size_t payloadSz_;
    std::optional<int32_t> exclusiveId_;
};
}  // namespace datasystem
#endif  // DATASYSTEM_COMMON_RPC_ZMQ_STREAM_H
