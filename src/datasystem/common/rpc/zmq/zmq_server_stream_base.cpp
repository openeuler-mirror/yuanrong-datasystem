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
 * Description: Zmq server streaming api.
 * Including stream RPC and non-blocking unary RPC.
 * In stream RPC, we have three combinations of reader and writer streaming mode for client and server, respectively.
 * In non-blocking unary RPC, we have ServerUnaryWriterReader.
 */

#include "datasystem/common/rpc/zmq/zmq_server_stream_base.h"

namespace datasystem {
ServerStreamBase::ServerStreamBase(std::shared_ptr<ZmqServerMsgQueRef> mQueue, MetaPb meta, bool sendPayload,
                                   bool recvPayload, int64_t seqNo)
    : StreamBase::StreamBase(sendPayload, recvPayload), mQue_(std::move(mQueue)), expectedSeqNo_(seqNo)
{
    meta_ = std::move(meta);
}

ServerStreamBase::~ServerStreamBase()
{
    // The socket here is from ZmqService::WorkerCB passed down to CallMethod().
    // Once we go out of scope, we can reuse the WorkerCB.
    const std::string &workerId = meta_.worker_id();
    VLOG(RPC_LOG_LEVEL) << FormatString("~ServerStreamBase for %s", workerId);
    ZmqService *svc = nullptr;
    int32_t id;
    Status rc = ZmqService::ParseWorkerId(workerId, svc, id);
    if (rc.IsError()) {
        LOG(ERROR) << rc.ToString();
        return;
    }
    svc->UpdateStreamWorkerSeqNo(id, expectedSeqNo_);
}

Status ServerStreamBase::SendStatus(const Status &rc)
{
    // Do not send back RPC error because we allow idle stream rpc. The server virtual
    // function may time out in reading but that's okay.
    RETURN_OK_IF_TRUE(IsRpcError(rc));
    VLOG(RPC_LOG_LEVEL) << FormatString("Stream worker %s sending rc %d back to client %s", meta_.worker_id(), rc,
                                        meta_.client_id());
    // Map TRY_AGAIN to RPC_CANCELLED.
    ZmqMessage rcMsg;
    if (rc.GetCode() != K_TRY_AGAIN) {
        rcMsg = StatusToZmqMessage(rc);
    } else {
        auto msg = rc.GetMsg();
        rcMsg = StatusToZmqMessage(Status(StatusCode::K_RPC_CANCELLED, msg));
    }
    outMsg_.push_back(std::move(rcMsg));
    return SendAll(ZmqSendFlags::NONE);
}

Status ServerStreamBase::SendAll(ZmqSendFlags flags)
{
    return ZmqService::SendAll(outMsg_, meta_,
                               [this, &flags](ZmqMetaMsgFrames &e) { return mQue_->SendMsg(e, flags); });
}

Status ServerStreamBase::ReadAll(ZmqRecvFlags flags)
{
    const int64_t maxSeqNo = std::numeric_limits<int64_t>::max();
    auto readAndParse = [this, &flags](const std::string &expectedClient, int64_t &seqNo, MetaPb &meta,
                                       ZmqMsgFrames &frames) {
        ZmqMetaMsgFrames p;
        // Map K_TRY_AGAIN from ReceiveMsg to RPC error to avoid infinite loop.
        CHECK_FAIL_RETURN_STATUS(mQue_->ReceiveMsg(p, flags).IsOk(), K_RPC_CANCELLED, "Nothing sent from the client");
        frames = std::move(p.second);
        meta = std::move(p.first);
        // Drain stale messages if this server stream is reused.
        CHECK_FAIL_RETURN_STATUS(meta.client_id() == expectedClient, K_TRY_AGAIN,
                                 FormatString("Draining stale %d messages", frames.size()));
        // Next is the sequence number. -1 marks the end of stream.
        CHECK_FAIL_RETURN_STATUS(!frames.empty(), StatusCode::K_INVALID, "Invalid stream.");
        ZmqMessage seqNoMsg = std::move(frames.front());
        frames.pop_front();
        RETURN_IF_NOT_OK(ZmqMessageToInt64(seqNoMsg, seqNo));
        CHECK_FAIL_RETURN_STATUS(seqNo != ZMQ_END_SEQNO, StatusCode::K_RPC_STREAM_END, "The stream end.");
        return Status::OK();
    };
    Status rc;
    MetaPb meta;
    do {
        int64_t readSeqNo;
        // If the sort heap is not empty, check its top element if it is what we are looking for.
        // We also push the ZMQ_END_SEQNO (which is converted into maxSeqNo) into the stream unconditionally.
        if (!sort_.Empty() && (sort_.Peek() == expectedSeqNo_ || sort_.Peek() == maxSeqNo)) {
            sort_.Pop(readSeqNo, meta, inMsg_);
            CHECK_FAIL_RETURN_STATUS(readSeqNo != maxSeqNo, StatusCode::K_RPC_STREAM_END, "The stream end.");
            break;
        }
        rc = readAndParse(meta_.client_id(), readSeqNo, meta, inMsg_);
        if (rc.IsError()) {
            if (rc.GetCode() == K_TRY_AGAIN) {
                continue;
            }
            if (rc.GetCode() == K_RPC_STREAM_END) {
                // Note that ZMQ_END_SEQNO (-1) can be out of order, we shouldn't exit too early
                // until we exhaust all the elements in the sort heap. We will just add it to
                // heap sort unconditionally.
                // Convert ZMQ_END_SEQNO to the max int64 because its value is -1 and is always the
                // smallest if we peek.
                sort_.Push(maxSeqNo, meta, inMsg_);
                rc = Status(K_TRY_AGAIN, "");
                continue;
            }
            return rc;
        }
        // If the sequence number doesn't match expected, park it in the sort heap
        if (readSeqNo == expectedSeqNo_) {
            break;
        } else {
            sort_.Push(readSeqNo, meta, inMsg_);
            rc = Status(K_TRY_AGAIN, FormatString("Expect %d but get %d", expectedSeqNo_, readSeqNo));
        }
    } while (rc.GetCode() == K_TRY_AGAIN);
    meta_ = meta;
    // While we allow the incoming data to be in any random order, we have to force
    // our going data to be in order. The simplest way is to use only one route to return
    meta_.set_event_type(EventType::ZMQ);
    ++expectedSeqNo_;
    return Status::OK();
}

Status ServerStreamBase::Finish()
{
    return SendStatus(Status(StatusCode::K_RPC_STREAM_END, "Finished the rpc stream"));
}

Status ServerStreamBase::SendPayload(std::vector<RpcMessage> &buffer)
{
    CHECK_FAIL_RETURN_STATUS(HasRecvPayloadOp(), StatusCode::K_INVALID,
                             "recv_payload_option is not specified in the proto");
    size_t bufSz = 0;
    RETURN_IF_NOT_OK(ZmqPayload::AddPayloadFrames(buffer, outMsg_, bufSz, false));
    VLOG(RPC_LOG_LEVEL) << FormatString("Stream worker %s send %zu pay load bytes to client %s", meta_.worker_id(),
                                        bufSz, meta_.client_id());
    return SendAll(ZmqSendFlags::NONE);
}

Status ServerStreamBase::SendPayload(const std::vector<MemView> &payload)
{
    CHECK_FAIL_RETURN_STATUS(HasRecvPayloadOp(), StatusCode::K_INVALID,
                             "recv_payload_option is not specified in the proto");
    size_t bufSz = 0;
    RETURN_IF_NOT_OK(ZmqPayload::AddPayloadFrames(payload, outMsg_, bufSz, false));
    VLOG(RPC_LOG_LEVEL) << FormatString("Stream worker %s send %zu pay load bytes to client %s", meta_.worker_id(),
                                        bufSz, meta_.client_id());
    return SendAll(ZmqSendFlags::NONE);
}

Status ServerStreamBase::ReceivePayload(std::vector<RpcMessage> &payload)
{
    CHECK_FAIL_RETURN_STATUS(HasSendPayloadOp(), StatusCode::K_INVALID,
                             "send_payload_option is not specified in the proto");
    std::unique_ptr<ZmqPayloadEntry> entry;
    RETURN_IF_NOT_OK(ZmqPayload::ProcessEmbeddedPayload(inMsg_, entry));
    payload = std::move(entry->recvBuf);
    VLOG(RPC_LOG_LEVEL) << FormatString("Stream worker %s received %d payload bytes from client %s", meta_.worker_id(),
                                        entry->len, meta_.client_id());
    return Status::OK();
}

void HeapSort::Push(int64_t seqNo, MetaPb &meta, ZmqMsgFrames &frames)
{
    VLOG(RPC_LOG_LEVEL) << FormatString("HeapSort push %d Client %s", seqNo, meta.client_id());
    auto ele = HeapElement(seqNo, meta, frames);
    que_->push(std::move(ele));
}

void HeapSort::Pop(int64_t &seqNo, MetaPb &meta, ZmqMsgFrames &frames)
{
    auto &ele = const_cast<HeapElement &>(que_->top());
    frames = std::move(ele.frames_);
    meta = std::move(ele.meta_);
    seqNo = ele.seqNo_;
    que_->pop();
    VLOG(RPC_LOG_LEVEL) << FormatString("HeapSort pop %d Client %s ", seqNo, meta.client_id());
}
}  // namespace datasystem
