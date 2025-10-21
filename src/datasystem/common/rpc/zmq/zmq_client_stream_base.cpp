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
 * Description: Zmq client streaming api
 * Including stream RPC and non-blocking unary RPC.
 * In stream RPC, we have three combinations of reader and writer streaming mode for client and server, respectively.
 * In non-blocking unary RPC, we have ServerUnaryWriterReader.
 */

#include "datasystem/common/rpc/zmq/zmq_client_stream_base.h"

namespace datasystem {
ClientStreamBase::ClientStreamBase(std::shared_ptr<ZmqMsgQueRef> mQue, const std::string &svcName, int32_t methodIndex,
                                   std::string workerId, bool sendPayload, bool recvPayload)
    : StreamBase::StreamBase(sendPayload, recvPayload), mQue_(std::move(mQue)), seqNo_(0)
{
    meta_ = CreateMetaData(svcName, methodIndex, ZMQ_INVALID_PAYLOAD_INX, mQue_->GetId());
    meta_.set_worker_id(std::move(workerId));
}

Status ClientStreamBase::SendAll(ZmqSendFlags flags)
{
    // During initial handshake, we already got the server thread that will serve us.
    // Add that to the front of the message frames.
    StartTheClock(meta_);
    ZmqMetaMsgFrames p(meta_, std::move(outMsg_));
    return mQue_->SendMsg(p, flags);
}

Status ClientStreamBase::Finish()
{
    VLOG(RPC_LOG_LEVEL) << "Client " << meta_.client_id() << " sending Finish now.\n";
    ZmqMessage seqMsg = ZmqInt64ToMessage(ZMQ_END_SEQNO);
    outMsg_.push_back(std::move(seqMsg));
    return SendAll(ZmqSendFlags::NONE);
}

Status ClientStreamBase::SendPayloadImpl(const std::vector<MemView> &payload)
{
    CHECK_FAIL_RETURN_STATUS(HasSendPayloadOp(), StatusCode::K_INVALID,
                             "send_payload_option is not specified in the proto");
    size_t bufSz = 0;
    RETURN_IF_NOT_OK(ZmqPayload::AddPayloadFrames(payload, outMsg_, bufSz, false));
    VLOG(RPC_LOG_LEVEL) << FormatString("Client %s send %zu payload bytes to Service %s Method %d", meta_.client_id(),
                                        bufSz, meta_.svc_name(), meta_.method_index());
    return SendAll(ZmqSendFlags::NONE);
}

Status ClientStreamBase::SendPayload(const std::vector<MemView> &payload)
{
    Status status = SendPayloadImpl(payload);
    return status;
}

Status ClientStreamBase::ReceivePayload(std::vector<RpcMessage> &recvBuffer)
{
    CHECK_FAIL_RETURN_STATUS(HasRecvPayloadOp(), StatusCode::K_INVALID,
                             "rev_payload_option is not specified in the proto");
    std::unique_ptr<ZmqPayloadEntry> entry;
    RETURN_IF_NOT_OK(ZmqPayload::ProcessEmbeddedPayload(inMsg_, entry));
    recvBuffer = std::move(entry->recvBuf);
    VLOG(RPC_LOG_LEVEL) << "Client " << meta_.client_id() << " receive " << entry->len << " payload bytes from Service "
                        << meta_.svc_name() << " Method " << meta_.method_index() << std::endl;
    return Status::OK();
}

ClientStreamBase::~ClientStreamBase()
{
    mQue_->Close();
}

Status ClientStreamBase::ReadAll(ZmqRecvFlags flags)
{
    inMsg_.clear();
    ZmqMetaMsgFrames reply;
    RETURN_IF_NOT_OK(mQue_->ClientReceiveMsg(reply, flags));
    inMsg_ = std::move(reply.second);
    PerfPoint::RecordElapsed(PerfKey::ZMQ_STUB_FRONT_TO_BACK, GetLapTime(reply.first, "ZMQ_STUB_FRONT_TO_BACK"));
    return Status::OK();
}
}  // namespace datasystem
