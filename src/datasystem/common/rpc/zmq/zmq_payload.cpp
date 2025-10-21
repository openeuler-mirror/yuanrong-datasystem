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
 * Description: Zmq payload.
 */
#include "datasystem/common/rpc/zmq/zmq_payload.h"

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <numeric>

#include "datasystem/common/flags/flags.h"
#include "datasystem/common/rpc/zmq/rpc_service_method.h"
#include "datasystem/common/rpc/zmq/zmq_common.h"
#include "datasystem/common/rpc/zmq/zmq_msg_decoder.h"

namespace datasystem {
Status ZmqPayload::AddPayloadFrames(std::vector<RpcMessage> &buffer, ZmqMsgFrames &frames, size_t &bufSz,
                                    bool tagPayloadFrame)
{
    bufSz = 0;
    for (auto &v : buffer) {
        if (SIZE_MAX - bufSz < v.Size()) {
            RETURN_STATUS_LOG_ERROR(K_RUNTIME_ERROR,
                                    FormatString("Computation overflow: bufSz=%zu , v.size()=%zu.", bufSz, v.Size()));
        }
        bufSz += v.Size();
    }
    frames.push_back(ZmqInt64ToMessage(bufSz));

    if (tagPayloadFrame) {
        frames.back().SetType(ZmqMessage::ZmqMsgType::PAYLOAD_SZ);
    }
    size_t length = frames.size();
    frames.resize(length + buffer.size());
    for (size_t i = 0; i < buffer.size(); i++) {
        buffer[i].MoveToMsg(frames[length + i]);
    }
    return Status::OK();
}

Status ZmqPayload::AddPayloadFrames(const std::vector<MemView> &payload, ZmqMsgFrames &frames, size_t &bufSz,
                                    bool tagPayloadFrame)
{
    const int32_t maxInt = std::numeric_limits<int32_t>::max();
    std::vector<RpcMessage> zpl;
    // Create special message_t so user buffer won't be deallocated by accident.
    for (auto buf : payload) {
        size_t remaining = buf.Size();
        while (remaining > 0) {
            // Break up the buffer into smaller chunks if it exceeds 2g-1. Low level api
            // returns a 32 bits integer as return code. -1 means error. On success, it is the
            // number of bytes it is sent or received. The confusion is what if we send more
            // than 2g of data, the return code will become negative. To avoid this
            // confusion, we send the buffer in pieces. The catch is the receiving side will
            // not receive one single contiguous buffer.
            int32_t bufSize = (remaining < static_cast<size_t>(maxInt)) ? static_cast<int32_t>(remaining) : maxInt;
            zpl.emplace_back();
            auto &msg = zpl.back();
            RETURN_IF_NOT_OK(msg.ZeroCopyBuffer(const_cast<void *>(buf.Data()), bufSize));
            remaining -= bufSize;
            buf += bufSize;
        }
    }
    return AddPayloadFrames(zpl, frames, bufSz, tagPayloadFrame);
}

Status ZmqPayload::ProcessEmbeddedPayload(ZmqMsgFrames &frames, std::unique_ptr<ZmqPayloadEntry> &out)
{
    CHECK_FAIL_RETURN_STATUS(!frames.empty(), StatusCode::K_INVALID, "Expect a 64 bit integer");
    ZmqMessage lengthMsg = std::move(frames.front());
    frames.pop_front();
    int64_t sz = 0;
    RETURN_IF_NOT_OK(ZmqMessageToInt64(lengthMsg, sz));
    auto entry = std::make_unique<ZmqPayloadEntry>();
    entry->len = sz;
    int64_t remainingSz = sz;
    CHECK_FAIL_RETURN_STATUS(remainingSz >= 0, K_RUNTIME_ERROR, "Negative payload size");
    // Client can send the buffer in multipart buffers. So keep on receiving until we got them all.
    while (remainingSz > 0) {
        CHECK_FAIL_RETURN_STATUS(!frames.empty(), StatusCode::K_INVALID, "Not enough payload frames");
        ZmqMessage msg = std::move(frames.front());
        frames.pop_front();
        size_t n = msg.Size();
        remainingSz -= n;
        entry->recvBuf.emplace_back(std::move(msg));
    }
    CHECK_FAIL_RETURN_STATUS(remainingSz == 0, StatusCode::K_RUNTIME_ERROR,
                             FormatString("remaining Sz (%lld) != 0", remainingSz));
    out = std::move(entry);
    return Status::OK();
}

Status ZmqPayload::CreateAsyncPayloadEntry(const HandshakePb &rq, const MetaPb &meta,
                                           std::unique_ptr<AsyncPayloadEntry> &out)
{
    uint64_t totalSize = std::accumulate(rq.data_sizes().begin(), rq.data_sizes().end(), 0ul);
    CHECK_FAIL_RETURN_STATUS(
        totalSize == rq.payload_sz(), K_RUNTIME_ERROR,
        FormatString("The payload size %zu not match with total data size %zu", rq.payload_sz(), totalSize));
    auto entry = std::make_unique<AsyncPayloadEntry>();
    entry->len = rq.payload_sz();
    try {
        entry->buf = std::make_unique<char[]>(entry->len);
    } catch (const std::bad_alloc &e) {
        RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, e.what());
    }
    entry->originalSizes = { rq.data_sizes().begin(), rq.data_sizes().end() };
    entry->numPending = rq.total_packets();
    entry->meta = meta;
    auto *buf = entry->buf.get();
    for (auto size : entry->originalSizes) {
        entry->recvBuf.emplace_back();
        auto &payload = entry->recvBuf.back();
        // Piggyback on the ShmGuard implementation
        auto hint = std::make_unique<std::shared_ptr<char[]>>(entry->buf);
        RETURN_IF_NOT_OK(payload.TransferOwnership(
            buf, size,
            [](void *data, void *hint) {
                (void)data;
                auto impl = reinterpret_cast<std::shared_ptr<char[]> *>(hint);
                delete impl;
            },
            hint.release()));
        buf += size;
    }
    out = std::move(entry);
    return Status::OK();
}

size_t GetPayloadSz(const std::vector<MemView> &payload, std::vector<uint32_t> &originalSizes)
{
    size_t bufSz = 0;
    for (const auto &v : payload) {
        bufSz += v.Size();
        originalSizes.push_back(v.Size());
    }
    return bufSz;
}

Status SendPacket(const std::shared_ptr<ZmqMsgQueRef> &mQue, const std::string &svcName, ZmqPayloadIterator &iter,
                  int64_t seqNo, uint32_t chunkSz, const HandshakeTokenPb &token)
{
    CHECK_FAIL_RETURN_STATUS(!iter.IsEnd(), K_RUNTIME_ERROR,
                             "Iterator reached the end but we still have buffer to send.");
    // We may need to send in multiple parts to fill up to one chunk size.
    // Let's keep track of these pieces.
    VLOG(RPC_LOG_LEVEL) << FormatString("Send packet with seqno %d", seqNo);
    std::deque<MemView> bufList;
    uint32_t curSize = 0;
    uint32_t remainingSz = chunkSz;
    while (remainingSz > 0 && !iter.IsEnd()) {
        auto buf = (*iter);
        if (buf.Size() >= remainingSz) {
            bufList.emplace_back(buf.Data(), remainingSz);
            iter += remainingSz;
            remainingSz = 0;
            curSize = chunkSz;
        } else {
            bufList.emplace_back(buf.Data(), buf.Size());
            curSize += buf.Size();
            remainingSz -= buf.Size();
            iter += buf.Size();
        }
    }
    PayloadDirectGetRspPb rq;
    uint8_t *dest = reinterpret_cast<uint8_t *>(token.token()) + static_cast<uint64_t>(seqNo) * chunkSz;
    rq.set_addr(reinterpret_cast<int64_t>(dest));
    rq.set_sz(curSize);

    MetaPb hdr = CreateMetaData(svcName, ZMQ_PAYLOAD_PUT_METHOD, token.payload_index(), mQue->GetId());
    ZmqMsgFrames frames;
    RETURN_IF_NOT_OK(PushBackProtobufToFrames(rq, frames));
    // Need to tag this special message for V2MTP
    frames.front().SetType(ZmqMessage::ZmqMsgType::DECODER);
    size_t bufSz = 0;
    while (!bufList.empty()) {
        auto &ele = bufList.front();
        frames.emplace_back();
        auto &msg = frames.back();
        RETURN_IF_NOT_OK(msg.ZeroCopyBuffer(const_cast<void *>(ele.Data()), ele.Size()));
        bufSz += ele.Size();
        bufList.pop_front();
    }
    VLOG(RPC_LOG_LEVEL) << "Sending packet of bufSz " << bufSz;
    ZmqMetaMsgFrames p(hdr, std::move(frames));
    RETURN_IF_NOT_OK(mQue->SendMsg(p));
    return Status::OK();
}

Status SendPayload(const std::shared_ptr<ZmqMsgQueRef> &mQue, const std::string &svcName, uint32_t chunkSz,
                   const HandshakeTokenPb &token, int64_t totalPacket, const std::vector<MemView> &payload)
{
    // At this point the server has allocated the buffer to hold the payload.
    // The following code is copied from zmq_perf_client
    // The server side is already receiving these packets in parallel.
    // Potential optimization is to start a thread pool to send all the packets in parallel but
    // keep in mind, each thread must have its own zmq socket (and connect) because zmq socket
    // is not thread safe.
    ZmqPayloadIterator iter(payload);
    for (int64_t seqNo = 0; seqNo < totalPacket; ++seqNo) {
        // There is no ack coming back rather than we are receiving totalPackets acks.
        RETURN_IF_NOT_OK(SendPacket(mQue, svcName, iter, seqNo, chunkSz, token));
    }
    // Server will not send back any ack. So we just quit.
    return Status::OK();
}

Status ZmqPayload::SendAsync(const std::shared_ptr<ZmqMsgQueRef> &mQue, const MetaPb &meta, uint32_t chunkSz,
                             ZmqMsgFrames &frames, const std::vector<MemView> &payload)
{
    /**
     * Send the meta hdr and the handshake message. Let the server to prepare how much
     * memory to set aside.
     */
    std::vector<uint32_t> originalSizes;
    size_t bufSz = GetPayloadSz(payload, originalSizes);
    auto totalPacket = (bufSz != 0) ? ((bufSz - 1) / chunkSz + 1) : 0;
    HandshakePb handshake;
    handshake.set_payload_sz(bufSz);
    handshake.set_total_packets(totalPacket);
    *handshake.mutable_data_sizes() = { originalSizes.begin(), originalSizes.end() };
    VLOG(RPC_LOG_LEVEL) << "Sending " << bufSz << " bytes in " << totalPacket << " packets";
    MetaPb hdr = CreateMetaData(meta.svc_name(), ZMQ_PAYLOAD_HANDSHAKE_METHOD, ZMQ_INVALID_PAYLOAD_INX, mQue->GetId());
    // Put handshake before actual request frames (actual meta + actual req pb)
    RETURN_IF_NOT_OK(PushFrontProtobufToFrames(meta, frames));
    RETURN_IF_NOT_OK(PushFrontProtobufToFrames(handshake, frames));
    ZmqMetaMsgFrames p(hdr, std::move(frames));
    RETURN_IF_NOT_OK(mQue->SendMsg(p));

    // Wait for ack and get the token back.
    ZmqMetaMsgFrames rsp;
    RETURN_IF_NOT_OK(mQue->ReceiveMsg(rsp));
    ZmqMessage reply;
    RETURN_IF_NOT_OK(AckRequest(rsp.second, reply));
    HandshakeTokenPb token;
    RETURN_IF_NOT_OK(ParseFromZmqMessage(reply, token));
    /**
     * Now we split the cursor buffer into smaller packets and each one can be
     * sent in parallel (if we use a thread pool). Regardless, the packets will
     * arrive at the server in parallel and not necessary in the same order. But
     * this will be handled by the ZMQ server
     */
    RETURN_IF_NOT_OK(SendPayload(mQue, meta.svc_name(), chunkSz, token, totalPacket, payload));
    return Status::OK();
}

BankColumns ZmqPayloadBank::GetAndErase(int64_t id)
{
    BankColumns ele;
    WriteLock xlock(&mux_);
    auto it = bank_.find(id);
    if (it != bank_.end()) {
        ele = std::move(it->second);
        auto &entry = std::get<ZmqPayloadBank::COL::K_ASYNC>(ele);
        if (entry) {
            ZmqMsgDecoder::DeregisterAllocation(reinterpret_cast<void *>(entry->buf.get()));
        }
        bank_.erase(it);
    }
    return ele;
}

int64_t ZmqPayloadBank::SavePayload(int fd, ZmqMsgFrames &&p, std::unique_ptr<AsyncPayloadEntry> &&async,
                                    std::unique_ptr<TimerQueue::TimerImpl> &&timer)
{
    int64_t id = GetId();
    WriteLock xlock(&mux_);
    bank_.emplace(id, std::make_tuple(fd, std::move(p), std::move(async), std::move(timer)));
    return id;
}

int64_t ZmqPayloadBank::GetId()
{
    return id_.fetch_add(1);
}

ZmqPayloadBank::ZmqPayloadBank() : id_(0)
{
}

Status ZmqPayloadBank::CreateAsyncPayloadEntry(const HandshakePb &rq, const MetaPb &meta, ZmqMsgFrames &inMsg,
                                               HandshakeTokenPb &reply)
{
    std::unique_ptr<AsyncPayloadEntry> entry;
    RETURN_IF_NOT_OK(ZmqPayload::CreateAsyncPayloadEntry(rq, meta, entry));
    ZmqMsgDecoder::RegisterAllocation(reinterpret_cast<void *>(entry->buf.get()), entry->len);
    auto idx = GetId();
    reply.set_token(reinterpret_cast<int64_t>(entry->buf.get()));
    reply.set_payload_index(idx);
    TimerQueue::TimerImpl timerImpl;
    RETURN_IF_NOT_OK(TimerQueue::GetInstance()->AddTimer(
        RPC_TIMEOUT, [this, idx] { GetAndErase(idx); }, timerImpl));
    WriteLock xlock(&mux_);
    auto it = bank_.emplace(idx, std::make_tuple(-1, std::move(inMsg), std::move(entry),
                                                 std::make_unique<TimerQueue::TimerImpl>(timerImpl)));
    CHECK_FAIL_RETURN_STATUS(it.second, K_RUNTIME_ERROR, "Unable to insert into map");
    return Status::OK();
}
}  // namespace datasystem
