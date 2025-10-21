/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2023. All rights reserved.
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
 * Description: Zmq Message Transport Protocol.
 */

#include <cstdint>
#include <functional>
#include <limits>
#include <map>

#include "datasystem/common/rpc/zmq/zmq_msg_decoder.h"
#include "datasystem/common/rpc/zmq/zmq_stub_conn.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"

namespace datasystem {
std::mutex ZmqMsgDecoder::allocMux_;
std::map<uint64_t, uint64_t, std::greater<uint64_t>> ZmqMsgDecoder::regAlloc_{};

size_t ZmqMsgDecoder::NumUnRead() const
{
    return static_cast<size_t>(bytesReceived_) - pos_;
}

bool ZmqMsgDecoder::Empty() const
{
    return NumUnRead() == 0;
}

Status ZmqMsgDecoder::Recv()
{
    auto *buf = wa_.get();
    // wa_[0 .. pos) can be discarded.
    // move wa_[pos .. bytesReceived) to the front
    if (pos_ > 0) {
        auto moveAmount = bytesReceived_ - pos_;
        if (moveAmount > 0) {
            VLOG(RPC_LOG_LEVEL) << FormatString("Decoder: move %zd bytes from %zu to the beginning", moveAmount, pos_);
            auto err = memmove_s(buf, K_WA_SIZE, buf + pos_, moveAmount);
            CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(err == 0, K_RUNTIME_ERROR, FormatString("Memmove failed %d", errno));
        }
        bytesReceived_ = static_cast<ssize_t>(moveAmount);
        pos_ = 0;
    }
    // Read as much as possible starting from position bytesReceived_.
    // It is assumed the underlying file descriptor is non-blocking
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(K_WA_SIZE >= bytesReceived_, K_RUNTIME_ERROR,
                                         FormatString("Invalid bytesReceived_ %zu", bytesReceived_));
    ssize_t bytesReceived = recv(fd_, buf + bytesReceived_, K_WA_SIZE - bytesReceived_, 0);
    if (bytesReceived == -1) {
        auto rc = UnixSockFd::ErrnoToStatus(errno, fd_);
        return rc;
    }
    if (bytesReceived == 0) {
        RETURN_STATUS(StatusCode::K_RPC_CANCELLED, "bytesReceived is 0");
    }
    bytesReceived_ += bytesReceived;
    VLOG(RPC_LOG_LEVEL) << FormatString("Bytes received %zd. Head %zu. Tail %zd", bytesReceived, pos_, bytesReceived_);
    return Status::OK();
}

Status ZmqMsgDecoder::TransferFromWA(void *dest, size_t sz, size_t &bytesReceived)
{
    bytesReceived = std::min<size_t>(NumUnRead(), sz);
    if (bytesReceived > 0) {
        VLOG(RPC_LOG_LEVEL) << FormatString("Copy %zu bytes from workarea pos %zu to msg buffer", bytesReceived, pos_);
        auto *buf = wa_.get() + pos_;
        auto err = memcpy_s(dest, sz, buf, bytesReceived);
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(err == 0, K_RUNTIME_ERROR,
                                             FormatString("Memcpy %zu bytes failed. Errno %d", bytesReceived, errno));
        if (SIZE_MAX - bytesReceived < pos_) {
            RETURN_STATUS_LOG_ERROR(K_RUNTIME_ERROR, FormatString("Computation overflow: bytesReceived=%zu , pos_=%zu.",
                                                                  bytesReceived, pos_));
        }
        pos_ += bytesReceived;
    }
    return Status::OK();
}

Status ZmqMsgDecoder::DecodeHdrLen(MsgState &state)
{
    CHECK_FAIL_RETURN_STATUS(state == MsgState::HDR_LEN_READY, K_RUNTIME_ERROR, "Wrong state");
    if (Empty()) {
        UnixSockFd sock(fd_);
        RETURN_IF_NOT_OK(sock.RecvProtobuf(hdr_));
        // Move the state to detect if it is V1 or V2
        state = MsgState::MTP_DETECT;
        return Status::OK();
    }
    // First 4 bytes is the length field.
    uint32_t sz;
    if (NumUnRead() < sizeof(sz)) {
        RETURN_IF_NOT_OK(Recv());
        CHECK_FAIL_RETURN_STATUS(NumUnRead() >= sizeof(sz), K_TRY_AGAIN, "Waiting for more network data to arrive");
    }
    {
        uint8_t *arr = wa_.get() + pos_;
        google::protobuf::io::ArrayInputStream osWrapper(arr, sizeof(sz), sizeof(sz));
        google::protobuf::io::CodedInputStream input(&osWrapper);
        CHECK_FAIL_RETURN_STATUS(input.ReadLittleEndian32(&sz), K_RUNTIME_ERROR, "Google read error");
    }
    // Move to the next state
    state = MsgState::HDR_BODY_READY;
    rpcHdrSz_ = sz;
    pos_ += sizeof(sz);
    return Status::OK();
}

Status ZmqMsgDecoder::DecodeHdrBody(MsgState &state)
{
    CHECK_FAIL_RETURN_STATUS(state == MsgState::HDR_BODY_READY, K_RUNTIME_ERROR, "Wrong state");
    if (NumUnRead() < rpcHdrSz_) {
        RETURN_IF_NOT_OK(Recv());
        CHECK_FAIL_RETURN_STATUS(NumUnRead() >= rpcHdrSz_, K_TRY_AGAIN, "Waiting for more network data to arrive");
    }
    hdr_.Clear();
    uint8_t *arr = wa_.get() + pos_;
    bool success = hdr_.ParseFromArray(arr, rpcHdrSz_);
    if (success) {
        state = MsgState::MTP_DETECT;
        pos_ += rpcHdrSz_;
        return Status::OK();
    }
    RETURN_STATUS(StatusCode::K_INVALID, "Failed to parse MultiMsgHdrPb");
}

Status ZmqMsgDecoder::DetectMTP(MsgState &state)
{
    CHECK_FAIL_RETURN_STATUS(state == MsgState::MTP_DETECT, K_RUNTIME_ERROR, "Wrong state");
    // V2 sends an empty header
    newFormat_ = hdr_.msg_size_size() == 0;
    if (newFormat_) {
        VLOG(RPC_LOG_LEVEL) << FormatString("V2 format detected for fd %d", fd_);
        state = MsgState::FLAGS_READY;
    } else {
        // V1 format
        VLOG(RPC_LOG_LEVEL) << FormatString("V1 format detected for fd %d", fd_);
        state = MsgState::DOWNLEVEL_CLIENT;
    }
    return Status::OK();
}

Status ZmqMsgDecoder::V1Client(MsgState &state)
{
    CHECK_FAIL_RETURN_STATUS(state == MsgState::DOWNLEVEL_CLIENT, K_RUNTIME_ERROR, "Wrong state");
    // This is similar to ReceiveMsgFramesV1 except we already have the hdr_ and
    // possibly some bytes in the work area.
    v1Frames_.clear();
    const int numMsg = hdr_.msg_size_size();
    VLOG(RPC_LOG_LEVEL) << FormatString("Prepare to receive %d frames from fd %d using V1 format", numMsg, fd_);
    for (auto i = 0; i < hdr_.msg_size_size(); ++i) {
        size_t msgReadSoFar = 0;
        ZmqMessage msg;
        auto sz = hdr_.msg_size(i);
        RETURN_IF_NOT_OK(msg.AllocMem(sz));
        RETURN_IF_NOT_OK(TransferFromWA(msg.Data(), sz, msgReadSoFar));
        // For the rest we will simply read directly into the ZmqMessage.
        if (msgReadSoFar < sz) {
            UnixSockFd sock(fd_);
            // We will block ourselves until we get all the data.
            RETURN_IF_NOT_OK(
                sock.Recv(reinterpret_cast<uint8_t *>(msg.Data()) + msgReadSoFar, sz - msgReadSoFar, true));
        }
        VLOG(RPC_LOG_LEVEL) << "Frame (" << i << ") received. Size " << msg.Size() << " ... " << msg;
        v1Frames_.push_back(std::move(msg));
    }
    // Read all the message parts for this rpc. Go back to start of the next rpc message
    state = MsgState::HDR_LEN_READY;
    return Status::OK();
}

Status ZmqMsgDecoder::DecodeFlag(MsgState &state)
{
    CHECK_FAIL_RETURN_STATUS(state == MsgState::FLAGS_READY, K_RUNTIME_ERROR, "Wrong state");
    inProcess_ = ZmqMessage();
    msgSize_ = 0;
    // Check if we have at least one byte
    if (Empty()) {
        RETURN_IF_NOT_OK(Recv());
    }
    uint8_t *buf = wa_.get();
    flag_ = static_cast<MTP_PROTOCOL>(buf[pos_]);
    const int DISPLAY_LENGTH = 2;
    VLOG(RPC_LOG_LEVEL) << "Flag = %x" << std::hex << std::setfill('0') << std::setw(DISPLAY_LENGTH) << flag_;
    // Change the state
    if (flag_ & MTP_PROTOCOL::MTP_LONG) {
        state = MsgState::EIGHT_BYTE_SIZE_READY;
    } else {
        state = MsgState::ONE_BYTE_SIZE_READY;
    }
    pos_ += 1;
    return Status::OK();
}

Status ZmqMsgDecoder::DecodeOneByteLength(MsgState &state)
{
    CHECK_FAIL_RETURN_STATUS(state == MsgState::ONE_BYTE_SIZE_READY, K_RUNTIME_ERROR, "Wrong state");
    // Check if we have at least one byte
    if (Empty()) {
        RETURN_IF_NOT_OK(Recv());
    }
    auto *buf = wa_.get() + pos_;
    msgSize_ = static_cast<uint8_t>(buf[0]);
    VLOG(RPC_LOG_LEVEL) << FormatString("Message length: %d", msgSize_);
    // Move the state to the real payload
    state = MsgState::MESSAGE_READY;
    pos_ += 1;
    return Status::OK();
}

Status ZmqMsgDecoder::DecodeEightByteLength(MsgState &state)
{
    CHECK_FAIL_RETURN_STATUS(state == MsgState::EIGHT_BYTE_SIZE_READY, K_RUNTIME_ERROR, "Wrong state");
    // Check if we have enough bytes to process, we need to have at least 8 bytes
    // in the work area
    if (NumUnRead() < K_EIGHT_BYTE) {
        RETURN_IF_NOT_OK(Recv());
        CHECK_FAIL_RETURN_STATUS(NumUnRead() >= K_EIGHT_BYTE, K_TRY_AGAIN, "Waiting for more network data to arrive");
    }
    {
        auto *buf = wa_.get() + pos_;
        google::protobuf::io::ArrayInputStream osWrapper(buf, K_EIGHT_BYTE, K_EIGHT_BYTE);
        google::protobuf::io::CodedInputStream input(&osWrapper);
        CHECK_FAIL_RETURN_STATUS(input.ReadLittleEndian64(&msgSize_), K_RUNTIME_ERROR, "Google read error");
    }
    VLOG(RPC_LOG_LEVEL) << FormatString("Message length: %d", msgSize_);
    state = MsgState::MESSAGE_READY;
    pos_ += K_EIGHT_BYTE;
    return Status::OK();
}

void ZmqMsgDecoder::RegisterAllocation(void *dest, uint64_t sz)
{
    uint64_t addr = reinterpret_cast<uint64_t>(dest);
    std::lock_guard<std::mutex> lock(ZmqMsgDecoder::allocMux_);
    ZmqMsgDecoder::regAlloc_.emplace(addr, sz);
}

void ZmqMsgDecoder::DeregisterAllocation(void *dest)
{
    uint64_t addr = reinterpret_cast<uint64_t>(dest);
    std::lock_guard<std::mutex> lock(ZmqMsgDecoder::allocMux_);
    ZmqMsgDecoder::regAlloc_.erase(addr);
}

Status ZmqMsgDecoder::FindRegisteredAlloc(void *dest, uint64_t sz)
{
    uint64_t addr = reinterpret_cast<uint64_t>(dest);
    Status err = Status(K_OUT_OF_RANGE, "No registered allocation found for the destination addr");
    std::lock_guard<std::mutex> lock(ZmqMsgDecoder::allocMux_);
    auto it = ZmqMsgDecoder::regAlloc_.lower_bound(addr);
    if (it != ZmqMsgDecoder::regAlloc_.end()) {
        if (std::numeric_limits<uint64_t>::max() - addr < sz) {
            return err;
        }
        if ((addr < it->first) || ((addr + sz) > (it->first + it->second))) {
            return err;
        }
        VLOG(RPC_LOG_LEVEL) << FormatString("(0x%x, 0x%x) inside (0x%x, 0x%x)", addr, addr + sz, it->first,
                                  it->first + it->second);
        return Status::OK();
    }
    return err;
}

Status ZmqMsgDecoder::ReadMessage(MsgState &state, void *dest, size_t sz)
{
    CHECK_FAIL_RETURN_STATUS(state == MsgState::MESSAGE_READY, K_RUNTIME_ERROR, "Wrong state");
    auto chgState = [this, &state]() {
        // Next state depends on if we have more parts coming
        state = (flag_ & MTP_MORE) ? MsgState::FLAGS_READY : MsgState::HDR_LEN_READY;
    };
    // Edge case. Empty message
    if (msgSize_ == 0) {
        chgState();
        return Status::OK();
    }

    // Set aside memory for the real message. If user provides a buffer, verify its length
    if (dest != nullptr) {
        // Check that the address falls into allocated memory.
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(ZmqMsgDecoder::FindRegisteredAlloc(dest, sz),
                                         "The destination is not existing allocated memory.");
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
            sz >= msgSize_, K_RUNTIME_ERROR,
            FormatString("User provided buffer is too small %zu. Expect %zu", sz, msgSize_));
        inProcess_.TransferOwnership(dest, sz, nullptr, nullptr);
    } else {
        VLOG(RPC_LOG_LEVEL) << FormatString("Allocate %zu bytes for incoming message", msgSize_);
        RETURN_IF_NOT_OK(inProcess_.AllocMem(msgSize_));
    }

    size_t msgReadSoFar = 0;
    // We may have read part of the message in the work area already. If so,
    // copy it to the ZmqMessage
    RETURN_IF_NOT_OK(TransferFromWA(inProcess_.Data(), msgSize_, msgReadSoFar));
    // For the rest or large payload, we will simply read directly into the ZmqMessage.
    if (msgReadSoFar < msgSize_) {
        // We will block ourselves until we get all the data.
        UnixSockFd sock(fd_);
        RETURN_IF_NOT_OK(
            sock.Recv(reinterpret_cast<uint8_t *>(inProcess_.Data()) + msgReadSoFar, msgSize_ - msgReadSoFar, true));
    }
    chgState();
    return Status::OK();
}

Status ZmqMsgDecoder::Decode(void *dest, size_t sz)
{
    if (msgState_ == MsgState::HDR_LEN_READY) {
        RETURN_IF_NOT_OK(DecodeHdrLen(msgState_));
    }
    if (msgState_ == MsgState::HDR_BODY_READY) {
        RETURN_IF_NOT_OK(DecodeHdrBody(msgState_));
    }
    if (msgState_ == MsgState::MTP_DETECT) {
        RETURN_IF_NOT_OK(DetectMTP(msgState_));
    }
    if (msgState_ == MsgState::DOWNLEVEL_CLIENT) {
        RETURN_IF_NOT_OK(V1Client(msgState_));
    }
    if (msgState_ == MsgState::FLAGS_READY) {
        RETURN_IF_NOT_OK(DecodeFlag(msgState_));
    }
    if (msgState_ == MsgState::ONE_BYTE_SIZE_READY) {
        RETURN_IF_NOT_OK(DecodeOneByteLength(msgState_));
    }
    if (msgState_ == MsgState::EIGHT_BYTE_SIZE_READY) {
        RETURN_IF_NOT_OK(DecodeEightByteLength(msgState_));
    }
    if (msgState_ == MsgState::MESSAGE_READY) {
        RETURN_IF_NOT_OK(ReadMessage(msgState_, dest, sz));
    }
    return Status::OK();
}

Status ZmqMsgDecoder::GetMessage(ZmqMessage &outMsg, bool &more)
{
    Status rc;
    more = false;
    do {
        rc = Decode();
        RETURN_IF_NOT_OK_EXCEPT(rc, K_TRY_AGAIN);
        if (rc.GetCode() == K_TRY_AGAIN) {
            continue;
        }
        // If downlevel client, simply break out from the loop,
        // we have got all the messages already.
        if (!newFormat_) {
            break;
        }
        // The rest is for V2 MTP only.
        more = (flag_ & MTP_MORE) != 0;
        outMsg = std::move(inProcess_);
        if (flag_ & MTP_PROTOCOL::MTP_DECODER) {
            outMsg.SetType(ZmqMessage::ZmqMsgType::DECODER);
        }
    } while (rc.GetCode() == K_TRY_AGAIN);
    return rc;
}

Status ZmqMsgDecoder::ReceiveMsgFramesV1(ZmqMsgFrames &frames) const
{
    UnixSockFd sock(fd_);
    MultiMsgHdrPb hdr;
    RETURN_IF_NOT_OK(sock.RecvProtobuf(hdr));
    const int numMsg = hdr.msg_size_size();
    VLOG(RPC_LOG_LEVEL) << FormatString("Prepare to receive %d frames from fd %d using V1 format", numMsg,
                                        sock.GetFd());
    for (int i = 0; i < numMsg; ++i) {
        ZmqMessage msg;
        RETURN_IF_NOT_OK(msg.AllocMem(hdr.msg_size(i)));
        RETURN_IF_NOT_OK(sock.Recv(msg.Data(), msg.Size(), true));
        VLOG(RPC_LOG_LEVEL) << "Frame (" << i << ") received. Size " << msg.Size() << " ... " << msg;
        frames.push_back(std::move(msg));
    }
    return Status::OK();
}

Status ZmqMsgDecoder::ReceiveMsgFramesV2(ZmqMsgFrames &frames)
{
    // Version 2 supports writing payload into user's provided buffer.
    // It is not as simple as user provides us a call back function or
    // a buffer that we can call the function or write into that buffer.
    // For example, we may not know the tenant id until part of the rpc
    // message is de-serialized. But that is worker's logic, and we are
    // in a chicken and egg situation.
    // So this is what we will do. When we see a frame that is tagged with
    // PAYLOAD_SZ, we know it is the beginning of a payload frames.
    // We will pause at this point and keep track which state we are in,
    // and return the current frames to the caller and let them process the request.
    // The caller should have sufficient information to compute the tenant id and
    // where and how much memory to allocate.
    // To resume, caller will call another form  ReceivePayload() with a buffer
    // for us to store the payload.

    bool more = false;
    Status rc;
    curFrame_ = 0;
    do {
        ZmqMessage msg;
        RETURN_IF_NOT_OK(GetMessage(msg, more));
        // If we detect downlevel client using V1, underlying logic has already got all the
        // parts, and we break out from the loop.
        if (!newFormat_) {
            while (!v1Frames_.empty()) {
                ZmqMessage v1Msg = std::move(v1Frames_.front());
                v1Frames_.pop_front();
                frames.push_back(std::move(v1Msg));
            }
            break;
        }
        VLOG(RPC_LOG_LEVEL) << "Frame (" << curFrame_ << ") received. Size " << msg.Size() << " ... " << msg;
        if (msg.GetType() == ZmqMessage::ZmqMsgType::DECODER) {
            // A decoder frame is for us only. It contains the address where we should write the
            // subsequent payload to.
            PayloadDirectGetRspPb pb;
            RETURN_IF_NOT_OK(ParseFromZmqMessage(msg, pb));
            RETURN_IF_NOT_OK(ReceivePayloadIntoMemory(reinterpret_cast<void *>(pb.addr()), pb.sz()));
            // We should have exhausted all the frames.
            CHECK_FAIL_RETURN_STATUS(msgState_ == MsgState::HDR_LEN_READY, K_RUNTIME_ERROR,
                                     FormatString("Unexpected state %d", static_cast<int>(msgState_)));
            more = false;
        }
        frames.push_back(std::move(msg));
        ++curFrame_;
    } while (more);
    return Status::OK();
}

Status ZmqMsgDecoder::ReceivePayloadIntoMemory(void *dest, size_t sz)
{
    CHECK_FAIL_RETURN_STATUS(dest != nullptr, K_RUNTIME_ERROR, "Null destination");
    VLOG(RPC_LOG_LEVEL) << FormatString("Prepare to receive %zu bytes into user provided memory at 0x%x", sz,
                                        reinterpret_cast<intptr_t>(dest));
    auto *ptr = reinterpret_cast<uint8_t *>(dest);
    Status rc;
    while (sz > 0) {
        rc = Decode(ptr, sz);
        RETURN_IF_NOT_OK_EXCEPT(rc, K_TRY_AGAIN);
        if (rc.GetCode() == K_TRY_AGAIN) {
            continue;
        }
        ptr += msgSize_;
        sz -= msgSize_;
        VLOG(RPC_LOG_LEVEL) << "Frame (" << curFrame_++ << ") received. Size " << msgSize_;
    }
    return Status::OK();
}

ZmqMsgDecoder::ZmqMsgDecoder(int fd)
    : fd_(fd),
      curFrame_(0),
      msgState_(MsgState::HDR_LEN_READY),
      flag_(MTP_PROTOCOL::MTP_NONE),
      bytesReceived_(0),
      pos_(0),
      msgSize_(0),
      rpcHdrSz_(0),
      newFormat_(true)
{
    wa_ = std::make_unique<uint8_t[]>(K_WA_SIZE);
}

ZmqMsgDecoder::~ZmqMsgDecoder()
{
}

Status ZmqMsgEncoder::SendMessage(const ZmqMessage &msg, bool more) const
{
    struct {
        uint8_t flag_;
        char len_[K_EIGHT_BYTE];
    } hdr{};
    static_assert(sizeof(hdr) == K_EIGHT_BYTE + 1, "Doesn't expect a gap");
    auto sz = msg.Size();
    hdr.flag_ = more ? MTP_MORE : 0;
    if (sz > std::numeric_limits<uint8_t>::max()) {
        hdr.flag_ |= MTP_LONG;
        google::protobuf::io::ArrayOutputStream osWrapper(hdr.len_, K_EIGHT_BYTE);
        google::protobuf::io::CodedOutputStream output(&osWrapper);
        output.WriteLittleEndian64(sz);
    } else {
        hdr.len_[0] = static_cast<char>(sz);
    }
    // In order to support write directly into receiver's shared memory, we need to
    // tag certain frames for the receiving side.
    auto type = msg.GetType();
    if (type == ZmqMessage::ZmqMsgType::DECODER) {
        hdr.flag_ |= MTP_DECODER;
    }
    UnixSockFd sock(fd_);
    const int SHORT_LENGTH = 2;
    MemView buf(&hdr, (hdr.flag_ & MTP_LONG) ? K_EIGHT_BYTE + 1 : SHORT_LENGTH);
    RETURN_IF_NOT_OK(sock.Send(buf));
    if (sz > 0) {
        buf = MemView(msg.Data(), sz);
        RETURN_IF_NOT_OK(sock.Send(buf));
    }
    return Status::OK();
}

Status ZmqMsgEncoder::SendMsgFramesV1(ZmqMsgFrames &que) const
{
    UnixSockFd sock(fd_);
    MultiMsgHdrPb hdr;
    auto it = que.begin();
    while (it != que.end()) {
        hdr.mutable_msg_size()->Add(it->Size());
        ++it;
    }
    VLOG(RPC_LOG_LEVEL) << FormatString("Prepare to send %d frames to fd %d using V1 format", hdr.msg_size_size(),
                                        sock.GetFd());
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(sock.SendProtobuf(hdr), FormatString("Errno = %d", errno));
    int i = 0;
    while (!que.empty()) {
        auto &msg = que.front();
        MemView buf(msg.Data(), msg.Size());
        RETURN_IF_NOT_OK(sock.Send(buf));
        VLOG(RPC_LOG_LEVEL) << "Frame (" << i++ << ") sent. Size " << msg.Size() << " ... " << msg;
        que.pop_front();
    }
    return Status::OK();
}

Status ZmqMsgEncoder::SendMsgFramesV2(ZmqMsgFrames &que) const
{
    VLOG(RPC_LOG_LEVEL) << FormatString("Prepare to send %d frames to fd %d using V2 format", que.size(), fd_);
    // We will send an empty MultiMsgHdrPb to be compatible with V1,
    // and remote peer can distinguish if it is V1 or V2 format.
    {
        UnixSockFd sock(fd_);
        MultiMsgHdrPb hdr;
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(sock.SendProtobuf(hdr), FormatString("Errno = %d", errno));
    }
    int i = 0;
    bool more = true;
    do {
        auto msg = std::move(que.front());
        que.pop_front();
        more = !que.empty();
        RETURN_IF_NOT_OK(SendMessage(msg, more));
        VLOG(RPC_LOG_LEVEL) << "Frame (" << i++ << ") sent. Size " << msg.Size() << " ... " << msg;
    } while (more);
    return Status::OK();
}

Status ZmqMsgEncoder::SendMsgFrames(EventType type, ZmqMsgFrames &frames)
{
    if (type == V2MTP) {
        return SendMsgFramesV2(frames);
    } else if (type == V1MTP) {
        VLOG(RPC_LOG_LEVEL) << FormatString("Fall back to V1 format for fd %d", fd_);
        return SendMsgFramesV1(frames);
    }
    RETURN_STATUS(K_INVALID, FormatString("Unsupported type %d", type));
}
}  // namespace datasystem
