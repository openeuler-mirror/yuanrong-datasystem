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

#ifndef DATASYSTEM_COMMON_RPC_ZMQ_MSG_DECODER_H
#define DATASYSTEM_COMMON_RPC_ZMQ_MSG_DECODER_H

#include "datasystem/common/rpc/unix_sock_fd.h"
#include "datasystem/common/rpc/zmq/zmq_common.h"
#include "datasystem/common/rpc/zmq/zmq_message.h"
#include "datasystem/common/util/locks.h"

/**
 * Encoder/Decoder class to receive ZmqMessages from a socket
 * The current version is V2 is based on a simplified version of https://rfc.zeromq.org/spec/15/
 * with customization. V2 is backward compatible with V1.
 * UDS connection will remain using V1 protocol for old level client.
 * A rpc message is composed of multiple message frames regardless of V1 or V2.
 * For V2,
 * (a) First message frame is an empty MultiMsgHdrPb (to distinguish V1 or V2), followed by a series of
 *     message frames.
 * (b) The first byte of a message frame is a flag field with values from MTP_PROTOCOL enum below.
 * (c) The next byte (or next 8 bytes) is the length of message frame body that follows
 * (d) Message body (with length calculated in (c) above).
 * (e) Repeat (b) to (d) until all message frames are consumed for the current RPC message which will be
 *     then transferred to caller
 *
 * V2 supports the caller to provide its own buffer to store the payload frames. Otherwise payloads are
 * stored in private memory.
 */
namespace datasystem {
constexpr static int K_EIGHT_BYTE = 8;
enum MTP_PROTOCOL : uint8_t { MTP_NONE = 0x00, MTP_MORE = 0x01, MTP_LONG = 0x02, MTP_DECODER = 0x04 };

class ZmqMsgDecoder {
public:
    // State of receiving a ZmqMessage.
    enum class MsgState : int {
        HDR_LEN_READY = 0,
        HDR_BODY_READY,
        MTP_DETECT,
        DOWNLEVEL_CLIENT,
        FLAGS_READY,
        ONE_BYTE_SIZE_READY,
        EIGHT_BYTE_SIZE_READY,
        MESSAGE_READY
    };
    explicit ZmqMsgDecoder(int fd);
    ~ZmqMsgDecoder();

    /**
     * Version 1 protocol of receiving ZmqMessages
     * @param frames
     * @return
     * @note Doesn't support write into user provided buffers
     */
    Status ReceiveMsgFramesV1(ZmqMsgFrames &frames) const;

    /**
     * Version 2 protocol of receiving ZmqMessages
     * @param frames
     * @return
     * @note Based on ZMTP draft 15
     */
    Status ReceiveMsgFramesV2(ZmqMsgFrames &frames);

    /**
     * @brief Merge subsequent payload frames into user provided memory
     * @param dest Address of destination
     * @param sz Size of the destination
     * @return Status of the call
     * @note Only V2 MTP can support this form of direct write.
     */
    Status ReceivePayloadIntoMemory(void *dest, size_t sz);

    /**
     * @return File descriptor
     */
    auto GetFd() const
    {
        return fd_;
    }

    /**
     * @brief Return if it is a V2 client or not.
     * @note Only called after we receive a message
     */
    bool V2Client() const
    {
        return newFormat_;
    }

    /**
     * @brief Static function helper to register the memory allocation for addr ptr verification.
     * @param[in] dest Address of destination.
     * @param[in] sz Size of the destination.
     */
    static void RegisterAllocation(void *dest, uint64_t sz);

    /**
     * @brief Static function helper to de-register the memory allocation.
     * @param[in] dest Address of destination.
     */
    static void DeregisterAllocation(void *dest);

    /**
     * @brief Static function helper to check whether the destination is legit memory.
     * @param[in] dest Address of destination.
     * @param[in] sz Size of the destination.
     * @return Status of the call.
     */
    static Status FindRegisteredAlloc(void *dest, uint64_t sz);

private:
    constexpr static int K_WA_SIZE = 1024;
    int fd_;
    int curFrame_;
    MsgState msgState_;
    MTP_PROTOCOL flag_;
    std::unique_ptr<uint8_t[]> wa_;
    ssize_t bytesReceived_;
    size_t pos_;
    size_t msgSize_;
    uint32_t rpcHdrSz_;
    MultiMsgHdrPb hdr_;
    ZmqMessage inProcess_;
    ZmqMsgFrames v1Frames_;
    bool newFormat_;
    static std::mutex allocMux_;
    static std::map<uint64_t, uint64_t, std::greater<uint64_t>> regAlloc_;

    bool Empty() const;
    size_t NumUnRead() const;
    Status Recv();
    Status DecodeHdrLen(MsgState &state);
    Status DecodeHdrBody(MsgState &state);
    Status DetectMTP(MsgState &state);
    Status V1Client(MsgState &state);
    Status DecodeFlag(MsgState &state);
    Status DecodeOneByteLength(MsgState &state);
    Status DecodeEightByteLength(MsgState &state);
    Status ReadMessage(MsgState &state, void *, size_t sz);
    Status GetMessage(ZmqMessage &outMsg, bool &more);
    Status Decode(void *dest = nullptr, size_t sz = 0);
    Status TransferFromWA(void *dest, size_t sz, size_t &bytesReceived);
};

/**
 * Encoder class to send ZmqMessages on a socket
 */
class ZmqMsgEncoder {
public:
    explicit ZmqMsgEncoder(int fd) : fd_(fd)
    {
    }
    ~ZmqMsgEncoder() = default;

    /**
     * Sending ZmqMessages
     */
    Status SendMsgFrames(EventType type, ZmqMsgFrames &frames);

private:
    int fd_;

    /**
     * Version 1 protocol of sending ZmqMessages
     * @param frames
     * @return
     * @note Doesn't support write into user provided buffers
     */
    Status SendMsgFramesV1(ZmqMsgFrames &que) const;

    /**
     * Version 2 protocol of sending ZmqMessages
     * @param que
     * @return
     */
    Status SendMsgFramesV2(ZmqMsgFrames &que) const;

    Status SendMessage(const ZmqMessage &msg, bool more) const;
};
}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_RPC_ZMQ_MSG_DECODER_H
