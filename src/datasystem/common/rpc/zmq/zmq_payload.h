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
#ifndef DATASYSTEM_COMMON_RPC_ZMQ_PAYLOAD_H
#define DATASYSTEM_COMMON_RPC_ZMQ_PAYLOAD_H

#include <atomic>
#include <chrono>
#include <deque>
#include <mutex>
#include <utility>
#include <tuple>
#include <vector>

#include "datasystem/common/eventloop/timer_queue.h"
#include "datasystem/common/rpc/rpc_message.h"
#include "datasystem/common/rpc/zmq/zmq_common.h"
#include "datasystem/common/rpc/zmq/zmq_msg_queue.h"
#include "datasystem/protos/meta_zmq.pb.h"
#include "datasystem/common/util/locks.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {
/**
 * A payload is not part of any rpc messages defined in the proto file.
 * It is always sent using low level ZMQ routines to avoid memory copy
 * because it tends to be huge in size.
 */
struct ZmqPayloadEntry {
    ZmqPayloadEntry() : len(0)
    {
    }
    ~ZmqPayloadEntry() = default;
    uint64_t len;
    std::vector<RpcMessage> recvBuf;
};

struct AsyncPayloadEntry : public ZmqPayloadEntry {
    AsyncPayloadEntry() : ZmqPayloadEntry::ZmqPayloadEntry(), numPending(0)
    {
    }
    ~AsyncPayloadEntry() = default;
    std::shared_ptr<char[]> buf;
    std::vector<uint32_t> originalSizes;
    std::atomic<uint64_t> numPending;
    MetaPb meta;
};

class ZmqPayload {
public:
    ZmqPayload() = default;
    ~ZmqPayload() = default;

    /**
     * @brief A helper function for the server to prepare payload frames to be sent to the client.
     * @param[in] buffer Payload buffer.
     * @param[in] frames Zmq message frames.
     * @param[out] bufSz Size of the payload.
     */
    static Status AddPayloadFrames(std::vector<RpcMessage> &buffer, ZmqMsgFrames &frames, size_t &bufSz,
                                   bool tagPayloadFrame);

    /**
     * @brief A helper function for the server to prepare payload frames to be sent to the client.
     * @details This function will append (a) the size of the payload, (b) clock start time, (c) a vector of
     * ZmqMessage which are the real payloads to the zmq frames.
     * @note Currently this function is only used on the server side, client side will call SendDirect() instead.
     * @param[in] playload Payload reference.
     * @param[in] frames Zmq message frames.
     * @param[out] bufSz Size of the payload.
     */
    static Status AddPayloadFrames(const std::vector<MemView> &payload, ZmqMsgFrames &frames, size_t &bufSz,
                                   bool tagPayloadFrame);

    /**
     * @brief Process message frames onto payload.
     * @param[in] frames Frames to process.
     * @param[out] out Payload to be loaded.
     * @return Ok is frames are process successfully.
     */
    static Status ProcessEmbeddedPayload(ZmqMsgFrames &frames, std::unique_ptr<ZmqPayloadEntry> &out);

    /**
     * @brief Create an async payload entry. Called by the server
     * @return Status object
     */
    static Status CreateAsyncPayloadEntry(const HandshakePb &rq, const MetaPb &meta,
                                          std::unique_ptr<AsyncPayloadEntry> &out);

    /**
     * @brief Split a payload into pieces with the size of each piece less than or equal to chunkSz and sent
     *        in parallel.
     * @return Status object
     */
    static Status SendAsync(const std::shared_ptr<ZmqMsgQueRef> &mQue, const MetaPb &meta, uint32_t chunkSz,
                     ZmqMsgFrames &frames, const std::vector<MemView> &payload);
};

/**
 * This is a simple iterator to traverse the vector of MemView.
 */
class ZmqPayloadIterator {
public:
    ZmqPayloadIterator() : pyl_(), index_(0), curPtr_(nullptr, 0)
    {
    }
    explicit ZmqPayloadIterator(std::vector<MemView> pylObj) : pyl_(std::move(pylObj)), index_(0)
    {
        if (!pyl_.empty()) {
            curPtr_ = MemView(pyl_[index_]);
        }
    }
    ~ZmqPayloadIterator() = default;

    MemView operator*()
    {
        return curPtr_;
    }

    /**
     * This move forward the curPtr_ to (at most) n bytes but stay within the last
     * const_buffer unless the last const_buffer has already reached the end.
     * @param n
     * @return
     */
    ZmqPayloadIterator &operator+=(uint n)
    {
        if (!isEnd_) {
            curPtr_ += n;
            if (curPtr_.Size() == 0) {
                if (++index_ < pyl_.size()) {
                    curPtr_ = pyl_[index_];
                } else {
                    // We reach the end
                    isEnd_ = true;
                }
            }
        }
        return *this;
    }

    bool IsEnd() const
    {
        return isEnd_;
    }

private:
    std::vector<MemView> pyl_;
    uint32_t index_;
    MemView curPtr_;
    bool isEnd_ = false;
};

/**
 * ZMQ server will store all the payload in a std::map.
 * The client will pick up the payload later together with
 * the rpc request.
 */
typedef std::tuple<int, ZmqMsgFrames, std::unique_ptr<AsyncPayloadEntry>, std::unique_ptr<TimerQueue::TimerImpl>>
    BankColumns;
class ZmqPayloadBank {
public:
    enum COL : uint8_t { K_FD = 0, K_MSG = 1, K_ASYNC = 2, K_TIMER = 3 };
    ZmqPayloadBank();
    ~ZmqPayloadBank() = default;
    int64_t GetId();
    int64_t SavePayload(int fd, ZmqMsgFrames &&p, std::unique_ptr<AsyncPayloadEntry> &&async,
                        std::unique_ptr<TimerQueue::TimerImpl> &&timer);
    BankColumns GetAndErase(int64_t id);
    Status CreateAsyncPayloadEntry(const HandshakePb &rq, const MetaPb &meta, ZmqMsgFrames &inMsg,
                                   HandshakeTokenPb &reply);

private:
    friend class ZmqService;
    std::atomic<int64_t> id_;
    WriterPrefRWLock mux_;
    std::unordered_map<int64_t, BankColumns> bank_;
};
}  // namespace datasystem
#endif  // DATASYSTEM_COMMON_RPC_ZMQ_PAYLOAD_H
