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
 * In non-blocking unary RPC, we have ServerUnaryWriterReaderImpl.
 */
#ifndef DATASYSTEM_COMMON_RPC_ZMQ_STREAM_SERVER_H
#define DATASYSTEM_COMMON_RPC_ZMQ_STREAM_SERVER_H

#include <queue>
#include <utility>
#include <mutex>
#include <condition_variable>
#include "datasystem/common/rpc/rpc_message.h"
#include "datasystem/common/rpc/zmq/zmq_service.h"
#include "datasystem/common/rpc/zmq/zmq_stream_base.h"
#include "datasystem/common/log/log_helper.h"
#include "datasystem/common/util/thread_local.h"

namespace datasystem {
/**
 * A min heap priority queue for sorting the stream element
 */
class HeapSort {
public:
    class HeapElement {
    public:
        int64_t seqNo_;
        MetaPb meta_;
        ZmqMsgFrames frames_;
        HeapElement(int64_t seqNo, MetaPb &meta, ZmqMsgFrames &frames)
            : seqNo_(seqNo), meta_(meta), frames_(std::move(frames))
        {
        }
        ~HeapElement() = default;
        HeapElement(HeapElement &&rhs) noexcept
            : seqNo_(rhs.seqNo_), meta_(std::move(rhs.meta_)), frames_(std::move(rhs.frames_))
        {
        }
        HeapElement &operator=(HeapElement &&rhs) noexcept
        {
            if (this != &rhs) {
                std::swap(this->seqNo_, rhs.seqNo_);
                std::swap(this->meta_, rhs.meta_);
                std::swap(this->frames_, rhs.frames_);
            }
            return *this;
        }
        HeapElement(const HeapElement &rhs) = delete;
        HeapElement &operator=(const HeapElement &rhs) = delete;
    };
    struct Compare {
        bool operator()(const HeapElement &a, const HeapElement &b)
        {
            return a.seqNo_ > b.seqNo_;
        }
    };

    HeapSort() : que_(std::make_unique<std::priority_queue<HeapElement, std::vector<HeapElement>, Compare>>())
    {
    }
    ~HeapSort() = default;

    auto Empty() const
    {
        return que_->empty();
    }

    auto Size() const
    {
        return que_->size();
    }

    void Push(int64_t seqNo, MetaPb &meta, ZmqMsgFrames &frames);

    void Pop(int64_t &seqNo, MetaPb &meta, ZmqMsgFrames &frames);

    int64_t Peek()
    {
        return que_->top().seqNo_;
    }

private:
    std::unique_ptr<std::priority_queue<HeapElement, std::vector<HeapElement>, Compare>> que_;
};

/**
 * A base class for rpc streaming (server side).
 */
class ServerStreamBase : public StreamBase {
public:
    explicit ServerStreamBase(std::shared_ptr<ZmqServerMsgQueRef> mQueue, MetaPb meta, bool sendPayload,
                              bool recvPayload, int64_t seqNo);

    virtual ~ServerStreamBase() override;

    Status SendStatus(const Status &rc);

    Status SendAll(ZmqSendFlags flags) override;

    template <typename W>
    Status WritePb(const W &pb)
    {
        VLOG(RPC_LOG_LEVEL) << "Stream worker " << meta_.worker_id() << " sending rc " << Status::OK() << " message "
                            << LogHelper::IgnoreSensitive(pb) << " back to client " << meta_.client_id() << std::endl;
        ZmqMessage rcMsg = StatusToZmqMessage(Status::OK());
        outMsg_.push_back(std::move(rcMsg));
        RETURN_IF_NOT_OK(PushBackProtobufToFrames(pb, outMsg_));
        RETURN_OK_IF_TRUE(HasRecvPayloadOp());
        return SendAll(ZmqSendFlags::NONE);
    }

    /**
     * @brief Send a payload after sending the request protobuf
     * @note The option recv_payload_option must be set in the proto. Must be called after Write()
     * @param buffer Sending payload buffer.
     * @return Status of call.
     */
    Status SendPayload(std::vector<RpcMessage> &buffer);

    /**
     * @brief Send a payload after sending the request protobuf
     * @note The option recv_payload_option must be set in the proto. Must be called after Write()
     * @param payload Sending payload buffer.
     * @return Status of call.
     */
    Status SendPayload(const std::vector<MemView> &payload);

    Status ReadAll(ZmqRecvFlags flags) override;

    template <typename R>
    Status ReadPb(R &pb)
    {
        VLOG(RPC_LOG_LEVEL) << "Stream worker " << meta_.worker_id() << " reading" << std::endl;
        RETURN_IF_NOT_OK(ReadAll(ZmqRecvFlags::NONE));
        CHECK_FAIL_RETURN_STATUS(!inMsg_.empty(), StatusCode::K_INVALID, "Invalid stream.");
        ZmqMessage protoMsg = std::move(inMsg_.front());
        inMsg_.pop_front();
        RETURN_IF_NOT_OK(ParseFromZmqMessage(protoMsg, pb));
        g_SerializedMessage = std::move(protoMsg);
        g_ReqAk = meta_.access_key();
        g_ReqSignature = meta_.signature();
        g_ReqTimestamp = meta_.timestamp();
        return Status::OK();
    }

    /**
     * @brief Receive a payload after receiving response protobuf.
     * @note The option send_payload_option must be set in the proto. Must be called after Read()
     * @param payload receiving payload buffers.
     * @return Status of call.
     */
    Status ReceivePayload(std::vector<RpcMessage> &payload);

    Status Finish() override;

protected:
    std::shared_ptr<ZmqServerMsgQueRef> mQue_;
    HeapSort sort_;
    int64_t expectedSeqNo_;
};

/**
 * Only server side is streaming.
 * @tparam W Stream RPC mode, WritePb type.
 */
template <typename W>
class ServerWriterImpl : public ServerStreamBase {
public:
    explicit ServerWriterImpl(std::shared_ptr<ZmqServerMsgQueRef> zmqServerMsgQueRefPtr, MetaPb meta, bool sendPayload,
                              bool recvPayload, int64_t seqNo)
        : ServerStreamBase(std::move(zmqServerMsgQueRefPtr), meta, sendPayload, recvPayload, seqNo)
    {
    }
    ~ServerWriterImpl() override
    {
        // See ~ServerStreamBase
        expectedSeqNo_ = std::numeric_limits<int64_t>::max();
    }

    Status Write(const W &pb)
    {
        return ServerStreamBase::WritePb(pb);
    }
};

/**
 * Only the client side is streaming.
 */
template <typename R>
class ServerReaderImpl : public ServerStreamBase {
public:
    explicit ServerReaderImpl(std::shared_ptr<ZmqServerMsgQueRef> zmqServerMsgQueRefPtr, MetaPb meta, bool sendPayload,
                              bool recvPayload, int64_t seqNo)
        : ServerStreamBase(std::move(zmqServerMsgQueRefPtr), meta, sendPayload, recvPayload, seqNo)
    {
    }

    ~ServerReaderImpl() override = default;

    Status Read(R &pb)
    {
        Status rc = ServerStreamBase::ReadPb(pb);
        if (rc.GetCode() == K_RPC_STREAM_END) {
            // See ~ServerStreamBase
            expectedSeqNo_ = std::numeric_limits<int64_t>::max();
        }
        return rc;
    }

    Status Finish() override
    {
        RETURN_STATUS(StatusCode::K_INVALID, "Invalid Finish operation");
    }
};

/**
 * Both sides are streaming.
 * @tparam W Stream RPC mode, WritePb type.
 * @tparam R Stream RPC mode, ReadPb type.
 */
template <typename W, typename R>
class ServerWriterReaderImpl : public ServerStreamBase {
public:
    explicit ServerWriterReaderImpl(std::shared_ptr<ZmqServerMsgQueRef> sock, MetaPb meta, bool sendPayload,
                                    bool recvPayload, int64_t seqNo)
        : ServerStreamBase(sock, meta, sendPayload, recvPayload, seqNo)
    {
    }

    ~ServerWriterReaderImpl() override = default;

    Status Read(R &pb)
    {
        Status rc = ServerStreamBase::ReadPb(pb);
        if (rc.GetCode() == K_RPC_STREAM_END) {
            // See ~ServerStreamBase
            expectedSeqNo_ = std::numeric_limits<int64_t>::max();
        }
        return rc;
    }

    Status Write(const W &pb)
    {
        return ServerStreamBase::WritePb(pb);
    }
};

template <typename W, typename R>
class ServerUnaryWriterReaderImpl : public StreamBase {
public:
    explicit ServerUnaryWriterReaderImpl(std::shared_ptr<ZmqServerMsgQueRef> mQue, const MetaPb &meta,
                                         ZmqMsgFrames &&inMsg, bool sendPayload, bool recvPayload)
        : StreamBase::StreamBase(sendPayload, recvPayload),
          mQue_(std::move(mQue)),
          writeOnce_(false),
          readOnce_(false),
          requestComplete_(true)
    {
        meta_ = meta;
        inMsg_ = std::move(inMsg);
        enableMsgQ_ = (mQue_ != nullptr);
    }

    ~ServerUnaryWriterReaderImpl() override = default;

    virtual Status SendStatus(const Status &rc)
    {
        bool expected = false;
        if (writeOnce_.compare_exchange_strong(expected, true)) {
            VLOG(RPC_LOG_LEVEL) << FormatString("Server uses unary socket sending rc %d back to client %s", rc,
                                                meta_.client_id());
            ZmqMessage rcMsg = StatusToZmqMessage(rc);
            outMsg_.push_back(std::move(rcMsg));
            if (enableMsgQ_) {
                return SendAll(ZmqSendFlags::NONE);
            }
            return Status::OK();
        } else {
            // Already sent something to the client, this socket is immediately marked as reusable for other clients.
            // So we just log error here and return Status::OK to make sure this socket won't return this error to
            // another client.
            LOG(WARNING) << "ServerUnaryWriterReaderImpl is only supposed to be used once!\n";
            LOG(WARNING) << "Original rc: " << rc << std::endl;
            return Status::OK();
        }
    }

    Status GetOutMsg(ZmqMsgFrames &outMsg)
    {
        // Most codepaths do not have async handling, and requestComplete_ will always be true.
        // If it does have async, then requestComplete_ will be initialized to false and we as the parent need to wait
        // for the child thread to inform us when it is safe to continue processing after the request is done.
        if (!requestComplete_) {
            VLOG(RPC_LOG_LEVEL) << "Work agent needs to wait for async request to complete before returning results.";
            std::unique_lock<std::mutex> lock(requestCompleteMtx_);
            requestCompleteCond_.wait(lock, [this] { return (requestComplete_ == true); });
            // There is no need to flip the requestComplete to false again because this class is instantiated for every
            // request and is not re-used.
            VLOG(RPC_LOG_LEVEL) << "Work agent was notified that the request completed.";
        }
        outMsg = std::move(outMsg_);
        return Status::OK();
    }

    virtual bool EnableMsgQ()
    {
        return enableMsgQ_;
    }

    void SetRequestInProgress()
    {
        if (!enableMsgQ_) {
            requestComplete_ = false;
        }
        // no-op if message queues were used. This call only relevent for exclusive connection mode.
        // requestComplete_ remains true in that case.
    }

    void SetRequestComplete()
    {
        if (!enableMsgQ_ && !requestComplete_) {
            // Signal that the request is done.
            std::unique_lock<std::mutex> lock(requestCompleteMtx_);
            requestComplete_ = true;
            lock.unlock();
            requestCompleteCond_.notify_one();
        }
        // no-op if message queues were used. This call only relevent for exclusive connection mode.
        // no-op if the requestComplete_ was already true. Its atomic so flag checks won't race. The mutex is only used
        // for causing parent to sleep and child to wake it up.
    }

    Status SendAll(ZmqSendFlags flags) override
    {
        PerfPoint::RecordElapsed(PerfKey::ZMQ_APP_WORKLOAD, GetLapTime(meta_, "ZMQ_APP_WORKLOAD"));
        RETURN_OK_IF_TRUE(mQue_ == nullptr);
        return ZmqService::SendAll(outMsg_, meta_,
                                   [this, &flags](ZmqMetaMsgFrames &e) { return mQue_->SendMsg(e, flags); });
    }

    virtual Status Write(const W &pb)
    {
        bool expected = false;
        if (writeOnce_.compare_exchange_strong(expected, true)) {
            VLOG(RPC_LOG_LEVEL) << "Server uses unary socket sending rc " << Status::OK() << " message "
                                << LogHelper::IgnoreSensitive(pb) << " back to client " << meta_.client_id()
                                << std::endl;
            PerfPoint point(PerfKey::ZMQ_RESPONSE_PROTO_TO_MSG);
            ZmqMessage rcMsg = StatusToZmqMessage(Status::OK());
            outMsg_.push_back(std::move(rcMsg));
            RETURN_IF_NOT_OK(PushBackProtobufToFrames(pb, outMsg_));
            point.Record();
            PerfPoint::RecordElapsed(PerfKey::ZMQ_RESPONSE_SIZE_AFTER_SERIALIZE, outMsg_.back().Size());
            RETURN_OK_IF_TRUE(HasRecvPayloadOp());
            if (enableMsgQ_) {
                return SendAll(ZmqSendFlags::NONE);
            }
            return Status::OK();
        } else {
            RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, "ServerUnaryWriterReaderImpl is only supposed to be used once!");
        }
    }

    virtual Status ConstructWriteMsg(const W &pb, ZmqMsgFrames &outMsg)
    {
        CHECK_FAIL_RETURN_STATUS(!enableMsgQ_, StatusCode::K_RUNTIME_ERROR,
                                 "Invoke ConstructWriteMsg() only if enableMsgQ_ flag is off.");
        bool expected = false;
        if (writeOnce_.compare_exchange_strong(expected, true)) {
            VLOG(RPC_LOG_LEVEL) << "Server uses unary socket sending rc " << Status::OK() << " message "
                                << LogHelper::IgnoreSensitive(pb) << " back to client " << meta_.client_id()
                                << std::endl;
            ZmqMessage rcMsg = StatusToZmqMessage(Status::OK());
            outMsg.push_back(std::move(rcMsg));
            RETURN_IF_NOT_OK(PushBackProtobufToFrames(pb, outMsg));
            RETURN_OK_IF_TRUE(HasRecvPayloadOp());
            return Status::OK();
        } else {
            RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, "ServerUnaryWriterReaderImpl is only supposed to be used once!");
        }
    }

    /**
     * @brief Send a payload after sending the request protobuf
     * @note The option recv_payload_option must be set in the proto. Must be called after Write()
     * @param[in] buffer Sending payload buffer.
     * @return Status of call.
     */
    Status SendAndTagPayload(std::vector<datasystem::RpcMessage> &buffer, bool tagPayloadFrame)
    {
        CHECK_FAIL_RETURN_STATUS(HasRecvPayloadOp(), StatusCode::K_INVALID,
                                 "recv_payload_option is not specified in the proto");
        // We can't use the SendAll because we will then send a duplicate clientUuid.
        size_t bufSz = 0;
        RETURN_IF_NOT_OK(ZmqPayload::AddPayloadFrames(buffer, outMsg_, bufSz, tagPayloadFrame));
        VLOG(RPC_LOG_LEVEL) << FormatString("Server uses unary socket to send %zu payload bytes to client %s", bufSz,
                                            meta_.client_id());
        VLOG(RPC_LOG_LEVEL) << FormatString(
            "Server uses unary socket to send %zu payload bytes to Service %s Method"
            "%d to client %s",
            bufSz, meta_.svc_name(), meta_.method_index(), meta_.client_id());
        if (enableMsgQ_) {
            return SendAll(ZmqSendFlags::NONE);
        }
        return Status::OK();
    }

    virtual Status SendPayload(std::vector<datasystem::RpcMessage> &buffer)
    {
        return SendAndTagPayload(buffer, false);
    }

    /**
     * @brief Send a payload after sending the request protobuf
     * @note The option recv_payload_option must be set in the proto. Must be called after Write()
     * @param[in] payload Sending payload buffer.
     * @return Status of call.
     */
    Status SendAndTagPayload(const std::vector<MemView> &payload, bool tagPayloadFrame)
    {
        CHECK_FAIL_RETURN_STATUS(HasRecvPayloadOp(), StatusCode::K_INVALID,
                                 "recv_payload_option is not specified in the proto");
        // We can't use the SendAll because we will then send a duplicate clientUuid.
        size_t bufSz = 0;
        RETURN_IF_NOT_OK(ZmqPayload::AddPayloadFrames(payload, outMsg_, bufSz, tagPayloadFrame));
        VLOG(RPC_LOG_LEVEL) << FormatString(
            "Server uses unary socket to send %zu payload bytes to Service %s Method %d to client %s", bufSz,
            meta_.svc_name(), meta_.method_index(), meta_.client_id());
        if (enableMsgQ_) {
            return SendAll(ZmqSendFlags::NONE);
        }
        return Status::OK();
    }

    virtual Status SendPayload(const std::vector<MemView> &payload)
    {
        return SendAndTagPayload(payload, false);
    }

    /**
     * @brief The caller has read in the whole msg frames for us.
     * @details ReadAll is a no-op
     */
    Status ReadAll(ZmqRecvFlags flags) override
    {
        (void)flags;
        return Status::OK();
    }
    /**
     * @brief Read message into proto buffer.
     * @param[out] pb Buffer to read into.
     * @return Status of the call.
     */
    virtual Status Read(R &pb)
    {
        bool expected = false;
        if (readOnce_.compare_exchange_strong(expected, true)) {
            VLOG(RPC_LOG_LEVEL) << "Server uses unary socket reading";
            ZmqMessage protoMsg = std::move(inMsg_.front());
            inMsg_.pop_front();
            PerfPoint point(PerfKey::ZMQ_REQUEST_MSG_TO_PROTO);
            RETURN_IF_NOT_OK(ParseFromZmqMessage(protoMsg, pb));
            point.Record();
            PerfPoint::RecordElapsed(PerfKey::ZMQ_REQUEST_SIZE_BEFORE_DESERIALIZE, protoMsg.Size());
            g_SerializedMessage = std::move(protoMsg);
            g_ReqAk = meta_.access_key();
            g_ReqSignature = meta_.signature();
            g_ReqTimestamp = meta_.timestamp();
            return Status::OK();
        } else {
            RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, "ServerUnaryWriterReaderImpl is only supposed to be used once!");
        }
    }

    /**
     * @brief Receive a payload after receiving response protobuf.
     * @note The option send_payload_option must be set in the proto. Must be called after Read().
     * @param[out] payload receiving payload buffers.
     * @return Status of call.
     */
    Status ReceivePayload(std::vector<RpcMessage> &payload)
    {
        CHECK_FAIL_RETURN_STATUS(HasSendPayloadOp(), StatusCode::K_INVALID,
                                 "send_payload_option is not specified in the proto");
        std::unique_ptr<ZmqPayloadEntry> entry;
        RETURN_IF_NOT_OK(ZmqPayload::ProcessEmbeddedPayload(inMsg_, entry));
        payload = std::move(entry->recvBuf);
        VLOG(RPC_LOG_LEVEL) << FormatString("Server uses unary socket received %d payload bytes to client %s",
                                            entry->len, meta_.client_id());
        return Status::OK();
    }

    Status Finish() override
    {
        return { StatusCode::K_INVALID, "ServerUnaryWriterReaderImpl doesn't support Finish()!" };
    }

protected:
    std::shared_ptr<ZmqServerMsgQueRef> mQue_;

private:
    std::atomic<bool> writeOnce_;
    std::atomic<bool> readOnce_;
    bool enableMsgQ_;
    std::atomic<bool> requestComplete_;
    std::mutex requestCompleteMtx_;
    std::condition_variable requestCompleteCond_;
};
}  // namespace datasystem
#endif  // DATASYSTEM_COMMON_RPC_ZMQ_STREAM_SERVER_H
