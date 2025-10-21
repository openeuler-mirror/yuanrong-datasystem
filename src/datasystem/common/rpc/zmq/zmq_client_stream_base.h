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
#ifndef DATASYSTEM_COMMON_RPC_ZMQ_STREAM_CLIENT_H
#define DATASYSTEM_COMMON_RPC_ZMQ_STREAM_CLIENT_H

#include "datasystem/common/rpc/rpc_client_stream_base_common.h"
#include "datasystem/common/rpc/rpc_unary_client_impl.h"
#include "datasystem/common/rpc/zmq/zmq_stream_base.h"
#include "datasystem/common/rpc/zmq/zmq_stub.h"

namespace datasystem {
/**
 * Base helper class for streaming rpc (client side).
 */
class ClientStreamBase : public StreamBase {
public:
    explicit ClientStreamBase(std::shared_ptr<ZmqMsgQueRef> mQue, const std::string &svcName, int32_t methodIndex,
                              std::string workerId, bool sendPayload, bool recvPayload);

    virtual ~ClientStreamBase() override;

    Status SendAll(ZmqSendFlags flags) override;
    Status ReadAll(ZmqRecvFlags flags) override;

    template <typename W>
    Status WritePb(const W &pb)
    {
        Status status = WritePbImpl(pb);
        return status;
    }

    /**
     * @brief Send a payload after sending the request protobuf.
     * @note The option send_payload_option must be set in the proto. Must be called after Write().
     * @param[in] payload Payload const buffers.
     * @return Status of call.
     */
    Status SendPayload(const std::vector<MemView> &payload);

    template <typename R>
    Status ReadPb(R &pb)
    {
        Status status = ReadPbImpl(pb);
        return status;
    }

    /**
     * @brief Receive a payload after receiving response protobuf.
     * @note The option recv_payload_option must be set in the proto. Must be called after Read()
     * @param[out] recvBuffer Received payloads.
     * @return Status of call.
     */
    Status ReceivePayload(std::vector<RpcMessage> &recvBuffer);

    /**
     * @brief Done sending sequence of protobuf to the server.
     * @return Status of call.
     */
    Status Finish() override;

protected:
    std::shared_ptr<ZmqMsgQueRef> mQue_;

private:
    template <typename W>
    Status WritePbImpl(const W &pb)
    {
        VLOG(RPC_LOG_LEVEL) << FormatString("Client %s stream writing to Service %s Method %d SeqNo %d",
                                            meta_.client_id(), meta_.svc_name(), meta_.method_index(), seqNo_);
        outMsg_.clear();
        // But we will tag a sequence number. -1 will mark the end to write stream.
        ZmqMessage seqMsg = ZmqInt64ToMessage(seqNo_++);
        outMsg_.push_back(std::move(seqMsg));
        SetMetaAuthInfo();
        RETURN_IF_NOT_OK(PushBackProtobufToFrames(pb, outMsg_));
        // If there is payload coming after, we can return.
        RETURN_OK_IF_TRUE(HasSendPayloadOp());
        return SendAll(ZmqSendFlags::NONE);
    }

    Status SendPayloadImpl(const std::vector<MemView> &payload);

    template <typename R>
    Status ReadPbImpl(R &pb)
    {
        VLOG(RPC_LOG_LEVEL) << "Client " << meta_.client_id() << " reading" << std::endl;
        RETURN_IF_NOT_OK(ReadAll(ZmqRecvFlags::NONE));
        ZmqMessage msg;
        RETURN_IF_NOT_OK(AckRequest(inMsg_, msg));
        RETURN_IF_NOT_OK(ParseFromZmqMessage(msg, pb));
        return Status::OK();
    }

    int64_t seqNo_;
};

/**
 * Client side streaming.
 * Server will send back the final protobuf.
 * @tparam W Stream RPC mode, WritePb type.
 */
template <typename W>
class ClientWriterImpl : public ClientStreamBase {
public:
    explicit ClientWriterImpl(std::shared_ptr<ZmqMsgQueRef> s, const std::string &svcName, int32_t methodIndex,
                              std::string workerId, bool sendPayload, bool recvPayload)
        : ClientStreamBase(std::move(s), svcName, methodIndex, workerId, sendPayload, recvPayload)
    {
    }
    ~ClientWriterImpl() override = default;

    Status Write(const W &pb)
    {
        return ClientStreamBase::WritePb(pb);
    }

    template <typename R>
    Status Read(R &pb)
    {
        return ClientStreamBase::ReadPb(pb);
    }
};

/**
 * Server side streaming.
 * Write disable.
 */
template <typename R>
class ClientReaderImpl : public ClientStreamBase {
public:
    explicit ClientReaderImpl(std::shared_ptr<ZmqMsgQueRef> s, const std::string &svcName, int32_t methodIndex,
                              std::string workerId, bool sendPayload, bool recvPayload)
        : ClientStreamBase(std::move(s), svcName, methodIndex, workerId, sendPayload, recvPayload)
    {
    }
    ~ClientReaderImpl() override = default;

    template <typename W>
    Status Write(const W &pb)
    {
        return ClientStreamBase::WritePb(pb);
    }

    /**
     * Synchronous read a protobuf.
     * @param[out] pb The RespPb.
     * @return Status of call.
     */
    Status Read(R &pb)
    {
        return ClientStreamBase::ReadPb(pb);
    }

    Status Finish() override
    {
        RETURN_STATUS(StatusCode::K_INVALID, "Invalid Finish operation for ClientReaderImpl");
    }
};

/**
 * Both side streaming.
 * @tparam W Stream RPC mode, WritePb type.
 * @tparam R Stream RPC mode, ReadPb type.
 */
template <typename W, typename R>
class ClientWriterReaderImpl : public ClientStreamBase {
public:
    explicit ClientWriterReaderImpl(std::shared_ptr<ZmqMsgQueRef> s, const std::string &svcName, int32_t methodIndex,
                                    std::string workerId, bool sendPayload, bool recvPayload)
        : ClientStreamBase(std::move(s), svcName, methodIndex, workerId, sendPayload, recvPayload)
    {
    }
    ~ClientWriterReaderImpl() override = default;

    Status Write(const W &pb)
    {
        PerfPoint point(PerfKey::ZMQ_STREAM_WRITE);
        return ClientStreamBase::WritePb(pb);
    }

    Status Read(R &pb)
    {
        PerfPoint point(PerfKey::ZMQ_STREAM_READ);
        return ClientStreamBase::ReadPb(pb);
    }
};
}  // namespace datasystem
#endif  // DATASYSTEM_COMMON_RPC_ZMQ_STREAM_CLIENT_H
