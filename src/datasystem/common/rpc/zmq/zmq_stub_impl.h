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
 * Description: Zmq Stub.
 */
#ifndef DATASYSTEM_COMMON_RPC_ZMQ_STUB_IMPL_H
#define DATASYSTEM_COMMON_RPC_ZMQ_STUB_IMPL_H

#include <atomic>
#include <cstring>
#include <map>
#include <memory>
#include <mutex>
#include <utility>
#include <vector>

#include "datasystem/common/perf/perf_manager.h"
#include "datasystem/common/rpc/rpc_channel.h"
#include "datasystem/common/rpc/zmq/rpc_service_method.h"
#include "datasystem/common/rpc/zmq/zmq_payload.h"
#include "datasystem/common/rpc/zmq/zmq_stub_conn.h"
#include "datasystem/common/rpc/zmq/zmq_constants.h"
#include "datasystem/common/util/wait_post.h"
#include "datasystem/protos/meta_zmq.pb.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/log/log_helper.h"
#include "datasystem/common/log/log.h"

namespace datasystem {

/**
 * @brief A stub provides methods to send/receive rpc service to the server.
 * The zmq plugin will generate a subclass to inherit from this class.
 * In general there is no need to call the methods in this class directly.
 */
class ZmqStubImpl {
public:
    explicit ZmqStubImpl(std::shared_ptr<RpcChannel> channel, int32_t timeoutMs = -1);

    virtual ~ZmqStubImpl();

    /**
     * @brief This method writes the request asynchronously and get a tagId.
     * @details This method sends an service request (as defined in the corresponding proto file)
     * to the remote server asynchronously. Optionally a payload can ride along the
     * ride. The payload will be sent directly without incurring the cost of memory
     * copy. In return, a tag id (int64) will be returned to the caller which can be
     * used to retrieve the service reply.
     * @tparam T ReqPb type.
     * @param[in] opt Zmq options.
     * @param[in] svcName Full service name (including the package).
     * @param[in] method Method object in the service definition.
     * @param[in] request Request to be sent to the server.
     * @param[in] payload Optional payload buffer.
     * @param[out] tagId Tag to be returned.
     * @return Status of call.
     */
    template <typename T>
    Status AsyncWriteImpl(const RpcOptions &opt, const std::string &svcName, std::shared_ptr<RpcServiceMethod> &method,
                          const T &request, const std::vector<MemView> &payload, int64_t &tagId)
    {
        std::shared_ptr<ZmqMsgQueRef> mQue;
        RETURN_IF_NOT_OK(CreateMsgQ(mQue, svcName, opt));
        const std::string clientId = mQue->GetId();
        VLOG(RPC_LOG_LEVEL) << "Client " << clientId << " requesting service " << svcName << " Method "
                            << method->MethodName() << std::endl;
        int64_t payloadIndex = method->HasPayloadSendOption() ? ZMQ_EMBEDDED_PAYLOAD_INX : ZMQ_INVALID_PAYLOAD_INX;
        // MetaPb.trace_id comes from Trace::GetTraceID() inside CreateMetaData; mint UUID if thread-local is empty.
        // If a trace already exists (Context::SetTraceId or nested RPC), SetTraceUUID is a no-op.
        MetaPb meta = CreateMetaData(svcName, method->MethodIndex(), payloadIndex, clientId);
        ZmqMsgFrames frames;
        // Set up the frames in order. Protobuf and then payload (if any).
        RETURN_IF_NOT_OK(PushBackProtobufToFrames(request, frames));
        // Send embedded payload after the meta.
        if (payloadIndex == ZMQ_EMBEDDED_PAYLOAD_INX) {
            size_t bufSz = 0;
            RETURN_IF_NOT_OK(ZmqPayload::AddPayloadFrames(payload, frames, bufSz, false));
            VLOG(RPC_LOG_LEVEL) << "Embedding " << bufSz << " payload bytes in method " << method->MethodName();
        }
        // Put the frames onto the outbound queue.
        auto p = std::make_pair(meta, std::move(frames));
        // Copy p.first before it is moved (SendMsg consumes p.first)
        MetaPb clientMeta = p.first;
        Status rc = mQue->SendMsg(p);
        if (rc.GetCode() == K_TRY_AGAIN && opt.GetTimeout() > 0) {
            rc = Status(StatusCode::K_RPC_CANCELLED, rc.GetMsg());
        }
        RETURN_IF_NOT_OK(rc);
        // Since this is an async call, we will need to save the socket for call back.
        // Store copy of p.first (with all client ticks) so AsyncRead can merge them into response MetaPb
        tagId = Insert(std::move(mQue), svcName, method->MethodIndex(), std::move(clientMeta));
        return Status::OK();
    }

    template <typename T>
    Status AsyncWrite(const RpcOptions &opt, const std::string &svcName, std::shared_ptr<RpcServiceMethod> &method,
                      const T &request, const std::vector<MemView> &payload, int64_t &tagId)
    {
        Status status = AsyncWriteImpl(opt, svcName, method, request, payload, tagId);
        return status;
    }

    /**
     * @brief This method reads the reply sent from an earlier asynchronous request.
     * @tparam T RespPb type.
     * @param[in] tagId Tag id returned by previous AsyncWrite.
     * @param[in] svcName Service name.
     * @param[in] method Method object in the service definition.
     * @param[out] reply Reply protobuf.
     * @param[out] recvBuffer Receive buffers.
     * @param[in] flags ZMQ flag. Either none or no wait. If no wait, TRY_AGAIN may return if reply not ready.
     * @return Status of call.
     */
    template <typename T>
    Status AsyncReadImpl(int64_t tagId, const std::string &svcName, std::shared_ptr<RpcServiceMethod> &method, T &reply,
                         std::vector<RpcMessage> &recvBuffer, ZmqRecvFlags flags = ZmqRecvFlags::NONE)
    {
        auto asyncCall = Get(tagId);
        if (asyncCall == nullptr) {
            return { StatusCode::K_INVALID, __LINE__, __FILE__, "Tag " + std::to_string(tagId) + " not found" };
        }
        CHECK_FAIL_RETURN_STATUS(strcmp(svcName.data(), asyncCall->svcName_.data()) == 0, K_RUNTIME_ERROR,
                                 "Tag doesn't match service name");
        CHECK_FAIL_RETURN_STATUS(method->MethodIndex() == asyncCall->methodIndex_, K_RUNTIME_ERROR,
                                 "Tag doesn't mach method");
        auto &mQue = asyncCall->mQue_;
        const std::string clientId = mQue->GetId();
        ZmqMetaMsgFrames rsp;
        // Check anything in the incoming queue.
        Status rc = mQue->ReceiveMsg(rsp, flags);
        if (rc.GetCode() == K_TRY_AGAIN) {
            // Do nothing if flags for non-blocking call.
            if (flags == ZmqRecvFlags::DONTWAIT) {
                return rc;
            }
            rc = Status(StatusCode::K_RPC_UNAVAILABLE, std::string("[RPC_RECV_TIMEOUT] ") + rc.GetMsg());
            // Drop the connection as we don't know the state of the server.
            LOG(WARNING) << "Rpc service for client " << clientId << " has not responded within the allowed time.";
            mQue->Close();
            Remove(tagId);
        }
        RETURN_IF_NOT_OK(rc);
        // Take out the tag from the map.
        Remove(tagId);
        ZmqMessage replyMsg;
        PerfPoint::RecordElapsed(PerfKey::ZMQ_STUB_FRONT_TO_BACK, GetLapTime(rsp.first, "ZMQ_STUB_FRONT_TO_BACK"));
        RecordTick(rsp.first, TICK_CLIENT_END);
        RecordRpcLatencyMetrics(rsp.first);
        rc = AckRequest(rsp.second, replyMsg);
        RETURN_IF_NOT_OK(rc);
        RETURN_IF_NOT_OK(ParseFromZmqMessage<T>(replyMsg, reply));
        VLOG(RPC_LOG_LEVEL) << "Client " << clientId << " received reply "
                            << "from Service " << svcName << " Method " << method->MethodName() << ", msg:\n"
                            << LogHelper::IgnoreSensitive(reply) << std::endl;
        // If there is a payload to follow.
        if (method->HasPayloadRecvOption()) {
            std::unique_ptr<ZmqPayloadEntry> entry;
            RETURN_IF_NOT_OK(ZmqPayload::ProcessEmbeddedPayload(rsp.second, entry));
            size_t bufSz = entry->len;
            recvBuffer = std::move(entry->recvBuf);
            VLOG(RPC_LOG_LEVEL) << "Client " << clientId << " received " << bufSz << " embedded payload from Service "
                                << svcName << " Method " << method->MethodName() << std::endl;
        }
        return Status::OK();
    }

    template <typename T>
    Status AsyncRead(int64_t tagId, const std::string &svcName, std::shared_ptr<RpcServiceMethod> &method, T &reply,
                     std::vector<RpcMessage> &recvBuffer, ZmqRecvFlags flags = ZmqRecvFlags::NONE)
    {
        Status status = AsyncReadImpl(tagId, svcName, method, reply, recvBuffer, flags);
        return status;
    }

    /**
     * @brief Forget a previous AsyncWrite request.
     * @details The underlying dealer socket will disconnect from backend and
     * free for reuse.
     * @param[in] tag Tag to identify a request.
     */
    void ForgetRequest(int64_t tag);

    /**
     * @brief Latency test used by internal benchmark tool.
     * @param[in] svcName service name.
     * @param[out] perfRun MetaPb structure to hold perf run data.
     * @param[in] payload Payload buffers.
     * @param[in] opts Zmq options.
     * @return Status of call.
     */
    Status PayloadTick(const std::string &svcName, MetaPb &perfRun, const std::vector<MemView> &payload,
                       const RpcOptions &opts = RpcOptions());

    /**
     * @brief Get the stream peer for stream rpc.
     * @param[in] svcName Rpc Service Name.
     * @param[in] methodIndex Rpc method index.
     * @param[in] opt Zmq options.
     * @param[out] mQue Zmq message queue reference.
     * @param[out] workerId Serving worker id.
     * @return Status of call, indicating whether A ZMQ socket already connected to the peer and a workerId that serves
     * this rpc.
     */
    Status GetStreamPeer(const std::string &svcName, int32_t methodIndex, const RpcOptions &opt,
                         std::shared_ptr<ZmqMsgQueRef> &mQue, std::string &workerId);

    /**
     * @brief Initialization. If requesting uds connection, the connection will be established asynchronously.
     * @param[in] stub Zmq stub.
     * @return Status of call.
     */
    Status InitConn(datasystem::ZmqStub *stub);

    /**
     * @brief Create a message queue for remote rpc.
     * @param[out] mQue Zmq message queue reference.
     * @param[in] serviceName Rpc Service Name.
     * @param[in] opts Zmq options.
     * @return Status of call.
     */
    Status CreateMsgQ(std::shared_ptr<ZmqMsgQueRef> &mQue, const std::string &serviceName,
                      const RpcOptions &opts = RpcOptions());

    /**
     * @brief Cleanup before destroying zmq stub impl.
     */
    void CleanUp();

    /**
     * @brief Check if the peer is alive.
     * @param[in] threshold The threshold time to determine if the peer is alive.
     * @return true if the peer lost contact for less than the threshold seconds.
     */
    bool IsPeerAlive(uint32_t threshold);

    /**
     * @brief Get Credential
     */
    RpcCredential GetCredential() const
    {
        return channel_->GetCredential();
    }

    void CacheSession(bool);

    Status GetInitStatus();

private:
    // Connection handle
    std::shared_ptr<ZmqBaseStubConn> conn_;
    std::shared_ptr<SockConnEntry> sockConn_;

    // A structure to store async events.
    class AsyncCallBack {
    public:
        AsyncCallBack(std::shared_ptr<ZmqMsgQueRef> mQue, std::string svcName, int32_t methodIndex,
                      MetaPb clientMeta);

        ~AsyncCallBack()
        {
            mQue_->Close();
        }

        const MetaPb &GetClientMeta() const { return clientMeta_; }

    private:
        friend class ZmqStubImpl;
        std::shared_ptr<ZmqMsgQueRef> mQue_;
        std::string svcName_;
        int32_t methodIndex_;
        MetaPb clientMeta_;
    };

    /**
     * @brief Search a previous AsyncWrite call from a given tag.
     * @param[in] tag Tag to identify a request.
     * @return AsyncCallBack object.
     */
    std::shared_ptr<AsyncCallBack> Get(int64_t tag) const;

    /**
     * @brief Remove a previous AsyncWrite call.
     * @param[in] tagId Tag to identify a request.
     */
    void Remove(int64_t tagId);

    /**
     * @brief Insert an AsyncWrite.
     * @param[in] mQue Zmq message queue reference.
     * @param[in] svcName Service name.
     * @param[in] methodIndex Method index.
     * @param[in] clientMeta Original client MetaPb with ticks.
     * @return tag Tag to identify a request.
     */
    int64_t Insert(std::shared_ptr<ZmqMsgQueRef> mQue, const std::string &svcName, int32_t methodIndex,
                   MetaPb clientMeta);

    std::map<int64_t, std::shared_ptr<AsyncCallBack>> asyncCallBack_;
    Status proxyRc_;
    std::shared_ptr<RpcChannel> channel_;
    std::atomic<int64_t> seqNo_;
    std::shared_ptr<StubInfo> handle_;
    mutable std::mutex mux_;
    std::string serviceName_;
    int32_t timeoutMs_;
};
}  // namespace datasystem
#endif  // DATASYSTEM_COMMON_RPC_ZMQ_STUB_IMPL_H
