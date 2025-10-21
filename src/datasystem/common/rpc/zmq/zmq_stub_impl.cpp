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
 * Description: Zmq Stub Impl.
 */
#include "datasystem/common/rpc/zmq/zmq_stub_impl.h"

#include <sys/types.h>

#include <utility>

#include "datasystem/common/perf/perf_manager.h"
#include "datasystem/common/rpc/rpc_channel.h"
#include "datasystem/common/rpc/zmq/zmq_stub.h"
#include "datasystem/common/util/gflag/common_gflags.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/protos/meta_zmq.pb.h"

namespace datasystem {
ZmqStubImpl::AsyncCallBack::AsyncCallBack(std::shared_ptr<ZmqMsgQueRef> mQue, std::string svcName, int32_t methodIndex)
    : mQue_(std::move(mQue)), svcName_(std::move(svcName)), methodIndex_(methodIndex)
{
}

std::shared_ptr<ZmqStubImpl::AsyncCallBack> ZmqStubImpl::Get(int64_t tag) const
{
    std::lock_guard<std::mutex> lock(mux_);
    auto it = asyncCallBack_.find(tag);
    if (it != asyncCallBack_.end()) {
        return it->second;
    } else {
        return nullptr;
    }
}

void ZmqStubImpl::Remove(int64_t tagId)
{
    std::lock_guard<std::mutex> lock(mux_);
    if (!asyncCallBack_.erase(tagId)) {
        LOG(ERROR) << FormatString("Remove failed, tagId [%d] may not in this map", tagId);
    }
}

int64_t ZmqStubImpl::Insert(std::shared_ptr<ZmqMsgQueRef> mQue, const std::string &svcName, int32_t methodIndex)
{
    auto async_call = std::make_shared<AsyncCallBack>(std::move(mQue), svcName, methodIndex);
    auto id = seqNo_.fetch_add(1);
    std::lock_guard<std::mutex> lock(mux_);
    asyncCallBack_.emplace(id, std::move(async_call));
    return id;
}

Status AckRequest(ZmqMsgFrames &frames, ZmqMessage &reply)
{
    CHECK_FAIL_RETURN_STATUS(!frames.empty(), K_RUNTIME_ERROR, "Empty frames");
    /**
     * The first message is always the Status object.
     */
    ZmqMessage first = std::move(frames.front());
    frames.pop_front();

    /**
     * We can do an early exit if there is any error. Server will
     * not send any message body.
     */
    RETURN_IF_NOT_OK(ZmqMessageToStatus(first));

    if (!frames.empty()) {
        reply = std::move(frames.front());
        frames.pop_front();
    }
    return Status::OK();
}

void ZmqStubImpl::ForgetRequest(int64_t tag)
{
    Remove(tag);
}

Status ZmqStubImpl::GetStreamPeer(const std::string &svcName, int32_t methodIndex, const RpcOptions &opts,
                                  std::shared_ptr<ZmqMsgQueRef> &mQue, std::string &workerId)
{
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(proxyRc_, FormatString("Please recreate stub for service %s channel %s. "
                                                            "Init error", svcName, channel_->GetZmqEndPoint()));
    PerfPoint point(PerfKey::ZMQ_GET_STREAM_PEER);
    // We are going to repeatedly to send internal request in retryTimeout (60s by default)
    // but each rpc is very short about just 1200ms.
    int retryTimeout = opts.GetTimeout();
    auto func = [this, &svcName, &methodIndex](std::shared_ptr<ZmqMsgQueRef> &sock, const RpcOptions &opts,
                                               ZmqMessage &reply) {
        RETURN_IF_NOT_OK(CreateMsgQ(sock, svcName, opts));
        // We do not need to start the clock for this inner MetaPb.
        MetaPb rq = CreateMetaData(svcName, methodIndex, ZMQ_INVALID_PAYLOAD_INX, sock->GetId());
        sock->UpdateOpts(opts);
        VLOG(RPC_LOG_LEVEL) << FormatString("Client %s requesting GetStreamPeer method", sock->GetId());
        ZmqMsgFrames frames;
        MetaPb meta = CreateMetaData(svcName, ZMQ_STREAM_WORKER_METHOD, ZMQ_INVALID_PAYLOAD_INX, sock->GetId());
        RETURN_IF_NOT_OK(PushBackProtobufToFrames(rq, frames));
        ZmqMetaMsgFrames p({ meta, std::move(frames) });
        RETURN_IF_NOT_OK(sock->SendMsg(p));
        ZmqMetaMsgFrames rsp;
        RETURN_IF_NOT_OK(sock->ReceiveMsg(rsp));
        PerfPoint::RecordElapsed(PerfKey::ZMQ_STUB_FRONT_TO_BACK, GetLapTime(rsp.first, "ZMQ_STUB_FRONT_TO_BACK"));
        RETURN_IF_NOT_OK(AckRequest(rsp.second, reply));
        return Status::OK();
    };
    Status rc;
    std::shared_ptr<ZmqMsgQueRef> sock;
    ZmqMessage reply;
    RpcOptions options(opts);
    options.SetTimeout(STUB_INTERNAL_TIMEOUT);
    auto startTick = std::chrono::steady_clock::now();
    const uint32_t sleepTime = 100;
    bool doRetry = false;
    do {
        rc = func(sock, options, reply);
        auto endTick = std::chrono::steady_clock::now();
        auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(endTick - startTick).count();
        if (ms >= retryTimeout) {
            break;
        }
        // We must re-create a new msgQ in case of timeout or any kind of error.
        // Reusing the same msgQ may receive a reply from previous rpc.
        if (rc.IsError()) {
            sock.reset();
        }
        doRetry = (rc.GetCode() == StatusCode::K_RPC_UNAVAILABLE || rc.GetCode() == StatusCode::K_TRY_AGAIN);
        if (doRetry) {
            // Retry after 100ms so it does not recreate too many ZmqFrontend on K_TRY_AGAIN
            std::this_thread::sleep_for(std::chrono::milliseconds(sleepTime));
        }
    } while (doRetry);
    RETURN_IF_NOT_OK(rc);
    workerId = ZmqMessageToString(reply);
    VLOG(RPC_LOG_LEVEL) << "Serving worker: " << workerId;
    // InvokeInternalSvc will change timeout, reset it.
    sock->UpdateOpts(opts);
    mQue = std::move(sock);
    return Status::OK();
}

Status ZmqStubImpl::InitConn(datasystem::ZmqStub *stub)
{
    serviceName_ = stub->ServiceName();
    VLOG(RPC_LOG_LEVEL) << FormatString("InitConn for service %s channel %s starts", serviceName_,
                                        channel_->GetZmqEndPoint());
    Timer timer;
    auto initFunc = [this, &stub]() -> Status {
        RETURN_IF_NOT_OK(ZmqStubConnMgr::Instance()->GetConn(stub, handle_, channel_, timeoutMs_, conn_, sockConn_));
        return Status::OK();
    };
    proxyRc_ = initFunc();
    if (proxyRc_.IsOk()) {
        VLOG(RPC_LOG_LEVEL) << FormatString("InitConn for service %s channel %s Elapsed: [%.6lf]s", serviceName_,
                                            channel_->GetZmqEndPoint(), timer.ElapsedSecond());
    } else {
        LOG(WARNING) << FormatString("InitConn for service %s channel %s unsuccessful. rc %s", serviceName_,
                                     channel_->GetZmqEndPoint(), proxyRc_.ToString());
    }
    return proxyRc_;
}

ZmqStubImpl::ZmqStubImpl(std::shared_ptr<RpcChannel> channel, int32_t timeoutMs)
    : channel_(std::move(channel)), seqNo_(0), handle_(nullptr), timeoutMs_(timeoutMs)
{
}

ZmqStubImpl::~ZmqStubImpl() = default;

void ZmqStubImpl::CleanUp()
{
    Timer timer;
    // Clear the map.
    {
        std::lock_guard<std::mutex> lock(mux_);
        asyncCallBack_.clear();
    }
    // Last step is unregister, ZmqStubConnMgrImpl handler thread will periodically unregister stubs.
    handle_.reset();

    VLOG(RPC_LOG_LEVEL) << FormatString("~ZmqStub %s Elapsed: [%.6lf]s", serviceName_, timer.ElapsedSecond());
}

Status ZmqStubImpl::PayloadTick(const std::string &svcName, MetaPb &perfRun, const std::vector<MemView> &payload,
                                const RpcOptions &opts)
{
    std::shared_ptr<ZmqMsgQueRef> mQue;
    RETURN_IF_NOT_OK(CreateMsgQ(mQue, svcName, opts));
    MetaPb meta = CreateMetaData(svcName, ZMQ_PAYLOAD_TICK_METHOD, ZMQ_INVALID_PAYLOAD_INX, mQue->GetId());
    auto frames = ZmqMsgFrames();
    size_t bufSz = 0;
    ZmqPayload::AddPayloadFrames(payload, frames, bufSz, false);
    ZmqMetaMsgFrames p(meta, std::move(frames));
    RETURN_IF_NOT_OK(mQue->SendMsg(p));
    ZmqMetaMsgFrames reply;
    RETURN_IF_NOT_OK(mQue->ReceiveMsg(reply));
    PerfPoint::RecordElapsed(PerfKey::ZMQ_STUB_FRONT_TO_BACK, GetLapTime(reply.first, "ZMQ_STUB_FRONT_TO_BACK"));
    ZmqMessage replyMsg;
    RETURN_IF_NOT_OK(AckRequest(reply.second, replyMsg));
    perfRun = reply.first;
    return Status::OK();
}

Status ZmqStubImpl::CreateMsgQ(std::shared_ptr<ZmqMsgQueRef> &mQue, const std::string &serviceName,
                               const RpcOptions &opts)
{
    ZmqMsgQueRef ref;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
        proxyRc_, FormatString("Please recreate stub for service %s channel %s. Init error", serviceName,
                               channel_->GetZmqEndPoint()));
    RETURN_IF_NOT_OK(conn_->WaitForConnect(handle_, std::min<int64_t>(opts.GetTimeout(), STUB_FRONTEND_TIMEOUT)));
    RETURN_IF_NOT_OK(conn_->CreateMsgQ(handle_, ref, opts));
    mQue = std::make_shared<ZmqMsgQueRef>(std::move(ref));
    return Status::OK();
}

bool ZmqStubImpl::IsPeerAlive(uint32_t threshold)
{
    return conn_->IsPeerAlive(threshold);
}

void ZmqStubImpl::CacheSession(bool cache)
{
    conn_->CacheSession(cache);
}

Status ZmqStubImpl::GetInitStatus()
{
    return proxyRc_;
}
}  // namespace datasystem
