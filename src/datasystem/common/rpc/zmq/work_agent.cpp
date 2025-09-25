/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
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

#include "datasystem/common/rpc/zmq/work_agent.h"
#include <string>
#include <iostream>

namespace datasystem {
WorkAgent::WorkAgent(const UnixSockFd &fd, ZmqService *svc, bool uds)
    : sockFd_(fd),
      uds_(uds),
      decoder_(std::make_unique<ZmqMsgDecoder>(&sockFd_)),
      encoder_(std::make_unique<ZmqMsgEncoder>(&sockFd_)),
      svc_(svc)
{
}

Status WorkAgent::ClientToService(ZmqMetaMsgFrames &p)
{
    // *** Protocol FRAME 0 ***
    // First, get the request header
    // We can use V2 protocol to receive. It is compatible with V1
    ZmqMsgFrames frames;
    auto rc = decoder_->ReceiveMsgFramesV2(frames);
    if (rc.IsError()) {
        LOG(ERROR) << FormatString("Error detected in decoder on fd %d. %s", sockFd_.GetFd(), rc.ToString());
        interrupted_.store(true, std::memory_order_release);
        return rc;
    }

    VLOG(RPC_LOG_LEVEL) << "# of frames received " << frames.size();
    // MetaPb is embedded in the incoming socket connection. For now, pass a fake one.
    p.first = MetaPb();
    p.second = std::move(frames);
    return Status::OK();
}

Status WorkAgent::ServiceToClient(ZmqMetaMsgFrames &p)
{
    MetaPb &meta = p.first;
    CHECK_FAIL_RETURN_STATUS(meta.ticks_size() > 0, K_RUNTIME_ERROR,
                             FormatString("Incomplete MetaPb:\n%s", meta.DebugString()));
    PerfPoint::RecordElapsed(PerfKey::ZMQ_APP_WORKLOAD, GetLapTime(meta, "ZMQ_APP_WORKLOAD"));
    ZmqMsgFrames &frames = p.second;
    TraceGuard traceGuard = Trace::Instance().SetTraceNewID(meta.trace_id());
    // No need to prepend the gateway if it is direct connection
    RETURN_IF_NOT_OK(PushFrontProtobufToFrames(meta, frames));
    RETURN_IF_NOT_OK(PushFrontStringToFrames(meta.client_id(), frames));
    // We need to match the client protocol to be downward compatible
    auto rc = encoder_->SendMsgFrames(static_cast<EventType>(meta.event_type()), frames);
    if (rc.IsError()) {
        LOG(ERROR) << FormatString("Error detected in encoder on fd %d. %s", sockFd_.GetFd(), rc.ToString());
        interrupted_.store(true, std::memory_order_release);
        return rc;
    }
    return Status::OK();
}

Status WorkAgent::DoWork()
{
    VLOG(RPC_LOG_LEVEL) << "Work Agent Doing work";
    ZmqMetaMsgFrames inMsg;
    ZmqMetaMsgFrames outMsg;
    // receive msg from client
    RETURN_IF_NOT_OK(ClientToService(inMsg));
    // execute internal method and get the reply
    EventType type = uds_ ? EventType::V1MTP : (decoder_->V2Client() ? EventType::V2MTP : EventType::V1MTP);
    RETURN_IF_NOT_OK(svc_->DirectExecInternalMethod(sockFd_.GetFd(), type, inMsg, outMsg));
    // send reply back to client
    RETURN_IF_NOT_OK(ServiceToClient(outMsg));
    return Status::OK();
}

Status WorkAgent::Run()
{
    CHECK_FAIL_RETURN_STATUS(!interrupted_.load(std::memory_order_acquire), K_RUNTIME_ERROR,
                             FormatString("Wrong state in work agent. Interrupted flag should be false."));
    while (!interrupted_.load(std::memory_order_acquire)) {
        DoWork();
    }
    // Once the interrupt condition is triggered, close the socket.
    CloseSocket();
    return Status::OK();
}

Status WorkAgent::CloseSocket()
{
    VLOG(RPC_LOG_LEVEL) << "WorkAgent shuts down and closes socket fd " << sockFd_.GetFd();
    sockFd_.Close();
    return Status::OK();
}

Status WorkAgent::Stop()
{
    interrupted_.store(true, std::memory_order_release);
    return Status::OK();
}

}  // namespace datasystem
