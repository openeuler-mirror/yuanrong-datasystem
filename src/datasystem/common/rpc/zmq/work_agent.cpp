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

#include "datasystem/common/perf/perf_manager.h"

// This macro is for use in the void Run() function. Since Run() cannot return an error, it logs
// the error, closes the socket, and then breaks out of any code by returning.
// Since Run() is the main thread loop, this results in thread exit after it returns.
#define LOG_AND_QUIT_IF_ERROR(statement_)                                                             \
    do {                                                                                              \
        Status rc_ = (statement_);                                                                    \
        if (rc_.IsError()) {                                                                          \
            if (rc_.GetCode() != K_RPC_CANCELLED) {                                                   \
                LOG(ERROR) << "Exclusive connection work agent encounters error: " << rc_.ToString(); \
            }                                                                                         \
            CloseSocket();                                                                            \
            return;                                                                                   \
        }                                                                                             \
    } while (false)

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
    RETURN_IF_NOT_OK(decoder_->ReceiveMsgFramesV2(frames));
    VLOG(RPC_LOG_LEVEL) << "# of frames received " << frames.size();

    // Parse the message, populate the meta from the frames into p.first
    ZmqCurveUserId userId;
    EventType type = uds_ ? EventType::V1MTP : (decoder_->V2Client() ? EventType::V2MTP : EventType::V1MTP);
    RETURN_IF_NOT_OK(
        ParseMsgFrames(frames, p.first, sockFd_.GetFd(), type, userId, PerfKey::ZMQ_NETWORK_TRANSFER_SERVER_UDS));
    RecordTick(p.first, TICK_SERVER_EXEC_START);
    // The meta has been extracted into p.first. Remaining frames will be moved into p.second
    p.second = std::move(frames);
    return Status::OK();
}

Status WorkAgent::ServiceToClient(ZmqMetaMsgFrames &p)
{
    MetaPb &meta = p.first;
    CHECK_FAIL_RETURN_STATUS(meta.ticks_size() > 0, K_RUNTIME_ERROR,
                             FormatString("Incomplete MetaPb:\n%s", meta.DebugString()));
    PerfPoint::RecordElapsed(PerfKey::ZMQ_APP_WORKLOAD, GetLapTime(meta, "ZMQ_APP_WORKLOAD"));
    RecordTick(meta, TICK_SERVER_EXEC_END);
    RecordTick(meta, TICK_SERVER_SEND);
    RecordServerLatencyMetrics(meta);
    ZmqMsgFrames &frames = p.second;
    TraceGuard traceGuard = SetTraceContextFromMeta(meta);
    // No need to prepend the gateway if it is direct connection
    RETURN_IF_NOT_OK(PushFrontProtobufToFrames(meta, frames));
    RETURN_IF_NOT_OK(PushFrontStringToFrames(meta.client_id(), frames));
    // We need to match the client protocol to be downward compatible
    RETURN_IF_NOT_OK(encoder_->SendMsgFrames(static_cast<EventType>(meta.event_type()), frames));
    return Status::OK();
}

void WorkAgent::Run(const ThreadPool::ThreadPoolUsage &poolUsage)
{
    if (interrupted_.load(std::memory_order_acquire)) {
        LOG(ERROR) << "Wrong state in work agent. Interrupted flag should be false.";
        CloseSocket();
        return;
    }

    ZmqMetaMsgFrames inMsg;
    LOG_AND_QUIT_IF_ERROR(ClientToService(inMsg));

    // The stat was captured before "we" were added to the pool, so +1 the active agents. This is just fyi,
    // so we don't need to be 100% accurate here (timing issues)
    LOG(INFO) << FormatString("New work agent for exclusive connection %s. sock_fd: %s. Num active agents: %d",
                              inMsg.first.gateway_id(), this->GetFd(),
                              poolUsage.runningTasksNum + poolUsage.waitingTaskNum + 1);

    // main run loop of the work agent
    while (!interrupted_.load(std::memory_order_acquire)) {
        ZmqMetaMsgFrames outMsg;
        // Call the request handler now via DirectExecInternalMethod().
        // If the called logic has an error that needs to be sent back to the client, DirectExecInternalMethod()
        // returns OK and the error message is embedded into the outMsg. The subsequent call to ServiceToClient() will
        // flow that error message to the client and then this work agent remains active (there is nothing wrong with
        // the connection).
        // Any other errors will break from this loop and the work agent will quit, terminating the connection.
        LOG_AND_QUIT_IF_ERROR(svc_->DirectExecInternalMethod(inMsg, outMsg));
        LOG_AND_QUIT_IF_ERROR(ServiceToClient(outMsg));

        // Reset the inMsg pair so that it can be re-used for the next request. Then read in the next request.
        MetaPb meta;
        inMsg.first = meta;
        inMsg.second.clear();
        LOG_AND_QUIT_IF_ERROR(ClientToService(inMsg));
    }
    // Once the interrupt condition is triggered, close the socket.
    CloseSocket();
}

Status WorkAgent::CloseSocket()
{
    LOG(INFO) << "WorkAgent shuts down and closes socket fd " << sockFd_.GetFd();
    sockFd_.Close();
    return Status::OK();
}

Status WorkAgent::Stop()
{
    interrupted_.store(true, std::memory_order_release);
    return Status::OK();
}

int WorkAgent::GetFd() const
{
    return sockFd_.GetFd();
}

}  // namespace datasystem
