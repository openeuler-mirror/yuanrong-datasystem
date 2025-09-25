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

/**
 * Description: Zmq server work agent for exclusive connections.
 */
#ifndef DATASYSTEM_WORKER_WORK_AGENT_H
#define DATASYSTEM_WORKER_WORK_AGENT_H

#include <thread>
#include <future>
#include <atomic>
#include <mutex>
#include "datasystem/common/rpc/unix_sock_fd.h"
#include "datasystem/common/rpc/zmq/zmq_msg_decoder.h"
#include "datasystem/common/rpc/zmq/zmq_service.h"

namespace datasystem {
class ZmqService;

class WorkAgent {
public:
    WorkAgent(const UnixSockFd &fd, ZmqService *svc, bool uds);
    ~WorkAgent() = default;
    Status Run();
    Status DoWork();
    Status Stop();
    Status CloseSocket();

private:
    friend class ZmqService;
    Status ClientToService(ZmqMetaMsgFrames &p);
    Status ServiceToClient(ZmqMetaMsgFrames &p);
    UnixSockFd sockFd_;
    bool uds_;
    std::atomic<bool> interrupted_{ false };
    std::thread::id workerId_;

    std::unique_ptr<ZmqMsgDecoder> decoder_;
    std::unique_ptr<ZmqMsgEncoder> encoder_;
    ZmqService *svc_;
};

}  // namespace datasystem
#endif  // DATASYSTEM_WORKER_WORK_AGENT_H
