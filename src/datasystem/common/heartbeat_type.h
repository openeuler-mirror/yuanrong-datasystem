/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
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
 * Description: Shared HeartbeatType enum referenced by both client and worker to keep the two
 * historical definitions (datasystem:: and datasystem::worker::) in sync at compile time.
 */
#ifndef DATASYSTEM_COMMON_HEARTBEAT_TYPE_H
#define DATASYSTEM_COMMON_HEARTBEAT_TYPE_H

namespace datasystem {
/**
 * @brief How the worker detects a client's liveness. SOCKET_HEARTBEAT monitors the fd-passing
 * socketFd via epoll (millisecond-grade EOF/RST detection); RPC_HEARTBEAT relies on RPC heartbeat
 * timeouts; NO_HEARTBEAT disables detection. See shm-and-heartbeat design §4.3.1.
 */
enum class HeartbeatType { NO_HEARTBEAT = 0, RPC_HEARTBEAT = 1, SOCKET_HEARTBEAT = 2 };
}  // namespace datasystem
#endif  // DATASYSTEM_COMMON_HEARTBEAT_TYPE_H
