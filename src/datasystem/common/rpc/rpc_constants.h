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
 * Description: RPC static configured parameters.
 */
#ifndef DATASYSTEM_COMMON_RPC_RPC_CONSTANTS_H
#define DATASYSTEM_COMMON_RPC_RPC_CONSTANTS_H

#include <stdint.h>
#include <sys/stat.h>

namespace datasystem {
static constexpr int RPC_TIMEOUT = 60 * 1000;          // Default 60 seconds RPC timeout.
static constexpr int RPC_MINIMUM_TIMEOUT = 500;        // The minimum time of RPC timeout interval.
static constexpr int RPC_BACKEND_TIMEOUT = 10 * 1000;  // 10 seconds backend timeout.
static constexpr int RPC_POLL_TIME = 100;              // 100 milliseconds polling time.
static constexpr int RPC_NUM_BACKEND = 32;             // Default number of backend threads.
static constexpr int RPC_HWM = 1000;                   // Default max watermark for outstanding msg.
static constexpr int RPC_HWM_QUEUE = 330;              // Default max watermark for outBoundMsg queue size.
static constexpr int RPC_LOG_LEVEL = 3;                // Normal output log level for Comm Framework.
static constexpr int RPC_KEY_LOG_LEVEL = 1;            // Init or terminate output log level for Rpc modules.
static constexpr int RPC_DEBUG_LOG_LEVEL = 4;          // deBug log level for RPC modules.
static constexpr int RPC_HEAVY_SERVICE_HWM = 2048;     // Maximum outstanding rpc requests for a busy rpc service.
static constexpr int RPC_LIGHT_SERVICE_HWM = 64;       // Maximum outstanding rpc requests for a light rpc service.
static constexpr int RPC_SOCKET_BACKLOG = 1024;        // Backlog for unix socket listen queue.
static constexpr int RPC_NO_FILE_FD = -1;              // Initial value of file descriptor.
static constexpr mode_t RPC_SOCK_MODE = 01660;         // Default unix socket permission mode.
static constexpr int ONE_THOUSAND = 1000;              // One thousand.
#ifdef WITH_TESTS
static constexpr int STUB_FRONTEND_TIMEOUT = 3000;     // Timeout for UnixSockFd and ZmqStub::InitFrontend in test.
#else
static constexpr int STUB_FRONTEND_TIMEOUT = 3000;     // Timeout for UnixSockFd and ZmqStub::InitFrontend.
#endif
static constexpr int CACHE_UDS_SOCK_FD_TIMEOUT_MS = RPC_TIMEOUT + 10'000;  // Timeout for caching UnixSockFd
static constexpr int RPC_EIGHT = 8;                    // constant 8
}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_RPC_RPC_CONSTANTS_H
