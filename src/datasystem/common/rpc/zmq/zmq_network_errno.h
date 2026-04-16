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
 * Description: Classify errno for ZMQ socket I/O (e.g. zmq_network_error_total). Single source of truth
 * for production code and unit tests — keep the predicate list in this header only.
 */
#ifndef DATASYSTEM_COMMON_RPC_ZMQ_NETWORK_ERRNO_H
#define DATASYSTEM_COMMON_RPC_ZMQ_NETWORK_ERRNO_H

#include <cerrno>

namespace datasystem {

// True if errno is treated as a network-class failure for ZMQ metrics (see zmq_socket_ref.cpp).
inline bool IsZmqSocketNetworkErrno(int e)
{
    return e == ECONNREFUSED || e == ECONNRESET || e == ECONNABORTED || e == EHOSTUNREACH || e == ENETUNREACH ||
           e == ENETDOWN || e == ETIMEDOUT || e == EPIPE || e == ENOTCONN;
}

}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_RPC_ZMQ_NETWORK_ERRNO_H
