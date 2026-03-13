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
 * Description: Pass file descriptor by unix domain socket.
 */

#ifndef DATASYSTEM_COMMON_UTIL_FD_PASS_H
#define DATASYSTEM_COMMON_UTIL_FD_PASS_H

#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {
/**
 * @brief Send file fd via unix domain socket.
 * @param[in] sock Unix domain socket fd.
 * @param[in] isScmTcp Whether to use SCMTCP for passing file descriptors.
 * @param[in] serverFds File fds.
 * @param[in] requestId The unique id of this request.
 * @return Status of the call.
 */
Status SockSendFd(int sock, bool isScmTcp, const std::vector<int> &serverFds, uint64_t requestId);

/**
 * @brief Receive file fd via unix domain socket, this
 *        function would block if fd has not been send.
 * @param[in] sock Unix domain socket fd.
 * @param[in] isScmTcp Whether to use SCMTCP for passing file descriptors.
 * @param[out] fds File fds.
 * @param[out] requestId The unique id of this request.
 * @return Status of the call.
 */
Status SockRecvFd(int sock, bool isScmTcp, std::vector<int> &clientFds, uint64_t &requestId);
}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_UTIL_FD_PASS_H
