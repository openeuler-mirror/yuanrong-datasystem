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

#include "datasystem/common/util/fd_pass.h"

#include <cstdint>
#include <functional>
#include <string>
#include <utility>
#include <vector>

#include <sys/socket.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>
#include <securec.h>

#include "datasystem/common/log/log.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"

namespace datasystem {
namespace {
Status InitCMsg(struct cmsghdr &cmsg, const std::vector<int> &fds)
{
    cmsg.cmsg_level = SOL_SOCKET;
    cmsg.cmsg_type = SCM_RIGHTS;
    cmsg.cmsg_len = CMSG_LEN(sizeof(int) * fds.size());

    int ret = memcpy_s(CMSG_DATA(&cmsg), cmsg.cmsg_len, &fds[0], sizeof(int) * fds.size());
    CHECK_FAIL_RETURN_STATUS(ret == EOK, StatusCode::K_RUNTIME_ERROR,
                             FormatString("Copy cmsg failed, the memcpy_s return: %d", ret));
    return Status::OK();
}

bool IsFdPassMsg(const struct cmsghdr &cmsg)
{
    return cmsg.cmsg_level == SOL_SOCKET && cmsg.cmsg_type == SCM_RIGHTS;
}

Status RunUtilOkOrReturnError(const std::function<ssize_t(int, struct msghdr *, int)> &func, int sock,
                              struct msghdr *msg, int flag, size_t &byteSize)
{
    byteSize = 0;
    while (true) {
        ssize_t ret = func(sock, msg, flag);
        if (ret > 0) {
            byteSize = static_cast<size_t>(ret);
            return Status::OK();
        }
        if (ret == 0) {
            RETURN_STATUS(StatusCode::K_UNKNOWN_ERROR, "Unexpected EOF read.");
        }
        // If ret < 0, and errno is 'EAGAIN', 'EINTR' or 'EWOULDBLOCK', it means we meets
        // system interrupt or buffer is full, these error encourage us try again.
        if (errno == EAGAIN || errno == EINTR || errno == EWOULDBLOCK) {
            continue;
        }
        // Unrecoverable error, just go die.
        RETURN_STATUS(StatusCode::K_UNKNOWN_ERROR, "Pass fd meets unexpected error: " + std::to_string(errno));
    }
}
}  // namespace

Status SockSendFd(int sock, const std::vector<int> &serverFds,
                  const std::vector<std::pair<void *, size_t>> &bufferVec = {})
{
    msghdr msg;
    auto ret = memset_s(&msg, sizeof(msghdr), 0, sizeof(msghdr));
    CHECK_FAIL_RETURN_STATUS(ret == EOK, StatusCode::K_RUNTIME_ERROR,
                             FormatString("memset_s failed, the memcpy_s return: %d", ret));

    std::vector<iovec> iovVec;
    if (!bufferVec.empty()) {
        iovVec.reserve(bufferVec.size());
        std::transform(bufferVec.begin(), bufferVec.end(), std::back_inserter(iovVec),
                       [](const auto &p) { return iovec{ p.first, p.second }; });
        msg.msg_iov = iovVec.data();
        msg.msg_iovlen = iovVec.size();
    }

    std::vector<char> cMsgBuf;
    if (!serverFds.empty()) {
        cMsgBuf.resize(CMSG_SPACE(sizeof(int) * serverFds.size()), 0);
        msg.msg_control = cMsgBuf.data();
        msg.msg_controllen = static_cast<socklen_t>(cMsgBuf.size());
        auto *cmptr = CMSG_FIRSTHDR(&msg);
        RETURN_RUNTIME_ERROR_IF_NULL(cmptr);
        RETURN_IF_NOT_OK(InitCMsg(*cmptr, serverFds));
    }

    size_t byteSize;
    RETURN_IF_NOT_OK(RunUtilOkOrReturnError(sendmsg, sock, &msg, 0, byteSize));
    return Status::OK();
}

// This function only supports a single iovec, so it is called a semi-finished product.
Status SemiSockRecvFd(int sock, std::vector<int> *clientFds, std::vector<char> *buffer)
{
    const size_t maxFdCount = 64;
    const size_t maxBufferSize = sizeof(int64_t);

    msghdr msg;
    auto ret = memset_s(&msg, sizeof(msghdr), 0, sizeof(msghdr));
    CHECK_FAIL_RETURN_STATUS(ret == EOK, StatusCode::K_RUNTIME_ERROR,
                             FormatString("memset_s failed, the memcpy_s return: %d", ret));

    std::vector<char> dataToRecv;
    iovec iov;
    if (buffer != nullptr) {
        dataToRecv.assign(maxBufferSize, 0);
        iov.iov_base = dataToRecv.data();
        iov.iov_len = maxBufferSize;
        msg.msg_iov = &iov;
        msg.msg_iovlen = 1;
    }

    std::vector<char> buf;
    if (clientFds != nullptr) {
        buf.resize(CMSG_SPACE(sizeof(int) * maxFdCount), 0);
        msg.msg_control = buf.data();
        msg.msg_controllen = static_cast<socklen_t>(buf.size());
    }

    size_t byteSize;
    RETURN_IF_NOT_OK(RunUtilOkOrReturnError(recvmsg, sock, &msg, 0, byteSize));

    if (buffer != nullptr) {
        dataToRecv.resize(byteSize);
        *buffer = std::move(dataToRecv);
    }

    std::vector<int> recvFdsFromCMsg;
    bool failed = false;
    for (auto *hdr = CMSG_FIRSTHDR(&msg); hdr != nullptr; hdr = CMSG_NXTHDR(&msg, hdr)) {
        if (!IsFdPassMsg(*hdr)) {
            continue;
        }
        size_t msgLen = hdr->cmsg_len - (CMSG_DATA(hdr) - reinterpret_cast<unsigned char *>(hdr));
        auto cnt = static_cast<ssize_t>(msgLen / sizeof(int));
        int *data = reinterpret_cast<int *>(CMSG_DATA(hdr));
        for (ssize_t i = 0; i < cnt; ++i) {
            if (data[i] < 0) {
                failed = true;
            }
            recvFdsFromCMsg.emplace_back(data[i]);
        }
    }

    if (failed) {
        for (auto fd : recvFdsFromCMsg) {
            if (fd >= 0) {
                RETRY_ON_EINTR(close(fd));
            }
        }
        RETURN_STATUS(StatusCode::K_UNKNOWN_ERROR, "We receive the invalid fd");
    }

    if (clientFds != nullptr) {
        *clientFds = std::move(recvFdsFromCMsg);
    }
    return Status::OK();
}

Status SockSendFd(int sock, bool isScmTcp, const std::vector<int> &serverFds, uint64_t requestId)
{
    LOG(INFO) << FormatString("Send fds[%s] using socket[%d] with requestId[%llu], isScmTcp[%d]",
                              VectorToString(serverFds), sock, requestId, isScmTcp);
    std::vector<std::pair<void *, size_t>> bufferVec;
    if (requestId > 0) {
        bufferVec.emplace_back(&requestId, sizeof(uint64_t));
    } else {
        static const char dummy = '0';
        bufferVec.emplace_back(const_cast<void *>(static_cast<const void *>(&dummy)), 1UL);
    }
    if (!isScmTcp) {
        return SockSendFd(sock, serverFds, bufferVec);
    }
    // SCMTCP not support send data and fds at the same time
    RETURN_IF_NOT_OK(SockSendFd(sock, {}, bufferVec));
    return SockSendFd(sock, serverFds);
}

Status SockRecvFd(int sock, bool isScmTcp, std::vector<int> &clientFds, uint64_t &requestId)
{
    std::vector<char> buffer;
    if (!isScmTcp) {
        RETURN_IF_NOT_OK(SemiSockRecvFd(sock, &clientFds, &buffer));
    } else {
        RETURN_IF_NOT_OK(SemiSockRecvFd(sock, nullptr, &buffer));
        RETURN_IF_NOT_OK(SemiSockRecvFd(sock, &clientFds, nullptr));
    }
    if (buffer.size() == sizeof(uint64_t)) {
        requestId = *reinterpret_cast<uint64_t *>(buffer.data());
    } else {
        requestId = 0;
    }
    return Status::OK();
}
}  // namespace datasystem
