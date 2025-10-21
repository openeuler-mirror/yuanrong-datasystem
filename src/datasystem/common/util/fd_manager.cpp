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
 * Description: Add and close socket fd.
 */
#include "datasystem/common/util/fd_manager.h"

#include <unistd.h>
#include <sys/socket.h>
#include <thread>

#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/log/log.h"

namespace datasystem {
std::once_flag FdManager::init_;
std::unique_ptr<FdManager> FdManager::instance_ = nullptr;

FdManager *FdManager::Instance()
{
    std::call_once(init_, []() { instance_ = std::make_unique<FdManager>(Token()); });
    return instance_.get();
}

FdManager::FdManager(Token t)
{
    (void)t;
}

FdManager::~FdManager() = default;

Status FdManager::AddFd(int socketFd)
{
    std::lock_guard<std::mutex> lck(mutex_);
    if (socketFd < 0) {
        LOG(ERROR) << "socket fd less than 0";
        RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, "Socket fd less than 0");
    }
    if (fds_.count(socketFd) != 0) {
        LOG(ERROR) << FormatString("socket fd exists: %d", socketFd);
        RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, "Socket fd exists");
    }
    fds_.insert(socketFd);
    return Status::OK();
}

Status FdManager::CloseFd(int socketFd)
{
    std::lock_guard<std::mutex> lck(mutex_);
    if (socketFd < 0) {
        LOG(ERROR) << "socket fd less than 0";
        RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, "Socket fd less than 0");
    }
    if (fds_.count(socketFd) == 0) {
        LOG(ERROR) << "socket fd not exist " << socketFd;
        RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, "Socket fd not exist");
    }
    int ret = close(socketFd);
    fds_.erase(socketFd);
    if (ret == -1) {
        LOG(ERROR) << "Close fd failed, fd = " << socketFd << " errno = " << errno;
        RETURN_STATUS(StatusCode::K_RUNTIME_ERROR,
                      FormatString("Close fd [fd = %d] failed. Error no: [%s]", socketFd, StrErr(errno)));
    }
    return Status::OK();
}

Status FdManager::CloseAllFds()
{
    std::lock_guard<std::mutex> lck(mutex_);
    bool success = true;
    for (int socketFd : fds_) {
        int ret = close(socketFd);
        if (ret == -1) {
            std::string msg = FormatString("Close fd [fd = %d] failed. Error no: [%s]", socketFd, StrErr(errno));
            LOG(ERROR) << msg;
            success = false;
        }
    }
    fds_.clear();
    if (!success) {
        RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, "Close fds failed.");
    }
    return Status::OK();
}

void FdManager::ShutdownAndTrack(int fd)
{
    std::lock_guard<std::mutex> lck(mutex_);
    int ret = shutdown(fd, SHUT_RDWR);
    LOG(INFO) << FormatString("shutdown socket fd:%d, ret: %d ", fd, ret);
    RETRY_ON_EINTR(close(fd));
    // Track the last time it was shutdown.
    reuse_[fd] = std::chrono::steady_clock::now().time_since_epoch().count();
}

Status FdManager::WaitForReuse(int fd, int threshold, int sleepMS)
{
    std::unique_lock<std::mutex> lck(mutex_);
    auto it = reuse_.find(fd);
    RETURN_OK_IF_TRUE(it == reuse_.end());

    auto now = std::chrono::steady_clock::now().time_since_epoch().count();
    // Convert nano seconds to milli seconds.
    constexpr static int NANO_TO_MS = 1'000'000;
    int duration = (now - it->second) / NANO_TO_MS;
    if (threshold > duration) {
        // Let go of the lock before we go to sleep
        lck.unlock();
        auto diff = std::min<int>(threshold - duration, sleepMS);
        LOG(INFO) << FormatString("Socket fd %d has just been shutdown recently. Sleep %d ms before using it", fd,
                                  diff);
        std::this_thread::sleep_for(std::chrono::milliseconds(diff));
    }
    return Status::OK();
}
}  // namespace datasystem
