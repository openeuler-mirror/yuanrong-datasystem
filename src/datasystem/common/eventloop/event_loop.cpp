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
 * Description: EventLoop implementation using epoll api.
 */
#include "datasystem/common/eventloop/event_loop.h"

#include <string>
#include <thread>

#include <unistd.h>

#include "datasystem/common/util/fd_manager.h"
#include "datasystem/common/util/strings_util.h"

namespace datasystem {
EventLoop::~EventLoop()
{
    Finish();
}

Status EventLoop::Init()
{
    efd_ = epoll_create1(0);
    if (efd_ == -1) {
        RETURN_STATUS_LOG_ERROR(K_RUNTIME_ERROR, FormatString("epoll_create1 failed:%d", errno));
    }
    // create eventfd
    stopFd_ = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
    if (stopFd_ == -1) {
        RETRY_ON_EINTR(close(efd_));
        RETURN_STATUS_LOG_ERROR(K_RUNTIME_ERROR, FormatString("eventfd failed:%d", errno));
    }
    auto status = AddFdEvent(stopFd_, EPOLLIN | EPOLLHUP | EPOLLERR, nullptr, nullptr);
    if (status.IsError()) {
        RETRY_ON_EINTR(close(efd_));
        RETRY_ON_EINTR(close(stopFd_));
        LOG(ERROR) << "Failed to init event loop: " << status.ToString();
        return status;
    }
    loopThread_ = Thread(&EventLoop::Run, this);
    return Status::OK();
}

void EventLoop::Finish()
{
    if (stopLoop_) {
        return;
    }
    stopLoop_ = true;
    uint64_t one = 1;
    if (stopFd_ != -1 && write(stopFd_, &one, sizeof(one)) != sizeof(one)) {
        LOG(ERROR) << "StopEventLoop failed";
    }
    if (loopThread_.joinable()) {
        loopThread_.join();
    }
    if (stopFd_ != -1) {
        RETRY_ON_EINTR(close(stopFd_));
        stopFd_ = -1;
    }
    if (efd_ != -1) {
        RETRY_ON_EINTR(close(efd_));
        efd_ = -1;
    }
}

void EventLoop::Run()
{
    while (!stopLoop_) {
        int nevent = epoll_wait(efd_, eventList_.data(), EPOLL_EVENTS_SIZE, -1);
        if (nevent > 0) {
            HandleEvent(eventList_.data(), nevent);
            continue;
        }
        if (errno != EINTR) {
            LOG(ERROR) << "epoll_wait failed:" << errno;
        }
    }
}

Status EventLoop::AddFdEvent(int fd, uint32_t tEvents, std::function<void()> readCallBack,
                             std::function<void()> writeCallBack)
{
    std::lock_guard<std::mutex> lock(eventsLock_);
    eventMap_[fd] = std::make_shared<EventData>(fd, tEvents, std::move(readCallBack), std::move(writeCallBack));
    auto status = UpdateFdEventUnlock(EPOLL_CTL_ADD, fd, tEvents);
    if (status.IsError()) {
        (void)eventMap_.erase(fd);
        return status;
    }
    return Status::OK();
}

Status EventLoop::DelFdEvent(int fd)
{
    std::lock_guard<std::mutex> lock(eventsLock_);
    RETURN_IF_NOT_OK(UpdateFdEventUnlock(EPOLL_CTL_DEL, fd, 0));
    CHECK_FAIL_RETURN_STATUS(eventMap_.erase(fd) > 0, K_RUNTIME_ERROR, "Failed to erase fd from map.");
    return Status::OK();
}

Status EventLoop::ModifyFdEvent(int fd, uint32_t tEvents)
{
    std::lock_guard<std::mutex> lock(eventsLock_);
    return UpdateFdEventUnlock(EPOLL_CTL_MOD, fd, tEvents);
}

Status EventLoop::UpdateFdEventUnlock(int operation, int fd, uint32_t tEvents)
{
    auto iter = eventMap_.find(fd);
    if (iter == eventMap_.end()) {
        RETURN_STATUS(K_NOT_FOUND, FormatString("Not found event fd:%d", fd));
    }
    iter->second->events = tEvents;
    struct epoll_event ev;
    int ret = memset_s(&ev, sizeof(ev), 0, sizeof(ev));
    CHECK_FAIL_RETURN_STATUS(ret == EOK, K_RUNTIME_ERROR,
                             FormatString("UpdateFdEventUnlock failed, memset_s ret: %d", ret));
    ev.events = iter->second->events;
    ev.data.ptr = iter->second.get();
    CHECK_FAIL_RETURN_STATUS(epoll_ctl(efd_, operation, fd, &ev) == 0, K_RUNTIME_ERROR,
                             FormatString("epoll_ctl failed:%d", errno));
    return Status::OK();
}

void EventLoop::HandleEvent(const struct epoll_event *tEvents, int nevent)
{
    for (int i = 0; i < nevent; i++) {
        auto *tev = reinterpret_cast<EventData *>(tEvents[i].data.ptr);
        if (tEvents[i].events & EPOLLIN) {
            uint64_t count;
            // We use LT mode for epoll so we need to read the fd. And this function is only used for timer queue.
            if (read(tev->fd, &count, sizeof(uint64_t)) != sizeof(uint64_t)) {
                LOG(ERROR) << "read fd fail in HandleEvent:" << errno;
                continue;
            }
            if (tev->readCallBack) {
                tev->readCallBack();
            }
        } else if (tEvents[i].events & EPOLLOUT) {
            if (tev->writeCallBack) {
                tev->writeCallBack();
            }
        } else {
            LOG(ERROR) << "epoll event invalid: " << tEvents[i].events;
        }
    }
}

void SockEventLoop::HandleEvent(const struct epoll_event *tEvents, int nevent)
{
    for (int i = 0; i < nevent; i++) {
        auto *tev = reinterpret_cast<EventData *>(tEvents[i].data.ptr);
        if (tEvents[i].events & EPOLLIN) {
            ReadSockAndCallBack(tev);
        } else if (tEvents[i].events & EPOLLOUT) {
            if (tev->writeCallBack) {
                tev->writeCallBack();
            }
        } else {
            LOG(ERROR) << "epoll event invalid: " << tEvents[i].events;
        }
    }
}

void SockEventLoop::ReadSockAndCallBack(const EventLoop::EventData *tev)
{
    uint64_t count;
    while (true) {
        ssize_t ret = read(tev->fd, &count, sizeof(uint64_t));
        int err = errno;
        if (ret == -1) {
            if (err == EAGAIN) {
                continue;
            }
            // When errno is EINVAL, it means we shutdown the fd.
            if (err == EINVAL && tev->readCallBack) {
                LOG(INFO) << FormatString("Socket fd(%d) disconnection, run all callback.", tev->fd);
                tev->readCallBack();
            }
        } else if (ret == 0 && tev->readCallBack) {
            LOG(INFO) << FormatString("Socket fd(%d) disconnection, run all callback.", tev->fd);
            tev->readCallBack();
        }
        break;
    }
    (void)DelFdEvent(tev->fd);
}
}  // namespace datasystem
