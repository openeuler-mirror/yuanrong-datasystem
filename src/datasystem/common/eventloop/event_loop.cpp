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
    CHECK_FAIL_RETURN_STATUS(eventMap_.find(fd) == eventMap_.end(), K_DUPLICATED,
                             FormatString("Event fd %d is already registered", fd));
    const uint64_t token = nextEventToken_++;
    auto eventData = std::make_shared<EventData>(fd, tEvents, token, std::move(readCallBack), std::move(writeCallBack));
    eventMap_[fd] = eventData;
    eventTokenMap_[token] = eventData;
    auto status = UpdateFdEventUnlock(EPOLL_CTL_ADD, fd, tEvents);
    if (status.IsError()) {
        (void)eventMap_.erase(fd);
        (void)eventTokenMap_.erase(token);
        return status;
    }
    return Status::OK();
}

Status EventLoop::DelFdEvent(int fd)
{
    std::lock_guard<std::mutex> lock(eventsLock_);
    auto iter = eventMap_.find(fd);
    if (iter == eventMap_.end()) {
        return Status::OK();
    }
    // For EPOLL_CTL_DEL, ENOENT/EBADF means the fd is no longer registered with epoll (already
    // removed or closed) — treat as success and proceed to erase from the local map so callers
    // can re-add later without hitting spurious K_NOT_FOUND.
    if (epoll_ctl(efd_, EPOLL_CTL_DEL, fd, nullptr) != 0 && errno != ENOENT && errno != EBADF) {
        RETURN_STATUS(K_RUNTIME_ERROR, FormatString("epoll_ctl DEL failed for fd %d: errno=%d", fd, errno));
    }
    (void)eventTokenMap_.erase(iter->second->token);
    (void)eventMap_.erase(iter);
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
    ev.data.u64 = iter->second->token;
    CHECK_FAIL_RETURN_STATUS(epoll_ctl(efd_, operation, fd, &ev) == 0, K_RUNTIME_ERROR,
                             FormatString("epoll_ctl failed:%d", errno));
    return Status::OK();
}

std::shared_ptr<EventLoop::EventData> EventLoop::FindEventData(uint64_t token)
{
    std::lock_guard<std::mutex> lock(eventsLock_);
    auto iter = eventTokenMap_.find(token);
    return iter == eventTokenMap_.end() ? nullptr : iter->second;
}

void EventLoop::HandleEvent(const struct epoll_event *tEvents, int nevent)
{
    for (int i = 0; i < nevent; i++) {
        auto tev = FindEventData(tEvents[i].data.u64);
        if (tev == nullptr) {
            continue;
        }
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
        // epoll only carries a token. Resolve it under eventsLock_ before dereferencing EventData so a
        // concurrent DelFdEvent cannot leave this thread with an unsafe raw pointer. The local shared_ptr
        // keeps the callback alive even when it removes its own registration.
        auto tev = FindEventData(tEvents[i].data.u64);
        if (tev == nullptr) {
            continue;
        }
        const int fd = tev->fd;
        // EPOLLHUP/EPOLLERR (with or without EPOLLIN) means the peer closed/crashed. When a crash
        // produces HUP/ERR without IN (e.g. RST with no pending readable data), the old code fell
        // through to the else branch and only logged, so the lost-handle callback never fired. Handle
        // these first and treat them as a disconnect before any read attempt.
        if (tEvents[i].events & (EPOLLHUP | EPOLLERR)) {
            if (tev->readCallBack) {
                LOG(INFO) << FormatString("Socket fd(%d) peer closed (events=0x%x), run all callback.", fd,
                    tEvents[i].events);
                tev->readCallBack();
            }
            Status delStatus = DelFdEvent(fd);
            if (delStatus.IsError()) {
                // DelFdEvent can fail if the fd was already removed (e.g. the lost-handle callback
                // itself removed it). Worst case is epoll re-firing this handled HUP once; the callback
                // is idempotent, so this is a state-consistency warning, not a crash (review fix #16).
                LOG(ERROR) << FormatString("DelFdEvent failed after HUP/ERR on fd(%d): %s", fd,
                    delStatus.GetMsg());
            }
            continue;
        }
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

void SockEventLoop::ReadSockAndCallBack(const std::shared_ptr<EventLoop::EventData> &tev)
{
    const int fd = tev->fd;
    uint64_t count;
    while (true) {
        ssize_t ret = read(fd, &count, sizeof(uint64_t));
        int err = errno;
        if (ret == -1) {
            if (err == EAGAIN) {
                // No data right now and the peer is still alive (non-blocking fd). The old code did
                // `continue` here, which busy-loops on a level-triggered epoll. Break out instead and
                // keep the fd registered (do NOT DelFdEvent) so epoll can notify again on the next
                // readable/disconnect event.
                return;
            }
            // Any other read error (EINVAL for local shutdown, ECONNRESET/EPIPE on peer crash with
            // pending data) is a disconnect: run the lost-handle callback. The old code only handled
            // EINVAL here, so ECONNRESET/EPIPE fell through without invoking the callback, leaving the
            // crash undetected.
            if (tev->readCallBack) {
                LOG(INFO) << FormatString("Socket fd(%d) disconnection (errno=%d), run all callback.", fd, err);
                tev->readCallBack();
            }
        } else if (ret == 0 && tev->readCallBack) {
            LOG(INFO) << FormatString("Socket fd(%d) disconnection, run all callback.", fd);
            tev->readCallBack();
        }
        break;
    }
    (void)DelFdEvent(fd);
}
}  // namespace datasystem
