/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2023. All rights reserved.
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
 * Description: Zmq epoll abstraction.
 */

#include "datasystem/common/rpc/zmq/zmq_epoll.h"

#include <utility>
#include "datasystem/common/util/gflag/common_gflags.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {
std::unique_ptr<ZmqPollEntry> ZmqCreatePollEntry(int fd, std::vector<std::weak_ptr<BaseHint>> hint,
                                                 ZmqCallBackFunc inFunc, ZmqCallBackFunc outFunc)
{
    auto pe = std::make_unique<ZmqPollEntry>();
    pe->fd_ = fd;
    pe->ev_.events = 0;
    pe->ev_.data.ptr = pe.get();
    pe->hint_ = std::move(hint);
    pe->handle_ = pe.get();
    pe->inEventFunc_ = std::move(inFunc);
    pe->outEventFunc_ = std::move(outFunc);
    return pe;
}

Status ZmqEpoll::Init(const std::string &startupMsg)
{
    efd_ = epoll_create1(0);
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(efd_ > 0, K_RUNTIME_ERROR,
                                         "Unable to create event loop. Errno: " + std::to_string(errno));
    auto traceId = Trace::Instance().GetTraceID();
    identStr_ = startupMsg.empty() ? "ZmqEpoll" : startupMsg;
    thrd_ = std::make_unique<Thread>([this, traceId]() {
        if (!Thread::SetCurrentThreadNice(FLAGS_io_thread_nice)) {
            LOG(WARNING) << "Failed to set nice for ZmqEpoll thread, nice=" << FLAGS_io_thread_nice
                         << ", errno=" << errno;
        }
        TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceId);
        VLOG(RPC_KEY_LOG_LEVEL) << FormatString("%s starts", identStr_);
        Status rc;
        int timeout = RPC_POLL_TIME;
        do {
            rc = HandleEvent(timeout);
            if (rc.GetCode() == K_NOT_FOUND) {
                // Idle. Wait for a full 100ms in the next iteration
                timeout = RPC_POLL_TIME;
                continue;
            }
            timeout = 0;
            if (rc.IsError() && rc.GetCode() != K_SHUTTING_DOWN) {
                LOG(ERROR) << "ZmqEpoll HandleEvent failed"
                           << ":" << rc.ToString();
            }
        } while (rc.GetCode() != K_SHUTTING_DOWN);
    });
    thrd_->set_name("ZmqEpoll");
    return Status::OK();
}

Status ZmqEpoll::AddFd(int fd, std::unique_ptr<ZmqPollEntry> &&pe, ZmqEpollHandle &handle) const
{
    auto err = epoll_ctl(efd_, EPOLL_CTL_ADD, fd, &pe->ev_);
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(err != -1, K_RUNTIME_ERROR, FormatString("epoll_ctl error: Errno %d", errno));
    auto pollEntry = std::move(pe);
    handle = pollEntry.release();
    VLOG(RPC_LOG_LEVEL) << FormatString("%s add fd %d to epoll list", identStr_, fd);
    return Status::OK();
}

Status ZmqEpoll::RemoveFd(ZmqEpollHandle &handle)
{
    RETURN_OK_IF_TRUE(handle == nullptr);
    auto *pe = static_cast<ZmqPollEntry *>(handle);
    auto fd = pe->fd_;
    auto err = epoll_ctl(efd_, EPOLL_CTL_DEL, fd, &pe->ev_);
    pe->fd_ = ZMQ_NO_FILE_FD;
    WriteLock lock(&mux_);
    pendingClose_.push_back(pe);
    handle = nullptr;
    CHECK_FAIL_RETURN_STATUS(err != -1, K_RUNTIME_ERROR, FormatString("epoll_ctl error: Errno %d", errno));
    return Status::OK();
}

Status ZmqEpoll::SetPollIn(ZmqEpollHandle handle) const
{
    RETURN_RUNTIME_ERROR_IF_NULL(handle);
    auto *pe = static_cast<ZmqPollEntry *>(handle);
    pe->ev_.events |= EPOLLIN;
    auto err = epoll_ctl(efd_, EPOLL_CTL_MOD, pe->fd_, &pe->ev_);
    CHECK_FAIL_RETURN_STATUS(err != -1, K_RUNTIME_ERROR, FormatString("epoll_ctl error: Errno %d", errno));
    return Status::OK();
}

Status ZmqEpoll::UnsetPollIn(ZmqEpollHandle handle) const
{
    auto *pe = static_cast<ZmqPollEntry *>(handle);
    pe->ev_.events &= ~(static_cast<uint32_t>(EPOLLIN));
    auto err = epoll_ctl(efd_, EPOLL_CTL_MOD, pe->fd_, &pe->ev_);
    CHECK_FAIL_RETURN_STATUS(err != -1, K_RUNTIME_ERROR, FormatString("epoll_ctl error: Errno %d", errno));
    return Status::OK();
}

Status ZmqEpoll::SetPollEdgeTriggered(ZmqEpollHandle handle) const
{
    auto *pe = static_cast<ZmqPollEntry *>(handle);
    pe->ev_.events |= EPOLLET;
    auto err = epoll_ctl(efd_, EPOLL_CTL_MOD, pe->fd_, &pe->ev_);
    CHECK_FAIL_RETURN_STATUS(err != -1, K_RUNTIME_ERROR, FormatString("epoll_ctl error: Errno %d", errno));
    return Status::OK();
}

Status ZmqEpoll::UnsetPollEdgeTriggered(ZmqEpollHandle handle) const
{
    auto *pe = static_cast<ZmqPollEntry *>(handle);
    pe->ev_.events &= ~(static_cast<uint32_t>(EPOLLET));
    auto err = epoll_ctl(efd_, EPOLL_CTL_MOD, pe->fd_, &pe->ev_);
    CHECK_FAIL_RETURN_STATUS(err != -1, K_RUNTIME_ERROR, FormatString("epoll_ctl error: Errno %d", errno));
    return Status::OK();
}

Status ZmqEpoll::SetPollOut(ZmqEpollHandle handle) const
{
    auto *pe = static_cast<ZmqPollEntry *>(handle);
    pe->ev_.events |= EPOLLOUT;
    auto err = epoll_ctl(efd_, EPOLL_CTL_MOD, pe->fd_, &pe->ev_);
    CHECK_FAIL_RETURN_STATUS(err != -1, K_RUNTIME_ERROR, FormatString("epoll_ctl error: Errno %d", errno));
    return Status::OK();
}

Status ZmqEpoll::UnsetPollOut(ZmqEpollHandle handle) const
{
    auto *pe = static_cast<ZmqPollEntry *>(handle);
    pe->ev_.events &= ~(static_cast<uint32_t>(EPOLLOUT));
    auto err = epoll_ctl(efd_, EPOLL_CTL_MOD, pe->fd_, &pe->ev_);
    CHECK_FAIL_RETURN_STATUS(err != -1, K_RUNTIME_ERROR, FormatString("epoll_ctl error: Errno %d", errno));
    return Status::OK();
}

void ZmqEpoll::HandlePendingClose()
{
    WriteLock lock(&mux_);
    for (auto &pe : pendingClose_) {
        delete pe;
    }
    pendingClose_.clear();
}

Status ZmqEpoll::HandleEvent(int timeout)
{
    auto n = epoll_wait(efd_, eventInboundList_.data(), ZMQ_MAX_EPOLL, timeout);
    if (n < 0) {
        if (errno == EINTR) {
            RETURN_STATUS(StatusCode::K_TRY_AGAIN, "The error code of epoll_wait is EINTR");
        } else {
            RETURN_STATUS_LOG_ERROR(K_RUNTIME_ERROR, "epoll_wait failed with errno " + std::to_string(errno));
        }
    }
    if (globalInterrupt_) {
        RETURN_STATUS(StatusCode::K_SHUTTING_DOWN, "Shutting down the socket.");
    }
    Status rc;
    for (int i = 0; i < n; ++i) {
        auto &ev = eventInboundList_[i];
        auto *pe = static_cast<ZmqPollEntry *>(ev.data.ptr);
        if (ev.events & EPOLLIN) {
            if (pe == nullptr || pe->fd_ == ZMQ_NO_FILE_FD || pe->inEventFunc_ == nullptr) {
                continue;
            }
            rc = pe->inEventFunc_(pe, ev.events);
            VLOG(RPC_LOG_LEVEL) << FormatString("%s fd %d", rc.ToString(), pe->fd_);
        }
        if (ev.events & EPOLLOUT) {
            // We need to check again the pe and fd again because we can get both
            // EPOLLIN and EPOLLOUT event at the same time. Running inEventFunc_ may
            // change the state of pe
            if (pe == nullptr || pe->fd_ == ZMQ_NO_FILE_FD || pe->outEventFunc_ == nullptr) {
                continue;
            }
            rc = pe->outEventFunc_(pe, ev.events);
            VLOG(RPC_LOG_LEVEL) << FormatString("%s fd %d", rc.ToString(), pe->fd_);
        }
    }
    HandlePendingClose();
    return (n == 0) ? Status(StatusCode::K_NOT_FOUND, "Idle") : Status::OK();
}

ZmqEpoll::ZmqEpoll() : efd_(ZMQ_NO_FILE_FD), eventInboundList_(ZMQ_MAX_EPOLL), globalInterrupt_(false)
{
}

ZmqEpoll::~ZmqEpoll()
{
    Stop();
    if (efd_ != ZMQ_NO_FILE_FD) {
        RETRY_ON_EINTR(close(efd_));
    }
    HandlePendingClose();
}

void ZmqEpoll::Stop()
{
    globalInterrupt_ = true;
    if (thrd_) {
        thrd_->join();
        thrd_.reset();
    }
}
}  // namespace datasystem
