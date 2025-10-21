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

#ifndef DATASYSTEM_COMMON_RPC_ZMQ_EPOLL_H
#define DATASYSTEM_COMMON_RPC_ZMQ_EPOLL_H

#include <sys/epoll.h>
#include <sys/eventfd.h>

#include "datasystem/common/rpc/zmq/zmq_common.h"
#include "datasystem/common/util/locks.h"
#include "datasystem/common/util/thread.h"

namespace datasystem {
struct ZmqPollEntry;
typedef std::function<Status(ZmqPollEntry *, uint32_t events)> ZmqCallBackFunc;
struct BaseHint {
    virtual ~BaseHint() = default;
};

struct ZmqPollEntry {
    int fd_{ ZMQ_NO_FILE_FD };
    epoll_event ev_{};
    std::vector<std::weak_ptr<BaseHint>> hint_;
    void *handle_{ nullptr };
    ZmqCallBackFunc inEventFunc_{ nullptr };
    ZmqCallBackFunc outEventFunc_{ nullptr };
};

/**
 * @brief Create an epoll entry
 * @param fd
 * @param hint
 * @param inFunc call back function EPOLLIN
 * @return ZmqPollEntry
 */
std::unique_ptr<ZmqPollEntry> ZmqCreatePollEntry(int fd,
                                                 std::vector<std::weak_ptr<BaseHint>> hint,
                                                 ZmqCallBackFunc inFunc,
                                                 ZmqCallBackFunc outFunc = nullptr);

typedef void *ZmqEpollHandle;
class ZmqEpoll {
public:
    ZmqEpoll();
    ~ZmqEpoll();
    ZmqEpoll(const ZmqEpoll &) = delete;
    ZmqEpoll &operator=(const ZmqEpoll &) = delete;
    ZmqEpoll(ZmqEpoll &&) = delete;
    ZmqEpoll &operator=(ZmqEpoll &&) = delete;

    Status Init(const std::string &startupMsg);
    Status AddFd(int fd, std::unique_ptr<ZmqPollEntry> &&pe, ZmqEpollHandle &handle) const;
    Status RemoveFd(ZmqEpollHandle &handle);
    Status SetPollIn(ZmqEpollHandle handle) const;
    Status UnsetPollIn(ZmqEpollHandle handle) const;
    Status SetPollEdgeTriggered(ZmqEpollHandle handle) const;
    Status UnsetPollEdgeTriggered(ZmqEpollHandle handle) const;
    Status SetPollOut(ZmqEpollHandle handle) const;
    Status UnsetPollOut(ZmqEpollHandle handle) const;
    void Stop();

private:
    std::string identStr_;
    int efd_;
    std::unique_ptr<Thread> thrd_;
    WriterPrefRWLock mux_;
    std::vector<ZmqPollEntry *> pendingClose_;
    std::vector<struct epoll_event> eventInboundList_;
    std::atomic<bool> globalInterrupt_;
    Status HandleEvent(int timeout);
    void HandlePendingClose();
};
}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_RPC_ZMQ_EPOLL_H
