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
 * Description: EventLoop declaration.
 */
#ifndef DATASYSTEM_COMMON_EVENTLOOP_EVLOOP_H
#define DATASYSTEM_COMMON_EVENTLOOP_EVLOOP_H

#include <atomic>
#include <functional>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <queue>
#include <thread>

#include <securec.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>

#include "datasystem/common/log/log.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/thread.h"

namespace datasystem {
class EventLoop {
public:
    EventLoop() : efd_(-1), stopLoop_(false), stopFd_(-1), eventList_(EPOLL_EVENTS_SIZE){};

    virtual ~EventLoop();

    EventLoop(const EventLoop &) = delete;

    EventLoop &operator=(const EventLoop &) = delete;

    /**
     * @brief Init the eventloop instance.
     * @return Status of the call.
     */
    Status Init();

    /**
     * @brief AddFdEvent add the fd and the hook function.
     * @param[in] fd The file descriptor.
     * @param[in] events The event to be watched.
     * @param[in] readCallBack The hook function for readable event.
     * @param[in] writeCallBack The hook function for writable event.
     * @return Status of the call.
     */
    Status AddFdEvent(int fd, uint32_t events, std::function<void()> readCallBack, std::function<void()> writeCallBack);

    /**
     * @brief ModifyFdEvent modify event to be watched for the fd.
     * @param[in] fd The file descriptor.
     * @param[in] events The event to be watched.
     * @return Status of the call.
     */
    Status ModifyFdEvent(int fd, uint32_t events);

    /**
     * @brief Delete fd in eventloop.
     * @param[in] fd The file descriptor.
     * @return Status of the call.
     */
    Status DelFdEvent(int fd);

protected:
    static constexpr int EPOLL_EVENTS_SIZE = 64;
    struct EventData {
        int fd;
        uint32_t events;  // The epoll events that we are interested in, e.g. EPOLLIN.
        std::function<void()> readCallBack;
        std::function<void()> writeCallBack;
        EventData(int fd, uint32_t events, std::function<void()> readCallBack, std::function<void()> writeCallBack)
            : fd(fd), events(events), readCallBack(std::move(readCallBack)), writeCallBack(std::move(writeCallBack))
        {
        }
    };

    /**
     * @brief Start eventloop.
     */
    void Run();

    /**
     * @brief The processing after event triggering.
     * @param[in] events Event array.
     * @param[in] nevent Number of Events Triggered.
     */
    virtual void HandleEvent(const struct epoll_event *events, int nevent);

    /**
     * @brief Update the event.
     * @param[in] operation The update operation.
     * @param[in] fd The file descriptor.
     * @param[in] tEvents The event to be watched.
     * @return Status of the call.
     */
    Status UpdateFdEventUnlock(int operation, int fd, uint32_t tEvents);

    /**
     * @brief Finish the eventloop.
     */
    void Finish();

    int efd_;     // The fd used to listen the epoll events.
    std::atomic<bool> stopLoop_ = { false };  // Whether the loop should be stopped or not.
    int stopFd_;  // The fd is used to trigger the next event loop so that the above stopLoop_ flag can come into play.
    Thread loopThread_;  // The thread to run the event loop.

    std::vector<struct epoll_event> eventList_;
    std::mutex eventsLock_;
    std::map<int, std::shared_ptr<EventData>> eventMap_;
};

class SockEventLoop : public EventLoop {
public:
    SockEventLoop() = default;
    ~SockEventLoop() override = default;

protected:
    /**
     * @brief The processing after socket listen event triggering.
     * @param[in] events Socket event array.
     * @param[in] nevent Number of Events Triggered.
     */
    void HandleEvent(const struct epoll_event *events, int nevent) override;

    /**
     * @brief Get the socket read result and call callback function.
     * @param[in] tev The eventdata which will be read.
     */
    void ReadSockAndCallBack(const EventData *tev);
};
}  // namespace datasystem
#endif  // DATASYSTEM_COMMON_EVENTLOOP_EVLOOP_H
