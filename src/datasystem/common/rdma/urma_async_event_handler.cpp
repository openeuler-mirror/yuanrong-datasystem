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
 * Description: URMA async event handler for Jetty/JFC error events.
 */
#include "datasystem/common/rdma/urma_async_event_handler.h"

#include <sys/epoll.h>
#include <unistd.h>

#include <cerrno>

#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/log/trace.h"
#include "datasystem/common/rdma/urma_dlopen_util.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {
namespace {
constexpr uint32_t K_URMA_ERROR_LOG_EVERY_N = 100;
constexpr int K_URMA_ASYNC_EVENT_EPOLL_TIMEOUT_MS = 100;
}  // namespace

void UrmaAsyncEventHandler::Init(UrmaResource *resource)
{
    urmaResource_ = resource;
}

void UrmaAsyncEventHandler::Start(const std::atomic<bool> &stopFlag)
{
    serverAsyncEventThread_ = std::make_unique<std::thread>(&UrmaAsyncEventHandler::Run, this, std::ref(stopFlag));
}

void UrmaAsyncEventHandler::Stop()
{
    if (serverAsyncEventThread_ && serverAsyncEventThread_->joinable()) {
        LOG(INFO) << "Waiting for Async Event thread to exit";
        serverAsyncEventThread_->join();
        serverAsyncEventThread_.reset();
    }
}

Status UrmaAsyncEventHandler::GetAsyncEvent(urma_async_event_t &event)
{
    event = {};
    CHECK_FAIL_RETURN_STATUS(urmaResource_ != nullptr, K_RUNTIME_ERROR, "URMA resource is null");
    INJECT_POINT("UrmaManager.InjectAsyncEvent", [this, &event](int eventType) {
        switch (eventType) {
            case URMA_EVENT_JETTY_ERR: {
                std::shared_ptr<UrmaJetty> jetty;
                RETURN_IF_NOT_OK(urmaResource_->GetAnyValidJetty(jetty));
                event.urma_ctx = urmaResource_->GetContext();
                event.event_type = URMA_EVENT_JETTY_ERR;
                event.element.jetty = jetty->Raw();
                return Status::OK();
            }
            case URMA_EVENT_JFC_ERR:
                event.urma_ctx = urmaResource_->GetContext();
                event.event_type = URMA_EVENT_JFC_ERR;
                event.element.jfc = urmaResource_->GetJfc();
                CHECK_FAIL_RETURN_STATUS(event.element.jfc != nullptr, K_RUNTIME_ERROR,
                                         "URMA JFC is null for injected async event");
                return Status::OK();
            default:
                return Status(K_INVALID, FormatString("Unsupported injected async event type: %d", eventType));
        }
    });

    urma_status_t rc = ds_urma_get_async_event(urmaResource_->GetContext(), &event);
    if (rc != URMA_SUCCESS) {
        return Status(K_URMA_ERROR, FormatString("urma_get_async_event failed: %d", rc));
    }
    return Status::OK();
}

Status UrmaAsyncEventHandler::Run(const std::atomic<bool> &stopFlag)
{
    LOG(INFO) << "[URMA_AE] Async event thread started";
    CHECK_FAIL_RETURN_STATUS(urmaResource_ != nullptr, K_RUNTIME_ERROR, "URMA resource is null");
    auto *context = urmaResource_->GetContext();
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(context != nullptr, K_RUNTIME_ERROR, "[URMA_AE] URMA context is null");
    int asyncFd = context->async_fd;
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(asyncFd > 0, K_RUNTIME_ERROR,
                                         FormatString("[URMA_AE] Invalid URMA async fd: %d", asyncFd));
    int epollFd = epoll_create(1);
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(epollFd > 0, K_RUNTIME_ERROR,
                                         FormatString("[URMA_AE] epoll_create failed: %d", errno));
    Raii closeEpollFd([epollFd]() { RETRY_ON_EINTR(close(epollFd)); });
    epoll_event asyncEvent{};
    asyncEvent.events = EPOLLIN;
    asyncEvent.data.fd = asyncFd;
    if (epoll_ctl(epollFd, EPOLL_CTL_ADD, asyncFd, &asyncEvent) != 0) {
        RETURN_STATUS_LOG_ERROR(K_RUNTIME_ERROR,
                                FormatString("[URMA_AE] epoll_ctl add async fd %d failed: %d", asyncFd, errno));
    }

    while (!stopFlag.load()) {
        epoll_event epollEvent;
        int nevent = epoll_wait(epollFd, &epollEvent, 1, K_URMA_ASYNC_EVENT_EPOLL_TIMEOUT_MS);
        INJECT_POINT_NO_RETURN("UrmaManager.InjectAsyncEvent", [&nevent, &epollEvent](int) {
            nevent = 1;
            epollEvent.events = EPOLLIN;
        });
        if (nevent == 0 || (nevent < 0 && errno == EINTR) || stopFlag) {
            continue;
        }
        if (nevent < 0) {
            LOG_FIRST_AND_EVERY_N(ERROR, K_URMA_ERROR_LOG_EVERY_N) << "[URMA_AE] epoll_wait failed: " << errno;
            continue;
        }
        if ((epollEvent.events & (EPOLLERR | EPOLLHUP | EPOLLRDBAND)) != 0) {
            LOG_FIRST_AND_EVERY_N(ERROR, K_URMA_ERROR_LOG_EVERY_N)
                << "[URMA_AE] async fd epoll event invalid: " << epollEvent.events;
            continue;
        }

        urma_async_event_t event;
        auto traceGuard = Trace::Instance().SetTraceUUID();
        auto rc = GetAsyncEvent(event);
        if (rc.IsError()) {
            LOG_FIRST_AND_EVERY_N(ERROR, K_URMA_ERROR_LOG_EVERY_N)
                << "[URMA_AE] GetAsyncEvent failed: " << rc.ToString();
            continue;
        }
        LOG(WARNING) << "[URMA_AE] Received async event, type=" << static_cast<int>(event.event_type);
        auto handleRc = HandleUrmaAsyncEvent(event);
        if (handleRc.IsError()) {
            LOG(ERROR) << "[URMA_AE] HandleUrmaAsyncEvent failed: " << handleRc.ToString();
        }
        ds_urma_ack_async_event(&event);
    }
    LOG(INFO) << "[URMA_AE] Async event thread exiting";
    return Status::OK();
}

Status UrmaAsyncEventHandler::HandleUrmaAsyncEvent(const urma_async_event_t &event)
{
    switch (event.event_type) {
        case URMA_EVENT_JETTY_ERR:
            return HandleJettyErrAsyncEvent(event.element.jetty);
        case URMA_EVENT_JFC_ERR:
            return HandleJfcErrAsyncEvent(event.element.jfc);
        default:
            LOG(WARNING) << "[URMA_AE] Unhandled async event type=" << static_cast<int>(event.event_type);
            return Status::OK();
    }
}

Status UrmaAsyncEventHandler::HandleJettyErrAsyncEvent(urma_jetty_t *rawJetty)
{
    CHECK_FAIL_RETURN_STATUS(urmaResource_ != nullptr, K_RUNTIME_ERROR, "URMA resource is null");
    if (rawJetty == nullptr) {
        LOG(ERROR) << "[URMA_AE_JETTY_ERR] rawJetty is null in async event";
        return Status::OK();
    }
    const uint32_t jettyId = rawJetty->jetty_id.id;
    LOG(WARNING) << "[URMA_AE_JETTY_ERR] jettyId=" << jettyId;

    std::shared_ptr<UrmaJetty> failedJetty;
    auto lookupRc = urmaResource_->GetJettyById(jettyId, failedJetty);
    if (lookupRc.IsError() || failedJetty == nullptr) {
        LOG(WARNING) << "[URMA_AE_JETTY_ERR] Jetty " << jettyId
                     << " not found in registry, may have already been cleaned up";
        return Status::OK();
    }

    auto connection = failedJetty->GetConnection().lock();
    if (connection == nullptr) {
        LOG(WARNING) << "[URMA_AE_JETTY_ERR] Jetty " << jettyId
                     << " has no bound connection, cannot trigger recovery";
        return Status::OK();
    }

    LOG(WARNING) << "[URMA_AE_JETTY_ERR] Triggering ReCreateJetty for jettyId=" << jettyId;
    auto recreateRc = connection->ReCreateJetty(*urmaResource_, failedJetty);
    if (recreateRc.IsError()) {
        LOG(ERROR) << "[URMA_AE_JETTY_ERR] ReCreateJetty failed for jettyId=" << jettyId << ": "
                   << recreateRc.ToString();
    } else {
        LOG(INFO) << "[URMA_AE_JETTY_ERR] ReCreateJetty succeeded for jettyId=" << jettyId;
    }
    INJECT_POINT("UrmaManager.HandleJettyErrAsyncEvent");
    return Status::OK();
}

Status UrmaAsyncEventHandler::HandleJfcErrAsyncEvent(urma_jfc_t *rawJfc)
{
    INJECT_POINT("UrmaManager.HandleJfcErrAsyncEvent");
    if (rawJfc == nullptr) {
        LOG(ERROR) << "[URMA_AE_JFC_ERR] rawJfc is null in async event";
        return Status::OK();
    }
    LOG(WARNING) << "[URMA_AE_JFC_ERR] jfc_id=" << rawJfc->jfc_id.id << ", no recovery action taken at this stage";
    return Status::OK();
}

}  // namespace datasystem
