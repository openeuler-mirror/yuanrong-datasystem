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
 * Description: URMA async event handler for JFS/JFC error events.
 */
#ifndef DATASYSTEM_COMMON_RDMA_URMA_ASYNC_EVENT_HANDLER_H
#define DATASYSTEM_COMMON_RDMA_URMA_ASYNC_EVENT_HANDLER_H

#include <atomic>
#include <memory>
#include <thread>

#include <ub/umdk/urma/urma_api.h>

#include "datasystem/common/rdma/urma_resource.h"
#include "datasystem/utils/status.h"

namespace datasystem {

class UrmaAsyncEventHandler {
public:
    UrmaAsyncEventHandler() = default;
    ~UrmaAsyncEventHandler() = default;

    UrmaAsyncEventHandler(const UrmaAsyncEventHandler &) = delete;
    UrmaAsyncEventHandler &operator=(const UrmaAsyncEventHandler &) = delete;
    UrmaAsyncEventHandler(UrmaAsyncEventHandler &&) = delete;
    UrmaAsyncEventHandler &operator=(UrmaAsyncEventHandler &&) = delete;

    /**
     * @brief Bind the URMA resource owned by UrmaManager.
     * @param[in] resource URMA resource raw pointer.
     */
    void Init(UrmaResource *resource);

    /**
     * @brief Start the async-event thread. The resource lifetime must cover the thread.
     * @param[in] stopFlag Shared stop flag controlled by UrmaManager.
     */
    void Start(const std::atomic<bool> &stopFlag);

    /**
     * @brief Stop and join the async-event thread.
     */
    void Stop();

private:
    /**
     * @brief Main loop for the async-event thread.
     * @param[in] stopFlag Shared stop flag controlled by UrmaManager.
     * @return Status of the loop.
     */
    Status Run(const std::atomic<bool> &stopFlag);

    /**
     * @brief Read a single async event from URMA or test injection.
     * @param[out] event The async event payload when available.
     * @return Status of the call.
     */
    Status GetAsyncEvent(urma_async_event_t &event);

    /**
     * @brief Dispatch a single async event to the appropriate handler.
     * @param[in] event The URMA async event received from the driver.
     * @return Status of the call.
     */
    Status HandleUrmaAsyncEvent(const urma_async_event_t &event);

    /**
     * @brief Handle a JFS_ERR async event by looking up the JFS and triggering recovery.
     * @param[in] rawJfs Raw JFS handle from the async event.
     * @return Status of the call.
     */
    Status HandleJfsErrAsyncEvent(urma_jfs_t *rawJfs);

    /**
     * @brief Handle a JFC_ERR async event.
     * @param[in] rawJfc Raw JFC handle from the async event.
     * @return Status of the call.
     */
    Status HandleJfcErrAsyncEvent(urma_jfc_t *rawJfc);

    UrmaResource *urmaResource_{ nullptr };
    std::unique_ptr<std::thread> serverAsyncEventThread_{ nullptr };
};

}  // namespace datasystem
#endif  // DATASYSTEM_COMMON_RDMA_URMA_ASYNC_EVENT_HANDLER_H
