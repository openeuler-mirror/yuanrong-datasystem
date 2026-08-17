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

#include "datasystem/common/coordinator/event_notify_executor.h"

#include "datasystem/common/util/status_helper.h"

namespace datasystem {
EventNotifyExecutor::~EventNotifyExecutor()
{
    Stop();
}

Status EventNotifyExecutor::Start(size_t workerCount, bthread_tag_t tag, Worker worker)
{
    CHECK_FAIL_RETURN_STATUS(workerCount > 0, K_INVALID, "Event notify worker count must be positive");
    CHECK_FAIL_RETURN_STATUS(static_cast<bool>(worker), K_INVALID, "Event notify worker callback must not be empty");
    CHECK_FAIL_RETURN_STATUS(workers_.empty(), K_RUNTIME_ERROR, "Event notify executor is already running");
    worker_ = std::move(worker);
    bthread_attr_t attr = BTHREAD_ATTR_NORMAL;
    attr.tag = tag;
    workers_.reserve(workerCount);
    workerContexts_.reserve(workerCount);
    for (size_t i = 0; i < workerCount; ++i) {
        auto context = std::make_unique<WorkerContext>(WorkerContext{ this, i });
        bthread_t workerId;
        const int rc = bthread_start_background(&workerId, &attr, &EventNotifyExecutor::Run, context.get());
        if (rc != 0) {
            RETURN_STATUS(K_RUNTIME_ERROR, "Failed to start event notify bthread");
        }
        workerContexts_.push_back(std::move(context));
        workers_.push_back(workerId);
    }
    return Status::OK();
}

void EventNotifyExecutor::Stop()
{
    for (const auto &workerId : workers_) {
        bthread_stop(workerId);
    }
    for (const auto &workerId : workers_) {
        bthread_join(workerId, nullptr);
    }
    workers_.clear();
    workerContexts_.clear();
    worker_ = nullptr;
}

void *EventNotifyExecutor::Run(void *arg)
{
    auto *context = static_cast<WorkerContext *>(arg);
    context->executor->worker_(context->workerIndex);
    return nullptr;
}
}  // namespace datasystem
