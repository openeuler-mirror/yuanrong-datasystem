/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
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
 * Description: Implementation of AsyncUpdateLocationManager.
 */
#include "datasystem/worker/object_cache/async_update_location_manager.h"

#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/perf/perf_manager.h"

namespace datasystem {
namespace object_cache {
Status AsyncUpdateLocationManager::Init(std::function<void(UpdateLocationTask &&task)> updateLocationFunc)
{
    updateLocationFunc_ = std::move(updateLocationFunc);
    running_.store(true);
    for (int i = 0; i < UPDATE_LOCATION_QUEUE_NUM; i++) {
        threadPool_.emplace_back(Thread(&AsyncUpdateLocationManager::Execute, this, i));
    }
    return Status::OK();
}

void AsyncUpdateLocationManager::Stop()
{
    if (running_.exchange(false)) {
        LOG(INFO) << "AsyncUpdateLocationManager exit";
        for (auto &thread : threadPool_) {
            thread.join();
        }
    }
}

Status AsyncUpdateLocationManager::Execute(int threadNum)
{
    const int64_t sleepTime = 10;
    LOG(INFO) << "AsyncUpdateLocationManager start execute, threadNum: " << threadNum;
    UpdateLocationTask task;
    bool isEmpty;
    while (true) {
        {
            std::lock_guard<std::mutex> lock(*locks_[threadNum]);
            isEmpty = queues_[threadNum].empty();
            if (!running_.load() && isEmpty) {
                break;
            }
            if (!isEmpty) {
                task = std::move(queues_[threadNum].front());
                queues_[threadNum].pop_front();
            }
        }
        if (isEmpty) {
            std::this_thread::sleep_for(std::chrono::milliseconds(sleepTime));
            continue;
        }
        if (!updateLocationFunc_) {
            LOG(ERROR) << "updateLocationFunc_ is null";
            break;
        }
        updateLocationFunc_(std::move(task));
    }
    return Status::OK();
}

size_t AsyncUpdateLocationManager::ObjectKey2QueueIndex(const std::string &objectKey) const
{
    if (objectKey.empty()) {
        LOG(WARNING) << "The objectKey is empty, please check it. And the task will be add to the queue 0";
        return 0;
    }
    std::hash<std::string> hash;
    return hash(objectKey) % UPDATE_LOCATION_QUEUE_NUM;
}

Status AsyncUpdateLocationManager::AddTask(UpdateLocationTask &&task)
{
    INJECT_POINT("AsyncUpdateLocationAddTask.failed");
    PerfPoint point(PerfKey::WORKER_ASYNC_UPDATE_LOCATION_ADD);
    CHECK_FAIL_RETURN_STATUS(running_.load(),
                             StatusCode::K_RUNTIME_ERROR, "The async update location manager is not running");
    size_t index = ObjectKey2QueueIndex(task.GetFirstObjectKey());
    LOG(INFO) << "AddTask to the async queue, objectKey: " << task.GetFirstObjectKey() << ", index: " << index;
    std::lock_guard<std::mutex> lock(*locks_[index]);
    if (!queues_[index].empty() &&
        queues_[index].back().GetParams().size() + task.GetParams().size() <= AGGREGRATE_TASK_PARAMS_THRESHOLD) {
            queues_[index].back().MergeTaskParams(std::move(task));
    } else {
        queues_[index].emplace_back(std::move(task));
    }
    return Status::OK();
}

}  // namespace object_cache
}  // namespace datasystem
