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
 * Description: Defines the worker service processing publish process.
 */

#ifndef DATASYSTEM_WORKER_OBJECT_CACHE_ASYNC_UPDATE_LOCATION_MANAGER_H
#define DATASYSTEM_WORKER_OBJECT_CACHE_ASYNC_UPDATE_LOCATION_MANAGER_H

#include <deque>
#include <future>
#include <map>
#include <vector>

#include "datasystem/common/util/thread.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace object_cache {
const int UPDATE_LOCATION_QUEUE_NUM = 4; // The number of queues for update location task
const int AGGREGRATE_TASK_PARAMS_THRESHOLD = 300; // The number of queues for update location task

struct UpdateLocationParam {
    std::string objectKey;
    uint64_t version;
    uint32_t data_format;
};

class UpdateLocationTask {
public:
    UpdateLocationTask() = default;
    explicit UpdateLocationTask(UpdateLocationParam param) : params_({std::move(param)}) {};

    explicit UpdateLocationTask(std::vector<UpdateLocationParam> params) : params_(std::move(params)) {};

    ~UpdateLocationTask() = default;
    /**
     * @brief GetParams of the UpdateLocationTask.
     * @return return the params of the UpdateLocationTask.
     */
    const std::vector<UpdateLocationParam> &GetParams() const { return params_; };

    /**
     * @brief Extract the UpdateLocationTask params.
     * @return return the params of the UpdateLocationTask.
     */
    std::vector<UpdateLocationParam> ExtractParams() { return std::move(params_); };

    /**
     * @brief Merge the UpdateLocationTask params.
     */
    void MergeTaskParams(UpdateLocationTask &&task)
    {
        params_.reserve(AGGREGRATE_TASK_PARAMS_THRESHOLD);
        auto &&otherParams = task.ExtractParams();
        params_.insert(params_.end(), std::make_move_iterator(otherParams.begin()),
                      std::make_move_iterator(otherParams.end()));
    };

    /**
     * @brief GetFirstObjectKey of the UpdateLocationTask.
     * @return return the first objecyKey.
     */
    const std::string &GetFirstObjectKey()
    {
        static const std::string emptyString;
        if (!params_.empty()) {
            return params_[0].objectKey;
        }
        return emptyString;
    };

private:
    std::vector<UpdateLocationParam> params_;
};

class AsyncUpdateLocationManager {
public:
    AsyncUpdateLocationManager()
    {
        for (size_t i = 0; i < UPDATE_LOCATION_QUEUE_NUM; ++i) {
            locks_.push_back(std::make_unique<std::mutex>());
            queues_.push_back(std::deque<UpdateLocationTask>());
        }
    }

    ~AsyncUpdateLocationManager()
    {
        Stop();
    }

    AsyncUpdateLocationManager(const AsyncUpdateLocationManager&) = delete;

    AsyncUpdateLocationManager& operator=(const AsyncUpdateLocationManager&) = delete;

    /**
     * @brief Init the AsyncUpdateLocationManager. This should be called before any other function.
     * @return Status of the call.
     */
    Status Init(std::function<void(UpdateLocationTask &&task)> updateLocationFunc);

    /**
     * @brief Stop the AsyncUpdateLocationManager.
     */
    void Stop();

    /**
     * @brief Add task to queue of the AsyncUpdateLocationManager.
     * @param[in] task Task to be added.
     * @return Status of the call.
     */
    Status AddTask(UpdateLocationTask &&task);

private:
    Status Execute(int threadNum);
    size_t ObjectKey2QueueIndex(const std::string &objectKey) const;

    std::vector<std::unique_ptr<std::mutex>> locks_; // the lock for each queue in queues_
    std::vector<std::deque<UpdateLocationTask>> queues_;
    std::vector<Thread> threadPool_;
    std::atomic<bool> running_{ false };
    std::function<void(UpdateLocationTask &&task)> updateLocationFunc_;
};
}  // namespace object_cache
}  // namespace datasystem
#endif  // DATASYSTEM_WORKER_OBJECT_CACHE_ASYNC_UPDATE_LOCATION_MANAGER_H
