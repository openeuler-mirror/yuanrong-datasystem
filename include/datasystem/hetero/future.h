
/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2024. All rights reserved.
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
 * Description: Data system Future class implementation.
 */

#ifndef DATASYSTEM_HETERO_FUTURE_H
#define DATASYSTEM_HETERO_FUTURE_H

#include <future>
#include <memory>

#include "datasystem/utils/status.h"

namespace datasystem {
class AclRtEventWrapper;
class __attribute((visibility("default"))) Future {
public:
    /**
     * @brief To wait for and access the result of the asynchronous operation.
     * @param[in] timeoutMs Timeout interval for invoking.
     * If subTimeoutMs > 0, blocks until timeout has elapsed or the result becomes available,
     * if subTimeoutMs = 0, return result status immediately,
     * @return Status of the result.
     */
    Status Get(uint64_t subTimeoutMs = 60000);

private:
    friend class PromiseWithEvent;
    Future(std::shared_future<Status> future, std::shared_ptr<AclRtEventWrapper> event, const std::string &objectKey);

    std::shared_future<Status> future_;
    std::shared_ptr<AclRtEventWrapper> event_;
    std::string objectKey_;
};
}  // namespace datasystem
#endif