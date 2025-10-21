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
 * Description: deadlock util.
 */

#ifndef DATASYSTEM_COMMON_UTIL_DEADLOCK_UTIL_H
#define DATASYSTEM_COMMON_UTIL_DEADLOCK_UTIL_H

#include <functional>

#include "datasystem/common/util/status_helper.h"

namespace datasystem {
/**
 * @brief Call the registered function, if a deadlock occurs, it will be retried.
 * @param[in] fn The implementation of the registered function.
 * @return Status of the call.
 */
Status RetryWhenDeadlock(const std::function<Status()> &fn);
}  // namespace datasystem
#endif  // DATASYSTEM_COMMON_UTIL_DEADLOCK_UTIL_H