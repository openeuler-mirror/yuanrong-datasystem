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
 * Description: Log failure handler.
 */
#ifndef DATASYSTEM_COMMON_LOG_FAILURE_HANDLER_H
#define DATASYSTEM_COMMON_LOG_FAILURE_HANDLER_H

namespace datasystem {

/**
 * @brief Write a failure message to a log file
 *
 * @param[in] data The failure message string to log.
 */
void FailureWriter(const char *data);

/**
 * @brief Installs signal handlers for critical failure signals
 *
 * @param[in] arg0 Typically the program name (from main()'s argv[0]), used in failure logs to identify the process.
 */
void InstallFailureSignalHandler(const char *arg0);

}  // namespace datasystem

#endif