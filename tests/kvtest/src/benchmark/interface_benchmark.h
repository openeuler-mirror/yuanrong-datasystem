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

/** Description: Declares synchronized single-interface benchmark orchestration. */
#pragma once

#include <atomic>
#include <string>

#include "common/config.h"

/** @brief Return whether the mode uses the synchronized single-interface benchmark engine. */
bool IsInterfaceBenchmarkMode(TestMode mode);

/**
 * @brief Run a synchronized single-interface benchmark.
 * @param[in] cfg Benchmark configuration.
 * @param[in] configPath Configuration file used by re-executed child processes.
 * @param[in] running Process-wide stop flag.
 * @return Process exit code.
 */
int RunInterfaceBenchmark(const Config &cfg, const std::string &configPath, std::atomic<bool> &running);
