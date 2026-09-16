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

/** Description: Defines the result of one kvtest benchmark SDK operation. */
#pragma once

#include <cstdint>

/** @brief Describes one benchmark SDK operation result. */
struct BenchmarkOpResult {
    bool success;
    bool notFound = false;
    bool timeout = false;
};

/** @brief Holds an optional measured interval for one benchmark operation. */
struct BenchmarkOpTiming {
    int64_t startNs = 0;
    int64_t endNs = 0;
};
