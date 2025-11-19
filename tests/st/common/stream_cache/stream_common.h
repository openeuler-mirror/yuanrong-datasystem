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
 * Description: Test StreamPage StreamPageOwner classes.
 */
#ifndef DATASYSTEM_TEST_ST_WORKER_STREAM_CACHE_SINGLE_STREAM_COMMONa_H
#define DATASYSTEM_TEST_ST_WORKER_STREAM_CACHE_SINGLE_STREAM_COMMONa_H

#include <cstdint>

#include "common.h"
#include "mock_evictmanager.h"

namespace datasystem {
namespace st {
constexpr uint64_t SHM_CAP = 128L * 1024L * 1024L;
constexpr uint64_t BIG_SHM_CAP = 10L * 1024L * 1024L * 1024L;
constexpr uint64_t PAGE_SIZE = 4L * 1024L;
constexpr uint64_t BIG_PAGE_SIZE = 512L * 1024L;
constexpr uint64_t BIG_SIZE_RATIO = 16L;
constexpr uint64_t BIG_SIZE = PAGE_SIZE / BIG_SIZE_RATIO;
constexpr uint64_t KB = 1024L;
constexpr uint64_t MB = 1024L * KB;
constexpr uint64_t GB = 1024L * MB;
constexpr uint64_t TEST_STREAM_SIZE = 64 * MB;

constexpr int NUM_ELES = 50;

}  // namespace st
}  // namespace datasystem

#endif
