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

#include <cstddef>
#include <iostream>

#include <jemalloc/jemalloc.h>

int main()
{
    bool profilingSupported = false;
    size_t valueSize = sizeof(profilingSupported);
    if (mallctl("config.prof", &profilingSupported, &valueSize, nullptr, 0) != 0) {
        std::cerr << "Failed to read jemalloc config.prof" << std::endl;
        return 1;
    }
#ifdef EXPECT_JEMALLOC_PROF
    constexpr bool expected = true;
#else
    constexpr bool expected = false;
#endif
    if (profilingSupported != expected) {
        std::cerr << "Unexpected jemalloc profiling capability: " << profilingSupported << std::endl;
        return 1;
    }
    return 0;
}
