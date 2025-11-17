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
 * Description: Define stream cache struct for receiving element.
 */
#ifndef DATASYSTEM_CLIENT_STREAM_CACHE_RECEIVE_ELEMENT_H
#define DATASYSTEM_CLIENT_STREAM_CACHE_RECEIVE_ELEMENT_H

#include <cstdint>

namespace datasystem {
namespace client {
namespace stream_cache {
struct ReceiveElement {
    int workerFd;
    uint64_t eleOffset;
    uint64_t eleSize;
};
}  // namespace stream_cache
}  // namespace client
}  // namespace datasystem
#endif