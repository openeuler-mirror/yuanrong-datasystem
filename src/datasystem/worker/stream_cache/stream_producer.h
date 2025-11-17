/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2023. All rights reserved.
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

#ifndef DATASYSTEM_WORKER_STREAM_CACHE_STREAM_PRODUCER_H
#define DATASYSTEM_WORKER_STREAM_CACHE_STREAM_PRODUCER_H

#include <string>

namespace datasystem {
namespace worker {
namespace stream_cache {
/**
 * @brief This simple class provides a pair of named strings for improved code readability (better than std::pair)
 * since it avoids getting mixed up on which string is first or second. Used for making lists of producers by stream.
 */
class StreamProducer {
public:
    StreamProducer(const std::string &streamName, const std::string &producerId)
        : streamName_(streamName), producerId_(producerId)
    {
    }
    ~StreamProducer() = default;
    std::string streamName_;
    std::string producerId_;
};
}  // namespace stream_cache
}  // namespace worker
}  // namespace datasystem
#endif  // DATASYSTEM_WORKER_STREAM_CACHE_STREAM_PRODUCER_H
