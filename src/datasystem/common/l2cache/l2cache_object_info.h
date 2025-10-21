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
 * Description: the response of get object list
 */
#ifndef DATASYSTEM_COMMON_L2CACHE_OBJECT_INFO_H
#define DATASYSTEM_COMMON_L2CACHE_OBJECT_INFO_H
#include <string>
#include <vector>

namespace datasystem {
struct L2CacheObjectInfo {
    std::string contentType;  // This field is empty if use OBS.
    std::string lastModified;
    uint64_t size;
    // key composition is: <tenantId>/<objectKey>/<version>
    std::string key;
    // the version parse from the key
    uint64_t version;
};
}  // namespace datasystem
#endif  // DATASYSTEM_COMMON_L2CACHE_OBJECT_INFO_H