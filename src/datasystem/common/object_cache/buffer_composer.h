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
 * Description: Implementation of compose and decompose buffer data.
 */
#ifndef DATASYSTEM_COMMON_OBJECT_CACHE_BUFFER_COMPOSER_H
#define DATASYSTEM_COMMON_OBJECT_CACHE_BUFFER_COMPOSER_H

#include <iomanip>
#include <memory>
#include <vector>

#include "datasystem/hetero/device_common.h"
#include "datasystem/object/buffer.h"

namespace datasystem {
namespace object_cache {

struct BlobListInfo {
    int64_t keyNums = 0;
    int64_t nonExistNums = 0;
    // the key copy size
    int64_t totalSize = 0;
    int64_t minBlobNums = 0;
    int64_t maxBlobNums = 0;
    // avg blob nums per key
    int64_t avgBlobNums = 0;
    int64_t minBlockSize = 0;
    int64_t maxBlockSize = 0;
    // avg blob size per blob/key
    int64_t avgBlockSize = 0;

    std::string ToString(bool isSet) const
    {
        std::ostringstream oss;
        oss << "{\"KeyNum\":" << keyNums;
        if (isSet) {
            oss << ",\"SetKeyNum\":" << nonExistNums;
        }
        oss << ",\"KeySize\":" << totalSize;
        oss << ",\"BlobNum\":[" << minBlobNums << "," << maxBlobNums << "," << avgBlobNums << "]";
        std::string sizeDescStr = isSet ? ",\"BlobSize\":[" : ",\"KeySize\":[";
        oss << sizeDescStr << minBlockSize << "," << maxBlockSize << "," << avgBlockSize << "]}";
        return oss.str();
    }
};

/**
 * @brief Prepare the data sizes by user data list
 * @param[out] sizeList The list of all data sizes
 * @param[in] devBlobList The user data list
 * @param[in] blobInfo The information of blob
 * @return K_OK on any object success; the error code otherwise.
 */
Status PrepareDataSizeList(std::vector<size_t> &sizeList, const std::vector<DeviceBlobList> &devBlobList,
                           BlobListInfo &blobInfo);

/**
 * @brief Compose buffer list by user data list
 * @param[out] bufferList Compose the user data to bufferList
 * @param[in] devBlobList The user data list
 */
void ComposeBufferData(std::vector<std::shared_ptr<Buffer>> &bufferList,
                       const std::vector<DeviceBlobList> &devBlobList);

}  // namespace object_cache
}  // namespace datasystem

#endif