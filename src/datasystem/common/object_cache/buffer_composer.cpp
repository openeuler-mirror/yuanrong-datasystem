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

/**
 * Description: Implementation of compose and decompose buffer data.
 */
#include "datasystem/common/object_cache/buffer_composer.h"

#include <algorithm>
#include <limits>

#include "datasystem/common/util/status_helper.h"

namespace datasystem {
namespace object_cache {

uint64_t GetComposedBufferHeaderSize(size_t blobCount, uint32_t memoryAlignment)
{
    const auto alignment = static_cast<uint64_t>(memoryAlignment);
    const auto headerSize = sizeof(uint64_t) * (blobCount + 2);
    return (headerSize + alignment - 1) & ~(alignment - 1);
}

Status PrepareDataSizeList(std::vector<size_t> &sizeList, const std::vector<DeviceBlobList> &devBlobList,
                           BlobListInfo &blobInfo, uint32_t memoryAlignment)
{
    blobInfo.nonExistNums = devBlobList.size();
    if (blobInfo.nonExistNums <= 0) {
        return Status::OK();
    }

    int64_t blobNumSum = 0;
    size_t blobSizeCount = 0;
    blobInfo.minBlobNums = std::numeric_limits<int64_t>::max();
    blobInfo.maxBlobNums = std::numeric_limits<int64_t>::min();
    blobInfo.minBlockSize = std::numeric_limits<int64_t>::max();
    blobInfo.maxBlockSize = std::numeric_limits<int64_t>::min();
    for (const auto &blobList : devBlobList) {
        // For Length, Prefix Sum Arr, in O(1) and O(num+1) space.
        // Keep the payload aligned with the shared-memory allocation policy.
        auto &info = blobList.blobs;
        uint64_t num = info.size();
        uint64_t sz = GetComposedBufferHeaderSize(num, memoryAlignment);

        const auto blobNum = static_cast<int64_t>(info.size());
        blobNumSum += blobNum;
        blobInfo.minBlobNums = std::min(blobInfo.minBlobNums, blobNum);
        blobInfo.maxBlobNums = std::max(blobInfo.maxBlobNums, blobNum);
        for (auto &desc : info) {
            sz += desc.size;
            blobInfo.totalSize += desc.size;
            const auto blobSize = static_cast<int64_t>(desc.size);
            blobInfo.minBlockSize = std::min(blobInfo.minBlockSize, blobSize);
            blobInfo.maxBlockSize = std::max(blobInfo.maxBlockSize, blobSize);
            ++blobSizeCount;
        }

        sizeList.emplace_back(sz);
    }
    blobInfo.avgBlobNums = blobNumSum / blobInfo.nonExistNums;
    blobInfo.avgBlockSize = blobInfo.totalSize / blobSizeCount;

    return Status::OK();
}

void ComposeBufferData(std::vector<std::shared_ptr<Buffer>> &bufferList,
                       const std::vector<DeviceBlobList> &devBlobList, uint32_t memoryAlignment)
{
    // Record MetaData of SubBuffers.
    // | NumOfBuffers (n) | Off0 | Off1 | Offn | Padding | Buf1 | Buf2 | ... Bufn |
    uint64_t preOccupySize = 2;

    for (uint64_t i = 0; i < bufferList.size(); i++) {
        auto &buf = bufferList[i];
        auto &blobs = devBlobList[i].blobs;
        auto prefixSumArr = reinterpret_cast<uint64_t *>(buf->MutableData());

        uint64_t descSz = GetComposedBufferHeaderSize(blobs.size(), memoryAlignment);

        prefixSumArr[0] = blobs.size();
        prefixSumArr[1] = descSz;
        for (uint64_t j = 0; j < blobs.size(); j++) {
            prefixSumArr[j + preOccupySize] = prefixSumArr[j + 1] + blobs[j].size;
        }
    }
}
}  // namespace object_cache
}  // namespace datasystem
