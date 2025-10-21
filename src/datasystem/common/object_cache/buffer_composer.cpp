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

#include <numeric>

#include "datasystem/common/util/status_helper.h"

namespace datasystem {
namespace object_cache {

Status PrepareDataSizeList(std::vector<size_t> &sizeList, const std::vector<std::vector<DataInfo>> &dataInfoList,
                           BlobListInfo &blobInfo)
{
    const uint64_t dataAlignSize = 64;
    blobInfo.nonExistNums = dataInfoList.size();
    std::vector<int64_t> blobNumsList;
    std::vector<int64_t> blobSizeList;
    for (const auto &info : dataInfoList) {
        // For Length, Prefix Sum Arr, in O(1) and O(num+1) space.
        // Round to 64x.
        uint64_t num = info.size();
        uint64_t sz = sizeof(uint64_t) * (num + 2);
        sz = (sz + dataAlignSize - 1) / dataAlignSize * dataAlignSize;
 
        int prevDevIdx = -1;
        blobNumsList.emplace_back(info.size());
        for (auto &desc : info) {
            if (prevDevIdx == -1) {
                prevDevIdx = desc.deviceIdx;
            }
            if (desc.deviceIdx == -1 || prevDevIdx != desc.deviceIdx) {
                return { K_INVALID, "Please set deviceIdx in datainfo." };
            }
            sz += desc.Size();
            blobInfo.totalSize += desc.Size();
            blobSizeList.emplace_back(desc.Size());
        }
 
        sizeList.emplace_back(sz);
    }
    if (blobInfo.nonExistNums <= 0) {
        return Status::OK();
    }
    uint64_t blobNumSum = std::accumulate(blobNumsList.begin(), blobNumsList.end(), 0L);
    blobInfo.avgBlobNums = blobNumSum / blobInfo.nonExistNums;
    blobInfo.minBlobNums = *std::min_element(blobNumsList.begin(), blobNumsList.end());
    blobInfo.maxBlobNums = *std::max_element(blobNumsList.begin(), blobNumsList.end());
    blobInfo.avgBlockSize = blobInfo.totalSize / blobSizeList.size();
    blobInfo.minBlockSize = *std::min_element(blobSizeList.begin(), blobSizeList.end());
    blobInfo.maxBlockSize = *std::max_element(blobSizeList.begin(), blobSizeList.end());
 
    return Status::OK();
}
 
void ComposeBufferData(std::vector<std::shared_ptr<Buffer>> &bufferList,
                       const std::vector<std::vector<DataInfo>> &dataInfoList)
{
    // Record MetaData of SubBuffers.
    // | NumOfBuffers (n) | Off0 | Off1 | Offn | Padding | Buf1 | Buf2 | ... Bufn |
    uint64_t preOccupySize = 2;
    const uint64_t dataAlignSize = 64;

    for (uint64_t i = 0; i < bufferList.size(); i++) {
        auto &buf = bufferList[i];
        auto &dataInfos = dataInfoList[i];
        auto prefixSumArr = reinterpret_cast<uint64_t *>(buf->MutableData());
 
        uint64_t num = dataInfos.size();
        uint64_t descSz = sizeof(uint64_t) * (num + preOccupySize);
        descSz = (descSz + dataAlignSize - 1) / dataAlignSize * dataAlignSize;
 
        prefixSumArr[0] = dataInfos.size();
        prefixSumArr[1] = descSz;
        for (uint64_t j = 0; j < dataInfos.size(); j++) {
            prefixSumArr[j + preOccupySize] = prefixSumArr[j + 1] + dataInfos[j].size;
        }
    }
}
 
size_t GetComposedBufferSize(void* mutableData)
{
    auto offsetArrPtr = reinterpret_cast<uint64_t *>(mutableData);
    if (offsetArrPtr) {
        return *offsetArrPtr;
    }
    return 0;
}
}
}
