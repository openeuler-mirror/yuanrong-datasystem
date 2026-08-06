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
namespace {

Status GetComposedBufferHeaderSizeChecked(size_t blobCount, uint32_t memoryAlignment, uint64_t &headerSize)
{
    CHECK_FAIL_RETURN_STATUS(memoryAlignment > 0 && (memoryAlignment & (memoryAlignment - 1)) == 0, K_INVALID,
                             FormatString("memoryAlignment %u must be a non-zero power of two", memoryAlignment));
    constexpr uint64_t kHeaderFieldCount = 2;
    constexpr uint64_t kFieldSize = sizeof(uint64_t);
    constexpr uint64_t kUint64Max = std::numeric_limits<uint64_t>::max();
    CHECK_FAIL_RETURN_STATUS(blobCount <= kUint64Max / kFieldSize - kHeaderFieldCount, K_INVALID,
                             "Composed buffer header size overflows uint64_t");
    const auto rawHeaderSize = (static_cast<uint64_t>(blobCount) + kHeaderFieldCount) * kFieldSize;
    const auto alignmentPadding = static_cast<uint64_t>(memoryAlignment) - 1;
    CHECK_FAIL_RETURN_STATUS(rawHeaderSize <= kUint64Max - alignmentPadding, K_INVALID,
                             "Aligned composed buffer header size overflows uint64_t");
    headerSize = (rawHeaderSize + alignmentPadding) & ~alignmentPadding;
    return Status::OK();
}

}  // namespace

uint64_t GetComposedBufferHeaderSize(size_t blobCount, uint32_t memoryAlignment)
{
    const auto alignment = static_cast<uint64_t>(memoryAlignment);
    const auto headerSize = sizeof(uint64_t) * (blobCount + 2);
    return (headerSize + alignment - 1) & ~(alignment - 1);
}

Status PrepareDataSizeList(std::vector<size_t> &sizeList, const std::vector<DeviceBlobList> &devBlobList,
                           BlobListInfo &blobInfo, uint32_t memoryAlignment)
{
    constexpr auto kInt64Max = std::numeric_limits<int64_t>::max();
    CHECK_FAIL_RETURN_STATUS(devBlobList.size() <= static_cast<size_t>(kInt64Max), K_INVALID,
                             "DeviceBlobList count exceeds int64_t limit");
    blobInfo.nonExistNums = static_cast<int64_t>(devBlobList.size());
    blobInfo.totalSize = 0;
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
        uint64_t sz = 0;
        RETURN_IF_NOT_OK(GetComposedBufferHeaderSizeChecked(num, memoryAlignment, sz));

        CHECK_FAIL_RETURN_STATUS(info.size() <= static_cast<size_t>(kInt64Max), K_INVALID,
                                 "Blob count exceeds int64_t limit");
        const auto blobNum = static_cast<int64_t>(info.size());
        CHECK_FAIL_RETURN_STATUS(blobNumSum <= kInt64Max - blobNum, K_INVALID,
                                 "Total blob count exceeds int64_t limit");
        blobNumSum += blobNum;
        blobInfo.minBlobNums = std::min(blobInfo.minBlobNums, blobNum);
        blobInfo.maxBlobNums = std::max(blobInfo.maxBlobNums, blobNum);
        for (auto &desc : info) {
            // Guard against header+payload overflow; report an error rather than wrap around (an over-large
            // object would later overflow the prefix-sum or buffer size).
            constexpr uint64_t kUint64Max = std::numeric_limits<uint64_t>::max();
            if (sz > kUint64Max - desc.size) {
                return Status(K_INVALID, "Composed buffer size overflows uint64_t");
            }
            sz += desc.size;
            // BlobListInfo.totalSize is int64_t. A single blob larger than INT64_MAX bytes is not a valid
            // input; reject it rather than performing an implementation-defined narrowing cast (which would
            // risk signed-overflow UB in the saturation check below).
            constexpr uint64_t kInt64MaxAsUint = static_cast<uint64_t>(kInt64Max);
            if (desc.size > kInt64MaxAsUint) {
                return Status(K_INVALID, "blob size exceeds int64_t limit");
            }
            // Saturate the running total at INT64_MAX using an unsigned comparison (totalSize + desc.size
            // computed in uint64_t, then bounded before the int64_t store).
            const uint64_t newTotal = static_cast<uint64_t>(blobInfo.totalSize) + desc.size;
            blobInfo.totalSize = (newTotal > kInt64MaxAsUint) ? kInt64Max : static_cast<int64_t>(newTotal);
            const auto blobSize = static_cast<int64_t>(desc.size);  // safe: bounded above to <= INT64_MAX
            blobInfo.minBlockSize = std::min(blobInfo.minBlockSize, blobSize);
            blobInfo.maxBlockSize = std::max(blobInfo.maxBlockSize, blobSize);
            ++blobSizeCount;
        }

        sizeList.emplace_back(sz);
    }
    blobInfo.avgBlobNums = blobNumSum / blobInfo.nonExistNums;
    // Empty lists are retained as existence-check placeholders; avoid dividing by zero when a request contains only
    // such entries.
    blobInfo.avgBlockSize = (blobSizeCount > 0) ? blobInfo.totalSize / blobSizeCount : 0;

    return Status::OK();
}

Status ComposeBufferDataRefs(const std::vector<const DeviceBlobList *> &deviceBlobRefs,
                             const std::vector<std::shared_ptr<Buffer>> &bufferList, uint32_t memoryAlignment)
{
    // Record MetaData of SubBuffers.
    // | NumOfBuffers (n) | Off0 | Off1 | Offn | Padding | Buf1 | Buf2 | ... Bufn |
    constexpr uint64_t preOccupySize = 2;
    CHECK_FAIL_RETURN_STATUS(!deviceBlobRefs.empty(), K_INVALID, "The deviceBlobRefs is empty.");
    CHECK_FAIL_RETURN_STATUS(!bufferList.empty(), K_INVALID, "The bufferList is empty.");
    CHECK_FAIL_RETURN_STATUS(deviceBlobRefs.size() == bufferList.size(), K_INVALID,
                             FormatString("The deviceBlobRefs size %zu is not equal to bufferList size %zu",
                                          deviceBlobRefs.size(), bufferList.size()));
    for (uint64_t i = 0; i < bufferList.size(); i++) {
        CHECK_FAIL_RETURN_STATUS(deviceBlobRefs[i] != nullptr, K_INVALID,
                                 FormatString("deviceBlobRefs[%zu] is null", i));
        const auto &blobs = deviceBlobRefs[i]->blobs;
        auto &buf = bufferList[i];
        CHECK_FAIL_RETURN_STATUS(buf != nullptr, K_INVALID, FormatString("bufferList[%zu] is null", i));
        CHECK_FAIL_RETURN_STATUS(blobs.size() > 0, K_INVALID, FormatString("The blobs list is empty, index: %zu", i));
        auto *bufferData = buf->MutableData();
        CHECK_FAIL_RETURN_STATUS(bufferData != nullptr, K_INVALID, FormatString("bufferList[%zu] data is null", i));
        auto prefixSumArr = reinterpret_cast<uint64_t *>(bufferData);
        uint64_t descSz = 0;
        RETURN_IF_NOT_OK(GetComposedBufferHeaderSizeChecked(blobs.size(), memoryAlignment, descSz));
        // Validate that the composed object (header + all blob payloads) fits in the allocated buffer.
        uint64_t composedSize = descSz;
        constexpr uint64_t kUint64Max = std::numeric_limits<uint64_t>::max();
        for (const auto &blob : blobs) {
            CHECK_FAIL_RETURN_STATUS(composedSize <= kUint64Max - blob.size, K_INVALID,
                                     "Composed buffer size overflows uint64_t");
            composedSize += blob.size;
        }
        const auto bufferSize = static_cast<uint64_t>(buf->GetSize());
        CHECK_FAIL_RETURN_STATUS(composedSize <= bufferSize, K_INVALID,
                                 FormatString("Composed size %zu exceeds allocated buffer size %zu, index %zu",
                                              composedSize, bufferSize, i));

        prefixSumArr[0] = blobs.size();
        prefixSumArr[1] = descSz;
        for (uint64_t j = 0; j < blobs.size(); j++) {
            // Running prefix-sum offset; overflow already guarded by the composed-size check above.
            prefixSumArr[j + preOccupySize] = prefixSumArr[j + 1] + blobs[j].size;
        }
    }
    return Status::OK();
}
}  // namespace object_cache
}  // namespace datasystem
