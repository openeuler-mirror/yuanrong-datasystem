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

#ifndef DATASYSTEM_COMMON_DEVICE_DEVICE_BATCH_COPY_HELPER_H
#define DATASYSTEM_COMMON_DEVICE_DEVICE_BATCH_COPY_HELPER_H

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <numeric>
#include <vector>

#include "datasystem/common/device/device_manager_base.h"
#include "datasystem/common/device/device_resource_manager.h"
#include "datasystem/common/object_cache/buffer_composer.h"
#include "datasystem/object/buffer.h"
#include "datasystem/utils/device_blob.h"

namespace datasystem {

struct BufferView {
    void *ptr;
    size_t size;
};

struct DeviceBatchCopyHelper {
public:
    static bool is64BitAligned(void *ptr)
    {
        constexpr uintptr_t alignmentMask = 0x7;
        uintptr_t address = reinterpret_cast<uintptr_t>(ptr);
        return (address & alignmentMask) == 0;
    }

    void Reset()
    {
        batchSize = 0;
        dataSizeList.clear();
        srcList.clear();
        dstList.clear();
        srcBuffers.clear();
        dstBuffers.clear();
        bufferMetas.clear();
    }

    Status Prepare(const std::vector<DeviceBlobList> &devBlobList, std::vector<Buffer *> &bufferList,
                   MemcpyKind copyKind)
    {
        Reset();
        CHECK_FAIL_RETURN_STATUS(!devBlobList.empty(), K_INVALID, "The devBlobList is empty.");
        CHECK_FAIL_RETURN_STATUS(!bufferList.empty(), K_INVALID, "The bufferList is empty.");
        CHECK_FAIL_RETURN_STATUS(devBlobList.size() == bufferList.size(), K_INVALID,
                                 FormatString("The devBlobList size %zu is not equal to bufferList size %zu",
                                              devBlobList.size(), bufferList.size()));
        return PrepareImpl(devBlobList.size(), [&devBlobList](size_t index) {
            return devBlobList[index];
        }, bufferList, copyKind);
    }

    /**
     * @brief Pointer-reference preparation path. Identical semantics to Prepare(), but reads DeviceBlobList
     * by const pointer instead of by value, so callers that already hold a request-owned vector (synchronous
     * MGetH2D/MSetD2H view path, async ownership copy) never copy a DeviceBlobList or Blob.
     *
     * Direction-neutral: copyKind selects which side is source/destination (matching Prepare's swap).
     * H2D callers pass HOST_TO_DEVICE; D2H callers pass DEVICE_TO_HOST.
     *
     * Populates all six helper fields, matching Prepare(), because DIRECT, FFTS fallback, parallel-direct and
     * CUDA sync paths all consume the flat arrays. The shared metadata-prefix parser validates the physical prefix
     * size and every offset before forming host views, so malformed remote metadata cannot underflow or escape the
     * received buffer.
     */
    Status PrepareRefs(const std::vector<const DeviceBlobList *> &deviceBlobRefs,
                       const std::vector<Buffer *> &bufferList, MemcpyKind copyKind)
    {
        Reset();
        CHECK_FAIL_RETURN_STATUS(!deviceBlobRefs.empty(), K_INVALID, "The devBlobList is empty.");
        CHECK_FAIL_RETURN_STATUS(!bufferList.empty(), K_INVALID, "The bufferList is empty.");
        CHECK_FAIL_RETURN_STATUS(deviceBlobRefs.size() == bufferList.size(), K_INVALID,
                                 FormatString("The devBlobList size %zu is not equal to bufferList size %zu",
                                              deviceBlobRefs.size(), bufferList.size()));
        CHECK_FAIL_RETURN_STATUS(deviceBlobRefs.front() != nullptr, K_INVALID, "deviceBlobRefs[0] is null");
        const auto expectedDeviceIdx = deviceBlobRefs.front()->deviceIdx;
        for (size_t i = 0; i < deviceBlobRefs.size(); i++) {
            CHECK_FAIL_RETURN_STATUS(deviceBlobRefs[i] != nullptr, K_INVALID,
                                     FormatString("deviceBlobRefs[%zu] is null", i));
            CHECK_FAIL_RETURN_STATUS(deviceBlobRefs[i]->deviceIdx == expectedDeviceIdx, K_INVALID,
                                     FormatString("The deviceIdx of deviceBlobRefs[%zu](%d) is not the same as "
                                                  "the first deviceIdx(%d)",
                                                  i, deviceBlobRefs[i]->deviceIdx, expectedDeviceIdx));
        }
        return PrepareImpl(deviceBlobRefs.size(), [&deviceBlobRefs](size_t index) {
            return *deviceBlobRefs[index];
        }, bufferList, copyKind);
    }

    void PrintGetPerfInfo(DeviceBatchCopyHelper &helper)
    {
        if (FLAGS_v < 1 || helper.bufferMetas.empty()) {
            return;
        }
        object_cache::BlobListInfo infoList;
        infoList.keyNums = helper.bufferMetas.size();
        int64_t blobSum =
            std::accumulate(helper.bufferMetas.begin(), helper.bufferMetas.end(), int64_t{ 0 },
                            [](int64_t total, const BufferMetaInfo &view) { return total + view.blobCount; });
        infoList.minBlobNums = std::min_element(helper.bufferMetas.begin(), helper.bufferMetas.end(),
                                                [](const BufferMetaInfo &view1, const BufferMetaInfo &view2) {
                                                    return view1.blobCount < view2.blobCount;
                                                })
                                   ->blobCount;
        infoList.maxBlobNums = std::max_element(helper.bufferMetas.begin(), helper.bufferMetas.end(),
                                                [](const BufferMetaInfo &view1, const BufferMetaInfo &view2) {
                                                    return view1.blobCount < view2.blobCount;
                                                })
                                   ->blobCount;
        infoList.avgBlobNums = blobSum / infoList.keyNums;

        infoList.totalSize = std::accumulate(helper.srcBuffers.begin(), helper.srcBuffers.end(), int64_t{ 0 },
                                             [](int64_t total, const BufferView &view) { return total + view.size; });
        infoList.minBlockSize =
            std::min_element(helper.srcBuffers.begin(), helper.srcBuffers.end(),
                             [](const BufferView &view1, const BufferView &view2) { return view1.size < view2.size; })
                ->size;
        infoList.maxBlockSize =
            std::max_element(helper.srcBuffers.begin(), helper.srcBuffers.end(),
                             [](const BufferView &view1, const BufferView &view2) { return view1.size < view2.size; })
                ->size;
        infoList.avgBlockSize = infoList.totalSize / infoList.keyNums;
        VLOG(1) << infoList.ToString(false);
    }

    size_t batchSize = 0;
    std::vector<size_t> dataSizeList;
    std::vector<void *> srcList;
    std::vector<void *> dstList;
    std::vector<BufferView> srcBuffers;
    std::vector<BufferView> dstBuffers;
    std::vector<BufferMetaInfo> bufferMetas;

private:
    // Bundles the flat arrays the per-object parse loop appends to, so PrepareObjectEntry stays under the codecheck
    // function-size limit without threading seven references through the call.
    struct PrepareAccumulators {
        std::vector<BufferMetaInfo> &bufferMetas;
        std::vector<BufferView> &hostBuffers;
        std::vector<BufferView> &deviceBuffers;
        std::vector<void *> &hostPointerList;
        std::vector<void *> &devPointerList;
        std::vector<size_t> &dataSizeList;
        size_t &batchSize;
    };

    // Parses one composed buffer's metadata prefix and per-blob offsets, validates the device/host size match, and
    // appends the resulting views and pointers to the flat arrays. Extracted from PrepareImpl to keep that function
    // within the codecheck 50-line limit; both value and reference inputs share this path.
    template <typename BlobListGetter>
    static Status PrepareObjectEntry(size_t i, const std::vector<Buffer *> &bufferList,
                                     BlobListGetter blobListGetter, size_t keyStartInBlobs,
                                     PrepareAccumulators acc)
    {
        const auto &blobs = blobListGetter(i).blobs;
        if (bufferList[i] == nullptr) {
            return Status::OK();
        }
        auto &buffer = bufferList[i];
        auto rawData = buffer->MutableData();
        CHECK_FAIL_RETURN_STATUS(rawData != nullptr, K_INVALID,
                                 FormatString("bufferList[%zu] data is null", i));
        const auto rawBufferSize = buffer->GetSize();
        CHECK_FAIL_RETURN_STATUS(rawBufferSize >= 0, K_INVALID,
                                 FormatString("bufferList[%zu] size is negative: %ld", i, rawBufferSize));
        const size_t bufferSize = static_cast<size_t>(rawBufferSize);
        uint8_t *hostRawPointer = nullptr;
        uint64_t *offsets = nullptr;
        size_t dataSize = 0;
        RETURN_IF_NOT_OK(ParseComposedBufferPrefix(i, rawData, blobs, bufferSize, hostRawPointer, offsets, dataSize));
        acc.bufferMetas.emplace_back(
            BufferMetaInfo{ .blobCount = blobs.size(), .firstBlobOffset = keyStartInBlobs, .size = dataSize });
        acc.hostBuffers.emplace_back(BufferView{ .ptr = hostRawPointer + offsets[0], .size = dataSize });
        return AppendBlobEntries(i, blobs, bufferSize, hostRawPointer, offsets, acc);
    }

    // Validates the composed-buffer prefix header (count + first offset), returning the host base pointer, the
    // offset table, and the contiguous data size. Extracted to keep PrepareObjectEntry within the size limit.
    static Status ParseComposedBufferPrefix(size_t i, void *rawData, const std::vector<Blob> &blobs,
                                            size_t bufferSize, uint8_t *&hostRawPointer, uint64_t *&offsets,
                                            size_t &dataSize)
    {
        constexpr size_t kPrefixFieldCount = 2;
        CHECK_FAIL_RETURN_STATUS(
            blobs.size() <= (std::numeric_limits<size_t>::max() / sizeof(uint64_t)) - kPrefixFieldCount,
            K_INVALID, "Composed buffer prefix size overflows size_t");
        const auto prefixBytes = (blobs.size() + kPrefixFieldCount) * sizeof(uint64_t);
        CHECK_FAIL_RETURN_STATUS(bufferSize >= prefixBytes, K_INVALID,
                                 FormatString("Composed buffer prefix size %zu exceeds buffer size %zu, index %zu",
                                              prefixBytes, bufferSize, i));
        auto offsetArrPtr = reinterpret_cast<uint64_t *>(rawData);
        hostRawPointer = reinterpret_cast<uint8_t *>(rawData);
        auto sz = *offsetArrPtr;
        offsets = offsetArrPtr + 1;
        CHECK_FAIL_RETURN_STATUS(
            sz == blobs.size() && sz > 0, K_INVALID,
            FormatString("Blobs count mismatch in devBlobList between sender and receiver, sender count is: %ld, "
                         "receiver count is: %ld, mismatch devBlobList index: %zu, mismatch key index: %zu",
                         sz, blobs.size(), i, i));
        CHECK_FAIL_RETURN_STATUS(offsets[0] >= prefixBytes && offsets[0] <= bufferSize, K_INVALID,
                                 FormatString("Invalid first blob offset %lu for buffer size %zu, index %zu",
                                              offsets[0], bufferSize, i));
        dataSize = bufferSize - offsets[0];
        return Status::OK();
    }

    // Walks each blob's offset range, validates monotonicity and device/host size equality, and appends the
    // per-blob views and pointers to the flat arrays. Extracted to keep PrepareObjectEntry within the size limit.
    static Status AppendBlobEntries(size_t i, const std::vector<Blob> &blobs, size_t bufferSize,
                                    uint8_t *hostRawPointer, uint64_t *offsets, PrepareAccumulators acc)
    {
        for (size_t j = 0; j < blobs.size(); j++) {
            CHECK_FAIL_RETURN_STATUS(offsets[j] <= offsets[j + 1] && offsets[j + 1] <= bufferSize, K_INVALID,
                                     FormatString("Invalid blob offsets at object index %zu, blob index %zu",
                                                  i, j));
            auto hostDataSize = offsets[j + 1] - offsets[j];
            auto devicePointer = blobs[j].pointer;
            auto deviceDataSize = blobs[j].size;
            auto hostPointer = hostRawPointer + offsets[j];
            if (!is64BitAligned(hostPointer)) {
                LOG(WARNING) << "host memory is not 64 aligned: " << hostRawPointer;
            }
            if (!is64BitAligned(devicePointer)) {
                LOG(WARNING) << "deivce memory is not 64 aligned: " << devicePointer;
            }
            CHECK_FAIL_RETURN_STATUS(static_cast<size_t>(hostDataSize) == deviceDataSize, K_RUNTIME_ERROR,
                                     "The data size of device and host is not equal.");
            acc.deviceBuffers.emplace_back(BufferView{ .ptr = devicePointer, .size = hostDataSize });
            acc.hostPointerList.emplace_back(hostPointer);
            acc.devPointerList.emplace_back(devicePointer);
            acc.dataSizeList.emplace_back(hostDataSize);
            acc.batchSize++;
        }
        return Status::OK();
    }

    template <typename BlobListGetter>
    Status PrepareImpl(size_t objectCount, BlobListGetter blobListGetter, const std::vector<Buffer *> &bufferList,
                       MemcpyKind copyKind)
    {
        std::vector<void *> hostPointerList;
        std::vector<void *> devPointerList;
        std::vector<BufferView> hostBuffers;
        std::vector<BufferView> deviceBuffers;
        CHECK_FAIL_RETURN_STATUS(objectCount == bufferList.size(), K_INVALID,
                                 FormatString("The devBlobList size %zu is not equal to bufferList size %zu",
                                              objectCount, bufferList.size()));

        // Pre-pass: compute exact sizes once so the flat arrays allocate without reallocation growth. Keeping this
        // traversal and the metadata parse below in one implementation makes value and reference inputs follow the
        // same validation, offset handling, and copy-kind assignment rules.
        size_t nonNullObjectCount = 0;
        size_t totalBlobCount = 0;
        for (size_t i = 0; i < objectCount; i++) {
            if (bufferList[i] == nullptr) {
                continue;
            }
            nonNullObjectCount++;
            const auto blobCount = blobListGetter(i).blobs.size();
            CHECK_FAIL_RETURN_STATUS(totalBlobCount <= SIZE_MAX - blobCount, K_INVALID,
                                     "Total blob count overflows size_t");
            totalBlobCount += blobCount;
        }
        // srcList/dstList are move-assigned from the temporary pointer lists below, so only reserve the containers
        // that are built incrementally here.
        bufferMetas.reserve(nonNullObjectCount);
        dataSizeList.reserve(totalBlobCount);
        hostBuffers.reserve(nonNullObjectCount);
        deviceBuffers.reserve(totalBlobCount);
        hostPointerList.reserve(totalBlobCount);
        devPointerList.reserve(totalBlobCount);

        size_t keyStartInBlobs = 0;
        PrepareAccumulators acc{ bufferMetas,        hostBuffers,        deviceBuffers,
                                 hostPointerList,   devPointerList,     dataSizeList,
                                 batchSize };
        for (size_t i = 0; i < objectCount; i++) {
            RETURN_IF_NOT_OK(PrepareObjectEntry(i, bufferList, blobListGetter, keyStartInBlobs, acc));
            if (bufferList[i] != nullptr) {
                keyStartInBlobs += blobListGetter(i).blobs.size();
            }
        }
        return AssignCopyKind(copyKind, hostBuffers, deviceBuffers, hostPointerList, devPointerList);
    }

    // Moves the gathered host/device buffers and pointers into src/dst by copy direction. Extracted to keep
    // PrepareImpl within the codecheck 50-line limit.
    Status AssignCopyKind(MemcpyKind copyKind, std::vector<BufferView> &hostBuffers,
                           std::vector<BufferView> &deviceBuffers, std::vector<void *> &hostPointerList,
                           std::vector<void *> &devPointerList)
    {
        if (copyKind == MemcpyKind::HOST_TO_DEVICE) {
            srcBuffers = std::move(hostBuffers);
            dstBuffers = std::move(deviceBuffers);
            srcList = std::move(hostPointerList);
            dstList = std::move(devPointerList);
        } else if (copyKind == MemcpyKind::DEVICE_TO_HOST) {
            srcBuffers = std::move(deviceBuffers);
            dstBuffers = std::move(hostBuffers);
            srcList = std::move(devPointerList);
            dstList = std::move(hostPointerList);
        } else {
            RETURN_STATUS(K_INVALID, "Invalid MemcpyKind");
        }
        return Status::OK();
    }
};

}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_DEVICE_DEVICE_BATCH_COPY_HELPER_H
