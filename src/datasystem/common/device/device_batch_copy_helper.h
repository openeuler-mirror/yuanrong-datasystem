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
#include <numeric>
#include <vector>

#include "datasystem/common/device/device_manager_base.h"
#include "datasystem/common/device/device_resource_manager.h"
#include "datasystem/common/object_cache/buffer_composer.h"

namespace datasystem {

struct BufferView {
    void *ptr;
    size_t size;
};

struct DeviceBatchCopyHelper {
    bool is64BitAligned(void *ptr)
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
        std::vector<void *> hostPointerList;
        std::vector<void *> devPointerList;
        std::vector<BufferView> hostBuffers;
        std::vector<BufferView> deviceBuffers;
        hostBuffers.reserve(devBlobList.size());
        deviceBuffers.reserve(devBlobList.size());
        CHECK_FAIL_RETURN_STATUS(!devBlobList.empty(), K_INVALID, "The devBlobList is empty.");
        CHECK_FAIL_RETURN_STATUS(!bufferList.empty(), K_INVALID, "The bufferList is empty.");
        CHECK_FAIL_RETURN_STATUS(devBlobList.size() == bufferList.size(), K_INVALID,
                                 FormatString("The devBlobList size %zu is not equal to bufferList size %zu",
                                              devBlobList.size(), bufferList.size()));
        size_t keyStartInBlobs = 0;
        for (size_t i = 0; i < devBlobList.size(); i++) {
            auto &blobs = devBlobList[i].blobs;
            if (bufferList[i] == nullptr) {
                continue;
            }
            auto &buffer = bufferList[i];
            auto offsetArrPtr = reinterpret_cast<uint64_t *>(buffer->MutableData());
            auto hostRawPointer = reinterpret_cast<uint8_t *>(buffer->MutableData());
            auto sz = *offsetArrPtr;
            auto offsets = offsetArrPtr + 1;
            CHECK_FAIL_RETURN_STATUS(
                sz == blobs.size() && sz > 0, K_INVALID,
                FormatString("Blobs count mismatch in devBlobList between sender and receiver, sender count is: %ld, "
                             "receiver count is: %ld, mismatch devBlobList index: %zu, mismatch key index: %zu",
                             sz, blobs.size(), i, i));
            size_t dataSize = buffer->GetSize() - offsets[0];
            bufferMetas.emplace_back(
                BufferMetaInfo{ .blobCount = blobs.size(), .firstBlobOffset = keyStartInBlobs, .size = dataSize });
            hostBuffers.emplace_back(BufferView{ .ptr = hostRawPointer + offsets[0], .size = dataSize });
            for (size_t j = 0; j < blobs.size(); j++) {
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
                deviceBuffers.emplace_back(BufferView{ .ptr = devicePointer, .size = hostDataSize });
                hostPointerList.emplace_back(hostPointer);
                devPointerList.emplace_back(devicePointer);
                dataSizeList.emplace_back(hostDataSize);
                batchSize++;
            }
            keyStartInBlobs += blobs.size();
        }
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

    void PrintGetPerfInfo(DeviceBatchCopyHelper &helper)
    {
        if (helper.bufferMetas.empty()) {
            return;
        }
        object_cache::BlobListInfo infoList;
        infoList.keyNums = helper.bufferMetas.size();
        int64_t blobSum =
            std::accumulate(helper.bufferMetas.begin(), helper.bufferMetas.end(), 0,
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

        infoList.totalSize = std::accumulate(helper.srcBuffers.begin(), helper.srcBuffers.end(), 0,
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
        LOG(INFO) << infoList.ToString(false);
    }

    size_t batchSize = 0;
    std::vector<size_t> dataSizeList;
    std::vector<void *> srcList;
    std::vector<void *> dstList;
    std::vector<BufferView> srcBuffers;
    std::vector<BufferView> dstBuffers;
    std::vector<BufferMetaInfo> bufferMetas;
};

}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_DEVICE_DEVICE_BATCH_COPY_HELPER_H
