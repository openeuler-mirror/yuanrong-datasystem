/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
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

#include "datasystem/client/object_cache/device/page_attn_utils.h"

#include "datasystem/common/util/status_helper.h"

namespace datasystem {

Status PageAttnUtils::CheckLayerTensorShape(const std::vector<uint64_t> &layerTensorShape)
{
    if (layerTensorShape.empty()) {
        RETURN_STATUS(K_INVALID, "The tensor shape is empty");
    }

    if (layerTensorShape.size() <= 1 || layerTensorShape.size() > MAX_TENSOR_SHAPE_LENGTH) {
        RETURN_STATUS(K_INVALID, "LayerTensorShape out of expected range 2-" + std::to_string(MAX_TENSOR_SHAPE_LENGTH));
    }

    auto max_shape_length = std::max_element(layerTensorShape.begin() + 1, layerTensorShape.end());
    if (*max_shape_length > MAX_BLOCK_DIM) {
        RETURN_STATUS(K_INVALID, "Block dimension exceeds" + std::to_string(MAX_BLOCK_DIM));
    }

    return Status::OK();
}

Blob PageAttnUtils::Blk2Blob(uint64_t ptr, size_t elemSize, size_t numBlockElem, uint32_t blockId)
{
    size_t blockSize = elemSize * numBlockElem;
    intptr_t address = ptr + (blockSize * blockId);
    void *pointer = reinterpret_cast<void *>(address);
    return Blob{ .pointer = pointer, .size = blockSize };
}

DeviceBlobList PageAttnUtils::Blks2DevBlobList(int32_t deviceIdx, uint64_t ptr, size_t elemSize, size_t numBlockElem,
                                               const std::vector<uint32_t> &blockIds)
{
    std::vector<Blob> blobs(blockIds.size());
    for (size_t i = 0; i < blockIds.size(); ++i) {
        blobs[i] = Blk2Blob(ptr, elemSize, numBlockElem, blockIds[i]);
    }

    return DeviceBlobList{ .blobs = blobs, deviceIdx = deviceIdx };
}

Status PageAttnUtils::LayerwiseDevBlobLists(int32_t deviceIdx, const std::vector<Tensor> &layerTensors,
                                            const std::vector<uint32_t> &blockIds,
                                            std::vector<DeviceBlobList> &outDevBlobLists)
{
    size_t oriSize = outDevBlobLists.size();
    outDevBlobLists.resize(oriSize + layerTensors.size());
    // Process each layer independently
    for (size_t i = 0; i < layerTensors.size(); ++i) {
        const Tensor &layer = layerTensors[i];
        RETURN_IF_NOT_OK(CheckLayerTensorShape(layer.shape));

        size_t numBlockElem = 1;
        // Compute elements per block: product of all dims except first (num_blocks)
        for (size_t j = 1; j < layer.shape.size(); ++j) {
            numBlockElem *= layer.shape[j];
        }

        outDevBlobLists[oriSize + i] = Blks2DevBlobList(deviceIdx, layer.ptr, layer.elemSize, numBlockElem, blockIds);
    }

    return Status::OK();
}

Status PageAttnUtils::BlockwiseDevBlobLists(int32_t deviceIdx, const std::vector<Tensor> &layerTensors,
                                            const std::vector<uint32_t> &blockIds,
                                            std::vector<DeviceBlobList> &outDevBlobLists)
{
    std::vector<size_t> numBlockElems(layerTensors.size());
    for (size_t j = 0; j < layerTensors.size(); ++j) {
        const Tensor &layer = layerTensors[j];
        RETURN_IF_NOT_OK(CheckLayerTensorShape(layer.shape));

        size_t numBlockElem = 1;
        // Compute elements per block: product of all dims except first
        for (size_t k = 1; k < layer.shape.size(); ++k) {
            numBlockElem *= layer.shape[k];
        }

        numBlockElems[j] = numBlockElem;
    }

    size_t oriSize = outDevBlobLists.size();
    outDevBlobLists.resize(oriSize + blockIds.size());
    // Process each block independently
    for (size_t i = 0; i < blockIds.size(); ++i) {
        std::vector<Blob> blobs(layerTensors.size());
        // Extract Blob from each layer for current blockId
        for (size_t j = 0; j < layerTensors.size(); ++j) {
            const Tensor &layer = layerTensors[j];
            blobs[j] = Blk2Blob(layer.ptr, layer.elemSize, numBlockElems[j], blockIds[i]);
        }

        outDevBlobLists[oriSize + i] = DeviceBlobList{ .blobs = blobs, .deviceIdx = deviceIdx };
    }

    return Status::OK();
}

}  // namespace datasystem