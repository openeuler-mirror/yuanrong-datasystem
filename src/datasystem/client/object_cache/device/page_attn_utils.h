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

/**
 * Description: The methods of PagedAttention.
 */
#ifndef DATASYSTEM_CLIENT_OBJECT_CACHE_PAGE_ATTN_UTILS
#define DATASYSTEM_CLIENT_OBJECT_CACHE_PAGE_ATTN_UTILS

#include <algorithm>

#include "datasystem/hetero/device_common.h"
#include "datasystem/utils/status.h"

namespace datasystem {

constexpr int MAX_TENSOR_SHAPE_LENGTH = 4;
constexpr int MAX_BLOCK_DIM = 10240;

class __attribute__((visibility("default"))) PageAttnUtils {
public:
    PageAttnUtils() = delete;

    /// \brief Validates the shape of a layer tensor.
    /// \param[in] layerTensorShape - Shape of the tensor, e.g., [num_blocks, dim1, dim2, ...]
    /// \return Status::OK() if shape is valid.
    ///         Otherwise, returns K_INVALID status with message if:
    ///         - shape is empty
    ///         - size not in [2, MAX_TENSOR_SHAPE_LENGTH]
    ///         - any dimension (except first) exceeds MAX_BLOCK_DIM
    static Status CheckLayerTensorShape(const std::vector<uint64_t> &layerTensorShape);

    /// \brief Convert a block into a Blob object.
    /// \param[in] ptr - Base memory address of the layer tensor.
    /// \param[in] elemSize - Byte size of a single element (e.g., 4 for fp32, 2 for fp16).
    /// \param[in] numBlockElem - Number of elements in one block; product of all dimensions in layer tensor shape
    /// except the first (which is total block count).
    /// \param[in] blockId - Block ID.
    /// \return Returns a Blob object representing the memory segment of the specified block.
    static Blob Blk2Blob(uint64_t ptr, size_t elemSize, size_t numBlockElem, uint32_t blockId);

    /// \brief Convert a list of blocks from a single layer into a DeviceBlobList object.
    /// \param[in] deviceIdx - Target device ID.
    /// \param[in] ptr - Base memory address of the layer tensor.
    /// \param[in] elemSize - Byte size of a single element (e.g., 4 for fp32, 2 for fp16).
    /// \param[in] numBlockElem - Number of elements in one block; product of all dimensions in layer tensor shape
    /// except the first (which is total block count).
    /// \param[in] blockIds - List of block ID.
    /// \return Returns a DeviceBlobList object representing the memory segments of the specified block list.
    static DeviceBlobList Blks2DevBlobList(int32_t deviceIdx, uint64_t ptr, size_t elemSize, size_t numBlockElem,
                                           const std::vector<uint32_t> &blockIds);

    /// \brief Generate a collection of DeviceBlobList for each layer — contains Blobs of all blockIds in that layer.
    /// \param[in] deviceIdx - Target device index.
    /// \param[in] layerTensors - List of layer tensors.
    /// \param[in] blockIds - List of block ID.
    /// \param[out] outDevBlobLists - Output collection of DeviceBlobList objects.
    ///                               Length equals layerTensors.size(); outDevBlobLists[i] corresponds to
    ///                               layerTensors[i].
    /// \return K_OK on any object success; the error code otherwise.
    static Status LayerwiseDevBlobLists(int32_t deviceIdx, const std::vector<Tensor> &layerTensors,
                                        const std::vector<uint32_t> &blockIds,
                                        std::vector<DeviceBlobList> &outDevBlobLists);

    /// \brief Generate a collection of DeviceBlobList for each blockId — contains Blob from every layer for that block.
    /// \param[in] deviceIdx - Target device index.
    /// \param[in] layerTensors - List of layer tensors.
    /// \param[in] blockIds - List of block ID.
    /// \param[out] outDevBlobLists - Output collection of DeviceBlobList objects.
    ///                               Length equals blockIds.size(); outDevBlobLists[i] contains Blobs from all layers
    ///                               for blockIds[i].
    /// \return K_OK on any object success; the error code otherwise.
    static Status BlockwiseDevBlobLists(int32_t deviceIdx, const std::vector<Tensor> &layerTensors,
                                        const std::vector<uint32_t> &blockIds,
                                        std::vector<DeviceBlobList> &outDevBlobLists);
};
}  // namespace datasystem
#endif