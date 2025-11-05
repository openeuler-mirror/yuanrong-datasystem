
/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2024. All rights reserved.
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
 * Description: Definition of heterogeneous device tensor description structure.
 */

#ifndef DATASYSTEM_HETERO_DEVICE_COMMON_H
#define DATASYSTEM_HETERO_DEVICE_COMMON_H

#include <string>
#include <vector>

namespace datasystem {
struct Blob {
    void *pointer = 0;
    uint64_t size = 0;
};

struct DeviceBlobList {
    // Constraint: The blobs under the same DeviceBlobList need to be under the same card.
    std::vector<Blob> blobs;
    int32_t deviceIdx = -1;
    int32_t srcOffset = 0;  // Sender's Data Starting Offset (Bytes)
};

struct Tensor {
    uint64_t ptr;
    uint32_t elemSize;
    std::vector<uint64_t> shape;
};
}  // namespace datasystem
#endif