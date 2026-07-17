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

#include <cstdlib>
#include <cstdint>
#include <cstring>
#include <functional>
#include <gtest/gtest.h>
#include <memory>
#include <unordered_set>
#include <vector>

#define private public
#include "datasystem/object/buffer.h"
#undef private

#include "datasystem/common/object_cache/buffer_composer.h"
#include "datasystem/common/object_cache/object_base.h"

namespace datasystem {
namespace object_cache {
namespace {

TEST(BufferComposerTest, UsesMemoryAlignmentForComposedHeaderAndObjectSize)
{
    constexpr size_t blob0Size = 1024;
    constexpr size_t blob1Size = 2048;
    DeviceBlobList blobList;
    blobList.blobs = { { nullptr, blob0Size }, { nullptr, blob1Size } };

    for (const auto alignment : { uint32_t{ 64 }, uint32_t{ 4096 } }) {
        std::vector<size_t> sizeList;
        BlobListInfo blobInfo;

        auto status = PrepareDataSizeList(sizeList, { blobList }, blobInfo, alignment);

        ASSERT_TRUE(status.IsOk()) << status.ToString();
        ASSERT_EQ(sizeList.size(), 1);
        EXPECT_EQ(GetComposedBufferHeaderSize(blobList.blobs.size(), alignment), alignment);
        EXPECT_EQ(sizeList[0], alignment + blob0Size + blob1Size);
        EXPECT_EQ(blobInfo.totalSize, blob0Size + blob1Size);
    }
}

TEST(BufferComposerTest, RoundsLargeDescriptorHeaderToMemoryAlignment)
{
    EXPECT_EQ(GetComposedBufferHeaderSize(7, 64), 128);
}

TEST(BufferComposerTest, ComputesBlobStatisticsAcrossObjects)
{
    DeviceBlobList firstBlobList;
    firstBlobList.blobs = { { nullptr, 1024 }, { nullptr, 2048 } };
    DeviceBlobList secondBlobList;
    secondBlobList.blobs = { { nullptr, 512 } };
    std::vector<size_t> sizeList;
    BlobListInfo blobInfo;

    ASSERT_TRUE(PrepareDataSizeList(sizeList, { firstBlobList, secondBlobList }, blobInfo, 64).IsOk());

    EXPECT_EQ(blobInfo.nonExistNums, 2);
    EXPECT_EQ(blobInfo.totalSize, 3584);
    EXPECT_EQ(blobInfo.minBlobNums, 1);
    EXPECT_EQ(blobInfo.maxBlobNums, 2);
    EXPECT_EQ(blobInfo.avgBlobNums, 1);
    EXPECT_EQ(blobInfo.minBlockSize, 512);
    EXPECT_EQ(blobInfo.maxBlockSize, 2048);
    EXPECT_EQ(blobInfo.avgBlockSize, 1194);
}

TEST(BufferComposerTest, WritesAlignedPayloadOffsets)
{
    constexpr uint32_t alignment = 4096;
    constexpr size_t blob0Size = 1024;
    constexpr size_t blob1Size = 2048;
    DeviceBlobList blobList;
    blobList.blobs = { { nullptr, blob0Size }, { nullptr, blob1Size } };

    const auto objectSize = GetComposedBufferHeaderSize(blobList.blobs.size(), alignment) + blob0Size + blob1Size;
    auto bufferInfo = std::make_shared<ObjectBufferInfo>();
    bufferInfo->pointer = static_cast<uint8_t *>(std::calloc(1, objectSize));
    ASSERT_NE(bufferInfo->pointer, nullptr);
    bufferInfo->dataSize = objectSize;
    bufferInfo->metadataSize = 0;
    auto buffer = std::make_shared<Buffer>();
    buffer->bufferInfo_ = std::move(bufferInfo);
    std::vector<std::shared_ptr<Buffer>> buffers{ buffer };

    ComposeBufferData(buffers, { blobList }, alignment);

    const auto *prefix = static_cast<const uint64_t *>(buffer->MutableData());
    EXPECT_EQ(prefix[0], blobList.blobs.size());
    EXPECT_EQ(prefix[1], alignment);
    EXPECT_EQ(prefix[2], alignment + blob0Size);
    EXPECT_EQ(prefix[3], alignment + blob0Size + blob1Size);
}

}  // namespace
}  // namespace object_cache
}  // namespace datasystem
