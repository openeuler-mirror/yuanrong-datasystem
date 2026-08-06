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
#include <functional>
#include <gtest/gtest.h>
#include <limits>
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
    std::vector<const DeviceBlobList *> refs{ &blobList };

    ASSERT_TRUE(ComposeBufferDataRefs(refs, buffers, alignment).IsOk());

    const auto *prefix = static_cast<const uint64_t *>(buffer->MutableData());
    EXPECT_EQ(prefix[0], blobList.blobs.size());
    EXPECT_EQ(prefix[1], alignment);
    EXPECT_EQ(prefix[2], alignment + blob0Size);
    EXPECT_EQ(prefix[3], alignment + blob0Size + blob1Size);
}

namespace {
// Build a Buffer whose backing memory is calloc'd and whose dataSize is exactly objectSize. The Buffer
// owns the calloc'd pointer and frees it in ~Buffer(); OwnedBuffer only holds the shared_ptr so the
// backing memory is released exactly once.
struct OwnedBuffer {
    std::shared_ptr<Buffer> buffer;
    ~OwnedBuffer()
    {
        buffer.reset();
    }
    std::shared_ptr<Buffer> Make(uint64_t objectSize)
    {
        auto bufferInfo = std::make_shared<ObjectBufferInfo>();
        bufferInfo->pointer = static_cast<uint8_t *>(std::calloc(1, objectSize));
        bufferInfo->dataSize = objectSize;
        bufferInfo->metadataSize = 0;
        buffer = std::make_shared<Buffer>();
        buffer->bufferInfo_ = std::move(bufferInfo);
        return buffer;
    }
};

DeviceBlobList MakeBlobList(std::initializer_list<uint64_t> sizes)
{
    DeviceBlobList list;
    list.blobs.reserve(sizes.size());
    for (auto s : sizes) {
        list.blobs.push_back({ nullptr, s });
    }
    return list;
}
}  // namespace

TEST(BufferComposerTest, ComposeBufferDataRefsWritesPrefixForSingleObject)
{
    constexpr uint32_t alignment = 64;
    const auto blobList = MakeBlobList({ 100, 200, 300 });
    const auto headerSize = GetComposedBufferHeaderSize(blobList.blobs.size(), alignment);
    const auto objectSize = headerSize + 100 + 200 + 300;
    OwnedBuffer refOwned;
    auto refBuffer = refOwned.Make(objectSize);
    std::vector<std::shared_ptr<Buffer>> refBuffers{ refBuffer };
    std::vector<const DeviceBlobList *> refs{ &blobList };

    auto status = ComposeBufferDataRefs(refs, refBuffers, alignment);
    ASSERT_TRUE(status.IsOk()) << status.ToString();

    const auto *prefix = static_cast<const uint64_t *>(refBuffer->MutableData());
    EXPECT_EQ(prefix[0], blobList.blobs.size());
    EXPECT_EQ(prefix[1], GetComposedBufferHeaderSize(blobList.blobs.size(), alignment));
    EXPECT_EQ(prefix[2], prefix[1] + 100);
    EXPECT_EQ(prefix[3], prefix[2] + 200);
    EXPECT_EQ(prefix[4], prefix[3] + 300);
}

TEST(BufferComposerTest, ComposeBufferDataRefsWritesPrefixForMultipleObjects)
{
    constexpr uint32_t alignment = 64;
    const auto firstBlobList = MakeBlobList({ 100, 200 });
    const auto secondBlobList = MakeBlobList({ 50, 75, 125 });
    const auto firstHeaderSize = GetComposedBufferHeaderSize(firstBlobList.blobs.size(), alignment);
    const auto secondHeaderSize = GetComposedBufferHeaderSize(secondBlobList.blobs.size(), alignment);

    OwnedBuffer firstOwned;
    OwnedBuffer secondOwned;
    auto firstBuffer = firstOwned.Make(firstHeaderSize + 100 + 200);
    auto secondBuffer = secondOwned.Make(secondHeaderSize + 50 + 75 + 125);
    std::vector<std::shared_ptr<Buffer>> buffers{ firstBuffer, secondBuffer };
    std::vector<const DeviceBlobList *> refs{ &firstBlobList, &secondBlobList };

    ASSERT_TRUE(ComposeBufferDataRefs(refs, buffers, alignment).IsOk());

    const auto *firstPrefix = static_cast<const uint64_t *>(firstBuffer->MutableData());
    EXPECT_EQ(firstPrefix[0], firstBlobList.blobs.size());
    EXPECT_EQ(firstPrefix[1], firstHeaderSize);
    EXPECT_EQ(firstPrefix[2], firstHeaderSize + 100);
    EXPECT_EQ(firstPrefix[3], firstHeaderSize + 100 + 200);

    const auto *secondPrefix = static_cast<const uint64_t *>(secondBuffer->MutableData());
    EXPECT_EQ(secondPrefix[0], secondBlobList.blobs.size());
    EXPECT_EQ(secondPrefix[1], secondHeaderSize);
    EXPECT_EQ(secondPrefix[2], secondHeaderSize + 50);
    EXPECT_EQ(secondPrefix[3], secondHeaderSize + 50 + 75);
    EXPECT_EQ(secondPrefix[4], secondHeaderSize + 50 + 75 + 125);
}

TEST(BufferComposerTest, ComposeBufferDataRefsRejectsNullRef)
{
    constexpr uint32_t alignment = 64;
    const auto blobList = MakeBlobList({ 100 });
    OwnedBuffer owned;
    auto buffer = owned.Make(GetComposedBufferHeaderSize(1, alignment) + 100);
    std::vector<std::shared_ptr<Buffer>> buffers{ buffer };
    std::vector<const DeviceBlobList *> refs{ nullptr };

    auto status = ComposeBufferDataRefs(refs, buffers, alignment);
    EXPECT_FALSE(status.IsOk());
}

TEST(BufferComposerTest, ComposeBufferDataRefsRejectsCountMismatch)
{
    constexpr uint32_t alignment = 64;
    const auto blobList = MakeBlobList({ 100 });
    OwnedBuffer owned;
    auto buffer = owned.Make(GetComposedBufferHeaderSize(1, alignment) + 100);
    std::vector<std::shared_ptr<Buffer>> buffers{ buffer, buffer };
    std::vector<const DeviceBlobList *> refs{ &blobList };  // 1 ref, 2 buffers

    auto status = ComposeBufferDataRefs(refs, buffers, alignment);
    EXPECT_FALSE(status.IsOk());
}

TEST(BufferComposerTest, ComposeBufferDataRefsRejectsEmptyBlobList)
{
    constexpr uint32_t alignment = 64;
    DeviceBlobList blobList;  // empty blobs
    OwnedBuffer owned;
    auto buffer = owned.Make(GetComposedBufferHeaderSize(0, alignment));
    std::vector<std::shared_ptr<Buffer>> buffers{ buffer };
    std::vector<const DeviceBlobList *> refs{ &blobList };

    auto status = ComposeBufferDataRefs(refs, buffers, alignment);
    EXPECT_FALSE(status.IsOk());
}

TEST(BufferComposerTest, ComposeBufferDataRefsRejectsUndersizedBuffer)
{
    constexpr uint32_t alignment = 64;
    const auto blobList = MakeBlobList({ 100, 200 });
    // Allocate less than header + 100 + 200.
    OwnedBuffer owned;
    auto buffer = owned.Make(GetComposedBufferHeaderSize(2, alignment) + 100);
    std::vector<std::shared_ptr<Buffer>> buffers{ buffer };
    std::vector<const DeviceBlobList *> refs{ &blobList };

    auto status = ComposeBufferDataRefs(refs, buffers, alignment);
    EXPECT_FALSE(status.IsOk());
}

TEST(BufferComposerTest, ComposeBufferDataRefsRejectsSizeOverflow)
{
    constexpr uint32_t alignment = 64;
    DeviceBlobList blobList;
    blobList.blobs.push_back({ nullptr, std::numeric_limits<uint64_t>::max() });
    OwnedBuffer owned;
    // Overflow is detected while summing descriptor sizes; no huge host allocation is required.
    auto buffer = owned.Make(GetComposedBufferHeaderSize(1, alignment));
    std::vector<std::shared_ptr<Buffer>> buffers{ buffer };
    std::vector<const DeviceBlobList *> refs{ &blobList };

    auto status = ComposeBufferDataRefs(refs, buffers, alignment);
    EXPECT_FALSE(status.IsOk());
}

TEST(BufferComposerTest, PrepareDataSizeListAcceptsEmptyBlobListForExistenceFiltering)
{
    // MSetD2H performs the existence check after sizing. Empty descriptors remain valid placeholders when the key
    // already exists; new keys are rejected later by the payload validator before the device copy.
    DeviceBlobList first;
    std::vector<size_t> sizeList;
    BlobListInfo blobInfo;
    auto status = PrepareDataSizeList(sizeList, { first }, blobInfo, 64);
    ASSERT_TRUE(status.IsOk()) << status.ToString();
    ASSERT_EQ(sizeList.size(), 1);
    EXPECT_EQ(sizeList[0], GetComposedBufferHeaderSize(0, 64));
}

TEST(BufferComposerTest, PrepareDataSizeListRejectsOversizeBlob)
{
    DeviceBlobList blobList;
    blobList.blobs.push_back({ nullptr, static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) + 1 });
    std::vector<size_t> sizeList;
    BlobListInfo blobInfo;
    auto status = PrepareDataSizeList(sizeList, { blobList }, blobInfo, 64);
    EXPECT_FALSE(status.IsOk());
}

TEST(BufferComposerTest, PrepareDataSizeListRejectsInvalidAlignment)
{
    const auto blobList = MakeBlobList({ 100 });
    std::vector<size_t> sizeList;
    BlobListInfo blobInfo;

    EXPECT_FALSE(PrepareDataSizeList(sizeList, { blobList }, blobInfo, 0).IsOk());
    EXPECT_FALSE(PrepareDataSizeList(sizeList, { blobList }, blobInfo, 3).IsOk());
}

TEST(BufferComposerTest, ComposeBufferDataRefsRejectsNullBuffer)
{
    const auto blobList = MakeBlobList({ 100 });
    std::vector<std::shared_ptr<Buffer>> buffers{ nullptr };
    std::vector<const DeviceBlobList *> refs{ &blobList };

    auto status = ComposeBufferDataRefs(refs, buffers, 64);
    EXPECT_FALSE(status.IsOk());
}

TEST(BufferComposerTest, ComposeBufferDataRefsRejectsNullBufferData)
{
    const auto blobList = MakeBlobList({ 100 });
    auto bufferInfo = std::make_shared<ObjectBufferInfo>();
    bufferInfo->pointer = nullptr;
    bufferInfo->dataSize = GetComposedBufferHeaderSize(1, 64) + 100;
    auto buffer = std::make_shared<Buffer>();
    buffer->bufferInfo_ = std::move(bufferInfo);
    std::vector<std::shared_ptr<Buffer>> buffers{ buffer };
    std::vector<const DeviceBlobList *> refs{ &blobList };

    auto status = ComposeBufferDataRefs(refs, buffers, 64);
    EXPECT_FALSE(status.IsOk());
}

}  // namespace
}  // namespace object_cache
}  // namespace datasystem
