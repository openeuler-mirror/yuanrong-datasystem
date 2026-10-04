/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2022. All rights reserved.
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
 * Description: Mmap table class test.
 */
#include "datasystem/client/mmap_manager/immap_table.h"

#include <algorithm>
#include <tuple>

#ifdef __linux__
#include <linux/memfd.h>
#include <linux/magic.h>
#endif
#include <sys/mman.h>
#include <sys/syscall.h>
#include <sys/vfs.h>

#include "datasystem/client/mmap_manager/shm_mmap_table.h"
#include "datasystem/client/mmap_manager/shm_mmap_table_entry.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/raii.h"
#include "common/binmock/binmock.h"
#include "ut/common.h"

using namespace datasystem::client;

namespace datasystem {
namespace ut {
class MmapTableTest : public CommonTest {
public:
    void SetUp() override
    {
        mmapTable_ = std::make_unique<ShmMmapTable>(false, std::make_shared<HostMemoryPinManager>());
        int32_t mmapSize = 1024;
        clientFd1_ = CreateFd(mmapSize);
        clientFd2_ = CreateFd(mmapSize);
        ASSERT_GE(clientFd1_, 0);
        ASSERT_GE(clientFd2_, 0);
    };

    // every TEST_F macro will call TearDown when end
    void TearDown() override
    {
        close(clientFd1_);
        clientFd1_ = 0;
        close(clientFd2_);
        clientFd2_ = 0;
    };

    int32_t CreateFd(int32_t size)
    {
        std::string tmpfs = "MmapTableTest";
        int32_t fd = syscall(SYS_memfd_create, tmpfs.c_str(), MFD_ALLOW_SEALING);
        int ret = ftruncate(fd, static_cast<off_t>(size));
        if (ret != 0) {
            return -1;
        }
        return fd;
    }

    int32_t clientFd1_;
    int32_t clientFd2_;
    std::unique_ptr<IMmapTable> mmapTable_;
};

TEST_F(MmapTableTest, TestMmapTableBasicFunction)
{
    LOG(INFO) << "Test mmap table basic function.";

    // Add to the mmapTable.
    int fakeFd = -1; // fake fd is -1
    int workerFd = 10; // worker fd is 10
    DS_ASSERT_OK(mmapTable_->MmapAndStoreFd(clientFd1_, workerFd, 1024, ""));  // size is 1024
    DS_ASSERT_OK(mmapTable_->MmapAndStoreFd(clientFd2_, workerFd, 1024, ""));  // size is 1024

    uint8_t *pointer;
    DS_ASSERT_OK(mmapTable_->LookupFdPointer(workerFd, &pointer));
    DS_ASSERT_NOT_OK(mmapTable_->LookupFdPointer(fakeFd, &pointer));

    auto existed = mmapTable_->FindFd(workerFd);
    ASSERT_EQ(existed, true);
    existed = mmapTable_->FindFd(fakeFd);
    ASSERT_EQ(existed, false);
}

TEST_F(MmapTableTest, TestMmapRejectsNullHostMemoryPinManager)
{
    ShmMmapTable mmapTable(false, nullptr);
    auto rc = mmapTable.MmapAndStoreFd(clientFd1_, 10, 1024, "");
    EXPECT_EQ(rc.GetCode(), StatusCode::K_RUNTIME_ERROR);
}

TEST_F(MmapTableTest, TestMmapTableEntryInvalidParameter)
{
    LOG(INFO) << "Test mmap table entry invalid parameter.";

    int fakeFd = -1;
    auto entry = std::make_unique<ShmMmapTableEntry>(clientFd1_, 0);
    Status status = entry->Init(false, "");
    ASSERT_EQ(status.GetCode(), StatusCode::K_INVALID);

    auto entry1 = std::make_unique<ShmMmapTableEntry>(fakeFd, 5120);
    status = entry1->Init(false, "");
    ASSERT_EQ(status.GetCode(), StatusCode::K_RUNTIME_ERROR);
}

TEST_F(MmapTableTest, TestGetMmapEntry)
{
    LOG(INFO) << "Test mmap table decrease mmap ref.";

    int workerFd1 = 10;
    int workerFd2 = 11;
    int32_t mmapSize = 1024;
    DS_ASSERT_OK(mmapTable_->MmapAndStoreFd(clientFd1_, workerFd1, mmapSize, ""));
    DS_ASSERT_OK(mmapTable_->MmapAndStoreFd(clientFd2_, workerFd2, mmapSize, ""));
    ASSERT_TRUE(mmapTable_->FindFd(workerFd1));
    ASSERT_TRUE(mmapTable_->FindFd(workerFd2));

    ASSERT_TRUE(mmapTable_->GetMmapEntryByFd(workerFd1) != nullptr);
    ASSERT_TRUE(mmapTable_->GetMmapEntryByFd(workerFd2) != nullptr);

    ASSERT_TRUE(mmapTable_->FindFd(workerFd1));
    ASSERT_TRUE(mmapTable_->FindFd(workerFd2));

    auto fds = mmapTable_->GetFds();
    ASSERT_EQ(fds.size(), 2);
    EXPECT_NE(std::find(fds.begin(), fds.end(), workerFd1), fds.end());
    EXPECT_NE(std::find(fds.begin(), fds.end(), workerFd2), fds.end());

    mmapTable_->ClearExpiredFds({ workerFd1 });
    ASSERT_FALSE(mmapTable_->FindFd(workerFd1));
    ASSERT_TRUE(mmapTable_->FindFd(workerFd2));

    mmapTable_->CleanInvalidMmapTable();
    ASSERT_FALSE(mmapTable_->FindFd(workerFd2));
}

TEST_F(MmapTableTest, TestCudaMemcpySegmentsFollowPinFragmentBoundaries)
{
    const size_t pageSize = static_cast<size_t>(sysconf(_SC_PAGESIZE));
    constexpr size_t pinSliceSize = 64UL * 1024UL * 1024UL;
    const size_t mmapSize = pinSliceSize * 2 + pageSize;
    int fd = CreateFd(static_cast<int32_t>(mmapSize));
    ASSERT_GE(fd, 0);
    ShmMmapTableEntry entry(fd, mmapSize);
    DS_ASSERT_OK(entry.Init(false, ""));

    std::vector<size_t> segmentSizes;
    DS_ASSERT_OK(entry.GetMemcpySegmentSizes(entry.Pointer(), mmapSize, segmentSizes));
    ASSERT_EQ(segmentSizes.size(), 3);
    EXPECT_EQ(segmentSizes[0], pinSliceSize);
    EXPECT_EQ(segmentSizes[1], pinSliceSize);
    EXPECT_EQ(segmentSizes[2], pageSize);

    segmentSizes.clear();
    // Start one page before a slice boundary and cross one complete slice into a third slice.
    const auto *copyStart = entry.Pointer() + pinSliceSize - pageSize;
    DS_ASSERT_OK(entry.GetMemcpySegmentSizes(copyStart, pinSliceSize + pageSize * 2, segmentSizes));
    ASSERT_EQ(segmentSizes.size(), 3);
    EXPECT_EQ(segmentSizes[0], pageSize);
    EXPECT_EQ(segmentSizes[1], pinSliceSize);
    EXPECT_EQ(segmentSizes[2], pageSize);

    segmentSizes.clear();
    auto status = entry.GetMemcpySegmentSizes(entry.Pointer() + mmapSize - pageSize, pageSize * 2, segmentSizes);
    EXPECT_EQ(status.GetCode(), StatusCode::K_INVALID);
}

namespace {
constexpr size_t HUGE_PAGE_2_MIB = 2UL * 1024UL * 1024UL;
constexpr size_t HUGE_PAGE_1_GIB = 1024UL * 1024UL * 1024UL;
constexpr size_t UNMAP_SLICE_SIZE = 512UL * 1024UL * 1024UL;
}

class HugeTlbMmapTableTest : public MmapTableTest,
                           public testing::WithParamInterface<std::tuple<size_t, size_t, std::vector<size_t>, bool>> {};

TEST_P(HugeTlbMmapTableTest, UnmapUsesActualPageSize)
{
    const auto pageSize = std::get<0>(GetParam());
    const auto logicalSize = std::get<1>(GetParam());
    const auto &expectedSizes = std::get<2>(GetParam());
    const bool skipSleep = std::get<3>(GetParam());
    constexpr char skipSleepInject[] = "ShmMmapTableEntry.UnmapMemory.skipSleep";
    constexpr char beforeSleepInject[] = "ShmMmapTableEntry.UnmapMemory.beforeSleep";
    Raii clearInject([&] {
        (void)inject::Clear(skipSleepInject);
        (void)inject::Clear(beforeSleepInject);
    });
    DS_ASSERT_OK(inject::Set(beforeSleepInject, "call()"));
    if (skipSleep) {
        DS_ASSERT_OK(inject::Set(skipSleepInject, "call()"));
    }
    using testing::_;
    auto *base = reinterpret_cast<uint8_t *>(HUGE_PAGE_1_GIB);
    Raii releaseStubs([] { RELEASE_STUBS });
    BINEXPECT_CALL(&fstatfs, (_, _)).WillOnce(testing::Invoke([&](int, struct statfs *info) {
        info->f_type = HUGETLBFS_MAGIC;
        info->f_bsize = pageSize;
        return 0;
    }));
    BINEXPECT_CALL(&mmap, (_, _, _, _, _, _)).WillRepeatedly(testing::Invoke(
        [](void *address, size_t length, int prot, int flags, int fd, off_t offset) {
            return reinterpret_cast<void *>(syscall(SYS_mmap, address, length, prot, flags, fd, offset));
        }));
    BINEXPECT_CALL(&mmap, (_, logicalSize, _, _, clientFd1_, _)).WillOnce(testing::Return(base));
    BINEXPECT_CALL(&madvise, (_, _, _)).WillOnce(testing::Return(0));
    size_t offset = 0;
    size_t calls = 0;
    BINEXPECT_CALL(&munmap, (_, _)).WillRepeatedly(testing::Invoke([](void *address, size_t length) {
        return static_cast<int>(syscall(SYS_munmap, address, length));
    }));
    const auto inMapping = testing::Truly([=](void *address) {
        const auto value = reinterpret_cast<uintptr_t>(address);
        return value >= HUGE_PAGE_1_GIB && value < HUGE_PAGE_1_GIB + logicalSize;
    });
    BINEXPECT_CALL(&munmap, (inMapping, _)).Times(expectedSizes.size()).WillRepeatedly(testing::Invoke(
        [&](void *address, size_t length) {
            EXPECT_EQ(address, base + offset);
            EXPECT_EQ(reinterpret_cast<uintptr_t>(address) % pageSize, 0);
            EXPECT_EQ(length % pageSize, 0);
            EXPECT_EQ(length, expectedSizes.at(calls++));
            offset += length;
            return 0;
        }));
    {
        ShmMmapTableEntry entry(clientFd1_, logicalSize);
        DS_ASSERT_OK(entry.Init(true, ""));
        EXPECT_EQ(entry.GetMmapSize(), logicalSize);
        clientFd1_ = -1;
    }
    EXPECT_EQ(calls, expectedSizes.size());
    EXPECT_EQ(offset, logicalSize);
    EXPECT_EQ(inject::GetExecuteCount(beforeSleepInject), skipSleep ? 0 : expectedSizes.size() - 1);
}

INSTANTIATE_TEST_SUITE_P(
    PageAlignment, HugeTlbMmapTableTest,
    testing::Values(
        std::make_tuple(HUGE_PAGE_2_MIB, UNMAP_SLICE_SIZE + HUGE_PAGE_2_MIB,
                        std::vector<size_t>{ UNMAP_SLICE_SIZE, HUGE_PAGE_2_MIB }, false),
        std::make_tuple(HUGE_PAGE_2_MIB, UNMAP_SLICE_SIZE * 2 + HUGE_PAGE_2_MIB,
                        std::vector<size_t>{ UNMAP_SLICE_SIZE, UNMAP_SLICE_SIZE, HUGE_PAGE_2_MIB }, false),
        std::make_tuple(HUGE_PAGE_1_GIB, HUGE_PAGE_1_GIB * 2,
                        std::vector<size_t>{ HUGE_PAGE_1_GIB, HUGE_PAGE_1_GIB }, false),
        std::make_tuple(HUGE_PAGE_1_GIB, HUGE_PAGE_1_GIB, std::vector<size_t>{ HUGE_PAGE_1_GIB }, false),
        std::make_tuple(HUGE_PAGE_2_MIB, HUGE_PAGE_2_MIB, std::vector<size_t>{ HUGE_PAGE_2_MIB }, false),
        std::make_tuple(HUGE_PAGE_2_MIB, UNMAP_SLICE_SIZE * 2 + HUGE_PAGE_2_MIB,
                        std::vector<size_t>{ UNMAP_SLICE_SIZE, UNMAP_SLICE_SIZE, HUGE_PAGE_2_MIB }, true)));

TEST_F(MmapTableTest, HugeTlbRejectsInvalidPageSizeBeforeMapping)
{
    using testing::_;
    Raii releaseStubs([] { RELEASE_STUBS });
    BINEXPECT_CALL(&fstatfs, (_, _))
        .WillOnce(testing::Return(-1))
        .WillOnce(testing::Invoke([](int, struct statfs *info) {
            info->f_type = 0;
            info->f_bsize = HUGE_PAGE_2_MIB;
            return 0;
        }))
        .WillOnce(testing::Invoke([](int, struct statfs *info) {
            info->f_type = HUGETLBFS_MAGIC;
            info->f_bsize = 0;
            return 0;
        }));
    for (int attempt = 0; attempt < 3; ++attempt) {
        ShmMmapTableEntry entry(clientFd1_, HUGE_PAGE_2_MIB);
        DS_ASSERT_NOT_OK(entry.Init(true, ""));
        EXPECT_EQ(entry.Pointer(), nullptr);
    }
}

class MmapUnmapFailureTest : public MmapTableTest,
                            public testing::WithParamInterface<std::tuple<size_t, bool>> {};

TEST_P(MmapUnmapFailureTest, CleansOnlyStillOwnedSuffix)
{
    using testing::_;
    const auto failedFragment = std::get<0>(GetParam());
    const auto cleanupFails = std::get<1>(GetParam());
    constexpr char beforeSleepInject[] = "ShmMmapTableEntry.UnmapMemory.beforeSleep";
    Raii clearInject([&] { (void)inject::Clear(beforeSleepInject); });
    DS_ASSERT_OK(inject::Set(beforeSleepInject, "call()"));
    const size_t pageSize = static_cast<size_t>(sysconf(_SC_PAGESIZE));
    const size_t mmapSize = UNMAP_SLICE_SIZE * 2 + pageSize;
    const size_t failedOffset = failedFragment * UNMAP_SLICE_SIZE;
    int fd = CreateFd(static_cast<int32_t>(mmapSize));
    ASSERT_GE(fd, 0);
    auto entry = std::make_unique<ShmMmapTableEntry>(fd, mmapSize);
    DS_ASSERT_OK(entry->Init(false, ""));
    auto *base = const_cast<uint8_t *>(entry->Pointer());
    size_t prefixSize = 0;
    size_t calls = 0;
    Raii cleanup([&] {
        if (prefixSize > 0) {
            syscall(SYS_munmap, base, prefixSize);
        }
        if (cleanupFails) {
            syscall(SYS_munmap, base + failedOffset, mmapSize - failedOffset);
        }
    });
    {
        Raii releaseStubs([] { RELEASE_STUBS });
        BINEXPECT_CALL(&munmap, (_, _)).WillRepeatedly(testing::Invoke([](void *address, size_t length) {
            return static_cast<int>(syscall(SYS_munmap, address, length));
        }));
        const auto inMapping = testing::Truly([=](void *address) {
            auto value = reinterpret_cast<uintptr_t>(address);
            return value >= reinterpret_cast<uintptr_t>(base)
                   && value - reinterpret_cast<uintptr_t>(base) < mmapSize;
        });
        BINEXPECT_CALL(&munmap, (inMapping, _)).Times(failedFragment + 2).WillRepeatedly(testing::Invoke(
            [&](void *address, size_t length) {
                const size_t attempt = calls++;
                if (attempt < failedFragment) {
                    EXPECT_EQ(address, base + attempt * UNMAP_SLICE_SIZE);
                    EXPECT_EQ(length, UNMAP_SLICE_SIZE);
                    int ret = static_cast<int>(syscall(SYS_munmap, address, length));
                    EXPECT_EQ(ret, 0);
                    void *reused = mmap(address, length, PROT_READ | PROT_WRITE,
                                        MAP_PRIVATE | MAP_ANONYMOUS | MAP_FIXED_NOREPLACE, -1, 0);
                    EXPECT_EQ(reused, address);
                    if (reused == address) {
                        prefixSize += length;
                        *static_cast<uint8_t *>(reused) = 0x5a;
                    }
                    return ret;
                }
                EXPECT_EQ(address, base + failedOffset);
                EXPECT_EQ(length, attempt == failedFragment
                                      ? std::min(UNMAP_SLICE_SIZE, mmapSize - failedOffset)
                                      : mmapSize - failedOffset);
                if (attempt == failedFragment || cleanupFails) {
                    errno = ENOMEM;
                    return -1;
                }
                return static_cast<int>(syscall(SYS_munmap, address, length));
            }));
        entry.reset();
    }
    EXPECT_EQ(calls, failedFragment + 2);
    EXPECT_EQ(inject::GetExecuteCount(beforeSleepInject), failedFragment);
    unsigned char state = 0;
    for (size_t offset = 0; offset < prefixSize; offset += UNMAP_SLICE_SIZE) {
        ASSERT_EQ(mincore(base + offset, pageSize, &state), 0);
        EXPECT_EQ(base[offset], 0x5a);
    }
    for (size_t offset : { failedOffset, mmapSize - pageSize }) {
        int ret = mincore(base + offset, pageSize, &state);
        EXPECT_EQ(ret, cleanupFails ? 0 : -1);
        if (!cleanupFails) {
            EXPECT_EQ(errno, ENOMEM);
        }
    }
}

INSTANTIATE_TEST_SUITE_P(UnmapFailure, MmapUnmapFailureTest,
                         testing::Values(std::make_tuple(size_t{ 0 }, false), std::make_tuple(size_t{ 1 }, false),
                                         std::make_tuple(size_t{ 2 }, false), std::make_tuple(size_t{ 1 }, true)));

}  // namespace ut
}  // namespace datasystem
