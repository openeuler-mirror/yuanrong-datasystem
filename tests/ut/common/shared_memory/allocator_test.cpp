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
 * Description: allocator class test.
 */
#include "datasystem/common/shared_memory/allocator.h"

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_set>
#include <vector>
#include "datasystem/common/string_intern/string_ref.h"
#include "gtest/gtest.h"

#define JEMALLOC_NO_DEMANGLE
#include <jemalloc/jemalloc.h>
#undef JEMALLOC_NO_DEMANGLE

#include "common.h"
#include "datasystem/common/constants.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/perf/perf_manager.h"
#include "datasystem/common/rpc/rpc_message.h"
#include "datasystem/common/shared_memory/arena.h"
#include "datasystem/common/shared_memory/arena_group_key.h"
#include "datasystem/common/shared_memory/jemalloc.h"
#include "datasystem/common/shared_memory/mmap/allocation.h"
#include "datasystem/common/shared_memory/shm_unit.h"
#include "datasystem/common/shared_memory/shm_unit_info.h"
#include "datasystem/common/util/file_util.h"
#include "datasystem/common/util/random_data.h"
#include "datasystem/common/util/thread_pool.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/common/util/validator.h"
#include "datasystem/common/util/wait_post.h"
#include "datasystem/common/log/log.h"

using datasystem::memory::Allocator;
using datasystem::memory::ArenaGroup;

using namespace ::testing;

DS_DECLARE_uint32(arena_per_tenant);
DS_DECLARE_uint32(shared_disk_arena_per_tenant);
DS_DECLARE_bool(enable_huge_tlb);
DS_DECLARE_string(shared_disk_directory);

namespace datasystem {
namespace ut {

struct AllocatorConfig {
    uint64_t shmSize = 1;
    uint64_t shdSize = 0;
    bool populate = false;
    bool scaling = true;
    ssize_t decayMs = 5'000;
    int objectThreshold = 100;
    int streamThreshold = 100;
    ServiceType serviceType = ServiceType::OBJECT;
    memory::CacheType cacheType = memory::CacheType::MEMORY;

    AllocatorConfig() = default;

    AllocatorConfig(uint64_t size) : shmSize(size)
    {
    }

    AllocatorConfig(uint64_t size, memory::CacheType type) : shmSize(size), shdSize(size), cacheType(type)
    {
    }
};

class AllocatorTest : public CommonTest {
public:
    void TearDown() override
    {
        datasystem::memory::Allocator::Instance()->Shutdown();
    }

    Status Init(const AllocatorConfig &config)
    {
        config_ = config;
        if (config_.cacheType == memory::CacheType::DISK) {
            std::string dir = GetTestCaseDataDir() + "/shared_disk/";
            FLAGS_shared_disk_directory = dir.c_str();
        }
        return datasystem::memory::Allocator::Instance()->Init(config_.shmSize, config_.shdSize, config_.populate,
                                                               config_.scaling, config_.decayMs,
                                                               config_.objectThreshold, config_.streamThreshold);
    }

    uint64_t MaxSize()
    {
        return config_.cacheType == memory::CacheType::MEMORY ? config_.shmSize : config_.shdSize;
    }

    Status AllocateMemory(uint64_t needSize, ShmUnitInfo &unit, const std::string &tenantId = DEFAULT_TENANT_ID)
    {
        return datasystem::memory::Allocator::Instance()->AllocateMemory(
            tenantId, needSize, config_.populate, unit.pointer, unit.fd, unit.offset, unit.mmapSize,
            config_.serviceType, config_.cacheType);
    }

    Status AllocateMemory(ShmUnit &unit, uint64_t needSize, const std::string &tenantId = DEFAULT_TENANT_ID)
    {
        return unit.AllocateMemory(tenantId, needSize, config_.populate, config_.serviceType, config_.cacheType);
    }

    Status FreeMemory(void *&pointer, const std::string &tenantId = DEFAULT_TENANT_ID)
    {
        return datasystem::memory::Allocator::Instance()->FreeMemory(tenantId, pointer, config_.serviceType,
                                                                     config_.cacheType);
    }

    Status FreeMemory(ShmUnit &unit)
    {
        return unit.FreeMemory();
    }

    Status FdToPointer(int fd, std::pair<void *, uint64_t> &ptrMmapSz, const std::string &tenantId = DEFAULT_TENANT_ID)
    {
        return datasystem::memory::Allocator::Instance()->FdToPointer({ tenantId, config_.cacheType }, fd, ptrMmapSz);
    }

    Status CreateArenaGroup(uint64_t maxSize, std::shared_ptr<ArenaGroup> &arenaGroup,
                            const std::string &tenantId = DEFAULT_TENANT_ID)
    {
        return datasystem::memory::Allocator::Instance()->CreateArenaGroup(tenantId, maxSize, arenaGroup,
                                                                           config_.cacheType);
    }

    Status DestroyArenaGroup(const std::string &tenantId = DEFAULT_TENANT_ID)
    {
        return datasystem::memory::Allocator::Instance()->DestroyArenaGroup({ tenantId, config_.cacheType });
    }

    uint64_t GetMemoryUsage(const std::string &tenantId = DEFAULT_TENANT_ID)
    {
        return datasystem::memory::Allocator::Instance()->GetMemoryUsage(tenantId, config_.cacheType);
    }

    uint64_t GetMaxMemorySize(ServiceType serviceType = ServiceType::OBJECT)
    {
        return datasystem::memory::Allocator::Instance()->GetMaxMemorySize(serviceType, config_.cacheType);
    }

protected:
    // Test Function
    void TestParallelAllocate();
    void TestAllocateMemoryWithDefaultParam();
    void TestAllocateManyPages();
    void TestAllocateDifferSz();
    void TestAllocatedAddresses();
    void TestDoubleFree();
    void TestArenaBasicFunction();
    void TestAllocMemByteAlign();
    void TestCreateArenaConcurrently();
    void TestShmUnits1();
    void TestShmUnits2();
    void TestUsedupAndFree();
    void TestUsedupAndFree2();
    void TestCreateTenantArena();
    void TestCreateMultiTenantArena();
    void TestCreateTenantArenaConcurrent();
    void FakeAllocate();

    static constexpr int SEC_PER_MIN = 60;
    AllocatorConfig config_;
};

static bool IsGoodAllocate(const std::pair<void *, uint64_t> &addr1, const std::pair<void *, uint64_t> &addr2)
{
    auto pHead1 = reinterpret_cast<uintptr_t>(addr1.first);
    auto pTail1 = reinterpret_cast<uintptr_t>(addr1.first) + static_cast<ptrdiff_t>(addr1.second) - 1;
    auto pHead2 = reinterpret_cast<uintptr_t>(addr2.first);
    auto pTail2 = reinterpret_cast<uintptr_t>(addr2.first) + static_cast<ptrdiff_t>(addr2.second) - 1;

    return pHead1 > pTail2 || pTail1 < pHead2;
}

static void ResetShmUnit(ShmUnit &shmUnit)
{
    shmUnit.pointer = nullptr;
    shmUnit.fd = -1;
    shmUnit.offset = 0;
    shmUnit.mmapSize = 0;
    shmUnit.size = 0;
}

static void ExpectUnChanged(ShmUnit &shmUnit)
{
    EXPECT_EQ(shmUnit.pointer, nullptr);
    EXPECT_EQ(shmUnit.fd, -1);
    EXPECT_EQ(shmUnit.offset, 0);
    EXPECT_EQ(shmUnit.mmapSize, 0ul);
    EXPECT_EQ(shmUnit.size, 0ul);
}

int MockUnlink(const char *pathName)
{
    (void)pathName;
    return -2;
}

int MockFtruncate(int fd, off_t length)
{
    (void)fd;
    (void)length;
    return -2;
}

int MockJeMallCtl(const std::string &hook_name, const char *name, void *oldp, size_t *oldlenp, void *newp,
                  size_t newlen)
{
    LOG(INFO) << hook_name << "; " << name;
    return name != hook_name ? datasystem_mallctl(name, oldp, oldlenp, newp, newlen) : -1;
}

int MockJeMallCtlCreateArenas(const char *name, void *oldp, size_t *oldlenp, void *newp, size_t newlen)
{
    LOG(INFO) << __FUNCTION__;
    return MockJeMallCtl("arenas.create", name, oldp, oldlenp, newp, newlen);
}

void AllocatorTest::TestParallelAllocate()
{
    Timer timer;
    for (int numThreads = 1; numThreads <= 128; numThreads *= 2) {
        std::vector<std::promise<double>> timeLst(numThreads);
        ThreadPool pool(numThreads);
        timer.Reset();
        for (int i = 0; i < numThreads; i++) {
            pool.Submit([this, i, &timeLst]() {
                Timer timer;
                ShmUnit shmUnit;
                shmUnit.pointer = nullptr;
                DS_ASSERT_OK(AllocateMemory(16 * 1024ul * 1024ul, shmUnit));
                DS_ASSERT_OK(FreeMemory(shmUnit.pointer));
                timeLst[i].set_value(timer.ElapsedMilliSecond());
            });
        }
        std::vector<double> resLst(numThreads);
        for (auto i = 0; i < numThreads; i++) {
            resLst[i] = timeLst[i].get_future().get();
        }
        auto elapsed = timer.ElapsedMilliSecond();
        auto sum = std::accumulate(resLst.begin(), resLst.end(), 0.0);
        LOG(INFO) << FormatString("%.6lf ms, all cpu time: %.6lf ms, acc: %.3lfX", elapsed, sum, sum / elapsed);
    }
}

TEST_F(AllocatorTest, TestParallelAllocate)
{
    uint64_t maxSize = 3u * 1024l * 1024ul * 1024ul;
    DS_ASSERT_OK(Init(maxSize));
    TestParallelAllocate();
}

TEST_F(AllocatorTest, TestParallelAllocateDisk)
{
    uint64_t maxSize = 3u * 1024l * 1024ul * 1024ul;
    DS_ASSERT_OK(Init({ maxSize, memory::CacheType::DISK }));
    TestParallelAllocate();
}

void AllocatorTest::TestAllocateMemoryWithDefaultParam()
{
    LOG(INFO) << "Test allocate memory with default parameter values.";
    ShmUnit shmUnit;
    ResetShmUnit(shmUnit);

    ASSERT_EQ(GetMaxMemorySize(), size_t(MaxSize()));
    ASSERT_EQ(GetMemoryUsage(), size_t(0));

    EXPECT_EQ(AllocateMemory(MaxSize() + 1, shmUnit).GetCode(), StatusCode::K_OUT_OF_MEMORY);
    ExpectUnChanged(shmUnit);

    uint64_t needSize = 16;
    ResetShmUnit(shmUnit);
    DS_ASSERT_OK(AllocateMemory(needSize, shmUnit));
    ASSERT_EQ(GetMemoryUsage(), needSize);

    DS_ASSERT_OK(FreeMemory(shmUnit.pointer));

    int testInt = 0;
    void *testVoidPtr = (void *)&testInt;
    DS_EXPECT_NOT_OK(FreeMemory(testVoidPtr));

    ASSERT_EQ(GetMemoryUsage(), size_t(0));
}

TEST_F(AllocatorTest, TestAllocateMemoryWithDefaultParam)
{
    uint64_t maxSize = 64 * 1024ul * 1024ul;
    DS_ASSERT_OK(Init(maxSize));
    TestAllocateMemoryWithDefaultParam();
}

TEST_F(AllocatorTest, TestAllocateMemoryWithDefaultParamDisk)
{
    uint64_t maxSize = 64 * 1024ul * 1024ul;
    DS_ASSERT_OK(Init({ maxSize, memory::CacheType::DISK }));
    TestAllocateMemoryWithDefaultParam();
}

TEST_F(AllocatorTest, TestAllocateMemoryWithThreshold)
{
    LOG(INFO) << "Test allocate memory with threshold.";
    auto *allocator = datasystem::memory::Allocator::Instance();
    uint64_t maxSize = 64 * 1024ul * 1024ul;
    uint64_t scShmPercentage = 90, ocShmPercentage = 80;
    uint64_t maxOcSize = (maxSize * ocShmPercentage) / 100, maxScSize = (maxSize * scShmPercentage) / 100;
    ssize_t decayMs = 5000;
    ShmUnit shmUnit;
    ResetShmUnit(shmUnit);

    DS_ASSERT_OK(allocator->Init(maxSize, 0, false, true, decayMs, ocShmPercentage, scShmPercentage));
    ASSERT_EQ(allocator->GetMaxMemorySize(ServiceType::OBJECT), size_t(maxOcSize));
    ASSERT_EQ(allocator->GetMaxMemorySize(ServiceType::STREAM), size_t(maxScSize));
    ASSERT_EQ(allocator->GetMemoryUsage(), size_t(0));

    EXPECT_EQ(allocator
                  ->AllocateMemory(DEFAULT_TENANT_ID, maxOcSize + 1, false, shmUnit.pointer, shmUnit.fd, shmUnit.offset,
                                   shmUnit.mmapSize, ServiceType::OBJECT)
                  .GetCode(),
              StatusCode::K_OUT_OF_MEMORY);
    ExpectUnChanged(shmUnit);

    uint64_t needSize = 16;
    ResetShmUnit(shmUnit);
    DS_ASSERT_OK(allocator->AllocateMemory(DEFAULT_TENANT_ID, needSize, false, shmUnit.pointer, shmUnit.fd,
                                           shmUnit.offset, shmUnit.mmapSize, ServiceType::STREAM));
    ASSERT_EQ(allocator->GetMemoryUsage(), needSize);

    DS_ASSERT_NOT_OK(allocator->FreeMemory(shmUnit.pointer, ServiceType::OBJECT));
    DS_ASSERT_OK(allocator->FreeMemory(shmUnit.pointer, ServiceType::STREAM));

    ASSERT_EQ(allocator->GetMemoryUsage(), size_t(0));
    allocator->Shutdown();
}

TEST_F(AllocatorTest, TestSinglePattern)
{
    LOG(INFO) << "Test allocator single pattern.";
    {
        auto *a1 = datasystem::memory::Allocator::Instance();
        auto *a2 = datasystem::memory::Allocator::Instance();
        ASSERT_TRUE(a1 != nullptr);
        ASSERT_TRUE(a2 != nullptr);
        ASSERT_EQ(a1, a2);
    }
    {
        // test it in multi-threads.
        Barrier barr(2);
        datasystem::memory::Allocator *a1 = nullptr;
        datasystem::memory::Allocator *a2 = nullptr;

        std::thread t1([&]() {
            barr.Wait();
            a1 = datasystem::memory::Allocator::Instance();
        });
        std::thread t2([&]() {
            barr.Wait();
            a2 = datasystem::memory::Allocator::Instance();
        });

        t1.join();
        t2.join();

        ASSERT_TRUE(a1 != nullptr);
        ASSERT_TRUE(a2 != nullptr);
        ASSERT_EQ(a1, a2);
    }
}

void AllocatorTest::TestAllocateManyPages()
{
    LOG(INFO) << "Test allocate many pages.";

    size_t pageSize = 4 * 1024ul;
    ShmUnit shmUnit;
    ResetShmUnit(shmUnit);
    Status rc;
    size_t count = 0;
    do {
        rc = AllocateMemory(pageSize, shmUnit);
        count++;
    } while (rc.IsOk());

    size_t theoryCount = MaxSize() / pageSize;
    ASSERT_GE(count, static_cast<size_t>(theoryCount * 0.9));
}

TEST_F(AllocatorTest, TestAllocateManyPages)
{
    size_t maxSize = 128 * 1024ul * 1024ul;
    DS_ASSERT_OK(Init(maxSize));
    TestAllocateManyPages();
}

TEST_F(AllocatorTest, TestAllocateManyPagesDisk)
{
    size_t maxSize = 128 * 1024ul * 1024ul;
    DS_ASSERT_OK(Init({ maxSize, memory::CacheType::DISK }));
    TestAllocateManyPages();
}

void AllocatorTest::TestAllocateDifferSz()
{
    std::vector<std::unique_ptr<ShmUnit>> shmUnits;
    size_t sz = 0;
    for (auto i = 1u; i < 2048; i++) {
        auto shmUnit = std::make_unique<ShmUnit>();
        DS_ASSERT_OK(AllocateMemory(*shmUnit, i * i));
        sz += i * i;
        LOG(INFO) << FormatString("i: %zu, Total: %zu", i, sz);
        shmUnits.emplace_back(std::move(shmUnit));

        // This will be free when shmUnit go out of scope
        ShmUnit shmUnit2;
        DS_ASSERT_OK(AllocateMemory(shmUnit2, i));
        sz += i;
    }
    for (auto &shmUnit : shmUnits) {
        DS_ASSERT_OK(FreeMemory(*shmUnit));
    }
    PerfManager::Instance()->PrintPerfLog();
    PerfManager::Instance()->ResetPerfLog();
    ShmUnit shmUnit1;
    for (size_t i = 1; i < 1950; i++) {
        LOG(INFO) << FormatString("i: %zu", i * 4ul * 1024 * 1024);
        auto status = AllocateMemory(shmUnit1, i * 4ul * 1024 * 1024);
        if (status.IsOk()) {
            DS_ASSERT_OK(FreeMemory(shmUnit1));
        } else {
            LOG(ERROR) << status.ToString();
            break;
        }
    }
    PerfManager::Instance()->PrintPerfLog();
}

TEST_F(AllocatorTest, TestAllocateDifferSz)
{
    AllocatorConfig config;
    config.shmSize = 8ul * 1024 * 1024 * 1024;
    config.decayMs = 10 * 1000 * 60;
    DS_ASSERT_OK(Init(config));
    TestAllocateDifferSz();
}

TEST_F(AllocatorTest, TestAllocateDifferSzDisk)
{
    AllocatorConfig config;
    config.shdSize = 8ul * 1024 * 1024 * 1024;
    config.decayMs = 10 * 1000 * 60;
    config.cacheType = memory::CacheType::DISK;
    DS_ASSERT_OK(Init(config));
    TestAllocateDifferSz();
}

TEST_F(AllocatorTest, TestAllocateVariableMemory)
{
    LOG(INFO) << "Test allocate variable memory.";
    auto *allocator = datasystem::memory::Allocator::Instance();
    size_t maxSize = 1024 * 1024ul * 1024ul;
    DS_ASSERT_OK(allocator->Init(maxSize));
    ASSERT_EQ(allocator->GetMaxMemorySize(), size_t(maxSize));
    ASSERT_EQ(allocator->GetMemoryUsage(), size_t(0));

    std::vector<size_t> pageSizes = { 4, 4096, 20 * 1024ul * 1024ul };
    for (size_t i = 0; i < 10; ++i) {
        size_t num = pageSizes.size() * 40;
        std::vector<ShmUnit> shmUnits(num);
        for (size_t j = 0; j < num; ++j) {
            size_t pageSize = pageSizes[j % 3];
            DS_ASSERT_OK(allocator->AllocateMemory(DEFAULT_TENANT_ID, pageSize, false, shmUnits[j].pointer,
                                                   shmUnits[j].fd, shmUnits[j].offset, shmUnits[j].mmapSize));
        }

        for (size_t j = 0; j < num; ++j) {
            allocator->FreeMemory(shmUnits[j].pointer);
        }
        sleep(1);  // wait 1s for free.
    }

    ASSERT_EQ(allocator->GetMaxMemorySize(), size_t(maxSize));
    ASSERT_EQ(allocator->GetMemoryUsage(), size_t(0));
    allocator->Shutdown();
}

TEST_F(AllocatorTest, TestAllocateMemoryInMultiThreads)
{
    LOG(INFO) << "Test allocate memory in multi threads.";
    auto *allocator = datasystem::memory::Allocator::Instance();
    size_t maxSize = 2 * 1024 * 1024ul * 1024ul;
    DS_ASSERT_OK(allocator->Init(maxSize));
    ASSERT_EQ(allocator->GetMaxMemorySize(), size_t(maxSize));
    ASSERT_EQ(allocator->GetMemoryUsage(), size_t(0));

    // run 10 threads to allocate memory.
    size_t threadNum = 10;
    Barrier barr(threadNum);
    std::vector<std::thread> threads(threadNum);

    for (size_t i = 0; i < 10; ++i) {
        threads[i] = std::thread([&]() {
            barr.Wait();
            size_t num = 200;
            std::vector<ShmUnit> shmUnits(num);
            for (size_t j = 0; j < num; ++j) {
                size_t pageSize = j % 2 == 0 ? 4 * 1024ul : 2 * 1024ul * 1024ul;
                Status rc = allocator->AllocateMemory(DEFAULT_TENANT_ID, pageSize, false, shmUnits[j].pointer,
                                                      shmUnits[j].fd, shmUnits[j].offset, shmUnits[j].mmapSize);
                if (rc.IsError()) {
                    LOG(ERROR) << rc.ToString();
                }
            }
            std::unordered_set<void *> pointers;
            for (const auto &unit : shmUnits) {
                pointers.emplace(unit.pointer);
            }
            LOG(INFO) << "set size: " << pointers.size();

            for (size_t j = 0; j < num; ++j) {
                Status rc = allocator->FreeMemory(shmUnits[j].pointer);
                if (rc.IsError()) {
                    LOG(ERROR) << rc.ToString();
                }
            }
        });
    }

    for (auto &t : threads) {
        t.join();
    }

    ASSERT_EQ(allocator->GetMaxMemorySize(), size_t(maxSize));
    ASSERT_EQ(allocator->GetMemoryUsage(), size_t(0));
    allocator->Shutdown();
}


void AllocatorTest::TestAllocatedAddresses()
{
    LOG(INFO) << "Test allocated addresses.";

    ASSERT_EQ(GetMaxMemorySize(), MaxSize());
    ASSERT_EQ(GetMemoryUsage(), size_t(0));

    Status rc;
    std::vector<std::pair<void *, uint64_t>> addrs;
    std::vector<size_t> pageSizes = { 4, 4096, 2 * 1000ul * 1000ul };
    size_t count = 0;
    std::vector<std::unique_ptr<ShmUnit>> shmUnits;
    do {
        size_t pageSize = pageSizes[count % 3];
        auto shmUnit = std::make_unique<ShmUnit>();
        ResetShmUnit(*shmUnit);
        rc = AllocateMemory(pageSize, *shmUnit, std::to_string(count));
        if (rc.IsOk()) {
            addrs.emplace_back(shmUnit->pointer, pageSize);
        }
        shmUnits.emplace_back(std::move(shmUnit));
        count++;
    } while (rc.IsOk());

    for (size_t i = 0; i < addrs.size(); ++i) {
        for (size_t j = i + 1; j < addrs.size(); ++j) {
            EXPECT_TRUE(IsGoodAllocate(addrs[i], addrs[j]));
        }
    }
}

#ifndef USE_URMA
TEST_F(AllocatorTest, TestAllocatedAddresses)
{
    size_t maxSize = 4 * 1024 * 1024ul * 1024ul;
    DS_ASSERT_OK(Init(maxSize));
    TestAllocatedAddresses();
}
#endif

TEST_F(AllocatorTest, TestAllocatedAddressesDisk)
{
    size_t maxSize = 4 * 1024 * 1024ul * 1024ul;
    DS_ASSERT_OK(Init({ maxSize, memory::CacheType::DISK }));
    TestAllocatedAddresses();
}

void AllocatorTest::TestDoubleFree()
{
    LOG(INFO) << "Test double free.";
    ASSERT_EQ(GetMaxMemorySize(), size_t(MaxSize()));
    ASSERT_EQ(GetMemoryUsage(), size_t(0));

    ShmUnit shmUnit;
    DS_ASSERT_OK(AllocateMemory(shmUnit, 4 * 1024ul * 1024ul));
    void *pointer = shmUnit.pointer;
    DS_ASSERT_OK(FreeMemory(pointer));
    DS_ASSERT_NOT_OK(FreeMemory(shmUnit));
}

TEST_F(AllocatorTest, TestDoubleFree)
{
    size_t maxSize = 64 * 1024ul * 1024ul;
    DS_ASSERT_OK(Init(maxSize));
    TestDoubleFree();
}

TEST_F(AllocatorTest, TestDoubleFreeDisk)
{
    size_t maxSize = 64 * 1024ul * 1024ul;
    DS_ASSERT_OK(Init({ maxSize, memory::CacheType::DISK }));
    TestDoubleFree();
}

void AllocatorTest::TestArenaBasicFunction()
{
    LOG(INFO) << "Test arena basic function.";
    std::vector<void *> pointers;
    Status rc;
    do {
        ShmUnitInfo unit;
        rc = AllocateMemory(4 * 1024ul, unit);
        if (rc.IsOk()) {
            pointers.emplace_back(unit.pointer);
            std::pair<void *, uint64_t> ptrMmapSz;
            DS_ASSERT_OK(FdToPointer(unit.fd, ptrMmapSz));
        }
    } while (rc.IsOk());

    LOG(INFO) << "last error:" << rc.ToString();
    LOG(INFO) << "alloc count:" << pointers.size();

    for (void *pointer : pointers) {
        DS_ASSERT_OK(FreeMemory(pointer));
    }
}

TEST_F(AllocatorTest, TestArenaBasicFunction)
{
    AllocatorConfig config;
    config.shmSize = 64 * 1024ul * 1024ul;  // 64 MB
    config.decayMs = 1'000;  // 1'000 MS
    DS_ASSERT_OK(Init(config));
    TestArenaBasicFunction();
}

TEST_F(AllocatorTest, TestArenaBasicFunctionDisk)
{
    AllocatorConfig config;
    config.shdSize = 64 * 1024ul * 1024ul;  // 64 MB
    config.decayMs = 1'000;  // 1'000 MS
    config.cacheType = memory::CacheType::DISK;
    DS_ASSERT_OK(Init(config));
    TestArenaBasicFunction();
}

TEST_F(AllocatorTest, TestTenantAllocMemory)
{
    FLAGS_v = 3;
    const size_t shmSize = 64 * 1024ul * 1024ul + 4096 * 2;
    ssize_t decayMs = 1'000;
    auto allocator = datasystem::memory::Allocator::Instance();
    DS_ASSERT_OK(allocator->Init(shmSize, 0, false, true, decayMs));

    const size_t allocSize = 30 * 1024ul * 1024ul;

    ShmUnit shm1;
    ShmUnit shm2;
    DS_ASSERT_OK(shm1.AllocateMemory("tenant1", allocSize, false));
    DS_ASSERT_OK(shm2.AllocateMemory("tenant2", allocSize, false));
    shm1.FreeMemory();
    shm2.FreeMemory();
    allocator->Shutdown();
}

void AllocatorTest::TestAllocMemByteAlign()
{
    const uint64_t alignByte = 64;
    const size_t allocSize = 30 * 1024ul * 1024ul;
    ShmUnit shm1;
    ShmUnit shm2;
    DS_ASSERT_OK(AllocateMemory(shm1, allocSize, "tenant1"));
    DS_ASSERT_OK(AllocateMemory(shm2, allocSize, "tenant2"));

    ASSERT_TRUE(ulong(shm1.pointer) % alignByte == 0);
    ASSERT_TRUE(ulong(shm2.pointer) % alignByte == 0);
    shm1.FreeMemory();
    shm2.FreeMemory();
}

TEST_F(AllocatorTest, TestAllocMemByteAlign)
{
    AllocatorConfig config;
    const uint64_t size = 64 * 1024ul * 1024ul + 4096 * 2;
    config.shmSize = size;
    config.decayMs = 1'000;  // 1'000 MS
    DS_ASSERT_OK(Init(config));
    TestAllocMemByteAlign();
}

TEST_F(AllocatorTest, TestAllocMemByteAlignDisk)
{
    AllocatorConfig config;
    const uint64_t size = 64 * 1024ul * 1024ul + 4096 * 2;
    config.shdSize = size;
    config.decayMs = 1'000;  // 1'000 MS
    config.cacheType = memory::CacheType::DISK;
    DS_ASSERT_OK(Init(config));
    TestAllocMemByteAlign();
}

void AllocatorTest::TestCreateArenaConcurrently()
{
    LOG(INFO) << "Test create arenas concurrently.";
    size_t num = 10;
    size_t loop = 10;
    std::vector<std::thread> threads(num);
    for (size_t i = 0; i < num; ++i) {
        threads[i] = std::thread([this, loop]() {
            for (size_t k = 0; k < loop; ++k) {
                size_t size = 1 * 1024ul * 1024ul;
                std::shared_ptr<ArenaGroup> arenaGroup;
                DS_ASSERT_OK(CreateArenaGroup(size, arenaGroup));
            }
        });
    }

    for (auto &t : threads) {
        t.join();
    }
}

TEST_F(AllocatorTest, TestCreateArenaConcurrently)
{
    uint64_t maxSize = 1024ul * 1024ul;
    DS_ASSERT_OK(Init(maxSize));
    TestCreateArenaConcurrently();
}

TEST_F(AllocatorTest, TestCreateArenaConcurrentlyDisk)
{
    uint64_t maxSize = 1024ul * 1024ul;
    DS_ASSERT_OK(Init({ maxSize, memory::CacheType::DISK }));
    TestCreateArenaConcurrently();
}

TEST_F(AllocatorTest, DISABLED_TestCreateArenaEdgeCond)
{
    LOG(INFO) << "Test create arenas edge";
    const size_t edgeVal = (1 << 21) * 0.78;
    std::vector<size_t> ss({ edgeVal, edgeVal + 1, edgeVal - 1 });
    const size_t initSize = 1024;
    FLAGS_enable_huge_tlb = true;
    DS_ASSERT_OK(datasystem::memory::Allocator::Instance()->Init(initSize));
    for (size_t size : ss) {
        std::shared_ptr<ArenaGroup> arenaGroup;
        DS_ASSERT_OK(datasystem::memory::Allocator::Instance()->CreateArenaGroup(size, arenaGroup));
    }
};

TEST_F(AllocatorTest, TestReuseArena)
{
    const size_t arenaPerTenant = 8;
    FLAGS_arena_per_tenant = arenaPerTenant;
    LOG(INFO) << "Test reuse arena.";
    size_t size = 1 * 1024ul * 1024ul;
    ssize_t decayMs = 1000;
    auto allocator = datasystem::memory::Allocator::Instance();
    DS_ASSERT_OK(allocator->Init(size, 0, false, true, decayMs));

    std::shared_ptr<ArenaGroup> arenaGroup;
    DS_ASSERT_OK(allocator->CreateArenaGroup(size, arenaGroup));
    auto arenaIds1 = arenaGroup->GetArenaIds();
    ASSERT_EQ(allocator->GetArenaManager()->GetArenaCounts(), arenaPerTenant);
    DS_ASSERT_OK(arenaGroup->DestroyAll());
    ASSERT_EQ(allocator->GetArenaManager()->GetArenaCounts(), 0ul);

    DS_ASSERT_OK(allocator->CreateArenaGroup(size, arenaGroup));
    auto arenaIds2 = arenaGroup->GetArenaIds();
    ASSERT_EQ(allocator->GetArenaManager()->GetArenaCounts(), arenaPerTenant);
    DS_ASSERT_OK(arenaGroup->DestroyAll());
    ASSERT_EQ(allocator->GetArenaManager()->GetArenaCounts(), 0ul);

    std::sort(arenaIds1.begin(), arenaIds1.end());
    std::sort(arenaIds2.begin(), arenaIds2.end());
    ASSERT_EQ(arenaIds1, arenaIds2);

    allocator->Shutdown();
    ASSERT_EQ(allocator->GetArenaManager()->GetArenaCounts(), 0ul);
}

void AllocatorTest::TestShmUnits1()
{
    auto pool = std::make_shared<ThreadPool>(1);
    uint64_t allocSize = 100;
    ASSERT_EQ(GetMaxMemorySize(), MaxSize());
    ASSERT_EQ(GetMemoryUsage(), size_t(0));
    ShmUnit shmUnit1;
    DS_ASSERT_OK(AllocateMemory(shmUnit1, allocSize));
    ShmView currView = shmUnit1.GetShmView();
    std::string id("123");
    // test a construction from view
    ShmUnit shmUnit2(ShmKey::Intern(id), currView, nullptr);
    // test a construction using fd/mmapsize
    ShmUnit shmUnit3(1, 1);
    // Test a copy
    std::string testData("123");
    DS_ASSERT_OK(shmUnit1.MemoryCopy(reinterpret_cast<const uint8_t *>(testData.data()), testData.size(), pool));
}

TEST_F(AllocatorTest, TestShmUnits1)
{
    uint64_t maxSize = 4 * 1024;
    DS_ASSERT_OK(Init(maxSize));
    TestShmUnits1();
}

TEST_F(AllocatorTest, TestShmUnits1Disk)
{
    uint64_t maxSize = 4 * 1024;
    DS_ASSERT_OK(Init({ maxSize, memory::CacheType::DISK }));
    TestShmUnits1();
}

void AllocatorTest::TestShmUnits2()
{
    auto pool = std::make_shared<ThreadPool>(1);
    uint64_t allocSize = 100;
    uint64_t bytesPerBuffer = 25;
    ASSERT_EQ(GetMaxMemorySize(), MaxSize());
    ASSERT_EQ(GetMemoryUsage(), size_t(0));
    ShmUnit shmUnit1;
    DS_ASSERT_OK(AllocateMemory(shmUnit1, allocSize));
    std::string buffer(bytesPerBuffer, 'a');

    std::vector<RpcMessage> rpcPayloads;
    std::vector<std::pair<const uint8_t *, uint64_t>> payloads;
    // 5 buffers, total of 125 bytes
    for (int i = 0; i < 5; ++i) {
        rpcPayloads.emplace_back();
        auto &msg = rpcPayloads.back();
        ASSERT_EQ(msg.AllocMem(bytesPerBuffer), Status::OK());
    }
    // We could have just populated the vector of pairs, but just wanted to demonstrate here how to parse the rpc
    // buffers to build buffers with native type only.  There's a requirement that ShmUnit needs to be independent
    // and not rely on third party code so it can't take rpc buffers directly.
    for (auto &msg : rpcPayloads) {
        payloads.emplace_back(reinterpret_cast<const uint8_t *>(msg.Data()), msg.Size());
    }

    // The ShmUnit only has 100 bytes, so trying to copy in 125 bytes is expected to cause a failure and avoid
    // memory corruption.
    Status rc = shmUnit1.MemoryCopy(payloads, pool, 0);
    LOG(INFO) << "MemoryCopy gave gc: " << rc.ToString();
    ASSERT_EQ(rc.GetCode(), StatusCode::K_RUNTIME_ERROR);
}

TEST_F(AllocatorTest, TestShmUnits2)
{
    uint64_t maxSize = 4 * 1024;
    DS_ASSERT_OK(Init(maxSize));
    TestShmUnits2();
}

TEST_F(AllocatorTest, TestShmUnits2Disk)
{
    uint64_t maxSize = 4 * 1024;
    DS_ASSERT_OK(Init({ maxSize, memory::CacheType::DISK }));
    TestShmUnits2();
}

void AllocatorTest::TestUsedupAndFree()
{
    auto usedUpAndFreeFunc = [this](int &successCnt) {
        Status status;
        std::vector<std::unique_ptr<ShmUnit>> list;
        successCnt = 0;
        const size_t allocSize = 1024 * 1024;
        while (true) {
            auto shmUnit = std::make_unique<ShmUnit>();
            status = AllocateMemory(*shmUnit, allocSize);
            if (status.IsError()) {
                break;
            }
            list.emplace_back(std::move(shmUnit));
            successCnt++;
        }
        LOG(INFO) << "success:" << successCnt;
        ASSERT_EQ(status.GetCode(), K_OUT_OF_MEMORY);
    };

    int firstTime = 0;
    usedUpAndFreeFunc(firstTime);

    int secondTime = 0;
    usedUpAndFreeFunc(secondTime);
    ASSERT_LE(std::abs(firstTime - secondTime), 2);
}

TEST_F(AllocatorTest, TestUsedupAndFree)
{
    uint64_t maxSize = 1024 * 1024ul * 1024ul;
    DS_ASSERT_OK(Init(maxSize));
    TestUsedupAndFree();
}

TEST_F(AllocatorTest, TestUsedupAndFreeDisk)
{
    uint64_t maxSize = 1024 * 1024ul * 1024ul;
    DS_ASSERT_OK(Init({ maxSize, memory::CacheType::DISK }));
    TestUsedupAndFree();
}

void AllocatorTest::TestUsedupAndFree2()
{
    const uint64_t bigSize = 100 * 1024ul * 1024ul + 35ul;
    const uint64_t smallSize = 10 * 1024ul + 35ul;

    std::vector<void *> pointers;
    Status rc;
    uint64_t cnt = 0;
    do {
        ShmUnitInfo unit;
        rc = AllocateMemory(bigSize, unit);
        if (rc.IsOk()) {
            pointers.emplace_back(unit.pointer);
            cnt++;
        }
    } while (rc.IsOk());

    auto rate1 = cnt * bigSize * 100ul / MaxSize();
    EXPECT_GE(rate1, 85ul);

    LOG(INFO) << "count = " << cnt << ", rate = " << rate1;

    for (void *pointer : pointers) {
        DS_ASSERT_OK(FreeMemory(pointer));
    }
    pointers.clear();

    cnt = 0;
    do {
        ShmUnitInfo unit;
        rc = AllocateMemory(smallSize, unit);
        if (rc.IsOk()) {
            pointers.emplace_back(unit.pointer);
            cnt++;
        }
    } while (rc.IsOk());

    auto rate2 = cnt * smallSize * 100ul / MaxSize();
    EXPECT_GE(rate2, 80ul);
    LOG(INFO) << "count = " << cnt << ", rate = " << rate2;
}

TEST_F(AllocatorTest, TestUsedupAndFree2)
{
    AllocatorConfig config;
    config.shmSize = 2 * 1024ul * 1024ul * 1024ul;  // 2 GB
    config.decayMs = 10 * SEC_PER_MIN * MS_PER_SECOND;  // 10 minus.
    DS_ASSERT_OK(Init(config));
    TestUsedupAndFree2();
}

TEST_F(AllocatorTest, TestUsedupAndFree2Disk)
{
    AllocatorConfig config;
    config.shdSize = 2 * 1024ul * 1024ul * 1024ul;  // 2 GB
    config.decayMs = 10 * SEC_PER_MIN * MS_PER_SECOND;  // 10 minus.
    config.cacheType = memory::CacheType::DISK;
    DS_ASSERT_OK(Init(config));
    TestUsedupAndFree2();
}

void AllocatorTest::TestCreateTenantArena()
{
    LOG(INFO) << "Test allocate memory.";
    ShmUnit shmUnit;
    ResetShmUnit(shmUnit);

    std::string tenantId1 = "tenant1";

    EXPECT_EQ(AllocateMemory(MaxSize() + 1, shmUnit, tenantId1).GetCode(), StatusCode::K_OUT_OF_MEMORY);
    ExpectUnChanged(shmUnit);

    uint64_t needSize = 16;
    ResetShmUnit(shmUnit);
    DS_ASSERT_OK(AllocateMemory(needSize, shmUnit, tenantId1));
    ASSERT_EQ(GetMemoryUsage(tenantId1), needSize);

    DS_ASSERT_OK(FreeMemory(shmUnit.pointer, tenantId1));

    int testInt = 0;
    void *testVoidPtr = (void *)&testInt;
    DS_EXPECT_NOT_OK(FreeMemory(testVoidPtr, tenantId1));
    LOG(INFO) << "Now Destroy tenant's arena";
    DS_EXPECT_OK(DestroyArenaGroup(tenantId1));
    sleep(1);
    // Create the arena again to check the previous one has been removed.
    std::shared_ptr<ArenaGroup> arenaGroup;
    LOG(INFO) << "Now create tenant's arena again";
    DS_EXPECT_OK(CreateArenaGroup(needSize, arenaGroup, tenantId1));

    ASSERT_EQ(GetMemoryUsage(tenantId1), size_t(0));
}

TEST_F(AllocatorTest, TestCreateTenantArena)
{
    uint64_t maxSize = 64 * 1024ul * 1024ul;
    DS_ASSERT_OK(Init(maxSize));
    TestCreateTenantArena();
}

TEST_F(AllocatorTest, TestCreateTenantArenaDisk)
{
    uint64_t maxSize = 64 * 1024ul * 1024ul;
    DS_ASSERT_OK(Init({ maxSize, memory::CacheType::DISK }));
    TestCreateTenantArena();
}

void AllocatorTest::TestCreateMultiTenantArena()
{
    ShmUnit shmUnit1;
    ShmUnit shmUnit2;
    ShmUnit shmUnit3;
    ResetShmUnit(shmUnit1);
    ResetShmUnit(shmUnit2);
    ResetShmUnit(shmUnit3);

    std::string tenantId1 = "tenant1";
    std::string tenantId2 = "tenant2";
    std::string tenantId3 = "tenant3";

    uint64_t needSize = 16;

    DS_ASSERT_OK(AllocateMemory(needSize, shmUnit1, tenantId1));
    DS_ASSERT_OK(AllocateMemory(needSize, shmUnit2, tenantId2));
    DS_ASSERT_OK(AllocateMemory(needSize, shmUnit3, tenantId3));

    ASSERT_EQ(GetMemoryUsage(tenantId1), needSize);
    ASSERT_EQ(GetMemoryUsage(tenantId2), needSize);
    ASSERT_EQ(GetMemoryUsage(tenantId3), needSize);

    DS_ASSERT_OK(FreeMemory(shmUnit1.pointer, tenantId1));
    DS_ASSERT_OK(FreeMemory(shmUnit2.pointer, tenantId2));
    DS_ASSERT_OK(FreeMemory(shmUnit3.pointer, tenantId3));

    int testInt = 0;
    void *testVoidPtr = (void *)&testInt;
    DS_EXPECT_NOT_OK(FreeMemory(testVoidPtr, tenantId1));
    LOG(INFO) << "Now Destroy tenant's arena";
    DS_EXPECT_OK(DestroyArenaGroup(tenantId1));
    DS_EXPECT_OK(DestroyArenaGroup(tenantId2));
    DS_EXPECT_OK(DestroyArenaGroup(tenantId3));
    sleep(1);
    // Create the arena again to check the previous one has been removed.
    std::shared_ptr<ArenaGroup> arenaGroup;
    LOG(INFO) << "Now create tenant's arena again";
    DS_EXPECT_OK(CreateArenaGroup(needSize, arenaGroup, tenantId1));
    DS_EXPECT_OK(CreateArenaGroup(needSize, arenaGroup, tenantId2));
    DS_EXPECT_OK(CreateArenaGroup(needSize, arenaGroup, tenantId3));

    ASSERT_EQ(GetMemoryUsage(tenantId1), size_t(0));
    ASSERT_EQ(GetMemoryUsage(tenantId2), size_t(0));
    ASSERT_EQ(GetMemoryUsage(tenantId3), size_t(0));
}

TEST_F(AllocatorTest, TestCreateMultiTenantArena)
{
    uint64_t maxSize = 64 * 1024ul * 1024ul;
    DS_ASSERT_OK(Init(maxSize));
    TestCreateMultiTenantArena();
}

TEST_F(AllocatorTest, TestCreateMultiTenantArenaDisk)
{
    uint64_t maxSize = 64 * 1024ul * 1024ul;
    DS_ASSERT_OK(Init({ maxSize, memory::CacheType::DISK }));
    TestCreateMultiTenantArena();
}

void AllocatorTest::TestCreateTenantArenaConcurrent()
{
    LOG(INFO) << "Test create arenas concurrently.";
    size_t tenantNum = 100;
    size_t num = 10;
    size_t loop = 10;
    ThreadPool tpool(tenantNum * num);
    std::vector<std::future<void>> futs;
    futs.reserve(tenantNum * num);
    for (size_t tenantId = 0; tenantId < tenantNum; tenantId++) {
        for (size_t i = 0; i < num; ++i) {
            futs.emplace_back(tpool.Submit([this, loop, tenantId]() {
                for (size_t k = 0; k < loop; ++k) {
                    size_t size = 1 * 1024ul * 1024ul;
                    std::shared_ptr<ArenaGroup> arena;
                    DS_ASSERT_OK(datasystem::memory::Allocator::Instance()->GetArenaManager()->GetOrCreateArenaGroup(
                        { std::to_string(tenantId), config_.cacheType }, size, arena));
                }
            }));
        }
    }

    for (auto &f : futs) {
        f.get();
    }

    for (size_t tenantId = 0; tenantId < tenantNum; tenantId++) {
        DS_ASSERT_OK(DestroyArenaGroup(std::to_string(tenantId)));
    }
    for (size_t tenantId = 0; tenantId < tenantNum; tenantId++) {
        DS_ASSERT_NOT_OK(DestroyArenaGroup(std::to_string(tenantId)));
    }
}

TEST_F(AllocatorTest, TestCreateTenantArenaConcurrent)
{
    uint64_t maxSize = 1024ul * 1024ul;
    DS_ASSERT_OK(Init(maxSize));
    TestCreateTenantArenaConcurrent();
}

TEST_F(AllocatorTest, TestCreateTenantArenaConcurrentDisk)
{
    uint64_t maxSize = 1024ul * 1024ul;
    DS_ASSERT_OK(Init({ maxSize, memory::CacheType::DISK }));
    TestCreateTenantArenaConcurrent();
}

void AllocatorTest::FakeAllocate()
{
    FLAGS_v = 3;  // vlog is 3.
    const uint64_t smallSize = 1024ul * 1024ul;
    const uint64_t bigSize = 5 * 1024ul * 1024ul * 1024ul;
    auto count = 0;
    std::vector<void *> pointers;
    while (true) {
        ShmUnitInfo unit;
        Status rc = AllocateMemory(smallSize, unit);
        if (rc.IsError()) {
            LOG(ERROR) << rc;
            break;
        }
        pointers.emplace_back(unit.pointer);
        count++;
    }
    int minCount = 8000;
    ASSERT_TRUE(count > minCount);
    for (auto &pointer : pointers) {
        DS_ASSERT_OK(FreeMemory(pointer));
    }
    pointers.clear();
    ShmUnitInfo unit;
    sleep(5);  // wait 5s for free
    DS_ASSERT_OK(AllocateMemory(bigSize, unit));
}

TEST_F(AllocatorTest, FakeAllocate)
{
    FLAGS_arena_per_tenant = 1;
    AllocatorConfig config;
    config.shmSize = 10 * 1024ul * 1024ul * 1024ul;  // 10 GB
    config.decayMs = 10 * SEC_PER_MIN * MS_PER_SECOND;  // 10 minus.
    DS_ASSERT_OK(Init(config));
    FakeAllocate();
}

TEST_F(AllocatorTest, FakeAllocateDisk)
{
    FLAGS_shared_disk_arena_per_tenant = 1;
    AllocatorConfig config;
    config.shdSize = 10 * 1024ul * 1024ul * 1024ul;  // 10 GB
    config.decayMs = 10 * SEC_PER_MIN * MS_PER_SECOND;  // 10 minus.
    config.cacheType = memory::CacheType::DISK;
    DS_ASSERT_OK(Init(config));
    FakeAllocate();
}

TEST_F(AllocatorTest, TestMultiArenaCache)
{
    FLAGS_v = 1;
    FLAGS_arena_per_tenant = 3;
    const size_t shmMaxSize = 4 * 1024ul * 1024ul * 1024ul + 8 * 1024ul * 1024ul;
    ssize_t decayMs = 10'000 * 60;
    const uint64_t smallSize = 1024ul * 1024ul;
    const uint64_t bigSize = 2 * 1024ul * 1024ul * 1024;
    DS_ASSERT_OK(datasystem::memory::Allocator::Instance()->Init(shmMaxSize, 0, false, true, decayMs));

    ShmUnitInfo unit1;
    ShmUnitInfo unit2;
    DS_ASSERT_OK(datasystem::memory::Allocator::Instance()->AllocateMemory(
        DEFAULT_TENANT_ID, bigSize, false, unit1.pointer, unit1.fd, unit1.offset, unit1.mmapSize));
    DS_ASSERT_OK(datasystem::memory::Allocator::Instance()->AllocateMemory(
        DEFAULT_TENANT_ID, bigSize, false, unit2.pointer, unit2.fd, unit2.offset, unit2.mmapSize));
    LOG(INFO) << "fallocate size:" << datasystem::memory::Allocator::Instance()->GetTotalPhysicalMemoryUsage();
    ASSERT_GE(datasystem::memory::Allocator::Instance()->GetTotalPhysicalMemoryUsage(), bigSize * 2);
    ASSERT_EQ(datasystem::memory::Allocator::Instance()->GetTotalMemoryUsage(), bigSize * 2);
    DS_ASSERT_OK(datasystem::memory::Allocator::Instance()->FreeMemory(unit1.pointer));
    DS_ASSERT_OK(datasystem::memory::Allocator::Instance()->FreeMemory(unit2.pointer));
    ASSERT_GE(datasystem::memory::Allocator::Instance()->GetTotalPhysicalMemoryUsage(), bigSize * 2);
    ASSERT_EQ(datasystem::memory::Allocator::Instance()->GetTotalMemoryUsage(), 0ul);

    std::vector<void *> pointers;
    size_t count = 0;
    while (true) {
        ShmUnitInfo unit;
        Status rc = datasystem::memory::Allocator::Instance()->AllocateMemory(
            DEFAULT_TENANT_ID, smallSize, false, unit.pointer, unit.fd, unit.offset, unit.mmapSize);
        if (rc.IsError()) {
            LOG(ERROR) << rc;
            break;
        }
        count++;
    }
    LOG(INFO) << "allocated size:" << datasystem::memory::Allocator::Instance()->GetTotalMemoryUsage();
    ASSERT_EQ(datasystem::memory::Allocator::Instance()->GetTotalMemoryUsage(), count * smallSize);
    ASSERT_GE(count + 2, shmMaxSize / (4096 + smallSize));
    datasystem::memory::Allocator::Instance()->Shutdown();
}

TEST_F(AllocatorTest, TestDestructorOrder)
{
    FLAGS_v = 1;
    FLAGS_arena_per_tenant = 3;
    const size_t shmMaxSize = 4 * 1024ul * 1024ul;
    ssize_t decayMs = 10'000 * 60;
    const uint64_t size = 1024ul * 1024ul;
    DS_ASSERT_OK(datasystem::memory::Allocator::Instance()->Init(shmMaxSize, 0, false, true, decayMs));
    std::shared_ptr<ArenaGroup> arenaGroup;
    std::string tenantId = "t1";
    DS_ASSERT_OK(datasystem::memory::Allocator::Instance()->CreateArenaGroup(tenantId, shmMaxSize, arenaGroup));
    ShmUnitInfo unit;
    DS_ASSERT_OK(datasystem::memory::Allocator::Instance()->AllocateMemory(DEFAULT_TENANT_ID, size, false, unit.pointer,
                                                                           unit.fd, unit.offset, unit.mmapSize));
    DS_ASSERT_OK(datasystem::memory::Allocator::Instance()->AllocateMemory(tenantId, size, false, unit.pointer, unit.fd,
                                                                           unit.offset, unit.mmapSize));
}

TEST_F(AllocatorTest, DecommitSizeOverflow)
{
    FLAGS_v = 1;
    const ssize_t decayMs = 10'000;
    auto &allocator = *datasystem::memory::Allocator::Instance();
    const size_t maxSize = 4 * 1024 * 1024ul * 1024ul;
    DS_ASSERT_OK(allocator.Init(maxSize, 0, false, true, decayMs));
    inject::Set("arena.decommit", "10%call(10240)");
    auto func = []() {
        RandomData random;
        std::vector<std::shared_ptr<ShmUnit>> shms;
        while (true) {
            const uint64_t maxSize = 30 * 1024 * 1024;
            uint64_t size = random.GetRandomUint64(1, maxSize);
            auto unit = std::make_shared<ShmUnit>();
            Status rc = unit->AllocateMemory(DEFAULT_TENANT_ID, size, true);
            if (rc.IsError()) {
                break;
            }
            shms.emplace_back(std::move(unit));
        }
    };
    func();
    const int sleepMs = 12 * 1000;
    std::this_thread::sleep_for(std::chrono::milliseconds(sleepMs));
    LOG(INFO) << "after sleep:" << allocator.GetTotalPhysicalMemoryUsage();
    ASSERT_LE(allocator.GetTotalPhysicalMemoryUsage(), maxSize);
}

TEST_F(AllocatorTest, UsageSizeOverflowOtherCase)
{
    auto &allocator = *datasystem::memory::Allocator::Instance();
    allocator.Init(1);
    DS_ASSERT_OK(inject::Set("allocator.size", FormatString("call(%zu)", 1)));
    ASSERT_LE(allocator.GetTotalPhysicalMemoryUsage(), 1ul);

    const uint64_t limit = 0xFFFFFFFFFFFF;
    DS_ASSERT_OK(inject::Set("allocator.size", FormatString("call(%zu)", limit - 1)));
    ASSERT_LE(allocator.GetTotalPhysicalMemoryUsage(), limit - 1);

    DS_ASSERT_OK(inject::Set("allocator.size", FormatString("call(%zu)", limit)));
    ASSERT_LE(allocator.GetTotalPhysicalMemoryUsage(), limit);

    DS_ASSERT_OK(inject::Set("allocator.size", FormatString("call(%zu)", limit + 1)));
    ASSERT_LE(allocator.GetTotalPhysicalMemoryUsage(), 0ul);
}

#ifndef USE_URMA
TEST_F(AllocatorTest, TestAllocatedTenantMax)
{
    int32_t arenaPerTenant = 16;
    FLAGS_arena_per_tenant = arenaPerTenant;
    LOG(INFO) << "Test allocated addresses.";
    auto *allocator = datasystem::memory::Allocator::Instance();
    size_t maxSize = 4 * 1024 * 1024ul * 1024ul;
    DS_ASSERT_OK(allocator->Init(maxSize));
    ASSERT_EQ(allocator->GetMaxMemorySize(), size_t(maxSize));
    ASSERT_EQ(allocator->GetMemoryUsage(), size_t(0));

    Status rc;
    std::vector<size_t> pageSizes = { 4, 1024, 4096 };
    size_t count = 0;
    std::vector<std::unique_ptr<ShmUnit>> shmUnits;
    auto shmUnit = std::make_unique<ShmUnit>();
    ResetShmUnit(*shmUnit);
    rc = allocator->AllocateMemory(DEFAULT_TENANT_ID, pageSizes[0], false, shmUnit->pointer, shmUnit->fd,
                                   shmUnit->offset, shmUnit->mmapSize);
    if (rc.IsOk()) {
        count++;
        shmUnits.emplace_back(std::move(shmUnit));
    }
    do {
        size_t pageSize = pageSizes[count % 3];
        auto shmUnit = std::make_unique<ShmUnit>();
        ResetShmUnit(*shmUnit);
        auto tenantId = DEFAULT_TENANT_ID + std::to_string(count);
        rc = allocator->AllocateMemory(tenantId, pageSize, false, shmUnit->pointer, shmUnit->fd, shmUnit->offset,
                                       shmUnit->mmapSize);
        if (rc.IsOk()) {
            count++;
        }
        shmUnits.emplace_back(std::move(shmUnit));
    } while (rc.IsOk());

    ASSERT_TRUE(rc.ToString().find("create tenant failed, the maximum tenant size") != std::string::npos);
    ASSERT_EQ(count, 255);  // max tenant size is 255
    allocator->Shutdown();
}
#endif

TEST_F(AllocatorTest, OpenTmpFileFailed)
{
    FLAGS_shared_disk_arena_per_tenant = 1;
    uint64_t maxSize = 1024ul * 1024ul;
    datasystem::inject::Set("DiskMmap.OpenTmpFail", "call()");
    // Creating a tmp file using mkstemp.
    DS_ASSERT_OK(Init({ maxSize, memory::CacheType::DISK }));
    // Need unlink tmp file.
    ASSERT_TRUE(IsEmptyDir(FLAGS_shared_disk_directory));
    const uint64_t allocSize = 1024ul;
    ShmUnitInfo unit;
    DS_ASSERT_OK(AllocateMemory(allocSize, unit));
    DS_ASSERT_OK(FreeMemory(unit.pointer));
}

TEST_F(AllocatorTest, CapacityScaling)
{
    uint64_t maxSize = 64 * 1024ul * 1024ul;
    AllocatorConfig config;
    config.shdSize = maxSize;
    config.decayMs = 1'000;
    config.cacheType = memory::CacheType::DISK;
    DS_ASSERT_OK(Init(config));
    std::string tenant1 = "tenant1";
    std::string tenant2 = "tenant2";
    ShmUnit unit;
    const uint64_t allocSize = 25 * 1024ul * 1024ul;
    DS_ASSERT_OK(AllocateMemory(unit, allocSize, tenant1));
    ShmUnit unit2;
    DS_ASSERT_OK(AllocateMemory(unit2, allocSize, tenant2));
    ShmUnit unit3;
    ASSERT_EQ(AllocateMemory(allocSize, unit3, tenant2).GetCode(), K_OUT_OF_MEMORY);
    DS_ASSERT_OK(FreeMemory(unit));
    sleep(1);
    DS_ASSERT_OK(AllocateMemory(allocSize, unit3, tenant2));
}

TEST_F(AllocatorTest, SharedDiskNotEnable)
{
    FLAGS_shared_disk_arena_per_tenant = 1;
    uint64_t maxSize = 1024ul * 1024ul;
    DS_ASSERT_OK(Init({ maxSize, memory::CacheType::DISK }));
    FLAGS_shared_disk_directory = "";
    const uint64_t allocSize = 1024ul;
    ShmUnitInfo unit;
    ASSERT_EQ(AllocateMemory(allocSize, unit).GetCode(), K_INVALID);
}

class AllocatorHybridTest : public CommonTest {
public:
    void SetUp() override
    {
        std::string dir = GetTestCaseDataDir() + "/shared_disk/";
        FLAGS_shared_disk_directory = dir.c_str();
    }

    void TearDown() override
    {
        datasystem::memory::Allocator::Instance()->Shutdown();
    }

    Status AllocateMemory(uint64_t needSize, ShmUnitInfo &unit, memory::CacheType cacheType,
                          const std::string &tenantId = DEFAULT_TENANT_ID)
    {
        return datasystem::memory::Allocator::Instance()->AllocateMemory(tenantId, needSize, false, unit.pointer,
                                                                         unit.fd, unit.offset, unit.mmapSize,
                                                                         ServiceType::OBJECT, cacheType);
    }

    Status FreeMemory(void *&pointer, memory::CacheType cacheType, const std::string &tenantId = DEFAULT_TENANT_ID)
    {
        return datasystem::memory::Allocator::Instance()->FreeMemory(tenantId, pointer, ServiceType::OBJECT, cacheType);
    }
};

TEST_F(AllocatorHybridTest, ParallelAllocate)
{
    uint64_t maxSize = 2u * 1024l * 1024ul * 1024ul;
    DS_ASSERT_OK(datasystem::memory::Allocator::Instance()->Init(maxSize, maxSize));
    const int maxParallel = 128;
    for (int numThreads = 1; numThreads <= maxParallel; numThreads <<= 1) {
        std::vector<std::promise<double>> timeLst(numThreads);
        ThreadPool pool(numThreads);
        for (int i = 0; i < numThreads; i++) {
            auto type = i % 2 ? memory::CacheType::MEMORY : memory::CacheType::DISK;
            pool.Submit([this, type]() {
                ShmUnit shmUnit;
                shmUnit.pointer = nullptr;
                uint64_t allocSize = 16 * 1024ul * 1024ul;
                DS_ASSERT_OK(AllocateMemory(allocSize, shmUnit, type));
                DS_ASSERT_OK(FreeMemory(shmUnit.pointer, type));
            });
        }
    }
}

#ifndef USE_URMA
TEST_F(AllocatorHybridTest, AllocatedTenantMax)
{
    int count = 0;
    size_t size = 1024ul * 1024ul;
    DS_ASSERT_OK(datasystem::memory::Allocator::Instance()->Init(size, size));
    Status rc;
    while (rc.IsOk()) {
        std::shared_ptr<ArenaGroup> arena;
        auto cacheType = count % 2 ? memory::CacheType::MEMORY : memory::CacheType::DISK;
        rc = datasystem::memory::Allocator::Instance()->GetArenaManager()->GetOrCreateArenaGroup(
            { std::to_string(count), cacheType }, size, arena);
        count++;
    }
    count--;

    ASSERT_TRUE(rc.ToString().find("create tenant failed, the maximum tenant size") != std::string::npos);
    ASSERT_EQ(count, 169);  // max tenant size is 169: 4096 / (16 + 8) -1 = 169
}
#endif
}  // namespace ut
}  // namespace datasystem
