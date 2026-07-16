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
#include "datasystem/client/mmap_manager.h"

#include <atomic>
#include <chrono>
#include <thread>

#include "datasystem/common/shared_memory/shm_unit_info.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/wait_post.h"
#include "datasystem/client/client_worker_common_api.h"

#include "ut/common.h"

#ifdef __linux__
#include <linux/memfd.h>
#include <sys/syscall.h>
#include <unistd.h>
#endif

using namespace datasystem::client;

namespace datasystem {
namespace ut {

namespace {
class MmapUtFakeWorkerApi : public ClientWorkerRemoteCommonApi {
public:
    explicit MmapUtFakeWorkerApi(HostPort hp)
        : IClientWorkerCommonApi(HostPort(hp), HeartbeatType::RPC_HEARTBEAT, false, nullptr),
          ClientWorkerRemoteCommonApi(std::move(hp))
    {
    }

    void SetTestMemfd(int fd)
    {
        testMemfd_ = fd;
    }

    void SetGetClientFdDelayMs(int delayMs)
    {
        delayMs_ = delayMs;
    }

    int GetClientFdCallCount() const
    {
        return getClientFdCalls_.load();
    }

    Status GetClientFd(const std::vector<int> &workerFds, std::vector<int> &clientFds,
                       const std::string &tenantId) override
    {
        (void)tenantId;
        ++getClientFdCalls_;
        clientFds.clear();
        if (testMemfd_ < 0) {
            return Status(StatusCode::K_RUNTIME_ERROR, "test memfd not set");
        }
        if (delayMs_ > 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(delayMs_));
        }
        for (size_t i = 0; i < workerFds.size(); ++i) {
            int cfd = dup(testMemfd_);
            if (cfd < 0) {
                return Status(StatusCode::K_RUNTIME_ERROR, "dup memfd failed");
            }
            clientFds.push_back(cfd);
        }
        return Status::OK();
    }

private:
    int testMemfd_ = -1;
    int delayMs_ = 0;
    std::atomic<int> getClientFdCalls_{ 0 };
};
}  // namespace

class MmapManagerTest : public CommonTest {};

TEST_F(MmapManagerTest, TestCleanRef)
{
    // todo: The mmapManager and mmapTable should be combined.
    // This testcase is just for increased coverage.
    HostPort hostPort("127.0.0.1", 8080);
    auto clientApi = std::make_shared<ClientWorkerRemoteCommonApi>(hostPort);
    MmapManager mmapManager(clientApi, false);

    mmapManager.CleanInvalidMmapTable();
    mmapManager.GetMmapEntryByFd(-1);
}

TEST_F(MmapManagerTest, TestLookupUnitsAndMmapFdsShmPathWithStubGetClientFd)
{
#if defined(__linux__)
    const int mmapSize = 4096;
    int memfd = static_cast<int>(syscall(SYS_memfd_create, "mmap_mgr_ut", MFD_ALLOW_SEALING));
    ASSERT_GE(memfd, 0);
    ASSERT_EQ(0, ftruncate(memfd, mmapSize));

    auto api = std::make_shared<MmapUtFakeWorkerApi>(HostPort("127.0.0.1", 1));
    api->SetTestMemfd(memfd);

    MmapManager mmapManager(api, false);
    auto unit = std::make_shared<ShmUnitInfo>(901, static_cast<uint64_t>(mmapSize));
    std::vector<std::shared_ptr<ShmUnitInfo>> units{ unit };

    Status st = mmapManager.LookupUnitsAndMmapFds("tenant_ut", units);
    ASSERT_TRUE(st.IsOk()) << st.ToString();
    ASSERT_NE(unit->pointer, nullptr);

    void *firstPtr = unit->pointer;
    st = mmapManager.LookupUnitsAndMmapFds("tenant_ut", units);
    ASSERT_TRUE(st.IsOk()) << st.ToString();
    EXPECT_EQ(unit->pointer, firstPtr);

    ASSERT_EQ(0, close(memfd));
#else
    GTEST_SKIP() << "Linux memfd + ShmMmapTable path only";
#endif
}

TEST_F(MmapManagerTest, TestLookupUnitsAndMmapFdsConcurrentSameFdOnlyTransfersOnce)
{
#if defined(__linux__)
    const int mmapSize = 4096;
    int memfd = static_cast<int>(syscall(SYS_memfd_create, "mmap_mgr_ut_concurrent", MFD_ALLOW_SEALING));
    ASSERT_GE(memfd, 0);
    ASSERT_EQ(0, ftruncate(memfd, mmapSize));

    auto api = std::make_shared<MmapUtFakeWorkerApi>(HostPort("127.0.0.1", 1));
    api->SetTestMemfd(memfd);
    api->SetGetClientFdDelayMs(50);

    MmapManager mmapManager(api, false);
    Barrier barrier(4);
    std::vector<std::shared_ptr<ShmUnitInfo>> units;
    std::vector<std::thread> threads;
    units.reserve(4);
    threads.reserve(4);
    for (int i = 0; i < 4; ++i) {
        units.emplace_back(std::make_shared<ShmUnitInfo>(902, static_cast<uint64_t>(mmapSize)));
    }

    for (int i = 0; i < 4; ++i) {
        threads.emplace_back([&, i] {
            std::vector<std::shared_ptr<ShmUnitInfo>> localUnits{ units[i] };
            barrier.Wait();
            Status st = mmapManager.LookupUnitsAndMmapFds("tenant_ut", localUnits);
            ASSERT_TRUE(st.IsOk()) << st.ToString();
            ASSERT_NE(localUnits[0]->pointer, nullptr);
        });
    }

    for (auto &thread : threads) {
        thread.join();
    }

    for (const auto &unit : units) {
        ASSERT_NE(unit->pointer, nullptr);
        EXPECT_EQ(unit->pointer, units[0]->pointer);
    }
    EXPECT_EQ(api->GetClientFdCallCount(), 1);

    ASSERT_EQ(0, close(memfd));
#else
    GTEST_SKIP() << "Linux memfd + ShmMmapTable path only";
#endif
}

// UC6 / review fix #3/#4: per-shm_id scoped reclaim must not cross workers.
TEST_F(MmapManagerTest, TestClearExpiredByShmIdDoesNotCrossWorker)
{
#if defined(__linux__)
    const int mmapSize = 4096;
    auto api = std::make_shared<MmapUtFakeWorkerApi>(HostPort("127.0.0.1", 1));
    MmapManager mmapManager(api, false);

    // mmap two distinct worker fds via LookupUnitsAndMmapFds so both entries exist in the table.
    int memfdA = static_cast<int>(syscall(SYS_memfd_create, "shm_id_ut_a", MFD_ALLOW_SEALING));
    int memfdB = static_cast<int>(syscall(SYS_memfd_create, "shm_id_ut_b", MFD_ALLOW_SEALING));
    ASSERT_GE(memfdA, 0);
    ASSERT_GE(memfdB, 0);
    ASSERT_EQ(0, ftruncate(memfdA, mmapSize));
    ASSERT_EQ(0, ftruncate(memfdB, mmapSize));
    api->SetTestMemfd(memfdA);
    auto unitA = std::make_shared<ShmUnitInfo>(901, static_cast<uint64_t>(mmapSize));
    std::vector<std::shared_ptr<ShmUnitInfo>> unitsA{ unitA };
    ASSERT_TRUE(mmapManager.LookupUnitsAndMmapFds("t", unitsA).IsOk());
    api->SetTestMemfd(memfdB);
    auto unitB = std::make_shared<ShmUnitInfo>(902, static_cast<uint64_t>(mmapSize));
    std::vector<std::shared_ptr<ShmUnitInfo>> unitsB{ unitB };
    ASSERT_TRUE(mmapManager.LookupUnitsAndMmapFds("t", unitsB).IsOk());

    // Associate each workerFd with its own shm_id.
    mmapManager.AssociateShmId(unitA->fd, "shm-A");
    mmapManager.AssociateShmId(unitB->fd, "shm-B");
    EXPECT_EQ(mmapManager.GetWorkerFdByShmId("shm-A"), unitA->fd);
    EXPECT_EQ(mmapManager.GetWorkerFdByShmId("shm-B"), unitB->fd);

    // Reclaim A's expired fd: must free A only, leave B intact.
    mmapManager.ClearExpiredByShmId("shm-A", { unitA->fd });
    EXPECT_EQ(mmapManager.GetMmapEntryByFd(unitA->fd), nullptr) << "A's entry freed";
    EXPECT_NE(mmapManager.GetMmapEntryByFd(unitB->fd), nullptr) << "B's entry intact (UC6)";

    // Even if A's list erroneously contains B's fd, only A's workerFd is touched.
    mmapManager.ClearExpiredByShmId("shm-A", { unitA->fd, unitB->fd });
    EXPECT_NE(mmapManager.GetMmapEntryByFd(unitB->fd), nullptr) << "B still intact";

    // Unknown shm_id is a no-op (no global fallback that could reclaim B).
    mmapManager.ClearExpiredByShmId("shm-UNKNOWN", { unitB->fd });
    EXPECT_NE(mmapManager.GetMmapEntryByFd(unitB->fd), nullptr) << "unknown shm_id must not reclaim";

    // ClearByShmId releases B wholesale and removes its mapping.
    mmapManager.ClearByShmId("shm-B");
    EXPECT_EQ(mmapManager.GetMmapEntryByFd(unitB->fd), nullptr);
    EXPECT_EQ(mmapManager.GetWorkerFdByShmId("shm-B"), -1) << "B's mapping removed";

    ASSERT_EQ(0, close(memfdA));
    ASSERT_EQ(0, close(memfdB));
#else
    GTEST_SKIP() << "Linux memfd + ShmMmapTable path only";
#endif
}
}  // namespace ut
}  // namespace datasystem
