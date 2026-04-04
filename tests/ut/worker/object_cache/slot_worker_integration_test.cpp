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

/**
 * Description: Tests worker-scoped slot integration.
 */

#include "datasystem/worker/object_cache/slot_recovery_orchestrator.h"
#include "datasystem/worker/worker_oc_server.h"

#include <fcntl.h>
#include <unistd.h>

#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "common.h"
#include "datasystem/common/l2cache/persistence_api.h"
#include "datasystem/common/l2cache/slot_client/slot_file_util.h"
#include "datasystem/common/l2cache/slot_client/slot_index_codec.h"
#include "datasystem/common/l2cache/slot_client/slot.h"
#include "datasystem/common/l2cache/slot_client/slot_manifest.h"
#include "datasystem/common/util/file_util.h"

DS_DECLARE_string(l2_cache_type);
DS_DECLARE_string(sfs_path);
DS_DECLARE_string(cluster_name);
DS_DECLARE_string(worker_address);
DS_DECLARE_uint32(distributed_disk_slot_num);
DS_DECLARE_uint32(distributed_disk_sync_interval_ms);
DS_DECLARE_uint64(distributed_disk_sync_batch_bytes);

namespace datasystem {
namespace ut {

namespace {
std::string MakeTempDir()
{
    std::string pattern = "/tmp/slot_worker_ut_XXXXXX";
    std::vector<char> buffer(pattern.begin(), pattern.end());
    buffer.push_back('\0');
    auto *dir = mkdtemp(buffer.data());
    EXPECT_NE(dir, nullptr);
    return dir == nullptr ? "" : std::string(dir);
}

std::shared_ptr<std::stringstream> MakeBody(const std::string &payload)
{
    auto body = std::make_shared<std::stringstream>();
    *body << payload;
    return body;
}

std::string ReadAll(const std::shared_ptr<std::stringstream> &content)
{
    return content->str();
}
}  // namespace

class SlotWorkerIntegrationTest : public CommonTest {
public:
    void SetUp() override
    {
        CommonTest::SetUp();
        tempRoot_ = MakeTempDir();
        sfsPath_ = tempRoot_ + "/sfs";
        DS_ASSERT_OK(CreateDir(sfsPath_, true));
        FLAGS_l2_cache_type = "distributed_disk";
        FLAGS_sfs_path = sfsPath_;
        FLAGS_cluster_name = "slot_worker_cluster";
        FLAGS_distributed_disk_slot_num = 8;
        oldSyncIntervalMs_ = FLAGS_distributed_disk_sync_interval_ms;
        oldSyncBatchBytes_ = FLAGS_distributed_disk_sync_batch_bytes;
        FLAGS_distributed_disk_sync_interval_ms = 0;
        FLAGS_distributed_disk_sync_batch_bytes = 1;
        SetWorkerNamespace("127.0.0.1:31501");
    }

    void TearDown() override
    {
        SetSlotWorkerNamespace(DEFAULT_WORKER_NAME);
        FLAGS_distributed_disk_sync_interval_ms = oldSyncIntervalMs_;
        FLAGS_distributed_disk_sync_batch_bytes = oldSyncBatchBytes_;
        if (!tempRoot_.empty()) {
            (void)RemoveAll(tempRoot_);
        }
    }

protected:
    void SetWorkerNamespace(const std::string &workerAddress)
    {
        FLAGS_worker_address = workerAddress;
        SetSlotWorkerNamespace(SanitizeSlotWorkerNamespace(workerAddress));
    }

    std::string CurrentSlotRoot() const
    {
        return BuildSlotStoreRoot(sfsPath_, FLAGS_cluster_name);
    }

    std::string SlotRootForWorker(const std::string &workerAddress) const
    {
        return BuildSlotStoreRootForWorker(sfsPath_, FLAGS_cluster_name, SanitizeSlotWorkerNamespace(workerAddress));
    }

    std::string SlotPathForKey(const std::string &objectKey) const
    {
        uint32_t slotId = static_cast<uint32_t>(std::hash<std::string>{}(objectKey) % FLAGS_distributed_disk_slot_num);
        return JoinPath(CurrentSlotRoot(), FormatSlotDir(slotId));
    }

    std::string ActiveIndexPath(const std::string &slotPath) const
    {
        SlotManifestData manifest;
        auto rc = SlotManifest::Load(slotPath, manifest);
        EXPECT_TRUE(rc.IsOk()) << rc.ToString() << ".";
        EXPECT_FALSE(manifest.activeIndex.empty());
        return JoinPath(slotPath, manifest.activeIndex);
    }

    void AppendCorruption(const std::string &slotPath)
    {
        auto indexPath = ActiveIndexPath(slotPath);
        int fd = -1;
        DS_ASSERT_OK(OpenFile(indexPath, O_WRONLY | O_APPEND, &fd));
        const std::string garbage = "broken_tail";
        DS_ASSERT_OK(WriteFile(fd, garbage.data(), garbage.size(), FileSize(indexPath)));
        close(fd);
    }

    std::string tempRoot_;
    std::string sfsPath_;
    uint32_t oldSyncIntervalMs_{ 0 };
    uint64_t oldSyncBatchBytes_{ 0 };
};

TEST_F(SlotWorkerIntegrationTest, SlotRootUsesWorkerNamespace)
{
    SetSlotWorkerNamespace(DEFAULT_WORKER_NAME);
    EXPECT_EQ(BuildSlotStoreRoot(sfsPath_, FLAGS_cluster_name),
              JoinPath(JoinPath(JoinPath(JoinPath(sfsPath_, "datasystem"), FLAGS_cluster_name), "slot_store"),
                       DEFAULT_WORKER_NAME));

    const auto workerAddress = std::string("10.11.12.13:31501");
    const auto workerNamespace = SanitizeSlotWorkerNamespace(workerAddress);
    SetSlotWorkerNamespace(workerNamespace);
    EXPECT_EQ(BuildSlotStoreRoot(sfsPath_, FLAGS_cluster_name), SlotRootForWorker(workerAddress));
    EXPECT_EQ(workerNamespace, "10.11.12.13_31501");
}

TEST_F(SlotWorkerIntegrationTest, WorkerInitSetsSlotNamespace)
{
    SetSlotWorkerNamespace(DEFAULT_WORKER_NAME);
    FLAGS_l2_cache_type = "distributed_disk";
    FLAGS_worker_address = "10.2.3.4:31501";
    auto *server = new worker::WorkerOCServer(HostPort("127.0.0.1:31501"), HostPort("127.0.0.1:31501"),
                                              HostPort("127.0.0.1:31500"));
    server->InitSlotWorkerNamespace();
    EXPECT_EQ(GetSlotWorkerNamespace(), SanitizeSlotWorkerNamespace(FLAGS_worker_address));

    FLAGS_l2_cache_type = "sfs";
    SetSlotWorkerNamespace(DEFAULT_WORKER_NAME);
    FLAGS_worker_address = "10.2.3.5:31501";
    server->InitSlotWorkerNamespace();
    EXPECT_EQ(GetSlotWorkerNamespace(), DEFAULT_WORKER_NAME);
    (void)server;
}

TEST_F(SlotWorkerIntegrationTest, RepairLocalSlotsOnly)
{
    SetWorkerNamespace("127.0.0.1:31511");
    const auto slotRoot = CurrentSlotRoot();
    const auto slot1Path = JoinPath(slotRoot, FormatSlotDir(1));
    const auto slot3Path = JoinPath(slotRoot, FormatSlotDir(3));

    Slot slot1(1, slot1Path, 1024);
    Slot slot3(3, slot3Path, 1024);
    DS_ASSERT_OK(slot1.Save("tenant/slot1", 1, MakeBody("alpha")));
    DS_ASSERT_OK(slot3.Save("tenant/slot3", 1, MakeBody("beta")));
    AppendCorruption(slot1Path);

    object_cache::SlotRecoveryOrchestrator orchestrator(sfsPath_);
    DS_ASSERT_OK(orchestrator.Init());
    DS_ASSERT_OK(orchestrator.RepairLocalSlots());

    Slot repairedSlot1(1, slot1Path, 1024);
    Slot repairedSlot3(3, slot3Path, 1024);
    auto content1 = std::make_shared<std::stringstream>();
    auto content3 = std::make_shared<std::stringstream>();
    DS_ASSERT_OK(repairedSlot1.Get("tenant/slot1", 1, content1));
    DS_ASSERT_OK(repairedSlot3.Get("tenant/slot3", 1, content3));
    EXPECT_EQ(ReadAll(content1), "alpha");
    EXPECT_EQ(ReadAll(content3), "beta");
    EXPECT_FALSE(FileExist(JoinPath(slotRoot, FormatSlotDir(2))));
}

TEST_F(SlotWorkerIntegrationTest, RepairSkipsOtherNamespaces)
{
    const auto workerA = std::string("127.0.0.1:31521");
    const auto workerB = std::string("127.0.0.1:31522");

    SetWorkerNamespace(workerA);
    const auto slotRootA = CurrentSlotRoot();
    const auto slotAPath = JoinPath(slotRootA, FormatSlotDir(1));
    Slot slotA(1, slotAPath, 1024);
    DS_ASSERT_OK(slotA.Save("tenant/workerA", 1, MakeBody("payload_a")));
    AppendCorruption(slotAPath);

    const auto slotRootB = SlotRootForWorker(workerB);
    const auto slotBPath = JoinPath(slotRootB, FormatSlotDir(1));
    Slot slotB(1, slotBPath, 1024);
    DS_ASSERT_OK(slotB.Save("tenant/workerB", 1, MakeBody("payload_b")));
    AppendCorruption(slotBPath);
    const auto workerBIndexSize = FileSize(ActiveIndexPath(slotBPath));

    object_cache::SlotRecoveryOrchestrator orchestrator(sfsPath_);
    DS_ASSERT_OK(orchestrator.Init());
    DS_ASSERT_OK(orchestrator.RepairLocalSlots());

    Slot repairedSlotA(1, slotAPath, 1024);
    auto contentA = std::make_shared<std::stringstream>();
    DS_ASSERT_OK(repairedSlotA.Get("tenant/workerA", 1, contentA));
    EXPECT_EQ(ReadAll(contentA), "payload_a");
    EXPECT_EQ(FileSize(ActiveIndexPath(slotBPath)), workerBIndexSize);
}

TEST_F(SlotWorkerIntegrationTest, StartupRepairRestoresRead)
{
    SetWorkerNamespace("127.0.0.1:31531");
    const std::string objectKey = "tenant/startup_repair";
    const std::string payload = "startup_payload";

    auto writer = PersistenceApi::Create();
    DS_ASSERT_OK(writer->Init());
    DS_ASSERT_OK(writer->Save(objectKey, 1, 1000, MakeBody(payload)));
    AppendCorruption(SlotPathForKey(objectKey));

    object_cache::SlotRecoveryOrchestrator orchestrator(sfsPath_);
    DS_ASSERT_OK(orchestrator.Init());
    DS_ASSERT_OK(orchestrator.RepairLocalSlots());

    auto reader = PersistenceApi::Create();
    DS_ASSERT_OK(reader->Init());
    auto content = std::make_shared<std::stringstream>();
    DS_ASSERT_OK(reader->Get(objectKey, 1, 1000, content));
    EXPECT_EQ(ReadAll(content), payload);
}

}  // namespace ut
}  // namespace datasystem
