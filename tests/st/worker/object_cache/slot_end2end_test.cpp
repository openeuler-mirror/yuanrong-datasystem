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
 * Description: End-to-end tests for worker-scoped slot storage on top of real datasystem_worker processes.
 */

#include <fcntl.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <functional>
#include <sstream>
#include <thread>

#include "common.h"
#include "client/kv_cache/kv_client_scale_common.h"
#include "client/object_cache/oc_client_common.h"
#include "datasystem/common/kvstore/etcd/etcd_constants.h"
#include "datasystem/common/l2cache/slot_client/slot_file_util.h"
#include "datasystem/common/l2cache/slot_client/slot_internal_config.h"
#include "datasystem/common/kvstore/etcd/etcd_constants.h"
#include "datasystem/common/l2cache/slot_client/slot_index_codec.h"
#include "datasystem/common/l2cache/slot_client/slot_manifest.h"
#include "datasystem/common/util/file_util.h"
#include "datasystem/master/object_cache/store/object_meta_store.h"
#include "datasystem/protos/slot_recovery.pb.h"

DS_DECLARE_string(cluster_name);

namespace datasystem {
namespace st {
namespace {
constexpr uint32_t SLOT_NUM = DISTRIBUTED_DISK_SLOT_NUM;
constexpr int WAIT_PATH_TIMEOUT_MS = 15000;
constexpr int WAIT_PATH_INTERVAL_MS = 50;
constexpr int WAIT_GET_TIMEOUT_MS = 15000;
constexpr int WAIT_GET_INTERVAL_MS = 200;
constexpr int WAIT_SLOT_RECOVERY_TIMEOUT_MS = 20000;
constexpr uint64_t NODE_TIMEOUT_S = 1;
constexpr uint64_t PASSIVE_NODE_DEAD_TIMEOUT_S = 3;
constexpr uint64_t RESTART_NODE_DEAD_TIMEOUT_S = 30;
constexpr uint64_t HEARTBEAT_INTERVAL_MS = 500;
constexpr int S2MS = 1000;
constexpr uint64_t LARGE_OBJECT_BYTES = 1024UL * 1024UL;
constexpr char CLUSTER_NAME[] = "slot_e2e_cluster";
}  // namespace

class SlotEndToEndTest : public KVClientScaleCommon {
public:
    std::string CurrentTestName() const
    {
        auto *testInfo = ::testing::UnitTest::GetInstance()->current_test_info();
        return testInfo == nullptr ? "" : testInfo->name();
    }

    bool IsPassiveScaleDownCase() const
    {
        const auto testName = CurrentTestName();
        return testName == "PassiveScaleDownRecoversSlotAndMetadata"
               || testName == "PassiveScaleDownRecoversLargeObjectInDedicatedDataFile";
    }

    bool IsLargeObjectCase() const
    {
        const auto testName = CurrentTestName();
        return testName == "WorkerRestartRecoversLargeObjectInDedicatedDataFile"
               || testName == "PassiveScaleDownRecoversLargeObjectInDedicatedDataFile";
    }

    bool IsBackgroundCompactMutationCase() const
    {
        return CurrentTestName() == "BackgroundCompactSurvivesConcurrentMutations";
    }

    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numEtcd = 1;
        opts.numWorkers = 2;
        opts.enableDistributedMaster = "true";
        opts.waitWorkerReady = false;
        opts.addNodeTime = SCALE_DOWN_ADD_TIME;
        distributedDiskPath_ = testCasePath_ + "/distributed_disk";
        DS_ASSERT_OK(CreateDir(distributedDiskPath_, true));
        const bool autoDeleteDeadNode = IsPassiveScaleDownCase();
        const uint64_t nodeDeadTimeoutS =
            autoDeleteDeadNode ? PASSIVE_NODE_DEAD_TIMEOUT_S : RESTART_NODE_DEAD_TIMEOUT_S;
        std::stringstream ss;
        ss << "-l2_cache_type=distributed_disk "
           << "-distributed_disk_path=" << distributedDiskPath_ << " "
           << "-cluster_name=" << CLUSTER_NAME << " "
           << "-distributed_disk_max_data_file_size_mb=" << (IsLargeObjectCase() ? 1 : 1024) << " "
           << "-distributed_disk_compact_interval_s=" << (IsBackgroundCompactMutationCase() ? 1 : 3600) << " "
           << "-distributed_disk_sync_interval_ms=0 "
           << "-distributed_disk_sync_batch_bytes=1 "
           << "-enable_metadata_recovery=true "
           << "-auto_del_dead_node=" << (autoDeleteDeadNode ? "true " : "false ")
           << "-heartbeat_interval_ms=" << HEARTBEAT_INTERVAL_MS << " "
           << "-node_timeout_s=" << NODE_TIMEOUT_S << " "
           << "-node_dead_timeout_s=" << nodeDeadTimeoutS << " "
           << "-v=1 "
           << "-enable_l2_cache_fallback=false";
        opts.workerGflagParams = ss.str();
    }

    void SetUp() override
    {
        CommonTest::SetUp();
        FLAGS_cluster_name = CLUSTER_NAME;
        DS_ASSERT_OK(Init());
        ASSERT_TRUE(cluster_ != nullptr);
        DS_ASSERT_OK(cluster_->StartEtcdCluster());
        InitTestEtcdInstance();
        auto createRc = db_->CreateTable(ETCD_SLOT_RECOVERY_TABLE, ETCD_SLOT_RECOVERY_TABLE);
        ASSERT_TRUE(createRc.IsOk() || createRc.GetCode() == K_DUPLICATED) << createRc.ToString();
        DS_ASSERT_OK(cluster_->StartWorkers());
        DS_ASSERT_OK(cluster_->WaitUntilClusterReadyOrTimeout(30));
        DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 0));
        DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 1));
    }

    void VoluntaryScaleDownInject(int workerIdx)
    {
        std::string checkFilePath = FLAGS_log_dir.c_str();
        std::string client = "client";
        checkFilePath = checkFilePath.substr(0, checkFilePath.length() - client.length()) + "/worker"
                        + std::to_string(workerIdx) + "/log/worker-status";
        std::ofstream ofs(checkFilePath);
        if (!ofs.is_open()) {
            LOG(ERROR) << "Can not open worker status file in " << checkFilePath
                       << ", voluntary scale in will not start, errno: " << errno;
        } else {
            ofs << "voluntary scale in\n";
        }
        ofs.close();
        kill(cluster_->GetWorkerPid(workerIdx), SIGTERM);
    }

    void TearDown() override
    {
        db_.reset();
        ExternalClusterTest::TearDown();
    }

protected:
    uint32_t SlotIdForKey(const std::string &key) const
    {
        return static_cast<uint32_t>(std::hash<std::string>{}(key) % SLOT_NUM);
    }

    std::string FindPeerKeyInSameSlot(const std::string &seedKey) const
    {
        const auto slotId = SlotIdForKey(seedKey);
        for (uint32_t i = 0; i < 1024; ++i) {
            auto candidate = seedKey + "_peer_" + std::to_string(i);
            if (candidate != seedKey && SlotIdForKey(candidate) == slotId) {
                return candidate;
            }
        }
        return "";
    }

    std::string FindKeyForSlot(uint32_t slotId, const std::string &prefix) const
    {
        for (uint32_t i = 0; i < 4096; ++i) {
            auto candidate = prefix + "_" + std::to_string(i);
            if (SlotIdForKey(candidate) == slotId) {
                return candidate;
            }
        }
        return "";
    }

    std::string WorkerSlotRoot(uint32_t workerIndex) const
    {
        HostPort workerAddr;
        auto rc = cluster_->GetWorkerAddr(workerIndex, workerAddr);
        EXPECT_TRUE(rc.IsOk()) << rc.ToString() << ".";
        return BuildSlotStoreRootForWorker(distributedDiskPath_, CLUSTER_NAME,
                                           SanitizeSlotWorkerNamespace(workerAddr.ToString()));
    }

    std::string SlotPathForWorkerAndKey(uint32_t workerIndex, const std::string &key) const
    {
        return JoinPath(WorkerSlotRoot(workerIndex), FormatSlotDir(SlotIdForKey(key)));
    }

    std::string ActiveIndexPath(const std::string &slotPath) const
    {
        SlotManifestData manifest;
        auto rc = SlotManifest::Load(slotPath, manifest);
        EXPECT_TRUE(rc.IsOk()) << rc.ToString() << ".";
        EXPECT_FALSE(manifest.activeIndex.empty());
        return JoinPath(slotPath, manifest.activeIndex);
    }

    bool WaitUntilPathExists(const std::string &path) const
    {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(WAIT_PATH_TIMEOUT_MS);
        while (std::chrono::steady_clock::now() < deadline) {
            if (FileExist(path)) {
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(WAIT_PATH_INTERVAL_MS));
        }
        return FileExist(path);
    }

    bool WaitUntilPathRemoved(const std::string &path) const
    {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(WAIT_PATH_TIMEOUT_MS);
        while (std::chrono::steady_clock::now() < deadline) {
            if (!FileExist(path)) {
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(WAIT_PATH_INTERVAL_MS));
        }
        return !FileExist(path);
    }

    bool WaitUntilManifestCompacted(const std::string &slotPath, SlotManifestData &manifest) const
    {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(WAIT_GET_TIMEOUT_MS);
        while (std::chrono::steady_clock::now() < deadline) {
            SlotManifestData current;
            if (SlotManifest::Load(slotPath, current).IsOk() && current.lastCompactEpochMs > 0
                && current.state == SlotState::NORMAL && current.opType == SlotOperationType::NONE) {
                manifest = std::move(current);
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(WAIT_GET_INTERVAL_MS));
        }
        return false;
    }

    bool WaitUntilGetSucceeds(const std::shared_ptr<KVClient> &client, const std::string &key,
                              const std::string &expectedValue) const
    {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(WAIT_GET_TIMEOUT_MS);
        while (std::chrono::steady_clock::now() < deadline) {
            std::string value;
            auto rc = client->Get(key, value);
            if (rc.IsOk()) {
                return value == expectedValue;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(WAIT_GET_INTERVAL_MS));
        }
        return false;
    }

    bool WaitUntilSlotContainsDelete(const std::string &slotPath, const std::string &key, uint64_t version) const
    {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(WAIT_GET_TIMEOUT_MS);
        while (std::chrono::steady_clock::now() < deadline) {
            SlotManifestData manifest;
            if (SlotManifest::Load(slotPath, manifest).IsOk() && !manifest.activeIndex.empty()) {
                std::vector<SlotRecord> records;
                size_t validBytes = 0;
                auto rc = SlotIndexCodec::ReadAllRecords(JoinPath(slotPath, manifest.activeIndex), records, validBytes);
                if (rc.IsOk()) {
                    for (const auto &record : records) {
                        if (record.type == SlotRecordType::DELETE && record.del.key == key
                            && record.del.version == version) {
                            return true;
                        }
                    }
                }
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(WAIT_GET_INTERVAL_MS));
        }
        return false;
    }

    bool WaitUntilSlotContainsPut(const std::string &slotPath, const std::string &key) const
    {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(WAIT_GET_TIMEOUT_MS);
        while (std::chrono::steady_clock::now() < deadline) {
            SlotManifestData manifest;
            if (SlotManifest::Load(slotPath, manifest).IsOk() && !manifest.activeIndex.empty()) {
                std::vector<SlotRecord> records;
                size_t validBytes = 0;
                auto rc = SlotIndexCodec::ReadAllRecords(JoinPath(slotPath, manifest.activeIndex), records, validBytes);
                if (rc.IsOk()) {
                    for (const auto &record : records) {
                        if (record.type == SlotRecordType::PUT && record.put.key == key) {
                            return true;
                        }
                    }
                }
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(WAIT_GET_INTERVAL_MS));
        }
        return false;
    }

    std::string MetaEtcdKeyForObject(const std::string &key) const
    {
        std::stringstream table;
        table << "/" << CLUSTER_NAME << ETCD_META_TABLE_PREFIX << ETCD_HASH_SUFFIX << "/";
        const auto hash = MurmurHash3_32(key);
        return table.str() + master::Hash2Str(hash) + "/" + key;
    }

    bool WaitUntilSlotContainsDataFileOfSize(const std::string &slotPath, size_t expectedSize) const
    {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(WAIT_GET_TIMEOUT_MS);
        while (std::chrono::steady_clock::now() < deadline) {
            SlotManifestData manifest;
            if (SlotManifest::Load(slotPath, manifest).IsOk()) {
                for (const auto &dataFile : manifest.activeData) {
                    if (FileSize(JoinPath(slotPath, dataFile), false) == static_cast<off_t>(expectedSize)) {
                        return true;
                    }
                }
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(WAIT_GET_INTERVAL_MS));
        }
        return false;
    }

    std::string MakeLargeObjectValue(char ch) const
    {
        return std::string(LARGE_OBJECT_BYTES, ch);
    }

    bool WaitUntilSlotRecoveryIncidentsCleared() const
    {
        std::vector<std::pair<std::string, std::string>> keyValues;
        Status status;
        const auto deadline =
            std::chrono::steady_clock::now() + std::chrono::milliseconds(WAIT_SLOT_RECOVERY_TIMEOUT_MS);
        while (std::chrono::steady_clock::now() < deadline) {
            keyValues.clear();
            status = db_->GetAll(ETCD_SLOT_RECOVERY_TABLE, keyValues);
            if (status.GetCode() == K_NOT_FOUND) {
                return true;
            }
            if (status.IsOk() && keyValues.empty()) {
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(WAIT_GET_INTERVAL_MS));
        }
        keyValues.clear();
        status = db_->GetAll(ETCD_SLOT_RECOVERY_TABLE, keyValues);
        return status.GetCode() == K_NOT_FOUND || (status.IsOk() && keyValues.empty());
    }

    void WaitAllNodesJoinIntoHashRingFast(int num, uint64_t timeoutSec = 60, std::string azName = "")
    {
        int S2Ms = 1000;
        WaitHashRingChange(
            [&](const HashRingPb &hashRing) {
                if (hashRing.workers_size() != num || hashRing.add_node_info_size() != 0
                    || hashRing.del_node_info_size() != 0) {
                    return false;
                }
                for (auto &worker : hashRing.workers()) {
                    if (worker.second.state() != WorkerPb::ACTIVE) {
                        return false;
                    }
                }
                return true;
            },
            timeoutSec * S2Ms, azName);
    }

    std::string WorkerAddress(uint32_t workerIndex) const
    {
        HostPort workerAddr;
        auto rc = cluster_->GetWorkerAddr(workerIndex, workerAddr);
        EXPECT_TRUE(rc.IsOk()) << rc.ToString() << ".";
        return workerAddr.ToString();
    }

    bool LoadSlotRecoveryIncident(const std::string &incidentKey, SlotRecoveryInfoPb &info) const
    {
        std::vector<std::pair<std::string, std::string>> keyValues;
        auto rc = db_->GetAll(ETCD_SLOT_RECOVERY_TABLE, keyValues);
        if (rc.GetCode() == K_NOT_FOUND) {
            return false;
        }
        EXPECT_TRUE(rc.IsOk()) << rc.ToString();
        for (const auto &keyValue : keyValues) {
            if (keyValue.first != incidentKey) {
                continue;
            }
            if (!info.ParseFromString(keyValue.second)) {
                return false;
            }
            return true;
        }
        return false;
    }

    bool WaitUntilIncidentSatisfies(const std::string &incidentKey,
                                    const std::function<bool(const SlotRecoveryInfoPb &)> &predicate,
                                    SlotRecoveryInfoPb *latest = nullptr) const
    {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(WAIT_GET_TIMEOUT_MS);
        while (std::chrono::steady_clock::now() < deadline) {
            SlotRecoveryInfoPb current;
            if (LoadSlotRecoveryIncident(incidentKey, current) && predicate(current)) {
                if (latest != nullptr) {
                    *latest = current;
                }
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(WAIT_GET_INTERVAL_MS));
        }
        if (latest != nullptr) {
            latest->Clear();
            (void)LoadSlotRecoveryIncident(incidentKey, *latest);
        }
        return false;
    }

    std::string DumpSlotRecoveryState() const
    {
        std::vector<std::pair<std::string, std::string>> keyValues;
        auto rc = db_->GetAll(ETCD_SLOT_RECOVERY_TABLE, keyValues);
        std::ostringstream oss;
        oss << "slot_recovery_state: ";
        if (rc.GetCode() == K_NOT_FOUND) {
            oss << "empty";
            return oss.str();
        }
        if (rc.IsError()) {
            oss << rc.ToString();
            return oss.str();
        }
        oss << "incident_count=" << keyValues.size();
        for (const auto &keyValue : keyValues) {
            oss << " key=" << keyValue.first;
        }
        return oss.str();
    }

    void AppendBrokenTail(const std::string &filePath) const
    {
        int fd = open(filePath.c_str(), O_WRONLY | O_APPEND);
        ASSERT_GE(fd, 0) << "open failed for " << filePath;
        const std::string tail = "broken_tail";
        auto written = write(fd, tail.data(), tail.size());
        ASSERT_EQ(written, static_cast<ssize_t>(tail.size()));
        ASSERT_EQ(fsync(fd), 0);
        ASSERT_EQ(close(fd), 0);
    }

    std::string distributedDiskPath_;
};

TEST_F(SlotEndToEndTest, WorkerRestartRepairsSlot)
{
    std::shared_ptr<KVClient> client;
    InitTestKVClient(0, client);

    const std::string key = "tenant_slot_restart_repair";
    const std::string value = GenRandomString(4096);
    SetParam param{ .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE };
    DS_ASSERT_OK(client->Set(key, value, param));

    std::string readBack;
    DS_ASSERT_OK(client->Get(key, readBack));
    ASSERT_EQ(readBack, value);

    const auto slotPathBeforeRestart = SlotPathForWorkerAndKey(0, key);
    ASSERT_TRUE(WaitUntilPathExists(slotPathBeforeRestart)) << slotPathBeforeRestart;
    const auto indexPathBeforeRestart = ActiveIndexPath(slotPathBeforeRestart);
    ASSERT_TRUE(FileExist(indexPathBeforeRestart));
    const auto originalSize = FileSize(indexPathBeforeRestart);

    client.reset();
    DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, 0));

    AppendBrokenTail(indexPathBeforeRestart);
    const auto corruptedSize = FileSize(indexPathBeforeRestart);
    ASSERT_GT(corruptedSize, originalSize);

    DS_ASSERT_OK(cluster_->StartNode(WORKER, 0, ""));
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 0));

    const auto slotPathAfterRestart = SlotPathForWorkerAndKey(0, key);
    ASSERT_TRUE(WaitUntilPathExists(slotPathAfterRestart)) << slotPathAfterRestart;
    const auto indexPathAfterRestart = ActiveIndexPath(slotPathAfterRestart);

    std::vector<SlotRecord> repairedRecords;
    size_t repairedValidBytes = 0;
    DS_ASSERT_OK(SlotIndexCodec::ReadAllRecords(indexPathAfterRestart, repairedRecords, repairedValidBytes));
    ASSERT_EQ(repairedValidBytes, static_cast<size_t>(FileSize(indexPathAfterRestart)));
    ASSERT_FALSE(repairedRecords.empty());
    ASSERT_TRUE(std::any_of(repairedRecords.begin(), repairedRecords.end(), [&](const SlotRecord &record) {
        return record.type == SlotRecordType::PUT && record.put.key == key;
    }));

    std::vector<SlotRecord> finalRecords;
    size_t finalValidBytes = 0;
    DS_ASSERT_OK(SlotIndexCodec::ReadAllRecords(indexPathAfterRestart, finalRecords, finalValidBytes));
    ASSERT_EQ(finalValidBytes, static_cast<size_t>(FileSize(indexPathAfterRestart)));
}

TEST_F(SlotEndToEndTest, MultiWorkerUsesScopedSlots)
{
    std::shared_ptr<KVClient> client0;
    std::shared_ptr<KVClient> client1;
    InitTestKVClient(0, client0);
    InitTestKVClient(1, client1);

    const std::string key0 = "tenant_shared_slot_worker0";
    const std::string key1 = FindPeerKeyInSameSlot(key0);
    ASSERT_FALSE(key1.empty());
    ASSERT_EQ(SlotIdForKey(key0), SlotIdForKey(key1));

    const std::string value0 = "worker0_" + GenRandomString(2048);
    const std::string value1 = "worker1_" + GenRandomString(2048);
    SetParam param{ .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE };
    DS_ASSERT_OK(client0->Set(key0, value0, param));
    DS_ASSERT_OK(client1->Set(key1, value1, param));

    const auto slotPath0 = SlotPathForWorkerAndKey(0, key0);
    const auto slotPath1 = SlotPathForWorkerAndKey(1, key1);
    ASSERT_TRUE(WaitUntilPathExists(slotPath0)) << slotPath0;
    ASSERT_TRUE(WaitUntilPathExists(slotPath1)) << slotPath1;
    ASSERT_NE(slotPath0, slotPath1);
    ASSERT_EQ(FormatSlotDir(SlotIdForKey(key0)), FormatSlotDir(SlotIdForKey(key1)));

    std::string get0;
    std::string get1;
    DS_ASSERT_OK(client0->Get(key0, get0));
    DS_ASSERT_OK(client1->Get(key1, get1));
    ASSERT_EQ(get0, value0);
    ASSERT_EQ(get1, value1);
}

TEST_F(SlotEndToEndTest, DeleteWritesOwnerTombstone)
{
    std::shared_ptr<KVClient> client;
    InitTestKVClient(0, client);

    const std::string key = "tenant_slot_delete_tombstone";
    const std::string value = GenRandomString(2048);
    SetParam param{ .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE };
    DS_ASSERT_OK(client->Set(key, value, param));

    const auto slotPath = SlotPathForWorkerAndKey(0, key);
    ASSERT_TRUE(WaitUntilPathExists(slotPath)) << slotPath;
    ASSERT_TRUE(WaitUntilGetSucceeds(client, key, value));

    DS_ASSERT_OK(client->Del(key));
    ASSERT_TRUE(WaitUntilSlotContainsDelete(slotPath, key, UINT64_MAX));

    std::string afterDelete;
    ASSERT_TRUE(!client->Get(key, afterDelete).IsOk());
}

TEST_F(SlotEndToEndTest, ConcurrentWriteReadDeleteWorks)
{
    std::shared_ptr<KVClient> client0;
    std::shared_ptr<KVClient> client1;
    InitTestKVClient(0, client0);
    InitTestKVClient(1, client1);

    constexpr int keyCount = 12;
    SetParam param{ .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE };
    std::vector<std::pair<std::string, std::string>> keyValues;
    keyValues.reserve(keyCount);
    for (int idx = 0; idx < keyCount; ++idx) {
        keyValues.emplace_back("tenant_slot_concurrent_" + std::to_string(idx), "value_" + GenRandomString(512));
    }

    std::vector<std::thread> writers;
    for (int idx = 0; idx < keyCount; ++idx) {
        writers.emplace_back([&, idx]() {
            auto &client = (idx % 2 == 0) ? client0 : client1;
            DS_ASSERT_OK(client->Set(keyValues[idx].first, keyValues[idx].second, param));
        });
    }
    for (auto &writer : writers) {
        writer.join();
    }

    for (int idx = 0; idx < keyCount; ++idx) {
        auto &client = (idx % 2 == 0) ? client0 : client1;
        ASSERT_TRUE(WaitUntilGetSucceeds(client, keyValues[idx].first, keyValues[idx].second));
    }

    std::vector<std::thread> deleters;
    for (int idx = 0; idx < keyCount; idx += 3) {
        deleters.emplace_back([&, idx]() {
            auto &client = (idx % 2 == 0) ? client0 : client1;
            DS_ASSERT_OK(client->Del(keyValues[idx].first));
        });
    }
    for (auto &deleter : deleters) {
        deleter.join();
    }

    for (int idx = 0; idx < keyCount; ++idx) {
        auto &client = (idx % 2 == 0) ? client0 : client1;
        const auto slotPath = SlotPathForWorkerAndKey(idx % 2 == 0 ? 0 : 1, keyValues[idx].first);
        if (idx % 3 == 0) {
            ASSERT_TRUE(WaitUntilSlotContainsDelete(slotPath, keyValues[idx].first, UINT64_MAX));
            std::string value;
            ASSERT_TRUE(!client->Get(keyValues[idx].first, value).IsOk());
        } else {
            ASSERT_TRUE(WaitUntilGetSucceeds(client, keyValues[idx].first, keyValues[idx].second));
        }
    }
}

TEST_F(SlotEndToEndTest, WorkerRestartRecoversSlotAndMetadata)
{
    WaitAllNodesJoinIntoHashRing(2, 20);

    std::shared_ptr<KVClient> client0;
    std::shared_ptr<KVClient> client1;
    InitTestKVClient(0, client0);
    InitTestKVClient(1, client1);

    const std::string key = "tenant_slot_restart_recover_metadata";
    const std::string value = "restart_value_" + GenRandomString(2048);
    SetParam param{ .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE };
    DS_ASSERT_OK(client0->Set(key, value, param));
    ASSERT_TRUE(WaitUntilGetSucceeds(client0, key, value));

    const auto sourceSlotPath = SlotPathForWorkerAndKey(0, key);
    ASSERT_TRUE(WaitUntilPathExists(sourceSlotPath)) << sourceSlotPath;
    ASSERT_TRUE(WaitUntilSlotContainsPut(sourceSlotPath, key)) << sourceSlotPath;

    client0.reset();
    ASSERT_EQ(kill(cluster_->GetWorkerPid(0), SIGTERM), 0);

    DS_ASSERT_OK(cluster_->StartNode(WORKER, 0, ""));
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 0));
    WaitAllNodesJoinIntoHashRing(2, 20);

    ASSERT_TRUE(WaitUntilGetSucceeds(client1, key, value));
    InitTestKVClient(0, client0);
    DS_ASSERT_OK(client0->Set(key, value, param));
}

TEST_F(SlotEndToEndTest, DistributedDiskDoesNotWriteMetadataToEtcd)
{
    std::shared_ptr<KVClient> client;
    InitTestKVClient(0, client);

    const std::string key = "tenant_slot_distributed_disk_no_etcd_meta";
    const std::string value = "distributed_disk_value_" + GenRandomString(2048);
    SetParam param{ .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE };
    DS_ASSERT_OK(client->Set(key, value, param));

    ASSERT_TRUE(WaitUntilGetSucceeds(client, key, value));

    RangeSearchResult res;
    DS_ASSERT_NOT_OK(db_->RawGet(MetaEtcdKeyForObject(key), res));
}

TEST_F(SlotEndToEndTest, PassiveScaleDownRecoversSlotAndMetadata)
{
    WaitAllNodesJoinIntoHashRing(2, 20);

    std::shared_ptr<KVClient> client0;
    std::shared_ptr<KVClient> client1;
    InitTestKVClient(0, client0);
    InitTestKVClient(1, client1);

    const std::string key = "tenant_slot_passive_scale_down";
    const std::string value = "scale_down_value_" + GenRandomString(2048);
    SetParam param{ .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE };
    DS_ASSERT_OK(client0->Set(key, value, param));
    ASSERT_TRUE(WaitUntilGetSucceeds(client0, key, value));

    const auto sourceSlotPath = SlotPathForWorkerAndKey(0, key);
    ASSERT_TRUE(WaitUntilPathExists(sourceSlotPath)) << sourceSlotPath;
    ASSERT_TRUE(WaitUntilSlotContainsPut(sourceSlotPath, key)) << sourceSlotPath;

    ASSERT_EQ(kill(cluster_->GetWorkerPid(0), SIGKILL), 0);
    WaitAllNodesJoinIntoHashRing(1, PASSIVE_NODE_DEAD_TIMEOUT_S + 10);

    const auto targetSlotPath = SlotPathForWorkerAndKey(1, key);
    ASSERT_TRUE(WaitUntilPathExists(targetSlotPath)) << targetSlotPath;
    ASSERT_TRUE(WaitUntilSlotContainsPut(targetSlotPath, key)) << targetSlotPath;
    ASSERT_TRUE(WaitUntilGetSucceeds(client1, key, value));

    ASSERT_TRUE(WaitUntilSlotRecoveryIncidentsCleared()) << DumpSlotRecoveryState();
}

TEST_F(SlotEndToEndTest, WorkerRestartRecoversLargeObjectInDedicatedDataFile)
{
    WaitAllNodesJoinIntoHashRing(2, 20);

    std::shared_ptr<KVClient> client0;
    std::shared_ptr<KVClient> client1;
    InitTestKVClient(0, client0);
    InitTestKVClient(1, client1);

    const std::string key = "tenant_slot_restart_large_object";
    const std::string value = MakeLargeObjectValue('R');
    SetParam param{ .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE };
    DS_ASSERT_OK(client0->Set(key, value, param));
    ASSERT_TRUE(WaitUntilGetSucceeds(client0, key, value));

    const auto sourceSlotPath = SlotPathForWorkerAndKey(0, key);
    ASSERT_TRUE(WaitUntilPathExists(sourceSlotPath)) << sourceSlotPath;
    ASSERT_TRUE(WaitUntilSlotContainsPut(sourceSlotPath, key)) << sourceSlotPath;
    ASSERT_TRUE(WaitUntilSlotContainsDataFileOfSize(sourceSlotPath, value.size())) << sourceSlotPath;

    client0.reset();
    ASSERT_EQ(kill(cluster_->GetWorkerPid(0), SIGTERM), 0);

    DS_ASSERT_OK(cluster_->StartNode(WORKER, 0, ""));
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 0));
    WaitAllNodesJoinIntoHashRing(2, 20);

    ASSERT_TRUE(WaitUntilGetSucceeds(client1, key, value));
}

TEST_F(SlotEndToEndTest, PassiveScaleDownRecoversLargeObjectInDedicatedDataFile)
{
    WaitAllNodesJoinIntoHashRing(2, 20);

    std::shared_ptr<KVClient> client0;
    std::shared_ptr<KVClient> client1;
    InitTestKVClient(0, client0);
    InitTestKVClient(1, client1);

    const std::string key = "tenant_slot_passive_scale_down_large_object";
    const std::string value = MakeLargeObjectValue('P');
    SetParam param{ .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE };
    DS_ASSERT_OK(client0->Set(key, value, param));
    ASSERT_TRUE(WaitUntilGetSucceeds(client0, key, value));

    const auto sourceSlotPath = SlotPathForWorkerAndKey(0, key);
    ASSERT_TRUE(WaitUntilPathExists(sourceSlotPath)) << sourceSlotPath;
    ASSERT_TRUE(WaitUntilSlotContainsPut(sourceSlotPath, key)) << sourceSlotPath;
    ASSERT_TRUE(WaitUntilSlotContainsDataFileOfSize(sourceSlotPath, value.size())) << sourceSlotPath;

    ASSERT_EQ(kill(cluster_->GetWorkerPid(0), SIGKILL), 0);
    WaitAllNodesJoinIntoHashRing(1, PASSIVE_NODE_DEAD_TIMEOUT_S + 10);

    const auto targetSlotPath = SlotPathForWorkerAndKey(1, key);
    ASSERT_TRUE(WaitUntilPathExists(targetSlotPath)) << targetSlotPath;
    ASSERT_TRUE(WaitUntilSlotContainsPut(targetSlotPath, key)) << targetSlotPath;
    ASSERT_TRUE(WaitUntilSlotContainsDataFileOfSize(targetSlotPath, value.size())) << targetSlotPath;
    ASSERT_TRUE(WaitUntilGetSucceeds(client1, key, value));
}

TEST_F(SlotEndToEndTest, BackgroundCompactSurvivesConcurrentMutations)
{
    std::shared_ptr<KVClient> client0;
    InitTestKVClient(0, client0);

    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "slotstore.Slot.Compact.BeforeCommit", "1*sleep(1000)"));

    SetParam param{ .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE };
    const std::string keepKey = "tenant_slot_background_compact_keep";
    const std::string deleteKey = "tenant_slot_background_compact_delete";
    const std::string initialValue = "initial_" + GenRandomString(1024);
    const std::string updatedValue = "updated_" + GenRandomString(1024);

    DS_ASSERT_OK(client0->Set(keepKey, initialValue, param));
    DS_ASSERT_OK(client0->Set(deleteKey, "delete_me", param));
    ASSERT_TRUE(WaitUntilGetSucceeds(client0, keepKey, initialValue));

    const auto slotPath = SlotPathForWorkerAndKey(0, keepKey);
    ASSERT_TRUE(WaitUntilPathExists(slotPath)) << slotPath;

    auto waitUntilInjectExecuted = [&](const std::string &name) -> bool {
        uint64_t executeCount = 0;
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(WAIT_GET_TIMEOUT_MS);
        while (std::chrono::steady_clock::now() < deadline) {
            auto rc = cluster_->GetInjectActionExecuteCount(WORKER, 0, name, executeCount);
            if (!rc.IsOk()) {
                return false;
            }
            if (executeCount >= 1) {
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(WAIT_GET_INTERVAL_MS));
        }
        return false;
    };

    ASSERT_TRUE(waitUntilInjectExecuted("slotstore.Slot.Compact.BeforeCommit"));

    DS_ASSERT_OK(client0->Set(keepKey, updatedValue, param));
    DS_ASSERT_OK(client0->Del(deleteKey));

    SlotManifestData manifest;
    ASSERT_TRUE(WaitUntilManifestCompacted(slotPath, manifest));
    ASSERT_NE(manifest.activeIndex.find("index_compact_"), std::string::npos);

    ASSERT_TRUE(WaitUntilGetSucceeds(client0, keepKey, updatedValue));
    std::string deletedValue;
    ASSERT_TRUE(!client0->Get(deleteKey, deletedValue).IsOk());

    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, 0, "slotstore.Slot.Compact.BeforeCommit"));
}

TEST_F(SlotEndToEndTest, VoluntaryScaleDownMovesSlotAndMetadata)
{
    WaitAllNodesJoinIntoHashRing(2, 20);

    std::shared_ptr<KVClient> client0;
    std::shared_ptr<KVClient> client1;
    InitTestKVClient(0, client0);
    InitTestKVClient(1, client1);

    std::vector<std::string> keys;
    std::string value = "value_" + GenRandomString(1024);
    for (int i = 0; i < 10; ++i) {
        std::string key = "tenant_slot_voluntary_scale_down_" + std::to_string(i);
        keys.push_back(key);
        SetParam param{ .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE };
        DS_ASSERT_OK(client0->Set(key, value, param));
    }
    client0.reset();
    VoluntaryScaleDownInject(0);
    WaitAllNodesJoinIntoHashRing(1, 20);

    for(const auto &key: keys) {
        std::string getValue;
        DS_ASSERT_OK(client1->Get(key, getValue));
        ASSERT_EQ(getValue, value);
    }

    client1.reset();
    kill(cluster_->GetWorkerPid(0), SIGTERM);  // worker index 0
    const int interval = 2000;                 // wait 10000ms for clean map;
    std::this_thread::sleep_for(std::chrono::milliseconds(interval));
    DS_ASSERT_OK(cluster_->StartNode(WORKER, 0, ""));
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 0));
    InitTestKVClient(1, client1);
    for(const auto &key: keys) {
        std::string getValue;
        DS_ASSERT_OK(client1->Get(key, getValue));
        ASSERT_EQ(getValue, value);
    }
}

class SlotEndToEndScaleTest : public SlotEndToEndTest {
public:

    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numEtcd = 1;
        opts.numWorkers = 3;
        opts.enableDistributedMaster = "true";
        opts.waitWorkerReady = false;
        opts.addNodeTime = SCALE_DOWN_ADD_TIME;
        distributedDiskPath_ = testCasePath_ + "/distributed_disk";
        DS_ASSERT_OK(CreateDir(distributedDiskPath_, true));
        std::stringstream ss;
        ss << "-l2_cache_type=distributed_disk "
           << "-distributed_disk_path=" << distributedDiskPath_ << " "
           << "-cluster_name=" << CLUSTER_NAME << " "
           << "-distributed_disk_sync_interval_ms=0 "
           << "-distributed_disk_sync_batch_bytes=1 "
           << "-enable_metadata_recovery=true "
           << "-auto_del_dead_node=true "
           << "-heartbeat_interval_ms=" << HEARTBEAT_INTERVAL_MS << " "
           << "-node_timeout_s=" << NODE_TIMEOUT_S << " "
           << "-node_dead_timeout_s=" << PASSIVE_NODE_DEAD_TIMEOUT_S << " "
           << "-v=1 "
           << "-enable_l2_cache_fallback=false";
        opts.workerGflagParams = ss.str();
    }
};

TEST_F(SlotEndToEndScaleTest, VoluntaryScaleDownAndScaleDown)
{
    WaitAllNodesJoinIntoHashRing(3, 20);

    std::shared_ptr<KVClient> client0;
    std::shared_ptr<KVClient> client1;
    InitTestKVClient(0, client0);
    InitTestKVClient(1, client1);

    std::vector<std::string> keys;
    std::string value = "value_" + GenRandomString(1024);
    for (int i = 0; i < 10; ++i) {
        std::string key = "tenant_slot_voluntary_scale_down_" + std::to_string(i);
        keys.push_back(key);
        SetParam param{ .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE };
        DS_ASSERT_OK(client0->Set(key, value, param));
    }
    client0.reset();
    VoluntaryScaleDownInject(0);
    WaitAllNodesJoinIntoHashRing(2, 20);

    for(const auto &key: keys) {
        std::string getValue;
        DS_ASSERT_OK(client1->Get(key, getValue));
        ASSERT_EQ(getValue, value);
    }

    kill(cluster_->GetWorkerPid(2), SIGTERM);  // worker index 0
    WaitAllNodesJoinIntoHashRing(1, 20);
    for(const auto &key: keys) {
        std::string getValue;
        DS_ASSERT_OK(client1->Get(key, getValue));
        ASSERT_EQ(getValue, value);
    }
}

TEST_F(SlotEndToEndScaleTest, SameSlotDualFailure)
{
    LOG(INFO) << "Scenario: worker0 and worker1 each own data in every slot, then both workers fail and worker2 "
                 "must recover same-slot data from both failed workers.";
    WaitAllNodesJoinIntoHashRing(3, 20);

    std::shared_ptr<KVClient> client0;
    std::shared_ptr<KVClient> client1;
    std::shared_ptr<KVClient> client2;
    InitTestKVClient(0, client0);
    InitTestKVClient(1, client1);
    InitTestKVClient(2, client2);

    SetParam param{ .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE };
    std::vector<std::pair<std::string, std::string>> worker0Keys;
    std::vector<std::pair<std::string, std::string>> worker1Keys;
    worker0Keys.reserve(SLOT_NUM);
    worker1Keys.reserve(SLOT_NUM);
    for (uint32_t slotId = 0; slotId < SLOT_NUM; ++slotId) {
        const std::string worker0Key = FindKeyForSlot(slotId, "tenant_slot_recovery_chain_worker0");
        const std::string worker1Key = FindKeyForSlot(slotId, "tenant_slot_recovery_chain_worker1");
        ASSERT_FALSE(worker0Key.empty()) << "worker0 slotId=" << slotId;
        ASSERT_FALSE(worker1Key.empty()) << "worker1 slotId=" << slotId;
        ASSERT_EQ(SlotIdForKey(worker0Key), slotId);
        ASSERT_EQ(SlotIdForKey(worker1Key), slotId);

        worker0Keys.emplace_back(worker0Key, "value_worker0_slot_" + std::to_string(slotId));
        worker1Keys.emplace_back(worker1Key, "value_worker1_slot_" + std::to_string(slotId));
    }

    for (const auto &keyValue : worker0Keys) {
        DS_ASSERT_OK(client0->Set(keyValue.first, keyValue.second, param));
    }
    for (const auto &keyValue : worker1Keys) {
        DS_ASSERT_OK(client1->Set(keyValue.first, keyValue.second, param));
    }

    ASSERT_EQ(kill(cluster_->GetWorkerPid(0), SIGKILL), 0);
    ASSERT_EQ(kill(cluster_->GetWorkerPid(1), SIGKILL), 0);
    WaitAllNodesJoinIntoHashRing(1, PASSIVE_NODE_DEAD_TIMEOUT_S + 10);

    for (const auto &keyValue : worker0Keys) {
        ASSERT_TRUE(WaitUntilGetSucceeds(client2, keyValue.first, keyValue.second));
    }
    for (const auto &keyValue : worker1Keys) {
        ASSERT_TRUE(WaitUntilGetSucceeds(client2, keyValue.first, keyValue.second));
    }
}

class SlotEndToEndPassiveScaleDownTest : public SlotEndToEndTest {
public:
    bool UseSmallClusterConfig() const
    {
        return CurrentTestName() == "RecoveryPreloadOomKeepsReceiverData";
    }

    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numEtcd = 1;
        opts.numWorkers = UseSmallClusterConfig() ? 2 : 3;
        opts.enableDistributedMaster = "true";
        opts.waitWorkerReady = true;
        opts.addNodeTime = 1;
        if (UseSmallClusterConfig()) {
            opts.injectActions = "SlotClient.Init.SetSlotNum:return(2)";
        }
        distributedDiskPath_ = testCasePath_ + "/distributed_disk";
        DS_ASSERT_OK(CreateDir(distributedDiskPath_, true));
        std::stringstream ss;
        ss << "-l2_cache_type=distributed_disk "
           << "-distributed_disk_path=" << distributedDiskPath_ << " "
           << "-cluster_name=" << CLUSTER_NAME << " "
           << "-distributed_disk_sync_interval_ms=0 "
           << "-distributed_disk_sync_batch_bytes=1 "
           << "-enable_metadata_recovery=true "
           << "-heartbeat_interval_ms=" << HEARTBEAT_INTERVAL_MS << " "
           << "-node_timeout_s=" << NODE_TIMEOUT_S << " "
           << "-node_dead_timeout_s=" << PASSIVE_NODE_DEAD_TIMEOUT_S << " "
           << "-v=1 "
           << "-enable_l2_cache_fallback=false "
           << "-enable_reconciliation=false "
           << "-shared_memory_size_mb=16 ";
        opts.workerGflagParams = ss.str();
    }
};

TEST_F(SlotEndToEndPassiveScaleDownTest, RecoveryTakeoverOwnerFailsAgainDataIntact)
{
    LOG(INFO) << "Scenario: worker0 fails, worker1 starts recovering worker0, then worker1 fails and worker2 "
                 "takes over via successor incident.";

    std::shared_ptr<KVClient> client0;
    std::shared_ptr<KVClient> client1;
    std::shared_ptr<KVClient> client2;
    InitTestKVClient(0, client0);
    InitTestKVClient(1, client1);
    InitTestKVClient(2, client2);

    std::vector<std::pair<std::string, std::string>> keyValues;
    SetParam param{ .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE };
    for (uint32_t slotId = 0; slotId < SLOT_NUM; ++slotId) {
        const std::string worker0Key = FindKeyForSlot(slotId, "tenant_slot_owner_fails_again_worker0");
        const std::string worker1Key = FindKeyForSlot(slotId, "tenant_slot_owner_fails_again_worker1");
        ASSERT_FALSE(worker0Key.empty());
        ASSERT_FALSE(worker1Key.empty());
        const std::string worker0Value = "value_owner_fails_again_worker0_" + std::to_string(slotId);
        const std::string worker1Value = "value_owner_fails_again_worker1_" + std::to_string(slotId);
        keyValues.emplace_back(worker0Key, worker0Value);
        keyValues.emplace_back(worker1Key, worker1Value);
        DS_ASSERT_OK(client0->Set(worker0Key, worker0Value, param));
        DS_ASSERT_OK(client1->Set(worker1Key, worker1Value, param));
    }

    const std::string worker0 = WorkerAddress(0);
    const std::string worker1 = WorkerAddress(1);
    DS_ASSERT_OK(
        cluster_->SetInjectAction(WORKER, 1, "SlotRecoveryManager.ExecuteRecoveryTask.BeforeRecover", "1*sleep(1500)"));

    ASSERT_EQ(kill(cluster_->GetWorkerPid(0), SIGKILL), 0);
    WaitAllNodesJoinIntoHashRingFast(2, PASSIVE_NODE_DEAD_TIMEOUT_S + 6);
    // Wait until worker1 has already claimed worker0's task, then inject the second failure.
    ASSERT_TRUE(WaitUntilIncidentSatisfies(worker0, [&](const SlotRecoveryInfoPb &info) {
        return std::any_of(info.recovery_tasks().begin(), info.recovery_tasks().end(), [&](const auto &task) {
            return task.owner_worker() == worker1 && task.task_status() == RecoveryTaskPb::IN_PROGRESS;
        });
    })) << DumpSlotRecoveryState();

    DS_ASSERT_OK(cluster_->KillWorker(1));
    WaitAllNodesJoinIntoHashRingFast(1, PASSIVE_NODE_DEAD_TIMEOUT_S + 6);

    // E2E focus: incident chain converges and all data remains readable after consecutive failures.
    ASSERT_TRUE(WaitUntilSlotRecoveryIncidentsCleared()) << DumpSlotRecoveryState();
    for (const auto &keyValue : keyValues) {
        ASSERT_TRUE(WaitUntilGetSucceeds(client2, keyValue.first, keyValue.second));
    }
}

TEST_F(SlotEndToEndPassiveScaleDownTest, RecoveryTakeoverOwnerRestartDataIntact)
{
    LOG(INFO) << "Scenario: worker0 fails, worker1 is recovering worker0 and then crashes/restarts; worker2 stays "
                 "alive and data should stay intact.";

    std::shared_ptr<KVClient> client0;
    std::shared_ptr<KVClient> client1;
    std::shared_ptr<KVClient> client2;
    InitTestKVClient(0, client0);
    InitTestKVClient(1, client1);
    InitTestKVClient(2, client2);

    std::vector<std::pair<std::string, std::string>> keyValues;
    SetParam param{ .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE };
    for (uint32_t slotId = 0; slotId < SLOT_NUM; ++slotId) {
        const std::string worker0Key = FindKeyForSlot(slotId, "tenant_slot_successor_order_worker0");
        const std::string worker1Key = FindKeyForSlot(slotId, "tenant_slot_successor_order_worker1");
        ASSERT_FALSE(worker0Key.empty());
        ASSERT_FALSE(worker1Key.empty());
        const std::string worker0Value = "value_successor_order_worker0_" + std::to_string(slotId);
        const std::string worker1Value = "value_successor_order_worker1_" + std::to_string(slotId);
        keyValues.emplace_back(worker0Key, worker0Value);
        keyValues.emplace_back(worker1Key, worker1Value);
        DS_ASSERT_OK(client0->Set(worker0Key, worker0Value, param));
        DS_ASSERT_OK(client1->Set(worker1Key, worker1Value, param));
    }

    const std::string worker0 = WorkerAddress(0);
    const std::string worker1 = WorkerAddress(1);
    DS_ASSERT_OK(
        cluster_->SetInjectAction(WORKER, 1, "SlotRecoveryManager.ExecuteRecoveryTask.BeforeRecover", "1*sleep(1500)"));

    DS_ASSERT_OK(cluster_->KillWorker(0));

    ASSERT_TRUE(WaitUntilIncidentSatisfies(worker0, [&](const SlotRecoveryInfoPb &info) {
        return std::any_of(info.recovery_tasks().begin(), info.recovery_tasks().end(), [&](const auto &task) {
            return task.owner_worker() == worker1 && task.task_status() == RecoveryTaskPb::IN_PROGRESS;
        });
    })) << DumpSlotRecoveryState();

    DS_ASSERT_OK(cluster_->KillWorker(1));

    // Restart worker1 after it fails during in-progress recovery.
    DS_ASSERT_OK(cluster_->StartNode(WORKER, 1, ""));
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 1));

    // E2E focus: incident chain converges and all data remains readable after consecutive failures.
    ASSERT_TRUE(WaitUntilSlotRecoveryIncidentsCleared()) << DumpSlotRecoveryState();
    for (const auto &keyValue : keyValues) {
        ASSERT_TRUE(WaitUntilGetSucceeds(client2, keyValue.first, keyValue.second));
    }
}

TEST_F(SlotEndToEndPassiveScaleDownTest, RestoreObjectWithTtl)
{
    std::shared_ptr<KVClient> client0;
    std::shared_ptr<KVClient> client1;
    InitTestKVClient(0, client0);
    InitTestKVClient(1, client1);

    constexpr int ttl = 5;
    SetParam param{ .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE, .ttlSecond = ttl };
    const std::string objKey0 = FindKeyForSlot(0, "object_with_ttl_worker0");
    ASSERT_FALSE(objKey0.empty());
    const std::string value0 = "value_worker0_" + std::to_string(0);
    DS_ASSERT_OK(client0->Set(objKey0, value0, param));
    Timer remainTtl(static_cast<int64_t>(ttl * S2MS));
    std::this_thread::sleep_for(std::chrono::milliseconds((500)));

    client0.reset();
    DS_ASSERT_OK(cluster_->KillWorker(0));
    WaitAllNodesJoinIntoHashRingFast(2, PASSIVE_NODE_DEAD_TIMEOUT_S + 6);
    ASSERT_TRUE(WaitUntilSlotRecoveryIncidentsCleared()) << DumpSlotRecoveryState();

    std::this_thread::sleep_for(std::chrono::milliseconds((remainTtl.GetRemainingTimeMs() + 1) * S2MS));
    std::string val;
    ASSERT_EQ(client1->Get(objKey0, val).GetCode(), K_NOT_FOUND);
}

TEST_F(SlotEndToEndPassiveScaleDownTest, VoluntaryToPassiveScaleDown)
{
    std::shared_ptr<KVClient> client0;
    InitTestKVClient(0, client0);

    std::vector<std::string> keys;
    std::string value = "value_" + GenRandomString(128);
    SetParam param{ .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE };
    for (int i = 0; i < 10; ++i) {
        std::string key = "slot_voluntary_to_passive_" + std::to_string(i);
        keys.push_back(key);
        DS_ASSERT_OK(client0->Set(key, value, param));
    }
    client0.reset();
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "VoluntaryScaledown.MigrateData.Delay", "sleep(3000)"));
    VoluntaryScaleDownInject(0);
    std::this_thread::sleep_for(std::chrono::milliseconds(S2MS));
    DS_ASSERT_OK(cluster_->KillWorker(0));

    WaitAllNodesJoinIntoHashRingFast(2, PASSIVE_NODE_DEAD_TIMEOUT_S + 6);
    ASSERT_TRUE(WaitUntilSlotRecoveryIncidentsCleared()) << DumpSlotRecoveryState();

    std::shared_ptr<KVClient> client1;
    InitTestKVClient(1, client1);
    for (const auto &key : keys) {
        std::string getValue;
        DS_ASSERT_OK(client1->Get(key, getValue));
        ASSERT_EQ(getValue, value);
    }
}

TEST_F(SlotEndToEndPassiveScaleDownTest, VoluntaryToPassiveScaleDownRemoveOldSlot)
{
    std::shared_ptr<KVClient> client0, client1;
    InitTestKVClient(0, client0);
    InitTestKVClient(1, client1);
    const auto worker0SlotRoot = WorkerSlotRoot(0);
    std::vector<std::string> keys;
    std::string value = "value_" + GenRandomString(128);
    std::string value1 = "value_" + GenRandomString(128);
    SetParam param{ .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE };
    for (int i = 0; i < 10; ++i) {
        std::string key = "slot_voluntary_to_passive_" + std::to_string(i);
        keys.push_back(key);
        DS_ASSERT_OK(client0->Set(key, value, param));
        DS_ASSERT_OK(client1->Set(key, value1, param));
    }
    ASSERT_TRUE(WaitUntilPathExists(worker0SlotRoot)) << worker0SlotRoot;
    client0.reset();
    VoluntaryScaleDownInject(0);


    WaitAllNodesJoinIntoHashRingFast(2, PASSIVE_NODE_DEAD_TIMEOUT_S + 6);
    ASSERT_TRUE(WaitUntilSlotRecoveryIncidentsCleared()) << DumpSlotRecoveryState();
    ASSERT_TRUE(WaitUntilPathRemoved(worker0SlotRoot)) << worker0SlotRoot;
    
    for (const auto &key : keys) {
        std::string getValue;
        DS_ASSERT_OK(client1->Get(key, getValue));
        ASSERT_EQ(getValue, value1);
    }
}

TEST_F(SlotEndToEndPassiveScaleDownTest, RecoveryPreloadOomKeepsReceiverData)
{
    LOG(INFO) << "Scenario: worker1 is near high water, worker0 fails, and slot recovery preload should stop "
                 "without evicting worker1's own WRITE_BACK_L2_CACHE_EVICT data.";

    std::shared_ptr<KVClient> client0;
    std::shared_ptr<KVClient> client1;
    InitTestKVClient(0, client0);
    InitTestKVClient(1, client1);

    constexpr int objectCountPerWorker = 10;
    constexpr size_t objectBytes = 900UL * 1024UL;
    SetParam param{ .writeMode = WriteMode::WRITE_BACK_L2_CACHE_EVICT };
    const std::string objectValue(objectBytes, 'x');
    std::vector<std::string> worker0Keys;
    std::vector<std::string> worker1Keys;
    for (int idx = 0; idx < objectCountPerWorker; ++idx) {
        auto worker0Key = FindKeyForSlot(0, "slot_recovery_oom_worker0_" + std::to_string(idx));
        auto worker1Key = FindKeyForSlot(0, "slot_recovery_oom_worker1_" + std::to_string(idx));
        ASSERT_FALSE(worker0Key.empty());
        ASSERT_FALSE(worker1Key.empty());
        worker0Keys.emplace_back(worker0Key);
        worker1Keys.emplace_back(worker1Key);
    }

    for (const auto &key : worker0Keys) {
        DS_ASSERT_OK(client0->Set(key, objectValue, param));
    }
    for (const auto &key : worker1Keys) {
        DS_ASSERT_OK(client1->Set(key, objectValue, param));
    }
    for (const auto &key : worker0Keys) {
        ASSERT_TRUE(WaitUntilSlotContainsPut(SlotPathForWorkerAndKey(0, key), key)) << key;
    }
    for (const auto &key : worker1Keys) {
        ASSERT_TRUE(WaitUntilSlotContainsPut(SlotPathForWorkerAndKey(1, key), key)) << key;
    }

    client0.reset();
    DS_ASSERT_OK(cluster_->KillWorker(0));
    WaitAllNodesJoinIntoHashRingFast(1, PASSIVE_NODE_DEAD_TIMEOUT_S + 6);
    ASSERT_TRUE(WaitUntilSlotRecoveryIncidentsCleared()) << DumpSlotRecoveryState();

    for (const auto &key : worker1Keys) {
        std::string value;
        DS_ASSERT_OK(client1->Get(key, value));
    }
}

TEST_F(SlotEndToEndPassiveScaleDownTest, VoluntaryToPassiveScaleDownRemoveOldSlot)
{
    std::shared_ptr<KVClient> client0, client1;
    InitTestKVClient(0, client0);
    InitTestKVClient(1, client1);
    const auto worker0SlotRoot = WorkerSlotRoot(0);
    std::vector<std::string> keys;
    std::string value = "value_" + GenRandomString(128);
    std::string value1 = "value_" + GenRandomString(128);
    SetParam param{ .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE };
    for (int i = 0; i < 10; ++i) {
        std::string key = "slot_voluntary_to_passive_" + std::to_string(i);
        keys.push_back(key);
        DS_ASSERT_OK(client0->Set(key, value, param));
        DS_ASSERT_OK(client1->Set(key, value1, param));
    }
    ASSERT_TRUE(WaitUntilPathExists(worker0SlotRoot)) << worker0SlotRoot;
    client0.reset();
    VoluntaryScaleDownInject(0);


    WaitAllNodesJoinIntoHashRingFast(2, PASSIVE_NODE_DEAD_TIMEOUT_S + 6);
    ASSERT_TRUE(WaitUntilSlotRecoveryIncidentsCleared()) << DumpSlotRecoveryState();
    ASSERT_TRUE(WaitUntilPathRemoved(worker0SlotRoot)) << worker0SlotRoot;
    
    for (const auto &key : keys) {
        std::string getValue;
        DS_ASSERT_OK(client1->Get(key, getValue));
        ASSERT_EQ(getValue, value1);
    }
}

class SlotEndToEndScaleUpTest : public SlotEndToEndTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numEtcd = 1;
        opts.numWorkers = 3;
        opts.enableDistributedMaster = "true";
        opts.waitWorkerReady = false;
        opts.addNodeTime = 0;
        distributedDiskPath_ = testCasePath_ + "/distributed_disk";
        DS_ASSERT_OK(CreateDir(distributedDiskPath_, true));
        std::stringstream ss;
        ss << "-l2_cache_type=distributed_disk "
           << "-distributed_disk_path=" << distributedDiskPath_ << " "
           << "-cluster_name=" << CLUSTER_NAME << " "
           << "-distributed_disk_sync_interval_ms=0 "
           << "-distributed_disk_sync_batch_bytes=1 "
           << "-enable_metadata_recovery=true "
           << "-auto_del_dead_node=true "
           << "-heartbeat_interval_ms=" << HEARTBEAT_INTERVAL_MS << " "
           << "-node_timeout_s=" << NODE_TIMEOUT_S << " "
           << "-node_dead_timeout_s=" << PASSIVE_NODE_DEAD_TIMEOUT_S << " "
           << "-v=1 "
            << "-enable_l2_cache_fallback=false "
            << "-enable_reconciliation=false";
        opts.workerGflagParams = ss.str();
    }

    void SetUp() override
    {
        CommonTest::SetUp();
        FLAGS_cluster_name = CLUSTER_NAME;
        DS_ASSERT_OK(Init());
        ASSERT_TRUE(cluster_ != nullptr);
        externalCluster_ = dynamic_cast<ExternalCluster *>(cluster_.get());
        ASSERT_TRUE(externalCluster_ != nullptr);
        DS_ASSERT_OK(cluster_->StartEtcdCluster());
        KVClientCommon::InitTestEtcdInstance();
        auto createRc = db_->CreateTable(ETCD_SLOT_RECOVERY_TABLE, ETCD_SLOT_RECOVERY_TABLE);
        ASSERT_TRUE(createRc.IsOk() || createRc.GetCode() == K_DUPLICATED) << createRc.ToString();
    }

protected:
    void StartWorkersAndWaitReady(const std::initializer_list<uint32_t> &workers, int waitReadySec = 20)
    {
        for (auto worker : workers) {
            ASSERT_TRUE(externalCluster_->StartWorker(worker, HostPort(), "").IsOk()) << worker;
        }
        for (auto worker : workers) {
            DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, worker, waitReadySec));
        }
    }

    void FillAllSlotsFromWorker(uint32_t clientWorkerIdx, const std::string &prefix,
                                std::vector<std::pair<std::string, std::string>> &keyValues)
    {
        std::shared_ptr<KVClient> writer;
        InitTestKVClient(clientWorkerIdx, writer);
        SetParam param{ .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE };
        keyValues.clear();
        for (uint32_t slotId = 0; slotId < SLOT_NUM; ++slotId) {
            const auto key = FindKeyForSlot(slotId, prefix);
            ASSERT_FALSE(key.empty()) << "slotId=" << slotId;
            const auto value = "value_slot_" + std::to_string(slotId) + "_" + GenRandomString(128);
            DS_ASSERT_OK(writer->Set(key, value, param));
            keyValues.emplace_back(std::make_pair(key, value));
        }
    }

    ExternalCluster *externalCluster_ = nullptr;
};

TEST_F(SlotEndToEndScaleUpTest, PlannerAssignsToRestartNode)
{
    // 1. Start w0,w1,w2
    StartWorkersAndWaitReady({ 0, 1, 2 });
    WaitAllNodesJoinIntoHashRing(3, 20);

    // 2. w1 set keys that cover all slots.
    std::vector<std::pair<std::string, std::string>> keyValues;
    FillAllSlotsFromWorker(1, "value_worker1_slot", keyValues);

    // 3. Concurrent restart w0 and passive scale-down w1.
    std::thread passiveDownW1([&]() { DS_ASSERT_OK(cluster_->KillWorker(1)); });
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    DS_ASSERT_OK(cluster_->KillWorker(0));
    std::this_thread::sleep_for(std::chrono::milliseconds(PASSIVE_NODE_DEAD_TIMEOUT_S * S2MS));
    DS_ASSERT_OK(cluster_->StartNode(WORKER, 0, ""));
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 0, 20));
    passiveDownW1.join();

    WaitAllNodesJoinIntoHashRing(2, PASSIVE_NODE_DEAD_TIMEOUT_S + 10);
    // 4. Planner sees restarted w0 and assigns tasks to w0, but w0 does not enter DemoteTimedOutNodes window in time.
    ASSERT_TRUE(WaitUntilSlotRecoveryIncidentsCleared()) << DumpSlotRecoveryState();
    std::shared_ptr<KVClient> client0;
    InitTestKVClient(0, client0);
    for (const auto &keyValue : keyValues) {
        ASSERT_TRUE(WaitUntilGetSucceeds(client0, keyValue.first, keyValue.second));
    }
}

TEST_F(SlotEndToEndScaleUpTest, PlannerAssignsToScaleUpNode)
{
    // 1. Start w0,w1
    StartWorkersAndWaitReady({ 0, 1 });
    WaitAllNodesJoinIntoHashRing(2, 20);

    // 2. w1 set keys that cover all slots.
    std::vector<std::pair<std::string, std::string>> keyValues;
    FillAllSlotsFromWorker(1, "value_worker1_slot", keyValues);

    // 3. Concurrent passive scale-down w1 and scale-up w2.
    std::thread passiveDownW1([&]() { DS_ASSERT_OK(cluster_->KillWorker(1)); });
    std::this_thread::sleep_for(std::chrono::milliseconds(PASSIVE_NODE_DEAD_TIMEOUT_S * 1000));
    StartWorkersAndWaitReady({ 2 });
    passiveDownW1.join();

    WaitAllNodesJoinIntoHashRing(2, PASSIVE_NODE_DEAD_TIMEOUT_S + 10);
    // 4. Planner sees scale-up w2 and assigns tasks to w2, but w2 does not enter DemoteTimedOutNodes window in time.
    ASSERT_TRUE(WaitUntilSlotRecoveryIncidentsCleared()) << DumpSlotRecoveryState();
    std::shared_ptr<KVClient> client0;
    InitTestKVClient(0, client0);
    for (const auto &keyValue : keyValues) {
        ASSERT_TRUE(WaitUntilGetSucceeds(client0, keyValue.first, keyValue.second));
    }
}
}  // namespace st
}  // namespace datasystem
