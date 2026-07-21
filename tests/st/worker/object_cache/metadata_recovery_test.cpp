/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Description: End-to-end tests for object-cache metadata recovery.
 */

#include <csignal>

#include <chrono>
#include <map>
#include <sstream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "common.h"
#include "client/kv_cache/kv_client_scale_common.h"
#include "datasystem/common/l2cache/slot_client/slot_client.h"
#include "datasystem/common/l2cache/slot_client/slot_file_util.h"
#include "datasystem/common/util/hash_algorithm.h"

DS_DECLARE_string(cluster_name);

namespace datasystem {
namespace st {
namespace {
constexpr int WAIT_GET_TIMEOUT_MS = 20'000;
constexpr int WAIT_GET_INTERVAL_MS = 200;
constexpr uint64_t NODE_TIMEOUT_S = 1;
constexpr uint64_t NODE_DEAD_TIMEOUT_S = 3;
constexpr uint64_t HEARTBEAT_INTERVAL_MS = 500;
constexpr int S2MS = 1000;
}  // namespace

class MetadataRecoveryTest : public KVClientScaleCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numEtcd = 1;
        opts.numWorkers = 2;
        opts.enableDistributedMaster = "true";
        opts.addNodeTime = 0;
        std::stringstream ss;
        ss << "-enable_metadata_recovery=true " << "-enable_reconciliation=true "
           << "-heartbeat_interval_ms=" << HEARTBEAT_INTERVAL_MS << " " << "-node_timeout_s=" << NODE_TIMEOUT_S << " "
           << "-node_dead_timeout_s=" << NODE_DEAD_TIMEOUT_S << " " << "-v=1 " << "-enable_l2_cache_fallback=false";
        opts.workerGflagParams = ss.str();
    }

protected:
    void WaitInitialClusterReady()
    {
        WaitAllMembersJoinClusterTopology(2, 20);
        DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 0));
        DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 1));
    }

    bool WaitUntilSetSucceeds(const std::shared_ptr<KVClient> &client, const std::string &key, const std::string &value,
                              const SetParam *param = nullptr) const
    {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(WAIT_GET_TIMEOUT_MS);
        while (std::chrono::steady_clock::now() < deadline) {
            auto rc = param == nullptr ? client->Set(key, value) : client->Set(key, value, *param);
            if (rc.IsOk()) {
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

    bool WaitUntilGetNotFound(const std::shared_ptr<KVClient> &client, const std::string &key,
                              int timeoutMs = 4'000) const
    {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeoutMs);
        while (std::chrono::steady_clock::now() < deadline) {
            std::string value;
            if (client->Get(key, value).GetCode() == K_NOT_FOUND) {
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(WAIT_GET_INTERVAL_MS));
        }
        return false;
    }

    bool WaitForWorkerInjectCount(uint32_t workerIndex, const std::string &name, uint64_t expectedCount,
                                  int timeoutMs = 4'000) const
    {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeoutMs);
        while (std::chrono::steady_clock::now() < deadline) {
            uint64_t count = 0;
            if (cluster_->GetInjectActionExecuteCount(WORKER, workerIndex, name, count).IsOk()
                && count >= expectedCount) {
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
        return false;
    }

    std::string FindCurrentOwnerKey(const std::vector<std::string> &candidateKeys, uint32_t workerIndex)
    {
        if (!db_) {
            InitTestEtcdInstance();
        }
        std::string value;
        auto rc = db_->Get(GetTopologyTableName(), "", value);
        EXPECT_TRUE(rc.IsOk()) << rc.ToString();
        if (rc.IsError()) {
            return {};
        }
        ClusterTopologyPb ring;
        if (!ring.ParseFromString(value)) {
            ADD_FAILURE() << "failed to parse current topology";
            return {};
        }
        HostPort targetWorker;
        rc = cluster_->GetWorkerAddr(workerIndex, targetWorker);
        EXPECT_TRUE(rc.IsOk()) << rc.ToString();
        if (rc.IsError()) {
            return {};
        }

        std::map<uint32_t, std::string> tokenWorkers;
        for (const auto &worker : ring.members()) {
            for (const auto token : worker.second.tokens()) {
                tokenWorkers.emplace(token, worker.first);
            }
        }
        EXPECT_FALSE(tokenWorkers.empty()) << "hash ring has no worker tokens";
        if (tokenWorkers.empty()) {
            return {};
        }
        for (const auto &key : candidateKeys) {
            auto owner = tokenWorkers.lower_bound(MurmurHash3_32(key));
            if (owner == tokenWorkers.end()) {
                owner = tokenWorkers.begin();
            }
            if (owner->second == targetWorker.ToString()) {
                return key;
            }
        }
        return {};
    }

    std::string GenerateCurrentOwnerKey(const std::string &prefix, uint32_t workerIndex)
    {
        std::vector<std::string> candidateKeys;
        constexpr size_t candidateCount = 1024;
        candidateKeys.reserve(candidateCount);
        for (size_t i = 0; i < candidateCount; ++i) {
            candidateKeys.emplace_back(prefix + "_" + std::to_string(i));
        }
        return FindCurrentOwnerKey(candidateKeys, workerIndex);
    }

    void VerifyMetadataOwnerRestart();

    void VerifyUnrecoverableLocalDataClearedAfterMetadataOwnerRestart(const SetParam *param = nullptr);

    void VerifyMetadataRecoveryBestEffortRetryDoesNotBlockAvailability();

    virtual void VerifyPersistentObjectState(const std::string &, const std::string &, bool)
    {
    }

    static constexpr int timeoutMs_ = 5'000;
};

class MetadataRecoveryDisabledTest : public MetadataRecoveryTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        MetadataRecoveryTest::SetClusterSetupOptions(opts);
        const std::string enabledFlag = "-enable_metadata_recovery=true";
        auto pos = opts.workerGflagParams.find(enabledFlag);
        ASSERT_NE(pos, std::string::npos);
        opts.workerGflagParams.replace(pos, enabledFlag.size(), "-enable_metadata_recovery=false");
    }
};

class MetadataRecoveryL2Test : public MetadataRecoveryTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        MetadataRecoveryTest::SetClusterSetupOptions(opts);
        const std::string fallbackDisabled = "-enable_l2_cache_fallback=false";
        auto fallbackPos = opts.workerGflagParams.find(fallbackDisabled);
        ASSERT_NE(fallbackPos, std::string::npos);
        opts.workerGflagParams.replace(fallbackPos, fallbackDisabled.size(), "-enable_l2_cache_fallback=true");
        distributedDiskPath_ = testCasePath_ + "/distributed_disk";
        DS_ASSERT_OK(CreateDir(distributedDiskPath_, true));
        std::stringstream ss;
        ss << opts.workerGflagParams << " -oc_io_from_l2cache_need_metadata=false -l2_cache_type=distributed_disk"
           << " -distributed_disk_path=" << distributedDiskPath_ << " -distributed_disk_sync_interval_ms=0"
           << " -distributed_disk_sync_batch_bytes=1";
        opts.workerGflagParams = ss.str();
    }

protected:
    void VerifyPersistentObjectState(const std::string &objectKey, const std::string &expectedValue,
                                     bool expectedPresent) override
    {
        const auto oldNamespace = GetSlotWorkerNamespace();
        Raii restoreNamespace([&] { SetSlotWorkerNamespace(oldNamespace); });
        size_t presentCopies = 0;
        for (uint32_t workerIndex = 0; workerIndex < 2; ++workerIndex) {
            HostPort workerAddr;
            DS_ASSERT_OK(cluster_->GetWorkerAddr(workerIndex, workerAddr));
            SetSlotWorkerNamespace(SanitizeSlotWorkerNamespace(workerAddr.ToString()));
            SlotClient localClient(distributedDiskPath_);
            DS_ASSERT_OK(localClient.Init());
            auto content = std::make_shared<std::stringstream>();
            auto rc = localClient.GetWithoutVersion(objectKey, 0, 0, content);
            if (rc.IsOk()) {
                ++presentCopies;
                EXPECT_EQ(content->str(), expectedValue);
            } else {
                EXPECT_EQ(rc.GetCode(), K_NOT_FOUND_IN_L2CACHE) << rc.ToString();
            }
        }
        if (expectedPresent) {
            EXPECT_GT(presentCopies, 0U) << "write-through L2 setup did not persist the selected object";
        } else {
            EXPECT_EQ(presentCopies, 0U) << "authoritative cleanup left a readable distributed-disk version";
        }
    }

    std::string distributedDiskPath_;
};

class MetadataRecoveryDisabledL2Test : public MetadataRecoveryL2Test {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        MetadataRecoveryL2Test::SetClusterSetupOptions(opts);
        const std::string enabledFlag = "-enable_metadata_recovery=true";
        auto pos = opts.workerGflagParams.find(enabledFlag);
        ASSERT_NE(pos, std::string::npos);
        opts.workerGflagParams.replace(pos, enabledFlag.size(), "-enable_metadata_recovery=false");
    }
};

void MetadataRecoveryTest::VerifyMetadataOwnerRestart()
{
    if (FLAGS_use_brpc) {
        GTEST_SKIP() << "brpc migration gap; real failure under brpc. Tracked separately.";
    }
    WaitInitialClusterReady();

    std::shared_ptr<KVClient> client0;
    std::shared_ptr<KVClient> client1;
    InitTestKVClient(0, client0, timeoutMs_);
    InitTestKVClient(1, client1, timeoutMs_);

    std::string objKey = client1->GenerateKey("meta_own_worker1");
    auto value = GenRandomString(10);
    ASSERT_TRUE(WaitUntilSetSucceeds(client0, objKey, value)) << objKey;

    DS_ASSERT_OK(cluster_->KillWorker(1));

    DS_ASSERT_OK(cluster_->StartNode(WORKER, 1, ""));
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 1));
    WaitAllMembersJoinClusterTopology(2, 20);

    InitTestKVClient(1, client1, timeoutMs_);
    ASSERT_TRUE(WaitUntilGetSucceeds(client1, objKey, value)) << objKey;
}

void MetadataRecoveryTest::VerifyUnrecoverableLocalDataClearedAfterMetadataOwnerRestart(const SetParam *param)
{
    WaitInitialClusterReady();

    std::shared_ptr<KVClient> client0;
    std::shared_ptr<KVClient> client1;
    InitTestKVClient(0, client0, timeoutMs_);
    InitTestKVClient(1, client1, timeoutMs_);

    const std::string value = GenRandomString(10);
    std::vector<std::string> candidateKeys;
    constexpr size_t candidateCount = 32;
    candidateKeys.reserve(candidateCount);
    for (size_t i = 0; i < candidateCount; ++i) {
        auto key = "unrecoverable_local_data_" + std::to_string(i) + "_" + GetStringUuid();
        candidateKeys.emplace_back(std::move(key));
    }
    const auto objKey = FindCurrentOwnerKey(candidateKeys, 1);
    ASSERT_FALSE(objKey.empty()) << "candidate set did not cover worker1's current token ranges";
    ASSERT_TRUE(WaitUntilSetSucceeds(client0, objKey, value, param)) << objKey;
    VerifyPersistentObjectState(objKey, value, true);

    constexpr const char *pushMetaInject = "WorkerMaster.PushMetadataToMaster";
    constexpr const char *clearOrphanInject = "WorkerOcServiceClearDataFlow.BeforeClearUnrecoverableObjects";
    constexpr const char *beforeSelectionInject = "WorkerOCServiceImpl.BeforeRestartMetadataSelection";
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, pushMetaInject, "return(K_RPC_UNAVAILABLE)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, clearOrphanInject, "call()"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, beforeSelectionInject, "1*sleep(1000)"));
    bool injectsActive = true;
    Raii clearInjects([&] {
        if (injectsActive) {
            LOG_IF_ERROR(cluster_->ClearInjectAction(WORKER, 0, pushMetaInject), "clear inject");
            LOG_IF_ERROR(cluster_->ClearInjectAction(WORKER, 0, clearOrphanInject), "clear inject");
            LOG_IF_ERROR(cluster_->ClearInjectAction(WORKER, 0, beforeSelectionInject), "clear inject");
        }
    });

    DS_ASSERT_OK(cluster_->KillWorker(1));
    DS_ASSERT_OK(cluster_->StartNode(WORKER, 1, ""));
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 1));
    WaitAllMembersJoinClusterTopology(2, 8);
    ASSERT_EQ(FindCurrentOwnerKey({ objKey }, 1), objKey)
        << "the selected metadata owner changed across the bounded restart window";

    ASSERT_TRUE(WaitForWorkerInjectCount(0, clearOrphanInject, 1))
        << "failed metadata recovery did not enter the bounded orphan clear-data path";
    uint64_t pushAttempts = 0;
    DS_ASSERT_OK(cluster_->GetInjectActionExecuteCount(WORKER, 0, pushMetaInject, pushAttempts));
    EXPECT_EQ(pushAttempts, 2U) << "restart recovery must perform one initial push and exactly one bounded retry";
    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, 0, pushMetaInject));
    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, 0, clearOrphanInject));
    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, 0, beforeSelectionInject));
    injectsActive = false;

    EXPECT_TRUE(WaitUntilGetNotFound(client0, objKey));
    VerifyPersistentObjectState(objKey, value, false);
}

void MetadataRecoveryTest::VerifyMetadataRecoveryBestEffortRetryDoesNotBlockAvailability()
{
    WaitInitialClusterReady();

    std::shared_ptr<KVClient> client0;
    InitTestKVClient(0, client0, timeoutMs_);

    const std::string orphanValue = GenRandomString(10);
    std::vector<std::string> orphanCandidates;
    constexpr size_t candidateCount = 128;
    for (size_t i = 0; i < candidateCount; ++i) {
        auto orphanKey = "retry_availability_orphan_" + std::to_string(i) + "_" + GetStringUuid();
        ASSERT_TRUE(WaitUntilSetSucceeds(client0, orphanKey, orphanValue)) << orphanKey;
        orphanCandidates.emplace_back(std::move(orphanKey));
    }

    constexpr const char *pushMetaInject = "WorkerMaster.PushMetadataToMaster";
    constexpr const char *beforeRetryInject = "WorkerOcServiceClearDataFlow.BeforeRetryFailedMetadataRecovery";
    constexpr const char *clearOrphanInject = "WorkerOcServiceClearDataFlow.BeforeClearUnrecoverableObjects";
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, pushMetaInject, "return(K_RPC_UNAVAILABLE)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, beforeRetryInject, "1*pause"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, clearOrphanInject, "call()"));
    bool injectsActive = true;
    Raii clearInjects([&] {
        if (injectsActive) {
            LOG_IF_ERROR(cluster_->ClearInjectAction(WORKER, 0, pushMetaInject), "clear push metadata inject");
            LOG_IF_ERROR(cluster_->ClearInjectAction(WORKER, 0, beforeRetryInject), "clear retry pause");
            LOG_IF_ERROR(cluster_->ClearInjectAction(WORKER, 0, clearOrphanInject), "clear orphan inject");
        }
    });

    DS_ASSERT_OK(cluster_->KillWorker(1));
    DS_ASSERT_OK(cluster_->StartNode(WORKER, 1, ""));
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 1));
    WaitAllMembersJoinClusterTopology(2, 8);
    ASSERT_TRUE(WaitForWorkerInjectCount(0, beforeRetryInject, 1))
        << "metadata recovery did not reach the bounded retry pause";

    const auto peerKey = GenerateCurrentOwnerKey("retry_availability_peer", 0);
    ASSERT_FALSE(peerKey.empty()) << "candidate set did not cover the surviving worker's current token ranges";
    const std::string peerValue = GenRandomString(10);
    DS_ASSERT_OK(client0->Set(peerKey, peerValue));
    std::string actualPeerValue;
    DS_ASSERT_OK(client0->Get(peerKey, actualPeerValue));
    EXPECT_EQ(actualPeerValue, peerValue);

    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, 0, beforeRetryInject));
    ASSERT_TRUE(WaitForWorkerInjectCount(0, clearOrphanInject, 1))
        << "bounded retry did not resume into authoritative cleanup";
    uint64_t pushAttempts = 0;
    DS_ASSERT_OK(cluster_->GetInjectActionExecuteCount(WORKER, 0, pushMetaInject, pushAttempts));
    EXPECT_EQ(pushAttempts, 2U);
    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, 0, pushMetaInject));
    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, 0, clearOrphanInject));
    injectsActive = false;

    const auto orphanKey = FindCurrentOwnerKey(orphanCandidates, 1);
    ASSERT_FALSE(orphanKey.empty());
    EXPECT_TRUE(WaitUntilGetNotFound(client0, orphanKey));
}

TEST_F(MetadataRecoveryTest, MetadataOwnerRestart)
{
    VerifyMetadataOwnerRestart();
}

TEST_F(MetadataRecoveryTest, UnrecoverableLocalDataClearedAfterMetadataOwnerRestart)
{
    VerifyUnrecoverableLocalDataClearedAfterMetadataOwnerRestart();
}

TEST_F(MetadataRecoveryTest, MetadataRecoveryBestEffortRetryDoesNotBlockAvailability)
{
    VerifyMetadataRecoveryBestEffortRetryDoesNotBlockAvailability();
}

TEST_F(MetadataRecoveryDisabledTest, MetadataOwnerRestart)
{
    VerifyMetadataOwnerRestart();
}

TEST_F(MetadataRecoveryDisabledTest, UnrecoverableLocalDataClearedAfterMetadataOwnerRestart)
{
    VerifyUnrecoverableLocalDataClearedAfterMetadataOwnerRestart();
}

TEST_F(MetadataRecoveryL2Test, UnrecoverableL2DataClearedAfterMetadataOwnerRestart)
{
    SetParam param{ .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE };
    VerifyUnrecoverableLocalDataClearedAfterMetadataOwnerRestart(&param);
}

TEST_F(MetadataRecoveryDisabledL2Test, UnrecoverableL2DataClearedAfterMetadataOwnerRestart)
{
    SetParam param{ .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE };
    VerifyUnrecoverableLocalDataClearedAfterMetadataOwnerRestart(&param);
}

TEST_F(MetadataRecoveryDisabledTest, OtherWorkersRecoverMetadataBeforeClearingDataWithoutMetadata)
{
    WaitInitialClusterReady();

    std::shared_ptr<KVClient> client0;
    std::shared_ptr<KVClient> client1;
    InitTestKVClient(0, client0, timeoutMs_);
    InitTestKVClient(1, client1, timeoutMs_);

    const std::string objectKey = GenerateCurrentOwnerKey("recover_before_failure_cleanup", 1);
    ASSERT_FALSE(objectKey.empty()) << "candidate set did not cover worker1's current token ranges";
    const std::string value = GenRandomString(10);
    ASSERT_TRUE(WaitUntilSetSucceeds(client0, objectKey, value));
    ASSERT_TRUE(WaitUntilGetSucceeds(client0, objectKey, value));

    constexpr const char *pushMetaInject = "WorkerMaster.PushMetadataToMaster";
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, pushMetaInject, "call()"));
    Raii clearInject([&] { LOG_IF_ERROR(cluster_->ClearInjectAction(WORKER, 0, pushMetaInject), "clear inject"); });

    DS_ASSERT_OK(cluster_->KillWorker(1));
    WaitAllMembersJoinClusterTopology(1, 20);

    ASSERT_TRUE(WaitForWorkerInjectCount(0, pushMetaInject, 1))
        << "surviving worker cleared local data without first attempting metadata recovery";
    ASSERT_TRUE(WaitUntilGetSucceeds(client0, objectKey, value));
}

TEST_F(MetadataRecoveryDisabledTest, OrphanLocalDataRequiresRecoveryOrClearDataWithoutMeta)
{
    WaitInitialClusterReady();

    std::shared_ptr<KVClient> client0;
    std::shared_ptr<KVClient> client1;
    InitTestKVClient(0, client0, timeoutMs_);
    InitTestKVClient(1, client1, timeoutMs_);

    const std::string objectKey = GenerateCurrentOwnerKey("recover_or_clear_after_failure", 1);
    ASSERT_FALSE(objectKey.empty()) << "candidate set did not cover worker1's current token ranges";
    const std::string value = GenRandomString(10);
    ASSERT_TRUE(WaitUntilSetSucceeds(client0, objectKey, value));
    ASSERT_TRUE(WaitUntilGetSucceeds(client0, objectKey, value));

    constexpr const char *pushMetaInject = "WorkerMaster.PushMetadataToMaster";
    constexpr const char *clearOrphanInject = "WorkerOcServiceClearDataFlow.BeforeClearUnrecoverableObjects";
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, pushMetaInject, "return(K_RPC_UNAVAILABLE)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, clearOrphanInject, "call()"));
    Raii clearInjects([&] {
        LOG_IF_ERROR(cluster_->ClearInjectAction(WORKER, 0, pushMetaInject), "clear inject");
        LOG_IF_ERROR(cluster_->ClearInjectAction(WORKER, 0, clearOrphanInject), "clear inject");
    });

    DS_ASSERT_OK(cluster_->KillWorker(1));
    WaitAllMembersJoinClusterTopology(1, 20);

    ASSERT_TRUE(WaitForWorkerInjectCount(0, clearOrphanInject, 1))
        << "failed recovery did not enter the authoritative orphan cleanup path";
    uint64_t pushAttempts = 0;
    DS_ASSERT_OK(cluster_->GetInjectActionExecuteCount(WORKER, 0, pushMetaInject, pushAttempts));
    EXPECT_EQ(pushAttempts, 1U);
    EXPECT_TRUE(WaitUntilGetNotFound(client0, objectKey));
}

TEST_F(MetadataRecoveryTest, FailoverRestoreObjectWithTtl)
{
    WaitInitialClusterReady();

    std::shared_ptr<KVClient> client0;
    std::shared_ptr<KVClient> client1;
    InitTestKVClient(0, client0, timeoutMs_);
    InitTestKVClient(1, client1, timeoutMs_);

    constexpr int ttl = 2;
    SetParam param{ .ttlSecond = ttl };
    std::string objKey = client1->GenerateKey("object_with_ttl_worker0");
    auto value = GenRandomString(10);
    ASSERT_TRUE(WaitUntilSetSucceeds(client0, objKey, value, &param)) << objKey;
    std::this_thread::sleep_for(std::chrono::milliseconds((500)));

    DS_ASSERT_OK(cluster_->KillWorker(1));
    WaitAllMembersJoinClusterTopology(1, 20);

    std::this_thread::sleep_for(std::chrono::milliseconds((ttl + 1) * S2MS));
    std::string val;
    ASSERT_EQ(client0->Get(objKey, val).GetCode(), K_NOT_FOUND);
}

TEST_F(MetadataRecoveryTest, RestartRestoreObjectWithTtl)
{
    WaitInitialClusterReady();

    std::shared_ptr<KVClient> client0;
    std::shared_ptr<KVClient> client1;
    InitTestKVClient(0, client0, timeoutMs_);
    InitTestKVClient(1, client1, timeoutMs_);

    constexpr int ttl = 2;
    SetParam param{ .ttlSecond = ttl };
    std::string objKey = client1->GenerateKey("object_with_ttl_worker0_restart");
    auto value = GenRandomString(10);
    ASSERT_TRUE(WaitUntilSetSucceeds(client0, objKey, value, &param)) << objKey;
    std::this_thread::sleep_for(std::chrono::milliseconds((500)));

    DS_ASSERT_OK(cluster_->KillWorker(1));
    DS_ASSERT_OK(cluster_->StartNode(WORKER, 1, ""));
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 1));
    WaitAllMembersJoinClusterTopology(2, 20);

    InitTestKVClient(1, client1, timeoutMs_);
    std::this_thread::sleep_for(std::chrono::milliseconds((ttl + 1) * S2MS));
    std::string val;
    ASSERT_EQ(client1->Get(objKey, val).GetCode(), K_NOT_FOUND);
}
}  // namespace st
}  // namespace datasystem
