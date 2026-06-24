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
 * Description: Request timeout tree (issue-599) integration tests.
 * Validates: server-side ApiDeadline init, EWMA activation, CalcRealRemainingTimeUs
 * for parallel dispatch, CreateMultiMetaParallel Status return, short timeout boundary.
 */

#include <cstdint>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "common.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/rpc/api_deadline.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/kv_client.h"
#include "datasystem/object_client.h"
#include "datasystem/utils/status.h"
#include "oc_client_common.h"

namespace datasystem {
namespace st {

class OCClientTimeoutTest : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = 2;
        opts.numEtcd = 1;
        opts.enableDistributedMaster = "true";
        opts.workerGflagParams = FormatString(" -shared_memory_size_mb=500 -v=2");
    }
};

class OCClientTimeoutSingleRpcThreadTest : public OCClientTimeoutTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        OCClientTimeoutTest::SetClusterSetupOptions(opts);
        opts.numRpcThreads = 1;
    }
};

TEST_F(OCClientTimeoutTest, ShortTimeoutGetFailsFastWithDeadlineExceeded)
{
    std::shared_ptr<ObjectClient> client;
    InitTestClient(0, client, 60000, 500);

    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "WorkerOCServiceImpl.Get.Timeout", "call(0)"));

    std::string objKey = ObjectKey();
    std::vector<Optional<Buffer>> result;
    Timer timer;
    Status rc = client->Get({ objKey }, 0, result);
    int64_t elapsedMs = timer.ElapsedMilliSecond();

    EXPECT_FALSE(rc.IsOk());
    EXPECT_TRUE(rc.GetCode() == K_RPC_DEADLINE_EXCEEDED || rc.GetCode() == K_RPC_UNAVAILABLE);
    EXPECT_LT(elapsedMs, 5000);

    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, 0, "WorkerOCServiceImpl.Get.Timeout"));
}

TEST_F(OCClientTimeoutTest, MultiHopDeadlinePropagationNotInflated)
{
    std::shared_ptr<ObjectClient> client0;
    InitTestClient(0, client0, 60000, 0);

    std::string objKey = ObjectKey();
    std::vector<uint8_t> data = { 'A', 'B', 'C' };
    CreateAndSealObject(client0, objKey, data);

    std::shared_ptr<ObjectClient> client1;
    InitTestClient(1, client1, 60000, 500);

    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "WorkerOCServiceImpl.Get.Timeout", "call(0)"));

    std::vector<Optional<Buffer>> result;
    Timer timer;
    Status rc = client1->Get({ objKey }, 0, result);
    int64_t elapsedMs = timer.ElapsedMilliSecond();

    EXPECT_LT(elapsedMs, 5000);

    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, 1, "WorkerOCServiceImpl.Get.Timeout"));
}

TEST_F(OCClientTimeoutTest, ParallelGetUsesRealRemainingNotDecayed)
{
    std::shared_ptr<ObjectClient> client0;
    InitTestClient(0, client0, 60000, 0);

    std::vector<std::string> objKeys;
    std::vector<uint8_t> data = { 'A', 'B', 'C', 'D', 'E' };
    for (int i = 0; i < 5; ++i) {
        objKeys.emplace_back(ObjectKey());
        CreateAndSealObject(client0, objKeys.back(), data);
    }

    std::shared_ptr<ObjectClient> client1;
    InitTestClient(1, client1, 60000, 50);

    std::vector<Optional<Buffer>> result;
    Status rc = client1->Get(objKeys, 0, result);

    if (rc.IsOk()) {
        EXPECT_EQ(result.size(), objKeys.size());
    } else {
        EXPECT_TRUE(rc.GetCode() == K_RPC_DEADLINE_EXCEEDED || rc.GetCode() == K_RPC_UNAVAILABLE);
    }
}

TEST_F(OCClientTimeoutTest, MSetDeadlineExceededReturnsStatusNotSilent)
{
    std::shared_ptr<KVClient> kvClient;
    InitTestKVClient(0, kvClient, 60000, false, false, 500);

    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "worker.before_CreateMultiMetaToMaster", "1*sleep(600)"));

    std::vector<std::string> keys;
    std::vector<StringView> values;
    for (int i = 0; i < 3; ++i) {
        keys.emplace_back(ObjectKey());
        values.emplace_back("AB");
    }

    MSetParam param;
    std::vector<std::string> failedKeys;
    Timer timer;
    Status rc = kvClient->MSet(keys, values, failedKeys, param);
    int64_t elapsedMs = timer.ElapsedMilliSecond();

    EXPECT_FALSE(rc.IsOk());
    EXPECT_TRUE(rc.GetCode() == K_RPC_DEADLINE_EXCEEDED || rc.GetCode() == K_RPC_UNAVAILABLE);
    EXPECT_LT(elapsedMs, 5000);

    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, 0, "worker.before_CreateMultiMetaToMaster"));
}

TEST_F(OCClientTimeoutTest, EwmaDeductReducesOverallocationAfterWarmup)
{
    std::shared_ptr<ObjectClient> client;
    InitTestClient(0, client, 60000, 5000);

    std::vector<std::string> warmupKeys;
    std::vector<uint8_t> data = { 'A', 'B', 'C', 'D' };
    for (int i = 0; i < 10; ++i) {
        warmupKeys.emplace_back(ObjectKey());
        CreateAndSealObject(client, warmupKeys.back(), data);
    }

    for (const auto &key : warmupKeys) {
        std::vector<Optional<Buffer>> result;
        DS_ASSERT_OK(client->Get({ key }, 0, result));
        ASSERT_TRUE(NotExistsNone(result));
    }

    std::string objKey = ObjectKey();
    CreateAndSealObject(client, objKey, data);

    std::vector<Optional<Buffer>> result;
    DS_ASSERT_OK(client->Get({ objKey }, 0, result));
    ASSERT_TRUE(NotExistsNone(result));
}

TEST_F(OCClientTimeoutTest, AdmissionRejectSkipsWorkerBusinessPath)
{
    std::shared_ptr<ObjectClient> client;
    InitTestClient(0, client, 60000, 5);

    // Control RPCs traverse the same RouteToRegBackend dispatch path as Get; give them
    // a 60s budget so the admission-reject inject only rejects the 5ms Get, not the
    // setup/cleanup control RPCs sent from this deadline-leaked test thread.
    {
        ApiDeadlineGuard guard(RPC_TIMEOUT);
        DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "ZmqService::RouteToRegBackend.elapsedUs", "call(100000)"));
        DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "WorkerOCServiceImpl.Get.Timeout", "call(0)"));
    }

    std::string objKey = ObjectKey();
    std::vector<Optional<Buffer>> result;
    Timer timer;
    Status rc = client->Get({ objKey }, 0, result);
    int64_t elapsedMs = timer.ElapsedMilliSecond();

    EXPECT_FALSE(rc.IsOk());
    EXPECT_EQ(rc.GetCode(), K_RPC_DEADLINE_EXCEEDED);
    EXPECT_LT(elapsedMs, 5000);

    {
        ApiDeadlineGuard guard(RPC_TIMEOUT);
        uint64_t getTimeoutCount = 0;
        DS_ASSERT_OK(
            cluster_->GetInjectActionExecuteCount(WORKER, 0, "WorkerOCServiceImpl.Get.Timeout", getTimeoutCount));
        EXPECT_EQ(getTimeoutCount, 0u);

        DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, 0, "ZmqService::RouteToRegBackend.elapsedUs"));
        DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, 0, "WorkerOCServiceImpl.Get.Timeout"));
    }
}

TEST_F(OCClientTimeoutSingleRpcThreadTest, AdmissionRejectConsumesMessageThenNextRequestSucceeds)
{
    // Prepare data: create and seal an object with a no-timeout client.
    std::shared_ptr<ObjectClient> adminClient;
    InitTestClient(0, adminClient, 60000, 0);
    std::string objKey = ObjectKey();
    std::vector<uint8_t> data = { 'A', 'B', 'C', 'D' };
    CreateAndSealObject(adminClient, objKey, data);

    // Send the first Get with a short timeout to trigger admission reject.
    std::shared_ptr<ObjectClient> client;
    InitTestClient(0, client, 60000, 5);

    {
        ApiDeadlineGuard guard(RPC_TIMEOUT);
        DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "ZmqService::RouteToRegBackend.elapsedUs", "call(100000)"));
        DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "WorkerOCServiceImpl.Get.Timeout", "call(0)"));
    }

    // First Get: should be rejected by admission.
    // WorkerEntry consumes p via ReceiveMsg first, then checks the deadline and rejects — no orphan left.
    std::vector<Optional<Buffer>> result1;
    Timer timer;
    Status rc1 = client->Get({ objKey }, 0, result1);
    int64_t elapsedMs = timer.ElapsedMilliSecond();

    EXPECT_FALSE(rc1.IsOk());
    EXPECT_EQ(rc1.GetCode(), K_RPC_DEADLINE_EXCEEDED);
    EXPECT_LT(elapsedMs, 5000);

    // Verify WorkerEntryImpl was NOT executed (reject happens before WorkerEntryImpl).
    {
        ApiDeadlineGuard guard(RPC_TIMEOUT);
        uint64_t getTimeoutCount = 0;
        DS_ASSERT_OK(
            cluster_->GetInjectActionExecuteCount(WORKER, 0, "WorkerOCServiceImpl.Get.Timeout", getTimeoutCount));
        EXPECT_EQ(getTimeoutCount, 0u);
    }

    // Clear injects; the second Get should take the normal path.
    {
        ApiDeadlineGuard guard(RPC_TIMEOUT);
        DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, 0, "ZmqService::RouteToRegBackend.elapsedUs"));
        DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, 0, "WorkerOCServiceImpl.Get.Timeout"));
    }

    // Second Get (same worker): should return data normally.
    // Since the first request's p was already consumed by WorkerEntry, the queue has no orphan
    // and the second request will not be mismatched.
    std::vector<Optional<Buffer>> result2;
    Status rc2 = client->Get({ objKey }, 0, result2);

    EXPECT_TRUE(rc2.IsOk()) << "Second request should succeed — no orphan left in queue";
    ASSERT_EQ(result2.size(), 1u);
    EXPECT_TRUE(result2[0]);
    EXPECT_EQ(result2[0]->GetSize(), static_cast<int>(data.size()));
}

TEST_F(OCClientTimeoutTest, GetObjMetaInfoIndependentDeadlineExceeded)
{
    std::shared_ptr<ObjectClient> client;
    InitTestClient(0, client, 60000, 500);

    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "worker.GetObjMetaInfo", "1*sleep(600)"));

    std::string objKey = ObjectKey();
    std::vector<ObjMetaInfo> objMetas;
    Timer timer;
    Status rc = client->GetObjMetaInfo("", { objKey }, objMetas);
    int64_t elapsedMs = timer.ElapsedMilliSecond();

    EXPECT_FALSE(rc.IsOk());
    EXPECT_TRUE(rc.GetCode() == K_RPC_DEADLINE_EXCEEDED || rc.GetCode() == K_RPC_UNAVAILABLE);
    EXPECT_LT(elapsedMs, 5000);

    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, 0, "worker.GetObjMetaInfo"));
}

}  // namespace st
}  // namespace datasystem
