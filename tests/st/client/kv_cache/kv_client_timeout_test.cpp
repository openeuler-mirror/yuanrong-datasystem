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
 * Description: Request timeout tree (issue-599) KV-level integration tests.
 * Validates: KVClient Set, Get, MSet, Exist with short requestTimeoutMs fail fast
 * and return explicit deadline/error codes rather than falling back to 60s.
 */

#include <cstdint>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "common.h"
#include "datasystem/common/flags/common_flags.h"  // FLAGS_use_brpc
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/kv_client.h"
#include "datasystem/utils/status.h"
#include "kv_client_common.h"

namespace datasystem {
namespace st {

class KVClientTimeoutTest : public KVClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = 2;
        opts.numEtcd = 1;
        opts.enableDistributedMaster = "true";
        opts.workerGflagParams = FormatString(" -shared_memory_size_mb=500 -v=2");
    }
};

TEST_F(KVClientTimeoutTest, KVGetShortTimeoutFailsFastWithDeadlineExceeded)
{
    std::shared_ptr<KVClient> client;
    InitTestKVClient(0, client, 60000, false, 500);

    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "WorkerOCServiceImpl.Get.Timeout", "call(0)"));

    std::string key = ObjectKey();
    std::string val;
    Timer timer;
    Status rc = client->Get(key, val, 0);
    int64_t elapsedMs = timer.ElapsedMilliSecond();

    EXPECT_FALSE(rc.IsOk());
    EXPECT_EQ(rc.GetCode(), K_RPC_DEADLINE_EXCEEDED);
    EXPECT_LT(elapsedMs, 5000);

    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, 0, "WorkerOCServiceImpl.Get.Timeout"));
}

TEST_F(KVClientTimeoutTest, KVMSetShortTimeoutDeadlineExceeded)
{
    std::shared_ptr<KVClient> client;
    InitTestKVClient(0, client, 60000, false, 500);

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
    Status rc = client->MSet(keys, values, failedKeys, param);
    int64_t elapsedMs = timer.ElapsedMilliSecond();

    EXPECT_FALSE(rc.IsOk());
    EXPECT_EQ(rc.GetCode(), K_RPC_DEADLINE_EXCEEDED);
    EXPECT_LT(elapsedMs, 5000);

    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, 0, "worker.before_CreateMultiMetaToMaster"));
}

TEST_F(KVClientTimeoutTest, KVExistShortTimeoutDeadlineExceeded)
{
    std::shared_ptr<KVClient> client;
    InitTestKVClient(0, client, 60000, false, 500);

    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "Exist.Sleep", "1*sleep(600)"));

    std::string key = ObjectKey();
    std::vector<bool> exists;
    Timer timer;
    Status rc = client->Exist({ key }, exists);
    int64_t elapsedMs = timer.ElapsedMilliSecond();

    EXPECT_FALSE(rc.IsOk());
    EXPECT_EQ(rc.GetCode(), K_RPC_DEADLINE_EXCEEDED);
    EXPECT_LT(elapsedMs, 5000);

    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, 0, "Exist.Sleep"));
}

TEST_F(KVClientTimeoutTest, Set3msCreateSlowExhaustsSharedBudget)
{
    // A 5ms Create delay deterministically exhausts the 3ms shared budget before the master RPC.
    // Verifies budget is shared across the Set chain, not reinflated on retry.
    std::shared_ptr<KVClient> client;
    InitTestKVClient(0, client, 60000, false, 3);

    constexpr int createDelayMs = 5;
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "worker.before_CreateMultiMetaToMaster",
                                           FormatString("1*sleep(%d)", createDelayMs)));

    std::vector<std::string> keys;
    std::vector<StringView> values;
    for (int i = 0; i < 3; ++i) {
        keys.emplace_back(ObjectKey());
        values.emplace_back("AB");
    }

    MSetParam param;
    std::vector<std::string> failedKeys;
    Timer timer;
    Status rc = client->MSet(keys, values, failedKeys, param);
    int64_t elapsedMs = timer.ElapsedMilliSecond();

    EXPECT_FALSE(rc.IsOk());
    EXPECT_EQ(rc.GetCode(), K_RPC_DEADLINE_EXCEEDED);
    EXPECT_LT(elapsedMs, 50);

    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, 0, "worker.before_CreateMultiMetaToMaster"));
}

TEST_F(KVClientTimeoutTest, ShortTimeoutThresholdDecayBoundary)
{
    // 1ms budget: even a 1ms sleep in the Create phase exhausts the entire budget.
    // If the client->worker hop already exhausts the budget, the admission check
    // rejects before the injection fires. Either path yields K_RPC_DEADLINE_EXCEEDED.
    {
        std::shared_ptr<KVClient> client;
        InitTestKVClient(0, client, 60000, false, 1);

        DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0,
            "worker.before_CreateMultiMetaToMaster", "1*sleep(1)"));

        std::vector<std::string> keys;
        std::vector<StringView> values;
        for (int i = 0; i < 3; ++i) {
            keys.emplace_back(ObjectKey());
            values.emplace_back("AB");
        }
        MSetParam param;
        std::vector<std::string> failedKeys;
        Timer timer;
        Status rc = client->MSet(keys, values, failedKeys, param);
        int64_t elapsedMs = timer.ElapsedMilliSecond();

        EXPECT_FALSE(rc.IsOk());
        EXPECT_EQ(rc.GetCode(), K_RPC_DEADLINE_EXCEEDED);
        EXPECT_LT(elapsedMs, 50);

        DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, 0, "worker.before_CreateMultiMetaToMaster"));
    }

    // Exact threshold arithmetic is covered by TimeoutDuration unit tests. At the ST boundary, scheduler and RPC
    // overhead may consume the entire 10ms budget, so either completion or deadline exhaustion is valid.
    {
        std::shared_ptr<KVClient> client;
        InitTestKVClient(0, client, 60000, false, 10);

        std::vector<std::string> keys;
        std::vector<StringView> values;
        for (int i = 0; i < 3; ++i) {
            keys.emplace_back(ObjectKey());
            values.emplace_back("AB");
        }
        MSetParam param;
        std::vector<std::string> failedKeys;
        Timer timer;
        Status rc = client->MSet(keys, values, failedKeys, param);
        EXPECT_TRUE(rc.IsOk() || rc.GetCode() == K_RPC_DEADLINE_EXCEEDED) << rc.ToString();
        EXPECT_LT(timer.ElapsedMilliSecond(), 100);
    }

    // >10ms applies decay: after client->worker hop (~1ms), remaining is at the
    // SHORT_TIMEOUT_THRESHOLD_US=10000us boundary. If remaining >10000us, 0.8x decay
    // gives ~8400-8800us -> CeilUsToMs=9ms. If remaining <=10000us, no decay gives
    // CeilUsToMs=10ms. In both cases RPC timeout <=10ms, so sleep(11) always exceeds it.
    // minOnceRpcTimeoutMs=10 prevents retry after the first timeout.
    {
        std::shared_ptr<KVClient> client;
        InitTestKVClient(0, client, 60000, false, 11);

        DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "master.CreateMultiMeta.begin", "1*sleep(11)"));
        DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "master.CreateMultiMeta.begin", "1*sleep(11)"));

        std::vector<std::string> keys;
        std::vector<StringView> values;
        for (int i = 0; i < 3; ++i) {
            keys.emplace_back(ObjectKey());
            values.emplace_back("AB");
        }
        MSetParam param;
        std::vector<std::string> failedKeys;
        Timer timer;
        Status rc = client->MSet(keys, values, failedKeys, param);
        int64_t elapsedMs = timer.ElapsedMilliSecond();

        EXPECT_FALSE(rc.IsOk());
        EXPECT_EQ(rc.GetCode(), K_RPC_DEADLINE_EXCEEDED);
        EXPECT_LT(elapsedMs, 100);

        DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, 0, "master.CreateMultiMeta.begin"));
        DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, 1, "master.CreateMultiMeta.begin"));
    }
}

TEST_F(KVClientTimeoutTest, RetryDoesNotReinflateSharedBudget)
{
    std::shared_ptr<KVClient> client;
    InitTestKVClient(0, client, 60000, false, 50);

    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0,
        "worker.before_CreateMultiMetaToMaster", "1*sleep(48)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0,
        "master.CreateMultiMeta.begin", "2*return(K_RPC_DEADLINE_EXCEEDED)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1,
        "master.CreateMultiMeta.begin", "2*return(K_RPC_DEADLINE_EXCEEDED)"));

    std::vector<std::string> keys = { ObjectKey() };
    std::vector<StringView> values = { "AB" };

    MSetParam param;
    std::vector<std::string> failedKeys;
    Timer timer;
    Status rc = client->MSet(keys, values, failedKeys, param);
    int64_t elapsedMs = timer.ElapsedMilliSecond();
    EXPECT_EQ(rc.GetCode(), K_RPC_DEADLINE_EXCEEDED);
    EXPECT_LT(elapsedMs, 200);

    uint64_t count0 = 0;
    uint64_t count1 = 0;
    DS_ASSERT_OK(cluster_->GetInjectActionExecuteCount(WORKER, 0, "master.CreateMultiMeta.begin", count0));
    DS_ASSERT_OK(cluster_->GetInjectActionExecuteCount(WORKER, 1, "master.CreateMultiMeta.begin", count1));
    // Depending on scheduling, the shared budget may expire before or during the first master attempt.
    // More than one attempt would prove that a retry incorrectly re-inflated the original budget.
    EXPECT_LE(count0 + count1, 1u);

    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, 0, "worker.before_CreateMultiMetaToMaster"));
    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, 0, "master.CreateMultiMeta.begin"));
    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, 1, "master.CreateMultiMeta.begin"));
}

TEST_F(KVClientTimeoutTest, KVMSetMemoryCopySlowDeadlineExceeded)
{
    // 3ms budget: MultiCreate for tiny values finishes within budget, then the
    // MemoryCopyParallel.slow inject stalls the copy past the remaining budget.
    // Exercises the deadline check + slow-path log added around MemoryCopyParallel
    // (mirrors single Set's TimedMemoryCopyWithDeadline) so MSet fails fast instead
    // of completing a slow copy silently.
    std::shared_ptr<KVClient> client;
    InitTestKVClient(0, client, 60000, false, 3);

    DS_ASSERT_OK(inject::Set("ObjectClientImpl.MemoryCopyParallel.slow", "1*sleep(20)"));

    std::vector<std::string> keys;
    std::vector<StringView> values;
    for (int i = 0; i < 3; ++i) {
        keys.emplace_back(ObjectKey());
        values.emplace_back("AB");
    }

    MSetParam param;
    std::vector<std::string> failedKeys;
    Timer timer;
    Status rc = client->MSet(keys, values, failedKeys, param);
    int64_t elapsedMs = timer.ElapsedMilliSecond();

    EXPECT_FALSE(rc.IsOk());
    EXPECT_EQ(rc.GetCode(), K_RPC_DEADLINE_EXCEEDED);
    EXPECT_LT(elapsedMs, 5000);

    (void)inject::Clear("ObjectClientImpl.MemoryCopyParallel.slow");
}

}  // namespace st
}  // namespace datasystem
