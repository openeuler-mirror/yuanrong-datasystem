/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
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
 * Description: kv client mset scale test
 */

#include "client/object_cache/oc_client_common.h"
#include "client/kv_cache/kv_client_scale_common.h"
#include "common.h"

namespace datasystem {
namespace st {
class KVClientMSetScaleTest : public STCScaleTest {
protected:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numEtcd = 1;
        opts.numWorkers = workerNum_;
        opts.numOBS = 1;
        opts.addNodeTime = SCALE_UP_ADD_TIME;
        opts.enableDistributedMaster = "true";
        opts.workerGflagParams = " -v=1 -node_timeout_s=3 ";
        opts.waitWorkerReady = false;
    }
    int workerNum_ = 4;
};

TEST_F(KVClientMSetScaleTest, MSetDuringNodeAddition)
{
    std::string value{ "anyValue" };
    const int w2Index = 2;
    StartWorkerAndWaitReady({ 0, 1, w2Index });
    InitTestKVClient(0, client_);
    InitTestKVClient(w2Index, client2_);

    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "HashRing.SubmitScaleUpTask.skip", "return(2)"));
    const int w3Index = 3;
    StartWorkerAndWaitReady({ w3Index });  // scale-up worker3
    const int waitScaleUpBeginTime = 3;
    sleep(waitScaleUpBeginTime);  // scale-up begin
    int idx = 0;
    std::vector<std::string> keys;
    std::vector<StringView> valViews;
    MSetParam param{ .writeMode = WriteMode::NONE_L2_CACHE, .ttlSecond = 0, .existence = ExistenceOpt::NX };
    const int total = 500;
    while (idx < total) {
        keys.clear();
        valViews.clear();
        int num = randomData_.GetRandomUint32(1, 8);  // Randomly MSetTx num.
        for (int j = 0; j < num && idx < total; j++) {
            keys.emplace_back("redirect_test_" + std::to_string(idx++));
            valViews.emplace_back(value);
        }
        DS_ASSERT_OK(client_->MSetTx(keys, valViews, param));
    }
    const int waitScaleUpFinishTime = 2;
    sleep(waitScaleUpFinishTime);  // scale-up should be finished.

    std::string outValue;
    for (int i = 0; i < total; i++) {
        auto key = "redirect_test_" + std::to_string(i);
        DS_ASSERT_OK(client2_->Get(key, outValue));
        ASSERT_EQ(value, outValue);
        DS_ASSERT_OK(client_->Get(key, outValue));
        ASSERT_EQ(value, outValue);
    }
}

TEST_F(KVClientMSetScaleTest, LEVEL1_ContinuousRedirection)
{
    StartWorkerAndWaitReady({ 0 });
    InitTestKVClient(0, client_, 20'000); // Init client with 20'000 timeout
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "worker.CreateMultiMetaParallel.begin", "4*sleep(4000)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "worker.CreateMultiMetaPhaseTwo.delay", "4*sleep(4000)"));
    const int threadNum = 4;
    ThreadPool threadPool(threadNum);
    std::vector<std::future<void>> futs;
    for (int i = 0; i < threadNum; i++) {
        futs.emplace_back(threadPool.Submit([this]() {
            const int objNum = 8;
            std::vector<std::string> keys;
            std::vector<StringView> valViews;
            for (int i = 0; i < objNum; i++) {
                keys.emplace_back(GetStringUuid());
                valViews.emplace_back("anyValue");
            }
            MSetParam param{ .writeMode = WriteMode::NONE_L2_CACHE, .ttlSecond = 0, .existence = ExistenceOpt::NX };
            DS_ASSERT_OK(client_->MSetTx(keys, valViews, param));
        }));
    }
    // The object has been migrated from worker0 to worker1, so the metadata creation will be redirected.
    ASSERT_TRUE(externalCluster_->StartWorker(1, HostPort(), " -inject_actions=BatchMigrateMetadata.delay:call(5)")
                    .IsOk());  // scale up worker1
    sleep(SCALE_UP_ADD_TIME + 1);
    // The object has been migrated from worker1 to worker2, so the 2pc will be redirected.
    ASSERT_TRUE(externalCluster_->StartWorker(2, HostPort()).IsOk());  // scale up worker2
    for (auto &fut : futs) {
        fut.wait();
    }
}
}  // namespace st
}  // namespace datasystem