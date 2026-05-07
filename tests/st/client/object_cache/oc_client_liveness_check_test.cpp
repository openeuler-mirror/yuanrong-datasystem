/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2023. All rights reserved.
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
 * Description: worker liveness check test.
 */

#include <chrono>
#include <thread>
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/timer.h"
#include "oc_client_common.h"
#include "zmq_curve_test_common.h"

namespace datasystem {
namespace st {
class OCClientLivenessCheckTest : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        const int workerCount = 5;
        opts.numEtcd = 1;
        opts.numWorkers = workerCount;
        opts.enableDistributedMaster = "true";
        opts.enableLivenessProbe = true;
        opts.workerGflagParams =
            FormatString(" -authorization_enable=true -liveness_probe_timeout_s=%d", livenessProbeTimeoutS_);
    }

    void SetUp() override
    {
        CommonTest::SetUp();
        DS_ASSERT_OK(Init());
        DS_ASSERT_OK(cluster_->StartEtcdCluster());
        externalCluster_ = dynamic_cast<ExternalCluster *>(cluster_.get());
    }

    void StartWorkerAndWaitReady(std::initializer_list<uint32_t> indexes, int maxWaitTimeSec = 20)
    {
        for (auto i : indexes) {
            ASSERT_TRUE(externalCluster_->StartWorker(i, HostPort()).IsOk()) << i;
        }
        for (auto i : indexes) {
            ASSERT_TRUE(cluster_->WaitNodeReady(WORKER, i, maxWaitTimeSec).IsOk()) << i;
        }
        WaitWorkerLiveness(indexes);
    }

    void WaitWorkerLiveness(std::initializer_list<uint32_t> indexes, bool liveness = true,
                            uint64_t timeoutMs = waitTimeoutMs)
    {
        auto timeOut = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeoutMs);
        bool flag = false;
        while (std::chrono::steady_clock::now() < timeOut) {
            if (IsWorkerLiveness(indexes, liveness)) {
                flag = true;
                break;
            }
            const int interval = 100;  // 100ms;
            std::this_thread::sleep_for(std::chrono::milliseconds(interval));
        }
        ASSERT_TRUE(flag);
    }

    bool IsWorkerLiveness(std::initializer_list<uint32_t> indexes, bool liveness = true)
    {
        for (auto index : indexes) {
            if (cluster_->IsWorkerLiveness(index) != liveness) {
                return false;
            }
        }
        return true;
    }

protected:
    ExternalCluster *externalCluster_ = nullptr;
    int livenessProbeTimeoutS_ = 10;
    const static uint64_t waitTimeoutMs = 30000;
};

TEST_F(OCClientLivenessCheckTest, TestWaitWorkerLiveness)
{
    StartWorkerAndWaitReady({ 0, 1, 2 });
    std::this_thread::sleep_for(std::chrono::seconds(livenessProbeTimeoutS_ + 1));
    ASSERT_TRUE(IsWorkerLiveness({ 0, 1, 2 }));
}

TEST_F(OCClientLivenessCheckTest, LEVEL1_TestWorkerRpcPending)
{
    StartWorkerAndWaitReady({ 0, 1, 2, 3, 4 });
    int workerNum = 5;
    for (int index = 0; index < workerNum; index++) {
        DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, index, "WorkerLivenessCheck.GetServicesName.failed",
                                               "1*return(K_WORKER_ABNORMAL)"));
    }
    Timer timer;
    WaitWorkerLiveness({ 0, 1, 2, 3, 4 }, false);
    LOG(INFO) << "time elapsed:" << timer.ElapsedMilliSecond();
}

TEST_F(OCClientLivenessCheckTest, TestRocksdbFaultInAllowedTime)
{
    StartWorkerAndWaitReady({ 0, 1 });
    // inject fault.
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "master.rocksdb.put", "return(K_KVSTORE_ERROR)"));
    Timer timer;
    ASSERT_TRUE(IsWorkerLiveness({ 0 }));
    LOG(INFO) << "time elapsed:" << timer.ElapsedMilliSecond();
    ASSERT_TRUE(IsWorkerLiveness({ 1 }));
}

TEST_F(OCClientLivenessCheckTest, TestRocksdbFaultOverAllowedTime)
{
    StartWorkerAndWaitReady({ 0, 1 });
    // inject fault.
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "WorkerLivenessCheck.CheckRocksDbService.failed",
                                           "return(K_WORKER_ABNORMAL)"));
    Timer timer;
    ASSERT_TRUE(IsWorkerLiveness({ 0 }));
    LOG(INFO) << "time elapsed:" << timer.ElapsedMilliSecond();
    ASSERT_TRUE(IsWorkerLiveness({ 1 }));
}

class OCClientThreadPoolLivenessCheckTest : public OCClientLivenessCheckTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        const int workerCount = 2;
        opts.numEtcd = 1;
        opts.numWorkers = workerCount;
        opts.enableDistributedMaster = "true";
        opts.enableLivenessProbe = true;
        opts.workerGflagParams =
            FormatString(" -authorization_enable=true -liveness_probe_timeout_s=%d -rpc_thread_num=4 -log_monitor=true",
                         livenessProbeTimeoutS_);
    }
};

TEST_F(OCClientThreadPoolLivenessCheckTest, LEVEL1_TestThreadFull)
{
    StartWorkerAndWaitReady({ 0, 1 });
    Timer timer;
    auto runTime = 20;
    auto task = [&]() {
        std::shared_ptr<KVClient> client;
        std::shared_ptr<KVClient> client1;
        InitTestKVClient(0, client);
        InitTestKVClient(1, client1);
        while (timer.ElapsedSecond() < runTime) {
            client->Set("key", "value");
            std::string value;
            client->Get("key", value);
            client1->Get("key", value);
            client1->Del("key");
        }
    };
    int threadNum = 20;
    ThreadPool threadPool(threadNum);
    std::vector<std::future<void>> futures;
    for (int i = 0; i < threadNum; i++) {
        futures.emplace_back(threadPool.Submit(task));
    }
    for (auto &f : futures) {
        f.get();
    }
    ASSERT_TRUE(IsWorkerLiveness({ 0, 1 }));
}
}  // namespace st
}  // namespace datasystem
