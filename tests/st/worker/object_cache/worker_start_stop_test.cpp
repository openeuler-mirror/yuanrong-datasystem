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
 * Description: worker start/stop test.
 */

#include <chrono>
#include "common.h"

namespace datasystem {
namespace st {
class WorkerStartStopTest : public ExternalClusterTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numEtcd = 1;
        opts.numWorkers = 1;
        opts.waitWorkerReady = false;
        opts.enableDistributedMaster = "true";
        opts.workerGflagParams = "-check_async_queue_empty_time_s=1";
    }

    void SetUp() override
    {
        CommonTest::SetUp();
        DS_ASSERT_OK(Init());
        ASSERT_TRUE(cluster_ != nullptr);
        DS_ASSERT_OK(cluster_->StartEtcdCluster());
    }
};

TEST_F(WorkerStartStopTest, StartThenStopImmediately)
{
    using Clock = std::chrono::steady_clock;
    DS_ASSERT_OK(cluster_->StartWorkers());
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 0));
    auto readyTime = Clock::now();

    ASSERT_TRUE(cluster_->CheckWorkerProcess(0));
    auto startTime = Clock::now();
    DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, 0));
    auto stopTime = Clock::now();

    auto startToReadyMs = std::chrono::duration_cast<std::chrono::milliseconds>(readyTime - startTime).count();
    auto readyToStopMs = std::chrono::duration_cast<std::chrono::milliseconds>(stopTime - readyTime).count();
    auto startToStopMs = std::chrono::duration_cast<std::chrono::milliseconds>(stopTime - startTime).count();
    LOG(INFO) << "[WorkerStartStopTest] worker start->ready cost " << startToReadyMs
              << " ms, ready->stop cost " << readyToStopMs
              << " ms, start->stop total cost " << startToStopMs << " ms.";
    ASSERT_FALSE(cluster_->CheckWorkerProcess(0));
}
}  // namespace st
}  // namespace datasystem
