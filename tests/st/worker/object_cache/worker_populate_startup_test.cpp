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
 * Description: Startup smoke test for shared memory pre-populate.
 */

#include "common.h"

#include <thread>

#include "cluster/external_cluster.h"

namespace datasystem {
namespace st {

namespace {
    constexpr int WORKER_INDEX = 0;
    constexpr int HOLD_SECONDS = 10;
}

class WorkerPopulateStartupTest : public ExternalClusterTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numEtcd = 1;
        opts.numMasters = 0;
        opts.numWorkers = 1;
        opts.waitWorkerReady = true;
        opts.workerGflagParams =
            " -shared_memory_size_mb=33792"
            " -shared_memory_populate=true"
            " -arena_per_tenant=1"
            " -enable_fallocate=false";
    }
};

TEST_F(WorkerPopulateStartupTest, DISABLED_Level1_Populate40GStart)
{
    auto *cluster = dynamic_cast<ExternalCluster *>(cluster_.get());
    ASSERT_NE(cluster, nullptr);
    std::this_thread::sleep_for(std::chrono::seconds(HOLD_SECONDS));
    ASSERT_TRUE(cluster->CheckWorkerProcess(WORKER_INDEX));
}

}  // namespace st
}  // namespace datasystem

