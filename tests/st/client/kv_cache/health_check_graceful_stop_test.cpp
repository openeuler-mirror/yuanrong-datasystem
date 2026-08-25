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

/** Description: Regression coverage for issue #849 / PR !1759. */

#include <chrono>
#include <csignal>
#include <cstdint>
#include <memory>
#include <string>
#include <thread>

#include <gtest/gtest.h>

#include "client/object_cache/oc_client_common.h"
#include "datasystem/kv_client.h"
#include "datasystem/utils/status.h"

namespace datasystem::st {
namespace {
constexpr uint32_t WORKER_COUNT = 2;
constexpr uint32_t STOPPED_WORKER_INDEX = 0;
constexpr int64_t SCALE_DOWN_DEADLINE_MS = 15'000;
constexpr int32_t HEALTH_CHECK_POLL_INTERVAL_MS = 50;
}  // namespace

class HealthCheckGracefulStopTest : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numEtcd = 1;
        opts.numWorkers = WORKER_COUNT;
        opts.enableDistributedMaster = "true";
        opts.workerGflagParams = " -shared_memory_size_mb=512 -ipc_through_shared_memory=true"
                                 " -oc_shm_transfer_threshold_kb=1 -arena_per_tenant=1"
                                 " -use_brpc=true -enable_urma=false";
    }

    void SetUp() override
    {
        ExternalClusterTest::SetUp();
        externalCluster_ = dynamic_cast<ExternalCluster *>(cluster_.get());
        ASSERT_NE(externalCluster_, nullptr);
        InitTestKVClient(STOPPED_WORKER_INDEX, client_);
        ASSERT_NE(client_, nullptr);
    }

    void TearDown() override
    {
        client_.reset();
        ExternalClusterTest::TearDown();
    }

    bool SendNonScaleInGracefulStop(uint32_t workerIndex)
    {
        pid_t pid = externalCluster_->GetWorkerPid(workerIndex);
        return pid > 0 && ::kill(pid, SIGTERM) == 0;
    }

    Status PollHealthCheckForScaleDown(std::string &observedCodes)
    {
        Status lastRc(K_NOT_READY, "Worker has not exposed scale down yet");
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(SCALE_DOWN_DEADLINE_MS);
        while (std::chrono::steady_clock::now() < deadline) {
            Status rc = client_->HealthCheck();
            lastRc = rc;
            observedCodes += std::to_string(rc.GetCode()) + ",";
            if (rc.GetCode() == StatusCode::K_SCALE_DOWN) {
                return rc;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(HEALTH_CHECK_POLL_INTERVAL_MS));
        }
        return lastRc;
    }

protected:
    ExternalCluster *externalCluster_{ nullptr };
    std::shared_ptr<KVClient> client_;
};

TEST_F(HealthCheckGracefulStopTest, LEVEL1_NonScaleInGracefulStopExposesScaleDown)
{
    DS_ASSERT_OK(client_->HealthCheck());

    // Do not create worker-status or enable lossless exit: this exercises the non-scale-in path.
    ASSERT_TRUE(SendNonScaleInGracefulStop(STOPPED_WORKER_INDEX));

    std::string observedCodes;
    Status rc = PollHealthCheckForScaleDown(observedCodes);
    EXPECT_EQ(rc.GetCode(), StatusCode::K_SCALE_DOWN)
        << "expected K_SCALE_DOWN during non scale-in graceful stop; observed codes: " << observedCodes;
}
}  // namespace datasystem::st
