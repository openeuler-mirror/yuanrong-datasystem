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

/** Description: End-to-end validation for routing worker state and disconnect filters. */

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>

#include <gtest/gtest.h>

#include "client/object_cache/oc_client_common.h"
#include "datasystem/client/object_cache/routing/worker_router.h"
#include "datasystem/protos/cluster_topology.pb.h"

namespace datasystem {
namespace st {
namespace {
constexpr size_t KEY_SEARCH_LIMIT = 100'000;
constexpr int EVICTION_THRESHOLD = 100;  // matches BrokenFilter::EVICT_CONSECUTIVE_FAILURES
constexpr int64_t BROKEN_FILTER_RECOVERY_TIMEOUT_MS = 10'000;
constexpr int64_t RETRY_INTERVAL_MS = 200;
constexpr char ROUTE_KEY_PREFIX[] = "routing_failure_switch_";
}  // namespace

class RoutingFailureSwitchTest : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numEtcd = 1;
        opts.numWorkers = 2;
        opts.enableDistributedMaster = "true";
        opts.workerGflagParams =
            " -shared_memory_size_mb=512 -ipc_through_shared_memory=false -arena_per_tenant=1"
            " -enable_urma=false";
    }

    void SetUp() override
    {
        ExternalClusterTest::SetUp();
        WaitForActiveTopology();
    }

    void TearDown() override
    {
        ExternalClusterTest::TearDown();
    }

protected:
    void WaitForActiveTopology()
    {
        const auto deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(30);
        while (std::chrono::steady_clock::now() < deadline) {
            ClusterTopologyPb topology;
            if (ReadTopology(topology) && topology.members_size() == 2) {
                bool allActive = true;
                for (const auto &member : topology.members()) {
                    allActive = allActive && member.second.state() == MembershipPb::ACTIVE;
                }
                if (allActive) {
                    return;
                }
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        FAIL() << "Timed out waiting for two active workers";
    }

    bool ReadTopology(ClusterTopologyPb &topology)
    {
        return cluster_->ReadClusterTopology(topology).IsOk();
    }

    std::shared_ptr<std::unordered_map<std::string, std::string>> BuildHostIdMap(
        const ClusterTopologyPb &topology)
    {
        auto hostIdMap = std::make_shared<std::unordered_map<std::string, std::string>>();
        for (const auto &member : topology.members()) {
            (*hostIdMap)[member.first] = "routing-st-host";
        }
        return hostIdMap;
    }

    std::string FindKeyForWorker(client::WorkerRouter &router, const HostPort &targetWorker)
    {
        for (size_t i = 0; i < KEY_SEARCH_LIMIT; ++i) {
            std::string key = ROUTE_KEY_PREFIX + std::to_string(i);
            HostPort selectedWorker;
            Status rc = router.SelectWorker(key, client::DataPlacementPolicy::PREFERRED_META_OWNER, selectedWorker);
            if (rc.IsOk() && selectedWorker == targetWorker) {
                return key;
            }
        }
        ADD_FAILURE() << "Unable to find a route key for " << targetWorker;
        return "";
    }

    void VerifyStateFilter(const ClusterTopologyPb &topology,
                           const std::shared_ptr<std::unordered_map<std::string, std::string>> &hostIdMap,
                           client::WorkerRouter &router, const HostPort &remainingWorker,
                           const HostPort &targetWorker, const std::string &key)
    {
        HostPort selectedWorker;
        DS_ASSERT_OK(router.SelectWorker(key, client::DataPlacementPolicy::PREFERRED_META_OWNER, selectedWorker));
        EXPECT_EQ(selectedWorker, targetWorker);

        auto leavingTopology = std::make_shared<ClusterTopologyPb>(topology);
        (*leavingTopology->mutable_members())[targetWorker.ToString()].set_state(MembershipPb::LEAVING);
        std::unique_ptr<client::PreparedClusterTopology> prepared;
        DS_ASSERT_OK(client::PreparedClusterTopology::Create(std::move(*leavingTopology), prepared));
        router.UpdateHashRing(*prepared, *hostIdMap);
        DS_ASSERT_OK(router.SelectWorker(key, client::DataPlacementPolicy::PREFERRED_META_OWNER, selectedWorker));
        EXPECT_EQ(selectedWorker, remainingWorker);
    }

    void VerifyDisconnectFallbackAndRecovery(client::WorkerRouter &router, const HostPort &remainingWorker,
                                               const HostPort &targetWorker, const std::string &key)
    {
        // BrokenFilter debounces: a worker is evicted only after EVICT_CONSECUTIVE_FAILURES
        // (100) disconnects, so reach the threshold before asserting it is skipped.
        for (int i = 0; i < EVICTION_THRESHOLD; ++i) {
            router.UpdateState(targetWorker, K_CLIENT_WORKER_DISCONNECT);
        }
        HostPort selectedWorker;
        DS_ASSERT_OK(router.SelectWorker(key, client::DataPlacementPolicy::PREFERRED_META_OWNER, selectedWorker));
        ASSERT_EQ(selectedWorker, remainingWorker);

        const auto deadline =
            std::chrono::steady_clock::now() + std::chrono::milliseconds(BROKEN_FILTER_RECOVERY_TIMEOUT_MS);
        while (std::chrono::steady_clock::now() < deadline) {
            DS_ASSERT_OK(router.SelectWorker(key, client::DataPlacementPolicy::PREFERRED_META_OWNER, selectedWorker));
            if (selectedWorker == targetWorker) {
                return;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(RETRY_INTERVAL_MS));
        }
        FAIL() << "Broken worker filter did not expire within the recovery deadline";
    }
};

TEST_F(RoutingFailureSwitchTest, LEVEL1_StateAndDisconnectFiltersConverge)
{
    ClusterTopologyPb topology;
    ASSERT_TRUE(ReadTopology(topology));
    auto activeTopology = std::make_shared<ClusterTopologyPb>(topology);
    auto hostIdMap = BuildHostIdMap(topology);

    HostPort remainingWorker;
    HostPort targetWorker;
    DS_ASSERT_OK(cluster_->GetWorkerAddr(0, remainingWorker));
    DS_ASSERT_OK(cluster_->GetWorkerAddr(1, targetWorker));

    client::WorkerRouter router("routing-st-host");
    std::unique_ptr<client::PreparedClusterTopology> prepared;
    DS_ASSERT_OK(client::PreparedClusterTopology::Create(std::move(*activeTopology), prepared));
    router.UpdateHashRing(*prepared, *hostIdMap);
    const std::string key = FindKeyForWorker(router, targetWorker);
    ASSERT_FALSE(key.empty());

    VerifyStateFilter(topology, hostIdMap, router, remainingWorker, targetWorker, key);
    router.UpdateHashRing(*prepared, *hostIdMap);
    VerifyDisconnectFallbackAndRecovery(router, remainingWorker, targetWorker, key);
}

}  // namespace st
}  // namespace datasystem
