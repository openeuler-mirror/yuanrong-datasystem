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

#include <atomic>
#include <memory>
#include <string>
#include <unordered_map>

#include <gtest/gtest.h>

#include "datasystem/client/routing/hash_ring_refresher.h"
#include "datasystem/client/routing/routing.h"
#include "datasystem/client/routing/worker_router.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/protos/cluster_topology.pb.h"
#include "ut/common.h"

namespace datasystem {
namespace ut {

class RoutingFacadeTest : public CommonTest {
protected:
    static void FillRing(::datasystem::ClusterTopologyPb &ring, std::unordered_map<std::string, std::string> &hostIdMap)
    {
        auto &worker = (*ring.mutable_members())["127.0.0.1:1000"];
        worker.set_state(::datasystem::MembershipPb::ACTIVE);
        worker.add_tokens(100u);
        hostIdMap["127.0.0.1:1000"] = "host-a";
    }
};

TEST_F(RoutingFacadeTest, TestInitFetchesRingAndStartsFacade)
{
    auto router = std::make_shared<client::WorkerRouter>("");
    std::atomic<int> fetchCount{ 0 };
    auto fetch = [&fetchCount](const HostPort &, uint64_t, ::datasystem::ClusterTopologyPb &ring, std::string &,
                               uint64_t &newVersion, bool &changed,
                               std::unordered_map<std::string, std::string> &hostIdMap) {
        fetchCount.fetch_add(1);
        FillRing(ring, hostIdMap);
        newVersion = 1;
        changed = true;
        return Status::OK();
    };
    auto refresher = std::make_shared<client::HashRingRefresher>(router, fetch);
    client::Routing routing(router, refresher, 60'000);

    DS_ASSERT_OK(routing.Init("host-a", HostPort("127.0.0.1", 1000)));
    EXPECT_GE(fetchCount.load(), 1);

    HostPort worker;
    DS_ASSERT_OK(routing.SelectWorker("key", client::SelectStrategy::HASH_RING_AFFINITY, worker));
    EXPECT_EQ(worker.ToString(), "127.0.0.1:1000");
    routing.Shutdown();
}

TEST_F(RoutingFacadeTest, TestCallsBeforeInitFail)
{
    auto router = std::make_shared<client::WorkerRouter>("host-a");
    auto fetch = [](const HostPort &, uint64_t, ::datasystem::ClusterTopologyPb &, std::string &, uint64_t &, bool &,
                    std::unordered_map<std::string, std::string> &) { return Status::OK(); };
    auto refresher = std::make_shared<client::HashRingRefresher>(router, fetch);
    client::Routing routing(router, refresher);

    HostPort worker;
    EXPECT_EQ(routing.SelectWorker("key", client::SelectStrategy::HASH_RING_AFFINITY, worker).GetCode(), K_NOT_READY);
}

TEST_F(RoutingFacadeTest, TestInitRejectsInvalidDependenciesAndArguments)
{
    auto router = std::make_shared<client::WorkerRouter>("host-a");
    auto fetch = [](const HostPort &, uint64_t, ::datasystem::ClusterTopologyPb &, std::string &, uint64_t &, bool &,
                    std::unordered_map<std::string, std::string> &) { return Status::OK(); };
    auto refresher = std::make_shared<client::HashRingRefresher>(router, fetch);

    client::Routing noRouter(nullptr, refresher);
    EXPECT_TRUE(noRouter.Init("host-a", HostPort("127.0.0.1", 1000)).IsError());

    client::Routing noRefresher(router, nullptr);
    EXPECT_TRUE(noRefresher.Init("host-a", HostPort("127.0.0.1", 1000)).IsError());

    client::Routing routing(router, refresher);
    EXPECT_EQ(routing.Init("host-a", HostPort()).GetCode(), K_INVALID);
    DS_ASSERT_OK(routing.Init("", HostPort("127.0.0.1", 1000)));
    routing.Shutdown();
}

}  // namespace ut
}  // namespace datasystem
