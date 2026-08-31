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
 * Description: Shared immutable metadata route for Object-cache unit fixtures.
 */
#ifndef DATASYSTEM_TESTS_UT_WORKER_OBJECT_CACHE_TEST_METADATA_ROUTE_H
#define DATASYSTEM_TESTS_UT_WORKER_OBJECT_CACHE_TEST_METADATA_ROUTE_H

#include <algorithm>
#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "datasystem/cluster/algorithm/hash_algorithm.h"
#include "datasystem/cluster/executor/topology_phase_callbacks.h"
#include "datasystem/cluster/repository/topology_key_helper.h"
#include "datasystem/cluster/repository/topology_repository_codec.h"
#include "datasystem/cluster/runtime/topology_engine.h"
#include "datasystem/worker/metadata_route_resolver.h"
#include "ut/cluster/testing/fake_coordinator_service_proxy.h"

namespace datasystem::ut {

inline const worker::MetadataRouteResolver &GetTestMetadataRoute()
{
    static const worker::MetadataRouteResolver route(nullptr, [] {
        worker::MetadataRouteOptions options;
        options.centralizedMode = true;
        options.masterAddress = HostPort("127.0.0.1", 31500);
        return options;
    }());
    return route;
}

class TestTopologyPhaseCallbacks final : public cluster::ITopologyPhaseCallbacks {
public:
    ~TestTopologyPhaseCallbacks() override = default;

    Status OnScaleOut(const cluster::TopologyCallbackContext &) override
    {
        return Status::OK();
    }

    Status OnScaleIn(const cluster::TopologyCallbackContext &) override
    {
        return Status::OK();
    }

    Status OnScaleInDataDrain(const cluster::TopologyCallbackContext &) override
    {
        return Status::OK();
    }

    Status PrepareScaleInCleanup(const cluster::TopologyCallbackContext &,
                                 std::unique_ptr<cluster::TopologyPreparedCleanup> &cleanup) override
    {
        cleanup = std::make_unique<cluster::TopologyPreparedCleanup>(
            [] { return Status::OK(); },
            [](std::chrono::steady_clock::time_point, const cluster::CancellationToken &) { return Status::OK(); });
        return Status::OK();
    }

    Status OnFailure(const cluster::TopologyCallbackContext &) override
    {
        return Status::OK();
    }
};

class ObjectTopologyTestRuntime final {
public:
    ObjectTopologyTestRuntime() = default;
    ~ObjectTopologyTestRuntime()
    {
        if (engine_ != nullptr) {
            constexpr auto shutdownWait = std::chrono::seconds(1);
            (void)engine_->Shutdown(std::chrono::steady_clock::now() + shutdownWait);
        }
    }

    Status Init(const HostPort &localAddress,
                std::function<void(cluster::TopologyAvailabilityLevel)> availabilityHandler = nullptr)
    {
        if (engine_ != nullptr) {
            return Status::OK();
        }
        cluster::CoordinatorWatchIngress ingress;
        ingress.bind = [this](cluster::CoordinatorWatchIngress::Handler handler) {
            std::lock_guard<std::mutex> lock(ingressMutex_);
            watchHandler_ = std::move(handler);
            return Status::OK();
        };
        ingress.unbindAndDrain = [this](std::chrono::steady_clock::time_point) {
            std::lock_guard<std::mutex> lock(ingressMutex_);
            watchHandler_ = nullptr;
            return Status::OK();
        };
        cluster::TopologyEngine::Builder builder;
        builder.SetClusterName("")
            .SetLocalAddress(localAddress.ToString())
            .UseCoordinator(proxy_, std::move(ingress))
            .SetPhaseCallbacks(callbacks_)
            .SetWorkerProbeHandler([](cluster::WorkerProbeRequest) { return Status::OK(); })
            .SetAvailabilityHandler(std::move(availabilityHandler))
            .SetNodeDeadTimeout(std::chrono::seconds(30));
        return builder.Build(engine_);
    }

    cluster::TopologyEngine *Engine() const
    {
        return engine_.get();
    }

    Status StartWithActiveLocalMember(const HostPort &localAddress)
    {
        CHECK_FAIL_RETURN_STATUS(engine_ != nullptr, K_NOT_READY, "test topology engine is not initialized");
        const auto address = localAddress.ToString();
        cluster::TopologyState topology;
        topology.clusterHasInit = true;
        topology.version = 1;
        topology.members = { cluster::Member{ { std::string(16, 'l'), address }, cluster::MemberState::ACTIVE,
                                              MakeTokens(address, 0) } };
        std::unique_ptr<cluster::TopologyKeyHelper> keys;
        RETURN_IF_NOT_OK(cluster::TopologyKeyHelper::Create("", keys));
        std::string encoded;
        RETURN_IF_NOT_OK(cluster::TopologyRepositoryCodec::EncodeTopology(topology, encoded));
        RETURN_IF_NOT_OK(
            proxy_.PutRaw(keys->TopologyTable() + "/" + cluster::TopologyKeyHelper::TopologyKey(), encoded));
        return engine_->Start();
    }

    Status TriggerAuthorityConflict(const HostPort &localAddress)
    {
        const auto address = localAddress.ToString();
        cluster::TopologyState topology;
        topology.clusterHasInit = true;
        topology.version = 1;
        topology.members = { cluster::Member{ { std::string(16, 'l'), address }, cluster::MemberState::ACTIVE,
                                              MakeTokens(address, 1), MakeTokenSeedOverrides(1) } };
        std::unique_ptr<cluster::TopologyKeyHelper> keys;
        RETURN_IF_NOT_OK(cluster::TopologyKeyHelper::Create("", keys));
        const auto topologyKey = keys->TopologyTable() + "/" + cluster::TopologyKeyHelper::TopologyKey();
        std::string encoded;
        RETURN_IF_NOT_OK(cluster::TopologyRepositoryCodec::EncodeTopology(topology, encoded));
        RETURN_IF_NOT_OK(proxy_.PutRaw(topologyKey, encoded));
        const auto watches = proxy_.WatchCalls();
        const auto found = std::find_if(watches.begin(), watches.end(), [&topologyKey](const auto &watch) {
            return watch.key == topologyKey;
        });
        CHECK_FAIL_RETURN_STATUS(found != watches.end(), K_NOT_FOUND, "topology watch is not registered");
        cluster::CoordinatorWatchIngress::Handler handler;
        {
            std::lock_guard<std::mutex> lock(ingressMutex_);
            handler = watchHandler_;
        }
        CHECK_FAIL_RETURN_STATUS(handler != nullptr, K_NOT_READY, "topology watch ingress is not bound");
        RETURN_IF_NOT_OK(handler("coordinator-test", found->watchId,
                                 { cluster::CoordinationEventType::PUT, topologyKey, "", 2, 2 }));
        constexpr auto isolationWait = std::chrono::seconds(1);
        constexpr auto pollingInterval = std::chrono::milliseconds(1);
        const auto deadline = std::chrono::steady_clock::now() + isolationWait;
        while (std::chrono::steady_clock::now() < deadline) {
            if (engine_->GetAvailability() == cluster::TopologyAvailabilityLevel::ROLE_ISOLATED) {
                return Status::OK();
            }
            std::this_thread::sleep_for(pollingInterval);
        }
        RETURN_STATUS(K_RUNTIME_ERROR, "topology engine did not enter role isolation after authority conflict");
    }

private:
    static std::vector<uint32_t> MakeTokens(const std::string &address, uint32_t seed)
    {
        constexpr uint32_t tokenCount = 4;
        std::vector<uint32_t> tokens;
        tokens.reserve(tokenCount);
        for (uint32_t index = 0; index < tokenCount; ++index) {
            tokens.emplace_back(cluster::HashAlgorithm::MakeToken(address, index, seed));
        }
        return tokens;
    }

    static std::vector<cluster::TokenSeedOverride> MakeTokenSeedOverrides(uint32_t seed)
    {
        constexpr uint32_t tokenCount = 4;
        std::vector<cluster::TokenSeedOverride> overrides;
        overrides.reserve(tokenCount);
        for (uint32_t index = 0; index < tokenCount; ++index) {
            overrides.emplace_back(cluster::TokenSeedOverride{ index, seed });
        }
        return overrides;
    }

    cluster::testing::FakeCoordinatorServiceProxy proxy_;
    TestTopologyPhaseCallbacks callbacks_;
    std::unique_ptr<cluster::TopologyEngine> engine_;
    std::mutex ingressMutex_;
    cluster::CoordinatorWatchIngress::Handler watchHandler_;
};

inline ObjectTopologyTestRuntime &GetObjectTopologyTestRuntime()
{
    static ObjectTopologyTestRuntime runtime;
    return runtime;
}

}  // namespace datasystem::ut

#endif  // DATASYSTEM_TESTS_UT_WORKER_OBJECT_CACHE_TEST_METADATA_ROUTE_H
