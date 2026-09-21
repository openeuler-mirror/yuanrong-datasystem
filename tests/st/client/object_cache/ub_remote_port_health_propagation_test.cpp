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

/** Description: A/B integration tests for remote UB port-health propagation into client routing. */

#include <chrono>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>
#include <unistd.h>

#include <gtest/gtest.h>

#include "common.h"
#include "oc_client_common.h"
#include "datasystem/client/object_cache/routing/ub_health_filter.h"
#include "datasystem/client/object_cache/routing/worker_router.h"
#include "datasystem/client/object_cache/transport/rpc/worker_rpc_client.h"
#include "datasystem/common/rpc/api_deadline.h"
#include "datasystem/common/object_cache/ub_health_summary_codec.h"
#include "datasystem/common/rdma/fast_transport_manager_wrapper.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/worker/object_cache/worker_worker_oc_api.h"
#include "datasystem/common/rpc/rpc_stub_cache_mgr.h"

namespace datasystem::st {
#ifdef USE_URMA_MOCK
class UbPortHealthRpcPropagationTest : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = 1;
        opts.numEtcd = 1;
        opts.enableDistributedMaster = "true";
        opts.workerGflagParams = " -enable_urma=true -shared_memory_size_mb=128 -arena_per_tenant=1";
    }

    void SetUp() override
    {
        const char *previousUds = getenv("URMA_MOCK_UDS_BASE_DIR");
        if (previousUds != nullptr) {
            previousUds_ = previousUds;
        }
        const auto uds = "/tmp/ds_urma_port_rpc_" + std::to_string(getpid());
        ASSERT_EQ(setenv("URMA_MOCK_UDS_BASE_DIR", uds.c_str(), 1), 0);
        ExternalClusterTest::SetUp();
        ConnectOptions options;
        InitConnectOpt(0, options);
        worker_ = HostPort(options.host, options.port);
        auto signature = std::make_shared<Signature>(options.accessKey, options.secretKey);
        rpc_ = std::make_shared<client::WorkerRpcClient>(worker_, std::move(signature));
        DS_ASSERT_OK(rpc_->Init());
    }

    void TearDown() override
    {
        rpc_.reset();
        ExternalClusterTest::TearDown();
        if (previousUds_.has_value()) {
            (void)setenv("URMA_MOCK_UDS_BASE_DIR", previousUds_->c_str(), 1);
        } else {
            (void)unsetenv("URMA_MOCK_UDS_BASE_DIR");
        }
    }

    Status WaitForPorts(const std::string &incarnation, uint32_t bad, UbHealthSummary &summary)
    {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (std::chrono::steady_clock::now() < deadline) {
            ApiDeadlineGuard requestBudget(1000);
            QueryUbPortHealthRspPb response;
            auto status = rpc_->QueryUbPortHealth(incarnation, 1000, response);
            if (status.IsOk() && response.has_health_summary()
                && DecodeUbHealthSummary(response.health_summary(), summary).IsOk()
                && summary.portHealth.has_value() && HasKnownUbPortHealth(*summary.portHealth)
                && !summary.portHealth->verificationPending && summary.portHealth->badPortCount == bad) {
                return Status::OK();
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
        return Status(K_NOT_READY, "Worker port facts did not converge through QueryUbPortHealth RPC");
    }

protected:
    HostPort worker_;
    std::shared_ptr<client::WorkerRpcClient> rpc_;
    std::optional<std::string> previousUds_;
};

TEST_F(UbPortHealthRpcPropagationTest, UserCtlPortMatrixTravelsThroughRpcToWorkerRouter)
{
    GetHashRingRspPb ring;
    {
        ApiDeadlineGuard requestBudget(5000);
        DS_ASSERT_OK(rpc_->InvokeGetHashRing(0, ring));
    }
    ASSERT_TRUE(ring.has_hash_ring());
    const auto member = ring.hash_ring().members().find(worker_.ToString());
    ASSERT_NE(member, ring.hash_ring().members().end());
    const auto incarnation = member->second.id();
    auto registry = std::make_shared<client::WorkerUbHealthRegistry>();
    auto filter = std::make_shared<client::UbHealthFilter>(registry);
    client::WorkerRouter router("rpc-client", registry,
                                std::vector<std::shared_ptr<client::IWorkerFilter>>{ filter });
    std::unique_ptr<client::PreparedClusterTopology> prepared;
    DS_ASSERT_OK(client::PreparedClusterTopology::Create(ClusterTopologyPb(ring.hash_ring()), prepared));
    router.UpdateHashRing(*prepared, {});
    filter->ApplyTopologyIncarnations(ring.hash_ring());
    bool previouslyIsolated = false;
    for (uint32_t bad : { 0u, 4u, 3u }) {
        DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "UrmaMock.QueryPortStatus",
                                               "call(4," + std::to_string(bad) + ")"));
        UbHealthSummary summary;
        DS_ASSERT_OK(WaitForPorts(incarnation, bad, summary));
        (void)filter->ObserveSummary(summary, incarnation);
        EXPECT_EQ(filter->IsAvailable(worker_, client::WorkerAccessAction::CONTROL), !previouslyIsolated);
        ASSERT_TRUE(filter->ApplySummary(summary, incarnation));
        auto snapshot = router.GetUbRoutingHealthSnapshot();
        ASSERT_NE(snapshot->workers.find(worker_), snapshot->workers.end());
        const auto &health = snapshot->workers.at(worker_);
        EXPECT_EQ(health.portHealth.totalPortCount, 4u);
        EXPECT_EQ(health.portHealth.badPortCount, bad);
        EXPECT_EQ(health.HealthyPortCount(), 4u - bad);
        previouslyIsolated = bad == 4;
        EXPECT_EQ(filter->IsAvailable(worker_, client::WorkerAccessAction::CONTROL), !previouslyIsolated);
    }
}
class UbWorkerPeerPortHealthTest : public UbPortHealthRpcPropagationTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        UbPortHealthRpcPropagationTest::SetClusterSetupOptions(opts);
        opts.numWorkers = 2;
        opts.workerGflagParams += " -enable_worker_worker_batch_get=false -ipc_through_shared_memory=false"
                                 " -enable_transport_fallback=false";
    }

    Status WaitForInjection(uint32_t worker, const std::string &point, uint64_t minimum)
    {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (std::chrono::steady_clock::now() < deadline) {
            uint64_t count = 0;
            RETURN_IF_NOT_OK(cluster_->GetInjectActionExecuteCount(WORKER, worker, point, count));
            if (count >= minimum) {
                return Status::OK();
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        return Status(K_NOT_READY, "Worker injection was not reached: " + point);
    }

    void PrepareRemoteObjects(std::shared_ptr<ObjectClient> &requester, std::shared_ptr<ObjectClient> &provider,
                              std::vector<std::string> &keys, const std::string &data)
    {
        const auto initBound = [this](uint32_t index, std::shared_ptr<ObjectClient> &client) {
            ConnectOptions options;
            InitConnectOpt(index, options);
            options.enableLocalCache = true;
            options.enableCrossNodeConnection = false;
            options.requestTimeoutMs = 1000;
            client = std::make_shared<ObjectClient>(options);
            return client->Init();
        };
        DS_ASSERT_OK(initBound(0, requester));
        DS_ASSERT_OK(initBound(1, provider));
        constexpr size_t objectCount = 4;
        for (size_t i = 0; i < objectCount; ++i) {
            keys.emplace_back("ub-requester-" + GetStringUuid());
            DS_ASSERT_OK(provider->Put(keys.back(), reinterpret_cast<const uint8_t *>(data.data()), data.size(), {}));
        }
    }

    void CheckValue(std::vector<Optional<Buffer>> &buffers, const std::string &data)
    {
        ASSERT_EQ(buffers.size(), 1u);
        ASSERT_TRUE(buffers.front());
        ASSERT_EQ(buffers.front()->GetSize(), data.size());
        EXPECT_EQ(std::memcmp(buffers.front()->MutableData(), data.data(), data.size()), 0);
    }

    Status GetAfterRandomizedRecovery(const std::shared_ptr<ObjectClient> &requester, const std::string &key,
                                      std::vector<Optional<Buffer>> &buffers)
    {
        const auto deadline =
            std::chrono::steady_clock::now() + std::chrono::milliseconds(UB_REMOTE_PORT_HEALTH_RETRY_MAX_MS + 5'000);
        Status lastStatus;
        while (std::chrono::steady_clock::now() < deadline) {
            buffers.clear();
            lastStatus = requester->Get({ key }, 0, buffers);
            if (lastStatus.IsOk()) {
                return lastStatus;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
        }
        return Status(K_RUNTIME_ERROR, "Worker UB recovery query did not converge: " + lastStatus.ToString());
    }
};

TEST_F(UbWorkerPeerPortHealthTest, WorkerRpcQueryUsesTheCachedPortHealthContract)
{
    GetHashRingRspPb ring;
    DS_ASSERT_OK(rpc_->InvokeGetHashRing(0, ring));
    ASSERT_TRUE(ring.has_hash_ring());
    const auto member = ring.hash_ring().members().find(worker_.ToString());
    ASSERT_NE(member, ring.hash_ring().members().end());
    const auto incarnation = member->second.id();
    ConnectOptions source;
    InitConnectOpt(1, source);
    constexpr uint64_t stubCacheSize = 100;
    DS_ASSERT_OK(RpcStubCacheMgr::Instance().Init(stubCacheSize, HostPort(source.host, source.port)));
    auto signature = std::make_shared<AkSkManager>();
    DS_ASSERT_OK(signature->SetClientAkSk(source.accessKey, source.secretKey));
    std::shared_ptr<object_cache::WorkerRemoteWorkerOCApi> peerRpc;
    DS_ASSERT_OK(object_cache::CreateRemoteWorkerApi(
        worker_.ToString(), HostPort(source.host, source.port), signature, peerRpc));
    for (uint32_t bad : { 4u, 3u, 0u }) {
        DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "UrmaMock.QueryPortStatus",
                                               "call(4," + std::to_string(bad) + ")"));
        UbHealthSummary cached;
        DS_ASSERT_OK(WaitForPorts(incarnation, bad, cached));
        QueryUbPortHealthRspPb response;
        DS_ASSERT_OK(peerRpc->QueryUbPortHealth(incarnation, 1000, response));
        UbHealthSummary received;
        DS_ASSERT_OK(DecodeUbHealthSummary(response.health_summary(), received));
        ASSERT_TRUE(received.portHealth.has_value());
        EXPECT_EQ(received.incarnation, incarnation);
        EXPECT_EQ(received.portHealth->badPortCount, bad);
        EXPECT_EQ(received.portHealth->totalPortCount, 4u);
    }
    QueryUbPortHealthRspPb staleResponse;
    EXPECT_EQ(peerRpc->QueryUbPortHealth("retired-worker", 1000, staleResponse).GetCode(), K_NOT_READY);
    EXPECT_FALSE(staleResponse.has_health_summary());
}

TEST_F(UbWorkerPeerPortHealthTest, Cqe9IsolatesRequesterAndStopsRemoteGetWhenFallbackDisabled)
{
    std::shared_ptr<ObjectClient> requester;
    std::shared_ptr<ObjectClient> provider;
    const std::string data(1024, 'u');
    std::vector<std::string> keys;
    PrepareRemoteObjects(requester, provider, keys, data);
    ASSERT_EQ(keys.size(), 4u);
    const std::string beforeRpc =
        "worker_oc_service_get_impl.pull_object_data_from_remote_worker.before_get_from_remote";
    const std::string cqePoint = "UrmaManager.CheckCompletionRecordStatus";
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, beforeRpc, "call()"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "UrmaMock.QueryPortStatus", "call(4,4)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, cqePoint, "1*call(0,9)"));
    std::vector<Optional<Buffer>> buffers;
    const auto first = requester->Get({ keys.front() }, 0, buffers);
    if (first.IsOk()) {
        CheckValue(buffers, data);
    }
    DS_ASSERT_OK(WaitForInjection(1, cqePoint, 1));
    DS_ASSERT_OK(WaitForInjection(0, "UrmaMock.QueryPortStatus", 1));
    GetHashRingRspPb ring;
    DS_ASSERT_OK(rpc_->InvokeGetHashRing(0, ring));
    ASSERT_TRUE(ring.has_hash_ring());
    const auto incarnation = ring.hash_ring().members().at(worker_.ToString()).id();
    UbHealthSummary health;
    DS_ASSERT_OK(WaitForPorts(incarnation, 4, health));
    uint64_t before = 0;
    DS_ASSERT_OK(cluster_->GetInjectActionExecuteCount(WORKER, 0, beforeRpc, before));
    EXPECT_GT(before, 0u);
    for (size_t i = 1; i < keys.size(); ++i) {
        buffers.clear();
        EXPECT_TRUE(requester->Get({ keys[i] }, 0, buffers).IsError());
    }
    uint64_t after = 0;
    DS_ASSERT_OK(cluster_->GetInjectActionExecuteCount(WORKER, 0, beforeRpc, after));
    EXPECT_EQ(after, before);
    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, 1, cqePoint));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "UrmaMock.QueryPortStatus", "call(4,3)"));
    DS_ASSERT_OK(WaitForPorts(incarnation, 3, health));
    DS_ASSERT_OK(GetAfterRandomizedRecovery(requester, keys.back(), buffers));
    CheckValue(buffers, data);
}
class UbClientWritebackPortHealthTest : public UbPortHealthRpcPropagationTest {
public:
    void SetUp() override
    {
        const auto *previous = getenv("DATASYSTEM_UB_GET_DATA_SIZE_BYTES");
        if (previous != nullptr) {
            previousInlineSize_ = previous;
        }
        ASSERT_EQ(setenv("DATASYSTEM_UB_GET_DATA_SIZE_BYTES", "4096", 1), 0);
        UbPortHealthRpcPropagationTest::SetUp();
    }

    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        UbPortHealthRpcPropagationTest::SetClusterSetupOptions(opts);
        opts.workerGflagParams += " -ipc_through_shared_memory=false";
    }

    void TearDown() override
    {
        (void)inject::Clear("UrmaMock.QueryPortStatus");
        UbPortHealthRpcPropagationTest::TearDown();
        if (previousInlineSize_.has_value()) {
            (void)setenv("DATASYSTEM_UB_GET_DATA_SIZE_BYTES", previousInlineSize_->c_str(), 1);
        } else {
            (void)unsetenv("DATASYSTEM_UB_GET_DATA_SIZE_BYTES");
        }
    }

    Status WaitForClientPorts(uint32_t bad)
    {
        std::shared_ptr<UbPortHealthMonitor> monitor;
        RETURN_IF_NOT_OK(GetLocalUbPortHealthMonitor(monitor));
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while (std::chrono::steady_clock::now() < deadline) {
            auto summary = monitor->GetSummary();
            if (summary.has_value() && HasKnownUbPortHealth(*summary)
                && !summary->verificationPending && summary->badPortCount == bad) {
                return Status::OK();
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        return Status(K_NOT_READY, "Client port-health monitor did not converge after Worker CQE9");
    }

private:
    std::optional<std::string> previousInlineSize_;
};

TEST_F(UbClientWritebackPortHealthTest, Cqe9ClosesClientGateUntilPartialRecovery)
{
    ConnectOptions options;
    InitConnectOpt(0, options);
    options.enableLocalCache = false;
    options.enableCrossNodeConnection = false;
    options.requestTimeoutMs = 3000;
    auto client = std::make_shared<ObjectClient>(options);
    DS_ASSERT_OK(client->Init());
    const auto key = "ub-client-e9-" + GetStringUuid();
    const std::string data(1024, 'c');
    DS_ASSERT_OK(client->Put(key, reinterpret_cast<const uint8_t *>(data.data()), data.size(), {}));
    DS_ASSERT_OK(WaitForClientPorts(0));
    const std::string rpcPoint = "worker.QueryAndGet.EncodeLocalHitFailure";
    const std::string cqePoint = "UrmaManager.CheckCompletionRecordStatus";
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, rpcPoint, "call()"));
    DS_ASSERT_OK(inject::Set("UrmaMock.QueryPortStatus", "call(4,4)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, cqePoint, "1*call(0,9)"));
    std::vector<Optional<Buffer>> buffers;
    (void)client->Get({ key }, 0, buffers);
    uint64_t faultCount = 0;
    DS_ASSERT_OK(cluster_->GetInjectActionExecuteCount(WORKER, 0, cqePoint, faultCount));
    ASSERT_GT(faultCount, 0u);
    DS_ASSERT_OK(WaitForClientPorts(4));
    uint64_t callsBefore = 0;
    DS_ASSERT_OK(cluster_->GetInjectActionExecuteCount(WORKER, 0, rpcPoint, callsBefore));
    EXPECT_GT(callsBefore, 0u);
    constexpr size_t blockedRequests = 3;
    for (size_t i = 0; i < blockedRequests; ++i) {
        buffers.clear();
        EXPECT_EQ(client->Get({ key }, 0, buffers).GetCode(), K_URMA_WORKER_UNAVAILABLE);
        EXPECT_EQ(client->Put(key, reinterpret_cast<const uint8_t *>(data.data()), data.size(), {}).GetCode(),
                  K_URMA_WORKER_UNAVAILABLE);
    }
    uint64_t callsAfter = 0;
    DS_ASSERT_OK(cluster_->GetInjectActionExecuteCount(WORKER, 0, rpcPoint, callsAfter));
    EXPECT_EQ(callsAfter, callsBefore);
    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, 0, cqePoint));
    DS_ASSERT_OK(inject::Set("UrmaMock.QueryPortStatus", "call(4,3)"));
    DS_ASSERT_OK(WaitForClientPorts(3));
    buffers.clear();
    DS_ASSERT_OK(client->Get({ key }, 0, buffers));
    ASSERT_EQ(buffers.size(), 1u);
    ASSERT_TRUE(buffers.front());
    ASSERT_EQ(buffers.front()->GetSize(), data.size());
    EXPECT_EQ(std::string(static_cast<const char *>(buffers.front()->MutableData()), data.size()), data);
}

#endif
}  // namespace datasystem::st
