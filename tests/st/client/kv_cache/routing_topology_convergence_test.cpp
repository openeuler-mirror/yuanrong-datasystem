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

/** Description: U6 topology convergence and leaving-write interception system test. */

#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "client/object_cache/oc_client_common.h"
#include "common_distributed_ext.h"
#include "datasystem/client/routing/routing.h"
#include "datasystem/client/transport/rpc/worker_rpc_client.h"
#include "datasystem/common/ak_sk/signature.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/kv_client.h"
#include "datasystem/protos/cluster_topology.pb.h"

DS_DECLARE_bool(use_brpc);

namespace datasystem::st {
namespace {
constexpr uint32_t WORKER_COUNT = 3;
constexpr uint32_t INITIAL_WORKER_INDEX = 0;
constexpr uint32_t LEAVING_WORKER_INDEX = 1;
constexpr int32_t CONTROL_RPC_TIMEOUT_MS = 3'000;
constexpr int64_t EXIT_GATE_TIMEOUT_MS = 10'000;
constexpr int64_t ROUTING_CONVERGENCE_TIMEOUT_MS = 15'000;
constexpr uint64_t VALUE_SIZE = 1'024;
constexpr size_t KEY_SEARCH_LIMIT = 100'000;
constexpr char CREATE_INJECT[] = "worker.Create.begin";
constexpr char PUBLISH_INJECT[] = "worker.PublishObjectWithLock.begin";
constexpr char ROUTED_KEY_PREFIX[] = "u6_topology_convergence_";
}  // namespace

class RoutingTopologyConvergenceTest : public OCClientCommon, public CommonDistributedExt {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numEtcd = 1;
        opts.numWorkers = WORKER_COUNT;
        opts.enableDistributedMaster = "true";
        opts.workerGflagParams =
            " -shared_memory_size_mb=512 -ipc_through_shared_memory=true -oc_shm_transfer_threshold_kb=1"
            " -arena_per_tenant=1"
            " -use_brpc=true -enable_urma=false -enable_leaving_intercept=true"
            " -enable_lossless_data_exit_mode=true";
    }

    void SetUp() override
    {
        previousUseBrpc_ = FLAGS_use_brpc;
        FLAGS_use_brpc = true;
        ExternalClusterTest::SetUp();
        externalCluster_ = dynamic_cast<ExternalCluster *>(cluster_.get());
        ASSERT_NE(externalCluster_, nullptr);
        CommonDistributedExt::InitTestEtcdInstance();

        InitRouting();
        InitLeavingWorkerClients();
    }

    void TearDown() override
    {
        gateClient_.reset();
        publishClient_.reset();
        multiPublishClient_.reset();
        createClient_.reset();
        multiCreateClient_.reset();
        if (routing_ != nullptr) {
            routing_->Shutdown();
            routing_.reset();
        }
        etcd_.reset();
        ExternalClusterTest::TearDown();
        FLAGS_use_brpc = previousUseBrpc_;
    }

protected:
    BaseCluster *GetCluster() override
    {
        return cluster_.get();
    }

    void InitRouting()
    {
        HostPort initialWorker;
        DS_ASSERT_OK(cluster_->GetWorkerAddr(INITIAL_WORKER_INDEX, initialWorker));
        ConnectOptions options;
        InitConnectOpt(INITIAL_WORKER_INDEX, options);
        auto signature = std::make_shared<Signature>(options.accessKey, options.secretKey);
        routing_ = std::make_shared<client::Routing>(MakeRpcConfig(), std::move(signature));
        DS_ASSERT_OK(routing_->Init("", initialWorker));
    }

    void InitLeavingWorkerClients()
    {
        DS_ASSERT_OK(cluster_->GetWorkerAddr(LEAVING_WORKER_INDEX, leavingWorker_));
        InitTestKVClient(LEAVING_WORKER_INDEX, gateClient_);
        InitTestKVClient(LEAVING_WORKER_INDEX, publishClient_);
        InitTestKVClient(LEAVING_WORKER_INDEX, multiPublishClient_);
        InitTestKVClient(LEAVING_WORKER_INDEX, createClient_);
        InitTestKVClient(LEAVING_WORKER_INDEX, multiCreateClient_);
    }

    BrpcChannelConfig MakeRpcConfig() const
    {
        BrpcChannelConfig config;
        config.timeout_ms = CONTROL_RPC_TIMEOUT_MS;
        config.connect_timeout_ms = CONTROL_RPC_TIMEOUT_MS;
        config.max_retry = 0;
        return config;
    }

    void VerifyVersionAwareHashRing()
    {
        HostPort initialWorker;
        DS_ASSERT_OK(cluster_->GetWorkerAddr(INITIAL_WORKER_INDEX, initialWorker));
        ConnectOptions options;
        InitConnectOpt(INITIAL_WORKER_INDEX, options);
        auto signature = std::make_shared<Signature>(options.accessKey, options.secretKey);
        client::WorkerRpcClient rpcClient(initialWorker, signature, MakeRpcConfig());
        DS_ASSERT_OK(rpcClient.Init());

        GetHashRingRspPb changed;
        DS_ASSERT_OK(rpcClient.InvokeGetHashRing(0, changed));
        ASSERT_TRUE(changed.hash_ring_changed());
        ASSERT_TRUE(changed.has_hash_ring());
        ASSERT_FALSE(changed.host_id_map().empty());

        GetHashRingRspPb unchanged;
        DS_ASSERT_OK(rpcClient.InvokeGetHashRing(changed.version(), unchanged));
        EXPECT_FALSE(unchanged.hash_ring_changed());
        EXPECT_FALSE(unchanged.has_hash_ring());
        EXPECT_TRUE(unchanged.host_id_map().empty());
        EXPECT_EQ(unchanged.version(), changed.version());
    }

    void WaitForLeavingGate()
    {
        Status lastRc(K_NOT_READY, "Worker has not entered the exit gate");
        const std::string gateValue(VALUE_SIZE, 'g');
        const auto deadline =
            std::chrono::steady_clock::now() + std::chrono::milliseconds(EXIT_GATE_TIMEOUT_MS);
        while (std::chrono::steady_clock::now() < deadline) {
            lastRc = gateClient_->Set("u6_leaving_gate_probe", gateValue);
            if (lastRc.GetCode() == K_SCALE_DOWN) {
                return;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
        FAIL() << "Worker did not expose K_SCALE_DOWN through the real exit gate: " << lastRc.ToString();
    }

    void PreparePublishRequests(std::shared_ptr<Buffer> &publishBuffer,
                                std::vector<std::shared_ptr<Buffer>> &multiPublishBuffers)
    {
        SetParam param;
        DS_ASSERT_OK(publishClient_->Create("u6_publish", VALUE_SIZE, param, publishBuffer));
        ASSERT_NE(publishBuffer, nullptr);
        const std::string publishValue(VALUE_SIZE, 'p');
        DS_ASSERT_OK(publishBuffer->MemoryCopy(publishValue.data(), publishValue.size()));

        const std::vector<std::string> keys{ "u6_multi_publish_0", "u6_multi_publish_1" };
        const std::vector<uint64_t> sizes(keys.size(), VALUE_SIZE);
        DS_ASSERT_OK(multiPublishClient_->MCreate(keys, sizes, param, multiPublishBuffers));
        ASSERT_EQ(multiPublishBuffers.size(), keys.size());
        for (size_t i = 0; i < multiPublishBuffers.size(); ++i) {
            const std::string value(VALUE_SIZE, static_cast<char>('a' + i));
            DS_ASSERT_OK(multiPublishBuffers[i]->MemoryCopy(value.data(), value.size()));
        }
    }

    void VerifyAllWriteEntrypointsReject(const std::shared_ptr<Buffer> &publishBuffer,
                                         const std::vector<std::shared_ptr<Buffer>> &multiPublishBuffers)
    {
        EXPECT_EQ(publishClient_->Set(publishBuffer).GetCode(), K_SCALE_DOWN);
        EXPECT_EQ(multiPublishClient_->MSet(multiPublishBuffers).GetCode(), K_SCALE_DOWN);

        SetParam param;
        std::shared_ptr<Buffer> createBuffer;
        EXPECT_EQ(createClient_->Create("u6_create_rejected", VALUE_SIZE, param, createBuffer).GetCode(),
                  K_SCALE_DOWN);

        std::vector<std::shared_ptr<Buffer>> multiCreateBuffers;
        EXPECT_EQ(multiCreateClient_
                      ->MCreate({ "u6_multi_create_0", "u6_multi_create_1" }, { VALUE_SIZE, VALUE_SIZE }, param,
                                multiCreateBuffers)
                      .GetCode(),
                  K_SCALE_DOWN);
    }

    void WaitForRoutingConvergence(const std::string &key, HostPort &selectedWorker)
    {
        Status lastRc(K_NOT_READY, "Routing has not converged");
        const auto deadline =
            std::chrono::steady_clock::now() + std::chrono::milliseconds(ROUTING_CONVERGENCE_TIMEOUT_MS);
        while (std::chrono::steady_clock::now() < deadline) {
            lastRc = routing_->SelectWorker(key, client::DataPlacementPolicy::PREFERRED_META_OWNER, selectedWorker);
            if (lastRc.IsOk() && selectedWorker != leavingWorker_) {
                return;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        FAIL() << "Routing did not converge away from leaving worker " << leavingWorker_ << ": " << lastRc.ToString();
    }

    std::string FindRoutingKeyForWorker(const HostPort &targetWorker)
    {
        Status lastRc(K_NOT_READY, "Routing key search has not started");
        for (size_t i = 0; i < KEY_SEARCH_LIMIT; ++i) {
            std::string key = ROUTED_KEY_PREFIX + std::to_string(i);
            HostPort selectedWorker;
            lastRc = routing_->SelectWorker(key, client::DataPlacementPolicy::PREFERRED_META_OWNER, selectedWorker);
            if (lastRc.IsOk() && selectedWorker == targetWorker) {
                VLOG(1) << "Found routing key for " << targetWorker << " after " << i + 1 << " attempts";
                return key;
            }
        }
        ADD_FAILURE() << "Unable to find a routing key for " << targetWorker << " after scanning " << KEY_SEARCH_LIMIT
                      << " keys: " << lastRc.ToString();
        return {};
    }

    void InitClientForWorker(const HostPort &worker, std::shared_ptr<KVClient> &client)
    {
        ConnectOptions options;
        InitConnectOpt(INITIAL_WORKER_INDEX, options);
        options.host = worker.Host();
        options.port = worker.Port();
        options.enableLocalCache = true;
        client = std::make_shared<KVClient>(options);
        DS_ASSERT_OK(client->Init());
    }

    void GetWriteProcessorCounts(uint64_t &createCount, uint64_t &publishCount)
    {
        DS_ASSERT_OK(externalCluster_->GetInjectActionExecuteCount(WORKER, LEAVING_WORKER_INDEX, CREATE_INJECT,
                                                                  createCount));
        DS_ASSERT_OK(externalCluster_->GetInjectActionExecuteCount(WORKER, LEAVING_WORKER_INDEX, PUBLISH_INJECT,
                                                                  publishCount));
    }

    ExternalCluster *externalCluster_ = nullptr;
    HostPort leavingWorker_;
    std::shared_ptr<client::Routing> routing_;
    std::shared_ptr<KVClient> gateClient_;
    std::shared_ptr<KVClient> publishClient_;
    std::shared_ptr<KVClient> multiPublishClient_;
    std::shared_ptr<KVClient> createClient_;
    std::shared_ptr<KVClient> multiCreateClient_;
    bool previousUseBrpc_ = false;
};

TEST_F(RoutingTopologyConvergenceTest, U6ScaleDownRejectsWritesAndConvergesRouting)
{
    VerifyVersionAwareHashRing();

    std::shared_ptr<Buffer> publishBuffer;
    std::vector<std::shared_ptr<Buffer>> multiPublishBuffers;
    PreparePublishRequests(publishBuffer, multiPublishBuffers);
    DS_ASSERT_OK(externalCluster_->SetInjectAction(WORKER, LEAVING_WORKER_INDEX, CREATE_INJECT, "call()"));
    DS_ASSERT_OK(externalCluster_->SetInjectAction(WORKER, LEAVING_WORKER_INDEX, PUBLISH_INJECT, "call()"));

    const std::string routedKey = FindRoutingKeyForWorker(leavingWorker_);
    ASSERT_FALSE(routedKey.empty());
    const std::string routedValue(VALUE_SIZE, 'r');
    HostPort selectedWorker;
    DS_ASSERT_OK(routing_->SelectWorker(routedKey, client::DataPlacementPolicy::PREFERRED_META_OWNER, selectedWorker));
    ASSERT_EQ(selectedWorker, leavingWorker_);
    ClusterTopologyPb initialTopology;
    GetClusterTopologyPb(initialTopology);

    VoluntaryScaleDownInject(LEAVING_WORKER_INDEX);
    WaitForLeavingGate();
    uint64_t createCountBefore = 0;
    uint64_t publishCountBefore = 0;
    GetWriteProcessorCounts(createCountBefore, publishCountBefore);
    VerifyAllWriteEntrypointsReject(publishBuffer, multiPublishBuffers);
    uint64_t createCountAfter = 0;
    uint64_t publishCountAfter = 0;
    GetWriteProcessorCounts(createCountAfter, publishCountAfter);
    EXPECT_EQ(createCountAfter, createCountBefore);
    EXPECT_EQ(publishCountAfter, publishCountBefore);

    publishBuffer.reset();
    multiPublishBuffers.clear();
    gateClient_.reset();
    publishClient_.reset();
    multiPublishClient_.reset();
    createClient_.reset();
    multiCreateClient_.reset();
    WaitForRoutingConvergence(routedKey, selectedWorker);
    std::shared_ptr<KVClient> remainingWorkerClient;
    InitClientForWorker(selectedWorker, remainingWorkerClient);
    DS_ASSERT_OK(remainingWorkerClient->Set(routedKey, routedValue));
    WaitAllMembersJoinClusterTopology(WORKER_COUNT - 1, 30);

    ClusterTopologyPb finalTopology;
    GetClusterTopologyPb(finalTopology);
    EXPECT_GT(finalTopology.version(), initialTopology.version());
    EXPECT_EQ(finalTopology.members().count(leavingWorker_.ToString()), 0u);

    std::string actual;
    DS_ASSERT_OK(remainingWorkerClient->Get(routedKey, actual));
    EXPECT_EQ(actual, routedValue);
}

}  // namespace datasystem::st
