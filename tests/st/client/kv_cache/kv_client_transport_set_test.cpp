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

/** Description: Tests the routed KVClient Set transaction. */

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "client/object_cache/oc_client_common.h"
#include "datasystem/client/routing/routing.h"
#include "datasystem/common/ak_sk/ak_sk_manager.h"
#include "datasystem/common/ak_sk/signature.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/kvstore/etcd/etcd_store.h"
#include "datasystem/common/log/access_recorder.h"
#include "datasystem/common/rpc/rpc_message.h"
#include "datasystem/common/rpc/rpc_stub_cache_mgr.h"
#include "datasystem/common/util/hash_algorithm.h"
#include "datasystem/common/util/request_context.h"
#include "datasystem/kv_client.h"
#include "datasystem/protos/cluster_topology.pb.h"
#include "datasystem/worker/object_cache/worker_master_oc_api.h"

DS_DECLARE_bool(use_brpc);
DS_DECLARE_string(sdk_data_placement_policy);

namespace datasystem {
namespace st {
namespace {
constexpr uint32_t WORKER_NUM = 2;
constexpr uint32_t READER_WORKER_INDEX = 0;
constexpr uint32_t ROUTED_CLIENT_WORKER_INDEX = 1;
constexpr size_t VALUE_SIZE = 128 * 1024;
constexpr size_t KEY_SEARCH_LIMIT = 100'000;
constexpr char SKIP_WARMUP_INJECT[] = "ObjectClientImpl.ClientWorkerWarmup.skip";
constexpr char PUBLISH_INJECT[] = "WorkerRpcClient.InvokeSet.beforeRpc";
constexpr char HOST_ID_ENV_NAME[] = "routing_transport_set_host_id";
constexpr char HOST_ID_VALUE[] = "routing-transport-set-host";

const char *ExpectedTransport()
{
#ifdef USE_URMA
    return "UB";
#else
    // Same-host routed Set/Get travels over the SHM transporter (TransportHint::SHM_CANDIDATE)
    // once the routing snapshot partitions workers by hostId and the SDK's own hostId resolves.
    return "SHM";
#endif
}
}  // namespace

class KVClientTransportSetTest : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        FLAGS_v = 1;
        opts.numEtcd = 1;
        opts.numWorkers = WORKER_NUM;
        opts.enableDistributedMaster = "true";
        opts.workerGflagParams =
            " -shared_memory_size_mb=512 -ipc_through_shared_memory=false -arena_per_tenant=1 -use_brpc=true";
        opts.workerGflagParams += " -host_id_env_name=" + std::string(HOST_ID_ENV_NAME);
#ifdef USE_URMA
        opts.workerGflagParams += " -enable_urma=true -enable_transport_fallback=false";
#else
        opts.workerGflagParams += " -enable_urma=false";
#endif
    }

    void SetUp() override
    {
        previousUseBrpc_ = FLAGS_use_brpc;
        previousPlacementPolicy_ = FLAGS_sdk_data_placement_policy;
        FLAGS_use_brpc = true;
        FLAGS_sdk_data_placement_policy = "PREFERRED_META_OWNER";
        ASSERT_EQ(setenv(HOST_ID_ENV_NAME, HOST_ID_VALUE, 1), 0);
        DS_ASSERT_OK(inject::Set(SKIP_WARMUP_INJECT, "call()"));
        ExternalClusterTest::SetUp();

        etcd_ = InitTestEtcdInstance();
        ASSERT_NE(etcd_, nullptr);
        InitRoutedClient();
        InitLocalClient();
        InitTestKVClient(READER_WORKER_INDEX, readerClient_);
        InitMasterApis();
    }

    void TearDown() override
    {
        masterApis_.clear();
        akSkManager_.reset();
        routedClient_.reset();
        localClient_.reset();
        readerClient_.reset();
        etcd_.reset();
        (void)inject::Clear(PUBLISH_INJECT);
        (void)inject::Clear(SKIP_WARMUP_INJECT);
        ExternalClusterTest::TearDown();
        (void)unsetenv(HOST_ID_ENV_NAME);
        FLAGS_use_brpc = previousUseBrpc_;
        FLAGS_sdk_data_placement_policy = previousPlacementPolicy_;
    }

protected:
    void InitRoutedClient()
    {
        ConnectOptions options;
        InitConnectOpt(ROUTED_CLIENT_WORKER_INDEX, options);
        options.enableLocalCache = false;
        routedClient_ = std::make_shared<KVClient>(options);
        DS_ASSERT_OK(routedClient_->Init());
    }

    void ReinitRoutedClient(const std::string &policy)
    {
        routedClient_.reset();
        FLAGS_sdk_data_placement_policy = policy;
        InitRoutedClient();
    }

    void InitLocalClient()
    {
        ConnectOptions options;
        InitConnectOpt(ROUTED_CLIENT_WORKER_INDEX, options);
        options.enableLocalCache = true;
        localClient_ = std::make_shared<KVClient>(options);
        DS_ASSERT_OK(localClient_->Init());
    }

    void InitMasterApis()
    {
        DS_ASSERT_OK(RpcStubCacheMgr::Instance().Init(100));
        akSkManager_ = std::make_shared<AkSkManager>();
        DS_ASSERT_OK(akSkManager_->SetClientAkSk("QTWAOYTTINDUT2QVKYUC",
                                                "MFyfvK41ba2giqM7**********KGpownRZlmVmHc"));

        HostPort localAddress;
        DS_ASSERT_OK(cluster_->GetWorkerAddr(READER_WORKER_INDEX, localAddress));
        queryAddress_ = localAddress.ToString();
        masterApis_.reserve(WORKER_NUM);
        for (uint32_t i = 0; i < WORKER_NUM; ++i) {
            HostPort workerAddress;
            DS_ASSERT_OK(cluster_->GetWorkerAddr(i, workerAddress));
            auto api =
                std::make_unique<worker::WorkerRemoteMasterOCApi>(workerAddress, localAddress, akSkManager_);
            DS_ASSERT_OK(api->Init());
            masterApis_.emplace_back(std::move(api));
        }
    }

    Status QueryPrimaryWorker(const std::string &key, HostPort &primaryWorker)
    {
        Status lastRc(K_NOT_FOUND, "Object metadata does not exist");
        bool querySucceeded = false;
        for (const auto &api : masterApis_) {
            master::QueryMetaReqPb request;
            master::QueryMetaRspPb response;
            std::vector<RpcMessage> payloads;
            request.add_ids(key);
            request.set_address(queryAddress_);
            GetRequestContext()->reqTimeoutDuration.Reset();
            Status rc = api->QueryMeta(request, 0, response, payloads);
            if (rc.IsError()) {
                if (!querySucceeded) {
                    lastRc = rc;
                }
                continue;
            }
            querySucceeded = true;
            for (const auto &queryMeta : response.query_metas()) {
                if (queryMeta.meta().object_key() == key) {
                    return primaryWorker.ParseString(queryMeta.meta().primary_address());
                }
            }
        }
        if (!querySucceeded) {
            return lastRc;
        }
        lastRc = Status(K_NOT_FOUND, "Object metadata does not exist");
        return lastRc;
    }

    Status FindRouteKeyToWorker(uint32_t workerIndex, const std::string &prefix, std::string &key)
    {
        RETURN_RUNTIME_ERROR_IF_NULL(etcd_);
        std::string value;
        RETURN_IF_NOT_OK(etcd_->Get(GetTopologyTableName(), "", value));
        ClusterTopologyPb ring;
        CHECK_FAIL_RETURN_STATUS(ring.ParseFromString(value), K_RUNTIME_ERROR, "Parse hash ring failed");
        HostPort targetWorker;
        RETURN_IF_NOT_OK(cluster_->GetWorkerAddr(workerIndex, targetWorker));
        CHECK_FAIL_RETURN_STATUS(ring.members().find(targetWorker.ToString()) != ring.members().end(), K_NOT_FOUND,
                                 "Target worker is absent from hash ring");

        std::map<uint32_t, std::string> tokenWorkers;
        for (const auto &worker : ring.members()) {
            for (const auto token : worker.second.tokens()) {
                tokenWorkers.emplace(token, worker.first);
            }
        }
        CHECK_FAIL_RETURN_STATUS(!tokenWorkers.empty(), K_NOT_FOUND, "Hash ring has no worker tokens");
        for (size_t i = 0; i < KEY_SEARCH_LIMIT; ++i) {
            std::string candidate = prefix + std::to_string(i);
            auto owner = tokenWorkers.lower_bound(MurmurHash3_32(candidate));
            if (owner == tokenWorkers.end()) {
                owner = tokenWorkers.begin();
            }
            if (owner->second == targetWorker.ToString()) {
                key = std::move(candidate);
                return Status::OK();
            }
        }
        return Status(K_NOT_FOUND, "Unable to find a key for the target worker");
    }

    Status FindSameNodeDivergentRouteKey(const std::string &prefix, std::string &key, HostPort &metaOwner,
                                         HostPort &preferredWorker)
    {
        RETURN_RUNTIME_ERROR_IF_NULL(etcd_);
        std::string value;
        RETURN_IF_NOT_OK(etcd_->Get(GetTopologyTableName(), "", value));
        ClusterTopologyPb ring;
        CHECK_FAIL_RETURN_STATUS(ring.ParseFromString(value), K_RUNTIME_ERROR, "Parse hash ring failed");
        std::map<uint32_t, std::string> tokenWorkers;
        std::vector<HostPort> sameNodeWorkers;
        for (const auto &worker : ring.members()) {
            if (worker.second.state() != MembershipPb::ACTIVE) {
                continue;
            }
            HostPort address;
            RETURN_IF_NOT_OK(address.ParseString(worker.first));
            sameNodeWorkers.emplace_back(std::move(address));
            for (const auto token : worker.second.tokens()) {
                tokenWorkers.emplace(token, worker.first);
            }
        }
        CHECK_FAIL_RETURN_STATUS(sameNodeWorkers.size() == WORKER_NUM, K_NOT_READY,
                                 "Expected all external-cluster workers on the same host");
        CHECK_FAIL_RETURN_STATUS(!tokenWorkers.empty(), K_NOT_FOUND, "Hash ring has no worker tokens");
        std::sort(sameNodeWorkers.begin(), sameNodeWorkers.end());
        for (size_t i = 0; i < KEY_SEARCH_LIMIT; ++i) {
            std::string candidate = prefix + std::to_string(i);
            const uint32_t keyHash = MurmurHash3_32(candidate);
            auto owner = tokenWorkers.lower_bound(keyHash);
            if (owner == tokenWorkers.end()) {
                owner = tokenWorkers.begin();
            }
            HostPort candidateOwner;
            RETURN_IF_NOT_OK(candidateOwner.ParseString(owner->second));
            const HostPort &candidatePreferred = sameNodeWorkers[keyHash % sameNodeWorkers.size()];
            if (candidatePreferred != candidateOwner) {
                key = std::move(candidate);
                metaOwner = std::move(candidateOwner);
                preferredWorker = candidatePreferred;
                return Status::OK();
            }
        }
        return Status(K_NOT_FOUND, "Unable to find a key whose same-node worker differs from its metadata owner");
    }

    Status FindWorkerIndex(const HostPort &worker, uint32_t &workerIndex)
    {
        for (uint32_t i = 0; i < WORKER_NUM; ++i) {
            HostPort candidate;
            RETURN_IF_NOT_OK(cluster_->GetWorkerAddr(i, candidate));
            if (candidate == worker) {
                workerIndex = i;
                return Status::OK();
            }
        }
        return Status(K_NOT_FOUND, "Worker address is absent from the external cluster");
    }

    void AssertValue(const std::string &key, const std::string &expected)
    {
        std::string actual;
        DS_ASSERT_OK(readerClient_->Get(key, actual));
        ASSERT_EQ(actual, expected);
    }

    void AssertPrimaryWorker(const std::string &key, uint32_t workerIndex)
    {
        HostPort primaryWorker;
        DS_ASSERT_OK(QueryPrimaryWorker(key, primaryWorker));
        HostPort expectedWorker;
        DS_ASSERT_OK(cluster_->GetWorkerAddr(workerIndex, expectedWorker));
        ASSERT_EQ(primaryWorker, expectedWorker);
    }

    std::shared_ptr<KVClient> routedClient_;
    std::shared_ptr<KVClient> localClient_;
    std::shared_ptr<KVClient> readerClient_;
    std::shared_ptr<AkSkManager> akSkManager_;
    std::unique_ptr<EtcdStore> etcd_;
    std::vector<std::unique_ptr<worker::WorkerRemoteMasterOCApi>> masterApis_;
    std::string queryAddress_;
    bool previousUseBrpc_ = false;
    std::string previousPlacementPolicy_;
};

TEST_F(KVClientTransportSetTest, RoutedSetPublishesDataAndMetadata)
{
    std::string key;
    DS_ASSERT_OK(FindRouteKeyToWorker(READER_WORKER_INDEX, "transport_set_normal_", key));
    const std::string value(VALUE_SIZE, 'n');

    DS_ASSERT_OK(routedClient_->Set(key, value));

    ASSERT_EQ(AccessTransportTracker::ToString(), ExpectedTransport());
    AssertValue(key, value);
    HostPort primaryWorker;
    DS_ASSERT_OK(QueryPrimaryWorker(key, primaryWorker));
    HostPort expectedWorker;
    DS_ASSERT_OK(cluster_->GetWorkerAddr(READER_WORKER_INDEX, expectedWorker));
    ASSERT_EQ(primaryWorker, expectedWorker);
}

TEST_F(KVClientTransportSetTest, U1U2DirectWriteReadAtMetadataOwner)
{
    std::string key;
    DS_ASSERT_OK(FindRouteKeyToWorker(READER_WORKER_INDEX, "routing_u1_u2_", key));
    const std::string value(VALUE_SIZE, 'u');

    DS_ASSERT_OK(routedClient_->Set(key, value));

    ASSERT_EQ(AccessTransportTracker::ToString(), ExpectedTransport());
    std::string actual;
    DS_ASSERT_OK(routedClient_->Get(key, actual));
    ASSERT_EQ(actual, value);
    HostPort primaryWorker;
    DS_ASSERT_OK(QueryPrimaryWorker(key, primaryWorker));
    HostPort expectedWorker;
    DS_ASSERT_OK(cluster_->GetWorkerAddr(READER_WORKER_INDEX, expectedWorker));
    ASSERT_EQ(primaryWorker, expectedWorker);
}

TEST_F(KVClientTransportSetTest, DefaultPolicyRoutesSetAndMSetToSameNodeWorkers)
{
    ReinitRoutedClient("PREFERRED_SAME_NODE");
    std::string setKey;
    HostPort setMetaOwner;
    HostPort setPreferredWorker;
    DS_ASSERT_OK(FindSameNodeDivergentRouteKey("routing_default_set_", setKey, setMetaOwner, setPreferredWorker));
    ASSERT_NE(setMetaOwner, setPreferredWorker);
    uint32_t setMetaOwnerIndex = 0;
    uint32_t setPreferredWorkerIndex = 0;
    DS_ASSERT_OK(FindWorkerIndex(setMetaOwner, setMetaOwnerIndex));
    DS_ASSERT_OK(FindWorkerIndex(setPreferredWorker, setPreferredWorkerIndex));

    const std::string setValue(VALUE_SIZE, 'd');
    DS_ASSERT_OK(routedClient_->Set(setKey, setValue));
    AssertValue(setKey, setValue);
    AssertPrimaryWorker(setKey, setPreferredWorkerIndex);

    std::string msetKey;
    HostPort msetMetaOwner;
    HostPort msetPreferredWorker;
    DS_ASSERT_OK(
        FindSameNodeDivergentRouteKey("routing_default_mset_", msetKey, msetMetaOwner, msetPreferredWorker));
    const std::string msetValue(VALUE_SIZE, 'm');
    std::vector<std::string> failedKeys;
    DS_ASSERT_OK(routedClient_->MSet({ msetKey }, { StringView(msetValue) }, failedKeys));
    ASSERT_TRUE(failedKeys.empty());
    AssertValue(msetKey, msetValue);
    uint32_t msetPreferredWorkerIndex = 0;
    DS_ASSERT_OK(FindWorkerIndex(msetPreferredWorker, msetPreferredWorkerIndex));
    AssertPrimaryWorker(msetKey, msetPreferredWorkerIndex);
}

TEST_F(KVClientTransportSetTest, RequiredSameNodeRejectsBeforeBusinessRpc)
{
    HostPort initialWorker;
    DS_ASSERT_OK(cluster_->GetWorkerAddr(ROUTED_CLIENT_WORKER_INDEX, initialWorker));
    BrpcChannelConfig channelConfig;
    channelConfig.timeout_ms = 3'000;
    channelConfig.connect_timeout_ms = 3'000;
    channelConfig.max_retry = 0;
    channelConfig.enable_circuit_breaker = false;
    ConnectOptions options;
    InitConnectOpt(ROUTED_CLIENT_WORKER_INDEX, options);
    auto signature = std::make_shared<Signature>(options.accessKey, options.secretKey);
    client::Routing routing(channelConfig, std::move(signature));
    DS_ASSERT_OK(routing.Init("routing-st-nonexistent-host", initialWorker));

    HostPort selectedWorker;
    EXPECT_EQ(routing
                  .SelectWorker("routing_required_same_node", client::DataPlacementPolicy::REQUIRED_SAME_NODE,
                                selectedWorker)
                  .GetCode(),
              K_NO_AVAILABLE_WORKER);
    routing.Shutdown();
}

TEST_F(KVClientTransportSetTest, LocalCacheEnabledSetUsesConnectedWorker)
{
    std::string key;
    DS_ASSERT_OK(FindRouteKeyToWorker(READER_WORKER_INDEX, "transport_set_local_", key));
    const std::string value(VALUE_SIZE, 'l');

    DS_ASSERT_OK(localClient_->Set(key, value));

    AssertValue(key, value);
    HostPort primaryWorker;
    DS_ASSERT_OK(QueryPrimaryWorker(key, primaryWorker));
    HostPort expectedWorker;
    DS_ASSERT_OK(cluster_->GetWorkerAddr(ROUTED_CLIENT_WORKER_INDEX, expectedWorker));
    ASSERT_EQ(primaryWorker, expectedWorker);
}

TEST_F(KVClientTransportSetTest, RoutedMSetGroupsObjectsByMetadataOwner)
{
    std::string worker0Key;
    std::string worker1Key;
    DS_ASSERT_OK(FindRouteKeyToWorker(READER_WORKER_INDEX, "transport_mset_worker0_", worker0Key));
    DS_ASSERT_OK(FindRouteKeyToWorker(ROUTED_CLIENT_WORKER_INDEX, "transport_mset_worker1_", worker1Key));
    const std::vector<std::string> keys{ worker0Key, worker1Key };
    const std::vector<std::string> values{ std::string(VALUE_SIZE, 'a'), std::string(VALUE_SIZE, 'b') };
    const std::vector<StringView> valueViews{ values[0], values[1] };
    std::vector<std::string> failedKeys;

    DS_ASSERT_OK(routedClient_->MSet(keys, valueViews, failedKeys));

    ASSERT_TRUE(failedKeys.empty());
    AssertValue(keys[0], values[0]);
    AssertValue(keys[1], values[1]);
    AssertPrimaryWorker(keys[0], READER_WORKER_INDEX);
    AssertPrimaryWorker(keys[1], ROUTED_CLIENT_WORKER_INDEX);
}

TEST_F(KVClientTransportSetTest, LocalCacheEnabledMSetUsesConnectedWorker)
{
    std::string firstKey;
    std::string secondKey;
    DS_ASSERT_OK(FindRouteKeyToWorker(READER_WORKER_INDEX, "transport_mset_local0_", firstKey));
    DS_ASSERT_OK(FindRouteKeyToWorker(READER_WORKER_INDEX, "transport_mset_local1_", secondKey));
    const std::vector<std::string> keys{ firstKey, secondKey };
    const std::vector<std::string> values{ std::string(VALUE_SIZE, 'c'), std::string(VALUE_SIZE, 'd') };
    const std::vector<StringView> valueViews{ values[0], values[1] };
    std::vector<std::string> failedKeys;

    DS_ASSERT_OK(localClient_->MSet(keys, valueViews, failedKeys));

    ASSERT_TRUE(failedKeys.empty());
    AssertValue(keys[0], values[0]);
    AssertValue(keys[1], values[1]);
    AssertPrimaryWorker(keys[0], ROUTED_CLIENT_WORKER_INDEX);
    AssertPrimaryWorker(keys[1], ROUTED_CLIENT_WORKER_INDEX);
}

TEST_F(KVClientTransportSetTest, ScaleDownPublishReroutesWholeTransaction)
{
    std::string key;
    DS_ASSERT_OK(FindRouteKeyToWorker(READER_WORKER_INDEX, "transport_set_scale_down_", key));
    HostPort firstWorker;
    DS_ASSERT_OK(cluster_->GetWorkerAddr(READER_WORKER_INDEX, firstWorker));
    DS_ASSERT_OK(inject::Set(PUBLISH_INJECT, "1*return(K_SCALE_DOWN)"));
    const std::string value(VALUE_SIZE, 's');

    DS_ASSERT_OK(routedClient_->Set(key, value));

    HostPort reroutedWorker;
    DS_ASSERT_OK(QueryPrimaryWorker(key, reroutedWorker));
    ASSERT_NE(reroutedWorker, firstWorker);
    AssertValue(key, value);
}

TEST_F(KVClientTransportSetTest, AmbiguousPublishFailureIsNotReplayedOnAnotherWorker)
{
    std::string key;
    DS_ASSERT_OK(FindRouteKeyToWorker(READER_WORKER_INDEX, "transport_set_publish_failure_", key));
    HostPort firstWorker;
    DS_ASSERT_OK(cluster_->GetWorkerAddr(READER_WORKER_INDEX, firstWorker));
    DS_ASSERT_OK(inject::Set(PUBLISH_INJECT, "2*return(K_RPC_UNAVAILABLE)"));
    const std::string value(VALUE_SIZE, 'f');

    Status rc = routedClient_->Set(key, value);

    ASSERT_EQ(rc.GetCode(), K_RPC_UNAVAILABLE) << rc.ToString();
    HostPort unexpectedPrimary;
    ASSERT_EQ(QueryPrimaryWorker(key, unexpectedPrimary).GetCode(), K_NOT_FOUND);

    DS_ASSERT_OK(routedClient_->Set(key, value));
    HostPort retryWorker;
    DS_ASSERT_OK(QueryPrimaryWorker(key, retryWorker));
    ASSERT_NE(retryWorker, firstWorker);
    AssertValue(key, value);
}
}  // namespace st
}  // namespace datasystem
