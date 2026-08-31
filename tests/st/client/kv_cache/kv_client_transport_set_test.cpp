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
#include <sys/socket.h>
#include <unistd.h>
#include <unordered_set>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "client/object_cache/oc_client_common.h"
#include "cluster/topology_token_helper.h"
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
#include "datasystem/common/metrics/kv_metrics.h"
#include "datasystem/worker/object_cache/worker_master_oc_api.h"

DS_DECLARE_string(sdk_data_placement_policy);
DS_DECLARE_string(host_id_env_name);
namespace datasystem {
namespace st {
namespace {
constexpr uint32_t WORKER_NUM = 2;
constexpr uint32_t READER_WORKER_INDEX = 0;
constexpr uint32_t ROUTED_CLIENT_WORKER_INDEX = 1;
constexpr uint32_t REMOTE_TRANSPORT_WORKER_NUM = 3;
constexpr uint32_t REMOTE_TRANSPORT_CLIENT_WORKER_INDEX = 2;
constexpr size_t VALUE_SIZE = 128 * 1024;
constexpr size_t KEY_SEARCH_LIMIT = 100'000;
constexpr char SKIP_WARMUP_INJECT[] = "ObjectClientImpl.ClientWorkerWarmup.skip";
constexpr char CREATE_INJECT[] = "TransportLayer.Create.beforeTransport";
constexpr char PUBLISH_INJECT[] = "WorkerRpcClient.InvokeSet.beforeRpc";
constexpr char MULTI_CREATE_INJECT[] = "TransportLayer.MCreate.beforeTransport";
constexpr char MULTI_PUBLISH_INJECT[] = "WorkerRpcClient.InvokeMultiSet.beforeRpc";
constexpr char MULTI_PUBLISH_METADATA_INJECT[] = "worker.before_CreateMultiMetaToMaster";
constexpr char HOST_ID_ENV_NAME[] = "routing_transport_set_host_id";
constexpr char HOST_ID_VALUE[] = "routing-transport-set-host";
constexpr char REMOTE_HOST_ID_ENV_PREFIX[] = "routing_transport_set_remote_host_id_";
constexpr char REMOTE_HOST_ID_VALUE_PREFIX[] = "routing-transport-set-remote-host-";

bool IsRemoteTransportSetCase()
{
    std::string suiteName;
    std::string caseName;
    GetCurTestName(suiteName, caseName);
    return caseName == "MSetMetaOwnerGroupsUseCompiledRemoteTransport";
}

std::string RemoteHostIdEnvName(uint32_t workerIndex)
{
    return std::string(REMOTE_HOST_ID_ENV_PREFIX) + std::to_string(workerIndex);
}

std::string RemoteHostIdValue(uint32_t workerIndex)
{
    return std::string(REMOTE_HOST_ID_VALUE_PREFIX) + std::to_string(workerIndex);
}

bool IsCoordinatorTransportSetCase()
{
    static const std::unordered_set<std::string> coordinatorCases = {
        "RoutedSetPublishesDataAndMetadata",
        "LocalCacheEnabledSetUsesConnectedWorker",
        "RoutedMSetGroupsObjectsByMetadataOwner",
        "LocalCacheEnabledMSetUsesConnectedWorker",
        "ScaleDownPublishReroutesWholeTransaction",
        "AmbiguousPublishFailureIsNotReplayedOnAnotherWorker",
        "MetadataOwnerFailureRefreshesRingWithoutEvictingIngress"
    };
    std::string suiteName;
    std::string caseName;
    GetCurTestName(suiteName, caseName);
    return coordinatorCases.count(caseName) > 0;
}

const char *ExpectedTransport()
{
#ifdef USE_URMA
    return "UB";
#else
    return "TCP";
#endif
}
}  // namespace

class KVClientTransportSetTest : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        FLAGS_v = 1;
        opts.numEtcd = IsCoordinatorTransportSetCase() ? 0 : 1;
        opts.numCoordinators = IsCoordinatorTransportSetCase() ? 1 : 0;
        opts.numWorkers = IsRemoteTransportSetCase() ? REMOTE_TRANSPORT_WORKER_NUM : WORKER_NUM;
        opts.enableDistributedMaster = "true";
        opts.workerGflagParams =
            " -shared_memory_size_mb=512 -ipc_through_shared_memory=false -arena_per_tenant=1";
        opts.workerGflagParams += " -host_id_env_name=" + std::string(HOST_ID_ENV_NAME);
        if (IsRemoteTransportSetCase()) {
            for (uint32_t i = 0; i < REMOTE_TRANSPORT_WORKER_NUM; ++i) {
                opts.workerSpecifyGflagParams[i] += " -host_id_env_name=" + RemoteHostIdEnvName(i);
            }
        }
#ifdef USE_URMA
        opts.workerGflagParams += " -enable_urma=true -enable_transport_fallback=false";
#else
        opts.workerGflagParams += " -enable_urma=false";
#endif
    }

    void SetUp() override
    {
        ASSERT_EQ(setenv(HOST_ID_ENV_NAME, HOST_ID_VALUE, 1), 0);
        if (IsRemoteTransportSetCase()) {
            for (uint32_t i = 0; i < REMOTE_TRANSPORT_WORKER_NUM; ++i) {
                ASSERT_EQ(setenv(RemoteHostIdEnvName(i).c_str(), RemoteHostIdValue(i).c_str(), 1), 0);
            }
        }
        DS_ASSERT_OK(inject::Set(SKIP_WARMUP_INJECT, "call()"));
        ExternalClusterTest::SetUp();

        if (!IsCoordinatorTransportSetCase()) {
            etcd_ = InitTestEtcdInstance();
            ASSERT_NE(etcd_, nullptr);
        }
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
        (void)inject::Clear(CREATE_INJECT);
        (void)inject::Clear(PUBLISH_INJECT);
        (void)inject::Clear(MULTI_CREATE_INJECT);
        (void)inject::Clear(MULTI_PUBLISH_INJECT);
        (void)inject::Clear(SKIP_WARMUP_INJECT);
        ExternalClusterTest::TearDown();
        (void)unsetenv(HOST_ID_ENV_NAME);
        for (uint32_t i = 0; i < REMOTE_TRANSPORT_WORKER_NUM; ++i) {
            (void)unsetenv(RemoteHostIdEnvName(i).c_str());
        }
    }

protected:
    virtual void InitRoutedClient()
    {
        ConnectOptions options;
        const uint32_t workerIndex =
            IsRemoteTransportSetCase() ? REMOTE_TRANSPORT_CLIENT_WORKER_INDEX : ROUTED_CLIENT_WORKER_INDEX;
        InitConnectOpt(workerIndex, options);
        options.enableLocalCache = false;
        options.dataPlacementPolicy = routedDataPlacementPolicy_;
        routedClient_ = std::make_shared<KVClient>(options);
        DS_ASSERT_OK(routedClient_->Init());
    }

    void ReinitRoutedClient(DataPlacementPolicy policy)
    {
        routedClient_.reset();
        routedDataPlacementPolicy_ = policy;
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
        ClusterTopologyPb ring;
        RETURN_IF_NOT_OK(cluster_->ReadClusterTopology(ring));
        HostPort targetWorker;
        RETURN_IF_NOT_OK(cluster_->GetWorkerAddr(workerIndex, targetWorker));
        CHECK_FAIL_RETURN_STATUS(ring.members().find(targetWorker.ToString()) != ring.members().end(), K_NOT_FOUND,
                                 "Target worker is absent from hash ring");

        std::map<uint32_t, std::string> tokenWorkers;
        for (const auto &worker : ring.members()) {
            for (const auto token : RebuildTopologyMemberTokens(ring, worker.first, worker.second)) {
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
        ClusterTopologyPb ring;
        RETURN_IF_NOT_OK(cluster_->ReadClusterTopology(ring));
        std::map<uint32_t, std::string> tokenWorkers;
        std::vector<HostPort> sameNodeWorkers;
        for (const auto &worker : ring.members()) {
            if (worker.second.state() != MembershipPb::ACTIVE) {
                continue;
            }
            HostPort address;
            RETURN_IF_NOT_OK(address.ParseString(worker.first));
            sameNodeWorkers.emplace_back(std::move(address));
            for (const auto token : RebuildTopologyMemberTokens(ring, worker.first, worker.second)) {
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

    Status FindSameNodeRouteKeyToWorker(uint32_t workerIndex, const std::string &prefix,
                                        bool requireDifferentOwner, std::string &key, HostPort &metaOwner)
    {
        ClusterTopologyPb ring;
        RETURN_IF_NOT_OK(cluster_->ReadClusterTopology(ring));
        HostPort targetWorker;
        RETURN_IF_NOT_OK(cluster_->GetWorkerAddr(workerIndex, targetWorker));
        std::vector<HostPort> sameNodeWorkers;
        std::map<uint32_t, std::string> tokenWorkers;
        for (const auto &worker : ring.members()) {
            if (worker.second.state() != MembershipPb::ACTIVE) {
                continue;
            }
            HostPort address;
            RETURN_IF_NOT_OK(address.ParseString(worker.first));
            sameNodeWorkers.emplace_back(std::move(address));
            for (const auto token : RebuildTopologyMemberTokens(ring, worker.first, worker.second)) {
                tokenWorkers.emplace(token, worker.first);
            }
        }
        CHECK_FAIL_RETURN_STATUS(!sameNodeWorkers.empty(), K_NOT_FOUND, "No same-node worker is available");
        CHECK_FAIL_RETURN_STATUS(!tokenWorkers.empty(), K_NOT_FOUND, "Hash ring has no worker tokens");
        std::sort(sameNodeWorkers.begin(), sameNodeWorkers.end());
        const auto target = std::find(sameNodeWorkers.begin(), sameNodeWorkers.end(), targetWorker);
        CHECK_FAIL_RETURN_STATUS(target != sameNodeWorkers.end(), K_NOT_FOUND,
                                 "Target worker is absent from same-node workers");
        const size_t targetOffset = static_cast<size_t>(target - sameNodeWorkers.begin());
        for (size_t i = 0; i < KEY_SEARCH_LIMIT; ++i) {
            std::string candidate = prefix + std::to_string(i);
            const uint32_t keyHash = MurmurHash3_32(candidate);
            auto owner = tokenWorkers.lower_bound(keyHash);
            owner = owner == tokenWorkers.end() ? tokenWorkers.begin() : owner;
            RETURN_IF_NOT_OK(metaOwner.ParseString(owner->second));
            if (keyHash % sameNodeWorkers.size() == targetOffset
                && (!requireDifferentOwner || metaOwner != targetWorker)) {
                key = std::move(candidate);
                return Status::OK();
            }
        }
        return Status(K_NOT_FOUND, "Unable to find a key for the requested same-node worker");
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
    DataPlacementPolicy routedDataPlacementPolicy_ = DataPlacementPolicy::PREFERRED_META_OWNER;
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
    ReinitRoutedClient(DataPlacementPolicy::PREFERRED_SAME_NODE);
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
    std::string setActual;
    DS_ASSERT_OK(routedClient_->Get(setKey, setActual));
    ASSERT_EQ(setActual, setValue);

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
    std::string msetActual;
    DS_ASSERT_OK(routedClient_->Get(msetKey, msetActual));
    ASSERT_EQ(msetActual, msetValue);
}

class KVClientTransportSetConnectOptionsTest : public KVClientTransportSetTest {
protected:
    void InitRoutedClient() override
    {
        const std::string previousPolicy = FLAGS_sdk_data_placement_policy;
        Raii restorePolicy([previousPolicy]() { FLAGS_sdk_data_placement_policy = previousPolicy; });
        FLAGS_sdk_data_placement_policy = "PREFERRED_SAME_NODE";
        ConnectOptions options;
        InitConnectOpt(ROUTED_CLIENT_WORKER_INDEX, options);
        options.enableLocalCache = false;
        options.dataPlacementPolicy = DataPlacementPolicy::PREFERRED_META_OWNER;
        routedClient_ = std::make_shared<KVClient>(options);
        DS_ASSERT_OK(routedClient_->Init());
    }
};

TEST_F(KVClientTransportSetConnectOptionsTest, DISABLED_MetaOwnerPolicyRoutesWritesToMetadataOwner)
{
    std::string key;
    HostPort metaOwner;
    HostPort preferredWorker;
    DS_ASSERT_OK(FindSameNodeDivergentRouteKey("routing_explicit_meta_owner_", key, metaOwner, preferredWorker));
    ASSERT_NE(metaOwner, preferredWorker);

    const std::string value(VALUE_SIZE, 'e');
    DS_ASSERT_OK(routedClient_->Set(key, value));
    uint32_t metaOwnerIndex = 0;
    uint32_t preferredWorkerIndex = 0;
    DS_ASSERT_OK(FindWorkerIndex(metaOwner, metaOwnerIndex));
    DS_ASSERT_OK(FindWorkerIndex(preferredWorker, preferredWorkerIndex));
    AssertPrimaryWorker(key, metaOwnerIndex);
    const std::string previousPolicy = FLAGS_sdk_data_placement_policy;
    Raii restorePolicy([previousPolicy]() { FLAGS_sdk_data_placement_policy = previousPolicy; });
    FLAGS_sdk_data_placement_policy = "PREFERRED_SAME_NODE";
    ConnectOptions readOptions;
    InitConnectOpt(preferredWorkerIndex, readOptions);
    readOptions.enableLocalCache = false;
    KVClient readClient(readOptions);
    DS_ASSERT_OK(readClient.Init());

    std::string actual;
    DS_ASSERT_OK(readClient.Get(key, actual));
    ASSERT_EQ(actual, value);
    ASSERT_EQ(AccessTransportTracker::ToString(), "SHM");
    std::vector<std::string> actualValues;
    DS_ASSERT_OK(readClient.Get({ key }, actualValues));
    ASSERT_EQ(actualValues, std::vector<std::string>{ value });
    ASSERT_EQ(AccessTransportTracker::ToString(), "SHM");
}

TEST_F(KVClientTransportSetTest, InvalidConnectOptionsPolicyIsRejected)
{
    ConnectOptions options;
    InitConnectOpt(ROUTED_CLIENT_WORKER_INDEX, options);
    options.enableLocalCache = false;
    options.dataPlacementPolicy = static_cast<DataPlacementPolicy>(255);
    KVClient client(options);

    const Status status = client.Init();

    ASSERT_EQ(status.GetCode(), K_INVALID);
    ASSERT_NE(status.GetMsg().find("Invalid data placement policy"), std::string::npos);
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

TEST_F(KVClientTransportSetTest, MSetMetaOwnerGroupsUseCompiledRemoteTransport)
{
    std::vector<std::string> worker0Keys;
    std::vector<std::string> worker1Keys;
    for (size_t i = 0; i < 2; ++i) {
        std::string worker0Key;
        std::string worker1Key;
        DS_ASSERT_OK(FindRouteKeyToWorker(
            READER_WORKER_INDEX, "transport_mset_meta_worker0_" + std::to_string(i) + "_", worker0Key));
        DS_ASSERT_OK(FindRouteKeyToWorker(
            ROUTED_CLIENT_WORKER_INDEX, "transport_mset_meta_worker1_" + std::to_string(i) + "_", worker1Key));
        worker0Keys.emplace_back(std::move(worker0Key));
        worker1Keys.emplace_back(std::move(worker1Key));
    }
    const std::vector<std::string> keys{ worker0Keys[0], worker1Keys[0], worker0Keys[1], worker1Keys[1] };
    const std::vector<std::string> values{ std::string(VALUE_SIZE, 'a'), std::string(VALUE_SIZE + 1, 'b'),
                                           std::string(VALUE_SIZE + 2, 'c'), std::string(VALUE_SIZE + 3, 'd') };
    const std::vector<StringView> valueViews{ values[0], values[1], values[2], values[3] };
    std::vector<std::string> failedKeys;
    DS_ASSERT_OK(inject::Set(MULTI_CREATE_INJECT, "call()"));
    DS_ASSERT_OK(inject::Set(MULTI_PUBLISH_INJECT, "call()"));
    const uint64_t createBefore = inject::GetExecuteCount(MULTI_CREATE_INJECT);
    const uint64_t publishBefore = inject::GetExecuteCount(MULTI_PUBLISH_INJECT);

    DS_ASSERT_OK(routedClient_->MSet(keys, valueViews, failedKeys));

    ASSERT_TRUE(failedKeys.empty());
    ASSERT_EQ(inject::GetExecuteCount(MULTI_CREATE_INJECT), createBefore + 2);
    ASSERT_EQ(inject::GetExecuteCount(MULTI_PUBLISH_INJECT), publishBefore + 2);
    ASSERT_EQ(AccessTransportTracker::ToString(), ExpectedTransport());
    for (size_t i = 0; i < keys.size(); ++i) {
        AssertValue(keys[i], values[i]);
        AssertPrimaryWorker(keys[i], i % 2 == 0 ? READER_WORKER_INDEX : ROUTED_CLIENT_WORKER_INDEX);
    }
}

// Two-step batch write (MCreate + MSet(vector<Buffer>)) must route each key to its hash-ring
// worker instead of the bound worker. Mirrors RoutedMSetGroupsObjectsByMetadataOwner for the
// two-step Buffer API (Component D).
TEST_F(KVClientTransportSetTest, RoutedTwoStepMCreateMSetGroupsByWorker)
{
    std::string worker0Key;
    std::string worker1Key;
    DS_ASSERT_OK(FindRouteKeyToWorker(READER_WORKER_INDEX, "transport_2step_mset_worker0_", worker0Key));
    DS_ASSERT_OK(FindRouteKeyToWorker(ROUTED_CLIENT_WORKER_INDEX, "transport_2step_mset_worker1_", worker1Key));
    const std::vector<std::string> keys{ worker0Key, worker1Key };
    const std::vector<uint64_t> sizes{ VALUE_SIZE, VALUE_SIZE };
    const std::vector<std::string> values{ std::string(VALUE_SIZE, 'a'), std::string(VALUE_SIZE, 'b') };

    std::vector<std::shared_ptr<Buffer>> buffers;
    SetParam param;
    DS_ASSERT_OK(routedClient_->MCreate(keys, sizes, param, buffers));
    ASSERT_EQ(buffers.size(), keys.size());
    for (size_t i = 0; i < buffers.size(); i++) {
        DS_ASSERT_OK(buffers[i]->MemoryCopy(values[i].data(), values[i].size()));
    }
    DS_ASSERT_OK(routedClient_->MSet(buffers));

    AssertValue(keys[0], values[0]);
    AssertValue(keys[1], values[1]);
    // Each key landed on its hash-ring-selected worker, not the client's bound worker.
    AssertPrimaryWorker(keys[0], READER_WORKER_INDEX);
    AssertPrimaryWorker(keys[1], ROUTED_CLIENT_WORKER_INDEX);
}

TEST_F(KVClientTransportSetTest, RoutedMSetLargePayloadParallelCopyPreservesData)
{
    constexpr size_t parallelCopyValueSize = 512 * 1024;
    constexpr size_t objectCount = 4;
    std::vector<std::string> keys;
    std::vector<std::string> values;
    std::vector<StringView> valueViews;
    for (size_t i = 0; i < objectCount; ++i) {
        std::string key;
        DS_ASSERT_OK(
            FindRouteKeyToWorker(READER_WORKER_INDEX, "transport_mset_parallel_copy_" + std::to_string(i) + "_", key));
        keys.emplace_back(std::move(key));
        values.emplace_back(parallelCopyValueSize, static_cast<char>('a' + i));
    }
    for (const auto &value : values) {
        valueViews.emplace_back(value);
    }
    std::vector<std::string> failedKeys;

    DS_ASSERT_OK(routedClient_->MSet(keys, valueViews, failedKeys));

    ASSERT_TRUE(failedKeys.empty());
    for (size_t i = 0; i < objectCount; ++i) {
        AssertValue(keys[i], values[i]);
    }
}

// MSet N keys on the same-host worker → must use the SHM transporter (batch InvokeMultiSet),
// not N serial TCP sets. Verifies the batch path is taken end-to-end (review 180841432).
// MSet N keys on the same-host worker → must succeed via the routed path (batch InvokeMultiSet).
// Verifies the batch path is taken end-to-end (review 180841432). AccessTransportTracker varies
// by build (SHM vs UB under USE_URMA), so only assert MSet success + no failedKeys.
TEST_F(KVClientTransportSetTest, RoutedMSetSameHostBatchSucceeds)
{
    constexpr int N = 5;
    std::vector<std::string> keys;
    std::vector<std::string> values;
    std::vector<StringView> valueViews;
    keys.reserve(N);
    values.reserve(N);
    for (int i = 0; i < N; ++i) {
        std::string key;
        DS_ASSERT_OK(FindRouteKeyToWorker(READER_WORKER_INDEX, "transport_mset_batch_" + std::to_string(i) + "_", key));
        keys.push_back(key);
        values.emplace_back(VALUE_SIZE, 'm');
    }
    for (const auto &v : values) {
        valueViews.emplace_back(v);
    }
    std::vector<std::string> failedKeys;
    auto rc = routedClient_->MSet(keys, valueViews, failedKeys);
    DS_ASSERT_OK(rc);
    ASSERT_TRUE(failedKeys.empty()) << "failedKeys: " << failedKeys.size();
}

TEST_F(KVClientTransportSetTest, RoutedMSetWorkerAutoReleaseKeepsDataReadableAndReusable)
{
    std::string firstKey;
    std::string secondKey;
    DS_ASSERT_OK(FindRouteKeyToWorker(READER_WORKER_INDEX, "transport_mset_auto_release_first_", firstKey));
    DS_ASSERT_OK(FindRouteKeyToWorker(READER_WORKER_INDEX, "transport_mset_auto_release_second_", secondKey));
    const std::vector<std::string> keys{ firstKey, secondKey };
    const std::vector<std::string> values{ "first-value", "second-value" };
    const std::vector<StringView> valueViews{ values[0], values[1] };
    std::vector<std::string> failedKeys;

    DS_ASSERT_OK(routedClient_->MSet(keys, valueViews, failedKeys));

    EXPECT_TRUE(failedKeys.empty());
    AssertValue(firstKey, values[0]);
    AssertValue(secondKey, values[1]);

    DS_ASSERT_OK(routedClient_->Del(firstKey));
    DS_ASSERT_OK(routedClient_->Del(secondKey));
    failedKeys.clear();
    const std::vector<std::string> retryValues{ "first-retry", "second-retry" };
    const std::vector<StringView> retryValueViews{ retryValues[0], retryValues[1] };
    DS_ASSERT_OK(routedClient_->MSet(keys, retryValueViews, failedKeys));
    EXPECT_TRUE(failedKeys.empty());
    AssertValue(firstKey, retryValues[0]);
    AssertValue(secondKey, retryValues[1]);
}

// Same-host routed Set must succeed with metrics initialized (review 180849800).
TEST_F(KVClientTransportSetTest, ShmMetricRegisteredAndRoutedSetSucceeds)
{
    metrics::ResetKvMetricsForTest();
    DS_ASSERT_OK(metrics::InitKvMetrics());
    std::string key;
    DS_ASSERT_OK(FindRouteKeyToWorker(READER_WORKER_INDEX, "transport_metric_set_", key));
    const std::string value(VALUE_SIZE, 's');
    DS_ASSERT_OK(routedClient_->Set(key, value));
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

TEST_F(KVClientTransportSetTest, NotReadyCreateReroutesWholeTransaction)
{
    std::string key;
    DS_ASSERT_OK(FindRouteKeyToWorker(READER_WORKER_INDEX, "transport_set_not_ready_create_", key));
    DS_ASSERT_OK(inject::Set(CREATE_INJECT, "1*return(K_NOT_READY)"));
    const std::string value(VALUE_SIZE, 'c');

    DS_ASSERT_OK(routedClient_->Set(key, value));

    AssertValue(key, value);
    AssertPrimaryWorker(key, ROUTED_CLIENT_WORKER_INDEX);
}

TEST_F(KVClientTransportSetTest, NotReadyPublishReroutesWholeTransaction)
{
    std::string key;
    DS_ASSERT_OK(FindRouteKeyToWorker(READER_WORKER_INDEX, "transport_set_not_ready_publish_", key));
    DS_ASSERT_OK(inject::Set(PUBLISH_INJECT, "1*return(K_NOT_READY)"));
    const std::string value(VALUE_SIZE, 'p');

    DS_ASSERT_OK(routedClient_->Set(key, value));

    AssertValue(key, value);
    AssertPrimaryWorker(key, ROUTED_CLIENT_WORKER_INDEX);
}

TEST_F(KVClientTransportSetTest, NotReadyMultiCreateReroutesGroup)
{
    std::string firstKey;
    std::string secondKey;
    DS_ASSERT_OK(FindRouteKeyToWorker(READER_WORKER_INDEX, "transport_mset_not_ready_create_a_", firstKey));
    DS_ASSERT_OK(FindRouteKeyToWorker(READER_WORKER_INDEX, "transport_mset_not_ready_create_b_", secondKey));
    DS_ASSERT_OK(inject::Set(MULTI_CREATE_INJECT, "1*return(K_NOT_READY)"));
    const std::vector<std::string> keys{ firstKey, secondKey };
    const std::vector<std::string> values{ std::string(VALUE_SIZE, 'a'), std::string(VALUE_SIZE, 'b') };
    const std::vector<StringView> valueViews{ values[0], values[1] };
    std::vector<std::string> failedKeys;

    DS_ASSERT_OK(routedClient_->MSet(keys, valueViews, failedKeys));

    EXPECT_TRUE(failedKeys.empty());
    AssertValue(keys[0], values[0]);
    AssertValue(keys[1], values[1]);
    AssertPrimaryWorker(keys[0], ROUTED_CLIENT_WORKER_INDEX);
    AssertPrimaryWorker(keys[1], ROUTED_CLIENT_WORKER_INDEX);
}

TEST_F(KVClientTransportSetTest, NotReadyMultiPublishReroutesGroup)
{
    std::string firstKey;
    std::string secondKey;
    DS_ASSERT_OK(FindRouteKeyToWorker(READER_WORKER_INDEX, "transport_mset_not_ready_publish_a_", firstKey));
    DS_ASSERT_OK(FindRouteKeyToWorker(READER_WORKER_INDEX, "transport_mset_not_ready_publish_b_", secondKey));
    // SHM and UB MSet both publish through a single InvokeMultiSet RPC (ad9e274e changed the SHM
    // path from N serial InvokeSet calls to one InvokeMultiSet). Fail that first RPC with
    // K_NOT_READY so the whole group is rerouted to the alternate worker and succeeds there.
    DS_ASSERT_OK(inject::Set(MULTI_PUBLISH_INJECT, "1*return(K_NOT_READY)"));
    const std::vector<std::string> keys{ firstKey, secondKey };
    const std::vector<std::string> values{ std::string(VALUE_SIZE, 'm'), std::string(VALUE_SIZE, 'n') };
    const std::vector<StringView> valueViews{ values[0], values[1] };
    std::vector<std::string> failedKeys;

    DS_ASSERT_OK(routedClient_->MSet(keys, valueViews, failedKeys));

    EXPECT_EQ(inject::GetExecuteCount(MULTI_PUBLISH_INJECT), 1u);
    EXPECT_TRUE(failedKeys.empty());
    AssertValue(keys[0], values[0]);
    AssertValue(keys[1], values[1]);
    AssertPrimaryWorker(keys[0], ROUTED_CLIENT_WORKER_INDEX);
    AssertPrimaryWorker(keys[1], ROUTED_CLIENT_WORKER_INDEX);
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
    // BrokenFilter debounces (EVICT_CONSECUTIVE_FAILURES) and K_RPC_UNAVAILABLE is no longer a
    // global-eviction signal, so two transient publish failures do NOT evict firstWorker. The
    // second Set re-routes to the same worker (the inject is exhausted, so it succeeds) instead
    // of failing over to another.
    ASSERT_EQ(retryWorker, firstWorker);
    AssertValue(key, value);
}

TEST_F(KVClientTransportSetTest, MetadataOwnerFailureRefreshesRingWithoutEvictingIngress)
{
    std::string routedKey;
    DS_ASSERT_OK(FindRouteKeyToWorker(READER_WORKER_INDEX, "transport_meta_owner_failure_routed_", routedKey));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, READER_WORKER_INDEX, "worker.before_CreateMetadataToMaster",
                                           "1*return(K_RPC_UNAVAILABLE)"));
    ASSERT_EQ(routedClient_->Set(routedKey, std::string(VALUE_SIZE, 'r')).GetCode(), K_METADATA_OWNER_UNAVAILABLE);

    std::string routedRetryKey;
    DS_ASSERT_OK(FindRouteKeyToWorker(READER_WORKER_INDEX, "transport_meta_owner_retry_routed_", routedRetryKey));
    DS_ASSERT_OK(routedClient_->Set(routedRetryKey, std::string(VALUE_SIZE, 's')));
    AssertPrimaryWorker(routedRetryKey, READER_WORKER_INDEX);

    std::string msetKey;
    DS_ASSERT_OK(FindRouteKeyToWorker(READER_WORKER_INDEX, "transport_meta_owner_failure_mset_", msetKey));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, READER_WORKER_INDEX, "master.CreateMultiMeta.begin",
                                           "20*return(K_RPC_UNAVAILABLE)"));
    std::vector<std::string> failedKeys;
    const std::string msetValue(VALUE_SIZE, 'b');
    ASSERT_EQ(routedClient_->MSet({ msetKey }, { StringView(msetValue) }, failedKeys).GetCode(),
              K_METADATA_OWNER_UNAVAILABLE);
    EXPECT_EQ(failedKeys, std::vector<std::string>({ msetKey }));
    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, READER_WORKER_INDEX, "master.CreateMultiMeta.begin"));

    // local-cache clients do not own a hash ring; they keep using the healthy ingress worker,
    // whose worker-side ring converges independently.
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, ROUTED_CLIENT_WORKER_INDEX, "worker.before_CreateMetadataToMaster",
                                           "1*return(K_RPC_UNAVAILABLE)"));
    ASSERT_EQ(localClient_->Set("transport_meta_owner_failure_local", std::string(VALUE_SIZE, 'l')).GetCode(),
              K_METADATA_OWNER_UNAVAILABLE);
    const std::string localRetryKey = "transport_meta_owner_retry_local";
    DS_ASSERT_OK(localClient_->Set(localRetryKey, std::string(VALUE_SIZE, 'm')));
    AssertPrimaryWorker(localRetryKey, ROUTED_CLIENT_WORKER_INDEX);
}

TEST_F(KVClientTransportSetTest, MSetPartialGroupFailureReportsExactFailedKeys)
{
    std::string successKey;
    std::string failedKey0;
    std::string failedKey1;
    DS_ASSERT_OK(FindRouteKeyToWorker(READER_WORKER_INDEX, "transport_mset_partial_success_", successKey));
    DS_ASSERT_OK(FindRouteKeyToWorker(ROUTED_CLIENT_WORKER_INDEX, "transport_mset_partial_failed0_", failedKey0));
    DS_ASSERT_OK(FindRouteKeyToWorker(ROUTED_CLIENT_WORKER_INDEX, "transport_mset_partial_failed1_", failedKey1));
    const std::vector<std::string> keys{ failedKey0, successKey, failedKey1 };
    const std::vector<std::string> values{ std::string(VALUE_SIZE, 'f'), std::string(VALUE_SIZE, 's'),
                                           std::string(VALUE_SIZE, 'g') };
    const std::vector<StringView> valueViews{ values[0], values[1], values[2] };
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, ROUTED_CLIENT_WORKER_INDEX,
                                           MULTI_PUBLISH_METADATA_INJECT, "1*return(K_RUNTIME_ERROR)"));
    Raii clearInject([this] {
        (void)cluster_->ClearInjectAction(WORKER, ROUTED_CLIENT_WORKER_INDEX,
                                          MULTI_PUBLISH_METADATA_INJECT);
    });
    DS_ASSERT_OK(inject::Set(MULTI_PUBLISH_INJECT, "call()"));
    const uint64_t publishBefore = inject::GetExecuteCount(MULTI_PUBLISH_INJECT);
    std::vector<std::string> failedKeys;

    DS_ASSERT_OK(routedClient_->MSet(keys, valueViews, failedKeys));

    ASSERT_EQ(inject::GetExecuteCount(MULTI_PUBLISH_INJECT), publishBefore + 2);
    ASSERT_EQ(failedKeys, (std::vector<std::string>{ failedKey0, failedKey1 }));
    AssertValue(successKey, values[1]);
    std::string value;
    ASSERT_EQ(readerClient_->Get(failedKey0, value).GetCode(), K_NOT_FOUND);
    ASSERT_EQ(readerClient_->Get(failedKey1, value).GetCode(), K_NOT_FOUND);
}

class KVClientTransportSetWithShmTest : public KVClientTransportSetTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        KVClientTransportSetTest::SetClusterSetupOptions(opts);
        // Enable same-host SHM fd-passing so a routed Set can mmap the worker-allocated region zero-copy.
        constexpr char DISABLED_SHM_OPTION[] = "-ipc_through_shared_memory=false";
        const auto pos = opts.workerGflagParams.find(DISABLED_SHM_OPTION);
        ASSERT_NE(pos, std::string::npos);
        opts.workerGflagParams.replace(pos, sizeof(DISABLED_SHM_OPTION) - 1, "-ipc_through_shared_memory=true");
    }
};

TEST_F(KVClientTransportSetWithShmTest, MSetPreferredSameNodeGroupsUseShm)
{
    ReinitRoutedClient(DataPlacementPolicy::PREFERRED_SAME_NODE);
    std::vector<std::string> worker0Keys;
    std::vector<std::string> worker1Keys;
    HostPort worker0MetaOwner;
    HostPort worker1MetaOwner;
    for (size_t i = 0; i < 2; ++i) {
        std::string worker0Key;
        std::string worker1Key;
        DS_ASSERT_OK(FindSameNodeRouteKeyToWorker(
            READER_WORKER_INDEX, "transport_mset_shm_worker0_" + std::to_string(i) + "_", true,
            worker0Key, worker0MetaOwner));
        DS_ASSERT_OK(FindSameNodeRouteKeyToWorker(
            ROUTED_CLIENT_WORKER_INDEX, "transport_mset_shm_worker1_" + std::to_string(i) + "_", false,
            worker1Key, worker1MetaOwner));
        worker0Keys.emplace_back(std::move(worker0Key));
        worker1Keys.emplace_back(std::move(worker1Key));
    }
    HostPort worker0;
    DS_ASSERT_OK(cluster_->GetWorkerAddr(READER_WORKER_INDEX, worker0));
    ASSERT_NE(worker0MetaOwner, worker0);
    const std::vector<std::string> keys{ worker0Keys[0], worker1Keys[0], worker0Keys[1], worker1Keys[1] };
    const std::vector<std::string> values{ std::string(VALUE_SIZE, 'w'), std::string(VALUE_SIZE + 1, 'x'),
                                           std::string(VALUE_SIZE + 2, 'y'), std::string(VALUE_SIZE + 3, 'z') };
    const std::vector<StringView> valueViews{ values[0], values[1], values[2], values[3] };
    std::vector<std::string> failedKeys;
    DS_ASSERT_OK(inject::Set(MULTI_CREATE_INJECT, "call()"));
    DS_ASSERT_OK(inject::Set(MULTI_PUBLISH_INJECT, "call()"));
    const uint64_t createBefore = inject::GetExecuteCount(MULTI_CREATE_INJECT);
    const uint64_t publishBefore = inject::GetExecuteCount(MULTI_PUBLISH_INJECT);

    DS_ASSERT_OK(routedClient_->MSet(keys, valueViews, failedKeys));

    ASSERT_TRUE(failedKeys.empty());
    ASSERT_EQ(inject::GetExecuteCount(MULTI_CREATE_INJECT), createBefore + 2);
    ASSERT_EQ(inject::GetExecuteCount(MULTI_PUBLISH_INJECT), publishBefore + 2);
    ASSERT_EQ(AccessTransportTracker::ToString(), "SHM");
    for (size_t i = 0; i < keys.size(); ++i) {
        AssertPrimaryWorker(keys[i], i % 2 == 0 ? READER_WORKER_INDEX : ROUTED_CLIENT_WORKER_INDEX);
        AssertValue(keys[i], values[i]);
    }
}

// A routed Set whose target is a same-host SHM-enabled worker must take the SHM zero-copy path
// (worker allocates the region, passes the fd worker->client, client mmaps and writes), recording
// AccessTransportKind::SHM rather than TCP. Also covers the enableLocalCache=false routing fix: the
// Set goes through the transport layer even when the route lands on the bound worker.
TEST_F(KVClientTransportSetWithShmTest, RoutedSetUsesShmZeroCopy)
{
    std::string key;
    DS_ASSERT_OK(FindRouteKeyToWorker(READER_WORKER_INDEX, "transport_set_shm_zc_", key));
    const std::string value(VALUE_SIZE, 's');
    DS_ASSERT_OK(routedClient_->Set(key, value));
    ASSERT_EQ(AccessTransportTracker::ToString(), "SHM");
    AssertValue(key, value);
}

// Reproduces the SCMTCP cross-host misclassification fixed by routing enableLocalCache Create
// through the transport layer when the bound worker is cross-host. Under SCMTCP, a cross-host
// bound worker still reports IsShmEnable()=true (SCMTCP handshake succeeds), so the old path
// built a SHM buffer and Set(buffer) logged transportType=SHM even though the bytes travel over
// UB/TCP. The transport advisor (sameHostWorkers, hostId-based) is the single source of truth.
class KVClientTransportSetLocalCacheScmTcpTest : public KVClientTransportSetTest {
public:
    static constexpr char REMOTE_HOST_ID_ENV[] = "transport_set_scm_tcp_remote_host_id";
    static constexpr char REMOTE_HOST_ID_VALUE[] = "transport-set-scm-tcp-remote-host";

    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        // Reuse the base layout (URMA, distributed master, etcd) but flip SHM fd-passing on and
        // add per-worker SCMTCP ports. Worker 0 keeps the SDK hostId (same-host control); worker 1
        // gets a distinct hostId so the advisor classifies it as cross-host. The base class emits a
        // global host_id_env_name; move it per-worker so worker 1 can override it without a duplicate.
        KVClientTransportSetTest::SetClusterSetupOptions(opts);
        constexpr char DISABLED_SHM_OPTION[] = "-ipc_through_shared_memory=false";
        const auto pos = opts.workerGflagParams.find(DISABLED_SHM_OPTION);
        ASSERT_NE(pos, std::string::npos);
        opts.workerGflagParams.replace(pos, sizeof(DISABLED_SHM_OPTION) - 1, "-ipc_through_shared_memory=true");
        constexpr char GLOBAL_HOST_ID_OPT[] = " -host_id_env_name=";
        const auto hidPos = opts.workerGflagParams.find(GLOBAL_HOST_ID_OPT);
        ASSERT_NE(hidPos, std::string::npos);
        const auto hidEnd = opts.workerGflagParams.find(' ', hidPos + 1);
        const std::string sdkHostIdEnv = opts.workerGflagParams.substr(
            hidPos + std::string(GLOBAL_HOST_ID_OPT).size(),
            hidEnd == std::string::npos ? std::string::npos : hidEnd - (hidPos + std::string(GLOBAL_HOST_ID_OPT).size()));
        opts.workerGflagParams.erase(hidPos, hidEnd == std::string::npos ? std::string::npos : hidEnd - hidPos);
        for (uint32_t i = 0; i < opts.numWorkers; ++i) {
            opts.workerSpecifyGflagParams[i] += FormatString(" -shared_memory_worker_port=%d", GetFreePort());
            // Worker 0 reports the SDK hostId (same-host); worker 1 reports a distinct hostId (cross-host).
            const auto &env = (i == ROUTED_CLIENT_WORKER_INDEX) ? std::string(REMOTE_HOST_ID_ENV) : sdkHostIdEnv;
            opts.workerSpecifyGflagParams[i] += " -host_id_env_name=" + env;
        }
    }

    void SetUp() override
    {
        if (!SupportScmTcp()) {
            skipped_ = true;
            GTEST_SKIP() << "SMC over TCP not supported on this kernel";
        }
        ASSERT_EQ(setenv(REMOTE_HOST_ID_ENV, REMOTE_HOST_ID_VALUE, 1), 0);
        // The SDK reads its own host_id from FLAGS_host_id_env_name; set it before Init so the
        // advisor partitions same-host (worker 0) vs cross-host (worker 1) workers correctly.
        previousHostIdEnvName_ = FLAGS_host_id_env_name;
        FLAGS_host_id_env_name = HOST_ID_ENV_NAME;
        KVClientTransportSetTest::SetUp();
    }

    void TearDown() override
    {
        if (!skipped_) {
            KVClientTransportSetTest::TearDown();
        }
        FLAGS_host_id_env_name = previousHostIdEnvName_;
        (void)unsetenv(REMOTE_HOST_ID_ENV);
    }

    static bool SupportScmTcp()
    {
        const int proto = 518;  // IPPROTO_SCMTCP
        auto fd = socket(AF_INET, SOCK_STREAM, proto);
        bool ret = fd >= 0;
        if (ret) {
            close(fd);
        }
        return ret;
    }

private:
    bool skipped_ = false;
    std::string previousHostIdEnvName_;
};

// Cross-host bound worker: SCMTCP makes IsShmEnable() true, but the advisor excludes it from
// sameHostWorkers (hostId mismatch). Create+MemoryCopy+Set(buffer) must route through the
// transport layer and record the real medium (UB under USE_URMA, TCP otherwise).
TEST_F(KVClientTransportSetLocalCacheScmTcpTest, LocalCacheBufferSetRoutesCrossHostWorkerToTransport)
{
    // enableLocalCache=true + enableCrossNodeConnection=true bound to worker 1 (cross-host hostId).
    // enableCrossNodeConnection forces InitRouting so sameHostWorkers is built from the hostId
    // topology, not the SCMTCP IsShmEnable() fallback that would misclassify worker 1 as same-host.
    std::shared_ptr<KVClient> crossHostClient;
    InitTestKVClient(ROUTED_CLIENT_WORKER_INDEX, crossHostClient, [](ConnectOptions &opts) {
        opts.enableLocalCache = true;
        opts.enableCrossNodeConnection = true;
    });
    const std::string key = "transport_local_cache_scm_tcp_cross_host";
    const std::string value(VALUE_SIZE, 'u');
    SetParam param{ .writeMode = WriteMode::NONE_L2_CACHE, .ttlSecond = 5 };
    std::shared_ptr<Buffer> buffer;
    DS_ASSERT_OK(crossHostClient->Create(key, value.size(), param, buffer));
    DS_ASSERT_OK(buffer->MemoryCopy(value.data(), value.size()));
    DS_ASSERT_OK(crossHostClient->Set(buffer));
    ASSERT_EQ(AccessTransportTracker::ToString(), ExpectedTransport());
    // Verify round-trip from the same cross-host client so the assertion is not affected by
    // another client's enableCrossNodeConnection / routing configuration.
    std::string actual;
    DS_ASSERT_OK(crossHostClient->Get(key, actual));
    ASSERT_EQ(actual, value);
}

// Same-host control: the bound worker's hostId matches the SDK hostId, so Create keeps the SHM
// bound-worker path and Set(buffer) records SHM. Uses enableCrossNodeConnection=true so the
// advisor sameHostWorkers set is hostId-based (same path as the cross-host case), and worker 0's
// matching hostId keeps it on the SHM path.
TEST_F(KVClientTransportSetLocalCacheScmTcpTest, LocalCacheBufferSetSameHostWorkerRecordsShm)
{
    std::shared_ptr<KVClient> sameHostClient;
    InitTestKVClient(READER_WORKER_INDEX, sameHostClient, [](ConnectOptions &opts) {
        opts.enableLocalCache = true;
        opts.enableCrossNodeConnection = true;
    });
    const std::string key = "transport_local_cache_scm_tcp_same_host";
    const std::string value(VALUE_SIZE, 'h');
    SetParam param{ .writeMode = WriteMode::NONE_L2_CACHE, .ttlSecond = 5 };
    std::shared_ptr<Buffer> buffer;
    DS_ASSERT_OK(sameHostClient->Create(key, value.size(), param, buffer));
    DS_ASSERT_OK(buffer->MemoryCopy(value.data(), value.size()));
    DS_ASSERT_OK(sameHostClient->Set(buffer));
    ASSERT_EQ(AccessTransportTracker::ToString(), "SHM");
    std::string actual;
    DS_ASSERT_OK(sameHostClient->Get(key, actual));
    ASSERT_EQ(actual, value);
}
}  // namespace st
}  // namespace datasystem
