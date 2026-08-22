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

/** Description: Tests KVClient reads through the transport layer (enableLocalCache=false). */

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <future>
#include <map>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "client/object_cache/oc_client_common.h"
#include "common_distributed_ext.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/kvstore/etcd/etcd_store.h"
#include "datasystem/common/log/access_recorder.h"
#include "datasystem/common/util/hash_algorithm.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/thread_pool.h"
#include "datasystem/kv/read_only_buffer.h"
#include "datasystem/kv_client.h"
#include "datasystem/protos/cluster_topology.pb.h"


namespace datasystem {
namespace st {
namespace {
constexpr uint32_t META_OWNER_INDEX = 0;
constexpr uint32_t TRANSPORT_CLIENT_WORKER_INDEX = 1;
constexpr uint32_t DATA_WORKER_INDEX = 2;
constexpr uint32_t WORKER_NUM = 3;
constexpr int32_t CLIENT_TIMEOUT_MS = 3'000;
constexpr int32_t SHM_LATCH_TIMEOUT_MS = 1'000;
constexpr size_t VALUE_SIZE = 128 * 1024;
constexpr size_t MIXED_OVERSIZED_INLINE_VALUE_SIZE = VALUE_SIZE + 1;
constexpr size_t INLINE_DATA_LIMIT = 512 * 1024;
constexpr size_t LARGE_VALUE_SIZE = 8 * 1024 * 1024;
constexpr size_t KEY_SEARCH_LIMIT = 100'000;
constexpr uint64_t MIXED_TCP_DATA_RPC_COUNT = 1;
constexpr uint64_t MIXED_UB_DATA_RPC_COUNT = 2;
constexpr char REAL_ROUTE_KEY_PREFIX[] = "transport_real_route_";
constexpr char SUCCESS_KEY_PREFIX[] = "transport_get_success_";
constexpr char INJECT_RUNTIME_ERROR_KEY_PREFIX[] = "transport_get_inject_runtime_";
constexpr char INJECT_NOT_FOUND_KEY_PREFIX[] = "transport_get_inject_not_found_";
constexpr char UB_GET_SIZE_ENV[] = "DATASYSTEM_UB_GET_DATA_SIZE_BYTES";
constexpr char SKIP_WARMUP_INJECT[] = "ObjectClientImpl.ClientWorkerWarmup.skip";
constexpr char QUERY_AND_GET_INJECT[] = "client.transport.query_and_get";
constexpr char GET_OBJECT_REMOTE_INJECT[] = "client.transport.get_object_remote";
constexpr char BATCH_GET_OBJECT_REMOTE_INJECT[] = "client.transport.batch_get_object_remote";
constexpr char WORKER_OC_GET_INJECT[] = "client.transport.worker_oc_get";
constexpr char WORKER_OC_GET_ENTRY_INJECT[] = "worker.PreProcessGetObject.begin";
constexpr char REGISTER_SHM_CLIENT_INJECT[] = "client.transport.register_shm_client";
constexpr char GET_CLIENT_FD_INJECT[] = "client.transport.get_client_fd";
constexpr char SHM_HEARTBEAT_INJECT[] = "client.transport.shm_heartbeat";
constexpr char DRAIN_BEFORE_SNAPSHOT_INJECT[] = "WorkerOCServiceImpl.DrainTopologyScaleInData.beforeSnapshot";
constexpr char QUERY_AND_GET_TCP_HIT_INJECT[] = "worker.QueryAndGet.EncodeTcp";
constexpr char QUERY_AND_GET_UB_HIT_INJECT[] = "worker.QueryAndGet.EncodeUb";
constexpr char QUERY_AND_GET_SHM_HIT_INJECT[] = "worker.QueryAndGet.EncodeShm";
constexpr char QUERY_AND_GET_METADATA_MISS_INJECT[] = "worker.QueryAndGet.QueryMissMetadata";
constexpr char QUERY_AND_GET_INLINE_FAILURE_INJECT[] = "worker.QueryAndGet.EncodeLocalHitFailure";
constexpr char SHM_SESSION_UNAVAILABLE_BEFORE_BUILD_INJECT[] =
    "client.transport.query_and_get.shm_session_unavailable_before_build";
constexpr char SHM_MATERIALIZATION_FAILURE_INJECT[] =
    "client.transport.query_and_get.shm_materialization_failure";
constexpr char SHM_LATCH_FAIL_INJECT[] = "worker.ShmGuard.TryRLatch.Fail";
constexpr char PROVIDER_GET_ENTER_INJECT[] = "worker.GetObjectRemote.afterRead";
constexpr char PROVIDER_BATCH_GET_ENTER_INJECT[] = "worker.BatchGetObjectRemote.afterRead";
constexpr char URMA_CQE_ERROR_INJECT[] = "UrmaManager.CheckCompletionRecordStatus";
constexpr char SELF_PROBE_SUCCEEDED_INJECT[] = "PeerUbAdmission.CompleteProbe.success";
constexpr char PROVIDER_RECOVERY_PROBE_SUCCEEDED_INJECT[] =
    "WorkerWorkerTransportService.ProbeProviderUbRecovery.success";
constexpr char LOCAL_OBSERVATION_INJECT[] = "client.ub_health_filter.local_observation";
constexpr char LOCAL_READ_DENIED_INJECT[] = "client.ub_health_filter.local_read_denied";
constexpr char PROVIDER_PROBE_RECOVERED_INJECT[] = "client.ub_health_filter.provider_probe_recovered";
constexpr char SKIP_HEARTBEAT_UB_SUMMARY_INJECT[] = "client.heartbeat_ub_health_summary.skip";
constexpr char GLOBAL_UNAVAILABLE_APPLIED_INJECT[] = "client.ub_health_filter.global_unavailable_applied";
constexpr char GLOBAL_READ_DENIED_INJECT[] = "client.ub_health_filter.global_read_denied";
constexpr char SHM_HOST_ID_ENV_NAME[] = "transport_get_shm_host_id";
constexpr char SHM_HOST_ID_VALUE[] = "transport-get-shm-host";
constexpr char MIXED_HOST_ID_ENV_PREFIX[] = "transport_get_mixed_host_id_";
constexpr char MIXED_HOST_ID_VALUE_PREFIX[] = "transport-get-mixed-host-";

struct TransportRpcCounts {
    uint64_t queryAndGet = 0;
    uint64_t getObjectRemote = 0;
    uint64_t batchGetObjectRemote = 0;
    uint64_t workerOcGet = 0;
    uint64_t registerShmClient = 0;
    uint64_t getClientFd = 0;
    uint64_t shmHeartbeat = 0;
};

struct WorkerQueryAndGetCounts {
    uint64_t tcpHits = 0;
    uint64_t ubHits = 0;
    uint64_t shmHits = 0;
    uint64_t metadataMisses = 0;
};

struct MixedPathCounts {
    TransportRpcCounts rpc;
    WorkerQueryAndGetCounts localOwner;
    WorkerQueryAndGetCounts remoteOwner;
};

constexpr bool IsUrmaBuild()
{
#ifdef USE_URMA
    return true;
#else
    return false;
#endif
}

const char *ExpectedTransport()
{
    return IsUrmaBuild() ? "UB" : "TCP";
}

std::string MixedHostIdEnvName(uint32_t workerIndex)
{
    return std::string(MIXED_HOST_ID_ENV_PREFIX) + std::to_string(workerIndex);
}

std::string MixedHostIdValue(uint32_t workerIndex)
{
    return std::string(MIXED_HOST_ID_VALUE_PREFIX) + std::to_string(workerIndex);
}
}  // namespace

class KVClientTransportGetTest : public OCClientCommon, public CommonDistributedExt {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        FLAGS_v = 1;
        opts.numEtcd = 1;
        opts.numWorkers = WORKER_NUM;
        opts.enableDistributedMaster = "true";
        opts.workerGflagParams =
            " -shared_memory_size_mb=512 -ipc_through_shared_memory=false -arena_per_tenant=1";
#ifdef USE_URMA
        opts.workerGflagParams += " -enable_urma=true -enable_transport_fallback=false";
#else
        opts.workerGflagParams += " -enable_urma=false";
#endif
        opts.injectActions = "worker.batch_get_failure_for_keys:call()";
        opts.injectActions += ";" + std::string(QUERY_AND_GET_TCP_HIT_INJECT) + ":call()";
        opts.injectActions += ";" + std::string(QUERY_AND_GET_UB_HIT_INJECT) + ":call()";
        opts.injectActions += ";" + std::string(QUERY_AND_GET_SHM_HIT_INJECT) + ":call()";
        opts.injectActions += ";" + std::string(QUERY_AND_GET_METADATA_MISS_INJECT) + ":call()";
    }

    void SetUp() override
    {
        const char *ubGetSize = std::getenv(UB_GET_SIZE_ENV);
        if (ubGetSize != nullptr) {
            hadPreviousUbGetSize_ = true;
            previousUbGetSize_ = ubGetSize;
        }
        DS_ASSERT_OK(inject::Set(SKIP_WARMUP_INJECT, "call()"));
        DS_ASSERT_OK(inject::Set(QUERY_AND_GET_INJECT, "call()"));
        DS_ASSERT_OK(inject::Set(GET_OBJECT_REMOTE_INJECT, "call()"));
        DS_ASSERT_OK(inject::Set(BATCH_GET_OBJECT_REMOTE_INJECT, "call()"));
        DS_ASSERT_OK(inject::Set(WORKER_OC_GET_INJECT, "call()"));
        DS_ASSERT_OK(inject::Set(REGISTER_SHM_CLIENT_INJECT, "call()"));
        DS_ASSERT_OK(inject::Set(GET_CLIENT_FD_INJECT, "call()"));
        DS_ASSERT_OK(inject::Set(SHM_HEARTBEAT_INJECT, "call()"));
        ExternalClusterTest::SetUp();
        CommonDistributedExt::InitTestEtcdInstance();

        etcd_ = OCClientCommon::InitTestEtcdInstance();
        ASSERT_NE(etcd_, nullptr);
        InitTestKVClient(META_OWNER_INDEX, writer_, CLIENT_TIMEOUT_MS);
#ifdef USE_URMA
        SetUbGetSize(UbInlineBufferSize());
#endif
        InitTransportClient();
    }

    void TearDown() override
    {
        reader_.reset();
        writer_.reset();
        etcd_.reset();
        CommonDistributedExt::etcd_.reset();
        RestoreUbGetSize();
        (void)inject::Clear(SKIP_WARMUP_INJECT);
        (void)inject::Clear(QUERY_AND_GET_INJECT);
        (void)inject::Clear(GET_OBJECT_REMOTE_INJECT);
        (void)inject::Clear(LOCAL_OBSERVATION_INJECT);
        (void)inject::Clear(LOCAL_READ_DENIED_INJECT);
        (void)inject::Clear(PROVIDER_PROBE_RECOVERED_INJECT);
        (void)inject::Clear(SKIP_HEARTBEAT_UB_SUMMARY_INJECT);
        (void)inject::Clear(GLOBAL_UNAVAILABLE_APPLIED_INJECT);
        (void)inject::Clear(GLOBAL_READ_DENIED_INJECT);
        (void)inject::Clear(BATCH_GET_OBJECT_REMOTE_INJECT);
        (void)inject::Clear(WORKER_OC_GET_INJECT);
        (void)inject::Clear(REGISTER_SHM_CLIENT_INJECT);
        (void)inject::Clear(GET_CLIENT_FD_INJECT);
        (void)inject::Clear(SHM_HEARTBEAT_INJECT);
        ExternalClusterTest::TearDown();
    }

protected:
    BaseCluster *GetCluster() override
    {
        return cluster_.get();
    }

    // enableLocalCache=false reader: the client under test, exercising TransportLayer::Get.
    void InitTransportClient()
    {
        ConnectOptions options;
        InitConnectOpt(TransportClientWorkerIndex(), options, CLIENT_TIMEOUT_MS);
        options.enableLocalCache = false;
        options.dataPlacementPolicy = DataPlacementPolicy::PREFERRED_META_OWNER;
        reader_ = std::make_shared<KVClient>(options);
        DS_ASSERT_OK(reader_->Init());
    }

    virtual uint32_t TransportClientWorkerIndex() const
    {
        return TRANSPORT_CLIENT_WORKER_INDEX;
    }

    virtual size_t UbInlineBufferSize() const
    {
        return INLINE_DATA_LIMIT;
    }

    void SetUbGetSize(size_t size)
    {
        ASSERT_EQ(setenv(UB_GET_SIZE_ENV, std::to_string(size).c_str(), 1), 0);
    }

    void RestoreUbGetSize()
    {
        if (hadPreviousUbGetSize_) {
            (void)setenv(UB_GET_SIZE_ENV, previousUbGetSize_.c_str(), 1);
        } else {
            (void)unsetenv(UB_GET_SIZE_ENV);
        }
    }

    void GetRealHashKeysToWorker(uint32_t workerIndex, size_t keyCount, std::vector<std::string> &keys)
    {
        GetRealHashKeysToWorker(workerIndex, keyCount, REAL_ROUTE_KEY_PREFIX, keys);
    }

    void GetRealHashKeysToWorker(uint32_t workerIndex, size_t keyCount, const std::string &keyPrefix,
                                 std::vector<std::string> &keys)
    {
        ASSERT_NE(etcd_, nullptr);
        std::string value;
        DS_ASSERT_OK(etcd_->Get(GetTopologyTableName(), "", value));
        ClusterTopologyPb ring;
        ASSERT_TRUE(ring.ParseFromString(value));

        HostPort targetWorker;
        DS_ASSERT_OK(cluster_->GetWorkerAddr(workerIndex, targetWorker));
        ASSERT_NE(ring.members().find(targetWorker.ToString()), ring.members().end());
        std::map<uint32_t, std::string> tokenWorkers;
        for (const auto &worker : ring.members()) {
            for (const auto token : worker.second.tokens()) {
                tokenWorkers.emplace(token, worker.first);
            }
        }
        ASSERT_FALSE(tokenWorkers.empty());

        keys.clear();
        for (size_t candidateIndex = 0; candidateIndex < KEY_SEARCH_LIMIT && keys.size() < keyCount; ++candidateIndex) {
            std::string candidate = keyPrefix + std::to_string(workerIndex) + "_" + std::to_string(candidateIndex);
            auto owner = tokenWorkers.upper_bound(MurmurHash3_32(candidate));
            if (owner == tokenWorkers.end()) {
                owner = tokenWorkers.begin();
            }
            if (owner->second == targetWorker.ToString()) {
                keys.emplace_back(std::move(candidate));
            }
        }
        ASSERT_EQ(keys.size(), keyCount);
    }

    void GetRpcCounts(TransportRpcCounts &counts)
    {
        counts.queryAndGet = inject::GetExecuteCount(QUERY_AND_GET_INJECT);
        counts.getObjectRemote = inject::GetExecuteCount(GET_OBJECT_REMOTE_INJECT);
        counts.batchGetObjectRemote = inject::GetExecuteCount(BATCH_GET_OBJECT_REMOTE_INJECT);
        counts.workerOcGet = inject::GetExecuteCount(WORKER_OC_GET_INJECT);
        counts.registerShmClient = inject::GetExecuteCount(REGISTER_SHM_CLIENT_INJECT);
        counts.getClientFd = inject::GetExecuteCount(GET_CLIENT_FD_INJECT);
        counts.shmHeartbeat = inject::GetExecuteCount(SHM_HEARTBEAT_INJECT);
    }

    void GetWorkerQueryAndGetCounts(uint32_t workerIndex, WorkerQueryAndGetCounts &counts)
    {
        DS_ASSERT_OK(cluster_->GetInjectActionExecuteCount(
            WORKER, workerIndex, QUERY_AND_GET_TCP_HIT_INJECT, counts.tcpHits));
        DS_ASSERT_OK(cluster_->GetInjectActionExecuteCount(
            WORKER, workerIndex, QUERY_AND_GET_UB_HIT_INJECT, counts.ubHits));
        DS_ASSERT_OK(cluster_->GetInjectActionExecuteCount(
            WORKER, workerIndex, QUERY_AND_GET_SHM_HIT_INJECT, counts.shmHits));
        DS_ASSERT_OK(cluster_->GetInjectActionExecuteCount(
            WORKER, workerIndex, QUERY_AND_GET_METADATA_MISS_INJECT, counts.metadataMisses));
    }

    // Generate N distinct keys without making placement assumptions.
    std::vector<std::string> MakeRandomKeys(size_t count)
    {
        std::vector<std::string> keys;
        keys.reserve(count);
        for (size_t i = 0; i < count; ++i) {
            keys.emplace_back("transport_get_" + std::to_string(i) + "_" + GetStringUuid());
        }
        return keys;
    }

    std::vector<std::string> MakeKeysAcrossMetaOwners(size_t countPerOwner)
    {
        constexpr size_t MAX_CANDIDATES = 10'000;
        const size_t workerCount = cluster_->GetWorkerNum();
        std::vector<std::vector<std::string>> keysByOwner(workerCount);
        size_t selected = 0;
        for (size_t i = 0; i < MAX_CANDIDATES && selected < workerCount * countPerOwner; ++i) {
            std::string key = "transport_get_owner_" + std::to_string(i) + "_" + GetStringUuid();
            WorkerEntry owner;
            GetMetaLocationById(key, { 0, 1, 2 }, owner);
            if (owner.index < 0 || static_cast<size_t>(owner.index) >= workerCount) {
                ADD_FAILURE() << "invalid metadata owner index " << owner.index;
                return {};
            }
            auto &ownerKeys = keysByOwner[owner.index];
            if (ownerKeys.size() < countPerOwner) {
                ownerKeys.emplace_back(std::move(key));
                ++selected;
            }
        }
        EXPECT_EQ(selected, workerCount * countPerOwner);

        std::vector<std::string> keys;
        keys.reserve(selected);
        for (size_t position = 0; position < countPerOwner; ++position) {
            for (auto &ownerKeys : keysByOwner) {
                if (position < ownerKeys.size()) {
                    keys.emplace_back(std::move(ownerKeys[position]));
                }
            }
        }
        return keys;
    }

#ifdef USE_URMA
    void PrepareUbReplicaScenario(const std::string &key, const std::string &value,
                                  std::shared_ptr<KVClient> &requester)
    {
        SetUbGetSize(value.size() * 2);
        DS_ASSERT_OK(writer_->Set(key, value));
        std::shared_ptr<KVClient> replicaWarmer;
        InitTestKVClient(TRANSPORT_CLIENT_WORKER_INDEX, replicaWarmer, CLIENT_TIMEOUT_MS);
        std::string warmedValue;
        constexpr auto warmupTimeout = std::chrono::seconds(20);
        constexpr auto warmupRetryInterval = std::chrono::milliseconds(200);
        const auto warmupDeadline = std::chrono::steady_clock::now() + warmupTimeout;
        Status warmupStatus(K_URMA_DATA_WORKER_UNAVAILABLE, "waiting for startup UB admission");
        while (std::chrono::steady_clock::now() < warmupDeadline) {
            warmupStatus = replicaWarmer->Get(key, warmedValue);
            if (warmupStatus.IsOk() || warmupStatus.GetCode() != K_URMA_DATA_WORKER_UNAVAILABLE) {
                break;
            }
            std::this_thread::sleep_for(warmupRetryInterval);
        }
        DS_ASSERT_OK(warmupStatus);
        ASSERT_EQ(warmedValue, value);
        replicaWarmer.reset();

        // Disable QueryAndGet inline data for the requester so the injected Provider failure is observed by
        // ReplicaReader instead of being consumed as a metadata fast-path miss.
        SetUbGetSize(0);
        ConnectOptions requesterOptions;
        InitConnectOpt(2, requesterOptions, CLIENT_TIMEOUT_MS);
        requesterOptions.enableLocalCache = false;
        requester = std::make_shared<KVClient>(requesterOptions);
        DS_ASSERT_OK(requester->Init());

        DS_ASSERT_OK(inject::Set(LOCAL_OBSERVATION_INJECT, "call()"));
        DS_ASSERT_OK(inject::Set(LOCAL_READ_DENIED_INJECT, "call()"));
        DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, META_OWNER_INDEX, PROVIDER_GET_ENTER_INJECT, "call()"));
        DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, META_OWNER_INDEX, URMA_CQE_ERROR_INJECT, "1*call(0, 4)"));
    }

    void VerifyLocalObservationAndReplicaSwitch(const std::shared_ptr<KVClient> &requester, const std::string &key,
                                                const std::string &value, uint64_t &providerRequestsAfterFailure)
    {
        const uint64_t observationBaseline = inject::GetExecuteCount(LOCAL_OBSERVATION_INJECT);
        Optional<Buffer> firstBuffer;
        auto firstStatus = requester->Get(key, firstBuffer);
        ASSERT_EQ(firstStatus.GetCode(), K_URMA_ERROR) << firstStatus.ToString();
        ASSERT_FALSE(firstBuffer);
        ASSERT_EQ(inject::GetExecuteCount(LOCAL_OBSERVATION_INJECT), observationBaseline + 1);
        DS_ASSERT_OK(cluster_->GetInjectActionExecuteCount(WORKER, META_OWNER_INDEX, PROVIDER_GET_ENTER_INJECT,
                                                           providerRequestsAfterFailure));
        ASSERT_EQ(providerRequestsAfterFailure, 1u);

        const uint64_t localDenyBaseline = inject::GetExecuteCount(LOCAL_READ_DENIED_INJECT);
        Optional<Buffer> secondBuffer;
        DS_ASSERT_OK(requester->Get(key, secondBuffer));
        ASSERT_TRUE(secondBuffer);
        AssertBufferEqual(*secondBuffer, value);
        ASSERT_GT(inject::GetExecuteCount(LOCAL_READ_DENIED_INJECT), localDenyBaseline);
        uint64_t requestsAfterReplicaSwitch = 0;
        DS_ASSERT_OK(cluster_->GetInjectActionExecuteCount(WORKER, META_OWNER_INDEX, PROVIDER_GET_ENTER_INJECT,
                                                           requestsAfterReplicaSwitch));
        ASSERT_EQ(requestsAfterReplicaSwitch, providerRequestsAfterFailure);
    }

    void VerifyGlobalFactIsolation(const std::string &key, const std::string &value,
                                   uint64_t providerRequestsAfterFailure)
    {
        writer_.reset();
        DS_ASSERT_OK(inject::Set(GLOBAL_UNAVAILABLE_APPLIED_INJECT, "call()"));
        DS_ASSERT_OK(inject::Set(GLOBAL_READ_DENIED_INJECT, "call()"));
        const uint64_t appliedBaseline = inject::GetExecuteCount(GLOBAL_UNAVAILABLE_APPLIED_INJECT);
        ConnectOptions observerOptions;
        InitConnectOpt(META_OWNER_INDEX, observerOptions, CLIENT_TIMEOUT_MS);
        observerOptions.enableLocalCache = false;
        auto observer = std::make_shared<KVClient>(observerOptions);
        DS_ASSERT_OK(observer->Init());

        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while (inject::GetExecuteCount(GLOBAL_UNAVAILABLE_APPLIED_INJECT) == appliedBaseline
               && std::chrono::steady_clock::now() < deadline) {
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
        ASSERT_GT(inject::GetExecuteCount(GLOBAL_UNAVAILABLE_APPLIED_INJECT), appliedBaseline);
        const uint64_t observationsBeforeRead = inject::GetExecuteCount(LOCAL_OBSERVATION_INJECT);
        const uint64_t globalDenyBaseline = inject::GetExecuteCount(GLOBAL_READ_DENIED_INJECT);
        Optional<Buffer> observedBuffer;
        DS_ASSERT_OK(observer->Get(key, observedBuffer));
        ASSERT_TRUE(observedBuffer);
        AssertBufferEqual(*observedBuffer, value);
        ASSERT_GT(inject::GetExecuteCount(GLOBAL_READ_DENIED_INJECT), globalDenyBaseline);
        ASSERT_EQ(inject::GetExecuteCount(LOCAL_OBSERVATION_INJECT), observationsBeforeRead);

        uint64_t requestsAfterGlobalRead = 0;
        DS_ASSERT_OK(cluster_->GetInjectActionExecuteCount(WORKER, META_OWNER_INDEX, PROVIDER_GET_ENTER_INJECT,
                                                           requestsAfterGlobalRead));
        ASSERT_EQ(requestsAfterGlobalRead, providerRequestsAfterFailure);
    }

    void PrepareUbBatchReplicaScenario(const std::vector<std::string> &keys,
                                       const std::vector<std::string> &values,
                                       std::shared_ptr<KVClient> &requester)
    {
        size_t warmupBufferSize = 0;
        for (const auto &value : values) {
            warmupBufferSize += value.size();
        }
        SetUbGetSize(warmupBufferSize);
        for (size_t i = 0; i < keys.size(); ++i) {
            DS_ASSERT_OK(writer_->Set(keys[i], values[i]));
        }
        std::shared_ptr<KVClient> replicaWarmer;
        InitTestKVClient(TRANSPORT_CLIENT_WORKER_INDEX, replicaWarmer, CLIENT_TIMEOUT_MS);
        std::vector<std::string> warmedValues;
        DS_ASSERT_OK(replicaWarmer->Get(keys, warmedValues));
        ASSERT_EQ(warmedValues.size(), values.size());
        for (size_t i = 0; i < values.size(); ++i) {
            AssertTwoStrs(warmedValues[i], values[i]);
        }
        replicaWarmer.reset();

        SetUbGetSize(INLINE_DATA_LIMIT);
        ConnectOptions requesterOptions;
        InitConnectOpt(2, requesterOptions, CLIENT_TIMEOUT_MS);
        requesterOptions.enableLocalCache = false;
        requester = std::make_shared<KVClient>(requesterOptions);
        DS_ASSERT_OK(requester->Init());
        DS_ASSERT_OK(inject::Set(LOCAL_OBSERVATION_INJECT, "call()"));
        DS_ASSERT_OK(inject::Set(LOCAL_READ_DENIED_INJECT, "call()"));
        DS_ASSERT_OK(
            cluster_->SetInjectAction(WORKER, META_OWNER_INDEX, PROVIDER_BATCH_GET_ENTER_INJECT, "call()"));
        DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, META_OWNER_INDEX, URMA_CQE_ERROR_INJECT, "1*call(0, 4)"));
    }

    void VerifyBatchObservationAndReplicaSwitch(const std::shared_ptr<KVClient> &requester,
                                                const std::vector<std::string> &keys,
                                                const std::vector<std::string> &values)
    {
        const uint64_t observationBaseline = inject::GetExecuteCount(LOCAL_OBSERVATION_INJECT);
        std::vector<Optional<Buffer>> firstBuffers;
        const Status firstStatus = requester->Get(keys, firstBuffers);
        ASSERT_TRUE(firstStatus.IsOk() || firstStatus.GetCode() == K_URMA_ERROR) << firstStatus.ToString();
        ASSERT_EQ(firstBuffers.size(), keys.size());
        ASSERT_TRUE(std::any_of(firstBuffers.begin(), firstBuffers.end(), [](const auto &buffer) { return !buffer; }));
        ASSERT_GT(inject::GetExecuteCount(LOCAL_OBSERVATION_INJECT), observationBaseline);
        uint64_t providerRequests = 0;
        DS_ASSERT_OK(cluster_->GetInjectActionExecuteCount(
            WORKER, META_OWNER_INDEX, PROVIDER_BATCH_GET_ENTER_INJECT, providerRequests));
        ASSERT_EQ(providerRequests, 1u);

        const uint64_t denyBaseline = inject::GetExecuteCount(LOCAL_READ_DENIED_INJECT);
        std::vector<Optional<Buffer>> secondBuffers;
        DS_ASSERT_OK(requester->Get(keys, secondBuffers));
        ASSERT_EQ(secondBuffers.size(), values.size());
        for (size_t i = 0; i < values.size(); ++i) {
            ASSERT_TRUE(secondBuffers[i]) << "missing buffer at position " << i;
            AssertBufferEqual(*secondBuffers[i], values[i]);
        }
        ASSERT_GT(inject::GetExecuteCount(LOCAL_READ_DENIED_INJECT), denyBaseline);
        uint64_t requestsAfterSwitch = 0;
        DS_ASSERT_OK(cluster_->GetInjectActionExecuteCount(
            WORKER, META_OWNER_INDEX, PROVIDER_BATCH_GET_ENTER_INJECT, requestsAfterSwitch));
        ASSERT_EQ(requestsAfterSwitch, providerRequests);
    }
#endif

    std::unique_ptr<EtcdStore> etcd_;
    std::shared_ptr<KVClient> writer_;
    std::shared_ptr<KVClient> reader_;
    bool hadPreviousUbGetSize_ = false;
    std::string previousUbGetSize_;
    bool previousUseBrpc_ = false;
};

class KVClientTransportGetWithShmTest : public KVClientTransportGetTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        KVClientTransportGetTest::SetClusterSetupOptions(opts);
        constexpr char DISABLED_SHM_OPTION[] = "-ipc_through_shared_memory=false";
        const auto pos = opts.workerGflagParams.find(DISABLED_SHM_OPTION);
        ASSERT_NE(pos, std::string::npos);
        opts.workerGflagParams.replace(pos, sizeof(DISABLED_SHM_OPTION) - 1, "-ipc_through_shared_memory=true");
        opts.workerGflagParams += " -host_id_env_name=" + std::string(SHM_HOST_ID_ENV_NAME);
        // The reader is initially bound to Worker 1, but the object under test is routed to Worker 0.
        // Keep Worker 1 SHM-disabled to prove target capability is not inferred from the bound worker.
        opts.workerSpecifyGflagParams[TRANSPORT_CLIENT_WORKER_INDEX] = "-ipc_through_shared_memory=false";
    }

    void SetUp() override
    {
        ASSERT_EQ(setenv(SHM_HOST_ID_ENV_NAME, SHM_HOST_ID_VALUE, 1), 0);
        KVClientTransportGetTest::SetUp();
    }

    void TearDown() override
    {
        KVClientTransportGetTest::TearDown();
        (void)unsetenv(SHM_HOST_ID_ENV_NAME);
    }
};

class KVClientTransportGetDrainingRealUrmaTest : public KVClientTransportGetWithShmTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        KVClientTransportGetWithShmTest::SetClusterSetupOptions(opts);
        opts.workerGflagParams += " -enable_lossless_data_exit_mode=true";
    }

    void SetUp() override
    {
#if !defined(USE_URMA)
        GTEST_SKIP() << "Real URMA draining fallback ST requires USE_URMA.";
#elif defined(USE_URMA_MOCK)
        GTEST_SKIP() << "Real URMA draining fallback ST does not run with USE_URMA_MOCK.";
#else
        if (std::getenv("DS_URMA_DEV_NAME") == nullptr) {
            GTEST_SKIP() << "Real URMA draining fallback ST requires DS_URMA_DEV_NAME and a usable URMA device.";
        }
        setupAttempted_ = true;
        KVClientTransportGetWithShmTest::SetUp();
#endif
    }

    void TearDown() override
    {
        if (setupAttempted_) {
            KVClientTransportGetWithShmTest::TearDown();
        }
    }

private:
    bool setupAttempted_ = false;
};

class KVClientTransportGetWithTargetShmDisabledTest : public KVClientTransportGetTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        KVClientTransportGetTest::SetClusterSetupOptions(opts);
        constexpr char DISABLED_SHM_OPTION[] = "-ipc_through_shared_memory=false";
        const auto pos = opts.workerGflagParams.find(DISABLED_SHM_OPTION);
        ASSERT_NE(pos, std::string::npos);
        opts.workerGflagParams.replace(pos, sizeof(DISABLED_SHM_OPTION) - 1, "-ipc_through_shared_memory=true");
        opts.workerGflagParams += " -host_id_env_name=" + std::string(SHM_HOST_ID_ENV_NAME);
        // The reader is initially bound to SHM-enabled Worker 1, while the routed target Worker 0
        // is SHM-disabled. Target capability must be probed from Worker 0 rather than inferred from Worker 1.
        opts.workerSpecifyGflagParams[META_OWNER_INDEX] = "-ipc_through_shared_memory=false";
    }

    void SetUp() override
    {
        ASSERT_EQ(setenv(SHM_HOST_ID_ENV_NAME, SHM_HOST_ID_VALUE, 1), 0);
        KVClientTransportGetTest::SetUp();
    }

    void TearDown() override
    {
        KVClientTransportGetTest::TearDown();
        (void)unsetenv(SHM_HOST_ID_ENV_NAME);
    }
};

class KVClientTransportGetWithAllWorkersShmTest : public KVClientTransportGetTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        KVClientTransportGetTest::SetClusterSetupOptions(opts);
        constexpr char DISABLED_SHM_OPTION[] = "-ipc_through_shared_memory=false";
        const auto pos = opts.workerGflagParams.find(DISABLED_SHM_OPTION);
        ASSERT_NE(pos, std::string::npos);
        opts.workerGflagParams.replace(pos, sizeof(DISABLED_SHM_OPTION) - 1, "-ipc_through_shared_memory=true");
        opts.workerGflagParams += " -host_id_env_name=" + std::string(SHM_HOST_ID_ENV_NAME);
        opts.injectActions += ";" + std::string(WORKER_OC_GET_ENTRY_INJECT) + ":call()";
    }

    void SetUp() override
    {
        ASSERT_EQ(setenv(SHM_HOST_ID_ENV_NAME, SHM_HOST_ID_VALUE, 1), 0);
        KVClientTransportGetTest::SetUp();
    }

    void TearDown() override
    {
        KVClientTransportGetTest::TearDown();
        (void)unsetenv(SHM_HOST_ID_ENV_NAME);
    }

protected:
    std::string GetKeyWithMetaOwnerAndSameNodeWorker(uint32_t metaOwnerIndex, uint32_t sameNodeWorkerIndex)
    {
        std::string topology;
        Status status = etcd_->Get(GetTopologyTableName(), "", topology);
        if (status.IsError()) {
            ADD_FAILURE() << status.ToString();
            return {};
        }
        ClusterTopologyPb ring;
        if (!ring.ParseFromString(topology)) {
            ADD_FAILURE() << "Failed to parse topology";
            return {};
        }
        HostPort sameNodeWorker;
        status = cluster_->GetWorkerAddr(sameNodeWorkerIndex, sameNodeWorker);
        if (status.IsError()) {
            ADD_FAILURE() << status.ToString();
            return {};
        }
        std::vector<HostPort> sameNodeWorkers;
        std::map<uint32_t, std::string> tokenWorkers;
        for (uint32_t i = 0; i < cluster_->GetWorkerNum(); ++i) {
            HostPort worker;
            status = cluster_->GetWorkerAddr(i, worker);
            if (status.IsError()) {
                ADD_FAILURE() << status.ToString();
                return {};
            }
            sameNodeWorkers.emplace_back(std::move(worker));
        }
        for (const auto &worker : ring.members()) {
            for (const auto token : worker.second.tokens()) {
                tokenWorkers.emplace(token, worker.first);
            }
        }
        std::sort(sameNodeWorkers.begin(), sameNodeWorkers.end());
        const auto selected = std::find(sameNodeWorkers.begin(), sameNodeWorkers.end(), sameNodeWorker);
        if (selected == sameNodeWorkers.end()) {
            ADD_FAILURE() << "Same-node worker is absent from the topology";
            return {};
        }
        const size_t selectedIndex = static_cast<size_t>(selected - sameNodeWorkers.begin());
        HostPort expectedOwner;
        status = cluster_->GetWorkerAddr(metaOwnerIndex, expectedOwner);
        if (status.IsError()) {
            ADD_FAILURE() << status.ToString();
            return {};
        }

        for (size_t candidateIndex = 0; candidateIndex < KEY_SEARCH_LIMIT; ++candidateIndex) {
            std::string key = "transport_get_policy_" + std::to_string(candidateIndex);
            const uint32_t keyHash = MurmurHash3_32(key);
            auto owner = tokenWorkers.lower_bound(keyHash);
            if (owner == tokenWorkers.end()) {
                owner = tokenWorkers.begin();
            }
            HostPort ownerWorker;
            status = ownerWorker.ParseString(owner->second);
            if (status.IsError()) {
                ADD_FAILURE() << status.ToString();
                return {};
            }
            if (ownerWorker == expectedOwner && keyHash % sameNodeWorkers.size() == selectedIndex) {
                return key;
            }
        }
        ADD_FAILURE() << "Unable to find a key for the requested metadata owner and same-node worker";
        return {};
    }
};

class KVClientTransportGetMixedPathTest : public KVClientTransportGetTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        KVClientTransportGetTest::SetClusterSetupOptions(opts);
        constexpr char DISABLED_SHM_OPTION[] = "-ipc_through_shared_memory=false";
        const auto pos = opts.workerGflagParams.find(DISABLED_SHM_OPTION);
        ASSERT_NE(pos, std::string::npos);
        opts.workerGflagParams.replace(pos, sizeof(DISABLED_SHM_OPTION) - 1, "-ipc_through_shared_memory=true");
        for (uint32_t i = 0; i < WORKER_NUM; ++i) {
            opts.workerSpecifyGflagParams[i] += " -host_id_env_name=" + MixedHostIdEnvName(i);
        }
    }

    void SetUp() override
    {
        for (uint32_t i = 0; i < WORKER_NUM; ++i) {
            ASSERT_EQ(setenv(MixedHostIdEnvName(i).c_str(), MixedHostIdValue(i).c_str(), 1), 0);
        }
        KVClientTransportGetTest::SetUp();
    }

    void TearDown() override
    {
        KVClientTransportGetTest::TearDown();
        for (uint32_t i = 0; i < WORKER_NUM; ++i) {
            (void)unsetenv(MixedHostIdEnvName(i).c_str());
        }
    }

protected:
    uint32_t TransportClientWorkerIndex() const override
    {
        return META_OWNER_INDEX;
    }

    size_t UbInlineBufferSize() const override
    {
        return VALUE_SIZE;
    }

    void AssertMixedPathBuffers(std::vector<Optional<Buffer>> &buffers,
                                const std::vector<std::string> &values)
    {
        ASSERT_EQ(buffers.size(), 4u);
        ASSERT_FALSE(buffers[0]);
        for (size_t i = 1; i < buffers.size(); ++i) {
            ASSERT_TRUE(buffers[i]);
        }
        AssertBufferEqual(*buffers[1], values[1]);
        AssertBufferEqual(*buffers[2], values[2]);
        AssertBufferEqual(*buffers[3], values[0]);
    }

    void GetMixedPathCounts(MixedPathCounts &counts)
    {
        GetRpcCounts(counts.rpc);
        GetWorkerQueryAndGetCounts(META_OWNER_INDEX, counts.localOwner);
        GetWorkerQueryAndGetCounts(TRANSPORT_CLIENT_WORKER_INDEX, counts.remoteOwner);
    }

    void AssertMixedPathCounts(const MixedPathCounts &before, const MixedPathCounts &after)
    {
        ASSERT_EQ(after.rpc.queryAndGet, before.rpc.queryAndGet + 2);
        const uint64_t expectedDataRpcCount =
            IsUrmaBuild() ? MIXED_UB_DATA_RPC_COUNT : MIXED_TCP_DATA_RPC_COUNT;
        ASSERT_EQ(after.rpc.getObjectRemote, before.rpc.getObjectRemote + expectedDataRpcCount);
        ASSERT_EQ(after.localOwner.shmHits, before.localOwner.shmHits + 1);
        ASSERT_EQ(after.localOwner.metadataMisses, before.localOwner.metadataMisses + 1);
        ASSERT_EQ(after.remoteOwner.tcpHits, before.remoteOwner.tcpHits + (IsUrmaBuild() ? 0 : 1));
        ASSERT_EQ(after.remoteOwner.ubHits, before.remoteOwner.ubHits);
        ASSERT_EQ(after.remoteOwner.metadataMisses, before.remoteOwner.metadataMisses + 1);
    }
};

TEST_F(KVClientTransportGetWithAllWorkersShmTest, SameNodeMetadataOwnerHitUsesShmInline)
{
    std::vector<std::string> keys;
    GetRealHashKeysToWorker(META_OWNER_INDEX, 2, keys);
    ASSERT_EQ(keys.size(), 2u);
    const std::string value(VALUE_SIZE, 'a');
    DS_ASSERT_OK(writer_->Set(keys[0], value));
    DS_ASSERT_OK(writer_->Set(keys[1], value));

    Optional<Buffer> warmup;
    DS_ASSERT_OK(reader_->Get(keys[0], warmup));
    ASSERT_TRUE(warmup);
    ASSERT_EQ(AccessTransportTracker::ToString(), "SHM");

    TransportRpcCounts rpcBefore;
    WorkerQueryAndGetCounts workerBefore;
    GetRpcCounts(rpcBefore);
    GetWorkerQueryAndGetCounts(META_OWNER_INDEX, workerBefore);
    Optional<Buffer> buffer;
    DS_ASSERT_OK(reader_->Get(keys[1], buffer));
    TransportRpcCounts rpcAfter;
    WorkerQueryAndGetCounts workerAfter;
    GetRpcCounts(rpcAfter);
    GetWorkerQueryAndGetCounts(META_OWNER_INDEX, workerAfter);

    ASSERT_TRUE(buffer);
    AssertBufferEqual(*buffer, value);
    ASSERT_EQ(AccessTransportTracker::ToString(), "SHM");
    ASSERT_EQ(rpcAfter.queryAndGet, rpcBefore.queryAndGet + 1);
    ASSERT_EQ(rpcAfter.getObjectRemote, rpcBefore.getObjectRemote);
    ASSERT_EQ(rpcAfter.workerOcGet, rpcBefore.workerOcGet);
    ASSERT_EQ(rpcAfter.registerShmClient, rpcBefore.registerShmClient);
    ASSERT_EQ(rpcAfter.getClientFd, rpcBefore.getClientFd);
    ASSERT_EQ(workerAfter.tcpHits, workerBefore.tcpHits);
    ASSERT_EQ(workerAfter.ubHits, workerBefore.ubHits);
    ASSERT_EQ(workerAfter.shmHits, workerBefore.shmHits + 1);
    ASSERT_EQ(workerAfter.metadataMisses, workerBefore.metadataMisses);
}

TEST_F(KVClientTransportGetWithAllWorkersShmTest, UnavailableShmSessionFallsBackBeforeDispatch)
{
    std::vector<std::string> keys;
    GetRealHashKeysToWorker(META_OWNER_INDEX, 1, keys);
    const std::string value(VALUE_SIZE, 's');
    DS_ASSERT_OK(writer_->Set(keys.front(), value));
    const uint64_t injectCount = inject::GetExecuteCount(SHM_SESSION_UNAVAILABLE_BEFORE_BUILD_INJECT);
    DS_ASSERT_OK(inject::Set(SHM_SESSION_UNAVAILABLE_BEFORE_BUILD_INJECT, "1*call()"));
    Raii clearInject([] { (void)inject::Clear(SHM_SESSION_UNAVAILABLE_BEFORE_BUILD_INJECT); });

    Optional<Buffer> buffer;
    DS_ASSERT_OK(reader_->Get(keys.front(), buffer));

    ASSERT_TRUE(buffer);
    AssertBufferEqual(*buffer, value);
    ASSERT_EQ(AccessTransportTracker::ToString(), ExpectedTransport());
    ASSERT_EQ(inject::GetExecuteCount(SHM_SESSION_UNAVAILABLE_BEFORE_BUILD_INJECT), injectCount + 1);
}

TEST_F(KVClientTransportGetWithAllWorkersShmTest, ShmMaterializationFailureFallsBackPerKey)
{
    std::vector<std::string> keys;
    GetRealHashKeysToWorker(META_OWNER_INDEX, 1, keys);
    const std::string value(VALUE_SIZE, 'm');
    DS_ASSERT_OK(writer_->Set(keys.front(), value));
    DS_ASSERT_OK(inject::Set(SHM_MATERIALIZATION_FAILURE_INJECT, "1*return(K_RUNTIME_ERROR)"));
    Raii clearInject([] { (void)inject::Clear(SHM_MATERIALIZATION_FAILURE_INJECT); });

    Optional<Buffer> buffer;
    DS_ASSERT_OK(reader_->Get(keys.front(), buffer));

    ASSERT_TRUE(buffer);
    AssertBufferEqual(*buffer, value);
    ASSERT_EQ(AccessTransportTracker::ToString(), "SHM");
}

TEST_F(KVClientTransportGetTest, CrossNodeMetadataOwnerHitUsesUbInline)
{
#ifndef USE_URMA
    GTEST_SKIP() << "QueryAndGet UB inline ST requires USE_URMA.";
#else
    std::vector<std::string> keys;
    GetRealHashKeysToWorker(META_OWNER_INDEX, 1, keys);
    ASSERT_EQ(keys.size(), 1u);
    const std::string value(VALUE_SIZE, 'b');
    DS_ASSERT_OK(writer_->Set(keys[0], value));
    TransportRpcCounts rpcBefore;
    WorkerQueryAndGetCounts workerBefore;
    GetRpcCounts(rpcBefore);
    GetWorkerQueryAndGetCounts(META_OWNER_INDEX, workerBefore);

    Optional<Buffer> buffer;
    DS_ASSERT_OK(reader_->Get(keys[0], buffer));
    TransportRpcCounts rpcAfter;
    WorkerQueryAndGetCounts workerAfter;
    GetRpcCounts(rpcAfter);
    GetWorkerQueryAndGetCounts(META_OWNER_INDEX, workerAfter);

    ASSERT_TRUE(buffer);
    AssertBufferEqual(*buffer, value);
    ASSERT_EQ(AccessTransportTracker::ToString(), "UB");
    ASSERT_EQ(rpcAfter.queryAndGet, rpcBefore.queryAndGet + 1);
    ASSERT_EQ(rpcAfter.getObjectRemote, rpcBefore.getObjectRemote);
    ASSERT_EQ(rpcAfter.workerOcGet, rpcBefore.workerOcGet);
    ASSERT_EQ(workerAfter.tcpHits, workerBefore.tcpHits);
    ASSERT_EQ(workerAfter.ubHits, workerBefore.ubHits + 1);
    ASSERT_EQ(workerAfter.shmHits, workerBefore.shmHits);
    ASSERT_EQ(workerAfter.metadataMisses, workerBefore.metadataMisses);
#endif
}

TEST_F(KVClientTransportGetTest, CrossNodeMetadataOwnerHitUsesTcpInline)
{
#ifdef USE_URMA
    GTEST_SKIP() << "QueryAndGet TCP-only ST requires a non-URMA build.";
#else
    std::vector<std::string> keys;
    GetRealHashKeysToWorker(META_OWNER_INDEX, 1, keys);
    ASSERT_EQ(keys.size(), 1u);
    const std::string value(VALUE_SIZE, 'c');
    DS_ASSERT_OK(writer_->Set(keys[0], value));
    TransportRpcCounts rpcBefore;
    WorkerQueryAndGetCounts workerBefore;
    GetRpcCounts(rpcBefore);
    GetWorkerQueryAndGetCounts(META_OWNER_INDEX, workerBefore);

    Optional<Buffer> buffer;
    DS_ASSERT_OK(reader_->Get(keys[0], buffer));
    TransportRpcCounts rpcAfter;
    WorkerQueryAndGetCounts workerAfter;
    GetRpcCounts(rpcAfter);
    GetWorkerQueryAndGetCounts(META_OWNER_INDEX, workerAfter);

    ASSERT_TRUE(buffer);
    AssertBufferEqual(*buffer, value);
    ASSERT_EQ(AccessTransportTracker::ToString(), "TCP");
    ASSERT_EQ(rpcAfter.queryAndGet, rpcBefore.queryAndGet + 1);
    ASSERT_EQ(rpcAfter.getObjectRemote, rpcBefore.getObjectRemote);
    ASSERT_EQ(rpcAfter.workerOcGet, rpcBefore.workerOcGet);
    ASSERT_EQ(workerAfter.tcpHits, workerBefore.tcpHits + 1);
    ASSERT_EQ(workerAfter.ubHits, workerBefore.ubHits);
    ASSERT_EQ(workerAfter.shmHits, workerBefore.shmHits);
    ASSERT_EQ(workerAfter.metadataMisses, workerBefore.metadataMisses);
#endif
}

TEST_F(KVClientTransportGetWithAllWorkersShmTest, MetadataMissReadsSameNodeDataWorkerWithShm)
{
    std::vector<std::string> ownerKeys;
    std::vector<std::string> dataWorkerKeys;
    GetRealHashKeysToWorker(META_OWNER_INDEX, 2, ownerKeys);
    GetRealHashKeysToWorker(DATA_WORKER_INDEX, 1, dataWorkerKeys);
    ASSERT_EQ(ownerKeys.size(), 2u);
    ASSERT_EQ(dataWorkerKeys.size(), 1u);
    const std::string value(VALUE_SIZE, 'd');
    std::shared_ptr<KVClient> dataWriter;
    InitTestKVClient(DATA_WORKER_INDEX, dataWriter, CLIENT_TIMEOUT_MS);
    DS_ASSERT_OK(writer_->Set(ownerKeys[0], value));
    DS_ASSERT_OK(dataWriter->Set(dataWorkerKeys[0], value));
    DS_ASSERT_OK(dataWriter->Set(ownerKeys[1], value));

    Optional<Buffer> warmup;
    DS_ASSERT_OK(reader_->Get(ownerKeys[0], warmup));
    DS_ASSERT_OK(reader_->Get(dataWorkerKeys[0], warmup));
    TransportRpcCounts rpcBefore;
    WorkerQueryAndGetCounts ownerBefore;
    GetRpcCounts(rpcBefore);
    GetWorkerQueryAndGetCounts(META_OWNER_INDEX, ownerBefore);

    Optional<Buffer> buffer;
    DS_ASSERT_OK(reader_->Get(ownerKeys[1], buffer));
    TransportRpcCounts rpcAfter;
    WorkerQueryAndGetCounts ownerAfter;
    GetRpcCounts(rpcAfter);
    GetWorkerQueryAndGetCounts(META_OWNER_INDEX, ownerAfter);

    ASSERT_TRUE(buffer);
    AssertBufferEqual(*buffer, value);
    ASSERT_EQ(AccessTransportTracker::ToString(), "SHM");
    ASSERT_EQ(rpcAfter.queryAndGet, rpcBefore.queryAndGet + 1);
    ASSERT_EQ(rpcAfter.workerOcGet, rpcBefore.workerOcGet + 1);
    ASSERT_EQ(rpcAfter.getObjectRemote, rpcBefore.getObjectRemote);
    ASSERT_EQ(ownerAfter.tcpHits, ownerBefore.tcpHits);
    ASSERT_EQ(ownerAfter.ubHits, ownerBefore.ubHits);
    ASSERT_EQ(ownerAfter.shmHits, ownerBefore.shmHits);
    ASSERT_EQ(ownerAfter.metadataMisses, ownerBefore.metadataMisses + 1);
}

TEST_F(KVClientTransportGetTest, MetadataMissReadsCrossNodeDataWorker)
{
    std::vector<std::string> keys;
    GetRealHashKeysToWorker(META_OWNER_INDEX, 1, keys);
    ASSERT_EQ(keys.size(), 1u);
    const std::string value(VALUE_SIZE, 'e');
    std::shared_ptr<KVClient> dataWriter;
    InitTestKVClient(DATA_WORKER_INDEX, dataWriter, CLIENT_TIMEOUT_MS);
    DS_ASSERT_OK(dataWriter->Set(keys[0], value));
    TransportRpcCounts rpcBefore;
    WorkerQueryAndGetCounts ownerBefore;
    GetRpcCounts(rpcBefore);
    GetWorkerQueryAndGetCounts(META_OWNER_INDEX, ownerBefore);

    Optional<Buffer> buffer;
    DS_ASSERT_OK(reader_->Get(keys[0], buffer));
    TransportRpcCounts rpcAfter;
    WorkerQueryAndGetCounts ownerAfter;
    GetRpcCounts(rpcAfter);
    GetWorkerQueryAndGetCounts(META_OWNER_INDEX, ownerAfter);

    ASSERT_TRUE(buffer);
    AssertBufferEqual(*buffer, value);
    ASSERT_EQ(AccessTransportTracker::ToString(), ExpectedTransport());
    ASSERT_EQ(rpcAfter.queryAndGet, rpcBefore.queryAndGet + 1);
    ASSERT_EQ(rpcAfter.getObjectRemote, rpcBefore.getObjectRemote + 1);
    ASSERT_EQ(rpcAfter.workerOcGet, rpcBefore.workerOcGet);
    ASSERT_EQ(ownerAfter.tcpHits, ownerBefore.tcpHits);
    ASSERT_EQ(ownerAfter.ubHits, ownerBefore.ubHits);
    ASSERT_EQ(ownerAfter.shmHits, ownerBefore.shmHits);
    ASSERT_EQ(ownerAfter.metadataMisses, ownerBefore.metadataMisses + 1);
}

TEST_F(KVClientTransportGetMixedPathTest, MultiKeyMixedPathsPreserveOrder)
{
    std::vector<std::string> localOwnerKeys;
    std::vector<std::string> remoteOwnerKeys;
    GetRealHashKeysToWorker(META_OWNER_INDEX, 3, localOwnerKeys);
    GetRealHashKeysToWorker(TRANSPORT_CLIENT_WORKER_INDEX, 2, remoteOwnerKeys);
    ASSERT_EQ(localOwnerKeys.size(), 3u);
    ASSERT_EQ(remoteOwnerKeys.size(), 2u);
    std::shared_ptr<KVClient> remoteOwnerWriter;
    std::shared_ptr<KVClient> dataWriter;
    InitTestKVClient(TRANSPORT_CLIENT_WORKER_INDEX, remoteOwnerWriter, CLIENT_TIMEOUT_MS);
    InitTestKVClient(DATA_WORKER_INDEX, dataWriter, CLIENT_TIMEOUT_MS);
    const std::vector<std::string> values = { std::string(VALUE_SIZE, 'f'), std::string(VALUE_SIZE, 'g'),
                                              std::string(MIXED_OVERSIZED_INLINE_VALUE_SIZE, 'h') };
    DS_ASSERT_OK(writer_->Set(localOwnerKeys[0], values[0]));
    DS_ASSERT_OK(writer_->Set(localOwnerKeys[1], values[0]));
    DS_ASSERT_OK(dataWriter->Set(localOwnerKeys[2], values[1]));
    DS_ASSERT_OK(remoteOwnerWriter->Set(remoteOwnerKeys[0], values[2]));
    Optional<Buffer> warmup;
    DS_ASSERT_OK(reader_->Get(localOwnerKeys[0], warmup));

    MixedPathCounts before;
    GetMixedPathCounts(before);
    const std::vector<std::string> keys = { remoteOwnerKeys[1], localOwnerKeys[2], remoteOwnerKeys[0],
                                            localOwnerKeys[1] };
    std::vector<Optional<Buffer>> buffers;
    DS_ASSERT_OK(reader_->Get(keys, buffers));
    MixedPathCounts after;
    GetMixedPathCounts(after);

    AssertMixedPathBuffers(buffers, values);
    ASSERT_EQ(AccessTransportTracker::ToString(), ExpectedTransport());
    AssertMixedPathCounts(before, after);
}

// Regression: when local cache is disabled, dataPlacementPolicy is a write-only setting. Even when
// its same-node choice differs from the metadata owner, Get must enter the metadata-owner transport flow.
TEST_F(KVClientTransportGetWithAllWorkersShmTest, GetIgnoresSameNodeWritePlacementPolicy)
{
    const std::string key = GetKeyWithMetaOwnerAndSameNodeWorker(META_OWNER_INDEX, TRANSPORT_CLIENT_WORKER_INDEX);
    ASSERT_FALSE(key.empty());
    const std::string value(VALUE_SIZE, 'p');
    DS_ASSERT_OK(writer_->Set(key, value));

    for (const auto policy : { DataPlacementPolicy::PREFERRED_SAME_NODE,
                               DataPlacementPolicy::REQUIRED_SAME_NODE,
                               DataPlacementPolicy::PREFERRED_META_OWNER }) {
        reader_.reset();
        ConnectOptions options;
        InitConnectOpt(TRANSPORT_CLIENT_WORKER_INDEX, options, CLIENT_TIMEOUT_MS);
        options.enableLocalCache = false;
        options.dataPlacementPolicy = policy;
        reader_ = std::make_shared<KVClient>(options);
        DS_ASSERT_OK(reader_->Init());

        TransportRpcCounts before;
        GetRpcCounts(before);
        uint64_t metaOwnerGetBefore = 0;
        uint64_t nonOwnerGetBefore = 0;
        DS_ASSERT_OK(cluster_->GetInjectActionExecuteCount(
            WORKER, META_OWNER_INDEX, WORKER_OC_GET_ENTRY_INJECT, metaOwnerGetBefore));
        DS_ASSERT_OK(cluster_->GetInjectActionExecuteCount(
            WORKER, TRANSPORT_CLIENT_WORKER_INDEX, WORKER_OC_GET_ENTRY_INJECT, nonOwnerGetBefore));
        Optional<Buffer> buffer;
        DS_ASSERT_OK(reader_->Get(key, buffer));
        TransportRpcCounts after;
        GetRpcCounts(after);
        uint64_t metaOwnerGetAfter = 0;
        uint64_t nonOwnerGetAfter = 0;
        DS_ASSERT_OK(cluster_->GetInjectActionExecuteCount(
            WORKER, META_OWNER_INDEX, WORKER_OC_GET_ENTRY_INJECT, metaOwnerGetAfter));
        DS_ASSERT_OK(cluster_->GetInjectActionExecuteCount(
            WORKER, TRANSPORT_CLIENT_WORKER_INDEX, WORKER_OC_GET_ENTRY_INJECT, nonOwnerGetAfter));

        ASSERT_TRUE(buffer);
        AssertBufferEqual(*buffer, value);
        ASSERT_EQ(after.queryAndGet, before.queryAndGet + 1);
        ASSERT_EQ(nonOwnerGetAfter, nonOwnerGetBefore)
            << "Get must not probe a same-host non-owner before metadata-owner QueryAndGet";
        ASSERT_EQ(metaOwnerGetAfter, metaOwnerGetBefore);
        ASSERT_EQ(after.workerOcGet, before.workerOcGet);
        ASSERT_EQ(AccessTransportTracker::ToString(), "SHM");
    }
}

TEST_F(KVClientTransportGetTest, LocalCacheGetStaysOnBoundWorkerWhenCrossNodeConnectionIsEnabled)
{
    reader_.reset();
    ConnectOptions options;
    InitConnectOpt(TRANSPORT_CLIENT_WORKER_INDEX, options, CLIENT_TIMEOUT_MS, true);
    options.enableLocalCache = true;
    options.dataPlacementPolicy = DataPlacementPolicy::PREFERRED_META_OWNER;
    reader_ = std::make_shared<KVClient>(options);
    DS_ASSERT_OK(reader_->Init());

    std::vector<std::string> keys;
    GetRealHashKeysToWorker(META_OWNER_INDEX, 1, keys);
    ASSERT_EQ(keys.size(), 1u);
    const std::string value(VALUE_SIZE, 'l');
    DS_ASSERT_OK(writer_->Set(keys.front(), value));

    TransportRpcCounts before;
    GetRpcCounts(before);
    Optional<Buffer> buffer;
    DS_ASSERT_OK(reader_->Get(keys.front(), buffer));
    TransportRpcCounts after;
    GetRpcCounts(after);

    ASSERT_TRUE(buffer);
    AssertBufferEqual(*buffer, value);
    ASSERT_EQ(after.queryAndGet, before.queryAndGet);
}

TEST_F(KVClientTransportGetWithShmTest, NonBoundSameHostWorkerUsesWorkerOcFdPassing)
{
    std::vector<std::string> keys;
    GetRealHashKeysToWorker(META_OWNER_INDEX, 1, keys);
    ASSERT_EQ(keys.size(), 1u);
    const std::string value(LARGE_VALUE_SIZE, 's');
    DS_ASSERT_OK(writer_->Set(keys.front(), value));

    Optional<Buffer> localBuffer;
    DS_ASSERT_OK(writer_->Get(keys.front(), localBuffer));
    ASSERT_TRUE(localBuffer);
    AssertBufferEqual(*localBuffer, value);

    TransportRpcCounts before;
    GetRpcCounts(before);
    constexpr size_t CONCURRENT_GET_COUNT = 8;
    const std::string &key = keys.front();
    std::vector<std::future<Status>> futures;
    futures.reserve(CONCURRENT_GET_COUNT);
    for (size_t i = 0; i < CONCURRENT_GET_COUNT; ++i) {
        futures.emplace_back(std::async(std::launch::async, [this, &key, &value]() {
            Optional<Buffer> buffer;
            Status rc = reader_->Get(key, buffer);
            if (rc.IsError()) {
                return rc;
            }
            if (!buffer || buffer->GetSize() != static_cast<int64_t>(value.size()) || buffer->ImmutableData() == nullptr
                || std::memcmp(buffer->ImmutableData(), value.data(), value.size()) != 0) {
                return Status(K_RUNTIME_ERROR, "Concurrent routed SHM Get returned invalid data");
            }
            return Status::OK();
        }));
    }
    for (auto &future : futures) {
        DS_ASSERT_OK(future.get());
    }
    TransportRpcCounts after;
    GetRpcCounts(after);

    ASSERT_EQ(after.queryAndGet, before.queryAndGet + CONCURRENT_GET_COUNT);
    ASSERT_EQ(after.workerOcGet, before.workerOcGet);
    ASSERT_EQ(after.getObjectRemote, before.getObjectRemote);
    ASSERT_EQ(after.batchGetObjectRemote, before.batchGetObjectRemote);
    ASSERT_EQ(after.registerShmClient, before.registerShmClient + 1);
    ASSERT_EQ(after.getClientFd, before.getClientFd + 1);

    Optional<Buffer> reusedBuffer;
    DS_ASSERT_OK(reader_->Get(keys.front(), reusedBuffer));
    TransportRpcCounts reused;
    GetRpcCounts(reused);
    ASSERT_TRUE(reusedBuffer);
    AssertBufferEqual(*reusedBuffer, value);
    ASSERT_EQ(AccessTransportTracker::ToString(), "SHM");
    ASSERT_EQ(reused.workerOcGet, after.workerOcGet);
    ASSERT_EQ(reused.registerShmClient, after.registerShmClient);
    ASSERT_EQ(reused.getClientFd, after.getClientFd);

    std::vector<std::string> batchKeys;
    GetRealHashKeysToWorker(META_OWNER_INDEX, 4, batchKeys);
    batchKeys.erase(std::remove(batchKeys.begin(), batchKeys.end(), key), batchKeys.end());
    ASSERT_GE(batchKeys.size(), 3u);
    batchKeys.resize(3);
    std::vector<std::string> batchValues;
    for (size_t i = 0; i < batchKeys.size(); ++i) {
        batchValues.emplace_back(INLINE_DATA_LIMIT + i + 1, static_cast<char>('a' + i));
        DS_ASSERT_OK(writer_->Set(batchKeys[i], batchValues[i]));
    }
    TransportRpcCounts beforeBatch;
    GetRpcCounts(beforeBatch);
    std::vector<Optional<Buffer>> batchBuffers;
    DS_ASSERT_OK(reader_->Get(batchKeys, batchBuffers));
    TransportRpcCounts afterBatch;
    GetRpcCounts(afterBatch);
    ASSERT_EQ(batchBuffers.size(), batchKeys.size());
    for (size_t i = 0; i < batchBuffers.size(); ++i) {
        ASSERT_TRUE(batchBuffers[i]);
        AssertBufferEqual(*batchBuffers[i], batchValues[i]);
    }
    ASSERT_EQ(afterBatch.queryAndGet, beforeBatch.queryAndGet + 1);
    ASSERT_EQ(afterBatch.workerOcGet, beforeBatch.workerOcGet);
    ASSERT_EQ(afterBatch.getObjectRemote, beforeBatch.getObjectRemote);
    ASSERT_EQ(afterBatch.batchGetObjectRemote, beforeBatch.batchGetObjectRemote);
    ASSERT_EQ(afterBatch.registerShmClient, beforeBatch.registerShmClient);
    ASSERT_LE(afterBatch.getClientFd, beforeBatch.getClientFd + 1);

    constexpr auto MAINTENANCE_WAIT = std::chrono::seconds(7);
    constexpr auto POLL_INTERVAL = std::chrono::milliseconds(100);
    const auto maintenanceDeadline = std::chrono::steady_clock::now() + MAINTENANCE_WAIT;
    TransportRpcCounts maintained = afterBatch;
    while (maintained.shmHeartbeat == afterBatch.shmHeartbeat
           && std::chrono::steady_clock::now() < maintenanceDeadline) {
        std::this_thread::sleep_for(POLL_INTERVAL);
        GetRpcCounts(maintained);
    }
    ASSERT_GT(maintained.shmHeartbeat, afterBatch.shmHeartbeat);

    Optional<Buffer> postHeartbeatBuffer;
    DS_ASSERT_OK(reader_->Get(key, postHeartbeatBuffer));
    TransportRpcCounts postHeartbeat;
    GetRpcCounts(postHeartbeat);
    ASSERT_TRUE(postHeartbeatBuffer);
    AssertBufferEqual(*postHeartbeatBuffer, value);
    ASSERT_EQ(AccessTransportTracker::ToString(), "SHM");
    ASSERT_EQ(postHeartbeat.workerOcGet, maintained.workerOcGet);
    ASSERT_EQ(postHeartbeat.registerShmClient, maintained.registerShmClient);
    ASSERT_EQ(postHeartbeat.getClientFd, maintained.getClientFd);
}

TEST_F(KVClientTransportGetDrainingRealUrmaTest, DrainingTargetUsesUb)
{
    std::vector<std::string> keys;
    GetRealHashKeysToWorker(META_OWNER_INDEX, 1, keys);
    ASSERT_EQ(keys.size(), 1u);
    const std::string value(LARGE_VALUE_SIZE, 'd');
    DS_ASSERT_OK(writer_->Set(keys.front(), value));
    writer_.reset();

    uint64_t drainBaseline = 0;
    DS_ASSERT_OK(cluster_->GetInjectActionExecuteCount(WORKER, META_OWNER_INDEX, DRAIN_BEFORE_SNAPSHOT_INJECT,
                                                       drainBaseline));
    DS_ASSERT_OK(
        cluster_->SetInjectAction(WORKER, META_OWNER_INDEX, DRAIN_BEFORE_SNAPSHOT_INJECT, "1*pause()"));
    Raii releaseDrain([this] {
        (void)cluster_->ClearInjectAction(WORKER, META_OWNER_INDEX, DRAIN_BEFORE_SNAPSHOT_INJECT);
    });

    VoluntaryScaleDownInject(static_cast<int>(META_OWNER_INDEX));
    constexpr auto drainWait = std::chrono::seconds(10);
    constexpr auto pollInterval = std::chrono::milliseconds(50);
    const auto deadline = std::chrono::steady_clock::now() + drainWait;
    uint64_t drainCount = drainBaseline;
    while (drainCount == drainBaseline && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(pollInterval);
        DS_ASSERT_OK(cluster_->GetInjectActionExecuteCount(WORKER, META_OWNER_INDEX, DRAIN_BEFORE_SNAPSHOT_INJECT,
                                                           drainCount));
    }
    ASSERT_GT(drainCount, drainBaseline);

    TransportRpcCounts before;
    GetRpcCounts(before);
    Optional<Buffer> firstBuffer;
    Optional<Buffer> secondBuffer;
    DS_ASSERT_OK(reader_->Get(keys.front(), firstBuffer));
    ASSERT_EQ(AccessTransportTracker::ToString(), "UB");
    DS_ASSERT_OK(reader_->Get(keys.front(), secondBuffer));
    TransportRpcCounts after;
    GetRpcCounts(after);

    ASSERT_TRUE(firstBuffer);
    ASSERT_TRUE(secondBuffer);
    AssertBufferEqual(*firstBuffer, value);
    AssertBufferEqual(*secondBuffer, value);
    ASSERT_EQ(AccessTransportTracker::ToString(), "UB");
    ASSERT_EQ(after.queryAndGet, before.queryAndGet + 2);
    ASSERT_GE(after.registerShmClient, before.registerShmClient);
    ASSERT_LE(after.registerShmClient, before.registerShmClient + 1);
    ASSERT_EQ(after.getObjectRemote, before.getObjectRemote + 2);
}


TEST_F(KVClientTransportGetWithShmTest, PinPendingSingleAndBatchReadOnlyGetUsePageableMemory)
{
    std::vector<std::string> keys;
    GetRealHashKeysToWorker(META_OWNER_INDEX, 3, keys);
    ASSERT_EQ(keys.size(), 3u);
    std::vector<std::string> values{ std::string(LARGE_VALUE_SIZE, 'p'), std::string(LARGE_VALUE_SIZE, 'q'),
                                     std::string(LARGE_VALUE_SIZE, 'r') };
    for (size_t i = 0; i < keys.size(); ++i) {
        DS_ASSERT_OK(writer_->Set(keys[i], values[i]));
    }

    DS_ASSERT_OK(inject::Set("ShmMmapTableEntry.PinHostMemory", "1*pause()"));
    Raii clearPin([] { (void)inject::Clear("ShmMmapTableEntry.PinHostMemory"); });
    DS_ASSERT_OK(inject::Set("Buffer.AllocatePageableMemory", "call()"));
    Raii clearAlloc([] { (void)inject::Clear("Buffer.AllocatePageableMemory"); });

    Optional<ReadOnlyBuffer> singleBuffer;
    DS_ASSERT_OK(reader_->Get(keys[0], singleBuffer));
    ASSERT_TRUE(singleBuffer);
    ASSERT_EQ(singleBuffer->GetSize(), static_cast<int64_t>(values[0].size()));
    ASSERT_EQ(std::memcmp(singleBuffer->ImmutableData(), values[0].data(), values[0].size()), 0);
    ASSERT_EQ(AccessTransportTracker::ToString(), "SHM");

    std::vector<Optional<ReadOnlyBuffer>> batchBuffers;
    DS_ASSERT_OK(reader_->Get({ keys[1], keys[2] }, batchBuffers));
    ASSERT_EQ(batchBuffers.size(), 2u);
    for (size_t i = 0; i < batchBuffers.size(); ++i) {
        ASSERT_TRUE(batchBuffers[i]);
        ASSERT_EQ(batchBuffers[i]->GetSize(), static_cast<int64_t>(values[i + 1].size()));
        ASSERT_EQ(std::memcmp(batchBuffers[i]->ImmutableData(), values[i + 1].data(), values[i + 1].size()), 0);
    }
    ASSERT_EQ(AccessTransportTracker::ToString(), "SHM");
    ASSERT_GE(inject::GetExecuteCount("Buffer.AllocatePageableMemory"), 3u);
}

TEST_F(KVClientTransportGetWithTargetShmDisabledTest, BoundWorkerShmDoesNotEnableTargetWorkerShm)
{
    std::vector<std::string> keys;
    GetRealHashKeysToWorker(META_OWNER_INDEX, 1, keys);
    ASSERT_EQ(keys.size(), 1u);
    const std::string value(LARGE_VALUE_SIZE, 'd');
    DS_ASSERT_OK(writer_->Set(keys.front(), value));

    TransportRpcCounts before;
    GetRpcCounts(before);
    Optional<Buffer> buffer;
    Status rc = reader_->Get(keys.front(), buffer);
    TransportRpcCounts after;
    GetRpcCounts(after);

    DS_ASSERT_OK(rc);
    ASSERT_TRUE(buffer);
    ASSERT_GE(after.queryAndGet, before.queryAndGet);
    ASSERT_EQ(after.workerOcGet, before.workerOcGet);
    ASSERT_GE(after.getObjectRemote, before.getObjectRemote);
    ASSERT_LE(after.getObjectRemote, before.getObjectRemote + 1);
    ASSERT_EQ(after.batchGetObjectRemote, before.batchGetObjectRemote);
    ASSERT_EQ(after.registerShmClient, before.registerShmClient);
    ASSERT_EQ(after.getClientFd, before.getClientFd);
}

TEST_F(KVClientTransportGetTest, InlineHitSkipsSecondPhase)
{
    std::vector<std::string> keys;
    GetRealHashKeysToWorker(META_OWNER_INDEX, 1, keys);
    ASSERT_EQ(keys.size(), 1u);
    const std::string &key = keys.front();
    const std::string value(INLINE_DATA_LIMIT, 's');
    DS_ASSERT_OK(writer_->Set(key, value));

    TransportRpcCounts before;
    GetRpcCounts(before);
    Optional<Buffer> buffer;
    DS_ASSERT_OK(reader_->Get(key, buffer));
    TransportRpcCounts after;
    GetRpcCounts(after);

    ASSERT_TRUE(buffer);
    AssertBufferEqual(*buffer, value);
    ASSERT_EQ(AccessTransportTracker::ToString(), ExpectedTransport());
    ASSERT_EQ(after.queryAndGet, before.queryAndGet + 1);
    ASSERT_EQ(after.getObjectRemote, before.getObjectRemote);
}

// Multi-key batch returns every value in input order.
TEST_F(KVClientTransportGetTest, MultiKeyGetSameOwner)
{
    std::vector<std::string> keys;
    GetRealHashKeysToWorker(META_OWNER_INDEX, 3, keys);
    ASSERT_EQ(keys.size(), 3u);
    std::vector<std::string> values;
    for (size_t i = 0; i < keys.size(); ++i) {
        values.emplace_back(VALUE_SIZE + i * 1024, 'a' + static_cast<char>(i));
        DS_ASSERT_OK(writer_->Set(keys[i], values[i]));
    }

    std::vector<Optional<Buffer>> buffers;
    DS_ASSERT_OK(reader_->Get(keys, buffers));

    ASSERT_EQ(buffers.size(), keys.size());
    for (size_t i = 0; i < buffers.size(); ++i) {
        ASSERT_TRUE(buffers[i]);
        AssertBufferEqual(*buffers[i], values[i]);
    }
    ASSERT_EQ(AccessTransportTracker::ToString(), ExpectedTransport());
}

// Keys spanning multiple meta owners still return values in input order.
TEST_F(KVClientTransportGetTest, MultiKeyGetDifferentOwners)
{
    std::vector<std::string> localOwnerKeys;
    std::vector<std::string> remoteOwnerKeys;
    GetRealHashKeysToWorker(META_OWNER_INDEX, 4, localOwnerKeys);
    GetRealHashKeysToWorker(TRANSPORT_CLIENT_WORKER_INDEX, 4, remoteOwnerKeys);
    ASSERT_EQ(localOwnerKeys.size(), 4u);
    ASSERT_EQ(remoteOwnerKeys.size(), 4u);
    std::vector<std::string> keys;
    keys.reserve(localOwnerKeys.size() + remoteOwnerKeys.size());
    for (size_t i = 0; i < localOwnerKeys.size(); ++i) {
        keys.emplace_back(localOwnerKeys[i]);
        keys.emplace_back(remoteOwnerKeys[i]);
    }
    std::vector<std::string> values;
    for (size_t i = 0; i < keys.size(); ++i) {
        values.emplace_back(VALUE_SIZE, 'a' + static_cast<char>(i % 26));
        DS_ASSERT_OK(writer_->Set(keys[i], values[i]));
    }

    std::vector<Optional<Buffer>> buffers;
    DS_ASSERT_OK(reader_->Get(keys, buffers));

    ASSERT_EQ(buffers.size(), keys.size());
    for (size_t i = 0; i < buffers.size(); ++i) {
        ASSERT_TRUE(buffers[i]) << "missing buffer at position " << i;
        AssertBufferEqual(*buffers[i], values[i]);
    }
    ASSERT_EQ(AccessTransportTracker::ToString(), ExpectedTransport());
}

TEST_F(KVClientTransportGetTest, ProviderError4CreatesLocalObservationAndNextReadSwitchesReplica)
{
#ifndef USE_URMA
    GTEST_SKIP() << "Direct read UB admission ST requires USE_URMA.";
#else
    const std::string key = "transport_get_ub_local_observation_" + GetStringUuid();
    const std::string value(INLINE_DATA_LIMIT * 2, 'u');
    uint64_t providerRequestsAfterFailure = 0;
    std::shared_ptr<KVClient> requester;
    PrepareUbReplicaScenario(key, value, requester);
    VerifyLocalObservationAndReplicaSwitch(requester, key, value, providerRequestsAfterFailure);
    VerifyGlobalFactIsolation(key, value, providerRequestsAfterFailure);
#endif
}

TEST_F(KVClientTransportGetTest, BatchProviderError4CreatesObservationAndNextBatchSwitchesReplica)
{
#ifndef USE_URMA
    GTEST_SKIP() << "Direct Batch Read UB admission ST requires USE_URMA.";
#else
    const std::vector<std::string> keys = {
        "transport_batch_ub_observation_0_" + GetStringUuid(),
        "transport_batch_ub_observation_1_" + GetStringUuid(),
    };
    const std::vector<std::string> values = {
        std::string(INLINE_DATA_LIMIT * 2, 'a'),
        std::string(INLINE_DATA_LIMIT * 2 + 4096, 'b'),
    };
    std::shared_ptr<KVClient> requester;
    PrepareUbBatchReplicaScenario(keys, values, requester);
    VerifyBatchObservationAndReplicaSwitch(requester, keys, values);
#endif
}

// Absent object yields empty locations -> K_NOT_FOUND, no data fetch.
TEST_F(KVClientTransportGetTest, ObjectNotFound)
{
    std::vector<std::string> keys;
    GetRealHashKeysToWorker(META_OWNER_INDEX, 1, keys);
    ASSERT_EQ(keys.size(), 1u);
    Optional<Buffer> buffer;
    ASSERT_EQ(reader_->Get(keys.front(), buffer).GetCode(), StatusCode::K_NOT_FOUND);
    ASSERT_FALSE(buffer);
}

TEST_F(KVClientTransportGetTest, NonLocalMetaOwnerFallsBack)
{
    std::vector<std::string> keys;
    GetRealHashKeysToWorker(TRANSPORT_CLIENT_WORKER_INDEX, 1, keys);
    ASSERT_EQ(keys.size(), 1u);
    const std::string &key = keys.front();
    const std::string value(VALUE_SIZE, 'n');
    DS_ASSERT_OK(writer_->Set(key, value));

    TransportRpcCounts before;
    GetRpcCounts(before);
    Optional<Buffer> buffer;
    DS_ASSERT_OK(reader_->Get(key, buffer));
    TransportRpcCounts after;
    GetRpcCounts(after);

    ASSERT_TRUE(buffer);
    AssertBufferEqual(*buffer, value);
    ASSERT_EQ(AccessTransportTracker::ToString(), ExpectedTransport());
    ASSERT_EQ(after.queryAndGet, before.queryAndGet + 1);
    ASSERT_EQ(after.getObjectRemote, before.getObjectRemote + 1);
}

TEST_F(KVClientTransportGetTest, InlineEncodeFailureFallsBackPerKey)
{
    std::vector<std::string> keys;
    GetRealHashKeysToWorker(META_OWNER_INDEX, 2, keys);
    const std::vector<std::string> values = { std::string(VALUE_SIZE, 'f'), std::string(VALUE_SIZE, 'g') };
    DS_ASSERT_OK(writer_->Set(keys[0], values[0]));
    DS_ASSERT_OK(writer_->Set(keys[1], values[1]));
    DS_ASSERT_OK(cluster_->SetInjectAction(
        WORKER, META_OWNER_INDEX, QUERY_AND_GET_INLINE_FAILURE_INJECT, "1*return(K_RUNTIME_ERROR)"));
    Raii clearInject([this] {
        (void)cluster_->ClearInjectAction(WORKER, META_OWNER_INDEX, QUERY_AND_GET_INLINE_FAILURE_INJECT);
    });

    std::vector<Optional<Buffer>> buffers;
    DS_ASSERT_OK(reader_->Get(keys, buffers));

    ASSERT_EQ(buffers.size(), keys.size());
    for (size_t i = 0; i < buffers.size(); ++i) {
        ASSERT_TRUE(buffers[i]);
        AssertBufferEqual(*buffers[i], values[i]);
    }
    ASSERT_EQ(AccessTransportTracker::ToString(), ExpectedTransport());
}

// One key's data read fails while the others succeed; overall K_OK with the failed slot empty.
TEST_F(KVClientTransportGetTest, PartialDataFailure)
{
    std::vector<std::string> successKeys;
    std::vector<std::string> failedKeys;
    GetRealHashKeysToWorker(TRANSPORT_CLIENT_WORKER_INDEX, 2, SUCCESS_KEY_PREFIX, successKeys);
    GetRealHashKeysToWorker(TRANSPORT_CLIENT_WORKER_INDEX, 1, INJECT_RUNTIME_ERROR_KEY_PREFIX, failedKeys);
    const std::vector<std::string> keys = { successKeys[0], failedKeys[0], successKeys[1] };
    const std::vector<std::string> values = { std::string(VALUE_SIZE, 'a'), std::string(VALUE_SIZE, 'b'),
                                              std::string(VALUE_SIZE, 'c') };
    for (size_t i = 0; i < keys.size(); ++i) {
        DS_ASSERT_OK(writer_->Set(keys[i], values[i]));
    }

    std::vector<Optional<Buffer>> buffers;
    DS_ASSERT_OK(reader_->Get(keys, buffers));

    ASSERT_EQ(buffers.size(), keys.size());
    ASSERT_TRUE(buffers[0]);
    AssertBufferEqual(*buffers[0], values[0]);
    ASSERT_FALSE(buffers[1]);
    ASSERT_TRUE(buffers[2]);
    AssertBufferEqual(*buffers[2], values[2]);
    ASSERT_EQ(AccessTransportTracker::ToString(), ExpectedTransport());
}

TEST_F(KVClientTransportGetTest, AllKeysFailReturnFirstError)
{
    std::vector<std::string> runtimeErrorKeys;
    std::vector<std::string> notFoundKeys;
    GetRealHashKeysToWorker(TRANSPORT_CLIENT_WORKER_INDEX, 1, INJECT_RUNTIME_ERROR_KEY_PREFIX, runtimeErrorKeys);
    GetRealHashKeysToWorker(TRANSPORT_CLIENT_WORKER_INDEX, 1, INJECT_NOT_FOUND_KEY_PREFIX, notFoundKeys);
    const std::vector<std::string> keys = { runtimeErrorKeys[0], notFoundKeys[0] };
    const std::vector<std::string> values = { std::string(VALUE_SIZE, 'a'), std::string(VALUE_SIZE, 'b') };
    for (size_t i = 0; i < keys.size(); ++i) {
        DS_ASSERT_OK(writer_->Set(keys[i], values[i]));
    }

    std::vector<Optional<Buffer>> buffers;
    const auto start = std::chrono::steady_clock::now();
    const Status rc = reader_->Get(keys, buffers);
    const auto elapsedMs = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start).count();
    const auto code = rc.GetCode();
    ASSERT_TRUE(code == StatusCode::K_RUNTIME_ERROR || code == StatusCode::K_WORKER_PULL_OBJECT_NOT_FOUND
                || code == StatusCode::K_RPC_DEADLINE_EXCEEDED)
        << "unexpected status: " << rc.ToString() << ", transport: " << AccessTransportTracker::ToString()
        << ", elapsedMs: " << elapsedMs << ", buffers: " << buffers.size() << "/" << keys.size();
    ASSERT_EQ(buffers.size(), keys.size());
    for (const auto &b : buffers) {
        ASSERT_FALSE(b);
    }
    ASSERT_LT(elapsedMs, 2 * CLIENT_TIMEOUT_MS) << "failing batch ran " << elapsedMs << "ms";
}

TEST_F(KVClientTransportGetTest, LargeObjectRoundTrip)
{
    std::vector<std::string> keys;
    GetRealHashKeysToWorker(META_OWNER_INDEX, 1, keys);
    ASSERT_EQ(keys.size(), 1u);
    const std::string value(LARGE_VALUE_SIZE, 'L');
    DS_ASSERT_OK(writer_->Set(keys.front(), value));

    Optional<Buffer> buffer;
    DS_ASSERT_OK(reader_->Get(keys.front(), buffer));

    ASSERT_TRUE(buffer);
    AssertBufferEqual(*buffer, value);
    ASSERT_EQ(AccessTransportTracker::ToString(), ExpectedTransport());
}

TEST_F(KVClientTransportGetTest, InlineCapacityLimitFallsBack)
{
#ifdef USE_URMA
    reader_.reset();
    SetUbGetSize(VALUE_SIZE / 2);
    InitTransportClient();
    const size_t valueSize = VALUE_SIZE;
#else
    const size_t valueSize = INLINE_DATA_LIMIT + 1;
#endif
    std::vector<std::string> keys;
    GetRealHashKeysToWorker(META_OWNER_INDEX, 1, keys);
    ASSERT_EQ(keys.size(), 1u);
    const std::string &key = keys.front();
    const std::string value(valueSize, 'L');
    DS_ASSERT_OK(writer_->Set(key, value));

    TransportRpcCounts before;
    GetRpcCounts(before);
    Optional<Buffer> buffer;
    DS_ASSERT_OK(reader_->Get(key, buffer));
    TransportRpcCounts after;
    GetRpcCounts(after);

    ASSERT_TRUE(buffer);
    ASSERT_EQ(buffer->GetSize(), value.size());
    AssertBufferEqual(*buffer, value);
    ASSERT_EQ(AccessTransportTracker::ToString(), ExpectedTransport());
    ASSERT_EQ(after.queryAndGet, before.queryAndGet + 1);
    ASSERT_EQ(after.getObjectRemote, before.getObjectRemote + 1);
}

TEST_F(KVClientTransportGetTest, DirectBatchGetRoundTrips32Keys)
{
    const auto keys = MakeRandomKeys(32);
    std::vector<std::string> values;
    values.reserve(keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        values.emplace_back(VALUE_SIZE + i, 'a' + static_cast<char>(i % 26));
        DS_ASSERT_OK(writer_->Set(keys[i], values[i]));
    }

    std::vector<Optional<Buffer>> buffers;
    DS_ASSERT_OK(reader_->Get(keys, buffers));

    ASSERT_EQ(buffers.size(), keys.size());
    for (size_t i = 0; i < buffers.size(); ++i) {
        ASSERT_TRUE(buffers[i]) << "missing buffer at position " << i;
        AssertBufferEqual(*buffers[i], values[i]);
    }
    ASSERT_EQ(AccessTransportTracker::ToString(), ExpectedTransport());
}

TEST_F(KVClientTransportGetTest, DirectBatchGetPreservesOrderAcrossMetadataOwners)
{
    const auto keys = MakeKeysAcrossMetaOwners(4);
    ASSERT_EQ(keys.size(), cluster_->GetWorkerNum() * 4);
    std::vector<std::string> values;
    values.reserve(keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        values.emplace_back(VALUE_SIZE + i * 17, 'A' + static_cast<char>(i % 26));
        DS_ASSERT_OK(writer_->Set(keys[i], values[i]));
    }

    std::vector<Optional<Buffer>> buffers;
    DS_ASSERT_OK(reader_->Get(keys, buffers));

    ASSERT_EQ(buffers.size(), keys.size());
    for (size_t i = 0; i < buffers.size(); ++i) {
        ASSERT_TRUE(buffers[i]) << "missing buffer at position " << i;
        AssertBufferEqual(*buffers[i], values[i]);
    }
    ASSERT_EQ(AccessTransportTracker::ToString(), ExpectedTransport());
}

TEST_F(KVClientTransportGetTest, U7LargeBatchUsesStableRoutingSnapshotAcrossMetadataOwners)
{
    constexpr size_t keysPerOwner = 32;
    const auto keys = MakeKeysAcrossMetaOwners(keysPerOwner);
    ASSERT_EQ(keys.size(), cluster_->GetWorkerNum() * keysPerOwner);

    std::vector<std::string> values;
    values.reserve(keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        values.emplace_back(VALUE_SIZE + i, 'A' + static_cast<char>(i % 26));
    }
    std::vector<StringView> valueViews;
    valueViews.reserve(values.size());
    for (const auto &value : values) {
        valueViews.emplace_back(value);
    }
    std::vector<std::string> failedKeys;
    DS_ASSERT_OK(writer_->MSet(keys, valueViews, failedKeys));
    ASSERT_TRUE(failedKeys.empty());

    std::vector<Optional<Buffer>> buffers;
    DS_ASSERT_OK(reader_->Get(keys, buffers));
    ASSERT_EQ(buffers.size(), keys.size());
    for (size_t i = 0; i < buffers.size(); ++i) {
        ASSERT_TRUE(buffers[i]) << "missing buffer at position " << i;
        AssertBufferEqual(*buffers[i], values[i]);
    }
    ASSERT_EQ(AccessTransportTracker::ToString(), ExpectedTransport());
}

TEST_F(KVClientTransportGetTest, DirectBatchGetReturnsExistingValuesWithMissingSlots)
{
    auto existingKeys = MakeRandomKeys(3);
    const std::vector<std::string> values = { std::string(VALUE_SIZE, 'x'), std::string(VALUE_SIZE + 1, 'y'),
                                              std::string(VALUE_SIZE + 2, 'z') };
    for (size_t i = 0; i < existingKeys.size(); ++i) {
        DS_ASSERT_OK(writer_->Set(existingKeys[i], values[i]));
    }
    const std::vector<std::string> keys = { existingKeys[0], "missing_" + GetStringUuid(), existingKeys[1],
                                            "missing_" + GetStringUuid(), existingKeys[2] };

    std::vector<Optional<Buffer>> buffers;
    DS_ASSERT_OK(reader_->Get(keys, buffers));

    ASSERT_EQ(buffers.size(), keys.size());
    ASSERT_TRUE(buffers[0]);
    AssertBufferEqual(*buffers[0], values[0]);
    ASSERT_FALSE(buffers[1]);
    ASSERT_TRUE(buffers[2]);
    AssertBufferEqual(*buffers[2], values[1]);
    ASSERT_FALSE(buffers[3]);
    ASSERT_TRUE(buffers[4]);
    AssertBufferEqual(*buffers[4], values[2]);
    ASSERT_EQ(AccessTransportTracker::ToString(), ExpectedTransport());
}

TEST_F(KVClientTransportGetTest, DirectBatchGetAllMissingReturnsNotFound)
{
    const std::vector<std::string> keys = { "missing_first_" + GetStringUuid(),
                                            "missing_second_" + GetStringUuid(),
                                            "missing_third_" + GetStringUuid() };
    std::vector<Optional<Buffer>> buffers;

    ASSERT_EQ(reader_->Get(keys, buffers).GetCode(), StatusCode::K_NOT_FOUND);
    ASSERT_EQ(buffers.size(), keys.size());
    for (const auto &buffer : buffers) {
        ASSERT_FALSE(buffer);
    }
}

TEST_F(KVClientTransportGetTest, DirectBatchGetAllUnavailableReturnsFirstInputError)
{
    std::vector<std::string> notFoundKeys;
    std::vector<std::string> runtimeErrorKeys;
    GetRealHashKeysToWorker(TRANSPORT_CLIENT_WORKER_INDEX, 1, INJECT_NOT_FOUND_KEY_PREFIX, notFoundKeys);
    GetRealHashKeysToWorker(TRANSPORT_CLIENT_WORKER_INDEX, 1, INJECT_RUNTIME_ERROR_KEY_PREFIX, runtimeErrorKeys);
    const std::vector<std::string> keys = { notFoundKeys[0], runtimeErrorKeys[0] };
    DS_ASSERT_OK(writer_->Set(keys[0], std::string(VALUE_SIZE, 'n')));
    DS_ASSERT_OK(writer_->Set(keys[1], std::string(VALUE_SIZE, 'r')));
    std::vector<Optional<Buffer>> buffers;

    const auto start = std::chrono::steady_clock::now();
    const Status rc = reader_->Get(keys, buffers);
    const auto elapsedMs = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start).count();
    const auto code = rc.GetCode();
    ASSERT_TRUE(code == StatusCode::K_RUNTIME_ERROR || code == StatusCode::K_WORKER_PULL_OBJECT_NOT_FOUND
                || code == StatusCode::K_RPC_DEADLINE_EXCEEDED)
        << "unexpected status: " << rc.ToString() << ", transport: " << AccessTransportTracker::ToString()
        << ", elapsedMs: " << elapsedMs << ", buffers: " << buffers.size() << "/" << keys.size();
    ASSERT_EQ(buffers.size(), keys.size());
    for (const auto &buffer : buffers) {
        ASSERT_FALSE(buffer);
    }
    ASSERT_LT(elapsedMs, 2 * CLIENT_TIMEOUT_MS) << "failing batch ran " << elapsedMs << "ms";
}

TEST_F(KVClientTransportGetTest, DirectBatchGetRetriesChangedSizesWithoutCorruptingNeighbors)
{
#ifndef USE_URMA
    GTEST_SKIP() << "Direct Batch Get size-change ST requires USE_URMA.";
#else
    constexpr auto WAIT_TIMEOUT = std::chrono::seconds(5);
    constexpr auto POLL_INTERVAL = std::chrono::milliseconds(50);
    const auto keys = MakeRandomKeys(2);
    const std::vector<std::string> initialValues = { std::string(VALUE_SIZE, 'a'),
                                                     std::string(VALUE_SIZE + 1024, 'b') };
    const std::vector<std::string> updatedValues = { std::string(VALUE_SIZE + 8192, 'A'),
                                                     std::string(VALUE_SIZE + 16 * 1024, 'B') };
    for (size_t i = 0; i < keys.size(); ++i) {
        DS_ASSERT_OK(writer_->Set(keys[i], initialValues[i]));
    }

    std::vector<Optional<Buffer>> buffers;
    std::string actualTransport;
    std::promise<Status> getPromise;
    auto getFuture = getPromise.get_future();
    std::thread getThread;
    bool pauseCleared = false;
    Raii cleanup([&]() {
        if (!pauseCleared) {
            (void)inject::Clear(BATCH_GET_OBJECT_REMOTE_INJECT);
        }
        if (getThread.joinable()) {
            getThread.join();
        }
    });

    DS_ASSERT_OK(inject::Set(BATCH_GET_OBJECT_REMOTE_INJECT, "pause()"));
    const uint64_t baselineCount = inject::GetExecuteCount(BATCH_GET_OBJECT_REMOTE_INJECT);

    getThread = std::thread([&]() {
        auto rc = reader_->Get(keys, buffers);
        actualTransport = AccessTransportTracker::ToString();
        getPromise.set_value(std::move(rc));
    });

    bool dataRequestPaused = false;
    const auto deadline = std::chrono::steady_clock::now() + WAIT_TIMEOUT;
    while (std::chrono::steady_clock::now() < deadline) {
        dataRequestPaused = inject::GetExecuteCount(BATCH_GET_OBJECT_REMOTE_INJECT) > baselineCount;
        if (dataRequestPaused || getFuture.wait_for(POLL_INTERVAL) == std::future_status::ready) {
            break;
        }
    }
    ASSERT_TRUE(dataRequestPaused) << "direct Batch Get did not reach the paused client data request";

    for (size_t i = 0; i < keys.size(); ++i) {
        DS_ASSERT_OK(writer_->Set(keys[i], updatedValues[i]));
    }

    const Status clearStatus = inject::Clear(BATCH_GET_OBJECT_REMOTE_INJECT);
    ASSERT_TRUE(clearStatus.IsOk()) << clearStatus.ToString();
    pauseCleared = true;
    getThread.join();
    const Status getStatus = getFuture.get();
    DS_ASSERT_OK(getStatus);
    ASSERT_EQ(buffers.size(), keys.size());
    for (size_t i = 0; i < buffers.size(); ++i) {
        ASSERT_TRUE(buffers[i]) << "missing buffer at position " << i;
        AssertBufferEqual(*buffers[i], updatedValues[i]);
    }
    ASSERT_EQ(actualTransport, ExpectedTransport());
#endif
}

TEST_F(KVClientTransportGetTest, DirectBatchGetConcurrentOverlappingRequests)
{
    constexpr size_t KEY_COUNT = 32;
    constexpr size_t THREAD_COUNT = 4;
    constexpr size_t KEYS_PER_REQUEST = 16;
    constexpr size_t KEY_STRIDE = 4;
    const auto keys = MakeRandomKeys(KEY_COUNT);
    std::unordered_map<std::string, std::string> expected;
    for (size_t i = 0; i < keys.size(); ++i) {
        auto inserted = expected.emplace(keys[i], std::string(VALUE_SIZE + i, 'a' + static_cast<char>(i % 26)));
        DS_ASSERT_OK(writer_->Set(inserted.first->first, inserted.first->second));
    }

    struct GetResult {
        Status status;
        std::vector<std::string> keys;
        std::vector<Optional<Buffer>> buffers;
        std::string transport;
    };
    std::promise<void> ready;
    std::shared_future<void> start(ready.get_future());
    ThreadPool pool(THREAD_COUNT);
    std::vector<std::future<GetResult>> futures;
    for (size_t thread = 0; thread < THREAD_COUNT; ++thread) {
        futures.emplace_back(pool.Submit([&, thread]() {
            GetResult result;
            result.keys.reserve(KEYS_PER_REQUEST);
            for (size_t i = 0; i < KEYS_PER_REQUEST; ++i) {
                result.keys.emplace_back(keys[(thread * KEY_STRIDE + i) % keys.size()]);
            }
            start.wait();
            result.status = reader_->Get(result.keys, result.buffers);
            result.transport = AccessTransportTracker::ToString();
            return result;
        }));
    }
    ready.set_value();

    for (auto &future : futures) {
        auto result = future.get();
        DS_ASSERT_OK(result.status);
        ASSERT_EQ(result.buffers.size(), result.keys.size());
        for (size_t i = 0; i < result.keys.size(); ++i) {
            ASSERT_TRUE(result.buffers[i]) << "missing buffer at position " << i;
            AssertBufferEqual(*result.buffers[i], expected.at(result.keys[i]));
        }
        ASSERT_EQ(result.transport, ExpectedTransport());
    }
}

TEST_F(KVClientTransportGetTest, DirectBatchGetUbBufferSurvivesSiblingReset)
{
#ifndef USE_URMA
    GTEST_SKIP() << "Direct Batch Get UB owner-lifetime ST requires USE_URMA.";
#else
    constexpr size_t KEY_COUNT = 8;
    constexpr size_t SURVIVOR_INDEX = 3;
    const auto keys = MakeRandomKeys(KEY_COUNT);
    std::vector<std::string> values;
    values.reserve(keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        values.emplace_back(VALUE_SIZE + i * 4096, 'A' + static_cast<char>(i));
        DS_ASSERT_OK(writer_->Set(keys[i], values[i]));
    }

    std::vector<Optional<Buffer>> buffers;
    const Status getStatus = reader_->Get(keys, buffers);
    const std::string actualTransport = AccessTransportTracker::ToString();
    DS_ASSERT_OK(getStatus);
    ASSERT_EQ(buffers.size(), keys.size());
    for (size_t i = 0; i < buffers.size(); ++i) {
        ASSERT_TRUE(buffers[i]) << "missing buffer at position " << i;
        AssertBufferEqual(*buffers[i], values[i]);
    }

    Optional<Buffer> survivor = std::move(buffers[SURVIVOR_INDEX]);
    const std::string survivorValue = values[SURVIVOR_INDEX];
    buffers.clear();
    buffers.shrink_to_fit();

    ASSERT_TRUE(survivor);
    AssertBufferEqual(*survivor, survivorValue);
    if (actualTransport != "UB") {
        GTEST_SKIP() << "URMA runtime did not execute UB direct read: " << actualTransport;
    }
#endif
}

TEST_F(KVClientTransportGetTest, DirectBatchGetUbConcurrentOverlappingBatches)
{
#ifndef USE_URMA
    GTEST_SKIP() << "Direct Batch Get UB concurrency ST requires USE_URMA.";
#else
    constexpr size_t KEY_COUNT = 24;
    constexpr size_t THREAD_COUNT = 4;
    constexpr size_t ITERATION_COUNT = 4;
    constexpr size_t KEYS_PER_REQUEST = 12;
    constexpr size_t KEY_STRIDE = 3;
    const auto keys = MakeRandomKeys(KEY_COUNT);
    std::unordered_map<std::string, std::string> expected;
    for (size_t i = 0; i < keys.size(); ++i) {
        auto inserted = expected.emplace(keys[i], std::string(VALUE_SIZE + i * 257, 'a' + static_cast<char>(i % 26)));
        DS_ASSERT_OK(writer_->Set(inserted.first->first, inserted.first->second));
    }

    struct ConcurrentGetResult {
        std::vector<Status> statuses;
        bool contentsMatch = true;
        std::string mismatch;
        std::vector<std::string> transports;
    };
    std::promise<void> ready;
    std::shared_future<void> start(ready.get_future());
    ThreadPool pool(THREAD_COUNT);
    std::vector<std::future<ConcurrentGetResult>> futures;
    for (size_t thread = 0; thread < THREAD_COUNT; ++thread) {
        futures.emplace_back(pool.Submit([&, thread]() {
            ConcurrentGetResult result;
            result.statuses.reserve(ITERATION_COUNT);
            result.transports.reserve(ITERATION_COUNT);
            start.wait();
            for (size_t iteration = 0; iteration < ITERATION_COUNT; ++iteration) {
                std::vector<std::string> requestKeys;
                requestKeys.reserve(KEYS_PER_REQUEST);
                for (size_t i = 0; i < KEYS_PER_REQUEST; ++i) {
                    requestKeys.emplace_back(keys[(thread * KEY_STRIDE + iteration + i) % keys.size()]);
                }
                std::vector<Optional<Buffer>> buffers;
                const Status rc = reader_->Get(requestKeys, buffers);
                result.statuses.emplace_back(rc);
                result.transports.emplace_back(AccessTransportTracker::ToString());
                if (rc.IsError()) {
                    break;
                }
                if (buffers.size() != requestKeys.size()) {
                    result.contentsMatch = false;
                    result.mismatch = "result count does not match request count";
                    break;
                }
                for (size_t i = 0; i < requestKeys.size(); ++i) {
                    const auto &value = expected.at(requestKeys[i]);
                    const void *data = buffers[i] ? buffers[i]->ImmutableData() : nullptr;
                    if (!buffers[i] || buffers[i]->GetSize() != static_cast<int64_t>(value.size())
                        || data == nullptr || std::memcmp(data, value.data(), value.size()) != 0) {
                        result.contentsMatch = false;
                        result.mismatch = "value mismatch at thread " + std::to_string(thread) + ", iteration "
                                          + std::to_string(iteration) + ", position " + std::to_string(i);
                        break;
                    }
                }
                if (!result.contentsMatch) {
                    break;
                }
            }
            return result;
        }));
    }
    ready.set_value();

    std::vector<ConcurrentGetResult> results;
    results.reserve(futures.size());
    size_t ubObservationCount = 0;
    for (auto &future : futures) {
        results.emplace_back(future.get());
        ubObservationCount += static_cast<size_t>(
            std::count(results.back().transports.begin(), results.back().transports.end(), "UB"));
    }
    for (const auto &result : results) {
        for (const auto &status : result.statuses) {
            DS_ASSERT_OK(status);
        }
        ASSERT_EQ(result.statuses.size(), ITERATION_COUNT);
        ASSERT_TRUE(result.contentsMatch) << result.mismatch;
        ASSERT_EQ(result.transports.size(), ITERATION_COUNT);
    }
    if (ubObservationCount == 0) {
        GTEST_SKIP() << "URMA runtime did not execute UB in any concurrent direct-read context.";
    }
    for (const auto &result : results) {
        for (const auto &transport : result.transports) {
            ASSERT_EQ(transport, "UB") << "every concurrent Get context must exercise UB";
        }
    }
#endif
}

// Reproduces issue #749: with enable_local_cache=false the direct Get path goes through
// WorkerWorkerOCServiceImpl::GetObjectRemote, which takes the SHM read latch via ShmGuard::TryRLatch.
// When the latch cannot be acquired (write-side contention during set/eviction/migration under load),
// TryRLatch previously returned K_RUNTIME_ERROR (code=5). Because ReplicaReader treats K_RUNTIME_ERROR as
// a non-retryable location error, every direct Get surfaced code=5 to the client — matching the 100%
// get failure observed in the issue while Set stayed healthy. The latch failure is transient contention,
// so it must return K_TRY_AGAIN to let the reader retry until the API deadline.
class KVClientTransportGetShmLatchTest : public OCClientCommon, public CommonDistributedExt {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numEtcd = 1;
        opts.numWorkers = 3;
        opts.enableDistributedMaster = "true";
        // Enable SHM so the worker GetObjectRemote path exercises ShmGuard::TryRLatch.
        opts.workerGflagParams =
            " -shared_memory_size_mb=512 -ipc_through_shared_memory=true -arena_per_tenant=1"
            " -enable_urma=false";
    }

    void SetUp() override
    {
        DS_ASSERT_OK(inject::Set(SKIP_WARMUP_INJECT, "call()"));
        DS_ASSERT_OK(inject::Set(QUERY_AND_GET_INJECT, "call()"));
        DS_ASSERT_OK(inject::Set(GET_OBJECT_REMOTE_INJECT, "call()"));
        ExternalClusterTest::SetUp();
        CommonDistributedExt::InitTestEtcdInstance();

        // Reuse the base class CommonDistributedExt::etcd_ instead of shadowing it, so TearDown resets a
        // single owner. InitTestEtcdInstance is idempotent (early-returns when etcd_ != nullptr).
        ASSERT_NE(etcd_, nullptr);
        InitTestKVClient(META_OWNER_INDEX, writer_, CLIENT_TIMEOUT_MS);
        InitTransportClient();
    }

    void TearDown() override
    {
        reader_.reset();
        writer_.reset();
        etcd_.reset();
        (void)inject::Clear(SKIP_WARMUP_INJECT);
        (void)inject::Clear(QUERY_AND_GET_INJECT);
        (void)inject::Clear(GET_OBJECT_REMOTE_INJECT);
        ClearShmLatchFailureEverywhere();
        ExternalClusterTest::TearDown();
    }

protected:
    BaseCluster *GetCluster() override
    {
        return cluster_.get();
    }

    void InitTransportClient()
    {
        ConnectOptions options;
        InitConnectOpt(TRANSPORT_CLIENT_WORKER_INDEX, options, CLIENT_TIMEOUT_MS);
        options.requestTimeoutMs = SHM_LATCH_TIMEOUT_MS;
        options.enableLocalCache = false;
        reader_ = std::make_shared<KVClient>(options);
        DS_ASSERT_OK(reader_->Init());
    }

    // Pin the SHM read-latch to fail on every worker so the direct Get exercises the retryable
    // error path regardless of which worker owns the pulled object's metadata or replicas.
    void PinShmLatchFailureEverywhere()
    {
        for (uint32_t i = 0; i < cluster_->GetWorkerNum(); ++i) {
            DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, i, SHM_LATCH_FAIL_INJECT, "return()"));
        }
    }

    // Symmetric cleanup so neither the test body nor TearDown can forget to clear the inject.
    void ClearShmLatchFailureEverywhere()
    {
        for (uint32_t i = 0; i < cluster_->GetWorkerNum(); ++i) {
            (void)cluster_->ClearInjectAction(WORKER, i, SHM_LATCH_FAIL_INJECT);
        }
    }

    std::shared_ptr<KVClient> writer_;
    std::shared_ptr<KVClient> reader_;
    bool previousUseBrpc_ = false;
};

// Baseline: a direct Get with SHM enabled succeeds and returns the written value.
TEST_F(KVClientTransportGetShmLatchTest, DirectGetSucceedsWithShmEnabled)
{
    const std::string key = "shm_latch_baseline_" + GetStringUuid();
    const std::string value(VALUE_SIZE, 'x');
    DS_ASSERT_OK(writer_->Set(key, value));

    std::vector<Optional<Buffer>> buffers;
    DS_ASSERT_OK(reader_->Get({ key }, buffers));
    ASSERT_EQ(buffers.size(), 1u);
    ASSERT_TRUE(buffers[0]);
    AssertBufferEqual(*buffers[0], value);
}

// Regression: unresolved SHM read-latch contention must surface as a retryable error, never as
// K_RUNTIME_ERROR (code=5). With the latch pinned to fail, the reader retries until the API deadline
// and returns K_RPC_DEADLINE_EXCEEDED; after clearing the inject the same key reads successfully.
TEST_F(KVClientTransportGetShmLatchTest, LatchFailureIsRetryableNotRuntimeError)
{
    // Keep the data on writer worker 0 while routing metadata through worker 1, which deterministically
    // exercises worker-to-worker SHM and its read latch instead of an inline metadata-owner hit.
    const std::string key = GetObjectKeyHashToWorker(etcd_.get(), TRANSPORT_CLIENT_WORKER_INDEX);
    const std::string value(VALUE_SIZE, 'y');
    DS_ASSERT_OK(writer_->Set(key, value));

    // Inject before the first read so the target worker cannot cache a remote copy during a sanity Get.
    PinShmLatchFailureEverywhere();
    // Single-key Get is required for the K_RPC_DEADLINE_EXCEEDED assertion: ObjectReadFlow::ReadObjects
    // takes its ready.size()==1 branch (ReplicaReader::Read), where K_TRY_AGAIN stays retryable until
    // CheckDeadline/Backoff exhaust the API deadline. A multi-key request would take ReplicaReader::ReadBatch,
    // whose FinishUnresolvedWithDeadline returns lastStatus (K_TRY_AGAIN) under some states — not deadline.
    std::vector<Optional<Buffer>> buffers;
    const auto start = std::chrono::steady_clock::now();
    const Status rc = reader_->Get({ key }, buffers);
    const auto elapsedMs = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start).count();
    // With the latch pinned to fail, TryRLatch returns K_TRY_AGAIN on every replica, the reader retries
    // via CheckDeadline/Backoff until the API deadline, and ReplicaReader::Read returns
    // K_RPC_DEADLINE_EXCEEDED. K_RUNTIME_ERROR here would mean the bug is back. Accepting other retryable
    // codes would mask a regression where the reader stops exhausting the deadline, so pin the final code.
    ASSERT_NE(rc.GetCode(), StatusCode::K_RUNTIME_ERROR)
        << "direct Get must not surface SHM latch contention as K_RUNTIME_ERROR";
    ASSERT_EQ(rc.GetCode(), StatusCode::K_RPC_DEADLINE_EXCEEDED)
        << "expected deadline exhaustion after latch-contention retries, got: " << rc.ToString();
    // Wall-clock guard: the SHM attempt and transport fallback must share one API deadline. Allow one
    // extra deadline for scheduling jitter while rejecting a fallback that reinitializes the full budget.
    ASSERT_LT(elapsedMs, 2 * SHM_LATCH_TIMEOUT_MS)
        << "latch-contention Get ran " << elapsedMs << "ms, expected near the API deadline";

    ClearShmLatchFailureEverywhere();
    std::vector<Optional<Buffer>> recovered;
    DS_ASSERT_OK(reader_->Get({ key }, recovered));
    ASSERT_EQ(recovered.size(), 1u);
    ASSERT_TRUE(recovered[0]);
    AssertBufferEqual(*recovered[0], value);
}

// Regression: provider-local ERROR 4 must remain a Local Observation, while the provider's own
// lease echo must not prevent a dedicated probe from recovering that observation.
TEST_F(KVClientTransportGetTest, UbWritebackSelfObservationRecoversAfterDedicatedProbe)
{
#ifndef USE_URMA
    GTEST_SKIP() << "UB provider writeback failure ST requires USE_URMA.";
#else
    constexpr uint32_t INJECT_FAILURE_COUNT = 6;
    constexpr int32_t RECOVERY_TIMEOUT_S = 60;

    // Pin the data owner to worker0 so the reader (bound to worker1, enableLocalCache=false) exercises
    // the remote GetObjectRemote path whose provider-side UB writeback is served by worker0.
    std::vector<std::string> keys;
    GetRealHashKeysToWorker(META_OWNER_INDEX, 1, keys);
    ASSERT_EQ(keys.size(), 1u);
    const std::string &key = keys.front();
    const std::string value(VALUE_SIZE, 'q');
    DS_ASSERT_OK(writer_->Set(key, value));
    {
        std::string baseline;
        DS_ASSERT_OK(reader_->Get(key, baseline));
        ASSERT_EQ(baseline, value);
    }

    // Failure window: the provider-side UB writeback fails with cqeStatus=4 for the first N completions.
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, META_OWNER_INDEX, URMA_CQE_ERROR_INJECT,
                                           std::to_string(INJECT_FAILURE_COUNT) + "*call(0, 4)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, META_OWNER_INDEX, SELF_PROBE_SUCCEEDED_INJECT, "call()"));
    {
        std::string failedValue;
        const Status failedRc = reader_->Get(key, failedValue);
        ASSERT_TRUE(failedRc.IsError())
            << "Get during the UB failure window must fail, got: " << failedRc.ToString();
    }

    // Recovery acceptance: the provider must retain the self observation until its dedicated probe
    // succeeds, then publish writable=true and serve the remote Get again.
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(RECOVERY_TIMEOUT_S);
    bool recovered = false;
    uint64_t finalExecuted = 0;
    uint64_t selfProbeSucceeded = 0;
    while (std::chrono::steady_clock::now() < deadline) {
        std::string recoveredValue;
        const bool readable = reader_->Get(key, recoveredValue).IsOk() && recoveredValue == value;
        (void)cluster_->GetInjectActionExecuteCount(WORKER, META_OWNER_INDEX, URMA_CQE_ERROR_INJECT, finalExecuted);
        (void)cluster_->GetInjectActionExecuteCount(WORKER, META_OWNER_INDEX, SELF_PROBE_SUCCEEDED_INJECT,
                                                    selfProbeSucceeded);
        if (readable && selfProbeSucceeded > 0) {
            recovered = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }
    ASSERT_GE(finalExecuted, 1u) << "injected UB failure window was never triggered on the provider";
    ASSERT_GE(selfProbeSucceeded, 1u) << "provider self observation recovered without a dedicated probe";
    ASSERT_TRUE(recovered) << "Get did not recover after the UB data-plane failure window ended";
#endif
}

TEST_F(KVClientTransportGetTest, ProviderRecoveryProbeWorksWithoutHeartbeatHealthSummary)
{
#ifndef USE_URMA
    GTEST_SKIP() << "UB Provider recovery ST requires USE_URMA.";
#else
    constexpr int32_t RECOVERY_TIMEOUT_S = 60;
    std::vector<std::string> keys;
    GetRealHashKeysToWorker(META_OWNER_INDEX, 1, keys);
    ASSERT_EQ(keys.size(), 1u);
    const std::string &key = keys.front();
    const std::string value(VALUE_SIZE, 'r');
    std::shared_ptr<KVClient> requester;
    PrepareUbReplicaScenario(key, value, requester);
    DS_ASSERT_OK(inject::Set(PROVIDER_PROBE_RECOVERED_INJECT, "call()"));
    DS_ASSERT_OK(inject::Set(SKIP_HEARTBEAT_UB_SUMMARY_INJECT, "call()"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, META_OWNER_INDEX, PROVIDER_RECOVERY_PROBE_SUCCEEDED_INJECT,
                                           "call()"));

    const uint64_t observationBaseline = inject::GetExecuteCount(LOCAL_OBSERVATION_INJECT);
    Optional<Buffer> failedBuffer;
    const Status failedRc = requester->Get(key, failedBuffer);
    ASSERT_EQ(failedRc.GetCode(), K_URMA_ERROR) << failedRc.ToString();
    ASSERT_FALSE(failedBuffer);
    ASSERT_EQ(inject::GetExecuteCount(LOCAL_OBSERVATION_INJECT), observationBaseline + 1);
    uint64_t providerRequestsAfterFailure = 0;
    DS_ASSERT_OK(cluster_->GetInjectActionExecuteCount(WORKER, META_OWNER_INDEX, PROVIDER_GET_ENTER_INJECT,
                                                       providerRequestsAfterFailure));
    ASSERT_EQ(providerRequestsAfterFailure, 1u);

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(RECOVERY_TIMEOUT_S);
    uint64_t workerProbeSucceeded = 0;
    uint64_t providerRequestsAfterRecovery = providerRequestsAfterFailure;
    bool recovered = false;
    while (std::chrono::steady_clock::now() < deadline) {
        (void)cluster_->GetInjectActionExecuteCount(WORKER, META_OWNER_INDEX,
                                                    PROVIDER_RECOVERY_PROBE_SUCCEEDED_INJECT,
                                                    workerProbeSucceeded);
        Optional<Buffer> recoveredBuffer;
        const Status readRc = requester->Get(key, recoveredBuffer);
        (void)cluster_->GetInjectActionExecuteCount(WORKER, META_OWNER_INDEX, PROVIDER_GET_ENTER_INJECT,
                                                    providerRequestsAfterRecovery);
        if (workerProbeSucceeded > 0 && inject::GetExecuteCount(PROVIDER_PROBE_RECOVERED_INJECT) > 0
            && providerRequestsAfterRecovery > providerRequestsAfterFailure && readRc.IsOk() && recoveredBuffer) {
            AssertBufferEqual(*recoveredBuffer, value);
            recovered = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
    ASSERT_GT(inject::GetExecuteCount(SKIP_HEARTBEAT_UB_SUMMARY_INJECT), 0u)
        << "heartbeat health summaries were not explicitly suppressed";
    ASSERT_GT(workerProbeSucceeded, 0u) << "Worker did not execute its dedicated Worker-to-Client UB probe";
    ASSERT_GT(inject::GetExecuteCount(PROVIDER_PROBE_RECOVERED_INJECT), 0u)
        << "Client did not commit Provider recovery through the on-demand probe";
    ASSERT_GT(providerRequestsAfterRecovery, providerRequestsAfterFailure)
        << "Recovered Provider was not readmitted for business Get";
    ASSERT_TRUE(recovered) << "Provider remained filtered after its dedicated recovery probe succeeded";
#endif
}

}  // namespace st
}  // namespace datasystem
