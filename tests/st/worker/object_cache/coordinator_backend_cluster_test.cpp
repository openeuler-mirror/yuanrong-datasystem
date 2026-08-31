/*
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

/**
 * Description: ST for coordinator-backed worker cluster coordination.
 */
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <initializer_list>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "common.h"
#include "cluster/external_cluster.h"
#include "cluster/topology_token_helper.h"
#include "oc_client_common.h"
#include "datasystem/cluster/membership/membership_value_codec.h"
#include "datasystem/cluster/repository/topology_key_helper.h"
#include "datasystem/common/coordinator/coordinator_service_proxy.h"
#include "datasystem/common/coordinator/key_value_entry.h"
#include "datasystem/common/coordinator/static_coordinator_discovery.h"
#include "datasystem/common/flags/common_flags.h"
#include "datasystem/common/rpc/rpc_stub_cache_mgr.h"
#include "datasystem/common/util/hash_algorithm.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/protos/cluster_topology.pb.h"
#include "datasystem/protos/coordinator.pb.h"
#include "datasystem/utils/service_discovery.h"

namespace datasystem {
namespace st {
namespace {
constexpr int WAIT_TOPOLOGY_TIMEOUT_SEC = 10;
constexpr int WAIT_SCALE_TIMEOUT_SEC = 30;
constexpr int WAIT_TOPOLOGY_INTERVAL_MS = 100;
constexpr int TARGET_WORKER_COORDINATOR_BLINK_SEC = 3;
constexpr int INJECT_EXECUTION_TIMEOUT_SEC = 5;
constexpr int COORDINATOR_EVIDENCE_TIMEOUT_SEC = 30;
constexpr int WITNESS_ROUND_ROLLOVER_TIMEOUT_SEC = 12;
constexpr int REAL_FAILURE_REMOVAL_TIMEOUT_SEC = 150;
constexpr int THREE_WORKER_TEST_TIMEOUT_SEC = 240;
constexpr int COORDINATOR_LEADER_PROBE_TIMEOUT_MS = 500;
constexpr int WAIT_COORDINATOR_LEADER_TIMEOUT_SEC = 15;
constexpr int COORDINATOR_SD_CONNECT_TIMEOUT_MS = 60000;
constexpr int FAULT_REQUEST_TIMEOUT_MS = 20;
constexpr int FAULT_CONNECT_TIMEOUT_MS = 1000;
constexpr int FAULT_NODE_TIMEOUT_SEC = 3;
constexpr int FAULT_ACCESS_RECOVERY_EXPECT_MS = 3000;
constexpr int FAULT_ISOLATION_EXPECT_MS = 3000;
constexpr int FAULT_RECOVERY_TIMEOUT_MS = 12000;
constexpr int FAULT_TRAFFIC_INTERVAL_MS = 20;
constexpr int FAULT_BLINK_MAX_DURATION_MS = 2000;
constexpr int FAULT_BLINK_REPEAT = 5;
constexpr uint64_t FAULT_BLINK_KEEPALIVE_FAILURES = 1;
constexpr int FAULT_BLINK_RECOVERY_TIMEOUT_MS = 3000;
constexpr int FAULT_BLINK_TOPOLOGY_TIMEOUT_SEC = 4;
constexpr int FAULT_TOPOLOGY_RPC_TIMEOUT_MS = 500;
constexpr size_t FAULT_REQUIRED_CONSECUTIVE_SUCCESSES = 3;
constexpr size_t FAULT_REPORT_LANES_PER_WORKER = 3;
constexpr size_t FAULT_REPORT_KEY_COUNT = 32;
constexpr uint64_t FAULT_RANDOM_KEY_SEED = 20260806;
constexpr size_t FAULT_KEY_SEARCH_LIMIT = 100'000;
constexpr char FAULT_META_VALUE[] = "x";
constexpr size_t TEST_KEY_COUNT = 100;
constexpr char COORDINATOR_KEEPALIVE_INJECT_NAME[] = "CoordinationBackend.KeepAlive.returnError";
constexpr char COORDINATOR_KEEPALIVE_INJECT_ACTION[] = "return(K_RPC_UNAVAILABLE)";
constexpr char WITNESS_PROBE_FAILURE_INJECT[] = "WorkerWorkerOCServiceImpl.GetClusterState.returnError";

class CoordinatorIsolationGuard {
public:
    explicit CoordinatorIsolationGuard(BaseCluster &cluster) : cluster_(cluster)
    {
    }

    ~CoordinatorIsolationGuard()
    {
        auto rc = Clear();
        if (!rc.IsOk()) {
            LOG(ERROR) << "Failed to clear Coordinator isolation injections: " << rc;
        }
    }

    CoordinatorIsolationGuard(const CoordinatorIsolationGuard &) = delete;
    CoordinatorIsolationGuard &operator=(const CoordinatorIsolationGuard &) = delete;

    Status Start(std::initializer_list<uint32_t> workerIndexes)
    {
        CHECK_FAIL_RETURN_STATUS(workerIndexes_.empty(), K_RUNTIME_ERROR,
                                 "Coordinator isolation guard is already active");
        for (auto workerIndex : workerIndexes) {
            workerIndexes_.emplace_back(workerIndex);
            auto rc = cluster_.SetInjectAction(WORKER, workerIndex, COORDINATOR_KEEPALIVE_INJECT_NAME,
                                               COORDINATOR_KEEPALIVE_INJECT_ACTION);
            if (!rc.IsOk()) {
                return Rollback(rc);
            }
        }

        auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(INJECT_EXECUTION_TIMEOUT_SEC);
        while (std::chrono::steady_clock::now() < deadline) {
            bool allExecuted = true;
            for (auto workerIndex : workerIndexes_) {
                uint64_t executeCount = 0;
                auto rc = cluster_.GetInjectActionExecuteCount(WORKER, workerIndex, COORDINATOR_KEEPALIVE_INJECT_NAME,
                                                               executeCount);
                if (!rc.IsOk() || executeCount == 0) {
                    allExecuted = false;
                    break;
                }
            }
            if (allExecuted) {
                return Status::OK();
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(WAIT_TOPOLOGY_INTERVAL_MS));
        }
        return Rollback(Status(K_RUNTIME_ERROR, "Timed out waiting for Coordinator isolation injection execution"));
    }

    Status Clear()
    {
        Status firstError = Status::OK();
        for (auto workerIndex : workerIndexes_) {
            auto rc = cluster_.ClearInjectAction(WORKER, workerIndex, COORDINATOR_KEEPALIVE_INJECT_NAME);
            if (!rc.IsOk() && firstError.IsOk()) {
                firstError = rc;
            }
        }
        workerIndexes_.clear();
        return firstError;
    }

    void ReleaseWithoutClear()
    {
        workerIndexes_.clear();
    }

private:
    Status Rollback(const Status &cause)
    {
        auto clearRc = Clear();
        if (!clearRc.IsOk()) {
            LOG(ERROR) << "Failed to roll back Coordinator isolation injections: " << clearRc;
        }
        return cause;
    }

    BaseCluster &cluster_;
    std::vector<uint32_t> workerIndexes_;
};

constexpr size_t STALE_TOPOLOGY_MEMBER_COUNT = 4;

enum class CoordinationBackendType : uint8_t {
    ETCD,
    COORDINATOR,
};

std::string CoordinationBackendName(const testing::TestParamInfo<CoordinationBackendType> &info)
{
    return info.param == CoordinationBackendType::ETCD ? "Etcd" : "Coordinator";
}

std::string WorkerStateToString(MembershipPb::StatePb state)
{
    switch (state) {
        case MembershipPb::INITIAL:
            return "INITIAL";
        case MembershipPb::JOINING:
            return "JOINING";
        case MembershipPb::ACTIVE:
            return "ACTIVE";
        case MembershipPb::PRE_LEAVING:
            return "PRE_LEAVING";
        case MembershipPb::LEAVING:
            return "LEAVING";
        case MembershipPb::FAILED:
            return "FAILED";
        default:
            return "UNKNOWN(" + std::to_string(static_cast<int>(state)) + ")";
    }
}

std::string WorkerStatesToString(const std::map<std::string, MembershipPb::StatePb> &workers)
{
    std::vector<std::string> workerStates;
    workerStates.reserve(workers.size());
    for (const auto &worker : workers) {
        workerStates.emplace_back(worker.first + ":" + WorkerStateToString(worker.second));
    }
    return VectorToString(workerStates);
}

std::string AddressesToString(const std::set<std::string> &addresses)
{
    return VectorToString(std::vector<std::string>(addresses.begin(), addresses.end()));
}

}  // namespace

class CoordinatorBackendClusterTest : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numEtcd = 0;
        opts.numCoordinators = 1;
        opts.numWorkers = 2;
        opts.isObjectCache = true;
        opts.enableDistributedMaster = "true";
        opts.masterIdx = 0;
        opts.waitWorkerReady = true;
        opts.waitAfterStart = false;
        opts.disableRocksDB = true;
        opts.workerGflagParams =
            " -shared_memory_size_mb=64 -node_timeout_s=2 -node_dead_timeout_s=4 -add_node_wait_time_s=1"
            " -log_async=false -enable_reconciliation=false -enable_lossless_data_exit_mode=true";
        opts.coordinatorGflagParams = " -v=1 -node_timeout_s=1 -node_dead_timeout_s=2 -scale_in_collect_window_ms=1000";
        coordinatorCount_ = opts.numCoordinators;
    }

protected:
    Status GetCoordinatorAddressList(std::string &addresses, uint32_t firstIndex = 0)
    {
        addresses.clear();
        auto *externalCluster = dynamic_cast<ExternalCluster *>(cluster_.get());
        CHECK_FAIL_RETURN_STATUS(externalCluster != nullptr, K_RUNTIME_ERROR, "Not an ExternalCluster");
        for (uint32_t offset = 0; offset < coordinatorCount_; ++offset) {
            const auto index = (firstIndex + offset) % coordinatorCount_;
            HostPort coordinatorAddr;
            RETURN_IF_NOT_OK(externalCluster->GetCoordinatorAddr(index, coordinatorAddr));
            if (!addresses.empty()) {
                addresses += ",";
            }
            addresses += coordinatorAddr.ToString();
        }
        return Status::OK();
    }

    Status CreateCoordinatorProxy(const std::string &serviceAddress,
                                  std::unique_ptr<ICoordinatorServiceProxy> &coordinatorProxy)
    {
        RETURN_IF_NOT_OK(RpcStubCacheMgr::Instance().Init(100));
        auto coordinatorDiscovery = std::make_shared<StaticCoordinatorDiscovery>(serviceAddress);
        coordinatorProxy = std::make_unique<CoordinatorServiceProxyBrpcImpl>(std::move(coordinatorDiscovery));
        return coordinatorProxy->Init();
    }

    Status GetCoordinatorProxy()
    {
        if (coordinatorProxy_ != nullptr) {
            return Status::OK();
        }
        auto *externalCluster = dynamic_cast<ExternalCluster *>(cluster_.get());
        CHECK_FAIL_RETURN_STATUS(externalCluster != nullptr, K_RUNTIME_ERROR, "Not an ExternalCluster");

        uint32_t leaderIndex = 0;
        RETURN_IF_NOT_OK(WaitServingCoordinator(leaderIndex, WAIT_SCALE_TIMEOUT_SEC));
        HostPort coordinatorAddr;
        RETURN_IF_NOT_OK(externalCluster->GetCoordinatorAddr(leaderIndex, coordinatorAddr));
        RETURN_IF_NOT_OK(CreateCoordinatorProxy(coordinatorAddr.ToString(), coordinatorProxy_));
        return Status::OK();
    }

    Status ReadMembershipStates(std::map<std::string, cluster::MemberLifecycleState> &states)
    {
        RETURN_IF_NOT_OK(GetCoordinatorProxy());
        CHECK_FAIL_RETURN_STATUS(coordinatorProxy_ != nullptr, K_RUNTIME_ERROR, "Coordinator proxy is null");
        std::unique_ptr<cluster::TopologyKeyHelper> topologyKeys;
        RETURN_IF_NOT_OK(cluster::TopologyKeyHelper::Create(GetTestClusterName(), topologyKeys));
        const std::string prefix = topologyKeys->MembershipTable() + "/";
        const std::string rangeEnd = StringPlusOne(prefix);
        CHECK_FAIL_RETURN_STATUS(!rangeEnd.empty(), K_RUNTIME_ERROR, "Failed to build membership range end");

        std::vector<KeyValueEntry> kvs;
        int64_t revision = 0;
        RETURN_IF_NOT_OK(coordinatorProxy_->Range(prefix, rangeEnd, kvs, revision,
                                                   DEFAULT_COORDINATOR_RPC_TIMEOUT_MS));
        states.clear();
        for (const auto &entry : kvs) {
            CHECK_FAIL_RETURN_STATUS(entry.key.rfind(prefix, 0) == 0 && entry.key.size() > prefix.size(),
                                     K_RUNTIME_ERROR, "Unexpected membership key: " + entry.key);
            cluster::MembershipValue value;
            RETURN_IF_NOT_OK(cluster::MembershipValueCodec::Decode(entry.value, value));
            states.emplace(entry.key.substr(prefix.size()), value.lifecycleState);
        }
        return Status::OK();
    }

    Status WaitForReadyMemberships(std::initializer_list<uint32_t> workerIndexes, int timeoutSec,
                                   size_t consecutiveSnapshots = 1)
    {
        std::set<std::string> expectedWorkers;
        for (auto workerIndex : workerIndexes) {
            HostPort workerAddress;
            RETURN_IF_NOT_OK(cluster_->GetWorkerAddr(workerIndex, workerAddress));
            expectedWorkers.emplace(workerAddress.ToString());
        }

        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(timeoutSec);
        Status lastRc(K_RUNTIME_ERROR, "Membership state has not been read");
        std::map<std::string, cluster::MemberLifecycleState> lastStates;
        size_t readySnapshots = 0;
        while (std::chrono::steady_clock::now() < deadline) {
            lastRc = ReadMembershipStates(lastStates);
            const bool allReady = lastRc.IsOk()
                                  && std::all_of(expectedWorkers.begin(), expectedWorkers.end(),
                                                 [&lastStates](const std::string &address) {
                                                     const auto found = lastStates.find(address);
                                                     return found != lastStates.end()
                                                            && found->second
                                                                   == cluster::MemberLifecycleState::READY;
                                                 });
            readySnapshots = allReady ? readySnapshots + 1 : 0;
            if (readySnapshots >= consecutiveSnapshots) {
                return Status::OK();
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(WAIT_TOPOLOGY_INTERVAL_MS));
        }
        return Status(K_RUNTIME_ERROR, "Timed out waiting for READY membership snapshots; last status: "
                                           + lastRc.ToString());
    }

    Status GetTopologyWorkers(ICoordinatorServiceProxy &proxy, std::map<std::string, MembershipPb::StatePb> &outWorkers,
                              int64_t timeoutMs = DEFAULT_COORDINATOR_RPC_TIMEOUT_MS)
    {
        std::unique_ptr<cluster::TopologyKeyHelper> topologyKeys;
        RETURN_IF_NOT_OK(cluster::TopologyKeyHelper::Create(GetTestClusterName(), topologyKeys));

        std::vector<KeyValueEntry> kvs;
        int64_t revision = 0;
        RETURN_IF_NOT_OK(proxy.Range(topologyKeys->TopologyTable() + "/", "", kvs, revision, timeoutMs));
        CHECK_FAIL_RETURN_STATUS(kvs.size() == 1, K_RUNTIME_ERROR,
                                 "Unexpected topology entry count: " + std::to_string(kvs.size()));

        ClusterTopologyPb topology;
        CHECK_FAIL_RETURN_STATUS(topology.ParseFromString(kvs.front().value), K_RUNTIME_ERROR,
                                 "Failed to parse ClusterTopologyPb from coordinator backend");

        outWorkers.clear();
        for (const auto &worker : topology.members()) {
            outWorkers.emplace(worker.first, worker.second.state());
        }
        LOG(INFO) << "ClusterTopologyPb:" << topology.ShortDebugString();
        return Status::OK();
    }

    Status WaitWorkersInCluster(ICoordinatorServiceProxy &proxy, const std::set<std::string> &expectedWorkers,
                                int timeoutSec = WAIT_TOPOLOGY_TIMEOUT_SEC)
    {
        auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(timeoutSec);
        Status lastRc = Status::OK();
        std::map<std::string, MembershipPb::StatePb> lastWorkers;
        while (std::chrono::steady_clock::now() < deadline) {
            lastRc = GetTopologyWorkers(proxy, lastWorkers);
            if (lastRc.IsOk() && lastWorkers.size() == expectedWorkers.size()) {
                bool allExpectedWorkersActive = true;
                for (const auto &worker : expectedWorkers) {
                    auto iter = lastWorkers.find(worker);
                    if (iter == lastWorkers.end() || iter->second != MembershipPb::ACTIVE) {
                        allExpectedWorkersActive = false;
                        break;
                    }
                }
                if (allExpectedWorkersActive) {
                    return Status::OK();
                }
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(WAIT_TOPOLOGY_INTERVAL_MS));
        }
        return Status(K_RUNTIME_ERROR,
                      "Timed out waiting for expected active workers in coordinator topology. "
                      "Expected: "
                          + VectorToString(std::vector<std::string>(expectedWorkers.begin(), expectedWorkers.end()))
                          + ", last workers: " + WorkerStatesToString(lastWorkers)
                          + ", last status: " + lastRc.ToString());
    }

    Status WaitWorkersNotInCluster(ICoordinatorServiceProxy &proxy, const std::set<std::string> &unexpectedWorkers,
                                   int timeoutSec = WAIT_TOPOLOGY_TIMEOUT_SEC)
    {
        auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(timeoutSec);
        Status lastRc = Status::OK();
        std::map<std::string, MembershipPb::StatePb> lastWorkers;
        while (std::chrono::steady_clock::now() < deadline) {
            lastRc = GetTopologyWorkers(proxy, lastWorkers);
            if (lastRc.IsOk()) {
                bool allUnexpectedWorkersAbsent = true;
                for (const auto &worker : unexpectedWorkers) {
                    if (lastWorkers.find(worker) != lastWorkers.end()) {
                        allUnexpectedWorkersAbsent = false;
                        break;
                    }
                }
                if (allUnexpectedWorkersAbsent) {
                    return Status::OK();
                }
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(WAIT_TOPOLOGY_INTERVAL_MS));
        }
        return Status(K_RUNTIME_ERROR,
                      "Timed out waiting for unexpected workers to leave coordinator topology. "
                      "Unexpected: "
                          + VectorToString(std::vector<std::string>(unexpectedWorkers.begin(), unexpectedWorkers.end()))
                          + ", last workers: " + WorkerStatesToString(lastWorkers)
                          + ", last status: " + lastRc.ToString());
    }

    std::vector<std::string> BuildKeys(const std::string &prefix)
    {
        std::vector<std::string> keys;
        keys.reserve(TEST_KEY_COUNT);
        for (size_t i = 0; i < TEST_KEY_COUNT; ++i) {
            keys.emplace_back(prefix + "_key_" + std::to_string(i));
        }
        return keys;
    }

    std::unordered_map<std::string, std::string> BuildValues(const std::vector<std::string> &keys,
                                                             const std::string &prefix)
    {
        std::unordered_map<std::string, std::string> values;
        values.reserve(keys.size());
        for (size_t i = 0; i < keys.size(); ++i) {
            values.emplace(keys[i], prefix + "_value_" + std::to_string(i));
        }
        return values;
    }

    Status SetKeyEventually(KVClient &client, const std::string &key, const std::string &value,
                            int timeoutSec = WAIT_SCALE_TIMEOUT_SEC)
    {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(timeoutSec);
        Status lastRc(K_RUNTIME_ERROR, "Set has not been attempted");
        while (std::chrono::steady_clock::now() < deadline) {
            lastRc = client.Set(key, value);
            if (lastRc.IsOk()) {
                return Status::OK();
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(WAIT_TOPOLOGY_INTERVAL_MS));
        }
        return Status(K_RUNTIME_ERROR, "Timed out waiting for Set to succeed for key " + key
                                           + ", last status: " + lastRc.ToString());
    }

    Status SetKeys(KVClient &client, const std::vector<std::string> &keys,
                   const std::unordered_map<std::string, std::string> &values)
    {
        for (const auto &key : keys) {
            auto iter = values.find(key);
            CHECK_FAIL_RETURN_STATUS(iter != values.end(), K_RUNTIME_ERROR, "Missing expected value for key " + key);
            RETURN_IF_NOT_OK(client.Set(key, iter->second));
        }
        return Status::OK();
    }

    Status SetKeysEventually(KVClient &client, const std::vector<std::string> &keys,
                             const std::unordered_map<std::string, std::string> &values)
    {
        for (const auto &key : keys) {
            auto iter = values.find(key);
            CHECK_FAIL_RETURN_STATUS(iter != values.end(), K_RUNTIME_ERROR, "Missing expected value for key " + key);
            RETURN_IF_NOT_OK(SetKeyEventually(client, key, iter->second));
        }
        return Status::OK();
    }

    Status GetAndCheckKeys(KVClient &client, const std::vector<std::string> &keys,
                           const std::unordered_map<std::string, std::string> &values)
    {
        for (const auto &key : keys) {
            auto iter = values.find(key);
            CHECK_FAIL_RETURN_STATUS(iter != values.end(), K_RUNTIME_ERROR, "Missing expected value for key " + key);
            std::string value;
            RETURN_IF_NOT_OK(client.Get(key, value));
            std::string errorMsg = "Unexpected value for key ";
            errorMsg.append(key).append(", expected ").append(iter->second).append(", actual ").append(value);
            CHECK_FAIL_RETURN_STATUS(value == iter->second, K_RUNTIME_ERROR, errorMsg);
        }
        return Status::OK();
    }

    void AssertSetKeys(std::shared_ptr<KVClient> &client, const std::vector<std::string> &keys,
                       const std::unordered_map<std::string, std::string> &values)
    {
        ASSERT_NE(client, nullptr);
        DS_ASSERT_OK(SetKeys(*client, keys, values));
    }

    void AssertSetKeysEventually(std::shared_ptr<KVClient> &client, const std::vector<std::string> &keys,
                                 const std::unordered_map<std::string, std::string> &values)
    {
        ASSERT_NE(client, nullptr);
        DS_ASSERT_OK(SetKeysEventually(*client, keys, values));
    }

    void AssertGetKeysEventually(std::shared_ptr<KVClient> &client, const std::vector<std::string> &keys,
                                 const std::unordered_map<std::string, std::string> &values)
    {
        ASSERT_NE(client, nullptr);
        DS_ASSERT_OK(cluster_->WaitForExpectedResult(
            [this, &client, &keys, &values]() {
                (void)this;
                return GetAndCheckKeys(*client, keys, values);
            },
            WAIT_SCALE_TIMEOUT_SEC, StatusCode::K_OK));
    }

    void InitKVClient(uint32_t workerIndex, std::shared_ptr<KVClient> &client)
    {
        InitTestKVClient(workerIndex, client);
    }

    void InitKVClientWithCoordinatorServiceDiscovery(std::shared_ptr<KVClient> &client)
    {
        uint32_t leaderIndex = 0;
        DS_ASSERT_OK(WaitServingCoordinator(leaderIndex));
        CoordinatorServiceDiscoveryOptions discoveryOptions;
        DS_ASSERT_OK(GetCoordinatorAddressList(discoveryOptions.serviceAddress, leaderIndex));
        discoveryOptions.clusterName = GetTestClusterName();
        discoveryOptions.affinityPolicy = ServiceAffinityPolicy::RANDOM;
        auto serviceDiscovery = std::make_shared<CoordinatorServiceDiscovery>(discoveryOptions);
        DS_ASSERT_OK(serviceDiscovery->Init());

        ConnectOptions connectOptions;
        connectOptions.connectTimeoutMs = COORDINATOR_SD_CONNECT_TIMEOUT_MS;
        connectOptions.accessKey = "QTWAOYTTINDUT2QVKYUC";
        connectOptions.secretKey = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
        connectOptions.serviceDiscovery = serviceDiscovery;
        client = std::make_shared<KVClient>(connectOptions);
        DS_ASSERT_OK(client->Init());
    }

    Status WaitServingCoordinator(uint32_t &leaderIndex, int timeoutSec = WAIT_COORDINATOR_LEADER_TIMEOUT_SEC,
                                  int excludedIndex = -1)
    {
        auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(timeoutSec);
        Status lastRc = Status::OK();
        while (std::chrono::steady_clock::now() < deadline) {
            for (uint32_t i = 0; i < coordinatorCount_; ++i) {
                if (static_cast<int>(i) == excludedIndex) {
                    continue;
                }
                auto *externalCluster = dynamic_cast<ExternalCluster *>(cluster_.get());
                CHECK_FAIL_RETURN_STATUS(externalCluster != nullptr, K_RUNTIME_ERROR, "Not an ExternalCluster");
                HostPort coordinatorAddr;
                RETURN_IF_NOT_OK(externalCluster->GetCoordinatorAddr(i, coordinatorAddr));
                std::unique_ptr<ICoordinatorServiceProxy> proxy;
                auto initRc = CreateCoordinatorProxy(coordinatorAddr.ToString(), proxy);
                if (initRc.IsError()) {
                    lastRc = initRc;
                    continue;
                }
                std::string coordinatorId;
                lastRc = proxy->GetCoordinatorId(coordinatorId, DEFAULT_COORDINATOR_RPC_TIMEOUT_MS);
                if (lastRc.IsError()) {
                    continue;
                }
                std::map<std::string, MembershipPb::StatePb> workers;
                lastRc = GetTopologyWorkers(*proxy, workers);
                if (lastRc.IsOk()) {
                    leaderIndex = i;
                    return Status::OK();
                }
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(WAIT_TOPOLOGY_INTERVAL_MS));
        }
        return Status(K_RUNTIME_ERROR, "Timed out waiting for serving coordinator, last status: " + lastRc.ToString());
    }

    void AssertCoordinatorLeaderElected()
    {
        uint32_t leaderIndex = 0;
        DS_ASSERT_OK(WaitServingCoordinator(leaderIndex));
        LOG(INFO) << "Serving coordinator leader index: " << leaderIndex;
    }

    void ShutdownCurrentCoordinatorLeader()
    {
        uint32_t oldLeaderIndex = 0;
        DS_ASSERT_OK(WaitServingCoordinator(oldLeaderIndex));
        DS_ASSERT_OK(cluster_->ShutdownNode(COORDINATOR, oldLeaderIndex));
        coordinatorProxy_.reset();
        uint32_t newLeaderIndex = 0;
        DS_ASSERT_OK(WaitServingCoordinator(newLeaderIndex, WAIT_SCALE_TIMEOUT_SEC, static_cast<int>(oldLeaderIndex)));
        EXPECT_NE(oldLeaderIndex, newLeaderIndex);
        LOG(INFO) << "Coordinator leader switched from " << oldLeaderIndex << " to " << newLeaderIndex;
    }

    void AssertWorkersInCluster(const std::vector<int> &workerIndexes, int timeoutSec = WAIT_SCALE_TIMEOUT_SEC)
    {
        std::set<std::string> expectedWorkers;
        for (int workerIndex : workerIndexes) {
            HostPort worker;
            DS_ASSERT_OK(cluster_->GetWorkerAddr(workerIndex, worker));
            expectedWorkers.insert(worker.ToString());
        }
        DS_ASSERT_OK(GetCoordinatorProxy());
        ASSERT_NE(coordinatorProxy_, nullptr);
        DS_ASSERT_OK(WaitWorkersInCluster(*coordinatorProxy_, expectedWorkers, timeoutSec));
    }

    void AssertWorkersNotInCluster(const std::vector<int> &workerIndexes, int timeoutSec = WAIT_SCALE_TIMEOUT_SEC)
    {
        std::set<std::string> unexpectedWorkers;
        for (int workerIndex : workerIndexes) {
            HostPort worker;
            DS_ASSERT_OK(cluster_->GetWorkerAddr(workerIndex, worker));
            unexpectedWorkers.insert(worker.ToString());
        }
        DS_ASSERT_OK(GetCoordinatorProxy());
        ASSERT_NE(coordinatorProxy_, nullptr);
        DS_ASSERT_OK(WaitWorkersNotInCluster(*coordinatorProxy_, unexpectedWorkers, timeoutSec));
    }

    Status AddWorkerAndWaitReady(uint32_t workerIndex, HostPort &workerAddr)
    {
        auto *externalCluster = dynamic_cast<ExternalCluster *>(cluster_.get());
        CHECK_FAIL_RETURN_STATUS(externalCluster != nullptr, K_RUNTIME_ERROR, "Not an ExternalCluster");
        HostPort masterAddr;
        RETURN_IF_NOT_OK(cluster_->GetWorkerAddr(0, masterAddr));
        workerAddr = HostPort("127.0.0.1", GetFreePort());
        const int directPort = GetFreePort();
        RETURN_IF_NOT_OK(externalCluster->AddNode(masterAddr, workerAddr.ToString(), directPort));
        return cluster_->WaitNodeReady(WORKER, workerIndex, WAIT_SCALE_TIMEOUT_SEC);
    }

    uint32_t coordinatorCount_ = 1;
    std::unique_ptr<ICoordinatorServiceProxy> coordinatorProxy_;
};

class CoordinatorBackendClusterThreeWorkerTest : public CoordinatorBackendClusterTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        CoordinatorBackendClusterTest::SetClusterSetupOptions(opts);
        opts.numWorkers = 3;
    }

protected:
    int GetTestCaseTimeoutSecs() const override
    {
        return THREE_WORKER_TEST_TIMEOUT_SEC;
    }

    Status GetWorkerAddresses(std::initializer_list<uint32_t> workerIndexes, std::set<std::string> &addresses)
    {
        addresses.clear();
        for (auto workerIndex : workerIndexes) {
            HostPort workerAddress;
            RETURN_IF_NOT_OK(cluster_->GetWorkerAddr(workerIndex, workerAddress));
            addresses.emplace(workerAddress.ToString());
        }
        return Status::OK();
    }

    Status ReadMembershipAddresses(std::set<std::string> &addresses)
    {
        RETURN_IF_NOT_OK(GetCoordinatorProxy());
        CHECK_FAIL_RETURN_STATUS(coordinatorProxy_ != nullptr, K_RUNTIME_ERROR, "Coordinator proxy is null");
        std::unique_ptr<cluster::TopologyKeyHelper> topologyKeys;
        RETURN_IF_NOT_OK(cluster::TopologyKeyHelper::Create(GetTestClusterName(), topologyKeys));
        const std::string prefix = topologyKeys->MembershipTable() + "/";
        const std::string rangeEnd = StringPlusOne(prefix);
        CHECK_FAIL_RETURN_STATUS(!rangeEnd.empty(), K_RUNTIME_ERROR, "Failed to build membership range end");

        std::vector<KeyValueEntry> kvs;
        int64_t revision = 0;
        RETURN_IF_NOT_OK(coordinatorProxy_->Range(prefix, rangeEnd, kvs, revision, DEFAULT_COORDINATOR_RPC_TIMEOUT_MS));
        addresses.clear();
        for (const auto &entry : kvs) {
            CHECK_FAIL_RETURN_STATUS(entry.key.rfind(prefix, 0) == 0 && entry.key.size() > prefix.size(),
                                     K_RUNTIME_ERROR, "Unexpected membership key: " + entry.key);
            addresses.emplace(entry.key.substr(prefix.size()));
        }
        return Status::OK();
    }

    Status WaitForMembershipLayout(std::initializer_list<uint32_t> presentWorkerIndexes,
                                   std::initializer_list<uint32_t> absentWorkerIndexes)
    {
        std::set<std::string> expectedPresent;
        std::set<std::string> expectedAbsent;
        RETURN_IF_NOT_OK(GetWorkerAddresses(presentWorkerIndexes, expectedPresent));
        RETURN_IF_NOT_OK(GetWorkerAddresses(absentWorkerIndexes, expectedAbsent));

        auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(COORDINATOR_EVIDENCE_TIMEOUT_SEC);
        Status lastRc(K_RUNTIME_ERROR, "Membership layout has not been read");
        std::set<std::string> lastAddresses;
        while (std::chrono::steady_clock::now() < deadline) {
            std::set<std::string> addresses;
            lastRc = ReadMembershipAddresses(addresses);
            lastAddresses = addresses;
            if (lastRc.IsOk() && addresses == expectedPresent) {
                bool allAbsent = true;
                for (const auto &address : expectedAbsent) {
                    if (addresses.count(address) > 0) {
                        allAbsent = false;
                        break;
                    }
                }
                if (allAbsent) {
                    return Status::OK();
                }
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(WAIT_TOPOLOGY_INTERVAL_MS));
        }
        return Status(K_RUNTIME_ERROR, "Timed out waiting for Coordinator membership layout. expected present: "
                                           + AddressesToString(expectedPresent) + ", expected absent: "
                                           + AddressesToString(expectedAbsent) + ", last addresses: "
                                           + AddressesToString(lastAddresses) + ", last status: " + lastRc.ToString());
    }

    Status ReadWitnessProbeEvent(uint32_t witnessWorkerIndex, coordinator::WorkerProbeEventValuePb &value,
                                 int64_t &modRevision)
    {
        RETURN_IF_NOT_OK(GetCoordinatorProxy());
        CHECK_FAIL_RETURN_STATUS(coordinatorProxy_ != nullptr, K_RUNTIME_ERROR, "Coordinator proxy is null");
        HostPort witnessAddress;
        RETURN_IF_NOT_OK(cluster_->GetWorkerAddr(witnessWorkerIndex, witnessAddress));
        std::unique_ptr<cluster::TopologyKeyHelper> topologyKeys;
        RETURN_IF_NOT_OK(cluster::TopologyKeyHelper::Create(GetTestClusterName(), topologyKeys));
        std::string probeKey;
        RETURN_IF_NOT_OK(cluster::TopologyKeyHelper::ProbeKey(witnessAddress.ToString(), probeKey));
        const std::string exactKey = topologyKeys->ProbeTable() + "/" + probeKey;

        std::vector<KeyValueEntry> kvs;
        int64_t revision = 0;
        RETURN_IF_NOT_OK(coordinatorProxy_->Range(exactKey, "", kvs, revision, DEFAULT_COORDINATOR_RPC_TIMEOUT_MS));
        CHECK_FAIL_RETURN_STATUS(
            kvs.size() == 1, K_RUNTIME_ERROR,
            "Unexpected Witness probe entry count for " + exactKey + ": " + std::to_string(kvs.size()));
        CHECK_FAIL_RETURN_STATUS(value.ParseFromString(kvs.front().value), K_RUNTIME_ERROR,
                                 "Failed to parse Witness probe event for " + exactKey);
        CHECK_FAIL_RETURN_STATUS(value.cluster_name() == GetTestClusterName() && !value.coordinator_id().empty()
                                     && !value.target_address().empty() && !value.target_member_id().empty()
                                     && value.probe_round() > 0,
                                 K_RUNTIME_ERROR, "Invalid Witness probe event: " + value.ShortDebugString());
        modRevision = kvs.front().modRevision;
        return Status::OK();
    }

    Status WaitForWitnessProbeEvent(uint32_t witnessWorkerIndex, uint32_t targetWorkerIndex,
                                    coordinator::WorkerProbeEventValuePb &event, int64_t &modRevision)
    {
        HostPort targetAddress;
        RETURN_IF_NOT_OK(cluster_->GetWorkerAddr(targetWorkerIndex, targetAddress));
        auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(COORDINATOR_EVIDENCE_TIMEOUT_SEC);
        Status lastRc(K_RUNTIME_ERROR, "Witness probe event has not been read");
        while (std::chrono::steady_clock::now() < deadline) {
            lastRc = ReadWitnessProbeEvent(witnessWorkerIndex, event, modRevision);
            if (lastRc.IsOk() && event.target_address() == targetAddress.ToString()) {
                return Status::OK();
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(WAIT_TOPOLOGY_INTERVAL_MS));
        }
        return Status(K_RUNTIME_ERROR, "Timed out waiting for Witness probe event for " + targetAddress.ToString()
                                           + ", last event: " + event.ShortDebugString()
                                           + ", last status: " + lastRc.ToString());
    }

    Status WaitForWitnessProbeRoundRollover(uint32_t witnessWorkerIndex, const std::string &targetAddress,
                                            uint64_t initialRound, int64_t initialRevision)
    {
        auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(WITNESS_ROUND_ROLLOVER_TIMEOUT_SEC);
        Status lastRc(K_RUNTIME_ERROR, "Witness probe rollover has not been read");
        coordinator::WorkerProbeEventValuePb event;
        int64_t modRevision = 0;
        while (std::chrono::steady_clock::now() < deadline) {
            lastRc = ReadWitnessProbeEvent(witnessWorkerIndex, event, modRevision);
            if (lastRc.IsOk() && event.target_address() == targetAddress && event.probe_round() != initialRound
                && modRevision > initialRevision) {
                return Status::OK();
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(WAIT_TOPOLOGY_INTERVAL_MS));
        }
        return Status(K_RUNTIME_ERROR, "Timed out waiting for Witness probe round rollover. target: " + targetAddress
                                           + ", initial round: " + std::to_string(initialRound) + ", last event: "
                                           + event.ShortDebugString() + ", last status: " + lastRc.ToString());
    }

    void AssertBidirectionalAccess(uint32_t first, uint32_t second, const std::string &prefix, bool waitForSet = false)
    {
        std::shared_ptr<KVClient> firstClient;
        std::shared_ptr<KVClient> secondClient;
        InitKVClient(first, firstClient);
        InitKVClient(second, secondClient);

        const auto firstKeys = BuildKeys(prefix + "_first_to_second");
        const auto firstValues = BuildValues(firstKeys, prefix + "_first_to_second");
        if (waitForSet) {
            AssertSetKeysEventually(firstClient, firstKeys, firstValues);
        } else {
            AssertSetKeys(firstClient, firstKeys, firstValues);
        }
        AssertGetKeysEventually(secondClient, firstKeys, firstValues);

        const auto secondKeys = BuildKeys(prefix + "_second_to_first");
        const auto secondValues = BuildValues(secondKeys, prefix + "_second_to_first");
        if (waitForSet) {
            AssertSetKeysEventually(secondClient, secondKeys, secondValues);
        } else {
            AssertSetKeys(secondClient, secondKeys, secondValues);
        }
        AssertGetKeysEventually(firstClient, secondKeys, secondValues);
    }

    void AssertBidirectionalAccessEventually(uint32_t first, uint32_t second, const std::string &prefix)
    {
        std::shared_ptr<KVClient> firstClient;
        std::shared_ptr<KVClient> secondClient;
        InitKVClient(first, firstClient);
        InitKVClient(second, secondClient);

        const auto firstKeys = BuildKeys(prefix + "_first_to_second");
        const auto firstValues = BuildValues(firstKeys, prefix + "_first_to_second");
        AssertSetKeysEventually(firstClient, firstKeys, firstValues);
        AssertGetKeysEventually(secondClient, firstKeys, firstValues);

        const auto secondKeys = BuildKeys(prefix + "_second_to_first");
        const auto secondValues = BuildValues(secondKeys, prefix + "_second_to_first");
        AssertSetKeysEventually(secondClient, secondKeys, secondValues);
        AssertGetKeysEventually(firstClient, secondKeys, secondValues);
    }

    void RestartAllWorkersWithCoordinatorRunning()
    {
        auto *externalCluster = dynamic_cast<ExternalCluster *>(cluster_.get());
        ASSERT_NE(externalCluster, nullptr);
        DS_ASSERT_OK(externalCluster->KillWorker(0));
        DS_ASSERT_OK(externalCluster->KillWorker(1));
        DS_ASSERT_OK(externalCluster->KillWorker(2));

        DS_ASSERT_OK(cluster_->StartNode(WORKER, 0, ""));
        DS_ASSERT_OK(cluster_->StartNode(WORKER, 1, ""));
        DS_ASSERT_OK(cluster_->StartNode(WORKER, 2, ""));

        DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 0, WAIT_SCALE_TIMEOUT_SEC));
        DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 1, WAIT_SCALE_TIMEOUT_SEC));
        DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 2, WAIT_SCALE_TIMEOUT_SEC));
    }
};

class CoordinatorBackendFaultIsolationTest : public CoordinatorBackendClusterTest {
public:
    void SetUp() override
    {
        originalEnableUrma_ = FLAGS_enable_urma;
        ExternalClusterTest::SetUp();
    }

    void TearDown() override
    {
        ExternalClusterTest::TearDown();
        FLAGS_enable_urma = originalEnableUrma_;
    }

    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        CoordinatorBackendClusterTest::SetClusterSetupOptions(opts);
        opts.numWorkers = 3;
        opts.disableRocksDB = false;
        opts.workerGflagParams =
            " -shared_memory_size_mb=64 -ipc_through_shared_memory=false -arena_per_tenant=1"
            " -node_timeout_s=" + std::to_string(FAULT_NODE_TIMEOUT_SEC)
            + " -node_dead_timeout_s=30 -client_dead_timeout_s=3"
            " -add_node_wait_time_s=1 -log_async=false -enable_reconciliation=false"
            " -enable_lossless_data_exit_mode=true";
#ifdef USE_URMA
        opts.workerGflagParams += " -enable_urma=true";
#else
        opts.workerGflagParams += " -enable_urma=false";
#endif
        opts.coordinatorGflagParams = " -v=1 -node_timeout_s=" + std::to_string(FAULT_NODE_TIMEOUT_SEC)
                                      + " -node_dead_timeout_s=30 -scale_in_collect_window_ms=0";
    }

protected:
    int GetTestCaseTimeoutSecs() const override
    {
        return THREE_WORKER_TEST_TIMEOUT_SEC;
    }

    void InitRoutedClient(uint32_t workerIndex, bool enableLocalCache, std::shared_ptr<KVClient> &client,
                          DataPlacementPolicy policy = DataPlacementPolicy::PREFERRED_META_OWNER)
    {
        ConnectOptions options;
        InitConnectOpt(workerIndex, options, FAULT_CONNECT_TIMEOUT_MS, true);
        options.enableLocalCache = enableLocalCache;
        options.requestTimeoutMs = FAULT_REQUEST_TIMEOUT_MS;
        options.dataPlacementPolicy = policy;
        client = std::make_shared<KVClient>(options);
        DS_ASSERT_OK(client->Init());
    }

    Status FindRouteKeyToWorker(uint32_t workerIndex, const std::string &prefix, std::string &key) const
    {
        ClusterTopologyPb ring;
        RETURN_IF_NOT_OK(cluster_->ReadClusterTopology(ring));
        HostPort targetWorker;
        RETURN_IF_NOT_OK(cluster_->GetWorkerAddr(workerIndex, targetWorker));

        std::map<uint32_t, std::string> tokenWorkers;
        for (const auto &worker : ring.members()) {
            if (worker.second.state() != MembershipPb::ACTIVE) {
                continue;
            }
            for (const auto token : RebuildTopologyMemberTokens(ring, worker.first, worker.second)) {
                tokenWorkers.emplace(token, worker.first);
            }
        }
        CHECK_FAIL_RETURN_STATUS(!tokenWorkers.empty(), K_NOT_FOUND, "Hash ring has no active worker tokens");
        for (size_t i = 0; i < FAULT_KEY_SEARCH_LIMIT; ++i) {
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

    Status WaitWorkerNotInCluster(uint32_t workerIndex, int timeoutSec)
    {
        HostPort worker;
        RETURN_IF_NOT_OK(cluster_->GetWorkerAddr(workerIndex, worker));
        RETURN_IF_NOT_OK(GetCoordinatorProxy());
        CHECK_FAIL_RETURN_STATUS(coordinatorProxy_ != nullptr, K_RUNTIME_ERROR, "Coordinator proxy is null");
        return WaitWorkersNotInCluster(*coordinatorProxy_, { worker.ToString() }, timeoutSec);
    }

    Status WaitWorkerInCluster(uint32_t workerIndex, int timeoutSec)
    {
        HostPort worker;
        RETURN_IF_NOT_OK(cluster_->GetWorkerAddr(workerIndex, worker));
        RETURN_IF_NOT_OK(GetCoordinatorProxy());
        CHECK_FAIL_RETURN_STATUS(coordinatorProxy_ != nullptr, K_RUNTIME_ERROR, "Coordinator proxy is null");
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(timeoutSec);
        Status lastRc;
        std::map<std::string, MembershipPb::StatePb> lastWorkers;
        while (std::chrono::steady_clock::now() < deadline) {
            lastRc = GetTopologyWorkers(*coordinatorProxy_, lastWorkers, FAULT_TOPOLOGY_RPC_TIMEOUT_MS);
            const auto workerIt = lastWorkers.find(worker.ToString());
            if (lastRc.IsOk() && workerIt != lastWorkers.end() && workerIt->second == MembershipPb::ACTIVE) {
                return Status::OK();
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(WAIT_TOPOLOGY_INTERVAL_MS));
        }
        return Status(K_RUNTIME_ERROR, "Timed out waiting for worker to stay in coordinator topology. Worker: "
                                           + worker.ToString() + ", last workers: " + WorkerStatesToString(lastWorkers)
                                           + ", last status: " + lastRc.ToString());
    }

    struct TimedRecoveryResult {
        bool recovered = false;
        int64_t elapsedMs = -1;
        uint64_t attempts = 0;
        uint64_t failures = 0;
        int64_t lastFailureElapsedMs = -1;
        int64_t noMoreFailAfterMs = -1;
        std::string lastStatus;
    };

    struct FaultClients {
        std::shared_ptr<KVClient> writer;
        std::shared_ptr<KVClient> reader;
        std::shared_ptr<KVClient> metadataReporter;
        std::shared_ptr<KVClient> metadataReporterPeer;
    };

    struct FaultTrafficState {
        std::atomic<bool> stopTraffic{ false };
        std::atomic<uint64_t> metadataAttempts{ 0 };
        std::atomic<uint64_t> metadataFailures{ 0 };
        std::mutex latestMutex;
        std::string latestKey;
        std::string latestValue;
        TimedRecoveryResult setResult;
        TimedRecoveryResult getResult;
        std::vector<std::thread> metadataTraffic;
    };

    TimedRecoveryResult MeasureUntilConsecutiveSuccess(
        const std::chrono::steady_clock::time_point &start,
        const std::function<Status(uint64_t)> &operation,
        int64_t timeoutMs = FAULT_RECOVERY_TIMEOUT_MS)
    {
        TimedRecoveryResult result;
        uint64_t consecutiveSuccesses = 0;
        int64_t currentSuccessStartMs = -1;
        const auto deadline = start + std::chrono::milliseconds(timeoutMs);
        while (std::chrono::steady_clock::now() < deadline) {
            auto rc = operation(result.attempts);
            const auto elapsedMs = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now() - start).count();
            ++result.attempts;
            result.lastStatus = rc.ToString();
            if (rc.IsOk()) {
                if (consecutiveSuccesses == 0) {
                    currentSuccessStartMs = elapsedMs;
                }
                ++consecutiveSuccesses;
                if (consecutiveSuccesses >= FAULT_REQUIRED_CONSECUTIVE_SUCCESSES) {
                    result.recovered = true;
                    result.noMoreFailAfterMs = currentSuccessStartMs;
                    break;
                }
            } else {
                ++result.failures;
                result.lastFailureElapsedMs = elapsedMs;
                consecutiveSuccesses = 0;
                currentSuccessStartMs = -1;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(FAULT_TRAFFIC_INTERVAL_MS));
        }
        result.elapsedMs = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - start).count();
        return result;
    }

    FaultClients InitFaultClients(bool enableLocalCache);
    void WarmupTargetMetadata(const FaultClients &clients, uint32_t workerIndex, const std::string &caseName,
                              bool enableLocalCache, std::string &targetMetaKey);
    void PrepareFailureReportKeys(uint32_t workerIndex, const std::string &caseName,
                                  std::vector<std::string> &reportKeys);
    void StartMetadataFailureTraffic(const FaultClients &clients, const std::vector<std::string> &reportKeys,
                                     FaultTrafficState &state);
    void StartKillRecoveryTraffic(const FaultClients &clients, const std::string &caseName, bool enableLocalCache,
                                  const std::chrono::steady_clock::time_point &measureStartAt,
                                  FaultTrafficState &state, std::thread &setThread, std::thread &getThread);
    void StopAndJoinFaultTraffic(FaultTrafficState &state, std::thread &setThread, std::thread &getThread);
    void LogKillWorkerResults(const std::string &caseName, const std::string &targetMetaKey,
                              const FaultTrafficState &state, const Status &isolationRc, int64_t isolationMs,
                              int64_t waitIsolationMs, int64_t totalMs);
    void WarmupBlinkTraffic(const FaultClients &clients, uint32_t workerIndex, const std::string &caseName,
                            bool enableLocalCache, std::string &latestKey, std::string &latestValue);
    uint64_t InjectKeepaliveBlink(uint32_t workerIndex);
    TimedRecoveryResult VerifyBlinkSetRecovery(const FaultClients &clients, const std::string &caseName, int round,
                                               std::string &latestKey, std::string &latestValue);

    void RunKillWorkerSetGetRecoverWithinTarget(bool enableLocalCache);
    void RunBlinkWorkerDoesNotIsolate(bool enableLocalCache);

private:
    bool originalEnableUrma_ = false;
};

class StaleTopologyBootstrapTest : public CoordinatorBackendClusterTest,
                                   public testing::WithParamInterface<CoordinationBackendType> {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        CoordinatorBackendClusterTest::SetClusterSetupOptions(opts);
        opts.numWorkers = STALE_TOPOLOGY_MEMBER_COUNT;
        opts.waitWorkerReady = false;
        if (GetParam() == CoordinationBackendType::ETCD) {
            opts.numEtcd = 1;
            opts.numCoordinators = 0;
            opts.coordinatorGflagParams.clear();
        }
    }

protected:
    int GetTestCaseTimeoutSecs() const override
    {
        return 120;
    }

    Status ReadTopologyWorkers(std::map<std::string, MembershipPb::StatePb> &workers)
    {
        ClusterTopologyPb topology;
        RETURN_IF_NOT_OK(cluster_->ReadClusterTopology(topology));
        workers.clear();
        for (const auto &member : topology.members()) {
            workers.emplace(member.first, member.second.state());
        }
        return Status::OK();
    }

    Status WaitForExactActiveWorkers(const std::set<std::string> &expectedWorkers,
                                     int timeoutSec = WAIT_SCALE_TIMEOUT_SEC)
    {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(timeoutSec);
        Status lastRc = Status::OK();
        std::map<std::string, MembershipPb::StatePb> lastWorkers;
        while (std::chrono::steady_clock::now() < deadline) {
            lastRc = ReadTopologyWorkers(lastWorkers);
            if (lastRc.IsOk() && lastWorkers.size() == expectedWorkers.size()
                && std::all_of(expectedWorkers.begin(), expectedWorkers.end(), [&](const std::string &address) {
                       const auto found = lastWorkers.find(address);
                       return found != lastWorkers.end() && found->second == MembershipPb::ACTIVE;
                   })) {
                return Status::OK();
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(WAIT_TOPOLOGY_INTERVAL_MS));
        }
        return Status(K_RUNTIME_ERROR,
                      "Timed out waiting for exact replacement topology. Expected: "
                          + VectorToString(std::vector<std::string>(expectedWorkers.begin(), expectedWorkers.end()))
                          + ", last workers: " + WorkerStatesToString(lastWorkers)
                          + ", last status: " + lastRc.ToString());
    }

    Status AddReplacementWorkers(std::set<std::string> &addresses, size_t &firstIndex)
    {
        auto *externalCluster = dynamic_cast<ExternalCluster *>(cluster_.get());
        CHECK_FAIL_RETURN_STATUS(externalCluster != nullptr, K_RUNTIME_ERROR, "Not an ExternalCluster");
        HostPort firstReplacement;
        firstIndex = externalCluster->GetWorkerNum();
        for (size_t offset = 0; offset < STALE_TOPOLOGY_MEMBER_COUNT; ++offset) {
            HostPort workerAddress("127.0.0.1", GetFreePort());
            if (offset == 0) {
                firstReplacement = workerAddress;
            }
            const auto &masterAddress = offset == 0 ? workerAddress : firstReplacement;
            RETURN_IF_NOT_OK(externalCluster->AddNode(masterAddress, workerAddress.ToString(), GetFreePort()));
            addresses.insert(workerAddress.ToString());
        }
        return Status::OK();
    }
};

TEST_P(StaleTopologyBootstrapTest, SingleCommittedWorkerFailureIsRemoved)
{
    auto *externalCluster = dynamic_cast<ExternalCluster *>(cluster_.get());
    ASSERT_NE(externalCluster, nullptr);

    std::set<std::string> expectedWorkers;
    for (size_t index = 0; index < STALE_TOPOLOGY_MEMBER_COUNT; ++index) {
        HostPort address;
        DS_ASSERT_OK(cluster_->GetWorkerAddr(index, address));
        expectedWorkers.insert(address.ToString());
    }
    DS_ASSERT_OK(WaitForExactActiveWorkers(expectedWorkers));

    const size_t failedWorkerIndex = STALE_TOPOLOGY_MEMBER_COUNT - 1;
    HostPort failedWorkerAddress;
    DS_ASSERT_OK(cluster_->GetWorkerAddr(failedWorkerIndex, failedWorkerAddress));
    DS_ASSERT_OK(externalCluster->KillWorker(failedWorkerIndex));
    expectedWorkers.erase(failedWorkerAddress.ToString());

    DS_ASSERT_OK(WaitForExactActiveWorkers(expectedWorkers));
}

TEST_P(StaleTopologyBootstrapTest, EntireCommittedRingCanBootstrapFromReadyReplacementWorkers)
{
    auto *externalCluster = dynamic_cast<ExternalCluster *>(cluster_.get());
    ASSERT_NE(externalCluster, nullptr);

    std::set<std::string> oldWorkers;
    for (size_t index = 0; index < STALE_TOPOLOGY_MEMBER_COUNT; ++index) {
        HostPort address;
        DS_ASSERT_OK(cluster_->GetWorkerAddr(index, address));
        oldWorkers.insert(address.ToString());
    }
    DS_ASSERT_OK(WaitForExactActiveWorkers(oldWorkers));

    for (size_t index = 0; index < STALE_TOPOLOGY_MEMBER_COUNT; ++index) {
        DS_ASSERT_OK(externalCluster->KillWorker(index));
    }
    std::map<std::string, MembershipPb::StatePb> retainedWorkers;
    DS_ASSERT_OK(ReadTopologyWorkers(retainedWorkers));
    for (const auto &address : oldWorkers) {
        EXPECT_NE(retainedWorkers.find(address), retainedWorkers.end());
    }

    std::set<std::string> replacements;
    size_t firstReplacementIndex;
    DS_ASSERT_OK(AddReplacementWorkers(replacements, firstReplacementIndex));
    DS_ASSERT_OK(WaitForExactActiveWorkers(replacements));
    for (size_t offset = 0; offset < STALE_TOPOLOGY_MEMBER_COUNT; ++offset) {
        DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, firstReplacementIndex + offset, WAIT_SCALE_TIMEOUT_SEC));
    }

    std::shared_ptr<KVClient> client;
    InitTestKVClient(STALE_TOPOLOGY_MEMBER_COUNT, client, 10'000, false, 10'000);
    ASSERT_NE(client, nullptr);
    const auto key = NewObjectKey();
    const std::string expectedValue = "replacement-topology-value";
    DS_ASSERT_OK(SetKeyEventually(*client, key, expectedValue));
    std::string observedValue;
    DS_ASSERT_OK(client->Get(key, observedValue));
    EXPECT_EQ(observedValue, expectedValue);
}

INSTANTIATE_TEST_SUITE_P(CoordinationBackends, StaleTopologyBootstrapTest,
                         testing::Values(CoordinationBackendType::ETCD, CoordinationBackendType::COORDINATOR),
                         CoordinationBackendName);

class CoordinatorBackendElectionClusterTest : public CoordinatorBackendClusterTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        CoordinatorBackendClusterTest::SetClusterSetupOptions(opts);
        opts.numCoordinators = 3;
        opts.enableCoordinatorElection = true;
        opts.workerGflagParams =
            " -shared_memory_size_mb=64 -node_timeout_s=8 -node_dead_timeout_s=12 -add_node_wait_time_s=1"
            " -log_async=false -enable_reconciliation=true -enable_lossless_data_exit_mode=true";
        coordinatorCount_ = opts.numCoordinators;
    }
};

class CoordinatorBackendRaftClusterTest : public CoordinatorBackendElectionClusterTest {};

class CoordinatorBackendRaftClusterThreeWorkerTest : public CoordinatorBackendRaftClusterTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        CoordinatorBackendRaftClusterTest::SetClusterSetupOptions(opts);
        opts.numWorkers = 3;
    }
};

TEST_F(CoordinatorBackendClusterTest, TwoWorkersCanReadKeysAcrossWorkers)
{
    std::shared_ptr<KVClient> client0;
    std::shared_ptr<KVClient> client1;
    InitKVClient(0, client0);
    InitKVClient(1, client1);

    const auto keys = BuildKeys("two_workers_cross_read");
    const auto values = BuildValues(keys, "two_workers_cross_read");
    AssertSetKeys(client0, keys, values);
    AssertGetKeysEventually(client1, keys, values);
}

TEST_F(CoordinatorBackendClusterTest, WorkersStayAliveDuringCoordinatorOutageAndRecover)
{
    auto t0 = std::chrono::steady_clock::now();
    AssertWorkersInCluster({ 0, 1 }, WAIT_TOPOLOGY_TIMEOUT_SEC);

    DS_ASSERT_OK(cluster_->ShutdownNodes(ClusterNodeType::COORDINATOR));
    coordinatorProxy_.reset();
    auto t1 = std::chrono::steady_clock::now();
    LOG(INFO) << "[TIMING] Coordinator shutdown done in "
              << std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count() << "ms";

    std::this_thread::sleep_for(std::chrono::seconds(5));
    EXPECT_TRUE(cluster_->CheckWorkerProcess(0));
    EXPECT_TRUE(cluster_->CheckWorkerProcess(1));
    auto t2 = std::chrono::steady_clock::now();
    LOG(INFO) << "[TIMING] Workers survived coordinator outage for "
              << std::chrono::duration_cast<std::chrono::milliseconds>(t2 - t1).count() << "ms";

    DS_ASSERT_OK(cluster_->StartCoordinatorCluster());
    AssertWorkersInCluster({ 0, 1 }, WAIT_SCALE_TIMEOUT_SEC);
    auto t3 = std::chrono::steady_clock::now();
    LOG(INFO) << "[TIMING] Coordinator recovery confirmed in "
              << std::chrono::duration_cast<std::chrono::milliseconds>(t3 - t2).count() << "ms";
    LOG(INFO) << "[TIMING] WorkersStayAliveDuringCoordinatorOutageAndRecover total test time: "
              << std::chrono::duration_cast<std::chrono::milliseconds>(t3 - t0).count() << "ms";
}

TEST_F(CoordinatorBackendClusterTest, SingleWorkerCoordinatorBlinkRecoversWithoutClusterDegrade)
{
    auto t0 = std::chrono::steady_clock::now();
    AssertWorkersInCluster({ 0, 1 }, WAIT_TOPOLOGY_TIMEOUT_SEC);

    std::shared_ptr<KVClient> client0;
    InitKVClient(0, client0);
    DS_ASSERT_OK(client0->Set("single_worker_coordinator_blink_key", "before_blink"));

    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 1, COORDINATOR_KEEPALIVE_INJECT_NAME,
                                           "100*return(K_RPC_UNAVAILABLE)"));
    bool keepAliveFailureInjected = true;
    Raii clearKeepAliveFailure([this, &keepAliveFailureInjected] {
        if (keepAliveFailureInjected) {
            (void)cluster_->ClearInjectAction(ClusterNodeType::WORKER, 1, COORDINATOR_KEEPALIVE_INJECT_NAME);
        }
    });

    std::this_thread::sleep_for(std::chrono::seconds(TARGET_WORKER_COORDINATOR_BLINK_SEC));

    DS_ASSERT_OK(client0->Set("single_worker_coordinator_blink_during_key", "during_blink"));
    EXPECT_TRUE(cluster_->CheckWorkerProcess(0));
    EXPECT_TRUE(cluster_->CheckWorkerProcess(1));
    AssertWorkersInCluster({ 0, 1 }, WAIT_TOPOLOGY_TIMEOUT_SEC);

    DS_ASSERT_OK(cluster_->ClearInjectAction(ClusterNodeType::WORKER, 1, COORDINATOR_KEEPALIVE_INJECT_NAME));
    keepAliveFailureInjected = false;
    AssertWorkersInCluster({ 0, 1 }, WAIT_SCALE_TIMEOUT_SEC);

    std::shared_ptr<KVClient> client1;
    InitKVClient(1, client1);
    std::string value;
    DS_ASSERT_OK(client1->Get("single_worker_coordinator_blink_key", value));
    EXPECT_EQ(value, "before_blink");
    DS_ASSERT_OK(client1->Get("single_worker_coordinator_blink_during_key", value));
    EXPECT_EQ(value, "during_blink");
    DS_ASSERT_OK(client1->Set("single_worker_coordinator_blink_recovered_key", "after_blink"));
    DS_ASSERT_OK(client0->Get("single_worker_coordinator_blink_recovered_key", value));
    EXPECT_EQ(value, "after_blink");

    auto t1 = std::chrono::steady_clock::now();
    LOG(INFO) << "[TIMING] SingleWorkerCoordinatorBlinkRecoversWithoutClusterDegrade total test time: "
              << std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count() << "ms";
}

TEST_F(CoordinatorBackendClusterTest, IsolatedWorkerRemovedThenColdRejoinsWithoutSuicide)
{
    auto t0 = std::chrono::steady_clock::now();
    AssertWorkersInCluster({ 0, 1 }, WAIT_TOPOLOGY_TIMEOUT_SEC);

    bool witnessProbeFailureInjected = false;
    bool keepAliveFailureInjected = false;
    Raii clearIsolationFailures([this, &witnessProbeFailureInjected, &keepAliveFailureInjected] {
        if (keepAliveFailureInjected) {
            (void)cluster_->ClearInjectAction(ClusterNodeType::WORKER, 1, COORDINATOR_KEEPALIVE_INJECT_NAME);
        }
        if (witnessProbeFailureInjected) {
            (void)cluster_->ClearInjectAction(ClusterNodeType::WORKER, 1, WITNESS_PROBE_FAILURE_INJECT);
        }
    });
    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 1, WITNESS_PROBE_FAILURE_INJECT,
                                           "100*return(K_RPC_UNAVAILABLE)"));
    witnessProbeFailureInjected = true;
    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 1, COORDINATOR_KEEPALIVE_INJECT_NAME,
                                           "100*return(K_RPC_UNAVAILABLE)"));
    keepAliveFailureInjected = true;

    AssertWorkersNotInCluster({ 1 }, WAIT_SCALE_TIMEOUT_SEC);
    EXPECT_TRUE(cluster_->CheckWorkerProcess(0));
    EXPECT_TRUE(cluster_->CheckWorkerProcess(1));

    std::shared_ptr<KVClient> isolatedClient;
    InitKVClient(1, isolatedClient);
    ASSERT_NE(isolatedClient, nullptr);
    DS_ASSERT_OK(cluster_->WaitForExpectedResult(
        [&isolatedClient] { return isolatedClient->Set("isolated_worker_removed_key", "during_isolation"); },
        WAIT_SCALE_TIMEOUT_SEC, K_NOT_READY));

    DS_ASSERT_OK(cluster_->ClearInjectAction(ClusterNodeType::WORKER, 1, COORDINATOR_KEEPALIVE_INJECT_NAME));
    keepAliveFailureInjected = false;
    DS_ASSERT_OK(cluster_->ClearInjectAction(ClusterNodeType::WORKER, 1, WITNESS_PROBE_FAILURE_INJECT));
    witnessProbeFailureInjected = false;
    AssertWorkersInCluster({ 0, 1 }, WAIT_SCALE_TIMEOUT_SEC);

    std::shared_ptr<KVClient> client0;
    InitKVClient(0, client0);
    DS_ASSERT_OK(SetKeyEventually(*isolatedClient, "isolated_worker_rejoined_key", "after_rejoin"));
    std::string value;
    DS_ASSERT_OK(client0->Get("isolated_worker_rejoined_key", value));
    EXPECT_EQ(value, "after_rejoin");

    auto t1 = std::chrono::steady_clock::now();
    LOG(INFO) << "[TIMING] IsolatedWorkerRemovedThenColdRejoinsWithoutSuicide total test time: "
              << std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count() << "ms";
}

TEST_F(CoordinatorBackendElectionClusterTest, WorkersStartAfterCoordinatorElectionAndServeRequests)
{
    AssertWorkersInCluster({ 0, 1 }, WAIT_SCALE_TIMEOUT_SEC);

    std::shared_ptr<KVClient> client0;
    std::shared_ptr<KVClient> client1;
    InitKVClient(0, client0);
    InitKVClient(1, client1);

    const auto keys = BuildKeys("coordinator_election_worker_start");
    const auto values = BuildValues(keys, "coordinator_election_worker_start");
    AssertSetKeys(client0, keys, values);
    AssertGetKeysEventually(client1, keys, values);
}

TEST_F(CoordinatorBackendClusterTest, AddedWorkerCanWriteKeysReadableFromExistingWorker)
{
    std::shared_ptr<KVClient> client0;
    std::shared_ptr<KVClient> client1;
    InitKVClient(0, client0);
    InitKVClient(1, client1);

    const auto initialKeys = BuildKeys("scale_up_initial");
    const auto initialValues = BuildValues(initialKeys, "scale_up_initial");
    AssertSetKeys(client0, initialKeys, initialValues);
    AssertGetKeysEventually(client1, initialKeys, initialValues);

    HostPort worker2;
    DS_ASSERT_OK(AddWorkerAndWaitReady(2, worker2));

    AssertWorkersInCluster({ 0, 1, 2 }, WAIT_SCALE_TIMEOUT_SEC);

    std::shared_ptr<KVClient> client2;
    InitKVClient(2, client2);
    const auto scaleUpKeys = BuildKeys("scale_up_new_worker");
    const auto scaleUpValues = BuildValues(scaleUpKeys, "scale_up_new_worker");
    AssertSetKeysEventually(client2, scaleUpKeys, scaleUpValues);
    AssertGetKeysEventually(client1, scaleUpKeys, scaleUpValues);
}

TEST_F(CoordinatorBackendClusterThreeWorkerTest, AllWorkersRestartWithCoordinatorRunning)
{
    AssertWorkersInCluster({ 0, 1, 2 });
    AssertBidirectionalAccess(0, 1, "all_worker_restart_before");

    RestartAllWorkersWithCoordinatorRunning();

    AssertWorkersInCluster({ 0, 1, 2 }, WAIT_SCALE_TIMEOUT_SEC);
    DS_ASSERT_OK(WaitForReadyMemberships({ 0, 1, 2 }, WAIT_SCALE_TIMEOUT_SEC));
    AssertBidirectionalAccessEventually(0, 2, "all_worker_restart_after");
}

TEST_F(CoordinatorBackendClusterThreeWorkerTest, TransientCoordinatorIsolationKeepsTopologyStable)
{
    AssertWorkersInCluster({ 0, 1, 2 });
    AssertBidirectionalAccess(0, 1, "transient_coordinator_isolation_baseline");

    CoordinatorIsolationGuard guard(*cluster_);
    DS_ASSERT_OK(guard.Start({ 1 }));
    DS_ASSERT_OK(guard.Clear());

    AssertWorkersInCluster({ 0, 1, 2 });
    DS_ASSERT_OK(WaitForReadyMemberships({ 0, 1, 2 }, WAIT_SCALE_TIMEOUT_SEC, 2));
    AssertBidirectionalAccessEventually(0, 1, "transient_coordinator_isolation_recovered");
}

TEST_F(CoordinatorBackendClusterThreeWorkerTest, SingleWorkerCoordinatorIsolationIsProtectedByWitness)
{
    CoordinatorIsolationGuard guard(*cluster_);
    DS_ASSERT_OK(guard.Start({ 1 }));
    DS_ASSERT_OK(WaitForMembershipLayout({ 0, 2 }, { 1 }));

    coordinator::WorkerProbeEventValuePb initialEvent;
    int64_t initialRevision = 0;
    DS_ASSERT_OK(WaitForWitnessProbeEvent(0, 1, initialEvent, initialRevision));
    DS_ASSERT_OK(WaitForWitnessProbeRoundRollover(0, initialEvent.target_address(), initialEvent.probe_round(),
                                                  initialRevision));

    AssertWorkersInCluster({ 0, 1, 2 });
    AssertBidirectionalAccess(0, 1, "single_worker_coordinator_isolation");
    DS_ASSERT_OK(guard.Clear());
    AssertWorkersInCluster({ 0, 1, 2 });
}

TEST_F(CoordinatorBackendClusterThreeWorkerTest, MultipleWorkersCoordinatorIsolationIsProtectedByWitness)
{
    CoordinatorIsolationGuard guard(*cluster_);
    DS_ASSERT_OK(guard.Start({ 1, 2 }));
    DS_ASSERT_OK(WaitForMembershipLayout({ 0 }, { 1, 2 }));

    std::this_thread::sleep_for(std::chrono::seconds(WITNESS_ROUND_ROLLOVER_TIMEOUT_SEC));
    AssertWorkersInCluster({ 0, 1, 2 });
    AssertBidirectionalAccess(0, 1, "multiple_workers_coordinator_isolation_worker1");
    AssertBidirectionalAccess(0, 2, "multiple_workers_coordinator_isolation_worker2");
    DS_ASSERT_OK(guard.Clear());
    AssertWorkersInCluster({ 0, 1, 2 });
}

TEST_F(CoordinatorBackendClusterThreeWorkerTest, ProtectedWorkerIsRemovedAfterRealFailure)
{
    CoordinatorIsolationGuard guard(*cluster_);
    DS_ASSERT_OK(guard.Start({ 2 }));
    DS_ASSERT_OK(WaitForMembershipLayout({ 0, 1 }, { 2 }));

    coordinator::WorkerProbeEventValuePb initialEvent;
    int64_t initialRevision = 0;
    DS_ASSERT_OK(WaitForWitnessProbeEvent(0, 2, initialEvent, initialRevision));
    AssertBidirectionalAccess(0, 2, "protected_worker_before_real_failure");
    DS_ASSERT_OK(WaitForWitnessProbeRoundRollover(0, initialEvent.target_address(), initialEvent.probe_round(),
                                                  initialRevision));
    AssertWorkersInCluster({ 0, 1, 2 });

    auto *externalCluster = dynamic_cast<ExternalCluster *>(cluster_.get());
    ASSERT_NE(externalCluster, nullptr);
    auto killRc = externalCluster->KillWorker(2);
    if (killRc.IsOk()) {
        guard.ReleaseWithoutClear();
    }
    DS_ASSERT_OK(killRc);

    AssertWorkersNotInCluster({ 2 }, REAL_FAILURE_REMOVAL_TIMEOUT_SEC);
    AssertWorkersInCluster({ 0, 1 });
    AssertBidirectionalAccessEventually(0, 1, "protected_worker_after_real_failure");
}

TEST_F(CoordinatorBackendClusterThreeWorkerTest, GracefulWorkerExitKeepsExistingKeysReadable)
{
    auto t0 = std::chrono::steady_clock::now();
    std::shared_ptr<KVClient> client0;
    std::shared_ptr<KVClient> client1;
    InitKVClient(0, client0);
    InitKVClient(1, client1);
    auto t1 = std::chrono::steady_clock::now();
    LOG(INFO) << "[TIMING] InitKVClient done in "
              << std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count() << "ms";

    const auto keys = BuildKeys("graceful_scale_down");
    const auto values = BuildValues(keys, "graceful_scale_down");
    AssertSetKeys(client0, keys, values);
    auto t2 = std::chrono::steady_clock::now();
    LOG(INFO) << "[TIMING] SetKeys before graceful exit done in "
              << std::chrono::duration_cast<std::chrono::milliseconds>(t2 - t1).count() << "ms";

    AssertGetKeysEventually(client1, keys, values);
    auto t3 = std::chrono::steady_clock::now();
    LOG(INFO) << "[TIMING] GetKeys before graceful exit done in "
              << std::chrono::duration_cast<std::chrono::milliseconds>(t3 - t2).count() << "ms";

    DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, 2));
    auto t4 = std::chrono::steady_clock::now();
    LOG(INFO) << "[TIMING] ShutdownNode graceful exit done in "
              << std::chrono::duration_cast<std::chrono::milliseconds>(t4 - t3).count() << "ms";

    AssertWorkersInCluster({ 0, 1 });
    auto t5 = std::chrono::steady_clock::now();
    LOG(INFO) << "[TIMING] AssertWorkersInCluster after graceful exit done in "
              << std::chrono::duration_cast<std::chrono::milliseconds>(t5 - t4).count() << "ms";

    AssertGetKeysEventually(client1, keys, values);
    auto t6 = std::chrono::steady_clock::now();
    LOG(INFO) << "[TIMING] GetKeys after graceful exit done in "
              << std::chrono::duration_cast<std::chrono::milliseconds>(t6 - t5).count() << "ms";
    LOG(INFO) << "[TIMING] GracefulWorkerExitKeepsExistingKeysReadable total test time: "
              << std::chrono::duration_cast<std::chrono::milliseconds>(t6 - t0).count() << "ms";
}

TEST_F(CoordinatorBackendClusterThreeWorkerTest, KilledWorkerScaleDownAllowsNewWritesReadableFromOtherWorker)
{
    auto t0 = std::chrono::steady_clock::now();
    std::shared_ptr<KVClient> client0;
    std::shared_ptr<KVClient> client1;
    InitKVClient(0, client0);
    InitKVClient(1, client1);
    auto t1 = std::chrono::steady_clock::now();
    LOG(INFO) << "[TIMING] InitKVClient done in "
              << std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count() << "ms";

    const auto initialKeys = BuildKeys("passive_scale_down_initial");
    const auto initialValues = BuildValues(initialKeys, "passive_scale_down_initial");
    AssertSetKeys(client0, initialKeys, initialValues);
    auto t2 = std::chrono::steady_clock::now();
    LOG(INFO) << "[TIMING] Initial SetKeys done in "
              << std::chrono::duration_cast<std::chrono::milliseconds>(t2 - t1).count() << "ms";

    AssertGetKeysEventually(client1, initialKeys, initialValues);
    auto t3 = std::chrono::steady_clock::now();
    LOG(INFO) << "[TIMING] Initial GetKeys done in "
              << std::chrono::duration_cast<std::chrono::milliseconds>(t3 - t2).count() << "ms";

    auto *externalCluster = dynamic_cast<ExternalCluster *>(cluster_.get());
    ASSERT_NE(externalCluster, nullptr);
    DS_ASSERT_OK(externalCluster->KillWorker(2));
    auto t4 = std::chrono::steady_clock::now();
    LOG(INFO) << "[TIMING] KillWorker done in "
              << std::chrono::duration_cast<std::chrono::milliseconds>(t4 - t3).count() << "ms";

    AssertWorkersNotInCluster({ 2 });
    auto t5 = std::chrono::steady_clock::now();
    LOG(INFO) << "[TIMING] AssertWorkersNotInCluster done in "
              << std::chrono::duration_cast<std::chrono::milliseconds>(t5 - t4).count() << "ms";

    const auto newKeys = BuildKeys("passive_scale_down_new_write");
    const auto newValues = BuildValues(newKeys, "passive_scale_down_new_write");
    AssertSetKeysEventually(client0, newKeys, newValues);
    auto t6 = std::chrono::steady_clock::now();
    LOG(INFO) << "[TIMING] New SetKeys after kill done in "
              << std::chrono::duration_cast<std::chrono::milliseconds>(t6 - t5).count() << "ms";

    AssertGetKeysEventually(client1, newKeys, newValues);
    auto t7 = std::chrono::steady_clock::now();
    LOG(INFO) << "[TIMING] New GetKeys after kill done in "
              << std::chrono::duration_cast<std::chrono::milliseconds>(t7 - t6).count() << "ms";
    LOG(INFO) << "[TIMING] Total test time: " << std::chrono::duration_cast<std::chrono::milliseconds>(t7 - t0).count()
              << "ms";
}

TEST_F(CoordinatorBackendClusterTest, RestartWorkerPropagatesTopologyByCoordinatorWatch)
{
    auto t0 = std::chrono::steady_clock::now();
    AssertWorkersInCluster({ 0, 1 }, WAIT_TOPOLOGY_TIMEOUT_SEC);
    auto t1 = std::chrono::steady_clock::now();
    LOG(INFO) << "[TIMING] First cluster state confirmed in "
              << std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count() << "ms";

    auto *externalCluster = dynamic_cast<ExternalCluster *>(cluster_.get());
    ASSERT_NE(externalCluster, nullptr);
    DS_ASSERT_OK(externalCluster->RestartWorkerAndWaitReadyOneByOne({ 1 }, SIGKILL, WAIT_TOPOLOGY_TIMEOUT_SEC));
    auto t2 = std::chrono::steady_clock::now();
    LOG(INFO) << "[TIMING] Worker restart + ready in "
              << std::chrono::duration_cast<std::chrono::milliseconds>(t2 - t1).count() << "ms";

    AssertWorkersInCluster({ 0, 1 }, WAIT_TOPOLOGY_TIMEOUT_SEC);
    DS_ASSERT_OK(WaitForReadyMemberships({ 0, 1 }, WAIT_TOPOLOGY_TIMEOUT_SEC));
    auto t3 = std::chrono::steady_clock::now();
    LOG(INFO) << "[TIMING] Cluster state after restart confirmed in "
              << std::chrono::duration_cast<std::chrono::milliseconds>(t3 - t2).count() << "ms";
}

CoordinatorBackendFaultIsolationTest::FaultClients CoordinatorBackendFaultIsolationTest::InitFaultClients(
    bool enableLocalCache)
{
    FaultClients clients;
    InitRoutedClient(1, enableLocalCache, clients.writer);
    InitRoutedClient(2, enableLocalCache, clients.reader, DataPlacementPolicy::PREFERRED_SAME_NODE);
    InitRoutedClient(0, true, clients.metadataReporter, DataPlacementPolicy::PREFERRED_SAME_NODE);
    InitRoutedClient(2, true, clients.metadataReporterPeer, DataPlacementPolicy::PREFERRED_SAME_NODE);
    FLAGS_enable_urma = false;
    return clients;
}

void CoordinatorBackendFaultIsolationTest::WarmupTargetMetadata(const FaultClients &clients, uint32_t workerIndex,
                                                               const std::string &caseName, bool enableLocalCache,
                                                               std::string &targetMetaKey)
{
    DS_ASSERT_OK(FindRouteKeyToWorker(workerIndex, caseName + "_target_meta_", targetMetaKey));
    DS_ASSERT_OK(clients.writer->Set(targetMetaKey, FAULT_META_VALUE));
    if (!enableLocalCache) {
        const std::vector<std::string> keys{ targetMetaKey };
        const std::unordered_map<std::string, std::string> values{ { targetMetaKey, FAULT_META_VALUE } };
        auto metadataReporter = clients.metadataReporter;
        AssertGetKeysEventually(metadataReporter, keys, values);
    }
}

void CoordinatorBackendFaultIsolationTest::PrepareFailureReportKeys(uint32_t workerIndex, const std::string &caseName,
                                                                    std::vector<std::string> &reportKeys)
{
    reportKeys.reserve(FAULT_REPORT_KEY_COUNT);
    for (size_t i = 0; i < FAULT_REPORT_KEY_COUNT; ++i) {
        std::string key;
        DS_ASSERT_OK(FindRouteKeyToWorker(workerIndex, caseName + "_failure_report_" + std::to_string(i) + "_", key));
        reportKeys.emplace_back(std::move(key));
    }
}

void CoordinatorBackendFaultIsolationTest::StartMetadataFailureTraffic(const FaultClients &clients,
                                                                       const std::vector<std::string> &reportKeys,
                                                                       FaultTrafficState &state)
{
    for (auto &client : { clients.metadataReporter, clients.metadataReporterPeer }) {
        for (size_t lane = 0; lane < FAULT_REPORT_LANES_PER_WORKER; ++lane) {
            state.metadataTraffic.emplace_back([&, client, lane] {
                size_t nextKey = lane;
                while (!state.stopTraffic.load()) {
                    auto rc = client->Set(reportKeys[nextKey % reportKeys.size()], FAULT_META_VALUE);
                    nextKey += FAULT_REPORT_LANES_PER_WORKER;
                    state.metadataAttempts.fetch_add(1);
                    if (rc.IsError()) {
                        state.metadataFailures.fetch_add(1);
                    }
                    std::this_thread::sleep_for(std::chrono::milliseconds(FAULT_TRAFFIC_INTERVAL_MS));
                }
            });
        }
    }
}

void CoordinatorBackendFaultIsolationTest::StartKillRecoveryTraffic(
    const FaultClients &clients, const std::string &caseName, bool enableLocalCache,
    const std::chrono::steady_clock::time_point &measureStartAt, FaultTrafficState &state, std::thread &setThread,
    std::thread &getThread)
{
    setThread = std::thread([&] {
        RandomData randomData(FAULT_RANDOM_KEY_SEED);
        state.setResult = MeasureUntilConsecutiveSuccess(measureStartAt, [&](uint64_t attempt) {
            const auto key = caseName + "_traffic_" + randomData.GetRandomString(16) + "_" + std::to_string(attempt);
            const auto value = "traffic-value-after-kill-" + std::to_string(attempt);
            auto rc = clients.writer->Set(key, value);
            if (rc.IsOk()) {
                std::lock_guard<std::mutex> lock(state.latestMutex);
                state.latestKey = key;
                state.latestValue = value;
            }
            return rc;
        });
    });
    getThread = std::thread([&] {
        state.getResult = MeasureUntilConsecutiveSuccess(measureStartAt, [&](uint64_t) {
            std::string key;
            std::string expected;
            {
                std::lock_guard<std::mutex> lock(state.latestMutex);
                key = state.latestKey;
                expected = state.latestValue;
            }
            if (key.empty()) {
                return Status(K_TRY_AGAIN, "No successful write after worker failure yet");
            }
            std::string actual;
            auto rc = (enableLocalCache ? clients.writer : clients.reader)->Get(key, actual);
            RETURN_IF_NOT_OK(rc);
            CHECK_FAIL_RETURN_STATUS(actual == expected, K_RUNTIME_ERROR,
                                     FormatString("Unexpected value for %s, expected %s, actual %s",
                                                  key, expected, actual));
            return Status::OK();
        });
    });
}

void CoordinatorBackendFaultIsolationTest::StopAndJoinFaultTraffic(FaultTrafficState &state, std::thread &setThread,
                                                                   std::thread &getThread)
{
    state.stopTraffic.store(true);
    setThread.join();
    getThread.join();
    for (auto &thread : state.metadataTraffic) {
        thread.join();
    }
}

void CoordinatorBackendFaultIsolationTest::LogKillWorkerResults(const std::string &caseName,
                                                               const std::string &targetMetaKey,
                                                               const FaultTrafficState &state,
                                                               const Status &isolationRc, int64_t isolationMs,
                                                               int64_t waitIsolationMs, int64_t totalMs)
{
    LOG(INFO) << "FAULT_ISOLATION_MEASURE phase=" << caseName << "_kill_set recovered=" << state.setResult.recovered
              << " elapsed_ms=" << state.setResult.elapsedMs << " attempts=" << state.setResult.attempts
              << " failures=" << state.setResult.failures << " last_failure_ms=" << state.setResult.lastFailureElapsedMs
              << " no_more_fail_after_ms=" << state.setResult.noMoreFailAfterMs
              << " last_status=" << state.setResult.lastStatus;
    LOG(INFO) << "FAULT_ISOLATION_MEASURE phase=" << caseName << "_kill_metadata_traffic attempts="
              << state.metadataAttempts.load() << " failures=" << state.metadataFailures.load()
              << " target_key=" << targetMetaKey;
    LOG(INFO) << "FAULT_ISOLATION_MEASURE phase=" << caseName << "_kill_get recovered=" << state.getResult.recovered
              << " elapsed_ms=" << state.getResult.elapsedMs << " attempts=" << state.getResult.attempts
              << " failures=" << state.getResult.failures << " last_failure_ms=" << state.getResult.lastFailureElapsedMs
              << " no_more_fail_after_ms=" << state.getResult.noMoreFailAfterMs
              << " last_status=" << state.getResult.lastStatus;
    LOG(INFO) << "FAULT_ISOLATION_MEASURE phase=" << caseName << "_kill_isolation elapsed_ms=" << isolationMs
              << " wait_after_traffic_ms=" << waitIsolationMs << " total_since_kill_start_ms=" << totalMs
              << " rc=" << isolationRc;
}

void CoordinatorBackendFaultIsolationTest::WarmupBlinkTraffic(const FaultClients &clients, uint32_t workerIndex,
                                                             const std::string &caseName, bool enableLocalCache,
                                                             std::string &latestKey, std::string &latestValue)
{
    RandomData randomData(FAULT_RANDOM_KEY_SEED + (enableLocalCache ? 1 : 2));
    latestKey = caseName + "_blink_warmup_" + randomData.GetRandomString(16);
    latestValue = FAULT_META_VALUE;
    DS_ASSERT_OK(clients.writer->Set(latestKey, latestValue));
    if (!enableLocalCache) {
        const std::vector<std::string> keys{ latestKey };
        const std::unordered_map<std::string, std::string> values{ { latestKey, latestValue } };
        auto reader = clients.reader;
        AssertGetKeysEventually(reader, keys, values);
    }

    std::string targetMetaKey;
    DS_ASSERT_OK(FindRouteKeyToWorker(workerIndex, caseName + "_blink_target_meta_", targetMetaKey));
    DS_ASSERT_OK(clients.writer->Set(targetMetaKey, FAULT_META_VALUE));
}

uint64_t CoordinatorBackendFaultIsolationTest::InjectKeepaliveBlink(uint32_t workerIndex)
{
    uint64_t injectCountBefore = 0;
    DS_EXPECT_OK(cluster_->GetInjectActionExecuteCount(ClusterNodeType::WORKER, workerIndex,
                                                       COORDINATOR_KEEPALIVE_INJECT_NAME, injectCountBefore));
    DS_EXPECT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, workerIndex, COORDINATOR_KEEPALIVE_INJECT_NAME,
                                           "100*return(K_RPC_UNAVAILABLE)"));
    bool keepAliveFailureInjected = true;
    Raii clearKeepAliveFailure([this, &keepAliveFailureInjected, workerIndex] {
        if (keepAliveFailureInjected) {
            (void)cluster_->ClearInjectAction(ClusterNodeType::WORKER, workerIndex, COORDINATOR_KEEPALIVE_INJECT_NAME);
        }
    });

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(FAULT_BLINK_MAX_DURATION_MS);
    uint64_t injectCountAfter = injectCountBefore;
    while (std::chrono::steady_clock::now() < deadline) {
        DS_EXPECT_OK(cluster_->GetInjectActionExecuteCount(ClusterNodeType::WORKER, workerIndex,
                                                           COORDINATOR_KEEPALIVE_INJECT_NAME, injectCountAfter));
        DS_EXPECT_OK(WaitWorkerInCluster(workerIndex, 1));
        std::this_thread::sleep_for(std::chrono::milliseconds(WAIT_TOPOLOGY_INTERVAL_MS));
    }
    DS_EXPECT_OK(cluster_->GetInjectActionExecuteCount(ClusterNodeType::WORKER, workerIndex,
                                                       COORDINATOR_KEEPALIVE_INJECT_NAME, injectCountAfter));
    DS_EXPECT_OK(cluster_->ClearInjectAction(ClusterNodeType::WORKER, workerIndex, COORDINATOR_KEEPALIVE_INJECT_NAME));
    keepAliveFailureInjected = false;
    return injectCountAfter - injectCountBefore;
}

CoordinatorBackendFaultIsolationTest::TimedRecoveryResult CoordinatorBackendFaultIsolationTest::VerifyBlinkSetRecovery(
    const FaultClients &clients, const std::string &caseName, int round, std::string &latestKey,
    std::string &latestValue)
{
    RandomData randomData(FAULT_RANDOM_KEY_SEED + round);
    const auto resumeAt = std::chrono::steady_clock::now();
    return MeasureUntilConsecutiveSuccess(resumeAt, [&](uint64_t attempt) {
        latestKey = caseName + "_blink_after_resume_" + std::to_string(round) + "_" + randomData.GetRandomString(16)
                    + "_" + std::to_string(attempt);
        latestValue = "resume-value-" + std::to_string(round) + "_" + std::to_string(attempt);
        return clients.writer->Set(latestKey, latestValue);
    }, FAULT_BLINK_RECOVERY_TIMEOUT_MS);
}

void CoordinatorBackendFaultIsolationTest::RunKillWorkerSetGetRecoverWithinTarget(bool enableLocalCache)
{
    constexpr uint32_t failedWorkerIndex = 1;
    auto clients = InitFaultClients(enableLocalCache);
    const auto caseName = std::string(enableLocalCache ? "local_cache_true" : "local_cache_false");

    std::string targetMetaKey;
    WarmupTargetMetadata(clients, failedWorkerIndex, caseName, enableLocalCache, targetMetaKey);
    std::vector<std::string> reportKeys;
    PrepareFailureReportKeys(failedWorkerIndex, caseName, reportKeys);

    auto *externalCluster = dynamic_cast<ExternalCluster *>(cluster_.get());
    ASSERT_NE(externalCluster, nullptr);
    const auto killStartAt = std::chrono::steady_clock::now();
    DS_ASSERT_OK(externalCluster->KillWorker(failedWorkerIndex));
    const auto trafficStartAt = std::chrono::steady_clock::now();
    const auto killWorkerMs = std::chrono::duration_cast<std::chrono::milliseconds>(
        trafficStartAt - killStartAt).count();
    LOG(INFO) << "FAULT_ISOLATION_MEASURE phase=" << caseName << "_kill_worker elapsed_ms=" << killWorkerMs;

    FaultTrafficState state;
    std::thread setThread;
    std::thread getThread;
    StartMetadataFailureTraffic(clients, reportKeys, state);
    StartKillRecoveryTraffic(clients, caseName, enableLocalCache, trafficStartAt, state, setThread, getThread);

    const auto isolationStart = std::chrono::steady_clock::now();
    auto isolationRc = WaitWorkerNotInCluster(failedWorkerIndex, WAIT_SCALE_TIMEOUT_SEC);
    const auto isolationMs = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - trafficStartAt).count();
    const auto totalMs = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - killStartAt).count();
    const auto waitIsolationMs = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - isolationStart).count();
    StopAndJoinFaultTraffic(state, setThread, getThread);
    LogKillWorkerResults(caseName, targetMetaKey, state, isolationRc, isolationMs, waitIsolationMs, totalMs);

    ASSERT_TRUE(isolationRc.IsOk()) << isolationRc;
    ASSERT_TRUE(state.setResult.recovered) << state.setResult.lastStatus;
    EXPECT_LE(state.setResult.elapsedMs, FAULT_ACCESS_RECOVERY_EXPECT_MS);
    ASSERT_TRUE(state.getResult.recovered);
    EXPECT_LE(state.getResult.elapsedMs, FAULT_ACCESS_RECOVERY_EXPECT_MS);
    EXPECT_LE(isolationMs, FAULT_ISOLATION_EXPECT_MS);
}

void CoordinatorBackendFaultIsolationTest::RunBlinkWorkerDoesNotIsolate(bool enableLocalCache)
{
    constexpr uint32_t blinkWorkerIndex = 1;
    auto clients = InitFaultClients(enableLocalCache);
    const auto caseName = std::string(enableLocalCache ? "local_cache_true" : "local_cache_false");
    std::string latestKey;
    std::string latestValue;
    WarmupBlinkTraffic(clients, blinkWorkerIndex, caseName, enableLocalCache, latestKey, latestValue);

    uint64_t totalKeepaliveFailures = 0;
    for (int round = 0; round < FAULT_BLINK_REPEAT; ++round) {
        const auto keepaliveFailures = InjectKeepaliveBlink(blinkWorkerIndex);
        totalKeepaliveFailures += keepaliveFailures;
        ASSERT_TRUE(WaitWorkerInCluster(blinkWorkerIndex, FAULT_BLINK_TOPOLOGY_TIMEOUT_SEC).IsOk());
        auto setResult = VerifyBlinkSetRecovery(clients, caseName, round, latestKey, latestValue);

        LOG(INFO) << "FAULT_ISOLATION_MEASURE phase=" << caseName << "_blink round=" << round
                  << " keepalive_failures=" << keepaliveFailures << " set_recovered=" << setResult.recovered
                  << " set_elapsed_ms=" << setResult.elapsedMs
                  << " set_last_failure_ms=" << setResult.lastFailureElapsedMs
                  << " set_no_more_fail_after_ms=" << setResult.noMoreFailAfterMs
                  << " last_status=" << setResult.lastStatus;

        ASSERT_TRUE(setResult.recovered) << setResult.lastStatus;
        ASSERT_TRUE(WaitWorkerInCluster(blinkWorkerIndex, FAULT_BLINK_TOPOLOGY_TIMEOUT_SEC).IsOk());
    }

    EXPECT_GE(totalKeepaliveFailures, FAULT_BLINK_KEEPALIVE_FAILURES);
    std::this_thread::sleep_for(std::chrono::seconds(FAULT_NODE_TIMEOUT_SEC + 1));
    ASSERT_TRUE(WaitWorkerInCluster(blinkWorkerIndex, FAULT_BLINK_TOPOLOGY_TIMEOUT_SEC).IsOk());
}

TEST_F(CoordinatorBackendFaultIsolationTest, DISABLED_LEVEL1_LocalCacheFalseKillWorkerSetGetRecoverWithinTarget)
{
    RunKillWorkerSetGetRecoverWithinTarget(false);
}

TEST_F(CoordinatorBackendFaultIsolationTest, DISABLED_LEVEL1_LocalCacheTrueKillWorkerSetGetRecoverWithinTarget)
{
    RunKillWorkerSetGetRecoverWithinTarget(true);
}

TEST_F(CoordinatorBackendFaultIsolationTest, DISABLED_LEVEL1_LocalCacheFalseFiveKeepaliveBlinkDoesNotIsolate)
{
    RunBlinkWorkerDoesNotIsolate(false);
}

TEST_F(CoordinatorBackendFaultIsolationTest, DISABLED_LEVEL1_LocalCacheTrueFiveKeepaliveBlinkDoesNotIsolate)
{
    RunBlinkWorkerDoesNotIsolate(true);
}

TEST_F(CoordinatorBackendRaftClusterTest, ThreeCoordinatorsElectLeaderAndWorkersJoin)
{
    AssertCoordinatorLeaderElected();
    AssertWorkersInCluster({ 0, 1 }, WAIT_SCALE_TIMEOUT_SEC);
}

TEST_F(CoordinatorBackendRaftClusterTest, ThreeCoordinatorRaftWorkerScaleUp)
{
    AssertCoordinatorLeaderElected();
    AssertWorkersInCluster({ 0, 1 }, WAIT_SCALE_TIMEOUT_SEC);

    HostPort worker2;
    DS_ASSERT_OK(AddWorkerAndWaitReady(2, worker2));
    AssertWorkersInCluster({ 0, 1, 2 }, WAIT_SCALE_TIMEOUT_SEC);

    std::shared_ptr<KVClient> client0;
    std::shared_ptr<KVClient> client2;
    InitKVClient(0, client0);
    InitKVClient(2, client2);
    const auto keys = BuildKeys("raft_scale_up");
    const auto values = BuildValues(keys, "raft_scale_up");
    AssertSetKeysEventually(client2, keys, values);
    AssertGetKeysEventually(client0, keys, values);
}

TEST_F(CoordinatorBackendRaftClusterThreeWorkerTest, ThreeCoordinatorRaftGracefulWorkerScaleDown)
{
    std::shared_ptr<KVClient> client0;
    std::shared_ptr<KVClient> client1;
    InitKVClient(0, client0);
    InitKVClient(1, client1);
    const auto keys = BuildKeys("raft_graceful_scale_down");
    const auto values = BuildValues(keys, "raft_graceful_scale_down");
    AssertSetKeys(client0, keys, values);
    AssertGetKeysEventually(client1, keys, values);

    DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, 2));
    AssertWorkersInCluster({ 0, 1 }, WAIT_SCALE_TIMEOUT_SEC);
    AssertGetKeysEventually(client1, keys, values);
}

TEST_F(CoordinatorBackendRaftClusterThreeWorkerTest, ThreeCoordinatorRaftPassiveWorkerScaleDown)
{
    std::shared_ptr<KVClient> client0;
    std::shared_ptr<KVClient> client1;
    InitKVClient(0, client0);
    InitKVClient(1, client1);

    auto *externalCluster = dynamic_cast<ExternalCluster *>(cluster_.get());
    ASSERT_NE(externalCluster, nullptr);
    DS_ASSERT_OK(externalCluster->KillWorker(2));
    AssertWorkersNotInCluster({ 2 }, WAIT_SCALE_TIMEOUT_SEC);

    const auto keys = BuildKeys("raft_passive_scale_down");
    const auto values = BuildValues(keys, "raft_passive_scale_down");
    AssertSetKeysEventually(client0, keys, values);
    AssertGetKeysEventually(client1, keys, values);
}

TEST_F(CoordinatorBackendRaftClusterTest, WorkerScaleUpAfterCoordinatorLeaderFailover)
{
    AssertWorkersInCluster({ 0, 1 }, WAIT_SCALE_TIMEOUT_SEC);
    ShutdownCurrentCoordinatorLeader();

    HostPort worker2;
    DS_ASSERT_OK(AddWorkerAndWaitReady(2, worker2));
    AssertWorkersInCluster({ 0, 1, 2 }, WAIT_SCALE_TIMEOUT_SEC);
}

TEST_F(CoordinatorBackendRaftClusterThreeWorkerTest, GracefulWorkerScaleDownAfterCoordinatorLeaderFailover)
{
    std::shared_ptr<KVClient> client0;
    std::shared_ptr<KVClient> client1;
    InitKVClient(0, client0);
    InitKVClient(1, client1);
    const auto keys = BuildKeys("raft_failover_graceful_scale_down");
    const auto values = BuildValues(keys, "raft_failover_graceful_scale_down");
    AssertSetKeys(client0, keys, values);

    ShutdownCurrentCoordinatorLeader();
    DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, 2));
    AssertWorkersInCluster({ 0, 1 }, WAIT_SCALE_TIMEOUT_SEC);
    AssertGetKeysEventually(client1, keys, values);
}

TEST_F(CoordinatorBackendRaftClusterThreeWorkerTest, PassiveWorkerScaleDownAfterCoordinatorLeaderFailover)
{
    std::shared_ptr<KVClient> client0;
    std::shared_ptr<KVClient> client1;
    InitKVClient(0, client0);
    InitKVClient(1, client1);
    ShutdownCurrentCoordinatorLeader();

    auto *externalCluster = dynamic_cast<ExternalCluster *>(cluster_.get());
    ASSERT_NE(externalCluster, nullptr);
    DS_ASSERT_OK(externalCluster->KillWorker(2));
    AssertWorkersNotInCluster({ 2 }, WAIT_SCALE_TIMEOUT_SEC);

    const auto keys = BuildKeys("raft_failover_passive_scale_down");
    const auto values = BuildValues(keys, "raft_failover_passive_scale_down");
    AssertSetKeysEventually(client0, keys, values);
    AssertGetKeysEventually(client1, keys, values);
}

TEST_F(CoordinatorBackendRaftClusterTest, SdkConnectsWorkerThroughCoordinatorServiceDiscovery)
{
    AssertCoordinatorLeaderElected();
    AssertWorkersInCluster({ 0, 1 }, WAIT_SCALE_TIMEOUT_SEC);

    std::shared_ptr<KVClient> discoveryClient;
    InitKVClientWithCoordinatorServiceDiscovery(discoveryClient);
    const auto keys = BuildKeys("raft_sdk_service_discovery");
    const auto values = BuildValues(keys, "raft_sdk_service_discovery");
    AssertSetKeys(discoveryClient, keys, values);
    AssertGetKeysEventually(discoveryClient, keys, values);
}
}  // namespace st
}  // namespace datasystem
