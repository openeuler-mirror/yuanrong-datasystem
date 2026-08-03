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
#include <chrono>
#include <initializer_list>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "common.h"
#include "cluster/external_cluster.h"
#include "oc_client_common.h"
#include "datasystem/cluster/repository/topology_key_helper.h"
#include "datasystem/common/coordinator/coordinator_service_proxy.h"
#include "datasystem/common/coordinator/key_value_entry.h"
#include "datasystem/common/coordinator/static_coordinator_discovery.h"
#include "datasystem/common/flags/common_flags.h"
#include "datasystem/common/rpc/rpc_stub_cache_mgr.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/protos/cluster_topology.pb.h"
#include "datasystem/protos/coordinator.pb.h"

namespace datasystem {
namespace st {
namespace {
constexpr int WAIT_TOPOLOGY_TIMEOUT_SEC = 10;
constexpr int WAIT_SCALE_TIMEOUT_SEC = 30;
constexpr int WAIT_TOPOLOGY_INTERVAL_MS = 100;
constexpr int TARGET_WORKER_COORDINATOR_BLINK_SEC = 3;
constexpr int INJECT_EXECUTION_TIMEOUT_SEC = 5;
constexpr int COORDINATOR_EVIDENCE_TIMEOUT_SEC = 30;
constexpr int WITNESS_ROUND_ROLLOVER_TIMEOUT_SEC = 75;
constexpr int REAL_FAILURE_REMOVAL_TIMEOUT_SEC = 150;
constexpr int THREE_WORKER_TEST_TIMEOUT_SEC = 240;
constexpr size_t TEST_KEY_COUNT = 100;
constexpr char COORDINATION_KEEPALIVE_FAILURE_INJECT[] = "CoordinationBackend.KeepAlive.returnError";
constexpr char COORDINATOR_KEEPALIVE_INJECT_NAME[] = "CoordinationBackend.KeepAlive.returnError";
constexpr char COORDINATOR_KEEPALIVE_INJECT_ACTION[] = "return(K_RPC_UNAVAILABLE)";

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
        opts.coordinatorGflagParams = " -v=1 -node_dead_timeout_s=4 -scale_in_collect_window_ms=1000";
    }

protected:
    Status GetCoordinatorProxy()
    {
        if (coordinatorProxy_ != nullptr) {
            return Status::OK();
        }
        HostPort coordinatorAddr;
        auto *externalCluster = dynamic_cast<ExternalCluster *>(cluster_.get());
        CHECK_FAIL_RETURN_STATUS(externalCluster != nullptr, K_RUNTIME_ERROR, "Not an ExternalCluster");
        RETURN_IF_NOT_OK(RpcStubCacheMgr::Instance().Init(100));
        RETURN_IF_NOT_OK(externalCluster->GetCoordinatorAddr(0, coordinatorAddr));
        auto coordinatorDiscovery = std::make_shared<StaticCoordinatorDiscovery>(coordinatorAddr.ToString());
        std::unique_ptr<ICoordinatorServiceProxy> coordinatorProxy;
        if (FLAGS_use_brpc) {
            coordinatorProxy = std::make_unique<CoordinatorServiceProxyBrpcImpl>(std::move(coordinatorDiscovery));
        } else {
            coordinatorProxy = std::make_unique<CoordinatorServiceProxyZmqImpl>(std::move(coordinatorDiscovery));
        }
        RETURN_IF_NOT_OK(coordinatorProxy->Init());
        coordinatorProxy_ = std::move(coordinatorProxy);
        return Status::OK();
    }

    Status GetTopologyWorkers(ICoordinatorServiceProxy &proxy, std::map<std::string, MembershipPb::StatePb> &outWorkers)
    {
        std::unique_ptr<cluster::TopologyKeyHelper> topologyKeys;
        RETURN_IF_NOT_OK(cluster::TopologyKeyHelper::Create(GetTestClusterName(), topologyKeys));

        std::vector<KeyValueEntry> kvs;
        int64_t revision = 0;
        RETURN_IF_NOT_OK(
            proxy.Range(topologyKeys->TopologyTable() + "/", "", kvs, revision, DEFAULT_COORDINATOR_RPC_TIMEOUT_MS));
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
        CHECK_FAIL_RETURN_STATUS(
            value.cluster_name() == GetTestClusterName() && !value.coordinator_id().empty()
                && !value.target_address().empty() && !value.target_member_id().empty() && value.probe_round() > 0,
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
                                           + ", initial round: " + std::to_string(initialRound)
                                           + ", last event: " + event.ShortDebugString()
                                           + ", last status: " + lastRc.ToString());
    }

    void AssertBidirectionalAccess(uint32_t first, uint32_t second, const std::string &prefix)
    {
        std::shared_ptr<KVClient> firstClient;
        std::shared_ptr<KVClient> secondClient;
        InitKVClient(first, firstClient);
        InitKVClient(second, secondClient);

        const auto firstKeys = BuildKeys(prefix + "_first_to_second");
        const auto firstValues = BuildValues(firstKeys, prefix + "_first_to_second");
        AssertSetKeys(firstClient, firstKeys, firstValues);
        AssertGetKeysEventually(secondClient, firstKeys, firstValues);

        const auto secondKeys = BuildKeys(prefix + "_second_to_first");
        const auto secondValues = BuildValues(secondKeys, prefix + "_second_to_first");
        AssertSetKeys(secondClient, secondKeys, secondValues);
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

    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 1, COORDINATION_KEEPALIVE_FAILURE_INJECT,
                                           "100*return(K_RPC_UNAVAILABLE)"));
    bool keepAliveFailureInjected = true;
    Raii clearKeepAliveFailure([this, &keepAliveFailureInjected] {
        if (keepAliveFailureInjected) {
            (void)cluster_->ClearInjectAction(ClusterNodeType::WORKER, 1, COORDINATION_KEEPALIVE_FAILURE_INJECT);
        }
    });

    std::this_thread::sleep_for(std::chrono::seconds(TARGET_WORKER_COORDINATOR_BLINK_SEC));

    DS_ASSERT_OK(client0->Set("single_worker_coordinator_blink_during_key", "during_blink"));
    EXPECT_TRUE(cluster_->CheckWorkerProcess(0));
    EXPECT_TRUE(cluster_->CheckWorkerProcess(1));
    AssertWorkersInCluster({ 0, 1 }, WAIT_TOPOLOGY_TIMEOUT_SEC);

    DS_ASSERT_OK(cluster_->ClearInjectAction(ClusterNodeType::WORKER, 1, COORDINATION_KEEPALIVE_FAILURE_INJECT));
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

    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 1, COORDINATION_KEEPALIVE_FAILURE_INJECT,
                                           "100*return(K_RPC_UNAVAILABLE)"));
    bool keepAliveFailureInjected = true;
    Raii clearKeepAliveFailure([this, &keepAliveFailureInjected] {
        if (keepAliveFailureInjected) {
            (void)cluster_->ClearInjectAction(ClusterNodeType::WORKER, 1, COORDINATION_KEEPALIVE_FAILURE_INJECT);
        }
    });

    AssertWorkersNotInCluster({ 1 }, WAIT_SCALE_TIMEOUT_SEC);
    EXPECT_TRUE(cluster_->CheckWorkerProcess(0));
    EXPECT_TRUE(cluster_->CheckWorkerProcess(1));

    std::shared_ptr<KVClient> isolatedClient;
    InitKVClient(1, isolatedClient);
    ASSERT_NE(isolatedClient, nullptr);
    DS_ASSERT_OK(cluster_->WaitForExpectedResult(
        [&isolatedClient] {
            return isolatedClient->Set("isolated_worker_removed_key", "during_isolation");
        },
        WAIT_SCALE_TIMEOUT_SEC, K_NOT_READY));

    DS_ASSERT_OK(cluster_->ClearInjectAction(ClusterNodeType::WORKER, 1, COORDINATION_KEEPALIVE_FAILURE_INJECT));
    keepAliveFailureInjected = false;
    AssertWorkersInCluster({ 0, 1 }, WAIT_SCALE_TIMEOUT_SEC);

    std::shared_ptr<KVClient> client0;
    InitKVClient(0, client0);
    DS_ASSERT_OK(isolatedClient->Set("isolated_worker_rejoined_key", "after_rejoin"));
    std::string value;
    DS_ASSERT_OK(client0->Get("isolated_worker_rejoined_key", value));
    EXPECT_EQ(value, "after_rejoin");

    auto t1 = std::chrono::steady_clock::now();
    LOG(INFO) << "[TIMING] IsolatedWorkerRemovedThenColdRejoinsWithoutSuicide total test time: "
              << std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count() << "ms";
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
    AssertSetKeys(client2, scaleUpKeys, scaleUpValues);
    AssertGetKeysEventually(client1, scaleUpKeys, scaleUpValues);
}

TEST_F(CoordinatorBackendClusterThreeWorkerTest, AllWorkersRestartWithCoordinatorRunning)
{
    AssertWorkersInCluster({ 0, 1, 2 });
    AssertBidirectionalAccess(0, 1, "all_worker_restart_before");

    RestartAllWorkersWithCoordinatorRunning();

    AssertWorkersInCluster({ 0, 1, 2 }, WAIT_SCALE_TIMEOUT_SEC);
    AssertBidirectionalAccess(0, 2, "all_worker_restart_after");
}

TEST_F(CoordinatorBackendClusterThreeWorkerTest, TransientCoordinatorIsolationKeepsTopologyStable)
{
    AssertWorkersInCluster({ 0, 1, 2 });
    AssertBidirectionalAccess(0, 1, "transient_coordinator_isolation_baseline");

    CoordinatorIsolationGuard guard(*cluster_);
    DS_ASSERT_OK(guard.Start({ 1 }));
    DS_ASSERT_OK(guard.Clear());

    AssertWorkersInCluster({ 0, 1, 2 });
    AssertBidirectionalAccess(0, 1, "transient_coordinator_isolation_recovered");
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
    AssertBidirectionalAccess(0, 1, "protected_worker_after_real_failure");
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
    AssertSetKeys(client0, newKeys, newValues);
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
    auto t3 = std::chrono::steady_clock::now();
    LOG(INFO) << "[TIMING] Cluster state after restart confirmed in "
              << std::chrono::duration_cast<std::chrono::milliseconds>(t3 - t2).count() << "ms";
}
}  // namespace st
}  // namespace datasystem
