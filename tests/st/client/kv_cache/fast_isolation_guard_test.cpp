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

/** Description: End-to-end guards for fast-isolation voting and failover spread. */

#include <atomic>
#include <chrono>
#include <cstddef>
#include <fstream>
#include <memory>
#include <set>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <gtest/gtest.h>

#include "client/object_cache/oc_client_common.h"
#include "datasystem/client/object_cache/routing/worker_router.h"
#include "datasystem/common/util/hash_algorithm.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/kv_client.h"
#include "datasystem/protos/cluster_topology.pb.h"

namespace datasystem {
namespace st {
namespace {
constexpr size_t KEY_SEARCH_LIMIT = 100'000;
constexpr int64_t SPARSE_INTERVAL_MS = 2'000;
constexpr int64_t POLL_INTERVAL_MS = 200;
constexpr int64_t DENSE_INTERVAL_MS = 100;
// Budgets are anchored to KillWorker so the passive path (node_dead_timeout_s=60) can never
// leak into the observation window and fake a fast-path isolation.
constexpr int64_t ISOLATION_BUDGET_MS = 50'000;
constexpr int64_t RESILIENCE_BUDGET_MS = 45'000;
constexpr int64_t AVAILABILITY_BUDGET_MS = 60'000;
constexpr int SPARSE_SET_ROUNDS = 15;
constexpr int DENSE_SET_ROUNDS = 150;
constexpr int CONCURRENT_THREADS = 4;
constexpr int32_t CONNECT_TIMEOUT_MS = 2'000;
// SIGKILL turns one Set into a 4-attempt unsent-replay burst (UNSENT_RETRY_MAX_ATTEMPTS=3) that arms the
// per-target failure counter by itself; a frozen owner instead yields exactly one sent deadline failure per
// Set when this timeout is below META_ROUTE_ATTEMPT_TIMEOUT_MS, keeping the stream sparse per reporter.
constexpr int32_t REQUEST_TIMEOUT_MS = 200;
// Routed-client budget: the owner attempt fails fast (connection refused) and leaves the bulk of
// the budget for the excluded failover attempt on a survivor.
constexpr int32_t ROUTED_REQUEST_TIMEOUT_MS = 2'000;
constexpr size_t FAILOVER_WINDOW_SETS = 20;
// Every key hashes into one closed token interval of the same owner, so a deterministic ring
// failover would send the whole cohort to a single successor.
constexpr size_t SAME_SLOT_KEYS = 32;
constexpr char GUARD_KEY_PREFIX[] = "fast_isolation_guard_";
constexpr int64_t VOTE_LAND_TIMEOUT_MS = 15'000;
constexpr char COORDINATOR_CREATE_META_NEEDLE[] = "Processing CreateMetaReq";
constexpr char SUMMARY_RECEIVED_NEEDLE[] = "action=summary_received";

std::string WorkerInfoLogPath(const std::string &rootDir, uint32_t workerIndex)
{
    return rootDir + "/worker" + std::to_string(workerIndex) + "/log/datasystem_worker.INFO.log";
}

std::string CoordinatorInfoLogPath(const std::string &rootDir)
{
    return rootDir + "/coordinator0/log/datasystem_coordinator.INFO.log";
}

std::streamoff LogFileSize(const std::string &path)
{
    std::ifstream file(path, std::ios::binary | std::ios::ate);
    if (!file.good()) {
        return 0;
    }
    return static_cast<std::streamoff>(file.tellg());
}

size_t CountLogNeedleFromOffset(const std::string &path, std::streamoff offset, const std::string &needle)
{
    std::ifstream file(path);
    if (!file.good()) {
        return 0;
    }
    if (offset > 0) {
        file.seekg(offset);
    }
    size_t count = 0;
    std::string line;
    while (std::getline(file, line)) {
        if (line.find(needle) != std::string::npos) {
            ++count;
        }
    }
    return count;
}

bool LogContainsAll(const std::string &path, const std::vector<std::string> &needles)
{
    std::ifstream file(path);
    std::string line;
    while (std::getline(file, line)) {
        bool allPresent = true;
        for (const auto &needle : needles) {
            allPresent = allPresent && line.find(needle) != std::string::npos;
        }
        if (allPresent) {
            return true;
        }
    }
    return false;
}

bool PollLogContainsAll(const std::string &path, int64_t timeoutMs, const std::vector<std::string> &needles)
{
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeoutMs);
    while (std::chrono::steady_clock::now() < deadline) {
        if (LogContainsAll(path, needles)) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(POLL_INTERVAL_MS));
    }
    return false;
}
}  // namespace

class FastIsolationGuardTest : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numEtcd = 0;
        opts.numCoordinators = 1;
        opts.numWorkers = 5;
        opts.workerGflagParams =
            " -shared_memory_size_mb=512 -ipc_through_shared_memory=false -arena_per_tenant=1"
            " -enable_urma=false -node_timeout_s=3 -node_dead_timeout_s=60";
        opts.coordinatorGflagParams = " -node_timeout_s=3 -node_dead_timeout_s=60";
    }

    void SetUp() override
    {
        ExternalClusterTest::SetUp();
        for (uint32_t workerIndex = 0; workerIndex < 5; ++workerIndex) {
            std::shared_ptr<KVClient> client;
            InitTestKVClient(workerIndex, client, CONNECT_TIMEOUT_MS, REQUEST_TIMEOUT_MS);
            clients_.emplace_back(std::move(client));
        }
        WaitForActiveTopology();
    }

    void TearDown() override
    {
        clients_.clear();
        ExternalClusterTest::TearDown();
    }

protected:
    std::vector<std::shared_ptr<KVClient>> clients_;

    bool ReadTopology(ClusterTopologyPb &topology)
    {
        return cluster_->ReadClusterTopology(topology).IsOk();
    }

    void WaitForActiveTopology()
    {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(60);
        while (std::chrono::steady_clock::now() < deadline) {
            ClusterTopologyPb topology;
            if (ReadTopology(topology) && topology.members_size() == 5) {
                bool allActive = true;
                for (const auto &member : topology.members()) {
                    allActive = allActive && member.second.state() == MembershipPb::ACTIVE;
                }
                if (allActive) {
                    return;
                }
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(POLL_INTERVAL_MS));
        }
        FAIL() << "Timed out waiting for five active workers";
    }

    bool WaitForWorkerState(uint32_t workerIndex, MembershipPb::StatePb expected, int64_t timeoutMs)
    {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeoutMs);
        return WaitForWorkerStateUntil(workerIndex, expected, deadline);
    }

    bool IsWorkerInState(uint32_t workerIndex, MembershipPb::StatePb expected)
    {
        HostPort target;
        if (cluster_->GetWorkerAddr(workerIndex, target).IsError()) {
            return false;
        }
        ClusterTopologyPb topology;
        if (!ReadTopology(topology)) {
            return false;
        }
        auto found = topology.members().find(target.ToString());
        return (found == topology.members().end() ? MembershipPb::FAILED : found->second.state()) == expected;
    }

    bool WaitForWorkerStateUntil(uint32_t workerIndex, MembershipPb::StatePb expected,
                                 const std::chrono::steady_clock::time_point &deadline)
    {
        HostPort target;
        if (cluster_->GetWorkerAddr(workerIndex, target).IsError()) {
            return false;
        }
        while (std::chrono::steady_clock::now() < deadline) {
            ClusterTopologyPb topology;
            if (ReadTopology(topology)) {
                auto found = topology.members().find(target.ToString());
                const auto state = found == topology.members().end() ? MembershipPb::FAILED : found->second.state();
                if (state == expected) {
                    return true;
                }
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(POLL_INTERVAL_MS));
        }
        return false;
    }

    std::shared_ptr<std::unordered_map<std::string, std::string>> BuildHostIdMap(
        const ClusterTopologyPb &topology)
    {
        auto hostIdMap = std::make_shared<std::unordered_map<std::string, std::string>>();
        for (const auto &member : topology.members()) {
            (*hostIdMap)[member.first] = "fast-isolation-st-host";
        }
        return hostIdMap;
    }

    std::vector<std::string> FindKeysForWorker(client::WorkerRouter &router, const HostPort &targetWorker,
                                               size_t count)
    {
        std::vector<std::string> keys;
        for (size_t i = 0; i < KEY_SEARCH_LIMIT && keys.size() < count; ++i) {
            std::string key = GUARD_KEY_PREFIX + std::to_string(i);
            HostPort selectedWorker;
            Status rc = router.SelectWorker(key, client::DataPlacementPolicy::PREFERRED_META_OWNER,
                                            client::WorkerAccessAction::CONTROL, selectedWorker);
            if (rc.IsOk() && selectedWorker == targetWorker) {
                keys.push_back(key);
            }
        }
        return keys;
    }

    std::vector<std::string> FindSameSlotKeysForWorker(const std::shared_ptr<const client::TokenIndex> &idx,
                                                       const HostPort &targetWorker, size_t count)
    {
        int targetIndex = -1;
        for (size_t i = 0; i < idx->workers.size(); ++i) {
            if (idx->workers[i] == targetWorker) {
                targetIndex = static_cast<int>(i);
                break;
            }
        }
        if (targetIndex < 0) {
            return {};
        }
        uint32_t windowStart = 0;
        uint32_t windowEndInclusive = 0;
        uint64_t widest = 0;
        // Ring entries own (previousToken, token] ranges under the production lower_bound lookup:
        // an exact token hit belongs to that token's worker, so the interval of entry i ends at
        // its own token. The wrap-around interval of entry 0 is skipped to keep one contiguous range.
        for (size_t i = 1; i < idx->tokenToWorker.size(); ++i) {
            if (idx->tokenToWorker[i].second != targetIndex) {
                continue;
            }
            const uint32_t previousToken = idx->tokenToWorker[i - 1].first;
            const uint64_t width = static_cast<uint64_t>(idx->tokenToWorker[i].first) - previousToken;
            if (width > widest) {
                widest = width;
                windowStart = previousToken + 1;
                windowEndInclusive = idx->tokenToWorker[i].first;
            }
        }
        if (widest == 0) {
            return {};
        }
        std::vector<std::string> keys;
        for (size_t i = 0; i < KEY_SEARCH_LIMIT && keys.size() < count; ++i) {
            std::string key = GUARD_KEY_PREFIX + std::string("same_slot_") + std::to_string(i);
            const uint32_t hash = MurmurHash3_32(key);
            if (hash >= windowStart && hash <= windowEndInclusive) {
                keys.push_back(std::move(key));
            }
        }
        return keys;
    }

    Status BuildRouterFromTopology(const ClusterTopologyPb &topology, std::shared_ptr<client::WorkerRouter> &router)
    {
        auto ring = std::make_shared<ClusterTopologyPb>(topology);
        auto hostIdMap = BuildHostIdMap(topology);
        router = std::make_shared<client::WorkerRouter>("fast-isolation-st-host");
        std::unique_ptr<client::PreparedClusterTopology> prepared;
        RETURN_IF_NOT_OK(client::PreparedClusterTopology::Create(std::move(*ring), prepared));
        router->UpdateHashRing(*prepared, *hostIdMap);
        return Status::OK();
    }

    // The KV client funnels each Set through its connection worker, whose cross-worker CreateMeta toward the
    // dead owner records the peer failure. One client per surviving worker therefore yields one reporter per
    // survivor; the failure-report quorum and the worker-side session arming both count per reporter.

    void MakeRoutedClient(std::shared_ptr<KVClient> &client, int32_t requestTimeoutMs)
    {
        ConnectOptions connectOptions;
        InitConnectOpt(1, connectOptions, CONNECT_TIMEOUT_MS);
        connectOptions.enableLocalCache = false;
        connectOptions.dataPlacementPolicy = DataPlacementPolicy::PREFERRED_META_OWNER;
        connectOptions.requestTimeoutMs = requestTimeoutMs;
        client = std::make_shared<KVClient>(connectOptions);
        DS_ASSERT_OK(client->Init());
    }
};

TEST_F(FastIsolationGuardTest, LEVEL1_SparseFailureStillIsolates)
{
    ClusterTopologyPb topology;
    ASSERT_TRUE(ReadTopology(topology));
    HostPort dead;
    DS_ASSERT_OK(cluster_->GetWorkerAddr(0, dead));
    std::shared_ptr<client::WorkerRouter> router;
    DS_ASSERT_OK(BuildRouterFromTopology(topology, router));
    const auto keys = FindKeysForWorker(*router, dead, SPARSE_SET_ROUNDS * 4);
    ASSERT_EQ(keys.size(), static_cast<size_t>(SPARSE_SET_ROUNDS) * 4);

    // Frozen (SIGSTOP) owner: each Set records exactly one sent deadline failure per reporter, so the
    // sparse 2s stream can only arm through cross-gap accumulation (0/2/4s: the third failure's 4s
    // span covers the window), never through a dense burst.
    const auto budget = std::chrono::steady_clock::now() + std::chrono::milliseconds(ISOLATION_BUDGET_MS);
    auto *externalCluster = dynamic_cast<ExternalCluster *>(cluster_.get());
    ASSERT_NE(externalCluster, nullptr);
    DS_ASSERT_OK(externalCluster->StopWorker(0));
    const std::string value(64, 'x');
    for (int round = 0; round < SPARSE_SET_ROUNDS; ++round) {
        for (int i = 0; i < 4; ++i) {
            (void)clients_[1 + i]->Set(keys[round * 4 + i], value);
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(SPARSE_INTERVAL_MS));
    }
    const auto isolated = WaitForWorkerStateUntil(0, MembershipPb::FAILED, budget);
    DS_ASSERT_OK(cluster_->KillWorker(0));
    EXPECT_TRUE(isolated)
        << "sparse failure stream did not isolate the dead worker within the fast-path window";
}

// Complete end-to-end story of this change: a sparse failure stream arms the fast isolation path
// (evidence accumulates across gaps), the coordinator isolates the frozen owner through the
// reporting quorum, and the client's writes recover on the survivors with randomized spread.
TEST_F(FastIsolationGuardTest, LEVEL1_SparseFailureIsolatesAndWritesRecover)
{
    ClusterTopologyPb topology;
    ASSERT_TRUE(ReadTopology(topology));
    HostPort dead;
    DS_ASSERT_OK(cluster_->GetWorkerAddr(0, dead));
    std::shared_ptr<client::WorkerRouter> router;
    DS_ASSERT_OK(BuildRouterFromTopology(topology, router));
    const auto keys = FindKeysForWorker(*router, dead, SPARSE_SET_ROUNDS * 4);
    ASSERT_EQ(keys.size(), static_cast<size_t>(SPARSE_SET_ROUNDS) * 4);

    const auto budget = std::chrono::steady_clock::now() + std::chrono::milliseconds(ISOLATION_BUDGET_MS);
    auto *externalCluster = dynamic_cast<ExternalCluster *>(cluster_.get());
    ASSERT_NE(externalCluster, nullptr);
    DS_ASSERT_OK(externalCluster->StopWorker(0));
    const std::string value(64, 'x');
    bool isolated = false;
    for (int round = 0; round < SPARSE_SET_ROUNDS && !isolated
         && std::chrono::steady_clock::now() < budget; ++round) {
        for (int i = 0; i < 4; ++i) {
            (void)clients_[1 + i]->Set(keys[round * 4 + i], value);
        }
        isolated = IsWorkerInState(0, MembershipPb::FAILED);
        if (!isolated) {
            std::this_thread::sleep_for(std::chrono::milliseconds(SPARSE_INTERVAL_MS));
        }
    }
    EXPECT_TRUE(isolated)
        << "sparse failure stream did not isolate the frozen owner within the fast-path window";
    DS_ASSERT_OK(cluster_->KillWorker(0));

    // Real-SDK recovery: the writes go through a routed client (enableLocalCache=false, owner
    // placement), so the survivors actually serve them through the SDK WorkerRouter path. Record
    // each survivor log size before the recovery writes, then count master-side CreateMeta
    // receipts appended during the recovery window: the recovered keys must land on at least
    // three distinct new owners through the real client path.
    std::vector<std::streamoff> recoveryLogOffsets;
    for (uint32_t w = 1; w < 5; ++w) {
        recoveryLogOffsets.push_back(LogFileSize(WorkerInfoLogPath(cluster_->GetRootDir(), w)));
    }
    std::shared_ptr<KVClient> routedClient;
    MakeRoutedClient(routedClient, ROUTED_REQUEST_TIMEOUT_MS);

    std::atomic<int> failed{ 0 };
    std::vector<std::thread> workers;
    for (int t = 0; t < CONCURRENT_THREADS; ++t) {
        workers.emplace_back([this, &keys, &value, &failed, &routedClient, t] {
            for (size_t i = t; i < keys.size(); i += CONCURRENT_THREADS) {
                if (!routedClient->Set(keys[i], value).IsOk()) {
                    ++failed;
                }
            }
        });
    }
    for (auto &worker : workers) {
        worker.join();
    }
    EXPECT_EQ(failed.load(), 0);

    size_t ownersWithReceipts = 0;
    for (uint32_t w = 1; w < 5; ++w) {
        if (CountLogNeedleFromOffset(WorkerInfoLogPath(cluster_->GetRootDir(), w), recoveryLogOffsets[w - 1],
                                      COORDINATOR_CREATE_META_NEEDLE)
            > 0) {
            ++ownersWithReceipts;
        }
    }
    EXPECT_GE(ownersWithReceipts, 3U)
        << "recovery writes did not spread create_meta receipts across at least three owners";

    std::vector<HostPort> addresses;
    for (uint32_t i = 0; i < 5; ++i) {
        HostPort address;
        DS_ASSERT_OK(cluster_->GetWorkerAddr(i, address));
        addresses.push_back(address);
    }
    auto singleTokenRing = std::make_shared<ClusterTopologyPb>();
    singleTokenRing->set_tokens_per_member(1);
    for (const auto &address : addresses) {
        (*singleTokenRing->mutable_members())[address.ToString()].set_state(MembershipPb::ACTIVE);
    }
    auto singleTokenMap = std::make_shared<std::unordered_map<std::string, std::string>>();
    for (const auto &address : addresses) {
        (*singleTokenMap)[address.ToString()] = "fast-isolation-st-host";
    }
    client::WorkerRouter singleTokenRouter("fast-isolation-st-host");
    std::unique_ptr<client::PreparedClusterTopology> prepared;
    DS_ASSERT_OK(client::PreparedClusterTopology::Create(std::move(*singleTokenRing), prepared));
    singleTokenRouter.UpdateHashRing(*prepared, *singleTokenMap);

    std::set<std::string> destinations;
    for (const auto &key : keys) {
        HostPort rerouted;
        DS_ASSERT_OK(singleTokenRouter.SelectWorker(key, client::DataPlacementPolicy::PREFERRED_META_OWNER,
                                                    client::WorkerAccessAction::CONTROL, rerouted, { dead }));
        destinations.insert(rerouted.ToString());
    }
    EXPECT_GE(destinations.size(), 3U);
}

TEST_F(FastIsolationGuardTest, LEVEL1_ReporterKillKeepsFastIsolation)
{
    ClusterTopologyPb topology;
    ASSERT_TRUE(ReadTopology(topology));
    HostPort deadTarget;
    DS_ASSERT_OK(cluster_->GetWorkerAddr(0, deadTarget));
    HostPort doomedReporter;
    DS_ASSERT_OK(cluster_->GetWorkerAddr(1, doomedReporter));
    std::shared_ptr<client::WorkerRouter> router;
    DS_ASSERT_OK(BuildRouterFromTopology(topology, router));
    const auto keys = FindKeysForWorker(*router, deadTarget, DENSE_SET_ROUNDS);
    ASSERT_EQ(keys.size(), static_cast<size_t>(DENSE_SET_ROUNDS));

    // Phase 1: arm only the soon-to-die reporter. Its lone vote must really land on the
    // coordinator (proving the vote entered the evidence) while staying strictly below the
    // reporting quorum, so the target cannot be isolated before the reporter dies. Votes from
    // a reporter that is no longer an eligible member are pruned by design.
    DS_ASSERT_OK(cluster_->KillWorker(0));
    const std::string value(64, 'x');
    const std::string coordinatorLog = CoordinatorInfoLogPath(cluster_->GetRootDir());
    const std::vector<std::string> voteNeedles = { SUMMARY_RECEIVED_NEEDLE,
                                                  "reporter=" + doomedReporter.ToString(),
                                                  deadTarget.ToString() };
    for (int i = 0; i < DENSE_SET_ROUNDS && !IsWorkerInState(0, MembershipPb::FAILED)
         && !LogContainsAll(coordinatorLog, voteNeedles);
         ++i) {
        (void)clients_[1]->Set(keys[i % keys.size()], value);
        std::this_thread::sleep_for(std::chrono::milliseconds(DENSE_INTERVAL_MS));
    }
    ASSERT_TRUE(PollLogContainsAll(coordinatorLog, VOTE_LAND_TIMEOUT_MS, voteNeedles))
        << "worker 1 never delivered its failure vote for the dead target";
    ASSERT_FALSE(IsWorkerInState(0, MembershipPb::FAILED))
        << "a single reporter's vote must not reach the isolation quorum";

    // Phase 2: the vote's reporter dies. Its stale vote alone must not isolate the target; the
    // remaining reporters still have to form a valid quorum for the fast path to fire.
    const auto budget = std::chrono::steady_clock::now() + std::chrono::milliseconds(RESILIENCE_BUDGET_MS);
    DS_ASSERT_OK(cluster_->KillWorker(1));
    ASSERT_FALSE(WaitForWorkerState(0, MembershipPb::FAILED, 2'000))
        << "the dead reporter's stale vote must not isolate the target on its own";
    for (int i = 0; i < DENSE_SET_ROUNDS; ++i) {
        (void)clients_[2 + i % 3]->Set(keys[i % keys.size()], value);
        std::this_thread::sleep_for(std::chrono::milliseconds(DENSE_INTERVAL_MS));
    }
    EXPECT_TRUE(WaitForWorkerStateUntil(0, MembershipPb::FAILED, budget))
        << "fast isolation lost resilience after one reporter died";
}

TEST_F(FastIsolationGuardTest, LEVEL1_FailoverTrafficSpreadsToSurvivors)
{
    ClusterTopologyPb topology;
    ASSERT_TRUE(ReadTopology(topology));
    HostPort dead;
    DS_ASSERT_OK(cluster_->GetWorkerAddr(0, dead));
    std::shared_ptr<client::WorkerRouter> router;
    DS_ASSERT_OK(BuildRouterFromTopology(topology, router));
    const auto allKeys = FindKeysForWorker(*router, dead, DENSE_SET_ROUNDS + FAILOVER_WINDOW_SETS);
    ASSERT_EQ(allKeys.size(), static_cast<size_t>(DENSE_SET_ROUNDS) + FAILOVER_WINDOW_SETS);
    // The first keys drive the dense failure-report stream; the last keys belong exclusively to
    // the routed failover window so their worker-log footprints are attributable to it alone.
    const std::vector<std::string> keys(allKeys.begin(), allKeys.begin() + DENSE_SET_ROUNDS);
    const std::vector<std::string> windowKeys(allKeys.end() - FAILOVER_WINDOW_SETS, allKeys.end());

    const auto budget = std::chrono::steady_clock::now() + std::chrono::milliseconds(AVAILABILITY_BUDGET_MS);
    DS_ASSERT_OK(cluster_->KillWorker(0));
    ASSERT_TRUE(IsWorkerInState(0, MembershipPb::ACTIVE))
        << "the killed owner must still be ACTIVE when the failure window starts";

    // Real SDK failover window: while the topology still names the killed worker as the keys'
    // owner, a routed client reroutes every window key's publish to a surviving worker (the
    // dead owner's connection is marked broken process-wide, so each key lands on its ring
    // successor instead of the owner). The Set still fails at the metadata stage — the dead
    // owner still owns the metadata until isolation reassigns it — but each rerouted publish is
    // observable in the survivor logs. A collapsed reroute (fixed survivor regardless of key)
    // concentrates these publishes onto one worker and fails the spread assertion, while the
    // uniform-sampling semantics of in-request reroutes stay locked by the router UTs
    // (SelectFailoverWorkerSpreadsAndExhausts) and the local spread assertion at the bottom.
    auto routedClient = std::make_shared<KVClient>();
    MakeRoutedClient(routedClient, ROUTED_REQUEST_TIMEOUT_MS);
    const std::string value(64, 'x');
    for (const auto &key : windowKeys) {
        (void)routedClient->Set(key, value);
    }
    size_t windowReroutes = 0;
    size_t survivorsWithReroutes = 0;
    for (uint32_t w = 1; w < 5; ++w) {
        size_t workerReroutes = 0;
        for (const auto &key : windowKeys) {
            workerReroutes += CountLogNeedleFromOffset(WorkerInfoLogPath(cluster_->GetRootDir(), w), 0,
                                                        "[ObjectKey " + key + "] is being publishing");
        }
        if (workerReroutes > 0) {
            ++survivorsWithReroutes;
        }
        windowReroutes += workerReroutes;
    }
    EXPECT_GE(windowReroutes, FAILOVER_WINDOW_SETS)
        << "every window Set must reroute its publish attempt to a survivor";
    EXPECT_GE(survivorsWithReroutes, 3U)
        << "failure-window reroutes did not spread across at least three survivors";

    for (int i = 0; i < DENSE_SET_ROUNDS; ++i) {
        (void)clients_[1 + i % 4]->Set(keys[i % keys.size()], value);
        std::this_thread::sleep_for(std::chrono::milliseconds(DENSE_INTERVAL_MS));
    }
    ASSERT_TRUE(WaitForWorkerStateUntil(0, MembershipPb::FAILED, budget));

    std::atomic<int> failed{ 0 };
    std::vector<std::thread> workers;
    for (int t = 0; t < CONCURRENT_THREADS; ++t) {
        workers.emplace_back([this, &keys, &value, &failed, t] {
            for (size_t i = 0; i < keys.size(); ++i) {
                if (!clients_[t + 1]->Set(keys[i], value).IsOk()) {
                    ++failed;
                }
            }
        });
    }
    for (auto &worker : workers) {
        worker.join();
    }
    EXPECT_EQ(failed.load(), 0);

    std::vector<HostPort> addresses;
    for (uint32_t i = 0; i < 5; ++i) {
        HostPort address;
        DS_ASSERT_OK(cluster_->GetWorkerAddr(i, address));
        addresses.push_back(address);
    }
    auto singleTokenRing = std::make_shared<ClusterTopologyPb>();
    singleTokenRing->set_tokens_per_member(1);
    for (const auto &address : addresses) {
        (*singleTokenRing->mutable_members())[address.ToString()].set_state(MembershipPb::ACTIVE);
    }
    auto singleTokenMap = std::make_shared<std::unordered_map<std::string, std::string>>();
    for (const auto &address : addresses) {
        (*singleTokenMap)[address.ToString()] = "fast-isolation-st-host";
    }
    client::WorkerRouter singleTokenRouter("fast-isolation-st-host");
    std::unique_ptr<client::PreparedClusterTopology> prepared;
    DS_ASSERT_OK(client::PreparedClusterTopology::Create(std::move(*singleTokenRing), prepared));
    singleTokenRouter.UpdateHashRing(*prepared, *singleTokenMap);

    const auto spreadKeys = FindKeysForWorker(singleTokenRouter, dead, 100);
    ASSERT_GE(spreadKeys.size(), 10U);
    std::set<std::string> destinations;
    for (const auto &key : spreadKeys) {
        HostPort rerouted;
        DS_ASSERT_OK(singleTokenRouter.SelectWorker(key, client::DataPlacementPolicy::PREFERRED_META_OWNER,
                                                    client::WorkerAccessAction::CONTROL, rerouted, { dead }));
        destinations.insert(rerouted.ToString());
    }
    EXPECT_GE(destinations.size(), 3U);
}

TEST_F(FastIsolationGuardTest, LEVEL1_FailoverWindowSpreadsSameSlotKeysAcrossSurvivors)
{
    ClusterTopologyPb topology;
    ASSERT_TRUE(ReadTopology(topology));
    for (uint32_t i = 0; i < 5; ++i) {
        ASSERT_TRUE(IsWorkerInState(i, MembershipPb::ACTIVE)) << "worker " << i << " must stay ACTIVE";
    }
    HostPort owner;
    DS_ASSERT_OK(cluster_->GetWorkerAddr(0, owner));
    auto ring = std::make_shared<ClusterTopologyPb>(topology);
    std::unique_ptr<client::PreparedClusterTopology> prepared;
    DS_ASSERT_OK(client::PreparedClusterTopology::Create(std::move(*ring), prepared));
    // All keys hash into one closed token interval owned by the same worker under the production
    // hash rule, so every Set reaches the same owner first and the excluded retry alone decides
    // the destination.
    const auto sameSlotKeys = FindSameSlotKeysForWorker(prepared->GetTokenIndex(), owner, SAME_SLOT_KEYS);
    ASSERT_EQ(sameSlotKeys.size(), SAME_SLOT_KEYS) << "could not build a same-slot key cohort";

    const std::string value(64, 'x');
    // One routed client warmed up before the injection is armed: its cluster warmup must not
    // consume the owner's rejection budget, and K_NOT_READY reroutes stay inside each request.
    auto routedClient = std::make_shared<KVClient>();
    MakeRoutedClient(routedClient, ROUTED_REQUEST_TIMEOUT_MS);

    constexpr char injectPoint[] = "worker.PublishObjectWithLock.begin";
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, injectPoint, "return(K_NOT_READY)"));
    Raii clearInject([this, injectPoint] { (void)cluster_->ClearInjectAction(WORKER, 0, injectPoint); });

    for (const auto &key : sameSlotKeys) {
        const auto setStatus = routedClient->Set(key, value);
        ASSERT_TRUE(setStatus.IsOk()) << "set of key " << key << " must succeed on a survivor: "
                                      << setStatus.ToString();
    }
    uint64_t ownerRejects = 0;
    DS_ASSERT_OK(cluster_->GetInjectActionExecuteCount(WORKER, 0, injectPoint, ownerRejects));
    EXPECT_EQ(ownerRejects, SAME_SLOT_KEYS) << "every key must reach the same owner before rerouting";

    size_t survivorPublishes = 0;
    size_t survivorsWithPublishes = 0;
    // Worker log lines can trail the Set acknowledgements; poll briefly so the exact-count
    // assertions below do not race the log flush.
    const auto countDeadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (true) {
        survivorPublishes = 0;
        survivorsWithPublishes = 0;
        for (uint32_t w = 1; w < 5; ++w) {
            size_t workerPublishes = 0;
            for (const auto &key : sameSlotKeys) {
                workerPublishes += CountLogNeedleFromOffset(WorkerInfoLogPath(cluster_->GetRootDir(), w), 0,
                                                            "[ObjectKey " + key + "] is being publishing");
            }
            if (workerPublishes > 0) {
                ++survivorsWithPublishes;
            }
            survivorPublishes += workerPublishes;
        }
        if (survivorPublishes >= SAME_SLOT_KEYS || std::chrono::steady_clock::now() >= countDeadline) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    EXPECT_EQ(survivorPublishes, SAME_SLOT_KEYS) << "every key must publish exactly once on a survivor";
    EXPECT_GE(survivorsWithPublishes, 3U)
        << "same-slot reroutes did not spread across at least three survivors";
    for (uint32_t i = 0; i < 5; ++i) {
        EXPECT_TRUE(IsWorkerInState(i, MembershipPb::ACTIVE)) << "injected rejection must not change topology";
    }
    // The injected rejection must not perturb the topology: same version and same member
    // identities as before the failover window.
    ClusterTopologyPb observed;
    ASSERT_TRUE(ReadTopology(observed));
    EXPECT_EQ(observed.version(), topology.version());
    ASSERT_EQ(observed.members_size(), topology.members_size());
    for (const auto &entry : topology.members()) {
        const auto found = observed.members().find(entry.first);
        ASSERT_NE(found, observed.members().end()) << "member " << entry.first << " must still be present";
        EXPECT_EQ(found->second.id(), entry.second.id());
    }
}

}  // namespace st
}  // namespace datasystem
