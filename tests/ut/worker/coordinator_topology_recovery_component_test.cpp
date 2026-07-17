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

/**
 * Description: Component tests for Worker-initiated recovery across Coordinator lifetimes.
 */
#include "datasystem/worker/coordinator/topology_recovery_reporter.h"

#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "gtest/gtest.h"

#include "datasystem/cluster/model/topology_types.h"
#include "datasystem/cluster/repository/topology_key_helper.h"
#include "datasystem/cluster/repository/topology_repository_codec.h"
#include "datasystem/common/coordinator/coordinator_store.h"
#include "datasystem/common/coordinator/memory_kv_store.h"
#include "datasystem/common/coordinator/steady_clock.h"
#include "datasystem/common/coordinator/ttl_manager.h"
#include "datasystem/common/coordinator/watch_dispatcher.h"
#include "datasystem/common/coordinator/watch_registry.h"
#include "datasystem/coordinator/topology_recovery_manager.h"
#include "ut/common.h"

namespace datasystem::worker {
namespace {
using namespace std::chrono_literals;

constexpr char CLUSTER_NAME[] = "component-cluster";
constexpr char MEMBER_ADDRESS[] = "127.0.0.1:31001";
constexpr char COORDINATOR_A[] = "coordinator-id-a";
constexpr char COORDINATOR_B[] = "coordinator-id-b";
constexpr uint64_t TOPOLOGY_VERSION = 42;
constexpr uint64_t DISCOVERY_WINDOW_MS = 20;
constexpr auto TEST_TIMEOUT = 2s;
constexpr auto POLL_INTERVAL = 1ms;

class NoopWatchDispatcher final : public WatchDispatcher {
public:
    explicit NoopWatchDispatcher(WatchRegistry *registry) : WatchDispatcher(registry)
    {
    }

    ~NoopWatchDispatcher() override = default;

    Status DoNotify(int64_t, const std::string &, std::vector<std::shared_ptr<WatchEvent>> &) override
    {
        return Status::OK();
    }
};

struct RecoveryGeneration {
    explicit RecoveryGeneration(const std::string &coordinatorId)
        : memoryStore(std::make_shared<MemoryKvStore>()),
          registry(std::make_shared<WatchRegistry>()),
          dispatcher(std::make_shared<NoopWatchDispatcher>(registry.get())),
          clock(std::make_shared<SteadyClockMock>()),
          ttlManager(std::make_shared<TtlManager>(clock)),
          store(std::make_unique<CoordinatorStore>(memoryStore, registry, dispatcher, ttlManager)),
          manager(std::make_unique<coordinator::TopologyRecoveryManager>(coordinatorId, *store, clock, Options()))
    {
    }

    static coordinator::TopologyRecoveryOptions Options()
    {
        coordinator::TopologyRecoveryOptions options;
        options.discoveryWindow = std::chrono::milliseconds(DISCOVERY_WINDOW_MS);
        options.validationWaitTimeout = TEST_TIMEOUT;
        return options;
    }

    std::shared_ptr<MemoryKvStore> memoryStore;
    std::shared_ptr<WatchRegistry> registry;
    std::shared_ptr<NoopWatchDispatcher> dispatcher;
    std::shared_ptr<SteadyClockMock> clock;
    std::shared_ptr<TtlManager> ttlManager;
    std::unique_ptr<CoordinatorStore> store;
    std::unique_ptr<coordinator::TopologyRecoveryManager> manager;
};

class RecoveryLoopbackProxy final : public ICoordinatorServiceProxy {
public:
    ~RecoveryLoopbackProxy() override = default;

    void SetTarget(std::string coordinatorId, coordinator::TopologyRecoveryManager &manager)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        coordinatorId_ = std::move(coordinatorId);
        manager_ = &manager;
    }

    bool WaitForAttempts(size_t expected)
    {
        std::unique_lock<std::mutex> lock(mutex_);
        return cv_.wait_for(lock, TEST_TIMEOUT, [this, expected] { return attemptCount_ >= expected; });
    }

    void BlockNextResponse()
    {
        std::lock_guard<std::mutex> lock(mutex_);
        blockNextResponse_ = true;
    }

    bool WaitUntilResponseBlocked()
    {
        std::unique_lock<std::mutex> lock(mutex_);
        return cv_.wait_for(lock, TEST_TIMEOUT, [this] { return responseBlocked_; });
    }

    void ReleaseBlockedResponse()
    {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            releaseBlockedResponse_ = true;
        }
        cv_.notify_all();
    }

    Status Put(const std::string &, const std::string &, int64_t, int64_t, int64_t &, int64_t &, int32_t,
               std::string *, const std::string &) override
    {
        return Unused();
    }

    Status Range(const std::string &, const std::string &, std::vector<KeyValueEntry> &, int64_t &, int32_t,
                 std::string *) override
    {
        return Unused();
    }

    Status DeleteRange(const std::string &, const std::string &, int64_t &, int64_t &, int32_t) override
    {
        return Unused();
    }

    Status WatchRange(const std::string &, const std::string &, const std::string &, const std::string &, int64_t &,
                      std::vector<KeyValueEntry> &, int32_t, std::string *) override
    {
        return Unused();
    }

    Status CancelWatch(const std::string &, const std::vector<int64_t> &, const std::string &, int32_t) override
    {
        return Unused();
    }

    Status KeepAlive(const std::string &, int64_t &, int64_t &, int32_t, std::string *) override
    {
        return Unused();
    }

    Status CAS(const std::string &, const CasProcessFunc &, int64_t &, int64_t &) override
    {
        return Unused();
    }

    Status GetCoordinatorId(std::string &coordinatorId, int32_t) override
    {
        std::lock_guard<std::mutex> lock(mutex_);
        coordinatorId = coordinatorId_;
        return Status::OK();
    }

    Status ReportTopologyRecoveryCandidate(const coordinator::ReportTopologyRecoveryCandidateReqPb &request,
                                           coordinator::ReportTopologyRecoveryCandidateRspPb &response,
                                           int32_t) override
    {
        coordinator::TopologyRecoveryManager *manager = nullptr;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            manager = manager_;
        }
        CHECK_FAIL_RETURN_STATUS(manager != nullptr, K_NOT_READY, "component proxy has no recovery manager");
        coordinator::TopologyRecoveryCandidateReport report;
        report.reporterAddress = request.reporter_address();
        report.hasSnapshot = request.result() == coordinator::TOPOLOGY_RECOVERY_SNAPSHOT;
        report.topologyVersion = request.topology_version();
        report.canonicalDigest = request.topology_digest();
        report.canonicalTopology = request.canonical_topology();
        coordinator::TopologyRecoveryReportDecision decision;
        auto status = manager->ReportCandidate(request.cluster_name(), request.coordinator_id(), std::move(report),
                                               decision);
        if (status.IsOk()) {
            FillResponse(decision, response);
        }
        {
            std::unique_lock<std::mutex> lock(mutex_);
            ++attemptCount_;
            if (blockNextResponse_) {
                blockNextResponse_ = false;
                responseBlocked_ = true;
                cv_.notify_all();
                cv_.wait(lock, [this] { return releaseBlockedResponse_; });
            }
        }
        cv_.notify_all();
        return status;
    }

    void GetObservedCoordinatorId(std::string &coordinatorId) const override
    {
        std::lock_guard<std::mutex> lock(mutex_);
        coordinatorId = coordinatorId_;
    }

private:
    static Status Unused()
    {
        return Status(K_RUNTIME_ERROR, "unused component proxy operation");
    }

    static void FillResponse(const coordinator::TopologyRecoveryReportDecision &decision,
                             coordinator::ReportTopologyRecoveryCandidateRspPb &response)
    {
        using ResultPb = coordinator::ReportTopologyRecoveryCandidateRspPb;
        response.set_result(decision.result == coordinator::TopologyRecoveryReportResult::ACCEPTED
                                ? ResultPb::ACCEPTED
                                : ResultPb::MEMBERSHIP_NOT_READY);
        if (decision.result == coordinator::TopologyRecoveryReportResult::COORDINATOR_ID_MISMATCH) {
            response.set_result(ResultPb::COORDINATOR_ID_MISMATCH);
        }
        response.set_recovery_state(decision.state == coordinator::TopologyRecoveryState::READY
                                        ? coordinator::COORDINATOR_READY
                                        : coordinator::COORDINATOR_RECOVERING);
        response.set_payload_required(decision.payloadRequired);
    }

    // Protects the generation target, attempt count and response-blocking controls.
    mutable std::mutex mutex_;
    std::condition_variable cv_;
    std::string coordinatorId_;
    coordinator::TopologyRecoveryManager *manager_{ nullptr };
    size_t attemptCount_{ 0 };
    bool blockNextResponse_{ false };
    bool responseBlocked_{ false };
    bool releaseBlockedResponse_{ false };
};

std::string MembershipKey()
{
    std::unique_ptr<cluster::TopologyKeyHelper> keys;
    EXPECT_TRUE(cluster::TopologyKeyHelper::Create(CLUSTER_NAME, keys).IsOk());
    return keys->MembershipTable() + "/" + MEMBER_ADDRESS;
}

std::string TopologyKey()
{
    std::unique_ptr<cluster::TopologyKeyHelper> keys;
    EXPECT_TRUE(cluster::TopologyKeyHelper::Create(CLUSTER_NAME, keys).IsOk());
    return keys->TopologyTable() + "/";
}

std::string BuildCanonicalTopology()
{
    cluster::TopologyState topology;
    topology.clusterHasInit = true;
    topology.version = TOPOLOGY_VERSION;
    topology.members = { cluster::Member{ { std::string(16, 'a'), MEMBER_ADDRESS },
                                           cluster::MemberState::ACTIVE, { 10, 20, 30, 40 } } };
    std::string canonical;
    EXPECT_TRUE(cluster::TopologyRepositoryCodec::EncodeTopology(topology, canonical).IsOk());
    return canonical;
}

bool DriveReady(RecoveryGeneration &generation)
{
    generation.clock->AdvanceMs(DISCOVERY_WINDOW_MS);
    const auto deadline = std::chrono::steady_clock::now() + TEST_TIMEOUT;
    while (std::chrono::steady_clock::now() < deadline) {
        generation.manager->NotifyMembershipActivity(MembershipKey());
        if (generation.manager->GetState(CLUSTER_NAME) == coordinator::TopologyRecoveryState::READY) {
            return true;
        }
        std::this_thread::sleep_for(POLL_INTERVAL);
    }
    return false;
}

void ExpectInstalled(RecoveryGeneration &generation, const std::string &canonical)
{
    std::vector<KeyValueEntry> entries;
    int64_t revision = 0;
    DS_ASSERT_OK(generation.store->Range(TopologyKey(), "", entries, revision));
    ASSERT_EQ(entries.size(), 1);
    EXPECT_EQ(entries.front().value, canonical);
}

TEST(CoordinatorTopologyRecoveryComponentTest, WorkerRebuildsTwoFreshCoordinatorGenerations)
{
    const std::string canonical = BuildCanonicalTopology();
    RecoveryLoopbackProxy proxy;
    TopologyRecoveryReporterOptions reporterOptions;
    reporterOptions.reportTimeout = 100ms;
    reporterOptions.minRetryBackoff = 1ms;
    reporterOptions.maxRetryBackoff = 10ms;
    reporterOptions.maxInitialJitter = 0ms;
    TopologyRecoveryReporter reporter(
        proxy, CLUSTER_NAME, MEMBER_ADDRESS,
        [&canonical](uint64_t &version, std::string &snapshot) {
            version = TOPOLOGY_VERSION;
            snapshot = canonical;
            return Status::OK();
        },
        reporterOptions);
    reporter.NotifyRuntimeReady();

    RecoveryGeneration first(COORDINATOR_A);
    first.manager->ObserveMembershipChange(MembershipKey(), true);
    proxy.SetTarget(COORDINATOR_A, *first.manager);
    reporter.NotifyMembershipReady(COORDINATOR_A);
    ASSERT_TRUE(proxy.WaitForAttempts(2));
    ASSERT_TRUE(DriveReady(first));
    ExpectInstalled(first, canonical);

    RecoveryGeneration second(COORDINATOR_B);
    second.manager->ObserveMembershipChange(MembershipKey(), true);
    proxy.SetTarget(COORDINATOR_B, *second.manager);
    reporter.NotifyMembershipReady(COORDINATOR_B);
    ASSERT_TRUE(proxy.WaitForAttempts(4));
    ASSERT_TRUE(DriveReady(second));
    ExpectInstalled(second, canonical);
    DS_ASSERT_OK(reporter.Shutdown());
}

TEST(CoordinatorTopologyRecoveryComponentTest, CoordinatorSwitchBetweenEvidenceAndPayloadUsesNewGeneration)
{
    const std::string canonical = BuildCanonicalTopology();
    RecoveryLoopbackProxy proxy;
    TopologyRecoveryReporterOptions reporterOptions;
    reporterOptions.reportTimeout = 100ms;
    reporterOptions.minRetryBackoff = 1ms;
    reporterOptions.maxRetryBackoff = 10ms;
    reporterOptions.maxInitialJitter = 0ms;
    TopologyRecoveryReporter reporter(
        proxy, CLUSTER_NAME, MEMBER_ADDRESS,
        [&canonical](uint64_t &version, std::string &snapshot) {
            version = TOPOLOGY_VERSION;
            snapshot = canonical;
            return Status::OK();
        },
        reporterOptions);
    reporter.NotifyRuntimeReady();

    RecoveryGeneration first(COORDINATOR_A);
    first.manager->ObserveMembershipChange(MembershipKey(), true);
    proxy.SetTarget(COORDINATOR_A, *first.manager);
    proxy.BlockNextResponse();
    reporter.NotifyMembershipReady(COORDINATOR_A);
    if (!proxy.WaitUntilResponseBlocked()) {
        ADD_FAILURE() << "evidence response was not blocked";
        proxy.ReleaseBlockedResponse();
        EXPECT_TRUE(reporter.Shutdown().IsOk());
        return;
    }

    RecoveryGeneration second(COORDINATOR_B);
    second.manager->ObserveMembershipChange(MembershipKey(), true);
    proxy.SetTarget(COORDINATOR_B, *second.manager);
    reporter.NotifyMembershipReady(COORDINATOR_B);
    proxy.ReleaseBlockedResponse();

    ASSERT_TRUE(proxy.WaitForAttempts(3));
    ASSERT_TRUE(DriveReady(second));
    ExpectInstalled(second, canonical);
    DS_ASSERT_OK(reporter.Shutdown());
}

}  // namespace
}  // namespace datasystem::worker
