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
 * Description: Tests worker-initiated topology recovery candidate reporting.
 */
#include "datasystem/worker/coordinator/topology_recovery_reporter.h"

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <future>
#include <mutex>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "gtest/gtest.h"

namespace datasystem::worker {
namespace {
using namespace std::chrono_literals;

constexpr char CLUSTER_NAME[] = "cluster-a";
constexpr char REPORTER_ADDRESS[] = "127.0.0.1:31001";
constexpr char COORDINATOR_A[] = "coordinator-a";
constexpr char COORDINATOR_B[] = "coordinator-b";
constexpr uint64_t TOPOLOGY_VERSION = 42;
constexpr auto ASYNC_TIMEOUT = 2s;
constexpr auto REPORT_TIMEOUT = 37ms;
constexpr auto MIN_RETRY_BACKOFF = 10ms;
constexpr auto MAX_RETRY_BACKOFF = 40ms;
constexpr auto CANCEL_TEST_BACKOFF = 10s;
constexpr auto SIGNAL_RETRY_INTERVAL = 10ms;
constexpr auto SHUTDOWN_OBSERVATION = 100ms;

struct ReportAction {
    Status status{ Status::OK() };
    coordinator::ReportTopologyRecoveryCandidateRspPb::ResultPb result{
        coordinator::ReportTopologyRecoveryCandidateRspPb::ACCEPTED
    };
    coordinator::CoordinatorRecoveryStatePb recoveryState{ coordinator::COORDINATOR_READY };
    bool payloadRequired{ false };
    bool block{ false };
};

class FakeCoordinatorServiceProxy final : public ICoordinatorServiceProxy {
public:
    ~FakeCoordinatorServiceProxy() override = default;

    Status Put(const std::string &, const std::string &, int64_t, int64_t, int64_t &, int64_t &, int32_t,
               std::string *, const std::string &) override
    {
        return Unused("Put");
    }

    Status Range(const std::string &, const std::string &, std::vector<KeyValueEntry> &, int64_t &, int32_t,
                 std::string *) override
    {
        return Unused("Range");
    }

    Status DeleteRange(const std::string &, const std::string &, int64_t &, int64_t &, int32_t) override
    {
        return Unused("DeleteRange");
    }

    Status WatchRange(const std::string &, const std::string &, const std::string &, const std::string &, int64_t &,
                      std::vector<KeyValueEntry> &, int32_t, std::string *) override
    {
        return Unused("WatchRange");
    }

    Status CancelWatch(const std::string &, const std::vector<int64_t> &, const std::string &, int32_t) override
    {
        return Unused("CancelWatch");
    }

    Status KeepAlive(const std::string &, int64_t &, int64_t &, int32_t, std::string *) override
    {
        return Unused("KeepAlive");
    }

    Status CAS(const std::string &, const CasProcessFunc &, int64_t &, int64_t &) override
    {
        return Unused("CAS");
    }

    Status GetCoordinatorId(std::string &, int32_t) override
    {
        return Unused("GetCoordinatorId");
    }

    Status ReportTopologyRecoveryCandidate(const coordinator::ReportTopologyRecoveryCandidateReqPb &request,
                                           coordinator::ReportTopologyRecoveryCandidateRspPb &response,
                                           int32_t timeoutMs) override
    {
        ReportAction action;
        {
            std::unique_lock<std::mutex> lock(mutex_);
            requests_.push_back(request);
            timeouts_.push_back(timeoutMs);
            if (actions_.empty()) {
                action.status = Status(K_RUNTIME_ERROR, "unexpected recovery report");
            } else {
                action = actions_.front();
                actions_.pop_front();
            }
            ++startedCount_;
            cv_.notify_all();
            if (action.block) {
                blocked_ = true;
                cv_.notify_all();
                cv_.wait(lock, [this] { return releaseBlocked_; });
            }
        }
        if (action.status.IsOk()) {
            response.set_result(action.result);
            response.set_recovery_state(action.recoveryState);
            response.set_payload_required(action.payloadRequired);
        }
        {
            std::lock_guard<std::mutex> lock(mutex_);
            ++returnedCount_;
        }
        cv_.notify_all();
        return action.status;
    }

    void GetObservedCoordinatorId(std::string &coordinatorId) const override
    {
        coordinatorId.clear();
    }

    void PushAction(ReportAction action)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        actions_.push_back(std::move(action));
    }

    bool WaitForStarted(size_t count, std::chrono::milliseconds timeout = ASYNC_TIMEOUT)
    {
        std::unique_lock<std::mutex> lock(mutex_);
        return cv_.wait_for(lock, timeout, [this, count] { return startedCount_ >= count; });
    }

    bool WaitForReturned(size_t count)
    {
        std::unique_lock<std::mutex> lock(mutex_);
        return cv_.wait_for(lock, ASYNC_TIMEOUT, [this, count] { return returnedCount_ >= count; });
    }

    bool WaitUntilBlocked()
    {
        std::unique_lock<std::mutex> lock(mutex_);
        return cv_.wait_for(lock, ASYNC_TIMEOUT, [this] { return blocked_; });
    }

    void ReleaseBlockedCall()
    {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            releaseBlocked_ = true;
        }
        cv_.notify_all();
    }

    size_t StartedCount() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return startedCount_;
    }

    coordinator::ReportTopologyRecoveryCandidateReqPb RequestAt(size_t index) const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return requests_.at(index);
    }

    int32_t TimeoutAt(size_t index) const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return timeouts_.at(index);
    }

private:
    static Status Unused(const std::string &operation)
    {
        return Status(K_RUNTIME_ERROR, "unused fake " + operation);
    }

    // Protects scripted actions, captured calls, counters and blocking-call controls.
    mutable std::mutex mutex_;
    std::condition_variable cv_;
    std::deque<ReportAction> actions_;
    std::vector<coordinator::ReportTopologyRecoveryCandidateReqPb> requests_;
    std::vector<int32_t> timeouts_;
    size_t startedCount_{ 0 };
    size_t returnedCount_{ 0 };
    bool blocked_{ false };
    bool releaseBlocked_{ false };
};

TopologyRecoveryReporterOptions DefaultOptions()
{
    TopologyRecoveryReporterOptions options;
    options.reportTimeout = REPORT_TIMEOUT;
    options.minRetryBackoff = MIN_RETRY_BACKOFF;
    options.maxRetryBackoff = MAX_RETRY_BACKOFF;
    options.maxInitialJitter = 0ms;
    return options;
}

TopologyRecoveryReporter::SnapshotProvider Snapshot(uint64_t version, std::string canonical)
{
    return [version, canonical = std::move(canonical)](uint64_t &outputVersion, std::string &outputCanonical) {
        outputVersion = version;
        outputCanonical = canonical;
        return Status::OK();
    };
}

TopologyRecoveryReporter::SnapshotProvider NoSnapshot()
{
    return [](uint64_t &, std::string &) { return Status(K_NOT_FOUND, "no last-good topology"); };
}

void OpenReportGates(TopologyRecoveryReporter &reporter, const std::string &coordinatorId)
{
    reporter.NotifyRuntimeReady();
    reporter.NotifyMembershipReady(coordinatorId);
}

bool TriggerNextMembershipRound(TopologyRecoveryReporter &reporter, FakeCoordinatorServiceProxy &proxy,
                                const std::string &coordinatorId, size_t expectedCalls)
{
    const auto deadline = std::chrono::steady_clock::now() + ASYNC_TIMEOUT;
    while (std::chrono::steady_clock::now() < deadline) {
        reporter.NotifyMembershipReady(coordinatorId);
        if (proxy.WaitForStarted(expectedCalls, SIGNAL_RETRY_INTERVAL)) {
            return true;
        }
    }
    return false;
}

ReportAction Accepted(coordinator::CoordinatorRecoveryStatePb state, bool payloadRequired = false)
{
    ReportAction action;
    action.recoveryState = state;
    action.payloadRequired = payloadRequired;
    return action;
}

TEST(TopologyRecoveryReporterTest, ReportsNoSnapshotEvidenceWithoutPayload)
{
    FakeCoordinatorServiceProxy proxy;
    proxy.PushAction(Accepted(coordinator::COORDINATOR_READY));
    TopologyRecoveryReporter reporter(proxy, CLUSTER_NAME, REPORTER_ADDRESS, NoSnapshot(), DefaultOptions());

    OpenReportGates(reporter, COORDINATOR_A);

    ASSERT_TRUE(proxy.WaitForReturned(1));
    const auto request = proxy.RequestAt(0);
    EXPECT_EQ(request.cluster_name(), CLUSTER_NAME);
    EXPECT_EQ(request.coordinator_id(), COORDINATOR_A);
    EXPECT_EQ(request.reporter_address(), REPORTER_ADDRESS);
    EXPECT_EQ(request.result(), coordinator::TOPOLOGY_RECOVERY_NO_SNAPSHOT);
    EXPECT_EQ(request.topology_version(), 0UL);
    EXPECT_TRUE(request.topology_digest().empty());
    EXPECT_TRUE(request.canonical_topology().empty());
    EXPECT_EQ(proxy.TimeoutAt(0), DefaultOptions().reportTimeout.count());
}

TEST(TopologyRecoveryReporterTest, SendsSnapshotEvidenceThenRequestedPayload)
{
    const std::string canonical = "canonical-topology";
    FakeCoordinatorServiceProxy proxy;
    proxy.PushAction(Accepted(coordinator::COORDINATOR_RECOVERING, true));
    proxy.PushAction(Accepted(coordinator::COORDINATOR_READY));
    TopologyRecoveryReporter reporter(proxy, CLUSTER_NAME, REPORTER_ADDRESS,
                                      Snapshot(TOPOLOGY_VERSION, canonical), DefaultOptions());

    OpenReportGates(reporter, COORDINATOR_A);

    ASSERT_TRUE(proxy.WaitForReturned(2));
    const auto evidence = proxy.RequestAt(0);
    const auto payload = proxy.RequestAt(1);
    EXPECT_EQ(evidence.result(), coordinator::TOPOLOGY_RECOVERY_SNAPSHOT);
    EXPECT_EQ(evidence.topology_version(), TOPOLOGY_VERSION);
    EXPECT_FALSE(evidence.topology_digest().empty());
    EXPECT_TRUE(evidence.canonical_topology().empty());
    EXPECT_EQ(payload.topology_digest(), evidence.topology_digest());
    EXPECT_EQ(payload.canonical_topology(), canonical);
}

TEST(TopologyRecoveryReporterTest, RetriesRequestedPayloadAfterTransientFailure)
{
    const std::string canonical = "canonical-topology";
    FakeCoordinatorServiceProxy proxy;
    proxy.PushAction(Accepted(coordinator::COORDINATOR_RECOVERING, true));
    ReportAction transientFailure;
    transientFailure.status = Status(K_RPC_UNAVAILABLE, "transient payload failure");
    proxy.PushAction(std::move(transientFailure));
    proxy.PushAction(Accepted(coordinator::COORDINATOR_READY));
    TopologyRecoveryReporter reporter(proxy, CLUSTER_NAME, REPORTER_ADDRESS,
                                      Snapshot(TOPOLOGY_VERSION, canonical), DefaultOptions());

    OpenReportGates(reporter, COORDINATOR_A);

    ASSERT_TRUE(proxy.WaitForReturned(3));
    EXPECT_TRUE(proxy.RequestAt(0).canonical_topology().empty());
    EXPECT_EQ(proxy.RequestAt(1).canonical_topology(), canonical);
    EXPECT_EQ(proxy.RequestAt(2).canonical_topology(), canonical);
}

TEST(TopologyRecoveryReporterTest, DoesNotReportWhenSnapshotExportFails)
{
    FakeCoordinatorServiceProxy proxy;
    std::promise<void> exportAttempted;
    auto exportResult = exportAttempted.get_future();
    TopologyRecoveryReporter reporter(
        proxy, CLUSTER_NAME, REPORTER_ADDRESS,
        [&exportAttempted](uint64_t &, std::string &) {
            exportAttempted.set_value();
            return Status(K_RUNTIME_ERROR, "snapshot export failed");
        },
        DefaultOptions());

    OpenReportGates(reporter, COORDINATOR_A);

    ASSERT_EQ(exportResult.wait_for(ASYNC_TIMEOUT), std::future_status::ready);
    EXPECT_TRUE(reporter.Shutdown().IsOk());
    EXPECT_EQ(proxy.StartedCount(), 0UL);
}

TEST(TopologyRecoveryReporterTest, RequiresMembershipAndRuntimeReadiness)
{
    FakeCoordinatorServiceProxy proxy;
    proxy.PushAction(Accepted(coordinator::COORDINATOR_READY));
    TopologyRecoveryReporter reporter(proxy, CLUSTER_NAME, REPORTER_ADDRESS, NoSnapshot(), DefaultOptions());

    reporter.NotifyRuntimeReady();
    EXPECT_EQ(proxy.StartedCount(), 0UL);
    reporter.NotifyMembershipReady(COORDINATOR_A);
    ASSERT_TRUE(proxy.WaitForReturned(1));

    FakeCoordinatorServiceProxy secondProxy;
    secondProxy.PushAction(Accepted(coordinator::COORDINATOR_READY));
    TopologyRecoveryReporter secondReporter(secondProxy, CLUSTER_NAME, REPORTER_ADDRESS, NoSnapshot(),
                                            DefaultOptions());
    secondReporter.NotifyMembershipReady(COORDINATOR_A);
    EXPECT_EQ(secondProxy.StartedCount(), 0UL);
    secondReporter.NotifyRuntimeReady();
    EXPECT_TRUE(secondProxy.WaitForReturned(1));
}

TEST(TopologyRecoveryReporterTest, CoordinatorIdSwitchDiscardsOldRoundCompletion)
{
    FakeCoordinatorServiceProxy proxy;
    ReportAction oldRound = Accepted(coordinator::COORDINATOR_READY);
    oldRound.block = true;
    proxy.PushAction(std::move(oldRound));
    proxy.PushAction(Accepted(coordinator::COORDINATOR_READY));
    TopologyRecoveryReporter reporter(proxy, CLUSTER_NAME, REPORTER_ADDRESS, NoSnapshot(), DefaultOptions());

    OpenReportGates(reporter, COORDINATOR_A);
    ASSERT_TRUE(proxy.WaitUntilBlocked());
    reporter.NotifyMembershipReady(COORDINATOR_B);
    proxy.ReleaseBlockedCall();

    ASSERT_TRUE(proxy.WaitForReturned(2));
    EXPECT_EQ(proxy.RequestAt(0).coordinator_id(), COORDINATOR_A);
    EXPECT_EQ(proxy.RequestAt(1).coordinator_id(), COORDINATOR_B);
}

TEST(TopologyRecoveryReporterTest, ReadyCompletesCoordinatorRound)
{
    FakeCoordinatorServiceProxy proxy;
    proxy.PushAction(Accepted(coordinator::COORDINATOR_READY));
    TopologyRecoveryReporter reporter(proxy, CLUSTER_NAME, REPORTER_ADDRESS, NoSnapshot(), DefaultOptions());

    OpenReportGates(reporter, COORDINATOR_A);
    ASSERT_TRUE(proxy.WaitForReturned(1));
    reporter.NotifyMembershipReady(COORDINATOR_A);
    reporter.NotifyRuntimeReady();

    EXPECT_EQ(proxy.StartedCount(), 1UL);
}

TEST(TopologyRecoveryReporterTest, RecoveringWaitsForNextMembershipSignal)
{
    FakeCoordinatorServiceProxy proxy;
    proxy.PushAction(Accepted(coordinator::COORDINATOR_RECOVERING));
    proxy.PushAction(Accepted(coordinator::COORDINATOR_READY));
    TopologyRecoveryReporter reporter(proxy, CLUSTER_NAME, REPORTER_ADDRESS, NoSnapshot(), DefaultOptions());

    OpenReportGates(reporter, COORDINATOR_A);
    ASSERT_TRUE(proxy.WaitForReturned(1));
    EXPECT_EQ(proxy.StartedCount(), 1UL);
    ASSERT_TRUE(TriggerNextMembershipRound(reporter, proxy, COORDINATOR_A, 2));

    EXPECT_TRUE(proxy.WaitForReturned(2));
}

void VerifyRetryCanBeCancelled(StatusCode retryCode)
{
    FakeCoordinatorServiceProxy proxy;
    ReportAction retry;
    retry.status = Status(retryCode, "retryable failure");
    proxy.PushAction(std::move(retry));
    proxy.PushAction(Accepted(coordinator::COORDINATOR_READY));
    auto options = DefaultOptions();
    options.minRetryBackoff = CANCEL_TEST_BACKOFF;
    options.maxRetryBackoff = CANCEL_TEST_BACKOFF;
    TopologyRecoveryReporter reporter(proxy, CLUSTER_NAME, REPORTER_ADDRESS, NoSnapshot(), options);

    OpenReportGates(reporter, COORDINATOR_A);
    ASSERT_TRUE(proxy.WaitForReturned(1));
    reporter.NotifyMembershipReady(COORDINATOR_B);

    ASSERT_TRUE(proxy.WaitForReturned(2));
    EXPECT_EQ(proxy.RequestAt(1).coordinator_id(), COORDINATOR_B);
}

TEST(TopologyRecoveryReporterTest, TransportAndAdmissionBackoffAreCancellable)
{
    VerifyRetryCanBeCancelled(K_RPC_UNAVAILABLE);
    VerifyRetryCanBeCancelled(K_TRY_AGAIN);
}

TEST(TopologyRecoveryReporterTest, RejectedReportsWaitForNextMembershipSignal)
{
    FakeCoordinatorServiceProxy proxy;
    ReportAction mismatch;
    mismatch.result = coordinator::ReportTopologyRecoveryCandidateRspPb::COORDINATOR_ID_MISMATCH;
    ReportAction membershipNotReady;
    membershipNotReady.result = coordinator::ReportTopologyRecoveryCandidateRspPb::MEMBERSHIP_NOT_READY;
    proxy.PushAction(std::move(mismatch));
    proxy.PushAction(std::move(membershipNotReady));
    proxy.PushAction(Accepted(coordinator::COORDINATOR_READY));
    TopologyRecoveryReporter reporter(proxy, CLUSTER_NAME, REPORTER_ADDRESS, NoSnapshot(), DefaultOptions());

    OpenReportGates(reporter, COORDINATOR_A);
    ASSERT_TRUE(proxy.WaitForReturned(1));
    ASSERT_TRUE(TriggerNextMembershipRound(reporter, proxy, COORDINATOR_A, 2));
    ASSERT_TRUE(proxy.WaitForReturned(2));
    ASSERT_TRUE(TriggerNextMembershipRound(reporter, proxy, COORDINATOR_A, 3));

    EXPECT_TRUE(proxy.WaitForReturned(3));
}

TEST(TopologyRecoveryReporterTest, ShutdownInterruptsRetryAndDrainsInFlightReport)
{
    FakeCoordinatorServiceProxy proxy;
    ReportAction blocked = Accepted(coordinator::COORDINATOR_READY);
    blocked.block = true;
    proxy.PushAction(std::move(blocked));
    TopologyRecoveryReporter reporter(proxy, CLUSTER_NAME, REPORTER_ADDRESS, NoSnapshot(), DefaultOptions());
    OpenReportGates(reporter, COORDINATOR_A);
    ASSERT_TRUE(proxy.WaitUntilBlocked());

    auto shutdown = std::async(std::launch::async, [&reporter] { return reporter.Shutdown(); });
    EXPECT_EQ(shutdown.wait_for(SHUTDOWN_OBSERVATION), std::future_status::timeout);
    proxy.ReleaseBlockedCall();

    ASSERT_EQ(shutdown.wait_for(ASYNC_TIMEOUT), std::future_status::ready);
    EXPECT_TRUE(shutdown.get().IsOk());
    EXPECT_TRUE(reporter.Shutdown().IsOk());
}

TEST(TopologyRecoveryReporterTest, RejectsInvalidRetryAndDeadlineOptions)
{
    FakeCoordinatorServiceProxy proxy;
    auto options = DefaultOptions();
    options.minRetryBackoff = std::chrono::milliseconds::zero();
    EXPECT_THROW(TopologyRecoveryReporter(proxy, CLUSTER_NAME, REPORTER_ADDRESS, NoSnapshot(), options),
                 std::invalid_argument);

    options = DefaultOptions();
    options.maxRetryBackoff = options.minRetryBackoff / 2;
    EXPECT_THROW(TopologyRecoveryReporter(proxy, CLUSTER_NAME, REPORTER_ADDRESS, NoSnapshot(), options),
                 std::invalid_argument);
}

}  // namespace
}  // namespace datasystem::worker
