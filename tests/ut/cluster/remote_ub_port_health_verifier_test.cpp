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

#include <atomic>
#include <cstdint>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#define private public
#include "datasystem/cluster/ub_health/remote_ub_port_health_verifier.h"
#undef private
#include "datasystem/common/flags/common_flags.h"
#include "datasystem/common/object_cache/ub_port_health.h"
#include "datasystem/common/util/raii.h"

DS_DECLARE_bool(alsologtostderr);

namespace datasystem::cluster {
namespace {
const HostPort WORKER("127.0.0.1", 18480);
constexpr char INCARNATION[] = "worker-incarnation";

UbHealthSummary Summary(uint32_t totalPorts, uint32_t badPorts, uint64_t healthEpoch,
                        bool pending = false)
{
    UbHealthSummary summary;
    summary.worker = WORKER;
    summary.incarnation = INCARNATION;
    summary.portHealth = UbPortHealthSummary{ true, totalPorts, badPorts, healthEpoch, pending };
    return summary;
}

UbHealthSummary SummaryFor(const HostPort &worker, const std::string &incarnation,
                           uint32_t totalPorts, uint32_t badPorts, uint64_t healthEpoch)
{
    auto summary = Summary(totalPorts, badPorts, healthEpoch);
    summary.worker = worker;
    summary.incarnation = incarnation;
    return summary;
}

size_t CountOccurrences(const std::string &text, const std::string &needle)
{
    size_t count = 0;
    for (size_t position = 0; (position = text.find(needle, position)) != std::string::npos;
         position += needle.size()) {
        ++count;
    }
    return count;
}
}  // namespace

TEST(RemoteUbPortHealthVerifierTest, LogsOnlyChangedResponsesAndRetryStatus)
{
    const bool oldAlsoLogToStderr = FLAGS_alsologtostderr;
    Raii restoreFlag([oldAlsoLogToStderr] { FLAGS_alsologtostderr = oldAlsoLogToStderr; });
    FLAGS_alsologtostderr = true;
    RemoteUbPortHealthVerifier verifier(12345, 1'000, 1'000);

    testing::internal::CaptureStderr();
    ASSERT_TRUE(verifier.RequestVerification(WORKER, INCARNATION, 0));
    auto ticket = verifier.TryBeginDue(0);
    ASSERT_TRUE(ticket.has_value());
    EXPECT_TRUE(verifier.Complete(*ticket, Summary(4, 4, 1), Status::OK(), 1).evidenceAccepted);

    ticket = verifier.TryBeginDue(1'001);
    ASSERT_TRUE(ticket.has_value());
    EXPECT_TRUE(verifier.Complete(*ticket, Summary(4, 4, 1), Status::OK(), 1'002).evidenceAccepted);

    ticket = verifier.TryBeginDue(2'002);
    ASSERT_TRUE(ticket.has_value());
    EXPECT_TRUE(verifier.Complete(*ticket, std::nullopt,
                                  Status(K_RPC_DEADLINE_EXCEEDED, "timeout"), 2'003).retryScheduled);
    ticket = verifier.TryBeginDue(3'003);
    ASSERT_TRUE(ticket.has_value());
    EXPECT_TRUE(verifier.Complete(*ticket, std::nullopt,
                                  Status(K_RPC_DEADLINE_EXCEEDED, "timeout"), 3'004).retryScheduled);

    ticket = verifier.TryBeginDue(4'004);
    ASSERT_TRUE(ticket.has_value());
    EXPECT_TRUE(verifier.Complete(*ticket, Summary(4, 3, 2), Status::OK(), 4'005).evidenceAccepted);
    const auto logs = testing::internal::GetCapturedStderr();

    EXPECT_EQ(CountOccurrences(logs, "UB_PORT_QUERY action=response"), 2u) << logs;
    EXPECT_EQ(CountOccurrences(logs, "UB_PORT_QUERY action=retry"), 1u) << logs;
    EXPECT_NE(logs.find("decision=ISOLATE source=query_response"), std::string::npos) << logs;
    EXPECT_NE(logs.find("decision=RECOVER source=query_response"), std::string::npos) << logs;
    EXPECT_NE(logs.find("incarnation_prefix=" + FormatUbHealthIncarnationPrefix(INCARNATION)),
              std::string::npos) << logs;
    EXPECT_NE(logs.find("next_retry_ms=1000"), std::string::npos) << logs;
}
TEST(RemoteUbPortHealthVerifierTest, ClientModeQueriesEverySecondUntilAnyPortRecovers)
{
    RemoteUbPortHealthVerifier verifier(12345, 1'000, 1'000);
    ASSERT_TRUE(verifier.RequestVerification(WORKER, INCARNATION, 100));

    auto first = verifier.TryBeginDue(100);
    ASSERT_TRUE(first.has_value());
    auto isolated = verifier.Complete(*first, Summary(4, 4, 1), Status::OK(), 120);
    EXPECT_TRUE(isolated.evidenceAccepted);
    EXPECT_TRUE(isolated.retryScheduled);
    EXPECT_EQ(verifier.NextQueryDeadlineMs(), 1'120u);
    EXPECT_FALSE(verifier.RequestVerification(
        WORKER, INCARNATION, 200));
    EXPECT_EQ(verifier.NextQueryDeadlineMs(), 1'120u);
    EXPECT_FALSE(verifier.TryBeginDue(1'119).has_value());

    auto second = verifier.TryBeginDue(1'120);
    ASSERT_TRUE(second.has_value());
    auto recovered = verifier.Complete(*second, Summary(4, 3, 2), Status::OK(), 1'140);
    EXPECT_TRUE(recovered.evidenceAccepted);
    EXPECT_FALSE(recovered.retryScheduled);
    EXPECT_FALSE(verifier.NextQueryDeadlineMs().has_value());
}

TEST(RemoteUbPortHealthVerifierTest, QueryFailureRetriesUntilFirstUsablePortFact)
{
    RemoteUbPortHealthVerifier clientVerifier(12345, 1'000, 1'000);
    ASSERT_TRUE(clientVerifier.RequestVerification(
        WORKER, INCARNATION, 0));
    auto clientTicket = clientVerifier.TryBeginDue(0);
    ASSERT_TRUE(clientTicket.has_value());
    auto clientResult = clientVerifier.Complete(
        *clientTicket, std::nullopt, Status(K_RPC_DEADLINE_EXCEEDED, "timeout"), 10);
    EXPECT_FALSE(clientResult.evidenceAccepted);
    EXPECT_TRUE(clientResult.retryScheduled);
    EXPECT_EQ(clientVerifier.NextQueryDeadlineMs(), 1'010u);

    auto isolationTicket = clientVerifier.TryBeginDue(1'010);
    ASSERT_TRUE(isolationTicket.has_value());
    auto isolated = clientVerifier.Complete(*isolationTicket, Summary(4, 4, 1), Status::OK(), 1'020);
    EXPECT_TRUE(isolated.evidenceAccepted);
    EXPECT_TRUE(isolated.retryScheduled);
    auto recoveryTicket = clientVerifier.TryBeginDue(2'020);
    ASSERT_TRUE(recoveryTicket.has_value());
    auto recoveryFailure = clientVerifier.Complete(
        *recoveryTicket, std::nullopt, Status(K_RPC_DEADLINE_EXCEEDED, "timeout"), 2'030);
    EXPECT_TRUE(recoveryFailure.retryScheduled);
    EXPECT_EQ(clientVerifier.NextQueryDeadlineMs(), 3'030u);

    RemoteUbPortHealthVerifier workerVerifier(12345, 1'000, 1'000);
    ASSERT_TRUE(workerVerifier.RequestVerification(
        WORKER, INCARNATION, 0));
    auto workerTicket = workerVerifier.TryBeginDue(0);
    ASSERT_TRUE(workerTicket.has_value());
    auto workerResult = workerVerifier.Complete(
        *workerTicket, std::nullopt, Status(K_RPC_DEADLINE_EXCEEDED, "timeout"), 10);
    EXPECT_TRUE(workerResult.retryScheduled);
    EXPECT_EQ(workerVerifier.NextQueryDeadlineMs(), 1'010u);
    EXPECT_FALSE(workerVerifier.RequestVerification(
        WORKER, INCARNATION, 999));
    EXPECT_FALSE(workerVerifier.RequestVerification(
        WORKER, INCARNATION, 1'000));
    EXPECT_EQ(workerVerifier.NextQueryDeadlineMs(), 1'010u);
}

TEST(RemoteUbPortHealthVerifierTest, ConcurrentTriggersProduceOneInFlightQuery)
{
    RemoteUbPortHealthVerifier verifier(12345, 1'000, 1'000);
    std::atomic<size_t> acceptedRequests{ 0 };
    std::vector<std::thread> requesters;
    for (size_t index = 0; index < 16; ++index) {
        requesters.emplace_back([&] {
            if (verifier.RequestVerification(
                    WORKER, INCARNATION, 0)) {
                acceptedRequests.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }
    for (auto &requester : requesters) {
        requester.join();
    }
    EXPECT_EQ(acceptedRequests.load(std::memory_order_relaxed), 1u);

    std::atomic<size_t> tickets{ 0 };
    std::vector<std::thread> dispatchers;
    for (size_t index = 0; index < 16; ++index) {
        dispatchers.emplace_back([&] {
            if (verifier.TryBeginDue(0).has_value()) {
                tickets.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }
    for (auto &dispatcher : dispatchers) {
        dispatcher.join();
    }
    EXPECT_EQ(tickets.load(std::memory_order_relaxed), 1u);
}

TEST(RemoteUbPortHealthVerifierTest, TriggerDuringQuerySchedulesOneRateLimitedFollowUp)
{
    RemoteUbPortHealthVerifier verifier(12345, 1'000, 1'000);
    ASSERT_TRUE(verifier.RequestVerification(WORKER, INCARNATION, 100));
    auto ticket = verifier.TryBeginDue(100);
    ASSERT_TRUE(ticket.has_value());

    EXPECT_TRUE(verifier.RequestVerification(WORKER, INCARNATION, 150));
    EXPECT_FALSE(verifier.RequestVerification(WORKER, INCARNATION, 160));
    auto completion = verifier.Complete(*ticket, Summary(4, 0, 1), Status::OK(), 200);

    EXPECT_TRUE(completion.retryScheduled);
    EXPECT_EQ(verifier.NextQueryDeadlineMs(), 1'200u);
}

TEST(RemoteUbPortHealthVerifierTest, UnsupportedPeerUsesBoundedBackoff)
{
    RemoteUbPortHealthVerifier verifier(12345, 1'000, 1'000);
    ASSERT_TRUE(verifier.RequestVerification(WORKER, INCARNATION, 100));
    auto ticket = verifier.TryBeginDue(100);
    ASSERT_TRUE(ticket.has_value());

    auto completion = verifier.Complete(
        *ticket, std::nullopt, Status(K_NOT_SUPPORTED, "old Worker"), 120);

    EXPECT_TRUE(completion.retryScheduled);
    EXPECT_EQ(verifier.NextQueryDeadlineMs(), 30'120u);
}

TEST(RemoteUbPortHealthVerifierTest, SuccessfulRetryPreservesOtherIsolationDeadlines)
{
    const HostPort secondWorker("127.0.0.1", 18481);
    constexpr char secondIncarnation[] = "second-incarnation";
    RemoteUbPortHealthVerifier verifier(12345, 1'000, 1'000);
    ASSERT_TRUE(verifier.RequestVerification(WORKER, INCARNATION, 0));

    auto failedTicket = verifier.TryBeginDue(0);
    ASSERT_TRUE(failedTicket.has_value());
    ASSERT_EQ(failedTicket->peer, WORKER);
    ASSERT_TRUE(verifier.RequestVerification(secondWorker, secondIncarnation, 0));
    verifier.Complete(*failedTicket, std::nullopt, Status(K_RPC_DEADLINE_EXCEEDED, "timeout"), 10);

    auto isolatedTicket = verifier.TryBeginDue(10);
    ASSERT_TRUE(isolatedTicket.has_value());
    ASSERT_EQ(isolatedTicket->peer, secondWorker);
    verifier.Complete(*isolatedTicket,
                      SummaryFor(secondWorker, secondIncarnation, 4, 4, 1), Status::OK(), 120);
    EXPECT_EQ(verifier.NextQueryDeadlineMs(), 1'010u);

    auto retry = verifier.TryBeginDue(1'010);
    ASSERT_TRUE(retry.has_value());
    ASSERT_EQ(retry->peer, WORKER);
    verifier.Complete(*retry, Summary(4, 0, 1), Status::OK(), 1'020);

    EXPECT_EQ(verifier.NextQueryDeadlineMs(), 1'120u);
}

TEST(RemoteUbPortHealthVerifierTest, RandomPolicyCoversUnusableAndAllDownResults)
{
    const std::vector<std::pair<Status, std::optional<UbHealthSummary>>> results{
        { Status(K_RPC_UNAVAILABLE, "rpc failure"), std::nullopt },
        { Status::OK(), std::nullopt },
        { Status::OK(), Summary(4, 1, 1, true) },
        { Status::OK(), Summary(4, 4, 1) },
    };
    for (const auto &[status, summary] : results) {
        RemoteUbPortHealthVerifier verifier(12345, 17'000, 17'000);
        ASSERT_TRUE(verifier.RequestVerification(WORKER, INCARNATION, 100));
        auto ticket = verifier.TryBeginDue(100);
        ASSERT_TRUE(ticket.has_value());
        EXPECT_TRUE(verifier.Complete(*ticket, summary, status, 120).retryScheduled);
        EXPECT_EQ(verifier.NextQueryDeadlineMs(), 17'120u);
    }
}

TEST(RemoteUbPortHealthVerifierTest, RandomPolicyDerivesStableIndependentSequences)
{
    auto sequence = [](uint64_t seed, const HostPort &peer, const std::string &incarnation) {
        RemoteUbPortHealthVerifier verifier(seed, 1'000, 30'000);
        RemoteUbPortHealthVerifier::PeerState state;
        state.incarnation = incarnation;
        std::vector<uint64_t> delays;
        for (uint64_t generation = 1; generation <= 8; ++generation) {
            state.generation = generation;
            delays.emplace_back(verifier.RetryDeadlineMs(peer, state, 0));
        }
        return delays;
    };
    const auto first = sequence(12345, WORKER, INCARNATION);
    EXPECT_EQ(first, sequence(12345, WORKER, INCARNATION));
    EXPECT_NE(first, sequence(98765, WORKER, INCARNATION));
    EXPECT_NE(first, sequence(12345, HostPort("127.0.0.1", 18481), INCARNATION));
    EXPECT_NE(first, sequence(12345, WORKER, "replacement-incarnation"));
    for (const auto delay : first) {
        EXPECT_GE(delay, UB_REMOTE_PORT_HEALTH_RETRY_MIN_MS);
        EXPECT_LE(delay, UB_REMOTE_PORT_HEALTH_RETRY_MAX_MS);
    }
}

TEST(RemoteUbPortHealthVerifierTest, PassiveHintAndPeerRecoveryPreserveIndependentDeadlines)
{
    const HostPort secondWorker("127.0.0.1", 18481);
    constexpr char secondIncarnation[] = "second-incarnation";
    RemoteUbPortHealthVerifier verifier(12345, 17'000, 17'000);
    ASSERT_TRUE(verifier.RequestVerification(WORKER, INCARNATION, 0));
    ASSERT_TRUE(verifier.RequestVerification(secondWorker, secondIncarnation, 0));
    auto first = verifier.TryBeginDue(0);
    ASSERT_TRUE(first.has_value());
    auto second = verifier.TryBeginDue(0);
    ASSERT_TRUE(second.has_value());
    verifier.Complete(*first, SummaryFor(first->peer, first->incarnation, 4, 4, 1), Status::OK(), 10);
    verifier.Complete(*second, SummaryFor(second->peer, second->incarnation, 4, 4, 1), Status::OK(), 20);

    auto hint = Summary(4, 3, 2);
    EXPECT_FALSE(verifier.NotifySummaryHint(hint, 100));
    EXPECT_EQ(verifier.NextQueryDeadlineMs(), 17'010u);
    auto recovered = verifier.TryBeginDue(17'010);
    ASSERT_TRUE(recovered.has_value());
    ASSERT_EQ(recovered->peer, WORKER);
    verifier.Complete(*recovered, Summary(4, 0, 3), Status::OK(), 17'011);
    EXPECT_EQ(verifier.NextQueryDeadlineMs(), 17'020u);
}

TEST(RemoteUbPortHealthVerifierTest, SameEpochConfirmedPassiveRecoveryCancelsIsolationRetry)
{
    RemoteUbPortHealthVerifier verifier(12345, 17'000, 17'000);
    ASSERT_TRUE(verifier.RequestVerification(WORKER, INCARNATION, 0));
    auto ticket = verifier.TryBeginDue(0);
    ASSERT_TRUE(ticket.has_value());
    verifier.Complete(*ticket, Summary(4, 4, 1), Status::OK(), 10);
    ASSERT_TRUE(verifier.NextQueryDeadlineMs().has_value());

    EXPECT_FALSE(verifier.AcceptPassiveRecovery(Summary(4, 3, 1)));
    auto pending = Summary(4, 3, 2, true);
    EXPECT_FALSE(verifier.AcceptPassiveRecovery(pending));
    EXPECT_FALSE(verifier.NotifySummaryHint(pending, 20));
    EXPECT_TRUE(verifier.AcceptPassiveRecovery(Summary(4, 3, 2)));
    EXPECT_FALSE(verifier.NextQueryDeadlineMs().has_value());
}

TEST(RemoteUbPortHealthVerifierTest, SameEpochConflictingPassiveRecoveryKeepsIsolationRetry)
{
    RemoteUbPortHealthVerifier verifier(12345, 17'000, 17'000);
    ASSERT_TRUE(verifier.RequestVerification(WORKER, INCARNATION, 0));
    auto ticket = verifier.TryBeginDue(0);
    ASSERT_TRUE(ticket.has_value());
    verifier.Complete(*ticket, Summary(4, 4, 1), Status::OK(), 10);

    auto pending = Summary(4, 3, 2, true);
    EXPECT_FALSE(verifier.NotifySummaryHint(pending, 20));
    EXPECT_FALSE(verifier.AcceptPassiveRecovery(Summary(4, 2, 2)));
    EXPECT_EQ(verifier.NextQueryDeadlineMs(), 17'010u);
}

TEST(RemoteUbPortHealthVerifierTest, PassiveRecoveryFencesOlderInFlightQuery)
{
    RemoteUbPortHealthVerifier verifier(12345, 1'000, 1'000);
    ASSERT_TRUE(verifier.RequestVerification(WORKER, INCARNATION, 0));
    auto isolation = verifier.TryBeginDue(0);
    ASSERT_TRUE(isolation.has_value());
    verifier.Complete(*isolation, Summary(4, 4, 1), Status::OK(), 10);
    auto retry = verifier.TryBeginDue(1'010);
    ASSERT_TRUE(retry.has_value());

    EXPECT_TRUE(verifier.AcceptPassiveRecovery(Summary(4, 0, 2)));
    auto completion = verifier.Complete(*retry, std::nullopt, Status(K_NOT_SUPPORTED, "old Worker"), 1'020);
    EXPECT_FALSE(completion.evidenceAccepted);
    EXPECT_FALSE(completion.retryScheduled);
    EXPECT_FALSE(verifier.NextQueryDeadlineMs().has_value());
}

TEST(RemoteUbPortHealthVerifierTest, RandomBoundsAndUnsupportedBackoffRemainExact)
{
    for (const auto delay : { UB_REMOTE_PORT_HEALTH_RETRY_MIN_MS, UB_REMOTE_PORT_HEALTH_RETRY_MAX_MS }) {
        RemoteUbPortHealthVerifier verifier(12345, delay, delay);
        ASSERT_TRUE(verifier.RequestVerification(WORKER, INCARNATION, 100));
        auto ticket = verifier.TryBeginDue(100);
        ASSERT_TRUE(ticket.has_value());
        EXPECT_TRUE(verifier.Complete(*ticket, std::nullopt, Status(K_RPC_UNAVAILABLE, "retry"), 120).retryScheduled);
        EXPECT_EQ(verifier.NextQueryDeadlineMs(), 120 + delay);
    }
    RemoteUbPortHealthVerifier unsupported(12345, 1'000, 30'000);
    ASSERT_TRUE(unsupported.RequestVerification(WORKER, INCARNATION, 0));
    auto ticket = unsupported.TryBeginDue(0);
    ASSERT_TRUE(ticket.has_value());
    EXPECT_TRUE(unsupported.Complete(*ticket, std::nullopt, Status(K_NOT_SUPPORTED, "old worker"), 10).retryScheduled);
    EXPECT_EQ(unsupported.NextQueryDeadlineMs(), 30'010u);
}

TEST(RemoteUbPortHealthVerifierTest, InvalidRetryBoundsFallBackToProductionDefaults)
{
    for (const auto &[retryMinMs, retryMaxMs] :
         std::vector<std::pair<uint64_t, uint64_t>>{ { 0, 1'000 }, { 1'000, 30'001 }, { 2'000, 1'000 } }) {
        RemoteUbPortHealthVerifier verifier(12345, retryMinMs, retryMaxMs);
        EXPECT_EQ(verifier.retryMinMs_, UB_REMOTE_PORT_HEALTH_RETRY_MIN_MS);
        EXPECT_EQ(verifier.retryMaxMs_, UB_REMOTE_PORT_HEALTH_RETRY_MAX_MS);
        ASSERT_TRUE(verifier.RequestVerification(WORKER, INCARNATION, 100));
        auto ticket = verifier.TryBeginDue(100);
        ASSERT_TRUE(ticket.has_value());
        ASSERT_TRUE(verifier.Complete(*ticket, Summary(4, 4, 1), Status::OK(), 120).retryScheduled);
        ASSERT_TRUE(verifier.NextQueryDeadlineMs().has_value());
        EXPECT_GE(*verifier.NextQueryDeadlineMs(), 120 + UB_REMOTE_PORT_HEALTH_RETRY_MIN_MS);
        EXPECT_LE(*verifier.NextQueryDeadlineMs(), 120 + UB_REMOTE_PORT_HEALTH_RETRY_MAX_MS);
    }
}

TEST(RemoteUbPortHealthVerifierTest, RetiredTicketCannotConsumeSameIncarnationReplacement)
{
    RemoteUbPortHealthVerifier verifier(12345, 1'000, 30'000);
    ASSERT_TRUE(verifier.RequestVerification(WORKER, INCARNATION, 0));
    auto retired = verifier.TryBeginDue(0);
    ASSERT_TRUE(retired.has_value());
    verifier.ReconcileTopology({});
    verifier.ReconcileTopology({ { WORKER, INCARNATION } });
    ASSERT_TRUE(verifier.RequestVerification(WORKER, INCARNATION, 1));
    auto current = verifier.TryBeginDue(1);
    ASSERT_TRUE(current.has_value());
    EXPECT_NE(retired->generation, current->generation);
    EXPECT_FALSE(verifier.Complete(*retired, Summary(4, 4, 1), Status::OK(), 2).evidenceAccepted);
    EXPECT_TRUE(verifier.Complete(*current, Summary(4, 0, 2), Status::OK(), 3).evidenceAccepted);
}

TEST(RemoteUbPortHealthVerifierTest, SamePeerRapidTriggersRespectPerPeerMinQueryInterval)
{
    RemoteUbPortHealthVerifier verifier(12345, 1'000, 30'000);
    ASSERT_TRUE(verifier.RequestVerification(WORKER, INCARNATION, 0));
    auto ticket = verifier.TryBeginDue(0);
    ASSERT_TRUE(ticket.has_value());
    EXPECT_TRUE(verifier.Complete(*ticket, Summary(4, 0, 1), Status::OK(), 1).evidenceAccepted);
    // A GOOD completion resets the schedule. A new trigger within lastQueryMs + retryMinMs_
    // must be deferred to the per-peer floor instead of becoming immediately due.
    EXPECT_FALSE(verifier.NextQueryDeadlineMs().has_value());
    EXPECT_TRUE(verifier.RequestVerification(WORKER, INCARNATION, 2));
    EXPECT_EQ(verifier.NextQueryDeadlineMs(), 1'000u);
    EXPECT_FALSE(verifier.TryBeginDue(2).has_value());
    EXPECT_TRUE(verifier.TryBeginDue(1'000).has_value());
}

}  // namespace datasystem::cluster
