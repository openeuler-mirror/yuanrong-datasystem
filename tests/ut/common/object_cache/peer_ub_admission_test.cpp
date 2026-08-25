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

/** Description: Unit tests for local UB provider admission state. */

#include <gtest/gtest.h>

#include <atomic>
#include <limits>
#include <thread>
#include <vector>

#include "datasystem/common/flags/common_flags.h"
#include "datasystem/common/object_cache/peer_ub_admission.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/timer.h"

DS_DECLARE_bool(alsologtostderr);

namespace datasystem {
namespace {

const HostPort PEER("127.0.0.1", 31501);
const HostPort SELF("127.0.0.1", 31502);
constexpr char GLOBAL_SUMMARY_LOG_MARKER[] = "UB_HEALTH_SUMMARY action=global_summary_applied";
constexpr char ADMISSION_AVAILABLE_LOG_MARKER[] = "UB admission marked peer AVAILABLE";

struct CapturedProbeCompletion {
    bool recovered;
    std::string logs;
};

std::string CaptureGlobalSummaryReplace(PeerUbAdmission &admission,
                                        const std::vector<UbHealthSummary> &summaries)
{
    testing::internal::CaptureStderr();
    admission.ReplaceGlobalSummaries(summaries);
    return testing::internal::GetCapturedStderr();
}

CapturedProbeCompletion CaptureProbeCompletion(PeerUbAdmission &admission, const UbProbeToken &token,
                                               const Status &status, uint64_t nowMs,
                                               bool requireGlobalAvailable)
{
    testing::internal::CaptureStderr();
    const bool recovered = admission.CompleteProbe(token, status, nowMs, requireGlobalAvailable);
    return { recovered, testing::internal::GetCapturedStderr() };
}

TEST(PeerUbAdmissionTest, ExplicitError4BlocksProviderReadSource)
{
    PeerUbAdmission admission;
    UbOpOutcome outcome(PEER, UbOperationKind::WORKER_REMOTE_GET_WRITEBACK,
                        Status(K_URMA_ERROR, "provider write failed"));
    outcome.cqeStatus = 4;

    admission.ReportOutcome(outcome);

    EXPECT_EQ(admission.CheckReadSource(PEER).GetCode(), K_URMA_DATA_WORKER_UNAVAILABLE);
    auto state = admission.GetState(PEER);
    ASSERT_TRUE(state.has_value());
    EXPECT_EQ(state->state, UbAdmissionState::UNAVAILABLE);
    EXPECT_EQ(state->lastFailureClass, UbFailureClass::PORT_UNAVAILABLE_ERROR4);
}

TEST(PeerUbAdmissionTest, ExplicitError9BlocksRemotePeer)
{
    PeerUbAdmission admission;
    UbOpOutcome outcome(PEER, UbOperationKind::WORKER_REMOTE_GET_WRITEBACK,
                        Status(K_URMA_ERROR, "remote ACK timed out"));
    outcome.cqeStatus = URMA_REMOTE_ACK_TIMEOUT_STATUS;

    admission.ReportOutcome(outcome);

    EXPECT_EQ(admission.CheckReadSource(PEER).GetCode(), K_URMA_DATA_WORKER_UNAVAILABLE);
    const auto state = admission.GetState(PEER);
    ASSERT_TRUE(state.has_value());
    EXPECT_EQ(state->state, UbAdmissionState::UNAVAILABLE);
    EXPECT_EQ(state->lastFailureClass, UbFailureClass::REMOTE_UNAVAILABLE_ERROR9);
}

TEST(PeerUbAdmissionTest, RpcTimeoutIsSuspectAndDoesNotHardBlock)
{
    PeerUbAdmission admission;
    UbOpOutcome outcome(PEER, UbOperationKind::WORKER_REMOTE_GET_WRITEBACK,
                        Status(K_RPC_DEADLINE_EXCEEDED, "remote get timed out"));

    admission.ReportOutcome(outcome);

    EXPECT_TRUE(admission.CheckReadSource(PEER).IsOk());
    auto state = admission.GetState(PEER);
    ASSERT_TRUE(state.has_value());
    EXPECT_EQ(state->state, UbAdmissionState::SUSPECT);
}

TEST(PeerUbAdmissionTest, LateTimeoutCannotDowngradeHardUnavailableEvidence)
{
    PeerUbAdmission admission;
    UbOpOutcome hardFailure(PEER, UbOperationKind::WORKER_REMOTE_GET_WRITEBACK,
                             Status(K_URMA_ERROR, "provider CQE status 4"));
    hardFailure.cqeStatus = 4;
    admission.ReportOutcome(hardFailure);
    const auto hardState = admission.GetState(PEER);
    ASSERT_TRUE(hardState.has_value());

    UbOpOutcome lateTimeout(PEER, UbOperationKind::WORKER_REMOTE_GET_WRITEBACK,
                            Status(K_RPC_DEADLINE_EXCEEDED, "late request timeout"));
    admission.ReportOutcome(lateTimeout);

    const auto state = admission.GetState(PEER);
    ASSERT_TRUE(state.has_value());
    EXPECT_EQ(state->state, UbAdmissionState::UNAVAILABLE);
    EXPECT_EQ(state->lastFailureClass, UbFailureClass::PORT_UNAVAILABLE_ERROR4);
    EXPECT_EQ(state->epoch, hardState->epoch);
    EXPECT_EQ(admission.CheckReadSource(PEER).GetCode(), K_URMA_DATA_WORKER_UNAVAILABLE);
}

TEST(PeerUbAdmissionTest, StartupVerificationKeepsAdmissionOpen)
{
    PeerUbAdmission admission;
    admission.SetSelfWorker(PEER);
    admission.InitializeVerification(PEER, 10);
    EXPECT_TRUE(admission.BuildSelfHealthSummary(PEER).writable);
    EXPECT_TRUE(admission.CheckReadSource(PEER).IsOk());
    EXPECT_TRUE(admission.CheckWriteTarget(PEER, UbOperationKind::MIGRATION_WRITE).IsOk());
    auto probe = admission.TryBeginProbe(PEER, 10);
    ASSERT_TRUE(probe.has_value());

    UbOpOutcome lateTimeout(PEER, UbOperationKind::WORKER_REMOTE_GET_WRITEBACK,
                            Status(K_RPC_DEADLINE_EXCEEDED, "late request timeout"));
    admission.ReportOutcome(lateTimeout);

    const auto state = admission.GetState(PEER);
    ASSERT_TRUE(state.has_value());
    EXPECT_EQ(state->state, UbAdmissionState::SUSPECT);
    EXPECT_EQ(state->epoch, probe->epoch);
    EXPECT_TRUE(admission.CompleteProbe(*probe, Status::OK(), 11, false));
    EXPECT_EQ(admission.GetState(PEER)->state, UbAdmissionState::AVAILABLE);
}

TEST(PeerUbAdmissionTest, StartupVerificationTopologyNotReadyDoesNotQuarantinePeer)
{
    PeerUbAdmission admission;
    admission.SetSelfWorker(PEER);
    admission.InitializeVerification(PEER, 10);
    auto probe = admission.TryBeginProbe(PEER, 10);
    ASSERT_TRUE(probe.has_value());

    EXPECT_FALSE(admission.CompleteProbe(*probe, Status(K_NOT_READY, "topology is joining"), 11, false));

    const auto state = admission.GetState(PEER);
    ASSERT_TRUE(state.has_value());
    EXPECT_EQ(state->state, UbAdmissionState::SUSPECT);
    EXPECT_EQ(state->backoffLevel, 1U);
    EXPECT_TRUE(admission.BuildSelfHealthSummary(PEER).writable);
    EXPECT_TRUE(admission.CheckReadSource(PEER).IsOk());
    EXPECT_TRUE(admission.CheckWriteTarget(PEER, UbOperationKind::MIGRATION_WRITE).IsOk());
}

TEST(PeerUbAdmissionTest, ConcurrentTimeoutReportsCannotDowngradeUnavailableState)
{
    constexpr size_t TIMEOUT_REPORTERS = 8;
    constexpr uint32_t REPORTS_PER_THREAD = 100;
    PeerUbAdmission admission;
    UbOpOutcome hardFailure(PEER, UbOperationKind::WORKER_REMOTE_GET_WRITEBACK,
                             Status(K_URMA_ERROR, "provider CQE status 4"));
    hardFailure.cqeStatus = 4;
    admission.ReportOutcome(hardFailure);

    std::vector<std::thread> reporters;
    reporters.reserve(TIMEOUT_REPORTERS);
    for (size_t reporter = 0; reporter < TIMEOUT_REPORTERS; ++reporter) {
        reporters.emplace_back([&] {
            for (uint32_t report = 0; report < REPORTS_PER_THREAD; ++report) {
                UbOpOutcome timeout(PEER, UbOperationKind::WORKER_REMOTE_GET_WRITEBACK,
                                    Status(K_RPC_DEADLINE_EXCEEDED, "late request timeout"));
                admission.ReportOutcome(timeout);
            }
        });
    }
    for (auto &reporter : reporters) {
        reporter.join();
    }

    const auto state = admission.GetState(PEER);
    ASSERT_TRUE(state.has_value());
    EXPECT_EQ(state->state, UbAdmissionState::UNAVAILABLE);
    EXPECT_EQ(state->lastFailureClass, UbFailureClass::PORT_UNAVAILABLE_ERROR4);
}

TEST(PeerUbAdmissionTest, LegacyUrmaErrorWithoutRawEvidenceDoesNotQuarantine)
{
    PeerUbAdmission admission;
    UbOpOutcome outcome(PEER, UbOperationKind::WORKER_REMOTE_GET_WRITEBACK,
                        Status(K_URMA_ERROR, "legacy remote error"));

    admission.ReportOutcome(outcome);

    EXPECT_TRUE(admission.CheckReadSource(PEER).IsOk());
    EXPECT_FALSE(admission.GetState(PEER).has_value());
}

// Issue #958: CONNECT_OR_PATH_FAILURE (e.g. a post failure ret=4096) must be treated as SUSPECT,
// not hard UNAVAILABLE -- the code value cannot tell a local send fault from a peer receive
// fault, so hard-isolating on first sight over-blocks. SUSPECT records the failure and verifies
// it without blocking reads or writes; only authoritative CQE evidence may hard-isolate the path.
TEST(PeerUbAdmissionTest, ConnectOrPathFailureIsSuspectAndDoesNotHardBlock)
{
    PeerUbAdmission admission;
    UbOpOutcome outcome(PEER, UbOperationKind::WORKER_REMOTE_GET_WRITEBACK,
                        Status(K_URMA_ERROR, "post jetty send wr failed"));
    outcome.providerStatus = 4096;

    admission.ReportOutcome(outcome);

    EXPECT_TRUE(admission.CheckReadSource(PEER).IsOk());
    EXPECT_TRUE(admission.CheckWriteTarget(PEER, UbOperationKind::MIGRATION_WRITE).IsOk());
    auto state = admission.GetState(PEER);
    ASSERT_TRUE(state.has_value());
    EXPECT_EQ(state->state, UbAdmissionState::SUSPECT);
    EXPECT_EQ(state->lastFailureClass, UbFailureClass::CONNECT_OR_PATH_FAILURE);

    // The recovery probe decides the verdict: success recovers to AVAILABLE.
    const uint64_t probeNow = GetSteadyClockTimeStampMs() + 1'000;
    auto token = admission.TryBeginProbe(PEER, probeNow);
    ASSERT_TRUE(token.has_value());
    auto probing = admission.GetState(PEER);
    ASSERT_TRUE(probing.has_value());
    EXPECT_EQ(probing->state, UbAdmissionState::SUSPECT);
    EXPECT_TRUE(probing->probeInFlight);
    EXPECT_TRUE(admission.CheckReadSource(PEER).IsOk());
    EXPECT_TRUE(admission.CheckWriteTarget(PEER, UbOperationKind::MIGRATION_WRITE).IsOk());
    ASSERT_TRUE(admission.CompleteProbe(*token, Status::OK(), probeNow, false));
    EXPECT_TRUE(admission.CheckReadSource(PEER).IsOk());
    auto recovered = admission.GetState(PEER);
    ASSERT_TRUE(recovered.has_value());
    EXPECT_EQ(recovered->state, UbAdmissionState::AVAILABLE);
}

TEST(PeerUbAdmissionTest, SuspectProbeFailureKeepsAdmissionOpenAndBacksOff)
{
    PeerUbAdmission admission;
    admission.SetSelfWorker(PEER);
    UbOpOutcome outcome(PEER, UbOperationKind::MIGRATION_WRITE,
                        Status(K_URMA_WAIT_TIMEOUT, "peer operation timed out"));
    admission.ReportOutcome(outcome);
    const auto suspect = admission.GetState(PEER);
    ASSERT_TRUE(suspect.has_value());

    const uint64_t probeNow = suspect->backoffDeadlineMs;
    auto token = admission.TryBeginProbe(PEER, probeNow);
    ASSERT_TRUE(token.has_value());
    EXPECT_TRUE(admission.BuildSelfHealthSummary(PEER).writable);
    EXPECT_TRUE(admission.CheckWriteTarget(PEER, UbOperationKind::MIGRATION_WRITE).IsOk());

    EXPECT_FALSE(admission.CompleteProbe(
        *token, Status(K_RPC_DEADLINE_EXCEEDED, "recovery probe timed out"), probeNow, false));

    const auto afterFailure = admission.GetState(PEER);
    ASSERT_TRUE(afterFailure.has_value());
    EXPECT_EQ(afterFailure->state, UbAdmissionState::SUSPECT);
    EXPECT_FALSE(afterFailure->probeInFlight);
    EXPECT_EQ(afterFailure->backoffLevel, 2U);
    EXPECT_EQ(afterFailure->backoffDeadlineMs, probeNow + 2'000);
    EXPECT_TRUE(admission.BuildSelfHealthSummary(PEER).writable);
    EXPECT_TRUE(admission.CheckWriteTarget(PEER, UbOperationKind::MIGRATION_WRITE).IsOk());
    EXPECT_FALSE(admission.TryBeginProbe(PEER, afterFailure->backoffDeadlineMs - 1).has_value());
}

TEST(PeerUbAdmissionTest, HardFailureProbeStillBlocksAndRemainsUnavailableOnFailure)
{
    PeerUbAdmission admission;
    UbOpOutcome outcome(PEER, UbOperationKind::MIGRATION_WRITE, Status(K_URMA_ERROR, "CQE status 4"));
    outcome.cqeStatus = URMA_PORT_UNAVAILABLE_STATUS;
    admission.ReportOutcome(outcome);
    const auto unavailable = admission.GetState(PEER);
    ASSERT_TRUE(unavailable.has_value());

    auto token = admission.TryBeginProbe(PEER, unavailable->backoffDeadlineMs);
    ASSERT_TRUE(token.has_value());
    EXPECT_EQ(admission.GetState(PEER)->state, UbAdmissionState::PROBING);
    EXPECT_EQ(admission.CheckWriteTarget(PEER, UbOperationKind::MIGRATION_WRITE).GetCode(),
              K_URMA_WORKER_UNAVAILABLE);

    EXPECT_FALSE(admission.CompleteProbe(
        *token, Status(K_RPC_DEADLINE_EXCEEDED, "recovery probe timed out"), unavailable->backoffDeadlineMs, false));
    EXPECT_EQ(admission.GetState(PEER)->state, UbAdmissionState::UNAVAILABLE);
    EXPECT_EQ(admission.CheckWriteTarget(PEER, UbOperationKind::MIGRATION_WRITE).GetCode(),
              K_URMA_WORKER_UNAVAILABLE);
}

TEST(PeerUbAdmissionTest, HardUnavailableRecoveryLogsAvailableAfterCommit)
{
    const bool oldAlsoLogToStderr = FLAGS_alsologtostderr;
    Raii restoreFlag([oldAlsoLogToStderr] { FLAGS_alsologtostderr = oldAlsoLogToStderr; });
    FLAGS_alsologtostderr = true;

    struct HardFailureCase {
        int cqeStatus;
        UbFailureClass failureClass;
    };
    for (const auto &testCase : { HardFailureCase{ URMA_PORT_UNAVAILABLE_STATUS,
                                                   UbFailureClass::PORT_UNAVAILABLE_ERROR4 },
                                  HardFailureCase{ URMA_REMOTE_ACK_TIMEOUT_STATUS,
                                                   UbFailureClass::REMOTE_UNAVAILABLE_ERROR9 } }) {
        PeerUbAdmission admission;
        UbOpOutcome outcome(PEER, UbOperationKind::WORKER_REMOTE_GET_WRITEBACK,
                            Status(K_URMA_ERROR, "hard UB failure"));
        outcome.cqeStatus = testCase.cqeStatus;
        admission.ReportOutcome(outcome);
        const auto unavailable = admission.GetState(PEER);
        ASSERT_TRUE(unavailable.has_value());
        ASSERT_EQ(unavailable->state, UbAdmissionState::UNAVAILABLE);

        auto token = admission.TryBeginProbe(PEER, unavailable->backoffDeadlineMs);
        ASSERT_TRUE(token.has_value());
        const auto completion = CaptureProbeCompletion(
            admission, *token, Status::OK(), unavailable->backoffDeadlineMs, false);

        EXPECT_TRUE(completion.recovered);
        ASSERT_TRUE(admission.GetState(PEER).has_value());
        EXPECT_EQ(admission.GetState(PEER)->state, UbAdmissionState::AVAILABLE);
        const auto marker = completion.logs.find(ADMISSION_AVAILABLE_LOG_MARKER);
        ASSERT_NE(marker, std::string::npos) << completion.logs;
        EXPECT_EQ(completion.logs.find(ADMISSION_AVAILABLE_LOG_MARKER, marker + 1), std::string::npos)
            << completion.logs;
        EXPECT_NE(completion.logs.find("peer=" + PEER.ToString()), std::string::npos) << completion.logs;
        EXPECT_NE(completion.logs.find("statusCode=" + std::to_string(static_cast<int>(K_OK))), std::string::npos)
            << completion.logs;
        EXPECT_NE(completion.logs.find("recoveredFrom=UNAVAILABLE"), std::string::npos) << completion.logs;
        EXPECT_NE(completion.logs.find(
                      "previousStatusCode=" + std::to_string(static_cast<int>(K_URMA_ERROR))),
                  std::string::npos)
            << completion.logs;
        EXPECT_NE(completion.logs.find("previousFailureClass="
                                       + std::to_string(static_cast<int>(testCase.failureClass))),
                  std::string::npos)
            << completion.logs;
    }
}

TEST(PeerUbAdmissionTest, AvailableRecoveryLogExcludesSoftAndRejectedProbes)
{
    const bool oldAlsoLogToStderr = FLAGS_alsologtostderr;
    Raii restoreFlag([oldAlsoLogToStderr] { FLAGS_alsologtostderr = oldAlsoLogToStderr; });
    FLAGS_alsologtostderr = true;

    PeerUbAdmission softAdmission;
    UbOpOutcome softFailure(PEER, UbOperationKind::MIGRATION_WRITE,
                            Status(K_RPC_DEADLINE_EXCEEDED, "soft UB failure"));
    softAdmission.ReportOutcome(softFailure);
    auto softState = softAdmission.GetState(PEER);
    ASSERT_TRUE(softState.has_value());
    auto softToken = softAdmission.TryBeginProbe(PEER, softState->backoffDeadlineMs);
    ASSERT_TRUE(softToken.has_value());
    auto completion = CaptureProbeCompletion(
        softAdmission, *softToken, Status::OK(), softState->backoffDeadlineMs, false);
    EXPECT_TRUE(completion.recovered);
    EXPECT_EQ(completion.logs.find(ADMISSION_AVAILABLE_LOG_MARKER), std::string::npos) << completion.logs;

    PeerUbAdmission failedAdmission;
    UbOpOutcome hardFailure(PEER, UbOperationKind::MIGRATION_WRITE, Status(K_URMA_ERROR, "hard UB failure"));
    hardFailure.cqeStatus = URMA_PORT_UNAVAILABLE_STATUS;
    failedAdmission.ReportOutcome(hardFailure);
    auto failedState = failedAdmission.GetState(PEER);
    ASSERT_TRUE(failedState.has_value());
    auto failedToken = failedAdmission.TryBeginProbe(PEER, failedState->backoffDeadlineMs);
    ASSERT_TRUE(failedToken.has_value());
    completion = CaptureProbeCompletion(failedAdmission, *failedToken,
                                        Status(K_RPC_DEADLINE_EXCEEDED, "recovery probe failed"),
                                        failedState->backoffDeadlineMs, false);
    EXPECT_FALSE(completion.recovered);
    EXPECT_EQ(completion.logs.find(ADMISSION_AVAILABLE_LOG_MARKER), std::string::npos) << completion.logs;

    PeerUbAdmission globallyDeniedAdmission;
    globallyDeniedAdmission.ReportOutcome(hardFailure);
    UbHealthSummary unavailableSummary;
    unavailableSummary.worker = PEER;
    unavailableSummary.incarnation = "worker-incarnation";
    unavailableSummary.writable = false;
    globallyDeniedAdmission.ReplaceGlobalSummaries({ unavailableSummary });
    auto globallyDeniedState = globallyDeniedAdmission.GetState(PEER);
    ASSERT_TRUE(globallyDeniedState.has_value());
    auto globallyDeniedToken = globallyDeniedAdmission.TryBeginProbe(
        PEER, globallyDeniedState->backoffDeadlineMs);
    ASSERT_TRUE(globallyDeniedToken.has_value());
    completion = CaptureProbeCompletion(globallyDeniedAdmission, *globallyDeniedToken, Status::OK(),
                                        globallyDeniedState->backoffDeadlineMs, true);
    EXPECT_FALSE(completion.recovered);
    EXPECT_EQ(completion.logs.find(ADMISSION_AVAILABLE_LOG_MARKER), std::string::npos) << completion.logs;

    PeerUbAdmission staleAdmission;
    staleAdmission.ReportOutcome(hardFailure);
    auto staleState = staleAdmission.GetState(PEER);
    ASSERT_TRUE(staleState.has_value());
    auto staleToken = staleAdmission.TryBeginProbe(PEER, staleState->backoffDeadlineMs);
    ASSERT_TRUE(staleToken.has_value());
    staleAdmission.ReportOutcome(hardFailure);
    completion = CaptureProbeCompletion(staleAdmission, *staleToken, Status::OK(),
                                        staleState->backoffDeadlineMs, false);
    EXPECT_FALSE(completion.recovered);
    EXPECT_EQ(completion.logs.find(ADMISSION_AVAILABLE_LOG_MARKER), std::string::npos) << completion.logs;
}

TEST(PeerUbAdmissionTest, CancelProbeRestoresFailureWithoutQuarantiningProbeSubject)
{
    PeerUbAdmission admission;
    UbOpOutcome outcome(PEER, UbOperationKind::WORKER_REMOTE_GET_WRITEBACK,
                        Status(K_URMA_WAIT_TIMEOUT, "peer operation timed out"));
    admission.ReportOutcome(outcome);
    const auto beforeProbe = admission.GetState(PEER);
    ASSERT_TRUE(beforeProbe.has_value());

    const uint64_t probeNow = std::numeric_limits<uint64_t>::max() - 2'000;
    auto token = admission.TryBeginProbe(PEER, probeNow);
    ASSERT_TRUE(token.has_value());
    EXPECT_TRUE(admission.CancelProbe(*token, probeNow));

    const auto cancelled = admission.GetState(PEER);
    ASSERT_TRUE(cancelled.has_value());
    EXPECT_EQ(cancelled->state, UbAdmissionState::SUSPECT);
    EXPECT_EQ(cancelled->lastFailureClass, beforeProbe->lastFailureClass);
    EXPECT_EQ(cancelled->lastStatus.GetCode(), beforeProbe->lastStatus.GetCode());
    EXPECT_FALSE(admission.TryBeginProbe(PEER, probeNow).has_value());
}

TEST(PeerUbAdmissionTest, ResourcePressureDoesNotQuarantine)
{
    PeerUbAdmission admission;
    UbOpOutcome outcome(PEER, UbOperationKind::CLIENT_GET_WRITEBACK,
                        Status(K_URMA_TRY_AGAIN, "send lane exhausted"));

    admission.ReportOutcome(outcome);

    EXPECT_TRUE(admission.CheckReadSource(PEER).IsOk());
    EXPECT_FALSE(admission.GetState(PEER).has_value());
}

TEST(PeerUbAdmissionTest, SelfSummaryDoesNotExportObservedPeerFailure)
{
    PeerUbAdmission admission;
    const HostPort self("127.0.0.1", 31502);
    UbOpOutcome peerFailure(PEER, UbOperationKind::WORKER_REMOTE_GET_WRITEBACK,
                            Status(K_URMA_ERROR, "peer provider failed"));
    peerFailure.cqeStatus = 4;
    admission.ReportOutcome(peerFailure);

    auto summary = admission.BuildSelfHealthSummary(self);

    EXPECT_EQ(summary.worker, self);
    EXPECT_TRUE(summary.writable);
    EXPECT_EQ(summary.epoch, 0u);
}

TEST(PeerUbAdmissionTest, LeaseSyncSelfSummaryDoesNotInvalidateActiveProbe)
{
    PeerUbAdmission admission;
    const HostPort self("127.0.0.1", 31502);
    admission.SetSelfWorker(self);

    UbOpOutcome localFailure(self, UbOperationKind::WORKER_REMOTE_GET_WRITEBACK,
                             Status(K_URMA_ERROR, "self provider failed"));
    localFailure.cqeStatus = 4;
    admission.ReportOutcome(localFailure);
    EXPECT_EQ(admission.CheckWriteTarget(self, UbOperationKind::MIGRATION_WRITE).GetCode(),
              K_URMA_WORKER_UNAVAILABLE);

    auto selfSummary = admission.BuildSelfHealthSummary(self);
    selfSummary.incarnation = "self-incarnation";
    ASSERT_FALSE(selfSummary.writable);
    admission.ReplaceGlobalSummaries({ selfSummary });

    auto probe = admission.TryBeginProbe(self, std::numeric_limits<uint64_t>::max());
    ASSERT_TRUE(probe.has_value());
    EXPECT_TRUE(admission.CompleteProbe(*probe, Status::OK(), 11, true));
    const auto recovered = admission.GetState(self);
    ASSERT_TRUE(recovered.has_value());
    EXPECT_EQ(recovered->state, UbAdmissionState::AVAILABLE);
    EXPECT_TRUE(admission.BuildSelfHealthSummary(self).writable);
    EXPECT_TRUE(admission.CheckWriteTarget(self, UbOperationKind::MIGRATION_WRITE).IsOk());
}

TEST(PeerUbAdmissionTest, GlobalSummaryUsesEpochAndIncarnationFencingAndExpiresIndependently)
{
    PeerUbAdmission admission;
    UbHealthSummary unavailable;
    unavailable.worker = PEER;
    unavailable.incarnation = "worker-old";
    unavailable.writable = false;
    unavailable.epoch = 8;
    admission.ReplaceGlobalSummaries({ unavailable });
    EXPECT_EQ(admission.CheckReadSource(PEER).GetCode(), K_URMA_DATA_WORKER_UNAVAILABLE);

    auto staleRecovery = unavailable;
    staleRecovery.writable = true;
    staleRecovery.epoch = 7;
    admission.ReplaceGlobalSummaries({ staleRecovery });
    EXPECT_EQ(admission.CheckReadSource(PEER).GetCode(), K_URMA_DATA_WORKER_UNAVAILABLE);

    UbOpOutcome localObservation(PEER, UbOperationKind::WORKER_REMOTE_GET_WRITEBACK,
                                 Status(K_URMA_ERROR, "old worker provider failed"));
    localObservation.cqeStatus = 4;
    admission.ReportOutcome(localObservation);
    ASSERT_TRUE(admission.GetState(PEER).has_value());

    auto restarted = staleRecovery;
    restarted.incarnation = "worker-new";
    restarted.epoch = 1;
    admission.ReplaceGlobalSummaries({ restarted });
    EXPECT_TRUE(admission.CheckReadSource(PEER).IsOk());
    EXPECT_FALSE(admission.GetState(PEER).has_value());

    unavailable.epoch = 9;
    admission.ReplaceGlobalSummaries({ unavailable });
    EXPECT_TRUE(admission.CheckReadSource(PEER).IsOk());

    admission.ReplaceGlobalSummaries({});
    EXPECT_TRUE(admission.CheckReadSource(PEER).IsOk());
}

TEST(PeerUbAdmissionTest, GlobalSummaryLogsOnlyEffectiveOperationalTransitions)
{
    const bool oldAlsoLogToStderr = FLAGS_alsologtostderr;
    Raii restoreFlag([oldAlsoLogToStderr] { FLAGS_alsologtostderr = oldAlsoLogToStderr; });
    FLAGS_alsologtostderr = true;

    PeerUbAdmission admission;
    admission.SetSelfWorker(SELF);
    UbHealthSummary unavailable;
    unavailable.worker = PEER;
    unavailable.incarnation = "worker-incarnation-old";
    unavailable.writable = false;
    unavailable.state = UbAdmissionState::UNAVAILABLE;
    unavailable.reason = UbFailureClass::PORT_UNAVAILABLE_ERROR4;
    unavailable.lastStatusCode = K_URMA_ERROR;
    unavailable.epoch = 8;

    auto logs = CaptureGlobalSummaryReplace(admission, { unavailable });
    EXPECT_NE(logs.find(GLOBAL_SUMMARY_LOG_MARKER), std::string::npos) << logs;
    EXPECT_NE(logs.find("receiver=" + SELF.ToString()), std::string::npos) << logs;
    EXPECT_NE(logs.find("target=" + PEER.ToString()), std::string::npos) << logs;
    EXPECT_NE(logs.find("transition=quarantine_applied"), std::string::npos) << logs;
    EXPECT_NE(logs.find("incarnation_prefix=worker-incar"), std::string::npos) << logs;
    EXPECT_NE(logs.find("epoch=8"), std::string::npos) << logs;

    logs = CaptureGlobalSummaryReplace(admission, { unavailable });
    EXPECT_EQ(logs.find(GLOBAL_SUMMARY_LOG_MARKER), std::string::npos) << logs;

    auto updated = unavailable;
    updated.epoch = 9;
    logs = CaptureGlobalSummaryReplace(admission, { updated });
    EXPECT_NE(logs.find("transition=quarantine_updated"), std::string::npos) << logs;

    auto recovered = updated;
    recovered.writable = true;
    recovered.state = UbAdmissionState::AVAILABLE;
    recovered.reason = UbFailureClass::SUCCESS;
    recovered.lastStatusCode = K_OK;
    recovered.epoch = 10;
    logs = CaptureGlobalSummaryReplace(admission, { recovered });
    EXPECT_NE(logs.find("transition=recovery_applied"), std::string::npos) << logs;

    auto restarted = recovered;
    restarted.incarnation = "worker-incarnation-new";
    restarted.epoch = 1;
    logs = CaptureGlobalSummaryReplace(admission, { restarted });
    EXPECT_NE(logs.find("transition=incarnation_replaced"), std::string::npos) << logs;

    auto restartedUnavailable = restarted;
    restartedUnavailable.writable = false;
    restartedUnavailable.state = UbAdmissionState::UNAVAILABLE;
    restartedUnavailable.reason = UbFailureClass::PORT_UNAVAILABLE_ERROR4;
    restartedUnavailable.lastStatusCode = K_URMA_ERROR;
    restartedUnavailable.epoch = 2;
    CaptureGlobalSummaryReplace(admission, { restartedUnavailable });
    logs = CaptureGlobalSummaryReplace(admission, {});
    EXPECT_NE(logs.find("transition=summary_removed"), std::string::npos) << logs;
}

TEST(PeerUbAdmissionTest, RejectedGlobalSummariesDoNotLogAppliedMarker)
{
    const bool oldAlsoLogToStderr = FLAGS_alsologtostderr;
    Raii restoreFlag([oldAlsoLogToStderr] { FLAGS_alsologtostderr = oldAlsoLogToStderr; });
    FLAGS_alsologtostderr = true;

    PeerUbAdmission admission;
    admission.SetSelfWorker(SELF);
    admission.ReconcileTopologyWorkers({ PEER }, 10, 100);
    UbHealthSummary nonMember;
    nonMember.worker = HostPort("127.0.0.1", 31503);
    nonMember.incarnation = "non-member";
    nonMember.writable = false;
    EXPECT_EQ(CaptureGlobalSummaryReplace(admission, { nonMember }).find(GLOBAL_SUMMARY_LOG_MARKER),
              std::string::npos);

    UbHealthSummary unavailable;
    unavailable.worker = PEER;
    unavailable.incarnation = "worker-old";
    unavailable.writable = false;
    unavailable.epoch = 8;
    CaptureGlobalSummaryReplace(admission, { unavailable });

    auto staleRecovery = unavailable;
    staleRecovery.writable = true;
    staleRecovery.epoch = 7;
    EXPECT_EQ(CaptureGlobalSummaryReplace(admission, { staleRecovery }).find(GLOBAL_SUMMARY_LOG_MARKER),
              std::string::npos);

    auto restarted = staleRecovery;
    restarted.incarnation = "worker-new";
    restarted.epoch = 1;
    CaptureGlobalSummaryReplace(admission, { restarted });
    unavailable.epoch = 9;
    EXPECT_EQ(CaptureGlobalSummaryReplace(admission, { unavailable }).find(GLOBAL_SUMMARY_LOG_MARKER),
              std::string::npos);
}

TEST(UbHealthSummaryCacheTest, RejectsWrongIncarnationStaleEpochAndRetiredReplay)
{
    UbHealthSummaryCache cache;
    UbHealthSummary summary;
    summary.worker = PEER;
    summary.incarnation = "worker-old";
    summary.writable = false;
    summary.epoch = 5;

    EXPECT_FALSE(cache.Apply(summary, "unexpected"));
    EXPECT_TRUE(cache.Apply(summary, summary.incarnation));
    summary.epoch = 4;
    summary.writable = true;
    EXPECT_FALSE(cache.Apply(summary, summary.incarnation));

    summary.incarnation = "worker-new";
    summary.epoch = 1;
    EXPECT_TRUE(cache.Apply(summary, summary.incarnation));
    summary.incarnation = "worker-old";
    summary.epoch = 6;
    summary.writable = false;
    EXPECT_FALSE(cache.Apply(summary, summary.incarnation));

    auto stored = cache.Get(PEER);
    ASSERT_TRUE(stored.has_value());
    EXPECT_EQ(stored->incarnation, "worker-new");
    EXPECT_TRUE(stored->writable);
}

TEST(UbHealthSummaryCacheTest, SupportsConcurrentApplyAndGet)
{
    constexpr uint64_t ITERATIONS = 1000;
    constexpr size_t READER_COUNT = 4;
    UbHealthSummaryCache cache;
    UbHealthSummary summary;
    summary.worker = PEER;
    summary.incarnation = "worker-current";
    ASSERT_TRUE(cache.Apply(summary, summary.incarnation));
    const std::string expectedIncarnation = summary.incarnation;
    std::atomic<bool> start{ false };
    std::atomic<size_t> ready{ 0 };
    std::atomic<bool> valid{ true };
    std::vector<std::thread> readers;
    readers.reserve(READER_COUNT);
    for (size_t i = 0; i < READER_COUNT; ++i) {
        readers.emplace_back([&] {
            ready.fetch_add(1, std::memory_order_release);
            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            for (uint64_t read = 0; read < ITERATIONS; ++read) {
                const auto stored = cache.Get(PEER);
                if (!stored.has_value() || stored->worker != PEER || stored->incarnation != expectedIncarnation) {
                    valid.store(false, std::memory_order_release);
                }
            }
        });
    }
    while (ready.load(std::memory_order_acquire) != READER_COUNT) {
        std::this_thread::yield();
    }
    start.store(true, std::memory_order_release);
    for (uint64_t epoch = 1; epoch <= ITERATIONS; ++epoch) {
        summary.epoch = epoch;
        if (!cache.Apply(summary, expectedIncarnation)) {
            valid.store(false, std::memory_order_release);
        }
    }
    for (auto &reader : readers) {
        reader.join();
    }
    EXPECT_TRUE(valid.load(std::memory_order_acquire));
    const auto stored = cache.Get(PEER);
    ASSERT_TRUE(stored.has_value());
    EXPECT_EQ(stored->epoch, ITERATIONS);
}

TEST(PeerUbAdmissionTest, AvailableGlobalFactRequiresProbeBeforeRecovery)
{
    PeerUbAdmission admission;
    UbOpOutcome failure(PEER, UbOperationKind::MIGRATION_WRITE, Status(K_URMA_ERROR, "CQE status 4"));
    failure.cqeStatus = 4;
    admission.ReportOutcome(failure);
    auto summary = admission.BuildSelfHealthSummary(PEER);
    summary.incarnation = "worker-a";
    summary.writable = true;
    summary.epoch = 2;
    admission.ReplaceGlobalSummaries({ summary });

    EXPECT_EQ(admission.GetState(PEER)->state, UbAdmissionState::PROBING);
    EXPECT_EQ(admission.CheckWriteTarget(PEER, UbOperationKind::MIGRATION_WRITE).GetCode(),
              K_URMA_WORKER_UNAVAILABLE);
    auto token = admission.TryBeginProbe(PEER, std::numeric_limits<uint64_t>::max());
    ASSERT_TRUE(token.has_value());
    EXPECT_TRUE(admission.CompleteProbe(*token, Status::OK(), 100));
    EXPECT_TRUE(admission.CheckWriteTarget(PEER, UbOperationKind::MIGRATION_WRITE).IsOk());
}

TEST(PeerUbAdmissionTest, StaleProbeCannotOverrideNewFailure)
{
    PeerUbAdmission admission;
    admission.InitializeVerification(PEER, 10);
    auto token = admission.TryBeginProbe(PEER, 10);
    ASSERT_TRUE(token.has_value());
    UbOpOutcome newerFailure(PEER, UbOperationKind::MIGRATION_READ, Status(K_URMA_ERROR, "new CQE status 4"));
    newerFailure.cqeStatus = 4;
    admission.ReportOutcome(newerFailure);

    EXPECT_FALSE(admission.CompleteProbe(*token, Status::OK(), 20, false));
    EXPECT_EQ(admission.GetState(PEER)->state, UbAdmissionState::UNAVAILABLE);
}

TEST(PeerUbAdmissionTest, EmptyLeaseSnapshotDoesNotClearLocalObservation)
{
    PeerUbAdmission admission;
    UbOpOutcome failure(PEER, UbOperationKind::MIGRATION_WRITE, Status(K_URMA_ERROR, "CQE status 4"));
    failure.cqeStatus = 4;
    admission.ReportOutcome(failure);

    admission.ReplaceGlobalSummaries({});

    EXPECT_EQ(admission.CheckWriteTarget(PEER, UbOperationKind::MIGRATION_WRITE).GetCode(),
              K_URMA_WORKER_UNAVAILABLE);
    ASSERT_TRUE(admission.GetState(PEER).has_value());
    EXPECT_EQ(admission.GetState(PEER)->lastFailureClass, UbFailureClass::PORT_UNAVAILABLE_ERROR4);
}

TEST(PeerUbAdmissionTest, LateCqe4QuarantinesSelfSender)
{
    auto admission = std::make_shared<PeerUbAdmission>();
    admission->SetSelfWorker(PEER);
    const auto context = admission->BuildLateCompletionContext(UbOperationKind::MIGRATION_WRITE);
    ASSERT_TRUE(context.has_value());

    admission->OnLateUrmaCompletion(
        UrmaLateCompletion{ 5001, URMA_PORT_UNAVAILABLE_STATUS, "127.0.0.1:31502", "peer-incarnation" },
        context->ownerToken, context->peerToken);

    const auto state = admission->GetState(PEER);
    ASSERT_TRUE(state.has_value());
    EXPECT_EQ(state->state, UbAdmissionState::UNAVAILABLE);
    EXPECT_EQ(state->lastFailureClass, UbFailureClass::PORT_UNAVAILABLE_ERROR4);
    ASSERT_TRUE(state->cqeStatus.has_value());
    EXPECT_EQ(*state->cqeStatus, URMA_PORT_UNAVAILABLE_STATUS);
    EXPECT_EQ(admission->CheckWriteTarget(PEER, UbOperationKind::MIGRATION_WRITE).GetCode(),
              K_URMA_WORKER_UNAVAILABLE);
}

TEST(PeerUbAdmissionTest, OldLateCqeCannotQuarantineNewSelfGeneration)
{
    auto admission = std::make_shared<PeerUbAdmission>();
    admission->SetSelfWorker(PEER);
    const auto oldContext = admission->BuildLateCompletionContext(UbOperationKind::MIGRATION_WRITE);
    ASSERT_TRUE(oldContext.has_value());
    admission->ClearLocalState(PEER);

    admission->OnLateUrmaCompletion(
        UrmaLateCompletion{ 5002, URMA_PORT_UNAVAILABLE_STATUS, "127.0.0.1:31502", "old-incarnation" },
        oldContext->ownerToken, oldContext->peerToken);

    EXPECT_FALSE(admission->GetState(PEER).has_value());
    EXPECT_TRUE(admission->CheckWriteTarget(PEER, UbOperationKind::MIGRATION_WRITE).IsOk());
}

TEST(PeerUbAdmissionTest, LateCqe9QuarantinesRemotePeer)
{
    const HostPort self("127.0.0.1", 31502);
    auto admission = std::make_shared<PeerUbAdmission>();
    admission->SetSelfWorker(self);
    const auto context = admission->BuildLateCompletionContext(
        UbOperationKind::WORKER_REMOTE_GET_WRITEBACK, PEER);
    ASSERT_TRUE(context.has_value());

    admission->OnLateUrmaCompletion(
        UrmaLateCompletion{ 5003, URMA_REMOTE_ACK_TIMEOUT_STATUS, PEER.ToString(), "peer-incarnation" },
        context->ownerToken, context->peerToken);

    EXPECT_TRUE(admission->CheckWriteTarget(self, UbOperationKind::MIGRATION_WRITE).IsOk());
    EXPECT_EQ(admission->CheckReadSource(PEER).GetCode(), K_URMA_DATA_WORKER_UNAVAILABLE);
    const auto state = admission->GetState(PEER);
    ASSERT_TRUE(state.has_value());
    EXPECT_EQ(state->lastFailureClass, UbFailureClass::REMOTE_UNAVAILABLE_ERROR9);
}

TEST(PeerUbAdmissionTest, OldLateCqe9CannotQuarantineRecoveredRemotePeer)
{
    auto admission = std::make_shared<PeerUbAdmission>();
    const auto oldContext = admission->BuildLateCompletionContext(
        UbOperationKind::WORKER_REMOTE_GET_WRITEBACK, PEER);
    ASSERT_TRUE(oldContext.has_value());
    admission->ClearLocalState(PEER);

    admission->OnLateUrmaCompletion(
        UrmaLateCompletion{ 5004, URMA_REMOTE_ACK_TIMEOUT_STATUS, PEER.ToString(), "old-incarnation" },
        oldContext->ownerToken, oldContext->peerToken);

    EXPECT_FALSE(admission->GetState(PEER).has_value());
    EXPECT_TRUE(admission->CheckReadSource(PEER).IsOk());
}

TEST(PeerUbAdmissionTest, AuthoritativeRemovalBoundsStateAndRejectsOldReplay)
{
    PeerUbAdmission admission;
    admission.ReconcileTopologyWorkers({ PEER }, 100, 10);
    UbHealthSummary oldSummary;
    oldSummary.worker = PEER;
    oldSummary.incarnation = "worker-old";
    oldSummary.writable = false;
    admission.ReplaceGlobalSummaries({ oldSummary });
    admission.ReconcileTopologyWorkers({}, 101, 10);
    admission.ReconcileTopologyWorkers({}, 111, 10);
    auto stats = admission.GetStats();
    EXPECT_EQ(stats.localStates, 0u);
    EXPECT_EQ(stats.globalSummaries, 0u);
    EXPECT_EQ(stats.latestIncarnations, 0u);
    EXPECT_EQ(stats.replayTombstones, 1u);
    EXPECT_EQ(stats.peerCompletionGenerations, 0u);

    admission.ReconcileTopologyWorkers({ PEER }, 112, 10);
    admission.ReplaceGlobalSummaries({ oldSummary });
    EXPECT_TRUE(admission.CheckReadSource(PEER).IsOk());
    oldSummary.incarnation = "worker-new";
    admission.ReplaceGlobalSummaries({ oldSummary });
    EXPECT_EQ(admission.CheckReadSource(PEER).GetCode(), K_URMA_DATA_WORKER_UNAVAILABLE);

    admission.PruneExpiredTopologyState(121);
    EXPECT_EQ(admission.GetStats().replayTombstones, 0u);
}

TEST(PeerUbAdmissionTest, AuthoritativeRemovalDropsPeerCompletionGeneration)
{
    auto admission = std::make_shared<PeerUbAdmission>();
    admission->ReconcileTopologyWorkers({ PEER }, 100, 10);
    ASSERT_TRUE(admission->BuildLateCompletionContext(UbOperationKind::WORKER_REMOTE_GET_WRITEBACK, PEER)
                    .has_value());
    EXPECT_EQ(admission->GetStats().peerCompletionGenerations, 1u);

    admission->ReconcileTopologyWorkers({}, 101, 10);
    admission->ReconcileTopologyWorkers({}, 111, 10);

    EXPECT_EQ(admission->GetStats().peerCompletionGenerations, 0u);
}

TEST(UbHealthSummaryCacheTest, TopologyReconcileDropsRemovedWorkerBuckets)
{
    UbHealthSummaryCache cache;
    UbHealthSummary summary;
    summary.worker = PEER;
    summary.incarnation = "worker-old";
    ASSERT_TRUE(cache.Apply(summary, summary.incarnation));
    summary.incarnation = "worker-new";
    ASSERT_TRUE(cache.Apply(summary, summary.incarnation));
    cache.ReconcileWorkers({});
    EXPECT_EQ(cache.Size(), 0u);
    EXPECT_FALSE(cache.Get(PEER).has_value());
}

}  // namespace
}  // namespace datasystem
