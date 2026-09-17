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

#include <array>
#include <atomic>
#include <limits>
#include <thread>
#include <vector>

#include <bthread/bthread.h>
#include <bthread/countdown_event.h>

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

UbPortHealthSummary PortSummary(uint32_t totalPortCount, uint32_t badPortCount, uint64_t healthEpoch,
                                bool verificationPending = false)
{
    return { true, totalPortCount, badPortCount, healthEpoch, verificationPending };
}

constexpr UbPortHealthEvidenceSource QUERY = UbPortHealthEvidenceSource::QUERY_RESPONSE;
constexpr UbPortHealthEvidenceSource PASSIVE = UbPortHealthEvidenceSource::PASSIVE_SUMMARY;
constexpr UbPortHealthVerificationMode VERIFIED = UbPortHealthVerificationMode::VERIFIED_PORT_HEALTH;

void EnablePeerPortHealth(PeerUbAdmission &admission)
{
    admission.SetRemotePortHealthCapability(PEER, true, "peer-incarnation");
}

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

TEST(PeerUbAdmissionTest, ConcurrentRejectionsPermitVerifiedPartialRecovery)
{
    PeerUbAdmission admission(VERIFIED);
    EnablePeerPortHealth(admission);
    ASSERT_TRUE(admission.ApplyPortHealth(PEER, PortSummary(4, 4, 1), QUERY));
    const auto writeError = admission.CheckWriteTarget(PEER, UbOperationKind::CLIENT_PUT);
    const auto readError = admission.CheckReadSource(PEER);
    struct Context {
        PeerUbAdmission *admission;
        StatusCode writeCode;
        StatusCode readCode;
        bthread::CountdownEvent start{ 1 };
        std::atomic<bool> valid{ true };
    } context{ &admission, writeError.GetCode(), readError.GetCode() };
    std::vector<bthread_t> readers;
    for (size_t i = 0; i < 8; ++i) {
        bthread_t reader;
        auto run = [](void *arg) -> void * {
            auto &shared = *static_cast<Context *>(arg);
            shared.start.wait();
            for (size_t j = 0; j < 100; ++j) {
                const auto write = shared.admission->CheckWriteTarget(PEER, UbOperationKind::CLIENT_PUT);
                const auto read = shared.admission->CheckReadSource(PEER);
                if ((write.IsError() && write.GetCode() != shared.writeCode)
                    || (read.IsError() && read.GetCode() != shared.readCode)) {
                    shared.valid.store(false, std::memory_order_release);
                }
            }
            return nullptr;
        };
        ASSERT_EQ(bthread_start_background(&reader, nullptr, run, &context), 0);
        readers.emplace_back(reader);
    }
    context.start.signal();
    EXPECT_TRUE(admission.ApplyPortHealth(PEER, PortSummary(4, 3, 2), QUERY));
    for (const auto reader : readers) {
        EXPECT_EQ(bthread_join(reader, nullptr), 0);
    }
    EXPECT_TRUE(context.valid.load(std::memory_order_acquire));
    EXPECT_TRUE(admission.CheckWriteTarget(PEER, UbOperationKind::CLIENT_PUT).IsOk());
    EXPECT_TRUE(admission.CheckReadSource(PEER).IsOk());
}

TEST(PeerUbAdmissionTest, Error4TriggersVerificationAndAllDownFactBlocksReadSource)
{
    PeerUbAdmission admission(VERIFIED);
    EnablePeerPortHealth(admission);
    UbOpOutcome outcome(PEER, UbOperationKind::WORKER_REMOTE_GET_WRITEBACK,
                        Status(K_URMA_ERROR, "provider write failed"));
    outcome.cqeStatus = 4;

    admission.ReportOutcome(outcome);

    EXPECT_TRUE(admission.CheckReadSource(PEER).IsOk());
    auto state = admission.GetState(PEER);
    ASSERT_TRUE(state.has_value());
    EXPECT_EQ(state->state, UbAdmissionState::SUSPECT);
    EXPECT_EQ(state->lastFailureClass, UbFailureClass::PORT_UNAVAILABLE_ERROR4);

    EXPECT_TRUE(admission.ApplyPortHealth(PEER, PortSummary(4, 4, 1), QUERY));
    EXPECT_EQ(admission.CheckReadSource(PEER).GetCode(), K_URMA_DATA_WORKER_UNAVAILABLE);
    EXPECT_EQ(admission.GetState(PEER)->state, UbAdmissionState::UNAVAILABLE);
}

TEST(PeerUbAdmissionTest, Error9TriggersVerificationAndAllDownQueryFactBlocksReadSource)
{
    PeerUbAdmission admission(VERIFIED);
    EnablePeerPortHealth(admission);
    UbOpOutcome outcome(PEER, UbOperationKind::WORKER_REMOTE_GET_WRITEBACK,
                        Status(K_URMA_ERROR, "remote ACK timed out"));
    outcome.cqeStatus = URMA_REMOTE_ACK_TIMEOUT_STATUS;

    admission.ReportOutcome(outcome);

    EXPECT_TRUE(admission.CheckReadSource(PEER).IsOk());
    const auto state = admission.GetState(PEER);
    ASSERT_TRUE(state.has_value());
    EXPECT_EQ(state->state, UbAdmissionState::SUSPECT);
    EXPECT_EQ(state->lastFailureClass, UbFailureClass::REMOTE_UNAVAILABLE_ERROR9);

    EXPECT_FALSE(admission.ApplyPortHealth(PEER, PortSummary(4, 4, 1), PASSIVE));
    EXPECT_TRUE(admission.CheckReadSource(PEER).IsOk());
    EXPECT_TRUE(admission.ApplyPortHealth(PEER, PortSummary(4, 4, 2), QUERY));
    EXPECT_EQ(admission.CheckReadSource(PEER).GetCode(), K_URMA_DATA_WORKER_UNAVAILABLE);
    EXPECT_EQ(admission.CheckWriteTarget(PEER, UbOperationKind::MIGRATION_WRITE).GetCode(),
              K_URMA_WORKER_UNAVAILABLE);
}

TEST(PeerUbAdmissionTest, TwoNodeS2SourceIsolationPreservesHealthyTarget)
{
    // Distinct node identities exercise admission ownership, not physical port emulation.
    const HostPort source("127.0.0.1", 31501);
    const HostPort target("127.0.0.2", 31501);
    PeerUbAdmission sourceAdmission;
    PeerUbAdmission targetAdmission;
    sourceAdmission.SetSelfWorker(source);
    targetAdmission.SetSelfWorker(target);
    UbOpOutcome fault(source, UbOperationKind::MIGRATION_WRITE, Status(K_URMA_ERROR, "source local UB failure"));
    fault.cqeStatus = 4;
    sourceAdmission.ReportOutcome(fault);
    auto sourceSummary = sourceAdmission.BuildSelfHealthSummary(source);
    sourceSummary.incarnation = "source-node-1";
    targetAdmission.ReplaceGlobalSummaries({ sourceSummary });

    EXPECT_FALSE(sourceSummary.writable);
    EXPECT_EQ(sourceAdmission.CheckWriteTarget(source, UbOperationKind::MIGRATION_WRITE).GetCode(),
              K_URMA_WORKER_UNAVAILABLE);
    EXPECT_EQ(targetAdmission.CheckReadSource(source).GetCode(), K_URMA_DATA_WORKER_UNAVAILABLE);
    EXPECT_TRUE(sourceAdmission.CheckWriteTarget(target, UbOperationKind::MIGRATION_WRITE).IsOk());
    EXPECT_TRUE(targetAdmission.CheckReadSource(target).IsOk());
    EXPECT_TRUE(targetAdmission.CheckWriteTarget(target, UbOperationKind::MIGRATION_WRITE).IsOk());
    EXPECT_TRUE(targetAdmission.BuildSelfHealthSummary(target).writable);
}

TEST(PeerUbAdmissionTest, TwoNodeS2RemoteFailureDoesNotBecomeTargetSelfIsolation)
{
    const HostPort source("127.0.0.1", 31501);
    const HostPort target("127.0.0.2", 31501);
    PeerUbAdmission targetAdmission;
    targetAdmission.SetSelfWorker(target);
    UbOpOutcome remote(source, UbOperationKind::WORKER_REMOTE_GET_WRITEBACK,
                        Status(K_URMA_ERROR, "faulty source did not acknowledge"));
    remote.cqeStatus = URMA_REMOTE_ACK_TIMEOUT_STATUS;
    targetAdmission.ReportOutcome(remote);

    EXPECT_EQ(targetAdmission.CheckReadSource(source).GetCode(), K_URMA_DATA_WORKER_UNAVAILABLE);
    EXPECT_TRUE(targetAdmission.CheckReadSource(target).IsOk());
    EXPECT_TRUE(targetAdmission.BuildSelfHealthSummary(target).writable);
    const auto sourceState = targetAdmission.GetState(source);
    ASSERT_TRUE(sourceState.has_value());
    EXPECT_EQ(sourceState->lastFailureClass, UbFailureClass::REMOTE_UNAVAILABLE_ERROR9);
}

TEST(PeerUbAdmissionTest, TwoNodeS2RecoveryRequiresCurrentSummaryAndProbe)
{
    const HostPort source("127.0.0.1", 31501);
    const HostPort target("127.0.0.2", 31501);
    PeerUbAdmission sourceAdmission;
    PeerUbAdmission targetAdmission;
    sourceAdmission.SetSelfWorker(source);
    targetAdmission.SetSelfWorker(target);
    UbOpOutcome fault(source, UbOperationKind::MIGRATION_WRITE, Status(K_URMA_ERROR, "source UB failure"));
    fault.cqeStatus = 4;
    sourceAdmission.ReportOutcome(fault);
    auto stale = sourceAdmission.BuildSelfHealthSummary(source);
    stale.incarnation = "source-node-1";
    targetAdmission.ReplaceGlobalSummaries({ stale });
    fault.cqeStatus = URMA_REMOTE_ACK_TIMEOUT_STATUS;
    targetAdmission.ReportOutcome(fault);
    auto selfProbe = sourceAdmission.TryBeginProbe(source, std::numeric_limits<uint64_t>::max());
    ASSERT_TRUE(selfProbe.has_value());
    ASSERT_TRUE(sourceAdmission.CompleteProbe(*selfProbe, Status::OK(), 100, false));
    EXPECT_EQ(targetAdmission.CheckReadSource(source).GetCode(), K_URMA_DATA_WORKER_UNAVAILABLE);
    auto recovered = sourceAdmission.BuildSelfHealthSummary(source);
    recovered.incarnation = stale.incarnation;
    ASSERT_GT(recovered.epoch, stale.epoch);
    targetAdmission.ReplaceGlobalSummaries({ recovered });
    EXPECT_EQ(targetAdmission.CheckReadSource(source).GetCode(), K_URMA_DATA_WORKER_UNAVAILABLE);
    auto peerProbe = targetAdmission.TryBeginProbe(source, std::numeric_limits<uint64_t>::max());
    ASSERT_TRUE(peerProbe.has_value());
    ASSERT_TRUE(targetAdmission.CompleteProbe(*peerProbe, Status::OK(), 101));
    EXPECT_TRUE(targetAdmission.CheckReadSource(source).IsOk());
    targetAdmission.ReplaceGlobalSummaries({ stale });
    EXPECT_TRUE(targetAdmission.CheckReadSource(source).IsOk());
    EXPECT_TRUE(targetAdmission.CheckReadSource(target).IsOk());
    EXPECT_TRUE(targetAdmission.BuildSelfHealthSummary(target).writable);
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

// A bound remote port-health verifier owns the verdict, so CompleteProbe completes diagnostically
// and returns no admission decision. That completion must still release the peer with a probe
// backoff: leaving backoffDeadlineMs untouched re-arms NextProbeCandidate on the very next
// scheduler turn, which turns the recovery probe into an unpaced RPC loop.
TEST(PeerUbAdmissionTest, VerifierBoundProbeCompletionBacksOffBeforeNextCandidate)
{
    constexpr uint64_t nowMs = 1'000;
    PeerUbAdmission admission;
    std::atomic<uint32_t> verifications{ 0 };
    admission.SetRemotePortHealthVerificationTrigger([&verifications](const HostPort &peer) {
        EXPECT_EQ(peer, PEER);
        verifications.fetch_add(1, std::memory_order_acq_rel);
    });
    admission.InitializeVerification(PEER, nowMs);
    auto candidate = admission.NextProbeCandidate(nowMs);
    ASSERT_TRUE(candidate.has_value());
    EXPECT_EQ(*candidate, PEER);

    auto token = admission.TryBeginProbe(PEER, nowMs);
    ASSERT_TRUE(token.has_value());
    EXPECT_FALSE(admission.CompleteProbe(*token, Status::OK(), nowMs, false));
    EXPECT_EQ(verifications.load(std::memory_order_acquire), 1U);

    const auto state = admission.GetState(PEER);
    ASSERT_TRUE(state.has_value());
    EXPECT_FALSE(state->probeInFlight);
    EXPECT_EQ(state->backoffLevel, 1U);
    EXPECT_EQ(state->backoffDeadlineMs, nowMs + 1'000);
    EXPECT_FALSE(admission.TryBeginProbe(PEER, nowMs).has_value());
    EXPECT_FALSE(admission.NextProbeCandidate(state->backoffDeadlineMs - 1).has_value());
    EXPECT_EQ(admission.NextProbeDeadlineMs(), std::optional<uint64_t>{ state->backoffDeadlineMs });
}

TEST(PeerUbAdmissionTest, PortVerifiedIsolationOnlyRecoversByPortFact)
{
    PeerUbAdmission admission(VERIFIED);
    ASSERT_TRUE(admission.ApplyPortHealth(PEER, PortSummary(4, 4, 1), QUERY));
    EXPECT_EQ(admission.CheckReadSource(PEER).GetCode(), K_URMA_DATA_WORKER_UNAVAILABLE);

    EXPECT_FALSE(admission.ApplyPortHealth(PEER, PortSummary(4, 4, 0), QUERY));
    EXPECT_FALSE(admission.ApplyPortHealth(PEER, PortSummary(4, 5, 2), QUERY));
    EXPECT_FALSE(admission.ApplyPortHealth(PEER, PortSummary(4, 4, 3, true), QUERY));
    EXPECT_FALSE(admission.ApplyPortHealth(PEER, PortSummary(4, 3, 4), PASSIVE));
    EXPECT_EQ(admission.CheckReadSource(PEER).GetCode(), K_URMA_DATA_WORKER_UNAVAILABLE);

    EXPECT_TRUE(admission.ApplyPortHealth(PEER, PortSummary(4, 2, 5), QUERY));
    EXPECT_TRUE(admission.CheckReadSource(PEER).IsOk());
    EXPECT_TRUE(admission.ApplyPortHealth(PEER, PortSummary(4, 0, 6), QUERY));
    EXPECT_EQ(admission.GetState(PEER)->portHealthGoverned, false);
}

TEST(PeerUbAdmissionTest, PortHealthEpochRejectsOlderAndConflictingFacts)
{
    PeerUbAdmission admission(VERIFIED);
    ASSERT_TRUE(admission.ApplyPortHealth(PEER, PortSummary(4, 4, 2), QUERY));

    EXPECT_FALSE(admission.ApplyPortHealth(PEER, PortSummary(4, 3, 1), QUERY));
    EXPECT_FALSE(admission.ApplyPortHealth(PEER, PortSummary(4, 3, 2), QUERY));
    EXPECT_EQ(admission.GetState(PEER)->state, UbAdmissionState::UNAVAILABLE);

    EXPECT_TRUE(admission.ApplyPortHealth(PEER, PortSummary(4, 3, 3), QUERY));
    EXPECT_EQ(admission.GetState(PEER)->state, UbAdmissionState::AVAILABLE);
    EXPECT_FALSE(admission.ApplyPortHealth(PEER, PortSummary(4, 4, 2), QUERY));
    EXPECT_EQ(admission.GetState(PEER)->state, UbAdmissionState::AVAILABLE);
    ASSERT_TRUE(admission.GetState(PEER)->portHealth.has_value());
    EXPECT_EQ(admission.GetState(PEER)->portHealth->healthEpoch, 3u);
}

TEST(PeerUbAdmissionTest, TrustedIncarnationReplacementResetsPortHealthEpoch)
{
    PeerUbAdmission admission(VERIFIED);
    UbHealthSummary oldWorker;
    oldWorker.worker = PEER;
    oldWorker.incarnation = "worker-old";
    oldWorker.epoch = 1;
    admission.ReplaceGlobalSummaries({ oldWorker });
    ASSERT_TRUE(admission.ApplyPortHealth(PEER, PortSummary(4, 4, 8), QUERY));

    auto restartedWorker = oldWorker;
    restartedWorker.incarnation = "worker-new";
    restartedWorker.epoch = 1;
    admission.ReplaceGlobalSummaries({ restartedWorker });

    EXPECT_FALSE(admission.GetState(PEER).has_value());
    EXPECT_TRUE(admission.ApplyPortHealth(PEER, PortSummary(4, 4, 1), QUERY));
    ASSERT_TRUE(admission.GetState(PEER)->portHealth.has_value());
    EXPECT_EQ(admission.GetState(PEER)->portHealth->healthEpoch, 1u);
}

TEST(PeerUbAdmissionTest, PartialQuerySettlesSuspectAndAllowsNewFailureVerification)
{
    PeerUbAdmission admission(VERIFIED);
    EnablePeerPortHealth(admission);
    UbOpOutcome error4(PEER, UbOperationKind::MIGRATION_WRITE, Status(K_URMA_ERROR, "CQE status 4"));
    error4.cqeStatus = URMA_PORT_UNAVAILABLE_STATUS;
    admission.ReportOutcome(error4);
    ASSERT_EQ(admission.GetState(PEER)->state, UbAdmissionState::SUSPECT);

    ASSERT_TRUE(admission.ApplyPortHealth(PEER, PortSummary(4, 3, 1), QUERY));
    EXPECT_EQ(admission.GetState(PEER)->state, UbAdmissionState::AVAILABLE);
    EXPECT_FALSE(admission.GetState(PEER)->portHealthGoverned);

    UbOpOutcome timeout(PEER, UbOperationKind::MIGRATION_WRITE,
                        Status(K_RPC_DEADLINE_EXCEEDED, "path timeout"));
    admission.ReportOutcome(timeout);
    EXPECT_EQ(admission.GetState(PEER)->state, UbAdmissionState::SUSPECT);
    EXPECT_TRUE(admission.ApplyPortHealth(PEER, PortSummary(4, 3, 1), QUERY));
    EXPECT_EQ(admission.GetState(PEER)->state, UbAdmissionState::AVAILABLE);

    UbOpOutcome error9(PEER, UbOperationKind::MIGRATION_WRITE, Status(K_URMA_ERROR, "CQE status 9"));
    error9.cqeStatus = URMA_REMOTE_ACK_TIMEOUT_STATUS;
    admission.ReportOutcome(error9);
    EXPECT_EQ(admission.GetState(PEER)->state, UbAdmissionState::SUSPECT);
    EXPECT_FALSE(admission.TryBeginProbe(PEER, std::numeric_limits<uint64_t>::max()).has_value());
}

TEST(PeerUbAdmissionTest, PortHealthFactTransitionsLogAdmissionState)
{
    const bool oldAlsoLogToStderr = FLAGS_alsologtostderr;
    Raii restoreFlag([oldAlsoLogToStderr] { FLAGS_alsologtostderr = oldAlsoLogToStderr; });
    FLAGS_alsologtostderr = true;

    PeerUbAdmission admission(VERIFIED);
    testing::internal::CaptureStderr();
    EXPECT_TRUE(admission.ApplyPortHealth(PEER, PortSummary(4, 4, 1), QUERY));
    auto logs = testing::internal::GetCapturedStderr();
    EXPECT_NE(logs.find("UB admission marked peer UNAVAILABLE"), std::string::npos) << logs;
    EXPECT_NE(logs.find("peer=" + PEER.ToString()), std::string::npos) << logs;
    EXPECT_NE(logs.find("previous_state=0, state=2, bad=4, total=4"), std::string::npos) << logs;
    EXPECT_NE(logs.find("source=query_response"), std::string::npos) << logs;

    testing::internal::CaptureStderr();
    EXPECT_TRUE(admission.ApplyPortHealth(PEER, PortSummary(4, 1, 2), QUERY));
    logs = testing::internal::GetCapturedStderr();
    EXPECT_NE(logs.find("UB admission marked peer AVAILABLE"), std::string::npos) << logs;
    EXPECT_NE(logs.find("previous_state=2, state=0, previous_bad=4, bad=1, total=4, health_epoch=2"),
              std::string::npos)
        << logs;
    EXPECT_NE(logs.find("source=query_response"), std::string::npos) << logs;
}

TEST(PeerUbAdmissionTest, AvailableRecoveryLogExcludesSoftAndPortFactTransitions)
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

    PeerUbAdmission portAdmission(VERIFIED);
    ASSERT_TRUE(portAdmission.ApplyPortHealth(PEER, PortSummary(4, 4, 1), QUERY));
    EXPECT_FALSE(portAdmission.TryBeginProbe(PEER, std::numeric_limits<uint64_t>::max()).has_value());
    EXPECT_EQ(portAdmission.CheckWriteTarget(PEER, UbOperationKind::MIGRATION_WRITE).GetCode(),
              K_URMA_WORKER_UNAVAILABLE);

    PeerUbAdmission staleAdmission;
    staleAdmission.InitializeVerification(PEER, 10);
    auto staleToken = staleAdmission.TryBeginProbe(PEER, 10);
    ASSERT_TRUE(staleToken.has_value());
    UbOpOutcome newerPortEvidence(PEER, UbOperationKind::MIGRATION_WRITE,
                                  Status(K_URMA_ERROR, "new CQE status 9"));
    newerPortEvidence.cqeStatus = URMA_REMOTE_ACK_TIMEOUT_STATUS;
    staleAdmission.ReportOutcome(newerPortEvidence);
    completion = CaptureProbeCompletion(staleAdmission, *staleToken, Status::OK(), 20, false);
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
    const std::array<char, 16> binaryIncarnationBytes{
        '\0', '\n', '\r', '\x1f', ' ', '\x7f', static_cast<char>(0x80), static_cast<char>(0xff),
        '\x01', '\x02', '\x03', '\x04', '\x05', '\x06', '\x07', '\x08'
    };
    UbHealthSummary unavailable;
    unavailable.worker = PEER;
    unavailable.incarnation.assign(binaryIncarnationBytes.data(), binaryIncarnationBytes.size());
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
    EXPECT_NE(logs.find("incarnation_prefix=" + FormatUbHealthIncarnationPrefix(unavailable.incarnation)),
              std::string::npos) << logs;
    EXPECT_NE(logs.find("epoch=8"), std::string::npos) << logs;

    logs = CaptureGlobalSummaryReplace(admission, { unavailable });
    EXPECT_EQ(logs.find(GLOBAL_SUMMARY_LOG_MARKER), std::string::npos) << logs;

    auto updated = unavailable;
    updated.epoch = 9;
    updated.state = UbAdmissionState::PROBING;
    updated.backoffLevel = 2;
    updated.backoffDeadlineMs = 1'000;
    logs = CaptureGlobalSummaryReplace(admission, { updated });
    EXPECT_EQ(logs.find(GLOBAL_SUMMARY_LOG_MARKER), std::string::npos) << logs;

    updated.lastStatusCode = K_RPC_DEADLINE_EXCEEDED;
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
    restarted.incarnation.assign(16, '\x11');
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
    summary.portHealth = PortSummary(4, 4, 5);

    EXPECT_FALSE(cache.Apply(summary, "unexpected"));
    EXPECT_TRUE(cache.Apply(summary, summary.incarnation));
    summary.epoch = 4;
    summary.writable = true;
    EXPECT_FALSE(cache.Apply(summary, summary.incarnation));

    summary.incarnation = "worker-new";
    summary.epoch = 1;
    summary.portHealth = PortSummary(4, 0, 1);
    EXPECT_TRUE(cache.Apply(summary, summary.incarnation));
    summary.incarnation = "worker-old";
    summary.epoch = 6;
    summary.writable = false;
    EXPECT_FALSE(cache.Apply(summary, summary.incarnation));

    auto stored = cache.Get(PEER);
    ASSERT_TRUE(stored.has_value());
    EXPECT_EQ(stored->incarnation, "worker-new");
    EXPECT_TRUE(stored->writable);
    ASSERT_TRUE(stored->portHealth.has_value());
    EXPECT_EQ(stored->portHealth->healthEpoch, 1u);
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
    const auto duplicate = summary;
    struct Context {
        UbHealthSummaryCache *cache;
        const UbHealthSummary *duplicate;
        const std::string *incarnation;
        bthread::CountdownEvent ready{ READER_COUNT };
        bthread::CountdownEvent start{ 1 };
        std::atomic<bool> valid{ true };
    } context{ &cache, &duplicate, &expectedIncarnation };
    std::vector<bthread_t> readers;
    readers.reserve(READER_COUNT);
    for (size_t i = 0; i < READER_COUNT; ++i) {
        bthread_t reader;
        auto run = [](void *arg) -> void * {
            auto &shared = *static_cast<Context *>(arg);
            shared.ready.signal();
            shared.start.wait();
            for (uint64_t read = 0; read < ITERATIONS; ++read) {
                if (shared.cache->Apply(*shared.duplicate, *shared.incarnation)) {
                    shared.valid.store(false, std::memory_order_release);
                }
                const auto stored = shared.cache->Get(PEER);
                if (!stored.has_value() || stored->worker != PEER
                    || stored->incarnation != *shared.incarnation) {
                    shared.valid.store(false, std::memory_order_release);
                }
            }
            return nullptr;
        };
        ASSERT_EQ(bthread_start_background(&reader, nullptr, run, &context), 0);
        readers.emplace_back(reader);
    }
    ASSERT_EQ(context.ready.timed_wait(butil::seconds_from_now(2)), 0);
    context.start.signal();
    for (uint64_t epoch = 1; epoch <= ITERATIONS; ++epoch) {
        summary.epoch = epoch;
        if (!cache.Apply(summary, expectedIncarnation)) {
            context.valid.store(false, std::memory_order_release);
        }
    }
    for (const auto reader : readers) {
        EXPECT_EQ(bthread_join(reader, nullptr), 0);
    }
    EXPECT_TRUE(context.valid.load(std::memory_order_acquire));
    const auto stored = cache.Get(PEER);
    ASSERT_TRUE(stored.has_value());
    EXPECT_EQ(stored->epoch, ITERATIONS);
}

TEST(UbHealthSummaryCacheTest, ReturnsSummaryAcceptedBySameUpdate)
{
    UbHealthSummaryCache cache;
    UbHealthSummary summary;
    summary.worker = PEER;
    summary.incarnation = "worker-current";
    summary.epoch = 1;
    summary.portHealth = PortSummary(4, 4, 1);

    UbHealthSummary accepted;
    ASSERT_TRUE(cache.Apply(summary, summary.incarnation, accepted));
    EXPECT_TRUE(IsSameUbHealthSummary(summary, accepted));

    auto portRecovery = summary;
    portRecovery.portHealth = PortSummary(4, 3, 2);
    ASSERT_TRUE(cache.Apply(portRecovery, portRecovery.incarnation, accepted));
    EXPECT_EQ(accepted.epoch, summary.epoch);
    ASSERT_TRUE(accepted.portHealth.has_value());
    EXPECT_EQ(accepted.portHealth->badPortCount, 3u);
    EXPECT_EQ(accepted.portHealth->healthEpoch, 2u);
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

TEST(PeerUbAdmissionTest, PortHealthBroadcastOnlyHintsQueryAuthoritativeAdmission)
{
    PeerUbAdmission admission;
    EnablePeerPortHealth(admission);
    UbHealthSummary isolated;
    isolated.worker = PEER;
    isolated.incarnation = "peer-incarnation";
    isolated.writable = false;
    isolated.state = UbAdmissionState::UNAVAILABLE;
    isolated.epoch = 1;
    isolated.portHealth = PortSummary(4, 4, 1, true);

    admission.ReplaceGlobalSummaries({ isolated });
    EXPECT_TRUE(admission.CheckWriteTarget(PEER, UbOperationKind::MIGRATION_WRITE).IsOk());

    isolated.epoch = 2;
    isolated.portHealth = PortSummary(4, 4, 2);
    admission.ReplaceGlobalSummaries({ isolated });
    EXPECT_TRUE(admission.CheckWriteTarget(PEER, UbOperationKind::MIGRATION_WRITE).IsOk());

    ASSERT_TRUE(admission.ApplyPortHealth(PEER, *isolated.portHealth, QUERY));
    EXPECT_EQ(admission.CheckWriteTarget(PEER, UbOperationKind::MIGRATION_WRITE).GetCode(),
              K_URMA_WORKER_UNAVAILABLE);

    auto recovered = isolated;
    recovered.writable = true;
    recovered.state = UbAdmissionState::AVAILABLE;
    recovered.epoch = 3;
    recovered.portHealth = PortSummary(4, 3, 3);
    admission.ReplaceGlobalSummaries({ recovered });
    EXPECT_EQ(admission.CheckWriteTarget(PEER, UbOperationKind::MIGRATION_WRITE).GetCode(),
              K_URMA_WORKER_UNAVAILABLE);

    ASSERT_TRUE(admission.ApplyPortHealth(PEER, *recovered.portHealth, QUERY));
    EXPECT_TRUE(admission.CheckWriteTarget(PEER, UbOperationKind::MIGRATION_WRITE).IsOk());
}

}  // namespace
}  // namespace datasystem
