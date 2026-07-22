/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Description: Worker runtime state manager tests.
 */
#include "datasystem/worker/runtime/worker_runtime_state.h"

#include <algorithm>
#include <chrono>
#include <future>
#include <optional>
#include <thread>
#include <vector>

#include "datasystem/common/flags/common_flags.h"
#include "datasystem/common/metrics/kv_metrics.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/util/raii.h"
#include "gtest/gtest.h"

DS_DECLARE_string(log_dir);

namespace datasystem::worker {
namespace {
constexpr auto TRANSITION_RESIDENCE_DELAY = std::chrono::milliseconds(50);
constexpr auto READ_GUARD_BLOCK_TIMEOUT = std::chrono::milliseconds(50);
constexpr auto INJECT_WAIT_INTERVAL = std::chrono::milliseconds(1);
constexpr size_t INJECT_WAIT_RETRY_LIMIT = 100;

WorkerRunningEvidence CompleteEvidence()
{
    WorkerRunningEvidence evidence;
    evidence.membershipReady = true;
    evidence.topologyReady = true;
    evidence.metadataReady = true;
    evidence.slotReady = true;
    evidence.ownershipReady = true;
    evidence.resourceReady = true;
    return evidence;
}

void InitKvMetricsForWorkerRuntimeStateTest()
{
    FLAGS_log_dir = "/tmp";
    metrics::ResetKvMetricsForTest();
    ASSERT_TRUE(metrics::InitKvMetrics().IsOk());
}

TEST(WorkerRuntimeStateTest, RequiresCompleteEvidenceBeforeRunning)
{
    WorkerRuntimeStateManager state;
    state.MarkJoining("published local membership");

    auto evidence = CompleteEvidence();
    evidence.slotReady = false;
    EXPECT_FALSE(state.TryMarkRunning(evidence, "slot recovery pending"));

    auto snapshot = state.GetSnapshot();
    EXPECT_EQ(snapshot.mode, WorkerServiceMode::RECOVERING);
    EXPECT_EQ(snapshot.reason, WorkerIsolationReason::RECOVERY_EVIDENCE_INCOMPLETE);
    EXPECT_FALSE(snapshot.evidence.slotReady);

    EXPECT_TRUE(state.TryMarkRunning(CompleteEvidence(), "all startup evidence ready"));
    snapshot = state.GetSnapshot();
    EXPECT_EQ(snapshot.mode, WorkerServiceMode::RUNNING);
    EXPECT_EQ(snapshot.reason, WorkerIsolationReason::NONE);
    EXPECT_TRUE(snapshot.evidence.slotReady);
    EXPECT_EQ(snapshot.detail, "all startup evidence ready");
}

TEST(WorkerRuntimeStateTest, RecoveryPhaseTracksFirstMissingEvidence)
{
    WorkerRuntimeStateManager state;
    auto evidence = CompleteEvidence();
    evidence.metadataReady = false;
    evidence.slotReady = false;

    EXPECT_FALSE(state.TryMarkRunning(evidence, "metadata pending"));
    auto snapshot = state.GetSnapshot();
    EXPECT_EQ(snapshot.recoveryPhase, WorkerRecoveryPhase::METADATA);
    EXPECT_STREQ(ToString(snapshot.recoveryPhase), "METADATA");

    evidence.metadataReady = true;
    EXPECT_FALSE(state.TryMarkRunning(evidence, "slot pending"));
    EXPECT_EQ(state.GetSnapshot().recoveryPhase, WorkerRecoveryPhase::SLOT);

    EXPECT_TRUE(state.TryMarkRunning(CompleteEvidence(), "complete"));
    EXPECT_EQ(state.GetSnapshot().recoveryPhase, WorkerRecoveryPhase::COMPLETE);
}

TEST(WorkerRuntimeStateTest, SelfHealingMetricsUseFixedLowCardinalityDescriptors)
{
    size_t count = 0;
    const auto *descs = metrics::GetKvMetricDescs(count);
    std::vector<std::string> names;
    names.reserve(count);
    for (size_t i = 0; i < count; ++i) {
        names.emplace_back(descs[i].name);
    }

    for (const auto *expected : { "worker_service_mode", "worker_service_reason", "worker_recovery_phase",
                                  "worker_recovery_evidence_mask", "worker_mode_transition_latency",
                                  "worker_admission_reject_latency", "worker_admission_reject_local_isolated_total",
                                  "worker_object_table_lock_hold_latency", "worker_recovery_candidate_count",
                                  "worker_metadata_recovery_batch_latency", "worker_cleanup_batch_latency" }) {
        EXPECT_NE(std::find(names.begin(), names.end(), expected), names.end()) << expected;
    }
}

TEST(WorkerRuntimeStateTest, TransitionLatencyDoesNotMeasureStateResidenceTime)
{
    Raii restoreMetrics([] {
        FLAGS_log_dir = "/tmp";
        metrics::ResetKvMetricsForTest();
        (void)metrics::InitKvMetrics();
    });
    InitKvMetricsForWorkerRuntimeStateTest();
    WorkerRuntimeStateManager state;
    ASSERT_TRUE(state.TryMarkRunning(CompleteEvidence(), "ready"));
    std::this_thread::sleep_for(TRANSITION_RESIDENCE_DELAY);
    state.MarkLocalIsolated(WorkerIsolationReason::CONTROL_BACKEND_LOCAL_ISOLATION, "keepalive failed");

    std::string summary;
    for (const auto &part : metrics::DumpSummariesForTest()) {
        summary += part;
    }
    const auto metric = summary.find("\"name\":\"worker_mode_transition_latency\"");
    ASSERT_NE(metric, std::string::npos);
    const auto maxField = summary.find("\"max_us\":", metric);
    ASSERT_NE(maxField, std::string::npos);
    const auto maxUs = std::stoull(summary.substr(maxField + std::string("\"max_us\":").size()));
    EXPECT_LT(maxUs, 25'000U);
}

TEST(WorkerRuntimeStateTest, RuntimeTransitionsPublishModeReasonAndPhaseMetrics)
{
    Raii restoreMetrics([] {
        FLAGS_log_dir = "/tmp";
        metrics::ResetKvMetricsForTest();
        (void)metrics::InitKvMetrics();
    });
    InitKvMetricsForWorkerRuntimeStateTest();
    WorkerRuntimeStateManager state;
    ASSERT_TRUE(state.TryMarkRunning(CompleteEvidence(), "ready"));
    state.MarkLocalIsolated(WorkerIsolationReason::CONTROL_BACKEND_LOCAL_ISOLATION, "keepalive failed");

    std::string summary;
    for (const auto &part : metrics::DumpSummariesForTest()) {
        summary += part;
    }
    EXPECT_NE(summary.find("{\"name\":\"worker_service_mode\",\"total\":5,\"delta\":5}"), std::string::npos);
    EXPECT_NE(summary.find("{\"name\":\"worker_service_reason\",\"total\":4,\"delta\":4}"), std::string::npos);
    EXPECT_EQ(metrics::GetGauge(static_cast<uint16_t>(metrics::KvMetricId::WORKER_RECOVERY_PHASE)).Get(), 1);
    EXPECT_EQ(metrics::GetGauge(static_cast<uint16_t>(metrics::KvMetricId::WORKER_RECOVERY_EVIDENCE_MASK)).Get(), 1);
}

TEST(WorkerRuntimeStateTest, RejectedTerminalTransitionDoesNotPolluteTransitionLatency)
{
    Raii restoreMetrics([] {
        FLAGS_log_dir = "/tmp";
        metrics::ResetKvMetricsForTest();
        (void)metrics::InitKvMetrics();
    });
    InitKvMetricsForWorkerRuntimeStateTest();
    WorkerRuntimeStateManager state;
    ASSERT_TRUE(state.TryMarkRunning(CompleteEvidence(), "ready"));
    state.MarkDraining("draining");
    (void)metrics::DumpSummariesForTest();

    state.MarkRecovering(WorkerIsolationReason::RECOVERY_EVIDENCE_INCOMPLETE, "stale recovery");
    std::string summary;
    for (const auto &part : metrics::DumpSummariesForTest()) {
        summary += part;
    }
    const auto metric = summary.find("\"name\":\"worker_mode_transition_latency\"");
    ASSERT_NE(metric, std::string::npos);
    EXPECT_NE(summary.find("\"delta\":{\"count\":0", metric), std::string::npos);
}

TEST(WorkerRuntimeStateTest, SameModeRefreshDoesNotPolluteTransitionLatency)
{
    Raii restoreMetrics([] {
        FLAGS_log_dir = "/tmp";
        metrics::ResetKvMetricsForTest();
        (void)metrics::InitKvMetrics();
    });
    InitKvMetricsForWorkerRuntimeStateTest();
    WorkerRuntimeStateManager state;
    ASSERT_TRUE(state.TryMarkRunning(CompleteEvidence(), "ready"));
    (void)metrics::DumpSummariesForTest();

    ASSERT_TRUE(state.TryMarkRunning(CompleteEvidence(), "periodic refresh"));
    std::string summary;
    for (const auto &part : metrics::DumpSummariesForTest()) {
        summary += part;
    }
    const auto metric = summary.find("\"name\":\"worker_mode_transition_latency\"");
    ASSERT_NE(metric, std::string::npos);
    EXPECT_NE(summary.find("\"delta\":{\"count\":0", metric), std::string::npos);
}

TEST(WorkerRuntimeStateTest, PublishesCurrentSnapshotAfterMetricsInitialize)
{
    Raii restoreMetrics([] {
        FLAGS_log_dir = "/tmp";
        metrics::ResetKvMetricsForTest();
        (void)metrics::InitKvMetrics();
    });
    WorkerRuntimeStateManager state;
    ASSERT_TRUE(state.TryMarkRunning(CompleteEvidence(), "ready before metrics"));
    InitKvMetricsForWorkerRuntimeStateTest();

    state.PublishMetrics();

    EXPECT_EQ(metrics::GetGauge(static_cast<uint16_t>(metrics::KvMetricId::WORKER_SERVICE_MODE)).Get(), 3);
    EXPECT_EQ(metrics::GetGauge(static_cast<uint16_t>(metrics::KvMetricId::WORKER_SERVICE_REASON)).Get(), 1);
    EXPECT_EQ(metrics::GetGauge(static_cast<uint16_t>(metrics::KvMetricId::WORKER_RECOVERY_PHASE)).Get(), 8);
    EXPECT_EQ(metrics::GetGauge(static_cast<uint16_t>(metrics::KvMetricId::WORKER_RECOVERY_EVIDENCE_MASK)).Get(), 64);
}

TEST(WorkerRuntimeStateTest, MetricsReplayCannotOverwriteConcurrentTransition)
{
    Raii restoreMetrics([] {
        FLAGS_log_dir = "/tmp";
        metrics::ResetKvMetricsForTest();
        (void)metrics::InitKvMetrics();
    });
    InitKvMetricsForWorkerRuntimeStateTest();
    constexpr const char *injectPoint = "WorkerRuntimeState.PublishMetrics.BeforePublish";
    ASSERT_TRUE(inject::Set(injectPoint, "1*call(100)").IsOk());
    Raii clearInject([] { (void)inject::Clear(injectPoint); });
    WorkerRuntimeStateManager state;
    ASSERT_TRUE(state.TryMarkRunning(CompleteEvidence(), "ready"));

    auto replay = std::async(std::launch::async, [&state] { state.PublishMetrics(); });
    for (size_t i = 0; i < INJECT_WAIT_RETRY_LIMIT && inject::GetExecuteCount(injectPoint) == 0; ++i) {
        std::this_thread::sleep_for(INJECT_WAIT_INTERVAL);
    }
    ASSERT_EQ(inject::GetExecuteCount(injectPoint), 1U);
    auto isolation = std::async(std::launch::async, [&state] {
        state.MarkLocalIsolated(WorkerIsolationReason::CONTROL_BACKEND_LOCAL_ISOLATION, "keepalive failed");
    });
    ASSERT_EQ(replay.wait_for(std::chrono::seconds(1)), std::future_status::ready);
    ASSERT_EQ(isolation.wait_for(std::chrono::seconds(1)), std::future_status::ready);

    EXPECT_EQ(metrics::GetGauge(static_cast<uint16_t>(metrics::KvMetricId::WORKER_SERVICE_MODE)).Get(), 5);
    EXPECT_EQ(metrics::GetGauge(static_cast<uint16_t>(metrics::KvMetricId::WORKER_SERVICE_REASON)).Get(), 4);
    EXPECT_EQ(metrics::GetGauge(static_cast<uint16_t>(metrics::KvMetricId::WORKER_RECOVERY_EVIDENCE_MASK)).Get(), 1);
}

TEST(WorkerRuntimeStateTest, RecordsLocalIsolationAndRecoveryReason)
{
    WorkerRuntimeStateManager state;
    ASSERT_TRUE(state.TryMarkRunning(CompleteEvidence(), "steady"));

    state.MarkLocalIsolated(WorkerIsolationReason::CONTROL_BACKEND_LOCAL_ISOLATION, "keepalive failed");
    auto snapshot = state.GetSnapshot();
    EXPECT_EQ(snapshot.mode, WorkerServiceMode::LOCAL_ISOLATED);
    EXPECT_EQ(snapshot.reason, WorkerIsolationReason::CONTROL_BACKEND_LOCAL_ISOLATION);
    EXPECT_EQ(RecoveryEvidenceMask(snapshot.evidence), 0U);
    EXPECT_STREQ(ToString(snapshot.mode), "LOCAL_ISOLATED");
    EXPECT_STREQ(ToString(snapshot.reason), "CONTROL_BACKEND_LOCAL_ISOLATION");

    state.MarkRecovering(WorkerIsolationReason::CONTROL_BACKEND_LOCAL_ISOLATION, "control backend restored");
    snapshot = state.GetSnapshot();
    EXPECT_EQ(snapshot.mode, WorkerServiceMode::RECOVERING);
    EXPECT_EQ(snapshot.reason, WorkerIsolationReason::CONTROL_BACKEND_LOCAL_ISOLATION);
}

TEST(WorkerRuntimeStateTest, ReadGuardLinearizesIsolationTransition)
{
    WorkerRuntimeStateManager state;
    ASSERT_TRUE(state.TryMarkRunning(CompleteEvidence(), "steady"));
    std::optional<WorkerRuntimeStateReadGuard> guard(state.TryAcquireReadGuard());
    if (!guard.has_value()) {
        FAIL() << "read guard should be acquired while worker is running";
    }
    ASSERT_EQ(guard.value().GetSnapshot().mode, WorkerServiceMode::RUNNING);

    auto isolation = std::async(std::launch::async, [&state] {
        state.MarkLocalIsolated(WorkerIsolationReason::CONTROL_BACKEND_LOCAL_ISOLATION, "keepalive failed");
    });
    EXPECT_EQ(isolation.wait_for(READ_GUARD_BLOCK_TIMEOUT), std::future_status::timeout);
    EXPECT_FALSE(state.TryAcquireReadGuard().has_value());

    guard.reset();
    EXPECT_EQ(isolation.wait_for(std::chrono::seconds(1)), std::future_status::ready);
    EXPECT_EQ(state.GetSnapshot().mode, WorkerServiceMode::LOCAL_ISOLATED);
}

TEST(WorkerRuntimeStateTest, StaleCompleteEvidenceCannotReopenLocalIsolation)
{
    WorkerRuntimeStateManager state;
    ASSERT_TRUE(state.TryMarkRunning(CompleteEvidence(), "steady"));

    state.MarkRecovering(WorkerIsolationReason::RECOVERY_EVIDENCE_INCOMPLETE, "waiting for object-cache evidence");
    state.MarkLocalIsolated(WorkerIsolationReason::CONTROL_BACKEND_LOCAL_ISOLATION, "keepalive failed again");

    EXPECT_FALSE(state.TryMarkRunning(CompleteEvidence(), "stale complete evidence"));

    auto snapshot = state.GetSnapshot();
    EXPECT_EQ(snapshot.mode, WorkerServiceMode::LOCAL_ISOLATED);
    EXPECT_EQ(snapshot.reason, WorkerIsolationReason::CONTROL_BACKEND_LOCAL_ISOLATION);

    state.MarkRecovering(WorkerIsolationReason::CONTROL_BACKEND_LOCAL_ISOLATION, "control backend restored");
    EXPECT_TRUE(state.TryMarkRunning(CompleteEvidence(), "fresh recovery evidence"));
    snapshot = state.GetSnapshot();
    EXPECT_EQ(snapshot.mode, WorkerServiceMode::RUNNING);
    EXPECT_EQ(snapshot.reason, WorkerIsolationReason::NONE);
}

TEST(WorkerRuntimeStateTest, StaleCompleteEvidenceCannotReopenOutOfMemory)
{
    WorkerRuntimeStateManager state;
    ASSERT_TRUE(state.TryMarkRunning(CompleteEvidence(), "steady"));

    state.MarkOutOfMemory("allocation failed");
    EXPECT_FALSE(state.TryMarkRunning(CompleteEvidence(), "stale complete evidence"));

    auto snapshot = state.GetSnapshot();
    EXPECT_EQ(snapshot.mode, WorkerServiceMode::OUT_OF_MEMORY);
    EXPECT_EQ(snapshot.reason, WorkerIsolationReason::OUT_OF_MEMORY);
    EXPECT_FALSE(snapshot.evidence.resourceReady);
    EXPECT_TRUE(snapshot.evidence.membershipReady);
    EXPECT_TRUE(snapshot.evidence.ownershipReady);

    state.MarkRecovering(WorkerIsolationReason::OUT_OF_MEMORY, "memory pressure relieved; validating evidence");
    EXPECT_TRUE(state.TryMarkRunning(CompleteEvidence(), "fresh resource recovery evidence"));
    snapshot = state.GetSnapshot();
    EXPECT_EQ(snapshot.mode, WorkerServiceMode::RUNNING);
    EXPECT_EQ(snapshot.reason, WorkerIsolationReason::NONE);
}

TEST(WorkerRuntimeStateTest, DrainingIsTerminalForServingTransitions)
{
    WorkerRuntimeStateManager state;
    ASSERT_TRUE(state.TryMarkRunning(CompleteEvidence(), "steady"));

    state.MarkDraining("voluntary scale-down drain started");
    EXPECT_FALSE(state.TryMarkRunning(CompleteEvidence(), "late recovery evidence"));
    state.MarkRecovering(WorkerIsolationReason::RECOVERY_EVIDENCE_INCOMPLETE, "late topology recovery");
    state.MarkLocalIsolated(WorkerIsolationReason::CONTROL_BACKEND_LOCAL_ISOLATION, "late keepalive failure");
    state.MarkOutOfMemory("late oom");

    const auto snapshot = state.GetSnapshot();
    EXPECT_EQ(snapshot.mode, WorkerServiceMode::DRAINING);
    EXPECT_EQ(snapshot.reason, WorkerIsolationReason::NONE);
    EXPECT_EQ(snapshot.detail, "voluntary scale-down drain started");
}

TEST(WorkerRuntimeStateTest, StoppingIsTerminalForServingTransitions)
{
    WorkerRuntimeStateManager state;
    ASSERT_TRUE(state.TryMarkRunning(CompleteEvidence(), "steady"));

    state.MarkStopping(WorkerIsolationReason::PROCESS_STOPPING, "shutdown");
    EXPECT_FALSE(state.TryMarkRunning(CompleteEvidence(), "late recovery"));
    state.MarkLocalIsolated(WorkerIsolationReason::CONTROL_BACKEND_LOCAL_ISOLATION, "late keepalive");
    state.MarkOutOfMemory("late oom");

    const auto snapshot = state.GetSnapshot();
    EXPECT_EQ(snapshot.mode, WorkerServiceMode::STOPPING);
    EXPECT_EQ(snapshot.reason, WorkerIsolationReason::PROCESS_STOPPING);
    EXPECT_EQ(snapshot.detail, "shutdown");
}
}  // namespace
}  // namespace datasystem::worker
