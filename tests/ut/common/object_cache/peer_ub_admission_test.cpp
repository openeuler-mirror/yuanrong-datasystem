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

#include "datasystem/common/object_cache/peer_ub_admission.h"

namespace datasystem {
namespace {

const HostPort PEER("127.0.0.1", 31501);

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

TEST(PeerUbAdmissionTest, LegacyUrmaErrorWithoutRawEvidenceDoesNotQuarantine)
{
    PeerUbAdmission admission;
    UbOpOutcome outcome(PEER, UbOperationKind::WORKER_REMOTE_GET_WRITEBACK,
                        Status(K_URMA_ERROR, "legacy remote error"));

    admission.ReportOutcome(outcome);

    EXPECT_TRUE(admission.CheckReadSource(PEER).IsOk());
    EXPECT_FALSE(admission.GetState(PEER).has_value());
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
    admission.InitializeProbing(PEER, 10);
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

    admission.ReconcileTopologyWorkers({ PEER }, 112, 10);
    admission.ReplaceGlobalSummaries({ oldSummary });
    EXPECT_TRUE(admission.CheckReadSource(PEER).IsOk());
    oldSummary.incarnation = "worker-new";
    admission.ReplaceGlobalSummaries({ oldSummary });
    EXPECT_EQ(admission.CheckReadSource(PEER).GetCode(), K_URMA_DATA_WORKER_UNAVAILABLE);

    admission.PruneExpiredTopologyState(121);
    EXPECT_EQ(admission.GetStats().replayTombstones, 0u);
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
