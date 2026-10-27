/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <functional>
#include <thread>

#include <gtest/gtest.h>

#include "datasystem/cluster/ub_health/ub_health_lease_sync.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/log/spdlog/logger_provider.h"
#include "datasystem/common/log/spdlog/provider.h"
#include "datasystem/common/object_cache/ub_health_summary_codec.h"
#include "datasystem/common/util/file_util.h"
#include "ut/cluster/testing/fake_coordination_backend.h"

namespace datasystem::cluster {
namespace {
const HostPort SELF("127.0.0.1", 18480);
const HostPort GLOBAL_PEER("127.0.0.1", 18481);
const HostPort LOCAL_PEER("127.0.0.1", 18482);
constexpr char TABLE[] = "/datasystem/test/ub_health";

std::string Encode(const UbHealthSummary &summary)
{
    UbHealthSummaryPb pb;
    EncodeUbHealthSummary(summary, pb);
    return pb.SerializeAsString();
}

const UbHealthSummary *FindSummary(const std::vector<UbHealthSummary> &summaries,
                                   const HostPort &worker)
{
    auto iter = std::find_if(summaries.begin(), summaries.end(),
                             [&worker](const auto &summary) { return summary.worker == worker; });
    return iter == summaries.end() ? nullptr : &*iter;
}

constexpr auto SYNC_TEST_INTERVAL = std::chrono::milliseconds(10);

template <typename Pred>
bool WaitFor(Pred pred, std::chrono::milliseconds timeout)
{
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (!pred()) {
        if (std::chrono::steady_clock::now() >= deadline) {
            return false;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
    return true;
}

size_t CountLevelLines(const std::string &logs, const std::string &levelMarker, const std::string &needle)
{
    size_t count = 0;
    size_t lineStart = 0;
    while (lineStart < logs.size()) {
        const auto lineEnd = logs.find('\n', lineStart);
        const auto lineEndPos = lineEnd == std::string::npos ? logs.size() : lineEnd;
        const std::string line = logs.substr(lineStart, lineEndPos - lineStart);
        if (line.find(levelMarker) != std::string::npos && line.find(needle) != std::string::npos) {
            ++count;
        }
        lineStart = lineEnd == std::string::npos ? logs.size() : lineEnd + 1;
    }
    return count;
}
}  // namespace

class UbHealthLeaseSyncLogTest : public testing::Test {
protected:
    void SetUp() override
    {
        oldMinLogLevel_ = FLAGS_minloglevel;
        FLAGS_minloglevel = LogSeverity::INFO;
        previousProvider_ = Provider::Instance().GetLoggerProvider();
        char logDir[] = "/tmp/ds-lease-sync-log-XXXXXX";
        ASSERT_NE(mkdtemp(logDir), nullptr);
        logDir_ = logDir;
        loggerProvider_ = std::make_shared<LoggerProvider>(GlobalLogParam{});
        Provider::Instance().SetLoggerProvider(loggerProvider_);
        LogParam param;
        param.logDir = logDir_;
        param.fileNamePatterns = { "lease_sync_log.INFO", "lease_sync_log.WARNING", "lease_sync_log.ERROR" };
        param.alsoLog2Stderr = true;
        param.stderrLogLevel = "INFO";
        ASSERT_NE(loggerProvider_->InitDsLogger(param), nullptr);
    }

    void TearDown() override
    {
        if (loggerProvider_) {
            loggerProvider_->DropDsLogger();
        }
        Provider::Instance().SetLoggerProvider(previousProvider_);
        FLAGS_minloglevel = oldMinLogLevel_;
        if (!logDir_.empty()) {
            EXPECT_TRUE(RemoveAll(logDir_).IsOk());
        }
    }

    std::string CaptureLogs(const std::function<void()> &action)
    {
        testing::internal::CaptureStderr();
        action();
        EXPECT_TRUE(loggerProvider_->ForceFlush());
        return testing::internal::GetCapturedStderr();
    }

private:
    int oldMinLogLevel_ = 0;
    std::string logDir_;
    std::shared_ptr<LoggerProvider> previousProvider_;
    std::shared_ptr<LoggerProvider> loggerProvider_;
};

// LOG_EVERY_T keeps per-call-site state for the whole process, so the first WARNING and first ERROR of
// the Run() loop can only be emitted once across all tests; this test therefore drives both phases in a
// single timeline and must stay ahead of the behavioral tests below.
TEST_F(UbHealthLeaseSyncLogTest, MemberNotVisibleStaysWarningAndSidecarFailureEscalatesToError)
{
    std::atomic<int> incarnationCalls{ 0 };
    std::atomic<bool> sidecarFailing{ false };
    UbHealthLeaseSync::Config config{
        TABLE,
        SELF,
        [&incarnationCalls, &sidecarFailing](std::string &incarnation) {
            incarnationCalls.fetch_add(1);
            if (sidecarFailing.load()) {
                incarnation = "self-1";
                return Status::OK();
            }
            return Status(K_NOT_FOUND, "member address not found");
        },
        [] { return UbHealthSummary{}; },
        [](const auto &snapshot) { (void)snapshot; },
        SYNC_TEST_INTERVAL
    };
    UbHealthLeaseSync sync(
        [&sidecarFailing](const std::string &, const std::string &, const std::string &) {
            return sidecarFailing.load() ? Status(K_RUNTIME_ERROR, "ub sidecar prefix permission denied")
                                         : Status::OK();
        },
        [](const std::string &, std::vector<std::pair<std::string, std::string>> &records) {
            records.clear();
            return Status::OK();
        },
        std::move(config));

    const auto logs = CaptureLogs([&] {
        ASSERT_TRUE(sync.Start().IsOk());
        // Phase 0: local member not visible yet (startup race); repeated failures must stay WARNING.
        ASSERT_TRUE(WaitFor([&incarnationCalls] { return incarnationCalls.load() >= 5; },
                            std::chrono::seconds(2)));
        sidecarFailing.store(true);
        // Phase 1: member visible, the UB sidecar publish keeps failing; must escalate to ERROR.
        ASSERT_TRUE(WaitFor([&incarnationCalls] { return incarnationCalls.load() >= 10; },
                            std::chrono::seconds(2)));
        sync.Stop();
    });

    // Startup-race failures stay WARNING-level and are throttled to a single line carrying the
    // member-not-visible cause.
    EXPECT_EQ(CountLevelLines(logs, " | W | ", "Refresh UB health lease snapshot failed"), 1u) << logs;
    EXPECT_EQ(CountLevelLines(logs, " | W | ", "member address not found"), 1u) << logs;
    EXPECT_NE(logs.find("local member not visible yet"), std::string::npos) << logs;
    // The single ERROR must be the sidecar failure with its own root cause (a startup-race escalation
    // would make the ERROR count exceed one), and repeated sidecar failures are throttled within 60s.
    EXPECT_EQ(CountLevelLines(logs, " | E | ", "Refresh UB health lease snapshot failed"), 1u) << logs;
    EXPECT_EQ(CountLevelLines(logs, " | E | ", "ub sidecar prefix permission denied"), 1u) << logs;
}

TEST(UbHealthLeaseSyncTest, PublishesOneSelfRecordAndConsumesSelfOnlySnapshot)
{
    FakeCoordinationBackend backend;
    std::vector<UbHealthSummary> consumed;
    std::string selfIncarnation = "self-1";
    UbHealthLeaseSync::Config config{
        TABLE,
        SELF,
        [&selfIncarnation](std::string &incarnation) {
            incarnation = selfIncarnation;
            return Status::OK();
        },
        [] {
            UbHealthSummary summary;
            summary.writable = false;
            summary.epoch = 4;
            return summary;
        },
        [&consumed](const auto &snapshot) { consumed = snapshot; }
    };
    UbHealthLeaseSync sync(backend, std::move(config));

    ASSERT_TRUE(sync.SyncOnce().IsOk());

    std::vector<std::pair<std::string, std::string>> records;
    ASSERT_TRUE(backend.GetAll(TABLE, records).IsOk());
    ASSERT_EQ(records.size(), 1u);
    ASSERT_EQ(consumed.size(), 1u);
    EXPECT_EQ(consumed.front().worker, SELF);
    EXPECT_EQ(consumed.front().incarnation, "self-1");
    EXPECT_FALSE(consumed.front().writable);
    EXPECT_EQ(consumed.front().epoch, 4u);

    selfIncarnation = "self-2";
    ASSERT_TRUE(sync.SyncOnce().IsOk());
    ASSERT_EQ(consumed.size(), 1u);
    EXPECT_EQ(consumed.front().incarnation, "self-2");
}

TEST(UbHealthLeaseSyncTest, LeaseExpiryDropsGlobalObservationButPreservesLocalEvidence)
{
    FakeCoordinationBackend backend;
    PeerUbAdmission admission;
    UbHealthSummary global;
    global.worker = GLOBAL_PEER;
    global.incarnation = "global-1";
    global.writable = false;
    global.epoch = 2;
    backend.PutBytes(TABLE, GLOBAL_PEER.ToString(), Encode(global));
    UbOpOutcome local(LOCAL_PEER, UbOperationKind::WORKER_REMOTE_GET_WRITEBACK,
                      Status(K_URMA_ERROR, "local provider observation"));
    local.cqeStatus = 4;
    admission.ReportOutcome(local);
    UbHealthLeaseSync::Config config{
        TABLE,
        SELF,
        [](std::string &incarnation) {
            incarnation = "self-1";
            return Status::OK();
        },
        [] { return UbHealthSummary{}; },
        [&admission](const auto &snapshot) { admission.ReplaceGlobalSummaries(snapshot); }
    };
    UbHealthLeaseSync sync(backend, std::move(config));

    ASSERT_TRUE(sync.SyncOnce().IsOk());
    EXPECT_EQ(admission.CheckReadSource(GLOBAL_PEER).GetCode(), K_URMA_DATA_WORKER_UNAVAILABLE);
    EXPECT_EQ(admission.CheckReadSource(LOCAL_PEER).GetCode(), K_URMA_DATA_WORKER_UNAVAILABLE);

    ASSERT_TRUE(backend.Delete(TABLE, GLOBAL_PEER.ToString()).IsOk());
    ASSERT_TRUE(sync.SyncOnce().IsOk());
    EXPECT_TRUE(admission.CheckReadSource(GLOBAL_PEER).IsOk());
    EXPECT_EQ(admission.CheckReadSource(LOCAL_PEER).GetCode(), K_URMA_DATA_WORKER_UNAVAILABLE);
}
TEST(UbHealthLeaseSyncTest, SameIncarnationHealthEpochNeverMovesBackward)
{
    FakeCoordinationBackend backend;
    std::vector<UbHealthSummary> consumed;
    UbHealthSummary peer;
    peer.worker = GLOBAL_PEER;
    peer.incarnation = "global-1";
    peer.writable = false;
    peer.epoch = 5;
    peer.portHealth = UbPortHealthSummary{ true, 4, 4, 5, false };
    backend.PutBytes(TABLE, GLOBAL_PEER.ToString(), Encode(peer));
    UbHealthLeaseSync::Config config{
        TABLE,
        SELF,
        [](std::string &incarnation) {
            incarnation = "self-1";
            return Status::OK();
        },
        [] { return UbHealthSummary{}; },
        [&consumed](const auto &snapshot) { consumed = snapshot; }
    };
    UbHealthLeaseSync sync(backend, std::move(config));
    ASSERT_TRUE(sync.SyncOnce().IsOk());

    auto staleRecovery = peer;
    staleRecovery.writable = true;
    staleRecovery.epoch = 6;
    staleRecovery.portHealth = UbPortHealthSummary{ true, 4, 3, 4, false };
    backend.PutBytes(TABLE, GLOBAL_PEER.ToString(), Encode(staleRecovery));
    ASSERT_TRUE(sync.SyncOnce().IsOk());
    auto observed = FindSummary(consumed, GLOBAL_PEER);
    ASSERT_NE(observed, nullptr);
    EXPECT_EQ(observed->epoch, 6u);
    EXPECT_EQ(observed->portHealth->badPortCount, 4u);

    auto currentRecovery = staleRecovery;
    currentRecovery.portHealth->healthEpoch = 6;
    backend.PutBytes(TABLE, GLOBAL_PEER.ToString(), Encode(currentRecovery));
    ASSERT_TRUE(sync.SyncOnce().IsOk());
    observed = FindSummary(consumed, GLOBAL_PEER);
    ASSERT_NE(observed, nullptr);
    EXPECT_EQ(observed->epoch, 6u);
    EXPECT_EQ(observed->portHealth->badPortCount, 3u);
}

TEST(UbHealthLeaseSyncTest, NewPortHealthSurvivesOlderAdmissionEpoch)
{
    FakeCoordinationBackend backend;
    std::vector<UbHealthSummary> consumed;
    UbHealthSummary peer;
    peer.worker = GLOBAL_PEER;
    peer.incarnation = "global-1";
    peer.epoch = 8;
    peer.portHealth = UbPortHealthSummary{ true, 4, 0, 2, false };
    backend.PutBytes(TABLE, GLOBAL_PEER.ToString(), Encode(peer));
    UbHealthLeaseSync sync(
        backend,
        { TABLE,
          SELF,
          [](std::string &incarnation) {
              incarnation = "self-1";
              return Status::OK();
          },
          [] { return UbHealthSummary{}; },
          [&consumed](const auto &snapshot) { consumed = snapshot; } });
    ASSERT_TRUE(sync.SyncOnce().IsOk());

    auto newerPortHealth = peer;
    newerPortHealth.epoch = 7;
    newerPortHealth.portHealth = UbPortHealthSummary{ true, 4, 4, 3, false };
    backend.PutBytes(TABLE, GLOBAL_PEER.ToString(), Encode(newerPortHealth));
    ASSERT_TRUE(sync.SyncOnce().IsOk());

    auto observed = FindSummary(consumed, GLOBAL_PEER);
    ASSERT_NE(observed, nullptr);
    EXPECT_EQ(observed->epoch, 8u);
    ASSERT_TRUE(observed->portHealth.has_value());
    EXPECT_EQ(observed->portHealth->healthEpoch, 3u);
    EXPECT_EQ(observed->portHealth->badPortCount, 4u);
}

TEST(UbHealthLeaseSyncTest, InitialSyncFailuresKeepRetryingAndAdvance)
{
    FakeCoordinationBackend backend;
    std::atomic<int> remainingFailures{ 3 };
    std::atomic<int> published{ 0 };
    UbHealthLeaseSync::Config config{
        TABLE,
        SELF,
        [&remainingFailures](std::string &incarnation) {
            if (remainingFailures.fetch_sub(1) > 0) {
                return Status(K_NOT_FOUND, "member address not found");
            }
            incarnation = "self-1";
            return Status::OK();
        },
        [] { return UbHealthSummary{}; },
        [&published](const auto &snapshot) { published.fetch_add(static_cast<int>(snapshot.size())); },
        SYNC_TEST_INTERVAL
    };
    UbHealthLeaseSync sync(backend, std::move(config));
    ASSERT_TRUE(sync.Start().IsOk());

    // The first few startup failures must not stop the sync thread; it keeps retrying and publishes
    // regularly once the local lease snapshot is available.
    ASSERT_TRUE(WaitFor([&published] { return published.load() > 0; }, std::chrono::seconds(2)));
    sync.Stop();
}

TEST(UbHealthLeaseSyncTest, SustainedFailuresBeyondThresholdKeepRetrying)
{
    FakeCoordinationBackend backend;
    std::atomic<bool> failing{ true };
    std::atomic<int> incarnationCalls{ 0 };
    std::atomic<int> published{ 0 };
    UbHealthLeaseSync::Config config{
        TABLE,
        SELF,
        [&failing, &incarnationCalls](std::string &incarnation) {
            if (failing.load()) {
                incarnationCalls.fetch_add(1);
                return Status(K_NOT_FOUND, "member address not found");
            }
            incarnation = "self-1";
            return Status::OK();
        },
        [] { return UbHealthSummary{}; },
        [&published](const auto &snapshot) { published.fetch_add(static_cast<int>(snapshot.size())); },
        SYNC_TEST_INTERVAL
    };
    UbHealthLeaseSync sync(backend, std::move(config));
    ASSERT_TRUE(sync.Start().IsOk());

    // Failures beyond the escalation threshold must keep the thread alive and retrying, not break out.
    ASSERT_TRUE(WaitFor([&incarnationCalls] { return incarnationCalls.load() > 5; }, std::chrono::seconds(2)));
    failing.store(false);
    ASSERT_TRUE(WaitFor([&published] { return published.load() > 0; }, std::chrono::seconds(2)));
    sync.Stop();
}

}  // namespace datasystem::cluster
