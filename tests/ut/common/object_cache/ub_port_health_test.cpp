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

#include <gtest/gtest.h>

#include "tests/support/fake_ub_port_status_provider.h"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <future>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "datasystem/common/flags/common_flags.h"
#include "datasystem/common/log/logging.h"
#include "datasystem/common/object_cache/ub_port_health.h"
#include "datasystem/common/util/uuid_generator.h"

namespace datasystem {
namespace ut {
namespace {

UbPortHealthSummary Summary(uint32_t totalPortCount, uint32_t badPortCount, uint64_t healthEpoch,
                            bool verificationPending = false)
{
    return { true, totalPortCount, badPortCount, healthEpoch, verificationPending };
}

using FakeUbPortStatusProvider = ::datasystem::test::FakeUbPortStatusProvider;

class RecordingUbPortHealthObserver : public IUbPortHealthObserver {
public:
    void OnUbPortHealthChanged(const UbPortHealthSummary &summary) override
    {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            summaries_.push_back(summary);
        }
        cv_.notify_all();
    }

    bool WaitForSummaries(size_t expected)
    {
        std::unique_lock<std::mutex> lock(mutex_);
        return cv_.wait_for(lock, std::chrono::seconds(2),
                            [this, expected] { return summaries_.size() >= expected; });
    }

private:
    std::mutex mutex_;
    std::condition_variable cv_;
    std::vector<UbPortHealthSummary> summaries_;
};

class ThrowOnceUbPortStatusProvider : public IUbPortStatusProvider {
public:
    Status QueryPortStatus(std::vector<UbPortStatus> &portStatus) override
    {
        if (calls_.fetch_add(1, std::memory_order_acq_rel) == 0) {
            throw std::runtime_error("provider failure");
        }
        portStatus = { { 0, UbPortState::GOOD } };
        return Status::OK();
    }

    size_t Calls() const
    {
        return calls_.load(std::memory_order_acquire);
    }

private:
    std::atomic<size_t> calls_{ 0 };
};
}  // namespace

TEST(UbPortHealthTest, AnyHealthyPortRecoversIsolation)
{
    auto unavailable = Summary(4, 4, 1);
    EXPECT_TRUE(ShouldIsolateForUbPortHealth(unavailable));
    EXPECT_FALSE(ShouldRecoverFromUbIsolation(unavailable));

    auto degraded = Summary(4, 3, 2);
    EXPECT_TRUE(ShouldRecoverFromUbIsolation(degraded));
}

TEST(UbPortHealthTest, IncarnationLogPrefixUsesOnePrintableFormat)
{
    std::string uuidBytes(UUID_SIZE, '\0');
    for (size_t i = 0; i < uuidBytes.size(); ++i) {
        uuidBytes[i] = static_cast<char>(i);
    }
    const auto uuidPrefix = FormatUbHealthIncarnationPrefix(uuidBytes);
    EXPECT_EQ(uuidPrefix, "00010203-040");

    EXPECT_EQ(FormatUbHealthIncarnationPrefix("worker-incarnation"), "776f726b6572");
}

TEST(UbPortHealthTest, MonitorPublishesNormalizedImmutableSnapshot)
{
    Logging::ResetLoggingRuntimeForTest();
    auto provider = std::make_shared<FakeUbPortStatusProvider>();
    auto observer = std::make_shared<RecordingUbPortHealthObserver>();
    provider->SetResult(Status::OK(), { { 3, UbPortState::BAD }, { 1, UbPortState::GOOD },
                                       { 2, UbPortState::GOOD }, { 0, UbPortState::BAD } });
    provider->Pause();
    auto monitor = UbPortHealthMonitor::CreateForTest(provider, std::chrono::hours(1), observer,
                                                       UbPortHealthOwner::WORKER);

    testing::internal::CaptureStderr();
    ASSERT_TRUE(monitor->Start().IsOk());
    ASSERT_TRUE(provider->WaitForCalls(1));
    provider->Resume();
    ASSERT_TRUE(observer->WaitForSummaries(1));

    auto snapshot = monitor->GetSnapshot();
    ASSERT_NE(snapshot, nullptr);
    EXPECT_TRUE(snapshot->valid);
    EXPECT_EQ(snapshot->totalPortCount, 4u);
    EXPECT_EQ(snapshot->goodPortCount, 2u);
    EXPECT_EQ(snapshot->badPortCount, 2u);
    EXPECT_EQ(snapshot->unknownPortCount, 0u);
    EXPECT_EQ(snapshot->healthEpoch, UB_PORT_HEALTH_FIRST_EPOCH);
    ASSERT_EQ(snapshot->ports.size(), 4u);
    EXPECT_EQ(snapshot->ports.front().portIndex, 0u);
    EXPECT_EQ(snapshot->ports.back().portIndex, 3u);
    monitor->Stop();
    const auto logs = testing::internal::GetCapturedStderr();
    EXPECT_NE(logs.find("UB_PORT_HEALTH action=snapshot_changed owner=worker"), std::string::npos) << logs;
    EXPECT_NE(logs.find("bad=2 total=4 health_epoch=1"), std::string::npos) << logs;
    EXPECT_NE(logs.find("ports=0:BAD,1:GOOD,2:GOOD,3:BAD"), std::string::npos) << logs;
}

TEST(UbPortHealthTest, MonitorPreservesLastConfirmedSnapshotOnInvalidQuery)
{
    auto provider = std::make_shared<FakeUbPortStatusProvider>();
    auto observer = std::make_shared<RecordingUbPortHealthObserver>();
    provider->SetResult(Status::OK(), { { 0, UbPortState::GOOD }, { 1, UbPortState::GOOD } });
    provider->Pause();
    auto monitor = UbPortHealthMonitor::CreateForTest(provider, std::chrono::milliseconds(20), observer);

    ASSERT_TRUE(monitor->Start().IsOk());
    ASSERT_TRUE(provider->WaitForCalls(1));
    provider->Resume();
    ASSERT_TRUE(observer->WaitForSummaries(1));

    provider->SetResult(Status::OK(), { { 0, UbPortState::GOOD }, { 0, UbPortState::BAD } });
    provider->Pause();
    monitor->TriggerRefresh();
    ASSERT_TRUE(provider->WaitForCalls(2));
    provider->Resume();
    ASSERT_TRUE(monitor->EnsureFresh(std::chrono::hours(1)).IsError());

    auto snapshot = monitor->GetSnapshot();
    ASSERT_NE(snapshot, nullptr);
    EXPECT_TRUE(snapshot->valid);
    EXPECT_EQ(snapshot->totalPortCount, 2u);
    EXPECT_EQ(snapshot->badPortCount, 0u);
    EXPECT_EQ(snapshot->healthEpoch, 1u);
    EXPECT_TRUE(snapshot->verificationPending);
    EXPECT_EQ(snapshot->lastQueryStatus.GetCode(), K_INVALID);
    auto summary = monitor->GetSummary();
    ASSERT_TRUE(summary.has_value());
    EXPECT_TRUE(summary->verificationPending);
    monitor->Stop();
}

TEST(UbPortHealthTest, MonitorStopWaitsForInFlightProviderQuery)
{
    auto provider = std::make_shared<FakeUbPortStatusProvider>();
    provider->SetResult(Status::OK(), { { 0, UbPortState::GOOD } });
    provider->Pause();
    auto monitor = UbPortHealthMonitor::CreateForTest(provider, std::chrono::milliseconds(1), {});

    ASSERT_TRUE(monitor->Start().IsOk());
    ASSERT_TRUE(provider->WaitForCalls(1));
    auto stop = std::async(std::launch::async, [&monitor] { monitor->Stop(); });
    EXPECT_EQ(stop.wait_for(std::chrono::milliseconds(30)), std::future_status::timeout);

    provider->Resume();
    EXPECT_EQ(stop.wait_for(std::chrono::seconds(1)), std::future_status::ready);
    EXPECT_EQ(monitor->EnsureFresh(std::chrono::milliseconds(0)).GetCode(), K_NOT_READY);
}

TEST(UbPortHealthTest, ProviderExceptionClearsInFlightAndAllowsNextQuery)
{
    auto provider = std::make_shared<ThrowOnceUbPortStatusProvider>();
    auto monitor = UbPortHealthMonitor::CreateForTest(provider, std::chrono::milliseconds(1));
    ASSERT_TRUE(monitor->Start().IsOk());
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (provider->Calls() < 2 && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::yield();
    }
    ASSERT_GE(provider->Calls(), 2u);
    ASSERT_TRUE(monitor->EnsureFresh(std::chrono::hours(1)).IsOk());
    auto snapshot = monitor->GetSnapshot();
    ASSERT_NE(snapshot, nullptr);
    EXPECT_TRUE(snapshot->valid);
    EXPECT_FALSE(snapshot->verificationPending);
    EXPECT_EQ(snapshot->badPortCount, 0u);
    monitor->Stop();
}
}  // namespace ut
}  // namespace datasystem
