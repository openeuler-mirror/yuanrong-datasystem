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
#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <chrono>
#include <future>

#ifdef USE_URMA
#define private public
#include "datasystem/common/rdma/urma_manager.h"
#undef private

#include "datasystem/common/flags/flags.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/log/logging.h"
#include "datasystem/common/util/raii.h"
#include "common/binmock/binmock.h"

DS_DECLARE_bool(enable_urma_perf);

namespace datasystem {
namespace {
TEST(UrmaPerfControlTest, DisabledSkipsSdkAndExplicitEnableReachesCollection)
{
    constexpr char hook[] = "UrmaManager.PerfBeforeSdk";
    const bool originalFlag = FLAGS_enable_urma_perf;
    Raii restore([&] {
        FLAGS_enable_urma_perf = originalFlag;
        (void)inject::Clear(hook);
    });
    auto &manager = UrmaManager::Instance();
    // call() discards the callback status; return() stops before the provider is invoked.
    ASSERT_TRUE(inject::Set(hook, "return()").IsOk());
    FLAGS_enable_urma_perf = false;
    EXPECT_TRUE(manager.PerfThreadMain().IsOk());
    EXPECT_EQ(inject::GetExecuteCount(hook), 0u);
    FLAGS_enable_urma_perf = true;
    EXPECT_EQ(manager.PerfThreadMain().GetCode(), K_RUNTIME_ERROR);
    EXPECT_EQ(inject::GetExecuteCount(hook), 1u);
    FLAGS_enable_urma_perf = false;
    EXPECT_TRUE(manager.PerfThreadMain().IsOk());
    EXPECT_EQ(inject::GetExecuteCount(hook), 1u);
}

class UrmaClientOwnershipTest : public ::testing::Test {
protected:
    void SetUp() override
    {
        info.localAddress = HostPort("127.0.0.1", 19873);
        info.uniqueInstanceId = "ownership-test";
        info.eid = std::string(16, '\0');
        info.ToProto(*response.mutable_hand_shake());
        InstallConnection();
    }

    void TearDown() override
    {
        (void)manager.RemoveRemoteDevice(info.localAddress.ToString());
    }

    void InstallConnection()
    {
        TbbUrmaConnectionMap::accessor entry;
        manager.urmaConnectionMap_.insert(entry, info.localAddress.ToString());
        entry->second = std::make_shared<UrmaConnection>(nullptr, info);
    }

    UrmaManager &manager = UrmaManager::Instance();
    UrmaJfrInfo info;
    UrmaHandshakeRspPb response;
};

TEST_F(UrmaClientOwnershipTest, LastOwnerReleasesSharedConnection)
{
    std::shared_ptr<UrmaConnection> first;
    std::shared_ptr<UrmaConnection> second;
    ASSERT_TRUE(
        manager.FinalizeOutboundConnection(response, UrmaManager::ConnectionOwnership::CLIENT_REF, &first).IsOk());
    ASSERT_TRUE(
        manager.FinalizeOutboundConnection(response, UrmaManager::ConnectionOwnership::CLIENT_REF, &second).IsOk());
    ASSERT_EQ(first, second);
    auto lane = std::make_shared<UrmaSendLaneLease>(nullptr, 0, second);
    std::weak_ptr<UrmaConnection> lifetime = second;
    manager.ReleaseClientConnection(info.localAddress.ToString(), first);
    EXPECT_EQ(second->GetUrmaJfrInfo().uniqueInstanceId, info.uniqueInstanceId);
    EXPECT_EQ(manager.urmaConnectionMap_.count(info.localAddress.ToString()), 1u);
    manager.ReleaseClientConnection(info.localAddress.ToString(), second);
    EXPECT_EQ(manager.urmaConnectionMap_.count(info.localAddress.ToString()), 0u);
    EXPECT_EQ(second->GetUrmaJfrInfo().uniqueInstanceId, info.uniqueInstanceId);
    first.reset();
    second.reset();
    EXPECT_FALSE(lifetime.expired());
    EXPECT_EQ(lane->GetConnection()->GetUrmaJfrInfo().uniqueInstanceId, info.uniqueInstanceId);
    lane.reset();
    EXPECT_TRUE(lifetime.expired());
}

TEST_F(UrmaClientOwnershipTest, OwnershipModeRequiresMatchingOwnerArgument)
{
    std::shared_ptr<UrmaConnection> owner;
    EXPECT_EQ(manager.FinalizeOutboundConnection(response, UrmaManager::ConnectionOwnership::CLIENT_REF).GetCode(),
              K_INVALID);
    EXPECT_EQ(
        manager.FinalizeOutboundConnection(response, UrmaManager::ConnectionOwnership::WORKER_OWNED, &owner).GetCode(),
        K_INVALID);
    EXPECT_EQ(owner, nullptr);
}

TEST_F(UrmaClientOwnershipTest, CircuitBrokenReuseDoesNotGrantAnotherOwner)
{
    std::shared_ptr<UrmaConnection> owner;
    ASSERT_TRUE(
        manager.FinalizeOutboundConnection(response, UrmaManager::ConnectionOwnership::CLIENT_REF, &owner).IsOk());
    for (uint32_t i = 0; i < UrmaConnection::MAX_RETIRED_JETTIES; ++i) {
        owner->OnJettyRetired();
    }
    ASSERT_TRUE(owner->IsCircuitBroken());
    std::shared_ptr<UrmaConnection> rejected;
    EXPECT_EQ(
        manager.FinalizeOutboundConnection(response, UrmaManager::ConnectionOwnership::CLIENT_REF, &rejected).GetCode(),
        K_URMA_TRY_AGAIN);
    EXPECT_EQ(rejected, nullptr);
    EXPECT_EQ(owner->clientOwners_.load(), 1U);
    EXPECT_EQ(owner->GetUrmaJfrInfo().uniqueInstanceId, info.uniqueInstanceId);
    EXPECT_EQ(manager.urmaConnectionMap_.count(info.localAddress.ToString()), 1U);
}

TEST_F(UrmaClientOwnershipTest, LastClientOwnerPreservesWorkerConnectionAndInflightLease)
{
    ASSERT_TRUE(manager.FinalizeOutboundConnection(response, UrmaManager::ConnectionOwnership::WORKER_OWNED).IsOk());
    std::shared_ptr<UrmaConnection> owner;
    ASSERT_TRUE(
        manager.FinalizeOutboundConnection(response, UrmaManager::ConnectionOwnership::CLIENT_REF, &owner).IsOk());
    auto lane = std::make_shared<UrmaSendLaneLease>(nullptr, 0, owner);
    std::weak_ptr<UrmaConnection> lifetime = owner;
    manager.ReleaseClientConnection(info.localAddress.ToString(), owner);
    EXPECT_EQ(owner->clientOwners_.load(), 0U);
    EXPECT_EQ(manager.urmaConnectionMap_.count(info.localAddress.ToString()), 1U);
    EXPECT_TRUE(lane->GetConnection()->AcquireInflightSlot(1000).IsOk());
    lane->GetConnection()->ReleaseInflightSlot();
    ASSERT_TRUE(manager.RemoveRemoteDevice(info.localAddress.ToString()).IsOk());
    owner.reset();
    EXPECT_FALSE(lifetime.expired());
    EXPECT_EQ(lane->GetConnection()->GetUrmaJfrInfo().uniqueInstanceId, info.uniqueInstanceId);
    lane.reset();
    EXPECT_TRUE(lifetime.expired());
}

TEST_F(UrmaClientOwnershipTest, ReplacementSharesPeerBudgetButNotClientOwnership)
{
    std::shared_ptr<UrmaConnection> oldOwner;
    ASSERT_TRUE(
        manager.FinalizeOutboundConnection(response, UrmaManager::ConnectionOwnership::CLIENT_REF, &oldOwner).IsOk());
    for (uint32_t i = 0; i < UrmaConnection::MAX_RETIRED_JETTIES; ++i) {
        oldOwner->OnJettyRetired();
    }
    {
        std::lock_guard<bthread::Mutex> lock(oldOwner->peerState_->mutex);
        oldOwner->peerState_->retryAfter = std::chrono::steady_clock::time_point::min();
    }
    using testing::_;
    BINEXPECT_CALL(&UrmaManager::InitializeOutboundConnection, (_, _, _))
        .WillOnce(testing::Invoke([](const UrmaHandshakeReqPb &, const UrmaJfrInfo &remote,
                                    std::shared_ptr<UrmaConnection> &connection) {
            connection = std::make_shared<UrmaConnection>(nullptr, remote);
            return Status::OK();
        }));
    Raii releaseStubs([] { RELEASE_STUBS });
    std::shared_ptr<UrmaConnection> replacement;
    ASSERT_TRUE(manager.FinalizeOutboundConnection(response, UrmaManager::ConnectionOwnership::CLIENT_REF, &replacement)
                    .IsOk());
    ASSERT_NE(oldOwner, replacement);
    EXPECT_EQ(oldOwner->peerState_, replacement->peerState_);
    EXPECT_EQ(replacement->clientOwners_.load(), 1U);
    EXPECT_EQ(replacement->peerState_->retired, UrmaConnection::MAX_RETIRED_JETTIES);
    oldOwner->OnTransferFinished(true);
    EXPECT_EQ(replacement->peerState_->retired, UrmaConnection::MAX_RETIRED_JETTIES);
    ASSERT_TRUE(replacement->AcquireInflightSlot(1000).IsOk());
    EXPECT_EQ(replacement->AcquireInflightSlot(1000).GetCode(), K_URMA_TRY_AGAIN);
    replacement->OnTransferFinished(true);
    replacement->ReleaseInflightSlot();
    EXPECT_EQ(replacement->peerState_->retired, 0U);
    std::weak_ptr<UrmaConnection> oldLifetime = oldOwner;
    manager.ReleaseClientConnection(info.localAddress.ToString(), oldOwner);
    oldOwner.reset();
    EXPECT_TRUE(oldLifetime.expired());
    ASSERT_TRUE(replacement->AcquireInflightSlot(1000).IsOk());
    replacement->ReleaseInflightSlot();
    EXPECT_EQ(manager.urmaConnectionMap_.count(info.localAddress.ToString()), 1U);
    manager.ReleaseClientConnection(info.localAddress.ToString(), replacement);
    EXPECT_EQ(manager.urmaConnectionMap_.count(info.localAddress.ToString()), 0U);
}

TEST_F(UrmaClientOwnershipTest, ClientReplacementDoesNotInheritWorkerOwnership)
{
    ASSERT_TRUE(manager.FinalizeOutboundConnection(response, UrmaManager::ConnectionOwnership::WORKER_OWNED).IsOk());
    TbbUrmaConnectionMap::const_accessor current;
    ASSERT_TRUE(manager.urmaConnectionMap_.find(current, info.localAddress.ToString()));
    auto previous = current->second;
    current.release();
    previous->RequireReconnect();
    {
        std::lock_guard<bthread::Mutex> lock(previous->peerState_->mutex);
        previous->peerState_->retryAfter = std::chrono::steady_clock::time_point::min();
    }
    BINEXPECT_CALL(&UrmaManager::InitializeOutboundConnection, (testing::_, testing::_, testing::_))
        .WillOnce(testing::Invoke(
            [](const UrmaHandshakeReqPb &, const UrmaJfrInfo &remote, std::shared_ptr<UrmaConnection> &connection) {
                connection = std::make_shared<UrmaConnection>(nullptr, remote);
                return Status::OK();
            }));
    Raii releaseStubs([] { RELEASE_STUBS });
    std::shared_ptr<UrmaConnection> replacement;

    ASSERT_TRUE(manager.FinalizeOutboundConnection(response, UrmaManager::ConnectionOwnership::CLIENT_REF, &replacement)
                    .IsOk());

    EXPECT_FALSE(replacement->workerOwned_.load());
    EXPECT_EQ(replacement->clientOwners_.load(), 1U);
    manager.ReleaseClientConnection(info.localAddress.ToString(), replacement);
    EXPECT_EQ(manager.urmaConnectionMap_.count(info.localAddress.ToString()), 0U);
}

TEST_F(UrmaClientOwnershipTest, FailedReplacementPreservesPreviousOwnerAndDoesNotPublishNewOwner)
{
    std::shared_ptr<UrmaConnection> oldOwner;
    ASSERT_TRUE(
        manager.FinalizeOutboundConnection(response, UrmaManager::ConnectionOwnership::CLIENT_REF, &oldOwner).IsOk());
    oldOwner->RequireReconnect();
    {
        std::lock_guard<bthread::Mutex> lock(oldOwner->peerState_->mutex);
        oldOwner->peerState_->retryAfter = std::chrono::steady_clock::time_point::min();
    }
    using testing::_;
    BINEXPECT_CALL(&UrmaManager::InitializeOutboundConnection, (_, _, _))
        .WillOnce(testing::Return(Status(K_URMA_ERROR, "import failed")));
    Raii releaseStubs([] { RELEASE_STUBS });
    std::shared_ptr<UrmaConnection> replacement;
    EXPECT_EQ(manager.FinalizeOutboundConnection(response, UrmaManager::ConnectionOwnership::CLIENT_REF, &replacement)
                  .GetCode(),
              K_URMA_ERROR);
    EXPECT_EQ(replacement, nullptr);
    EXPECT_EQ(oldOwner->clientOwners_.load(), 1U);
    EXPECT_EQ(oldOwner->GetUrmaJfrInfo().uniqueInstanceId, info.uniqueInstanceId);
    TbbUrmaConnectionMap::const_accessor entry;
    ASSERT_TRUE(manager.urmaConnectionMap_.find(entry, info.localAddress.ToString()));
    EXPECT_EQ(entry->second, oldOwner);
    EXPECT_TRUE(oldOwner->IsCircuitBroken());
}

TEST_F(UrmaClientOwnershipTest, NewPeerIncarnationDoesNotInheritOldCircuitCooldown)
{
    std::shared_ptr<UrmaConnection> oldOwner;
    ASSERT_TRUE(
        manager.FinalizeOutboundConnection(response, UrmaManager::ConnectionOwnership::CLIENT_REF, &oldOwner).IsOk());
    for (uint32_t i = 0; i < UrmaConnection::MAX_RETIRED_JETTIES; ++i) {
        oldOwner->OnJettyRetired();
    }
    auto restarted = info;
    restarted.uniqueInstanceId = "restarted-owner";
    response.clear_hand_shake();
    restarted.ToProto(*response.mutable_hand_shake());
    using testing::_;
    BINEXPECT_CALL(&UrmaManager::InitializeOutboundConnection, (_, _, _))
        .WillOnce(testing::Invoke([](const UrmaHandshakeReqPb &, const UrmaJfrInfo &remote,
                                    std::shared_ptr<UrmaConnection> &connection) {
            connection = std::make_shared<UrmaConnection>(nullptr, remote);
            return Status::OK();
        }));
    Raii releaseStubs([] { RELEASE_STUBS });
    std::shared_ptr<UrmaConnection> replacement;
    ASSERT_TRUE(manager.FinalizeOutboundConnection(response, UrmaManager::ConnectionOwnership::CLIENT_REF, &replacement)
                    .IsOk());
    EXPECT_NE(replacement->peerState_, oldOwner->peerState_);
    EXPECT_EQ(replacement->peerState_->retired, 0U);
    EXPECT_FALSE(replacement->IsCircuitBroken());
    manager.ReleaseClientConnection(info.localAddress.ToString(), oldOwner);
    EXPECT_EQ(manager.urmaConnectionMap_.count(info.localAddress.ToString()), 1U);
}

TEST_F(UrmaClientOwnershipTest, OldOwnerCannotRemoveReplacement)
{
    std::shared_ptr<UrmaConnection> oldOwner;
    ASSERT_TRUE(
        manager.FinalizeOutboundConnection(response, UrmaManager::ConnectionOwnership::CLIENT_REF, &oldOwner).IsOk());
    ASSERT_TRUE(manager.RemoveRemoteDevice(info.localAddress.ToString()).IsOk());
    InstallConnection();
    std::shared_ptr<UrmaConnection> newOwner;
    ASSERT_TRUE(
        manager.FinalizeOutboundConnection(response, UrmaManager::ConnectionOwnership::CLIENT_REF, &newOwner).IsOk());
    ASSERT_NE(newOwner, oldOwner);
    Logging::ResetLoggingRuntimeForTest();
    testing::internal::CaptureStderr();
    manager.ReleaseClientConnection(info.localAddress.ToString(), oldOwner);
    const auto logs = testing::internal::GetCapturedStderr();
    EXPECT_NE(logs.find("Skip releasing replaced URMA client connection owner"), std::string::npos);
    EXPECT_EQ(newOwner->GetUrmaJfrInfo().uniqueInstanceId, info.uniqueInstanceId);
    EXPECT_EQ(manager.urmaConnectionMap_.count(info.localAddress.ToString()), 1u);
    manager.ReleaseClientConnection(info.localAddress.ToString(), newOwner);
    EXPECT_EQ(manager.urmaConnectionMap_.count(info.localAddress.ToString()), 0u);
}

TEST_F(UrmaClientOwnershipTest, UnownedAndRepeatedReleaseDoNotUnderflow)
{
    TbbUrmaConnectionMap::const_accessor entry;
    ASSERT_TRUE(manager.urmaConnectionMap_.find(entry, info.localAddress.ToString()));
    auto owner = entry->second;
    entry.release();
    manager.ReleaseClientConnection(info.localAddress.ToString(), nullptr);
    manager.ReleaseClientConnection(info.localAddress.ToString(), owner);
    EXPECT_EQ(static_cast<size_t>(owner->clientOwners_), 0U);
    EXPECT_EQ(manager.urmaConnectionMap_.count(info.localAddress.ToString()), 1U);
    ASSERT_TRUE(
        manager.FinalizeOutboundConnection(response, UrmaManager::ConnectionOwnership::CLIENT_REF, &owner).IsOk());
    manager.ReleaseClientConnection(info.localAddress.ToString(), owner);
    manager.ReleaseClientConnection(info.localAddress.ToString(), owner);
    EXPECT_EQ(static_cast<size_t>(owner->clientOwners_), 0U);
    EXPECT_EQ(manager.urmaConnectionMap_.count(info.localAddress.ToString()), 0U);
}

TEST_F(UrmaClientOwnershipTest, OwnerlessReplacementReclaimsOldGenerationAfterInflightLease)
{
    ASSERT_TRUE(manager.FinalizeOutboundConnection(response, UrmaManager::ConnectionOwnership::WORKER_OWNED).IsOk());
    TbbUrmaConnectionMap::const_accessor entry;
    ASSERT_TRUE(manager.urmaConnectionMap_.find(entry, info.localAddress.ToString()));
    std::weak_ptr<UrmaConnection> oldLifetime = entry->second;
    auto lane = std::make_shared<UrmaSendLaneLease>(nullptr, 0, entry->second);
    entry.release();
    auto restarted = info;
    restarted.uniqueInstanceId = "new-worker-incarnation";
    response.clear_hand_shake();
    restarted.ToProto(*response.mutable_hand_shake());
    using testing::_;
    BINEXPECT_CALL(&UrmaManager::InitializeOutboundConnection, (_, _, _))
        .WillOnce(testing::Invoke([](const UrmaHandshakeReqPb &, const UrmaJfrInfo &remote,
                                    std::shared_ptr<UrmaConnection> &connection) {
            connection = std::make_shared<UrmaConnection>(nullptr, remote);
            return Status::OK();
        }));
    Raii releaseStubs([] { RELEASE_STUBS });
    ASSERT_TRUE(manager.FinalizeOutboundConnection(response, UrmaManager::ConnectionOwnership::WORKER_OWNED).IsOk());
    EXPECT_FALSE(oldLifetime.expired());
    EXPECT_EQ(lane->GetConnection()->GetUrmaJfrInfo().uniqueInstanceId, info.uniqueInstanceId);
    lane.reset();
    EXPECT_TRUE(oldLifetime.expired());
    ASSERT_TRUE(manager.urmaConnectionMap_.find(entry, info.localAddress.ToString()));
    EXPECT_EQ(entry->second->clientOwners_.load(), 0U);
    EXPECT_TRUE(entry->second->workerOwned_.load());
    std::weak_ptr<UrmaConnection> newLifetime = entry->second;
    entry.release();
    ASSERT_TRUE(manager.RemoveRemoteDevice(info.localAddress.ToString()).IsOk());
    EXPECT_TRUE(newLifetime.expired());
}

TEST_F(UrmaClientOwnershipTest, ReuseDoesNotWaitForInflightConnectionReaders)
{
    for (const bool acquireOwner : { false, true }) {
        InstallConnection();
        TbbUrmaConnectionMap::const_accessor inflight;
        ASSERT_TRUE(manager.urmaConnectionMap_.find(inflight, info.localAddress.ToString()));
        const auto expected = inflight->second;
        std::shared_ptr<UrmaConnection> owner;
        std::promise<void> entered;
        auto task = std::async(std::launch::async, [&] {
            entered.set_value();
            return manager.FinalizeOutboundConnection(response,
                                                      acquireOwner ? UrmaManager::ConnectionOwnership::CLIENT_REF
                                                                   : UrmaManager::ConnectionOwnership::WORKER_OWNED,
                                                      acquireOwner ? &owner : nullptr);
        });
        entered.get_future().wait();
        // This bounds a potential deadlock, not a throughput requirement on a loaded test host.
        const auto ready = task.wait_for(std::chrono::seconds(5));
        // Release before asserting so the negative control cannot strand a waiting writer.
        inflight.release();
        EXPECT_EQ(ready, std::future_status::ready);
        EXPECT_TRUE(task.get().IsOk());
        if (acquireOwner) {
            EXPECT_EQ(owner, expected);
            manager.ReleaseClientConnection(info.localAddress.ToString(), owner);
            EXPECT_EQ(manager.urmaConnectionMap_.count(info.localAddress.ToString()), 0u);
        }
    }
}

TEST_F(UrmaClientOwnershipTest, ConcurrentReuseRetainsEveryOwner)
{
    constexpr size_t ownerCount = 8;
    std::vector<std::shared_ptr<UrmaConnection>> owners(ownerCount);
    std::vector<std::future<Status>> tasks;
    for (size_t i = 0; i < ownerCount; ++i) {
        tasks.emplace_back(std::async(std::launch::async, [&, i] {
            return manager.FinalizeOutboundConnection(response, UrmaManager::ConnectionOwnership::CLIENT_REF,
                                                      &owners[i]);
        }));
    }
    for (auto &task : tasks) {
        EXPECT_TRUE(task.get().IsOk());
    }
    for (size_t i = 0; i < ownerCount; ++i) {
        EXPECT_EQ(owners[i], owners[0]);
        manager.ReleaseClientConnection(info.localAddress.ToString(), owners[i]);
        EXPECT_EQ(manager.urmaConnectionMap_.count(info.localAddress.ToString()), i + 1 < ownerCount ? 1u : 0u);
    }
}

TEST_F(UrmaClientOwnershipTest, ReuseStillRejectsMissingHandshake)
{
    UrmaHandshakeRspPb missing;
    std::shared_ptr<UrmaConnection> owner;
    EXPECT_EQ(
        manager.FinalizeOutboundConnection(missing, UrmaManager::ConnectionOwnership::CLIENT_REF, &owner).GetCode(),
        K_INVALID);
    EXPECT_EQ(owner, nullptr);
    EXPECT_EQ(manager.urmaConnectionMap_.count(info.localAddress.ToString()), 1u);
}
}  // namespace
}  // namespace datasystem
#endif
