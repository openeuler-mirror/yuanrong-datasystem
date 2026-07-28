// Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

/**
 * Description: Unit tests for coordinator raft state machine event callbacks.
 */

#include <array>
#include <cstdint>
#include <functional>
#include <optional>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include <braft/raft.h>
#include <butil/endpoint.h>
#include <butil/status.h>

#include "ut/common.h"
#include "datasystem/coordinator/raft/coordinator_raft_state_machine.h"

namespace datasystem::coordinator {
namespace {
constexpr int64_t kLeaderTerm = 42;
constexpr int64_t kConfigurationIndex = 128;
constexpr int kLeaderStopErrorCode = 100;
constexpr char kLeaderStopMessage[] = "leadership transferred";
constexpr int kStateMachineErrorCode = 101;
constexpr char kStateMachineErrorMessage[] = "state machine apply failed";
constexpr char kLoopbackIp[] = "127.0.0.1";
constexpr int kFirstPeerPort = 18'480;
constexpr int kSecondPeerPort = 18'481;
constexpr char kCallbackFailureMarker[] = "Coordinator raft callback failure";
constexpr char kSensitiveExceptionText[] = "injected callback exception detail";
constexpr char kSensitivePayload[] = "private callback payload";

enum class CallbackEntry { LEADER_START, LEADER_STOP, CONFIGURATION_COMMITTED, ERROR, SHUTDOWN };
enum class NonStandardCallbackException { SENTINEL };

constexpr std::array<CallbackEntry, 4> kOrdinaryCallbackEntries{ CallbackEntry::LEADER_START,
                                                                CallbackEntry::LEADER_STOP,
                                                                CallbackEntry::CONFIGURATION_COMMITTED,
                                                                CallbackEntry::SHUTDOWN };

const char *CallbackEntryName(CallbackEntry entry)
{
    switch (entry) {
        case CallbackEntry::LEADER_START:
            return "onLeaderStart";
        case CallbackEntry::LEADER_STOP:
            return "onLeaderStop";
        case CallbackEntry::CONFIGURATION_COMMITTED:
            return "onConfigurationCommitted";
        case CallbackEntry::ERROR:
            return "onError";
        case CallbackEntry::SHUTDOWN:
            return "onShutdown";
    }
    return "unknown";
}

size_t CountOccurrences(const std::string &text, const std::string &needle)
{
    size_t count = 0;
    size_t position = 0;
    while ((position = text.find(needle, position)) != std::string::npos) {
        ++count;
        position += needle.size();
    }
    return count;
}

void SetThrowingOrdinaryCallback(CallbackEntry entry, const std::function<void()> &throwException,
                                 CoordinatorRaftEventCallbacks &callbacks)
{
    switch (entry) {
        case CallbackEntry::LEADER_START:
            callbacks.onLeaderStart = [throwException](int64_t) { throwException(); };
            return;
        case CallbackEntry::LEADER_STOP:
            callbacks.onLeaderStop = [throwException](Status) { throwException(); };
            return;
        case CallbackEntry::CONFIGURATION_COMMITTED:
            callbacks.onConfigurationCommitted = [throwException](std::vector<std::string>, int64_t) {
                throwException();
            };
            return;
        case CallbackEntry::SHUTDOWN:
            callbacks.onShutdown = [throwException] { throwException(); };
            return;
        case CallbackEntry::ERROR:
            FAIL() << "onError is not an ordinary callback";
            return;
    }
}

void DispatchCallback(CallbackEntry entry, CoordinatorRaftStateMachine &stateMachine)
{
    switch (entry) {
        case CallbackEntry::LEADER_START:
            stateMachine.on_leader_start(kLeaderTerm);
            return;
        case CallbackEntry::LEADER_STOP: {
            const butil::Status status(kLeaderStopErrorCode, "%s", kLeaderStopMessage);
            stateMachine.on_leader_stop(status);
            return;
        }
        case CallbackEntry::CONFIGURATION_COMMITTED: {
            const braft::Configuration configuration;
            stateMachine.on_configuration_committed(configuration, kConfigurationIndex);
            return;
        }
        case CallbackEntry::ERROR: {
            braft::Error error;
            error.set_type(braft::ERROR_TYPE_STATE_MACHINE);
            error.status() = butil::Status(kStateMachineErrorCode, "%s", kStateMachineErrorMessage);
            stateMachine.on_error(error);
            return;
        }
        case CallbackEntry::SHUTDOWN:
            stateMachine.on_shutdown();
            return;
    }
}

static_assert(std::is_base_of_v<braft::StateMachine, CoordinatorRaftStateMachine>);

TEST(CoordinatorRaftStateMachineTest, LeaderStartNotifiesOriginalTerm)
{
    std::optional<int64_t> notifiedTerm;
    CoordinatorRaftEventCallbacks callbacks;
    callbacks.onLeaderStart = [&notifiedTerm](int64_t term) { notifiedTerm = term; };
    CoordinatorRaftStateMachine stateMachine(std::move(callbacks));

    stateMachine.on_leader_start(kLeaderTerm);

    ASSERT_TRUE(notifiedTerm.has_value());
    EXPECT_EQ(*notifiedTerm, kLeaderTerm);
}

TEST(CoordinatorRaftStateMachineTest, LeaderStopConvertsButilStatusAndPreservesMessage)
{
    std::optional<Status> notifiedStatus;
    CoordinatorRaftEventCallbacks callbacks;
    callbacks.onLeaderStop = [&notifiedStatus](const Status &status) { notifiedStatus = status; };
    CoordinatorRaftStateMachine stateMachine(std::move(callbacks));
    const butil::Status leaderStopStatus(kLeaderStopErrorCode, "%s", kLeaderStopMessage);

    stateMachine.on_leader_stop(leaderStopStatus);

    ASSERT_TRUE(notifiedStatus.has_value());
    EXPECT_EQ(notifiedStatus->GetCode(), K_RUNTIME_ERROR);
    EXPECT_EQ(notifiedStatus->GetMsg(), kLeaderStopMessage);
}

TEST(CoordinatorRaftStateMachineTest, ErrorConvertsButilStatusAndPreservesMessage)
{
    std::optional<Status> notifiedStatus;
    CoordinatorRaftEventCallbacks callbacks;
    callbacks.onError = [&notifiedStatus](const Status &status) { notifiedStatus = status; };
    CoordinatorRaftStateMachine stateMachine(std::move(callbacks));
    braft::Error error;
    error.set_type(braft::ERROR_TYPE_STATE_MACHINE);
    error.status() = butil::Status(kStateMachineErrorCode, "%s", kStateMachineErrorMessage);

    stateMachine.on_error(error);

    ASSERT_TRUE(notifiedStatus.has_value());
    EXPECT_EQ(notifiedStatus->GetCode(), K_RUNTIME_ERROR);
    EXPECT_EQ(notifiedStatus->GetMsg(), kStateMachineErrorMessage);
}

TEST(CoordinatorRaftStateMachineTest, ConfigurationCommittedNotifiesPeerStringsAndIndex)
{
    std::vector<std::string> notifiedPeers;
    std::optional<int64_t> notifiedIndex;
    CoordinatorRaftEventCallbacks callbacks;
    callbacks.onConfigurationCommitted = [&notifiedPeers, &notifiedIndex](const std::vector<std::string> &peers,
                                                                          int64_t index) {
        notifiedPeers = peers;
        notifiedIndex = index;
    };
    CoordinatorRaftStateMachine stateMachine(std::move(callbacks));

    butil::EndPoint firstEndpoint;
    butil::EndPoint secondEndpoint;
    ASSERT_EQ(butil::str2endpoint(kLoopbackIp, kFirstPeerPort, &firstEndpoint), 0);
    ASSERT_EQ(butil::str2endpoint(kLoopbackIp, kSecondPeerPort, &secondEndpoint), 0);
    const braft::PeerId firstPeer(firstEndpoint);
    const braft::PeerId secondPeer(secondEndpoint);
    braft::Configuration configuration;
    configuration.add_peer(firstPeer);
    configuration.add_peer(secondPeer);

    stateMachine.on_configuration_committed(configuration, kConfigurationIndex);

    EXPECT_EQ(notifiedPeers, (std::vector<std::string>{ firstPeer.to_string(), secondPeer.to_string() }));
    ASSERT_TRUE(notifiedIndex.has_value());
    EXPECT_EQ(*notifiedIndex, kConfigurationIndex);
}

TEST(CoordinatorRaftStateMachineTest, ShutdownNotifiesCallback)
{
    bool notified = false;
    CoordinatorRaftEventCallbacks callbacks;
    callbacks.onShutdown = [&notified] { notified = true; };
    CoordinatorRaftStateMachine stateMachine(std::move(callbacks));

    stateMachine.on_shutdown();

    EXPECT_TRUE(notified);
}

TEST(CoordinatorRaftStateMachineTest, OrdinaryRuntimeErrorsAreReportedAndDetailsAreLogged)
{
    const std::string exceptionMessage = std::string(kSensitiveExceptionText) + ": " + kSensitivePayload;
    for (const auto entry : kOrdinaryCallbackEntries) {
        SCOPED_TRACE(CallbackEntryName(entry));
        int errorCount = 0;
        std::optional<Status> reportedStatus;
        CoordinatorRaftEventCallbacks callbacks;
        SetThrowingOrdinaryCallback(entry, [exceptionMessage] { throw std::runtime_error(exceptionMessage); }, callbacks);
        callbacks.onError = [&errorCount, &reportedStatus](Status status) {
            ++errorCount;
            reportedStatus = std::move(status);
        };
        CoordinatorRaftStateMachine stateMachine(std::move(callbacks));

        testing::internal::CaptureStderr();
        EXPECT_NO_THROW(DispatchCallback(entry, stateMachine));
        const auto capturedStderr = testing::internal::GetCapturedStderr();

        EXPECT_EQ(errorCount, 1);
        ASSERT_TRUE(reportedStatus.has_value());
        EXPECT_EQ(reportedStatus->GetCode(), K_RUNTIME_ERROR);
        EXPECT_EQ(CountOccurrences(reportedStatus->GetMsg(), kCallbackFailureMarker), 1U);
        EXPECT_EQ(reportedStatus->GetMsg().find(kSensitiveExceptionText), std::string::npos);
        EXPECT_EQ(reportedStatus->GetMsg().find(kSensitivePayload), std::string::npos);
        EXPECT_NE(capturedStderr.find(kSensitiveExceptionText), std::string::npos);
        EXPECT_NE(capturedStderr.find(kSensitivePayload), std::string::npos);
    }
}

TEST(CoordinatorRaftStateMachineTest, OrdinaryNonStandardExceptionsAreReportedOnce)
{
    for (const auto entry : kOrdinaryCallbackEntries) {
        SCOPED_TRACE(CallbackEntryName(entry));
        int errorCount = 0;
        std::optional<Status> reportedStatus;
        CoordinatorRaftEventCallbacks callbacks;
        SetThrowingOrdinaryCallback(entry, [] { throw NonStandardCallbackException::SENTINEL; }, callbacks);
        callbacks.onError = [&errorCount, &reportedStatus](Status status) {
            ++errorCount;
            reportedStatus = std::move(status);
        };
        CoordinatorRaftStateMachine stateMachine(std::move(callbacks));

        EXPECT_NO_THROW(DispatchCallback(entry, stateMachine));

        EXPECT_EQ(errorCount, 1);
        ASSERT_TRUE(reportedStatus.has_value());
        EXPECT_EQ(reportedStatus->GetCode(), K_RUNTIME_ERROR);
        EXPECT_EQ(CountOccurrences(reportedStatus->GetMsg(), kCallbackFailureMarker), 1U);
    }
}

TEST(CoordinatorRaftStateMachineTest, OrdinaryFailureWithThrowingOnErrorLogsBothFailures)
{
    const std::string exceptionMessage = std::string(kSensitiveExceptionText) + ": " + kSensitivePayload;
    int errorCount = 0;
    std::optional<Status> reportedStatus;
    CoordinatorRaftEventCallbacks callbacks;
    callbacks.onLeaderStart = [exceptionMessage](int64_t) { throw std::runtime_error(exceptionMessage); };
    callbacks.onError = [&errorCount, &reportedStatus](Status status) {
        ++errorCount;
        reportedStatus = std::move(status);
        throw NonStandardCallbackException::SENTINEL;
    };
    CoordinatorRaftStateMachine stateMachine(std::move(callbacks));

    testing::internal::CaptureStderr();
    EXPECT_NO_THROW(stateMachine.on_leader_start(kLeaderTerm));
    const auto capturedStderr = testing::internal::GetCapturedStderr();

    EXPECT_EQ(errorCount, 1);
    ASSERT_TRUE(reportedStatus.has_value());
    EXPECT_EQ(reportedStatus->GetCode(), K_RUNTIME_ERROR);
    EXPECT_EQ(CountOccurrences(reportedStatus->GetMsg(), kCallbackFailureMarker), 1U);
    EXPECT_EQ(reportedStatus->GetMsg().find(kSensitiveExceptionText), std::string::npos);
    EXPECT_EQ(reportedStatus->GetMsg().find(kSensitivePayload), std::string::npos);
    EXPECT_EQ(CountOccurrences(capturedStderr, kCallbackFailureMarker), 2U);
    EXPECT_NE(capturedStderr.find(kSensitiveExceptionText), std::string::npos);
    EXPECT_NE(capturedStderr.find(kSensitivePayload), std::string::npos);
}

TEST(CoordinatorRaftStateMachineTest, ThrowingOnErrorRuntimeErrorIsSwallowedAndDetailsAreLogged)
{
    const std::string exceptionMessage = std::string(kSensitiveExceptionText) + ": " + kSensitivePayload;
    int errorCount = 0;
    std::optional<Status> receivedStatus;
    CoordinatorRaftEventCallbacks callbacks;
    callbacks.onError = [&errorCount, &receivedStatus, &exceptionMessage](Status status) {
        ++errorCount;
        receivedStatus = std::move(status);
        throw std::runtime_error(exceptionMessage);
    };
    CoordinatorRaftStateMachine stateMachine(std::move(callbacks));

    testing::internal::CaptureStderr();
    EXPECT_NO_THROW(DispatchCallback(CallbackEntry::ERROR, stateMachine));
    const auto capturedStderr = testing::internal::GetCapturedStderr();

    EXPECT_EQ(errorCount, 1);
    ASSERT_TRUE(receivedStatus.has_value());
    EXPECT_EQ(receivedStatus->GetCode(), K_RUNTIME_ERROR);
    EXPECT_EQ(receivedStatus->GetMsg().find(kSensitiveExceptionText), std::string::npos);
    EXPECT_EQ(receivedStatus->GetMsg().find(kSensitivePayload), std::string::npos);
    EXPECT_EQ(CountOccurrences(capturedStderr, kCallbackFailureMarker), 1U);
    EXPECT_NE(capturedStderr.find(kSensitiveExceptionText), std::string::npos);
    EXPECT_NE(capturedStderr.find(kSensitivePayload), std::string::npos);
}

TEST(CoordinatorRaftStateMachineTest, ThrowingOnErrorNonStandardExceptionIsSwallowedWithoutRecursion)
{
    int errorCount = 0;
    CoordinatorRaftEventCallbacks callbacks;
    callbacks.onError = [&errorCount](Status) {
        ++errorCount;
        throw NonStandardCallbackException::SENTINEL;
    };
    CoordinatorRaftStateMachine stateMachine(std::move(callbacks));

    testing::internal::CaptureStderr();
    EXPECT_NO_THROW(DispatchCallback(CallbackEntry::ERROR, stateMachine));
    const auto capturedStderr = testing::internal::GetCapturedStderr();

    EXPECT_EQ(errorCount, 1);
    EXPECT_EQ(CountOccurrences(capturedStderr, kCallbackFailureMarker), 1U);
    EXPECT_EQ(capturedStderr.find(kSensitiveExceptionText), std::string::npos);
    EXPECT_EQ(capturedStderr.find(kSensitivePayload), std::string::npos);
}

TEST(CoordinatorRaftStateMachineTest, EmptyCallbacksAcceptSupportedEvents)
{
    CoordinatorRaftStateMachine stateMachine(CoordinatorRaftEventCallbacks{});
    const butil::Status leaderStopStatus(kLeaderStopErrorCode, "%s", kLeaderStopMessage);
    const braft::Configuration configuration;
    braft::Error error;
    error.set_type(braft::ERROR_TYPE_STATE_MACHINE);
    error.status() = butil::Status(kStateMachineErrorCode, "%s", kStateMachineErrorMessage);

    stateMachine.on_leader_start(kLeaderTerm);
    stateMachine.on_leader_stop(leaderStopStatus);
    stateMachine.on_configuration_committed(configuration, kConfigurationIndex);
    stateMachine.on_error(error);
    stateMachine.on_shutdown();
}
}  // namespace
}  // namespace datasystem::coordinator
