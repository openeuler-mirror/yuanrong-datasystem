/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
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
#include "datasystem/common/coordinator/coordinator_leader_router.h"

#include <algorithm>
#include <chrono>
#include <future>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "datasystem/common/coordinator/coordinator_log.h"
#include "datasystem/common/rpc/brpc_status_util.h"

namespace datasystem {
namespace {
TEST(CoordinatorLogTest, FormatsBinaryUuidAsEightHexCharacters)
{
    std::string id(UUID_SIZE, '\0');
    id[1] = '\n';
    id[2] = static_cast<char>(0x80);
    id[3] = static_cast<char>(0xff);
    const auto original = id;
    EXPECT_EQ(CoordinatorIdLogPrefix(id), "000a80ff");
    EXPECT_EQ(id, original);
    EXPECT_EQ(CoordinatorIdLogPrefix(std::string(UUID_SIZE, '\0')), "00000000");
    EXPECT_EQ(CoordinatorIdLogPrefix(std::string(UUID_SIZE, static_cast<char>(0xff))), "ffffffff");
}

TEST(CoordinatorLogTest, NeverEmitsMalformedBinaryInput)
{
    EXPECT_EQ(CoordinatorIdLogPrefix(""), "");
    EXPECT_EQ(CoordinatorIdLogPrefix(std::string(UUID_SIZE - 1, '\n')), "invalid");
    EXPECT_EQ(CoordinatorIdLogPrefix(std::string(UUID_SIZE + 1, static_cast<char>(0xff))), "invalid");
}

using Router = CoordinatorLeaderRouter;
using State = Router::RpcResponseHeader::State;

Router::RpcResult Response(State state, Status status = Status::OK(), std::string leaderAddress = {},
                           std::string coordinatorId = "coordinator-1", uint64_t leaderTerm = 1)
{
    Router::RpcResponseHeader header;
    header.state = state;
    header.leaderAddress = std::move(leaderAddress);
    header.coordinatorId = std::move(coordinatorId);
    header.leaderTerm = leaderTerm;
    return { std::move(status), std::move(header) };
}

Router::RpcResult TransportError(StatusCode code)
{
    return { Status(code, "injected transport error"), std::nullopt };
}

class CoordinatorLeaderRouterTest : public ::testing::Test {
protected:
    Router::Dependencies Dependencies()
    {
        return {
            .getCandidateSnapshot = [this] {
                ++snapshotCalls;
                if (snapshots.empty()) {
                    return std::vector<std::string>{};
                }
                const auto index = std::min(snapshotCalls - 1, snapshots.size() - 1);
                return snapshots[index];
            },
            .refreshCandidates = [this] { ++refreshCalls; },
            .publishLeaderIdentity =
                [this](const Router::LeaderIdentity &identity) { publishedIdentities.emplace_back(identity); },
            .now = [this] { return now; },
            .wait = [this](std::chrono::milliseconds duration) {
                waits.emplace_back(duration);
                now += duration;
            },
        };
    }

    Router::TimePoint Deadline(std::chrono::milliseconds timeout) const
    {
        return now + timeout;
    }

    Router::TimePoint now{};
    std::vector<std::vector<std::string>> snapshots;
    size_t snapshotCalls{ 0 };
    size_t refreshCalls{ 0 };
    std::vector<std::chrono::milliseconds> waits;
    std::vector<Router::LeaderIdentity> publishedIdentities;
};

TEST_F(CoordinatorLeaderRouterTest, RejectsInvalidExecution)
{
    Router router(Dependencies());
    const auto rpc = [](const HostPort &, std::chrono::milliseconds) { return Response(State::SERVING); };

    EXPECT_EQ(router.Execute({}, Deadline(std::chrono::seconds(1)), std::chrono::milliseconds(10),
                             std::chrono::milliseconds(1))
                  .GetCode(),
              K_INVALID);
    EXPECT_EQ(router.Execute(rpc, now, std::chrono::milliseconds(10), std::chrono::milliseconds(1)).GetCode(),
              K_RPC_DEADLINE_EXCEEDED);
    EXPECT_EQ(router.Execute(rpc, Deadline(std::chrono::seconds(1)), std::chrono::milliseconds::zero(),
                             std::chrono::milliseconds(1))
                  .GetCode(),
              K_INVALID);
    EXPECT_EQ(router.Execute(rpc, Deadline(std::chrono::seconds(1)), std::chrono::milliseconds(10),
                             std::chrono::milliseconds::zero())
                  .GetCode(),
              K_INVALID);
}

TEST_F(CoordinatorLeaderRouterTest, ReturnsNotReadyForEmptyCandidateSnapshot)
{
    Router router(Dependencies());

    const auto status =
        router.Execute([](const HostPort &, std::chrono::milliseconds) { return Response(State::SERVING); },
                       Deadline(std::chrono::seconds(1)), std::chrono::milliseconds(10),
                       std::chrono::milliseconds(1));

    EXPECT_EQ(status.GetCode(), K_NOT_READY);
    EXPECT_FALSE(router.GetLeaderIdentity().has_value());
}

TEST_F(CoordinatorLeaderRouterTest, CachesServingLeaderAndPrefersItOnTheNextCall)
{
    snapshots = { { "127.0.0.1:30001" }, { "127.0.0.1:30002" } };
    Router router(Dependencies());
    std::vector<std::string> attempts;
    auto rpc = [&attempts](const HostPort &address, std::chrono::milliseconds) {
        attempts.emplace_back(address.ToString());
        return Response(State::SERVING);
    };

    ASSERT_TRUE(router.Execute(rpc, Deadline(std::chrono::seconds(1)), std::chrono::milliseconds(10),
                               std::chrono::milliseconds(1))
                    .IsOk());
    ASSERT_TRUE(router.Execute(rpc, Deadline(std::chrono::seconds(1)), std::chrono::milliseconds(10),
                               std::chrono::milliseconds(1))
                    .IsOk());

    EXPECT_EQ(attempts, (std::vector<std::string>{ "127.0.0.1:30001", "127.0.0.1:30001" }));
    ASSERT_TRUE(router.GetLeaderIdentity().has_value());
    EXPECT_EQ(router.GetLeaderIdentity()->address.ToString(), "127.0.0.1:30001");
    ASSERT_EQ(publishedIdentities.size(), 1);
    EXPECT_EQ(publishedIdentities[0].address.ToString(), "127.0.0.1:30001");
    EXPECT_EQ(publishedIdentities[0].coordinatorId, "coordinator-1");
    EXPECT_EQ(publishedIdentities[0].leaderTerm, 1);
    EXPECT_EQ(publishedIdentities[0].routeEpoch, 1);
}

TEST_F(CoordinatorLeaderRouterTest, TriesFollowerRedirectBeforeRemainingCandidates)
{
    snapshots = { { "127.0.0.1:30001", "127.0.0.1:30003" } };
    Router router(Dependencies());
    std::vector<std::string> attempts;

    const auto status = router.Execute(
        [&attempts](const HostPort &address, std::chrono::milliseconds) {
            attempts.emplace_back(address.ToString());
            if (address.ToString() == "127.0.0.1:30001") {
                return Response(State::NOT_LEADER, Status(K_NOT_READY, "injected follower"), "127.0.0.1:30002");
            }
            return Response(State::SERVING);
        },
        Deadline(std::chrono::seconds(1)), std::chrono::milliseconds(10), std::chrono::milliseconds(1));

    EXPECT_TRUE(status.IsOk());
    EXPECT_EQ(attempts, (std::vector<std::string>{ "127.0.0.1:30001", "127.0.0.1:30002" }));
}

TEST_F(CoordinatorLeaderRouterTest, FollowerWithoutRedirectAdvancesToNextCandidate)
{
    snapshots = { { "127.0.0.1:30001", "127.0.0.1:30002" } };
    Router router(Dependencies());
    std::vector<std::string> attempts;

    const auto status = router.Execute(
        [&attempts](const HostPort &address, std::chrono::milliseconds) {
            attempts.emplace_back(address.ToString());
            return address.ToString() == "127.0.0.1:30001"
                       ? Response(State::NOT_LEADER, Status(K_NOT_READY, "injected follower"))
                       : Response(State::SERVING);
        },
        Deadline(std::chrono::seconds(1)), std::chrono::milliseconds(10), std::chrono::milliseconds(1));

    EXPECT_TRUE(status.IsOk());
    EXPECT_EQ(attempts, (std::vector<std::string>{ "127.0.0.1:30001", "127.0.0.1:30002" }));
}

TEST_F(CoordinatorLeaderRouterTest, ReturnsServingLeaderBusinessErrorWithoutFailover)
{
    snapshots = { { "127.0.0.1:30001", "127.0.0.1:30002" } };
    Router router(Dependencies());
    size_t attempts = 0;

    const auto status = router.Execute(
        [&attempts](const HostPort &, std::chrono::milliseconds) {
            ++attempts;
            return Response(State::SERVING, Status(K_INVALID, "injected business error"));
        },
        Deadline(std::chrono::seconds(1)), std::chrono::milliseconds(10), std::chrono::milliseconds(1));

    EXPECT_EQ(status.GetCode(), K_INVALID);
    EXPECT_EQ(attempts, 1);
}

TEST_F(CoordinatorLeaderRouterTest, ReturnsUnspecifiedProtocolErrorWithoutRetry)
{
    snapshots = { { "127.0.0.1:30001", "127.0.0.1:30002" } };
    Router router(Dependencies());
    size_t attempts = 0;

    const auto status = router.Execute(
        [&attempts](const HostPort &, std::chrono::milliseconds) {
            ++attempts;
            return Response(State::UNSPECIFIED, Status(K_INVALID, "injected protocol error"));
        },
        Deadline(std::chrono::seconds(1)), std::chrono::milliseconds(10), std::chrono::milliseconds(1));

    EXPECT_EQ(status.GetCode(), K_INVALID);
    EXPECT_EQ(attempts, 1);
    EXPECT_EQ(refreshCalls, 0);
}

TEST_F(CoordinatorLeaderRouterTest, RefreshesAndRetriesTransportFailuresUntilDeadline)
{
    snapshots = { { "127.0.0.1:30001", "127.0.0.1:30002" } };
    Router router(Dependencies());
    size_t attempts = 0;
    const auto deadline = Deadline(std::chrono::milliseconds(10));
    const auto status = router.Execute(
        [&](const HostPort &, std::chrono::milliseconds timeout) {
            ++attempts;
            EXPECT_LE(now + timeout, deadline);
            return TransportError(K_RPC_PEER_DEAD);
        }, deadline, std::chrono::milliseconds(10), std::chrono::milliseconds(1));
    EXPECT_EQ(status.GetCode(), K_RPC_PEER_DEAD);
    EXPECT_GT(attempts, 3U);
    EXPECT_EQ(now, deadline);
    EXPECT_EQ(refreshCalls, waits.size());
    EXPECT_FALSE(waits.empty());
}

TEST_F(CoordinatorLeaderRouterTest, ReadsFreshSnapshotForNextRoundAfterCoordinatorResponse)
{
    snapshots = { { "127.0.0.1:30001" }, { "127.0.0.1:30002" } };
    Router router(Dependencies());
    std::vector<std::string> attempts;

    const auto status = router.Execute(
        [&attempts](const HostPort &address, std::chrono::milliseconds) {
            attempts.emplace_back(address.ToString());
            return address.ToString() == "127.0.0.1:30001"
                       ? Response(State::NOT_LEADER, Status(K_NOT_READY, "injected no leader"))
                       : Response(State::SERVING);
        },
        Deadline(std::chrono::seconds(1)), std::chrono::milliseconds(10), std::chrono::milliseconds(1));

    EXPECT_TRUE(status.IsOk());
    EXPECT_EQ(attempts, (std::vector<std::string>{ "127.0.0.1:30001", "127.0.0.1:30002" }));
    EXPECT_EQ(snapshotCalls, 2);
    EXPECT_EQ(refreshCalls, 1);
    EXPECT_EQ(waits, (std::vector<std::chrono::milliseconds>{ std::chrono::milliseconds(1) }));
}

TEST_F(CoordinatorLeaderRouterTest, RedirectsBackToPreviouslyFailedLeader)
{
    snapshots = { { "127.0.0.1:30001", "127.0.0.1:30002" } };
    Router router(Dependencies());
    std::vector<std::string> attempts;
    const auto status = router.Execute(
        [&](const HostPort &address, std::chrono::milliseconds) {
            attempts.push_back(address.ToString());
            if (attempts.size() == 1) {
                return TransportError(K_RPC_PEER_DEAD);
            }
            return address.Port() == 30002
                ? Response(State::NOT_LEADER, Status::OK(), "127.0.0.1:30001")
                : Response(State::SERVING);
        }, Deadline(std::chrono::milliseconds(20)), std::chrono::milliseconds(10), std::chrono::milliseconds(1));
    EXPECT_TRUE(status.IsOk());
    EXPECT_EQ(attempts, (std::vector<std::string>{ "127.0.0.1:30001", "127.0.0.1:30002", "127.0.0.1:30001" }));
}

TEST_F(CoordinatorLeaderRouterTest, RetriesFollowerCandidateWithinOriginalDeadline)
{
    snapshots = { { "127.0.0.1:30001" } };
    Router router(Dependencies());
    size_t attempts = 0;
    const auto status = router.Execute(
        [&](const HostPort &, std::chrono::milliseconds) {
            return ++attempts < 5 ? Response(State::NOT_LEADER, Status(K_NOT_READY, "election pending"))
                                  : Response(State::SERVING);
        }, Deadline(std::chrono::milliseconds(20)), std::chrono::milliseconds(10), std::chrono::milliseconds(1));
    EXPECT_TRUE(status.IsOk());
    EXPECT_EQ(attempts, 5U);
    EXPECT_EQ(waits.size(), 4U);
}

TEST_F(CoordinatorLeaderRouterTest, RetriesCandidatesAfterStaticDiscoveryRefresh)
{
    snapshots = { { "127.0.0.1:30001", "127.0.0.1:30002", "127.0.0.1:30003", "127.0.0.1:30004",
                    "127.0.0.1:30005" } };
    Router router(Dependencies());
    std::unordered_map<std::string, size_t> dialCount;

    const auto status = router.Execute(
        [&dialCount](const HostPort &address, std::chrono::milliseconds) {
            ++dialCount[address.ToString()];
            const auto &value = address.ToString();
            if (value == "127.0.0.1:30001" || value == "127.0.0.1:30002" || value == "127.0.0.1:30003") {
                return TransportError(K_RPC_PEER_DEAD);
            }
            return Response(State::NOT_LEADER, Status(K_NOT_READY, "injected follower"));
        },
        Deadline(std::chrono::milliseconds(3'000)), std::chrono::milliseconds(10),
        std::chrono::milliseconds(1));

    EXPECT_EQ(status.GetCode(), K_NOT_READY);
    ASSERT_EQ(dialCount.size(), 5UL);
    size_t totalDials = 0;
    for (const auto &[address, count] : dialCount) {
        EXPECT_GT(count, 1UL) << address;
        totalDials += count;
    }
    EXPECT_GT(totalDials, 5UL);
    EXPECT_GE(snapshotCalls, 2UL);
}

TEST_F(CoordinatorLeaderRouterTest, TriesNextCandidateWhenRecoveringLeaderBecomesUnreachable)
{
    snapshots = { { "127.0.0.1:30001", "127.0.0.1:30002" } };
    Router router(Dependencies());
    std::vector<std::string> attempts;

    const auto status = router.Execute(
        [&attempts](const HostPort &address, std::chrono::milliseconds) {
            attempts.emplace_back(address.ToString());
            if (attempts.size() == 1) {
                return Response(State::RECOVERING, Status(K_NOT_READY, "injected recovery"));
            }
            if (address.ToString() == "127.0.0.1:30001") {
                return TransportError(K_RPC_PEER_DEAD);
            }
            return Response(State::SERVING);
        },
        Deadline(std::chrono::milliseconds(10)), std::chrono::milliseconds(10), std::chrono::milliseconds(1));

    EXPECT_TRUE(status.IsOk());
    EXPECT_EQ(attempts, (std::vector<std::string>{ "127.0.0.1:30001", "127.0.0.1:30001",
                                                  "127.0.0.1:30002" }));
    EXPECT_EQ(snapshotCalls, 1);
}

TEST_F(CoordinatorLeaderRouterTest, FollowsRedirectWhenRecoveringCoordinatorBecomesFollower)
{
    snapshots = { { "127.0.0.1:30001" } };
    Router router(Dependencies());
    std::vector<std::string> attempts;

    const auto status = router.Execute(
        [&attempts](const HostPort &address, std::chrono::milliseconds) {
            attempts.emplace_back(address.ToString());
            if (attempts.size() == 1) {
                return Response(State::RECOVERING, Status(K_NOT_READY, "injected recovery"));
            }
            if (address.ToString() == "127.0.0.1:30001") {
                return Response(State::NOT_LEADER, Status(K_NOT_READY, "injected follower"), "127.0.0.1:30002");
            }
            return Response(State::SERVING);
        },
        Deadline(std::chrono::milliseconds(10)), std::chrono::milliseconds(10), std::chrono::milliseconds(1));

    EXPECT_TRUE(status.IsOk());
    EXPECT_EQ(attempts, (std::vector<std::string>{ "127.0.0.1:30001", "127.0.0.1:30001",
                                                  "127.0.0.1:30002" }));
}

TEST_F(CoordinatorLeaderRouterTest, BoundsEveryAttemptByMaximumTimeoutAndRemainingDeadline)
{
    snapshots = { { "127.0.0.1:30001", "127.0.0.1:30002" } };
    Router router(Dependencies());
    std::vector<std::chrono::milliseconds> timeouts;

    const auto status = router.Execute(
        [this, &timeouts](const HostPort &address, std::chrono::milliseconds timeout) {
            timeouts.emplace_back(timeout);
            if (address.ToString() == "127.0.0.1:30001") {
                now += timeout;
                return TransportError(K_RPC_PEER_DEAD);
            }
            return Response(State::SERVING);
        },
        Deadline(std::chrono::milliseconds(15)), std::chrono::milliseconds(10), std::chrono::milliseconds(1));

    EXPECT_TRUE(status.IsOk());
    EXPECT_EQ(timeouts, (std::vector<std::chrono::milliseconds>{ std::chrono::milliseconds(7),
                                                                 std::chrono::milliseconds(8) }));
}

TEST_F(CoordinatorLeaderRouterTest, RecoveringLeaderRetainsApplicationStatusAtDeadline)
{
    snapshots = { { "127.0.0.1:30001" } };
    Router router(Dependencies());
    std::vector<std::chrono::milliseconds> timeouts;

    const auto status = router.Execute(
        [&timeouts](const HostPort &, std::chrono::milliseconds timeout) {
            timeouts.emplace_back(timeout);
            return Response(State::RECOVERING, Status(K_NOT_READY, "injected recovery"));
        },
        Deadline(std::chrono::milliseconds(25)), std::chrono::milliseconds(10), std::chrono::milliseconds(10));

    EXPECT_EQ(status.GetCode(), K_NOT_READY);
    EXPECT_EQ(timeouts, (std::vector<std::chrono::milliseconds>{ std::chrono::milliseconds(10),
                                                                 std::chrono::milliseconds(10),
                                                                 std::chrono::milliseconds(5) }));
    ASSERT_TRUE(router.GetLeaderIdentity().has_value());
    EXPECT_EQ(router.GetLeaderIdentity()->address.ToString(), "127.0.0.1:30001");
    EXPECT_EQ(publishedIdentities.size(), 1);
}

TEST_F(CoordinatorLeaderRouterTest, RecoveryEvidenceSurvivesFinalTransportTimeout)
{
    snapshots = { { "127.0.0.1:30001", "127.0.0.1:30002", "127.0.0.1:30003" } };
    Router router(Dependencies());
    constexpr auto budget = std::chrono::milliseconds(3'000);
    constexpr auto retryInterval = std::chrono::milliseconds(100);
    constexpr auto firstResponseElapsed = std::chrono::milliseconds(98);
    constexpr auto timerOvershoot = std::chrono::milliseconds(1);
    size_t calls = 0;
    std::vector<std::chrono::milliseconds> timeouts;
    const auto status = router.Execute(
        [&](const HostPort &, std::chrono::milliseconds timeout) {
            timeouts.emplace_back(timeout);
            ++calls;
            if (timeout > timerOvershoot) {
                if (calls == 1) {
                    now += firstResponseElapsed;
                }
                return Response(State::RECOVERING, Status(K_NOT_READY, "injected recovery"));
            }
            now += timeout + timerOvershoot;
            return TransportError(K_RPC_DEADLINE_EXCEEDED);
        },
        Deadline(budget), budget, retryInterval);

    constexpr size_t expectedCalls = 30;
    ASSERT_EQ(calls, expectedCalls);
    ASSERT_EQ(timeouts.back(), std::chrono::milliseconds(1));
    ASSERT_TRUE(router.GetLeaderIdentity().has_value());
    EXPECT_EQ(status.GetCode(), K_NOT_READY) << status.ToString();
}

TEST_F(CoordinatorLeaderRouterTest, RecoveryControlAcceptsRecoveringLeader)
{
    snapshots = { { "127.0.0.1:30001", "127.0.0.1:30002" } };
    Router router(Dependencies());
    size_t attempts = 0;

    const auto status = router.Execute(
        [&attempts](const HostPort &, std::chrono::milliseconds) {
            ++attempts;
            return Response(State::RECOVERING, Status::OK(), {}, "coordinator-recovering", 7);
        },
        Deadline(std::chrono::seconds(1)), std::chrono::milliseconds(10), std::chrono::milliseconds(1), true);

    EXPECT_TRUE(status.IsOk());
    EXPECT_EQ(attempts, 1);
    EXPECT_TRUE(waits.empty());
    ASSERT_EQ(publishedIdentities.size(), 1);
    EXPECT_EQ(publishedIdentities[0].address.ToString(), "127.0.0.1:30001");
    EXPECT_EQ(publishedIdentities[0].coordinatorId, "coordinator-recovering");
    EXPECT_EQ(publishedIdentities[0].leaderTerm, 7);
}

TEST_F(CoordinatorLeaderRouterTest, PublishesEachLeaderIdentityOnce)
{
    snapshots = { { "127.0.0.1:30001" } };
    Router router(Dependencies());
    size_t attempts = 0;

    const auto status = router.Execute(
        [&attempts](const HostPort &, std::chrono::milliseconds) {
            ++attempts;
            if (attempts == 1) {
                return Response(State::RECOVERING, Status(K_NOT_READY, "injected recovery"), {}, "coordinator-1", 1);
            }
            return Response(State::SERVING, Status::OK(), {}, "coordinator-1", 1);
        },
        Deadline(std::chrono::seconds(1)), std::chrono::milliseconds(10), std::chrono::milliseconds(1));

    EXPECT_TRUE(status.IsOk());
    ASSERT_EQ(publishedIdentities.size(), 1);
    EXPECT_EQ(publishedIdentities[0].routeEpoch, 1);
}

TEST_F(CoordinatorLeaderRouterTest, PublishesChangedLeaderWithIncreasingRouteEpoch)
{
    snapshots = { { "127.0.0.1:30001" }, { "127.0.0.1:30002" } };
    Router router(Dependencies());

    ASSERT_TRUE(router
                    .Execute(
                        [](const HostPort &, std::chrono::milliseconds) {
                            return Response(State::SERVING, Status::OK(), {}, "coordinator-1", 1);
                        },
                        Deadline(std::chrono::seconds(1)), std::chrono::milliseconds(10),
                        std::chrono::milliseconds(1))
                    .IsOk());
    ASSERT_TRUE(router
                    .Execute(
                        [](const HostPort &address, std::chrono::milliseconds) {
                            if (address.ToString() == "127.0.0.1:30001") {
                                return TransportError(K_RPC_PEER_DEAD);
                            }
                            return Response(State::SERVING, Status::OK(), {}, "coordinator-2", 2);
                        },
                        Deadline(std::chrono::seconds(1)), std::chrono::milliseconds(10),
                        std::chrono::milliseconds(1))
                    .IsOk());

    ASSERT_EQ(publishedIdentities.size(), 2);
    EXPECT_EQ(publishedIdentities[0].routeEpoch, 1);
    EXPECT_EQ(publishedIdentities[1].address.ToString(), "127.0.0.1:30002");
    EXPECT_EQ(publishedIdentities[1].coordinatorId, "coordinator-2");
    EXPECT_EQ(publishedIdentities[1].leaderTerm, 2);
    EXPECT_EQ(publishedIdentities[1].routeEpoch, 2);
}

TEST_F(CoordinatorLeaderRouterTest, RejectsLowerTermFromSameCoordinatorGeneration)
{
    snapshots = { { "127.0.0.1:30001" }, { "127.0.0.1:30002" } };
    Router router(Dependencies());

    ASSERT_TRUE(router
                    .Execute(
                        [](const HostPort &, std::chrono::milliseconds) {
                            return Response(State::SERVING, Status::OK(), {}, "coordinator-2", 2);
                        },
                        Deadline(std::chrono::seconds(1)), std::chrono::milliseconds(10),
                        std::chrono::milliseconds(1))
                    .IsOk());
    const auto status = router.Execute(
        [](const HostPort &address, std::chrono::milliseconds) {
            return address.ToString() == "127.0.0.1:30001"
                       ? Response(State::SERVING, Status::OK(), {}, "coordinator-2", 1)
                       : TransportError(K_RPC_PEER_DEAD);
        },
        Deadline(std::chrono::seconds(1)), std::chrono::milliseconds(10), std::chrono::milliseconds(1));

    EXPECT_EQ(status.GetCode(), K_RPC_PEER_DEAD);
    ASSERT_EQ(publishedIdentities.size(), 1);
    EXPECT_EQ(publishedIdentities[0].coordinatorId, "coordinator-2");
}

TEST_F(CoordinatorLeaderRouterTest, AcceptsLowerTermFromNewCoordinatorGeneration)
{
    snapshots = { { "127.0.0.1:30001" }, { "127.0.0.1:30002" } };
    Router router(Dependencies());

    ASSERT_TRUE(router
                    .Execute(
                        [](const HostPort &, std::chrono::milliseconds) {
                            return Response(State::SERVING, Status::OK(), {}, "coordinator-1", 94);
                        },
                        Deadline(std::chrono::seconds(1)), std::chrono::milliseconds(10),
                        std::chrono::milliseconds(1))
                    .IsOk());
    const auto status = router.Execute(
        [](const HostPort &address, std::chrono::milliseconds) {
            return address.ToString() == "127.0.0.1:30001"
                       ? TransportError(K_RPC_PEER_DEAD)
                       : Response(State::SERVING, Status::OK(), {}, "coordinator-2", 45);
        },
        Deadline(std::chrono::seconds(1)), std::chrono::milliseconds(10), std::chrono::milliseconds(1));

    EXPECT_TRUE(status.IsOk());
    ASSERT_EQ(publishedIdentities.size(), 2);
    EXPECT_EQ(publishedIdentities[1].address.ToString(), "127.0.0.1:30002");
    EXPECT_EQ(publishedIdentities[1].coordinatorId, "coordinator-2");
    EXPECT_EQ(publishedIdentities[1].leaderTerm, 45);
    EXPECT_EQ(publishedIdentities[1].routeEpoch, 2);
}

TEST_F(CoordinatorLeaderRouterTest, RejectedResponseCannotReturnSuccessAtDeadline)
{
    snapshots = { { "127.0.0.1:30001" } };
    Router router(Dependencies());
    auto execute = [&](uint64_t term) {
        return router.Execute(
            [term](const HostPort &, std::chrono::milliseconds) {
                return Response(State::SERVING, Status::OK(), {}, "coordinator", term);
            },
            Deadline(std::chrono::milliseconds(3)), std::chrono::milliseconds(1),
            std::chrono::milliseconds(1));
    };
    ASSERT_TRUE(execute(2).IsOk());
    const auto status = execute(1);
    EXPECT_EQ(status.GetCode(), K_TRY_AGAIN);
    EXPECT_NE(status.GetMsg().find("127.0.0.1:30001"), std::string::npos);
    ASSERT_EQ(publishedIdentities.size(), 1);
    EXPECT_EQ(router.GetLeaderIdentity()->leaderTerm, 2);
}

TEST_F(CoordinatorLeaderRouterTest, RecoveringSuccessAtDeadlineIncludesAddress)
{
    snapshots = { { "127.0.0.1:30001" } };
    Router router(Dependencies());
    const auto status = router.Execute(
        [](const HostPort &, std::chrono::milliseconds) { return Response(State::RECOVERING); },
        Deadline(std::chrono::milliseconds(3)), std::chrono::milliseconds(1),
        std::chrono::milliseconds(1));
    EXPECT_EQ(status.GetCode(), K_NOT_READY);
    EXPECT_NE(status.GetMsg().find("127.0.0.1:30001"), std::string::npos);
}

TEST_F(CoordinatorLeaderRouterTest, RecoveringRetryReservesBudgetForAlternative)
{
    snapshots = { { "127.0.0.1:30001", "127.0.0.1:30002" } };
    Router router(Dependencies());
    std::vector<std::chrono::milliseconds> budgets;
    std::vector<std::string> attempts;
    const auto status = router.Execute(
        [&](const HostPort &address, std::chrono::milliseconds timeout) {
            attempts.push_back(address.ToString());
            budgets.push_back(timeout);
            if (attempts.size() == 1) {
                return Response(State::RECOVERING, Status(K_NOT_READY, "recovering"));
            }
            if (address.ToString() == "127.0.0.1:30001") {
                now += timeout;
                return TransportError(K_RPC_DEADLINE_EXCEEDED);
            }
            return Response(State::SERVING);
        },
        Deadline(std::chrono::milliseconds(10)), std::chrono::milliseconds(10),
        std::chrono::milliseconds(1));
    EXPECT_TRUE(status.IsOk());
    EXPECT_EQ(attempts, (std::vector<std::string>{
                           "127.0.0.1:30001", "127.0.0.1:30001", "127.0.0.1:30002" }));
    EXPECT_EQ(budgets, (std::vector<std::chrono::milliseconds>{
                          std::chrono::milliseconds(5), std::chrono::milliseconds(4),
                          std::chrono::milliseconds(5) }));
}

TEST_F(CoordinatorLeaderRouterTest, LateResponseRetriesCurrentLeaderWithinSameCall)
{
    snapshots = { { "127.0.0.1:30001" }, { "127.0.0.1:30002" } };
    Router router(Dependencies());
    const auto deadline = Deadline(std::chrono::seconds(1));
    auto execute = [&](const Router::RpcCall &rpc) {
        return router.Execute(rpc, deadline, std::chrono::milliseconds(10), std::chrono::milliseconds(1));
    };
    ASSERT_TRUE(execute([](const HostPort &, std::chrono::milliseconds) {
        return Response(State::SERVING, Status::OK(), {}, "old", 94);
    }).IsOk());
    std::promise<void> entered;
    std::promise<void> release;
    auto released = release.get_future();
    std::vector<std::string> attempts;
    auto oldCall = std::async(std::launch::async, [&] {
        return execute([&](const HostPort &address, std::chrono::milliseconds) {
            attempts.push_back(address.ToString());
            if (attempts.size() == 1) {
                entered.set_value();
                released.wait();
                return Response(State::SERVING, Status::OK(), {}, "old", 94);
            }
            return Response(State::SERVING, Status::OK(), {}, "new", 45);
        });
    });
    entered.get_future().wait();
    const auto switched = execute([](const HostPort &address, std::chrono::milliseconds) {
        return address.ToString() == "127.0.0.1:30001"
                   ? TransportError(K_RPC_PEER_DEAD)
                   : Response(State::SERVING, Status::OK(), {}, "new", 45);
    });
    release.set_value();
    EXPECT_TRUE(switched.IsOk());
    EXPECT_TRUE(oldCall.get().IsOk());
    EXPECT_EQ(attempts, (std::vector<std::string>{ "127.0.0.1:30001", "127.0.0.1:30002" }));
    ASSERT_EQ(publishedIdentities.size(), 2);
    EXPECT_EQ(router.GetLeaderIdentity()->coordinatorId, "new");
    EXPECT_EQ(router.GetLeaderIdentity()->routeEpoch, 2);
    EXPECT_EQ(refreshCalls, 0);
    EXPECT_TRUE(waits.empty());
}

TEST_F(CoordinatorLeaderRouterTest, RedirectDuplicateDoesNotDiluteRecoveringBudget)
{
    snapshots = { { "127.0.0.1:30001", "127.0.0.1:30002", "127.0.0.1:30003" } };
    Router router(Dependencies());
    size_t calls = 0;
    std::vector<std::chrono::milliseconds> budgets;
    const auto status = router.Execute(
        [&](const HostPort &, std::chrono::milliseconds timeout) {
            budgets.push_back(timeout);
            ++calls;
            if (calls == 1) {
                return Response(State::NOT_LEADER, Status(K_NOT_READY, "redirect"), "127.0.0.1:30002");
            }
            if (calls == 2) {
                return Response(State::RECOVERING, Status(K_NOT_READY, "recovering"));
            }
            return Response(State::SERVING);
        },
        Deadline(std::chrono::milliseconds(12)), std::chrono::milliseconds(12),
        std::chrono::milliseconds(1));
    EXPECT_TRUE(status.IsOk());
    EXPECT_EQ(budgets, (std::vector<std::chrono::milliseconds>{
                          std::chrono::milliseconds(4), std::chrono::milliseconds(5),
                          std::chrono::milliseconds(5) }));
}

TEST_F(CoordinatorLeaderRouterTest, ConcurrentObservationOfSameIdentityDoesNotRetry)
{
    snapshots = { { "127.0.0.1:30001" } };
    Router router(Dependencies());
    size_t calls = 0;
    const auto status = router.Execute(
        [&](const HostPort &, std::chrono::milliseconds) {
            ++calls;
            EXPECT_TRUE(router.Execute(
                [](const HostPort &, std::chrono::milliseconds) { return Response(State::SERVING); },
                Deadline(std::chrono::seconds(1)), std::chrono::milliseconds(10),
                std::chrono::milliseconds(1)).IsOk());
            return Response(State::SERVING);
        },
        Deadline(std::chrono::seconds(1)), std::chrono::milliseconds(10), std::chrono::milliseconds(1));
    EXPECT_TRUE(status.IsOk());
    EXPECT_EQ(calls, 1);
    EXPECT_EQ(publishedIdentities.size(), 1);
    EXPECT_TRUE(waits.empty());
}

TEST_F(CoordinatorLeaderRouterTest, HeaderlessMembershipErrorReturnsWithoutTryingFollowers)
{
    snapshots = { { "127.0.0.1:30001", "127.0.0.1:30002", "127.0.0.1:30003" } };
    for (auto code : { K_TRY_AGAIN, K_NOT_FOUND }) {
        Router router(Dependencies());
        size_t calls = 0;
        Status error(code, "membership incarnation is stale");
        error.WithExtra(kBrpcServerRespondedExtra);
        const auto status = router.Execute(
            [&](const HostPort &address, std::chrono::milliseconds) -> Router::RpcResult {
                ++calls;
                if (address.ToString() == "127.0.0.1:30001") {
                    return { error, std::nullopt };
                }
                return Response(State::NOT_LEADER, Status::OK(), "127.0.0.1:30001");
            },
            Deadline(std::chrono::seconds(3)), std::chrono::seconds(3),
            std::chrono::milliseconds(1), true);
        EXPECT_EQ(status.GetCode(), code);
        EXPECT_EQ(status.GetMsg(), error.GetMsg());
        EXPECT_TRUE(IsBrpcServerApplicationError(status));
        EXPECT_EQ(calls, 1);
        EXPECT_TRUE(waits.empty());
    }
}

TEST_F(CoordinatorLeaderRouterTest, HeaderlessServerNotReadyStillTriesOtherCandidates)
{
    snapshots = { { "127.0.0.1:30001", "127.0.0.1:30002" } };
    Router router(Dependencies());
    size_t calls = 0;
    const auto status = router.Execute(
        [&](const HostPort &address, std::chrono::milliseconds) -> Router::RpcResult {
            ++calls;
            if (address.ToString() == "127.0.0.1:30001") {
                Status error(K_NOT_READY, "coordinator is recovering");
                error.WithExtra(kBrpcServerRespondedExtra);
                return { error, std::nullopt };
            }
            return Response(State::SERVING);
        },
        Deadline(std::chrono::seconds(3)), std::chrono::seconds(3), std::chrono::milliseconds(1));
    EXPECT_TRUE(status.IsOk());
    EXPECT_EQ(calls, 2);
}

TEST_F(CoordinatorLeaderRouterTest, HeaderlessBusinessErrorWithFollowersRetainsNotReady)
{
    snapshots = { { "127.0.0.1:30001", "127.0.0.1:30002", "127.0.0.1:30003" } };
    Router router(Dependencies());
    size_t calls = 0;
    const auto status = router.Execute(
        [&](const HostPort &address, std::chrono::milliseconds) -> Router::RpcResult {
            ++calls;
            now += std::chrono::microseconds(100);
            if (address.ToString() == "127.0.0.1:30001") {
                return { Status(K_NOT_READY, "cluster topology is recovering"), std::nullopt };
            }
            return Response(State::NOT_LEADER, Status::OK(), "127.0.0.1:30001");
        },
        Deadline(std::chrono::milliseconds(3)), std::chrono::milliseconds(3),
        std::chrono::milliseconds(1));
    EXPECT_EQ(status.GetCode(), K_NOT_READY);
    EXPECT_GE(calls, 3U);
}

TEST_F(CoordinatorLeaderRouterTest, UnattemptedCandidateCannotOverwriteAcceptedResponse)
{
    snapshots = { { "127.0.0.1:30001", "127.0.0.1:30002" } };
    for (const auto elapsed : { std::chrono::microseconds(1500), std::chrono::microseconds(2000) }) {
        Router router(Dependencies());
        size_t calls = 0;
        const auto status = router.Execute(
            [&](const HostPort &, std::chrono::milliseconds) {
                ++calls;
                now += elapsed;
                return Response(State::NOT_LEADER, Status(K_NOT_READY, "leader election pending"));
            },
            Deadline(std::chrono::milliseconds(2)), std::chrono::milliseconds(2),
            std::chrono::milliseconds(1));
        EXPECT_EQ(status.GetCode(), K_NOT_READY);
        EXPECT_EQ(status.GetMsg(), "leader election pending");
        EXPECT_EQ(calls, 1);
    }
}

TEST_F(CoordinatorLeaderRouterTest, LaterCandidateTransportFailurePreservesAcceptedResponse)
{
    snapshots = { { "127.0.0.1:30001", "127.0.0.1:30002" } };
    Router router(Dependencies());
    size_t calls = 0;
    const auto status = router.Execute(
        [&](const HostPort &, std::chrono::milliseconds) {
            if (++calls == 1) {
                return Response(State::NOT_LEADER, Status(K_NOT_READY, "leader election pending"));
            }
            return TransportError(K_RPC_PEER_DEAD);
        },
        Deadline(std::chrono::milliseconds(3)), std::chrono::milliseconds(1),
        std::chrono::milliseconds(1));
    EXPECT_EQ(status.GetCode(), K_NOT_READY);
    EXPECT_EQ(status.GetMsg(), "leader election pending");
    EXPECT_GT(calls, 2U);
}

TEST_F(CoordinatorLeaderRouterTest, DeadlineWithoutAcceptedResponseRemainsTimeout)
{
    snapshots = { { "127.0.0.1:30001", "127.0.0.1:30002" } };
    Router router(Dependencies());
    size_t calls = 0;
    const auto status = router.Execute(
        [&](const HostPort &, std::chrono::milliseconds) {
            ++calls;
            now += std::chrono::milliseconds(2);
            return TransportError(K_RPC_DEADLINE_EXCEEDED);
        },
        Deadline(std::chrono::milliseconds(2)), std::chrono::milliseconds(2),
        std::chrono::milliseconds(1));
    EXPECT_EQ(status.GetCode(), K_RPC_DEADLINE_EXCEEDED);
    EXPECT_EQ(calls, 1);
}

TEST_F(CoordinatorLeaderRouterTest, HeaderlessNotReadyDoesNotSkipHealthyCandidate)
{
    snapshots = { { "127.0.0.1:30001", "127.0.0.1:30002" } };
    Router router(Dependencies());
    ASSERT_TRUE(router.Execute(
        [](const HostPort &, std::chrono::milliseconds) { return Response(State::SERVING); },
        Deadline(std::chrono::milliseconds(10)), std::chrono::milliseconds(10),
        std::chrono::milliseconds(1)).IsOk());
    std::vector<std::string> attempts;
    const auto status = router.Execute(
        [&](const HostPort &address, std::chrono::milliseconds) -> Router::RpcResult {
            attempts.emplace_back(address.ToString());
            if (address.Port() == 30001) {
                return { Status(K_NOT_READY, "candidate recovering"), std::nullopt };
            }
            return Response(State::SERVING);
        },
        Deadline(std::chrono::milliseconds(10)), std::chrono::milliseconds(10), std::chrono::milliseconds(1));
    EXPECT_TRUE(status.IsOk());
    EXPECT_EQ(attempts, (std::vector<std::string>{ "127.0.0.1:30001", "127.0.0.1:30002" }));
    EXPECT_EQ(refreshCalls, 0U);
}

TEST_F(CoordinatorLeaderRouterTest, HeaderlessNotReadySurvivesLaterCandidateTimeout)
{
    snapshots = { { "127.0.0.1:30001", "127.0.0.1:30002" } };
    Router router(Dependencies());
    size_t attempts = 0;
    const auto status = router.Execute(
        [&](const HostPort &, std::chrono::milliseconds timeout) -> Router::RpcResult {
            if (++attempts == 1) {
                return { Status(K_NOT_READY, "candidate recovering"), std::nullopt };
            }
            now += timeout;
            return TransportError(K_RPC_DEADLINE_EXCEEDED);
        },
        Deadline(std::chrono::milliseconds(3)), std::chrono::milliseconds(3), std::chrono::milliseconds(1));
    EXPECT_EQ(status.GetCode(), K_NOT_READY);
    EXPECT_EQ(status.GetMsg().find("candidate recovering"), 0U);
    EXPECT_NE(status.GetMsg().find("first_timeout_address=127.0.0.1:30002"), std::string::npos);
    EXPECT_EQ(attempts, 2U);
}

TEST_F(CoordinatorLeaderRouterTest, HeaderlessNotReadyStopsBeforeDiscoveryRefresh)
{
    snapshots = { { "127.0.0.1:30001" } };
    Router router(Dependencies());
    size_t attempts = 0;
    const auto status = router.Execute(
        [&](const HostPort &, std::chrono::milliseconds) -> Router::RpcResult {
            ++attempts;
            return { Status(K_NOT_READY, "topology bootstrap is not ready"), std::nullopt };
        },
        Deadline(std::chrono::milliseconds(3)), std::chrono::milliseconds(3), std::chrono::milliseconds(1));
    EXPECT_EQ(status.GetCode(), K_NOT_READY);
    EXPECT_EQ(attempts, 1U);
    EXPECT_EQ(refreshCalls, 0U);
    EXPECT_TRUE(waits.empty());
    EXPECT_FALSE(router.GetLeaderIdentity().has_value());
    EXPECT_TRUE(publishedIdentities.empty());
}

TEST_F(CoordinatorLeaderRouterTest, RepeatedRouteChangesRespectOriginalDeadline)
{
    snapshots = { { "127.0.0.1:30001" } };
    Router router(Dependencies());
    const auto deadline = Deadline(std::chrono::milliseconds(3));
    size_t calls = 0;
    const auto status = router.Execute(
        [&](const HostPort &, std::chrono::milliseconds) {
            const auto id = std::to_string(++calls);
            EXPECT_TRUE(router.Execute(
                [&](const HostPort &, std::chrono::milliseconds) {
                    return Response(State::SERVING, Status::OK(), {}, id);
                },
                deadline, std::chrono::milliseconds(1), std::chrono::milliseconds(1)).IsOk());
            now += std::chrono::milliseconds(1);
            return Response(State::SERVING, Status::OK(), {}, "late");
        },
        deadline, std::chrono::milliseconds(1), std::chrono::milliseconds(1));
    EXPECT_EQ(status.GetCode(), K_RPC_DEADLINE_EXCEEDED);
    EXPECT_EQ(calls, 3);
    EXPECT_EQ(now, deadline);
    EXPECT_EQ(router.GetLeaderIdentity()->coordinatorId, "3");
}

TEST_F(CoordinatorLeaderRouterTest, RetriesTimedOutLeaderImmediatelyAfterRedirect)
{
    snapshots = { { "127.0.0.1:30001", "127.0.0.1:30002", "127.0.0.1:30003",
                    "127.0.0.1:30004", "127.0.0.1:30005" } };
    Router router(Dependencies());
    ASSERT_TRUE(router.Execute([](const HostPort &, std::chrono::milliseconds) { return Response(State::SERVING); },
                               Deadline(std::chrono::seconds(3)), std::chrono::seconds(3),
                               std::chrono::milliseconds(50)).IsOk());
    std::vector<std::string> attempts;
    std::vector<std::chrono::milliseconds> budgets;
    const auto deadline = Deadline(std::chrono::seconds(3));
    const auto status = router.Execute(
        [&](const HostPort &address, std::chrono::milliseconds timeout) {
            attempts.push_back(address.ToString());
            budgets.push_back(timeout);
            if (attempts.size() == 1) {
                now += timeout;
                return TransportError(K_RPC_DEADLINE_EXCEEDED);
            }
            return address.ToString() == "127.0.0.1:30001"
                       ? Response(State::SERVING)
                       : Response(State::NOT_LEADER, Status::OK(), "127.0.0.1:30001");
        },
        deadline, std::chrono::seconds(3), std::chrono::milliseconds(50), false);

    EXPECT_TRUE(status.IsOk()) << status.ToString();
    EXPECT_EQ(attempts, (std::vector<std::string>{ "127.0.0.1:30001", "127.0.0.1:30002", "127.0.0.1:30001" }));
    EXPECT_EQ(budgets.front(), std::chrono::milliseconds(1500));
    ASSERT_EQ(waits.size(), 1U);
    EXPECT_GE(waits.front(), std::chrono::milliseconds(50));
    EXPECT_LE(waits.front(), std::chrono::milliseconds(75));
    EXPECT_LT(now, deadline);
}

TEST_F(CoordinatorLeaderRouterTest, RedirectCanRetryAnInitiallyUnknownTimedOutLeader)
{
    snapshots = { { "127.0.0.1:30001", "127.0.0.1:30002" } };
    Router router(Dependencies());
    size_t leaderCalls = 0;
    size_t followerCalls = 0;
    const auto status = router.Execute(
        [&](const HostPort &address, std::chrono::milliseconds timeout) {
            if (address.ToString() == "127.0.0.1:30002") {
                ++followerCalls;
                return Response(State::NOT_LEADER, Status::OK(), "127.0.0.1:30001");
            }
            if (++leaderCalls < 3) {
                now += timeout;
                return TransportError(K_RPC_DEADLINE_EXCEEDED);
            }
            return Response(State::SERVING);
        },
        Deadline(std::chrono::seconds(3)), std::chrono::milliseconds(100), std::chrono::milliseconds(1),
        false);
    EXPECT_TRUE(status.IsOk()) << status.ToString();
    EXPECT_EQ(leaderCalls, 3U);
    EXPECT_EQ(followerCalls, 1U);
}

TEST_F(CoordinatorLeaderRouterTest, CachedLeaderRetriesWithoutCountLimit)
{
    snapshots = { { "127.0.0.1:30001" } };
    Router router(Dependencies());
    ASSERT_TRUE(router.Execute([](const HostPort &, std::chrono::milliseconds) { return Response(State::SERVING); },
                               Deadline(std::chrono::seconds(3)), std::chrono::seconds(3),
                               std::chrono::milliseconds(1)).IsOk());
    size_t attempts = 0;
    const auto deadline = Deadline(std::chrono::seconds(3));
    const auto status = router.Execute(
        [&](const HostPort &, std::chrono::milliseconds timeout) {
            ++attempts;
            now += timeout;
            return TransportError(K_RPC_DEADLINE_EXCEEDED);
        },
        deadline, std::chrono::milliseconds(100), std::chrono::milliseconds(1), false);
    EXPECT_EQ(status.GetCode(), K_RPC_DEADLINE_EXCEEDED);
    EXPECT_GT(attempts, 3U);
    EXPECT_GT(waits.size(), 2U);
    EXPECT_EQ(now, deadline);
    EXPECT_NE(status.ToString().find("first_timeout_address=127.0.0.1:30001"), std::string::npos);
    EXPECT_NE(status.ToString().find("first_timeout_ms=100"), std::string::npos);
    EXPECT_NE(status.ToString().find("retry_rounds="), std::string::npos);
}

TEST_F(CoordinatorLeaderRouterTest, DefaultRequestRetriesMoreThanTwice)
{
    snapshots = { { "127.0.0.1:30001" } };
    Router router(Dependencies());
    size_t calls = 0;
    const auto deadline = Deadline(std::chrono::milliseconds(20));
    const auto status = router.Execute(
        [&](const HostPort &, std::chrono::milliseconds timeout) {
            EXPECT_LE(now + timeout, deadline);
            return ++calls < 5 ? TransportError(K_RPC_DEADLINE_EXCEEDED) : Response(State::SERVING);
        }, deadline, std::chrono::milliseconds(10), std::chrono::milliseconds(1));
    EXPECT_TRUE(status.IsOk());
    EXPECT_EQ(calls, 5U);
    EXPECT_LT(now, deadline);
}

TEST_F(CoordinatorLeaderRouterTest, UnreachableLeaderImmediatelyAdvancesToFollower)
{
    snapshots = { { "127.0.0.1:30001", "127.0.0.1:30002" } };
    Router router(Dependencies());
    ASSERT_TRUE(router.Execute([](const HostPort &, std::chrono::milliseconds) { return Response(State::SERVING); },
        Deadline(std::chrono::seconds(3)), std::chrono::seconds(3), std::chrono::milliseconds(1)).IsOk());
    const auto started = now;
    std::vector<std::string> attempts;
    const auto status = router.Execute(
        [&](const HostPort &address, std::chrono::milliseconds) {
            EXPECT_EQ(now, started);
            attempts.push_back(address.ToString());
            return address.Port() == 30001 ? TransportError(K_RPC_PEER_DEAD) : Response(State::SERVING);
        }, Deadline(std::chrono::seconds(3)), std::chrono::seconds(3), std::chrono::milliseconds(1));
    EXPECT_TRUE(status.IsOk());
    EXPECT_EQ(attempts.size(), 2U);
    EXPECT_TRUE(waits.empty());
}

TEST_F(CoordinatorLeaderRouterTest, TimeoutDoesNotOverrideTerminalBusinessError)
{
    snapshots = { { "127.0.0.1:30001", "127.0.0.1:30002" } };
    Router router(Dependencies());
    size_t attempts = 0;
    const auto status = router.Execute(
        [&](const HostPort &, std::chrono::milliseconds timeout) {
            if (++attempts == 1) {
                now += timeout;
                return TransportError(K_RPC_DEADLINE_EXCEEDED);
            }
            return Response(State::SERVING, Status(K_INVALID, "invalid watch"));
        },
        Deadline(std::chrono::seconds(3)), std::chrono::milliseconds(100), std::chrono::milliseconds(1),
        false);
    EXPECT_EQ(status.GetCode(), K_INVALID);
    EXPECT_EQ(attempts, 2U);
    EXPECT_TRUE(waits.empty());
}

TEST_F(CoordinatorLeaderRouterTest, KnownLeaderBudgetIsIndependentOfReplicaCount)
{
    for (size_t replicas : { 3U, 5U, 7U }) {
        std::vector<std::string> candidates;
        for (size_t i = 0; i < replicas; ++i) {
            candidates.push_back("127.0.0.1:" + std::to_string(30001 + i));
        }
        snapshots = { candidates };
        Router router(Dependencies());
        const auto rpc = [](const HostPort &, std::chrono::milliseconds) { return Response(State::SERVING); };
        ASSERT_TRUE(router.Execute(rpc, Deadline(std::chrono::seconds(3)), std::chrono::seconds(3),
                                   std::chrono::milliseconds(1)).IsOk());
        ASSERT_TRUE(router.Execute(
            [](const HostPort &, std::chrono::milliseconds timeout) {
                EXPECT_EQ(timeout, std::chrono::milliseconds(1500));
                return Response(State::SERVING);
            },
            Deadline(std::chrono::seconds(3)), std::chrono::seconds(3), std::chrono::milliseconds(1)).IsOk());
    }
}

TEST_F(CoordinatorLeaderRouterTest, LeaderTimeoutRetriesRespectOriginalDeadline)
{
    snapshots = { { "127.0.0.1:30001" } };
    Router router(Dependencies());
    ASSERT_TRUE(router.Execute([](const HostPort &, std::chrono::milliseconds) { return Response(State::SERVING); },
                               Deadline(std::chrono::seconds(3)), std::chrono::seconds(3),
                               std::chrono::milliseconds(1)).IsOk());
    size_t attempts = 0;
    const auto deadline = Deadline(std::chrono::milliseconds(150));
    const auto status = router.Execute(
        [&](const HostPort &, std::chrono::milliseconds timeout) {
            ++attempts;
            EXPECT_LE(now + timeout, deadline);
            now += timeout;
            return TransportError(K_RPC_DEADLINE_EXCEEDED);
        },
        deadline, std::chrono::milliseconds(100), std::chrono::milliseconds(50), false);
    EXPECT_EQ(status.GetCode(), K_RPC_DEADLINE_EXCEEDED);
    EXPECT_EQ(attempts, 1U);
    EXPECT_EQ(now, deadline);
}

TEST_F(CoordinatorLeaderRouterTest, InitialLeaderBudgetHonorsFloorDeadlineAndMaximum)
{
    snapshots = { { "127.0.0.1:30001" } };
    Router router(Dependencies());
    ASSERT_TRUE(router.Execute([](const HostPort &, std::chrono::milliseconds) { return Response(State::SERVING); },
        Deadline(std::chrono::seconds(3)), std::chrono::seconds(3), std::chrono::milliseconds(1)).IsOk());
    struct BudgetCase { int total; int maximum; int expected; };
    for (const auto test : { BudgetCase{ 3000, 3000, 1500 }, BudgetCase{ 5000, 5000, 2500 },
                             BudgetCase{ 1500, 1500, 1000 }, BudgetCase{ 800, 800, 800 },
                             BudgetCase{ 3000, 100, 100 } }) {
        const auto status = router.Execute(
            [&](const HostPort &, std::chrono::milliseconds timeout) {
                EXPECT_EQ(timeout.count(), test.expected);
                now += timeout - std::chrono::milliseconds(1);
                return Response(State::SERVING);
            }, Deadline(std::chrono::milliseconds(test.total)), std::chrono::milliseconds(test.maximum),
            std::chrono::milliseconds(1));
        EXPECT_TRUE(status.IsOk());
    }
}

TEST_F(CoordinatorLeaderRouterTest, RedirectCanUseRemainingTimeAfterInitialLeaderBudgetExpires)
{
    snapshots = { { "127.0.0.1:30001", "127.0.0.1:30002" } };
    Router router(Dependencies());
    ASSERT_TRUE(router.Execute([](const HostPort &, std::chrono::milliseconds) { return Response(State::SERVING); },
        Deadline(std::chrono::seconds(3)), std::chrono::seconds(3), std::chrono::milliseconds(1)).IsOk());
    size_t calls = 0;
    const auto deadline = Deadline(std::chrono::seconds(3));
    const auto status = router.Execute(
        [&](const HostPort &address, std::chrono::milliseconds timeout) {
            ++calls;
            EXPECT_LE(now + timeout, deadline);
            if (calls == 1) {
                EXPECT_EQ(timeout.count(), 1500);
                now += timeout;
                return TransportError(K_RPC_DEADLINE_EXCEEDED);
            }
            if (address.Port() == 30002) {
                return Response(State::NOT_LEADER, Status::OK(), "127.0.0.1:30001");
            }
            EXPECT_EQ(timeout.count(), 1499);
            return Response(State::SERVING);
        }, deadline, std::chrono::seconds(3), std::chrono::milliseconds(1));
    EXPECT_TRUE(status.IsOk());
    EXPECT_EQ(calls, 3U);
}

}  // namespace
}  // namespace datasystem
