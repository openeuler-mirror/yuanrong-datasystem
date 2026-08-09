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
#include "datasystem/common/coordinator/coordinator_leader_router.h"

#include <chrono>
#include <string>
#include <utility>
#include <vector>

#include "gtest/gtest.h"

namespace datasystem {
namespace {
constexpr char COORDINATOR_ID[] = "0123456789abcdef";

class RouterDiscovery final : public IDeadlineAwareCoordinatorDiscovery {
public:
    explicit RouterDiscovery(std::vector<std::string> addresses) : addresses_(std::move(addresses)) {}

    Status GetCoordinators(std::vector<std::string> &addresses) override
    {
        addresses = addresses_;
        return Status::OK();
    }

    Status GetCoordinators(std::chrono::steady_clock::time_point, std::vector<std::string> &addresses) override
    {
        ++deadlineAwareCalls_;
        addresses = addresses_;
        return Status::OK();
    }

    size_t DeadlineAwareCalls() const { return deadlineAwareCalls_; }
    void SetAddresses(std::vector<std::string> addresses)
    {
        addresses_ = std::move(addresses);
    }

private:
    std::vector<std::string> addresses_;
    size_t deadlineAwareCalls_{ 0 };
};

HostPort Address(const std::string &value)
{
    HostPort address;
    EXPECT_TRUE(address.ParseString(value).IsOk());
    return address;
}

coordinator::ResponseHeader LeaderHeader(uint64_t term, coordinator::ResponseHeader::ServingStatePb state)
{
    coordinator::ResponseHeader header;
    header.set_coordinator_id(COORDINATOR_ID);
    header.set_is_leader(true);
    header.set_leader_term(term);
    header.set_serving_state(state);
    return header;
}

coordinator::ResponseHeader RecoveringLeaderHeader(uint64_t term)
{
    auto header = LeaderHeader(term, coordinator::ResponseHeader::LEADER_RECOVERING);
    header.set_is_leader(false);
    header.set_leader_address("127.0.0.1:30001");
    return header;
}

TEST(CoordinatorLeaderRouterTest, RetriesFollowerRedirectWithinOneLogicalCall)
{
    auto discovery = std::make_shared<RouterDiscovery>(std::vector<std::string>{ "127.0.0.1:30001" });
    CoordinatorLeaderRouter router(discovery, { Address("127.0.0.1:30001") });
    std::vector<std::string> attempted;

    const auto status = router.Execute([&](const HostPort &address, int32_t, coordinator::ResponseHeader &header,
                                           bool &hasHeader) {
        attempted.emplace_back(address.ToString());
        hasHeader = true;
        if (address.ToString() == "127.0.0.1:30001") {
            header = LeaderHeader(7, coordinator::ResponseHeader::FOLLOWER_SERVING);
            header.set_is_leader(false);
            header.set_leader_address("127.0.0.1:30002");
            return Status::OK();
        }
        header = LeaderHeader(7, coordinator::ResponseHeader::LEADER_SERVING);
        return Status::OK();
    });

    EXPECT_TRUE(status.IsOk());
    ASSERT_EQ(attempted.size(), 2);
    EXPECT_EQ(attempted[1], "127.0.0.1:30002");
    EXPECT_EQ(router.GetLeaderCache().address.ToString(), "127.0.0.1:30002");
}

TEST(CoordinatorLeaderRouterTest, PreservesFollowerRouteStatusWhenRedirectLeaderAndRemainingCandidatesAreDead)
{
    auto now = std::chrono::steady_clock::time_point{};
    auto discovery =
        std::make_shared<RouterDiscovery>(std::vector<std::string>{ "127.0.0.1:30001", "127.0.0.1:30003" });
    CoordinatorLeaderRouter router(discovery, { Address("127.0.0.1:30001"), Address("127.0.0.1:30003") },
                                   [&now] { return now; },
                                   [&now](std::chrono::milliseconds) { now += std::chrono::milliseconds(3'000); },
                                   std::chrono::milliseconds(3'000));
    std::vector<std::string> attempted;

    const auto status = router.Execute([&attempted](const HostPort &address, int32_t,
                                                    coordinator::ResponseHeader &header, bool &hasHeader) {
        attempted.emplace_back(address.ToString());
        if (address.ToString() == "127.0.0.1:30001") {
            hasHeader = true;
            header = LeaderHeader(7, coordinator::ResponseHeader::FOLLOWER_SERVING);
            header.set_is_leader(false);
            header.set_leader_address("127.0.0.1:30002");
            return Status::OK();
        }
        hasHeader = false;
        return Status(K_RPC_PEER_DEAD, "injected dead Coordinator");
    });

    EXPECT_EQ(status.GetCode(), K_NOT_READY);
    EXPECT_EQ(attempted,
              (std::vector<std::string>{ "127.0.0.1:30001", "127.0.0.1:30002", "127.0.0.1:30003" }));
}

TEST(CoordinatorLeaderRouterTest, RecoveringLeaderRequiresRecoveryControlRequest)
{
    auto now = std::chrono::steady_clock::time_point{};
    auto discovery = std::make_shared<RouterDiscovery>(std::vector<std::string>{ "127.0.0.1:30001" });
    CoordinatorLeaderRouter router(discovery, { Address("127.0.0.1:30001") }, [&now] { return now; },
                                   [&now](std::chrono::milliseconds) { now += std::chrono::seconds(3); });
    const auto attempt = [](const HostPort &, int32_t, coordinator::ResponseHeader &header, bool &hasHeader) {
        hasHeader = true;
        header = RecoveringLeaderHeader(7);
        return Status::OK();
    };

    EXPECT_EQ(router.Execute(attempt).GetCode(), K_NOT_READY);
    EXPECT_TRUE(router.Execute(attempt, true).IsOk());
}

TEST(CoordinatorLeaderRouterTest, RejectsLateLowerTermHeaderWithoutReplacingCache)
{
    auto now = std::chrono::steady_clock::time_point{};
    auto discovery =
        std::make_shared<RouterDiscovery>(std::vector<std::string>{ "127.0.0.1:30001", "127.0.0.1:30002" });
    CoordinatorLeaderRouter router(discovery, { Address("127.0.0.1:30001"), Address("127.0.0.1:30002") },
                                   [&now] { return now; },
                                   [&now](std::chrono::milliseconds delay) { now += delay; },
                                   std::chrono::milliseconds(3'000));

    ASSERT_TRUE(router.Execute([](const HostPort &, int32_t, coordinator::ResponseHeader &header, bool &hasHeader) {
        hasHeader = true;
        header = LeaderHeader(9, coordinator::ResponseHeader::LEADER_SERVING);
        return Status::OK();
    }).IsOk());
    const auto status = router.Execute([](const HostPort &, int32_t, coordinator::ResponseHeader &header,
                                          bool &hasHeader) {
        hasHeader = true;
        header = LeaderHeader(8, coordinator::ResponseHeader::LEADER_SERVING);
        return Status::OK();
    });

    EXPECT_EQ(status.GetCode(), K_NOT_READY);
    EXPECT_EQ(router.GetLeaderCache().leaderTerm, 9);
}

TEST(CoordinatorLeaderRouterTest, PublishesOnlyRealLeaderIdentityChanges)
{
    auto discovery = std::make_shared<RouterDiscovery>(std::vector<std::string>{ "127.0.0.1:30001" });
    CoordinatorLeaderRouter router(discovery, { Address("127.0.0.1:30001") });
    size_t notificationCount = 0;
    auto subscription = router.SubscribeLeaderChanges(
        [&notificationCount](const CoordinatorLeaderIdentity &) { ++notificationCount; });

    const auto attempt = [](const HostPort &, int32_t, coordinator::ResponseHeader &header, bool &hasHeader) {
        hasHeader = true;
        header = LeaderHeader(3, coordinator::ResponseHeader::LEADER_SERVING);
        return Status::OK();
    };
    ASSERT_TRUE(router.Execute(attempt).IsOk());
    ASSERT_TRUE(router.Execute(attempt).IsOk());

    EXPECT_EQ(notificationCount, 1);
    EXPECT_EQ(router.GetLeaderCache().routeEpoch, 1);
}

TEST(CoordinatorLeaderRouterTest, PrefersCachedLeaderBeforeDiscoveryCandidates)
{
    auto discovery =
        std::make_shared<RouterDiscovery>(std::vector<std::string>{ "127.0.0.1:30001", "127.0.0.1:30002" });
    CoordinatorLeaderRouter router(discovery, { Address("127.0.0.1:30001"), Address("127.0.0.1:30002") });
    ASSERT_TRUE(router.Execute([](const HostPort &address, int32_t, coordinator::ResponseHeader &header,
                                  bool &hasHeader) {
        hasHeader = true;
        if (address.ToString() == "127.0.0.1:30002") {
            header = LeaderHeader(4, coordinator::ResponseHeader::LEADER_SERVING);
            return Status::OK();
        }
        header = LeaderHeader(4, coordinator::ResponseHeader::FOLLOWER_SERVING);
        header.set_is_leader(false);
        return Status::OK();
    }).IsOk());

    std::vector<std::string> attempts;
    ASSERT_TRUE(router.Execute([&attempts](const HostPort &address, int32_t, coordinator::ResponseHeader &header,
                                            bool &hasHeader) {
        attempts.emplace_back(address.ToString());
        hasHeader = true;
        header = LeaderHeader(4, coordinator::ResponseHeader::LEADER_SERVING);
        return Status::OK();
    }).IsOk());
    ASSERT_FALSE(attempts.empty());
    EXPECT_EQ(attempts.front(), "127.0.0.1:30002");
}

TEST(CoordinatorLeaderRouterTest, UsesOneDiscoveryRefreshAfterCandidateAttemptsFail)
{
    auto discovery = std::make_shared<RouterDiscovery>(std::vector<std::string>{ "127.0.0.1:30002" });
    CoordinatorLeaderRouter router(discovery, { Address("127.0.0.1:30001") });
    std::vector<std::string> attempts;

    const auto status = router.Execute([&attempts](const HostPort &address, int32_t,
                                                   coordinator::ResponseHeader &header, bool &hasHeader) {
        attempts.emplace_back(address.ToString());
        hasHeader = true;
        if (address.ToString() == "127.0.0.1:30002") {
            header = LeaderHeader(2, coordinator::ResponseHeader::LEADER_SERVING);
            return Status::OK();
        }
        header = LeaderHeader(2, coordinator::ResponseHeader::FOLLOWER_SERVING);
        header.set_is_leader(false);
        return Status::OK();
    });

    EXPECT_TRUE(status.IsOk());
    EXPECT_EQ(discovery->DeadlineAwareCalls(), 1UL);
    ASSERT_EQ(attempts.size(), 2UL);
    EXPECT_EQ(attempts[1], "127.0.0.1:30002");
}

TEST(CoordinatorLeaderRouterTest, PreservesLastFailureWhenRefreshHasNoNewCandidate)
{
    auto now = std::chrono::steady_clock::time_point{};
    auto discovery = std::make_shared<RouterDiscovery>(std::vector<std::string>{ "127.0.0.1:30001" });
    CoordinatorLeaderRouter router(discovery, { Address("127.0.0.1:30001") }, [&now] { return now; },
                                   [&now](std::chrono::milliseconds) { now += std::chrono::milliseconds(3'000); },
                                   std::chrono::milliseconds(3'000));
    size_t calls = 0;

    const auto status = router.Execute([&calls](const HostPort &, int32_t, coordinator::ResponseHeader &, bool &) {
        ++calls;
        return Status(K_RPC_UNAVAILABLE, "injected failure");
    });

    EXPECT_EQ(status.GetCode(), K_RPC_UNAVAILABLE);
    EXPECT_EQ(calls, 1UL);
    EXPECT_EQ(discovery->DeadlineAwareCalls(), 1UL);
}

TEST(CoordinatorLeaderRouterTest, RefreshesUntilDiscoveryReturnsANewLeaderBeforeDeadline)
{
    auto now = std::chrono::steady_clock::time_point{};
    auto discovery = std::make_shared<RouterDiscovery>(std::vector<std::string>{ "127.0.0.1:30001" });
    bool publishNewLeader = false;
    CoordinatorLeaderRouter router(
        discovery, { Address("127.0.0.1:30001") }, [&now] { return now; },
        [&now, &discovery, &publishNewLeader](std::chrono::milliseconds delay) {
            now += delay;
            if (!publishNewLeader) {
                discovery->SetAddresses({ "127.0.0.1:30002" });
                publishNewLeader = true;
            }
        });
    std::vector<std::string> attempts;

    const auto status = router.Execute([&attempts](const HostPort &address, int32_t,
                                                   coordinator::ResponseHeader &header, bool &hasHeader) {
        attempts.emplace_back(address.ToString());
        hasHeader = true;
        if (address.ToString() == "127.0.0.1:30002") {
            header = LeaderHeader(2, coordinator::ResponseHeader::LEADER_SERVING);
            return Status::OK();
        }
        return Status(K_RPC_UNAVAILABLE, "injected failure");
    });

    EXPECT_TRUE(status.IsOk());
    EXPECT_EQ(discovery->DeadlineAwareCalls(), 2UL);
    ASSERT_EQ(attempts.size(), 2UL);
    EXPECT_EQ(attempts[1], "127.0.0.1:30002");
}

TEST(CoordinatorLeaderRouterTest, ReturnsNotReadyImmediatelyForBusinessRpcToRecoveringLeader)
{
    auto now = std::chrono::steady_clock::time_point{};
    auto discovery = std::make_shared<RouterDiscovery>(std::vector<std::string>{ "127.0.0.1:30001" });
    bool waited = false;
    CoordinatorLeaderRouter router(discovery, { Address("127.0.0.1:30001") }, [&now] { return now; },
                                   [&now, &waited](std::chrono::milliseconds delay) {
                                       waited = true;
                                       now += delay;
                                   });
    size_t attempts = 0;

    const auto status = router.Execute([&attempts](const HostPort &, int32_t, coordinator::ResponseHeader &header,
                                           bool &hasHeader) {
        ++attempts;
        hasHeader = true;
        header = RecoveringLeaderHeader(7);
        return Status::OK();
    });

    EXPECT_EQ(status.GetCode(), K_NOT_READY);
    EXPECT_EQ(attempts, 1UL);
    EXPECT_FALSE(waited);
}

TEST(CoordinatorLeaderRouterTest, StopsBeforeDispatchWhenTheSharedDeadlineExpires)
{
    auto now = std::chrono::steady_clock::time_point{};
    auto discovery = std::make_shared<RouterDiscovery>(std::vector<std::string>{ "127.0.0.1:30001" });
    CoordinatorLeaderRouter router(discovery, { Address("127.0.0.1:30001") }, [&now] {
        const auto result = now;
        now += std::chrono::milliseconds(3'000);
        return result;
    }, {}, std::chrono::milliseconds(3'000));
    size_t calls = 0;

    const auto status = router.Execute([&calls](const HostPort &, int32_t, coordinator::ResponseHeader &, bool &) {
        ++calls;
        return Status::OK();
    });

    EXPECT_EQ(status.GetCode(), K_RPC_DEADLINE_EXCEEDED);
    EXPECT_EQ(calls, 0UL);
}
}  // namespace
}  // namespace datasystem
