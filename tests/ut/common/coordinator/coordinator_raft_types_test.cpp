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

/**
 * Description: Unit tests for coordinator raft option validation.
 */

#include <cstdint>
#include <limits>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <butil/endpoint.h>

#include "ut/common.h"
#include "datasystem/coordinator/raft/coordinator_raft_peer.h"
#include "datasystem/coordinator/raft/coordinator_raft_types.h"

namespace datasystem::coordinator {
namespace {
static_assert(std::string_view(kCoordinatorRaftGroupId) == "datasystem-coordinator");

constexpr char kLocalPeer[] = "127.0.0.1:18480";
constexpr char kRemotePeer[] = "127.0.0.1:18481";
constexpr char kDataDir[] = "/raft-data";
constexpr int kHeartbeatIntervalMs = 100;
constexpr int kElectionTimeoutMs = 1'000;
constexpr int64_t kElectionTimeoutBelowMinimumMs =
    static_cast<int64_t>(kCoordinatorRaftMinElectionTimeoutMs) - 1;
constexpr int64_t kElectionTimeoutAboveMaximumMs =
    static_cast<int64_t>(kCoordinatorRaftMaxElectionTimeoutMs) + 1;
static_assert(kElectionTimeoutBelowMinimumMs >= std::numeric_limits<int>::min());
static_assert(kElectionTimeoutAboveMaximumMs <= std::numeric_limits<int>::max());

CoordinatorRaftOptions MakeOptions(RaftStartPlan startPlan)
{
    return CoordinatorRaftOptions{ kLocalPeer, kDataDir, kHeartbeatIntervalMs, kElectionTimeoutMs,
                                   std::move(startPlan) };
}

CoordinatorRaftOptions MakeBootstrapOptions(std::vector<std::string> initialPeers)
{
    return MakeOptions(RaftStartPlan{ BootstrapPlan{ std::move(initialPeers) } });
}

CoordinatorRaftOptions MakeValidBootstrapOptions()
{
    return MakeBootstrapOptions({ kLocalPeer, kRemotePeer });
}

void ExpectStatusCode(const CoordinatorRaftOptions &options, RaftMetadataState metadataState,
                      StatusCode expectedCode)
{
    EXPECT_EQ(ValidateCoordinatorRaftOptions(options, metadataState).GetCode(), expectedCode);
}

void ExpectInvalid(const CoordinatorRaftOptions &options, RaftMetadataState metadataState)
{
    ExpectStatusCode(options, metadataState, K_INVALID);
}

void ExpectStatusCodeForEveryStartPlan(RaftMetadataState metadataState, StatusCode expectedCode)
{
    ExpectStatusCode(MakeValidBootstrapOptions(), metadataState, expectedCode);
    ExpectStatusCode(MakeOptions(RaftStartPlan{ RecoverPlan{} }), metadataState, expectedCode);
    ExpectStatusCode(MakeOptions(RaftStartPlan{ WaitingToJoinPlan{} }), metadataState, expectedCode);
}
}  // namespace

TEST(CoordinatorRaftTypesTest, NormalizesStableIpv4PeerWithoutBraftIndex)
{
    braft::PeerId peer;

    DS_ASSERT_OK(ParseCoordinatorRaftPeer("127.0.0.1:18480", peer));
    EXPECT_EQ(CoordinatorRaftPeerAddress(peer), "127.0.0.1:18480");
    EXPECT_EQ(peer.idx, 0);

    DS_ASSERT_OK(ParseCoordinatorRaftPeer("127.0.0.1:01234", peer));
    EXPECT_EQ(CoordinatorRaftPeerAddress(peer), "127.0.0.1:1234");
    EXPECT_EQ(peer.idx, 0);
}

TEST(CoordinatorRaftTypesTest, RejectsMalformedPeerAddressesAndResetsOutput)
{
    const std::vector<std::string> invalidPeers{
        "",
        " 127.0.0.1:18480",
        "127.0.0.1:18480\t",
        "127.0.0.1:18 480",
        "127.0.0.1:+1",
        "127.0.0.1:-1",
        "127.0.0.1:",
        "127.0.0.1:18480x",
        "127.0.0.1:1848000000000000000000000000000000000000000",
        "127.0.0.1:18480/raft",
        "127.0.0.1:05999",
        "127.0.0.1:0",
        "127.0.0.1:65536",
        "127.0.0.1:18480:0",
        "127.0.0.1:18480:1",
        "[::1]:18480",
        "coordinator-0:18480",
        "0.0.0.0:18480",
        "127.0.0.999:18480"
    };
    for (const auto &address : invalidPeers) {
        SCOPED_TRACE("address: " + address);
        braft::PeerId peer;
        ASSERT_EQ(peer.parse(kLocalPeer), 0);
        ASSERT_FALSE(peer.is_empty());

        EXPECT_TRUE(ParseCoordinatorRaftPeer(address, peer).IsError());
        EXPECT_TRUE(peer.is_empty());
    }
}

TEST(CoordinatorRaftTypesTest, RejectedPeerAddressesDoNotLeakRawPayloadToStderr)
{
    const std::vector<std::string> invalidPeers{
        "malformed-peer-payload",
        "coordinator.example:18480",
        "[fd00::dead]:18480",
        "127.0.0.1:05999",
        "127.0.0.1:65536",
        "127.0.0.999:18480",
        "0.0.0.0:18480"
    };

    testing::internal::CaptureStderr();
    for (const auto &address : invalidPeers) {
        braft::PeerId peer;
        EXPECT_TRUE(ParseCoordinatorRaftPeer(address, peer).IsError());
        EXPECT_TRUE(peer.is_empty());
    }
    const auto capturedStderr = testing::internal::GetCapturedStderr();

    for (const auto &address : invalidPeers) {
        EXPECT_EQ(capturedStderr.find(address), std::string::npos);
    }
}

TEST(CoordinatorRaftTypesTest, FormatterFailsClosedForInvalidPeerIdentity)
{
    const braft::PeerId emptyPeer;
    ASSERT_TRUE(emptyPeer.is_empty());
    EXPECT_TRUE(CoordinatorRaftPeerAddress(emptyPeer).empty());

    braft::PeerId indexedPeer;
    ASSERT_EQ(indexedPeer.parse("127.0.0.1:18480:1"), 0);
    ASSERT_FALSE(indexedPeer.is_empty());
    ASSERT_EQ(indexedPeer.idx, 1);
    EXPECT_TRUE(CoordinatorRaftPeerAddress(indexedPeer).empty());

    constexpr int kValidPort = 18'480;
    const butil::EndPoint wildcardEndpoint(butil::IP_ANY, kValidPort);
    const braft::PeerId wildcardPeer(wildcardEndpoint, 0);
    ASSERT_FALSE(wildcardPeer.is_empty());
    ASSERT_EQ(wildcardPeer.addr.ip, butil::IP_ANY);
    ASSERT_EQ(wildcardPeer.addr.port, kValidPort);
    ASSERT_EQ(wildcardPeer.idx, 0);
    EXPECT_TRUE(CoordinatorRaftPeerAddress(wildcardPeer).empty());

    butil::EndPoint zeroPortEndpoint;
    ASSERT_EQ(butil::str2ip("127.0.0.1", &zeroPortEndpoint.ip), 0);
    zeroPortEndpoint.port = 0;
    const braft::PeerId zeroPortPeer(zeroPortEndpoint, 0);
    ASSERT_FALSE(zeroPortPeer.is_empty());
    ASSERT_NE(zeroPortPeer.addr.ip, butil::IP_ANY);
    ASSERT_EQ(zeroPortPeer.addr.port, 0);
    ASSERT_EQ(zeroPortPeer.idx, 0);
    EXPECT_TRUE(CoordinatorRaftPeerAddress(zeroPortPeer).empty());
}

TEST(CoordinatorRaftTypesTest, RejectsEmptyCommonFields)
{
    auto options = MakeValidBootstrapOptions();
    options.localPeer.clear();
    ExpectInvalid(options, RaftMetadataState::ABSENT);

    options = MakeValidBootstrapOptions();
    options.dataDir.clear();
    ExpectInvalid(options, RaftMetadataState::ABSENT);
}

TEST(CoordinatorRaftTypesTest, AcceptsInclusiveHeartbeatAndElectionRatioBounds)
{
    {
        auto options = MakeValidBootstrapOptions();
        options.heartbeatIntervalMs = kCoordinatorRaftMinHeartbeatIntervalMs;
        options.electionTimeoutMs = options.heartbeatIntervalMs * kCoordinatorRaftMinElectionHeartbeatRatio;
        DS_ASSERT_OK(ValidateCoordinatorRaftOptions(options, RaftMetadataState::ABSENT));
    }
    {
        auto options = MakeValidBootstrapOptions();
        options.heartbeatIntervalMs = kCoordinatorRaftMaxHeartbeatIntervalMs;
        options.electionTimeoutMs = options.heartbeatIntervalMs * kCoordinatorRaftMaxElectionHeartbeatRatio;
        DS_ASSERT_OK(ValidateCoordinatorRaftOptions(options, RaftMetadataState::ABSENT));
    }
}

TEST(CoordinatorRaftTypesTest, RejectsHeartbeatOutsideInclusiveBounds)
{
    for (const int heartbeatIntervalMs : { kCoordinatorRaftMinHeartbeatIntervalMs - 1,
                                           kCoordinatorRaftMaxHeartbeatIntervalMs + 1 }) {
        SCOPED_TRACE(heartbeatIntervalMs);
        auto options = MakeValidBootstrapOptions();
        options.heartbeatIntervalMs = heartbeatIntervalMs;

        const auto status = ValidateCoordinatorRaftOptions(options, RaftMetadataState::ABSENT);

        EXPECT_EQ(status.GetCode(), K_INVALID);
        EXPECT_NE(status.GetMsg().find("heartbeatIntervalMs=" + std::to_string(heartbeatIntervalMs)),
                  std::string::npos);
    }
}

TEST(CoordinatorRaftTypesTest, RejectsElectionTimeoutOutsideInclusiveBounds)
{
    for (const int electionTimeoutMs : { static_cast<int>(kElectionTimeoutBelowMinimumMs),
                                         static_cast<int>(kElectionTimeoutAboveMaximumMs) }) {
        SCOPED_TRACE(electionTimeoutMs);
        auto options = MakeValidBootstrapOptions();
        options.electionTimeoutMs = electionTimeoutMs;

        const auto status = ValidateCoordinatorRaftOptions(options, RaftMetadataState::ABSENT);

        EXPECT_EQ(status.GetCode(), K_INVALID);
        EXPECT_NE(status.GetMsg().find("electionTimeoutMs=" + std::to_string(electionTimeoutMs)),
                  std::string::npos);
        EXPECT_NE(status.GetMsg().find("[" + std::to_string(kCoordinatorRaftMinElectionTimeoutMs) + ", "
                                       + std::to_string(kCoordinatorRaftMaxElectionTimeoutMs) + "]"),
                  std::string::npos);
    }
}

TEST(CoordinatorRaftTypesTest, RejectsElectionTimeoutOutsideHeartbeatRatio)
{
    for (const int electionTimeoutMs : { kHeartbeatIntervalMs * (kCoordinatorRaftMinElectionHeartbeatRatio - 1),
                                         kHeartbeatIntervalMs * (kCoordinatorRaftMaxElectionHeartbeatRatio + 1) }) {
        SCOPED_TRACE(electionTimeoutMs);
        auto options = MakeValidBootstrapOptions();
        options.electionTimeoutMs = electionTimeoutMs;

        const auto status = ValidateCoordinatorRaftOptions(options, RaftMetadataState::ABSENT);

        EXPECT_EQ(status.GetCode(), K_INVALID);
        EXPECT_NE(status.GetMsg().find("times heartbeatIntervalMs"), std::string::npos);
    }
}

TEST(CoordinatorRaftTypesTest, RejectsElectionTimeoutNotIntegerMultipleOfHeartbeat)
{
    auto options = MakeValidBootstrapOptions();
    options.electionTimeoutMs = kElectionTimeoutMs - 1;

    const auto status = ValidateCoordinatorRaftOptions(options, RaftMetadataState::ABSENT);

    EXPECT_EQ(status.GetCode(), K_INVALID);
    EXPECT_NE(status.GetMsg().find("integer multiple"), std::string::npos);
}

TEST(CoordinatorRaftTypesTest, RejectsUnsupportedOrUnstablePeerAddresses)
{
    const std::vector<std::string> invalidPeers{
        "coordinator-0:18480", "[::1]:18480",      "0.0.0.0:18480",       "127.0.0.1",
        "127.0.0.1:0",        "127.0.0.1:65536", "127.0.0.999:18480"
    };
    for (const auto &peer : invalidPeers) {
        SCOPED_TRACE("peer: " + peer);
        auto options = MakeBootstrapOptions({ peer, kRemotePeer });
        options.localPeer = peer;

        ExpectInvalid(options, RaftMetadataState::ABSENT);
    }
}

TEST(CoordinatorRaftTypesTest, BootstrapRejectsEmptyInitialPeers)
{
    const auto options = MakeBootstrapOptions({});

    ExpectInvalid(options, RaftMetadataState::ABSENT);
}

TEST(CoordinatorRaftTypesTest, BootstrapRejectsInitialPeersWithoutLocalPeer)
{
    const auto options = MakeBootstrapOptions({ kRemotePeer });

    ExpectInvalid(options, RaftMetadataState::ABSENT);
}

TEST(CoordinatorRaftTypesTest, BootstrapRejectsInvalidInitialPeerAddress)
{
    const auto options = MakeBootstrapOptions({ kLocalPeer, "coordinator-1:18481" });

    ExpectInvalid(options, RaftMetadataState::ABSENT);
}

TEST(CoordinatorRaftTypesTest, BootstrapRejectsDuplicateNormalizedPeers)
{
    constexpr char kCanonicalPeer[] = "127.0.0.1:184";
    constexpr char kPaddedPeer[] = "127.0.0.1:0184";
    braft::PeerId normalizedPeer;
    DS_ASSERT_OK(ParseCoordinatorRaftPeer(kPaddedPeer, normalizedPeer));
    ASSERT_EQ(CoordinatorRaftPeerAddress(normalizedPeer), kCanonicalPeer);

    auto options = MakeBootstrapOptions({ kCanonicalPeer, kPaddedPeer });
    options.localPeer = kCanonicalPeer;
    const auto status = ValidateCoordinatorRaftOptions(options, RaftMetadataState::ABSENT);
    EXPECT_EQ(status.GetCode(), K_INVALID);
    EXPECT_NE(status.GetMsg().find("duplicate normalized peer address"), std::string::npos);
}

TEST(CoordinatorRaftTypesTest, BootstrapRejectsValidExistingMetadata)
{
    const auto options = MakeValidBootstrapOptions();

    ExpectInvalid(options, RaftMetadataState::VALID);
}

TEST(CoordinatorRaftTypesTest, BootstrapAcceptsValidOptionsWithAbsentMetadata)
{
    const auto options = MakeValidBootstrapOptions();

    DS_ASSERT_OK(ValidateCoordinatorRaftOptions(options, RaftMetadataState::ABSENT));
}

TEST(CoordinatorRaftTypesTest, RecoverOnlyAcceptsValidMetadata)
{
    const auto options = MakeOptions(RaftStartPlan{ RecoverPlan{} });

    DS_ASSERT_OK(ValidateCoordinatorRaftOptions(options, RaftMetadataState::VALID));
    ExpectInvalid(options, RaftMetadataState::ABSENT);
}

TEST(CoordinatorRaftTypesTest, WaitingToJoinOnlyAcceptsAbsentMetadata)
{
    const auto options = MakeOptions(RaftStartPlan{ WaitingToJoinPlan{} });

    DS_ASSERT_OK(ValidateCoordinatorRaftOptions(options, RaftMetadataState::ABSENT));
    ExpectInvalid(options, RaftMetadataState::VALID);
}

TEST(CoordinatorRaftTypesTest, CorruptMetadataIsDataInconsistencyForEveryStartPlan)
{
    ExpectStatusCodeForEveryStartPlan(RaftMetadataState::CORRUPT, K_DATA_INCONSISTENCY);
}

TEST(CoordinatorRaftTypesTest, UnknownMetadataIsNotReadyForEveryStartPlan)
{
    ExpectStatusCodeForEveryStartPlan(RaftMetadataState::UNKNOWN, K_NOT_READY);
}
}  // namespace datasystem::coordinator
