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
 * Description: Tests Worker metadata owner resolution and API manager routing.
 */
#include "datasystem/worker/metadata_route_resolver.h"

#include <chrono>
#include <functional>
#include <future>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "datasystem/cluster/algorithm/topology_algorithm.h"
#include "datasystem/cluster/routing/placement_facade.h"
#include "datasystem/cluster/runtime/topology_snapshot_state.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/worker/worker_master_api_manager_base.h"

namespace datasystem::ut {
namespace {
class RecordingRoutingAlgorithm final : public cluster::IRoutingAlgorithm {
public:
    ~RecordingRoutingAlgorithm() override = default;

    cluster::TopologyAlgorithmId GetId() const override
    {
        return "metadata-route-test";
    }

    uint32_t Hash(std::string_view placementKey) const noexcept override
    {
        auto iter = tokenByKey_.find(std::string(placementKey));
        return iter == tokenByKey_.end() ? 1U : iter->second;
    }

    Status LocateOwner(const cluster::TopologySnapshot &snapshot, uint32_t token,
                       const cluster::Member *&owner) const override
    {
        versions_.emplace_back(snapshot.Version());
        ++locateCount_;
        if (locateCount_ == 1 && firstLocateHook_) {
            firstLocateHook_();
        }
        RETURN_IF_NOT_OK(locateStatus_);
        auto iter = committedByToken_.find(token);
        if (iter != committedByToken_.end()) {
            owner = &iter->second;
            return Status::OK();
        }
        owner = &owner_;
        return Status::OK();
    }

    Status LocateProspectiveOwner(const cluster::TopologySnapshot &snapshot, uint32_t token,
                                  const cluster::Member *&owner) const override
    {
        (void)snapshot;
        auto iter = prospectiveByToken_.find(token);
        if (iter != prospectiveByToken_.end()) {
            owner = &iter->second;
            return Status::OK();
        }
        owner = prospectiveEnabled_ ? &prospective_ : &owner_;
        return Status::OK();
    }

    void SetOwnerAddress(std::string address)
    {
        owner_.identity.address = std::move(address);
        owner_.state = cluster::MemberState::ACTIVE;
    }

    void SetProspectiveAddress(std::string address)
    {
        prospective_.identity.address = std::move(address);
        prospective_.state = cluster::MemberState::ACTIVE;
        prospectiveEnabled_ = true;
    }

    void SetLocateStatus(Status status)
    {
        locateStatus_ = std::move(status);
    }

    void SetFirstLocateHook(std::function<void()> hook)
    {
        firstLocateHook_ = std::move(hook);
    }

    void RouteToken(uint32_t token, std::string address)
    {
        committedByToken_.emplace(token, MakeMember(std::move(address)));
    }

    void SetKeyToken(std::string key, uint32_t token)
    {
        tokenByKey_[std::move(key)] = token;
    }

    size_t LocateCount() const
    {
        return locateCount_;
    }

    const std::vector<uint64_t> &Versions() const
    {
        return versions_;
    }

private:
    static cluster::Member MakeMember(std::string address)
    {
        cluster::Member member;
        member.identity.address = std::move(address);
        member.state = cluster::MemberState::ACTIVE;
        return member;
    }

    cluster::Member owner_;
    cluster::Member prospective_;
    bool prospectiveEnabled_{ false };
    Status locateStatus_;
    std::map<uint32_t, cluster::Member> committedByToken_;
    std::map<uint32_t, cluster::Member> prospectiveByToken_;
    std::map<std::string, uint32_t> tokenByKey_;
    mutable size_t locateCount_{ 0 };
    mutable std::vector<uint64_t> versions_;
    mutable std::function<void()> firstLocateHook_;
};

class MetadataRouteResolverTest : public testing::Test {
protected:
    MetadataRouteResolverTest() : placement_(snapshots_, algorithm_, "127.0.0.1:18480")
    {
    }

    Status PublishVersion(uint64_t version, std::optional<cluster::ActiveBatch> batch = std::nullopt)
    {
        return PublishTopology(version, std::move(batch), {});
    }

    Status PublishTopology(uint64_t version, std::optional<cluster::ActiveBatch> batch,
                           const std::vector<std::pair<std::string, cluster::MemberState>> &members)
    {
        cluster::TopologyState state;
        state.version = version;
        state.activeBatch = std::move(batch);
        for (size_t index = 0; index < members.size(); ++index) {
            cluster::Member member;
            member.identity.id = std::string(16, static_cast<char>('a' + index));
            member.identity.address = members[index].first;
            member.state = members[index].second;
            member.tokens = { static_cast<uint32_t>(1000 * index + 1), static_cast<uint32_t>(1000 * index + 2),
                              static_cast<uint32_t>(1000 * index + 3), static_cast<uint32_t>(1000 * index + 4) };
            state.members.emplace_back(std::move(member));
        }
        std::shared_ptr<const cluster::TopologySnapshot> snapshot;
        RETURN_IF_NOT_OK(cluster::TopologySnapshot::Create(
            std::move(state), static_cast<int64_t>(version), std::string(64, version == 1 ? 'a' : 'b'), snapshot));
        cluster::SnapshotUpdateOutcome outcome;
        return snapshots_.Publish(std::move(snapshot), outcome);
    }

    RecordingRoutingAlgorithm algorithm_;
    cluster::TopologySnapshotState snapshots_;
    cluster::PlacementFacade placement_;
};

class TestMasterApi final {
public:
    Status Init()
    {
        initialized_ = true;
        return Status::OK();
    }

    bool Initialized() const
    {
        return initialized_;
    }

private:
    bool initialized_{ false };
};

class TestMasterApiManager final : public worker::WorkerMasterApiManagerBase<TestMasterApi> {
public:
    TestMasterApiManager(HostPort &workerAddress, const worker::MetadataRouteResolver &resolver)
        : WorkerMasterApiManagerBase<TestMasterApi>(workerAddress, nullptr, resolver)
    {
    }

    ~TestMasterApiManager() override = default;

    std::shared_ptr<TestMasterApi> CreateWorkerMasterApi(const HostPort &masterAddress) override
    {
        lastAddress_ = masterAddress;
        return std::make_shared<TestMasterApi>();
    }

    const HostPort &LastAddress() const
    {
        return lastAddress_;
    }

private:
    HostPort lastAddress_;
};
}  // namespace

TEST_F(MetadataRouteResolverTest, CentralizedModeDoesNotRequirePlacement)
{
    worker::MetadataRouteOptions options;
    options.centralizedMode = true;
    options.masterAddress = HostPort("127.0.0.1", 18481);
    worker::MetadataRouteResolver resolver(nullptr, options);
    HostPort owner("unchanged", 1);

    ASSERT_TRUE(resolver.ResolveOwner("object", owner).IsOk());
    EXPECT_EQ(owner, options.masterAddress);
    auto groups = resolver.GroupOwners({ "a", "b" });
    ASSERT_EQ(groups.groups.size(), 1U);
    EXPECT_EQ(groups.topologyVersion, 0U);
    EXPECT_EQ(groups.groups.at(options.masterAddress), (std::vector<std::string>{ "a", "b" }));
}

TEST_F(MetadataRouteResolverTest, DistributedModeResolvesKeyAndPreservesOutputOnFailure)
{
    ASSERT_TRUE(PublishVersion(1).IsOk());
    algorithm_.SetOwnerAddress("127.0.0.1:18482");
    worker::MetadataRouteResolver resolver(&placement_, worker::MetadataRouteOptions{});
    HostPort owner;

    ASSERT_TRUE(resolver.ResolveOwner(std::string_view("key\0suffix", 10), owner).IsOk());
    EXPECT_EQ(owner, HostPort("127.0.0.1", 18482));
    algorithm_.SetLocateStatus(Status(K_NOT_FOUND, "owner missing"));
    HostPort unchanged("127.0.0.1", 18500);
    EXPECT_EQ(resolver.ResolveOwner("missing", unchanged).GetCode(), K_NOT_FOUND);
    EXPECT_EQ(unchanged, HostPort("127.0.0.1", 18500));
}

TEST_F(MetadataRouteResolverTest, MissingPlacementPropagatesNotReady)
{
    worker::MetadataRouteResolver resolver(nullptr, worker::MetadataRouteOptions{});
    HostPort owner("127.0.0.1", 18500);

    EXPECT_EQ(resolver.ResolveOwner("key", owner).GetCode(), K_NOT_READY);
    EXPECT_EQ(owner, HostPort("127.0.0.1", 18500));
    auto groups = resolver.GroupOwners({ "a", "b" });
    EXPECT_EQ(groups.failures.at("a").GetCode(), K_NOT_READY);
    EXPECT_EQ(groups.failures.at("b").GetCode(), K_NOT_READY);
}

TEST_F(MetadataRouteResolverTest, BatchUsesOneSnapshotVersion)
{
    ASSERT_TRUE(PublishVersion(1).IsOk());
    algorithm_.SetOwnerAddress("127.0.0.1:18482");
    algorithm_.SetFirstLocateHook([this]() { EXPECT_TRUE(PublishVersion(2).IsOk()); });
    worker::MetadataRouteResolver resolver(&placement_, worker::MetadataRouteOptions{});

    auto groups = resolver.GroupOwners({ "a", "b" });

    EXPECT_TRUE(groups.failures.empty());
    EXPECT_EQ(groups.topologyVersion, 1U);
    EXPECT_EQ(algorithm_.Versions(), (std::vector<uint64_t>{ 1, 1 }));
    EXPECT_EQ(groups.groups.at(HostPort("127.0.0.1", 18482)).size(), 2U);
}

TEST_F(MetadataRouteResolverTest, ConcurrentPublishDoesNotMixGroupedOwnerSnapshotVersions)
{
    ASSERT_TRUE(PublishVersion(1).IsOk());
    algorithm_.SetOwnerAddress("127.0.0.1:18482");
    std::promise<void> firstLocateStarted;
    auto firstLocateSignal = firstLocateStarted.get_future();
    std::promise<void> continueRouting;
    auto continueSignal = continueRouting.get_future().share();
    algorithm_.SetFirstLocateHook([&firstLocateStarted, continueSignal] {
        firstLocateStarted.set_value();
        continueSignal.wait();
    });
    worker::MetadataRouteResolver resolver(&placement_, worker::MetadataRouteOptions{});
    worker::MetaOwnerRouteGroups groups;
    auto route = std::async(std::launch::async, [&resolver, &groups] {
        groups = resolver.GroupOwners({ "a", "b", "c" });
    });

    const bool routingPaused = firstLocateSignal.wait_for(std::chrono::seconds(2)) == std::future_status::ready;
    if (routingPaused) {
        EXPECT_TRUE(PublishVersion(2).IsOk());
    }
    continueRouting.set_value();
    route.get();

    ASSERT_TRUE(routingPaused);
    EXPECT_TRUE(groups.failures.empty());
    EXPECT_EQ(groups.topologyVersion, 1U);
    EXPECT_EQ(algorithm_.Versions(), (std::vector<uint64_t>{ 1, 1, 1 }));
    EXPECT_EQ(groups.groups.at(HostPort("127.0.0.1", 18482)).size(), 3U);
}

TEST_F(MetadataRouteResolverTest, IndexedBatchBuildsOnlyIndexedProjection)
{
    ASSERT_TRUE(PublishVersion(1).IsOk());
    algorithm_.SetOwnerAddress("127.0.0.1:18482");
    worker::MetadataRouteResolver resolver(&placement_, worker::MetadataRouteOptions{});

    auto groups = resolver.GroupIndexedOwners({ "a", "b" });

    ASSERT_TRUE(groups.failures.empty());
    EXPECT_EQ(groups.topologyVersion, 1U);
    EXPECT_EQ(groups.groups.at(HostPort("127.0.0.1", 18482)),
              (std::vector<std::pair<std::string, size_t>>{ { "a", 0 }, { "b", 1 } }));
}

TEST_F(MetadataRouteResolverTest, BatchFailureDoesNotFallbackToSingleKeyLocate)
{
    algorithm_.SetOwnerAddress("127.0.0.1:18482");
    worker::MetadataRouteResolver resolver(&placement_, worker::MetadataRouteOptions{});
    const std::vector<std::string> keys{ "key-0", "key-1" };

    auto groups = resolver.GroupOwners(keys);

    EXPECT_EQ(algorithm_.LocateCount(), 0U);
    ASSERT_EQ(groups.failures.size(), keys.size());
    EXPECT_EQ(groups.failures.at(keys.front()).GetCode(), K_NOT_READY);
    EXPECT_EQ(groups.failures.at(keys.back()).GetCode(), K_NOT_READY);
    EXPECT_TRUE(groups.groups.empty());
}

TEST_F(MetadataRouteResolverTest, GroupMigrateTargetsFollowsRedirectOverrideDuringScaleIn)
{
    ASSERT_TRUE(PublishTopology(1, cluster::ActiveBatch{ cluster::TopologyChangeType::SCALE_IN, 1 },
                                { { "127.0.0.1:18480", cluster::MemberState::ACTIVE },
                                  { "127.0.0.1:18482", cluster::MemberState::ACTIVE },
                                  { "127.0.0.1:18483", cluster::MemberState::LEAVING } })
                    .IsOk());
    algorithm_.SetOwnerAddress("127.0.0.1:18480");
    algorithm_.SetProspectiveAddress("127.0.0.1:18482");
    worker::MetadataRouteResolver resolver(&placement_, worker::MetadataRouteOptions{});

    auto groups = resolver.GroupMigrateTargets({ "k0", "k1" });

    EXPECT_TRUE(groups.failures.empty());
    EXPECT_EQ(groups.topologyVersion, 1U);
    EXPECT_EQ(groups.groups.at(HostPort("127.0.0.1", 18482)), (std::vector<std::string>{ "k0", "k1" }));
    EXPECT_EQ(groups.groups.count(HostPort("127.0.0.1", 18480)), 0U);
}

TEST_F(MetadataRouteResolverTest, GroupMigrateTargetsKeepsCommittedOwnerWithoutOverride)
{
    ASSERT_TRUE(PublishVersion(1).IsOk());
    worker::MetadataRouteResolver resolver(&placement_, worker::MetadataRouteOptions{});
    algorithm_.RouteToken(1, "127.0.0.1:18482");
    algorithm_.RouteToken(2, "127.0.0.1:18480");
    algorithm_.SetKeyToken("replica-key", 2);

    auto groups = resolver.GroupMigrateTargets({ "own-key", "replica-key" });

    EXPECT_TRUE(groups.failures.empty());
    EXPECT_EQ(groups.groups.at(HostPort("127.0.0.1", 18482)), std::vector<std::string>({ "own-key" }));
    EXPECT_EQ(groups.groups.at(HostPort("127.0.0.1", 18480)), std::vector<std::string>({ "replica-key" }));
}

TEST_F(MetadataRouteResolverTest, GroupMigrateTargetsIgnoresOverrideForWaitDecision)
{
    ASSERT_TRUE(PublishTopology(1, cluster::ActiveBatch{ cluster::TopologyChangeType::SCALE_OUT, 1 },
                                { { "127.0.0.1:18480", cluster::MemberState::ACTIVE },
                                  { "127.0.0.1:18482", cluster::MemberState::ACTIVE },
                                  { "127.0.0.1:18483", cluster::MemberState::JOINING } })
                    .IsOk());
    algorithm_.SetOwnerAddress("127.0.0.1:18480");
    algorithm_.SetProspectiveAddress("127.0.0.1:18482");
    worker::MetadataRouteResolver resolver(&placement_, worker::MetadataRouteOptions{});

    auto groups = resolver.GroupMigrateTargets({ "k0" });

    EXPECT_TRUE(groups.failures.empty());
    EXPECT_EQ(groups.groups.at(HostPort("127.0.0.1", 18480)), std::vector<std::string>({ "k0" }));
    EXPECT_EQ(groups.groups.count(HostPort("127.0.0.1", 18482)), 0U);
}

TEST_F(MetadataRouteResolverTest, GroupMigrateTargetsSplitsOversizedKeysetsIntoCompleteChunks)
{
    ASSERT_TRUE(PublishVersion(1).IsOk());
    algorithm_.RouteToken(1, "127.0.0.1:18482");
    algorithm_.RouteToken(2, "127.0.0.1:18483");
    worker::MetadataRouteResolver resolver(&placement_, worker::MetadataRouteOptions{});
    constexpr size_t keyCount = cluster::PlacementFacade::kMaxBatchKeys + 1;
    std::vector<std::string> keys;
    std::unordered_map<std::string, HostPort> expectedOwners;
    keys.reserve(keyCount);
    for (size_t index = 0; index < keyCount; ++index) {
        keys.emplace_back("ck-" + std::to_string(index));
        expectedOwners.emplace(keys.back(),
                               HostPort("127.0.0.1", index % 2 == 0 ? 18482 : 18483));
        if (index % 2 == 1) {
            algorithm_.SetKeyToken(keys.back(), 2);
        }
    }

    auto groups = resolver.GroupMigrateTargets(keys);

    EXPECT_TRUE(groups.failures.empty());
    std::unordered_set<std::string> routed;
    for (const auto &[owner, groupKeys] : groups.groups) {
        for (const auto &key : groupKeys) {
            EXPECT_EQ(routed.emplace(key).second, true) << key;
            EXPECT_EQ(expectedOwners.at(key), owner) << key;
        }
    }
    EXPECT_EQ(routed.size(), keyCount);
}

TEST_F(MetadataRouteResolverTest, GroupMigrateTargetsCentralizedModeGroupsEverythingOnMaster)
{
    worker::MetadataRouteOptions options;
    options.centralizedMode = true;
    options.masterAddress = HostPort("127.0.0.1", 18481);
    worker::MetadataRouteResolver resolver(nullptr, options);

    auto groups = resolver.GroupMigrateTargets({ "a", "b" });

    EXPECT_TRUE(groups.failures.empty());
    EXPECT_EQ(groups.groups.at(options.masterAddress), (std::vector<std::string>{ "a", "b" }));
}

TEST_F(MetadataRouteResolverTest, ApiManagerUsesBoundResolverAndKeepsAddressOverload)
{
    worker::MetadataRouteOptions options;
    options.centralizedMode = true;
    options.masterAddress = HostPort("127.0.0.1", 18482);
    worker::MetadataRouteResolver resolver(nullptr, options);
    HostPort workerAddress("127.0.0.1", 18481);
    TestMasterApiManager manager(workerAddress, resolver);
    std::shared_ptr<TestMasterApi> api;

    ASSERT_TRUE(manager.GetWorkerMasterApi("key", api).IsOk());
    ASSERT_NE(api, nullptr);
    EXPECT_TRUE(api->Initialized());
    EXPECT_EQ(manager.LastAddress(), options.masterAddress);
    ASSERT_TRUE(manager.GetWorkerMasterApi(HostPort("127.0.0.1", 18483), api).IsOk());
    EXPECT_EQ(manager.LastAddress(), HostPort("127.0.0.1", 18483));
}
}  // namespace datasystem::ut
