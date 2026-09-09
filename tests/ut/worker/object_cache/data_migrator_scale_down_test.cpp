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
 * Description: DataMigrator scale-down target selection and redirect retry tests.
 */

#include <atomic>
#include <chrono>
#include <cstdint>
#include <map>
#include <unordered_map>
#include <memory>
#include <string>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "ut/common.h"
#include "../../../common/binmock/binmock.h"
#include "datasystem/cluster/algorithm/hash_algorithm.h"
#include "datasystem/cluster/membership/membership_endpoint_view.h"
#include "datasystem/cluster/model/topology_types.h"
#include "datasystem/cluster/routing/placement_facade.h"
#include "datasystem/cluster/runtime/topology_snapshot_state.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/rpc/mem_view.h"
#include "datasystem/common/rpc/rpc_stub_cache_mgr.h"
#include "datasystem/common/shared_memory/allocator.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/l2cache/slot_client/slot_internal_config.h"
#include "datasystem/common/util/hash_algorithm.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/uuid_generator.h"
#include "datasystem/protos/worker_object.pb.h"
#include "datasystem/worker/metadata_route_resolver.h"
#include "datasystem/worker/object_cache/data_migrator/data_migrator.h"
#include "datasystem/worker/object_cache/object_endpoint_policy.h"
#include "datasystem/worker/object_cache/worker_worker_oc_api.h"
#include "eviction_manager_common.h"

using namespace datasystem::object_cache;
using namespace datasystem::worker;
using namespace ::testing;

namespace datasystem {
namespace ut {
namespace {
constexpr const char *LOCAL_ADDR = "127.0.0.1:18491";
constexpr const char *TARGET_ADDR = "127.0.0.1:18492";
constexpr const char *SPARE_ADDR = "127.0.0.1:18493";
constexpr const char *FAR_ADDR = "127.0.0.1:18494";

std::vector<uint32_t> MakeMemberTokens(const std::string &address)
{
    constexpr uint32_t tokenCount = 4;
    std::vector<uint32_t> tokens;
    tokens.reserve(tokenCount);
    for (uint32_t index = 0; index < tokenCount; ++index) {
        tokens.emplace_back(cluster::HashAlgorithm::MakeToken(address, index, 0));
    }
    return tokens;
}

cluster::Member MakeMember(const std::string &address)
{
    cluster::Member member;
    member.identity.id = std::string(16, address.back());
    member.identity.address = address;
    member.state = cluster::MemberState::ACTIVE;
    member.tokens = MakeMemberTokens(address);
    return member;
}

class ScaleDownTestRoutingAlgorithm final : public cluster::IRoutingAlgorithm {
public:
    ~ScaleDownTestRoutingAlgorithm() override = default;

    cluster::TopologyAlgorithmId GetId() const override
    {
        return "scale-down-migrate-test";
    }

    uint32_t Hash(std::string_view placementKey) const noexcept override
    {
        auto iter = tokenByKey_.find(std::string(placementKey));
        return iter == tokenByKey_.end() ? kDefaultToken : iter->second;
    }

    Status LocateOwner(const cluster::TopologySnapshot &, uint32_t token, const cluster::Member *&owner) const override
    {
        return Locate(token, committedByToken_, owner);
    }

    Status LocateProspectiveOwner(const cluster::TopologySnapshot &, uint32_t token,
                                  const cluster::Member *&owner) const override
    {
        return Locate(token, prospectiveByToken_, owner);
    }

    void RouteKey(const std::string &key, uint32_t token, const std::string &committed,
                  const std::string &prospective = "")
    {
        tokenByKey_[key] = token;
        committedByToken_[token] = MakeMember(committed);
        if (!prospective.empty()) {
            prospectiveByToken_[token] = MakeMember(prospective);
        }
    }

private:
    static constexpr uint32_t kDefaultToken = 1;

    Status Locate(uint32_t token, const std::map<uint32_t, cluster::Member> &owners,
                  const cluster::Member *&owner) const
    {
        auto iter = owners.find(token);
        CHECK_FAIL_RETURN_STATUS(iter != owners.end(), K_NOT_FOUND, "scale-down test route missing");
        owner = &iter->second;
        return Status::OK();
    }

    std::map<std::string, uint32_t> tokenByKey_;
    std::map<uint32_t, cluster::Member> committedByToken_;
    std::map<uint32_t, cluster::Member> prospectiveByToken_;
};

struct MigrateMockState {
    std::atomic<int> probeCalls{ 0 };
    std::atomic<int> sendCalls{ 0 };
    std::atomic<int> b1Sends{ 0 };
    std::atomic<int> a2Sends{ 0 };
};
}  // namespace

class DataMigratorScaleDownTest : public CommonTest, public EvictionManagerCommon {
public:
    void SetUp() override
    {
        CommonTest::SetUp();
        objectTable_ = std::make_shared<ObjectTable>();
        allocator = datasystem::memory::Allocator::Instance();
        allocator->Init(64UL * 1024UL * 1024UL);
        ASSERT_TRUE(RpcStubCacheMgr::Instance().Init(100).IsOk());

        cluster::TopologyState topology;
        topology.clusterHasInit = true;
        topology.version = 1;
        topology.members = { MakeMember(LOCAL_ADDR), MakeMember(TARGET_ADDR), MakeMember(SPARE_ADDR),
                             MakeMember(FAR_ADDR) };
        std::shared_ptr<const cluster::TopologySnapshot> snapshot;
        ASSERT_TRUE(cluster::TopologySnapshot::Create(std::move(topology), 1, std::string(64, 'a'), snapshot).IsOk());
        cluster::SnapshotUpdateOutcome outcome;
        ASSERT_TRUE(snapshots_.Publish(std::move(snapshot), outcome).IsOk());

        membership_ = std::make_unique<cluster::MembershipEndpointView>(snapshots_);
        facade_ = std::make_unique<cluster::PlacementFacade>(snapshots_, algorithm_, LOCAL_ADDR);
        resolver_ = std::make_unique<MetadataRouteResolver>(&*facade_, MetadataRouteOptions{});
        policy_ = std::make_unique<ObjectEndpointPolicy>(*resolver_, *membership_);
        DS_ASSERT_OK(local_.ParseString(LOCAL_ADDR));
    }

    void TearDown() override
    {
        RELEASE_STUBS;
        migrator_.reset();
        policy_.reset();
        resolver_.reset();
        facade_.reset();
        membership_.reset();
        objectTable_.reset();
        if (allocator != nullptr) {
            allocator->ResetForTest();
            allocator = nullptr;
        }
        CommonTest::TearDown();
    }

    void CreateMigrator(std::chrono::steady_clock::time_point deadline)
    {
        auto akSkManager = std::make_shared<AkSkManager>(0);
        migrator_ = std::make_unique<DataMigrator>(MigrateType::SCALE_DOWN, *resolver_, *membership_, *policy_,
                                                   nullptr, local_, akSkManager, objectTable_, "ut-scale-down-task",
                                                   DataMigrator::UNLIMITED_RETRY_COUNT, deadline);
        migrator_->Init();
    }

    void MockRemoteMigrateData(MigrateMockState &state, double probeRatio, const std::string &flakyKey,
                               const std::string &alwaysFailKey)
    {
        BINEXPECT_CALL(&WorkerRemoteWorkerOCApi::MigrateData, (_, _, _))
            .WillRepeatedly(Invoke([&state, probeRatio, flakyKey, alwaysFailKey](
                                       MigrateDataReqPb &req, const std::vector<MemView> &payloads,
                                       MigrateDataRspPb &rsp) {
                if (payloads.empty()) {
                    ++state.probeCalls;
                    rsp.set_available_ratio(probeRatio);
                    rsp.set_remain_bytes(1UL << 30);
                    rsp.set_disk_remain_bytes(1UL << 30);
                    rsp.set_limit_rate(1UL << 26);
                    rsp.set_scale_down_state(MigrateDataRspPb::NONE);
                    return Status::OK();
                }
                ++state.sendCalls;
                rsp.set_remain_bytes(1UL << 30);
                rsp.set_limit_rate(1UL << 26);
                for (const auto &object : req.objects()) {
                    const auto &key = object.object_key();
                    if (key == alwaysFailKey) {
                        ++state.b1Sends;
                        rsp.add_fail_ids(key);
                        continue;
                    }
                    if (key == flakyKey) {
                        ++state.a2Sends;
                        if (state.a2Sends.load() == 1) {
                            rsp.add_fail_ids(key);
                            continue;
                        }
                    }
                    rsp.add_success_ids(key);
                }
                return Status::OK();
            }));
    }

protected:
    ScaleDownTestRoutingAlgorithm algorithm_;
    cluster::TopologySnapshotState snapshots_;
    std::unique_ptr<cluster::MembershipEndpointView> membership_;
    std::unique_ptr<cluster::PlacementFacade> facade_;
    std::unique_ptr<MetadataRouteResolver> resolver_;
    std::unique_ptr<ObjectEndpointPolicy> policy_;
    std::unique_ptr<DataMigrator> migrator_;
    HostPort local_;
};

TEST_F(DataMigratorScaleDownTest, GroupedRedirectRetryEscalatesStageAndCompletes)
{
    MigrateMockState state;
    MockRemoteMigrateData(state, 10.0, "", "");
    const std::vector<std::string> keys{ "k0", "k1", "k2", "k3", "k4" };
    for (const auto &key : keys) {
        algorithm_.RouteKey(key, 2, TARGET_ADDR);
        DS_ASSERT_OK(CreateObject(key, 1024));
    }
    CreateMigrator(std::chrono::steady_clock::now() + std::chrono::seconds(5));

    Status rc = migrator_->Migrate(keys, {});

    ASSERT_TRUE(rc.IsOk()) << rc.ToString();
    std::vector<std::string> failedKeys;
    migrator_->GetFailedKeys(failedKeys);
    EXPECT_TRUE(failedKeys.empty()) << VectorToString(failedKeys);
    EXPECT_GE(state.probeCalls.load(), 3);
}

TEST_F(DataMigratorScaleDownTest, RouteFailureKeysFallBackToStandbyInsteadOfEmptyAddress)
{
    MigrateMockState state;
    MockRemoteMigrateData(state, 100.0, "", "");
    const std::vector<std::string> keys{ "f0", "f1", "f2" };
    for (const auto &key : keys) {
        DS_ASSERT_OK(CreateObject(key, 1024));
    }
    CreateMigrator(std::chrono::steady_clock::now() + std::chrono::seconds(5));

    Status rc = migrator_->Migrate(keys, {});

    ASSERT_TRUE(rc.IsOk()) << rc.ToString();
    std::vector<std::string> failedKeys;
    migrator_->GetFailedKeys(failedKeys);
    EXPECT_TRUE(failedKeys.empty()) << VectorToString(failedKeys);
    EXPECT_GT(state.sendCalls.load(), 0);
}

TEST_F(DataMigratorScaleDownTest, SameSlotSiblingGroupsKeepIndependentRetryBudgets)
{
    MigrateMockState state;
    const std::vector<std::string> keys{ "a1", "a2", "b1" };
    uint32_t slot = 0;
    std::unordered_map<std::string, std::string> slotMatchedKeys;
    for (const auto &key : keys) {
        for (uint32_t candidate = 0;; ++candidate) {
            const std::string candidateKey = key + "_" + std::to_string(candidate);
            const uint32_t candidateSlot = MurmurHash3_32(candidateKey) % DISTRIBUTED_DISK_SLOT_NUM;
            if (key == keys.front()) {
                slot = candidateSlot;
                slotMatchedKeys.emplace(key, candidateKey);
                break;
            }
            if (candidateSlot == slot) {
                slotMatchedKeys.emplace(key, candidateKey);
                break;
            }
        }
    }
    MockRemoteMigrateData(state, 100.0, slotMatchedKeys.at("a2"), slotMatchedKeys.at("b1"));
    algorithm_.RouteKey(slotMatchedKeys.at("a1"), 2, TARGET_ADDR);
    algorithm_.RouteKey(slotMatchedKeys.at("a2"), 2, TARGET_ADDR);
    algorithm_.RouteKey(slotMatchedKeys.at("b1"), 3, SPARE_ADDR);
    for (const auto &key : keys) {
        DS_ASSERT_OK(CreateObject(slotMatchedKeys.at(key), 1024));
    }
    CreateMigrator(std::chrono::steady_clock::now() + std::chrono::seconds(30));

    std::vector<std::string> slotKeys;
    for (const auto &key : keys) {
        slotKeys.emplace_back(slotMatchedKeys.at(key));
    }
    Status rc = migrator_->MigrateL2CacheBySlot(slotKeys);

    ASSERT_TRUE(rc.IsOk()) << rc.ToString();
    std::vector<std::string> failedKeys;
    migrator_->GetFailedKeys(failedKeys);
    EXPECT_EQ(failedKeys, std::vector<std::string>{ slotMatchedKeys.at("b1") });
    EXPECT_EQ(state.b1Sends.load(), 2);
    EXPECT_EQ(state.a2Sends.load(), 2);
}
}  // namespace ut
}  // namespace datasystem
