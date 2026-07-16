/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Metadata redirect handoff tests.
 */
#include "datasystem/master/metadata_redirect_helper.h"

#include <limits>

#include "datasystem/cluster/algorithm/hash_algorithm.h"
#include "datasystem/cluster/routing/placement_facade.h"
#include "datasystem/protos/master_object.pb.h"
#include "gtest/gtest.h"
#include "ut/common.h"

namespace datasystem::master {
namespace {

class CountingRoutingAlgorithm final : public cluster::IRoutingAlgorithm {
public:
    /**
     * @brief Destroy the counting routing test double.
     */
    ~CountingRoutingAlgorithm() override = default;

    cluster::TopologyAlgorithmId GetId() const override
    {
        return algorithm_.GetId();
    }

    uint32_t Hash(std::string_view placementKey) const noexcept override
    {
        return algorithm_.Hash(placementKey);
    }

    Status LocateOwner(const cluster::TopologySnapshot &snapshot, uint32_t token,
                       const cluster::Member *&owner) const override
    {
        ++locateCount_;
        return algorithm_.LocateOwner(snapshot, token, owner);
    }

    Status LocateProspectiveOwner(const cluster::TopologySnapshot &snapshot, uint32_t token,
                                  const cluster::Member *&owner) const override
    {
        return algorithm_.LocateProspectiveOwner(snapshot, token, owner);
    }

    size_t LocateCount() const
    {
        return locateCount_;
    }

private:
    cluster::HashAlgorithm algorithm_;
    mutable size_t locateCount_{ 0 };
};

class MetadataRedirectHelperForTest final : public MetadataRedirectHelper {
public:
    MetadataRedirectHelperForTest(const cluster::PlacementFacade *placement, bool centralizedMetadata,
                                  HostPort metadataAddress)
        : MetadataRedirectHelper(placement, centralizedMetadata, std::move(metadataAddress))
    {
    }

    using MetadataRedirectHelper::CheckNeedToRedirectOrNot;
    using MetadataRedirectHelper::EvaluateMetadataRedirect;
    using MetadataRedirectHelper::FillRedirectResponseInfo;
    using MetadataRedirectHelper::FillRedirectResponseInfos;

    bool metaFound{ false };

    void MarkMigrating(const std::string &id)
    {
        TbbMigratingTable::accessor accessor;
        if (migratingItems_.insert(accessor, id)) {
            accessor->second = true;
        }
    }

protected:
    bool MetaIsFound(const std::string &id) override
    {
        (void)id;
        return metaFound;
    }
};

TEST(MetadataRedirectHelperTest, ServesThenWaitsAndRedirectsDuringScaleInHandoff)
{
    cluster::TopologyState topology;
    topology.version = 2;
    topology.clusterHasInit = true;
    topology.activeBatch = cluster::ActiveBatch{ cluster::TopologyChangeType::SCALE_IN, 2 };
    topology.members = {
        cluster::Member{ { std::string(16, 'a'), "127.0.0.1:1" }, cluster::MemberState::LEAVING,
                         { std::numeric_limits<uint32_t>::max() } },
        cluster::Member{ { std::string(16, 'b'), "127.0.0.1:2" }, cluster::MemberState::ACTIVE, { 0 } },
    };
    std::shared_ptr<const cluster::TopologySnapshot> snapshot;
    DS_ASSERT_OK(cluster::TopologySnapshot::Create(topology, 2, std::string(64, 'a'), snapshot));
    cluster::TopologySnapshotState snapshots;
    cluster::SnapshotUpdateOutcome outcome;
    DS_ASSERT_OK(snapshots.Publish(snapshot, outcome));
    CountingRoutingAlgorithm algorithm;
    cluster::PlacementFacade placement(snapshots, algorithm, "127.0.0.1:1");
    MetadataRedirectHelperForTest helper(&placement, false, HostPort());

    bool redirect = true;
    std::string target;
    uint64_t topologyVersion = 0;
    helper.metaFound = true;
    DS_ASSERT_OK(helper.CheckNeedToRedirectOrNot("key", redirect, target, topologyVersion));
    EXPECT_FALSE(redirect);

    helper.MarkMigrating("key");
    DS_ASSERT_OK(helper.CheckNeedToRedirectOrNot("key", redirect, target, topologyVersion));
    EXPECT_TRUE(redirect);

    helper.metaFound = false;
    helper.CleanMigratingItems({ "key" });
    DS_ASSERT_OK(helper.CheckNeedToRedirectOrNot("key", redirect, target, topologyVersion));
    EXPECT_TRUE(redirect);
    EXPECT_EQ(target, "127.0.0.1:2");
    EXPECT_EQ(topologyVersion, 2);
    EXPECT_EQ(algorithm.LocateCount(), 3U);
}

TEST(MetadataRedirectHelperTest, RouteFailureIsReturnedWithoutChangingOutputs)
{
    MetadataRedirectHelperForTest helper(nullptr, false, HostPort());
    bool redirect = true;
    std::string target = "unchanged";
    uint64_t topologyVersion = 7;

    EXPECT_EQ(helper.CheckNeedToRedirectOrNot("key", redirect, target, topologyVersion).GetCode(), K_NOT_READY);
    EXPECT_TRUE(redirect);
    EXPECT_EQ(target, "unchanged");
    EXPECT_EQ(topologyVersion, 7U);
}

TEST(MetadataRedirectHelperTest, ScaleOutWaitIsReturnedAsMetadataMoving)
{
    cluster::TopologyState topology;
    topology.version = 2;
    topology.clusterHasInit = true;
    topology.activeBatch = cluster::ActiveBatch{ cluster::TopologyChangeType::SCALE_OUT, 2 };
    topology.members = {
        cluster::Member{ { std::string(16, 'a'), "127.0.0.1:1" }, cluster::MemberState::ACTIVE, { 0 } },
        cluster::Member{ { std::string(16, 'b'), "127.0.0.1:2" }, cluster::MemberState::JOINING,
                         { std::numeric_limits<uint32_t>::max() } },
    };
    std::shared_ptr<const cluster::TopologySnapshot> snapshot;
    DS_ASSERT_OK(cluster::TopologySnapshot::Create(topology, 2, std::string(64, 'a'), snapshot));
    cluster::TopologySnapshotState snapshots;
    cluster::SnapshotUpdateOutcome outcome;
    DS_ASSERT_OK(snapshots.Publish(snapshot, outcome));
    cluster::HashAlgorithm algorithm;
    cluster::PlacementFacade placement(snapshots, algorithm, "127.0.0.1:1");
    MetadataRedirectHelperForTest helper(&placement, false, HostPort());

    CreateMetaRspPb singleResponse;
    bool redirect = true;
    DS_ASSERT_OK(helper.FillRedirectResponseInfo(singleResponse, "key", redirect));
    EXPECT_TRUE(redirect);
    EXPECT_TRUE(singleResponse.meta_is_moving());
    EXPECT_FALSE(singleResponse.has_info());

    CreateMultiMetaRspPb batchResponse;
    std::vector<std::string> keys{ "key" };
    DS_ASSERT_OK(helper.FillRedirectResponseInfos(batchResponse, keys, true));
    EXPECT_TRUE(batchResponse.meta_is_moving());
    EXPECT_EQ(batchResponse.info_size(), 0);
}

TEST(MetadataRedirectHelperTest, CentralizedMetadataDoesNotRequirePlacement)
{
    MetadataRedirectHelperForTest helper(nullptr, true, HostPort("127.0.0.1", 18481));
    MetaRedirectDecision decision;

    DS_ASSERT_OK(helper.EvaluateMetadataRedirect("key", decision));
    EXPECT_FALSE(decision.redirect);
    EXPECT_FALSE(decision.moving);
    EXPECT_EQ(decision.targetAddress, "127.0.0.1:18481");
    EXPECT_EQ(decision.topologyVersion, 0U);
}

}  // namespace
}  // namespace datasystem::master
