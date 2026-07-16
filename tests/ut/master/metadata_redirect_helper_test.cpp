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
#include "gtest/gtest.h"
#include "ut/common.h"

namespace datasystem::master {
namespace {

class MetadataRedirectHelperForTest final : public MetadataRedirectHelper {
public:
    explicit MetadataRedirectHelperForTest(worker::WorkerTopologyReferences *references)
        : MetadataRedirectHelper(references)
    {
    }

    using MetadataRedirectHelper::CheckNeedToRedirectOrNot;

    bool metaFound{ false };
    bool migrating{ false };

    bool ItemIsMigrating(const std::string &id) override
    {
        (void)id;
        return migrating;
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
    cluster::HashAlgorithm algorithm;
    cluster::PlacementFacade placement(snapshots, algorithm);
    worker::WorkerTopologyReferences references;
    references.placement = &placement;
    references.localAddress = "127.0.0.1:1";
    MetadataRedirectHelperForTest helper(&references);

    bool redirect = true;
    std::string target;
    uint64_t topologyVersion = 0;
    helper.metaFound = true;
    helper.CheckNeedToRedirectOrNot("key", redirect, target, topologyVersion);
    EXPECT_FALSE(redirect);

    helper.migrating = true;
    helper.CheckNeedToRedirectOrNot("key", redirect, target, topologyVersion);
    EXPECT_TRUE(redirect);

    helper.metaFound = false;
    helper.migrating = false;
    helper.CheckNeedToRedirectOrNot("key", redirect, target, topologyVersion);
    EXPECT_TRUE(redirect);
    EXPECT_EQ(target, "127.0.0.1:2");
    EXPECT_EQ(topologyVersion, 2);
}

}  // namespace
}  // namespace datasystem::master
