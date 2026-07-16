/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Controllable real PlacementFacade fixture for Worker object-cache tests.
 */
#ifndef DATASYSTEM_TESTS_UT_WORKER_OBJECT_CACHE_TEST_PLACEMENT_FACADE_H
#define DATASYSTEM_TESTS_UT_WORKER_OBJECT_CACHE_TEST_PLACEMENT_FACADE_H

#include <cstdint>
#include <string>
#include <string_view>
#include <unordered_map>

#include "datasystem/cluster/algorithm/topology_algorithm.h"
#include "datasystem/cluster/routing/placement_facade.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem::ut {

class TestRoutingAlgorithm final : public cluster::IRoutingAlgorithm {
public:
    ~TestRoutingAlgorithm() override = default;

    cluster::TopologyAlgorithmId GetId() const override
    {
        return "worker-test";
    }

    uint32_t Hash(std::string_view placementKey) const noexcept override
    {
        auto iter = tokenByKey_.find(std::string(placementKey));
        return iter == tokenByKey_.end() ? 0 : iter->second;
    }

    Status LocateOwner(const cluster::TopologySnapshot &, uint32_t token, const cluster::Member *&owner) const override
    {
        auto iter = ownerByToken_.find(token);
        CHECK_FAIL_RETURN_STATUS(iter != ownerByToken_.end(), K_NOT_FOUND, "test route missing");
        owner = &iter->second;
        return Status::OK();
    }

    Status LocateProspectiveOwner(const cluster::TopologySnapshot &snapshot, uint32_t token,
                                  const cluster::Member *&owner) const override
    {
        return LocateOwner(snapshot, token, owner);
    }

    void SetOwner(const std::string &key, cluster::MemberIdentity owner)
    {
        auto [iter, inserted] = tokenByKey_.emplace(key, nextToken_);
        if (inserted) {
            ++nextToken_;
        }
        cluster::Member member;
        member.identity = std::move(owner);
        member.state = cluster::MemberState::ACTIVE;
        ownerByToken_[iter->second] = std::move(member);
    }

    void Clear()
    {
        tokenByKey_.clear();
        ownerByToken_.clear();
        nextToken_ = 1;
    }

private:
    uint32_t nextToken_{ 1 };
    std::unordered_map<std::string, uint32_t> tokenByKey_;
    std::unordered_map<uint32_t, cluster::Member> ownerByToken_;
};

class TestPlacementFacade final {
public:
    TestPlacementFacade() : facade_(snapshots_, algorithm_, "127.0.0.1:1")
    {
        cluster::TopologyState topology;
        topology.version = 1;
        std::shared_ptr<const cluster::TopologySnapshot> snapshot;
        auto rc = cluster::TopologySnapshot::Create(topology, 1, std::string(64, 'a'), snapshot);
        if (rc.IsOk()) {
            cluster::SnapshotUpdateOutcome outcome;
            rc = snapshots_.Publish(std::move(snapshot), outcome);
        }
        initStatus_ = std::move(rc);
    }

    ~TestPlacementFacade() = default;

    void SetOwner(const std::string &key, const HostPort &address, const std::string &id = "")
    {
        algorithm_.SetOwner(key, { id, address.ToString() });
    }

    void Clear()
    {
        algorithm_.Clear();
    }

    const cluster::PlacementFacade *operator&() const
    {
        return initStatus_.IsOk() ? &facade_ : nullptr;
    }

private:
    TestRoutingAlgorithm algorithm_;
    cluster::TopologySnapshotState snapshots_;
    cluster::PlacementFacade facade_;
    Status initStatus_;
};

}  // namespace datasystem::ut

#endif  // DATASYSTEM_TESTS_UT_WORKER_OBJECT_CACHE_TEST_PLACEMENT_FACADE_H
