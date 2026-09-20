/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: One-shot authoritative cluster topology reader.
 */
#include "datasystem/cluster/runtime/topology_reader.h"

#include <utility>

#include "datasystem/cluster/repository/topology_repository_codec.h"
#include "datasystem/common/ak_sk/hasher.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem::cluster {

TopologyReader::TopologyReader(TopologyRepository &repository) : repository_(repository)
{
}

Status TopologyReader::ReadTopologyOnly(int32_t timeoutMs, std::shared_ptr<const TopologySnapshot> &snapshot) const
{
    CHECK_FAIL_RETURN_STATUS(timeoutMs > 0, K_INVALID, "cluster topology read timeout must be positive");
    TopologyState state;
    int64_t revision = 0;
    std::string coordinatorId;
    RETURN_IF_NOT_OK(repository_.ReadTopology(timeoutMs, state, revision, &coordinatorId));
    return BuildFromState(std::move(state), revision, {}, snapshot, 0, std::move(coordinatorId));
}

Status TopologyReader::Read(int32_t timeoutMs, std::shared_ptr<const TopologySnapshot> &snapshot) const
{
    CHECK_FAIL_RETURN_STATUS(timeoutMs > 0, K_INVALID, "cluster topology read timeout must be positive");
    TopologyState state;
    int64_t revision = 0;
    std::string coordinatorId;
    RETURN_IF_NOT_OK(repository_.ReadTopology(timeoutMs, state, revision, &coordinatorId));
    std::unordered_map<std::string, std::string> hostIds;
    int64_t hostIdsRevision = 0;
    (void)repository_.ReadHostIds(hostIds, &hostIdsRevision);
    return BuildFromState(std::move(state), revision, std::move(hostIds), snapshot, hostIdsRevision,
                          std::move(coordinatorId));
}

Status TopologyReader::BuildFromEncodedTopology(const std::string &value, int64_t authorityRevision,
                                                std::unordered_map<std::string, std::string> hostIds,
                                                std::shared_ptr<const TopologySnapshot> &snapshot,
                                                int64_t hostIdsRevision, std::string coordinatorId)
{
    CHECK_FAIL_RETURN_STATUS(authorityRevision > 0, K_INVALID, "topology authority revision must be positive");
    TopologyState state;
    RETURN_IF_NOT_OK(TopologyRepositoryCodec::DecodeTopology(value, state));
    return BuildFromState(std::move(state), authorityRevision, std::move(hostIds), snapshot, hostIdsRevision,
                          std::move(coordinatorId));
}

Status TopologyReader::BuildFromState(TopologyState state, int64_t authorityRevision,
                                      std::unordered_map<std::string, std::string> hostIds,
                                      std::shared_ptr<const TopologySnapshot> &snapshot, int64_t hostIdsRevision,
                                      std::string coordinatorId)
{
    std::string canonical;
    RETURN_IF_NOT_OK(TopologyRepositoryCodec::EncodeTopology(state, canonical));
    std::string digest;
    Hasher hasher;
    RETURN_IF_NOT_OK(hasher.GetSha256Hex(canonical, digest));
    std::shared_ptr<const TopologySnapshot> candidate;
    RETURN_IF_NOT_OK(TopologySnapshot::Create(std::move(state), authorityRevision, std::move(digest), candidate,
                                              std::move(hostIds), hostIdsRevision, std::move(coordinatorId)));
    snapshot = std::move(candidate);
    return Status::OK();
}

Status TopologyReader::ReadIfChanged(int32_t timeoutMs, const TopologySnapshot &knownSnapshot,
                                     std::shared_ptr<const TopologySnapshot> &snapshot, bool &unchanged) const
{
    const auto knownAuthorityRevision = knownSnapshot.AuthorityRevision();
    CHECK_FAIL_RETURN_STATUS(timeoutMs > 0, K_INVALID, "cluster topology read timeout must be positive");
    CHECK_FAIL_RETURN_STATUS(knownAuthorityRevision > 0, K_INVALID, "known authority revision must be positive");
    TopologyState state;
    int64_t revision = 0;
    std::string coordinatorId;
    RETURN_IF_NOT_OK(repository_.ReadTopologyIfChanged(timeoutMs, knownAuthorityRevision, knownSnapshot.CoordinatorId(),
                                                       state, revision, coordinatorId, unchanged));
    std::unordered_map<std::string, std::string> hostIds;
    int64_t hostIdsRevision = 0;
    (void)repository_.ReadHostIds(hostIds, &hostIdsRevision);
    if (unchanged) {
        if (hostIdsRevision == 0
            || (knownSnapshot.HostIdsRevision() > 0 && knownSnapshot.HostIds() == hostIds)) {
            return Status::OK();
        }
        state = knownSnapshot.CopyState();
        revision = knownAuthorityRevision;
        unchanged = false;
    }
    return BuildFromState(std::move(state), revision, std::move(hostIds), snapshot, hostIdsRevision,
                          std::move(coordinatorId));
}

}  // namespace datasystem::cluster
