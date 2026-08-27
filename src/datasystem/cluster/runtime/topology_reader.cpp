/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: One-shot authoritative cluster topology reader.
 */
#include "datasystem/cluster/runtime/topology_reader.h"

#include "datasystem/cluster/repository/topology_repository_codec.h"
#include "datasystem/common/ak_sk/hasher.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem::cluster {

TopologyReader::TopologyReader(TopologyRepository &repository) : repository_(repository)
{
}

Status TopologyReader::Read(int32_t timeoutMs, std::shared_ptr<const TopologySnapshot> &snapshot) const
{
    CHECK_FAIL_RETURN_STATUS(timeoutMs > 0, K_INVALID, "cluster topology read timeout must be positive");
    TopologyState state;
    int64_t revision = 0;
    RETURN_IF_NOT_OK(repository_.ReadTopology(timeoutMs, state, revision));
    std::string canonical;
    RETURN_IF_NOT_OK(TopologyRepositoryCodec::EncodeTopology(state, canonical));
    std::string digest;
    Hasher hasher;
    RETURN_IF_NOT_OK(hasher.GetSha256Hex(canonical, digest));
    std::shared_ptr<const TopologySnapshot> candidate;
    RETURN_IF_NOT_OK(TopologySnapshot::Create(std::move(state), revision, std::move(digest), candidate));
    snapshot = std::move(candidate);
    return Status::OK();
}

Status TopologyReader::ReadIfChanged(int32_t timeoutMs, int64_t knownAuthorityRevision,
                                     std::shared_ptr<const TopologySnapshot> &snapshot, bool &unchanged) const
{
    CHECK_FAIL_RETURN_STATUS(timeoutMs > 0, K_INVALID, "cluster topology read timeout must be positive");
    CHECK_FAIL_RETURN_STATUS(knownAuthorityRevision > 0, K_INVALID, "known authority revision must be positive");
    TopologyState state;
    int64_t revision = 0;
    RETURN_IF_NOT_OK(
        repository_.ReadTopologyIfChanged(timeoutMs, knownAuthorityRevision, state, revision, unchanged));
    if (unchanged) {
        return Status::OK();
    }
    std::string canonical;
    RETURN_IF_NOT_OK(TopologyRepositoryCodec::EncodeTopology(state, canonical));
    std::string digest;
    Hasher hasher;
    RETURN_IF_NOT_OK(hasher.GetSha256Hex(canonical, digest));
    std::shared_ptr<const TopologySnapshot> candidate;
    RETURN_IF_NOT_OK(TopologySnapshot::Create(std::move(state), revision, std::move(digest), candidate));
    snapshot = std::move(candidate);
    return Status::OK();
}

}  // namespace datasystem::cluster
