/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: One-shot authoritative cluster topology reader.
 */
#ifndef DATASYSTEM_CLUSTER_RUNTIME_TOPOLOGY_READER_H
#define DATASYSTEM_CLUSTER_RUNTIME_TOPOLOGY_READER_H

#include <memory>
#include <string>
#include <unordered_map>

#include "datasystem/cluster/model/topology_snapshot.h"
#include "datasystem/cluster/repository/topology_repository.h"

namespace datasystem::cluster {

/**
 * @brief Stateless exact reader and immutable Snapshot builder.
 */
class TopologyReader final {
public:
    /**
     * @brief Bind a repository.
     * @param[in] repository Repository that outlives this reader.
     */
    explicit TopologyReader(TopologyRepository &repository);

    /**
     * @brief Destroy the stateless reader.
     */
    ~TopologyReader() = default;
    TopologyReader(const TopologyReader &) = delete;
    TopologyReader &operator=(const TopologyReader &) = delete;

    /**
     * @brief Exact-read and build one complete immutable Snapshot.
     * @param[in] timeoutMs Positive backend timeout in milliseconds.
     * @param[out] snapshot Snapshot unchanged on failure.
     * @return Repository, digest, or Snapshot validation status.
     */
    Status Read(int32_t timeoutMs, std::shared_ptr<const TopologySnapshot> &snapshot) const;

    // Controller owns membership facts separately; topology reads must not trigger another membership Range.
    Status ReadTopologyOnly(int32_t timeoutMs, std::shared_ptr<const TopologySnapshot> &snapshot) const;

    /**
     * @brief Conditionally exact-read and build an immutable Snapshot.
     * @param[in] timeoutMs Positive backend timeout in milliseconds.
     * @param[in] knownSnapshot Snapshot already held by the caller, including its membership projection.
     * @param[out] snapshot New Snapshot when changed; unchanged otherwise.
     * @param[out] unchanged Whether both topology and the membership projection are unchanged.
     * @return Repository, digest, or Snapshot validation status.
     */
    Status ReadIfChanged(int32_t timeoutMs, const TopologySnapshot &knownSnapshot,
                         std::shared_ptr<const TopologySnapshot> &snapshot, bool &unchanged) const;

    /**
     * @brief Validate an encoded complete topology and build an immutable Snapshot.
     * @param[in] value Complete encoded topology value.
     * @param[in] authorityRevision Authority revision carried with the value.
     * @param[in] hostIds Worker-address to host-id map read from the membership table.
     * @param[out] snapshot Snapshot unchanged on failure.
     * @param[in] hostIdsRevision Membership read revision, or zero when the projection is unknown.
     * @param[in] coordinatorId CoordinatorId carried with the topology value; empty for ETCD.
     * @return Decode, digest, or Snapshot validation status.
     */
    static Status BuildFromEncodedTopology(const std::string &value, int64_t authorityRevision,
                                           std::unordered_map<std::string, std::string> hostIds,
                                           std::shared_ptr<const TopologySnapshot> &snapshot,
                                           int64_t hostIdsRevision = 0, std::string coordinatorId = {});

private:
    static Status BuildFromState(TopologyState state, int64_t authorityRevision,
                                 std::unordered_map<std::string, std::string> hostIds,
                                 std::shared_ptr<const TopologySnapshot> &snapshot, int64_t hostIdsRevision,
                                 std::string coordinatorId);

    TopologyRepository &repository_;
};

}  // namespace datasystem::cluster

#endif  // DATASYSTEM_CLUSTER_RUNTIME_TOPOLOGY_READER_H
