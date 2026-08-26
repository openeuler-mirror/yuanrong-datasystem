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

    /**
     * @brief Conditionally exact-read and build an immutable Snapshot.
     * @param[in] timeoutMs Positive backend timeout in milliseconds.
     * @param[in] knownAuthorityRevision Authority revision already held by the caller.
     * @param[out] snapshot New Snapshot when changed; unchanged otherwise.
     * @param[out] unchanged Whether topology still has knownAuthorityRevision.
     * @return Repository, digest, or Snapshot validation status.
     */
    Status ReadIfChanged(int32_t timeoutMs, int64_t knownAuthorityRevision,
                         std::shared_ptr<const TopologySnapshot> &snapshot, bool &unchanged) const;

private:
    TopologyRepository &repository_;
};

}  // namespace datasystem::cluster

#endif  // DATASYSTEM_CLUSTER_RUNTIME_TOPOLOGY_READER_H
