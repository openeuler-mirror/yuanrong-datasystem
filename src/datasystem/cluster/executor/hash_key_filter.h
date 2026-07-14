/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Topology-internal MurmurHash3 key-scope predicate.
 */
#ifndef DATASYSTEM_CLUSTER_EXECUTOR_HASH_KEY_FILTER_H
#define DATASYSTEM_CLUSTER_EXECUTOR_HASH_KEY_FILTER_H

#include <vector>

#include "datasystem/cluster/executor/key_filter.h"
#include "datasystem/cluster/model/topology_types.h"

namespace datasystem::cluster {

/**
 * @brief Topology-internal hash implementation of the opaque correctness predicate.
 */
class HashKeyFilter final : public IKeyFilter {
public:
    /**
     * @brief Copy canonical task ranges.
     * @param[in] ranges Sorted non-overlapping ranges.
     */
    explicit HashKeyFilter(std::vector<TokenRange> ranges);

    /**
     * @brief Destroy the hash filter.
     */
    ~HashKeyFilter() override = default;

    /**
     * @brief Hash and test one key.
     * @param[in] key Binary-safe key.
     * @return True when in scope.
     */
    bool Contains(std::string_view key) const override;

    /**
     * @brief Return bounded diagnostics without boundaries.
     * @return Payload-free text.
     */
    std::string DebugString() const override;

private:
    std::vector<TokenRange> ranges_;
};

}  // namespace datasystem::cluster

#endif  // DATASYSTEM_CLUSTER_EXECUTOR_HASH_KEY_FILTER_H
