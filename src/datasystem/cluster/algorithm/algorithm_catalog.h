/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Process-local built-in cluster algorithm catalog.
 */
#ifndef DATASYSTEM_CLUSTER_ALGORITHM_ALGORITHM_CATALOG_H
#define DATASYSTEM_CLUSTER_ALGORITHM_ALGORITHM_CATALOG_H

#include "datasystem/cluster/algorithm/hash_algorithm.h"

namespace datasystem::cluster {

class AlgorithmCatalog final {
public:
    /**
     * @brief Construct the built-in catalog.
     */
    AlgorithmCatalog() = default;

    /**
     * @brief Destroy catalog-owned algorithms.
     */
    ~AlgorithmCatalog() = default;
    AlgorithmCatalog(const AlgorithmCatalog &) = delete;
    AlgorithmCatalog &operator=(const AlgorithmCatalog &) = delete;

    /**
     * @brief Resolve a routing facet.
     * @param[in] id Algorithm id.
     * @param[out] algorithm Non-owning facet.
     * @return K_OK or K_NOT_FOUND.
     */
    Status GetRouting(const TopologyAlgorithmId &id, const IRoutingAlgorithm *&algorithm) const;

    /**
     * @brief Resolve a planning facet.
     * @param[in] id Algorithm id.
     * @param[out] algorithm Non-owning facet.
     * @return K_OK or K_NOT_FOUND.
     */
    Status GetPlanning(const TopologyAlgorithmId &id, const IPlanningAlgorithm *&algorithm) const;

private:
    HashAlgorithm hash_;
};

}  // namespace datasystem::cluster

#endif  // DATASYSTEM_CLUSTER_ALGORITHM_ALGORITHM_CATALOG_H
