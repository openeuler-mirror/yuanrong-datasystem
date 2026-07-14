/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Algorithm-neutral opaque topology key-scope predicate.
 */
#ifndef DATASYSTEM_CLUSTER_EXECUTOR_KEY_FILTER_H
#define DATASYSTEM_CLUSTER_EXECUTOR_KEY_FILTER_H

#include <string>
#include <string_view>

namespace datasystem::cluster {

/**
 * @brief Correctness predicate exposed to one synchronous business callback.
 */
class IKeyFilter {
public:
    /**
     * @brief Destroy the key-filter interface.
     */
    virtual ~IKeyFilter() = default;

    /**
     * @brief Test one opaque key.
     * @param[in] key Binary-safe business key.
     * @return True when in scope.
     */
    virtual bool Contains(std::string_view key) const = 0;

    /**
     * @brief Return bounded diagnostics without scope boundaries.
     * @return Payload-free text.
     */
    virtual std::string DebugString() const = 0;
};

}  // namespace datasystem::cluster

#endif  // DATASYSTEM_CLUSTER_EXECUTOR_KEY_FILTER_H
