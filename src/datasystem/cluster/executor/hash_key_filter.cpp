/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Topology-internal MurmurHash3 key-scope predicate.
 */
#include "datasystem/cluster/executor/hash_key_filter.h"

#include <algorithm>

#include "datasystem/common/util/hash_algorithm.h"

namespace datasystem::cluster {

HashKeyFilter::HashKeyFilter(std::vector<TokenRange> ranges) : ranges_(std::move(ranges))
{
}

bool HashKeyFilter::Contains(std::string_view key) const
{
    const auto token = MurmurHash3_32(reinterpret_cast<const uint8_t *>(key.data()), key.size());
    auto iter = std::lower_bound(ranges_.begin(), ranges_.end(), token,
                                 [](const auto &range, uint32_t value) { return range.end < value; });
    return iter != ranges_.end() && iter->from <= token;
}

std::string HashKeyFilter::DebugString() const
{
    return "hash-scope range_count=" + std::to_string(ranges_.size());
}

}  // namespace datasystem::cluster
