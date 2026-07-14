/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Opaque hash key-filter tests.
 */
#include "datasystem/cluster/executor/hash_key_filter.h"

#include <limits>

#include "datasystem/common/util/hash_algorithm.h"
#include "gtest/gtest.h"

namespace datasystem::cluster {
namespace {

TEST(HashKeyFilterTest, HandlesExactBoundaryDisjointAndFullScopes)
{
    const std::string first = "first-key";
    const std::string second = "second-key";
    const auto firstToken = MurmurHash3_32(first);
    const auto secondToken = MurmurHash3_32(second);
    std::vector<TokenRange> ranges{ { firstToken, firstToken }, { secondToken, secondToken } };
    std::sort(ranges.begin(), ranges.end(), [](const auto &left, const auto &right) { return left.from < right.from; });
    HashKeyFilter filter(ranges);
    EXPECT_TRUE(filter.Contains(first));
    EXPECT_TRUE(filter.Contains(second));
    EXPECT_EQ(filter.DebugString(), "hash-scope range_count=2");
    HashKeyFilter full({ { 0, std::numeric_limits<uint32_t>::max() } });
    EXPECT_TRUE(full.Contains(std::string_view("binary\0key", 10)));
}

}  // namespace
}  // namespace datasystem::cluster
