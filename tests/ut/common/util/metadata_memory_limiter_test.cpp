/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
#include "datasystem/common/util/metadata_memory_limiter.h"
#include <memory>

#include <gtest/gtest.h>
#include "datasystem/common/util/raii.h"

#ifdef METADATA_MEMORY_LIMITER_STANDALONE_TEST
DS_DEFINE_uint64(max_object_metadata_size_mb, 1024, "Metadata admission limit for the standalone limiter test.");
#endif

namespace datasystem::ut {
TEST(MetadataMemoryLimiterTest, CountsSourcesIndependentlyAndAllowsExactLimit)
{
    const auto oldLimit = FLAGS_max_object_metadata_size_mb;
    Raii restore([oldLimit] { FLAGS_max_object_metadata_size_mb = oldLimit; });
    MetadataMemoryLimiter limiter;
    using Source = MetadataMemoryLimiter::Source;
    uint64_t objects = MB_TO_BYTES;
    uint64_t metas = MB_TO_BYTES;
    limiter.RegisterCounter(Source::OBJECT, [&] { return objects; });
    limiter.RegisterCounter(Source::META, [&] { return metas; });
    FLAGS_max_object_metadata_size_mb = 2000;
    ASSERT_TRUE(limiter.CheckAdmission().IsOk());
    FLAGS_max_object_metadata_size_mb = 1999;
    EXPECT_EQ(limiter.CheckAdmission().GetCode(), K_OUT_OF_MEMORY);
    objects = 0;
    FLAGS_max_object_metadata_size_mb = 1200;
    ASSERT_TRUE(limiter.CheckAdmission().IsOk());
    FLAGS_max_object_metadata_size_mb = 1199;
    EXPECT_EQ(limiter.CheckAdmission().GetCode(), K_OUT_OF_MEMORY);
    metas = 0;
    FLAGS_max_object_metadata_size_mb = 0;
    ASSERT_TRUE(limiter.CheckAdmission().IsOk());
}

TEST(MetadataMemoryLimiterTest, RejectsOverflowAndExpiredSourcesDoNotRetainOwners)
{
    const auto oldLimit = FLAGS_max_object_metadata_size_mb;
    Raii restore([oldLimit] { FLAGS_max_object_metadata_size_mb = oldLimit; });
    MetadataMemoryLimiter limiter;
    auto count = std::make_shared<uint64_t>(std::numeric_limits<uint64_t>::max());
    std::weak_ptr<uint64_t> weak = count;
    limiter.RegisterCounter(MetadataMemoryLimiter::Source::OBJECT, [weak] {
        auto locked = weak.lock();
        return locked ? *locked : 0;
    });
    FLAGS_max_object_metadata_size_mb = 1024;
    EXPECT_EQ(limiter.CheckAdmission().GetCode(), K_OUT_OF_MEMORY);
    count.reset();
    EXPECT_TRUE(weak.expired());
    FLAGS_max_object_metadata_size_mb = 0;
    ASSERT_TRUE(limiter.CheckAdmission().IsOk());
}
}  // namespace datasystem::ut
