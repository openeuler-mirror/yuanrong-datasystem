/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Opaque storage scan-plan surface tests.
 */
#include "datasystem/cluster/executor/storage_scan_plan.h"

#include <type_traits>

#include "gtest/gtest.h"

namespace datasystem::cluster {
namespace {

TEST(StorageScanPlanTest, CannotBeConstructedCopiedOrMovedByBusinessCode)
{
    EXPECT_FALSE(std::is_default_constructible_v<StorageScanPlan>);
    EXPECT_FALSE(std::is_copy_constructible_v<StorageScanPlan>);
    EXPECT_FALSE(std::is_move_constructible_v<StorageScanPlan>);
}

}  // namespace
}  // namespace datasystem::cluster
