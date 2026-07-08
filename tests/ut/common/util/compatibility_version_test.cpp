/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "datasystem/common/util/compatibility_manager.h"

#include "ut/common.h"

namespace datasystem {
namespace ut {

class CompatibilityVersionTest : public testing::Test {};

TEST_F(CompatibilityVersionTest, ToStringFormatsAsDotSeparatedString)
{
    CompatibilityVersion version(1, 0, 0);
    EXPECT_EQ(version.ToString(), "1.0.0");
}

TEST_F(CompatibilityVersionTest, FromStringParsesValidVersion)
{
    CompatibilityVersion version;
    auto rc = CompatibilityVersion::FromString("1.0.1", version);
    DS_ASSERT_OK(rc);
    EXPECT_EQ(version.Major(), 1UL);
    EXPECT_EQ(version.Minor(), 0UL);
    EXPECT_EQ(version.Patch(), 1UL);
    EXPECT_TRUE(version.IsValid());
}

TEST_F(CompatibilityVersionTest, DefaultConstructedVersionIsInvalidSentinel)
{
    CompatibilityVersion version;
    EXPECT_FALSE(version.IsValid());
    EXPECT_EQ(version.ToString(), "0.0.0");
}

TEST_F(CompatibilityVersionTest, FromStringRejectsInvalidFormat)
{
    CompatibilityVersion version;
    EXPECT_TRUE(CompatibilityVersion::FromString("", version).IsError());
    EXPECT_TRUE(CompatibilityVersion::FromString("abc", version).IsError());
    EXPECT_TRUE(CompatibilityVersion::FromString(".1.2", version).IsError());
    EXPECT_TRUE(CompatibilityVersion::FromString("1..2", version).IsError());
    EXPECT_TRUE(CompatibilityVersion::FromString("1.2.", version).IsError());
    EXPECT_TRUE(CompatibilityVersion::FromString("1.2", version).IsError());
    EXPECT_TRUE(CompatibilityVersion::FromString("1.2.3.4", version).IsError());
    EXPECT_TRUE(CompatibilityVersion::FromString("-1.2.3", version).IsError());
    EXPECT_TRUE(CompatibilityVersion::FromString("1.-2.3", version).IsError());
    EXPECT_TRUE(CompatibilityVersion::FromString("1.2.-3", version).IsError());
    EXPECT_TRUE(CompatibilityVersion::FromString("4294967296.0.0", version).IsError());
    EXPECT_TRUE(CompatibilityVersion::FromString("1.4294967296.0", version).IsError());
    EXPECT_TRUE(CompatibilityVersion::FromString("1.0.4294967296", version).IsError());
    EXPECT_TRUE(CompatibilityVersion::FromString("999999999999999999999999999999.0.0", version).IsError());
    // Leading zeros are rejected.
    EXPECT_TRUE(CompatibilityVersion::FromString("01.2.3", version).IsError());
    EXPECT_TRUE(CompatibilityVersion::FromString("1.02.3", version).IsError());
    EXPECT_TRUE(CompatibilityVersion::FromString("1.2.03", version).IsError());
}

TEST_F(CompatibilityVersionTest, EqualityOperatorWorks)
{
    CompatibilityVersion a(1, 0, 0);
    CompatibilityVersion b(1, 0, 0);
    CompatibilityVersion c(1, 0, 1);
    EXPECT_EQ(a, b);
    EXPECT_NE(a, c);
}

TEST_F(CompatibilityVersionTest, ComparisonOperatorsWork)
{
    CompatibilityVersion v100(1, 0, 0);
    CompatibilityVersion v101(1, 0, 1);
    CompatibilityVersion v110(1, 1, 0);
    CompatibilityVersion v200(2, 0, 0);

    EXPECT_LT(v100, v101);
    EXPECT_LT(v100, v110);
    EXPECT_LT(v100, v200);
    EXPECT_LT(v101, v110);
    EXPECT_LT(v101, v200);
    EXPECT_LT(v110, v200);

    EXPECT_LE(v100, v100);
    EXPECT_LE(v100, v101);

    EXPECT_GT(v101, v100);
    EXPECT_GE(v100, v100);
}

TEST_F(CompatibilityVersionTest, HashCanBeUsedInUnorderedMap)
{
    std::unordered_map<CompatibilityVersion, int> map;
    map[CompatibilityVersion(1, 0, 0)] = 1;
    map[CompatibilityVersion(1, 0, 1)] = 2;
    EXPECT_EQ(map.size(), 2UL);
    EXPECT_EQ(map[CompatibilityVersion(1, 0, 0)], 1);
    EXPECT_EQ(map[CompatibilityVersion(1, 0, 1)], 2);
}

class CompatibilityManagerTest : public testing::Test {};

TEST_F(CompatibilityManagerTest, GetCurrentCompatibilityVersionReturnsBaseline)
{
    auto baseline = CompatibilityManager::Instance().GetBaselineCompatibilityVersion();
    auto current = CompatibilityManager::Instance().GetCurrentCompatibilityVersion();
    EXPECT_EQ(baseline, CompatibilityVersion(1, 0, 0));
    EXPECT_EQ(current, baseline);
}

TEST_F(CompatibilityManagerTest, SupportsAlwaysReturnsFalseForNone)
{
    EXPECT_FALSE(CompatibilityManager::Instance().Supports(CompatibilityVersion(1, 0, 0), FeatureFlag::NONE));
    // Default-constructed (0.0.0) is not a declared version.
    EXPECT_FALSE(CompatibilityManager::Instance().Supports(CompatibilityVersion(), FeatureFlag::NONE));
    EXPECT_FALSE(CompatibilityManager::Instance().Supports(CompatibilityVersion(9, 9, 9), FeatureFlag::NONE));
}

TEST_F(CompatibilityManagerTest, UpdateClusterCompatibilityVersionStoresExactValue)
{
    auto &manager = CompatibilityManager::Instance();

    manager.UpdateClusterCompatibilityVersion(CompatibilityVersion(1, 0, 0));
    EXPECT_EQ(manager.GetClusterCompatibilityVersion(), CompatibilityVersion(1, 0, 0));

    manager.UpdateClusterCompatibilityVersion(CompatibilityVersion(1, 0, 1));
    EXPECT_EQ(manager.GetClusterCompatibilityVersion(), CompatibilityVersion(1, 0, 1));

    manager.UpdateClusterCompatibilityVersion(CompatibilityVersion(1, 0, 0));
    EXPECT_EQ(manager.GetClusterCompatibilityVersion(), CompatibilityVersion(1, 0, 0));
}

}  // namespace ut
}  // namespace datasystem
