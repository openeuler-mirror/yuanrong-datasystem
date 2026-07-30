// Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <string>
#include <vector>

#include "datasystem/common/coordinator/static_coordinator_discovery.h"
#include "ut/common.h"

namespace datasystem {
namespace ut {

TEST(StaticCoordinatorDiscoveryTest, ReturnsConfiguredAddressAndReplacesExistingOutput)
{
    StaticCoordinatorDiscovery discovery("127.0.0.1:31501");
    std::vector<std::string> coordinators{ "stale-address" };

    DS_ASSERT_OK(discovery.GetCoordinators(coordinators));

    ASSERT_EQ(coordinators.size(), 1UL);
    EXPECT_EQ(coordinators.front(), "127.0.0.1:31501");
    EXPECT_EQ(discovery.GetCount(), 1UL);
}

TEST(StaticCoordinatorDiscoveryTest, EmptyAddressReturnsEmptyOutput)
{
    StaticCoordinatorDiscovery discovery("");
    std::vector<std::string> coordinators{ "stale-address" };

    DS_ASSERT_OK(discovery.GetCoordinators(coordinators));

    EXPECT_TRUE(coordinators.empty());
    EXPECT_EQ(discovery.GetCount(), 0UL);
}

TEST(StaticCoordinatorDiscoveryTest, ReturnsCommaSeparatedAddressesAndCount)
{
    StaticCoordinatorDiscovery discovery("127.0.0.1:31502,127.0.0.1:31501,127.0.0.1:31502");
    std::vector<std::string> coordinators{ "stale-address" };

    DS_ASSERT_OK(discovery.GetCoordinators(coordinators));

    ASSERT_EQ(coordinators.size(), 2UL);
    EXPECT_EQ(coordinators[0], "127.0.0.1:31501");
    EXPECT_EQ(coordinators[1], "127.0.0.1:31502");
    EXPECT_EQ(discovery.GetCount(), 2UL);
}

TEST(StaticCoordinatorDiscoveryTest, TrimsAsciiWhitespaceDropsEmptyEntriesAndDeduplicates)
{
    StaticCoordinatorDiscovery discovery(" 127.0.0.1:31502,\t127.0.0.1:31501\n,, 127.0.0.1:31502 , ");
    std::vector<std::string> coordinators{ "stale-address" };

    DS_ASSERT_OK(discovery.GetCoordinators(coordinators));

    ASSERT_EQ(coordinators.size(), 2UL);
    EXPECT_EQ(coordinators[0], "127.0.0.1:31501");
    EXPECT_EQ(coordinators[1], "127.0.0.1:31502");
    EXPECT_EQ(discovery.GetCount(), 2UL);
}

TEST(StaticCoordinatorDiscoveryTest, RepeatedCallsReturnTheSameAddress)
{
    StaticCoordinatorDiscovery discovery("127.0.0.1:31501");
    std::vector<std::string> coordinators;

    DS_ASSERT_OK(discovery.GetCoordinators(coordinators));
    ASSERT_EQ(coordinators.size(), 1UL);
    EXPECT_EQ(coordinators.front(), "127.0.0.1:31501");

    coordinators = { "another-stale-address" };
    DS_ASSERT_OK(discovery.GetCoordinators(coordinators));
    ASSERT_EQ(coordinators.size(), 1UL);
    EXPECT_EQ(coordinators.front(), "127.0.0.1:31501");
}

}  // namespace ut
}  // namespace datasystem
