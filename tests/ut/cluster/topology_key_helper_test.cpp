/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Description: Cluster-scoped topology key contract tests.
 */
#include "datasystem/cluster/repository/topology_key_helper.h"

#include <memory>
#include <string>
#include <vector>

#include "gtest/gtest.h"

#include "ut/common.h"

namespace datasystem::cluster {
namespace {

std::string ExactPath(const std::string &table, const std::string &key)
{
    return key.empty() ? table : table + "/" + key;
}

TEST(TopologyKeyHelperTest, BuildsFiveClusterScopedKeyspaces)
{
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("cluster_a-1.0", keys));
    ASSERT_NE(keys, nullptr);
    EXPECT_EQ(keys->ClusterName(), "cluster_a-1.0");
    EXPECT_EQ(ExactPath(keys->TopologyTable(), TopologyKeyHelper::TopologyKey()), "/datasystem/cluster_a-1.0/topology");

    std::string key;
    const std::string taskId = "m-e42-0123456789abcdef0123456789abcdef";
    DS_ASSERT_OK(TopologyKeyHelper::TaskKey(taskId, key));
    EXPECT_EQ(ExactPath(keys->MigrateTaskTable(), key), "/datasystem/cluster_a-1.0/tasks/migrate/" + taskId);
    EXPECT_EQ(ExactPath(keys->DeleteTaskTable(), key), "/datasystem/cluster_a-1.0/tasks/delete/" + taskId);

    const std::string address = "127.0.0.1:7001";
    DS_ASSERT_OK(TopologyKeyHelper::NotifyKey(address, key));
    EXPECT_EQ(ExactPath(keys->NotifyTable(), key), "/datasystem/cluster_a-1.0/notify/" + address);
    DS_ASSERT_OK(TopologyKeyHelper::MembershipKey(address, key));
    EXPECT_EQ(ExactPath(keys->MembershipTable(), key), "/datasystem/cluster_a-1.0/cluster/" + address);
}

TEST(TopologyKeyHelperTest, BuildsUnscopedKeyspacesForEmptyClusterName)
{
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("", keys));
    ASSERT_NE(keys, nullptr);
    EXPECT_TRUE(keys->ClusterName().empty());
    EXPECT_EQ(ExactPath(keys->TopologyTable(), TopologyKeyHelper::TopologyKey()), "/datasystem/topology");

    std::string key;
    const std::string taskId = "m-e42-0123456789abcdef0123456789abcdef";
    DS_ASSERT_OK(TopologyKeyHelper::TaskKey(taskId, key));
    EXPECT_EQ(ExactPath(keys->MigrateTaskTable(), key), "/datasystem/tasks/migrate/" + taskId);
    EXPECT_EQ(ExactPath(keys->DeleteTaskTable(), key), "/datasystem/tasks/delete/" + taskId);

    const std::string address = "127.0.0.1:7001";
    DS_ASSERT_OK(TopologyKeyHelper::NotifyKey(address, key));
    EXPECT_EQ(ExactPath(keys->NotifyTable(), key), "/datasystem/notify/" + address);
    DS_ASSERT_OK(TopologyKeyHelper::MembershipKey(address, key));
    EXPECT_EQ(ExactPath(keys->MembershipTable(), key), "/datasystem/cluster/" + address);
}

TEST(TopologyKeyHelperTest, EnforcesClusterNameContractWithoutNormalization)
{
    std::unique_ptr<TopologyKeyHelper> keys;
    const std::string longestName = "a" + std::string(127, 'z');
    DS_ASSERT_OK(TopologyKeyHelper::Create(longestName, keys));
    EXPECT_EQ(keys->ClusterName(), longestName);

    auto *original = keys.get();
    const std::vector<std::string> invalidNames = { "-cluster", "a/b", "a%2Fb", "a" + std::string(128, 'z') };
    for (const auto &name : invalidNames) {
        EXPECT_EQ(TopologyKeyHelper::Create(name, keys).GetCode(), K_INVALID) << name;
        EXPECT_EQ(keys.get(), original);
    }
}

TEST(TopologyKeyHelperTest, RejectsPrefixUseOnExactCollections)
{
    std::string key = "unchanged";
    EXPECT_EQ(TopologyKeyHelper::TaskKey("", key).GetCode(), K_INVALID);
    EXPECT_EQ(TopologyKeyHelper::NotifyKey("", key).GetCode(), K_INVALID);
    EXPECT_EQ(TopologyKeyHelper::MembershipKey("", key).GetCode(), K_INVALID);
    EXPECT_EQ(key, "unchanged");
    EXPECT_TRUE(TopologyKeyHelper::TopologyKey().empty());
}

TEST(TopologyKeyHelperTest, ValidatesDeterministicTaskIdsAndCanonicalAddresses)
{
    std::string key;
    DS_ASSERT_OK(TopologyKeyHelper::TaskKey("d-e7-abcdefabcdefabcdefabcdefabcdefab", key));
    EXPECT_EQ(key, "d-e7-abcdefabcdefabcdefabcdefabcdefab");
    DS_ASSERT_OK(TopologyKeyHelper::NotifyKey("[::1]:7001", key));
    EXPECT_EQ(key, "[::1]:7001");

    const std::vector<std::string> invalidTaskIds = { "task-1", "m-e-abcdef", "m-e0-0123456789abcdef0123456789abcdef",
                                                      "m-e1-short", "m-e1-a/b" };
    for (const auto &taskId : invalidTaskIds) {
        EXPECT_EQ(TopologyKeyHelper::TaskKey(taskId, key).GetCode(), K_INVALID) << taskId;
    }
    EXPECT_EQ(TopologyKeyHelper::NotifyKey("127.0.0.1", key).GetCode(), K_INVALID);
    EXPECT_EQ(TopologyKeyHelper::MembershipKey("bad/address:7001", key).GetCode(), K_INVALID);
}

}  // namespace
}  // namespace datasystem::cluster
