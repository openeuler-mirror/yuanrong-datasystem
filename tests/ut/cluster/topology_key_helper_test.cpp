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

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "gtest/gtest.h"

#include "ut/common.h"

namespace datasystem::cluster {
namespace {

constexpr uint64_t KEYSPACE_CONTRACT_EPOCH = 42;
constexpr size_t ASCII_A_SOURCE_ID_SIZE = 16;
constexpr char ASCII_A_SOURCE_ID_HEX[] = "61616161616161616161616161616161";

std::string ExactPath(const std::string &table, const std::string &key)
{
    return key.empty() ? table : table + "/" + key;
}

TEST(TopologyKeyHelperTest, BuildsClusterScopedKeyspaces)
{
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("cluster_a-1.0", keys));
    ASSERT_NE(keys, nullptr);
    EXPECT_EQ(keys->ClusterName(), "cluster_a-1.0");
    EXPECT_EQ(keys->EtcdMembershipTablePrefix(), "/cluster_a-1.0/datasystem/cluster");
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
    DS_ASSERT_OK(TopologyKeyHelper::ScaleInMetadataDoneKey(KEYSPACE_CONTRACT_EPOCH,
                                                           std::string(ASCII_A_SOURCE_ID_SIZE, 'a'), taskId, key));
    EXPECT_EQ(ExactPath(keys->ScaleInMetadataDoneTable(), key),
              "/datasystem/cluster_a-1.0/scale-in-metadata-done/e" + std::to_string(KEYSPACE_CONTRACT_EPOCH) + "/"
                  + std::string(ASCII_A_SOURCE_ID_HEX) + "/" + taskId);
}

TEST(TopologyKeyHelperTest, BuildsUnscopedKeyspacesForEmptyClusterName)
{
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("", keys));
    ASSERT_NE(keys, nullptr);
    EXPECT_TRUE(keys->ClusterName().empty());
    EXPECT_EQ(keys->EtcdMembershipTablePrefix(), "/datasystem/cluster");
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
    DS_ASSERT_OK(TopologyKeyHelper::ScaleInMetadataDoneKey(KEYSPACE_CONTRACT_EPOCH,
                                                           std::string(ASCII_A_SOURCE_ID_SIZE, 'a'), taskId, key));
    EXPECT_EQ(ExactPath(keys->ScaleInMetadataDoneTable(), key),
              "/datasystem/scale-in-metadata-done/e" + std::to_string(KEYSPACE_CONTRACT_EPOCH) + "/"
                  + std::string(ASCII_A_SOURCE_ID_HEX) + "/" + taskId);
}

TEST(TopologyKeyHelperTest, ClassifiesEtcdWatchKeysWithoutLogicalPhysicalAmbiguity)
{
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("watch", keys));
    const std::string localAddress = "127.0.0.1:1";

    EXPECT_EQ(keys->ClassifyEtcdWatchKey(keys->TopologyTable() + "/", localAddress),
              TopologyEtcdKeyKind::TOPOLOGY);
    EXPECT_EQ(keys->ClassifyEtcdWatchKey(keys->NotifyTable() + "/" + localAddress, localAddress),
              TopologyEtcdKeyKind::LOCAL_NOTIFY);
    EXPECT_EQ(keys->ClassifyEtcdWatchKey(keys->EtcdMembershipTablePrefix() + "/127.0.0.1:2", localAddress),
              TopologyEtcdKeyKind::MEMBERSHIP);
    EXPECT_EQ(keys->ClassifyEtcdWatchKey(
                  keys->MigrateTaskTable() + "/m-e1-0123456789abcdef0123456789abcdef", localAddress),
              TopologyEtcdKeyKind::MIGRATE_TASK);
    EXPECT_EQ(keys->ClassifyEtcdWatchKey(
                  keys->DeleteTaskTable() + "/d-e1-0123456789abcdef0123456789abcdef", localAddress),
              TopologyEtcdKeyKind::DELETE_TASK);

    EXPECT_EQ(keys->ClassifyEtcdWatchKey(keys->NotifyTable() + "/127.0.0.1:2", localAddress),
              TopologyEtcdKeyKind::UNKNOWN);
    EXPECT_EQ(keys->ClassifyEtcdWatchKey(keys->MembershipTable() + "/127.0.0.1:2", localAddress),
              TopologyEtcdKeyKind::UNKNOWN);
    EXPECT_EQ(keys->ClassifyEtcdWatchKey(keys->DeleteTaskTable() + "-other/task", localAddress),
              TopologyEtcdKeyKind::UNKNOWN);
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
    EXPECT_EQ(TopologyKeyHelper::ScaleInMetadataDoneKey(0, "source", "task", key).GetCode(), K_INVALID);
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
