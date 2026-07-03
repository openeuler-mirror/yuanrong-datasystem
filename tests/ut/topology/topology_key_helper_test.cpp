/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 */

#include "datasystem/topology/repository/topology_key_helper.h"

#include <string>

#include "google/protobuf/descriptor.h"
#include "gtest/gtest.h"

#include "datasystem/protos/hash_ring.pb.h"
#include "tests/ut/common.h"

namespace datasystem {
namespace topology {
namespace {

bool HasField(const google::protobuf::Descriptor &descriptor, const std::string &fieldName)
{
    return descriptor.FindFieldByName(fieldName) != nullptr;
}

TEST(TopologySchemaSmokeTest, LegacyHashRingProtoStaysFrozen)
{
    const auto *hashRingDescriptor = HashRingPb::descriptor();
    ASSERT_NE(hashRingDescriptor, nullptr);
    EXPECT_FALSE(HasField(*hashRingDescriptor, "schema_version"));
    EXPECT_FALSE(HasField(*hashRingDescriptor, "version"));

    const auto *workerDescriptor = WorkerPb::descriptor();
    ASSERT_NE(workerDescriptor, nullptr);
    EXPECT_FALSE(HasField(*workerDescriptor, "metadata_db_id"));

    const auto *pool = google::protobuf::DescriptorPool::generated_pool();
    ASSERT_NE(pool, nullptr);
    EXPECT_EQ(pool->FindMessageTypeByName("datasystem.MigrateTaskPb"), nullptr);
    EXPECT_EQ(pool->FindMessageTypeByName("datasystem.DeleteNodeTaskPb"), nullptr);
    EXPECT_EQ(pool->FindMessageTypeByName("datasystem.TaskNotifyPb"), nullptr);
}

TEST(TopologySchemaSmokeTest, NewTopologyDtoTypesCompile)
{
    EXPECT_EQ(TOPOLOGY_SCHEMA_VERSION, 1u);

    TaskNotify notify;
    notify.nodeAddress = "127.0.0.1:7001";
    notify.type = TaskNotifyType::PASSIVE_SCALE_IN;
    notify.taskIds.emplace_back("delete-change-2-c-a");

    TopologyNode worker;
    worker.nodeId = "127.0.0.1:7001";
    worker.state = TopologyNodeState::ACTIVE;

    TransferTaskRecord transfer;
    transfer.taskId = "migrate-change-1-a-b";
    transfer.status = TaskTerminalStatus::RUNNING;

    RecoveryTaskRecord recovery;
    recovery.taskId = notify.taskIds.front();
    recovery.status = TaskTerminalStatus::RUNNING;

    EXPECT_EQ(notify.taskIds.size(), 1ul);
    EXPECT_EQ(notify.type, TaskNotifyType::PASSIVE_SCALE_IN);
    EXPECT_EQ(worker.state, TopologyNodeState::ACTIVE);
    EXPECT_EQ(recovery.status, TaskTerminalStatus::RUNNING);
}

TEST(TopologyKeyHelperTest, BuildsCanonicalRelativeTaskAndNotifyKeys)
{
    std::string key;
    DS_ASSERT_OK(TopologyKeyHelper::BuildMigrateTaskKey("migrate-change-1-a-b", key));
    EXPECT_EQ(key, "migrate_tasks/migrate-change-1-a-b");
    DS_ASSERT_OK(TopologyKeyHelper::BuildDeleteNodeTaskKey("delete-change-1-a-b", key));
    EXPECT_EQ(key, "delete_node_tasks/delete-change-1-a-b");
    DS_ASSERT_OK(TopologyKeyHelper::BuildNotifyKey("127.0.0.1:7001", key));
    EXPECT_EQ(key, "notify/127.0.0.1:7001");
}

TEST(TopologyKeyHelperTest, RejectsUnsafeTaskAndNotifyKeyParts)
{
    std::string key;
    EXPECT_EQ(TopologyKeyHelper::BuildMigrateTaskKey("", key).GetCode(), K_INVALID);
    EXPECT_EQ(TopologyKeyHelper::BuildMigrateTaskKey("..", key).GetCode(), K_INVALID);
    EXPECT_EQ(TopologyKeyHelper::BuildMigrateTaskKey("a/b", key).GetCode(), K_INVALID);
    EXPECT_EQ(TopologyKeyHelper::BuildMigrateTaskKey(std::string("bad") + static_cast<char>(1), key).GetCode(),
              K_INVALID);
    EXPECT_EQ(TopologyKeyHelper::BuildNotifyKey("", key).GetCode(), K_INVALID);
    EXPECT_EQ(TopologyKeyHelper::BuildNotifyKey("bad/worker", key).GetCode(), K_INVALID);
}

TEST(TopologyKeyHelperTest, ParsesFullAndRelativeTopologyKeys)
{
    TopologyKeyParts parts;
    DS_ASSERT_OK(TopologyKeyHelper::Parse("/datasystem/ring", parts));
    EXPECT_EQ(parts.type, TopologyKeyType::COMMITTED_TOPOLOGY);
    DS_ASSERT_OK(TopologyKeyHelper::Parse("/datasystem/ring/", parts));
    EXPECT_EQ(parts.type, TopologyKeyType::COMMITTED_TOPOLOGY);
    DS_ASSERT_OK(TopologyKeyHelper::Parse("", parts));
    EXPECT_EQ(parts.type, TopologyKeyType::COMMITTED_TOPOLOGY);
    DS_ASSERT_OK(TopologyKeyHelper::Parse("/slot_e2e_cluster/datasystem/ring", parts));
    EXPECT_EQ(parts.type, TopologyKeyType::COMMITTED_TOPOLOGY);
    DS_ASSERT_OK(TopologyKeyHelper::Parse("/slot_e2e_cluster/datasystem/ring/", parts));
    EXPECT_EQ(parts.type, TopologyKeyType::COMMITTED_TOPOLOGY);

    DS_ASSERT_OK(TopologyKeyHelper::Parse("/datasystem/ring/migrate_tasks/task-1", parts));
    EXPECT_EQ(parts.type, TopologyKeyType::MIGRATE_TASK);
    EXPECT_EQ(parts.taskId, "task-1");
    DS_ASSERT_OK(TopologyKeyHelper::Parse("migrate_tasks/task-1", parts));
    EXPECT_EQ(parts.type, TopologyKeyType::MIGRATE_TASK);
    EXPECT_EQ(parts.taskId, "task-1");

    DS_ASSERT_OK(TopologyKeyHelper::Parse("/datasystem/ring/delete_node_tasks/task-2", parts));
    EXPECT_EQ(parts.type, TopologyKeyType::DELETE_NODE_TASK);
    EXPECT_EQ(parts.taskId, "task-2");
    DS_ASSERT_OK(TopologyKeyHelper::Parse("delete_node_tasks/task-2", parts));
    EXPECT_EQ(parts.type, TopologyKeyType::DELETE_NODE_TASK);
    EXPECT_EQ(parts.taskId, "task-2");

    DS_ASSERT_OK(TopologyKeyHelper::Parse("/datasystem/ring/notify/127.0.0.1:7001", parts));
    EXPECT_EQ(parts.type, TopologyKeyType::NOTIFY);
    EXPECT_EQ(parts.nodeAddress, "127.0.0.1:7001");
    DS_ASSERT_OK(TopologyKeyHelper::Parse("notify/127.0.0.1:7001", parts));
    EXPECT_EQ(parts.type, TopologyKeyType::NOTIFY);
    EXPECT_EQ(parts.nodeAddress, "127.0.0.1:7001");

    DS_ASSERT_OK(TopologyKeyHelper::Parse("/slot_e2e_cluster/datasystem/ring/migrate_tasks/task-3", parts));
    EXPECT_EQ(parts.type, TopologyKeyType::MIGRATE_TASK);
    EXPECT_EQ(parts.taskId, "task-3");
    DS_ASSERT_OK(TopologyKeyHelper::Parse("/slot_e2e_cluster/datasystem/ring/notify/127.0.0.1:7001", parts));
    EXPECT_EQ(parts.type, TopologyKeyType::NOTIFY);
    EXPECT_EQ(parts.nodeAddress, "127.0.0.1:7001");

    DS_ASSERT_OK(TopologyKeyHelper::Parse("/datasystem/metadata/key", parts));
    EXPECT_EQ(parts.type, TopologyKeyType::UNRELATED);
}

TEST(TopologyKeyHelperTest, RejectsMalformedKnownPrefixKeys)
{
    TopologyKeyParts parts;
    EXPECT_EQ(TopologyKeyHelper::Parse("/datasystem/ring/migrate_tasks/", parts).GetCode(), K_INVALID);
    EXPECT_EQ(TopologyKeyHelper::Parse("migrate_tasks/", parts).GetCode(), K_INVALID);
    EXPECT_EQ(TopologyKeyHelper::Parse("/datasystem/ring/delete_node_tasks/", parts).GetCode(), K_INVALID);
    EXPECT_EQ(TopologyKeyHelper::Parse("delete_node_tasks/", parts).GetCode(), K_INVALID);
    EXPECT_EQ(TopologyKeyHelper::Parse("/datasystem/ring/notify/", parts).GetCode(), K_INVALID);
    EXPECT_EQ(TopologyKeyHelper::Parse("notify/", parts).GetCode(), K_INVALID);
    EXPECT_EQ(TopologyKeyHelper::Parse("/datasystem/ring/migrate_tasks/a/b", parts).GetCode(), K_INVALID);
    EXPECT_EQ(TopologyKeyHelper::Parse("/datasystem/ring/migrate_tasks/..", parts).GetCode(), K_INVALID);
    EXPECT_EQ(TopologyKeyHelper::Parse("/slot_e2e_cluster/datasystem/ring/migrate_tasks/", parts).GetCode(),
              K_INVALID);
    EXPECT_EQ(TopologyKeyHelper::Parse("/slot_e2e_cluster/datasystem/ring/notify/bad/worker", parts).GetCode(),
              K_INVALID);
}

}  // namespace
}  // namespace topology
}  // namespace datasystem
