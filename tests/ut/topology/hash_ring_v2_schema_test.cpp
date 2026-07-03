/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 */

#include "datasystem/protos/hash_ring_v2.pb.h"

#include <cstdint>
#include <string>

#include "google/protobuf/descriptor.h"
#include "gtest/gtest.h"

namespace datasystem {
namespace topology {
namespace {

constexpr uint32_t FIRST_TOKEN = 1;
constexpr uint32_t MIGRATE_RANGE_END = 10;
constexpr uint32_t RECOVERY_RANGE_START = 11;
constexpr uint32_t RECOVERY_RANGE_END = 20;
constexpr uint64_t TEST_TOPOLOGY_VERSION = 7;

bool HasField(const google::protobuf::Descriptor &descriptor, const std::string &fieldName)
{
    return descriptor.FindFieldByName(fieldName) != nullptr;
}

TEST(HashRingV2SchemaTest, DefinesIndependentV2Package)
{
    const auto *descriptor = v2::HashRingPb::descriptor();
    ASSERT_NE(descriptor, nullptr);
    EXPECT_EQ(descriptor->file()->package(), "datasystem.v2");
    EXPECT_EQ(descriptor->file()->name(), "datasystem/protos/hash_ring_v2.proto");
}

TEST(HashRingV2SchemaTest, RetainsWorkerUuidWithoutLegacyTaskFields)
{
    const auto *hashRingDescriptor = v2::HashRingPb::descriptor();
    ASSERT_NE(hashRingDescriptor, nullptr);
    EXPECT_TRUE(HasField(*hashRingDescriptor, "cluster_has_init"));
    EXPECT_TRUE(HasField(*hashRingDescriptor, "workers"));
    EXPECT_TRUE(HasField(*hashRingDescriptor, "version"));
    EXPECT_TRUE(HasField(*hashRingDescriptor, "schema_version"));
    EXPECT_FALSE(HasField(*hashRingDescriptor, "cluster_id"));
    EXPECT_FALSE(HasField(*hashRingDescriptor, "add_node_info"));
    EXPECT_FALSE(HasField(*hashRingDescriptor, "del_node_info"));

    const auto *workerDescriptor = v2::WorkerPb::descriptor();
    ASSERT_NE(workerDescriptor, nullptr);
    EXPECT_TRUE(HasField(*workerDescriptor, "hash_tokens"));
    EXPECT_TRUE(HasField(*workerDescriptor, "worker_uuid"));
    EXPECT_TRUE(HasField(*workerDescriptor, "state"));
    EXPECT_FALSE(HasField(*workerDescriptor, "metadata_db_id"));
    EXPECT_FALSE(HasField(*workerDescriptor, "need_scale_down"));
}

TEST(HashRingV2SchemaTest, DefinesCompactTaskPayloads)
{
    const auto *migrateDescriptor = v2::MigrateTaskPb::descriptor();
    ASSERT_NE(migrateDescriptor, nullptr);
    EXPECT_TRUE(HasField(*migrateDescriptor, "target_worker"));
    EXPECT_TRUE(HasField(*migrateDescriptor, "source_ranges"));
    EXPECT_FALSE(HasField(*migrateDescriptor, "task_id"));
    EXPECT_FALSE(HasField(*migrateDescriptor, "change_id"));
    EXPECT_FALSE(HasField(*migrateDescriptor, "executor_worker"));
    EXPECT_FALSE(HasField(*migrateDescriptor, "source_worker"));
    EXPECT_FALSE(HasField(*migrateDescriptor, "metadata_db_id"));
    EXPECT_FALSE(HasField(*migrateDescriptor, "terminal_status"));

    const auto *deleteDescriptor = v2::DeleteNodeTaskPb::descriptor();
    ASSERT_NE(deleteDescriptor, nullptr);
    EXPECT_TRUE(HasField(*deleteDescriptor, "failed_worker"));
    EXPECT_TRUE(HasField(*deleteDescriptor, "recovery_ranges"));
    EXPECT_FALSE(HasField(*deleteDescriptor, "task_id"));
    EXPECT_FALSE(HasField(*deleteDescriptor, "change_id"));
    EXPECT_FALSE(HasField(*deleteDescriptor, "recovery_worker"));
    EXPECT_FALSE(HasField(*deleteDescriptor, "metadata_db_id"));
    EXPECT_FALSE(HasField(*deleteDescriptor, "terminal_status"));

    const auto *notifyDescriptor = v2::TaskNotifyPb::descriptor();
    ASSERT_NE(notifyDescriptor, nullptr);
    EXPECT_TRUE(HasField(*notifyDescriptor, "type"));
    EXPECT_TRUE(HasField(*notifyDescriptor, "task_ids"));
    EXPECT_FALSE(HasField(*notifyDescriptor, "schema_version"));
    EXPECT_FALSE(HasField(*notifyDescriptor, "worker_address"));
    EXPECT_FALSE(HasField(*notifyDescriptor, "notify_revision"));
    EXPECT_FALSE(HasField(*notifyDescriptor, "migrate_task_ids"));
    EXPECT_FALSE(HasField(*notifyDescriptor, "delete_node_task_ids"));
    EXPECT_FALSE(HasField(*notifyDescriptor, "reason"));
    EXPECT_FALSE(HasField(*notifyDescriptor, "created_at_ms"));
}

TEST(HashRingV2SchemaTest, MessagesCompileWithNewDesignShape)
{
    v2::HashRingPb ring;
    ring.set_cluster_has_init(true);
    ring.set_version(TEST_TOPOLOGY_VERSION);
    ring.set_schema_version("1");
    auto &worker = (*ring.mutable_workers())["127.0.0.1:7001"];
    worker.add_hash_tokens(FIRST_TOKEN);
    worker.set_worker_uuid("legacy-worker-uuid");
    worker.set_state(v2::WorkerPb::ACTIVE);

    v2::MigrateTaskPb migrateTask;
    migrateTask.set_target_worker("127.0.0.1:7002");
    auto *sourceRange = migrateTask.add_source_ranges();
    sourceRange->set_worker("127.0.0.1:7001");
    sourceRange->set_from(FIRST_TOKEN);
    sourceRange->set_end(MIGRATE_RANGE_END);
    sourceRange->set_finished(false);

    v2::DeleteNodeTaskPb deleteTask;
    deleteTask.set_failed_worker("127.0.0.1:7003");
    auto *recoveryRange = deleteTask.add_recovery_ranges();
    recoveryRange->set_worker("127.0.0.1:7001");
    recoveryRange->set_from(RECOVERY_RANGE_START);
    recoveryRange->set_end(RECOVERY_RANGE_END);
    recoveryRange->set_finished(true);

    v2::TaskNotifyPb notify;
    notify.set_type(v2::TaskNotifyPb::SCALE_OUT);
    notify.add_task_ids("migrate-127.0.0.1:7001-127.0.0.1:7002");

    EXPECT_TRUE(ring.cluster_has_init());
    EXPECT_EQ(ring.version(), TEST_TOPOLOGY_VERSION);
    EXPECT_EQ(worker.worker_uuid(), "legacy-worker-uuid");
    EXPECT_EQ(migrateTask.source_ranges_size(), 1);
    EXPECT_EQ(deleteTask.recovery_ranges_size(), 1);
    EXPECT_EQ(notify.task_ids_size(), 1);
}

}  // namespace
}  // namespace topology
}  // namespace datasystem
