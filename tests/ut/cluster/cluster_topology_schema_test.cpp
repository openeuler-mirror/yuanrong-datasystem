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
 * Description: Cluster topology protobuf schema contract tests.
 */
#include "datasystem/protos/cluster_topology.pb.h"

#include <string>

#include "google/protobuf/descriptor.h"
#include "gtest/gtest.h"

namespace datasystem::cluster {
namespace {

using google::protobuf::Descriptor;
using google::protobuf::EnumDescriptor;

void ExpectField(const Descriptor *descriptor, const std::string &name, int number)
{
    const auto *field = descriptor->FindFieldByName(name);
    ASSERT_NE(field, nullptr) << descriptor->full_name() << "." << name;
    EXPECT_EQ(field->number(), number);
}

void ExpectEnumValue(const EnumDescriptor *descriptor, const std::string &name, int number)
{
    const auto *value = descriptor->FindValueByName(name);
    ASSERT_NE(value, nullptr) << descriptor->full_name() << "." << name;
    EXPECT_EQ(value->number(), number);
}

TEST(ClusterTopologySchemaTest, FreezesMembershipAndRangeSchema)
{
    const auto *membership = MembershipPb::descriptor();
    ASSERT_EQ(membership->field_count(), 3);
    ExpectField(membership, "tokens", 1);
    ExpectField(membership, "id", 2);
    ExpectField(membership, "state", 3);

    const auto *state = membership->FindEnumTypeByName("StatePb");
    ASSERT_NE(state, nullptr);
    ASSERT_EQ(state->value_count(), 6);
    ExpectEnumValue(state, "INITIAL", 0);
    ExpectEnumValue(state, "JOINING", 1);
    ExpectEnumValue(state, "ACTIVE", 2);
    ExpectEnumValue(state, "PRE_LEAVING", 3);
    ExpectEnumValue(state, "LEAVING", 4);
    ExpectEnumValue(state, "FAILED", 5);

    const auto *range = TokenRangePb::descriptor();
    ASSERT_EQ(range->field_count(), 4);
    ExpectField(range, "owner_address", 1);
    ExpectField(range, "from", 2);
    ExpectField(range, "end", 3);
    ExpectField(range, "finished", 4);
}

TEST(ClusterTopologySchemaTest, FreezesTaskAndBatchSchema)
{
    const auto *migrate = MigrateTaskPb::descriptor();
    ASSERT_EQ(migrate->field_count(), 2);
    ExpectField(migrate, "target_address", 1);
    ExpectField(migrate, "source_ranges", 2);

    const auto *remove = DeleteNodeTaskPb::descriptor();
    ASSERT_EQ(remove->field_count(), 2);
    ExpectField(remove, "failed_address", 1);
    ExpectField(remove, "recovery_ranges", 2);

    const auto *batch = ChangeBatchPb::descriptor();
    ASSERT_EQ(batch->field_count(), 2);
    ExpectField(batch, "type", 1);
    ExpectField(batch, "epoch", 2);

    const auto *type = batch->file()->FindEnumTypeByName("TypePb");
    ASSERT_NE(type, nullptr);
    ASSERT_EQ(type->value_count(), 3);
    ExpectEnumValue(type, "SCALE_OUT", 0);
    ExpectEnumValue(type, "SCALE_IN", 1);
    ExpectEnumValue(type, "FAILURE", 2);

    const auto *notify = TaskNotifyPb::descriptor();
    ASSERT_EQ(notify->field_count(), 2);
    ExpectField(notify, "type", 1);
    ExpectField(notify, "task_ids", 2);
}

TEST(ClusterTopologySchemaTest, FreezesRootAndActiveBatchPresence)
{
    const auto *topology = ClusterTopologyPb::descriptor();
    EXPECT_EQ(topology->file()->name(), "datasystem/protos/cluster_topology.proto");
    EXPECT_EQ(topology->file()->package(), "datasystem");
    ASSERT_EQ(topology->field_count(), 5);
    ExpectField(topology, "cluster_has_init", 1);
    ExpectField(topology, "members", 2);
    ExpectField(topology, "version", 3);
    ExpectField(topology, "schema_version", 4);
    ExpectField(topology, "active_batch", 5);

    ClusterTopologyPb value;
    EXPECT_FALSE(value.has_active_batch());
    value.mutable_active_batch()->set_epoch(1);
    EXPECT_TRUE(value.has_active_batch());
}

}  // namespace
}  // namespace datasystem::cluster
