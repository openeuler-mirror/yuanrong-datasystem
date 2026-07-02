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

/**
 * Description: Unit tests for the resource JSON schema (field mapping + ods whitelist + splitting).
 */
#include "datasystem/common/metrics/resource_json_schema.h"

#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "ut/common.h"

namespace datasystem {
namespace ut {
class ResourceJsonSchemaTest : public CommonTest {};

// SplitResourceFields: multi-field string splits on '/' into ordered tokens.
TEST_F(ResourceJsonSchemaTest, SplitMultiField)
{
    std::vector<std::string> tokens;
    SplitResourceFields("73814/80000/1073741824/0.069/0/0", '/', tokens);
    ASSERT_EQ(tokens.size(), static_cast<size_t>(6));
    EXPECT_EQ(tokens[0], "73814");
    EXPECT_EQ(tokens[3], "0.069");
    EXPECT_EQ(tokens[5], "0");
}

// SplitResourceFields: single-field metric (sep == '\0') keeps the whole string as one token.
TEST_F(ResourceJsonSchemaTest, SplitSingleField)
{
    std::vector<std::string> tokens;
    SplitResourceFields("42", '\0', tokens);
    ASSERT_EQ(tokens.size(), static_cast<size_t>(1));
    EXPECT_EQ(tokens[0], "42");
}

// SplitResourceFields: empty raw yields one empty token (caller decides all-zero fallback).
TEST_F(ResourceJsonSchemaTest, SplitEmpty)
{
    std::vector<std::string> tokens;
    SplitResourceFields("", '/', tokens);
    ASSERT_EQ(tokens.size(), static_cast<size_t>(1));
    EXPECT_EQ(tokens[0], "");
}

// GetResourceFieldDesc: SHARED_MEMORY has 6 fields, ods records the first 4 (memory/physical/total/usage)
// and drops sc_memory_usage / sc_memory_limit.
TEST_F(ResourceJsonSchemaTest, SharedMemoryWhitelist)
{
    const auto &desc = GetResourceFieldDesc(ResMetricName::SHARED_MEMORY);
    EXPECT_EQ(desc.groupName, std::string("shared_memory"));
    ASSERT_EQ(desc.fieldNames.size(), static_cast<size_t>(6));
    EXPECT_EQ(desc.fieldNames[0], std::string("memory_usage"));
    EXPECT_EQ(desc.fieldNames[3], std::string("worker_share_memory_usage"));
    ASSERT_EQ(desc.recordMask.size(), desc.fieldNames.size());
    EXPECT_TRUE(desc.recordMask[0]);
    EXPECT_TRUE(desc.recordMask[3]);
    EXPECT_FALSE(desc.recordMask[4]);  // sc_memory_usage
    EXPECT_FALSE(desc.recordMask[5]);  // sc_memory_limit
    EXPECT_TRUE(desc.recordGroup);
}

// GetResourceFieldDesc: thread-pool groups align with res_metrics.def sub-field order and are fully recorded.
TEST_F(ResourceJsonSchemaTest, ThreadPoolGroupOrder)
{
    const auto &desc = GetResourceFieldDesc(ResMetricName::WORKER_OC_SERVICE_THREAD_POOL);
    EXPECT_EQ(desc.groupName, std::string("worker_oc_service_thread_pool"));
    ASSERT_EQ(desc.fieldNames.size(), static_cast<size_t>(5));
    EXPECT_EQ(desc.fieldNames[0], std::string("idle_num"));
    EXPECT_EQ(desc.fieldNames[1], std::string("current_total_num"));
    EXPECT_EQ(desc.fieldNames[2], std::string("max_thread_num"));
    EXPECT_EQ(desc.fieldNames[3], std::string("waiting_task_num"));
    EXPECT_EQ(desc.fieldNames[4], std::string("thread_pool_usage"));
    for (bool recorded : desc.recordMask) {
        EXPECT_TRUE(recorded);
    }
}

// GetResourceFieldDesc: ods-flagged-false groups are recordGroup=false (omitted from JSON entirely).
TEST_F(ResourceJsonSchemaTest, OmittedGroups)
{
    EXPECT_FALSE(GetResourceFieldDesc(ResMetricName::SHARED_DISK).recordGroup);
    EXPECT_FALSE(GetResourceFieldDesc(ResMetricName::SC_LOCAL_CACHE).recordGroup);
    EXPECT_FALSE(GetResourceFieldDesc(ResMetricName::STREAM_COUNT).recordGroup);
    EXPECT_FALSE(GetResourceFieldDesc(ResMetricName::OBS_REQUEST_SUCCESS_RATE).recordGroup);
    EXPECT_FALSE(GetResourceFieldDesc(ResMetricName::WORKER_SC_SERVICE_THREAD_POOL).recordGroup);
    EXPECT_FALSE(GetResourceFieldDesc(ResMetricName::STREAM_REMOTE_SEND_SUCCESS_RATE).recordGroup);
}

// GetResourceFieldDesc: single-field recorded groups flatten (one field, one true mask).
TEST_F(ResourceJsonSchemaTest, SingleFieldGroups)
{
    const auto &activeClient = GetResourceFieldDesc(ResMetricName::ACTIVE_CLIENT_COUNT);
    ASSERT_EQ(activeClient.fieldNames.size(), static_cast<size_t>(1));
    EXPECT_EQ(activeClient.fieldNames[0], std::string("active_client_count"));
    EXPECT_TRUE(activeClient.recordMask[0]);
    EXPECT_TRUE(activeClient.recordGroup);

    const auto &etcdRate = GetResourceFieldDesc(ResMetricName::ETCD_REQUEST_SUCCESS_RATE);
    ASSERT_EQ(etcdRate.fieldNames.size(), static_cast<size_t>(1));
    EXPECT_TRUE(etcdRate.recordGroup);
}

// GetResourceFieldDesc: OC_HIT_NUM has 5 fields all recorded.
TEST_F(ResourceJsonSchemaTest, OcHitNumFields)
{
    const auto &desc = GetResourceFieldDesc(ResMetricName::OC_HIT_NUM);
    ASSERT_EQ(desc.fieldNames.size(), static_cast<size_t>(5));
    EXPECT_EQ(desc.fieldNames[0], std::string("mem_hit_num"));
    EXPECT_EQ(desc.fieldNames[4], std::string("miss_num"));
    for (bool recorded : desc.recordMask) {
        EXPECT_TRUE(recorded);
    }
}

TEST_F(ResourceJsonSchemaTest, BrpcStreamLeakCountGroup)
{
    const auto &desc = GetResourceFieldDesc(ResMetricName::BRPC_STREAM_LEAK_COUNT);
    EXPECT_EQ(desc.groupName, std::string("brpc_stream_leak_count"));
    ASSERT_EQ(desc.fieldNames.size(), static_cast<size_t>(1));
    EXPECT_EQ(desc.fieldNames[0], std::string("leak_count"));
    EXPECT_TRUE(desc.recordGroup);
}

TEST_F(ResourceJsonSchemaTest, DeferredCleanupQueueSizeGroup)
{
    const auto &desc = GetResourceFieldDesc(ResMetricName::DEFERRED_CLEANUP_QUEUE_SIZE);
    EXPECT_EQ(desc.groupName, std::string("deferred_cleanup_queue_size"));
    ASSERT_EQ(desc.fieldNames.size(), static_cast<size_t>(1));
    EXPECT_EQ(desc.fieldNames[0], std::string("queue_size"));
    EXPECT_TRUE(desc.recordGroup);
}
}  // namespace ut
}  // namespace datasystem
