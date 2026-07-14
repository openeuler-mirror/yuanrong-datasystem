/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
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
 * Description: Test the tools of protobuf.
 */

#include "datasystem/common/util/protobuf_util.h"

#include <gtest/gtest.h>

#include "ut/common.h"
#include "datasystem/protos/cluster_topology.pb.h"
#include "datasystem/protos/object_posix.pb.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace ut {
using ClusterTopologyPb = datasystem::ClusterTopologyPb;
using MembershipPb = datasystem::MembershipPb;

class ProtobufUtilTest : public CommonTest {
};

TEST_F(ProtobufUtilTest, TestCompareMapFields)
{
    ClusterTopologyPb pb1;
    ClusterTopologyPb pb2;
    // 1. test empty content.
    ASSERT_TRUE(CompareMapFields(pb1, pb2, "members"));

    MembershipPb wPb1;
    wPb1.set_id("test1");
    (*pb1.mutable_members())["worker1"] = wPb1;

    MembershipPb wPb2;
    wPb2.set_id("test1");
    (*pb2.mutable_members())["worker1"] = wPb2;

    // 1. Comparing two identical pbs, "TRUE" expected.
    ASSERT_TRUE(CompareMapFields(pb1, pb2, "members"));
    // 2. Continue verification after changing val, the expectation is still "TRUE".
    wPb2.set_id("test2");
    ASSERT_TRUE(CompareMapFields(pb1, pb2, "members"));
    // 3. But if the key is modified, the expectation will be "FALSE".
    (*pb2.mutable_members())["worker2"] = wPb2;
    ASSERT_FALSE(CompareMapFields(pb1, pb2, "members"));
}

TEST_F(ProtobufUtilTest, TestMisuseCompareMapFields)
{
    MembershipPb wPb1;
    wPb1.set_id("test1");
    ClusterTopologyPb pb1;
    (*pb1.mutable_members())["worker1"] = wPb1;

    MembershipPb wPb2;
    wPb2.set_id("test1");
    ClusterTopologyPb pb2;
    (*pb2.mutable_members())["worker1"] = wPb2;

    // 1. test wrong map field name
    ASSERT_FALSE(CompareMapFields(pb1, pb2, "worker"));
    // 2. test wrong type of field(not repeat)
    ASSERT_FALSE(CompareMapFields(pb1, pb2, "version"));
    // 3. test wrong type of field(repeat)
    ASSERT_FALSE(CompareMapFields(wPb1, wPb2, "tokens"));
    // 4. test wrong type of field(repeat non-basic type)
    MultiCreateRspPb mPb1;
    MultiCreateRspPb mPb2;
    ASSERT_FALSE(CompareMapFields<MultiCreateRspPb>(mPb1, mPb2, "results"));
}
}  // namespace ut
}  // namespace datasystem
