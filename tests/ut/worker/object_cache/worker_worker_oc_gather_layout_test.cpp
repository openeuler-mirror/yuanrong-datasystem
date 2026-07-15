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

/** Description: Tests worker Batch Get aggregate-gather layout validation. */

#include <gtest/gtest.h>

#include <limits>
#include <string>
#include <vector>

#include "datasystem/worker/object_cache/worker_worker_oc_gather_layout.h"

namespace datasystem {
namespace object_cache {
namespace {

GetObjectRemoteReqPb *AddUrmaRequest(BatchGetObjectRemoteReqPb &request, uint64_t dataSize, uint64_t segVa,
                                     uint64_t dataOffset, const std::string &host = "127.0.0.1", int32_t port = 1234)
{
    auto *item = request.add_requests();
    item->set_data_size(dataSize);
    auto *remote = item->mutable_urma_info();
    remote->set_seg_va(segVa);
    remote->set_seg_data_offset(dataOffset);
    remote->mutable_request_address()->set_host(host);
    remote->mutable_request_address()->set_port(port);
    remote->set_client_id("worker-client");
    return item;
}

GetObjectRemoteReqPb *AddUcpRequest(BatchGetObjectRemoteReqPb &request, uint64_t dataSize, uint64_t remoteBuf,
                                    const std::string &workerAddress = "worker-address")
{
    auto *item = request.add_requests();
    item->set_data_size(dataSize);
    auto *remote = item->mutable_ucp_info();
    remote->set_remote_buf(remoteBuf);
    remote->set_remote_worker_addr(workerAddress);
    remote->set_rkey("rkey");
    remote->mutable_remote_ip_addr()->set_host("127.0.0.1");
    remote->mutable_remote_ip_addr()->set_port(1234);
    return item;
}

void AllowAggregateGather(BatchGetObjectRemoteReqPb &request, uint64_t metadataSize)
{
    request.set_allow_aggregate_gather(true);
    request.set_aggregate_gather_metadata_size(metadataSize);
}

TEST(WorkerWorkerOcGatherLayoutTest, RejectsAbsentOrFalseGatherIntent)
{
    BatchGetObjectRemoteReqPb request;
    AddUrmaRequest(request, 4, 0x1000, 64);
    AddUrmaRequest(request, 4, 0x1000, 72);
    std::vector<AggregateGatherSubgroup> subgroups{ { 9, 9, 9 } };

    EXPECT_FALSE(BuildAggregateGatherPlan(request, 4, 1024, 1024, 1024, subgroups));
    EXPECT_TRUE(subgroups.empty());

    request.set_allow_aggregate_gather(false);
    EXPECT_FALSE(BuildAggregateGatherPlan(request, 4, 1024, 1024, 1024, subgroups));
    EXPECT_TRUE(subgroups.empty());
}

TEST(WorkerWorkerOcGatherLayoutTest, AcceptsExplicitCompatibleUrmaLayout)
{
    BatchGetObjectRemoteReqPb request;
    AllowAggregateGather(request, 64);
    AddUrmaRequest(request, 1, 0x1000, 128);
    AddUrmaRequest(request, 5, 0x1000, 196);
    AddUrmaRequest(request, 4, 0x1000, 268);
    std::vector<AggregateGatherSubgroup> subgroups;

    ASSERT_TRUE(BuildAggregateGatherPlan(request, 64, 1024, 1024, 1024, subgroups));
    ASSERT_EQ(subgroups.size(), 1u);
    EXPECT_EQ(subgroups[0].startIndex, 0u);
    EXPECT_EQ(subgroups[0].requestCount, 3u);
    EXPECT_EQ(subgroups[0].byteSize, 208u);
}

TEST(WorkerWorkerOcGatherLayoutTest, RejectsClientPayloadOnlyUrmaLayout)
{
    BatchGetObjectRemoteReqPb request;
    AllowAggregateGather(request, 64);
    AddUrmaRequest(request, 8, 0x1000, 0);
    AddUrmaRequest(request, 8, 0x1000, 16);
    AddUrmaRequest(request, 8, 0x1000, 32);
    std::vector<AggregateGatherSubgroup> subgroups;

    EXPECT_FALSE(BuildAggregateGatherPlan(request, 64, 1024, 1024, 1024, subgroups));
    EXPECT_TRUE(subgroups.empty());
}

TEST(WorkerWorkerOcGatherLayoutTest, RejectsFirstOffsetUnderflowEvenWithExplicitIntent)
{
    BatchGetObjectRemoteReqPb request;
    AllowAggregateGather(request, 64);
    AddUrmaRequest(request, 8, 0x1000, 63);
    std::vector<AggregateGatherSubgroup> subgroups;

    EXPECT_FALSE(BuildAggregateGatherPlan(request, 64, 1024, 1024, 1024, subgroups));
    EXPECT_TRUE(subgroups.empty());
}

TEST(WorkerWorkerOcGatherLayoutTest, RejectsUrmaSegmentOrDestinationMismatch)
{
    BatchGetObjectRemoteReqPb request;
    AllowAggregateGather(request, 4);
    AddUrmaRequest(request, 4, 0x1000, 64);
    auto *second = AddUrmaRequest(request, 4, 0x2000, 72);
    std::vector<AggregateGatherSubgroup> subgroups;

    EXPECT_FALSE(BuildAggregateGatherPlan(request, 4, 1024, 1024, 1024, subgroups));
    second->mutable_urma_info()->set_seg_va(0x1000);
    second->mutable_urma_info()->mutable_request_address()->set_host("127.0.0.2");
    EXPECT_FALSE(BuildAggregateGatherPlan(request, 4, 1024, 1024, 1024, subgroups));
}

TEST(WorkerWorkerOcGatherLayoutTest, ValidatesEachBatchBoundaryIndependently)
{
    BatchGetObjectRemoteReqPb request;
    AllowAggregateGather(request, 4);
    AddUrmaRequest(request, 4, 0x1000, 100);
    AddUrmaRequest(request, 4, 0x1000, 108);
    AddUrmaRequest(request, 4, 0x2000, 200);
    AddUrmaRequest(request, 4, 0x2000, 208);
    AddUrmaRequest(request, 4, 0x3000, 300);
    std::vector<AggregateGatherSubgroup> subgroups;

    ASSERT_TRUE(BuildAggregateGatherPlan(request, 4, 1024, 16, 2, subgroups));
    ASSERT_EQ(subgroups.size(), 3u);
    EXPECT_EQ(subgroups[0].startIndex, 0u);
    EXPECT_EQ(subgroups[0].requestCount, 2u);
    EXPECT_EQ(subgroups[1].startIndex, 2u);
    EXPECT_EQ(subgroups[1].requestCount, 2u);
    EXPECT_EQ(subgroups[2].startIndex, 4u);
    EXPECT_EQ(subgroups[2].requestCount, 1u);

    request.mutable_requests(2)->mutable_urma_info()->set_seg_data_offset(3);
    EXPECT_FALSE(BuildAggregateGatherPlan(request, 4, 1024, 16, 2, subgroups));
}

TEST(WorkerWorkerOcGatherLayoutTest, RejectsSizeAndRemoteRangeOverflow)
{
    BatchGetObjectRemoteReqPb request;
    AllowAggregateGather(request, 4);
    AddUrmaRequest(request, std::numeric_limits<uint64_t>::max(), 0x1000, 64);
    std::vector<AggregateGatherSubgroup> subgroups;

    EXPECT_FALSE(BuildAggregateGatherPlan(request, 4, std::numeric_limits<uint64_t>::max(),
                                          std::numeric_limits<uint64_t>::max(), 1024, subgroups));

    request.clear_requests();
    AddUrmaRequest(request, 4, 0x1000, std::numeric_limits<uint64_t>::max() - 4);
    EXPECT_FALSE(BuildAggregateGatherPlan(request, 4, 1024, 1024, 1024, subgroups));
}

TEST(WorkerWorkerOcGatherLayoutTest, ValidatesUcpLayoutAndIdentity)
{
    BatchGetObjectRemoteReqPb request;
    AllowAggregateGather(request, 4);
    AddUcpRequest(request, 4, 100);
    auto *second = AddUcpRequest(request, 4, 108);
    std::vector<AggregateGatherSubgroup> subgroups;

    ASSERT_TRUE(BuildAggregateGatherPlan(request, 4, 1024, 1024, 1024, subgroups));
    second->mutable_ucp_info()->set_remote_worker_addr("different-worker");
    EXPECT_FALSE(BuildAggregateGatherPlan(request, 4, 1024, 1024, 1024, subgroups));
}

TEST(WorkerWorkerOcGatherLayoutTest, RejectsAbsentOrMismatchedMetadataAttestationForOneItemSubgroup)
{
    BatchGetObjectRemoteReqPb request;
    request.set_allow_aggregate_gather(true);
    AddUrmaRequest(request, 4, 0x1000, 64);
    std::vector<AggregateGatherSubgroup> subgroups;

    EXPECT_FALSE(BuildAggregateGatherPlan(request, 4, 1024, 1024, 1024, subgroups));
    request.set_aggregate_gather_metadata_size(8);
    EXPECT_FALSE(BuildAggregateGatherPlan(request, 4, 1024, 1024, 1024, subgroups));
}

TEST(WorkerWorkerOcGatherLayoutTest, AcceptsExplicitZeroMetadataAttestationForOneItemSubgroup)
{
    BatchGetObjectRemoteReqPb request;
    AllowAggregateGather(request, 0);
    AddUrmaRequest(request, 4, 0x1000, 0);
    std::vector<AggregateGatherSubgroup> subgroups;

    ASSERT_TRUE(BuildAggregateGatherPlan(request, 0, 1024, 1024, 1024, subgroups));
    ASSERT_EQ(subgroups.size(), 1u);
    EXPECT_EQ(subgroups[0].byteSize, 4u);
}

TEST(WorkerWorkerOcGatherLayoutTest, ReceiverDecisionDefaultsSafeAndAcceptsMatchingAttestation)
{
    BatchGetObjectRemoteReqPb request;
    AddUrmaRequest(request, 4, 0x1000, 64);
    AddUrmaRequest(request, 4, 0x1000, 72);
    std::vector<AggregateGatherSubgroup> subgroups;

    EXPECT_FALSE(ShouldUseAggregateGather(request, false, 4, 1024, 1024, 1024, subgroups));
    AllowAggregateGather(request, 4);
    EXPECT_TRUE(ShouldUseAggregateGather(request, false, 4, 1024, 1024, 1024, subgroups));
    EXPECT_FALSE(ShouldUseAggregateGather(request, true, 4, 1024, 1024, 1024, subgroups));
    EXPECT_TRUE(subgroups.empty());
}

TEST(WorkerWorkerOcGatherLayoutTest, ProducerAttestationIncludesMetadataAndClearsUnsafeRequests)
{
    BatchGetObjectRemoteReqPb request;
    AddUrmaRequest(request, 4, 0x1000, 0);
    AddUrmaRequest(request, 4, 0x1000, 4);

    SetAggregateGatherAttestation(true, 0, request);
    EXPECT_TRUE(request.allow_aggregate_gather());
    EXPECT_EQ(request.aggregate_gather_metadata_size(), 0u);

    SetAggregateGatherAttestation(false, 64, request);
    EXPECT_FALSE(request.allow_aggregate_gather());
    EXPECT_EQ(request.aggregate_gather_metadata_size(), 0u);

    request.mutable_requests()->RemoveLast();
    SetAggregateGatherAttestation(true, 64, request);
    EXPECT_FALSE(request.allow_aggregate_gather());
    EXPECT_EQ(request.aggregate_gather_metadata_size(), 0u);
}

}  // namespace
}  // namespace object_cache
}  // namespace datasystem
