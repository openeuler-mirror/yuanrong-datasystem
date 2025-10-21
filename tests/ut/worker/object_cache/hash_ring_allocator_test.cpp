/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2022. All rights reserved.
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
 * Description: Test interface to HashRing
 */
#include <google/protobuf/util/message_differencer.h>
#include <atomic>
#include <chrono>
#include <string>

#include "common.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/util/container_util.h"
#include "datasystem/common/util/wait_post.h"
#include "datasystem/worker/hash_ring/hash_ring.h"
#include "datasystem/worker/hash_ring/hash_ring_tools.h"

using namespace datasystem::worker;

DS_DECLARE_string(etcd_address);
DS_DECLARE_string(master_address);
DS_DECLARE_uint32(add_node_wait_time_s);

namespace datasystem {
namespace ut {
namespace {
// The valid range is [start, end)
struct Range {
    uint32_t start;
    uint32_t end;
};

WorkerPb MakeWorkerPb(std::initializer_list<uint32_t> &&tokens, WorkerPb::StatePb state = WorkerPb::ACTIVE)
{
    WorkerPb pb;
    for (auto token : tokens) {
        pb.mutable_hash_tokens()->Add(std::move(token));
    }
    pb.set_state(state);
    return pb;
}

uint32_t operator"" _W(unsigned long long int x)
{
    static int w = 10'000;
    return x * w;
}

uint32_t operator"" _M(unsigned long long int x)
{
    static int m = 1'000'000;
    return x * m;
}
}  // namespace

class HashRingAllocatorTest : public CommonTest {
protected:
    void SetUp() override
    {
        FLAGS_v = 1;
    }

    ChangeNodePb::RangePb MakeRange(std::string nodeId, uint32_t begin, uint32_t end, bool lost_all_range = false)
    {
        ChangeNodePb::RangePb range;
        range.set_workerid(nodeId);
        range.set_from(begin);
        range.set_end(end);
        range.set_finished(false);
        range.set_lost_all_range(lost_all_range);
        return range;
    }
    static constexpr int num2_ = 2;
    static constexpr int num3_ = 3;
    static constexpr int num4_ = 4;
    static constexpr int num5_ = 5;
    static constexpr int num6_ = 6;
};

TEST_F(HashRingAllocatorTest, AveragellyGenerateAll)
{
    HashRingPb ring;
    ring.mutable_workers()->insert({ "Worker_A", WorkerPb{} });
    ring.mutable_workers()->insert({ "Worker_B", WorkerPb{} });
    ring.mutable_workers()->insert({ "Worker_C", WorkerPb{} });

    std::set<std::string> workers{ "Worker_A", "Worker_B", "Worker_C" };
    ring = HashRingAllocator::GenerateAllHashTokens(ring, workers);

    auto ownership = HashRingAllocator(ring).GetOwnerShip();
    auto totalOwnerShip =
        std::accumulate(ownership.begin(), ownership.end(), 0u,
                        [](uint32_t value, const decltype(ownership)::value_type &p) { return value + p.second; });
    EXPECT_EQ(totalOwnerShip, UINT32_MAX);

    auto min = std::min_element(ownership.begin(), ownership.end(), SortByValue{})->second;
    auto max = std::max_element(ownership.begin(), ownership.end(), SortByValue{})->second;
    EXPECT_TRUE(max - min <= workers.size());
}

TEST_F(HashRingAllocatorTest, AddNode)
{
    std::set<std::string> workers{ "Worker_A", "Worker_B", "Worker_C" };

    // A ring like: A -> B -> A -> C -> B -> C
    HashRingPb ring;
    auto averageOwnership = UINT32_MAX / num6_;
    ring.mutable_workers()->insert({ "Worker_A", MakeWorkerPb({ averageOwnership * 0, averageOwnership * num2_ }) });
    ring.mutable_workers()->insert({ "Worker_B", MakeWorkerPb({ averageOwnership * 1, averageOwnership * num4_ }) });
    ring.mutable_workers()->insert(
        { "Worker_C", MakeWorkerPb({ averageOwnership * num3_, averageOwnership * num5_ }) });

    HashRingAllocator ac(ring);
    std::vector<uint32_t> newNodeTokens;
    DS_EXPECT_OK(ac.AddNode("Worker_New", num3_, newNodeTokens));

    ac.Print();

    auto ownership = ac.GetOwnerShip();
    auto totalOwnerShip =
        std::accumulate(ownership.begin(), ownership.end(), 0u,
                        [](uint32_t value, const decltype(ownership)::value_type &p) { return value + p.second; });
    EXPECT_EQ(totalOwnerShip, UINT32_MAX);

    // expect to split A B C, not the first three node A B A
    EXPECT_LT(ownership["Worker_A"], averageOwnership * num2_);
    EXPECT_LT(ownership["Worker_B"], averageOwnership * num2_);
    EXPECT_LT(ownership["Worker_C"], averageOwnership * num2_);
}

TEST_F(HashRingAllocatorTest, AddNodeCrossingOrigin)
{
    std::set<std::string> workers{ "Worker_A", "Worker_B" };

    // A ring like: A -> B
    HashRingPb ring;
    ring.mutable_workers()->insert({ "Worker_A", MakeWorkerPb({ UINT32_MAX * 1 / num3_ }) });
    ring.mutable_workers()->insert({ "Worker_B", MakeWorkerPb({ UINT32_MAX * num3_ / num4_ }) });

    HashRingAllocator ac(ring);
    std::vector<uint32_t> newNodeTokens;
    DS_EXPECT_OK(ac.AddNode("Worker_New", 1, newNodeTokens));
    ac.Print();

    // check insert between 0 and first node
    constexpr uint32_t token = 3597035109;
    EXPECT_EQ(newNodeTokens.at(0), token);

    auto ownership = ac.GetOwnerShip();
    auto totalOwnerShip =
        std::accumulate(ownership.begin(), ownership.end(), 0u,
                        [](uint32_t value, const decltype(ownership)::value_type &p) { return value + p.second; });
    EXPECT_EQ(totalOwnerShip, UINT32_MAX);
}

TEST_F(HashRingAllocatorTest, GetAddNodeInfo)
{
    HashRingPb ring;
    // Ring as like: [Worker_A:10000] [Worker_B:40000] [Worker_A:90000] [Worker_C:200000]
    //    [Worker_B:6000000] [Worker_C:27000000]
    ring.mutable_workers()->insert({ "Worker_A", MakeWorkerPb({ 1_W, 9_W }) });
    ring.mutable_workers()->insert({ "Worker_B", MakeWorkerPb({ 4_W, 6_M }) });
    ring.mutable_workers()->insert({ "Worker_C", MakeWorkerPb({ 20_W, 27_M }) });

    HashRingAllocator ac(ring);
    ac.Print();

    // Ring as like: [Worker_A:10000] [Worker_B:40000] [Worker_New1:55000] [Worker_A:90000] [Worker_New1:120000]
    //    [Worker_New2:180000] [Worker_C:200000] [Worker_B:6000000] [Worker_C:27000000]
    const uint32_t token5p5w = 55'000;
    ac.AddNode("Worker_New1", { token5p5w, 12_W });
    ac.AddNode("Worker_New2", { 18_W });
    ac.Print();

    std::map<std::string, ChangeNodePb> addnodeInfo;
    ac.GetAddNodeInfo(addnodeInfo);

    ChangeNodePb expectPb1;
    expectPb1.mutable_changed_ranges()->Add(MakeRange("Worker_A", 4_W, token5p5w));
    expectPb1.mutable_changed_ranges()->Add(MakeRange("Worker_C", 9_W, 12_W));

    ChangeNodePb expectPb2;
    expectPb2.mutable_changed_ranges()->Add(MakeRange("Worker_C", 12_W, 18_W));

    constexpr int nodeInfoSize = 2;
    EXPECT_TRUE(addnodeInfo.size() == nodeInfoSize);
    EXPECT_EQ(expectPb1.ShortDebugString(), addnodeInfo["Worker_New1"].ShortDebugString());
    EXPECT_EQ(expectPb2.ShortDebugString(), addnodeInfo["Worker_New2"].ShortDebugString());
}

TEST_F(HashRingAllocatorTest, RemoveNode)
{
    HashRingPb ring;
    // Ring as like: [Worker_A:10000] [Worker_B:40000] [Worker_A:90000] [Worker_A:190000] [Worker_C:200000]
    //    [Worker_B:6000000] [Worker_C:27000000]
    ring.mutable_workers()->insert({ "Worker_A", MakeWorkerPb({ 1_W, 9_W, 19_W }) });
    ring.mutable_workers()->insert({ "Worker_B", MakeWorkerPb({ 4_W, 6_M }) });
    ring.mutable_workers()->insert({ "Worker_C", MakeWorkerPb({ 20_W, 27_M }) });

    HashRingAllocator ac(ring);
    ac.Print();
    ac.RemoveNode("Worker_A", ring);
    ChangeNodePb pb = ring.del_node_info().at("Worker_A");
    ac.Print();

    ChangeNodePb expectPb;
    expectPb.mutable_changed_ranges()->Add(MakeRange("Worker_B", 27_M, 1_W, true));
    expectPb.mutable_changed_ranges()->Add(MakeRange("Worker_C", 4_W, 19_W, true));

    EXPECT_EQ(expectPb.ShortDebugString(), pb.ShortDebugString());
    auto ownership = ac.GetOwnerShip();
    EXPECT_EQ(ownership.count("Worker_A"), 0u);
}

TEST_F(HashRingAllocatorTest, RemoveConsecutiveNode_CrossOrigin)
{
    HashRingPb ring;
    // Ring as like: [Worker_A:90000] [Worker_C:200000] [Worker_B:6000000] [Worker_A:27000000]
    ring.mutable_workers()->insert({ "Worker_A", MakeWorkerPb({ 9_W, 27_M }) });
    ring.mutable_workers()->insert({ "Worker_B", MakeWorkerPb({ 6_M }) });
    ring.mutable_workers()->insert({ "Worker_C", MakeWorkerPb({ 20_W }) });

    HashRingAllocator ac(ring);
    ac.Print();
    ac.RemoveNode("Worker_A", ring);
    ChangeNodePb pb = ring.del_node_info().at("Worker_A");
    ac.Print();

    ChangeNodePb expectPb;
    expectPb.mutable_changed_ranges()->Add(MakeRange("Worker_C", 6_M, 9_W, true));

    EXPECT_EQ(expectPb.ShortDebugString(), pb.ShortDebugString());
    auto ownership = ac.GetOwnerShip();
    EXPECT_EQ(ownership.count("Worker_A"), 0u);
}

/*
cover the following scenarios:
    1. a common joining token removed.
    2. the joining node has two consecutive tokens
    3. two consecutive tokens from different joining nodes
*/
TEST_F(HashRingAllocatorTest, RemoveJoiningNode)
{
    HashRingPb ring;
    // Ring as like: [Worker_A:3000000] [Join_1:6000000] [Worker_B:9000000] [Join_1:12000000] [Join_2:15000000]
    // [Worker_A:18000000] [Join_2:21000000] [Join_1:24000000] [Worker_B:27000000] [Join_2:30000000] [Join_2:33000000]
    ring.mutable_workers()->insert({ "Worker_A", MakeWorkerPb({ 3_M, 18_M }) });
    ring.mutable_workers()->insert({ "Worker_B", MakeWorkerPb({ 9_M, 27_M }) });
    HashRingAllocator ac(ring);
    ac.AddNode("Join_1", { 6_M, 12_M, 24_M });
    ac.AddNode("Join_2", { 15_M, 21_M, 30_M, 33_M });
    std::map<std::string, ChangeNodePb> addnodeInfo;
    ac.GetAddNodeInfo(addnodeInfo);

    ring.mutable_workers()->insert({ "Join_1", MakeWorkerPb({ 6_M, 12_M, 24_M }, WorkerPb::JOINING) });
    ring.mutable_workers()->insert({ "Join_2", MakeWorkerPb({ 15_M, 21_M, 30_M, 33_M }, WorkerPb::JOINING) });
    for (auto &info : addnodeInfo) {
        (*ring.mutable_add_node_info())[info.first] = std::move(info.second);
    }

    LOG(INFO) << "before: " << worker::HashRingToJsonString(ring);

    HashRingPb outRing = ring;
    ac = HashRingAllocator(outRing);
    DS_ASSERT_OK(ac.RemoveNode("Join_1", outRing));
    LOG(INFO) << "after:  " << worker::HashRingToJsonString(outRing);

    // some ranges merge to Join_2's addNodeInfo, some ranges go through to delNodeInfo
    std::string expectRingJson = R"({
        "workers": {
            "Worker_A": {"hashTokens": [3000000, 18000000], "state": "ACTIVE"},
            "Worker_B": {"hashTokens": [9000000, 27000000], "state": "ACTIVE"},
            "Join_1": {"hashTokens": [6000000, 12000000, 24000000], "state": "JOINING"},
            "Join_2": {"hashTokens": [15000000, 21000000, 30000000, 33000000], "state": "JOINING"}},
        "addNodeInfo": {
            "Join_2": {"changedRanges": [
                {"workerId": "Worker_A", "from": 12000000, "end": 15000000},
                {"workerId": "Worker_B", "from": 18000000, "end": 21000000},
                {"workerId": "Worker_A", "from": 27000000, "end": 30000000},
                {"workerId": "Worker_A", "from": 30000000, "end": 33000000},
                {"workerId": "Worker_A", "from": 9000000, "end": 12000000}]}},
        "delNodeInfo": {
            "Join_1": {"changedRanges": [
                {"workerId": "Worker_B", "from": 3000000, "end": 6000000},
                {"workerId": "Join_2", "from": 9000000, "end": 12000000},
                {"workerId": "Worker_B", "from": 21000000, "end": 24000000}]}}
    })";
    HashRingPb expectRing;
    auto rc = google::protobuf::util::JsonStringToMessage(expectRingJson, &expectRing);
    ASSERT_TRUE(rc.ok());
    google::protobuf::util::MessageDifferencer differencer;
    differencer.set_repeated_field_comparison(google::protobuf::util::MessageDifferencer::AS_SET);
    ASSERT_TRUE(differencer.Compare(expectRing, outRing));
}

TEST_F(HashRingAllocatorTest, Volun)
{
    std::string hashRingJsonStr = R"({"clusterId": "127.0.0.1:9254","workers": {
        "127.0.0.1:3333": {"hashTokens": [90000, 270000], "workerUuid": "uuid_of_3333", "state": "ACTIVE"},
        "127.0.0.1:1111": {"hashTokens": [30000, 180000], "workerUuid": "uuid_of_1111", "state": "LEAVING",
        "needScaleDown": true},
        "127.0.0.1:2222": {"hashTokens": [60000, 120000], "workerUuid": "uuid_of_2222", "state": "ACTIVE"}
        },
        "clusterHasInit": true, "addNodeInfo": {
        "127.0.0.1:3333": {"changedRanges": [{
            "workerId": "127.0.0.1:1111", "from": 120000, "end": 180000, "lostAllRange": true, "finished": true}
        ]},
        "127.0.0.1:2222": {"changedRanges": [{
            "workerId": "127.0.0.1:1111", "from": 270000, "end": 30000, "lostAllRange": true},
        {
            "workerId": "127.0.0.1:1111", "from": 2222222, "end": 2222222, "lostAllRange": true}]}},
        "keyWithWorkerIdMetaMap": {
            "uuid_of_4444": "127.0.0.1:1111"
        }}
    )";
    HashRingPb hashRing;
    auto rc = google::protobuf::util::JsonStringToMessage(hashRingJsonStr, &hashRing);
    ASSERT_TRUE(rc.ok());
    HashRingAllocator ac(hashRing);
    ac.RemoveNode("127.0.0.1:1111", hashRing);
    LOG(INFO) << worker::HashRingToJsonString(hashRing);
    ASSERT_TRUE(hashRing.del_node_info().size() == 1); // del node info size 1
    std::string node = "127.0.0.1:1111";
    int changedRangesSize = 3;
    ASSERT_TRUE((*hashRing.mutable_del_node_info())[node].changed_ranges().size() == changedRangesSize);
}

TEST_F(HashRingAllocatorTest, RemoveVoluntaryScaleDownNode)
{
    std::string hashRingJsonStr = R"({
        "workers": {
                "127.0.0.1:0": {"hashTokens": [3319479556, 4197122568, 670947709, 1478938431],
                "workerUuid": "ZjI5NTJmMGMtMDZiMC00Zjg2LTkxMjItY2M4NWU1OTkxNTli", "state": "ACTIVE"
            },
                "172.16.100.20:32501": {"hashTokens": [2289952072, 178643793, 3032030464, 1971607754],
                "workerUuid": "MWYzYWU4ZWEtNTI5Yi00ZmZlLTg0ZDEtZmZkNmEwYWQxNGIx", "state": "ACTIVE"
            },
                "172.16.63.22:32501": {"hashTokens": [1171995910, 3728370843, 3166022422, 4023719973],
                "workerUuid": "YzhiOWY0NDctYzU2OC00ODUxLWJhNTEtMzExM2IwZWQ5ODZj",
                "state": "LEAVING", "needScaleDown": true
            }
        },
        "clusterHasInit": true
    })";
    HashRingPb hashRing;
    auto rc = google::protobuf::util::JsonStringToMessage(hashRingJsonStr, &hashRing);
    ASSERT_TRUE(rc.ok()) << rc.ToString();
    LOG(INFO) << hashRing.DebugString();
    auto status = HashRingAllocator(hashRing).RemoveNode("172.16.63.22:32501", hashRing);
    LOG(INFO) << hashRing.DebugString();
    ASSERT_TRUE(hashRing.del_node_info().find("172.16.63.22:32501") != hashRing.del_node_info().end());
}
}  // namespace ut
}  // namespace datasystem
