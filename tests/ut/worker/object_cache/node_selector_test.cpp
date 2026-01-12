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
 * Description: Test EvictionManager.
 */
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "../../../common/binmock/binmock.h"
#include "common.h"
#include "datasystem/common/constants.h"
#include "datasystem/master/object_cache/master_oc_service_impl.h"
#include "datasystem/master/resource_manager.h"
#include "datasystem/worker/object_cache/data_migrator/strategy/node_selector.h"
#include "datasystem/worker/object_cache/worker_master_oc_api.h"

using namespace ::testing;
using namespace datasystem::object_cache;
using namespace datasystem::worker;
using namespace datasystem::master;
DS_DECLARE_string(etcd_address);
DS_DECLARE_uint32(node_dead_timeout_s); // resourceManager used
namespace datasystem {
namespace ut {
class NodeSelectorTest : public CommonTest {
public:
    NodeSelectorTest() = default;
    ~NodeSelectorTest() override = default;

    void SetUp() override
    {
        CommonTest::SetUp();
        HostPort addr("127.0.0.1", 5555);
        localAddr_ = addr;
        uint64_t sharedMemoryBytes = 64 * 1024ul * 1024ul;
        datasystem::memory::Allocator::Instance()->Init(sharedMemoryBytes);
        MockGetWorkerMasterApi();
        std::vector<NodeInfo> nodes;
        MockReportResource(nodes);
        etcdStore_ = std::make_unique<EtcdStore>(FLAGS_etcd_address);
        etcdStore_->Init();
        etcdCM_ = new EtcdClusterManager(localAddr_, localAddr_, etcdStore_.get());
        NodeSelector::Instance().Init(localAddr_.ToString(), etcdCM_, nullptr);
    }

    void TearDown() override
    {
        CommonTest::TearDown();
    }

    void MockHashRingGetStandbyWorkerByAddr(const std::queue<std::string> &queues)
    {
        if (queues.empty()) {
            return;
        }
        auto standbyWorkers = std::make_shared<std::queue<std::string>>(std::move(queues));
        BINEXPECT_CALL(&HashRing::GetStandbyWorkerByAddr, (_, _))
            .Times(AtLeast(1))
            .WillRepeatedly(
                Invoke([this, standbyWorkers](const std::string &workerAddr, std::string &nextWorker) {
                    (void)workerAddr;
                    LOG(INFO) << "Mock HashRing GetStandbyWorkerByAddr";
                    if (!standbyWorkers->empty()) {
                        nextWorker = standbyWorkers->front();
                        standbyWorkers->pop();
                        standbyWorkers->push(nextWorker);
                    } else {
                        nextWorker = localAddr_.ToString();
                    }
                    return Status::OK();
                }));
    }

    void MockReportResource(const std::vector<NodeInfo> &nodes)
    {
        BINEXPECT_CALL(&NodeSelector::ReportResource, (_, _, _))
            .WillRepeatedly(
                Invoke([this, nodes](const std::shared_ptr<worker::WorkerMasterOCApi> &workerMasterApi,
                                    master::ResourceReportReqPb &req, master::ResourceReportRspPb &rsp) {
                    (void)workerMasterApi;
                    (void)req;
                    LOG(INFO) << "mock report resource";
                    auto *stats = rsp.mutable_stats();
                    for (const auto &nodeInfo : nodes) {
                        auto *stat = stats->Add();
                        stat->set_address(nodeInfo.nodeId);
                        stat->set_available_memory(nodeInfo.availableMemory);
                        stat->set_is_ready(nodeInfo.isReady);
                    }
                    return Status::OK();
                }));
    }

    void MockGetWorkerMasterApi()
    {
        BINEXPECT_CALL(&NodeSelector::GetWorkerMasterApi, (_))
            .WillRepeatedly(
                Invoke([this](std::shared_ptr<worker::WorkerMasterOCApi> &workerMasterApi) {
                    (void)workerMasterApi;
                    LOG(INFO) << "mock get api";
                    return Status::OK();
                }));
    }

    void MockCollectClusterInfo(const std::vector<NodeInfo> nodes)
    {
        MockGetWorkerMasterApi();
        MockReportResource(nodes);
        NodeSelector::Instance().CollectClusterInfo();
    }

    Status CallGetStandbyWorker(const std::unordered_set<std::string> &excludeNodes, std::string &outNode)
    {
        return NodeSelector::Instance().GetStandbyWorker(excludeNodes, outNode);
    }

    std::vector<NodeInfo> GetNodeSelectorRankList()
    {
        return NodeSelector::Instance().rankList_;
    }

    HostPort localAddr_;
    std::unique_ptr<EtcdStore> etcdStore_;
    EtcdClusterManager *etcdCM_;
};

TEST_F(NodeSelectorTest, TestCollectClusterInfo)
{
    std::vector<NodeInfo> nodes;
    NodeInfo node0("127.0.0.1:1111", 10 * 1024 * 1024, true);
    NodeInfo node1("127.0.0.1:1112", 20 * 1024 * 1024, true);
    NodeInfo node2("127.0.0.1:1113", 30 * 1024 * 1024, false);
    NodeInfo node3("127.0.0.1:1114", 40 * 1024 * 1024, true);
    NodeInfo node4("127.0.0.1:1115", 50 * 1024 * 1024, true);
    NodeInfo node5("127.0.0.1:1116", 60 * 1024 * 1024, true);
    nodes.emplace_back(node0);
    nodes.emplace_back(node1);
    nodes.emplace_back(node2);
    nodes.emplace_back(node3);
    nodes.emplace_back(node4);
    nodes.emplace_back(node5);

    std::vector<NodeInfo> expectNodes;
    expectNodes.emplace_back(node5);
    expectNodes.emplace_back(node4);
    expectNodes.emplace_back(node3);
    expectNodes.emplace_back(node1);
    expectNodes.emplace_back(node0);
    expectNodes.emplace_back(node2);
    MockCollectClusterInfo(nodes);
    auto rankList = GetNodeSelectorRankList();
    bool isSorted = std::is_sorted(rankList.begin(), rankList.end(),
                                   [](const NodeInfo& a, const NodeInfo& b) {
                                        return b < a; });
    ASSERT_TRUE(isSorted);
    ASSERT_TRUE(rankList.size() == expectNodes.size());
    for (uint64_t i = 0; i < rankList.size(); ++i) {
        ASSERT_TRUE(rankList[i].nodeId == expectNodes[i].nodeId);
    }
}

TEST_F(NodeSelectorTest, TestSelectNode)
{
    // Case 1: rankList_ is empty, return the standby worker.
    std::vector<NodeInfo> nodes;
    MockCollectClusterInfo(nodes);
    std::queue<std::string> standbyWorkers;
    std::string standbyWorker = "workerAddress0";
    standbyWorkers.push(standbyWorker);
    MockHashRingGetStandbyWorkerByAddr(standbyWorkers);

    std::unordered_set<std::string> excludeNodes;
    excludeNodes.emplace(localAddr_.ToString());
    std::string preferNode;
    size_t needSize = 1024;
    std::string outNode;
    auto rc = NodeSelector::Instance().SelectNode(excludeNodes, preferNode, needSize, outNode);
    DS_ASSERT_OK(rc);
    ASSERT_TRUE(outNode == standbyWorker);

    // Case 2: The maximum remaining capacity in rankList_ is less than 1MB, return K_NO_SPACE
    std::string workerAddress0 = "127.0.0.1:1110";
    int64_t availableMemory0 = 10 * 1024;
    std::string workerAddress1 = "127.0.0.1:1111";
    int64_t availableMemory1 = 100 * 1024;
    nodes.emplace_back(NodeInfo(workerAddress0, availableMemory0, true));
    nodes.emplace_back(NodeInfo(workerAddress1, availableMemory1, true));
    outNode.clear();
    MockCollectClusterInfo(nodes);
    rc = NodeSelector::Instance().SelectNode(excludeNodes, preferNode, needSize, outNode);
    ASSERT_TRUE(rc.GetCode() == K_NO_SPACE);

    // Case 3: prefer node
    std::string workerAddress2 = "127.0.0.1:1112";
    int64_t availableMemory2 = 10 * 1024 * 1024;
    nodes.emplace_back(NodeInfo(workerAddress2, availableMemory2, true));
    preferNode = workerAddress1;
    MockCollectClusterInfo(nodes);
    outNode.clear();
    rc = NodeSelector::Instance().SelectNode(excludeNodes, preferNode, needSize, outNode);
    DS_ASSERT_OK(rc);
    ASSERT_TRUE(outNode == preferNode);

    // Case 4: Select the one of the max 5;
    std::string workerAddress3 = "127.0.0.1:1113";
    int64_t availableMemory3 = 30 * 1024 * 1024;
    nodes.emplace_back(NodeInfo(workerAddress3, availableMemory3, true));
    std::string workerAddress4 = "127.0.0.1:1114";
    int64_t availableMemory4 = 40 * 1024 * 1024;
    nodes.emplace_back(NodeInfo(workerAddress4, availableMemory4, true));
    std::string workerAddress5 = "127.0.0.1:1115";
    int64_t availableMemory5 = 50 * 1024 * 1024;
    nodes.emplace_back(NodeInfo(workerAddress5, availableMemory5, true));
    std::string workerAddress6 = "127.0.0.1:1116";
    int64_t availableMemory6 = 60 * 1024 * 1024;
    nodes.emplace_back(NodeInfo(workerAddress6, availableMemory6, true));
    std::string workerAddress7 = "127.0.0.1:1117";
    int64_t availableMemory7 = 70 * 1024 * 1024;
    nodes.emplace_back(NodeInfo(workerAddress7, availableMemory7, true));
    MockCollectClusterInfo(nodes);
    preferNode.clear();
    outNode.clear();
    rc = NodeSelector::Instance().SelectNode(excludeNodes, preferNode, needSize, outNode);
    DS_ASSERT_OK(rc);
    ASSERT_TRUE(outNode == workerAddress3 || outNode == workerAddress4 || outNode == workerAddress5 ||
                outNode == workerAddress6 || outNode == workerAddress7);

    // Case 5: Test isReady
    std::string unreadyAddress3 = "127.0.0.2:1113";
    int64_t unreadyAM3 = 300 * 1024 * 1024;
    nodes.emplace_back(NodeInfo(unreadyAddress3, unreadyAM3, false));
    std::string unreadyAddress4 = "127.0.0.2:1114";
    int64_t unreadyAM4 = 400 * 1024 * 1024;
    nodes.emplace_back(NodeInfo(unreadyAddress4, unreadyAM4, false));
    std::string unreadyAddress5 = "127.0.0.2:1115";
    int64_t unreadyAM5 = 500 * 1024 * 1024;
    nodes.emplace_back(NodeInfo(unreadyAddress5, unreadyAM5, false));
    std::string unreadyAddress6 = "127.0.0.2:1116";
    int64_t unreadyAM6 = 600 * 1024 * 1024;
    nodes.emplace_back(NodeInfo(unreadyAddress6, unreadyAM6, false));
    std::string unreadyAddress7 = "127.0.0.2:1117";
    int64_t unreadyAM7 = 700 * 1024 * 1024;
    nodes.emplace_back(NodeInfo(unreadyAddress7, unreadyAM7, false));
    MockCollectClusterInfo(nodes);
    preferNode.clear();
    outNode.clear();
    rc = NodeSelector::Instance().SelectNode(excludeNodes, preferNode, needSize, outNode);
    DS_ASSERT_OK(rc);
    ASSERT_TRUE(outNode == workerAddress3 || outNode == workerAddress4 || outNode == workerAddress5 ||
                outNode == workerAddress6 || outNode == workerAddress7);

    // Case 7: There is no nodes that available memory is larger than the needSize,
    // it will return the max availableMemory node which is ready and not in exclude nodes
    needSize = 1000 * 1024 * 1024;
    rc = NodeSelector::Instance().SelectNode(excludeNodes, preferNode, needSize, outNode);
    DS_ASSERT_OK(rc);
    ASSERT_TRUE(outNode == workerAddress7);
}

TEST_F(NodeSelectorTest, TestGetStandbyWorker)
{
    std::unordered_set<std::string> excludeNodes;
    std::string outNode;
    std::queue<std::string> standbyWorkers;
    std::string workerAddress0 = "workerAddress0";
    std::string workerAddress1 = "workerAddress1";
    std::string workerAddress2 = "workerAddress2";
    std::string workerAddress3 = "workerAddress3";
    std::string workerAddress4 = "workerAddress4";
    std::string workerAddress5 = "workerAddress5";
    // Case 1: The exclude nodes is empty, can get the first next worker address
    standbyWorkers.push(workerAddress0);
    standbyWorkers.push(workerAddress1);
    standbyWorkers.push(workerAddress2);
    standbyWorkers.emplace(localAddr_.ToString());
    MockHashRingGetStandbyWorkerByAddr(standbyWorkers);
    DS_ASSERT_OK(CallGetStandbyWorker(excludeNodes, outNode));
    ASSERT_TRUE(outNode == workerAddress0);

    // Case 2: The exclude is not empty and it size is less than 5,
    // can get the first worker that not in the exclude nodes
    excludeNodes.emplace(workerAddress0);
    excludeNodes.emplace(workerAddress1);
    standbyWorkers = {};
    standbyWorkers.push(workerAddress0);
    standbyWorkers.push(workerAddress1);
    standbyWorkers.push(workerAddress2);
    standbyWorkers.emplace(localAddr_.ToString());
    outNode.clear();
    DS_ASSERT_OK(CallGetStandbyWorker(excludeNodes, outNode));
    ASSERT_TRUE(outNode == workerAddress2);

    // Case 3: Except local address, all workers are in exclude nodes
    // can not fount the outNode and the error code is K_NOT_FOUND
    excludeNodes.emplace(workerAddress2);
    standbyWorkers = {};
    standbyWorkers.push(workerAddress0);
    standbyWorkers.push(workerAddress1);
    standbyWorkers.push(workerAddress2);
    standbyWorkers.emplace(localAddr_.ToString());
    outNode.clear();
    MockHashRingGetStandbyWorkerByAddr(standbyWorkers);
    ASSERT_TRUE(CallGetStandbyWorker(excludeNodes, outNode).GetCode() == K_NOT_FOUND);
    ASSERT_TRUE(outNode.empty());

    // Case 4: The pre 5 next worker is all in the out nodes, it reached the max query counts
    // can not found the standby worker and the error code is K_NOT_FOUND
    excludeNodes.emplace(workerAddress3);
    excludeNodes.emplace(workerAddress4);
    standbyWorkers = {};
    standbyWorkers.push(workerAddress0);
    standbyWorkers.push(workerAddress1);
    standbyWorkers.push(workerAddress2);
    standbyWorkers.push(workerAddress3);
    standbyWorkers.push(workerAddress4);
    standbyWorkers.push(workerAddress5);
    standbyWorkers.emplace(localAddr_.ToString());
    outNode.clear();
    MockHashRingGetStandbyWorkerByAddr(standbyWorkers);
    ASSERT_TRUE(CallGetStandbyWorker(excludeNodes, outNode).GetCode() == K_NOT_FOUND);
    ASSERT_TRUE(outNode.empty());
}

class ResourceManagerTest : public CommonTest, public ResourceManager {
public:
    ResourceManagerTest() = default;
    ~ResourceManagerTest() override = default;

    void SetUp() override
    {
        FLAGS_node_dead_timeout_s = 2;
    }

    void TearDown() override
    {
        CommonTest::TearDown();
    }

    void CallClearWriteSnapshot()
    {
        LOG(INFO) << "Call clear write snapshot";
        return ClearWriteSnapshot();
    }

    void CallSwitchSnapshots()
    {
        return SwitchSnapshots();
    }

    std::string localAddr_ = "127.0.0.1:6789";
};

TEST_F(ResourceManagerTest, TestReadWriteSnapshots) {
    std::vector<std::thread> threads;
    int loopCount = 100;
    for (int i = 0; i < loopCount; ++i) {
        threads.emplace_back([this, i]() {
            master::ResourceReportReqPb req;
            auto *stat = req.mutable_stat();
            stat->set_address("node" + std::to_string(i) + ":8080");
            stat->set_available_memory(i * 1024);
            stat->set_is_ready(true);
            master::ResourceReportRspPb rsp;
            ReportResource(req, rsp);
        });
    }
    for (auto &t : threads) {
        t.join();
    }
    master::ResourceReportReqPb req;
    master::WorkerStat *stat = req.mutable_stat();
    stat->set_address(localAddr_);
    master::ResourceReportRspPb rsp;
    ReportResource(req, rsp);
    // Step 1: At this time, the read snapshot is empty, and the data is in write snapshot.
    EXPECT_EQ(rsp.stats_size(), 0);

    // Step 2: At this time, switch the read/write snapshot,
    // so the rsp stats size is loopCount + 1.
    CallSwitchSnapshots();
    rsp.Clear();
    ReportResource(req, rsp);
    EXPECT_EQ(rsp.stats_size(), loopCount + 1);

    // Step 3: There is one data in write snapshot and be cleared, and switch it to read snapshot,
    // so the rsp stats size is 0;
    sleep(FLAGS_node_dead_timeout_s + 1);
    CallClearWriteSnapshot();
    CallSwitchSnapshots();
    rsp.Clear();
    ReportResource(req, rsp);
    EXPECT_EQ(rsp.stats_size(), 0);

    // Step 4: There is loopCount + 1 data in write snapshot and the loopCount data be cleared, only left one data in it,
    // and switch it to read snapshot, so the rsp stats size is 1(The one data is from step 3 request).
    CallClearWriteSnapshot();
    CallSwitchSnapshots();
    rsp.Clear();
    ReportResource(req, rsp);
    EXPECT_EQ(rsp.stats_size(), 1);
}
}  // namespace ut
}  // namespace datasystem
