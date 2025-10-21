/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
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
 * Description: State client hashring healing tests.
 */

#include <google/protobuf/util/json_util.h>
#include <gtest/gtest.h>

#include "client/kv_cache/kv_client_scale_common.h"
#include "common.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/utils/status.h"
#include "datasystem/worker/hash_ring/hash_ring.h"
#include "datasystem/worker/hash_ring/hash_ring_tools.h"

namespace datasystem {
namespace st {
class KVClientHashRingHealingTest : public STCScaleTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        const int workerNum = 5;
        opts.numEtcd = 1;
        opts.numWorkers = workerNum;
        opts.numOBS = 1;
        opts.enableDistributedMaster = "true";
        opts.workerGflagParams = FormatString(
            " -v=1 -node_timeout_s=%d -node_dead_timeout_s=%d "
            "-enable_hash_ring_self_healing=true -auto_del_dead_node=true",
            nodeTimeout_, nodeDeadTimeout_);
        opts.waitWorkerReady = false;
        opts.addNodeTime = addNodeWaitTime_;
    }

    void CheckWorkerShutdown(std::initializer_list<uint32_t> workerIds, uint64_t timeoutMs = shutdownTimeoutMs)
    {
        auto timeOut = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeoutMs);
        bool allWorkerShutdown = false;
        while (std::chrono::steady_clock::now() < timeOut && !allWorkerShutdown) {
            allWorkerShutdown = true;
            for (auto id : workerIds) {
                if (cluster_->CheckWorkerProcess(id)) {
                    allWorkerShutdown = false;
                    break;
                }
            }
            const int interval = 100;  // 100ms;
            std::this_thread::sleep_for(std::chrono::milliseconds(interval));
        }
        ASSERT_TRUE(allWorkerShutdown);
    }

    void ChangeWorkerToScaleUp(int workerIdx)
    {
        std::string hashRingStr;
        DS_ASSERT_OK(db_->Get(ETCD_RING_PREFIX, "", hashRingStr));
        HashRingPb ring;
        ASSERT_TRUE(ring.ParseFromString(hashRingStr));

        std::map<uint32_t, std::string> nodes;
        for (const auto &item : ring.workers()) {
            for (auto token : item.second.hash_tokens()) {
                nodes.emplace(token, item.first);
            }
        }
        ASSERT_TRUE(!nodes.empty());
        HostPort pendingWorkerAddress;
        DS_ASSERT_OK(cluster_->GetWorkerAddr(workerIdx, pendingWorkerAddress));

        std::string pendingWorker = pendingWorkerAddress.ToString();
        (*ring.mutable_workers())[pendingWorker].set_state(WorkerPb::JOINING);

        auto &addNodeInfo = *ring.mutable_add_node_info();
        auto iter = nodes.begin();
        bool first = true;
        while (iter != nodes.end()) {
            if (pendingWorker == iter->second) {
                ChangeNodePb::RangePb range;
                std::string workerId = worker::LoopNext(nodes, iter)->second;
                if (first) {
                    workerId += "-fake";
                }
                first = false;
                range.set_workerid(workerId);
                range.set_from(worker::LoopPrev(nodes, iter)->first);
                range.set_end(iter->first);
                range.set_finished(false);
                addNodeInfo[pendingWorker].mutable_changed_ranges()->Add(std::move(range));
            }
            iter++;
        }
        DS_ASSERT_OK(db_->Put(ETCD_RING_PREFIX, "", ring.SerializeAsString()));
        LOG(INFO) << "Ring After modify:" << worker::HashRingToJsonString(ring);
    }

    void ChangeWorkerToPassiveScaleDown(int workerIdx)
    {
        std::string hashRingStr;
        DS_ASSERT_OK(db_->Get(ETCD_RING_PREFIX, "", hashRingStr));
        HashRingPb ring;
        ASSERT_TRUE(ring.ParseFromString(hashRingStr));
        std::map<uint32_t, std::string> nodes;
        for (const auto &item : ring.workers()) {
            for (auto token : item.second.hash_tokens()) {
                nodes.emplace(token, item.first);
            }
        }
        ASSERT_TRUE(!nodes.empty());
        HostPort pendingWorkerAddress;
        DS_ASSERT_OK(cluster_->GetWorkerAddr(workerIdx, pendingWorkerAddress));

        std::string pendingWorker = pendingWorkerAddress.ToString();
        std::string uuid = (*ring.mutable_workers())[pendingWorker].worker_uuid();
        std::string standbyWorker;
        auto &delNodeInfo = *ring.mutable_del_node_info();
        auto iter = nodes.begin();
        bool first = true;
        while (iter != nodes.end()) {
            if (pendingWorker == iter->second) {
                ChangeNodePb::RangePb range;
                std::string workerId = worker::LoopNext(nodes, iter)->second;
                if (first) {
                    workerId += "-fake";
                }
                first = false;
                range.set_workerid(workerId);
                range.set_from(worker::LoopPrev(nodes, iter)->first);
                range.set_end(iter->first);
                range.set_finished(false);
                delNodeInfo[pendingWorker].mutable_changed_ranges()->Add(std::move(range));
            } else {
                standbyWorker = iter->second;
            }
            iter++;
        }
        if (!standbyWorker.empty()) {
            (*ring.mutable_key_with_worker_id_meta_map())[uuid] = standbyWorker;
        }
        DS_ASSERT_OK(db_->Put(ETCD_RING_PREFIX, "", ring.SerializeAsString()));
        LOG(INFO) << "Ring After modify:" << worker::HashRingToJsonString(ring);
    }

    void ChangeWorkerToVoluntaryScaleDown(int workerIdx)
    {
        std::string hashRingStr;
        DS_ASSERT_OK(db_->Get(ETCD_RING_PREFIX, "", hashRingStr));
        HashRingPb ring;
        ASSERT_TRUE(ring.ParseFromString(hashRingStr));

        std::map<uint32_t, std::string> nodes;
        for (const auto &item : ring.workers()) {
            for (auto token : item.second.hash_tokens()) {
                nodes.emplace(token, item.first);
            }
        }
        ASSERT_TRUE(!nodes.empty());
        HostPort pendingWorkerAddress;
        DS_ASSERT_OK(cluster_->GetWorkerAddr(workerIdx, pendingWorkerAddress));

        std::string pendingWorker = pendingWorkerAddress.ToString();
        (*ring.mutable_workers())[pendingWorker].set_state(WorkerPb::LEAVING);
        (*ring.mutable_workers())[pendingWorker].set_need_scale_down(true);

        auto &addNodeInfo = *ring.mutable_add_node_info();
        auto iter = nodes.begin();
        bool first = true;
        while (iter != nodes.end()) {
            if (pendingWorker == iter->second) {
                ChangeNodePb::RangePb range;
                std::string dstWorkerId = worker::LoopNext(nodes, iter)->second;
                std::string srcWorkerId = pendingWorker;
                if (first) {
                    srcWorkerId += "-fake";
                }
                first = false;
                range.set_workerid(srcWorkerId);
                range.set_from(worker::LoopPrev(nodes, iter)->first);
                range.set_end(iter->first);
                range.set_finished(false);
                addNodeInfo[dstWorkerId].mutable_changed_ranges()->Add(std::move(range));
            }
            iter++;
        }
        DS_ASSERT_OK(db_->Put(ETCD_RING_PREFIX, "", ring.SerializeAsString()));
        LOG(INFO) << "Ring After modify:" << worker::HashRingToJsonString(ring);
    }

    void WaitTokenAllocateForWorker(std::initializer_list<uint32_t> indexes, uint64_t timeoutMs = shutdownTimeoutMs)
    {
        for (auto index : indexes) {
            HostPort workerAddress;
            DS_ASSERT_OK(cluster_->GetWorkerAddr(index, workerAddress));
            WaitHashRingChange(
                [&workerAddress](const HashRingPb &ring) {
                    auto iter = ring.workers().find(workerAddress.ToString());
                    // Check whether hash token generate for new node.
                    return iter != ring.workers().end() && iter->second.hash_tokens_size() > 0;
                },
                timeoutMs);
        }
    }

    void WaitPassiveScaleDownFinish(std::initializer_list<uint32_t> indexes, uint64_t timeoutMs = shutdownTimeoutMs)
    {
        for (auto index : indexes) {
            HostPort workerAddress;
            DS_ASSERT_OK(cluster_->GetWorkerAddr(index, workerAddress));
            WaitHashRingChange(
                [&workerAddress](const HashRingPb &ring) {
                    return ring.workers().find(workerAddress.ToString()) == ring.workers().end()
                           && ring.del_node_info().find(workerAddress.ToString()) == ring.del_node_info().end();
                },
                timeoutMs);
        }
    }

    void StartInjectWorkerAndWaitReady(std::initializer_list<uint32_t> indexes, int maxWaitTimeSec = 20)
    {
        StartWorkerAndWaitReady(indexes, maxWaitTimeSec);
        for (auto index : indexes) {
            DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, index, "worker.HashRingHealthCheck", "1*call(3000)"));
        }
    }

protected:
    int addNodeWaitTime_ = 3;
    int nodeTimeout_ = 3;
    int nodeDeadTimeout_ = 5;
};

TEST_F(KVClientHashRingHealingTest, TestHashRingMissing)
{
    std::initializer_list<uint32_t> workerIds = { 0, 1 };
    StartInjectWorkerAndWaitReady(workerIds);
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "worker.TryGetAndParseHashRingPb.interval", "call(1000)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "worker.TryGetAndParseHashRingPb.interval", "call(1000)"));
    DS_ASSERT_OK(db_->Delete(ETCD_RING_PREFIX, ""));

    CheckWorkerShutdown(workerIds);

    // restart.
    StartInjectWorkerAndWaitReady(workerIds);
    WaitTokenAllocateForWorker(workerIds);
}

TEST_F(KVClientHashRingHealingTest, TestHashRingParseFailed)
{
    std::initializer_list<uint32_t> workerIds = { 0, 1 };
    StartInjectWorkerAndWaitReady(workerIds);
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "worker.TryGetAndParseHashRingPb.interval", "call(1000)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "worker.TryGetAndParseHashRingPb.interval", "call(1000)"));
    DS_ASSERT_OK(db_->Put(ETCD_RING_PREFIX, "", "fake ring str!!"));

    CheckWorkerShutdown(workerIds);
    // restart.
    StartInjectWorkerAndWaitReady(workerIds);
    WaitTokenAllocateForWorker(workerIds);
}

TEST_F(KVClientHashRingHealingTest, TestHashRingScaleUpPending)
{
    StartInjectWorkerAndWaitReady({ 0, 1, 2 });

    // 1. Change worker0 to pending joing state.
    ChangeWorkerToScaleUp(0);

    // 2. worker0 will shutdown.
    CheckWorkerShutdown({ 0 });

    // 3. start worker0 and worker4
    const uint32_t newWorkerIndex = 3;
    StartInjectWorkerAndWaitReady({ 0, newWorkerIndex });

    // 4. Check worker4 start joining.
    WaitTokenAllocateForWorker({ 0, newWorkerIndex });
}

TEST_F(KVClientHashRingHealingTest, TestHashRingPassiveScaleDownPending)
{
    StartInjectWorkerAndWaitReady({ 0, 1, 2 });

    // 1. Change worker0 to pending joing state.
    ChangeWorkerToPassiveScaleDown(0);

    // 2. worker0 will shutdown.
    CheckWorkerShutdown({ 0 });

    WaitPassiveScaleDownFinish({ 0 });

    // 3. start worker0 and worker4
    const uint32_t newWorkerIndex = 3;
    StartInjectWorkerAndWaitReady({ 0, newWorkerIndex });

    // 4. Check worker4 start joining.
    WaitTokenAllocateForWorker({ 0, newWorkerIndex });
}

TEST_F(KVClientHashRingHealingTest, LEVEL2_TestHashRingVoluntaryScaleDownPending)
{
    StartInjectWorkerAndWaitReady({ 0, 1, 2 });
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "worker.BeforeShutdown", "1*return(K_OK)"));

    // 1. Change worker0 to pending joing state.
    ChangeWorkerToVoluntaryScaleDown(0);

    // 2. worker0 will shutdown.
    CheckWorkerShutdown({ 0 });

    // 3. start worker0 and worker4
    const uint32_t newWorkerIndex = 3;
    StartInjectWorkerAndWaitReady({ 0, newWorkerIndex });

    // 4. Check worker4 start joining.
    WaitTokenAllocateForWorker({ 0, newWorkerIndex });
}
} // namespace st
} // namespace datasytem
