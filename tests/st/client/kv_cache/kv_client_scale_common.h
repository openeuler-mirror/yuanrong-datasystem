/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2024. All rights reserved.
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
 * Description: Util function for state client scale tests.
 */
#ifndef DATASYSTEM_TEST_ST_CLIENT_KV_CACHE_KV_CLIENT_SCALE_COMMON_H
#define DATASYSTEM_TEST_ST_CLIENT_KV_CACHE_KV_CLIENT_SCALE_COMMON_H

#include <unistd.h>
#include <chrono>
#include <csignal>
#include <initializer_list>
#include <string>

#include <gtest/gtest.h>

#include "client/kv_cache/kv_client_common.h"
#include "client/object_cache/oc_client_common.h"
#include "common.h"
#include "datasystem/common/util/hash_algorithm.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/utils/status.h"
#include "datasystem/worker/hash_ring/hash_ring.h"
#include "datasystem/worker/hash_ring/hash_ring_tools.h"
#include "datasystem/worker/hash_ring/hash_ring_allocator.h"

DS_DECLARE_string(etcd_address);

namespace datasystem {
namespace st {

constexpr int SCALE_UP_ADD_TIME = 3;
constexpr int SCALE_DOWN_ADD_TIME = 5;
constexpr int WORKER_RECEIVE_DELAY = 1;
class KVClientScaleCommon : virtual public OCClientCommon, public KVClientCommon {
public:
    void AssertAllNodesJoinIntoHashRing(int num)
    {
        if (!db_) {
            InitTestEtcdInstance();
        }
        std::string value;
        DS_ASSERT_OK(db_->Get(ETCD_RING_PREFIX, "", value));
        HashRingPb ring;
        ASSERT_TRUE(ring.ParseFromString(value));
        ASSERT_EQ(ring.workers_size(), num) << ring.ShortDebugString();
        for (auto &worker : ring.workers()) {
            ASSERT_TRUE(worker.second.state() == WorkerPb::ACTIVE) << ring.ShortDebugString();
        }
        ASSERT_TRUE(ring.add_node_info_size() == 0) << ring.ShortDebugString();
        ASSERT_TRUE(ring.del_node_info_size() == 0) << ring.ShortDebugString();
    }

    void AssertDelNodeInfoExist()
    {
        if (!db_) {
            InitTestEtcdInstance();
        }
        std::string value;
        DS_ASSERT_OK(db_->Get(ETCD_RING_PREFIX, "", value));
        HashRingPb ring;
        ASSERT_TRUE(ring.ParseFromString(value));
        ASSERT_TRUE(ring.del_node_info_size() != 0) << ring.ShortDebugString();
    }

    void WaitAllNodesJoinIntoHashRing(int num, uint64_t timeoutSec = 60, std::string azName = "")
    {
        int S2Ms = 1000;
        WaitHashRingChange(
            [&](const HashRingPb &hashRing) {
                if (hashRing.workers_size() != num || hashRing.add_node_info_size() != 0
                    || hashRing.del_node_info_size() != 0) {
                    return false;
                }
                for (auto &worker : hashRing.workers()) {
                    if (worker.second.state() != WorkerPb::ACTIVE) {
                        return false;
                    }
                }
                return true;
            },
            timeoutSec * S2Ms, azName);
        sleep(WORKER_RECEIVE_DELAY);
    }

    void WaitNodeVoluntaryScaleDownDone(const std::string &workerAddr, uint64_t timeoutSec = 60,
                                        std::string azName = "")
    {
        int S2Ms = 1000;
        WaitHashRingChange(
            [&](const HashRingPb &hashRing) {
                if (hashRing.add_node_info_size() != 0 || hashRing.del_node_info_size() != 0) {
                    return false;
                }
                for (auto &worker : hashRing.workers()) {
                    if (worker.first == workerAddr) {
                        return false;
                    }
                }
                return true;
            },
            timeoutSec * S2Ms, azName);
        sleep(WORKER_RECEIVE_DELAY);
    }

    template <typename F>
    void WaitHashRingChange(F &&f, uint64_t timeoutMs = shutdownTimeoutMs, std::string azName = "")
    {
        if (!db_) {
            InitTestEtcdInstance();
        }
        auto timeOut = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeoutMs);
        bool flag = false;
        HashRingPb ring;
        while (std::chrono::steady_clock::now() < timeOut) {
            std::string hashRingStr;
            auto trueRingTable = azName.empty() ? ETCD_RING_PREFIX : '/' + azName + ETCD_RING_PREFIX;
            DS_ASSERT_OK(db_->Get(trueRingTable, "", hashRingStr));
            ASSERT_TRUE(ring.ParseFromString(hashRingStr));
            if (f(ring)) {
                flag = true;
                break;
            }
            const int interval = 100;  // 100ms;
            std::this_thread::sleep_for(std::chrono::milliseconds(interval));
        }
        LOG(INFO) << "Check " << (flag ? "success" : "failed")
                  << ", Ring info:" << worker::HashRingToJsonString(ring);
        ASSERT_TRUE(flag);
    }

    void CheckMaster(std::initializer_list<uint32_t> indexes)
    {
        std::vector<std::shared_ptr<KVClient>> clients;
        for (auto index : indexes) {
            std::shared_ptr<KVClient> client;
            InitTestKVClient(index, client);
            clients.emplace_back(std::move(client));
        }
        // the assertion will fail if the hash ring on any worker is not inconsistent with the others.
        const auto testCount = 50;
        std::string outVal;
        for (auto i = 0; i < testCount; i++) {
            std::string hashKey = std::to_string(i);
            DS_ASSERT_OK(clients[i % indexes.size()]->Set(hashKey, "val"));
            std::string workeridKey = clients[i % indexes.size()]->Set("val");
            for (auto &client : clients) {
                DS_ASSERT_OK(client->Get(hashKey, outVal));
                DS_ASSERT_OK(client->Get(workeridKey, outVal));
            }
        }
    }

protected:
    const static uint64_t shutdownTimeoutMs = 60'000;  // 1min
};

class STCScaleTest : public KVClientScaleCommon {
public:
    void SetUp() override
    {
        CommonTest::SetUp();
        DS_ASSERT_OK(Init());
        ASSERT_TRUE(cluster_ != nullptr);
        DS_ASSERT_OK(cluster_->StartEtcdCluster());
        externalCluster_ = dynamic_cast<ExternalCluster *>(cluster_.get());
        InitTestEtcdInstance();
    }

    void TearDown() override
    {
        ResetClients();
        ExternalClusterTest::TearDown();
    }

    void StartForkWorkerAndWaitReady(std::initializer_list<uint32_t> indexes, int maxWaitTimeSec = 20)
    {
        for (auto i : indexes) {
            ASSERT_TRUE(externalCluster_->StartWorkerByForkProcess(i).IsOk()) << i;
        }
        for (auto i : indexes) {
            ASSERT_TRUE(cluster_->WaitNodeReady(WORKER, i, maxWaitTimeSec).IsOk()) << i;
            DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, i, "worker.HashRingHealthCheck", "1*call(3000)"));
        }
        for (auto i : indexes) {
            // When the scale-in scenario is tested, the scale-in failure may not be determined correctly.
            // Therefore, the scale-in failure is directly exited.
            DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, i, "Hashring.Scaletask.Fail", "abort()"));
        }
    }

    void StartWorkerAndWaitReady(std::initializer_list<uint32_t> indexes, int maxWaitTimeSec = 20,
                                 bool ResetGiveUpReconciliationTime = false, std::string gflag = "")
    {
        for (auto i : indexes) {
            if (ResetGiveUpReconciliationTime) {
                gflag += " -inject_actions=WorkerOCServiceImpl.GiveUpReconciliation.setHealthFile:call(5000)";
            }
            ASSERT_TRUE(externalCluster_->StartWorker(i, HostPort(), gflag).IsOk()) << i;
        }
        for (auto i : indexes) {
            ASSERT_TRUE(cluster_->WaitNodeReady(WORKER, i, maxWaitTimeSec).IsOk()) << i;
        }
        for (auto i : indexes) {
            // When the scale-in scenario is tested, the scale-in failure may not be determined correctly.
            // Therefore, the scale-in failure is directly exited.
            DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, i, "Hashring.Scaletask.Fail", "abort()"));
        }
    }

    void StartWorkerAndWaitReady(const std::initializer_list<uint32_t> indexes, const std::string &workerFlags,
                                 int maxWaitTimeSec = 120)
    {
        for (auto i : indexes) {
            ASSERT_TRUE(externalCluster_->StartWorker(i, HostPort(), workerFlags).IsOk()) << i;
        }
        for (auto i : indexes) {
            ASSERT_TRUE(cluster_->WaitNodeReady(WORKER, i, maxWaitTimeSec).IsOk()) << i;
        }
        for (auto i : indexes) {
            // When the scale-in scenario is tested, the scale-in failure may not be determined correctly.
            // Therefore, the scale-in failure is directly exited.
            DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, i, "Hashring.Scaletask.Fail", "abort()"));
        }
    }

    void RestartWorkerAndWaitReady(std::initializer_list<uint32_t> indexes, int signal = SIGTERM)
    {
        for (auto i : indexes) {
            if (signal == SIGTERM) {
                ASSERT_TRUE(cluster_->QuicklyShutdownWorker(i).IsOk()) << i;
            } else {
                ASSERT_TRUE(externalCluster_->KillWorker(i).IsOk()) << i;
            }
        }
        for (auto i : indexes) {
            ASSERT_TRUE(cluster_->StartNode(WORKER, i, " -client_reconnect_wait_s=1").IsOk()) << i;
        }
        int maxWaitTimeSec = 40;  // need reconliation
        for (auto i : indexes) {
            ASSERT_TRUE(cluster_->WaitNodeReady(WORKER, i, maxWaitTimeSec).IsOk()) << i;
        }
    }

    void InitClients()
    {
        InitTestKVClient(0, client_);
        InitTestKVClient(1, client1_);
        int worker2Index = 2;
        InitTestKVClient(worker2Index, client2_);
    }

    void ResetClients()
    {
        client_.reset();
        client1_.reset();
        client2_.reset();
    }

    std::string HashToStr(uint32_t hash)
    {
        const uint32_t width = 10;
        std::stringstream ss;
        ss << std::setfill('0') << std::setw(width) << hash;
        return ss.str();
    }

protected:
    ExternalCluster *externalCluster_ = nullptr;
    std::shared_ptr<KVClient> client_;
    std::shared_ptr<KVClient> client1_;
    std::shared_ptr<KVClient> client2_;
};
}  // namespace st
}  // namespace datasystem
#endif  // DATASYSTEM_TEST_ST_CLIENT_KV_CACHE_KV_CLIENT_SCALE_COMMON_H
