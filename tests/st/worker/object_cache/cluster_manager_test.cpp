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
#include <atomic>
#include <chrono>
#include <string>

#include "common.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/worker/cluster_manager/cluster_manager.h"

using namespace datasystem::worker;

DS_DECLARE_string(etcd_address);
DS_DECLARE_string(master_address);
DS_DECLARE_uint32(add_node_wait_time_s);
DS_DECLARE_int32(minloglevel);

namespace datasystem {
namespace st {
class ClusterManagerTest : public ExternalClusterTest {
protected:
    ClusterManagerTest() = default;

    ~ClusterManagerTest() = default;

    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numEtcd = 1;
        opts.numMasters = 0;
        opts.numWorkers = 0;
        FLAGS_v = 1;
        FLAGS_add_node_wait_time_s = 0;
    }

    void TearDown() override
    {
        ExternalClusterTest::TearDown();
    }

    void InitTestEtcdInstance()
    {
        std::string etcdAddress;
        for (size_t i = 0; i < cluster_->GetEtcdNum(); ++i) {
            std::pair<HostPort, HostPort> addrs;
            cluster_->GetEtcdAddrs(i, addrs);
            if (!etcdAddress.empty()) {
                etcdAddress += ",";
            }
            etcdAddress += addrs.first.ToString();
        }
        FLAGS_etcd_address = etcdAddress;
        LOG(INFO) << "The etcd address is:" << FLAGS_etcd_address << std::endl;
        db_ = std::make_unique<EtcdStore>(etcdAddress);
        if ((db_ != nullptr) && (db_->Init().IsOk())) {
            db_->DropTable(ETCD_RING_PREFIX);
            // We don't check rc here. If table to drop does not exist, it's fine.
            (void)db_->CreateTable(ETCD_RING_PREFIX, ETCD_RING_PREFIX);
        }
    }

    void InitAllClusterManagers(uint32_t workerNum, const int pqSize = 2000)
    {
        workerIds_.resize(workerNum);
        for (uint32_t i = 0; i < workerNum; i++) {
            HostPort addr;
            addr.ParseString("127.0.0.1:" + std::to_string(i));
            workerIds_[i] = addr.ToString();
            etcdStores_.emplace_back(std::make_unique<EtcdStore>(FLAGS_etcd_address));
            DS_ASSERT_OK(etcdStores_.back()->Init());
            clusterStores_.emplace_back(std::make_unique<EtcdClusterStore>(etcdStores_.back().get()));
            clusterManagers_.emplace_back(std::make_unique<ClusterManager>(addr, addr, clusterStores_.back().get(),
                                                                       nullptr, pqSize));
            ClusterInfo clusterInfo;
            DS_ASSERT_OK(ClusterManager::ConstructClusterInfoViaEtcd(etcdStores_.back().get(), clusterInfo));
            DS_ASSERT_OK(clusterManagers_.back()->Init(clusterInfo));
            clusterManagers_.back()->SetWorkerReady();
        }
    }

    void ShutDownAllClusterManagers()
    {
        for (auto &a : clusterManagers_) {
            a.reset();
        }
        for (auto &a : clusterStores_) {
            a.reset();
        }
        for (auto &a : etcdStores_) {
            a.reset();
        }
    }

    void InitClusterManagersInList(const std::vector<size_t> &idxs)
    {
        for (size_t i : idxs) {
            if (i < clusterManagers_.size()) {
                HostPort addr;
                addr.ParseString("127.0.0.1:" + std::to_string(i));
                etcdStores_[i] = std::make_unique<EtcdStore>(FLAGS_etcd_address);
                DS_ASSERT_OK(etcdStores_[i]->Init());
                clusterStores_[i] = std::make_unique<EtcdClusterStore>(etcdStores_[i].get());
                clusterManagers_[i] = std::make_unique<ClusterManager>(addr, addr, clusterStores_[i].get(),
                                                                   nullptr);
                ClusterInfo clusterInfo;
                DS_ASSERT_OK(ClusterManager::ConstructClusterInfoViaEtcd(etcdStores_[i].get(), clusterInfo));
                DS_ASSERT_OK(clusterManagers_[i]->Init(clusterInfo));
                clusterManagers_[i]->SetWorkerReady();
            }
        }
    }

    void ShutDownRestartAllClusterManagers(const std::vector<size_t> &idxs)
    {
        for (size_t i : idxs) {
            if (i < clusterManagers_.size()) {
                clusterManagers_[i].reset();
                clusterStores_[i].reset();
                etcdStores_[i].reset();
                HostPort addr;
                addr.ParseString("127.0.0.1:" + std::to_string(i));
                etcdStores_[i] = std::make_unique<EtcdStore>(FLAGS_etcd_address);
                DS_ASSERT_OK(etcdStores_[i]->Init());
                clusterStores_[i] = std::make_unique<EtcdClusterStore>(etcdStores_[i].get());
                clusterManagers_[i] = std::make_unique<ClusterManager>(addr, addr, clusterStores_[i].get(),
                                                                   nullptr);
                ClusterInfo clusterInfo;
                DS_ASSERT_OK(ClusterManager::ConstructClusterInfoViaEtcd(etcdStores_[i].get(), clusterInfo));
                DS_ASSERT_OK(clusterManagers_[i]->Init(clusterInfo));
                clusterManagers_[i]->SetWorkerReady();
            }
        }
    }

    std::unique_ptr<EtcdStore> db_;
    std::vector<std::string> workerIds_;
    std::vector<std::unique_ptr<EtcdStore>> etcdStores_;
    std::vector<std::unique_ptr<EtcdClusterStore>> clusterStores_;
    std::vector<std::unique_ptr<ClusterManager>> clusterManagers_;
};

TEST_F(ClusterManagerTest, DISABLED_StartAllClusterManagers1)
{
    FLAGS_minloglevel = 3;
    int workerNum = 50;
    int pqSize = 10;
    InitTestEtcdInstance();
    InitAllClusterManagers(workerNum, pqSize);
    DS_ASSERT_OK(inject::Set("ClusterManager.CheckWaitNodeTableComplete.returnError", "call()"));
    for (auto &cm : clusterManagers_) {
        DS_ASSERT_OK(cm->CheckWaitNodeTableComplete());
    }
    DS_ASSERT_OK(inject::Set("EtcdStore.LaunchKeepAliveThreads.shutdown", "call()"));
    DS_ASSERT_OK(inject::Set("EtcdStore.WatchRun.shutdown", "call()"));
    LOG(INFO) << "Start destructing cluster managers.";
    clusterManagers_.clear();
}

TEST_F(ClusterManagerTest, DISABLED_StartAllClusterManagers2)
{
    FLAGS_minloglevel = 3;
    int workerNum = 100;
    int pqSize = 50;
    InitTestEtcdInstance();
    InitAllClusterManagers(workerNum, pqSize);
    DS_ASSERT_OK(inject::Set("ClusterManager.CheckWaitNodeTableComplete.returnError", "call()"));
    for (auto &cm : clusterManagers_) {
        DS_ASSERT_OK(cm->CheckWaitNodeTableComplete());
    }
    DS_ASSERT_OK(inject::Set("EtcdStore.LaunchKeepAliveThreads.shutdown", "call()"));
    DS_ASSERT_OK(inject::Set("EtcdStore.WatchRun.shutdown", "call()"));
    LOG(INFO) << "Start destructing cluster managers.";
    clusterManagers_.clear();
}

TEST_F(ClusterManagerTest, DISABLED_LEVEL1_StartAllClusterManagers3)
{
    FLAGS_minloglevel = 3;
    int workerNum = 100;
    InitTestEtcdInstance();
    InitAllClusterManagers(workerNum);  // priority queue size == 2000 by default
    DS_ASSERT_OK(inject::Set("ClusterManager.CheckWaitNodeTableComplete.returnError", "call()"));
    for (auto &cm : clusterManagers_) {
        DS_ASSERT_OK(cm->CheckWaitNodeTableComplete());
    }
    LOG(INFO) << "Start destructing cluster managers.";
    ShutDownAllClusterManagers();
}

TEST_F(ClusterManagerTest, RestartAllClusterManagers1)
{
    int workerNum = 10;
    InitTestEtcdInstance();
    InitAllClusterManagers(workerNum);
    DS_ASSERT_OK(inject::Set("ClusterManager.CheckWaitNodeTableComplete.returnError", "call()"));
    for (auto &cm : clusterManagers_) {
        DS_ASSERT_OK(cm->CheckWaitNodeTableComplete());
    }
    // Shut down all nodes and then restart all nodes
    ShutDownAllClusterManagers();
    std::vector<size_t> idxs(workerNum);
    std::iota(idxs.begin(), idxs.end(), 0);
    inject::Set("ClusterManager.IfNeedTriggerReconciliation.noreconciliation", "return(K_OK)");
    DS_ASSERT_OK(inject::Set("ClusterManager.CheckWaitNodeTableComplete.returnError", "call()"));
    InitClusterManagersInList(idxs);
    for (auto &cm : clusterManagers_) {
        DS_ASSERT_OK(cm->CheckWaitNodeTableComplete());
    }
    ShutDownAllClusterManagers();
}

TEST_F(ClusterManagerTest, RestartAllClusterManagers2)
{
    int workerNum = 10;
    InitTestEtcdInstance();
    InitAllClusterManagers(workerNum);
    DS_ASSERT_OK(inject::Set("ClusterManager.CheckWaitNodeTableComplete.returnError", "call()"));
    for (auto &cm : clusterManagers_) {
        DS_ASSERT_OK(cm->CheckWaitNodeTableComplete());
    }
    // Shut down one node and then restart one node
    // Restart one and then shutdown the next one
    std::vector<size_t> idxs(workerNum);
    std::iota(idxs.begin(), idxs.end(), 0);
    inject::Set("ClusterManager.IfNeedTriggerReconciliation.noreconciliation", "return(K_OK)");
    ShutDownRestartAllClusterManagers(idxs);
    DS_ASSERT_OK(inject::Set("ClusterManager.CheckWaitNodeTableComplete.returnError", "call()"));
    for (auto &cm : clusterManagers_) {
        DS_ASSERT_OK(cm->CheckWaitNodeTableComplete());
    }
    ShutDownAllClusterManagers();
}

TEST_F(ClusterManagerTest, NoProgressEarlyTermination)
{
    int workerNum = 3;
    InitTestEtcdInstance();
    InitAllClusterManagers(workerNum);
    DS_ASSERT_OK(inject::Set("ClusterManager.CheckWaitNodeTableComplete.returnError", "call()"));
    for (auto &cm : clusterManagers_) {
        DS_ASSERT_OK(cm->CheckWaitNodeTableComplete());
    }
    ShutDownAllClusterManagers();
    std::vector<size_t> restartIdxs = {0};
    inject::Set("ClusterManager.IfNeedTriggerReconciliation.noreconciliation", "return(K_OK)");
    DS_ASSERT_OK(inject::Set("ClusterManager.CheckWaitNodeTableComplete.waitTime", "call(120)"));
    DS_ASSERT_OK(inject::Set("ClusterManager.CheckWaitNodeTableComplete.noProgressTimeout", "call(0)"));
    InitClusterManagersInList(restartIdxs);
    auto startTime = std::chrono::steady_clock::now();
    for (size_t i : restartIdxs) {
        DS_ASSERT_OK(clusterManagers_[i]->CheckWaitNodeTableComplete());
    }
    auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                       std::chrono::steady_clock::now() - startTime)
                       .count();
    ASSERT_LT(elapsed, 15) << "WaitNodeTable should terminate early, but took " << elapsed << "s";
    ShutDownAllClusterManagers();
}

TEST_F(ClusterManagerTest, NoProgressEarlyTerminationOnlyForRestart)
{
    int workerNum = 1;
    InitTestEtcdInstance();
    InitAllClusterManagers(workerNum);
    DS_ASSERT_OK(inject::Set("ClusterManager.CheckWaitNodeTableComplete.returnError", "call()"));
    for (auto &cm : clusterManagers_) {
        DS_ASSERT_OK(cm->CheckWaitNodeTableComplete());
    }
    DS_ASSERT_OK(inject::Set("ClusterManager.CheckWaitNodeTableComplete.hashWorkerNum", "call(2)"));
    DS_ASSERT_OK(inject::Set("ClusterManager.CheckWaitNodeTableComplete.waitTime", "call(2)"));
    DS_ASSERT_OK(inject::Set("ClusterManager.CheckWaitNodeTableComplete.noProgressTimeout", "call(0)"));
    ASSERT_EQ(inject::GetExecuteCount("ClusterManager.CheckWaitNodeTableComplete.noProgressBreak"), 0u);
    DS_ASSERT_OK(inject::Set("ClusterManager.CheckWaitNodeTableComplete.noProgressBreak", "call()"));

    DS_ASSERT_OK(clusterManagers_[0]->CheckWaitNodeTableComplete());
    ASSERT_EQ(inject::GetExecuteCount("ClusterManager.CheckWaitNodeTableComplete.noProgressBreak"), 0u);
    ShutDownAllClusterManagers();
}
}  // namespace st
}  // namespace datasystem
