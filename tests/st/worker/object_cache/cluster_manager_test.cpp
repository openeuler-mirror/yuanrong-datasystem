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
#include <condition_variable>
#include <mutex>
#include <string>

#include "common.h"
#include "coordinator_cluster_store_mock.h"
#include "datasystem/worker/cluster_manager/cluster_constants.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/kvstore/etcd/etcd_store.h"
#include "datasystem/worker/cluster_manager/cluster_manager.h"
#include "datasystem/topology/coordination_backend/coordination_backend.h"
#include "datasystem/topology/coordination_backend/etcd_coordination_backend.h"
#include "datasystem/worker/hash_ring/hash_ring.h"

using namespace datasystem::worker;

DS_DECLARE_string(etcd_address);
DS_DECLARE_string(master_address);
DS_DECLARE_uint32(add_node_wait_time_s);
DS_DECLARE_int32(heartbeat_interval_ms);
DS_DECLARE_uint32(node_dead_timeout_s);
DS_DECLARE_uint32(node_timeout_s);
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
            db_->DropTable(HASHRING_TABLE);
            // We don't check rc here. If table to drop does not exist, it's fine.
            (void)db_->CreateTable(HASHRING_TABLE, HASHRING_TABLE);
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
            clusterStores_.emplace_back(std::make_unique<topology::EtcdCoordinationBackend>(etcdStores_.back().get()));
            clusterManagers_.emplace_back(
                std::make_unique<ClusterManager>(addr, addr, clusterStores_.back().get(), nullptr, pqSize));
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
                clusterStores_[i] = std::make_unique<topology::EtcdCoordinationBackend>(etcdStores_[i].get());
                clusterManagers_[i] = std::make_unique<ClusterManager>(addr, addr, clusterStores_[i].get(), nullptr);
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
                clusterStores_[i] = std::make_unique<topology::EtcdCoordinationBackend>(etcdStores_[i].get());
                clusterManagers_[i] = std::make_unique<ClusterManager>(addr, addr, clusterStores_[i].get(), nullptr);
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
    std::vector<std::unique_ptr<topology::EtcdCoordinationBackend>> clusterStores_;
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
    std::vector<size_t> restartIdxs = { 0 };
    inject::Set("ClusterManager.IfNeedTriggerReconciliation.noreconciliation", "return(K_OK)");
    DS_ASSERT_OK(inject::Set("ClusterManager.CheckWaitNodeTableComplete.waitTime", "call(120)"));
    DS_ASSERT_OK(inject::Set("ClusterManager.CheckWaitNodeTableComplete.noProgressTimeout", "call(0)"));
    InitClusterManagersInList(restartIdxs);
    auto startTime = std::chrono::steady_clock::now();
    for (size_t i : restartIdxs) {
        DS_ASSERT_OK(clusterManagers_[i]->CheckWaitNodeTableComplete());
    }
    auto elapsed =
        std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now() - startTime).count();
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
class CoordinatorClusterManagerTest : public ExternalClusterTest {
protected:
    CoordinatorClusterManagerTest() = default;

    ~CoordinatorClusterManagerTest() = default;

    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numEtcd = 0;
        opts.numMasters = 0;
        opts.numWorkers = 0;
        FLAGS_v = 1;
        FLAGS_add_node_wait_time_s = 0;
    }

    void SetUp() override
    {
        ExternalClusterTest::SetUp();
        FLAGS_etcd_address = "coordinator_mock";
        DS_ASSERT_OK(inject::Set("test.start.notWait", "call(0)"));
        coordinatorProxy_ = std::make_unique<CoordinatorServiceProxyMock>();
    }

    void TearDown() override
    {
        ShutDownAllClusterManagers();
        coordinatorProxy_.reset();
        (void)inject::Clear("test.start.notWait");
        ExternalClusterTest::TearDown();
    }

    void RegisterCoordinatorWatcher(topology::CoordinationBackend *store)
    {
        coordinatorProxy_->RegisterWatcherHandler(
            store->GetWatcherAddr(),
            [store](topology::CoordinationEvent &&event) { store->HandleWatchEvent(std::move(event)); });
    }

    void InitAllClusterManagers(uint32_t workerNum, const int pqSize = 2000)
    {
        workerIds_.resize(workerNum);
        clusterStores_.resize(workerNum);
        clusterManagers_.resize(workerNum);
        for (uint32_t i = 0; i < workerNum; i++) {
            HostPort addr;
            addr.ParseString("127.0.0.1:" + std::to_string(i));
            workerIds_[i] = addr.ToString();
            clusterStores_[i] =
                std::make_unique<topology::CoordinationBackend>(coordinatorProxy_.get(), addr.ToString());
            RegisterCoordinatorWatcher(clusterStores_[i].get());
            clusterManagers_[i] =
                std::make_unique<ClusterManager>(addr, addr, clusterStores_[i].get(), nullptr, pqSize);
            ClusterInfo clusterInfo;
            DS_ASSERT_OK(ConstructClusterInfoViaCoordinator(clusterStores_[i].get(), clusterInfo));
            DS_ASSERT_OK(clusterManagers_[i]->Init(clusterInfo));
            clusterManagers_[i]->SetWorkerReady();
        }
    }

    void StopClusterManager(size_t index)
    {
        ASSERT_LT(index, clusterManagers_.size());
        clusterManagers_[index].reset();
        if (coordinatorProxy_ != nullptr && clusterStores_[index] != nullptr) {
            coordinatorProxy_->UnregisterWatcherHandler(clusterStores_[index]->GetWatcherAddr());
        }
        clusterStores_[index].reset();
    }

    void ShutDownAllClusterManagers()
    {
        for (size_t i = 0; i < clusterManagers_.size(); ++i) {
            if (clusterManagers_[i] != nullptr || clusterStores_[i] != nullptr) {
                StopClusterManager(i);
            }
        }
    }

    void InitClusterManagersInList(const std::vector<size_t> &idxs)
    {
        for (size_t i : idxs) {
            if (i < clusterManagers_.size()) {
                HostPort addr;
                addr.ParseString("127.0.0.1:" + std::to_string(i));
                clusterStores_[i] =
                    std::make_unique<topology::CoordinationBackend>(coordinatorProxy_.get(), addr.ToString());
                RegisterCoordinatorWatcher(clusterStores_[i].get());
                clusterManagers_[i] = std::make_unique<ClusterManager>(addr, addr, clusterStores_[i].get(), nullptr);
                ClusterInfo clusterInfo;
                DS_ASSERT_OK(ConstructClusterInfoViaCoordinator(clusterStores_[i].get(), clusterInfo));
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
                if (clusterStores_[i] != nullptr) {
                    coordinatorProxy_->UnregisterWatcherHandler(clusterStores_[i]->GetWatcherAddr());
                }
                clusterStores_[i].reset();
                HostPort addr;
                addr.ParseString("127.0.0.1:" + std::to_string(i));
                clusterStores_[i] =
                    std::make_unique<topology::CoordinationBackend>(coordinatorProxy_.get(), addr.ToString());
                RegisterCoordinatorWatcher(clusterStores_[i].get());
                clusterManagers_[i] = std::make_unique<ClusterManager>(addr, addr, clusterStores_[i].get(), nullptr);
                ClusterInfo clusterInfo;
                DS_ASSERT_OK(ConstructClusterInfoViaCoordinator(clusterStores_[i].get(), clusterInfo));
                DS_ASSERT_OK(clusterManagers_[i]->Init(clusterInfo));
                clusterManagers_[i]->SetWorkerReady();
            }
        }
    }

    void PutActiveRingToCoordinator(uint32_t workerNum)
    {
        HashRingPb ringPb;
        ringPb.set_cluster_has_init(true);
        for (uint32_t i = 0; i < workerNum; ++i) {
            auto *workerPb = &(*ringPb.mutable_workers())[workerIds_[i]];
            workerPb->set_worker_uuid(clusterManagers_[i]->GetHashRing()->GetLocalWorkerUuid());
            workerPb->set_state(WorkerPb::ACTIVE);
            for (uint32_t tokenIndex = 0; tokenIndex < 4; ++tokenIndex) {
                workerPb->mutable_hash_tokens()->Add((i * 4 + tokenIndex + 1) * 1000);
            }
        }
        topology::CoordinationBackend store(coordinatorProxy_.get(), "127.0.0.1:0");
        const std::string ringData = ringPb.SerializeAsString();
        DS_ASSERT_OK(
            store.CAS(HASHRING_TABLE, "", [&ringData](const std::string &, std::unique_ptr<std::string> &out, bool &) {
                out = std::make_unique<std::string>(ringData);
                return Status::OK();
            }));
    }

    void WaitRingsRunning(uint32_t workerNum)
    {
        Timer timer;
        while (timer.ElapsedMilliSecond() < 5'000) {
            bool allRunning = true;
            for (uint32_t i = 0; i < workerNum; ++i) {
                allRunning = allRunning && clusterManagers_[i]->GetHashRing()->IsRunning();
            }
            if (allRunning) {
                return;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        FAIL() << "Coordinator rings did not become running in time";
    }

    std::unique_ptr<CoordinatorServiceProxyMock> coordinatorProxy_;
    std::vector<std::string> workerIds_;
    std::vector<std::unique_ptr<topology::CoordinationBackend>> clusterStores_;
    std::vector<std::unique_ptr<ClusterManager>> clusterManagers_;
};

TEST_F(CoordinatorClusterManagerTest, WatcherAddressUsesProvidedHostPort)
{
    const std::string watcherAddr = "127.0.0.1:10000";
    topology::CoordinationBackend store(coordinatorProxy_.get(), watcherAddr);

    ASSERT_EQ(store.GetWatcherAddr(), watcherAddr);
}

TEST_F(CoordinatorClusterManagerTest, InitKeepAliveWritesNodeStateWithTimestamp)
{
    topology::CoordinationBackend store(coordinatorProxy_.get(), "127.0.0.1:0");
    const std::string workerAddr = "127.0.0.1:100";

    DS_ASSERT_OK(store.InitKeepAlive(CLUSTER_TABLE, workerAddr, false, true));

    std::string valueStr;
    DS_ASSERT_OK(store.Get(CLUSTER_TABLE, workerAddr, valueStr));
    KeepAliveValue value;
    DS_ASSERT_OK(KeepAliveValue::FromString(valueStr, value));
    ASSERT_EQ(value.state, "start");
    ASSERT_FALSE(value.timestamp.empty());
    ASSERT_NE(value.timestamp, "_");
}

TEST_F(CoordinatorClusterManagerTest, KeepAliveFailureEmitsLocalFakeDeleteWhenCoordinatorReachable)
{
    DS_ASSERT_OK(inject::Set("CoordinationBackend.KeepAlive.intervalMs", "call(10)"));
    DS_ASSERT_OK(inject::Set("CoordinationBackend.KeepAlive.confirmTimes", "call(1)"));
    DS_ASSERT_OK(inject::Set("CoordinationBackend.KeepAlive.returnError", "return(K_RPC_UNAVAILABLE)"));

    topology::CoordinationBackend store(coordinatorProxy_.get(), "127.0.0.1:0");
    std::mutex mutex;
    std::condition_variable cv;
    bool receivedDelete = false;
    std::string receivedKey;
    store.SetEventHandler([&](topology::CoordinationEvent &&event) {
        if (event.type == topology::CoordinationEventType::DELETE) {
            std::lock_guard<std::mutex> lock(mutex);
            receivedDelete = true;
            receivedKey = event.key;
            cv.notify_all();
        }
    });
    store.SetCheckStoreStateWhenNetworkFailedHandler([]() { return true; });

    const std::string workerAddr = "127.0.0.1:101";
    DS_ASSERT_OK(store.InitKeepAlive(CLUSTER_TABLE, workerAddr, false, true));

    std::unique_lock<std::mutex> lock(mutex);
    ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(2), [&]() { return receivedDelete; }));
    ASSERT_EQ(receivedKey, "/" + std::string(CLUSTER_TABLE) + "/" + workerAddr);
}

TEST_F(CoordinatorClusterManagerTest, StoppedClusterManagerMarksNodeDisconnected)
{
    FLAGS_node_timeout_s = 1;
    FLAGS_node_dead_timeout_s = 3;
    FLAGS_heartbeat_interval_ms = 100;
    const uint32_t workerNum = 3;
    InitAllClusterManagers(workerNum);
    PutActiveRingToCoordinator(workerNum);
    WaitRingsRunning(workerNum);
    DS_ASSERT_OK(inject::Set("ClusterManager.IfNeedTriggerReconciliation.noreconciliation", "return(K_OK)"));
    DS_ASSERT_OK(inject::Set("ClusterManager.CheckWaitNodeTableComplete.returnError", "call()"));
    for (auto &cm : clusterManagers_) {
        DS_ASSERT_OK(cm->CheckWaitNodeTableComplete());
    }

    HostPort stoppedAddr;
    DS_ASSERT_OK(stoppedAddr.ParseString(workerIds_[1]));
    StopClusterManager(1);

    Timer timer;
    Status status;
    while (timer.ElapsedMilliSecond() < 5'000) {
        status = clusterManagers_[0]->CheckConnection(stoppedAddr);
        if (status.GetCode() == K_MASTER_TIMEOUT) {
            return;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    ASSERT_EQ(status.GetCode(), K_MASTER_TIMEOUT) << status.ToString();
}

TEST_F(CoordinatorClusterManagerTest, RestartAllClusterManagers1)
{
    int workerNum = 10;
    InitAllClusterManagers(workerNum);
    DS_ASSERT_OK(inject::Set("ClusterManager.CheckWaitNodeTableComplete.returnError", "call()"));
    for (auto &cm : clusterManagers_) {
        DS_ASSERT_OK(cm->CheckWaitNodeTableComplete());
    }
    ShutDownAllClusterManagers();
    std::vector<size_t> idxs(workerNum);
    std::iota(idxs.begin(), idxs.end(), 0);
    inject::Set("ClusterManager.IfNeedTriggerReconciliation.noreconciliation", "return(K_OK)");
    DS_ASSERT_OK(inject::Set("ClusterManager.CheckWaitNodeTableComplete.returnError", "call()"));
    InitClusterManagersInList(idxs);
    for (auto &cm : clusterManagers_) {
        DS_ASSERT_OK(cm->CheckWaitNodeTableComplete());
    }
}

TEST_F(CoordinatorClusterManagerTest, RestartAllClusterManagers2)
{
    int workerNum = 10;
    InitAllClusterManagers(workerNum);
    DS_ASSERT_OK(inject::Set("ClusterManager.CheckWaitNodeTableComplete.returnError", "call()"));
    for (auto &cm : clusterManagers_) {
        DS_ASSERT_OK(cm->CheckWaitNodeTableComplete());
    }
    std::vector<size_t> idxs(workerNum);
    std::iota(idxs.begin(), idxs.end(), 0);
    inject::Set("ClusterManager.IfNeedTriggerReconciliation.noreconciliation", "return(K_OK)");
    ShutDownRestartAllClusterManagers(idxs);
    DS_ASSERT_OK(inject::Set("ClusterManager.CheckWaitNodeTableComplete.returnError", "call()"));
    for (auto &cm : clusterManagers_) {
        DS_ASSERT_OK(cm->CheckWaitNodeTableComplete());
    }
}

TEST_F(CoordinatorClusterManagerTest, NoProgressEarlyTermination)
{
    int workerNum = 3;
    InitAllClusterManagers(workerNum);
    DS_ASSERT_OK(inject::Set("ClusterManager.CheckWaitNodeTableComplete.returnError", "call()"));
    for (auto &cm : clusterManagers_) {
        DS_ASSERT_OK(cm->CheckWaitNodeTableComplete());
    }
    ShutDownAllClusterManagers();
    std::vector<size_t> restartIdxs = { 0 };
    inject::Set("ClusterManager.IfNeedTriggerReconciliation.noreconciliation", "return(K_OK)");
    DS_ASSERT_OK(inject::Set("ClusterManager.CheckWaitNodeTableComplete.waitTime", "call(120)"));
    DS_ASSERT_OK(inject::Set("ClusterManager.CheckWaitNodeTableComplete.noProgressTimeout", "call(0)"));
    InitClusterManagersInList(restartIdxs);
    auto startTime = std::chrono::steady_clock::now();
    for (size_t i : restartIdxs) {
        DS_ASSERT_OK(clusterManagers_[i]->CheckWaitNodeTableComplete());
    }
    auto elapsed =
        std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now() - startTime).count();
    ASSERT_LT(elapsed, 15) << "WaitNodeTable should terminate early, but took " << elapsed << "s";
}

TEST_F(CoordinatorClusterManagerTest, NoProgressEarlyTerminationOnlyForRestart)
{
    int workerNum = 1;
    InitAllClusterManagers(workerNum);
    PutActiveRingToCoordinator(workerNum);
    WaitRingsRunning(workerNum);
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
}
}  // namespace st
}  // namespace datasystem
