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
 * Description: hetero client scale tests.
 */
#include <unistd.h>
#include <chrono>
#include <csignal>
#include <semaphore.h>
#include <string>
#include <thread>
#include <unordered_map>

#include <google/protobuf/util/json_util.h>
#include <gtest/gtest.h>

#include "client/object_cache/oc_client_common.h"
#include "cluster/base_cluster.h"
#include "common.h"
#include "datasystem/common/rpc/rpc_constants.h"
#include "datasystem/common/device/ascend/acl_device_manager.h"
#include "datasystem/common/device/device_helper.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/gflag/flags.h"
#include "datasystem/common/util/hash_algorithm.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/file_util.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/common/util/uuid_generator.h"
#include "datasystem/master/object_cache/store/object_meta_store.h"
#include "datasystem/object_client.h"
#include "datasystem/object/object_enum.h"
#include "datasystem/utils/optional.h"
#include "datasystem/utils/status.h"
#include "device/dev_test_helper.h"

DS_DECLARE_string(etcd_address);
DS_DECLARE_string(log_dir);

namespace datasystem {
namespace st {

const std::string HOST_IP_PREFIX = "127.0.0.1";
class DevOCScaleTest : public DevTestHelper {
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
            (void)db_->CreateTable(std::string(ETCD_GLOBAL_CACHE_TABLE_PREFIX) + ETCD_HASH_SUFFIX,
                                   std::string(ETCD_GLOBAL_CACHE_TABLE_PREFIX) + ETCD_HASH_SUFFIX);
            (void)db_->CreateTable(std::string(ETCD_GLOBAL_CACHE_TABLE_PREFIX) + ETCD_WORKER_SUFFIX,
                                   std::string(ETCD_GLOBAL_CACHE_TABLE_PREFIX) + ETCD_WORKER_SUFFIX);
        }
    }

    void StartWorkerAndWaitReady(std::initializer_list<uint32_t> indexes, int maxWaitTimeSec = 20)
    {
        for (auto i : indexes) {
            ASSERT_TRUE(externalCluster_->StartWorker(i, HostPort()).IsOk()) << i;
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

    void SaveHashToken()
    {
        std::string value;
        db_->Get(ETCD_RING_PREFIX, "", value);
        HashRingPb ring;
        ring.ParseFromString(value);
        for (const auto &kv : ring.workers()) {
            const auto &workerId = kv.first;
            for (auto token : kv.second.hash_tokens()) {
                tokenMap_.insert({ token, workerId });
            }
        }
    }

    void GetHashOnWorker(size_t workerNum)
    {
        std::string value;
        db_->Get(ETCD_RING_PREFIX, "", value);
        HashRingPb ring;
        ring.ParseFromString(value);
        for (size_t i = 0; i < workerNum; ++i) {
            auto tokens = ring.workers().at(workerAddress_[i]).hash_tokens();
            workerHashValue_.emplace_back(*tokens.begin() - 1);
        }
        ASSERT_EQ(workerHashValue_.size(), workerNum);
    }

    bool CheckScaleDownFinished(size_t workerNum)
    {
        std::string value;
        db_->Get(ETCD_RING_PREFIX, "", value);
        HashRingPb ring;
        ring.ParseFromString(value);
        return ring.workers().size() == workerNum;
    }

    bool CheckWorkerFinished(size_t workerNum)
    {
        std::string value;
        db_->Get(ETCD_RING_PREFIX, "", value);
        HashRingPb ring;
        ring.ParseFromString(value);
        if (ring.workers().size() != workerNum) {  // worker num is 3;
            return false;
        }
        for (const auto &worker : ring.workers()) {
            if (worker.second.state() != WorkerPb::ACTIVE) {
                return false;
            }
        }
        return true;
    }

    bool WaitForScaleDownFinished(int timeoutS, int workerNum)
    {
        Timer timer;
        while (timer.ElapsedSecond() < timeoutS) {
            if (CheckScaleDownFinished(workerNum)) {
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(500));  // check for scale down interval 500ms
        }
        LOG(INFO) << "scale down finish time: " << timer.ElapsedMilliSecond();
        return false;
    }

    void ScaleDownInject(int workerIdx, int signal = SIGTERM)
    {
        std::string checkFilePath = FLAGS_log_dir.c_str();
        std::string client = "client";
        checkFilePath = checkFilePath.substr(0, checkFilePath.length() - client.length()) + "/worker"
                        + std::to_string(workerIdx) + "/log/worker-status";
        std::ofstream ofs(checkFilePath);
        if (!ofs.is_open()) {
            LOG(ERROR) << "Can not open worker status file in " << checkFilePath
                       << ", voluntary scale in will not start, errno: " << errno;
        } else {
            ofs << "voluntary scale in\n";
        }
        ofs.close();
        kill(cluster_->GetWorkerPid(workerIdx), signal);
    }

protected:
    ExternalCluster *externalCluster_ = nullptr;
    std::unique_ptr<EtcdStore> db_;
    const static uint64_t shutdownTimeoutMs = 60 * 1000;  // 1min
    std::vector<uint32_t> workerHashValue_;
    std::map<uint32_t, std::string> tokenMap_;
    std::unordered_map<HostPort, std::string> uuidMap_;
    std::vector<std::string> workerAddress_;
};

class DevOCScaleDownTest : public DevOCScaleTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numEtcd = 1;
        opts.numWorkers = 3;  // worker num is 3
        opts.numOBS = 1;
        opts.enableDistributedMaster = "true";
        opts.addNodeTime = 0;
        opts.workerGflagParams =
            FormatString(" -v=0 -node_timeout_s=%d -node_dead_timeout_s=%d -auto_del_dead_node=true", nodeTimeout_,
                         nodeDeadTimeout_);
        opts.waitWorkerReady = false;
        for (size_t i = 0; i < opts.numWorkers; i++) {
            opts.workerConfigs.emplace_back(HOST_IP_PREFIX + std::to_string(i), GetFreePort());
            workerAddress_.emplace_back(opts.workerConfigs.back().ToString());
        }
    }

    void SetWorker(int metaMasterIdx)
    {
        StartWorkerAndWaitReady({ 0, 1, 2 });
        SaveHashToken();
        GetHashOnWorker(workerAddress_.size());
        for (size_t i = 0; i < workerAddress_.size(); i++) {
            DS_ASSERT_OK(cluster_->SetInjectAction(
                ClusterNodeType::WORKER, i, "MurmurHash3SpecifyKey",
                FormatString("return(%s,%lu)", P2P_DEFAULT_MASTER, workerHashValue_[metaMasterIdx] - 1)));
        }
    }

protected:
    const int nodeTimeout_ = 3;
    const int nodeDeadTimeout_ = 8;
};

TEST_F(DevOCScaleDownTest, DISABLED_TestWorkerScaleDown2ClearClientMetaDFX)
{
    size_t blkSz = 100, numOfObjs = 1, blksPerObj = 1;
    std::vector<std::string> objectIds, failedIdList;
    std::vector<DeviceBlobList> devGetBlobList, devSetBlobList;
    std::shared_ptr<HeteroClient> client;
    for (auto j = 0ul; j < numOfObjs; j++) {
        objectIds.emplace_back(GetStringUuid());
    }
    auto initDev = [this, blksPerObj, numOfObjs, &blkSz, &devGetBlobList, &devSetBlobList](int id) {
        this->deviceIdx_ = id;
        InitAcl(deviceIdx_);
        PrePareDevData(numOfObjs, blksPerObj, blkSz, devGetBlobList, devSetBlobList, deviceIdx_);
    };

    // Synchronization primitives for inter-process coordination
    sem_t *signal1Sem = sem_open("/test_signal1", O_CREAT, 0644, 0);
    sem_t *signal2Sem = sem_open("/test_signal2", O_CREAT, 0644, 0);
    ASSERT_NE(signal1Sem, SEM_FAILED);
    ASSERT_NE(signal2Sem, SEM_FAILED);

    auto metaMasterIdx = 0;
    SetWorker(metaMasterIdx);

    auto childProcess1 = ForkForTest([&]() {
        int deviceId = 1;
        initDev(deviceId);
        InitTestHeteroClient(0, client);
        DS_ASSERT_OK(client->DevMSet(objectIds, devSetBlobList, failedIdList));
        DS_ASSERT_TRUE(failedIdList.empty(), true);

        // Wait for signal 2 from process 3 before exit
        sem_wait(signal2Sem);
        client.reset();
        LOG(INFO) << "Process 1: Received signal 2, proceeding with cleanup";
    });
    auto childProcess2 = ForkForTest([&]() {
        int deviceId = 2;
        initDev(deviceId);
        InitTestHeteroClient(1, client);
        DS_ASSERT_OK(client->DevMGet(objectIds, devGetBlobList, failedIdList, RPC_TIMEOUT));
        ASSERT_EQ(failedIdList.size(), 0);
        std::string value(devSetBlobList[0].blobs[0].size, 'b');
        for (size_t j = 0; j < numOfObjs; j++) {
            for (size_t k = 0; k < blksPerObj; k++) {
                CheckDevPtrContent(devGetBlobList[j].blobs[k].pointer, devGetBlobList[j].blobs[k].size, value);
            }
        }
        ScaleDownInject(1, SIGKILL);
        int stillAliveWorkers = 2;
        WaitForScaleDownFinished(20, stillAliveWorkers);
        std::this_thread::sleep_for(std::chrono::milliseconds(7000));

        // Send signal 1 to notify process 3 that scale down is complete
        sem_post(signal1Sem);
        LOG(INFO) << "Process 2: Sent signal 1 - scale down complete";

        // Wait for signal 2 from process 3 before exit
        sem_wait(signal2Sem);
        client.reset();
        LOG(INFO) << "Process 2: Received signal 2, proceeding with cleanup";
    });
    auto childProcess3 = ForkForTest([&]() {
        int deviceId = 3;
        initDev(deviceId);
        InitTestHeteroClient(2, client);
        LOG(INFO) << "Process 3: wait signal1CV";
        // Wait for signal 1 before proceeding with DevMGet
        sem_wait(signal1Sem);

        DS_ASSERT_OK(client->DevMGet(objectIds, devGetBlobList, failedIdList, 10 * 1000));
        ASSERT_EQ(failedIdList.size(), 0);
        std::string value(devSetBlobList[0].blobs[0].size, 'b');
        for (size_t j = 0; j < numOfObjs; j++) {
            for (size_t k = 0; k < blksPerObj; k++) {
                CheckDevPtrContent(devGetBlobList[j].blobs[k].pointer, devGetBlobList[j].blobs[k].size, value);
            }
        }

        // Send signal 2 to notify process 1 and process 2 they can exit
        sem_post(signal2Sem);
        sem_post(signal2Sem);
        LOG(INFO) << "Process 3: Sent signal 2 - operation complete";
    });
    DS_ASSERT_TRUE(WaitForChildFork(childProcess1), 0);
    DS_ASSERT_TRUE(WaitForChildFork(childProcess2), 0);
    DS_ASSERT_TRUE(WaitForChildFork(childProcess3), 0);
    sem_close(signal1Sem);
    sem_close(signal2Sem);
    sem_unlink("/test_signal1");
    sem_unlink("/test_signal2");
}
}  // namespace st
}  // namespace datasystem