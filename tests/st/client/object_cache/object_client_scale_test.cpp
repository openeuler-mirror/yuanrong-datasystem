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
 * Description: State client scale tests.
 */

#include <unistd.h>
#include <chrono>
#include <csignal>
#include <cstddef>
#include <memory>
#include <string>
#include <thread>

#include <google/protobuf/util/json_util.h>
#include <gtest/gtest.h>

#include "client/object_cache/oc_client_common.h"
#include "cluster/base_cluster.h"
#include "common.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/kvstore/etcd/etcd_constants.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/gflag/flags.h"
#include "datasystem/common/util/hash_algorithm.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/file_util.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/master/object_cache/store/object_meta_store.h"
#include "datasystem/object_client.h"
#include "datasystem/object/object_enum.h"
#include "datasystem/protos/master_object.pb.h"
#include "datasystem/kv_client.h"
#include "datasystem/utils/status.h"
#include "datasystem/worker/hash_ring/hash_ring.h"
#include "datasystem/worker/hash_ring/hash_ring_allocator.h"
#include "datasystem/worker/object_cache/worker_master_oc_api.h"
#include "datasystem/common/log/log.h"

DS_DECLARE_string(etcd_address);
DS_DECLARE_string(master_address);
DS_DECLARE_string(az_name);
DS_DECLARE_string(log_dir);

namespace datasystem {
namespace st {

constexpr int SCALE_DOWN_ADD_TIME = 0;
const std::string HOST_IP_PREFIX = "127.0.0.1";
class OCScaleTest : public OCClientCommon {
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

    void SetWorkerHashInjection(std::vector<uint32_t> injectNode = std::vector<uint32_t>{})
    {
        if (injectNode.size() == 0) {
            for (size_t i = 0; i < workerAddress_.size(); ++i) {
                DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, i, "MurmurHash3", "return()"));
            }
            return;
        }

        for (auto i : injectNode) {
            DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, i, "MurmurHash3", "return()"));
        }
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
            (void)db_->CreateTable(ETCD_CLUSTER_TABLE, "/" + std::string(ETCD_CLUSTER_TABLE));
            (void)db_->CreateTable(std::string(ETCD_GLOBAL_CACHE_TABLE_PREFIX) + ETCD_HASH_SUFFIX,
                                   std::string(ETCD_GLOBAL_CACHE_TABLE_PREFIX) + ETCD_HASH_SUFFIX);
            (void)db_->CreateTable(std::string(ETCD_GLOBAL_CACHE_TABLE_PREFIX) + ETCD_WORKER_SUFFIX,
                                   std::string(ETCD_GLOBAL_CACHE_TABLE_PREFIX) + ETCD_WORKER_SUFFIX);
        }
    }

    void GetWorkerUuidMap(std::map<std::string, std::string> &workerUuids)
    {
        if (!db_) {
            InitTestEtcdInstance();
        }

        std::string value;
        DS_ASSERT_OK(db_->Get(ETCD_RING_PREFIX, "", value));
        HashRingPb ring;
        ASSERT_TRUE(ring.ParseFromString(value));
        workerUuids.clear();
        for (auto &kv : ring.workers()) {
            workerUuids.emplace(kv.first, kv.second.worker_uuid());
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

    void GetHashOnWorker(size_t workerNum = 4)
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

    bool WaitForScaleDownFinished(int timeoutS, int workerNum)
    {
        Timer timer;
        while (timer.ElapsedSecond() < timeoutS) {
            if (CheckScaleDownFinished(workerNum)) {
                std::this_thread::sleep_for(
                    std::chrono::milliseconds(1000));  // wait 1000ms for etcd watch event finished.
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(500));  // check for scale down interval 500ms
        }
        LOG(INFO) << "scale down finish time: " << timer.ElapsedMilliSecond();
        // The nodes in the cluster will update the tokenMap_ only after receiving the completion of the scale down
        // task; wait 1s to ensure that the tokenMap_ update is completed; otherwise, the hash type key may still be
        // hashed to the scale down node.
        sleep(1);
        return false;
    }

    bool CheckScaleUpFinished(size_t workerNum)
    {
        std::string value;
        db_->Get(ETCD_RING_PREFIX, "", value);
        HashRingPb ring;
        if (!ring.ParseFromString(value)) {
            return false;
        };
        if (ring.workers().size() != workerNum) {
            return false;
        }
        for (const auto &info : ring.workers()) {
            if (info.second.state() != WorkerPb::ACTIVE) {
                return false;
            }
        }
        return true;
    }

    bool CheckScalleDownFinished(const std::string &workerAddr)
    {
        std::string value;
        auto status = db_->Get(ETCD_CLUSTER_TABLE, workerAddr, value);
        if (status.GetCode() == K_NOT_FOUND) {
            return true;
        }
        return false;
    }

    bool WaitForScaleUpFinished(int timeoutS, int workerNum)
    {
        Timer timer;
        while (timer.ElapsedSecond() < timeoutS) {
            if (CheckScaleUpFinished(workerNum)) {
                LOG(INFO) << "scale up finish time: " << timer.ElapsedMilliSecond();
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(500));  // check for scale down interval 500ms
        }
        return false;
    }

    bool WaitForVoluntaryDownFinished(int timeoutS, int workerNum, const std::string &workerAddr)
    {
        Timer timer;
        if (WaitForScaleUpFinished(timeoutS, workerNum)) {
            while (timer.ElapsedSecond() < timeoutS) {
                if (CheckScalleDownFinished(workerAddr)) {
                    LOG(INFO) << "scale down finish time: " << timer.ElapsedSecond() << " worker: " << workerAddr;
                    return true;
                }
                auto interval = 100;
                std::this_thread::sleep_for(std::chrono::milliseconds(interval));
            }
        }
        return false;
    }

    void VoluntaryScaleDownInject(int workerIdx)
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
        kill(cluster_->GetWorkerPid(workerIdx), SIGTERM);
    }

protected:
    ExternalCluster *externalCluster_ = nullptr;
    std::vector<std::string> workerAddress_;
    std::unique_ptr<EtcdStore> db_;
    const static uint64_t shutdownTimeoutMs = 60000;  // 1min
    std::vector<uint32_t> workerHashValue_;
};

class OCScaleDownTest : public OCScaleTest {
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numEtcd = 1;
        opts.numWorkers = 3;  // worker num is 3
        opts.numOBS = 0;
        opts.enableDistributedMaster = "true";
        opts.addNodeTime = SCALE_DOWN_ADD_TIME;
        opts.workerGflagParams =
            FormatString(" -v=2 -node_timeout_s=%d -node_dead_timeout_s=%d -auto_del_dead_node=true", nodeTimeout_,
                         nodeDeadTimeout_);
        opts.waitWorkerReady = false;
        for (size_t i = 0; i < opts.numWorkers; i++) {
            opts.workerConfigs.emplace_back(HOST_IP_PREFIX + std::to_string(i), GetFreePort());
            workerAddress_.emplace_back(opts.workerConfigs.back().ToString());
        }
    }

protected:
    const int nodeTimeout_ = 3;
    int nodeDeadTimeout_ = 8;
};

TEST_F(OCScaleDownTest, TestRefsScaleDownWithoutL2)
{
    StartWorkerAndWaitReady({ 0, 1, 2 });
    std::shared_ptr<ObjectClient> client, client1, client2;
    InitTestClient(0, client);
    InitTestClient(1, client1);
    InitTestClient(2, client2);  // client index is 2
    std::string objectPrefix = "objecttest_";
    std::vector<std::string> objectKeys;
    CreateParam param{ .writeMode = WriteMode::NONE_L2_CACHE };
    uint64_t timeout = 2000;
    int objNum = 30;
    std::string value = "data";
    std::vector<std::string> failObjects;
    for (int i = 0; i < objNum; i++) {
        auto objKey = objectPrefix + std::to_string(i);
        objectKeys.emplace_back(objKey);
        DS_ASSERT_OK(client->Put(objKey, reinterpret_cast<const uint8_t *>(value.data()), value.size(), param));
    }
    DS_ASSERT_OK(client->GIncreaseRef(objectKeys, failObjects));
    ASSERT_EQ(failObjects.size(), 0ul);
    DS_ASSERT_OK(client1->GIncreaseRef(objectKeys, failObjects));
    ASSERT_EQ(failObjects.size(), 0ul);
    std::vector<Optional<Buffer>> buffer1, buffer2;
    for (int i = 0; i < objNum; i++) {
        DS_ASSERT_OK(client->Get({ objectKeys[i] }, timeout, buffer1));
        DS_ASSERT_OK(client1->Get({ objectKeys[i] }, timeout, buffer2));
    }
    DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, 0));
    ASSERT_TRUE(WaitForScaleDownFinished(60, 2));  // wait for scale down timeout 60s, expect worker num 2.
    for (int i = 0; i < objNum; i++) {
        auto client1Ref = client1->QueryGlobalRefNum(objectKeys[i]);
        auto client2Ref = client2->QueryGlobalRefNum(objectKeys[i]);
        ASSERT_EQ(client1Ref, client2Ref);
        ASSERT_EQ(client1Ref, 1);
        failObjects.clear();
        DS_ASSERT_OK(client1->GDecreaseRef({ objectKeys[i] }, failObjects));
        ASSERT_EQ(failObjects.size(), 0ul);
        client1Ref = client1->QueryGlobalRefNum(objectKeys[i]);
        ASSERT_EQ(client1Ref, 0);
        buffer2.clear();
        DS_ASSERT_NOT_OK(client1->Get({ objectKeys[i] }, 0, buffer2));
    }
}

class OCScaleUpTest : public OCScaleTest {
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numEtcd = 1;
        opts.numWorkers = 3;  // worker num is 3
        opts.numOBS = 1;
        opts.enableDistributedMaster = "true";
        opts.addNodeTime = SCALE_DOWN_ADD_TIME;
        opts.workerGflagParams =
            FormatString(" -v=2 -node_timeout_s=%d -node_dead_timeout_s=%d -auto_del_dead_node=true", nodeTimeout_,
                         nodeDeadTimeout_);
        opts.waitWorkerReady = false;
        for (size_t i = 0; i < opts.numWorkers; i++) {
            opts.workerConfigs.emplace_back(HOST_IP_PREFIX + std::to_string(i), GetFreePort());
            workerAddress_.emplace_back(opts.workerConfigs.back().ToString());
        }
    }

protected:
    const int nodeTimeout_ = 3;
    const int nodeDeadTimeout_ = 8;
};

TEST_F(OCScaleUpTest, TestSubscribeScaleUp)
{
    StartWorkerAndWaitReady({ 0, 1 });
    InitTestEtcdInstance();
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "ScaleUpTask.NotRunVoluntaryDownTask", "abort()"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "ScaleUpTask.NotRunVoluntaryDownTask", "abort()"));
    std::shared_ptr<ObjectClient> client, client1;
    InitTestClient(0, client);
    InitTestClient(1, client1);
    std::string objectPrefix = "objecttest_";
    std::vector<std::string> objectKeys;
    std::vector<Optional<Buffer>> buffers;
    for (int i = 0; i < 40; i++) {  // obj num is 40
        objectKeys.emplace_back(objectPrefix + std::to_string(i));
    }
    std::thread thread1([&client, &buffers, &objectKeys]() {
        DS_ASSERT_OK(client->Get(objectKeys, 60000, buffers));  // sub timeout is 60000ms
    });

    StartWorkerAndWaitReady({ 2 });
    WaitForScaleUpFinished(60, 3);  // worker index is 3, timeout is 60
    CreateParam param{ .writeMode = WriteMode::NONE_L2_CACHE };
    std::string value = "data";
    std::vector<std::string> failObjects;
    for (const auto &id : objectKeys) {
        DS_ASSERT_OK(client->Put(id, reinterpret_cast<const uint8_t *>(value.data()), value.size(), param));
    }
    thread1.join();
    for (auto &buffer : buffers) {
        AssertBufferEqual(*buffer, value);
    }
}

TEST_F(OCScaleUpTest, TestNestedObjectScaleUp)
{
    FLAGS_v = 2;  // vlog is 2.
    StartWorkerAndWaitReady({ 0, 1 });
    InitTestEtcdInstance();
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "ScaleUpTask.NotRunVoluntaryDownTask", "abort()"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "ScaleUpTask.NotRunVoluntaryDownTask", "abort()"));
    std::shared_ptr<ObjectClient> client, client1;
    InitTestClient(0, client);
    InitTestClient(1, client1);
    CreateParam param{ .writeMode = WriteMode::NONE_L2_CACHE };
    std::string value = "data";
    std::string objectPrefix = "objecttest_";
    std::string nestedObjectPrefix = "nestobjecttest_";
    std::vector<std::string> objectKeys, nestedKeys;
    for (int i = 0; i < 40; i++) {  // obj num is 40
        auto objKey = objectPrefix + std::to_string(i);
        auto nestId = nestedObjectPrefix + std::to_string(i);
        objectKeys.emplace_back(objKey);
        nestedKeys.emplace_back(nestId);
        DS_ASSERT_OK(
            client->Put(objKey, reinterpret_cast<const uint8_t *>(value.data()), value.size(), param, { nestId }));
        DS_ASSERT_OK(client->Put(nestId, reinterpret_cast<const uint8_t *>(value.data()), value.size(), param));
    }
    std::vector<std::string> failedIds;
    DS_ASSERT_OK(client->GIncreaseRef(objectKeys, failedIds));
    ASSERT_TRUE(failedIds.empty());
    DS_ASSERT_OK(client->GIncreaseRef(nestedKeys, failedIds));
    ASSERT_TRUE(failedIds.empty());
    StartWorkerAndWaitReady({ 2 });
    WaitForScaleUpFinished(60, 3);  // worker index is 3, timeout is 60
    std::vector<Optional<Buffer>> buffers;
    for (int i = 0; i < 40; i++) {  // obj num is 40
        LOG(INFO) << objectKeys[i];
        failedIds.clear();
        DS_ASSERT_OK(client->GDecreaseRef({ nestedKeys[i] }, failedIds));
        ASSERT_TRUE(failedIds.empty());
        DS_ASSERT_OK(client->Get({ nestedKeys[i] }, 0, buffers));
        DS_ASSERT_OK(client->GDecreaseRef({ objectKeys[i] }, failedIds));
        ASSERT_TRUE(failedIds.empty());
        DS_ASSERT_NOT_OK(client->Get({ objectKeys[i] }, 0, buffers));
    }
    buffers.clear();
    sleep(1);
    DS_ASSERT_NOT_OK(client->Get(nestedKeys, 0, buffers));
}

TEST_F(OCScaleUpTest, TestNestedObjectScaleUpRedirect)
{
    FLAGS_v = 2;  // vlog is 2
    StartWorkerAndWaitReady({ 0, 1 });
    InitTestEtcdInstance();
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "ScaleUpTask.NotRunVoluntaryDownTask", "abort()"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "ScaleUpTask.NotRunVoluntaryDownTask", "abort()"));
    std::shared_ptr<ObjectClient> client, client1;
    InitTestClient(0, client);
    InitTestClient(1, client1);
    CreateParam param{ .writeMode = WriteMode::NONE_L2_CACHE };
    std::string value = "data";
    std::string objectPrefix = "objecttest_";
    std::string nestedObjectPrefix = "nestobjecttest_";
    std::vector<std::string> objectKeys, nestedKeys;
    for (int i = 0; i < 40; i++) {  // obj num is 40
        auto objKey = objectPrefix + std::to_string(i);
        auto nestId = nestedObjectPrefix + std::to_string(i);
        objectKeys.emplace_back(objKey);
        nestedKeys.emplace_back(nestId);
        DS_ASSERT_OK(
            client->Put(objKey, reinterpret_cast<const uint8_t *>(value.data()), value.size(), param, { nestId }));
        DS_ASSERT_OK(client->Put(nestId, reinterpret_cast<const uint8_t *>(value.data()), value.size(), param));
    }
    std::vector<std::string> failedIds;
    DS_ASSERT_OK(client->GIncreaseRef(objectKeys, failedIds));
    ASSERT_TRUE(failedIds.empty());
    DS_ASSERT_OK(client->GIncreaseRef(nestedKeys, failedIds));
    ASSERT_TRUE(failedIds.empty());
    StartWorkerAndWaitReady({ 2 });
    WaitForScaleUpFinished(60, 3);  // worker index is 3, timeout is 60
    std::vector<Optional<Buffer>> buffers;
    for (int i = 0; i < 40; i++) {  // obj num is 40
        LOG(INFO) << objectKeys[i];
        failedIds.clear();
        DS_ASSERT_OK(client->GDecreaseRef({ nestedKeys[i] }, failedIds));
        ASSERT_TRUE(failedIds.empty());
        DS_ASSERT_OK(client->Get({ nestedKeys[i] }, 0, buffers));
        DS_ASSERT_OK(client->GDecreaseRef({ objectKeys[i] }, failedIds));
        ASSERT_TRUE(failedIds.empty());
        DS_ASSERT_NOT_OK(client->Get({ objectKeys[i] }, 0, buffers));
    }
    buffers.clear();
    DS_ASSERT_NOT_OK(client->Get(nestedKeys, 0, buffers));
}

TEST_F(OCScaleUpTest, LEVEL2_TestNestedScaleUpRedirect)
{
    std::shared_ptr<ObjectClient> client0, client1, client2;
    std::vector<std::string> failObjects;
    StartWorkerAndWaitReady({ 0, 1 });
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "ScaleUpTask.NotRunVoluntaryDownTask", "abort()"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "ScaleUpTask.NotRunVoluntaryDownTask", "abort()"));
    InitTestClient(0, client0);
    InitTestClient(1, client1);
    uint64_t size = 1024;
    std::vector<uint8_t> data(size, 0);
    int objNum = 400;
    std::vector<Optional<Buffer>> buffers;
    ThreadPool threadPool(1);
    StartWorkerAndWaitReady({ 2 });
    std::vector<std::future<void>> futureVec;
    futureVec.push_back(threadPool.Submit([&client0, &failObjects, data, size, objNum, &buffers] {
        CreateParam param = CreateParam{};
        std::vector<Optional<Buffer>> dataList;
        for (auto i = 0; i < objNum; i++) {
            auto key = "remoteObj_" + std::to_string(i);
            auto nestedKey = "nestedobject_" + std::to_string(i);
            DS_ASSERT_OK(client0->GIncreaseRef({ key }, failObjects));
            DS_ASSERT_OK(client0->Put(key, data.data(), size, param, { nestedKey }));
            DS_ASSERT_OK(client0->Put(nestedKey, data.data(), size, param));
            DS_ASSERT_OK(client0->GDecreaseRef({ key }, failObjects));
            DS_ASSERT_NOT_OK(client0->Get({ key }, 0, buffers));
            buffers.clear();
        }
    }));
    // Migrate and Redirect
    for (auto &future : futureVec) {
        future.get();
    }
    sleep(1);
    for (auto i = 0; i < objNum; i++) {
        auto nestedKey = "nestedobject_" + std::to_string(i);
        buffers.clear();
        DS_ASSERT_NOT_OK(client0->Get({ nestedKey }, 0, buffers));
    }
}

TEST_F(OCScaleUpTest, TestQueryRefScaleUpRedirect)
{
    std::shared_ptr<ObjectClient> client0, client1, client2;
    std::vector<std::string> failObjects;
    StartWorkerAndWaitReady({ 0, 1 });
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "hashring.finishaddnodeinfo", "sleep(5000)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "hashring.finishaddnodeinfo", "sleep(5000)"));
    InitTestClient(0, client0);
    InitTestClient(1, client1);
    std::vector<Optional<Buffer>> buffers;
    ThreadPool threadPool(1);
    StartWorkerAndWaitReady({ 2 });
    std::vector<std::future<void>> futureVec;
    Timer timer;
    while (timer.ElapsedSecond() < 5) { // wait 5 s for scale up
        auto key = GetStringUuid();
        DS_ASSERT_OK(client0->GIncreaseRef({ key }, failObjects));
        ASSERT_EQ(client0->QueryGlobalRefNum(key), 1);
        DS_ASSERT_OK(client0->GDecreaseRef({ key }, failObjects));
        buffers.clear();
    }
}

TEST_F(OCScaleUpTest, LEVEL1_TestGincreaseScaleUpRedirect)
{
    std::shared_ptr<ObjectClient> client0, client1, client2;
    std::vector<std::string> failObjects;
    StartWorkerAndWaitReady({ 0, 1 });
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "ScaleUpTask.NotRunVoluntaryDownTask", "abort()"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "ScaleUpTask.NotRunVoluntaryDownTask", "abort()"));
    InitTestClient(0, client0);
    InitTestClient(1, client1);
    uint64_t size = 1024;
    std::vector<uint8_t> data(size, 0);
    int objNum = 200;
    std::vector<Optional<Buffer>> buffers;
    ThreadPool threadPool(1);
    StartWorkerAndWaitReady({ 2 });
    std::vector<std::future<void>> futureVec;
    futureVec.push_back(threadPool.Submit([&client0, &client1, &failObjects, data, size, objNum, &buffers] {
        CreateParam param = CreateParam{};
        std::vector<Optional<Buffer>> dataList;
        for (auto i = 0; i < objNum; i++) {
            auto key = "remoteObj_" + std::to_string(i);
            DS_ASSERT_OK(client0->GIncreaseRef({ key }, failObjects));
            DS_ASSERT_OK(client1->GIncreaseRef({ key }, failObjects));
            DS_ASSERT_OK(client0->Put(key, data.data(), size, param));
            DS_ASSERT_OK(client0->GDecreaseRef({ key }, failObjects));
            DS_ASSERT_OK(client1->GDecreaseRef({ key }, failObjects));
            DS_ASSERT_NOT_OK(client0->Get({ key }, 0, buffers));
        }
    }));
    // Migrate and Redirect
    sleep(1);
    for (auto &future : futureVec) {
        future.get();
    }
}

TEST_F(OCScaleUpTest, TestGincreaseIdsScaleUpRedirect)
{
    std::shared_ptr<ObjectClient> client0, client1, client2;
    std::vector<std::string> failObjects;
    StartWorkerAndWaitReady({ 0, 1 });
    InitTestClient(0, client0);
    InitTestClient(1, client1);
    uint64_t size = 1024;
    std::vector<uint8_t> data(size, 0);
    int objBatch = 40;
    int numPreBatch = 10;
    std::vector<Optional<Buffer>> buffers;
    ThreadPool threadPool(1);
    StartWorkerAndWaitReady({ 2 });
    std::vector<std::future<void>> futureVec;
    futureVec.push_back(threadPool.Submit([&client0, &failObjects, data, size, objBatch, numPreBatch, &buffers] {
        CreateParam param = CreateParam{};
        std::vector<Optional<Buffer>> dataList;
        for (auto i = 0; i < objBatch; i++) {
            std::vector<std::string> objKeys;
            for (auto j = 0; j < numPreBatch; j++) {
                auto key = "remoteObj_" + std::to_string(i);
                objKeys.emplace_back(key);
                DS_ASSERT_OK(client0->Put(key, data.data(), size, param));
            }
            DS_ASSERT_OK(client0->GIncreaseRef(objKeys, failObjects));
            ASSERT_TRUE(failObjects.empty());
            failObjects.clear();
            DS_ASSERT_OK(client0->GDecreaseRef(objKeys, failObjects));
            ASSERT_TRUE(failObjects.empty());
            failObjects.clear();
            DS_ASSERT_NOT_OK(client0->Get(objKeys, 0, buffers));
        }
    }));
    // Migrate and Redirect
    sleep(1);
    for (auto &future : futureVec) {
        future.get();
    }
}

class OCVoluntaryScaleDownTest : public OCScaleTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numEtcd = 1;
        opts.numWorkers = 3;  // worker num is 3
        opts.numOBS = 1;
        opts.enableSpill = true;
        opts.enableDistributedMaster = "true";
        opts.addNodeTime = SCALE_DOWN_ADD_TIME;
        uint64_t spillSize = 64 * 1024ul * 1024ul;
        opts.workerGflagParams = FormatString(
            " -v=2 -node_timeout_s=%d -node_dead_timeout_s=%d -auto_del_dead_node=true -spill_size_limit=%ld ",
            nodeTimeout_, nodeDeadTimeout_, spillSize);
        opts.waitWorkerReady = false;
        for (size_t i = 0; i < opts.numWorkers; i++) {
            opts.workerConfigs.emplace_back(HOST_IP_PREFIX + std::to_string(i), GetFreePort());
            workerAddress_.emplace_back(opts.workerConfigs.back().ToString());
        }

        const int worker0ConfigIndex = 0;
        const int worker1ConfigIndex = 1;
        const int worker2ConfigIndex = 2;

        worker0Address_ = opts.workerConfigs[worker0ConfigIndex];
        worker1Address_ = opts.workerConfigs[worker1ConfigIndex];
        worker2Address_ = opts.workerConfigs[worker2ConfigIndex];
    }

    void SetObjOnWorker(const int &workerIdx, std::shared_ptr<ObjectClient> client, const std::string &data,
                        WriteMode mode, std::vector<std::string> &objectKey)
    {
        for (uint32_t i = 0; i < objectKey.size(); ++i) {
            objectKey[i] = "a_key_hash_to_" + std::to_string(workerHashValue_[workerIdx] - i);
            CreateParam param{ .writeMode = mode };
            DS_ASSERT_OK(
                client->Put(objectKey[i], reinterpret_cast<const uint8_t *>(data.data()), data.size(), param, {}));
        }
    }

    Status InitInstanceBase()
    {
        hostPort_.ParseString("127.0.0.1:" + std::to_string(GetFreePort()));
        akSkManager_ = std::make_shared<AkSkManager>();
        akSkManager_->SetClientAkSk(accessKey_, secretKey_);
        RETURN_IF_NOT_OK(cluster_->GetWorkerAddr(0, worker0Address_));  // worker index is 0
        RETURN_IF_NOT_OK(cluster_->GetWorkerAddr(1, worker1Address_));  // worker index is 1
        int stubCacheNum = 100;
        RpcStubCacheMgr::Instance().Init(stubCacheNum);
        worker1MasterApi_ = std::make_unique<worker::WorkerRemoteMasterOCApi>(worker1Address_, hostPort_, akSkManager_);
        RETURN_IF_NOT_OK(worker1MasterApi_->Init());
        RETURN_IF_NOT_OK(cluster_->GetWorkerAddr(2, worker2Address_));  // worker index is 2
        worker2MasterApi_ = std::make_unique<worker::WorkerRemoteMasterOCApi>(worker2Address_, hostPort_, akSkManager_);
        RETURN_IF_NOT_OK(worker2MasterApi_->Init());
        return Status::OK();
    }

    Status QueryMeta(std::vector<std::string> &objKeys, std::vector<master::QueryMetaRspPb> &metaInfos)
    {
        master::QueryMetaReqPb queryReq;
        master::QueryMetaRspPb queryRsp;
        reqTimeoutDuration.Reset();
        *queryReq.mutable_ids() = { objKeys.begin(), objKeys.end() };
        queryReq.set_address(hostPort_.ToString());
        std::vector<RpcMessage> payloads;
        if (worker1MasterApi_ != nullptr) {
            LOG(INFO) << "send quemeta to worker1, lolcal address is :" << hostPort_.ToString();
            RETURN_IF_NOT_OK(worker1MasterApi_->QueryMeta(queryReq, 0, queryRsp, payloads));
            metaInfos.emplace_back(queryRsp);
        }
        master::QueryMetaReqPb queryReq1;
        master::QueryMetaRspPb queryRsp1;
        *queryReq1.mutable_ids() = { objKeys.begin(), objKeys.end() };
        queryReq1.set_address(hostPort_.ToString());
        payloads.clear();
        RETURN_OK_IF_TRUE(worker2MasterApi_ == nullptr);
        LOG(INFO) << "send quemeta to worker2, lolcal address is :" << hostPort_.ToString();
        RETURN_IF_NOT_OK(worker2MasterApi_->QueryMeta(queryReq1, 0, queryRsp1, payloads));
        metaInfos.emplace_back(queryRsp1);
        LOG(INFO) << "send quemeta to worker2, lolcal address is :" << hostPort_.ToString();
        return Status::OK();
    }

protected:
    const int nodeTimeout_ = 3;
    const int nodeDeadTimeout_ = 5;
    HostPort hostPort_;
    std::shared_ptr<AkSkManager> akSkManager_;
    std::unique_ptr<worker::WorkerRemoteMasterOCApi> worker1MasterApi_;
    std::unique_ptr<worker::WorkerRemoteMasterOCApi> worker2MasterApi_;
    HostPort worker1Address_;
    HostPort worker2Address_;
    HostPort worker0Address_;
    std::string accessKey_ = "QTWAOYTTINDUT2QVKYUC";
    std::string secretKey_ = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
};

TEST_F(OCVoluntaryScaleDownTest, VoluntaryWorkerScaleDownFinalStageLeaving)
{
    FLAGS_v = 2;  // vlog is 2
    const int kWorkerNode0 = 0;
    const int kFirstBatchObjects = 100;
    const int kSecondBatchObjects = 200;
    const int kWaitTimeSeconds = 20;
    const int kWorkerNum = 1;
    const int kGetObjectCount = 200;

    StartWorkerAndWaitReady({ 0, 1 });
    InitTestEtcdInstance();
    std::shared_ptr<ObjectClient> client, client1;
    InitTestClient(0, client);
    InitTestClient(1, client1);

    CreateParam param{ .writeMode = WriteMode::NONE_L2_CACHE };
    std::string objectPrefix = "objecttest_";
    std::vector<std::string> objectKeys;

    for (int i = 0; i < kFirstBatchObjects; i++) {
        std::string value(i + 1, 'x');
        auto objKey = objectPrefix + std::to_string(i);
        objectKeys.emplace_back(objKey);
        DS_ASSERT_OK(client->Put(objKey, reinterpret_cast<const uint8_t *>(value.data()), value.size(), param));
    }

    for (int i = kFirstBatchObjects; i < kSecondBatchObjects; i++) {
        std::string value(i + 1, 'x');
        auto objKey = objectPrefix + std::to_string(i);
        objectKeys.emplace_back(objKey);
        DS_ASSERT_OK(client1->Put(objKey, reinterpret_cast<const uint8_t *>(value.data()), value.size(), param));
    }

    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "MigrateStrategy.CheckCondition1", "1000*call(3)"));

    client.reset();
    VoluntaryScaleDownInject(kWorkerNode0);
    WaitForVoluntaryDownFinished(kWaitTimeSeconds, kWorkerNum, worker0Address_.ToString());
    std::vector<Optional<Buffer>> buffers;
    for (int i = 0; i < kGetObjectCount; i++) {
        std::string value(i + 1, 'x');
        LOG(INFO) << objectKeys[i];
        DS_ASSERT_OK(client1->Get({ objectKeys[i] }, 0, buffers));
        ASSERT_NE(buffers[0]->ImmutableData(), nullptr);
        AssertBufferEqual(*buffers[0], value);
    }
}

TEST_F(OCVoluntaryScaleDownTest, LEVEL2_VoluntaryWorkerScaleDownLeaving)
{
    FLAGS_v = 2;  // vlog is 2
    const int kWorkerNode0 = 0;
    const int kWorkerNode2 = 2;
    const int kFirstBatchObjects = 20;
    const int kSecondBatchObjects = 40;
    const int kWaitTimeSeconds = 20;
    const int kWorkerNum = 1;
    const int kGetObjectCount = 40;
    const int sleepDuration = 5;
    StartWorkerAndWaitReady({ 0, 1, 2 });
    InitTestEtcdInstance();
    std::shared_ptr<ObjectClient> client, client1, client2;
    InitTestClient(0, client);
    InitTestClient(1, client1);
    InitTestClient(2, client2);

    CreateParam param{ .writeMode = WriteMode::NONE_L2_CACHE };
    std::string value = "data";
    std::string objectPrefix = "objecttest_";
    std::vector<std::string> objectKeys;

    for (int i = 0; i < kFirstBatchObjects; i++) {
        auto objKey = objectPrefix + std::to_string(i);
        objectKeys.emplace_back(objKey);
        DS_ASSERT_OK(client->Put(objKey, reinterpret_cast<const uint8_t *>(value.data()), value.size(), param));
    }

    for (int i = kFirstBatchObjects; i < kSecondBatchObjects; i++) {
        auto objKey = objectPrefix + std::to_string(i);
        objectKeys.emplace_back(objKey);
        DS_ASSERT_OK(client2->Put(objKey, reinterpret_cast<const uint8_t *>(value.data()), value.size(), param));
    }

    client.reset();
    VoluntaryScaleDownInject(kWorkerNode0);
    client2.reset();
    VoluntaryScaleDownInject(kWorkerNode2);
    WaitForVoluntaryDownFinished(kWaitTimeSeconds, kWorkerNum, worker0Address_.ToString());
    WaitForVoluntaryDownFinished(kWaitTimeSeconds, kWorkerNum, worker2Address_.ToString());

    sleep(sleepDuration);
    std::vector<Optional<Buffer>> buffers;
    for (int i = 0; i < kGetObjectCount; i++) {  // obj num is 40
        LOG(INFO) << objectKeys[i];
        DS_ASSERT_OK(client1->Get({ objectKeys[i] }, 0, buffers));
        ASSERT_NE(buffers[0]->ImmutableData(), nullptr);
        AssertBufferEqual(*buffers[0], value);
    }
}

TEST_F(OCVoluntaryScaleDownTest, VoluntaryWorkerScaleDownAvailableSpaceRatio40)
{
    FLAGS_v = 2;  // vlog is 2
    const int kWorkerNode0 = 0;
    const int kFirstBatchObjects = 10;
    const int kSecondBatchObjects = 40;
    const int kWaitTimeSeconds = 20;
    const int kWorkerNum = 2;
    const int kObjectSize = 1024 * 1024;
    const int sleepDuration = 5;
    StartWorkerAndWaitReady({ 0, 1, 2 });
    InitTestEtcdInstance();
    std::shared_ptr<ObjectClient> client, client1, client2;
    InitTestClient(0, client);
    InitTestClient(1, client1);
    InitTestClient(2, client2);

    CreateParam param{ .writeMode = WriteMode::NONE_L2_CACHE };
    std::string value(kObjectSize, 'x');
    std::string objectPrefix = "objecttest_";
    std::vector<std::string> objectKeys;
    for (int i = 0; i < kFirstBatchObjects; i++) {
        auto objKey = objectPrefix + std::to_string(i);
        objectKeys.emplace_back(objKey);
        DS_ASSERT_OK(client->Put(objKey, reinterpret_cast<const uint8_t *>(value.data()), value.size(), param));
    }

    for (int i = kFirstBatchObjects; i < kSecondBatchObjects; i++) {
        auto objKey = objectPrefix + std::to_string(i);
        objectKeys.emplace_back(objKey);
        DS_ASSERT_OK(client2->Put(objKey, reinterpret_cast<const uint8_t *>(value.data()), value.size(), param));
    }

    client.reset();
    VoluntaryScaleDownInject(kWorkerNode0);

    WaitForVoluntaryDownFinished(kWaitTimeSeconds, kWorkerNum, worker0Address_.ToString());

    sleep(sleepDuration);

    std::vector<Optional<Buffer>> buffers;
    for (int i = 0; i < kFirstBatchObjects; i++) {  // obj num is 40
        LOG(INFO) << objectKeys[i];
        DS_ASSERT_OK(client1->Get({ objectKeys[i] }, 0, buffers));
        ASSERT_NE(buffers[0]->ImmutableData(), nullptr);
        AssertBufferEqual(*buffers[0], value);
    }
}

TEST_F(OCVoluntaryScaleDownTest, LEVEL2_VoluntaryWorkerScaleDown)
{
    FLAGS_v = 2;  // vlog is 2
    StartWorkerAndWaitReady({ 0, 1, 2 });
    InitTestEtcdInstance();
    std::shared_ptr<ObjectClient> client, client1, client2;
    InitTestClient(0, client);
    InitTestClient(1, client1);
    InitTestClient(2, client2);  // worker index is 2
    CreateParam param{ .writeMode = WriteMode::NONE_L2_CACHE };
    std::string value = "data";
    std::string objectPrefix = "objecttest_";
    std::string nestedObjectPrefix = "nestobjecttest_";
    std::vector<std::string> objectKeys, nestedKeys;
    for (int i = 0; i < 40; i++) {  // obj num is 40
        auto objKey = objectPrefix + std::to_string(i);
        objectKeys.emplace_back(objKey);
        DS_ASSERT_OK(client->Put(objKey, reinterpret_cast<const uint8_t *>(value.data()), value.size(), param));
    }
    std::vector<std::string> failedIds;
    DS_ASSERT_OK(client->GIncreaseRef(objectKeys, failedIds));
    ASSERT_TRUE(failedIds.empty());
    DS_ASSERT_OK(client2->GIncreaseRef(objectKeys, failedIds));
    ASSERT_TRUE(failedIds.empty());
    client2.reset();
    VoluntaryScaleDownInject(2);  // worker index is 2
    sleep(5);                     // wait 5s for VoluntaryScaleDown finish
    std::vector<Optional<Buffer>> buffers;
    for (int i = 0; i < 40; i++) {  // obj num is 40
        LOG(INFO) << objectKeys[i];
        failedIds.clear();
        DS_ASSERT_OK(client1->Get({ objectKeys[i] }, 0, buffers));
        ASSERT_NE(buffers[0]->ImmutableData(), nullptr);
        AssertBufferEqual(*buffers[0], value);
        DS_ASSERT_OK(client->GDecreaseRef({ objectKeys[i] }, failedIds));
        ASSERT_TRUE(failedIds.empty());
        DS_ASSERT_NOT_OK(client->Get({ objectKeys[i] }, 0, buffers));
    }
}

TEST_F(OCVoluntaryScaleDownTest, LEVEL2_VoluntaryWorkerScaleDown1)
{
    FLAGS_v = 2;  // vlog is 2
    StartWorkerAndWaitReady({ 0, 1, 2 });
    InitTestEtcdInstance();
    std::shared_ptr<ObjectClient> client, client1, client2;
    InitTestClient(0, client);
    InitTestClient(1, client1);
    InitTestClient(2, client2);  // worker index is 2
    CreateParam param{ .writeMode = WriteMode::NONE_L2_CACHE };
    std::string value = "data";
    std::string objectPrefix = "objecttest_";
    std::string nestedObjectPrefix = "nestobjecttest_";
    std::vector<std::string> objectKeys, nestedKeys;
    std::vector<std::string> failedIds;
    client2.reset();
    VoluntaryScaleDownInject(2);  // worker index is 2
    std::vector<Optional<Buffer>> buffers;
    for (int i = 0; i < 400; i++) {  // obj num is 400
        auto objKey = objectPrefix + std::to_string(i);
        objectKeys.emplace_back(objKey);
        DS_ASSERT_OK(client->Put(objKey, reinterpret_cast<const uint8_t *>(value.data()), value.size(), param));
        LOG(INFO) << objectKeys[i];
        failedIds.clear();
        DS_ASSERT_OK(client->GIncreaseRef({ objectKeys[i] }, failedIds));
        ASSERT_TRUE(failedIds.empty());
        DS_ASSERT_OK(client1->GIncreaseRef({ objectKeys[i] }, failedIds));
        ASSERT_TRUE(failedIds.empty());
        DS_ASSERT_OK(client1->Get({ objectKeys[i] }, 0, buffers));
        ASSERT_NE(buffers[0]->ImmutableData(), nullptr);
        AssertBufferEqual(*buffers[0], value);
        DS_ASSERT_OK(client1->GDecreaseRef({ objectKeys[i] }, failedIds));
        ASSERT_TRUE(failedIds.empty());
        DS_ASSERT_OK(client->GDecreaseRef({ objectKeys[i] }, failedIds));
        ASSERT_TRUE(failedIds.empty());
        DS_ASSERT_NOT_OK(client->Get({ objectKeys[i] }, 0, buffers));
    }
}

TEST_F(OCVoluntaryScaleDownTest, VoluntaryDownWorker1NoneL2EvictNoCopy)
{
    StartWorkerAndWaitReady({ 0, 1, 2 });
    InitTestEtcdInstance();
    SetWorkerHashInjection();
    GetHashOnWorker(3);  // worker num is 3
    std::shared_ptr<ObjectClient> client, client1, client2;
    InitTestClient(0, client);
    InitTestClient(1, client1);
    InitTestClient(2, client2);          // worker index is 2
    std::vector<std::string> objs(400);  // obj num is 400
    std::string value = "data";
    SetObjOnWorker(0, client, value, WriteMode::NONE_L2_CACHE_EVICT, objs);
    client.reset();
    VoluntaryScaleDownInject(0);  // worker index is 0
    std::vector<Optional<Buffer>> buffers;
    WaitForVoluntaryDownFinished(20, 2, worker0Address_.ToString());  // timeout is 20, left num is 2
    DS_ASSERT_NOT_OK(client2->Get(objs, 0, buffers));
}

TEST_F(OCVoluntaryScaleDownTest, VoluntaryDownWorker1NoneL2EvictWithCopy)
{
    StartWorkerAndWaitReady({ 0, 1, 2 });
    InitTestEtcdInstance();
    SetWorkerHashInjection();
    GetHashOnWorker(3);  // worker num is 3
    DS_ASSERT_OK(InitInstanceBase());
    std::shared_ptr<ObjectClient> client, client1, client2;
    InitTestClient(0, client);
    InitTestClient(1, client1);
    InitTestClient(2, client2);          // worker index is 2
    std::vector<std::string> objs(400);  // obj num is 400
    std::string value = "data";
    SetObjOnWorker(0, client, value, WriteMode::NONE_L2_CACHE_EVICT, objs);
    std::vector<Optional<Buffer>> buffers;
    DS_ASSERT_OK(client2->Get(objs, 0, buffers));
    client.reset();
    VoluntaryScaleDownInject(0);                                      // worker index is 0
    WaitForVoluntaryDownFinished(20, 2, worker0Address_.ToString());  // timeout is 20, left num is 2
    std::vector<master::QueryMetaRspPb> metaInfos;
    int metaNum = 0;
    DS_ASSERT_OK(QueryMeta(objs, metaInfos));
    for (const auto &metaInfo : metaInfos) {
        for (const auto &meta : metaInfo.query_metas()) {
            ASSERT_EQ(meta.meta().primary_address(), worker2Address_.ToString()) << meta.DebugString();
        }
        metaNum += metaInfo.query_metas_size();
    }
    ASSERT_EQ(metaNum, 400);  // obj is 400
}

TEST_F(OCVoluntaryScaleDownTest, VoluntaryDownWorker1NoneL2CacheWithCopy)
{
    StartWorkerAndWaitReady({ 0, 1, 2 });
    InitTestEtcdInstance();
    SetWorkerHashInjection();
    GetHashOnWorker(3);  // worker num is 3
    DS_ASSERT_OK(InitInstanceBase());
    std::shared_ptr<ObjectClient> client, client1, client2;
    InitTestClient(0, client);
    InitTestClient(1, client1);
    InitTestClient(2, client2);          // worker index is 2
    std::vector<std::string> objs(400);  // obj num is 400
    std::string value = "data";
    SetObjOnWorker(0, client, value, WriteMode::NONE_L2_CACHE, objs);
    std::vector<Optional<Buffer>> buffers;
    DS_ASSERT_OK(client2->Get(objs, 0, buffers));
    client.reset();
    VoluntaryScaleDownInject(0);                                      // worker index is 0
    WaitForVoluntaryDownFinished(20, 2, worker0Address_.ToString());  // timeout is 20, left num is 2
    std::vector<master::QueryMetaRspPb> metaInfos;
    int metaNum = 0;
    DS_ASSERT_OK(QueryMeta(objs, metaInfos));
    for (const auto &metaInfo : metaInfos) {
        for (const auto &meta : metaInfo.query_metas()) {
            ASSERT_EQ(meta.meta().primary_address(), worker2Address_.ToString()) << meta.DebugString();
        }
        metaNum += metaInfo.query_metas_size();
    }
    ASSERT_EQ(metaNum, 400);  // obj is 400
}

TEST_F(OCVoluntaryScaleDownTest, VoluntaryDownWorker1WriteBackWithCopy)
{
    DS_ASSERT_OK(cluster_->StartOBS());
    StartWorkerAndWaitReady({ 0, 1, 2 });
    InitTestEtcdInstance();
    SetWorkerHashInjection();
    GetHashOnWorker(3);  // worker num is 3
    DS_ASSERT_OK(InitInstanceBase());
    std::shared_ptr<ObjectClient> client, client1, client2;
    InitTestClient(0, client);
    InitTestClient(1, client1);
    InitTestClient(2, client2);          // worker index is 2
    std::vector<std::string> objs(400);  // obj num is 400
    std::string value = "data";
    SetObjOnWorker(0, client, value, WriteMode::WRITE_BACK_L2_CACHE, objs);
    std::vector<Optional<Buffer>> buffers;
    DS_ASSERT_OK(client2->Get(objs, 0, buffers));
    client.reset();
    VoluntaryScaleDownInject(0);                                      // worker index is 0
    WaitForVoluntaryDownFinished(20, 2, worker0Address_.ToString());  // timeout is 20, left num is 2
    int metaNum = 0;
    std::vector<master::QueryMetaRspPb> metaInfos;
    DS_ASSERT_OK(QueryMeta(objs, metaInfos));
    for (const auto &metaInfo : metaInfos) {
        for (const auto &meta : metaInfo.query_metas()) {
            ASSERT_EQ(meta.meta().primary_address(), worker2Address_.ToString());
        }
        metaNum += metaInfo.query_metas_size();
    }
    ASSERT_EQ(metaNum, 400);  // obj is 400
}

TEST_F(OCVoluntaryScaleDownTest, VoluntaryDownWorker1Worker2Failed)
{
    DS_ASSERT_OK(cluster_->StartOBS());
    StartWorkerAndWaitReady({ 0, 1, 2 });
    InitTestEtcdInstance();
    SetWorkerHashInjection();
    GetHashOnWorker(3);  // worker num is 3
    DS_ASSERT_OK(InitInstanceBase());
    std::shared_ptr<ObjectClient> client, client1, client2;
    InitTestClient(0, client);
    InitTestClient(1, client1);
    InitTestClient(2, client2);          // worker index is 2
    std::vector<std::string> objs(20);  // obj num is 20
    std::string value = "data";
    SetObjOnWorker(0, client, value, WriteMode::WRITE_THROUGH_L2_CACHE, objs);
    std::vector<Optional<Buffer>> buffers;
    DS_ASSERT_OK(client1->Get(objs, 0, buffers));
    client.reset();
    VoluntaryScaleDownInject(0);                      // worker index is 0
    DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, 2));  // worker index is 2
    worker2MasterApi_ = nullptr;
    WaitForVoluntaryDownFinished(20, 2, worker0Address_.ToString());  // timeout is 20, left num is 2
    sleep(6);                                                         // wait for scale down finish 6s
    int metaNum = 0;
    std::vector<master::QueryMetaRspPb> metaInfos;
    DS_ASSERT_OK(QueryMeta(objs, metaInfos));
    for (const auto &metaInfo : metaInfos) {
        for (const auto &meta : metaInfo.query_metas()) {
            ASSERT_EQ(meta.meta().primary_address(), worker1Address_.ToString());
        }
        metaNum += metaInfo.query_metas_size();
    }
    ASSERT_EQ(metaNum, 20);  // obj is 20
}

TEST_F(OCVoluntaryScaleDownTest, DISABLED_VoluntaryDownTwoWorkers)
{
    StartWorkerAndWaitReady({ 0, 1, 2 });
    InitTestEtcdInstance();
    SetWorkerHashInjection();
    GetHashOnWorker(3);  // worker num is 3
    DS_ASSERT_OK(InitInstanceBase());
    std::shared_ptr<ObjectClient> client, client1, client2;
    InitTestClient(0, client);
    InitTestClient(1, client1);
    InitTestClient(2, client2);          // worker index is 2
    std::vector<std::string> objs(400);  // obj num is 400
    std::string value = "data";
    SetObjOnWorker(0, client, value, WriteMode::NONE_L2_CACHE, objs);
    std::vector<Optional<Buffer>> buffers;
    DS_ASSERT_OK(client2->Get(objs, 0, buffers));
    SetObjOnWorker(0, client, value, WriteMode::NONE_L2_CACHE, objs);
    client.reset();
    client1.reset();
    client2.reset();
    VoluntaryScaleDownInject(0);  // worker index is 0
    VoluntaryScaleDownInject(1);
    worker1MasterApi_ = nullptr;
    WaitForVoluntaryDownFinished(10, 1, worker1Address_.ToString());  // timeout is 10, left num is 1
    int metaNum = 0;
    std::vector<master::QueryMetaRspPb> metaInfos;
    DS_ASSERT_OK(QueryMeta(objs, metaInfos));
    for (const auto &metaInfo : metaInfos) {
        for (const auto &meta : metaInfo.query_metas()) {
            ASSERT_EQ(meta.meta().primary_address(), worker2Address_.ToString());
        }
        metaNum += metaInfo.query_metas_size();
    }
    ASSERT_EQ(metaNum, 400);  // obj is 400
    LOG(INFO) << "==================";
}

TEST_F(OCVoluntaryScaleDownTest, LEVEL2_VoluntaryDownWorkerTwoWrokers)
{
    DS_ASSERT_OK(cluster_->StartOBS());
    StartWorkerAndWaitReady({ 0, 1, 2 });
    InitTestEtcdInstance();
    SetWorkerHashInjection();
    GetHashOnWorker(3);  // worker num is 3
    DS_ASSERT_OK(InitInstanceBase());
    std::shared_ptr<ObjectClient> client, client1, client2;
    InitTestClient(0, client);
    InitTestClient(1, client1);
    InitTestClient(2, client2);           // worker index is 2
    std::vector<std::string> objs(200);   // obj num is 200
    std::vector<std::string> objs1(400);  // obj num is 400
    std::string value = "data";
    SetObjOnWorker(0, client, value, WriteMode::WRITE_THROUGH_L2_CACHE, objs);
    SetObjOnWorker(1, client, value, WriteMode::WRITE_THROUGH_L2_CACHE, objs1);
    std::vector<Optional<Buffer>> buffers;
    DS_ASSERT_OK(client2->Get(objs, 0, buffers));
    buffers.clear();
    DS_ASSERT_OK(client2->Get(objs1, 0, buffers));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "ProcessVoluntaryScaledown", "1*call()"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "BatchMigrateMetadata.delay.left", "1*call(3)"));
    client1.reset();
    client2.reset();
    client.reset();
    VoluntaryScaleDownInject(0);                                      // worker index is 0
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));     // time interval is 1000gi
    VoluntaryScaleDownInject(1);                                      // worker index is 1
    ASSERT_TRUE(WaitForVoluntaryDownFinished(20, 1, worker0Address_.ToString()));  // timeout is 20, left num is 2
    ASSERT_TRUE(WaitForVoluntaryDownFinished(20, 1, worker1Address_.ToString()));  // timeout is 20, left num is 2
    int metaNum = 0;
    worker1MasterApi_ = nullptr;
    std::vector<master::QueryMetaRspPb> metaInfos;
    objs.insert(objs.end(), objs1.begin(), objs1.end());
    DS_ASSERT_OK(QueryMeta(objs, metaInfos));
    for (const auto &metaInfo : metaInfos) {
        for (const auto &meta : metaInfo.query_metas()) {
            ASSERT_EQ(meta.meta().primary_address(), worker2Address_.ToString()) << meta.DebugString();
        }
        metaNum += metaInfo.query_metas_size();
    }
    ASSERT_EQ(metaNum, 600);  // obj is 600
}

TEST_F(OCVoluntaryScaleDownTest, LEVEL2_VoluntaryDownMigrateRateLimit)
{
    StartWorkerAndWaitReady({ 0, 1, 2 });
    InitTestEtcdInstance();
    GetHashOnWorker(3);  // worker num is 3
    DS_ASSERT_OK(InitInstanceBase());
    std::shared_ptr<ObjectClient> client, client1, client2;
    InitTestClient(0, client);
    InitTestClient(1, client1);
    InitTestClient(2, client2);  // worker index is 2
    uint64_t count = 120;
    std::vector<std::string> Objects(count);
    std::vector<std::string> Objects1(count);
    std::string value = std::string(10 * 1024ul, 'a');
    SetObjOnWorker(0, client, value, WriteMode::NONE_L2_CACHE, Objects);
    SetObjOnWorker(2, client, value, WriteMode::NONE_L2_CACHE, Objects1); // worker index is 2
    client.reset();
    VoluntaryScaleDownInject(0);  // worker index is 0
    VoluntaryScaleDownInject(2);  // worker index is 2

    worker1MasterApi_ = nullptr;
    int waitTimeout = 10;
    int stillAliveWorkers = 1;  // worker index is 1
    WaitForVoluntaryDownFinished(waitTimeout, stillAliveWorkers, worker0Address_.ToString());
    WaitForVoluntaryDownFinished(waitTimeout, stillAliveWorkers, worker2Address_.ToString());

    std::vector<Optional<Buffer>> buffers;
    DS_ASSERT_OK(client1->Get(Objects, 0, buffers));
    ASSERT_EQ(buffers.size(), Objects.size());
    for (auto &buf : buffers) {
        AssertBufferEqual(*buf, value);
    }
}

TEST_F(OCVoluntaryScaleDownTest, LEVEL1_TestVoluntaryDownMigrateSmallObjectsData)
{
    LOG(INFO) << "Test voluntary scale down and migrate small objects data";
    StartWorkerAndWaitReady({ 0, 1, 2 });
    InitTestEtcdInstance();
    GetHashOnWorker(3);  // worker num is 3
    DS_ASSERT_OK(InitInstanceBase());
    std::shared_ptr<ObjectClient> client, client1, client2;
    InitTestClient(0, client);
    InitTestClient(1, client1);
    InitTestClient(2, client2);  // worker index is 2
    uint64_t count = 400;
    std::vector<std::string> noneL2CacheObjects(count);  // obj num is 400
    std::string value = "data";
    SetObjOnWorker(0, client, value, WriteMode::NONE_L2_CACHE, noneL2CacheObjects);
    client.reset();
    VoluntaryScaleDownInject(0);  // worker index is 0

    worker1MasterApi_ = nullptr;
    int waitTimeout = 10;
    int stillAliveWorkers = 2;
    WaitForVoluntaryDownFinished(waitTimeout, stillAliveWorkers, worker1Address_.ToString());

    // Local Get
    std::vector<Optional<Buffer>> buffers;
    DS_ASSERT_OK(client1->Get(noneL2CacheObjects, 0, buffers));
    ASSERT_EQ(buffers.size(), noneL2CacheObjects.size());
    for (auto &buf : buffers) {
        AssertBufferEqual(*buf, value);
    }

    // Remote Get
    std::vector<Optional<Buffer>> buffers1;
    DS_ASSERT_OK(client2->Get(noneL2CacheObjects, 0, buffers1));
    ASSERT_EQ(buffers1.size(), noneL2CacheObjects.size());
    for (auto &buf : buffers1) {
        AssertBufferEqual(*buf, value);
    }
}

TEST_F(OCVoluntaryScaleDownTest, TestVoluntaryDownMigrateWhenMetaAddressIsEmpty)
{
    LOG(INFO) << "Test voluntary scale down and migrate small objects data";
    StartWorkerAndWaitReady({ 0, 1, 2 });
    InitTestEtcdInstance();
    GetHashOnWorker(3);  // worker num is 3
    DS_ASSERT_OK(InitInstanceBase());
    std::shared_ptr<ObjectClient> client, client1, client2;
    InitTestClient(0, client);
    InitTestClient(1, client1);
    InitTestClient(2, client2);  // worker index is 2
    uint64_t count = 400;
    std::vector<std::string> noneL2CacheObjects(count);  // obj num is 400
    std::string value = "data";
    SetObjOnWorker(0, client, value, WriteMode::NONE_L2_CACHE, noneL2CacheObjects);
    client.reset();
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "WorkerOcService.MigrateData.GetMasterAddr", "1*call()"));
    VoluntaryScaleDownInject(0);  // worker index is 0

    worker1MasterApi_ = nullptr;
    int waitTimeout = 10;
    int stillAliveWorkers = 2;
    WaitForVoluntaryDownFinished(waitTimeout, stillAliveWorkers, worker1Address_.ToString());

    // Local Get
    std::vector<Optional<Buffer>> buffers;
    DS_ASSERT_OK(client1->Get(noneL2CacheObjects, 0, buffers));
    ASSERT_EQ(buffers.size(), noneL2CacheObjects.size());
    for (auto &buf : buffers) {
        AssertBufferEqual(*buf, value);
    }

    // Remote Get
    std::vector<Optional<Buffer>> buffers1;
    DS_ASSERT_OK(client2->Get(noneL2CacheObjects, 0, buffers1));
    ASSERT_EQ(buffers1.size(), noneL2CacheObjects.size());
    for (auto &buf : buffers1) {
        AssertBufferEqual(*buf, value);
    }
}


TEST_F(OCVoluntaryScaleDownTest, TestVoluntaryDownMigrateToSpillDir)
{
    LOG(INFO) << "Test voluntary scale down and migrate objects to spill dir";
    StartWorkerAndWaitReady({ 0, 1, 2 });
    InitTestEtcdInstance();
    SetWorkerHashInjection();
    GetHashOnWorker(3);  // worker num is 3
    DS_ASSERT_OK(InitInstanceBase());
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "worker.migrate_service.return", "2*return(K_OUT_OF_MEMORY)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 2, "worker.migrate_service.return", "2*return(K_OUT_OF_MEMORY)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "worker.migrate_service.memory_available", "1000*return()"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 2, "worker.migrate_service.memory_available", "1000*return()"));
    std::shared_ptr<ObjectClient> client, client1, client2;
    InitTestClient(0, client);
    InitTestClient(1, client1);
    InitTestClient(2, client2);  // worker index is 2
    uint64_t count = 400;
    std::vector<std::string> noneL2CacheObjects(count);  // obj num is 400
    std::string value = "data";
    SetObjOnWorker(0, client, value, WriteMode::NONE_L2_CACHE, noneL2CacheObjects);
    client.reset();
    VoluntaryScaleDownInject(0);  // worker index is 0

    worker1MasterApi_ = nullptr;
    int waitTimeout = 10;
    int stillAliveWorkers = 2;
    WaitForVoluntaryDownFinished(waitTimeout, stillAliveWorkers, worker1Address_.ToString());

    // Local Get
    std::vector<Optional<Buffer>> buffers;
    DS_ASSERT_OK(client1->Get(noneL2CacheObjects, 0, buffers));
    ASSERT_EQ(buffers.size(), noneL2CacheObjects.size());
    for (auto &buf : buffers) {
        AssertBufferEqual(*buf, value);
    }

    // Remote Get
    std::vector<Optional<Buffer>> buffers1;
    DS_ASSERT_OK(client2->Get(noneL2CacheObjects, 0, buffers1));
    ASSERT_EQ(buffers1.size(), noneL2CacheObjects.size());
    for (auto &buf : buffers1) {
        AssertBufferEqual(*buf, value);
    }
}

TEST_F(OCVoluntaryScaleDownTest, TestVoluntaryDownMigrateMeetsNoSpaceError)
{
    LOG(INFO) << "Test voluntary scale down and migrate objects meets no space error";
    StartWorkerAndWaitReady({ 0, 1, 2 });
    InitTestEtcdInstance();
    SetWorkerHashInjection();
    GetHashOnWorker(3);  // worker num is 3
    DS_ASSERT_OK(InitInstanceBase());
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "worker.migrate_service.return", "10*return(K_OUT_OF_MEMORY)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 2, "worker.migrate_service.return", "10*return(K_OUT_OF_MEMORY)"));
    std::shared_ptr<ObjectClient> client, client1, client2;
    InitTestClient(0, client);
    InitTestClient(1, client1);
    InitTestClient(2, client2);  // worker index is 2
    uint64_t count = 400;
    std::vector<std::string> noneL2CacheObjects(count);  // obj num is 400
    std::string value = "data";
    SetObjOnWorker(0, client, value, WriteMode::NONE_L2_CACHE, noneL2CacheObjects);
    client.reset();
    VoluntaryScaleDownInject(0);  // worker index is 0

    worker1MasterApi_ = nullptr;
    int waitTimeout = 10;
    int stillAliveWorkers = 2;
    WaitForVoluntaryDownFinished(waitTimeout, stillAliveWorkers, worker1Address_.ToString());

    // Local Get
    std::vector<Optional<Buffer>> buffers;
    DS_ASSERT_OK(client1->Get(noneL2CacheObjects, 0, buffers));
    ASSERT_EQ(buffers.size(), noneL2CacheObjects.size());
    for (auto &buf : buffers) {
        AssertBufferEqual(*buf, value);
    }

    // Remote Get
    std::vector<Optional<Buffer>> buffers1;
    DS_ASSERT_OK(client2->Get(noneL2CacheObjects, 0, buffers1));
    ASSERT_EQ(buffers1.size(), noneL2CacheObjects.size());
    for (auto &buf : buffers1) {
        AssertBufferEqual(*buf, value);
    }
}

TEST_F(OCVoluntaryScaleDownTest, TestMigrateDataAndMeetObjectUpdate)
{
    LOG(INFO) << "Test voluntary scale down and migrate objects meets object update";
    StartWorkerAndWaitReady({ 0, 1, 2 });
    InitTestEtcdInstance();
    SetWorkerHashInjection();
    GetHashOnWorker(3);  // worker num is 3
    DS_ASSERT_OK(InitInstanceBase());
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "worker.migrate_service.return", "1*sleep(5000)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 2, "worker.migrate_service.return", "1*sleep(5000)"));
    std::shared_ptr<ObjectClient> client, client1, client2;
    std::shared_ptr<KVClient> client3;
    InitTestClient(0, client);
    InitTestClient(1, client1);
    InitTestClient(2, client2);
    InitTestKVClient(2, client3);  // worker index is 2
    uint64_t count = 90;
    std::vector<std::string> noneL2CacheObjects(count);  // obj num is 400
    std::string value = "data";
    std::string newValue = "Stop the world";
    SetObjOnWorker(0, client, value, WriteMode::NONE_L2_CACHE, noneL2CacheObjects);
    client.reset();
    VoluntaryScaleDownInject(0);  // worker index is 0

    // Update/delete some objects
    uint64_t batch = 3;
    for (uint64_t i = 0; i < count; ++i) {
        CreateParam param;
        if (i < count / batch) {
            DS_ASSERT_OK(client1->Put(noneL2CacheObjects[i], reinterpret_cast<const uint8_t *>(newValue.data()),
                                      newValue.size(), param, {}));
        } else if (i < 2 * count / batch) {
            DS_ASSERT_OK(client2->Put(noneL2CacheObjects[i], reinterpret_cast<const uint8_t *>(newValue.data()),
                                      newValue.size(), param, {}));
        } else {
            DS_ASSERT_OK(client3->Del(noneL2CacheObjects[i]));
        }
    }

    worker1MasterApi_ = nullptr;
    int waitTimeout = 10;
    int stillAliveWorkers = 2;
    WaitForVoluntaryDownFinished(waitTimeout, stillAliveWorkers, worker1Address_.ToString());

    uint64_t nullObejcts = 30;
    uint64_t nullCount = 0;
    std::vector<Optional<Buffer>> buffers;
    DS_ASSERT_OK(client1->Get(noneL2CacheObjects, 0, buffers));
    ASSERT_EQ(buffers.size(), noneL2CacheObjects.size());
    for (auto &buf : buffers) {
        if (buf) {
            AssertBufferEqual(*buf, newValue);
        } else {
            ++nullCount;
        }
    }
    ASSERT_EQ(nullObejcts, nullCount);

    // Remote Get
    nullCount = 0;
    std::vector<Optional<Buffer>> buffers1;
    DS_ASSERT_OK(client2->Get(noneL2CacheObjects, 0, buffers1));
    ASSERT_EQ(buffers1.size(), noneL2CacheObjects.size());
    for (auto &buf : buffers1) {
        if (buf) {
            AssertBufferEqual(*buf, newValue);
        } else {
            ++nullCount;
        }
    }
    ASSERT_EQ(nullObejcts, nullCount);
}

TEST_F(OCVoluntaryScaleDownTest, TestMigrateDataAndPartOfObjectsFailed)
{
    LOG(INFO) << "Test voluntary scale down and migrate objects and parts of them failed";
    StartWorkerAndWaitReady({ 0, 1, 2 });
    InitTestEtcdInstance();
    SetWorkerHashInjection();
    GetHashOnWorker(3);  // worker num is 3
    DS_ASSERT_OK(InitInstanceBase());
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "worker.migrate_service.spill_available1", "1000*return()"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "worker.migrate_service.memory_available1", "500*return()"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 2, "worker.migrate_service.spill_available1", "1000*return()"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 2, "worker.migrate_service.memory_available1", "500*return()"));
    std::shared_ptr<ObjectClient> client, client1, client2;
    std::shared_ptr<KVClient> client3;
    InitTestClient(0, client);
    InitTestClient(1, client1);
    InitTestClient(2, client2);
    uint64_t count = 1000;
    std::vector<std::string> noneL2CacheObjects(count);  // obj num is 400
    std::string value = "data";
    SetObjOnWorker(0, client, value, WriteMode::NONE_L2_CACHE, noneL2CacheObjects);
    client.reset();
    VoluntaryScaleDownInject(0);  // worker index is 0

    worker1MasterApi_ = nullptr;
    int waitTimeout = 10;
    int stillAliveWorkers = 2;
    WaitForVoluntaryDownFinished(waitTimeout, stillAliveWorkers, worker1Address_.ToString());

    // Local Get
    std::vector<Optional<Buffer>> buffers;
    DS_ASSERT_OK(client1->Get(noneL2CacheObjects, 0, buffers));
    ASSERT_EQ(buffers.size(), noneL2CacheObjects.size());
    for (auto &buf : buffers) {
        AssertBufferEqual(*buf, value);
    }

    // Remote Get
    std::vector<Optional<Buffer>> buffers1;
    DS_ASSERT_OK(client2->Get(noneL2CacheObjects, 0, buffers1));
    ASSERT_EQ(buffers1.size(), noneL2CacheObjects.size());
    for (auto &buf : buffers1) {
        AssertBufferEqual(*buf, value);
    }
}

TEST_F(OCVoluntaryScaleDownTest, LEVEL2_VoluntaryDownTwoWorkersAndMigrateData)
{
    LOG(INFO) << "Test voluntary scale down two workers and migrate data";
    StartWorkerAndWaitReady({ 0, 1, 2 });
    InitTestEtcdInstance();
    SetWorkerHashInjection();
    GetHashOnWorker(3);  // worker num is 3
    DS_ASSERT_OK(InitInstanceBase());
    std::shared_ptr<ObjectClient> client, client1, client2;
    InitTestClient(0, client);
    InitTestClient(1, client1);
    InitTestClient(2, client2);  // worker index is 2
    uint64_t count = 400;
    std::vector<std::string> noneL2CacheObjects(count);  // obj num is 400
    std::string value = "data";
    SetObjOnWorker(0, client, value, WriteMode::NONE_L2_CACHE, noneL2CacheObjects);
    client.reset();
    VoluntaryScaleDownInject(0);                                   // worker index is 0
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));  // time interval is 1000
    int waitTimeout = 20;
    int stillAliveWorkers = 2;
    WaitForVoluntaryDownFinished(waitTimeout, stillAliveWorkers, worker1Address_.ToString());

    client1.reset();
    VoluntaryScaleDownInject(1);  // worker index is 0
    stillAliveWorkers = 1;
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));  // time interval is 1000
    WaitForVoluntaryDownFinished(waitTimeout, stillAliveWorkers, worker1Address_.ToString());

    // Get
    std::vector<Optional<Buffer>> buffers;
    DS_ASSERT_OK(client2->Get(noneL2CacheObjects, 0, buffers));
    ASSERT_EQ(buffers.size(), noneL2CacheObjects.size());
    for (auto &buf : buffers) {
        AssertBufferEqual(*buf, value);
    }
}

TEST_F(OCVoluntaryScaleDownTest, VoluntaryDownTwoWorkersAndMigrateData2)
{
    LOG(INFO) << "Test voluntary scale down two workers and migrate data";
    StartWorkerAndWaitReady({ 0, 1, 2 });
    InitTestEtcdInstance();
    SetWorkerHashInjection();
    GetHashOnWorker(3);  // worker num is 3
    DS_ASSERT_OK(InitInstanceBase());
    std::shared_ptr<ObjectClient> client, client1, client2;
    const int timeoutMs = 2000;
    InitTestClient(0, client, timeoutMs);
    InitTestClient(1, client1, timeoutMs);
    InitTestClient(2, client2, timeoutMs);  // worker index is 2
    uint64_t count = 400;
    std::vector<std::string> noneL2CacheObjects(count);  // obj num is 400
    std::string value = "data";
    SetObjOnWorker(0, client, value, WriteMode::NONE_L2_CACHE, noneL2CacheObjects);
    client.reset();
    client1.reset();
    VoluntaryScaleDownInject(0);  // worker index is 0
    VoluntaryScaleDownInject(1);
    int waitTimeout = 20;
    int stillAliveWorkers = 1;
    WaitForVoluntaryDownFinished(waitTimeout, stillAliveWorkers, worker1Address_.ToString());

    // Get
    std::vector<Optional<Buffer>> buffers;
    DS_ASSERT_OK(client2->Get(noneL2CacheObjects, 0, buffers));
    ASSERT_EQ(buffers.size(), noneL2CacheObjects.size());
    for (auto &buf : buffers) {
        AssertBufferEqual(*buf, value);
    }
}

TEST_F(OCVoluntaryScaleDownTest, LEVEL1_VoluntaryDownOnlyOneWorkerLeft)
{
    LOG(INFO) << "Test voluntary scale down two workers and migrate data";
    StartWorkerAndWaitReady({ 0 });
    InitTestEtcdInstance();
    std::shared_ptr<ObjectClient> client;
    InitTestClient(0, client);
    uint64_t count = 400;
    std::vector<std::string> noneL2CacheObjects(count);  // obj num is 400
    std::string value = "data";
    for (uint32_t i = 0; i < noneL2CacheObjects.size(); ++i) {
        noneL2CacheObjects[i] = "a_key_hash_to_" + std::to_string(i);
        CreateParam param{ .writeMode = WriteMode::NONE_L2_CACHE };
        DS_ASSERT_OK(client->Put(noneL2CacheObjects[i], reinterpret_cast<const uint8_t *>(value.data()), value.size(),
                                 param, {}));
    }
    client.reset();
    VoluntaryScaleDownInject(0);  // worker index is 0
    int waitTimeout = 20;
    int stillAliveWorkers = 0;
    DS_ASSERT_OK(cluster_->GetWorkerAddr(0, worker0Address_));
    WaitForVoluntaryDownFinished(waitTimeout, stillAliveWorkers, worker0Address_.ToString());
}

TEST_F(OCVoluntaryScaleDownTest, LEVEL1_VoluntaryDownOneWorkerWhenDestFailed)
{
    LOG(INFO) << "Test voluntary scale down two workers and migrate data";
    StartWorkerAndWaitReady({ 0, 1 });
    InitTestEtcdInstance();
    SetWorkerHashInjection({ 0, 1 });
    GetHashOnWorker(2);  // worker num is 2
    std::shared_ptr<ObjectClient> client;
    InitTestClient(0, client);
    uint64_t count = 300;
    std::vector<std::string> noneL2CacheObjects(count);  // obj num is 400
    std::string value = "data";
    SetObjOnWorker(0, client, value, WriteMode::NONE_L2_CACHE, noneL2CacheObjects);
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "ScaleUpTask.NotRunVoluntaryDownTask", "1*sleep(5000)"));
    client.reset();
    VoluntaryScaleDownInject(0);  // worker index is 0
    DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, 1));
    int waitTimeout = 20;
    int stillAliveWorkers = 0;
    DS_ASSERT_OK(cluster_->GetWorkerAddr(0, worker0Address_));
    WaitForVoluntaryDownFinished(waitTimeout, stillAliveWorkers, worker0Address_.ToString());
}

TEST_F(OCVoluntaryScaleDownTest, LEVEL1_VoluntaryDownOneWorkerWhenMigrateDataDestFailed)
{
    LOG(INFO) << "Test voluntary scale down two workers and migrate data";
    StartWorkerAndWaitReady({ 0, 1 });
    InitTestEtcdInstance();
    SetWorkerHashInjection({ 0, 1 });
    GetHashOnWorker(2);  // worker num is 2
    std::shared_ptr<ObjectClient> client;
    InitTestClient(0, client);
    uint64_t count = 300;
    std::vector<std::string> noneL2CacheObjects(count);  // obj num is 400
    std::string value = "data";
    SetObjOnWorker(0, client, value, WriteMode::NONE_L2_CACHE, noneL2CacheObjects);
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "VoluntaryScaledown.MigrateData.Delay", "1*sleep(5000)"));
    client.reset();
    VoluntaryScaleDownInject(0);  // worker index is 0
    DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, 1));
    int waitTimeout = 20;
    int stillAliveWorkers = 0;
    DS_ASSERT_OK(cluster_->GetWorkerAddr(0, worker0Address_));
    WaitForVoluntaryDownFinished(waitTimeout, stillAliveWorkers, worker0Address_.ToString());
}

TEST_F(OCVoluntaryScaleDownTest, LEVEL2_TestMigrateDataFailAndGet)
{
    LOG(INFO) << "Test Migrate data fail and get objects";
    StartWorkerAndWaitReady({ 0, 1, 2 });
    InitTestEtcdInstance();
    GetHashOnWorker(3);  // worker num is 3
    DS_ASSERT_OK(InitInstanceBase());
    DS_ASSERT_OK(
        cluster_->SetInjectAction(WORKER, 1, "worker.migrate_service.return", "200*return(K_RPC_UNAVAILABLE)"));
    DS_ASSERT_OK(
        cluster_->SetInjectAction(WORKER, 2, "worker.migrate_service.return", "200*return(K_RPC_UNAVAILABLE)"));
    std::shared_ptr<ObjectClient> client, client1;
    InitTestClient(0, client);
    InitTestClient(1, client1);  // worker index is 1
    uint64_t count = 400;
    std::vector<std::string> noneL2CacheObjects(count);  // obj num is 400
    std::string value = "data";
    SetObjOnWorker(0, client, value, WriteMode::NONE_L2_CACHE, noneL2CacheObjects);
    client.reset();
    VoluntaryScaleDownInject(0);  // worker index is 0
    sleep(2);                     // sleep 2 seconds
    // Get
    std::vector<Optional<Buffer>> buffers;
    DS_ASSERT_OK(client1->Get(noneL2CacheObjects, 0, buffers));
    ASSERT_EQ(buffers.size(), noneL2CacheObjects.size());
    for (auto &buf : buffers) {
        AssertBufferEqual(*buf, value);
    }

    int waitTimeout = 20;
    int stillAliveWorkers = 2;
    WaitForVoluntaryDownFinished(waitTimeout, stillAliveWorkers, worker1Address_.ToString());
}

TEST_F(OCVoluntaryScaleDownTest, VoluntaryDownMigrateDataMultiType)
{
    LOG(INFO) << "Test voluntary scale down and migrate memory and disk data";
    StartWorkerAndWaitReady({ 0, 1, 2 });
    InitTestEtcdInstance();
    SetWorkerHashInjection();
    GetHashOnWorker(3);  // worker num is 3
    DS_ASSERT_OK(InitInstanceBase());
    std::shared_ptr<ObjectClient> client, client1, client2;
    const int timeoutMs = 1'000;
    InitTestClient(0, client, timeoutMs);
    InitTestClient(1, client1, timeoutMs);
    InitTestClient(2, client2, timeoutMs);  // worker index is 2
    const size_t objCount = 5;
    std::vector<std::string> memoryObjects(objCount);
    const size_t objSize = 5 * 1024ul * 1024ul;  // 5 MB
    std::string value = GenRandomString(objSize);
    SetObjOnWorker(0, client, value, {}, memoryObjects);
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "StandbyWorkerNotSame", "return()"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "hashring.finishaddnodeinfo", "sleep(2000)"));
    client.reset();
    VoluntaryScaleDownInject(0);  // worker index is 0

    worker1MasterApi_ = nullptr;
    int waitTimeout = 10;
    int stillAliveWorkers = 2;
    WaitForVoluntaryDownFinished(waitTimeout, stillAliveWorkers, worker1Address_.ToString());

    std::vector<Optional<Buffer>> buffers;
    for (const auto &obj : memoryObjects) {
        auto rc = client1->Get({obj}, 0, buffers);
        if (rc.IsError()) {
            rc = client2->Get({obj}, 0, buffers);
        }
        DS_ASSERT_OK(rc);
        ASSERT_EQ(buffers.size(), 1);
        AssertBufferEqual(*buffers[0], value);
    }
}

class OCVoluntaryScaleDownNoSpillTest : public OCScaleTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numEtcd = 1;
        opts.numWorkers = 4;  // worker num is 4
        opts.numOBS = 0;
        opts.enableSpill = false;
        opts.enableDistributedMaster = "true";
        opts.addNodeTime = SCALE_DOWN_ADD_TIME;
        uint64_t sharedMemorySize = 100;
        opts.workerGflagParams = FormatString(
            " -v=2 -node_timeout_s=%d -node_dead_timeout_s=%d -auto_del_dead_node=true -shared_memory_size_mb=%ld ",
            nodeTimeout_, nodeDeadTimeout_, sharedMemorySize);
        opts.waitWorkerReady = false;
        for (size_t i = 0; i < opts.numWorkers; i++) {
            opts.workerConfigs.emplace_back(HOST_IP_PREFIX + std::to_string(i), GetFreePort());
            workerAddress_.emplace_back(opts.workerConfigs.back().ToString());
        }
        const int worker0ConfigIndex = 0;
        const int worker1ConfigIndex = 1;
        const int worker2ConfigIndex = 2;
        const int worker3ConfigIndex = 3;

        worker0Address_ = opts.workerConfigs[worker0ConfigIndex];
        worker1Address_ = opts.workerConfigs[worker1ConfigIndex];
        worker2Address_ = opts.workerConfigs[worker2ConfigIndex];
        worker3Address_ = opts.workerConfigs[worker3ConfigIndex];
    }

protected:
    const int nodeTimeout_ = 3;
    const int nodeDeadTimeout_ = 5;
    HostPort hostPort_;
    std::shared_ptr<AkSkManager> akSkManager_;
    HostPort worker0Address_;
    HostPort worker1Address_;
    HostPort worker2Address_;
    HostPort worker3Address_;
    std::string accessKey_ = "QTWAOYTTINDUT2QVKYUC";
    std::string secretKey_ = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
};

TEST_F(OCVoluntaryScaleDownNoSpillTest, VoluntaryWorkerMigrateDataFillUp)
{
    FLAGS_v = 2;  // vlog is 2
    const int kWaitTimeSeconds = 20;
    const int kWorkerNum = 2;
    const int sleepDuration = 5;
    const int client3Index = 3;
    const int valueSize = 1 * 1024 * 1024;
    const int kFirstBatchObjects = 26;
    const int kFirstOrigin = 100;
    const int kSecondBatchObjects = 50;
    const int kSecondOrigin = 200;
    const int kFourthBatchObjects = 79;
    const int kFourthOrigin = 200;
    const int voluntaryScaleDownInjectIndex0 = 0;
    const int voluntaryScaleDownInjectIndex1 = 1;

    InitTestEtcdInstance();
    StartWorkerAndWaitReady({ 0, 1, 2, 3 });
    std::map<std::string, std::string> workerAddrUuidMap;
    GetWorkerUuidMap(workerAddrUuidMap);

    std::shared_ptr<ObjectClient> client, client1, client2, client3;
    InitTestClient(0, client);
    InitTestClient(1, client1);
    InitTestClient(2, client2);
    InitTestClient(client3Index, client3);

    CreateParam param{ .writeMode = WriteMode::NONE_L2_CACHE };
    std::string objectPrefix = "objecttest_";
    std::vector<std::string> objectKeys;
    std::string value(valueSize, 'x');

    for (int i = kFirstOrigin; i < kFirstOrigin + kFirstBatchObjects; i++) {
        auto objKey = objectPrefix + std::to_string(i);
        objectKeys.emplace_back(objKey);
        DS_ASSERT_OK(client->Put(objKey, reinterpret_cast<const uint8_t *>(value.data()), value.size(), param));
    }

    for (int i = kSecondOrigin; i < kSecondOrigin + kSecondBatchObjects; i++) {
        auto objKey = objectPrefix + std::to_string(i);
        objectKeys.emplace_back(objKey);
        DS_ASSERT_OK(client1->Put(objKey, reinterpret_cast<const uint8_t *>(value.data()), value.size(), param));
    }

    for (int i = kFourthOrigin; i < kFourthOrigin + kFourthBatchObjects; i++) {
        auto objKey = objectPrefix + std::to_string(i);
        objectKeys.emplace_back(objKey);
        DS_ASSERT_OK(client3->Put(objKey, reinterpret_cast<const uint8_t *>(value.data()), value.size(), param));
    }

    client.reset();
    VoluntaryScaleDownInject(voluntaryScaleDownInjectIndex0);
    client1.reset();
    VoluntaryScaleDownInject(voluntaryScaleDownInjectIndex1);
    WaitForVoluntaryDownFinished(kWaitTimeSeconds, kWorkerNum, worker0Address_.ToString());
    WaitForVoluntaryDownFinished(kWaitTimeSeconds, kWorkerNum, worker1Address_.ToString());

    sleep(sleepDuration);

    std::vector<ObjMetaInfo> objMetas;
    for (int i = 0; i < kFirstBatchObjects + kSecondBatchObjects; i++) {
        DS_ASSERT_OK(client2->GetObjMetaInfo("", { objectKeys[i] }, objMetas));
        std::string worker2Uuid = workerAddrUuidMap.at(worker2Address_.ToString());
        ASSERT_EQ(std::count(objMetas[0].locations.begin(), objMetas[0].locations.end(), worker2Uuid), 1);
    }
}

class OCVScaleDownDiskTest : public OCVoluntaryScaleDownTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        OCVoluntaryScaleDownTest::SetClusterSetupOptions(opts);
        for (size_t i = 0; i < opts.numWorkers; i++) {
            std::string dir = GetTestCaseDataDir() + "/worker" + std::to_string(i) + "/shared_disk";
            opts.workerSpecifyGflagParams[i] = FormatString("-shared_disk_directory=%s", dir);
            if (i == 0) {
                opts.workerSpecifyGflagParams[i] += " -shared_disk_size_mb=32";
            } else {
                opts.workerSpecifyGflagParams[i] += " -shared_disk_size_mb=20";
            }
        }
    }

    void SetObjOnWorker(const int &workerIdx, std::shared_ptr<ObjectClient> client, const std::string &data,
                        CreateParam param, std::vector<std::string> &objectKey)
    {
        for (uint32_t i = 0; i < objectKey.size(); ++i) {
            objectKey[i] = "a_key_hash_to_" + std::to_string(workerHashValue_[workerIdx]--);
            DS_ASSERT_OK(
                client->Put(objectKey[i], reinterpret_cast<const uint8_t *>(data.data()), data.size(), param, {}));
        }
    }
};

TEST_F(OCVScaleDownDiskTest, LEVEL1_VoluntaryDownMigrateData)
{
    LOG(INFO) << "Test voluntary scale down and migrate disk data";
    StartWorkerAndWaitReady({ 0, 1, 2 });
    InitTestEtcdInstance();
    SetWorkerHashInjection();
    GetHashOnWorker(3);  // worker num is 3
    DS_ASSERT_OK(InitInstanceBase());
    std::shared_ptr<ObjectClient> client, client1, client2;
    InitTestClient(0, client);
    InitTestClient(1, client1);
    InitTestClient(2, client2);  // worker index is 2
    uint64_t objCount = 400;
    std::vector<std::string> diskObjects(objCount);  // obj num is 400
    std::string value = "data";
    SetObjOnWorker(0, client, value, { WriteMode::NONE_L2_CACHE, ConsistencyType::PRAM, CacheType::DISK }, diskObjects);
    client.reset();
    VoluntaryScaleDownInject(0);  // worker index is 0

    worker1MasterApi_ = nullptr;
    int waitTimeout = 10;
    int stillAliveWorkers = 2;
    WaitForVoluntaryDownFinished(waitTimeout, stillAliveWorkers, worker1Address_.ToString());

    // Local Get
    std::vector<Optional<Buffer>> buffers;
    DS_ASSERT_OK(client1->Get(diskObjects, 0, buffers));
    ASSERT_EQ(buffers.size(), diskObjects.size());
    for (auto &buf : buffers) {
        AssertBufferEqual(*buf, value);
    }

    // Remote Get
    std::vector<Optional<Buffer>> buffers1;
    DS_ASSERT_OK(client2->Get(diskObjects, 0, buffers1));
    ASSERT_EQ(buffers1.size(), diskObjects.size());
    for (auto &buf : buffers1) {
        AssertBufferEqual(*buf, value);
    }
}

TEST_F(OCVScaleDownDiskTest, VoluntaryDownMigrateDataMultiType)
{
    LOG(INFO) << "Test voluntary scale down and migrate memory and disk data";
    StartWorkerAndWaitReady({ 0, 1, 2 });
    InitTestEtcdInstance();
    SetWorkerHashInjection();
    GetHashOnWorker(3);  // worker num is 3
    DS_ASSERT_OK(InitInstanceBase());
    std::shared_ptr<ObjectClient> client, client1, client2;
    const int timeoutMs = 1'000;
    InitTestClient(0, client, timeoutMs);
    InitTestClient(1, client1, timeoutMs);
    InitTestClient(2, client2, timeoutMs);  // worker index is 2
    const size_t objCount = 5;
    std::vector<std::string> diskObjects(objCount);
    std::vector<std::string> memoryObjects(objCount);
    const size_t objSize = 5 * 1024ul * 1024ul;  // 5 MB
    std::string value = GenRandomString(objSize);
    SetObjOnWorker(0, client, value, { WriteMode::NONE_L2_CACHE, ConsistencyType::PRAM, CacheType::DISK }, diskObjects);
    SetObjOnWorker(0, client, value, {}, memoryObjects);
    client.reset();
    VoluntaryScaleDownInject(0);  // worker index is 0

    worker1MasterApi_ = nullptr;
    int waitTimeout = 10;
    int stillAliveWorkers = 2;
    WaitForVoluntaryDownFinished(waitTimeout, stillAliveWorkers, worker1Address_.ToString());

    std::vector<Optional<Buffer>> buffers;
    DS_ASSERT_OK(client1->Get(memoryObjects, 0, buffers));
    for (const auto &obj : diskObjects) {
        auto rc = client1->Get({ obj }, 0, buffers);
        if (rc.IsError()) {
            rc = client2->Get({ obj }, 0, buffers);
        }
        DS_ASSERT_OK(rc);
        ASSERT_EQ(buffers.size(), 1);
        AssertBufferEqual(*buffers[0], value);
    }
}
}  // namespace st
}  // namespace datasystem