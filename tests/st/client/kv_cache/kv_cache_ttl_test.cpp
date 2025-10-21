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
 * Description:
 */
#include <gtest/gtest.h>
#include <algorithm>

#include "common.h"
#include "datasystem/common/eventloop/timer_queue.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/timer.h"
#include "client/object_cache/oc_client_common.h"

namespace datasystem {
namespace st {
class KVCacheTtlTest : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        FLAGS_v = 1;
        opts.numOBS = 1;
        opts.numWorkers = 3;
        opts.numEtcd = 1;
        opts.workerGflagParams = "-node_timeout_s=5 -shared_memory_size_mb=2048 -v=2";
    }

    void SetUp() override
    {
        ExternalClusterTest::SetUp();
        InitTestKVClient(client1Index, client1);
        InitTestKVClient(client2Index, client2);
        InitTestKVClient(client3Index, client3);
    }

    void TearDown() override
    {
        client1.reset();
        client2.reset();
        client3.reset();
        ExternalClusterTest::TearDown();
    }

protected:
    std::shared_ptr<KVClient> client1;
    std::shared_ptr<KVClient> client2;
    std::shared_ptr<KVClient> client3;

private:
    const uint32_t client1Index = 0;
    const uint32_t client2Index = 1;
    const uint32_t client3Index = 2;
};

TEST_F(KVCacheTtlTest, DISABLED_BasicTest)
{
    // Test normally expired deleted objects.
    std::string key = "k1";
    std::string value = "hello";
    SetParam param{ .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE, .ttlSecond = 3 };
    DS_ASSERT_OK(client1->Set(key, value, param));

    // Update the expiration time of an object that never expires.
    std::string key2 = "k2";
    std::string value2 = "world";
    DS_ASSERT_OK(client1->Set(key2, value2, { .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE }));

    // Update the expiration time of the object before it expires.
    std::string key3 = "k3";
    std::string value3 = "welcome";
    DS_ASSERT_OK(client1->Set(key3, value3, { .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE, .ttlSecond = 30 }));

    // No object will expire before ttl second
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    std::string outValue;
    EXPECT_EQ(client2->Get(key, outValue), Status::OK());
    EXPECT_EQ(value, outValue);
    EXPECT_EQ(client2->Get(key2, outValue), Status::OK());
    EXPECT_EQ(value2, outValue);
    EXPECT_EQ(client2->Get(key3, outValue), Status::OK());
    EXPECT_EQ(value3, outValue);

    // Update expire time of k3
    DS_ASSERT_OK(client1->Set(key3, value3, { .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE, .ttlSecond = 3 }));

    // After expire time, k1 is deleted.
    std::this_thread::sleep_for(std::chrono::milliseconds(2500));
    EXPECT_NE(client1->Get(key, value), Status::OK());
    EXPECT_EQ(client1->Get(key2, value), Status::OK());
    EXPECT_EQ(client1->Get(key3, value), Status::OK());

    // Update the expire time of k2
    std::string value4 = "to";
    DS_ASSERT_OK(client1->Set(key2, value4, { .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE, .ttlSecond = 1 }));
    EXPECT_EQ(client2->Get(key2, outValue), Status::OK());
    EXPECT_TRUE(outValue == value4 || outValue == value2);

    // k2 and k3 will be deleted after expired.
    std::this_thread::sleep_for(std::chrono::milliseconds(2000));
    EXPECT_NE(client1->Get(key2, value), Status::OK());
    EXPECT_NE(client1->Get(key3, value), Status::OK());
}

TEST_F(KVCacheTtlTest, LEVEL2_MasterRestart)
{
    std::string key = "k1";
    std::string value = "hello";
    DS_ASSERT_OK(client1->Set(key, value, { .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE, .ttlSecond = 5 }));
    std::string key2 = "k2";
    DS_ASSERT_OK(client3->Set(key2, value, { .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE, .ttlSecond = 1 }));

    std::string outValue;
    EXPECT_EQ(client2->Get(key, outValue), Status::OK());
    EXPECT_EQ(value, outValue);

    // Shutdown master and then restart to verify the metadata recovery.
    cluster_->QuicklyShutdownWorker(0);
    DS_ASSERT_OK(cluster_->StartNode(WORKER, 0, " -client_reconnect_wait_s=1"));
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 0));

    std::this_thread::sleep_for(std::chrono::seconds(5));
    EXPECT_NE(client1->Get(key, value), Status::OK());
    EXPECT_NE(client3->Get(key2, value), Status::OK());
}

TEST_F(KVCacheTtlTest, LEVEL1_MasterWorkerDead)
{
    int eleNum = 6;
    std::vector<std::string> ids(eleNum, "");
    std::vector<std::string> values(eleNum, "");
    for (int i = 0; i < eleNum; i++) {
        std::string num = std::to_string(i);
        ids[i] = "k" + num;
        values[i] = "hello" + num;
        DS_ASSERT_OK(
            client1->Set(ids[i], values[i], { .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE, .ttlSecond = 2 }));
    }
    // Shutdown worker.
    cluster_->ShutdownNode(ClusterNodeType::WORKER, 0);
    std::this_thread::sleep_for(std::chrono::seconds(2));

    // After expired time restart worker and master
    DS_ASSERT_OK(cluster_->StartNode(ClusterNodeType::WORKER, 0, ""));
    DS_ASSERT_OK(cluster_->WaitNodeReady(ClusterNodeType::WORKER, 0));

    std::vector<std::string> failedKeys;
    EXPECT_EQ(client3->Del(ids, failedKeys), Status::OK());
    EXPECT_EQ(failedKeys.size(), static_cast<uint64_t>(0));
}

TEST_F(KVCacheTtlTest, LEVEL2_TtlWithDuplicatedObjectKey)
{
    std::string key = "k1";
    std::string value = "hello";
    DS_ASSERT_OK(client1->Set(key, value, { .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE, .ttlSecond = 5 }));
    std::string outValue;
    DS_ASSERT_OK(client2->Get(key, outValue));
    EXPECT_EQ(value, outValue);
    // ExpiredObjectManger getExpiredOjbect but don't notify to delete.
    DS_ASSERT_OK(
        cluster_->SetInjectAction(ClusterNodeType::WORKER, 0, "master.ExpiredObjectManager.AsyncDelete", "1*call(10)"));
    DS_ASSERT_OK(
        cluster_->SetInjectAction(ClusterNodeType::WORKER, 1, "master.ExpiredObjectManager.AsyncDelete", "1*call(10)"));
    std::this_thread::sleep_for(std::chrono::seconds(6));
    std::string newValue = "world";
    std::string newOutValue;
    // Before object really delete, update the object with new ttl will retry until successful.
    DS_EXPECT_OK(client1->Set(key, newValue, { .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE, .ttlSecond = 10 }));
    DS_EXPECT_OK(client2->Get(key, newOutValue));
    std::this_thread::sleep_for(std::chrono::seconds(5));
    DS_EXPECT_OK(client2->Get(key, newOutValue));
    EXPECT_EQ(newValue, newOutValue);
    std::this_thread::sleep_for(std::chrono::seconds(8));
    DS_ASSERT_NOT_OK(client2->Get(key, newOutValue));
}

TEST_F(KVCacheTtlTest, DISABLED_TTLWithBigObject)
{
    size_t num = 2;
    SetParam param = { .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE, .ttlSecond = 2 };

    std::string value = RandomData().GetPartRandomString(400ul * 1024 * 1024, 100);
    for (size_t i = 0; i < num; i++) {
        std::string key = "key" + std::to_string(i);
        LOG(INFO) << "prepare to set " << key;
        DS_ASSERT_OK(client1->Set(key, value, param));
        LOG(INFO) << "set " << key << " finish";
    }
    std::this_thread::sleep_for(std::chrono::seconds(2));
    for (size_t i = 0; i < num; i++) {
        std::string key = "key" + std::to_string(i);
        std::string valueGet;
        DS_ASSERT_NOT_OK(client1->Get(key, valueGet));
    }
}

class LocalSetRemoteGet {
public:
    explicit LocalSetRemoteGet(BaseCluster *cluster) : cluster_(cluster)
    {
    }

    ~LocalSetRemoteGet() = default;

    void Init(uint32_t objectNum, uint32_t maxTtlSecond, uint32_t perWorkerClientNum, uint32_t minSleepUs,
              uint32_t maxSleepUs)
    {
        objectNum_ = objectNum;
        maxTtlSecond_ = maxTtlSecond;
        minSleepUs_ = minSleepUs;
        maxSleepUs_ = maxSleepUs;

        uint32_t workerNum = cluster_->GetWorkerNum();
        clients_.resize(perWorkerClientNum * workerNum);  // 10 * 5
        clientData_.resize(clients_.size());
        LOG(INFO) << FormatString("The client size %zu, perWorkerClientNum:%u,  workerNum:%u", clients_.size(),
                                  perWorkerClientNum, workerNum);
        for (uint32_t i = 0; i < perWorkerClientNum; i++) {
            for (uint32_t j = 0; j < workerNum; j++) {  // 0...4
                std::cout << "i:" << i << ", j:" << j << ", i * perWorkerClientNum + j: " << i * workerNum + j
                          << std::endl;
                // 1 * 10 + j
                InitTestKVClient(j, clients_[i * workerNum + j]);
            }
        }
    }

    void Run()
    {
        uint64_t startTime = GetSteadyClockTimeStampUs();
        std::vector<std::shared_ptr<std::thread>> threads;
        for (uint32_t i = 0; i < clients_.size(); i++) {
            threads.push_back(std::make_shared<std::thread>(std::bind(&LocalSetRemoteGet::InsertTtlData, this, i)));
        }
        for (size_t i = 0; i < threads.size(); i++) {
            threads[i]->join();
        }
        uint64_t costTimeMs = (GetSteadyClockTimeStampUs() - startTime) / 1000;
        LOG(INFO) << "It cost " << costTimeMs << "ms to put " << clients_.size() * objectNum_ << " object.";
        std::this_thread::sleep_for(std::chrono::seconds(maxTtlSecond_ + 1));
        while (TimerQueue::GetInstance()->Size() > 0) {
            LOG(INFO) << "Current timer size:" << TimerQueue::GetInstance()->Size();
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
        clients_.clear();
        clientData_.clear();
        LOG(INFO) << "Expired succeed get num:" << succeedNum_.load() << ", failed get num:" << failedNum_;
    }

private:
    struct Data {
        uint32_t clientIndex;
        std::string key;
        std::string value;
        uint32_t ttlSecond;
    };

    void InitTestKVClient(uint32_t workerIndex, std::shared_ptr<KVClient> &client)
    {
        HostPort workerAddress;
        ASSERT_TRUE(workerIndex < cluster_->GetWorkerNum());
        DS_ASSERT_OK(cluster_->GetWorkerAddr(workerIndex, workerAddress));
        ConnectOptions connectOptions = { .host = workerAddress.Host(),
                                          .port = workerAddress.Port(),
                                          .connectTimeoutMs = 60 * 1000,
                                          .clientPublicKey = "",
                                          .clientPrivateKey = "",
                                          .serverPublicKey = "",
                                          .accessKey = "QTWAOYTTINDUT2QVKYUC",
                                          .secretKey = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc" };

        client = std::make_shared<KVClient>(connectOptions);
        DS_ASSERT_OK(client->Init());
    }

    void InsertTtlData(uint32_t clientIndex)
    {
        clientData_[clientIndex].resize(objectNum_);
        auto &data = clientData_[clientIndex];
        uint32_t dataLen = 50;
        uint32_t maxVerifyNum = 10;
        uint32_t verifyInterval = (objectNum_ > maxVerifyNum) ? (objectNum_ / maxVerifyNum) : 1;
        for (uint32_t i = 0; i < objectNum_; i++) {
            data[i].clientIndex = clientIndex;
            data[i].value = random_.GetRandomString(dataLen);
            data[i].ttlSecond = random_.GetRandomUint32(1, maxTtlSecond_);
            data[i].key = FormatString("client%u-%u-%u", clientIndex, i, data[i].ttlSecond);
            SetParam param{ .writeMode = WriteMode::NONE_L2_CACHE, .ttlSecond = data[i].ttlSecond };
            DS_ASSERT_OK(clients_[clientIndex]->Set(data[i].key, data[i].value, param));
            if (i % verifyInterval == 0) {
                TimerQueue::TimerImpl timer;
                TimerQueue::GetInstance()->AddTimer(
                    data[i].ttlSecond * TIME_UNIT_CONVERSION + MAX_DELAY_DELETE_MS,
                    std::bind(&LocalSetRemoteGet::GetDataAfterTtl, this, clientIndex, i), timer);
            }
            std::this_thread::sleep_for(std::chrono::microseconds(random_.GetRandomUint32(minSleepUs_, maxSleepUs_)));
        }
    }

    void GetDataAfterTtl(uint32_t clientIndex, uint32_t objectIndex)
    {
        std::string value;
        auto status = clients_[clientIndex]->Get(clientData_[clientIndex][objectIndex].key, value);
        EXPECT_NE(status, Status::OK());
        if (status.IsError()) {
            failedNum_.fetch_add(1);
        } else {
            succeedNum_.fetch_add(1);
        }
    }

    BaseCluster *cluster_;
    RandomData random_;
    std::vector<std::shared_ptr<KVClient>> clients_;
    std::vector<std::vector<Data>> clientData_;
    uint32_t objectNum_{ 0 };
    uint32_t maxTtlSecond_{ 0 };
    uint32_t minSleepUs_{ 0 };
    uint32_t maxSleepUs_{ 0 };
    std::atomic<uint32_t> failedNum_{ 0 };
    std::atomic<uint32_t> succeedNum_{ 0 };
    const uint32_t TIME_UNIT_CONVERSION = 1000;
    const uint32_t MAX_DELAY_DELETE_MS = 1000;
};

TEST_F(KVCacheTtlTest, MultiClientTest)
{
    FLAGS_v = 0;
    std::unique_ptr<LocalSetRemoteGet> localSetRemoteGet = std::make_unique<LocalSetRemoteGet>(cluster_.get());
    localSetRemoteGet->Init(1000, 1, 10, 1, 1);
    localSetRemoteGet->Run();
}

class CentralizedKVCacheTtlTest : public KVCacheTtlTest{
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        FLAGS_v = 1;
        opts.numOBS = 1;
        opts.numWorkers = 3;
        opts.numEtcd = 1;
        opts.workerGflagParams = "-node_timeout_s=5 -shared_memory_size_mb=2048 -v=2";
        opts.enableDistributedMaster = "false";
    }
 
};
 
TEST_F(CentralizedKVCacheTtlTest, WorkerDead)
{
    std::string key = "k1";
    std::string value = "hello";
    DS_ASSERT_OK(client2->Set(key, value, { .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE, .ttlSecond = 5 }));
    std::string outValue;
    EXPECT_EQ(client1->Get(key, outValue), Status::OK());
    EXPECT_EQ(value, outValue);
 
    // Shutdown worker.
    // Master notify worker delete, worker1 may rpc unavailable, make the real delete time > 5s
    cluster_->ShutdownNode(ClusterNodeType::WORKER, 1);
    std::this_thread::sleep_for(std::chrono::seconds(10));
    EXPECT_NE(client1->Get(key, value), Status::OK());
    EXPECT_NE(client3->Get(key, value), Status::OK());
}

class KVClientL2CacheTest : public KVCacheTtlTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        const uint32_t numWorkers = 3;
        opts.numEtcd = 1;
        opts.numWorkers = numWorkers;
        opts.numOBS = 1;
        opts.workerGflagParams = " -shared_memory_size_mb=100 ";
    }
};

TEST_F(KVClientL2CacheTest, TestWriteBackGetAfterDel)
{
    int32_t timeout = 3000;
    std::shared_ptr<KVClient> client0;
    std::shared_ptr<KVClient> client1;
    InitTestKVClient(0, client0, timeout);
    InitTestKVClient(1, client1, timeout);
    SetParam param{ .writeMode = WriteMode::WRITE_BACK_L2_CACHE };
    auto val = "aaaaaaaaa";
    auto key = client0->Set(val, param);
    sleep(1);
    ASSERT_TRUE(!key.empty());
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "AsyncMetaOpToEtcdStorageHandler", "sleep(1000)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "global_cache_delete.delete_objects", "sleep(3000)"));
    sleep(1);
    DS_ASSERT_OK(client0->Del(key));
    std::string outVal;
    DS_ASSERT_NOT_OK(client0->Get(key, outVal));
}
}  // namespace st
}  // namespace datasystem