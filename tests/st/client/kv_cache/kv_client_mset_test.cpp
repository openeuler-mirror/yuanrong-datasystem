/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2023. All rights reserved.
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
 * Description: state cache cross AZ test
 */

#include "datasystem/common/flags/flags.h"
#include <gtest/gtest.h>
#include <unistd.h>
#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <thread>

#include "common.h"
#include "datasystem/common/iam/tenant_auth_manager.h"
#include "datasystem/common/kvstore/etcd/etcd_store.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/memory.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/random_data.h"
#include "datasystem/common/util/rpc_util.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/thread_pool.h"
#include "client/object_cache/oc_client_common.h"
#include "datasystem/object/object_enum.h"
#include "datasystem/kv_client.h"
#include "datasystem/utils/status.h"

DS_DECLARE_string(etcd_address);
namespace datasystem {
namespace st {

constexpr size_t DEFAULT_WORKER_NUM = 3;

class KVClientMSetTest : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.waitWorkerReady = false;
        opts.numEtcd = 1;
        opts.numOBS = 1;
        opts.numWorkers = DEFAULT_WORKER_NUM;
        opts.enableDistributedMaster = "false";
        opts.workerGflagParams = "-shared_memory_size_mb=3000 -v=2 ";
    }

    void GenerateKeyValues(std::vector<std::string> &keys, std::vector<std::string> &values, int num, int valSize)
    {
        auto keySize = 20;
        for (int i = 0; i < num; i++) {
            keys.emplace_back(randomData_.GetRandomString(keySize));
            values.emplace_back(randomData_.GetRandomString(valSize));
        }
    }

    void GenerateValues(std::vector<std::string> &values, int num, int valSize)
    {
        for (int i = 0; i < num; i++) {
            values.emplace_back(randomData_.GetRandomString(valSize));
        }
    }

    void SetUp() override
    {
        CommonTest::SetUp();
        DS_ASSERT_OK(Init());
        ASSERT_TRUE(cluster_ != nullptr);
        DS_ASSERT_OK(cluster_->StartEtcdCluster());
        DS_ASSERT_OK(cluster_->StartOBS());
        DS_ASSERT_OK(cluster_->StartWorkers());
        for (size_t i = 0; i < DEFAULT_WORKER_NUM; i++) {
            DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, i));
        }
        InitTestKVClient(0, client0_, 20000);  // Init client0 to worker 0 with 20000ms timeout
        InitTestKVClient(1, client1_, 20000);  // Init client1 to worker 1 with 20000ms timeout
        InitTestKVClient(2, client2_, 20000);  // Init client2 to worker 2 with 20000ms timeout
    }

    void TearDown() override
    {
        client0_.reset();
        client1_.reset();
        client2_.reset();
        ExternalClusterTest::TearDown();
    }

protected:
    std::shared_ptr<KVClient> client0_, client1_, client2_;
};

class KVClientMSetPerfTest : public KVClientMSetTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.waitWorkerReady = false;
        opts.numEtcd = 1;
        opts.numOBS = 1;
        opts.numWorkers = DEFAULT_WORKER_NUM;
        opts.enableDistributedMaster = "false";
        opts.workerGflagParams = "-shared_memory_size_mb=3000 -v=0";
    }

    void SetUp() override
    {
        CommonTest::SetUp();
        DS_ASSERT_OK(Init());
        ASSERT_TRUE(cluster_ != nullptr);
        DS_ASSERT_OK(cluster_->StartEtcdCluster());
        DS_ASSERT_OK(cluster_->StartOBS());
        DS_ASSERT_OK(cluster_->StartWorkers());
        for (size_t i = 0; i < DEFAULT_WORKER_NUM; i++) {
            DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, i));
        }
        InitTestKVClient(0, client0_, 20000);  // Init client0 to worker 0 with 20000ms timeout
        InitTestKVClient(1, client1_, 20000);  // Init client1 to worker 1 with 20000ms timeout
        InitTestKVClient(2, client2_, 20000);  // Init client2 to worker 2 with 20000ms timeout
    }

    void TearDown() override
    {
        client0_.reset();
        client1_.reset();
        client2_.reset();
        ExternalClusterTest::TearDown();
    }

protected:
    std::shared_ptr<KVClient> client0_, client1_, client2_;
};

TEST_F(KVClientMSetPerfTest, MsetNtxSmallObj)
{
    MSetParam param;
    param.existence = ExistenceOpt::NX;
    std::vector<std::string> sizeNames{ "2KB", "64KB", "128KB" };
    std::vector<size_t> dataSizes{ 2048, 64 * 1024, 128 * 1024 }, maxElementSizes{ 64, 256, 1024 };
    auto repeateNum = 3u;
    std::vector<std::string> res, failedKeys;
    for (auto i = 0u; i < dataSizes.size(); i++) {
        for (auto k = 0ul; k < maxElementSizes.size(); k++) {
            std::vector<uint64_t> costs;
            for (auto n = 0u; n < repeateNum; n++) {
                std::vector<std::string> keys, vals;
                std::vector<StringView> values;
                auto dataSize = dataSizes[i];
                GenerateKeyValues(keys, vals, maxElementSizes[k], dataSize);
                for (const auto &val : vals) {
                    values.emplace_back(val);
                }
                std::vector<std::string> failedKeys;
                Timer t;
                DS_ASSERT_OK(client2_->MSet(keys, values, failedKeys, param));
                costs.emplace_back(t.ElapsedMicroSecond());
                DS_ASSERT_OK(client2_->Del(keys, failedKeys));
            }
            res.emplace_back(FormatString("data_size:%s, key_num:%ld, repeate_num:%ld ---------> avg cost: %d us ",
                                          sizeNames[i], maxElementSizes[k], repeateNum,
                                          std::accumulate(costs.begin(), costs.end(), 0) / costs.size()));
        }
    }
    LOG(INFO) << "--------------------------TEST RESULT--------------------------";
    for (auto item : res) {
        LOG(INFO) << item;
    }
    LOG(INFO) << "--------------------------    END     --------------------------";
}

TEST_F(KVClientMSetTest, CheckPrameterValidation)
{
    MSetParam param;
    param.existence = ExistenceOpt::NONE;
    std::vector<std::string> keys;
    std::vector<StringView> values;
    Status ret = client0_->MSetTx(keys, values, param);
    std::string str = ret.ToString();
    ASSERT_TRUE(str.find("The MSetTx only supports set not existence key now") != std::string::npos);

    param.existence = ExistenceOpt::NX;
    ret = client0_->MSetTx(keys, values, param);
    str = ret.ToString();
    ASSERT_TRUE(str.find("The keys should not be empty") != std::string::npos);

    size_t maxElementSize = 8;
    keys = std::vector<std::string>(maxElementSize + 1, "");
    ret = client0_->MSetTx(keys, values, param);
    str = ret.ToString();
    ASSERT_TRUE(str.find("The maximum size of keys in single operation is") != std::string::npos);

    keys = std::vector<std::string>(maxElementSize, "");
    ret = client0_->MSetTx(keys, values, param);
    str = ret.ToString();
    ASSERT_TRUE(str.find("The number of key and value is not the same") != std::string::npos);

    values = std::vector<StringView>(maxElementSize, "test");
    ret = client0_->MSetTx(keys, values, param);
    str = ret.ToString();
    ASSERT_TRUE(str.find("The key should not be empty") != std::string::npos);

    keys = std::vector<std::string>(maxElementSize, "key");
    ret = client0_->MSetTx(keys, values, param);
    str = ret.ToString();
    ASSERT_TRUE(str.find("The input parameter contains duplicate key") != std::string::npos);

    for (size_t i = 0; i < maxElementSize; ++i) {
        keys[i] = "key" + std::to_string(i);
    }
    DS_ASSERT_OK(client0_->MSetTx(keys, values, param));

    ret = client0_->MSetTx(keys, values, param);
    str = ret.ToString();
    ASSERT_TRUE(str.find("already exists in local worker, traceId") != std::string::npos);
}

TEST_F(KVClientMSetTest, MsetBigValAndSmallVal)
{
    MSetParam param;
    std::vector<std::string> keys;
    std::vector<StringView> values;
    param.existence = ExistenceOpt::NX;
    size_t maxElementSize = 8;
    int bigValSize = 1000 * 1024l;
    std::vector<std::string> vals;
    int val = 300;
    int randSet = 2;
    for (size_t i = 0; i < maxElementSize; ++i) {
        if (i % randSet == 0) {
            vals.emplace_back(randomData_.GetRandomString(bigValSize));
        } else {
            vals.emplace_back(randomData_.GetRandomString(val));
        }
    }
    for (size_t i = 0; i < maxElementSize; ++i) {
        auto key = "a_key_for_mset_" + std::to_string(i);
        keys.emplace_back(key);
        values.emplace_back(vals[i]);
    }
    DS_ASSERT_OK(client0_->MSetTx(keys, values, param));
    int i = 0;
    for (const auto &key : keys) {
        std::string val, val1;
        auto status = client0_->Get(key, val);
        DS_ASSERT_OK(client1_->Get(key, val1));
        ASSERT_EQ(val, values[i].data());
        ASSERT_EQ(val1, values[i].data());
        DS_ASSERT_OK(client0_->Set(key, "mmm"));
        DS_ASSERT_OK(client0_->Get(key, val));
        ASSERT_EQ(val, "mmm");
        DS_ASSERT_OK(client1_->Del(key));
        val.clear();
        DS_ASSERT_NOT_OK(client0_->Get(key, val));
        i++;
    }
}

TEST_F(KVClientMSetTest, MSetTest)
{
    MSetParam param;
    std::vector<std::string> keys, keys1;
    std::vector<StringView> values, values1, values2;
    param.existence = ExistenceOpt::NX;
    size_t maxElementSize = 8;
    auto dataSize = 1000 * 1024;
    auto dataSize1 = 20;
    std::vector<std::string> vals, vals1, vals2;
    GenerateKeyValues(keys, vals, maxElementSize, dataSize1);
    GenerateKeyValues(keys1, vals1, maxElementSize, dataSize);
    GenerateValues(vals2, maxElementSize, dataSize1);
    for (const auto &val : vals) {
        values.emplace_back(val);
    }

    for (const auto &val : vals1) {
        values1.emplace_back(val);
    }

    for (const auto &val : vals2) {
        values2.emplace_back(val);
    }

    DS_ASSERT_OK(client0_->MSetTx(keys, values, param));
    for (size_t i = 0; i < maxElementSize; i++) {
        std::string val;
        DS_ASSERT_OK(client2_->Get(keys[i], val));
        ASSERT_EQ(val, vals[i]);
        DS_ASSERT_OK(client0_->Set(keys[i], vals2[i]));
        DS_ASSERT_OK(client0_->Get(keys[i], val));
        ASSERT_EQ(val, vals2[i]);
        DS_ASSERT_OK(client1_->Del(keys[i]));
        val.clear();
        DS_ASSERT_NOT_OK(client0_->Get(keys[i], val));
    }

    DS_ASSERT_OK(client0_->MSetTx(keys1, values1, param));
    int i = 0;
    for (const auto &key : keys1) {
        std::string val, val1;
        DS_ASSERT_OK(client0_->Get(key, val));
        DS_ASSERT_OK(client2_->Get(key, val1));
        ASSERT_EQ(val, vals1[i]);
        ASSERT_EQ(val1, vals1[i]);
        DS_ASSERT_OK(client0_->Set(key, vals2[i]));
        DS_ASSERT_OK(client0_->Get(key, val));
        ASSERT_EQ(val, vals2[i]);
        DS_ASSERT_OK(client1_->Del(key));
        val.clear();
        DS_ASSERT_NOT_OK(client0_->Get(key, val));
        i++;
    }
}

TEST_F(KVClientMSetTest, MsetSubscribe)
{
    MSetParam param;
    std::vector<std::string> keys, vals;
    std::vector<StringView> values;
    param.existence = ExistenceOpt::NX;
    size_t maxElementSize = 8;
    auto lenKey = 20;
    GenerateKeyValues(keys, vals, maxElementSize, lenKey);
    for (const auto &val : vals) {
        values.emplace_back(val);
    }
    std::thread thread1([&] {
        std::vector<std::string> vals1;
        int timeout = 10000;
        DS_ASSERT_OK(client1_->Get(keys, vals1, timeout));
        ASSERT_EQ(vals1.size(), keys.size());
        for (size_t i = 0; i < vals1.size(); i++) {
            ASSERT_EQ(vals1[i], vals[i]);
        }
    });

    std::thread thread2([&] {
        std::vector<std::string> vals1;
        int timeout = 10000;
        DS_ASSERT_OK(client2_->Get(keys, vals1, timeout));
        ASSERT_EQ(vals1.size(), keys.size());
        for (size_t i = 0; i < vals1.size(); i++) {
            ASSERT_EQ(vals1[i], vals[i]);
        }
    });
    int waitTime = 2;
    std::this_thread::sleep_for(std::chrono::seconds(waitTime));
    DS_ASSERT_OK(client1_->MSetTx(keys, values, param));
    thread1.join();
    thread2.join();
}

TEST_F(KVClientMSetTest, LEVEL1_MsetTtl)
{
    MSetParam param;
    std::vector<std::string> keys, vals;
    std::vector<StringView> values;
    param.existence = ExistenceOpt::NX;
    uint32_t ttl = 3;
    param.ttlSecond = ttl;
    size_t maxElementSize = 3;
    auto dataSize = 30;
    GenerateKeyValues(keys, vals, maxElementSize, dataSize);
    for (const auto &val : vals) {
        values.emplace_back(val);
    }
    DS_ASSERT_OK(client1_->MSetTx(keys, values, param));
    for (size_t i = 0; i < maxElementSize; i++) {
        std::string val;
        DS_ASSERT_OK(client1_->Get(keys[i], val));
        ASSERT_EQ(val, vals[i]);
    }
    int waitTime = 5;
    std::this_thread::sleep_for(std::chrono::seconds(waitTime));
    for (const auto &key : keys) {
        std::string val;
        DS_ASSERT_NOT_OK(client1_->Get(key, val));
    }
}

TEST_F(KVClientMSetTest, LEVEL1_MsetRestartWorker)
{
    MSetParam param;
    std::vector<std::string> keys, vals;
    std::vector<StringView> values;
    param.existence = ExistenceOpt::NX;
    param.writeMode = WriteMode::WRITE_THROUGH_L2_CACHE;
    size_t maxElementSize = 3;
    auto dataSize = 30;
    GenerateKeyValues(keys, vals, maxElementSize, dataSize);
    for (const auto &val : vals) {
        values.emplace_back(val);
    }
    DS_ASSERT_OK(client1_->MSetTx(keys, values, param));
    for (size_t i = 0; i < maxElementSize; i++) {
        std::string val;
        DS_ASSERT_OK(client1_->Get(keys[i], val));
        ASSERT_EQ(val, vals[i]);
    }
    DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, 1));
    DS_ASSERT_OK(cluster_->StartNode(WORKER, 1, ""));
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 1));
    for (size_t i = 0; i < maxElementSize; i++) {
        std::string val;
        DS_ASSERT_OK(client1_->Get(keys[i], val));
        ASSERT_EQ(val, vals[i]);
    }
}

TEST_F(KVClientMSetTest, DISABLED_MSetAndAsyncGet)
{
    MSetParam param;
    std::vector<std::string> keys, vals;
    std::vector<StringView> values;
    param.existence = ExistenceOpt::NX;
    param.writeMode = WriteMode::WRITE_THROUGH_L2_CACHE;
    size_t maxElementSize = 3;

    auto dataSize = 30;
    GenerateKeyValues(keys, vals, maxElementSize, dataSize);
    for (const auto &val : vals) {
        values.emplace_back(val);
    }
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "OCMetadataManager.createMultiMeta.delay", "sleep(10000)"));
    std::thread thread3([&] { DS_ASSERT_OK(client1_->MSetTx(keys, values, param)); });

    std::thread thread([&] {
        std::vector<std::string> vals;
        auto status = client2_->Get(keys, vals);
        LOG(INFO) << status.ToString();
        DS_ASSERT_NOT_OK(status);
    });

    std::thread thread1([&] {
        std::vector<std::string> vals;
        int timeoutMs = 15000;
        DS_ASSERT_OK(client2_->Get(keys, vals, timeoutMs));
        ASSERT_EQ(vals.size(), keys.size());
        for (size_t i = 0; i < vals.size(); i++) {
            ASSERT_EQ(vals[i], vals[i]);
        }
    });

    thread.join();
    thread1.join();
    thread3.join();
}

TEST_F(KVClientMSetTest, MSetWithKeyExitst)
{
    MSetParam param;
    std::vector<std::string> keys, keys1, vals, vals1;
    std::vector<StringView> values, values1;
    param.existence = ExistenceOpt::NX;
    param.writeMode = WriteMode::WRITE_THROUGH_L2_CACHE;
    size_t maxElementSize = 8;
    auto dataSize = 1000 * 1024;
    auto dataSize1 = 40;
    GenerateKeyValues(keys, vals, maxElementSize, dataSize1);
    for (const auto &val : vals) {
        values.emplace_back(val);
    }
    GenerateKeyValues(keys1, vals1, maxElementSize, dataSize);
    for (const auto &val : vals1) {
        values1.emplace_back(val);
    }
    auto lastKey = keys.back();
    DS_ASSERT_OK(client0_->Set(lastKey, values.back()));
    DS_ASSERT_NOT_OK(client1_->MSetTx(keys, values, param));
    std::vector<std::string> valsGet;
    std::string val;
    DS_ASSERT_OK(client0_->Get(keys.back(), val));
    keys.pop_back();
    DS_ASSERT_NOT_OK(client2_->Get(keys, valsGet));
    DS_ASSERT_NOT_OK(client1_->Get(keys, valsGet));
    val.clear();
    DS_ASSERT_OK(client1_->Get(lastKey, val));
    ASSERT_EQ(val, vals.back());
    auto lastKey1 = keys1.back();
    DS_ASSERT_OK(client0_->Set(lastKey1, values1.back()));
    DS_ASSERT_NOT_OK(client1_->MSetTx(keys1, values1, param));
    std::vector<std::string> vals1Get;
    std::string val1;
    DS_ASSERT_OK(client0_->Get(keys1.back(), val1));
    keys1.pop_back();
    DS_ASSERT_NOT_OK(client2_->Get(keys1, vals1Get));
    DS_ASSERT_NOT_OK(client1_->Get(keys1, vals1Get));
    val1.clear();
    DS_ASSERT_OK(client1_->Get(lastKey1, val1));
    ASSERT_EQ(val1, vals1.back());
}

TEST_F(KVClientMSetTest, DISABLED_MSetAndAsyncDel)
{
    MSetParam param;
    std::vector<std::string> keys, vals;
    std::vector<StringView> values;
    param.existence = ExistenceOpt::NX;
    param.writeMode = WriteMode::WRITE_THROUGH_L2_CACHE;
    size_t maxElementSize = 5;
    std::shared_ptr<KVClient> client1;
    int workerIndex = 2;
    int timeoutMs = 2000;
    InitTestKVClient(workerIndex, client1, timeoutMs);
    auto dataSize1 = 40;
    GenerateKeyValues(keys, vals, maxElementSize, dataSize1);
    for (const auto &val : vals) {
        values.emplace_back(val);
    }
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "OCMetadataManager.createMultiMeta.delay", "sleep(8000)"));
    std::thread thread3([&] {
        int waitTime = 2;
        std::this_thread::sleep_for(std::chrono::seconds(waitTime));
        for (size_t i = 0; i < maxElementSize; i++) {
            DS_ASSERT_OK(client2_->Del(keys[i]));
        }
        auto timeoutMs = 20000;
        for (size_t i = 0; i < maxElementSize; i++) {
            std::string valGet;
            DS_ASSERT_OK(client2_->Get(keys[i], valGet, timeoutMs));
            ASSERT_EQ(valGet, vals[i]);
        }
        std::vector<std::string> failedKeys;
        DS_ASSERT_OK(client0_->Del(keys, failedKeys));
        for (const auto &key : keys) {
            std::string valGet;
            DS_ASSERT_NOT_OK(client1_->Get(key, valGet));
            DS_ASSERT_NOT_OK(client2_->Get(key, valGet));
        }
    });

    std::thread thread1([&] { DS_ASSERT_OK(client1_->MSetTx(keys, values, param)); });

    thread1.join();
    thread3.join();
}

TEST_F(KVClientMSetTest, DISABLED_LEVEL1_MsetNtxRestartWorker)
{
    MSetParam param;
    std::vector<std::string> keys, vals;
    std::vector<StringView> values;
    param.existence = ExistenceOpt::NONE;
    param.writeMode = WriteMode::WRITE_THROUGH_L2_CACHE;
    size_t maxElementSize = 20;
    auto dataSize = 30;
    GenerateKeyValues(keys, vals, maxElementSize, dataSize);
    for (const auto &val : vals) {
        values.emplace_back(val);
    }
    std::vector<std::string> failedKeys;
    DS_ASSERT_OK(client1_->MSet(keys, values, failedKeys, param));
    for (size_t i = 0; i < maxElementSize; i++) {
        std::string val;
        DS_ASSERT_OK(client1_->Get(keys[i], val));
        ASSERT_EQ(val, vals[i]);
    }
    DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, 1));
    DS_ASSERT_OK(cluster_->StartNode(WORKER, 1, ""));
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 1));
    for (size_t i = 0; i < maxElementSize; i++) {
        std::string val;
        DS_ASSERT_OK(client1_->Get(keys[i], val));
        ASSERT_EQ(val, vals[i]);
    }
}

TEST_F(KVClientMSetTest, MsetNtxTtl)
{
    MSetParam param;
    std::vector<std::string> keys, vals;
    std::vector<StringView> values;
    param.existence = ExistenceOpt::NONE;
    param.ttlSecond = 3;  // ttl second is 3s
    size_t maxElementSize = 8;
    auto dataSize = 30;
    GenerateKeyValues(keys, vals, maxElementSize, dataSize);
    for (const auto &val : vals) {
        values.emplace_back(val);
    }
    std::vector<std::string> failedKeys;
    DS_ASSERT_OK(client1_->MSet(keys, values, failedKeys, param));
    for (size_t i = 0; i < maxElementSize; i++) {
        std::string val;
        DS_ASSERT_OK(client1_->Get(keys[i], val));
        ASSERT_EQ(val, vals[i]);
    }
    auto waitTime = 5;
    std::this_thread::sleep_for(std::chrono::seconds(waitTime));
    for (size_t i = 0; i < maxElementSize; i++) {
        std::string val;
        DS_ASSERT_NOT_OK(client2_->Get(keys[i], val));
    }
}

TEST_F(KVClientMSetTest, MsetNtxExistenceNx)
{
    MSetParam param;
    std::vector<std::string> keys, vals;
    std::vector<StringView> values;
    param.existence = ExistenceOpt::NX;
    size_t maxElementSize = 20;
    auto dataSize = 30;
    GenerateKeyValues(keys, vals, maxElementSize, dataSize);
    for (const auto &val : vals) {
        values.emplace_back(val);
    }
    std::vector<std::string> failedKeys;
    size_t index = 3, index1 = 5;
    DS_ASSERT_OK(client2_->Set(keys[index], "qqq"));
    DS_ASSERT_OK(client2_->Set(keys[index1], "qqq"));
    DS_ASSERT_OK(client2_->MSet(keys, values, failedKeys, param));
    size_t failedSize = 2;
    ASSERT_EQ(failedKeys.size(), failedSize);
    for (size_t i = 0; i < maxElementSize; i++) {
        std::string val;
        DS_ASSERT_OK(client1_->Get(keys[i], val));
        if (i == index || i == index1) {
            ASSERT_EQ(val, "qqq");
        } else {
            ASSERT_EQ(val, vals[i]);
        }
    }
}

TEST_F(KVClientMSetTest, MsetNtxBigObj)
{
    MSetParam param;
    std::vector<std::string> keys, vals;
    std::vector<StringView> values;
    param.existence = ExistenceOpt::NX;
    size_t maxElementSize = 20;
    auto dataSize = 3000000;
    GenerateKeyValues(keys, vals, maxElementSize, dataSize);
    for (const auto &val : vals) {
        values.emplace_back(val);
    }
    std::vector<std::string> failedKeys;
    DS_ASSERT_OK(client2_->MSet(keys, values, failedKeys, param));
    for (size_t i = 0; i < maxElementSize; i++) {
        std::string val;
        DS_ASSERT_OK(client1_->Get(keys[i], val));
        ASSERT_EQ(val, vals[i]);
    }
}

TEST_F(KVClientMSetTest, LEVEL1_SetAndAsyncMset)
{
    MSetParam param;
    std::vector<std::string> keys;
    std::vector<StringView> values;
    param.existence = ExistenceOpt::NX;
    param.writeMode = WriteMode::WRITE_THROUGH_L2_CACHE;
    size_t maxElementSize = 8;
    std::shared_ptr<KVClient> client1;
    int workerIndex = 2;
    int timeoutMs = 2000;
    InitTestKVClient(workerIndex, client1, timeoutMs);
    for (size_t i = 0; i < maxElementSize; ++i) {
        auto key = "a_key_for_mset_" + std::to_string(i);
        keys.emplace_back(key);
        values.emplace_back("ppp");
    }
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "OCMetadataManager.createMultiMeta.delay", "sleep(15000)"));
    std::thread thread3([&] { DS_ASSERT_OK(client1_->MSetTx(keys, values, param)); });
    int waitTime = 2;
    SetParam param1 = { .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE };
    std::thread thread([&] {
        std::this_thread::sleep_for(std::chrono::seconds(waitTime));
        auto status = client1->Set(keys.back(), "oooo", param1);
        LOG(INFO) << status.ToString();
        DS_ASSERT_NOT_OK(status);
    });

    std::thread thread1([&] {
        std::this_thread::sleep_for(std::chrono::seconds(waitTime));
        auto status = client2_->Set(keys.back(), "oooo", param1);
        LOG(INFO) << status.ToString();
        DS_ASSERT_OK(status);
    });

    thread.join();
    thread1.join();
    thread3.join();
    std::string val;
    DS_ASSERT_OK(client0_->Get(keys.back(), val));
    ASSERT_EQ(val, "oooo");
}

TEST_F(KVClientMSetTest, MSetAndAsyncMset)
{
    MSetParam param;
    std::vector<std::string> keys, keys1, addKeys, vals, vals1;
    std::vector<StringView> values, values1;
    param.existence = ExistenceOpt::NX;
    param.writeMode = WriteMode::WRITE_THROUGH_L2_CACHE;
    size_t maxElementSize = 5;
    std::shared_ptr<KVClient> client1;
    int workerIndex = 2;
    int timeoutMs = 2000;
    auto dataSize1 = 40;
    int addNum = 2;
    GenerateKeyValues(keys, vals, maxElementSize, dataSize1);
    for (int i = 0; i < addNum; i++) {
        auto key = randomData_.GetRandomString(dataSize1);
        keys1.emplace_back(key);
        addKeys.emplace_back(key);
    }
    for (const auto &key : keys) {
        keys1.emplace_back(key);
    }
    GenerateValues(vals1, maxElementSize + addNum, dataSize1);
    InitTestKVClient(workerIndex, client1, timeoutMs);
    for (const auto &val : vals) {
        values.emplace_back(val);
    }

    for (const auto &val : vals1) {
        values1.emplace_back(val);
    }

    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "OCMetadataManager.createMultiMeta.delay", "sleep(15000)"));
    std::thread thread3([&] { DS_ASSERT_OK(client1_->MSetTx(keys, values, param)); });
    int waitTime = 2;
    std::thread thread([&] {
        std::this_thread::sleep_for(std::chrono::seconds(waitTime));
        DS_ASSERT_NOT_OK(client2_->MSetTx(keys1, values1, param));
    });

    thread.join();
    thread3.join();
    for (size_t i = 0; i < keys.size(); i++) {
        std::string valsGet;
        DS_ASSERT_OK(client0_->Get(keys[i], valsGet));
        ASSERT_EQ(valsGet, vals[i]);
    }
    for (const auto &key : addKeys) {
        std::string valsGet;
        DS_ASSERT_NOT_OK(client0_->Get(key, valsGet));
    }
}

TEST_F(KVClientMSetTest, MSetAndAsyncMsetTest)
{
    MSetParam param;
    std::vector<std::string> keys, keys1, addKeys, vals, vals1;
    std::vector<StringView> values, values1;
    param.existence = ExistenceOpt::NX;
    size_t maxElementSize = 5;
    std::shared_ptr<KVClient> client1;
    int workerIndex = 2, addNum = 2, waitTime = 2;
    int timeoutMs = 2000;
    auto dataSize1 = 40;
    GenerateKeyValues(keys, vals, maxElementSize, dataSize1);
    for (int i = 0; i < addNum; i++) {
        auto key = randomData_.GetRandomString(dataSize1);
        keys1.emplace_back(key);
        addKeys.emplace_back(key);
    }
    std::for_each(keys.begin(), keys.end(), [&](const std::string &key) { keys1.emplace_back(key); });
    GenerateValues(vals1, maxElementSize + addNum, dataSize1);
    InitTestKVClient(workerIndex, client1, timeoutMs);
    std::for_each(vals.begin(), vals.end(), [&](const std::string &val) { values.emplace_back(val); });
    std::for_each(vals1.begin(), vals1.end(), [&](const std::string &val) { values1.emplace_back(val); });

    InitTestKVClient(workerIndex, client1, timeoutMs);
    auto key = keys.back();
    DS_ASSERT_OK(client0_->Set(key, values.back()));
    keys.pop_back();
    values.pop_back();
    std::string call = "5*call(" + key + ")";
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "OCMetadataManager.multiSetDelay", call));
    std::thread thread3([&] { DS_ASSERT_NOT_OK(client1_->MSetTx(keys1, values1, param)); });
    std::this_thread::sleep_for(std::chrono::seconds(waitTime));
    std::thread thread([&] { DS_ASSERT_OK(client0_->MSetTx(keys, values, param)); });

    std::thread thread2([&] { DS_ASSERT_NOT_OK(client1->MSetTx(keys, values, param)); });

    thread.join();
    thread3.join();
    thread2.join();
    std::vector<std::string> valsGet;
    for (size_t i = 0; i < keys.size(); i++) {
        std::string valsGet;
        DS_ASSERT_OK(client0_->Get(keys[i], valsGet));
        ASSERT_EQ(valsGet, vals[i]);
    }
    std::string valGet1;
    DS_ASSERT_OK(client2_->Get(key, valGet1));
    ASSERT_EQ(valGet1, vals.back());
}

TEST_F(KVClientMSetTest, DISABLED_MsetAndAsyncMsetNtx)
{
    MSetParam param, param1;
    std::vector<std::string> keys, vals, vals1;
    std::vector<StringView> values, values1;
    param.existence = ExistenceOpt::NX;
    param1.existence = ExistenceOpt::NONE;
    size_t maxElementSize = 8;
    auto dataSize = 30;
    int timeoutMs = 3000;
    std::shared_ptr<KVClient> client1;
    GenerateKeyValues(keys, vals, maxElementSize, dataSize);
    GenerateValues(vals1, maxElementSize, dataSize);
    for (const auto &val : vals) {
        values.emplace_back(val);
    }
    for (const auto &val : vals1) {
        values1.emplace_back(val);
    }
    auto workerIndex = 0;
    InitTestKVClient(workerIndex, client1, timeoutMs);
    std::thread thread2([&] {
        DS_ASSERT_OK(
            cluster_->SetInjectAction(WORKER, workerIndex, "OCMetadataManager.createMultiMeta.delay", "sleep(10000)"));
        DS_ASSERT_OK(client1_->MSetTx(keys, values, param));
    });
    auto waitTime = 2;
    std::thread thread([&] {
        std::vector<std::string> failedKeys;
        std::this_thread::sleep_for(std::chrono::seconds(waitTime));
        DS_ASSERT_NOT_OK(client1->MSet(keys, values1, failedKeys, param1));
    });
    thread.join();
    thread2.join();
    for (size_t i = 0; i < keys.size(); i++) {
        std::string valsGet;
        DS_ASSERT_OK(client2_->Get(keys[i], valsGet));
        ASSERT_EQ(valsGet, vals[i]);
    }
    std::vector<std::string> failedKeys;
    DS_ASSERT_OK(client1->MSet(keys, values1, failedKeys, param1));
    size_t failedSize = 0;
    ASSERT_EQ(failedKeys.size(), failedSize);
    for (size_t i = 0; i < keys.size(); i++) {
        std::string valsGet;
        DS_ASSERT_OK(client0_->Get(keys[i], valsGet));
        ASSERT_EQ(valsGet, vals1[i]);
    }
}

TEST_F(KVClientMSetTest, MsetTxAfterMsetNtx)
{
    MSetParam param, param1;
    std::vector<std::string> keys, vals, vals1;
    std::vector<StringView> values, values1;
    param.existence = ExistenceOpt::NX;
    param1.existence = ExistenceOpt::NONE;
    size_t maxElementSize = 8;
    auto dataSize = 30;
    GenerateKeyValues(keys, vals, maxElementSize, dataSize);
    GenerateValues(vals1, maxElementSize, dataSize);
    for (const auto &val : vals) {
        values.emplace_back(val);
    }
    for (const auto &val : vals1) {
        values1.emplace_back(val);
    }
    std::vector<std::string> failedKeys;
    DS_ASSERT_OK(client1_->MSet(keys, values1, failedKeys, param1));
    DS_ASSERT_NOT_OK(client1_->MSetTx(keys, values, param));
    for (size_t i = 0; i < keys.size(); i++) {
        std::string valsGet;
        DS_ASSERT_OK(client1_->Get(keys[i], valsGet));
        ASSERT_EQ(valsGet, vals1[i]);
    }
}

TEST_F(KVClientMSetTest, TestMsetNtxNotUpdate)
{
    MSetParam param;
    std::vector<std::string> keys, vals;
    std::vector<StringView> values;
    param.existence = ExistenceOpt::NONE;
    size_t maxElementSize = 100;
    std::shared_ptr<KVClient> client1;
    auto dataSize1 = 40;
    size_t failedIdSize = 0;
    GenerateKeyValues(keys, vals, maxElementSize, dataSize1);
    std::for_each(vals.begin(), vals.end(), [&](const std::string &val) { values.emplace_back(val); });
    std::vector<std::string> failedIds;
    DS_ASSERT_OK(client1_->MSet(keys, values, failedIds, param));
    ASSERT_EQ(failedIds.size(), failedIdSize);
    for (size_t i = 0; i < keys.size(); i++) {
        std::string valGet;
        DS_ASSERT_OK(client2_->Get(keys[i], valGet));
        ASSERT_EQ(valGet, vals[i]);
        DS_ASSERT_OK(client0_->Del(keys[i]));
        std::string valGet1, valGet2;
        DS_ASSERT_NOT_OK(client1_->Get(keys[i], valGet2));
        DS_ASSERT_NOT_OK(client2_->Get(keys[i], valGet1));
    }
}

TEST_F(KVClientMSetTest, TestMsetNtxUpdate)
{
    MSetParam param;
    std::vector<std::string> keys, vals;
    std::vector<StringView> values;
    param.existence = ExistenceOpt::NONE;
    size_t maxElementSize = 100;
    std::shared_ptr<KVClient> client1;
    auto dataSize1 = 40;
    GenerateKeyValues(keys, vals, maxElementSize, dataSize1);
    auto step = 3;
    for (size_t i = 0; i < keys.size(); i++) {
        if (i % step == 0) {
            DS_ASSERT_OK(client2_->Set(keys[i], "pppp"));
        }
    }
    std::for_each(vals.begin(), vals.end(), [&](const std::string &val) { values.emplace_back(val); });
    std::vector<std::string> failedIds;
    DS_ASSERT_OK(client1_->MSet(keys, values, failedIds, param));
    size_t failedSize = 0;
    ASSERT_EQ(failedSize, failedIds.size());
    for (size_t i = 0; i < keys.size(); i++) {
        std::string valGet;
        DS_ASSERT_OK(client0_->Get(keys[i], valGet));
        ASSERT_EQ(valGet, vals[i]);
        DS_ASSERT_OK(client0_->Del(keys[i]));
        std::string valGet1, valGet2;
        DS_ASSERT_NOT_OK(client1_->Get(keys[i], valGet));
        DS_ASSERT_NOT_OK(client0_->Get(keys[i], valGet1));
    }
}

TEST_F(KVClientMSetTest, TestMsetNtxDuplicateKey)
{
    MSetParam param;
    std::vector<std::string> keys, vals, vals1;
    std::vector<StringView> values;
    param.existence = ExistenceOpt::NONE;
    size_t maxElementSize = 100;
    std::shared_ptr<KVClient> client1;
    size_t dataSize1 = 40, duplicateKeySize = 5;
    GenerateKeyValues(keys, vals, maxElementSize, dataSize1);
    GenerateValues(vals1, duplicateKeySize, dataSize1);
    std::for_each(vals.begin(), vals.end(), [&](const std::string &val) { values.emplace_back(val); });
    for (size_t i = 0; i < duplicateKeySize; i++) {
        keys.emplace_back(keys[i]);
        values.emplace_back(vals1[i]);
    }

    std::vector<std::string> failedIds;
    DS_ASSERT_OK(client1_->MSet(keys, values, failedIds, param));
    for (size_t i = 0; i < keys.size() - duplicateKeySize; i++) {
        std::string valGet;
        DS_ASSERT_OK(client0_->Get(keys[i], valGet));
        ASSERT_EQ(valGet, vals[i]);
        DS_ASSERT_OK(client2_->Del(keys[i]));
        std::string valGet1, valGet2;
        DS_ASSERT_NOT_OK(client1_->Get(keys[i], valGet));
        DS_ASSERT_NOT_OK(client0_->Get(keys[i], valGet1));
    }
    for (size_t i = 0; i < duplicateKeySize; i++) {
        ASSERT_EQ(failedIds[i], keys[maxElementSize + i]);
    }
}

TEST_F(KVClientMSetTest, TestMsetFailedKeys)
{
    MSetParam param;
    std::vector<std::string> keys, vals;
    std::vector<StringView> values;
    param.existence = ExistenceOpt::NONE;
    size_t maxElementSize = 100;
    std::shared_ptr<KVClient> client1;
    auto dataSize1 = 40;
    size_t failedIdSize = 8;
    auto worker0 = 0, worker1 = 1;
    GenerateKeyValues(keys, vals, maxElementSize, dataSize1);
    DS_ASSERT_OK(
        cluster_->SetInjectAction(WORKER, worker1, "AttachShmUnitToObject.error", "2*return(K_RUNTIME_ERROR)"));
    DS_ASSERT_OK(
        cluster_->SetInjectAction(WORKER, worker1, "SaveBinaryObjectToMemory.error", "3*return(K_RUNTIME_ERROR)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, worker0, "master.create_meta_failure", "3*return(K_RUNTIME_ERROR)"));
    std::for_each(vals.begin(), vals.end(), [&](const std::string &val) { values.emplace_back(val); });
    std::vector<std::string> failedIds;
    DS_ASSERT_OK(client1_->MSet(keys, values, failedIds, param));
    ASSERT_EQ(failedIds.size(), failedIdSize);
    for (size_t i = 0; i < keys.size(); i++) {
        std::string valGet;
        if (std::find(failedIds.begin(), failedIds.end(), keys[i]) != failedIds.end()) {
            DS_ASSERT_NOT_OK(client0_->Get(keys[i], valGet));
            DS_ASSERT_NOT_OK(client1_->Get(keys[i], valGet));
            continue;
        }
        DS_ASSERT_OK(client0_->Get(keys[i], valGet));
        ASSERT_EQ(valGet, vals[i]);
    }
    LOG(INFO) << failedIds.size();
}

TEST_F(KVClientMSetTest, TestMsetFailedKeysLastRc)
{
    MSetParam param;
    std::vector<std::string> keys, vals;
    std::vector<StringView> values;
    param.existence = ExistenceOpt::NONE;
    size_t maxElementSize = 100;
    std::shared_ptr<KVClient> client1;
    auto dataSize1 = 40;
    auto worker0 = 0, worker1 = 1, worekr2 = 2;
    GenerateKeyValues(keys, vals, maxElementSize, dataSize1);
    DS_ASSERT_OK(
        cluster_->SetInjectAction(WORKER, worker1, "AttachShmUnitToObject.error", "1000*return(K_UNKNOWN_ERROR)"));
    DS_ASSERT_OK(
        cluster_->SetInjectAction(WORKER, worekr2, "SaveBinaryObjectToMemory.error", "3000*return(K_RUNTIME_ERROR)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, worker0, "master.create_meta_failure", "3000*return(K_INVALID)"));
    std::for_each(vals.begin(), vals.end(), [&](const std::string &val) { values.emplace_back(val); });
    std::vector<std::string> failedIds;
    ASSERT_EQ(client1_->MSet(keys, values, failedIds, param).GetCode(), K_UNKNOWN_ERROR);
    ASSERT_EQ(failedIds.size(), maxElementSize);
    failedIds.clear();
    ASSERT_EQ(client2_->MSet(keys, values, failedIds, param).GetCode(), K_RUNTIME_ERROR);
    ASSERT_EQ(failedIds.size(), maxElementSize);
    failedIds.clear();
    ASSERT_EQ(client0_->MSet(keys, values, failedIds, param).GetCode(), K_INVALID);
    ASSERT_EQ(failedIds.size(), maxElementSize);
    for (size_t i = 0; i < keys.size(); i++) {
        std::string valGet;
        DS_ASSERT_NOT_OK(client0_->Get(keys[i], valGet));
        DS_ASSERT_NOT_OK(client1_->Get(keys[i], valGet));
    }
}

TEST_F(KVClientMSetTest, MSetNtxInvalidParam)
{
    MSetParam param;
    param.existence = ExistenceOpt::NX;
    std::vector<std::string> keys;
    std::vector<StringView> values;
    std::vector<std::string> outFailedKeys;

    param.existence = ExistenceOpt::NX;
    auto ret = client0_->MSet(keys, values, outFailedKeys, param);
    auto str = ret.ToString();
    ASSERT_TRUE(str.find("The keys should not be empty") != std::string::npos);

    size_t maxElementSize = 2000;
    keys = std::vector<std::string>(maxElementSize, "");
    ret = client0_->MSet(keys, values, outFailedKeys, param);

    maxElementSize = 10;  // batch set keys size is 10.
    keys = std::vector<std::string>(maxElementSize, "");
    ret = client0_->MSet(keys, values, outFailedKeys, param);
    str = ret.ToString();
    ASSERT_TRUE(str.find("The number of key and value is not the same") != std::string::npos);

    values = std::vector<StringView>(maxElementSize, "test");
    ret = client0_->MSet(keys, values, outFailedKeys, param);
    str = ret.ToString();
    ASSERT_TRUE(str.find("The key should not be empty") != std::string::npos);

    for (size_t i = 0; i < maxElementSize; ++i) {
        keys[i] = "key" + std::to_string(i);
    }

    values.pop_back();
    values.emplace_back("test");
    DS_ASSERT_OK(client0_->MSet(keys, values, outFailedKeys, param));
    param.existence = ExistenceOpt::NONE;
    DS_ASSERT_OK(client0_->MSet(keys, values, outFailedKeys, param));
}

TEST_F(KVClientMSetTest, LEVEL1_ConcurrentlyFailedFromMaster)
{
    MSetParam param;
    std::vector<std::string> keys, vals, getValue1, getValue2;
    std::vector<StringView> values;
    param.existence = ExistenceOpt::NX;
    param.writeMode = WriteMode::WRITE_THROUGH_L2_CACHE;
    size_t maxElementSize = 8;
    auto dataSize = 1024;
    GenerateKeyValues(keys, vals, maxElementSize, dataSize);
    for (const auto &val : vals) {
        values.emplace_back(val);
    }

    DS_ASSERT_OK(client1_->MSetTx(keys, values, param));
    DS_ASSERT_OK(client2_->Get(keys, getValue1));
    ASSERT_EQ(vals, getValue1);
    int loop = 100;
    std::thread t1([&]() {
        for (int i = 0; i < loop; ++i) {
            Status ret = client0_->MSetTx(keys, values, param);
            ASSERT_EQ(ret.GetCode(), K_OC_KEY_ALREADY_EXIST);
        }
    });
    std::thread t2([&]() {
        for (int i = 0; i < loop; ++i) {
            Status ret = client0_->MSetTx(keys, values, param);
            ASSERT_EQ(ret.GetCode(), K_OC_KEY_ALREADY_EXIST);
        }
    });
    std::thread t3([&]() {
        for (int i = 0; i < loop; ++i) {
            Status ret = client0_->MSetTx(keys, values, param);
            ASSERT_EQ(ret.GetCode(), K_OC_KEY_ALREADY_EXIST);
        }
    });
    t1.join();
    t2.join();
    t3.join();

    DS_ASSERT_OK(client0_->Get(keys, getValue2));
    ASSERT_EQ(vals, getValue2);

    std::vector<std::string> failedkeys;
    DS_ASSERT_OK(client0_->Del(keys, failedkeys));
}

TEST_F(KVClientMSetTest, MSetDuplicateObj)
{
    MSetParam param;
    std::vector<std::string> keys, keys1;
    std::vector<StringView> values, values1, values2;
    param.existence = ExistenceOpt::NX;
    size_t maxElementSize = 8;
    auto dataSize = 20;
    std::vector<std::string> vals;
    GenerateKeyValues(keys, vals, maxElementSize, dataSize);
    for (const auto &val : vals) {
        values.emplace_back(val);
    }
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "master.CreateMeta.delay", "1*sleep(1000)"));

    Status rc1, rc2;
    std::thread t1([&]() { rc1 = client0_->MSetTx(keys, values, param); });
    std::thread t2([&]() {
        constexpr int sleepTimeUs = 1'000;
        usleep(sleepTimeUs);
        rc2 = client1_->MSetTx(keys, values, param);
    });
    t1.join();
    t2.join();
    ASSERT_TRUE(rc1.IsOk() || rc2.IsOk());
    if (rc1.IsOk()) {
        ASSERT_EQ(rc2.GetCode(), K_OC_KEY_ALREADY_EXIST);
    } else {
        ASSERT_EQ(rc1.GetCode(), K_OC_KEY_ALREADY_EXIST);
    }
}

class KVClientMSetTenantAuthTest : public KVClientMSetTest {
public:
    std::vector<std::string> workerAddress_;

    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numOBS = 1;
        opts.numWorkers = DEFAULT_WORKER_NUM;
        opts.enableDistributedMaster = "false";
        opts.numEtcd = 1;
        std::string hostIp = "127.0.0.1";
        opts.workerConfigs.emplace_back(hostIp, GetFreePort());
        for (auto addr : opts.workerConfigs) {
            workerAddress_.emplace_back(addr.ToString());
        }
        opts.workerGflagParams = " -authorization_enable=true";
        opts.numEtcd = 1;
    }

    void InitTestKVClient(std::shared_ptr<KVClient> &client, std::string tenant, std::string token, std::string &ak,
                          std::string &sk)
    {
        std::string returnVal = "return(" + token + ", " + tenant + ")";
        DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "worker.auth", returnVal));
        ConnectOptions connectOptions;
        connectOptions.host = workerAddr_.Host();
        connectOptions.port = workerAddr_.Port();
        connectOptions.connectTimeoutMs = 60 * 1000;  // 60s
        connectOptions.requestTimeoutMs = 0;
        connectOptions.token = token;
        connectOptions.clientPublicKey = "";
        connectOptions.clientPrivateKey = "";
        connectOptions.serverPublicKey = "";
        connectOptions.accessKey = ak;
        connectOptions.secretKey = sk;
        connectOptions.tenantId = tenant;
        client = std::make_shared<KVClient>(connectOptions);
        DS_ASSERT_OK(client->Init());
    }

    void SetUp() override
    {
        ExternalClusterTest::SetUp();
        DS_ASSERT_OK(cluster_->GetWorkerAddr(0, workerAddr_));
    }

protected:
    HostPort workerAddr_;
    std::string accessKey = "QTWAOYTTINDUT2QVKYUC";
    std::string secretKey = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
};

TEST_F(KVClientMSetTenantAuthTest, TenantAuthTokenTest)
{
    std::shared_ptr<KVClient> client, client1;
    std::string ak, sk;
    InitTestKVClient(client, "tenant1", "token", ak, sk);
    InitTestKVClient(client1, "tenant2", "token1", ak, sk);

    MSetParam param;
    std::vector<std::string> keys, vals, vals1;
    std::vector<StringView> values, values1;
    param.existence = ExistenceOpt::NX;
    param.writeMode = WriteMode::WRITE_THROUGH_L2_CACHE;
    size_t maxElementSize = 8;
    int dataSize = 30;
    GenerateKeyValues(keys, vals, maxElementSize, dataSize);
    GenerateValues(vals1, maxElementSize, dataSize);
    for (const auto &val : vals) {
        values.emplace_back(val);
    }
    for (const auto &val : vals1) {
        values1.emplace_back(val);
    }
    DS_ASSERT_OK(client->MSetTx(keys, values, param));
    for (size_t i = 0; i < maxElementSize; i++) {
        std::string valGet;
        DS_ASSERT_OK(client->Get(keys[i], valGet));
        ASSERT_EQ(valGet, vals[i]);
    }
    DS_ASSERT_OK(client1->MSetTx(keys, values1, param));
    for (size_t i = 0; i < maxElementSize; i++) {
        std::string valGet;
        DS_ASSERT_OK(client1->Get(keys[i], valGet));
        ASSERT_EQ(valGet, vals1[i]);
    }
}

TEST_F(KVClientMSetTenantAuthTest, TenantAuthAkSkTest)
{
    std::shared_ptr<KVClient> client, client1;
    std::string ak, sk;
    InitTestKVClient(client, "tenant1", "token", ak, sk);
    InitTestKVClient(client1, "tenant2", "token1", accessKey, secretKey);

    MSetParam param;
    std::vector<std::string> keys, vals, vals1;
    std::vector<StringView> values, values1;
    param.existence = ExistenceOpt::NX;
    param.writeMode = WriteMode::WRITE_THROUGH_L2_CACHE;
    size_t maxElementSize = 8;
    int dataSize = 30;
    GenerateKeyValues(keys, vals, maxElementSize, dataSize);
    GenerateValues(vals1, maxElementSize, dataSize);
    for (const auto &val : vals) {
        values.emplace_back(val);
    }
    for (const auto &val : vals1) {
        values1.emplace_back(val);
    }
    DS_ASSERT_OK(client->MSetTx(keys, values, param));
    for (size_t i = 0; i < maxElementSize; i++) {
        std::string valGet;
        DS_ASSERT_OK(client->Get(keys[i], valGet));
        ASSERT_EQ(valGet, vals[i]);
    }
    DS_ASSERT_OK(client1->MSetTx(keys, values1, param));
    for (size_t i = 0; i < maxElementSize; i++) {
        std::string valGet;
        DS_ASSERT_OK(client1->Get(keys[i], valGet));
        ASSERT_EQ(valGet, vals1[i]);
    }
}

class KVClientMSetSpillTest : public KVClientMSetTest {
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numOBS = 1;
        opts.numWorkers = DEFAULT_WORKER_NUM;
        opts.enableSpill = true;
        opts.enableDistributedMaster = "false";
        opts.numEtcd = 1;
        std::string hostIp = "127.0.0.1";
        opts.workerGflagParams = " -authorization_enable=true -shared_memory_size_mb=50";
        opts.numEtcd = 1;
    }
};

TEST_F(KVClientMSetSpillTest, MsetSpillAndEvict)
{
    MSetParam param;
    std::vector<std::string> keys;
    std::vector<std::string> allKeys;
    std::vector<StringView> values;
    param.existence = ExistenceOpt::NX;
    size_t maxElementSize = 6;
    size_t valSize = 1 * 1024 * 1024;
    auto val = randomData_.GetRandomString(valSize);
    int allKeySize = 100;
    for (int i = 0; i < allKeySize; i++) {
        if (i % maxElementSize == 0 && i > 0) {
            DS_ASSERT_OK(client1_->MSetTx(keys, values, param));
            allKeys.insert(allKeys.end(), keys.begin(), keys.end());
            keys.clear();
            values.clear();
        }
        auto key = "a_key_for_mset_" + std::to_string(i);
        keys.emplace_back(key);
        values.emplace_back(val);
    }
    for (const auto &key : allKeys) {
        std::string valGet;
        DS_ASSERT_OK(client2_->Get(key, valGet));
        ASSERT_EQ(valGet, val);
    }
}

TEST_F(KVClientMSetSpillTest, TestMsetNtx)
{
    MSetParam param;
    std::vector<std::string> keys;
    std::vector<std::string> allKeys;
    std::vector<StringView> values;
    std::vector<std::string> outFailedIds;
    param.existence = ExistenceOpt::NX;
    size_t maxElementSize = 12;
    size_t valSize = SHM_THRESHOLD - 1;
    auto val = randomData_.GetRandomString(valSize);
    int allKeySize = 100;
    for (int i = 0; i < allKeySize; i++) {
        if (i % maxElementSize == 0 && i > 0) {
            DS_ASSERT_OK(client1_->MSet(keys, values, outFailedIds, param));
            allKeys.insert(allKeys.end(), keys.begin(), keys.end());
            keys.clear();
            values.clear();
        }
        auto key = "a_key_for_mset_" + std::to_string(i);
        keys.emplace_back(key);
        values.emplace_back(val);
    }
    for (const auto &key : allKeys) {
        std::string valGet;
        DS_ASSERT_OK(client2_->Get(key, valGet));
        ASSERT_EQ(valGet, val);
    }
}

class KVClientMSetDMTest : public KVClientMSetTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numEtcd = 1;
        opts.numOBS = 1;
        opts.numWorkers = DEFAULT_WORKER_NUM;
        opts.enableDistributedMaster = "true";
        opts.workerGflagParams = "-shared_memory_size_mb=3000 -v=2";
    }

    void SetUp() override
    {
        KVClientMSetTest::SetUp();
        GenerateDatas();
        mParam_.existence = ExistenceOpt::NX;
        sParam_.existence = ExistenceOpt::NX;
    }

    void GenerateDatas()
    {
        GenerateKeyValues(keys_, vals_, keyNum_, dataSize_);
        (void)client0_->GenerateKey(keys_[1], keys_[1]);
        (void)client1_->GenerateKey(keys_[keyNum_ - 1], keys_[keyNum_ - 1]);
        for (const auto &val : vals_) {
            valViews_.emplace_back(val);
        }
    }

    void ReGenerateDatas(int num = 8)
    {
        keyNum_ = num;
        keys_.clear();
        vals_.clear();
        valViews_.clear();
        GenerateKeyValues(keys_, vals_, keyNum_, dataSize_);
        for (const auto &val : vals_) {
            valViews_.emplace_back(val);
        }
    }

protected:
    size_t keyNum_ = 4;
    size_t dataSize_ = 1024;
    std::vector<std::string> keys_;
    std::vector<std::string> vals_;
    std::vector<StringView> valViews_;
    MSetParam mParam_;
    SetParam sParam_;
    static constexpr int sleepTimeUs_ = 1'000;
    static constexpr int timeoutMs_ = 2'000;
};

TEST_F(KVClientMSetDMTest, DISABLED_MSetNetFailure)
{
    DS_ASSERT_OK(
        cluster_->SetInjectAction(WORKER, 0, "OCMetadataManager.createMultiMeta.delay", "1*return(K_RPC_UNAVAILABLE)"));
    DS_ASSERT_OK(
        cluster_->SetInjectAction(WORKER, 0, "master.CreateMultiMetaPhaseTwo.begin", "1*return(K_RPC_UNAVAILABLE)"));
    DS_ASSERT_OK(client0_->MSetTx(keys_, valViews_, mParam_));
    std::vector<std::string> failedKeys;
    DS_ASSERT_OK(client0_->Del(keys_, failedKeys));
    ASSERT_TRUE(failedKeys.empty());

    DS_ASSERT_OK(
        cluster_->SetInjectAction(WORKER, 1, "master.CreateMultiMetaPhaseTwo.begin", "5*return(K_RPC_UNAVAILABLE)"));
    std::shared_ptr<KVClient> client0;
    InitTestKVClient(0, client0, timeoutMs_);
    const int threadNum = 2;
    ThreadPool threadPool(threadNum);
    auto fut = threadPool.Submit(
        [this, client0]() { ASSERT_EQ(client0->MSetTx(keys_, valViews_, mParam_).GetCode(), K_OC_KEY_ALREADY_EXIST); });
    auto fut1 = threadPool.Submit([this]() {
        usleep(sleepTimeUs_);
        Status rc = client2_->MSetTx(keys_, valViews_, mParam_);
        while (rc.GetCode() == K_OC_KEY_ALREADY_EXIST) {
            rc = client2_->MSetTx(keys_, valViews_, mParam_);
        }
        // rollback finished, mset success
        DS_ASSERT_OK(rc);
    });
    fut.get();
    fut1.get();
}

TEST_F(KVClientMSetDMTest, PreCommitTtl)
{
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "worker.CreateMultiMetaPhaseTwo.delay", "1*sleep(3000)"));
    for (size_t i = 0; i < DEFAULT_WORKER_NUM; i++) {
        DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, i, "master.CreateMultiMetaTx.pendingTtl", "call(2'000'000)"));
    }
    const int threadNum = 2;
    ThreadPool threadPool(threadNum);
    auto fut = threadPool.Submit(
        [this]() { ASSERT_EQ(client0_->MSetTx(keys_, valViews_, mParam_).GetCode(), K_OC_KEY_ALREADY_EXIST); });
    auto fut1 = threadPool.Submit([this]() {
        usleep(sleepTimeUs_);
        DS_ASSERT_OK(client1_->MSetTx(keys_, valViews_, mParam_));
    });
    fut.get();
    fut1.get();
}

TEST_F(KVClientMSetDMTest, LEVEL1_MSetRollbackFailed)
{
    DS_ASSERT_OK(
        cluster_->SetInjectAction(WORKER, 0, "master.CreateMultiMetaPhaseTwo.begin", "1*return(K_RUNTIME_ERROR)"));
    for (size_t i = 0; i < DEFAULT_WORKER_NUM; i++) {
        DS_ASSERT_OK(
            cluster_->SetInjectAction(WORKER, i, "master.RollbackMultiMeta.begin", "14*return(K_RPC_UNAVAILABLE)"));
    }
    std::shared_ptr<KVClient> client0;
    InitTestKVClient(0, client0, timeoutMs_);
    // The objects mset failed, enter the rollback queue.
    ASSERT_EQ(client0->MSetTx(keys_, valViews_, mParam_).GetCode(), K_RUNTIME_ERROR);

    std::vector<std::string> getVals;
    ASSERT_EQ(client0->Get(keys_, getVals).GetCode(), K_NOT_FOUND);
    ASSERT_EQ(client0->Set(keys_[0], valViews_[0], sParam_).GetCode(), K_OC_KEY_ALREADY_EXIST);

    auto keys2 = keys_;
    keys2[0] = randomData_.GetRandomString(keys_[0].size());
    const int threadNum = 3;
    ThreadPool threadPool(threadNum);
    std::shared_ptr<KVClient> client1;
    InitTestKVClient(1, client1, timeoutMs_);
    auto fut = threadPool.Submit([this, client1]() {
        Status rc = client1->Set(keys_[1], valViews_[1], sParam_);
        ASSERT_TRUE(IsRpcTimeoutOrTryAgain(rc) || rc.GetCode() == K_OC_KEY_ALREADY_EXIST);
    });
    auto fut1 = threadPool.Submit([this, client1, &keys2]() {
        ASSERT_EQ(client1->MSetTx(keys2, valViews_, mParam_).GetCode(), K_OC_KEY_ALREADY_EXIST);
    });
    auto fut2 = threadPool.Submit([this, client1]() {
        std::vector<std::string> getVals;
        Status rc = client1->Get(keys_, getVals);
        ASSERT_TRUE(IsRpcTimeoutOrTryAgain(rc) || rc.GetCode() == K_NOT_FOUND);
    });
    fut.get();
    fut1.get();
    fut2.get();

    Status rc = client0->MSetTx(keys2, valViews_, mParam_);
    while (rc.GetCode() == K_OC_KEY_ALREADY_EXIST) {
        rc = client0->MSetTx(keys2, valViews_, mParam_);
        sleep(1);
    }
    // rollback finished, mset success
    DS_ASSERT_OK(rc);
    for (size_t i = 0; i < keys2.size(); i++) {
        std::string val;
        DS_ASSERT_OK(client2_->Get(keys2[i], val));
        ASSERT_EQ(val, vals_[i]);
    }
}

TEST_F(KVClientMSetDMTest, DISABLED_ParallelWithOthers)
{
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "worker.CreateMultiMetaPhaseTwo.delay", "2*sleep(3000)"));
    const int threadNum = 3;
    ThreadPool threadPool(threadNum);
    auto fut = threadPool.Submit([this]() { DS_ASSERT_OK(client0_->MSetTx(keys_, valViews_, mParam_)); });

    std::shared_ptr<KVClient> client1;
    InitTestKVClient(1, client1, timeoutMs_);
    auto fut1 = threadPool.Submit([this, client1]() {
        sleep(1);
        size_t rdIdx = randomData_.GetRandomIndex(keys_.size());
        ASSERT_EQ(client1->Set(keys_[rdIdx], valViews_[rdIdx], sParam_).GetCode(), K_TRY_AGAIN);
    });
    auto fut2 = threadPool.Submit([this, client1]() {
        sleep(1);
        size_t rdIdx = randomData_.GetRandomIndex(keys_.size());
        std::string val;
        Status rc = client1->Get(keys_[rdIdx], val);
        ASSERT_TRUE(IsRpcTimeoutOrTryAgain(rc) || rc.GetCode() == K_NOT_FOUND);
    });
    fut.get();
    fut1.get();
    fut2.get();

    std::vector<std::string> failedKeys;
    DS_ASSERT_OK(client0_->Del(keys_, failedKeys));
    ASSERT_TRUE(failedKeys.empty());

    fut = threadPool.Submit([this]() { DS_ASSERT_OK(client0_->MSetTx(keys_, valViews_, mParam_)); });
    fut1 = threadPool.Submit([this, client1]() {
        usleep(sleepTimeUs_);
        size_t rdIdx = randomData_.GetRandomIndex(keys_.size());
        DS_ASSERT_OK(client1->Del(keys_[rdIdx]));
    });
    fut.get();
    fut1.get();
}

TEST_F(KVClientMSetDMTest, MSetParallel)
{
    const int parallelismNum = 10;
    ThreadPool threadPool(parallelismNum);
    std::vector<std::future<Status>> futs;
    std::vector<std::shared_ptr<KVClient>> clients;
    clients.resize(parallelismNum);
    for (int i = 0; i < parallelismNum; i++) {
        InitTestKVClient(i % DEFAULT_WORKER_NUM, clients[i]);
    }
    ReGenerateDatas();
    for (int i = 0; i < parallelismNum; i++) {
        futs.emplace_back(
            threadPool.Submit([this, clients, i]() { return clients[i]->MSetTx(keys_, valViews_, mParam_); }));
    }
    int succNum = 0;
    for (auto &fut : futs) {
        auto rc = fut.get();
        if (rc.IsOk()) {
            succNum++;
        } else {
            ASSERT_EQ(rc.GetCode(), K_OC_KEY_ALREADY_EXIST);
        }
    }
    ASSERT_EQ(succNum, 1);
}

TEST_F(KVClientMSetDMTest, MSetParallel2)
{
    const int parallelismNum = 8;
    ThreadPool threadPool(parallelismNum);
    std::vector<std::future<void>> futs;
    std::vector<std::shared_ptr<KVClient>> clients;
    clients.resize(parallelismNum);
    for (int i = 0; i < parallelismNum; i++) {
        InitTestKVClient(i % DEFAULT_WORKER_NUM, clients[i], timeoutMs_);
    }
    const int dataNum = 15;
    ReGenerateDatas(dataNum);
    auto fun = [this](std::shared_ptr<KVClient> client, std::vector<std::string> keys,
                      std::vector<StringView> valViews) {
        Status rc = client->MSetTx(keys, valViews, mParam_);
        while (IsRpcTimeoutOrTryAgain(rc) || rc.GetCode() == K_OC_KEY_ALREADY_EXIST) {
            std::vector<std::string> vals;
            rc = client->Get(keys, vals);
            ASSERT_TRUE(rc.IsOk() || IsRpcTimeoutOrTryAgain(rc) || rc.GetCode() == K_NOT_FOUND);
            std::vector<std::string> absentKeys;
            std::vector<StringView> absentValViews;
            for (size_t k = 0; k < keys.size(); k++) {
                if (vals[k].empty()) {
                    absentKeys.emplace_back(keys[k]);
                    absentValViews.emplace_back(valViews[k]);
                }
            }
            if (absentKeys.empty()) {
                return;
            }
            rc = client->MSetTx(absentKeys, absentValViews, mParam_);
        }
        DS_ASSERT_OK(rc);
    };
    for (int i = 0; i < parallelismNum; i++) {
        futs.emplace_back(threadPool.Submit([this, clients, i, fun]() {
            std::vector<std::string> keys;
            std::vector<StringView> valViews;
            const int maxNum = 8;
            for (int j = i; j < i + maxNum; j++) {
                keys.emplace_back(keys_[j]);
                valViews.emplace_back(valViews_[j]);
            }
            fun(clients[i], keys, valViews);
        }));
    }
    for (auto &fut : futs) {
        fut.wait();
    }
}

TEST_F(KVClientMSetDMTest, MSetMeetRollbackAsync)
{
    std::vector<std::string> keys;
    std::string key1;
    std::string key2;
    (void)client0_->GenerateKey("obj1", key1);
    (void)client1_->GenerateKey("obj2", key2);
    keys.emplace_back(key1);
    keys.emplace_back(key2);
    std::vector<StringView> vals{ "any", "any" };

    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "master.CreateMultiMeta.begin", "1*return(K_TRY_AGAIN)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "AsyncRollbackToMaster.delay", "1*call(2)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "master.RollbackMultiMeta.begin", "1*return(K_UNKNOWN_ERROR)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "master.CreateMultiMetaPhaseTwo.begin", "2*sleep(5000)"));
    const int threadNum = 2;
    ThreadPool threadPool(threadNum);
    // 1. w0 -> w1 create meta phase one
    // 2. w0 -> w1 rollback obj2
    // 3. w2 -> w1 create meta phase one
    // 4. w0 -> w1 create meta phase two  --> The obj2 metadata cannot be rolled back because it is occupied by w2.
    // 5. w2 -> w1 create meta phase two
    auto fut = threadPool.Submit(
        [this, keys, vals]() { ASSERT_EQ(client0_->MSetTx(keys, vals, mParam_).GetCode(), K_OC_KEY_ALREADY_EXIST); });
    auto fut1 = threadPool.Submit([this, keys, vals]() {
        sleep(3);  // wait obj2 rolled back
        DS_ASSERT_OK(client2_->MSetTx({ keys[1] }, { vals[1] }, mParam_));
    });
    fut.get();
    fut1.get();
}

TEST_F(KVClientMSetDMTest, PreCommitConflict)
{
    for (size_t i = 0; i < DEFAULT_WORKER_NUM; i++) {
        DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, i, "master.CreateMultiMetaTx.pendingTtl", "call(2'000'000)"));
    }
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "worker.CreateMultiMetaParallel.begin", "3*return(K_TRY_AGAIN)"));
    DS_ASSERT_OK(
        cluster_->SetInjectAction(WORKER, 0, "master.CreateMultiMetaPhaseTwo.begin", "1*return(K_RUNTIME_ERROR)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "master.CreateMultiMeta.begin", "5*return(K_TRY_AGAIN)"));

    std::string keyW0;
    (void)client0_->GenerateKey("obj0", keyW0);
    std::string keyW1;
    (void)client1_->GenerateKey("obj1", keyW1);
    std::string keyW2;
    (void)client2_->GenerateKey("obj2", keyW2);

    const int threadNum = 2;
    ThreadPool threadPool(threadNum);
    auto fut = threadPool.Submit(
        [this, &keyW1, &keyW2]() { DS_ASSERT_OK(client0_->MSetTx({ keyW1, keyW2 }, { "any", "any" }, mParam_)); });
    auto fut1 = threadPool.Submit([this, &keyW0, &keyW2]() {
        sleep(1);
        ASSERT_EQ(client1_->MSetTx({ keyW0, keyW2 }, { "any", "any" }, mParam_).GetCode(), K_RUNTIME_ERROR);
    });
    fut.get();
    fut1.get();
}

class KVClientMSetOOMTest : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numEtcd = 1;
        opts.numOBS = 1;
        opts.numWorkers = 1;
        opts.workerGflagParams = "-shared_memory_size_mb=10";
    }

    void SetUp() override
    {
        ExternalClusterTest::SetUp();
    }

    void TearDown() override
    {
        ExternalClusterTest::TearDown();
    }
};

TEST_F(KVClientMSetOOMTest, MSetOOM)
{
    std::vector<std::string> keys, vals;
    std::vector<StringView> values;
    int objNum = 2;
    int objSize = 6 * 1024 * 1024;
    for (int i = 0; i < objNum; i++) {
        keys.emplace_back("key" + std::to_string(i));
        vals.emplace_back(randomData_.GetRandomString(objSize));
    }
    for (const auto &val : vals) {
        values.emplace_back(val);
    }
    std::shared_ptr<KVClient> client;
    int timeoutMs = 1'000;
    InitTestKVClient(0, client, timeoutMs);
    MSetParam param;
    param.existence = ExistenceOpt::NX;
    ASSERT_EQ(client->MSetTx(keys, values, param).GetCode(), K_OUT_OF_MEMORY);
    keys.pop_back();
    values.pop_back();
    DS_ASSERT_OK(client->MSetTx(keys, values, param));
}
}  // namespace st
}  // namespace datasystem
