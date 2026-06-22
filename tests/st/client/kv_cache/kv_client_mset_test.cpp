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
        InitTestKVClient(0, client0_, 20000);
        InitTestKVClient(1, client1_, 20000);
        InitTestKVClient(2, client2_, 20000);
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

TEST_F(KVClientMSetTest, MSetTxDeprecatedApi)
{
    std::vector<std::string> keys{ "deprecated_msettx_key" };
    std::string value = "deprecated_msettx_value";
    std::vector<StringView> values;
    values.emplace_back(value);
    Status rc = client0_->MSetTx(keys, values);
    ASSERT_EQ(rc.GetCode(), K_RUNTIME_ERROR);
    ASSERT_NE(rc.GetMsg().find("deprecated API"), std::string::npos);
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
    size_t failedSize = 0;
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

TEST_F(KVClientMSetTest, MsetNtxNxExistingKeySameValueNoFailedKeys)
{
    MSetParam param;
    param.existence = ExistenceOpt::NX;
    std::string key = "mset_ntx_nx_existing_same_" + randomData_.GetRandomString(8);
    std::string presetValue = randomData_.GetRandomString(64 * 1024);
    DS_ASSERT_OK(client0_->Set(key, presetValue));

    std::vector<std::string> keys{ key };
    std::vector<std::string> rawVals{ presetValue };
    std::vector<StringView> values{ rawVals[0] };
    std::vector<std::string> failedKeys;
    DS_ASSERT_OK(client1_->MSet(keys, values, failedKeys, param));
    ASSERT_TRUE(failedKeys.empty());

    std::string valueGet;
    DS_ASSERT_OK(client2_->Get(key, valueGet));
    ASSERT_EQ(valueGet, presetValue);
}

TEST_F(KVClientMSetTest, LEVEL1_MsetNtxNxAddLocationAfterOwnerWorkerDown)
{
    MSetParam param;
    param.existence = ExistenceOpt::NX;
    std::string key = "mset_ntx_nx_location_" + randomData_.GetRandomString(8);
    std::string presetValue = randomData_.GetRandomString(64 * 1024);
    DS_ASSERT_OK(client1_->Set(key, presetValue));

    std::vector<std::string> keys{ key };
    std::vector<std::string> rawVals{ presetValue };
    std::vector<StringView> values{ rawVals[0] };
    std::vector<std::string> failedKeys;
    DS_ASSERT_OK(client2_->MSet(keys, values, failedKeys, param));
    ASSERT_TRUE(failedKeys.empty());

    HostPort worker2Addr;
    DS_ASSERT_OK(cluster_->GetWorkerAddr(2, worker2Addr));
    constexpr size_t centralMasterWorkerIdx = 0;
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, centralMasterWorkerIdx, "master.select_location",
                                           "return(" + worker2Addr.ToString() + ")"));
    DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, 1));
    std::string valueGet;
    Status rc = Status::OK();
    int retryTimes = 20;
    int waitMs = 200;
    for (int i = 0; i < retryTimes; ++i) {
        rc = client0_->Get(key, valueGet);
        if (rc.IsOk()) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(waitMs));
    }
    Status clearInjectRc = Status::OK();
    for (int i = 0; i < 5; ++i) {
        clearInjectRc = cluster_->ClearInjectAction(WORKER, centralMasterWorkerIdx, "master.select_location");
        if (clearInjectRc.IsOk()) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
    DS_ASSERT_OK(clearInjectRc);
    DS_ASSERT_OK(rc);
    ASSERT_EQ(valueGet, presetValue);
}

TEST_F(KVClientMSetTest, MsetNtxNxAllLocalExistSkipMasterCreate)
{
    MSetParam param;
    param.existence = ExistenceOpt::NX;
    std::vector<std::string> keys, vals;
    std::vector<StringView> values;
    size_t keyNum = 6;
    auto valSize = 256;
    GenerateKeyValues(keys, vals, keyNum, valSize);
    for (size_t i = 0; i < keyNum; ++i) {
        DS_ASSERT_OK(client1_->Set(keys[i], vals[i]));
        values.emplace_back(vals[i]);
    }

    for (size_t i = 0; i < DEFAULT_WORKER_NUM; ++i) {
        DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, i, "master.CreateMultiMeta.begin", "1*return(K_RUNTIME_ERROR)"));
    }
    std::vector<std::string> failedKeys;
    DS_ASSERT_OK(client1_->MSet(keys, values, failedKeys, param));
    ASSERT_TRUE(failedKeys.empty());
    for (size_t i = 0; i < keyNum; ++i) {
        std::string valueGet;
        DS_ASSERT_OK(client1_->Get(keys[i], valueGet));
        ASSERT_EQ(valueGet, vals[i]);
    }
    for (size_t i = 0; i < DEFAULT_WORKER_NUM; ++i) {
        DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, i, "master.CreateMultiMeta.begin"));
    }
}

TEST_F(KVClientMSetTest, MsetNtxNxMixedLocalExistAndAbsentFailedKeysPrecise)
{
    MSetParam param;
    param.existence = ExistenceOpt::NX;
    std::string keyExist = "mset_ntx_nx_exist_" + randomData_.GetRandomString(8);
    std::string keyFail = "mset_ntx_nx_fail_" + randomData_.GetRandomString(8);
    std::string keyOk = "mset_ntx_nx_ok_" + randomData_.GetRandomString(8);
    std::string existValue = randomData_.GetRandomString(64);
    std::string failValue = randomData_.GetRandomString(64);
    std::string okValue = randomData_.GetRandomString(64);
    DS_ASSERT_OK(client1_->Set(keyExist, existValue));

    std::vector<std::string> keys{ keyExist, keyFail, keyOk };
    std::vector<std::string> rawVals{ existValue, failValue, okValue };
    std::vector<StringView> values;
    for (const auto &val : rawVals) {
        values.emplace_back(val);
    }

    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "AttachShmUnitToObject.error", "1*return(K_RUNTIME_ERROR)"));
    std::vector<std::string> failedKeys;
    DS_ASSERT_OK(client1_->MSet(keys, values, failedKeys, param));
    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, 1, "AttachShmUnitToObject.error"));

    size_t failedSize = 1;
    ASSERT_EQ(failedKeys.size(), failedSize);
    ASSERT_EQ(failedKeys[0], keyFail);

    std::string valueGet;
    DS_ASSERT_OK(client0_->Get(keyExist, valueGet));
    ASSERT_EQ(valueGet, existValue);
    DS_ASSERT_OK(client0_->Get(keyOk, valueGet));
    ASSERT_EQ(valueGet, okValue);
    DS_ASSERT_NOT_OK(client0_->Get(keyFail, valueGet));
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

}  // namespace st
}  // namespace datasystem
