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
 * Description: test cases for c client api.
 */
#include <securec.h>
#include <string.h>
#include <cstddef>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "client/object_cache/oc_client_common.h"
#include "common.h"
#include "datasystem/common/log/log.h"
#include "datasystem/state_cache/state_client.h"

namespace datasystem {
namespace st {
class SCCWrapperTest : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = 3;
        opts.numMasters = 3;
        opts.numOBS = 1;
        opts.numEtcd = 1;
        opts.workerGflagParams = "-shared_memory_size_mb=10000";
    }

    StateClient_p CreateStateCacheClient(const std::string &workerHost, const int workerPort, const int timeOut,
                                         const std::string &clientPublicKey, const std::string &clientPrivateKey,
                                         const std::string &serverPublicKey, const std::string &accessKey,
                                         const std::string &secretKey, const std::string &tenantId,
                                         const std::string &enableCrossNodeConnection)
    {
        return SCCreateClient(workerHost.c_str(), workerPort, timeOut, clientPublicKey.c_str(),
                              clientPublicKey.length(), clientPrivateKey.c_str(), clientPrivateKey.length(),
                              serverPublicKey.c_str(), serverPublicKey.length(), accessKey.c_str(), accessKey.length(),
                              secretKey.c_str(), secretKey.length(), tenantId.c_str(), tenantId.length(),
                              enableCrossNodeConnection.c_str());
    }

protected:
    static const size_t GB = 1024 * 1024 * 1024;
    std::string ak_ = "QTWAOYTTINDUT2QVKYUC";
    std::string sk_ = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
};

TEST_F(SCCWrapperTest, ConnectSuccess)
{
    HostPort srcWorkerAddress;
    DS_ASSERT_OK(cluster_->GetWorkerAddr(0, srcWorkerAddress));
    auto stateClient = CreateStateCacheClient(
        srcWorkerAddress.Host(), srcWorkerAddress.Port(), 60000, "", "", "",
        ak_, sk_, "", "true");
    ASSERT_EQ(SCConnectWorker(stateClient).code, datasystem::K_OK);
    SCFreeClient(stateClient);
}

TEST_F(SCCWrapperTest, LEVEL1_ConnectFail)
{
    HostPort srcWorkerAddress;
    DS_ASSERT_OK(cluster_->GetWorkerAddr(0, srcWorkerAddress));
    auto stateClient = CreateStateCacheClient(
        srcWorkerAddress.Host(), 989, 60000, "", "", "",
        ak_.c_str(), sk_.c_str(), "", "true");
    ASSERT_EQ(SCConnectWorker(stateClient).code, datasystem::K_RPC_UNAVAILABLE);
    SCFreeClient(stateClient);
}

TEST_F(SCCWrapperTest, SetSuccess)
{
    HostPort srcWorkerAddress;
    DS_ASSERT_OK(cluster_->GetWorkerAddr(0, srcWorkerAddress));
    auto stateClient = CreateStateCacheClient(
        srcWorkerAddress.Host(), srcWorkerAddress.Port(), 60000, "", "", "",
        ak_, sk_, "", "true");
    ASSERT_EQ(SCConnectWorker(stateClient).code, datasystem::K_OK);

    const char *cKey = "key5485";
    const char *cVal = "data1";
    const size_t keyLen = 7;
    const size_t valLen = 5;
    ASSERT_EQ(SCSet(stateClient, cKey, keyLen, cVal, valLen, "NONE_L2_CACHE", 0, "NONE").code, datasystem::K_OK);
    SCFreeClient(stateClient);
}

TEST_F(SCCWrapperTest, GetSuccess)
{
    HostPort srcWorkerAddress;
    DS_ASSERT_OK(cluster_->GetWorkerAddr(0, srcWorkerAddress));
    auto stateClient = CreateStateCacheClient(
        srcWorkerAddress.Host(), srcWorkerAddress.Port(), 60000, "", "", "",
        ak_, sk_, "", "true");
    ASSERT_EQ(SCConnectWorker(stateClient).code, datasystem::K_OK);

    const char *cKey = "key1223";
    const char *cValS = "data1";
    const size_t valLen = 5;
    const size_t keyLen = 7;
    ASSERT_EQ(SCSet(stateClient, cKey, keyLen, cValS, valLen, "NONE_L2_CACHE", 0, "NX").code, datasystem::K_OK);

    char *cValG = nullptr;
    size_t valLenToGet;
    ASSERT_EQ(SCGet(stateClient, cKey, keyLen, 0, &cValG, &valLenToGet).code, datasystem::K_OK);
    ASSERT_EQ(std::string(cValS, 5), std::string(cValG, 5));
    free(cValG);
    SCFreeClient(stateClient);
}

TEST_F(SCCWrapperTest, GetBigObjectSuccess)
{
    HostPort srcWorkerAddress;
    DS_ASSERT_OK(cluster_->GetWorkerAddr(0, srcWorkerAddress));
    auto stateClient = CreateStateCacheClient(
        srcWorkerAddress.Host(), srcWorkerAddress.Port(), 60000, "", "", "",
        ak_, sk_, "", "true");
    ASSERT_EQ(SCConnectWorker(stateClient).code, datasystem::K_OK);
    std::string data = GenPartRandomString(3 * GB);
    const char *cKey = "key1223";
    const char *cValS = data.c_str();
    const size_t keyLen = 7;
    const size_t valLen = 3 * GB;
    ASSERT_EQ(SCSet(stateClient, cKey, keyLen, cValS, valLen, "NONE_L2_CACHE", 0, "NONE").code, datasystem::K_OK);

    char *cValG = nullptr;
    size_t valLenToGet;

    ASSERT_EQ(SCGet(stateClient, cKey, keyLen, 0, &cValG, &valLenToGet).code, datasystem::K_OK);
    ASSERT_EQ(std::string(cValS, 3 * GB), std::string(cValG, valLenToGet));
    free(cValG);
    SCFreeClient(stateClient);
}

TEST_F(SCCWrapperTest, DelSuccess)
{
    HostPort srcWorkerAddress;
    DS_ASSERT_OK(cluster_->GetWorkerAddr(0, srcWorkerAddress));
    auto stateClient = CreateStateCacheClient(
        srcWorkerAddress.Host(), srcWorkerAddress.Port(), 60000, "", "", "",
        ak_, sk_, "", "true");
    ASSERT_EQ(SCConnectWorker(stateClient).code, datasystem::K_OK);

    const char *cKey = "key1523";
    const char *cValS = "data1";
    const size_t valLen = 5;
    const size_t keyLen = 7;
    ASSERT_EQ(SCSet(stateClient, cKey, keyLen, cValS, valLen, "NONE_L2_CACHE", 0, "NONE").code, datasystem::K_OK);

    ASSERT_EQ(SCDel(stateClient, cKey, strlen(cKey)).code, datasystem::K_OK);
    SCFreeClient(stateClient);
}

TEST_F(SCCWrapperTest, GetArraySuccess)
{
    HostPort srcWorkerAddress;
    DS_ASSERT_OK(cluster_->GetWorkerAddr(0, srcWorkerAddress));
    auto stateClient = CreateStateCacheClient(
        srcWorkerAddress.Host(), srcWorkerAddress.Port(), 60000, "", "", "", ak_, sk_,
        "", "true");
    ASSERT_EQ(SCConnectWorker(stateClient).code, datasystem::K_OK);

    const char *cKey1 = "key1763";
    const char *cValS1 = "data1";
    const size_t valLen1 = 5;
    const size_t keyLen1 = 7;
    ASSERT_EQ(SCSet(stateClient, cKey1, keyLen1, cValS1, valLen1, "NONE_L2_CACHE", 0, "NONE").code, datasystem::K_OK);

    const char *cKey2 = "key1233";
    const char *cValS2 = "data2";
    const size_t valLen2 = 5;
    const size_t keyLen2 = 7;
    ASSERT_EQ(SCSet(stateClient, cKey2, keyLen2, cValS2, valLen2, "NONE_L2_CACHE", 0, "NONE").code, datasystem::K_OK);

    const char *cKeys[2];
    cKeys[0] = cKey1;
    cKeys[1] = cKey2;
    const size_t keysLen[2] = {7, 7};
    char **cVals = MakeCharsArray(2);
    size_t *valsLen = MakeNumArray(2);
    ASSERT_EQ(SCGetArray(stateClient, cKeys, keysLen, 2, 0, cVals, valsLen).code, datasystem::K_OK);
    ASSERT_EQ(std::string(cValS1, 5), std::string(cVals[0], valsLen[0]));
    ASSERT_EQ(std::string(cValS2, 5), std::string(cVals[1], valsLen[1]));
    FreeCharsArray(cVals, 2);
    SCFreeClient(stateClient);
}

TEST_F(SCCWrapperTest, GetArrayKeyMissing)
{
    HostPort srcWorkerAddress;
    DS_ASSERT_OK(cluster_->GetWorkerAddr(0, srcWorkerAddress));
    auto stateClient = CreateStateCacheClient(
        srcWorkerAddress.Host(), srcWorkerAddress.Port(), 60000, "", "", "", ak_, sk_,
        "", "true");
    ASSERT_EQ(SCConnectWorker(stateClient).code, datasystem::K_OK);

    const char *cKey1 = "key1453";
    const char *cKey2 = "key1298";
    const char *cValS2 = "data1";
    const size_t valLen = 5;
    const size_t keyLen2 = 7;
    ASSERT_EQ(SCSet(stateClient, cKey2, keyLen2, cValS2, valLen, "NONE_L2_CACHE", 0, "NONE").code, datasystem::K_OK);

    const char *cKeys[2];
    cKeys[0] = cKey1;
    cKeys[1] = cKey2;
    const size_t keysLen[2] = {7, 7};
    char **cVals = MakeCharsArray(2);
    size_t *valsLen = MakeNumArray(2);
    ASSERT_EQ(SCGetArray(stateClient, cKeys, keysLen, 2, 0, cVals, valsLen).code, datasystem::K_OK);
    ASSERT_EQ(cVals[0], nullptr);
    ASSERT_EQ(std::string(cValS2, 5), std::string(cVals[1], valsLen[1]));
    FreeCharsArray(cVals, 2);
    SCFreeClient(stateClient);
}

TEST_F(SCCWrapperTest, DelArraySuccess)
{
    HostPort srcWorkerAddress;
    DS_ASSERT_OK(cluster_->GetWorkerAddr(0, srcWorkerAddress));
    auto stateClient = CreateStateCacheClient(
        srcWorkerAddress.Host(), srcWorkerAddress.Port(), 60000, "", "", "", ak_, sk_,
        "", "true");
    ASSERT_EQ(SCConnectWorker(stateClient).code, datasystem::K_OK);

    const char *cKey1 = "key1863";
    const char *cValS1 = "data1";
    const size_t valLen = 5;
    const size_t keyLen = 7;
    ASSERT_EQ(SCSet(stateClient, cKey1, keyLen, cValS1, valLen, "NONE_L2_CACHE", 0, "NONE").code, datasystem::K_OK);

    const char *cKey2 = "key1253";
    const char *cValS2 = "data2";
    const size_t valLen2 = 5;
    const size_t keyLen2 = 7;
    ASSERT_EQ(SCSet(stateClient, cKey2, keyLen2, cValS2, valLen2, "NONE_L2_CACHE", 0, "NONE").code, datasystem::K_OK);

    const char *cKeys[2];
    cKeys[0] = cKey1;
    cKeys[1] = cKey2;
    uint64_t failedCount;
    uint64_t numKeys = 2;
    char **cVals = MakeCharsArray(numKeys);
    ASSERT_EQ(SCDelArray(stateClient, cKeys, numKeys, cVals, &failedCount).code, datasystem::K_OK);
    ASSERT_EQ(failedCount, 0ul);
    FreeCharsArray(cVals, numKeys);
    SCFreeClient(stateClient);
}

TEST_F(SCCWrapperTest, DelArrayKeyMissing)
{
    HostPort srcWorkerAddress;
    DS_ASSERT_OK(cluster_->GetWorkerAddr(0, srcWorkerAddress));
    auto stateClient = CreateStateCacheClient(
        srcWorkerAddress.Host(), srcWorkerAddress.Port(), 60000, "", "", "", ak_, sk_,
        "", "true");
    ASSERT_EQ(SCConnectWorker(stateClient).code, datasystem::K_OK);

    const char *cKey1 = "key1823";
    const char *cKey2 = "key1890";
    const char *cValS2 = "data2";
    const size_t valLen = 5;
    const size_t keyLen = 7;
    ASSERT_EQ(SCSet(stateClient, cKey2, keyLen, cValS2, valLen, "NONE_L2_CACHE", 0, "NONE").code, datasystem::K_OK);

    const char *cKeys[2];
    cKeys[0] = cKey1;
    cKeys[1] = cKey2;
    uint64_t failedCount;
    char **cVals = MakeCharsArray(2);
    ASSERT_EQ(SCDelArray(stateClient, cKeys, 2, cVals, &failedCount).code, datasystem::K_OK);
    ASSERT_EQ(failedCount, 0ul);
    FreeCharsArray(cVals, 2);
    SCFreeClient(stateClient);
}

TEST_F(SCCWrapperTest, TestWorkerAddressError)
{
    std::string st("");
    auto stateClient = CreateStateCacheClient(st, -20, 60000, "", "", "", ak_, sk_, "", "true");
    ASSERT_EQ(stateClient, nullptr);
    SCFreeClient(stateClient);
}

TEST_F(SCCWrapperTest, ClientCreateArgumentTest)
{
    HostPort srcWorkerAddress;
    DS_ASSERT_OK(cluster_->GetWorkerAddr(0, srcWorkerAddress));
    const char *clPubKey = "This is testing client public key";
    const char *clPriKey = "This is testing client private key";
    const char *srPubKey = "This is testing server public key";
    const char *accessKey = "This is testing access key";
    const char *secretKey = "This is testing secret key";
    auto stateClient = CreateStateCacheClient(srcWorkerAddress.Host(),
        srcWorkerAddress.Port(),
        60000,
        clPubKey,
        clPriKey,
        srPubKey,
        accessKey,
        secretKey,
        "",
        "true");
    ASSERT_NE(stateClient, nullptr);
    SCFreeClient(stateClient);
}

TEST_F(SCCWrapperTest, TestGenerateKey)
{
    HostPort srcWorkerAddress;
    DS_ASSERT_OK(cluster_->GetWorkerAddr(0, srcWorkerAddress));
    auto stateClient = CreateStateCacheClient(
        srcWorkerAddress.Host(), srcWorkerAddress.Port(), 60000, "", "", "", ak_, sk_,
        "", "true");
    ASSERT_EQ(SCConnectWorker(stateClient).code, datasystem::K_OK);

    char *key = nullptr;
    size_t keyLen = SCGenerateKey(stateClient, &key);
    const size_t generateKeyLen = 73;  // uuid;workeruuid, 36+1+36
    ASSERT_EQ(keyLen, generateKeyLen);
    std::string strKey = key;
    ASSERT_TRUE(strKey.find(";") != std::string::npos);
    delete[] key;

    SCFreeClient(stateClient);
}

class KVClientGetSubscribeTest : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        int numWorkers = 3;
        opts.isObjectCache = true;
        opts.masterIdx = 0;
        opts.numWorkers = numWorkers;
        opts.workerGflagParams = " -shared_memory_size_mb=12 -v=2";
        opts.masterGflagParams = " -v=1";
        opts.numEtcd = 1;
    }

    void TearDown() override
    {
        ExternalClusterTest::TearDown();
    }
};

TEST_F(KVClientGetSubscribeTest, TestGetSubscribeAndAddLoaction)
{
    LOG(INFO) << "Test get subscribe and add location";
    std::shared_ptr<KVClient> client0;
    std::shared_ptr<KVClient> client1;
    InitTestKVClient(0, client0);
    InitTestKVClient(1, client1);

    std::string key = "Welcome_to_join_the_conference";
    std::string value = randomData_.GetRandomString(1 * 1024ul * 1024ul);
    SetParam param;
    param.existence = ExistenceOpt::NX;
    std::thread t1([&client0, key, value]() {
        std::string getVal;
        DS_ASSERT_OK(client0->Get(key, getVal, 5'000));
        ASSERT_EQ(getVal, value);
    });

    std::thread t2([&client1, key, &value, param]() {
        sleep(2);
        DS_ASSERT_OK(client1->Set(key, value, param));
    });

    t1.join();
    t2.join();

    DS_ASSERT_OK(client1->Del(key));
    std::string getVal;
    DS_ASSERT_NOT_OK(client0->Get(key, getVal));
    DS_ASSERT_OK(client0->Set(key, value, param));
}

TEST_F(KVClientGetSubscribeTest, TestSetGetConcurrency)
{
    LOG(INFO) << "Test set get concurrency";
    std::shared_ptr<KVClient> client0;
    std::shared_ptr<KVClient> client1;
    InitTestKVClient(0, client0);
    InitTestKVClient(1, client1);

    std::string key = "Welcome_to_join_the_conference";
    std::string value = randomData_.GetRandomString(1024ul);
    SetParam param;
    param.existence = ExistenceOpt::NX;
    for (int k = 0; k < 5; ++k) {
        std::thread t1([&client0, key, value]() {
            std::string getVal;
            for (size_t i = 0; i < 100; ++i) {
                DS_ASSERT_OK(client0->Get(key, getVal, 5'000));
                ASSERT_EQ(getVal, value);
            }
        });

        std::thread t2([&client1, key, &value, param]() {
            for (size_t i = 0; i < 100; ++i) {
                if (i == 0) {
                    sleep(1);
                    DS_ASSERT_OK(client1->Set(key, value, param));
                } else {
                    DS_ASSERT_NOT_OK(client1->Set(key, value, param));
                }
            }
        });

        t1.join();
        t2.join();

        DS_ASSERT_OK(client1->Del(key));
    }
}

class KVClientGetTest : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        int numWorkers = 3;
        opts.isObjectCache = true;
        opts.masterIdx = 0;
        opts.numWorkers = numWorkers;
        opts.workerGflagParams = " -shared_memory_size_mb=8192 -v=2";
        opts.masterGflagParams = " -v=1";
        opts.numEtcd = 1;
    }

    void TearDown() override
    {
        ExternalClusterTest::TearDown();
    }
};

TEST_F(KVClientGetTest, EXCLUSIVE_LEVEL1_TestGetAllDataFromQueryResultDirectly)
{
    std::vector<std::string> objKeys = {"Kevin", "Bob", "Stuart", "Gru"};
    std::vector<uint64_t> sizes = {
        1024, 1024 * 1024ul, 2 * 1024ul * 1024ul * 1024ul + 1, 2 * 1024ul * 1024ul * 1024ul + 100};
    std::shared_ptr<KVClient> client0;
    std::shared_ptr<KVClient> client1;
    InitTestKVClient(0, client0);
    InitTestKVClient(1, client1);

    for (size_t i = 0; i < objKeys.size(); ++i) {
        std::string val(sizes[i], 'a' + i);
        DS_ASSERT_OK(client0->Set(objKeys[i], val));
    }

    std::vector<std::string> vals;
    DS_ASSERT_OK(client1->Get(objKeys, vals));
    ASSERT_EQ(vals.size(), objKeys.size());
    for (size_t i = 0; i < vals.size(); ++i) {
        ASSERT_EQ(vals[i].size(), sizes[i]);
        std::string expectedVal(sizes[i], 'a' + i);
        ASSERT_EQ(expectedVal, vals[i]);
    }
}

TEST_F(KVClientGetTest, EXCLUSIVE_TestGetAllDataFromQueryResultAndRemote)
{
    std::vector<std::string> localObjs = {"Kevin", "Bob", "Stuart"};
    std::vector<uint64_t> localSizes = {1024, 1024 * 1024ul, 2 * 1024ul * 1024ul * 1024ul + 100};
    std::vector<std::string> remoteObjs = {"LeBron", "James", "Nomadic", "Dynasty"};
    std::vector<uint64_t> remoteSizes = {2 * 1024, 1024 * 1024ul, 2 * 1024ul * 1024ul, 6024};
    std::shared_ptr<KVClient> client0;
    std::shared_ptr<KVClient> client1;
    std::shared_ptr<KVClient> client2;
    InitTestKVClient(0, client0);
    InitTestKVClient(1, client1);
    InitTestKVClient(1, client2);

    for (size_t i = 0; i < localObjs.size(); ++i) {
        std::string val(localSizes[i], 'l');
        DS_ASSERT_OK(client0->Set(localObjs[i], val));
    }

    for (size_t i = 0; i < remoteObjs.size(); ++i) {
        std::string val(remoteSizes[i], 'r');
        DS_ASSERT_OK(client1->Set(remoteObjs[i], val));
    }

    std::vector<std::string> getObjs(localObjs);
    getObjs.insert(getObjs.end(), remoteObjs.begin(), remoteObjs.end());
    std::vector<uint64_t> sizes(localSizes);
    sizes.insert(sizes.end(), remoteSizes.begin(), remoteSizes.end());
    std::vector<std::string> vals;
    DS_ASSERT_OK(client2->Get(getObjs, vals));
    ASSERT_EQ(vals.size(), getObjs.size());
    for (size_t i = 0; i < vals.size(); ++i) {
        ASSERT_EQ(vals[i].size(), sizes[i]);
        std::string expectedVal = i < localObjs.size() ? std::string(sizes[i], 'l') : std::string(sizes[i], 'r');
        ASSERT_EQ(expectedVal, vals[i]);
    }
}
}  // namespace st
}  // namespace datasystem
