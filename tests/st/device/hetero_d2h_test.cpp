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
 * Description: hetero d2h test.
 */
#include "device/dev_test_helper.h"

namespace datasystem {
using namespace acl;
namespace st {
class HeteroD2HTest : public DevTestHelper {
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = 1;
        opts.workerGflagParams =
            " -v=0 -authorization_enable=true -shared_memory_size_mb=4096 -enable_fallocate=false -arena_per_tenant=2 ";
        opts.enableDistributedMaster = "false";
        opts.numEtcd = 1;
        FLAGS_v = 0;
    }

    void SetUp() override
    {
        const char *ascend_root = std::getenv("ASCEND_HOME_PATH");
        if (ascend_root == nullptr) {
            DS_ASSERT_OK(datasystem::inject::Set("NO_USE_FFTS", "call()"));
            DS_ASSERT_OK(datasystem::inject::Set("client.GetOrCreateHcclComm.setIsSameNode", "call(0)"));
            BINEXPECT_CALL(AclDeviceManager::Instance, ()).WillRepeatedly(Return(&managerMock_));
        }
        ExternalClusterTest::SetUp();
    }

    void TearDown() override
    {
        ExternalClusterTest::TearDown();
    }
};

TEST_F(HeteroD2HTest, Perf)
{
    std::vector<uint64_t> keyNums{ 1, 10, 450, 500, 1000 };
    std::vector<std::vector<double>> costs(keyNums.size());
    std::shared_ptr<HeteroClient> client;
    InitAcl(0);
    InitTestHeteroClient(0, client);
    auto repeatNum = 100u, testIdx = 0u, blkSz = 1024u, blksPerObj = 1u;
    for (auto numOfObjs : keyNums) {
        auto idx = 0u;
        for (auto i = 0u; i < repeatNum; i++) {
            LOG(INFO) << FormatString("start test MSetD2H Test ##### %lu KB, Round:%lu", numOfObjs, idx);

            std::vector<std::string> inObjectKeys;
            std::vector<DeviceBlobList> devGetBlobList, devSetBlobList;
            for (auto j = 0ul; j < numOfObjs; j++) {
                inObjectKeys.emplace_back(FormatString("round_%lu_key_%s", idx, j));
            }
            PrePareDevData(numOfObjs, blksPerObj, blkSz, devGetBlobList, devSetBlobList, 0);
            Timer t;
            DS_ASSERT_OK(client->MSetD2H(inObjectKeys, devSetBlobList));
            costs[testIdx].push_back(t.ElapsedMicroSecond());
            idx++;
        }
        testIdx++;
    }
    for (auto i = 0u; i < keyNums.size(); i++) {
        LOG(INFO) << FormatString(
            "#### MSetD2H Test #####  %4lu KB, Key Size: %4lu B, Key num:%4lu  Round:%4lu, --- result(ms): %s",
            keyNums[i], blkSz * blksPerObj, keyNums[i], repeatNum, GetTimeCostUsState(costs[i]));
    }
}

TEST_F(HeteroD2HTest, TestNoExist)
{
    std::vector<uint64_t> keyNums{ 450, 500 };
    std::vector<std::vector<double>> costs(keyNums.size());
    std::shared_ptr<HeteroClient> client;
    InitAcl(0);
    InitTestHeteroClient(0, client);
    auto testIdx = 0u, blkSz = 1024u, blksPerObj = 1u;
    for (auto numOfObjs : keyNums) {
        LOG(INFO) << FormatString("start test MSetD2H Test ##### %lu KB", numOfObjs);
        std::vector<std::string> inObjectKeys, failedKeys;
        std::vector<DeviceBlobList> devGetBlobList, devSetBlobList;
        for (auto j = 0ul; j < numOfObjs; j++) {
            inObjectKeys.emplace_back(FormatString("key_%s", j));
        }
        PrePareDevData(numOfObjs, blksPerObj, blkSz, devGetBlobList, devSetBlobList, 0);
        DS_ASSERT_OK(client->MSetD2H(inObjectKeys, devSetBlobList));
        DS_ASSERT_OK(client->MGetH2D(inObjectKeys, devGetBlobList, failedKeys, MIN_RPC_TIMEOUT_MS));
        DS_ASSERT_TRUE(failedKeys.empty(), true);
        DS_ASSERT_OK(IsSameContent(devGetBlobList, devGetBlobList, 'b'));
        testIdx++;
    }
}

TEST_F(HeteroD2HTest, TestAllExist)
{
    std::vector<uint64_t> keyNums{ 450, 500 };
    std::vector<std::vector<double>> costs(keyNums.size());
    std::shared_ptr<HeteroClient> client;
    InitAcl(0);
    InitTestHeteroClient(0, client);
    auto testIdx = 0u, blkSz = 1024u, blksPerObj = 1u;
    for (auto numOfObjs : keyNums) {
        LOG(INFO) << FormatString("start test MSetD2H Test ##### %lu KB", numOfObjs);
        std::vector<std::string> inObjectKeys, failedKeys;
        std::vector<DeviceBlobList> devGetBlobList, devSetBlobList;
        for (auto j = 0ul; j < numOfObjs; j++) {
            inObjectKeys.emplace_back(FormatString("key_%s", j));
        }
        PrePareDevData(numOfObjs, blksPerObj, blkSz, devGetBlobList, devSetBlobList, 0);
        DS_ASSERT_OK(client->MSetD2H(inObjectKeys, devSetBlobList));
        DS_ASSERT_OK(client->MSetD2H(inObjectKeys, devSetBlobList));
        DS_ASSERT_OK(client->MGetH2D(inObjectKeys, devGetBlobList, failedKeys, MIN_RPC_TIMEOUT_MS));
        DS_ASSERT_TRUE(failedKeys.empty(), true);
        DS_ASSERT_OK(IsSameContent(devGetBlobList, devGetBlobList, 'b'));
        testIdx++;
    }
}

TEST_F(HeteroD2HTest, TestPartExist)
{
    std::vector<uint64_t> keyNums{ 450, 500 };
    std::vector<std::vector<double>> costs(keyNums.size());
    std::shared_ptr<HeteroClient> client;
    InitAcl(0);
    InitTestHeteroClient(0, client);
    auto testIdx = 0u, blkSz = 1024u, blksPerObj = 1u;
    for (auto numOfObjs : keyNums) {
        LOG(INFO) << FormatString("start test MSetD2H Test ##### %lu KB", numOfObjs);
        std::vector<std::string> inObjectKeys, failedKeys, partKeys;
        std::vector<DeviceBlobList> devGetBlobList, devSetBlobList, partDevBlobList;
        for (auto j = 0ul; j < numOfObjs; j++) {
            inObjectKeys.emplace_back(FormatString("key_%s", j));
        }
        PrePareDevData(numOfObjs, blksPerObj, blkSz, devGetBlobList, devSetBlobList, 0);
        std::vector<uint64_t> randIdxs = { 1, 5, 6, 7, 10, 33 };
        for (auto randIdx : randIdxs) {
            partKeys.push_back(inObjectKeys[randIdx]);
            partDevBlobList.push_back(devSetBlobList[randIdx]);
        }
        DS_ASSERT_OK(client->MSetD2H(partKeys, partDevBlobList));
        DS_ASSERT_OK(client->MSetD2H(inObjectKeys, devSetBlobList));
        DS_ASSERT_OK(client->MGetH2D(inObjectKeys, devGetBlobList, failedKeys, MIN_RPC_TIMEOUT_MS));
        DS_ASSERT_TRUE(failedKeys.empty(), true);
        DS_ASSERT_OK(IsSameContent(devGetBlobList, devGetBlobList, 'b'));
        testIdx++;
    }
}

}  // namespace st
}  // namespace datasystem
