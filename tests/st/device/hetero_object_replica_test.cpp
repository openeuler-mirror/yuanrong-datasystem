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
 * Description: Hetero object replica test.
 */

#include <gtest/gtest.h>
#include "common.h"
#include "datasystem/kv_client.h"
#include "datasystem/object/object_enum.h"
#include "device/dev_test_helper.h"
#include "gmock/gmock.h"

namespace datasystem {
using namespace acl;
namespace st {

class HeteroObjectReplicaTest : public DevTestHelper {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = DEFAULT_WORKER_NUM;
        opts.workerGflagParams = " -v=1 -shared_memory_size_mb=4096 ";
        opts.numEtcd = 1;
    }

    void SetUp() override
    {
        const char *ascend_root = std::getenv("ASCEND_HOME_PATH");
        if (ascend_root == nullptr) {
            BINEXPECT_CALL(AclDeviceManager::Instance, ()).WillRepeatedly([]() {
                return AclDeviceManagerMock::Instance();
            });
        }
        ExternalClusterTest::SetUp();
        InitTestHeteroClient(0, client0_, timeoutMs_);
        InitTestHeteroClient(1, client1_, timeoutMs_);
    }

    void TearDown() override
    {
        ExternalClusterTest::TearDown();
    }

    void CheckBlobContent(std::vector<DeviceBlobList> blobsList, std::string expectValue)
    {
        for (const auto &blobs : blobsList) {
            for (const auto &blob : blobs.blobs) {
                CheckDevPtrContent(blob.pointer, blob.size, expectValue);
            }
        }
    }

    void PrepareDevData(size_t numOfObjs, size_t blksPerObj, size_t blkSz, std::vector<DeviceBlobList> &blobList,
                        int32_t deviceId = 0, char fillChar = 'a')
    {
        DevPtrQueue que;
        que.Fill(numOfObjs, blksPerObj, DataType::DATA_TYPE_INT8, blkSz, deviceId, fillChar);

        std::vector<std::vector<DataInfo>> datInfoList;
        for (auto i = 0uL; i < numOfObjs; i++) {
            std::vector<DataInfo> infos;
            que.Get(infos);
            datInfoList.emplace_back(std::move(infos));
        }

        ConvertToDeviceBlobList(datInfoList, blobList);
    }

protected:
    int32_t deviceId_ = 4;
    size_t numOfObjs_ = 2;
    size_t blksPerObj_ = 1;
    size_t blkSz_ = 1;
    std::shared_ptr<HeteroClient> client0_;
    std::shared_ptr<HeteroClient> client1_;
    const int timeoutMs_ = 2'000;
};

TEST_F(HeteroObjectReplicaTest, DISABLED_ObjectExistence)
{
    InitAcl(deviceId_);

    std::vector<std::string> objectKeys;
    numOfObjs_ = 100;  // number of objects is 100
    for (auto i = 0ul; i < numOfObjs_; i++) {
        objectKeys.emplace_back(GetStringUuid());
    }
    std::vector<bool> exists;
    DS_ASSERT_OK(client0_->Exist(objectKeys, exists));
    EXPECT_THAT(exists, testing::Each(false));

    std::vector<DeviceBlobList> setBlobs1;
    PrepareDevData(numOfObjs_, blksPerObj_, blkSz_, setBlobs1, deviceId_, 'a');
    DS_ASSERT_OK(client0_->MSetD2H(objectKeys, setBlobs1));

    DS_ASSERT_OK(client0_->Exist(objectKeys, exists));
    EXPECT_THAT(exists, testing::Each(true));
    DS_ASSERT_OK(client1_->Exist(objectKeys, exists));
    EXPECT_THAT(exists, testing::Each(true));

    auto idxToDelete = randomData_.GetRandomIndex(objectKeys.size());
    std::vector<std::string> failedKeys;
    DS_ASSERT_OK(client1_->Delete({ objectKeys[idxToDelete] }, failedKeys));
    ASSERT_TRUE(failedKeys.empty());
    DS_ASSERT_OK(client0_->Exist(objectKeys, exists));
    EXPECT_THAT(exists, testing::Contains(true));
    ASSERT_TRUE(exists[idxToDelete] == false);

    DS_ASSERT_OK(client0_->Delete(objectKeys, failedKeys));
    DS_ASSERT_OK(client0_->Exist(objectKeys, exists));
    EXPECT_THAT(exists, testing::Each(false));
}

TEST_F(HeteroObjectReplicaTest, DISABLED_MSetD2HRepeat)
{
    InitAcl(deviceId_);

    std::vector<std::string> objectKeys;
    numOfObjs_ = 5;  // number of objects is 5
    for (auto i = 0ul; i < numOfObjs_; i++) {
        objectKeys.emplace_back(GetStringUuid());
    }
    std::vector<DeviceBlobList> setBlobs1;
    PrepareDevData(numOfObjs_, blksPerObj_, blkSz_, setBlobs1, deviceId_, 'a');
    DS_ASSERT_OK(client0_->MSetD2H(objectKeys, setBlobs1));
    SetParam setParam{ .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE };
    auto res = client0_->MSetD2H(objectKeys, setBlobs1, setParam);
    DS_ASSERT_TRUE((res.GetMsg().find("not support L2 CACHE write mode") != std::string::npos), true);
    std::vector<bool> exists;
    DS_ASSERT_OK(client0_->Exist(objectKeys, exists));
    EXPECT_THAT(exists, testing::Each(true));

    PrepareDevData(numOfObjs_, blksPerObj_, blkSz_, setBlobs1, deviceId_, 'b');
    DS_ASSERT_OK(client0_->MSetD2H(objectKeys, setBlobs1));

    std::vector<DeviceBlobList> getBlobs1;
    PrepareDevData(numOfObjs_, blksPerObj_, blkSz_, getBlobs1, deviceId_, 'N');
    std::vector<std::string> failedList;
    DS_ASSERT_OK(client0_->MGetH2D(objectKeys, getBlobs1, failedList, DEFAULT_GET_TIMEOUT));
    std::string value(getBlobs1[0].blobs[0].size, 'a');
    for (size_t j = 0; j < numOfObjs_; j++) {
        for (size_t k = 0; k < blkSz_; k++) {
            CheckDevPtrContent(getBlobs1[j].blobs[k].pointer, getBlobs1[j].blobs[k].size, value);
        }
    }
}

TEST_F(HeteroObjectReplicaTest, DISABLED_MSetD2HRepeatDiffWorker)
{
    InitAcl(deviceId_);

    std::vector<std::string> objectKeys;
    numOfObjs_ = 10;  // number of objects is 10
    for (auto i = 0ul; i < numOfObjs_; i++) {
        objectKeys.emplace_back(GetStringUuid());
    }
    std::vector<DeviceBlobList> setBlobs1;
    PrepareDevData(numOfObjs_, blksPerObj_, blkSz_, setBlobs1, deviceId_, 'a');
    DS_ASSERT_OK(client0_->MSetD2H(objectKeys, setBlobs1));

    std::vector<DeviceBlobList> setBlobs2;
    PrepareDevData(numOfObjs_, blksPerObj_, blkSz_, setBlobs2, deviceId_, 'b');
    // client1 can set replica successfully
    DS_ASSERT_OK(client1_->MSetD2H(objectKeys, setBlobs2));

    std::vector<DeviceBlobList> getBlobs1;
    PrepareDevData(numOfObjs_, blksPerObj_, blkSz_, getBlobs1, deviceId_, 'N');
    std::vector<std::string> failedList;
    DS_ASSERT_OK(client1_->MGetH2D(objectKeys, getBlobs1, failedList, DEFAULT_GET_TIMEOUT));
    ASSERT_TRUE(failedList.empty());
    std::string value1(getBlobs1[0].blobs[0].size, 'b');
    for (size_t j = 0; j < numOfObjs_; j++) {
        for (size_t k = 0; k < blkSz_; k++) {
            CheckDevPtrContent(getBlobs1[j].blobs[k].pointer, getBlobs1[j].blobs[k].size, value1);
        }
    }
}
}  // namespace st
}  // namespace datasystem
