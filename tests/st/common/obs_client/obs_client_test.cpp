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
 * Description: OBS client test.
 */

#include "common.h"
#include "datasystem/common/encrypt/secret_manager.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/l2cache/get_object_info_list_resp.h"
#include "datasystem/common/l2cache/obs_client/obs_client.h"
#include "datasystem/common/util/file_util.h"
#include "datasystem/common/util/validator.h"
#include "datasystem/common/util/random_data.h"


DS_DECLARE_string(obs_access_key);
DS_DECLARE_string(obs_secret_key);
DS_DECLARE_string(encrypt_kit);
DS_DECLARE_bool(obs_https_enabled);

namespace datasystem {
namespace st {
class ObsClientTest : public CommonTest {
public:
    void SetUp() override;

protected:

    void StreamingUpload();

    RandomData randData_;
    const std::string obsEndpoint_ = "ddl.test.com:19000";
    const std::string obsAk_ = "3rtJpvkP4zowTDsx6XiE";
    const std::string obsSk_ = "SJx5Zecs7SL7I6Au9XpylG9LwPF29kMwIxisI5Xs";
    const std::string bucket_ = "test";
    size_t timeout_ = 60000;
    std::unique_ptr<L2CacheClient> client_;
};

void ObsClientTest::SetUp()
{
    FLAGS_obs_access_key = obsAk_;
    FLAGS_obs_secret_key = obsSk_;
    FLAGS_obs_https_enabled = true;
    client_ = std::make_unique<ObsClient>(obsEndpoint_, bucket_);
    client_->Init();
}

void ObsClientTest::StreamingUpload()
{
    size_t sz = 100;
    std::shared_ptr<std::stringstream> buf = std::make_shared<std::stringstream>(randData_.GetRandomString(sz));
    std::string objPath = "StreamingUploadObj/0";
    DS_ASSERT_OK(client_->Upload(objPath, timeout_, buf));
    std::shared_ptr<GetObjectInfoListResp> listResp = std::make_shared<GetObjectInfoListResp>();
    DS_ASSERT_OK(client_->List(objPath, timeout_, false, listResp));
    auto list = listResp->GetObjectInfo();
    ASSERT_EQ(list.size(), 1u);
    ASSERT_EQ(list[0].key, objPath);
    std::shared_ptr<std::stringstream> outSs;
    client_->Download(objPath, timeout_, outSs);
    ASSERT_EQ(outSs->str(), buf->str());
    DS_ASSERT_OK(client_->Delete({ objPath }));
    std::shared_ptr<GetObjectInfoListResp> listResp2 = std::make_shared<GetObjectInfoListResp>();
    DS_ASSERT_OK(client_->List(objPath, timeout_, false, listResp2));
    list = listResp2->GetObjectInfo();
    ASSERT_EQ(list.size(), 0u);
}

TEST_F(ObsClientTest, DISABLED_StreamingUpload)
{
    StreamingUpload();
}

TEST_F(ObsClientTest, DISABLED_MultiPartUpload)
{
    size_t sz = 201 * 1024 * 1024;
    std::shared_ptr<std::stringstream> buf = std::make_shared<std::stringstream>(randData_.GetRandomString(sz));
    std::string objPath = "MultiPartUploadObj/0";
    DS_ASSERT_OK(client_->Upload(objPath, timeout_, buf));
    std::shared_ptr<GetObjectInfoListResp> listResp = std::make_shared<GetObjectInfoListResp>();
    DS_ASSERT_OK(client_->List(objPath, timeout_, false, listResp));
    auto list = listResp->GetObjectInfo();
    ASSERT_EQ(list.size(), 1u);
    ASSERT_EQ(list[0].key, objPath);
    std::shared_ptr<std::stringstream> outSs;
    client_->Download(objPath, timeout_, outSs);
    ASSERT_EQ(outSs->str(), buf->str());
    DS_ASSERT_OK(client_->Delete({ objPath }));
    std::shared_ptr<GetObjectInfoListResp> listResp2 = std::make_shared<GetObjectInfoListResp>();
    DS_ASSERT_OK(client_->List(objPath, timeout_, false, listResp2));
    list = listResp2->GetObjectInfo();
    ASSERT_EQ(list.size(), 0u);
}

TEST_F(ObsClientTest, DISABLED_ConcurrentUploadDownload)
{
    size_t numThread = 10;
    std::vector<std::shared_ptr<std::stringstream>> buffers(numThread);
    std::vector<std::string> paths(numThread);

    auto job = [this, &buffers, &paths] (int i) {
        static const size_t bound = 101 * 1024 * 1024;
        size_t sz = randData_.GetRandomUint64(1, bound);
        std::shared_ptr<std::stringstream> buf = std::make_shared<std::stringstream>(randData_.GetRandomString(sz));
        buffers[i] = buf;
        std::string objPath = "ConcurrentUploadObj/";
        objPath += std::to_string(i) + "/0";
        paths[i] = objPath;
        DS_ASSERT_OK(client_->Upload(objPath, timeout_, buf));
        std::shared_ptr<std::stringstream> outSs;
        client_->Download(objPath, timeout_, outSs);
        ASSERT_EQ(outSs->str(), buf->str());
    };

    std::vector<std::thread> threads;
    for (size_t i = 0; i < numThread; ++i) {
        threads.emplace_back(job, i);
    }
    for (auto &t : threads) {
        t.join();
    }

    std::sort(paths.begin(), paths.end());
    std::shared_ptr<GetObjectInfoListResp> listResp = std::make_shared<GetObjectInfoListResp>();
    DS_ASSERT_OK(client_->List("ConcurrentUploadObj/", timeout_, false, listResp));
    auto list = listResp->GetObjectInfo();
    ASSERT_EQ(list.size(), numThread);
    for (size_t i = 0; i < numThread; ++i) {
        ASSERT_EQ(list[i].key, paths[i]);
    }

    DS_ASSERT_OK(client_->Delete(paths));
    std::shared_ptr<GetObjectInfoListResp> listResp2 = std::make_shared<GetObjectInfoListResp>();
    DS_ASSERT_OK(client_->List("ConcurrentUploadObj/", timeout_, false, listResp2));
    list = listResp2->GetObjectInfo();
    ASSERT_EQ(list.size(), 0u);
}

TEST_F(ObsClientTest, DISABLED_ConcurrentOperations)
{
    size_t numThread = 50;
    auto job = [this] (int i) {
        static const size_t bound = 200;
        size_t sz = randData_.GetRandomUint64(1, bound);
        std::shared_ptr<std::stringstream> buf = std::make_shared<std::stringstream>(randData_.GetRandomString(sz));
        std::string objPath = "ConcurrentOperations/";
        objPath += std::to_string(i) + "/0";

        DS_ASSERT_OK(client_->Upload(objPath, timeout_, buf));

        std::shared_ptr<GetObjectInfoListResp> listResp = std::make_shared<GetObjectInfoListResp>();
        DS_ASSERT_OK(client_->List(objPath, timeout_, false, listResp));
        auto list = listResp->GetObjectInfo();
        ASSERT_EQ(list.size(), 1u);

        std::shared_ptr<std::stringstream> outSs;
        client_->Download(objPath, timeout_, outSs);
        ASSERT_EQ(outSs->str(), buf->str());

        DS_ASSERT_OK(client_->Delete({ objPath }));
        std::shared_ptr<GetObjectInfoListResp> listResp2 = std::make_shared<GetObjectInfoListResp>();
        DS_ASSERT_OK(client_->List("objPath", timeout_, false, listResp2));
        list = listResp2->GetObjectInfo();
        ASSERT_EQ(list.size(), 0u);
    };

    std::vector<std::thread> threads;
    for (size_t i = 0; i < numThread; ++i) {
        threads.emplace_back(job, i);
    }
    for (auto &t : threads) {
        t.join();
    }
}

TEST_F(ObsClientTest, DISABLED_ListLargeAmountObjects)
{
    size_t numObj = 2000;
    std::vector<std::string> paths(numObj);

    for (size_t i = 0; i < numObj; ++i) {
        static const size_t bound = 100;
        size_t sz = randData_.GetRandomUint64(1, bound);
        std::shared_ptr<std::stringstream> buf = std::make_shared<std::stringstream>(randData_.GetRandomString(sz));
        std::string objPath = "ListLargeAmountObjects/";
        objPath += std::to_string(i) + "/0";
        paths[i] = objPath;
        DS_ASSERT_OK(client_->Upload(objPath, timeout_, buf));
    }

    std::sort(paths.begin(), paths.end());
    std::shared_ptr<GetObjectInfoListResp> listResp = std::make_shared<GetObjectInfoListResp>();
    DS_ASSERT_OK(client_->List("ListLargeAmountObjects/", timeout_, false, listResp));
    auto list = listResp->GetObjectInfo();
    ASSERT_EQ(list.size(), numObj);
    for (size_t i = 0; i < numObj; ++i) {
        ASSERT_EQ(list[i].key, paths[i]);
    }

    DS_ASSERT_OK(client_->Delete(paths));
    std::shared_ptr<GetObjectInfoListResp> listResp2 = std::make_shared<GetObjectInfoListResp>();
    DS_ASSERT_OK(client_->List("ListLargeAmountObjects/", timeout_, false, listResp2));
    list = listResp2->GetObjectInfo();
    ASSERT_EQ(list.size(), 0u);
}

TEST_F(ObsClientTest, DISABLED_ListMultiVersion)
{
    size_t numObj = 100;
    std::vector<std::string> paths(numObj);

    std::string randomPrefix = RandomData().GetRandomString(5);

    for (size_t i = 0; i < numObj; ++i) {
        static const size_t bound = 100;
        size_t sz = randData_.GetRandomUint64(1, bound);
        std::shared_ptr<std::stringstream> buf = std::make_shared<std::stringstream>(randData_.GetRandomString(sz));
        std::string objPath = "ListMultiVersion_";
        objPath += randomPrefix;
        objPath += "/";
        objPath += std::to_string(i);
        paths[i] = objPath;
        DS_ASSERT_OK(client_->Upload(objPath, timeout_, buf));
    }

    std::sort(paths.begin(), paths.end());
    std::shared_ptr<GetObjectInfoListResp> listResp = std::make_shared<GetObjectInfoListResp>();
    DS_ASSERT_OK(client_->List("ListMultiVersion_" + randomPrefix, timeout_, false, listResp));
    auto list = listResp->GetObjectInfo();
    ASSERT_EQ(list.size(), numObj);
    for (size_t i = 0; i < numObj; ++i) {
        ASSERT_EQ(list[i].key, paths[i]);
    }
    ASSERT_EQ(listResp->MaxVersion(), numObj - 1);
    ASSERT_EQ(listResp->NextMarker(), paths.back());

    DS_ASSERT_OK(client_->Delete(paths));
    const int kDeleteWaitTimeSec = 2;
    std::this_thread::sleep_for(std::chrono::seconds(kDeleteWaitTimeSec));
    std::shared_ptr<GetObjectInfoListResp> listResp2 = std::make_shared<GetObjectInfoListResp>();
    DS_ASSERT_OK(client_->List("ListMultiVersion_" + randomPrefix, timeout_, false, listResp2));
    list = listResp2->GetObjectInfo();
    ASSERT_EQ(list.size(), 0u);
}

TEST_F(ObsClientTest, DISABLED_HandleFailedUpload)
{
    int timeoutMs = 5000;
    DS_ASSERT_OK(datasystem::inject::Set("ObsClient.OnePartUpload.sleepReturnFailure", "1*call(10000)"));
    size_t sz = 201 * 1024 * 1024;
    std::shared_ptr<std::stringstream> buf = std::make_shared<std::stringstream>(randData_.GetRandomString(sz));
    std::string objPath = "MultiPartUploadObj/0";
    Status rc = client_->Upload(objPath, timeoutMs, buf);
    ASSERT_EQ(rc.GetCode(), K_RUNTIME_ERROR);
    LOG(ERROR) << rc.GetMsg();
    std::shared_ptr<GetObjectInfoListResp> listResp = std::make_shared<GetObjectInfoListResp>();
    DS_ASSERT_OK(client_->List(objPath, timeout_, false, listResp));
    auto list = listResp->GetObjectInfo();
    ASSERT_EQ(list.size(), 0u);
}
}
}