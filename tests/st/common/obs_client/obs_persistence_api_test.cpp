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
 * Description: Persistence API using OBS client.
 */

#include "common.h"
#include "datasystem/common/l2cache/get_object_info_list_resp.h"
#include "datasystem/common/l2cache/persistence_api.h"
#include "datasystem/common/util/random_data.h"

DS_DECLARE_string(l2_cache_type);
DS_DECLARE_string(obs_endpoint);
DS_DECLARE_string(obs_bucket);
DS_DECLARE_string(obs_access_key);
DS_DECLARE_string(obs_secret_key);
DS_DECLARE_bool(obs_https_enabled);

namespace datasystem {
namespace st {
class ObsPersistenceApiTest : public CommonTest {
public:
    void SetUp() override;

protected:
    RandomData randData_;
    const std::string obsEndpoint_ = "ddl.test.huawei.com:19000";
    const std::string obsAk_ = "3rtJpvkP4zowTDsx6XiE";
    const std::string obsSk_ = "SJx5Zecs7SL7I6Au9XpylG9LwPF29kMwIxisI5Xs";
    const std::string bucket_ = "test";
    size_t timeout_ = 60000;
    std::unique_ptr<PersistenceApi> pApi_;
};

void ObsPersistenceApiTest::SetUp()
{
    FLAGS_l2_cache_type = "obs";
    FLAGS_obs_endpoint = obsEndpoint_;
    FLAGS_obs_bucket = bucket_;
    FLAGS_obs_access_key = obsAk_;
    FLAGS_obs_secret_key = obsSk_;
    FLAGS_obs_https_enabled = true;
    pApi_ = std::make_unique<PersistenceApi>();
    pApi_->Init();
}

TEST_F(ObsPersistenceApiTest, SaveSmallObject)
{
    size_t sz = 100;
    std::string key = "SaveSmallObject";
    std::shared_ptr<std::stringstream> buf = std::make_shared<std::stringstream>(randData_.GetRandomString(sz));
    DS_ASSERT_OK(pApi_->Save(key, 0, timeout_, buf));

    std::shared_ptr<std::stringstream> content;
    DS_ASSERT_OK(pApi_->Get(key, 0, timeout_, content));
    ASSERT_EQ(content->str(), buf->str());

    DS_ASSERT_OK(pApi_->Del(key, 0, true));
    DS_ASSERT_NOT_OK(pApi_->Get(key, 0, timeout_, content));
}

TEST_F(ObsPersistenceApiTest, LEVEL2_SaveLargeObject)
{
    size_t sz = 200 * 1024 * 1024;
    std::string key = "SaveLargeObject";
    std::shared_ptr<std::stringstream> buf = std::make_shared<std::stringstream>(randData_.GetRandomString(sz));
    DS_ASSERT_OK(pApi_->Save(key, 0, timeout_, buf));

    std::shared_ptr<std::stringstream> content;
    DS_ASSERT_OK(pApi_->Get(key, 0, timeout_, content));
    ASSERT_EQ(content->str(), buf->str());

    DS_ASSERT_OK(pApi_->Del(key, 0, true));
    DS_ASSERT_NOT_OK(pApi_->Get(key, 0, timeout_, content));
}

TEST_F(ObsPersistenceApiTest, GetLatestFromMultiVersion)
{
    size_t sz = 200 * 1024;
    std::string key = "GetLatest";
    std::shared_ptr<std::stringstream> buf1 = std::make_shared<std::stringstream>(randData_.GetRandomString(sz));
    std::shared_ptr<std::stringstream> buf2 = std::make_shared<std::stringstream>(randData_.GetRandomString(sz));
    DS_ASSERT_OK(pApi_->Save(key, 1, timeout_, buf1));
    DS_ASSERT_OK(pApi_->Save(key, 2, timeout_, buf2));

    std::shared_ptr<std::stringstream> content1, content2;
    DS_ASSERT_OK(pApi_->Get(key, 1, timeout_, content1));
    ASSERT_EQ(content1->str(), buf1->str());

    DS_ASSERT_OK(pApi_->GetWithoutVersion(key, timeout_, 0, content2));
    ASSERT_EQ(content2->str(), buf2->str());
}

TEST_F(ObsPersistenceApiTest, DISABLED_DeleteMultiVersion1)
{
    size_t objNum = 100;
    size_t sz = 100;
    std::string key = "DeleteMultiVersion1";
    for (size_t i = 0; i < objNum; ++i) {
        std::shared_ptr<std::stringstream> buf = std::make_shared<std::stringstream>(randData_.GetRandomString(sz));
        DS_ASSERT_OK(pApi_->Save(key, i, timeout_, buf));
    }
    for (size_t i = 0; i < objNum; ++i) {
        std::shared_ptr<std::stringstream> content;
        DS_ASSERT_OK(pApi_->Get(key, i, timeout_, content));
    }
    DS_ASSERT_OK(pApi_->Del(key, objNum - 1, true));
    for (size_t i = 0; i < objNum; ++i) {
        std::shared_ptr<std::stringstream> content;
        DS_ASSERT_NOT_OK(pApi_->Get(key, i, timeout_, content));
    }
}

TEST_F(ObsPersistenceApiTest, DISABLED_LEVEL1_DeleteMultiVersion2)
{
    size_t objNum = 100;
    size_t sz = 100;
    std::string key = "DeleteMultiVersion2";
    for (size_t i = 0; i < objNum; ++i) {
        std::shared_ptr<std::stringstream> buf = std::make_shared<std::stringstream>(randData_.GetRandomString(sz));
        DS_ASSERT_OK(pApi_->Save(key, i, timeout_, buf));
    }
    for (size_t i = 0; i < objNum; ++i) {
        std::shared_ptr<std::stringstream> content;
        DS_ASSERT_OK(pApi_->Get(key, i, timeout_, content));
    }
    DS_ASSERT_OK(pApi_->Del(key, objNum, false));
    for (size_t i = 0; i < objNum - 1; ++i) {
        std::shared_ptr<std::stringstream> content;
        DS_ASSERT_NOT_OK(pApi_->Get(key, i, timeout_, content));
    }
    std::shared_ptr<std::stringstream> content;
    DS_ASSERT_OK(pApi_->Get(key, objNum - 1, timeout_, content));
    DS_ASSERT_OK(pApi_->Del(key, objNum - 1, true));
    for (size_t i = 0; i < objNum; ++i) {
        std::shared_ptr<std::stringstream> content;
        DS_ASSERT_NOT_OK(pApi_->Get(key, i, timeout_, content));
    }
}

TEST_F(ObsPersistenceApiTest, DISABLED_LEVEL1_DeleteMultiVersion3)
{
    size_t objNum = 100;
    size_t sz = 100;
    std::string key = "DeleteMultiVersion3";
    for (size_t i = 0; i < objNum; ++i) {
        std::shared_ptr<std::stringstream> buf = std::make_shared<std::stringstream>(randData_.GetRandomString(sz));
        DS_ASSERT_OK(pApi_->Save(key, i, timeout_, buf));
    }
    for (size_t i = 0; i < objNum; ++i) {
        std::shared_ptr<std::stringstream> content;
        DS_ASSERT_OK(pApi_->Get(key, i, timeout_, content));
    }
    const int fifty = 50;
    DS_ASSERT_OK(pApi_->Del(key, objNum - fifty, false));
    for (size_t i = 0; i <= objNum - fifty; ++i) {
        std::shared_ptr<std::stringstream> content;
        DS_ASSERT_NOT_OK(pApi_->Get(key, i, timeout_, content));
    }
    for (size_t i = objNum - fifty + 1; i < objNum; ++i) {
        std::shared_ptr<std::stringstream> content;
        DS_ASSERT_OK(pApi_->Get(key, i, timeout_, content));
    }
    DS_ASSERT_OK(pApi_->Del(key, objNum - 1, true));
    for (size_t i = 0; i < objNum; ++i) {
        std::shared_ptr<std::stringstream> content;
        DS_ASSERT_NOT_OK(pApi_->Get(key, i, timeout_, content));
    }
}
}
}
