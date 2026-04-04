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
 * Description: Test PersistenceApi using SfsClient.
 */
#include <memory>
#include <dirent.h>

#include "common.h"
#include "datasystem/common/l2cache/get_object_info_list_resp.h"
#include "datasystem/common/l2cache/persistence_api.h"
#include "datasystem/common/util/random_data.h"

DS_DECLARE_string(l2_cache_type);
DS_DECLARE_string(sfs_path);

namespace datasystem {
namespace st {
class SfsPersistenceApiTest : public CommonTest {
public:
    void SetUp() override;
    void TearDown() override;

protected:
    void RecursiveDelete(const std::string &dirStr);
    std::unique_ptr<PersistenceApi> pApi_;
    RandomData rd_;
};

void SfsPersistenceApiTest::SetUp()
{
    FLAGS_l2_cache_type = "sfs";
    FLAGS_v = 1;
    FLAGS_sfs_path = std::string(std::getenv("HOME")) + "/sfs_persis_test";
    RecursiveDelete(FLAGS_sfs_path);
    LOG(INFO) << "Creating mock SFS. Path: " << FLAGS_sfs_path;
    ASSERT_EQ(mkdir(FLAGS_sfs_path.c_str(), S_IRWXU | S_IRWXG | S_IROTH), 0);
    pApi_ = PersistenceApi::Create();
    pApi_->Init();
}

void SfsPersistenceApiTest::TearDown()
{
    RecursiveDelete(FLAGS_sfs_path);
}

void SfsPersistenceApiTest::RecursiveDelete(const std::string &dirStr)
{
    struct stat info;
    if (stat(dirStr.c_str(), &info) != 0) {
        return;
    }
    DIR *dir = opendir(dirStr.c_str());
    struct dirent *ent;
    while ((ent = readdir(dir)) != nullptr) {
        if (strcmp(ent->d_name, "..") == 0 || strcmp(ent->d_name, ".") == 0) {
            continue;
        }
        auto fullPath = dirStr + "/" + ent->d_name;
        if (ent->d_type == DT_DIR) {
            RecursiveDelete(fullPath);
            continue;
        }
        if (std::remove(fullPath.c_str()) != 0) {
            LOG(ERROR) << "Failed to remove this path: " << fullPath;
        }
    }
    if (std::remove(dirStr.c_str()) != 0) {
        LOG(ERROR) << "Failed to remove this path: " << dirStr;
    }
}

TEST_F(SfsPersistenceApiTest, TestHappyPath)
{
    // write to different versions of different objects
    size_t timeoutMs = 60000;
    size_t objNum = 10;
    size_t versionLimit = 10;
    std::unordered_map<std::string, std::unordered_map<size_t, std::string>> objMap;

    for (size_t i = 0; i < objNum; ++i) {
        for (size_t j = 1; j <= rd_.GetRandomIndex(versionLimit) + 1; ++j) {
            std::stringstream objName;
            objName << "object" << i;
            std::shared_ptr<std::stringstream> body = std::make_shared<std::stringstream>();
            *body << "hello world:" << i << "_" << j;

            objMap[objName.str()][j] = body->str();
            DS_ASSERT_OK(pApi_->Save(objName.str(), j, timeoutMs, body));
        }
    }

    for (const auto &pr1 : objMap) {
        for (const auto &pr2 : pr1.second) {
            const std::string &name = pr1.first;
            size_t ver = pr2.first;
            const auto &bd = pr2.second;
            std::shared_ptr<std::stringstream> content = std::make_shared<std::stringstream>();
            DS_ASSERT_OK(pApi_->Get(name, ver, timeoutMs, content));
            ASSERT_EQ(bd, content->str());
        }
    }

    for (const auto &pr1 : objMap) {
        const std::string &name = pr1.first;
        size_t maxVer = 0;
        for (const auto &pr2 : pr1.second) {
            size_t ver = pr2.first;
            if (ver > maxVer) {
                maxVer = ver;
            }
        }
        std::shared_ptr<std::stringstream> content = std::make_shared<std::stringstream>();
        DS_ASSERT_OK(pApi_->GetWithoutVersion(name, timeoutMs, 0, content));
        ASSERT_EQ(objMap[name][maxVer], content->str());
        DS_ASSERT_OK(pApi_->Del(name, maxVer, true));
    }
}

TEST_F(SfsPersistenceApiTest, TestTimeOut)
{
    int shortTimeoutMs = 10;
    std::string objName = "object0";
    size_t version = 0;
    std::shared_ptr<std::stringstream> body = std::make_shared<std::stringstream>();
    size_t objectSize = 1024u * 1024u * 50 + 1u;
    *body << rd_.GetRandomString(objectSize);
    DS_ASSERT_NOT_OK(pApi_->Save(objName, version, shortTimeoutMs, body));

    int timeoutMs = 60000;
    std::shared_ptr<std::stringstream> content = std::make_shared<std::stringstream>();
    DS_ASSERT_NOT_OK(pApi_->Get(objName, version, timeoutMs, content));

    // upload again
    body->seekg(0);
    DS_ASSERT_OK(pApi_->Save(objName, version, timeoutMs, body));

    content = std::make_shared<std::stringstream>();
    DS_ASSERT_OK(pApi_->Get(objName, version, timeoutMs, content));
    ASSERT_EQ(body->str(), content->str());
    DS_ASSERT_OK(pApi_->Del(objName, version, true));
}
}
}
