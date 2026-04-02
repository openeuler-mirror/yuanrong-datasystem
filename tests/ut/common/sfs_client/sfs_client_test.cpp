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
 * Description: Test SfsClient.
 */

#include <memory>
#include <string>
#include <dirent.h>
#include <pwd.h>
#include <sys/types.h>
#include <unistd.h>

#include "ut/common.h"

#include "datasystem/common/l2cache/get_object_info_list_resp.h"
#include "datasystem/common/l2cache/sfs_client/sfs_client.h"
#include "datasystem/common/util/random_data.h"

DS_DECLARE_string(sfs_path);

namespace datasystem {
namespace ut {
class SfsClientTest : public CommonTest {
public:
    void SetUp() override;
    void TearDown() override;

protected:
    void RecursiveDelete(const std::string &dirStr);
    RandomData rd_;
};

void SfsClientTest::SetUp()
{
    FLAGS_v = 1;

    // Use /tmp for test directory to avoid permission issues
    FLAGS_sfs_path = "/tmp/sfs_test_" + std::to_string(getpid());
    RecursiveDelete(FLAGS_sfs_path);
    LOG(INFO) << "Creating mock SFS. Path: " << FLAGS_sfs_path;
    ASSERT_EQ(mkdir(FLAGS_sfs_path.c_str(), S_IRWXU | S_IRWXG | S_IROTH), 0);
}

void SfsClientTest::TearDown()
{
    RecursiveDelete(FLAGS_sfs_path);
}

void SfsClientTest::RecursiveDelete(const std::string &dirStr)
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

TEST_F(SfsClientTest, TestHappyPath)
{
    auto client = std::make_unique<SfsClient>(FLAGS_sfs_path);
    DS_ASSERT_OK(client->Init());
    // write to different versions of different objects
    int timeoutMs = 60000;
    size_t objNum = 100;
    size_t versionLimit = 100;
    std::unordered_map<std::string, std::string> objMap;
    for (size_t i = 0; i < objNum; ++i) {
        for (size_t j = 0; j <= rd_.GetRandomIndex(versionLimit); ++j) {
            std::stringstream objName;
            objName << "object" << i << "/" << j;
            std::shared_ptr<std::stringstream> body = std::make_shared<std::stringstream>();
            *body << "hello world:" << i << "_" << j;

            objMap[objName.str()] = body->str();
            DS_ASSERT_OK(client->Upload(objName.str(), timeoutMs, body));
        }
    }

    std::string prefix = "object";
    std::shared_ptr<GetObjectInfoListResp> infoRsp = std::make_shared<GetObjectInfoListResp>();
    DS_ASSERT_OK(client->List(prefix, timeoutMs, false, infoRsp));
    auto info = infoRsp->GetObjectInfo();
    for (auto objInfo : info) {
        const std::string &key = objInfo.key;
        auto it = objMap.find(key);
        ASSERT_NE(it, objMap.end());
        LOG(INFO) << "Last modified time: " << objInfo.lastModified;
        ASSERT_EQ(objInfo.size, it->second.size());
        auto pos = it->first.find_last_of('/');
        size_t ver = std::stoi(it->first.substr(pos + 1));
        ASSERT_EQ(objInfo.version, ver);
    }

    for (auto pr : objMap) {
        const std::string &key = pr.first;
        std::shared_ptr<std::stringstream> content = std::make_shared<std::stringstream>();
        DS_ASSERT_OK(client->Download(key, timeoutMs, content));
        ASSERT_EQ(pr.second, content->str());
    }

    std::vector<std::string> toDelete;
    std::transform(objMap.begin(), objMap.end(), std::back_inserter(toDelete),
        [](std::unordered_map<std::string, std::string>::value_type &pr) { return pr.first; });
    DS_ASSERT_OK(client->Delete(toDelete));
}

TEST_F(SfsClientTest, TestBigObject)
{
    auto client = std::make_unique<SfsClient>(FLAGS_sfs_path);
    DS_ASSERT_OK(client->Init());
    int timeoutMs = 60000;
    std::string objName = "object0/0";
    size_t objectSize = 1024u * 1024u + 1u;
    std::shared_ptr<std::stringstream> body = std::make_shared<std::stringstream>();
    *body << rd_.GetRandomString(objectSize);
    DS_ASSERT_OK(client->Upload(objName, timeoutMs, body));

    std::string prefix = "object";
    std::shared_ptr<GetObjectInfoListResp> infoRsp = std::make_shared<GetObjectInfoListResp>();
    DS_ASSERT_OK(client->List(prefix, timeoutMs, false, infoRsp));
    auto info = infoRsp->GetObjectInfo();
    ASSERT_EQ(info.size(), 1u);
    const auto &objInfo = info[0];
    const std::string &key = objInfo.key;
    ASSERT_EQ(key, objName);
    LOG(INFO) << "Last modified time: " << objInfo.lastModified;
    ASSERT_EQ(objInfo.size, objectSize);
    ASSERT_EQ(objInfo.version, 0);

    std::shared_ptr<std::stringstream> content = std::make_shared<std::stringstream>();
    DS_ASSERT_OK(client->Download(objName, timeoutMs, content));
    ASSERT_EQ(body->str(), content->str());

    std::vector<std::string> toDelete{objName};
    DS_ASSERT_OK(client->Delete(toDelete));

    // check if deletion is successful
    std::shared_ptr<GetObjectInfoListResp> infoRsp2 = std::make_shared<GetObjectInfoListResp>();
    DS_ASSERT_OK(client->List(prefix, timeoutMs, true, infoRsp2));
    info = infoRsp2->GetObjectInfo();
    ASSERT_EQ(info.size(), 0u);
}

TEST_F(SfsClientTest, TestTimeOut)
{
    auto client = std::make_unique<SfsClient>(FLAGS_sfs_path);
    DS_ASSERT_OK(client->Init());
    int timeoutMs = 10;
    std::string objName = "object0/0";
    size_t objectSize = 1024u * 1024u * 10 + 1u;
    std::shared_ptr<std::stringstream> body = std::make_shared<std::stringstream>();
    *body << rd_.GetRandomString(objectSize);
    // expect timeout when writing a big object
    auto rc = client->Upload(objName, timeoutMs, body);
    LOG(INFO) << rc.GetMsg();
    DS_ASSERT_NOT_OK(rc);

    std::string prefix = "object";
    std::shared_ptr<GetObjectInfoListResp> infoRsp = std::make_shared<GetObjectInfoListResp>();
    // list objects including those not finished
    // their key will come with a suffix "_"
    DS_ASSERT_OK(client->List(prefix, timeoutMs, true, infoRsp));
    std::vector<datasystem::L2CacheObjectInfo> info = infoRsp->GetObjectInfo();
    ASSERT_EQ(info.size(), 1u);
    const auto &objInfo = info[0];
    const std::string &key = objInfo.key;
    LOG(INFO) << "The key of the object not finished: " << key;
    ASSERT_EQ(key, objName + "_");
    LOG(INFO) << "Last modified time: " << objInfo.lastModified;
    ASSERT_TRUE(objInfo.size < objectSize);
    ASSERT_EQ(objInfo.version, 0);

    // should be able to delete it
    std::vector<std::string> toDelete{objName};
    DS_ASSERT_OK(client->Delete(toDelete));

    // check if deletion is successful
    std::shared_ptr<GetObjectInfoListResp> infoRsp2 = std::make_shared<GetObjectInfoListResp>();
    DS_ASSERT_OK(client->List(prefix, timeoutMs, true, infoRsp2));
    info = infoRsp2->GetObjectInfo();
    ASSERT_EQ(info.size(), 0u);
}

TEST_F(SfsClientTest, TestTimeOutThenUpload)
{
    auto client = std::make_unique<SfsClient>(FLAGS_sfs_path);
    DS_ASSERT_OK(client->Init());
    int shortTimeoutMs = 10;
    std::string objName = "object0/0";
    size_t objectSize = 1024u * 1024u * 10;
    std::shared_ptr<std::stringstream> body = std::make_shared<std::stringstream>();
    *body << rd_.GetRandomString(objectSize);
    // expect timeout when writing a big object
    DS_ASSERT_NOT_OK(client->Upload(objName, shortTimeoutMs, body));

    std::string prefix = "object";
    std::shared_ptr<GetObjectInfoListResp> infoRsp = std::make_shared<GetObjectInfoListResp>();
    DS_ASSERT_OK(client->List(prefix, shortTimeoutMs, true, infoRsp));
    std::vector<datasystem::L2CacheObjectInfo> info = infoRsp->GetObjectInfo();
    ASSERT_EQ(info.size(), 1u);
    ASSERT_EQ(info[0].key, objName + "_");

    // upload again successfully
    int timeoutMs = 60000;
    body->seekg(0);
    DS_ASSERT_OK(client->Upload(objName, timeoutMs, body));

    // list again
    // should get only object0/0 but not object0/0_
    std::shared_ptr<GetObjectInfoListResp> infoRsp2 = std::make_shared<GetObjectInfoListResp>();
    DS_ASSERT_OK(client->List(prefix, timeoutMs, true, infoRsp2));
    info = infoRsp2->GetObjectInfo();
    ASSERT_EQ(info.size(), 1u);
    ASSERT_EQ(info[0].key, objName);

    std::shared_ptr<std::stringstream> content = std::make_shared<std::stringstream>();
    DS_ASSERT_OK(client->Download(objName, timeoutMs, content));
    ASSERT_EQ(body->str(), content->str());

    std::vector<std::string> toDelete{objName};
    DS_ASSERT_OK(client->Delete(toDelete));
}

TEST_F(SfsClientTest, DISABLED_TestTimeoutDownload)
{
    auto client = std::make_unique<SfsClient>(FLAGS_sfs_path);
    DS_ASSERT_OK(client->Init());
    int timeoutMs = 60000;
    std::string objName = "object0/0";
    size_t objectSize = 1024u * 1024u * 10;
    std::shared_ptr<std::stringstream> body = std::make_shared<std::stringstream>();
    *body << rd_.GetRandomString(objectSize);
    // expect timeout when writing a big object
    DS_ASSERT_OK(client->Upload(objName, timeoutMs, body));

    int shortTimeoutMs = 10;
    std::shared_ptr<std::stringstream> content = std::make_shared<std::stringstream>();
    auto rc = client->Download(objName, shortTimeoutMs, content);
    LOG(INFO) << rc.GetMsg();
    DS_ASSERT_NOT_OK(rc);
    ASSERT_NE(body->str(), content->str());

    std::vector<std::string> toDelete{objName};
    DS_ASSERT_OK(client->Delete(toDelete));
}
}
}