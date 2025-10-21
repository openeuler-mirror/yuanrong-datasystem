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
 * Description: Test SfsClient and its async deletion logic in end-to-end way.
 */

#include <dirent.h>

#include "common.h"
#include "client/object_cache/oc_client_common.h"


using namespace datasystem::object_cache;

namespace datasystem {
namespace st {
class SfsEndToEndTest : public OCClientCommon {
public:
    void RecursiveDelete(const std::string &dirStr);
    std::vector<std::string> ListRegFiles(const std::string &dirPath);
    std::string PollForUnfinishedFileName(const std::string &sfsPath, const std::string &objName, int64_t timeoutSec);
    bool PollForFinishedFileName(const std::string &sfsPath, const std::string &objName, const std::string &fileName,
                                 int64_t timeoutSec);
};

std::vector<std::string> SfsEndToEndTest::ListRegFiles(const std::string &dirPath)
{
    struct stat info;
    if (stat(dirPath.c_str(), &info) != 0) {
        return {};
    }
    std::vector<std::string> ans;
    DIR *dir = opendir(dirPath.c_str());
    struct dirent *ent;
    while ((ent = readdir(dir)) != nullptr) {
        if (strcmp(ent->d_name, "..") == 0 || strcmp(ent->d_name, ".") == 0) {
            continue;
        }
        if (ent->d_type == DT_REG) {
            ans.push_back(ent->d_name);
        }
    }
    return ans;
}

std::string SfsEndToEndTest::PollForUnfinishedFileName(const std::string &sfsPath, const std::string &objName,
                                                       int64_t timeoutSec)
{
    int64_t timeoutNano = timeoutSec * 1000 * 1000 * 1000;
    std::string objPath = sfsPath + "/datasystem/" + objName;
    auto start = std::chrono::system_clock::now();
    while ((std::chrono::system_clock::now() - start).count() < timeoutNano) {
        const auto &files = ListRegFiles(objPath);
        for (const auto &name : files) {
            if (name.back() == '_') {
                return name;
            }
        }
    }
    return "";
}

bool SfsEndToEndTest::PollForFinishedFileName(const std::string &sfsPath, const std::string &objName,
                                              const std::string &fileName, int64_t timeoutSec)
{
    int64_t timeoutNano = timeoutSec * 1000 * 1000 * 1000;
    std::string objPath = sfsPath + "/datasystem/" + objName;
    auto start = std::chrono::system_clock::now();
    while ((std::chrono::system_clock::now() - start).count() < timeoutNano) {
        const auto &files = ListRegFiles(objPath);
        for (const auto &name : files) {
            if (name == fileName) {
                return true;
            }
        }
    }
    return false;
}

void SfsEndToEndTest::RecursiveDelete(const std::string &dirStr)
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

class SfsEndToEndTestAsync : public SfsEndToEndTest {
public:
void SetClusterSetupOptions(ExternalClusterOptions &opts)
{
    RecursiveDelete(sfsPath_);
    LOG(INFO) << "Creating mock SFS. Path: " << sfsPath_;
    ASSERT_EQ(mkdir(sfsPath_.c_str(), S_IRWXU | S_IRWXG | S_IROTH), 0);
    std::stringstream ss;
    ss << "-l2_cache_type=sfs "
       << "-sfs_path=" << sfsPath_ << " "
       << "-v=1";
    opts.workerGflagParams = ss.str();
    opts.numEtcd = 1;
}

void TearDown()
{
    RecursiveDelete(sfsPath_);
}

protected:
    std::string sfsPath_ = std::string(std::getenv("HOME")) + "/sfs_async_send";
};

TEST_F(SfsEndToEndTestAsync, DISABLED_TestAsyncSendTimedOutThenAutoRetry)
{
    std::shared_ptr<KVClient> client;
    InitTestKVClient(0, client);

    constexpr size_t valLen = 10 * 1024 * 1024;
    std::string value(valLen, 'a');
    SetParam param1{ .writeMode = WriteMode::WRITE_BACK_L2_CACHE };

    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "PersistenceApi.Save.timeout", "1*call(5)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "AsyncSendManager.Sender.verify", "call()"));

    DS_ASSERT_OK(client->Set("key_1", value, param1));
    int timeout = 20;
    auto unfinished = PollForUnfinishedFileName(sfsPath_, "key_1", timeout);
    ASSERT_TRUE(!unfinished.empty());
    LOG(INFO) << "The file failed to finish during upload: " << unfinished;

    int sleepTime = 10;
    std::this_thread::sleep_for(std::chrono::seconds(sleepTime));
    std::string finished = unfinished.substr(0, unfinished.size() - 1);
    int timeout2 = 5;
    ASSERT_TRUE(PollForFinishedFileName(sfsPath_, "key_1", finished, timeout2));
}

class SfsEndToEndTestSync : public SfsEndToEndTest {
public:
void SetClusterSetupOptions(ExternalClusterOptions &opts)
{
    RecursiveDelete(sfsPath_);
    LOG(INFO) << "Creating mock SFS. Path: " << sfsPath_;
    ASSERT_EQ(mkdir(sfsPath_.c_str(), S_IRWXU | S_IRWXG | S_IROTH), 0);
    std::stringstream ss;
    ss << "-l2_cache_type=sfs "
       << "-sfs_path=" << sfsPath_ << " "
       << "-v=1";
    opts.workerGflagParams = ss.str();
    opts.numEtcd = 1;
}

void TearDown()
{
    RecursiveDelete(sfsPath_);
}

protected:
    std::string sfsPath_ = std::string(std::getenv("HOME")) + "/sfs_sync_send";
};

TEST_F(SfsEndToEndTestSync, TestSyncSendTimedOutThenAsyncDelete)
{
    std::shared_ptr<KVClient> client;
    InitTestKVClient(0, client);

    constexpr size_t valLen = 10 * 1024 * 1024;
    std::string value(valLen, 'a');
    SetParam param1{ .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE };

    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "PersistenceApi.Save.timeout", "1*call(5)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "worker.DelPersistence.delay", "call(5)"));
    // timeout
    DS_ASSERT_NOT_OK(client->Set("key_1", value, param1));
    int timeout = 20;
    auto unfinished = PollForUnfinishedFileName(sfsPath_, "key_1", timeout);
    ASSERT_TRUE(!unfinished.empty());
    LOG(INFO) << "The file failed to finish during upload: " << unfinished;
    DS_ASSERT_OK(client->Del("key_1"));
    int sleepTime = 10;
    std::this_thread::sleep_for(std::chrono::seconds(sleepTime));
    std::string objPath = sfsPath_ + "/datasystem/key_1";
    auto files = ListRegFiles(objPath);
    ASSERT_TRUE(files.empty());
}
}  // namespace st
}  // namespace datasystem