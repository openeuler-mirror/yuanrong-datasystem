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
 * Description: Test SpillRequestHandler and SpillFileManager.
 */
#include "datasystem/worker/object_cache/worker_oc_spill.h"
#include "ut/common.h"
#include "datasystem/common/constants.h"
#include "datasystem/common/iam/tenant_auth_manager.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/perf/perf_manager.h"
#include "datasystem/common/util/file_util.h"
#include "datasystem/common/util/random_data.h"
#include "datasystem/common/util/thread_pool.h"
#include "datasystem/worker/object_cache/obj_cache_shm_unit.h"

DS_DECLARE_string(spill_directory);
DS_DECLARE_uint64(spill_file_open_limit);
DS_DECLARE_uint64(spill_file_max_size_mb);

using namespace datasystem::object_cache;

namespace datasystem {
namespace ut {

class SpillFileManagerTest : public CommonTest {
    void SetUp() override
    {
        FLAGS_v = 0;
        LOG_IF_ERROR(inject::Set("worker.Spill.Sync", "return()"), "set inject point failed");
    }
};

TEST_F(SpillFileManagerTest, TestWriteToFile)
{
    std::shared_ptr<SpillFileManager> fileMgr_ = std::make_shared<SpillFileManager>();
    std::string objectKey = "ThisisAObject";
    std::string in = "Test1234567890";
    fileMgr_->Init({ "./spill_TestWriteToFile" });
    DS_EXPECT_OK(fileMgr_->Spill(objectKey, &in.front(), in.size()));
    std::string out;
    out.resize(in.size());
    DS_EXPECT_OK(fileMgr_->LoadFromDisk(objectKey, &out.front(), in.size(), 0));
    ASSERT_EQ(in, out);
    uint64_t decSize;
    DS_EXPECT_OK(fileMgr_->DeleteFromDisk(objectKey, decSize));
    ASSERT_EQ(decSize, in.size());
}

TEST_F(SpillFileManagerTest, TestTenantIsolation1)
{
    std::shared_ptr<SpillFileManager> fileMgr_ = std::make_shared<SpillFileManager>();
    fileMgr_->Init({ "./spill_TestTenantIsolation1" });

    // No tenant id, big object, spill to file.
    std::string key1 = "key_1";
    std::string data1 = RandomData().GetRandomString(1024 * 1024);
    DS_EXPECT_OK(fileMgr_->Spill(key1, data1.data(), data1.size()));
    ASSERT_TRUE(fileMgr_->GetObjectLocation(key1).find(FLAGS_spill_directory + "/" + DEFAULT_TENANT_ID + "/")
                != std::string::npos);
    std::string out1;
    out1.resize(data1.size());
    DS_EXPECT_OK(fileMgr_->LoadFromDisk(key1, const_cast<char *>(out1.data()), out1.size(), 0));
    ASSERT_EQ(data1, out1);
    uint64_t decSize1;
    DS_EXPECT_OK(fileMgr_->DeleteFromDisk(key1, decSize1));
    ASSERT_EQ(decSize1, data1.size());

    // No tenant id, small object, spill to buffer.
    std::string key2 = "key_2";
    std::string data2 = RandomData().GetRandomString(1024);
    DS_EXPECT_OK(fileMgr_->Spill(key2, data2.data(), data2.size()));
    ASSERT_TRUE(fileMgr_->GetObjectLocation(key2).find(SPILL_BUFFER) != std::string::npos);
    std::string out2;
    out2.resize(data2.size());
    DS_EXPECT_OK(fileMgr_->LoadFromDisk(key2, const_cast<char *>(out2.data()), out2.size(), 0));
    ASSERT_EQ(data2, out2);
    uint64_t decSize2;
    DS_EXPECT_OK(fileMgr_->DeleteFromDisk(key2, decSize2));
    ASSERT_EQ(decSize2, data2.size());
}

TEST_F(SpillFileManagerTest, TestTenantIsolation2)
{
    std::shared_ptr<SpillFileManager> fileMgr_ = std::make_shared<SpillFileManager>();
    fileMgr_->Init({ "./spill_TestTenantIsolation2" });
    std::string tenantId = "tenant123";

    // Has tenant id, big object, spill to file.
    std::string key1 = tenantId + K_SEPARATOR + "key_1";
    std::string data1 = RandomData().GetRandomString(1024 * 1024);
    DS_EXPECT_OK(fileMgr_->Spill(key1, data1.data(), data1.size()));
    ASSERT_TRUE(fileMgr_->GetObjectLocation(key1).find(FLAGS_spill_directory + "/" + tenantId + "/")
                != std::string::npos);
    std::string out1;
    out1.resize(data1.size());
    DS_EXPECT_OK(fileMgr_->LoadFromDisk(key1, const_cast<char *>(out1.data()), out1.size(), 0));
    ASSERT_EQ(data1, out1);
    uint64_t decSize1;
    DS_EXPECT_OK(fileMgr_->DeleteFromDisk(key1, decSize1));
    ASSERT_EQ(decSize1, data1.size());

    // Has tenant id, small object, spill to file.
    std::string key2 = tenantId + K_SEPARATOR + "key_2";
    std::string data2 = RandomData().GetRandomString(1024);
    DS_EXPECT_OK(fileMgr_->Spill(key2, data2.data(), data2.size()));
    ASSERT_TRUE(fileMgr_->GetObjectLocation(key2).find(tenantId) != std::string::npos);
    std::string out2;
    out2.resize(data2.size());
    DS_EXPECT_OK(fileMgr_->LoadFromDisk(key2, const_cast<char *>(out2.data()), out2.size(), 0));
    ASSERT_EQ(data2, out2);
    uint64_t decSize2;
    DS_EXPECT_OK(fileMgr_->DeleteFromDisk(key2, decSize2));
    ASSERT_EQ(decSize2, data2.size());
}

TEST_F(SpillFileManagerTest, TestTenantIsolation3)
{
    std::shared_ptr<SpillFileManager> fileMgr_ = std::make_shared<SpillFileManager>();
    fileMgr_->Init({ "./spill_TestTenantIsolation3" });
    std::string tenantId = "/tenant/1/2/3";
    std::string key1 = tenantId + K_SEPARATOR + "key_1";
    std::replace(tenantId.begin(), tenantId.end(), '/', '_');
    std::string data1 = RandomData().GetRandomString(1024 * 1024);

    // Spill path is ./spill/_tenant_1_2_3/UUID.
    DS_EXPECT_OK(fileMgr_->Spill(key1, data1.data(), data1.size()));
    ASSERT_TRUE(fileMgr_->GetObjectLocation(key1).find(FLAGS_spill_directory + "/" + tenantId + "/")
                != std::string::npos);
    std::string out1;
    out1.resize(data1.size());
    DS_EXPECT_OK(fileMgr_->LoadFromDisk(key1, const_cast<char *>(out1.data()), out1.size(), 0));
    ASSERT_EQ(data1, out1);
    uint64_t decSize1;
    DS_EXPECT_OK(fileMgr_->DeleteFromDisk(key1, decSize1));
    ASSERT_EQ(decSize1, data1.size());
}

TEST_F(SpillFileManagerTest, LEVEL1_TestSpillBuffer)
{
    std::shared_ptr<SpillFileManager> fileMgr_ = std::make_shared<SpillFileManager>();
    fileMgr_->Init({ "./spill_TestSpillBuffer" });
    std::vector<size_t> sizeArray = { 10, 100, 1024, 10240, 102400 };
    int maxLoop = 100;
    int spillSize = 10 * 1024 * 1024;
    for (const auto &size : sizeArray) {
        std::vector<std::string> data;
        int loop = std::min<int>(spillSize / size, maxLoop);
        for (int i = 0; i < loop; i++) {
            std::string tmp = RandomData().GetPartRandomString(size, 10);
            data.emplace_back(tmp);
        }
        Timer timer;
        for (int i = 0; i < loop; i++) {
            std::string objectKey = "key_" + std::to_string(i);
            DS_EXPECT_OK(fileMgr_->Spill(objectKey, data[i].data(), data[i].size()));
        }
        auto writeTime = static_cast<uint64_t>(timer.ElapsedMilliSecond());

        char *buff = (char *)malloc(size);
        timer.Reset();
        for (int i = 0; i < loop; i++) {
            std::string objectKey = "key_" + std::to_string(i);
            DS_EXPECT_OK(fileMgr_->LoadFromDisk(objectKey, buff, size, 0));
            std::string out(buff, size);
            ASSERT_EQ(data[i], out);
        }
        auto readTime = static_cast<uint64_t>(timer.ElapsedMilliSecond());
        free(buff);

        timer.Reset();
        for (int i = 0; i < loop; i++) {
            std::string objectKey = "key_" + std::to_string(i);
            uint64_t decSize;
            DS_EXPECT_OK(fileMgr_->DeleteFromDisk(objectKey, decSize));
            ASSERT_EQ(decSize, size);
        }
        auto delTime = static_cast<uint64_t>(timer.ElapsedMilliSecond());
        LOG(INFO) << FormatString("size=%ld, loop=%ld, write time=%ld, read time=%ld, del time=%ld", size, loop,
                                  writeTime, readTime, delTime);
        PerfManager::Instance()->PrintPerfLog();
        PerfManager::Instance()->ResetPerfLog();
    }
}

TEST_F(SpillFileManagerTest, DISABLED_TestSpillBuffer2)
{
    std::shared_ptr<SpillFileManager> fileMgr_ = std::make_shared<SpillFileManager>();
    fileMgr_->Init({ "./spill_TestSpillBuffer2" });
    int loop = 100;
    std::vector<size_t> sizeArray = { 10, 100, 1024, 10240, 102400 };

    std::unordered_map<std::string, std::string> data;
    for (int i = 0; i < loop; i++) {
        for (const auto &size : sizeArray) {
            std::string objKey = "key_" + std::to_string(i) + "_" + std::to_string(size);
            std::string tmp = RandomData().GetRandomString(size);
            data.emplace(objKey, tmp);
        }
    }

    for (int i = 0; i < loop; i++) {
        for (const auto &size : sizeArray) {
            std::string objKey = "key_" + std::to_string(i) + "_" + std::to_string(size);
            ASSERT_EQ(data.count(objKey), static_cast<size_t>(1));
            DS_EXPECT_OK(fileMgr_->Spill(objKey, data[objKey].data(), data[objKey].size()));
        }
    }

    for (int i = 0; i < loop; i++) {
        for (const auto &size : sizeArray) {
            char *buff = (char *)malloc(size);
            std::string objKey = "key_" + std::to_string(i) + "_" + std::to_string(size);
            DS_EXPECT_OK(fileMgr_->LoadFromDisk(objKey, buff, size, 0));
            std::string out(buff, size);
            ASSERT_EQ(data[objKey], out);
            free(buff);
        }
    }

    for (int i = 0; i < loop; i++) {
        for (const auto &size : sizeArray) {
            std::string objKey = "key_" + std::to_string(i) + "_" + std::to_string(size);
            uint64_t decSize;
            DS_EXPECT_OK(fileMgr_->DeleteFromDisk(objKey, decSize));
            ASSERT_EQ(decSize, size);
        }
    }
}

TEST_F(SpillFileManagerTest, OpenFileLimitTest)
{
    FLAGS_spill_file_max_size_mb = 10;
    FLAGS_spill_file_open_limit = 1;
    std::string data = RandomData().GetPartRandomString(20 * 1024 * 1024, 10);

    std::shared_ptr<SpillFileManager> fileMgr = std::make_shared<SpillFileManager>();
    fileMgr->Init({ "./spill_OpenFileLimitTest" });

    std::string key0 = "key_1";
    DS_EXPECT_OK(fileMgr->Spill(key0, data.data(), data.size()));
    std::string key2 = "key_2";
    DS_EXPECT_OK(fileMgr->Spill(key2, data.data(), data.size()));

    auto cmd = "lsof " + fileMgr->GetObjectLocation(key0);
    ASSERT_TRUE(system(cmd.c_str()) != 0);
    cmd = "lsof " + fileMgr->GetObjectLocation(key2);
    ASSERT_TRUE(system(cmd.c_str()) == 0);

    std::string out;
    out.resize(data.size());
    DS_EXPECT_OK(fileMgr->LoadFromDisk(key0, const_cast<char *>(out.data()), out.size(), 0));
    ASSERT_EQ(out, data);
    DS_EXPECT_OK(fileMgr->LoadFromDisk(key2, const_cast<char *>(out.data()), out.size(), 0));
    ASSERT_EQ(out, data);

    uint64_t decSize;
    DS_EXPECT_OK(fileMgr->DeleteFromDisk(key0, decSize));
    DS_EXPECT_OK(fileMgr->DeleteFromDisk(key2, decSize));
}

TEST_F(SpillFileManagerTest, TestCloneSpillBuffer)
{
    SpillBuffer buffer;
    const int loop = 1000;
    const uint32_t minSize = 8;
    const uint32_t maxSize = 1024;
    std::vector<std::pair<std::string, bool>> dataList;
    for (int i = 0; i < loop; i++) {
        auto size = RandomData().GetRandomUint32(minSize, maxSize);
        std::string tmp = RandomData().GetPartRandomString(size, 10);
        dataList.emplace_back(tmp, true);
        std::string objectKey = "object-" + std::to_string(i);
        buffer.Append(objectKey, tmp.data(), tmp.size());
    }

    size_t remainingSize = 0;
    size_t remainingCount = 0;
    const uint32_t count = 2;
    for (int i = 0; i < loop; i++) {
        if (RandomData().GetRandomUint32(0, maxSize) > maxSize / count) {
            std::string objectKey = "object-" + std::to_string(i);
            buffer.Remove(objectKey);
            dataList[i].second = false;
        } else {
            remainingSize += dataList[i].first.size();
            remainingCount += 1;
        }
    }

    SpillBuffer copyBuffer;
    buffer.CloneTo(copyBuffer);
    ASSERT_EQ(copyBuffer.Size(), remainingSize);
    ASSERT_EQ(copyBuffer.GetIndex().size(), remainingCount);
    for (int i = 0; i < loop; i++) {
        if (dataList[i].second) {
            auto size = dataList[i].first.size();
            std::string objectKey = "object-" + std::to_string(i);
            LOG(INFO) << objectKey;
            auto buff = std::make_unique<char[]>(size);
            DS_ASSERT_OK(copyBuffer.CopyTo(objectKey, buff.get(), size, 0));
            std::string out(buff.get(), size);
            ASSERT_EQ(dataList[i].first, out);
        }
    }
}

TEST_F(SpillFileManagerTest, OffsetReadSpilledObject)
{
    int maxFileSizeMb = 10;
    FLAGS_spill_file_max_size_mb = maxFileSizeMb;
    FLAGS_spill_file_open_limit = 1;
    int dataSize1 = 1024 * 1024 * 2;
    int dataSize2 = 64;
    std::string data1 = RandomData().GetRandomString(dataSize1);
    std::string data2 = RandomData().GetRandomString(dataSize2);

    std::shared_ptr<SpillFileManager> fileMgr = std::make_shared<SpillFileManager>();
    fileMgr->Init({ "./spill_OffsetReadSpilledObject" });

    int loopCount = 10;
    std::vector<std::pair<std::string, std::string>> keys;
    for (int i = 0; i < loopCount; i++) {
        std::string key1 = "key1_" + std::to_string(i);
        DS_EXPECT_OK(fileMgr->Spill(key1, data1.data(), data1.size()));
        keys.emplace_back(std::make_pair(key1, data1));

        std::string key2 = "key2_" + std::to_string(i);
        DS_EXPECT_OK(fileMgr->Spill(key2, data2.data(), data2.size()));
        keys.emplace_back(std::make_pair(key2, data2));
    }

    for (auto kv : keys) {
        const auto &data = kv.second;
        // Test invalid size
        auto ptr = std::make_unique<char[]>(data.size());
        DS_EXPECT_NOT_OK(fileMgr->LoadFromDisk(kv.first, ptr.get(), 0, 1));

        // Test invalid offset
        DS_EXPECT_NOT_OK(fileMgr->LoadFromDisk(kv.first, ptr.get(), 1, data.size()));

        // Test read
        std::vector<std::pair<size_t, size_t>> offsetInfos = {
            { 0, data.size() }, { 0, 1 }, { data.size() - 1, 1 }, { 0, 10 }, { 16, 32 }, { data.size() - 10, 10 }
        };
        for (auto info : offsetInfos) {
            size_t readOffset = info.first;
            size_t readSize = info.second;
            DS_EXPECT_OK(fileMgr->LoadFromDisk(kv.first, ptr.get(), readSize, readOffset));
            std::string val(ptr.get(), readSize);
            ASSERT_EQ(val, data.substr(readOffset, readSize));
        }
    }
}

class SpillRequestHandlerTest : public CommonTest {
public:
    WorkerOcSpill *handler;

    void SetUp()
    {
        CommonTest::SetUp();
        FLAGS_spill_directory = "./spill" + GetStringUuid();
        FLAGS_v = 1;
        LOG_IF_ERROR(inject::Set("worker.Spill.Sync", "return()"), "set inject point failed");
        handler = WorkerOcSpill::Instance();
        handler->Init();
    }

    void CompactSmallObjFile()
    {
        size_t size = 512 * 1024;
        int loop = 3600;
        int mode = 256;
        std::string data(size, 's');
        std::unordered_set<int> objectKeys;
        for (int i = 0; i < loop; i++) {
            std::string objectKey = "key_small_obj" + std::to_string(i);
            data[0] = (char)i % mode;
            objectKeys.insert(i);
            DS_EXPECT_OK(handler->Spill(objectKey, const_cast<char *>(data.data()), data.size()));
        }

        std::string loadData;
        loadData.reserve(size);
        std::mt19937 gen((unsigned int)time(NULL));
        int minValue = 1;
        int maxValue = 10;
        int threshold = 6;
        std::uniform_int_distribution<int> distribute(minValue, maxValue);
        for (int i = 0; i < loop; i++) {
            std::string objectKey = "key_small_obj" + std::to_string(i);
            if (distribute(gen) <= threshold) {
                DS_EXPECT_OK(handler->Delete(objectKey));
                objectKeys.erase(i);
            }
        }

        int sleepTimeSec = 10;
        sleep(sleepTimeSec);
        for (auto i : objectKeys) {
            std::string objectKey = "key_small_obj" + std::to_string(i);
            data[0] = (char)i % mode;
            DS_EXPECT_OK(handler->Get(objectKey, const_cast<char *>(loadData.data()), size, 0));
            ASSERT_EQ(memcmp(loadData.c_str(), data.c_str(), size), 0);
            DS_EXPECT_OK(handler->Delete(objectKey));
        }
    }

    void GetAndDelWhenCompact()
    {
        size_t size = 512 * 1024;
        int loop = 3600;
        std::string data(size, 's');
        std::unordered_set<int> objectKeys;
        std::string loadData;
        loadData.reserve(size);
        std::mt19937 gen((unsigned int)time(NULL));
        int minValue = 1;
        int maxValue = 100;
        int mode = 256;
        std::uniform_int_distribution<int> distribute(minValue, maxValue);
        for (int i = 0; i < loop; i++) {
            std::string objectKey = "key_small_obj" + std::to_string(i);
            data[0] = (char)i % mode;
            objectKeys.insert(i);
            DS_EXPECT_OK(handler->Spill(objectKey, const_cast<char *>(data.data()), data.size()));
            int tmpThreshold = 60;
            if (distribute(gen) <= tmpThreshold) {
                DS_EXPECT_OK(handler->Delete(objectKey));
                objectKeys.erase(i);
            }
        }

        int sleepSec = 8;
        sleep(sleepSec);
        Timer timer;
        while (true) {
            std::unordered_set<int> objectKeysCopy(objectKeys.begin(), objectKeys.end());
            for (auto i : objectKeysCopy) {
                std::string objectKey = "key_small_obj" + std::to_string(i);
                data[0] = (char)i % mode;
                DS_EXPECT_OK(handler->Get(objectKey, const_cast<char *>(loadData.data()), size, 0));
                ASSERT_EQ(memcmp(loadData.c_str(), data.c_str(), size), 0);
                int tmpThreshold = 5;
                if (distribute(gen) <= tmpThreshold) {
                    DS_EXPECT_OK(handler->Delete(objectKey));
                    objectKeys.erase(i);
                }
            }
            std::string data1(size, 'A');
            int tmpLoop = 20;
            for (int i = 0; i < tmpLoop; i++) {
                const int tmp = 10000;
                std::string objectKey = "key_small_obj" + std::to_string(i + tmp);
                data1[0] = (char)(i + tmp) % mode;
                DS_EXPECT_OK(handler->Spill(objectKey, const_cast<char *>(data1.data()), data1.size()));
                DS_EXPECT_OK(handler->Get(objectKey, const_cast<char *>(loadData.data()), size, 0));
                ASSERT_EQ(memcmp(loadData.c_str(), data1.c_str(), size), 0);
                DS_EXPECT_OK(handler->Delete(objectKey));
            }
            auto costTime = static_cast<uint64_t>(timer.ElapsedMilliSecond());
            const uint64_t tmpThreshold = 1000 * 10;
            if (costTime > tmpThreshold) {
                break;
            }
        }
    }

    void GetAndDelWhenFallocate()
    {
        size_t size = 2 * 1024 * 1024;
        std::string data(size, 's');
        std::unordered_set<int> objectKeys;
        std::string loadData;
        loadData.reserve(size);
        std::mt19937 gen((unsigned int)time(NULL));
        int minValue = 1;
        int maxValue = 100;
        std::uniform_int_distribution<int> distribute(minValue, maxValue);
        int tmpLoop = 100;
        int mode = 256;
        for (int i = 0; i < tmpLoop; i++) {
            std::string objectKey = "key_small_obj" + std::to_string(i);
            data[0] = (char)i % mode;
            objectKeys.insert(i);
            DS_EXPECT_OK(handler->Spill(objectKey, const_cast<char *>(data.data()), data.size()));
            const int tmpThreshold = 50;
            if (distribute(gen) <= tmpThreshold) {
                DS_EXPECT_OK(handler->Delete(objectKey));
                objectKeys.erase(i);
            }
        }
        handler->ForceCompact();
        int sleepSec = 3;
        sleep(sleepSec);
        Timer timer;
        while (true) {
            std::unordered_set<int> objectKeysCopy(objectKeys.begin(), objectKeys.end());
            for (auto i : objectKeysCopy) {
                std::string objectKey = "key_small_obj" + std::to_string(i);
                data[0] = (char)i % mode;
                DS_EXPECT_OK(handler->Get(objectKey, const_cast<char *>(loadData.data()), size, 0));
                ASSERT_EQ(memcmp(loadData.c_str(), data.c_str(), size), 0);
                const int tmpThreshold = 5;
                if (distribute(gen) <= tmpThreshold) {
                    DS_EXPECT_OK(handler->Delete(objectKey));
                    objectKeys.erase(i);
                }
            }
            std::string data1(size, 'A');
            int loop = 10;
            for (int i = 0; i < loop; i++) {
                static const int tmpValue = 10000;
                std::string objectKey = "key_small_obj" + std::to_string(i + tmpValue);
                data1[0] = (char)(i + tmpValue) % mode;
                DS_EXPECT_OK(handler->Spill(objectKey, const_cast<char *>(data1.data()), data1.size()));
                DS_EXPECT_OK(handler->Get(objectKey, const_cast<char *>(loadData.data()), size, 0));
                ASSERT_EQ(memcmp(loadData.c_str(), data1.c_str(), size), 0);
                DS_EXPECT_OK(handler->Delete(objectKey));
            }
            handler->ForceCompact();
            auto costTime = static_cast<uint64_t>(timer.ElapsedMilliSecond());
            uint64_t timeThreshold = 1000 * 6;
            if (costTime > timeThreshold) {
                break;
            }
        }
    }

    void FallocateLargeObjFile01()
    {
        DS_ASSERT_OK(inject::Set("worker.Spill.Write", "return(K_OK)"));
        size_t size = 50 * 1024 * 1024;
        std::string data(size, 'l');
        size_t size2 = 1024 * 1024;
        std::string data1(size2, 'l');
        const int loop1 = 40;
        for (int i = 0; i < loop1; i++) {
            std::string objectKey = "key_large_obj" + std::to_string(i);
            DS_EXPECT_OK(handler->Spill(objectKey, const_cast<char *>(data.data()), data.size()));
            std::string objectKey2 = "key_large_obj_" + std::to_string(i);
            DS_EXPECT_OK(handler->Spill(objectKey2, const_cast<char *>(data1.data()), data1.size()));
        }
        std::string loadData;
        loadData.reserve(size);
        const int loop2 = 20;
        for (int i = 0; i < loop2; i++) {
            std::string objectKey = "key_large_obj" + std::to_string(i);
            DS_EXPECT_OK(handler->Delete(objectKey));
        }
        handler->ForceCompact();
        int sleepSec = 1;
        sleep(sleepSec);
        const int begin = 20;
        const int end = 40;
        for (int i = begin; i < end; i++) {
            std::string objectKey = "key_large_obj" + std::to_string(i);
            DS_EXPECT_OK(handler->Delete(objectKey));
        }
        handler->ForceCompact();
        // wait fallocate
        sleep(sleepSec);
    }
};

TEST_F(SpillRequestHandlerTest, EXCLUSIVE_CopyAndSplitBufferTest)
{
    RandomData random;
    size_t size = 2 * 1024L * 1024L * 1024L + 1024L * 1024L;
    std::string data = random.GetPartRandomString(size, 10);

    DS_ASSERT_OK(datasystem::memory::Allocator::Instance()->Init(size * 3));
    std::vector<RpcMessage> messages;
    DS_ASSERT_OK(CopyAndSplitBuffer(DEFAULT_TENANT_ID, data.data(), size, messages));

    size_t offset = 0;
    for (size_t i = 0; i < messages.size(); i++) {
        ASSERT_TRUE(offset + messages[i].Size() <= size);
        int ret = memcmp(data.data() + offset, messages[i].Data(), messages[i].Size());
        offset += messages[i].Size();
        ASSERT_EQ(ret, 0);
    }

    ASSERT_EQ(offset, size);
}

TEST_F(SpillRequestHandlerTest, TestSpillFile)
{
    std::string fileURL;
    std::string objectKey = "ThisisAObject";
    std::string content = "Test1234567890";
    std::string load;
    load.reserve(content.size());
    DS_EXPECT_OK(handler->Spill(objectKey, const_cast<char *>(content.data()), content.size()));
    DS_EXPECT_OK(handler->Get(objectKey, const_cast<char *>(load.data()), content.size()));
    std::string objectKey2 = "ThisisAObject2";
    std::string content2 = "ABCDEFGHIJKLMZ";
    DS_EXPECT_OK(handler->Spill(objectKey2, const_cast<char *>(content2.data()), content2.size()));
    DS_EXPECT_OK(handler->Get(objectKey2, const_cast<char *>(load.data()), content2.size()));
    DS_EXPECT_OK(handler->Delete(objectKey));
    DS_EXPECT_OK(handler->Delete(objectKey2));
}

TEST_F(SpillRequestHandlerTest, TestSpillFileDeleteAndAddAgain)
{
    std::string fileURL;
    std::string objectKey = "ThisisAObject";
    std::string content = "Test1234567890";
    std::string load;
    load.reserve(content.size());
    DS_EXPECT_OK(handler->Spill(objectKey, const_cast<char *>(content.data()), content.size()));
    DS_EXPECT_OK(handler->Get(objectKey, const_cast<char *>(load.data()), content.size()));
    std::string objectKey2 = "ThisisAObject2";
    std::string content2 = "ABCDEFGHIJKLMZ";
    DS_EXPECT_OK(handler->Spill(objectKey2, const_cast<char *>(content2.data()), content2.size()));
    DS_EXPECT_OK(handler->Get(objectKey2, const_cast<char *>(load.data()), content2.size()));
    DS_EXPECT_OK(handler->Delete(objectKey));
    DS_EXPECT_OK(handler->Delete(objectKey2));
    DS_EXPECT_OK(handler->Spill(objectKey, const_cast<char *>(content.data()), content.size()));
    DS_EXPECT_OK(handler->Spill(objectKey2, const_cast<char *>(content2.data()), content2.size()));
    DS_EXPECT_OK(handler->Get(objectKey2, const_cast<char *>(load.data()), content2.size()));
    DS_EXPECT_OK(handler->Get(objectKey, const_cast<char *>(load.data()), content.size()));
    DS_EXPECT_OK(handler->Delete(objectKey));
    DS_EXPECT_OK(handler->Delete(objectKey2));
}

TEST_F(SpillRequestHandlerTest, TestSpillMultiFileConcurrent1)
{
    // Test concurrent group 1
    std::string fileURL;
    std::string objectKey = "ThisisAObject";
    std::string content = "Test1234567890";
    int threadNum = 1024;
    std::atomic<int> okCount(0);
    std::atomic<int> notOkCount(0);
    std::vector<std::thread> clientThreads(threadNum);
    for (int i = 0; i < threadNum; ++i) {
        clientThreads[i] = std::thread([this, &objectKey, &content, &notOkCount, &okCount, i]() {
            std::ostringstream objKeyss;
            objKeyss << objectKey << i;
            auto rc = handler->Spill(objKeyss.str(), const_cast<char *>(content.data()), content.size());
            if (rc.IsError()) {
                notOkCount.fetch_add(1);
                LOG(INFO) << rc.GetCode() << ", " << rc.GetMsg();
            } else {
                okCount.fetch_add(1);
            }
        });
    }

    for (auto &t : clientThreads) {
        t.join();
    }

    EXPECT_EQ(okCount, threadNum);
}

TEST_F(SpillRequestHandlerTest, LEVEL1_TestSpillMultiFileConcurrent2)
{
    int loopPerThread = 5000;
    int threadNum = 10;
    std::vector<std::thread> threads(threadNum);
    std::vector<std::string> data;
    for (int i = 1; i <= threadNum; i++) {
        std::string tmp = RandomData().GetRandomString(i * 10);
        data.emplace_back(tmp);
    }

    Timer timer;
    for (int i = 0; i < threadNum; ++i) {
        threads[i] = std::thread([this, i, loopPerThread, data]() {
            for (int j = 0; j < loopPerThread; j++) {
                std::string objKey = "key_" + std::to_string(i) + "_" + std::to_string(j);
                DS_ASSERT_OK(handler->Spill(objKey, const_cast<char *>(data[i].data()), data[i].size()));
            }
        });
    }
    for (auto &t : threads) {
        t.join();
    }
    auto writeTime = static_cast<uint64_t>(timer.ElapsedMilliSecond());

    timer.Reset();
    for (int i = 0; i < threadNum; ++i) {
        threads[i] = std::thread([this, i, loopPerThread, data]() {
            size_t size = data[i].size();
            char *buff = (char *)malloc(size);
            for (int j = 0; j < loopPerThread; j++) {
                std::string objKey = "key_" + std::to_string(i) + "_" + std::to_string(j);
                DS_ASSERT_OK(handler->Get(objKey, buff, size));
                std::string out(buff, size);
                ASSERT_EQ(data[i], out);
            }
            free(buff);
        });
    }
    for (auto &t : threads) {
        t.join();
    }
    auto readTime = static_cast<uint64_t>(timer.ElapsedMilliSecond());

    timer.Reset();
    for (int i = 0; i < threadNum; ++i) {
        threads[i] = std::thread([this, i, loopPerThread]() {
            for (int j = 0; j < loopPerThread; j++) {
                std::string objKey = "key_" + std::to_string(i) + "_" + std::to_string(j);
                DS_ASSERT_OK(handler->Delete(objKey));
            }
        });
    }
    for (auto &t : threads) {
        t.join();
    }
    auto delTime = static_cast<uint64_t>(timer.ElapsedMilliSecond());
    LOG(INFO) << "write time = " << writeTime << ", read time = " << readTime << ", delete time = " << delTime;
}

TEST_F(SpillRequestHandlerTest, TestSpillBigObject)
{
    size_t size = 200 * 1024 * 1024;
    std::string data(size, 'a');
    for (int i = 0; i < 10; i++) {
        std::string objectKey = "key_" + std::to_string(i);
        DS_EXPECT_OK(handler->Spill(objectKey, const_cast<char *>(data.data()), data.size()));
    }
    for (int i = 0; i < 10; i++) {
        std::string objectKey = "key_" + std::to_string(i);
        DS_EXPECT_OK(handler->Delete(objectKey));
    }
    auto files = handler->GetSpilledFileName(FLAGS_spill_directory + SPILL_PATH_PREFIX);
    ASSERT_TRUE(files.empty());
}

TEST_F(SpillRequestHandlerTest, TestSpillBigObject2)
{
    auto size = static_cast<size_t>(2.3 * 1024UL * 1024UL * 1024UL);
    std::string data(size, 'a');
    std::string objectKey = "key_0";
    DS_EXPECT_OK(handler->Spill(objectKey, const_cast<char *>(data.data()), data.size()));
    std::string loadData;
    loadData.reserve(size);
    DS_EXPECT_OK(handler->Get("key_0", const_cast<char *>(loadData.data()), size));
    ASSERT_EQ(memcmp(loadData.c_str(), data.c_str(), size), 0);
    DS_EXPECT_OK(handler->Delete(objectKey));
}

TEST_F(SpillRequestHandlerTest, LEVEL1_TestSpillBigObject3)
{
    auto size = static_cast<size_t>(3 * 1024UL * 1024UL * 1024UL);
    std::string data(size, 'a');
    std::string objectKey = "key_0";
    DS_EXPECT_OK(handler->Spill(objectKey, const_cast<char *>(data.data()), data.size()));
    std::vector<datasystem::RpcMessage> loadMsgs;
    DS_EXPECT_OK(handler->Get("key_0", loadMsgs, size));
    size_t msgLen = 0;
    for (auto &msg : loadMsgs) {
        msgLen += msg.Size();
    }
    ASSERT_EQ(msgLen, size);
    size_t offset = 0;
    for (auto &msg : loadMsgs) {
        ASSERT_EQ(memcmp(msg.Data(), data.c_str() + offset, msg.Size()), 0);
        offset += msg.Size();
    }
    DS_EXPECT_OK(handler->Delete(objectKey));
}

TEST_F(SpillRequestHandlerTest, TestSpillSmallObjFile01)
{
    // Write small objects
    size_t size = 1024;
    std::string data(size, 's');
    for (int i = 0; i < 50; i++) {
        std::string objectKey = "key_small_obj" + std::to_string(i);
        DS_EXPECT_OK(handler->Spill(objectKey, const_cast<char *>(data.data()), data.size()));
    }

    std::string loadData;
    loadData.reserve(size);
    for (int i = 0; i < 50; i++) {
        std::string objectKey = "key_small_obj" + std::to_string(i);
        DS_EXPECT_OK(handler->Get(objectKey, const_cast<char *>(loadData.data()), size));
        ASSERT_EQ(memcmp(loadData.c_str(), data.c_str(), size), 0);
        DS_EXPECT_OK(handler->Delete(objectKey));
    }

    // Fill small object file
    size = 512 * 1024;
    loadData.reserve(size);
    std::string data1(size, 's');
    std::string objectKey = "key_small_obj";
    for (int i = 0; i < 200 * 2; i++) {
        DS_EXPECT_OK(handler->Spill(objectKey, const_cast<char *>(data1.data()), data1.size()));
        DS_EXPECT_OK(handler->Get(objectKey, const_cast<char *>(loadData.data()), size));
        ASSERT_EQ(memcmp(loadData.c_str(), data1.c_str(), size), 0);
        DS_EXPECT_OK(handler->Delete(objectKey));
    }
}

TEST_F(SpillRequestHandlerTest, TestSpillSmallObjFile02)
{
    // Write small objects
    size_t size = 1024;
    std::string data(size, 's');
    std::string objectKey = "key_small_obj";
    DS_ASSERT_OK(datasystem::memory::Allocator::Instance()->Init(6 * 1024 * 1024));
    DS_EXPECT_OK(handler->Spill(objectKey, const_cast<char *>(data.data()), data.size()));

    std::vector<datasystem::RpcMessage> loadMsgs;
    // Get from spill buffer
    DS_EXPECT_OK(handler->Get("key_small_obj", loadMsgs, size));
    ASSERT_EQ(loadMsgs.size(), 1u);
    ASSERT_EQ(loadMsgs[0].Size(), size);
    ASSERT_EQ(memcmp(loadMsgs[0].Data(), data.c_str(), size), 0);
    DS_EXPECT_OK(handler->Delete(objectKey));

    size = 2 * 1024 * 1024;
    std::string data1(size, 's');
    loadMsgs.clear();
    DS_EXPECT_OK(handler->Spill(objectKey, const_cast<char *>(data1.data()), data1.size()));
    // Get from disk
    DS_EXPECT_OK(handler->Get("key_small_obj", loadMsgs, size));
    ASSERT_EQ(loadMsgs.size(), 1u);
    ASSERT_EQ(loadMsgs[0].Size(), size);
    ASSERT_EQ(memcmp(loadMsgs[0].Data(), data1.c_str(), size), 0);
    DS_EXPECT_OK(handler->Delete(objectKey));
}

TEST_F(SpillRequestHandlerTest, TestSpillLargeObjFile01)
{
    // 1M large obj
    size_t size = 1 * 1024 * 1024;
    std::string data(size, 'l');
    for (int i = 0; i < 100; i++) {
        std::string objectKey = "key_large_obj" + std::to_string(i);
        DS_EXPECT_OK(handler->Spill(objectKey, const_cast<char *>(data.data()), data.size()));
    }

    std::string loadData;
    loadData.reserve(size);
    for (int i = 0; i < 100; i++) {
        std::string objectKey = "key_large_obj" + std::to_string(i);
        DS_EXPECT_OK(handler->Get(objectKey, const_cast<char *>(loadData.data()), size));
        ASSERT_EQ(memcmp(loadData.c_str(), data.c_str(), size), 0);
        DS_EXPECT_OK(handler->Delete(objectKey));
    }
}

TEST_F(SpillRequestHandlerTest, LEVEL1_TestSpillSmallAndLargeObjMix)
{
    size_t size1 = 1 * 1024;
    size_t size2 = 2 * 1024 * 1024;
    std::string smallData(size1, 's');
    std::string largeData(size2, 'l');
    int dataCount = 100;
    for (int i = 0; i < dataCount; i++) {
        std::string smallObjectKey = "key_small_obj" + std::to_string(i);
        std::string largeObjectKey = "key_large_obj" + std::to_string(i);
        DS_EXPECT_OK(handler->Spill(smallObjectKey, const_cast<char *>(smallData.data()), smallData.size()));
        DS_EXPECT_OK(handler->Spill(largeObjectKey, const_cast<char *>(largeData.data()), largeData.size()));
    }

    std::string loadSmallObjData;
    loadSmallObjData.reserve(size1);
    std::string loadLargeObjData;
    loadLargeObjData.reserve(size2);
    for (int i = 0; i < dataCount; i++) {
        std::string smallObjectKey = "key_small_obj" + std::to_string(i);
        DS_EXPECT_OK(handler->Get(smallObjectKey, const_cast<char *>(loadSmallObjData.data()), size1));
        ASSERT_EQ(memcmp(loadSmallObjData.c_str(), smallData.c_str(), size1), 0);
        DS_EXPECT_OK(handler->Delete(smallObjectKey));
        std::string largeObjectKey = "key_large_obj" + std::to_string(i);
        DS_EXPECT_OK(handler->Get(largeObjectKey, const_cast<char *>(loadLargeObjData.data()), size2));
        ASSERT_EQ(memcmp(loadLargeObjData.c_str(), largeData.c_str(), size2), 0);
        DS_EXPECT_OK(handler->Delete(largeObjectKey));
    }
    handler->ForceCompact();
    // wait fallocate
    int waitTime = 3;
    sleep(waitTime);
}

TEST_F(SpillRequestHandlerTest, DISABLED_TestSpillCompactSmallObjFile)
{
    CompactSmallObjFile();
}

TEST_F(SpillRequestHandlerTest, DISABLED_TestSpillGetAndDelWhenCompact)
{
    GetAndDelWhenCompact();
}

TEST_F(SpillRequestHandlerTest, LEVEL1_TestSpillGetAndDelWhenFallocate)
{
    GetAndDelWhenFallocate();
}

TEST_F(SpillRequestHandlerTest, LEVEL1_TestSpillFallocateLargeObjFile01)
{
    FallocateLargeObjFile01();
}

TEST_F(SpillRequestHandlerTest, DISABLED_OpenFileLimitTest1)
{
    FLAGS_spill_file_open_limit = 1;
    CompactSmallObjFile();
}

TEST_F(SpillRequestHandlerTest, DISABLED_OpenFileLimitTest2)
{
    FLAGS_spill_file_open_limit = 1;
    GetAndDelWhenCompact();
}

TEST_F(SpillRequestHandlerTest, LEVEL1_OpenFileLimitTest3)
{
    FLAGS_spill_file_open_limit = 1;
    GetAndDelWhenFallocate();
}

TEST_F(SpillRequestHandlerTest, LEVEL1_OpenFileLimitTest4)
{
    FLAGS_spill_file_open_limit = 1;
    FallocateLargeObjFile01();
}

TEST_F(SpillRequestHandlerTest, LEVEL1_DeleteEmptyFileTest)
{
    size_t size = 140 * 1024 * 1024;
    std::string data(size, 'l');
    std::string loadData;
    loadData.reserve(size);
    for (int i = 0; i < 8; i++) {
        std::string objectKey = "key_large_obj" + std::to_string(i);
        DS_EXPECT_OK(handler->Spill(objectKey, &data[0], data.size()));
    }
    for (int i = 0; i < 8; i++) {
        std::string objectKey = "key_large_obj" + std::to_string(i);
        DS_EXPECT_OK(handler->Get(objectKey, &loadData[0], size));
        ASSERT_EQ(memcmp(loadData.c_str(), data.c_str(), size), 0);
        DS_EXPECT_OK(handler->Delete(objectKey));
    }
    // wait compaction thread delete
    sleep(13);

    std::vector<std::string> filePaths;
    DS_EXPECT_OK(Glob(FLAGS_spill_directory + "/datasystem_spill_data/*", filePaths));
    for (int i = 0; i < 8; i++) {
        std::string objectKey = "key_large_obj" + std::to_string(i);
        DS_EXPECT_OK(handler->Spill(objectKey, &data[0], data.size()));
    }
    std::vector<std::string> paths;
    DS_EXPECT_OK(Glob(FLAGS_spill_directory + "/datasystem_spill_data/*", paths));
    std::set<std::string> pathSet = { paths.begin(), paths.end() };
    for (const auto &path : filePaths) {
        ASSERT_EQ((pathSet.find(path) == pathSet.end()), true);
    }
    for (int i = 0; i < 8; i++) {
        std::string objectKey = "key_large_obj" + std::to_string(i);
        DS_EXPECT_OK(handler->Get(objectKey, &loadData[0], size));
        ASSERT_EQ(memcmp(loadData.c_str(), data.c_str(), size), 0);
        DS_EXPECT_OK(handler->Delete(objectKey));
    }
}

TEST_F(SpillRequestHandlerTest, TestTenantIsolation1)
{
    // No tenant id, big object, spill to file.
    std::string key1 = "key_1";
    std::string data1 = RandomData().GetRandomString(1024 * 1024);
    std::string realSpillDirectory = FLAGS_spill_directory + "/datasystem_spill_data";
    DS_EXPECT_OK(handler->Spill(key1, data1.data(), data1.size()));
    ASSERT_TRUE(handler->GetObjectLocation(key1).find(realSpillDirectory + "/" + DEFAULT_TENANT_ID + "/")
                != std::string::npos);
    std::string out1;
    out1.resize(data1.size());
    DS_EXPECT_OK(handler->Get(key1, const_cast<char *>(out1.data()), out1.size()));
    ASSERT_EQ(data1, out1);
    DS_EXPECT_OK(handler->Delete(key1));

    // No tenant id, small object, spill to buffer.
    std::string key2 = "key_2";
    std::string data2 = RandomData().GetRandomString(1024);
    DS_EXPECT_OK(handler->Spill(key2, data2.data(), data2.size()));
    ASSERT_TRUE(handler->GetObjectLocation(key2).find(SPILL_BUFFER) != std::string::npos);
    std::string out2;
    out2.resize(data2.size());
    DS_EXPECT_OK(handler->Get(key2, const_cast<char *>(out2.data()), out2.size()));
    ASSERT_EQ(data2, out2);
    DS_EXPECT_OK(handler->Delete(key2));
}

TEST_F(SpillRequestHandlerTest, TestTenantIsolation2)
{
    std::string tenantId = "tenant123";

    // Has tenant id, big object, spill to file.
    std::string key1 = tenantId + K_SEPARATOR + "key_1";
    std::string realSpillDirectory = FLAGS_spill_directory + "/datasystem_spill_data";
    std::string data1 = RandomData().GetRandomString(1024 * 1024);
    DS_EXPECT_OK(handler->Spill(key1, data1.data(), data1.size()));
    ASSERT_TRUE(handler->GetObjectLocation(key1).find(realSpillDirectory + "/" + tenantId + "/") != std::string::npos);
    std::string out1;
    out1.resize(data1.size());
    DS_EXPECT_OK(handler->Get(key1, const_cast<char *>(out1.data()), out1.size()));
    ASSERT_EQ(data1, out1);
    DS_EXPECT_OK(handler->Delete(key1));

    // Has tenant id, small object, spill to file.
    std::string key2 = tenantId + K_SEPARATOR + "key_2";
    std::string data2 = RandomData().GetRandomString(1024);
    DS_EXPECT_OK(handler->Spill(key2, data2.data(), data2.size()));
    ASSERT_TRUE(handler->GetObjectLocation(key2).find(realSpillDirectory + "/" + tenantId + "/") != std::string::npos);
    std::string out2;
    out2.resize(data2.size());
    DS_EXPECT_OK(handler->Get(key2, const_cast<char *>(out2.data()), out2.size()));
    ASSERT_EQ(data2, out2);
    DS_EXPECT_OK(handler->Delete(key2));
}
}  // namespace ut
}  // namespace datasystem
