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
 * Description: State client offset read tests.
 */

#include "datasystem/common/util/uuid_generator.h"
#include "datasystem/kv_client.h"

#include <unistd.h>
#include <atomic>
#include <csignal>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <sstream>
#include <string>
#include <thread>
#include <type_traits>
#include <vector>

#include <gtest/gtest.h>

#include "client/object_cache/oc_client_common.h"
#include "common.h"

#include "datasystem/common/log/log.h"
#include "datasystem/common/util/file_util.h"
#include "datasystem/common/util/random_data.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/object/object_enum.h"
#include "datasystem/utils/connection.h"
#include "datasystem/utils/status.h"
#include "datasystem/worker/hash_ring/hash_ring.h"

DS_DECLARE_string(etcd_address);
DS_DECLARE_string(master_address);
DS_DECLARE_bool(log_monitor);

namespace datasystem {
namespace st {
namespace {
const std::string HOST_IP = "127.0.0.1";
}  // namespace

class KVClientOffsetReadOneHostTest : public OCClientCommon,
                                         public testing::WithParamInterface<datasystem::SetParam> {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.enableSpill = true;
        opts.workerGflagParams =
            "-shared_memory_size_mb=12 -log_async=false -log_monitor=true -shared_disk_directory=./" + GetStringUuid()
            + " -shared_disk_size_mb=100 -v=1 -spill_size_limit=" + std::to_string(maxSize_);
        opts.numEtcd = 1;
        opts.numWorkers = 1;
        opts.numOBS = 1;
        opts.enableDistributedMaster = "false";
    }

    void SetUp() override
    {
        type_ = GetParam();
        ExternalClusterTest::SetUp();
        InitTestKVClientWithTenant(0, client_);
    }

    void InitConnectOptW(uint32_t workerIndex, ConnectOptions &connectOptions, int32_t timeoutMs = 60000)
    {
        HostPort workerAddress;
        ASSERT_TRUE(workerIndex < cluster_->GetWorkerNum());
        DS_ASSERT_OK(cluster_->GetWorkerAddr(workerIndex, workerAddress));
        connectOptions = { .host = workerAddress.Host(), .port = workerAddress.Port(), .connectTimeoutMs = timeoutMs };
        connectOptions.accessKey = "QTWAOYTTINDUT2QVKYUC";
        connectOptions.secretKey = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
    }

    void InitTestKVClientWithTenant(uint32_t workerIndex, std::shared_ptr<KVClient> &client)
    {
        ConnectOptions connectOptions;
        InitConnectOptW(workerIndex, connectOptions);
        client = std::make_shared<KVClient>(connectOptions);
        DS_ASSERT_OK(client->Init());
    }

protected:
    std::shared_ptr<KVClient> client_;
    datasystem::SetParam type_;
    uint64_t maxSize_ = 64 * 1024ul * 1024ul;
};

TEST_P(KVClientOffsetReadOneHostTest, LEVEL1_TestReadKeyFromMemA)
{
    LOG(INFO) << "Test TestReadKeyFromMem";
    size_t dataSize = 4 * 1024ul * 1024ul;

    std::string data(dataSize, '0');
    std::string appendStr = "1234567890abcdefghijk";
    data += appendStr;
    datasystem::SetParam param = type_;
    auto key = client_->Set(data, param);

    uint64_t size = data.size() - dataSize;
    ReadParam readParam{ .key = key, .offset = dataSize, .size = size };
    std::vector<ReadParam> params = { readParam };
    std::vector<Optional<ReadOnlyBuffer>> buffers;
    client_->Read(params, buffers);
    Optional<ReadOnlyBuffer> buffer = buffers.back();
    std::string getValue(reinterpret_cast<const char *>(buffer->ImmutableData()), buffer->GetSize());
    LOG(INFO) << "Key is " << key << " get value size: " << buffer->GetSize() << ", data: " << getValue;
    ASSERT_EQ(buffer->GetSize(), size);
    ASSERT_EQ(getValue, appendStr);
    params.emplace_back(readParam);
    ASSERT_EQ(client_->Read(params, buffers).GetCode(), K_INVALID);
}

TEST_P(KVClientOffsetReadOneHostTest, TestReadKeyFromMemSameKey)
{
    LOG(INFO) << "Test TestReadKeyFromMem";
    size_t count = 6;
    size_t dataSize = 4 * 1024ul * 1024ul;
    std::vector<std::string> keys;

    std::string data(dataSize, '0');
    std::string appendStr = "1234567890abcdefghijk";
    data += appendStr;
    datasystem::SetParam param = type_;
    for (size_t i = 0; i < count; ++i) {
        auto key = client_->Set(data, param);
        ASSERT_FALSE(key.empty());
        keys.emplace_back(std::move(key));
    }

    auto key = client_->Set(data, param);
    keys.emplace_back(std::move(key));

    std::string getKey = keys.back();

    ReadParam readParam{ .key = getKey, .offset = 100, .size = 2000 };
    ReadParam readParam2{ .key = getKey, .offset = 3000, .size = 4000 };
    std::vector<ReadParam> params = { readParam, readParam2 };
    std::vector<Optional<ReadOnlyBuffer>> buffers;
    ASSERT_EQ(client_->Read(params, buffers).GetCode(), K_INVALID);
}

TEST_P(KVClientOffsetReadOneHostTest, TestReadKeyFromMemReadAfterGet)
{
    LOG(INFO) << "TestReadKeyOverSize";
    size_t count = 10;
    size_t dataSize = 4 * 1024ul * 1024ul;
    std::vector<std::string> keys;
    uint64_t size2 = 300;
    std::string data(dataSize, '0');
    std::string appendStr = "1234567890abcdefghijk000000";

    for (uint64_t i = 0; i < size2; i++) {
        data = appendStr + data;
    }
    datasystem::SetParam param = type_;
    for (size_t i = 0; i < count; ++i) {
        auto key = client_->Set(data, param);
        ASSERT_FALSE(key.empty());
        keys.emplace_back(std::move(key));
    }

    std::string getKey = keys.back();

    std::string val;
    DS_ASSERT_OK(client_->Get(getKey, val));

    ReadParam readParam{ .key = getKey, .offset = 1000, .size = 4000 };
    std::vector<ReadParam> params = { readParam };
    std::vector<Optional<ReadOnlyBuffer>> buffers;
    client_->Read(params, buffers);
    Optional<ReadOnlyBuffer> buffer = buffers.front();
    std::string getValue(reinterpret_cast<const char *>(buffer->ImmutableData()), buffer->GetSize());
    LOG(INFO) << "Key is " << getKey << " get value size: " << buffer->GetSize();

    uint64_t size1 = 4000;
    uint64_t size3 = 1000;
    ASSERT_EQ(buffer->GetSize(), size1);
    ASSERT_EQ(getValue, data.substr(size3, size1));

    DS_ASSERT_OK(client_->Get(getKey, val));
    ASSERT_EQ(getValue, val.substr(size3, size1));
    ASSERT_EQ(data, val);
}

TEST_P(KVClientOffsetReadOneHostTest, TestReadKeyPerf)
{
    LOG(INFO) << "Test TestReadKeyFromMem";
    size_t count = 10;
    size_t dataSize = 4 * 1024ul * 1024ul;
    std::vector<std::string> keys;

    std::string data(dataSize, '0');
    std::string appendStr = "1234567890abcdefghijk";
    data += appendStr;
    datasystem::SetParam param = type_;
    for (size_t i = 0; i < count; ++i) {
        auto key = client_->Set(data, param);
        ASSERT_FALSE(key.empty());
        keys.emplace_back(std::move(key));
    }

    auto key = client_->Set(data, param);
    keys.emplace_back(std::move(key));

    std::string getKey = keys.front();

    Timer timer;
    uint64_t offset = 100;
    uint64_t offset2 = 1024ul * 1024ul;
    uint64_t offset3 = 4 * 1024ul * 1024ul;
    std::vector<Optional<ReadOnlyBuffer>> buffers;
    int count2 = 100;
    int offset200 = 200;
    for (auto i = 0; i < count2; i++) {
        ReadParam readParam{ .key = getKey, .offset = static_cast<uint64_t>(i + offset200), .size = offset };
        std::vector<ReadParam> params = { readParam };
        client_->Read(params, buffers);
    }

    LOG(INFO) << "Key is " << getKey << ", offset: " << offset << ", cost: " << timer.ElapsedMilliSecondAndReset()
              << " us";

    for (auto i = 0; i < count2; i++) {
        ReadParam readParam2{ .key = getKey, .offset = static_cast<uint64_t>(i + offset200), .size = offset2 };
        std::vector<ReadParam> params2 = { readParam2 };
        client_->Read(params2, buffers);
    }
    LOG(INFO) << "Key is " << getKey << ", offset: " << offset2 << ", cost: " << timer.ElapsedMilliSecondAndReset()
              << " us";

    for (auto i = 0; i < count2; i++) {
        ReadParam readParam3{ .key = getKey,
                              .offset = static_cast<uint64_t>(i + offset200),
                              .size = offset3 - offset200 };
        std::vector<ReadParam> params3 = { readParam3 };
        client_->Read(params3, buffers);
    }
    LOG(INFO) << "Key is " << getKey << ", offset: " << offset3 << ", cost: " << timer.ElapsedMilliSecondAndReset()
              << " us";
}

TEST_P(KVClientOffsetReadOneHostTest, TestReadKeyFromDiskRepeatReadOneKey)
{
    LOG(INFO) << "TestReadKeyFromDiskRepeatReadOneKey";
    size_t count = 10;
    size_t dataSize = 4 * 1024ul * 1024ul;
    std::vector<std::string> keys;

    std::string data(dataSize, '0');
    std::string appendStr = "1234567890abcdefghijk";
    data += appendStr;
    data = appendStr + data;
    datasystem::SetParam param = type_;
    for (size_t i = 0; i < count; ++i) {
        auto key = client_->Set(data, param);
        ASSERT_FALSE(key.empty());
        keys.emplace_back(std::move(key));
    }

    std::string getKey = keys.front();

    uint64_t start1 = 0;
    uint64_t offset1 = 10;
    ReadParam readParam{ .key = getKey, .offset = start1, .size = offset1 };
    std::vector<ReadParam> params = { readParam };
    std::vector<Optional<ReadOnlyBuffer>> buffers;
    client_->Read(params, buffers);
    Optional<ReadOnlyBuffer> buffer = buffers.back();
    std::string getValue(reinterpret_cast<const char *>(buffer->ImmutableData()), buffer->GetSize());
    LOG(INFO) << "[FirstRead] Key is " << getKey << " get value size: " << buffer->GetSize();
    ASSERT_EQ(buffer->GetSize(), offset1);
    ASSERT_EQ(getValue, data.substr(start1, offset1));

    uint64_t start2 = 5;
    uint64_t offset2 = 10;
    ReadParam readParam1{ .key = getKey, .offset = start2, .size = offset2 };
    std::vector<ReadParam> params1 = { readParam1 };
    client_->Read(params1, buffers);
    Optional<ReadOnlyBuffer> buffer1 = buffers.back();
    std::string getValue2(reinterpret_cast<const char *>(buffer1->ImmutableData()), buffer1->GetSize());
    LOG(INFO) << "[diff start, Same offset] Key is " << getKey << " get value size: " << buffer1->GetSize();
    ASSERT_EQ(buffer1->GetSize(), offset2);
    ASSERT_EQ(getValue2, data.substr(start2, offset2));

    uint64_t start3 = 5;
    uint64_t offset3 = 30;
    ReadParam readParam3{ .key = getKey, .offset = start3, .size = offset3 };
    std::vector<ReadParam> params3 = { readParam3 };
    client_->Read(params3, buffers);
    buffer = buffers.back();
    std::string getValue3(reinterpret_cast<const char *>(buffer->ImmutableData()), buffer->GetSize());
    LOG(INFO) << "[Same start, diff offset] Key is " << getKey << " get value size: " << buffer->GetSize();
    ASSERT_EQ(buffer->GetSize(), offset3);
    ASSERT_EQ(getValue3, data.substr(start3, offset3));

    uint64_t start4 = 0;
    uint64_t offset4 = data.size();
    ReadParam readParam4{ .key = getKey, .offset = start4, .size = offset4 };
    std::vector<ReadParam> params4 = { readParam4 };
    client_->Read(params4, buffers);
    buffer = buffers.back();
    std::string getValue4(reinterpret_cast<const char *>(buffer->ImmutableData()), buffer->GetSize());
    LOG(INFO) << "[0 start, all offset] Key is " << getKey << " get value size: " << buffer->GetSize();
    ASSERT_EQ(buffer->GetSize(), offset4);
    ASSERT_EQ(getValue4, data.substr(start4, offset4));

    uint64_t start5 = 100;
    uint64_t offset5 = 2000;
    ReadParam readParam5{ .key = getKey, .offset = start5, .size = offset5 };
    std::vector<ReadParam> params5 = { readParam5 };
    client_->Read(params5, buffers);
    buffer = buffers.back();
    std::string getValue5(reinterpret_cast<const char *>(buffer->ImmutableData()), buffer->GetSize());
    LOG(INFO) << "[random start, random offset] Key is " << getKey << " get value size: " << buffer->GetSize();
    ASSERT_EQ(buffer->GetSize(), offset5);
    ASSERT_EQ(getValue5, data.substr(start5, offset5));

    std::string getKey2 = keys[1];
    ReadParam readParam6{ .key = getKey, .offset = start5, .size = offset5 };
    ReadParam readParam7{ .key = getKey2, .offset = start3, .size = offset5 };
    std::vector<ReadParam> params6 = { readParam6, readParam7 };
    client_->Read(params6, buffers);
    buffer = buffers.back();
    std::string getValue6(reinterpret_cast<const char *>(buffer->ImmutableData()), buffer->GetSize());
    LOG(INFO) << "[random start, random offset] Key is " << getKey << " get value size: " << buffer->GetSize();
    ASSERT_EQ(buffer->GetSize(), offset5);
    ASSERT_EQ(getValue6, data.substr(start3, offset5));
}

TEST_P(KVClientOffsetReadOneHostTest, DISABLED_TestReadKeyFromDisk)
{
    LOG(INFO) << "TestReadKeyFromDisk";
    size_t count = 4;
    size_t dataSize = 4 * 1024ul * 1024ul;
    std::vector<std::string> keys;

    std::string data(dataSize, '0');
    std::string appendStr = "1234567890abcdefghijk";
    data += appendStr;
    datasystem::SetParam param = type_;
    for (size_t i = 0; i < count; ++i) {
        auto key = client_->Set(data, param);
        ASSERT_FALSE(key.empty());
        keys.emplace_back(std::move(key));
    }

    auto key = client_->Set(data, param);
    keys.emplace_back(std::move(key));

    std::string getKey = keys.front();
    std::string getKey1 = keys[1];

    uint64_t offset = data.size() - dataSize;
    ReadParam readParam{ .key = getKey, .offset = dataSize, .size = offset };
    ReadParam readParam2{ .key = getKey1, .offset = dataSize - 2, .size = 10 };
    std::vector<ReadParam> params = { readParam, readParam2 };
    std::vector<Optional<ReadOnlyBuffer>> buffers;
    client_->Read(params, buffers);
    Optional<ReadOnlyBuffer> buffer = buffers.front();
    std::string getValue(reinterpret_cast<const char *>(buffer->ImmutableData()), buffer->GetSize());
    LOG(INFO) << "Key is " << getKey << " get value size: " << buffer->GetSize() << ", data: " << getValue;
    ASSERT_EQ(buffer->GetSize(), offset);
    ASSERT_EQ(getValue, appendStr);

    Optional<ReadOnlyBuffer> buffer1 = buffers[1];
    std::string getValue1(reinterpret_cast<const char *>(buffer1->ImmutableData()), buffer1->GetSize());
    LOG(INFO) << "Key is " << getKey1 << " get value size: " << buffer1->GetSize() << ", data: " << getValue1;
    uint64_t size10 = 10;
    ASSERT_EQ(buffer1->GetSize(), size10);
    ASSERT_EQ(getValue1, "0012345678");
}

TEST_P(KVClientOffsetReadOneHostTest, LEVEL2_TestReadKeyAfterGet)
{
    LOG(INFO) << "TestReadKeyOverSize";
    size_t count = 10;
    size_t dataSize = 4 * 1024ul * 1024ul;
    std::vector<std::string> keys;

    std::string data(dataSize, '0');
    std::string appendStr = "1234567890abcdefghijk000000";
    size_t count300 = 300;
    for (size_t i = 0; i < count300; i++) {
        data = appendStr + data;
    }
    datasystem::SetParam param = type_;
    for (size_t i = 0; i < count; ++i) {
        auto key = client_->Set(data, param);
        ASSERT_FALSE(key.empty());
        keys.emplace_back(std::move(key));
    }

    std::string getKey = keys.front();

    std::string val;
    DS_ASSERT_OK(client_->Get(getKey, val));

    ReadParam readParam{ .key = getKey, .offset = 1000, .size = 4000 };
    std::vector<ReadParam> params = { readParam };
    std::vector<Optional<ReadOnlyBuffer>> buffers;
    client_->Read(params, buffers);
    Optional<ReadOnlyBuffer> buffer = buffers.front();
    std::string getValue(reinterpret_cast<const char *>(buffer->ImmutableData()), buffer->GetSize());
    LOG(INFO) << "Key is " << getKey << " get value size: " << buffer->GetSize();
    uint64_t size4000 = 4000;
    uint64_t size1000 = 1000;
    ASSERT_EQ(buffer->GetSize(), size4000);
    ASSERT_EQ(getValue, data.substr(size1000, size4000));
}

TEST_P(KVClientOffsetReadOneHostTest, TestReadKeyFromMemForDiffRange)
{
    LOG(INFO) << "TestReadKeyFromMemForDiffRange";
    size_t dataSize = 4 * 1024ul * 1024ul;

    std::string data(dataSize, '0');
    std::string appendStr = "1234567890abcdefghijk";
    data += appendStr;
    datasystem::SetParam param = type_;
    auto key = client_->Set(data, param);
    ASSERT_FALSE(key.empty());

    std::string getKey = key;
    size_t totalSize = data.size();
    // offset out of range
    ReadParam readParam1{ .key = getKey, .offset = totalSize, .size = 1 };
    std::vector<Optional<ReadOnlyBuffer>> buffers1;
    ASSERT_EQ(client_->Read({ readParam1 }, buffers1).GetCode(), K_OUT_OF_RANGE);

    // size + offset out of range.
    ReadParam readParam2{ .key = getKey, .offset = totalSize - 2, .size = 5 };
    std::vector<Optional<ReadOnlyBuffer>> buffers2;
    DS_ASSERT_OK(client_->Read({ readParam2 }, buffers2));
    Optional<ReadOnlyBuffer> buffer = buffers2.front();
    std::string getValue(reinterpret_cast<const char *>(buffer->ImmutableData()), buffer->GetSize());
    LOG(INFO) << "Key is " << getKey << " get value size: " << buffer->GetSize();
    ASSERT_EQ(getValue, "jk");

    // full read.
    ReadParam readParam3{ .key = getKey, .offset = 0, .size = 0 };
    std::vector<Optional<ReadOnlyBuffer>> buffers3;
    DS_ASSERT_OK(client_->Read({ readParam3 }, buffers3));
    Optional<ReadOnlyBuffer> buffer2 = buffers3.front();
    std::string getValue2(reinterpret_cast<const char *>(buffer2->ImmutableData()), buffer2->GetSize());
    LOG(INFO) << "Key is " << getKey << " get value size: " << buffer2->GetSize();
    ASSERT_EQ(getValue2, data);
}

TEST_P(KVClientOffsetReadOneHostTest, TestReadKeyFromDiskForDiffRange)
{
    LOG(INFO) << "TestReadKeyFromDiskForDiffRange";
    size_t count = 10;
    size_t dataSize = 4 * 1024ul * 1024ul;
    std::vector<std::string> keys;

    std::string data(dataSize, '0');
    std::string appendStr = "1234567890abcdefghijk";
    data += appendStr;
    datasystem::SetParam param = type_;
    for (size_t i = 0; i < count; ++i) {
        auto key = client_->Set(data, param);
        ASSERT_FALSE(key.empty());
        keys.emplace_back(std::move(key));
    }

    auto key = client_->Set(data, param);
    keys.emplace_back(std::move(key));

    std::string getKey = keys.front();
    size_t totalSize = data.size();
    // offset out of range
    ReadParam readParam1{ .key = getKey, .offset = totalSize, .size = 1 };
    std::vector<Optional<ReadOnlyBuffer>> buffers1;
    ASSERT_EQ(client_->Read({ readParam1 }, buffers1).GetCode(), K_OUT_OF_RANGE);

    // size + offset out of range.
    ReadParam readParam2{ .key = getKey, .offset = totalSize - 2, .size = 5 };
    std::vector<Optional<ReadOnlyBuffer>> buffers2;
    DS_ASSERT_OK(client_->Read({ readParam2 }, buffers2));
    Optional<ReadOnlyBuffer> buffer = buffers2.front();
    std::string getValue(reinterpret_cast<const char *>(buffer->ImmutableData()), buffer->GetSize());
    LOG(INFO) << "Key is " << getKey << " get value size: " << buffer->GetSize();
    ASSERT_EQ(getValue, "jk");

    // full read.
    ReadParam readParam3{ .key = getKey, .offset = 0, .size = 0 };
    std::vector<Optional<ReadOnlyBuffer>> buffers3;
    DS_ASSERT_OK(client_->Read({ readParam3 }, buffers3));
    Optional<ReadOnlyBuffer> buffer2 = buffers3.front();
    std::string getValue2(reinterpret_cast<const char *>(buffer2->ImmutableData()), buffer2->GetSize());
    LOG(INFO) << "Key is " << getKey << " get value size: " << buffer2->GetSize();
    ASSERT_EQ(getValue2, data);
}

class KVClientOffsetRemoteReadTest : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        const int kNumEtcd = 1;
        const int kNumWorkers = 2;
        const int knumOBS = 1;
        const int kOcThreadNum = 64;
        const int kWorkerCount = 2;

        opts.enableSpill = true;
        opts.workerGflagParams =
            "-shared_memory_size_mb=200 "
            "-log_async=false "
            "-log_monitor=true "
            "-v=2 "
            "-spill_size_limit="
            + std::to_string(maxSize_);

        opts.numEtcd = kNumEtcd;
        opts.numWorkers = kNumWorkers;
        opts.numOBS = knumOBS;
        opts.enableDistributedMaster = "false";
        opts.numOcThreadNum = kOcThreadNum;
        opts.injectActions = "worker.Spill.Sync:return()";

        for (int i = 0; i < kWorkerCount; ++i) {
            std::string dir =
                "./ds/KVClientOffsetRemoteReadTest.RemoteRead/worker" + std::to_string(i) + "/shared_disk";
            int diskSize = (i == 0) ? 256 : 16;
            opts.workerSpecifyGflagParams[i] +=
                " -shared_disk_directory=" + dir + " -shared_disk_size_mb=" + std::to_string(diskSize);
        }
    }

    void SetUp() override
    {
        ExternalClusterTest::SetUp();
        InitTestKVClientWithTenant(0, client_);
        InitTestKVClientWithTenant(1, client1_);
    }

    void InitConnectOptW(uint32_t workerIndex, ConnectOptions &connectOptions, int32_t timeoutMs = 60000)
    {
        HostPort workerAddress;
        ASSERT_TRUE(workerIndex < cluster_->GetWorkerNum());
        DS_ASSERT_OK(cluster_->GetWorkerAddr(workerIndex, workerAddress));
        connectOptions = { .host = workerAddress.Host(), .port = workerAddress.Port(), .connectTimeoutMs = timeoutMs };
        connectOptions.accessKey = "QTWAOYTTINDUT2QVKYUC";
        connectOptions.secretKey = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
    }

    void InitTestKVClientWithTenant(uint32_t workerIndex, std::shared_ptr<KVClient> &client)
    {
        ConnectOptions connectOptions;
        InitConnectOptW(workerIndex, connectOptions);
        client = std::make_shared<KVClient>(connectOptions);
        DS_ASSERT_OK(client->Init());
    }

    void RemoteRead(std::shared_ptr<KVClient> client, const std::vector<std::string> &keys, const std::string &data,
                    size_t offset, size_t size)
    {
        const size_t kReadLoopCount = 50;
        for (size_t i = 0; i < kReadLoopCount; ++i) {
            ReadParam param = { .key = keys[0], .offset = offset, .size = size };
            std::vector<ReadParam> params = { param };
            std::string expectedValue = data.substr(offset, size);

            std::vector<Optional<ReadOnlyBuffer>> buffers;
            DS_ASSERT_OK(client->Read(params, buffers));

            std::string actualValue(reinterpret_cast<const char *>(buffers[0]->ImmutableData()), buffers[0]->GetSize());

            ASSERT_EQ(buffers[0]->GetSize(), size);
            ASSERT_EQ(actualValue, expectedValue);
        }
    }

protected:
    std::shared_ptr<KVClient> client_;
    std::shared_ptr<KVClient> client1_;
    datasystem::SetParam type_;
    uint64_t maxSize_ = 5 * 1024 * 1024ul * 1024ul;
};

TEST_F(KVClientOffsetRemoteReadTest, LEVEL2_ConcurrentReadSameKey)
{
    std::shared_ptr<KVClient> client;
    InitTestKVClient(0, client);
    std::shared_ptr<KVClient> client1;
    InitTestKVClient(1, client1);
    std::shared_ptr<KVClient> client2;
    InitTestKVClient(1, client2);
    size_t count = 100;
    size_t dataSize = 2 * 1024ul * 1024ul;
    std::vector<std::string> keys;
    std::string data = randomData_.GetRandomString((dataSize));

    datasystem::SetParam param{ .writeMode = WriteMode::NONE_L2_CACHE_EVICT };
    for (size_t i = 0; i < count; ++i) {
        auto key = client->Set(data, param);
        ASSERT_FALSE(key.empty());
    }
    for (size_t i = 0; i < count; ++i) {
        auto key = client->Set(data, param);
        ASSERT_FALSE(key.empty());
        keys.emplace_back(std::move(key));
    }

    int threadNum = 10;
    std::vector<std::future<void>> results;

    const size_t kOffset1 = 50;
    const size_t kSize1 = 2030;
    const size_t kOffset2 = 100;
    const size_t kSize2 = 1080;
    const int kThreadPoolSize = 50;
    {
        ThreadPool pool(kThreadPoolSize);
        for (int m = 0; m < threadNum; ++m) {
            results.emplace_back(pool.Submit(
                [this, client1, keys, data]() { this->RemoteRead(client1, keys, data, kOffset1, kSize1); }));
        }
        for (int m = 0; m < threadNum; ++m) {
            results.emplace_back(pool.Submit(
                [this, client2, keys, data]() { this->RemoteRead(client2, keys, data, kOffset2, kSize2); }));
        }
        for (auto &res : results) {
            (void)res.get();
        }
    }
    std::vector<std::string> failedKeys;
    DS_ASSERT_OK(client->Del(keys, failedKeys));
    ASSERT_EQ(failedKeys.size(), 0);
}

class KVClientOffsetReadRemoteTest : public OCClientCommon,
                                        public testing::WithParamInterface<datasystem::SetParam> {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.enableSpill = true;
        opts.workerGflagParams =
            "-shared_memory_size_mb=12 -log_async=false -log_monitor=true -shared_disk_directory=./" + GetStringUuid()
            + " -shared_disk_size_mb=1024 -v=1 -spill_size_limit=" + std::to_string(maxSize_);
        opts.numEtcd = 1;
        opts.numWorkers = 2;  // worker num is 2
        opts.numOBS = 1;
        opts.enableDistributedMaster = "false";
    }

    void SetUp() override
    {
        type_ = GetParam();
        ExternalClusterTest::SetUp();
        InitTestKVClientWithTenant(0, client_);
        InitTestKVClientWithTenant(1, client1_);
    }

    void InitConnectOptW(uint32_t workerIndex, ConnectOptions &connectOptions, int32_t timeoutMs = 60000)
    {
        HostPort workerAddress;
        ASSERT_TRUE(workerIndex < cluster_->GetWorkerNum());
        DS_ASSERT_OK(cluster_->GetWorkerAddr(workerIndex, workerAddress));
        connectOptions = { .host = workerAddress.Host(), .port = workerAddress.Port(), .connectTimeoutMs = timeoutMs };
        connectOptions.accessKey = "QTWAOYTTINDUT2QVKYUC";
        connectOptions.secretKey = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
    }

    void InitTestKVClientWithTenant(uint32_t workerIndex, std::shared_ptr<KVClient> &client)
    {
        ConnectOptions connectOptions;
        InitConnectOptW(workerIndex, connectOptions);
        client = std::make_shared<KVClient>(connectOptions);
        DS_ASSERT_OK(client->Init());
    }

protected:
    std::shared_ptr<KVClient> client_;
    std::shared_ptr<KVClient> client1_;
    datasystem::SetParam type_;
    uint64_t maxSize_ = 64 * 1024ul * 1024ul;
};

TEST_P(KVClientOffsetReadRemoteTest, SubscribeAndGetParal)
{
    std::shared_ptr<KVClient> client, client1, client2;
    InitTestKVClient(0, client);
    InitTestKVClient(0, client1);
    InitTestKVClient(1, client2);
    std::string key = "key";
    std::string value = "value";
    std::string valGet;
    std::thread t1([&]() {
        DS_ASSERT_OK(client->Get(key, valGet, 300'000));  // sub time is 300'000 ms
    });
    datasystem::SetParam param = type_;
    std::thread t2([&]() {
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));  // wait time is 1000ms
        DS_ASSERT_OK(client2->Set(key, value, param));
    });
    std::thread t3([&]() {
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));  // wait time is 1000ms
        DS_ASSERT_OK(client1->Get(key, valGet, 300'000));              // sub time is 300'000 ms
    });
    t1.join();
    t2.join();
    t3.join();
}

TEST_P(KVClientOffsetReadRemoteTest, LEVEL1_TestReadKeyRemoteGet)
{
    LOG(INFO) << "TestReadKeyOverSize";
    size_t count = 10;
    size_t dataSize = 4 * 1024ul * 1024ul;
    std::vector<std::string> keys;

    std::string data(dataSize, '0');
    std::string appendStr = "1234567890abcdefghijk000000";
    size_t count300 = 300;
    for (size_t i = 0; i < count300; i++) {
        data = appendStr + data;
    }
    datasystem::SetParam param = type_;
    for (size_t i = 0; i < count; ++i) {
        auto key = client_->Set(data, param);
        ASSERT_FALSE(key.empty());
        keys.emplace_back(std::move(key));
    }

    std::string getKey = keys.front();

    ReadParam readParam1{ .key = getKey, .offset = data.size(), .size = 4000 };
    std::vector<ReadParam> params1 = { readParam1 };
    std::vector<Optional<ReadOnlyBuffer>> buffers1;
    auto status = client1_->Read(params1, buffers1);
    ASSERT_EQ(status.GetCode(), K_OUT_OF_RANGE);
    ReadParam readParam{ .key = getKey, .offset = 1000, .size = 4000 };
    std::vector<ReadParam> params = { readParam };
    std::vector<Optional<ReadOnlyBuffer>> buffers;
    client1_->Read(params, buffers);
    Optional<ReadOnlyBuffer> buffer = buffers.front();
    std::string getValue(reinterpret_cast<const char *>(buffer->ImmutableData()), buffer->GetSize());
    LOG(INFO) << "Key is " << getKey << " get value size: " << buffer->GetSize();
    uint64_t size4000 = 4000;
    uint64_t size1000 = 1000;
    ASSERT_EQ(buffer->GetSize(), size4000);
    ASSERT_EQ(getValue, data.substr(size1000, size4000));
}

TEST_P(KVClientOffsetReadRemoteTest, OffsetAndSizeOutOfDataSize)
{
    LOG(INFO) << "OffsetAndSizeOutOfDataSize";
    size_t count = 10;
    size_t dataSize = 4 * 1024ul * 1024ul;
    std::vector<std::string> keys;

    std::string data(dataSize, '0');
    std::string appendStr = "1234567890abcdefghijk";
    data += appendStr;
    datasystem::SetParam param = type_;
    for (size_t i = 0; i < count; ++i) {
        auto key = client_->Set(data, param);
        ASSERT_FALSE(key.empty());
        keys.emplace_back(std::move(key));
    }
    std::vector<ReadParam> params;
    std::vector<std::pair<size_t, size_t>> offsetInfos = {
        { 0, data.size() }, { 0, 1 },   { data.size() - 1, 1 },   { 20, data.size() },
        { 0, 10 },          { 16, 32 }, { data.size() - 10, 10 }, { 0, data.size() + 10 }
    };
    for (auto &key : keys) {
        for (auto info : offsetInfos) {
            std::vector<Optional<ReadOnlyBuffer>> buffers;
            DS_ASSERT_OK(
                client1_->Read({ ReadParam{ .key = key, .offset = info.first, .size = info.second } }, buffers));
            Optional<ReadOnlyBuffer> buffer = buffers.front();
            std::string getValue(reinterpret_cast<const char *>(buffer->ImmutableData()), buffer->GetSize());
            auto receiveSz = info.first + info.second > data.size() ? data.size() - info.first : info.second;
            ASSERT_EQ(getValue, data.substr(info.first, receiveSz));
        }
    }
}

TEST_P(KVClientOffsetReadRemoteTest, TestReadOffsetNotFromShm)
{
    LOG(INFO) << "OffsetAndSizeOutOfDataSize";
    size_t count = 10;
    size_t dataSize = 4 * 1024ul;
    std::vector<std::string> keys;

    std::string data(dataSize, '0');
    std::string appendStr = "1234567890abcdefghijk";
    data += appendStr;
    datasystem::SetParam param = type_;
    for (size_t i = 0; i < count; ++i) {
        auto key = client_->Set(data, param);
        ASSERT_FALSE(key.empty());
        keys.emplace_back(std::move(key));
    }
 
    std::vector<ReadParam> params;
    std::vector<std::pair<size_t, size_t>> offsetInfos = {
        { 0, 1 },   { data.size() - 1, 1 },   { 20, data.size() },     { 0, 10 },
        { 16, 32 }, { data.size() - 10, 10 }, { 0, data.size() + 10 }, { 0, data.size() },
    };
    for (auto &key : keys) {
        for (auto info : offsetInfos) {
            LOG(INFO) << "------------" << key;
            std::vector<Optional<ReadOnlyBuffer>> buffers;
            DS_ASSERT_OK(
                client1_->Read({ ReadParam{ .key = key, .offset = info.first, .size = info.second } }, buffers));
            Optional<ReadOnlyBuffer> buffer = buffers.front();
            std::string getValue(reinterpret_cast<const char *>(buffer->ImmutableData()));
            auto receiveSz = info.first + info.second > data.size() ? data.size() - info.first : info.second;
            ASSERT_EQ(getValue, data.substr(info.first, receiveSz));
        }
    }
}

TEST_P(KVClientOffsetReadRemoteTest, TestInvalidOffset)
{
    LOG(INFO) << "OffsetAndSizeOutOfDataSize";
    size_t dataSize = 4 * 1024ul;
    std::vector<std::string> keys;
    int count = 10;
    std::string data(dataSize, '0');
    std::string appendStr = "1234567890abcdefghijk";
    data += appendStr;
    datasystem::SetParam param = type_;
    for (int i = 0; i < count; i++) {
        auto key = "adusaidu" + std::to_string(i);
        DS_ASSERT_OK(client_->Set(key, data, param));
        keys.emplace_back(key);
    }
    for (int i = 0; i < count; i++) {
        ReadParam readParam2{ .key = keys[i], .offset = data.size() + 2, .size = 5 };
        std::vector<Optional<ReadOnlyBuffer>> buffers2;
        ASSERT_EQ(client_->Read({ readParam2 }, buffers2).GetCode(), K_OUT_OF_RANGE);
    }
}

TEST_P(KVClientOffsetReadRemoteTest, TestGetFailedAfterRemoteRead)
{
    LOG(INFO) << "TestGetFailedAfterRemoteRead";
    size_t dataSize = 4 * 1024ul * 1024ul;
    std::vector<std::string> keys;

    std::string data(dataSize, '0');
    std::string appendStr = "1234567890abcdefghijk000000";
    data = appendStr + data;

    datasystem::SetParam param = type_;
    auto key = client_->Set(data, param);

    ReadParam readParam{ .key = key, .offset = 1000, .size = 4000 };
    std::vector<ReadParam> params = { readParam };
    std::vector<Optional<ReadOnlyBuffer>> buffers;
    DS_ASSERT_OK(client1_->Read(params, buffers));
    Optional<ReadOnlyBuffer> buffer = buffers.front();
    std::string getValue(reinterpret_cast<const char *>(buffer->ImmutableData()), buffer->GetSize());
    LOG(INFO) << "Key is " << key << " get value size: " << buffer->GetSize();
    uint64_t size4000 = 4000;
    uint64_t size1000 = 1000;
    ASSERT_EQ(buffer->GetSize(), size4000);
    ASSERT_EQ(getValue, data.substr(size1000, size4000));
    DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, 0));
    std::string val;
    DS_ASSERT_NOT_OK(client1_->Get(key, val));
}

TEST_P(KVClientOffsetReadRemoteTest, TestRemoteGetFromMaster)
{
    size_t dataSize = 4 * 1024ul;
    std::vector<std::string> keys;
    int count = 10;
    std::string data(dataSize, '0');
    std::string appendStr = "1234567890abcdefghijk";
    data += appendStr;
    datasystem::SetParam param = type_;
    for (int i = 0; i < count; i++) {
        auto key = "adusaidu" + std::to_string(i);
        DS_ASSERT_OK(client_->Set(key, data, param));
        keys.emplace_back(key);
    }
    for (int i = 0; i < count; i++) {
        ReadParam readParam2{ .key = keys[i], .offset = data.size() + 2, .size = 5 };
        std::vector<Optional<ReadOnlyBuffer>> buffers2;
        ASSERT_EQ(client1_->Read({ readParam2 }, buffers2).GetCode(), K_OUT_OF_RANGE);
    }
    std::vector<std::string> failedKeys;
    DS_ASSERT_OK(client_->Del(keys, failedKeys));
    ASSERT_EQ(failedKeys.size(), 0);
}

TEST_P(KVClientOffsetReadRemoteTest, LEVEL2_TestReadKeyRemoteGetFromDisk)
{
    LOG(INFO) << "TestReadKeyFromDisk";
    size_t count = 10;
    size_t dataSize = 4 * 1024ul * 1024ul;
    std::vector<std::string> keys;

    std::string data(dataSize, '0');
    std::string appendStr = "1234567890abcdefghijk";
    data += appendStr;
    datasystem::SetParam param = type_;
    for (size_t i = 0; i < count; ++i) {
        auto key = client_->Set(data, param);
        ASSERT_FALSE(key.empty());
        keys.emplace_back(std::move(key));
    }

    std::string getKey = keys.front();
    std::string getKey1 = keys[1];

    uint64_t offset = data.size() - dataSize;
    ReadParam readParam{ .key = getKey, .offset = dataSize, .size = offset };
    ReadParam readParam2{ .key = getKey1, .offset = dataSize - 2, .size = 10 };
    std::vector<ReadParam> params = { readParam, readParam2 };
    std::vector<Optional<ReadOnlyBuffer>> buffers;
    client1_->Read(params, buffers);
    Optional<ReadOnlyBuffer> buffer = buffers.front();
    std::string getValue(reinterpret_cast<const char *>(buffer->ImmutableData()), buffer->GetSize());
    LOG(INFO) << "Key is " << getKey << " get value size: " << buffer->GetSize() << ", data: " << getValue;
    ASSERT_EQ(buffer->GetSize(), offset);
    ASSERT_EQ(getValue, appendStr);

    Optional<ReadOnlyBuffer> buffer1 = buffers[1];
    std::string getValue1(reinterpret_cast<const char *>(buffer1->ImmutableData()), buffer1->GetSize());
    LOG(INFO) << "Key is " << getKey1 << " get value size: " << buffer1->GetSize() << ", data: " << getValue1;
    uint64_t size10 = 10;
    ASSERT_EQ(buffer1->GetSize(), size10);
    ASSERT_EQ(getValue1, "0012345678");
}

class KVClientOffsetReadRemoteParallelTest : public KVClientOffsetReadRemoteTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.workerGflagParams =
            "-shared_memory_size_mb=1024 -log_async=false -log_monitor=true -shared_disk_directory=./" + GetStringUuid()
            + " -shared_disk_size_mb=1024 -v=1 " + std::to_string(maxSize_);
        opts.numEtcd = 1;
        opts.numWorkers = 2;  // worker num is 2
        opts.numOBS = 1;
        opts.enableDistributedMaster = "false";
    }

    void SetUp() override
    {
        type_ = GetParam();
        ExternalClusterTest::SetUp();
        InitTestKVClientWithTenant(0, client_);
        InitTestKVClientWithTenant(1, client1_);
    }
};

TEST_P(KVClientOffsetReadRemoteParallelTest, TestReadKeyRemoteAndPublishParallel)
{
    LOG(INFO) << "TestReadKeyFromDisk";
    size_t count = 10;
    size_t dataSize = 4 * 1024ul * 1024ul;
    std::vector<std::string> keys;

    std::string data = randomData_.GetRandomString(dataSize);
    std::string data1 = randomData_.GetRandomString(dataSize);
    datasystem::SetParam param = type_;
    for (size_t i = 0; i < count; ++i) {
        auto key = client_->Set(data, param);
        ASSERT_FALSE(key.empty());
        keys.emplace_back(std::move(key));
    }

    std::vector<ReadParam> params1;
    std::vector<std::string> readOffset1;
    for (size_t i = 0; i < keys.size(); i++) {
        ReadParam param1 = { .key = keys[i],
                             .offset = (uint64_t)randomData_.GetRandomUint64(0L, (uint64_t)(dataSize / 2)),
                             .size = dataSize / 1024 };
        readOffset1.emplace_back(data.substr(param1.offset, param1.size));
        params1.emplace_back(param1);
    }

    std::thread t1([this, params1, readOffset1, keys] {
        std::vector<Optional<ReadOnlyBuffer>> buffers;
        DS_ASSERT_OK(client1_->Read(params1, buffers));
        for (size_t i = 0; i < buffers.size(); ++i) {
            LOG(INFO) << "------------" << keys[i];
            ASSERT_EQ(buffers[i]->GetSize(), params1[i].size);
        }
    });

    std::thread t2([this, param, data1, keys] {
        for (size_t i = 0; i < keys.size(); ++i) {
            LOG(INFO) << "------------" << keys[i];
            DS_ASSERT_OK(client1_->Set(keys[i], data1, param));
        }
    });

    t1.join();
    t2.join();
    std::vector<Optional<ReadOnlyBuffer>> buffers;
    DS_ASSERT_OK(client1_->Get(keys, buffers));
    for (size_t i = 0; i < buffers.size(); ++i) {
        LOG(INFO) << "------------" << keys[i];
        std::string getValue1(reinterpret_cast<const char *>(buffers[i]->ImmutableData()), buffers[i]->GetSize());
        ASSERT_EQ(buffers[i]->GetSize(), data1.size());
        ASSERT_EQ(getValue1, data1);
    }
}

TEST_P(KVClientOffsetReadRemoteParallelTest, TestReadKeyRemoteAndGetParallel)
{
    LOG(INFO) << "TestReadKeyFromDisk";
    size_t count = 10;
    size_t dataSize = 4 * 1024ul;
    std::vector<std::string> keys;

    std::string data = randomData_.GetRandomString(dataSize);
    datasystem::SetParam param = type_;
    for (size_t i = 0; i < count; ++i) {
        auto key = client_->Set(data, param);
        ASSERT_FALSE(key.empty());
        keys.emplace_back(std::move(key));
    }

    std::vector<ReadParam> params1;
    std::vector<std::string> readOffset1;
    for (size_t i = 0; i < keys.size(); i++) {
        ReadParam param1 = { .key = keys[i],
                             .offset = (uint64_t)randomData_.GetRandomUint64(0L, (uint64_t)(dataSize / 2)),
                             .size = dataSize / 1024 };
        readOffset1.emplace_back(data.substr(param1.offset, param1.size));
        params1.emplace_back(param1);
    }

    std::thread t1([this, params1, readOffset1, keys] {
        std::vector<Optional<ReadOnlyBuffer>> buffers;
        DS_ASSERT_OK(client1_->Read(params1, buffers));
        for (size_t i = 0; i < buffers.size(); ++i) {
            LOG(INFO) << "-------read-----" << keys[i];
            ASSERT_EQ(buffers[i]->GetSize(), params1[i].size);
        }
    });

    std::thread t2([this, dataSize, keys] {
        std::vector<Optional<ReadOnlyBuffer>> buffers;
        DS_ASSERT_OK(client1_->Get(keys, buffers));
        for (size_t i = 0; i < buffers.size(); ++i) {
            LOG(INFO) << "------get------" << keys[i];
            ASSERT_EQ(buffers[i]->GetSize(), dataSize);
        }
    });

    t1.join();
    t2.join();
}

TEST_P(KVClientOffsetReadRemoteParallelTest, TestReadKeyRemoteGetParallel)
{
    LOG(INFO) << "TestReadKeyFromDisk";
    size_t count = 10;
    size_t dataSize = 4 * 1024ul * 1024ul;
    std::vector<std::string> keys;

    std::string data = randomData_.GetRandomString(dataSize);
    std::string appendStr = "1234567890abcdefghijk";
    data += appendStr;
    datasystem::SetParam param = type_;
    for (size_t i = 0; i < count; ++i) {
        auto key = client_->Set(data, param);
        ASSERT_FALSE(key.empty());
        keys.emplace_back(std::move(key));
    }

    std::vector<ReadParam> params1, params2;
    std::vector<std::string> readOffset1, readOffset2;
    for (size_t i = 0; i < keys.size(); i++) {
        ReadParam param1 = { .key = keys[i],
                             .offset = (uint64_t)randomData_.GetRandomUint64(0L, (uint64_t)(dataSize / 2)),
                             .size = dataSize / 1024 };
        ReadParam param2 = { .key = keys[i],
                             .offset = (uint64_t)randomData_.GetRandomUint64(0L, (uint64_t)(dataSize / 2)),
                             .size = dataSize / 1024 };
        readOffset1.emplace_back(data.substr(param1.offset, param1.size));
        readOffset2.emplace_back(data.substr(param2.offset, param2.size));
        params1.emplace_back(param1);
        params2.emplace_back(param2);
    }

    std::thread t1([this, params1, readOffset1, keys] {
        std::vector<Optional<ReadOnlyBuffer>> buffers;
        DS_ASSERT_OK(client1_->Read(params1, buffers));
        for (size_t i = 0; i < buffers.size(); ++i) {
            LOG(INFO) << "------------" << keys[i];
            std::string getValue1(reinterpret_cast<const char *>(buffers[i]->ImmutableData()), buffers[i]->GetSize());
            ASSERT_EQ(buffers[i]->GetSize(), params1[i].size);
            ASSERT_EQ(getValue1, readOffset1[i]);
        }
    });

    std::thread t2([this, params2, readOffset2, keys] {
        std::vector<Optional<ReadOnlyBuffer>> buffers;
        DS_ASSERT_OK(client1_->Read(params2, buffers));
        for (size_t i = 0; i < buffers.size(); ++i) {
            LOG(INFO) << "------------" << keys[i] << params2[i].size << "----------" << params2[i].offset;
            std::string getValue1(reinterpret_cast<const char *>(buffers[i]->ImmutableData()), buffers[i]->GetSize());
            ASSERT_EQ(buffers[i]->GetSize(), params2[i].size);
            ASSERT_EQ(getValue1, readOffset2[i]);
        }
    });
    t1.join();
    t2.join();
}

TEST_P(KVClientOffsetReadRemoteParallelTest, TestReadKeyRemoteAndReadDiffSizeParallel)
{
    LOG(INFO) << "TestReadKeyFromDisk";
    size_t count = 10;
    size_t dataSize = 4 * 1024ul;
    std::vector<std::string> keys;

    std::string data = randomData_.GetRandomString(dataSize);
    datasystem::SetParam param = type_;
    for (size_t i = 0; i < count; ++i) {
        auto key = client_->Set(data, param);
        ASSERT_FALSE(key.empty());
        keys.emplace_back(std::move(key));
    }

    std::vector<ReadParam> params1, params2;
    std::vector<std::string> readOffset1;
    for (size_t i = 0; i < keys.size(); i++) {
        ReadParam param1 = { .key = keys[i],
                             .offset = (uint64_t)randomData_.GetRandomUint64(0L, (uint64_t)(dataSize / 2)),
                             .size = dataSize + 5 };
        readOffset1.emplace_back(data.substr(param1.offset, param1.size));
        params1.emplace_back(param1);
    }

    std::thread t1([this, params1, readOffset1, keys, dataSize] {
        std::vector<Optional<ReadOnlyBuffer>> buffers;
        DS_ASSERT_OK(client1_->Read(params1, buffers));
        for (size_t i = 0; i < buffers.size(); ++i) {
            LOG(INFO) << "-------read-----" << keys[i];
            ASSERT_EQ(buffers[i]->GetSize(), dataSize - params1[i].offset);
        }
    });

    for (size_t i = 0; i < keys.size(); i++) {
        ReadParam param1 = { .key = keys[i], .offset = params1[i].offset, .size = dataSize / 1024 / 2 };
        params2.emplace_back(param1);
    }

    std::thread t2([this, params2, keys] {
        std::vector<Optional<ReadOnlyBuffer>> buffers;
        DS_ASSERT_OK(client1_->Read(params2, buffers));
        for (size_t i = 0; i < buffers.size(); ++i) {
            LOG(INFO) << "-------read-----" << keys[i];
            ASSERT_EQ(buffers[i]->GetSize(), params2[i].size);
        }
    });

    t1.join();
    t2.join();
}

class KVClientWithoutL2Test : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.workerGflagParams = "-shared_memory_size_mb=1024 -log_async=false -log_monitor=true -v=2";
        opts.numEtcd = 1;
        opts.numWorkers = 2;  // worker num is 2
        opts.numOBS = 0;
        opts.enableDistributedMaster = "false";
    }

    void SetUp() override
    {
        ExternalClusterTest::SetUp();
        InitTestKVClient(0, client_);
        InitTestKVClient(1, client1_);
    }

protected:
    std::shared_ptr<KVClient> client_;
    std::shared_ptr<KVClient> client1_;
    uint64_t maxSize_ = 64 * 1024ul * 1024ul;
};

TEST_F(KVClientWithoutL2Test, TestReadKeyRemoteGet)
{
    LOG(INFO) << "TestReadKeyOverSize";
    auto key = "aaaaaaa";
    size_t dataSize = 4 * 1024ul * 1024ul;
    std::string data = randomData_.GetRandomString(dataSize);
    datasystem::SetParam param{ .writeMode = WriteMode::NONE_L2_CACHE_EVICT };
    DS_ASSERT_OK(client1_->Set(key, data, param));
    std::vector<Optional<ReadOnlyBuffer>> buffers;
    DS_ASSERT_OK(client_->Get({ key }, buffers));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "set.objectIsInvalidInmem", "1*call()"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "set.objectIsInComplete", "1*call()"));
    buffers.clear();
    DS_ASSERT_OK(client_->Get({ key }, buffers));
}

class KVClientSpillTest : public KVClientWithoutL2Test {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.enableSpill = true;
        opts.workerGflagParams = "-shared_memory_size_mb=10 -log_async=false -log_monitor=true -v=2";
        opts.numEtcd = 1;
        opts.numWorkers = 2;  // worker num is 2
        opts.numOBS = 0;
        opts.enableDistributedMaster = "false";
    }
};

TEST_F(KVClientSpillTest, TestReadKeyRemoteGet)
{
    InitTestKVClient(0, client_, 5000);       // timeout is 5000 ms
    InitTestKVClient(1, client1_, 5000);      // timeout is 5000 ms
    LOG(INFO) << "TestReadKeyOverSize";
    size_t dataSize = 1 * 1024ul * 1024ul;
    std::string data = randomData_.GetRandomString(dataSize);
    datasystem::SetParam param{ .writeMode = WriteMode::NONE_L2_CACHE };
    std::vector<std::string> keys;
    for (int i = 0; i < 15; i++) {         // obj num is 15
        auto key  = GetStringUuid();
        keys.emplace_back(key);
        DS_ASSERT_OK(client_->Set(key, data, param));
    }
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "LoadSpilledObjectToMemory", "1*return()"));
    std::vector<Optional<ReadOnlyBuffer>> buffers;
    client_->Get({ keys[0] }, buffers);
    DS_ASSERT_OK(client_->Get({ keys[0] }, buffers));
}

datasystem::SetParam ConstructParam(CacheType type)
{
    SetParam param;
    param.writeMode = WriteMode::NONE_L2_CACHE;
    param.cacheType = type;
    return param;
}

INSTANTIATE_TEST_SUITE_P(ReadOffsetParamTest, KVClientOffsetReadOneHostTest,
                         ::testing::Values(ConstructParam(CacheType::MEMORY), ConstructParam(CacheType::DISK)));

INSTANTIATE_TEST_SUITE_P(ReadOffsetParamTest, KVClientOffsetReadRemoteTest,
                         ::testing::Values(ConstructParam(CacheType::MEMORY), ConstructParam(CacheType::DISK)));

INSTANTIATE_TEST_SUITE_P(ReadOffsetParamTest, KVClientOffsetReadRemoteParallelTest,
                         ::testing::Values(ConstructParam(CacheType::MEMORY), ConstructParam(CacheType::DISK)));
}  // namespace st
}  // namespace datasystem