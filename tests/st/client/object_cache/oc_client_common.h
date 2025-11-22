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
 * Description: common class of client test
 */
#ifndef DATASYSTEM_TEST_ST_CLIENT_OBJECT_CACHE_OC_CLIENT_COMMON_H
#define DATASYSTEM_TEST_ST_CLIENT_OBJECT_CACHE_OC_CLIENT_COMMON_H

#include <climits>
#include <cstddef>
#include <cstdint>

#include "common.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/util/random_data.h"
#include "datasystem/common/util/thread_pool.h"
#include "datasystem/datasystem.h"
#include "datasystem/hetero_client.h"
#include "datasystem/object_client.h"
#include "datasystem/protos/hash_ring.pb.h"
#include "datasystem/protos/master_object.pb.h"
#include "datasystem/kv_client.h"
#include "datasystem/utils/connection.h"
#include "datasystem/common/kvstore/etcd/etcd_store.h"
#include "datasystem/common/util/uuid_generator.h"

DS_DECLARE_string(etcd_address);

namespace datasystem {
namespace st {
class OCClientCommon : public ExternalClusterTest {
public:
    void InitTestClient(uint32_t workerIndex, std::shared_ptr<ObjectClient> &client, int32_t timeoutMs = 60000)
    {
        ConnectOptions connectOptions;
        InitConnectOpt(workerIndex, connectOptions, timeoutMs);
        client = std::make_shared<ObjectClient>(connectOptions);
        DS_ASSERT_OK(client->Init());
    }

    template <typename F>
    void InitTestClient(uint32_t workerIndex, std::shared_ptr<ObjectClient> &client, F &&f)
    {
        ConnectOptions connectOptions;
        InitConnectOpt(workerIndex, connectOptions);
        connectOptions.accessKey = "";
        connectOptions.secretKey = "";
        f(connectOptions);
        client = std::make_shared<ObjectClient>(connectOptions);
        DS_ASSERT_OK(client->Init());
    }

    void InitConnectOpt(uint32_t workerIndex, ConnectOptions &connectOptions, int32_t timeoutMs = 60000,
                        bool enableCrossNode = false, bool enableExclusive = false)
    {
        HostPort workerAddress;
        ASSERT_TRUE(workerIndex < cluster_->GetWorkerNum());
        DS_ASSERT_OK(cluster_->GetWorkerAddr(workerIndex, workerAddress));
        connectOptions = { .host = workerAddress.Host(), .port = workerAddress.Port(), .connectTimeoutMs = timeoutMs };
        connectOptions.accessKey = "QTWAOYTTINDUT2QVKYUC";
        connectOptions.secretKey = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
        connectOptions.enableCrossNodeConnection = enableCrossNode;
        connectOptions.enableExclusiveConnection = enableExclusive;
    }

    void InitTestKVClient(uint32_t workerIndex, std::shared_ptr<KVClient> &client, int32_t timeoutMs = 60000,
                          bool enableCrossNode = false, bool enableExclusive = false)
    {
        ConnectOptions connectOptions;
        InitConnectOpt(workerIndex, connectOptions, timeoutMs, enableCrossNode, enableExclusive);
        client = std::make_shared<KVClient>(connectOptions);
        DS_ASSERT_OK(client->Init());
    }

    template <typename F>
    void InitTestKVClient(uint32_t workerIndex, std::shared_ptr<KVClient> &client, F &&f)
    {
        ConnectOptions connectOptions;
        InitConnectOpt(workerIndex, connectOptions);
        connectOptions.accessKey = "";
        connectOptions.secretKey = "";
        f(connectOptions);
        client = std::make_shared<KVClient>(connectOptions);
        DS_ASSERT_OK(client->Init());
    }

    void InitTestDsClient(uint32_t workerIndex, std::shared_ptr<DsClient> &client, int32_t timeoutMs = 60000)
    {
        ConnectOptions connectOptions;
        InitConnectOpt(workerIndex, connectOptions, timeoutMs);
        client = std::make_shared<DsClient>(connectOptions);
        DS_ASSERT_OK(client->Init());
    }

    void InitTestHeteroClient(uint32_t workerIndex, std::shared_ptr<HeteroClient> &client, int32_t timeoutMs = 60000)
    {
        ConnectOptions connectOptions;
        InitConnectOpt(workerIndex, connectOptions, timeoutMs);
        client = std::make_shared<HeteroClient>(connectOptions);
        DS_ASSERT_OK(client->Init());
    }

    template <typename F>
    void InitTestHeteroClient(uint32_t workerIndex, std::shared_ptr<HeteroClient> &client, F &&f)
    {
        ConnectOptions connectOptions;
        InitConnectOpt(workerIndex, connectOptions);
        connectOptions.accessKey = "";
        connectOptions.secretKey = "";
        f(connectOptions);
        client = std::make_shared<HeteroClient>(connectOptions);
        DS_ASSERT_OK(client->Init());
    }

    std::string ObjectKeyWithOwner(int workerIndex, std::unordered_map<HostPort, std::string> &uuidMap)
    {
        HostPort workerAddress;
        cluster_->GetWorkerAddr(workerIndex, workerAddress);
        std::string workerUuid = uuidMap[workerAddress];
        return GetStringUuid() + ";" + workerUuid;
    }

    std::string GetWorkerUuid(int workerIndex, std::unordered_map<HostPort, std::string> &uuidMap)
    {
        HostPort workerAddress;
        cluster_->GetWorkerAddr(workerIndex, workerAddress);
        return uuidMap[workerAddress];
    }

    std::string ObjectKey()
    {
        return GetStringUuid();
    }

    void CreateAndSealObject(std::shared_ptr<ObjectClient> client, const std::string &id, std::string &data,
                             const std::unordered_set<std::string> &nestedKeys = {})
    {
        std::shared_ptr<Buffer> buffer;
        CreateParam param;
        DS_ASSERT_OK(client->Create(id, data.size(), param, buffer));
        DS_ASSERT_OK(buffer->WLatch());
        DS_ASSERT_OK(buffer->MemoryCopy((void *)data.data(), data.size()));
        DS_ASSERT_OK(buffer->Seal(nestedKeys));
        DS_ASSERT_OK(buffer->UnWLatch());
    }

    void CreateAndSealObject(std::shared_ptr<ObjectClient> client, const std::string &id, std::vector<uint8_t> &data,
                             const std::unordered_set<std::string> &nestedKeys = {})
    {
        std::shared_ptr<Buffer> buffer;
        CreateParam param;
        DS_ASSERT_OK(client->Create(id, data.size(), param, buffer));
        DS_ASSERT_OK(buffer->WLatch());
        DS_ASSERT_OK(buffer->MemoryCopy(data.data(), data.size()));
        DS_ASSERT_OK(buffer->Seal(nestedKeys));
        DS_ASSERT_OK(buffer->UnWLatch());
    }

    void CreateAndPublishObject(std::shared_ptr<ObjectClient> client, const std::string &id, std::vector<uint8_t> &data)
    {
        std::shared_ptr<Buffer> buffer;
        CreateParam param;
        DS_ASSERT_OK(client->Create(id, data.size(), param, buffer));
        DS_ASSERT_OK(buffer->WLatch());
        DS_ASSERT_OK(buffer->MemoryCopy(data.data(), data.size()));
        DS_ASSERT_OK(buffer->Publish());
        DS_ASSERT_OK(buffer->UnWLatch());
    }

    void CreateObject(std::shared_ptr<ObjectClient> client, const std::string &id, std::vector<uint8_t> &data)
    {
        std::shared_ptr<Buffer> buffer;
        CreateParam param;
        DS_ASSERT_OK(client->Create(id, data.size(), param, buffer));
        DS_ASSERT_OK(buffer->MemoryCopy(data.data(), data.size()));
    }

    static void AssertTwoStrs(std::string &bufStr, const std::string &expected)
    {
        const int threshold = 100;
        size_t size = expected.length();
        // gcc 7.3 _Hash_bytes crashes when len is 2^31 or greater, so we cannot use std::hash.
        if (size > INT_MAX) {
            ASSERT_EQ(memcmp(bufStr.data(), expected.data(), size), 0);
        } else if (size > threshold) {
            std::hash<std::string> hash;
            ASSERT_EQ(bufStr.substr(0, threshold), expected.substr(0, threshold));
            ASSERT_EQ(hash(bufStr), hash(expected));
        } else {
            ASSERT_EQ(bufStr, expected);
        }
    }

    static void ArrToStr(void *data, size_t sz, std::string &str)
    {
        str.assign(reinterpret_cast<const char *>(data), sz);
    }

    static void AssertBufferEqual(Buffer &buffer, const std::string &expected)
    {
        ASSERT_EQ(static_cast<size_t>(buffer.GetSize()), expected.length()) << "Mismatching buffer size";
        std::string bufStr;
        ArrToStr(buffer.MutableData(), buffer.GetSize(), bufStr);
        AssertTwoStrs(bufStr, expected);
    }

    static void AssertBufferEqual(const std::shared_ptr<Buffer> &buffer, const std::string &expected)
    {
        ASSERT_EQ(static_cast<size_t>(buffer->GetSize()), expected.length()) << "Mismatching buffer size";
        std::string bufStr;
        ArrToStr(buffer->MutableData(), buffer->GetSize(), bufStr);
        AssertTwoStrs(bufStr, expected);
    }

    static void AssertBufferEqual(Buffer &buffer, uint8_t *inputData, size_t len)
    {
        ASSERT_EQ(static_cast<size_t>(buffer.GetSize()), len) << "Mismatching buffer size";
        std::string bufStr;
        std::string expected;
        ArrToStr(buffer.MutableData(), buffer.GetSize(), bufStr);
        ArrToStr(inputData, len, expected);
        AssertTwoStrs(bufStr, expected);
    }

    void AssertStatusCode(const Status &status, const StatusCode &code)
    {
        ASSERT_EQ(status.GetCode(), code);
    }

    void AssertStringOfStatus(const Status &status, const std::string &expectedString)
    {
        ASSERT_NE(status.ToString().find(expectedString), std::string::npos);
    }

    std::string GenRandomString(uint64_t size = 10)
    {
        return RandomData().GetRandomString(size);
    }

    std::string GenPartRandomString(uint64_t size = 10, uint64_t randomNum = 10)
    {
        return RandomData().GetPartRandomString(size, randomNum);
    }

    std::vector<uint64_t> GenRandomVector(size_t vecSize = 10)
    {
        std::vector<uint64_t> data;
        for (size_t i = 0; i < vecSize; ++i) {
            data.push_back(RandomData().GetRandomUint64());
        }
        return data;
    }

    static bool ExistsNone(const std::vector<Optional<Buffer>> &buffers)
    {
        return std::any_of(buffers.cbegin(), buffers.cend(), [](const Optional<Buffer> &buffer) { return !buffer; });
    }

    static bool NotExistsNone(const std::vector<Optional<Buffer>> &buffers)
    {
        return !ExistsNone(buffers);
    }

    template <typename T>
    static bool ExistsNone(const std::vector<T> &buffers)
    {
        return std::any_of(buffers.cbegin(), buffers.cend(), [](const T &sv) { return sv.empty(); });
    }

    template <typename T>
    static bool NotExistsNone(const std::vector<T> &buffers)
    {
        return !ExistsNone(buffers);
    }

    void GetWorkerUuids(EtcdStore *db, std::unordered_map<HostPort, std::string> &uuidMap)
    {
        std::string value;
        DS_ASSERT_OK(db->Get(ETCD_RING_PREFIX, "", value));
        HashRingPb ring;
        ring.ParseFromString(value);
        for (auto worker : ring.workers()) {
            HostPort workerAddr;
            DS_ASSERT_OK(workerAddr.ParseString(worker.first));
            uuidMap.emplace(std::move(workerAddr), worker.second.worker_uuid());
        }
    }

    std::unique_ptr<EtcdStore> InitTestEtcdInstance(std::string azName = "")
    {
        std::string etcdAddress = cluster_->GetEtcdAddrs();
        FLAGS_etcd_address = etcdAddress;
        LOG(INFO) << "The etcd address is:" << FLAGS_etcd_address << std::endl;
        auto db = std::make_unique<EtcdStore>(etcdAddress);
        if ((db != nullptr) && (db->Init().IsOk())) {
            auto prefix = azName.empty() ? "" : "/" + azName;
            // We don't check rc here. If table to drop does not exist, it's fine.
            (void)db->CreateTable(ETCD_RING_PREFIX, prefix + ETCD_RING_PREFIX);
            (void)db->CreateTable(ETCD_CLUSTER_TABLE, prefix + "/" + ETCD_CLUSTER_TABLE);
        }
        return db;
    }
};

#define ASSERT_BUF_EQ(buffer_, expected_)                                                                       \
    do {                                                                                                        \
        ASSERT_EQ(static_cast<size_t>((buffer_).GetSize()), (expected_).length()) << "Mismatching buffer size"; \
        std::string bufStr;                                                                                     \
        OCClientCommon::ArrToStr((buffer_).MutableData(), (buffer_).GetSize(), bufStr);                         \
        const int threshold = 100;                                                                              \
        if ((expected_).length() > threshold) {                                                                 \
            std::hash<std::string> hash;                                                                        \
            ASSERT_EQ(bufStr.substr(0, threshold), (expected_).substr(0, threshold));                           \
            ASSERT_EQ(hash(bufStr), hash(expected_));                                                           \
        } else {                                                                                                \
            ASSERT_EQ(bufStr, (expected_));                                                                     \
        }                                                                                                       \
    } while (0)

}  // namespace st
}  // namespace datasystem
#endif  // DATASYSTEM_TEST_ST_CLIENT_OBJECT_CACHE_OC_CLIENT_COMMON_H
