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
 * Description: Test ObjectMeta Storage basic functions.
 */

#include <unistd.h>
#include <cstdint>
#include <list>
#include <memory>
#include <string>
#include <unordered_map>

#include "gtest/gtest.h"

#include "common.h"
#include "datasystem/common/util/uuid_generator.h"
#include "datasystem/master/object_cache/store/object_meta_store.h"
#include "datasystem/master/object_cache/oc_metadata_manager.h"
#include "datasystem/common/kvstore/etcd/etcd_constants.h"
#include "datasystem/common/util/file_util.h"
#include "datasystem/common/util/hash_algorithm.h"
#include "datasystem/common/util/random_data.h"
#include "datasystem/common/log/log.h"
#include "datasystem/utils/status.h"
#include "datasystem/worker/cluster_manager/etcd_cluster_manager.h"
#include "datasystem/worker/hash_ring/hash_ring.h"
#include "datasystem/worker/object_cache/worker_oc_service_impl.h"

DS_DECLARE_string(etcd_address);

using namespace datasystem::master;
namespace datasystem {
namespace st {
void MakeObjectMetas(size_t createNum, std::unordered_map<std::string, ObjectMeta> &metas)
{
    for (size_t i = 0; i < createNum; i++) {
        ObjectMeta objectMeta;
        objectMeta.meta.set_object_key(std::to_string(i));
        objectMeta.meta.set_data_size(RandomData().GetRandomUint64());
        std::string address = "127.0.0.1:1000";
        objectMeta.locations[address] = AckState::ACK;
        metas.emplace(objectMeta.meta.object_key(), objectMeta);
    }
}

constexpr static int ETCD_KEYS_NUM{40};
constexpr static int SPLIT_KEY_NUM{2};

class ObjectMetaStoreEtcdTest : public ExternalClusterTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numEtcd = 1;
        opts.numMasters = 0;
        opts.numWorkers = 0;
    }

    void InitInstance()
    {
        std::string etcdAddress;
        for (size_t i = 0; i < cluster_->GetEtcdNum(); ++i) {
            std::pair<HostPort, HostPort> addrs;
            cluster_->GetEtcdAddrs(i, addrs);

            if (!etcdAddress.empty()) {
                etcdAddress += ",";
            }
            etcdAddress += addrs.first.ToString();
        }
        db_ = std::make_unique<EtcdStore>(etcdAddress);
        if ((db_ != nullptr) && (db_->Init().IsOk())) {
            db_->DropTable(ETCD_RING_PREFIX);
            // We don't check rc here. If table to drop does not exist, it's fine.
            (void)db_->CreateTable(ETCD_RING_PREFIX, ETCD_RING_PREFIX);
            DS_ASSERT_OK(db_->CreateTable(std::string(ETCD_META_TABLE_PREFIX) + ETCD_HASH_SUFFIX,
                                          std::string(ETCD_META_TABLE_PREFIX) + ETCD_HASH_SUFFIX));
            DS_ASSERT_OK(db_->CreateTable(std::string(ETCD_LOCATION_TABLE_PREFIX) + ETCD_HASH_SUFFIX,
                                          std::string(ETCD_LOCATION_TABLE_PREFIX) + ETCD_HASH_SUFFIX));
            DS_ASSERT_OK(db_->CreateTable(std::string(ETCD_ASYNC_WORKER_OP_TABLE_PREFIX) + ETCD_HASH_SUFFIX,
                                          std::string(ETCD_ASYNC_WORKER_OP_TABLE_PREFIX) + ETCD_HASH_SUFFIX));
            DS_ASSERT_OK(db_->CreateTable(std::string(ETCD_GLOBAL_CACHE_TABLE_PREFIX) + ETCD_HASH_SUFFIX,
                                          std::string(ETCD_GLOBAL_CACHE_TABLE_PREFIX) + ETCD_HASH_SUFFIX));

            // Worker table for key with worker id.
            DS_ASSERT_OK(db_->CreateTable(std::string(ETCD_META_TABLE_PREFIX) + ETCD_WORKER_SUFFIX,
                                          std::string(ETCD_META_TABLE_PREFIX) + ETCD_WORKER_SUFFIX));
            DS_ASSERT_OK(db_->CreateTable(std::string(ETCD_LOCATION_TABLE_PREFIX) + ETCD_WORKER_SUFFIX,
                                          std::string(ETCD_LOCATION_TABLE_PREFIX) + ETCD_WORKER_SUFFIX));
            DS_ASSERT_OK(db_->CreateTable(std::string(ETCD_ASYNC_WORKER_OP_TABLE_PREFIX) + ETCD_WORKER_SUFFIX,
                                          std::string(ETCD_ASYNC_WORKER_OP_TABLE_PREFIX) + ETCD_WORKER_SUFFIX));
            DS_ASSERT_OK(db_->CreateTable(std::string(ETCD_GLOBAL_CACHE_TABLE_PREFIX) + ETCD_WORKER_SUFFIX,
                                          std::string(ETCD_GLOBAL_CACHE_TABLE_PREFIX) + ETCD_WORKER_SUFFIX));
        }

        FLAGS_etcd_address = etcdAddress;
        HostPort addr;
        addr.ParseString("127.0.0.1:18481");
        etcdStore_ = std::make_unique<EtcdStore>(FLAGS_etcd_address);
        etcdStore_->Init();
        eviction_ = std::make_shared<object_cache::WorkerOcEvictionManager>(nullptr, addr, addr);
        worker_ = std::make_unique<object_cache::WorkerOCServiceImpl>(addr, addr, nullptr, nullptr, eviction_, nullptr,
                                                                      etcdStore_.get());
        cm_ = std::make_unique<EtcdClusterManager>(addr, addr, etcdStore_.get(), false);
        worker_->SetClusterManager(cm_.get());
        ClusterInfo clusterInfo;
        DS_ASSERT_OK(EtcdClusterManager::ConstructClusterInfoViaEtcd(etcdStore_.get(), clusterInfo));
        DS_ASSERT_OK(cm_->Init(clusterInfo));
        cm_->SetWorkerReady();
        cm_->CheckWaitNodeTableComplete();

        std::string backStorePath = cluster_->GetRootDir() + "/rocksdb";
        rocksStore_ = RocksStore::GetInstance(backStorePath);
        objectMetaStore_ = std::make_shared<ObjectMetaStore>(rocksStore_.get(), etcdStore_.get(), true);
        DS_ASSERT_OK(objectMetaStore_->Init());
    }

    // same as object meta store.
    std::string Hash2Str(uint32_t hash)
    {
        const uint32_t width = 10;
        std::stringstream ss;
        ss << std::setfill('0') << std::setw(width) << hash;
        return ss.str();
    }

    std::pair<std::string, bool> HashFunction(const std::string &key)
    {
        auto res = Split(key, ";");
        uint32_t hash;
        bool specKey;
        // key with worker id.
        if (res.size() == SPLIT_KEY_NUM) {
            hash = MurmurHash3_32(res[1]);
            specKey = true;
        } else {
            hash = MurmurHash3_32(key);
            specKey = false;
        }
        return { Hash2Str(hash), specKey };
    }

    void WaitAsyncTaskDone()
    {
        const int maxCount = 30;
        int count = 0;
        do {
            if (objectMetaStore_->AsyncQueueEmpty()) {
                return;
            }
            sleep(1);
            count++;
        } while (count < maxCount);
        ASSERT_TRUE(false) << "Failed to done async task in 30s";
    }

    void VerifyDataInAsyncTable(const std::string &workerIp,
                                const std::vector<std::pair<std::string, std::string>> &keys, const StatusCode code)
    {
        for (const auto &item : keys) {
            std::string value;
            auto res = Split(item.first, ";");
            std::string key = item.second + "/" + workerIp + "_" + item.first;
            if (res.size() == SPLIT_KEY_NUM) {
                ASSERT_EQ(
                    db_->Get(std::string(ETCD_ASYNC_WORKER_OP_TABLE_PREFIX) + ETCD_WORKER_SUFFIX, key, value).GetCode(),
                    code);
            } else {
                ASSERT_EQ(
                    db_->Get(std::string(ETCD_ASYNC_WORKER_OP_TABLE_PREFIX) + ETCD_HASH_SUFFIX, key, value).GetCode(),
                    code);
            }
        }
    }

    void VerifyDataInGlobalTable(const std::vector<std::pair<std::string, std::string>> &keys, uint64_t version,
                                 const StatusCode code)
    {
        for (const auto &item : keys) {
            std::string value;
            auto res = Split(item.first, ";");
            std::string key = item.second + "/" + item.first + "/" + std::to_string(version);
            if (res.size() == SPLIT_KEY_NUM) {
                ASSERT_EQ(
                    db_->Get(std::string(ETCD_GLOBAL_CACHE_TABLE_PREFIX) + ETCD_WORKER_SUFFIX, key, value).GetCode(),
                    code);
            } else {
                ASSERT_EQ(
                    db_->Get(std::string(ETCD_GLOBAL_CACHE_TABLE_PREFIX) + ETCD_HASH_SUFFIX, key, value).GetCode(),
                    code);
            }
        }
    }

protected:
    std::shared_ptr<RocksStore> rocksStore_;
    std::shared_ptr<ObjectMetaStore> objectMetaStore_;
    std::shared_ptr<EtcdStore> db_;
    std::unique_ptr<EtcdStore> etcdStore_;
    std::unique_ptr<EtcdClusterManager> cm_;
    std::unique_ptr<object_cache::WorkerOCServiceImpl> worker_;
    std::shared_ptr<object_cache::WorkerOcEvictionManager> eviction_;
};

TEST_F(ObjectMetaStoreEtcdTest, DISABLED_TestSyncPutNormalKeyToEtcd)
{
    LOG(INFO) << "Test sync put data to ETCD.";
    InitInstance();
    std::unordered_map<std::string, ObjectMeta> metas;
    std::vector<std::string> etcdKeys;
    size_t metasNum = 50;
    MakeObjectMetas(metasNum, metas);
    for (auto &item : metas) {
        auto res = HashFunction(item.first);
        etcdKeys.emplace_back(res.first + "/" + item.first);
        std::string serializedStr;
        DS_ASSERT_OK(objectMetaStore_->CreateSerializedStringForMeta(item.first, item.second.meta, serializedStr));
        DS_ASSERT_OK(objectMetaStore_->CreateOrUpdateMeta(item.first, serializedStr,
                                                          ObjectMetaStore::WriteType::ROCKS_SYNC_ETCD));
    }

    // verify
    for (const auto &key : etcdKeys) {
        std::string value;
        DS_ASSERT_OK(db_->Get(std::string(ETCD_META_TABLE_PREFIX) + ETCD_HASH_SUFFIX, key, value));
    }
}

TEST_F(ObjectMetaStoreEtcdTest, LEVEL1_TestSyncPutMixKeyToEtcd)
{
    LOG(INFO) << "Test sync put data to ETCD.";
    InitInstance();
    std::vector<std::pair<std::string, std::string>> etcdKeys;
    std::string workerId = cm_->GetLocalWorkerUuid();
    for (int i = 0; i < ETCD_KEYS_NUM; ++i) {
        std::string key = i % 2 == 0 ? "Worrior" + std::to_string(i) : "Worrior" + std::to_string(i) + ";" + workerId;
        auto res = HashFunction(key);
        etcdKeys.emplace_back(key, res.first);
    }

    std::string workerIp = "127.0.0.1:18481";
    for (const auto &item : etcdKeys) {
        DS_ASSERT_OK(objectMetaStore_->AddAsyncWorkerOp(workerIp, item.first, { NotifyWorkerOpType::CACHE_INVALID },
                                                        ObjectMetaStore::WriteType::ROCKS_SYNC_ETCD));
    }
    VerifyDataInAsyncTable(workerIp, etcdKeys, StatusCode::K_OK);
    for (const auto &item : etcdKeys) {
        DS_ASSERT_OK(objectMetaStore_->RemoveAsyncWorkerOp(workerIp, item.first));
    }
    VerifyDataInAsyncTable(workerIp, etcdKeys, StatusCode::K_NOT_FOUND);

    for (const auto &item : etcdKeys) {
        DS_ASSERT_OK(objectMetaStore_->AddAsyncWorkerOp(workerIp, item.first, { NotifyWorkerOpType::CACHE_INVALID },
                                                        ObjectMetaStore::WriteType::ROCKS_SYNC_ETCD));
    }
    VerifyDataInAsyncTable(workerIp, etcdKeys, StatusCode::K_OK);
    DS_ASSERT_OK(objectMetaStore_->RemoveAsyncWorkerOpByWorker(workerIp));
    VerifyDataInAsyncTable(workerIp, etcdKeys, StatusCode::K_NOT_FOUND);
}

TEST_F(ObjectMetaStoreEtcdTest, LEVEL1_TestASyncPutMixKeyToEtcd)
{
    LOG(INFO) << "Test async put data to ETCD.";
    InitInstance();
    std::vector<std::pair<std::string, std::string>> etcdKeys;
    std::string workerId = cm_->GetLocalWorkerUuid();
    for (int i = 0; i < ETCD_KEYS_NUM; ++i) {
        std::string key = i % 2 == 0 ? "Worrior" + std::to_string(i) : "Worrior" + std::to_string(i) + ";" + workerId;
        auto res = HashFunction(key);
        etcdKeys.emplace_back(key, res.first);
    }

    uint64_t version = 0;
    for (const auto &item : etcdKeys) {
        DS_ASSERT_OK(
            objectMetaStore_->AddDeletedObject(item.first, version, ObjectMetaStore::WriteType::ROCKS_ASYNC_ETCD));
    }

    // Wait async task done and verify
    WaitAsyncTaskDone();
    VerifyDataInGlobalTable(etcdKeys, version, StatusCode::K_OK);

    // add again and remove by worker ip and verify.
    for (const auto &item : etcdKeys) {
        DS_ASSERT_OK(objectMetaStore_->RemoveDeletedObject(item.first, version));
    }

    // Wait async task done and verify remove
    WaitAsyncTaskDone();
    VerifyDataInGlobalTable(etcdKeys, version, StatusCode::K_NOT_FOUND);

    std::string workerIp = "127.0.0.1:18481";
    // add again and remove by worker ip and verify.
    for (const auto &item : etcdKeys) {
        DS_ASSERT_OK(objectMetaStore_->AddAsyncWorkerOp(workerIp, item.first, { NotifyWorkerOpType::CACHE_INVALID },
                                                        ObjectMetaStore::WriteType::ROCKS_ASYNC_ETCD));
    }

    // verify add
    WaitAsyncTaskDone();
    VerifyDataInAsyncTable(workerIp, etcdKeys, StatusCode::K_OK);

    DS_ASSERT_OK(objectMetaStore_->RemoveAsyncWorkerOpByWorker(workerIp));

    // verify remove
    WaitAsyncTaskDone();
    VerifyDataInAsyncTable(workerIp, etcdKeys, StatusCode::K_NOT_FOUND);
}

TEST_F(ObjectMetaStoreEtcdTest, LEVEL1_TestRecoveryFromEtcd)
{
    datasystem::inject::Set("EtcdClusterManager.IfNeedTriggerReconciliation.noreconciliation", "return(K_OK)");
    LOG(INFO) << "Test recovery keys ETCD.";

    auto verifyFunc = [](const std::vector<std::pair<std::string, std::string>> &orgKeys,
                         const std::vector<std::pair<std::string, std::string>> &outMetas, bool isWorkerOp,
                         uint64_t version) {
        std::unordered_set<std::string> keys;
        for (const auto &item : orgKeys) {
            if (isWorkerOp) {
                keys.emplace("127.0.0.1:18481_" + item.first);
            } else {
                keys.emplace(item.first + "/" + std::to_string(version));
            }
        }

        for (const auto &item : outMetas) {
            ASSERT_TRUE(keys.find(item.first) != keys.end());
        }
    };

    InitInstance();
    std::vector<std::pair<std::string, std::string>> etcdKeys;
    std::string workerId = cm_->GetLocalWorkerUuid();
    for (int i = 0; i < ETCD_KEYS_NUM; ++i) {
        std::string key = i % 2 == 0 ? "Worrior" + std::to_string(i) : "Worrior" + std::to_string(i) + ";" + workerId;
        auto res = HashFunction(key);
        etcdKeys.emplace_back(key, res.first);
    }
    uint64_t version = 0;
    for (const auto &item : etcdKeys) {
        DS_ASSERT_OK(
            objectMetaStore_->AddDeletedObject(item.first, version, ObjectMetaStore::WriteType::ROCKS_ASYNC_ETCD));
        DS_ASSERT_OK(objectMetaStore_->AddAsyncWorkerOp("127.0.0.1:18481", item.first,
                                                        { NotifyWorkerOpType::CACHE_INVALID },
                                                        ObjectMetaStore::WriteType::ROCKS_SYNC_ETCD));
    }

    // wait for async task done and verify.
    WaitAsyncTaskDone();
    std::vector<std::pair<std::string, std::string>> outMetas;
    DS_ASSERT_OK(objectMetaStore_->GetFromEtcd(ETCD_ASYNC_WORKER_OP_TABLE_PREFIX, ASYNC_WORKER_OP_TABLE, { workerId },
                                               { { 0, UINT32_MAX } }, outMetas));
    ASSERT_EQ(outMetas.size(), 40ul);
    verifyFunc(etcdKeys, outMetas, true, version);

    outMetas.clear();
    DS_ASSERT_OK(objectMetaStore_->GetFromEtcd(ETCD_GLOBAL_CACHE_TABLE_PREFIX, GLOBAL_CACHE_TABLE, { workerId },
                                               { { 0, UINT32_MAX } }, outMetas));
    ASSERT_EQ(outMetas.size(), 40ul);
    verifyFunc(etcdKeys, outMetas, false, version);
}

TEST_F(ObjectMetaStoreEtcdTest, LEVEL1_TestEtcdQueueWithNoLimit)
{
    LOG(INFO) << "Test etcd queue is full.";
    InitInstance();
    DS_ASSERT_OK(datasystem::inject::Set("master.before_sub_async_send_etcd_req", "20000*return(K_OK)"));
    std::unordered_map<std::string, ObjectMeta> metas;
    size_t maxNum = 2048 * 8 + 1;
    MakeObjectMetas(maxNum, metas);
    for (size_t i = 0; i < maxNum; i++) {
        auto &item = metas[std::to_string(i)];
        std::string serializedStr;
        DS_ASSERT_OK(objectMetaStore_->CreateSerializedStringForMeta(std::to_string(i), item.meta, serializedStr));
        DS_ASSERT_OK(objectMetaStore_->CreateOrUpdateMeta(std::to_string(i), serializedStr,
                                                          ObjectMetaStore::WriteType::ROCKS_ASYNC_ETCD));
    }
}
}  // namespace st
}  // namespace datasystem
