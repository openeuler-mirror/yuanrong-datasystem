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
 * Description: Test migrate metadata manager class.
 */
#include "datasystem/master/object_cache/oc_migrate_metadata_manager.h"

#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <rocksdb/env.h>

#include "common.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/kvstore/rocksdb/rocks_store.h"
#include "datasystem/common/immutable_string/immutable_string_pool.h"
#include "datasystem/common/util/file_util.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/uri.h"
#include "datasystem/master/object_cache/oc_metadata_manager.h"
#include "datasystem/master/object_cache/oc_nested_manager.h"
#include "datasystem/protos/generic_service.pb.h"
#include "datasystem/protos/master_object.pb.h"
#include "datasystem/protos/object_posix.pb.h"
#include "datasystem/kv_client.h"
#include "datasystem/utils/status.h"
#include "datasystem/worker/cluster_manager/etcd_cluster_manager.h"
#include "datasystem/worker/hash_ring/hash_ring_allocator.h"
#include "datasystem/worker/object_cache/worker_master_oc_api.h"
#include "gtest/gtest.h"

DS_DECLARE_string(rocksdb_store_dir);
DS_DECLARE_string(etcd_address);
DS_DECLARE_string(master_address);

using namespace datasystem::master;
using namespace datasystem::worker;
namespace datasystem {
namespace st {
class OCMigrateMetadataManagerTest : public ExternalClusterTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numEtcd = 1;
        opts.numMasters = 1;
        opts.numWorkers = 1;
    }

    std::string GetWorkerAddr(int workerIndex = 0)
    {
        HostPort workerAddress;
        cluster_->GetWorkerAddr(workerIndex, workerAddress);
        return workerAddress.ToString();
    }

    Status InitInstanceBase()
    {
        // immutable string pool should be initiailzed first
        ImmutableStringPool::Instance().Init();
        intern::StringPool::InitAll();
        // The static members are destructed in the reverse order of their construction,
        // Below call guarantees that the destructor of Env is behind other Rocksdb singletons.
        (void)rocksdb::Env::Default();
        FLAGS_rocksdb_store_dir = GetTestCaseDataDir() + "/rocksdb";
        LOG_IF_ERROR(PreInitRocksDB(), "Failed to initialize the rocksdb database in advance.");
        hostPort_.ParseString("127.0.0.1:" + std::to_string(GetFreePort()));
        akSkManager_ = std::make_shared<AkSkManager>();
        akSkManager_->SetClientAkSk(accessKey_, secretKey_);
        TimerQueue::GetInstance()->Initialize();
        std::pair<HostPort, HostPort> addrs;
        RETURN_IF_NOT_OK(cluster_->GetEtcdAddrs(0, addrs));
        FLAGS_etcd_address = addrs.first.ToString();
        etcdStore_ = std::make_unique<EtcdStore>(FLAGS_etcd_address);
        FLAGS_master_address = hostPort_.ToString();
        RETURN_IF_NOT_OK(etcdStore_->Init());
        replicaManager_ = std::make_unique<ReplicaManager>();
        etcdCM_ = std::make_unique<EtcdClusterManager>(hostPort_, hostPort_, etcdStore_.get());
        ClusterInfo clusterInfo;
        RETURN_IF_NOT_OK(EtcdClusterManager::ConstructClusterInfoViaEtcd(etcdStore_.get(), clusterInfo));
        RETURN_IF_NOT_OK(etcdCM_->Init(clusterInfo));
        workerUuid_ = etcdCM_->GetLocalWorkerUuid();
        ReplicaManagerParam param;
        param.dbRootPath = FLAGS_rocksdb_store_dir;
        param.currWorkerId = workerUuid_;
        param.akSkManager = akSkManager_;
        param.etcdStore = etcdStore_.get();
        param.persistenceApi = nullptr;
        param.etcdCM = etcdCM_.get();
        param.masterAddress = hostPort_;
        param.masterWorkerService = nullptr;
        param.workerWorkerService = nullptr;
        param.isOcEnabled = true;
        param.isScEnabled = false;
        RETURN_IF_NOT_OK(replicaManager_->Init(param));
        RETURN_IF_NOT_OK(replicaManager_->AddOrSwitchTo(param.currWorkerId, ReplicaType::Primary));
        etcdCM_->SetWorkerReady();
        return Status::OK();
    }

    Status InitInstance()
    {
        InitInstanceBase();
        RETURN_IF_NOT_OK(
            OCMigrateMetadataManager::Instance().Init(hostPort_, akSkManager_, etcdCM_.get(), replicaManager_.get()));
        RETURN_IF_NOT_OK(cluster_->GetWorkerAddr(0, workerAddress_));
        int stubCacheNum = 100;
        RpcStubCacheMgr::Instance().Init(stubCacheNum);
        workerMasterApi_ = std::make_unique<WorkerRemoteMasterOCApi>(workerAddress_, hostPort_, akSkManager_);
        RETURN_IF_NOT_OK(workerMasterApi_->Init());
        RETURN_IF_NOT_OK(datasystem::inject::Set("IncreaseNestedRefCnt.local.addr", "return()"));
        RETURN_IF_NOT_OK(datasystem::inject::Set("ocMetaManager.noNeedGetFromLocal", "return()"));
        return Status::OK();
    }

    void TearDown() override
    {
        OCMigrateMetadataManager::Instance().Shutdown();
        etcdCM_.reset();
        etcdStore_.reset();
        ExternalClusterTest::TearDown();
    }

    Status CreateMetadata(int num, bool isNested = false)
    {
        for (int i = 0; i < num; ++i) {
            CreateMetaReqPb request;
            CreateMetaRspPb response;
            datasystem::ObjectMetaPb *metadata = request.mutable_meta();
            std::string objectKey = "MigrateMetadataTestId" + std::to_string(i);
            objectKeys_.emplace_back(objectKey);
            metadata->set_object_key(objectKey);
            metadata->set_data_size(dataSize_);
            metadata->set_life_state(static_cast<uint32_t>(lifeState_));
            metadata->set_ttl_second(ttlSecond_);
            ConfigPb *configPb = metadata->mutable_config();
            configPb->set_write_mode(static_cast<uint32_t>(writeMode_));
            configPb->set_data_format(static_cast<uint32_t>(dataFormat_));
            configPb->set_consistency_type(static_cast<uint32_t>(consistencyType_));
            request.set_address(hostPort_.ToString());
            if (isNested) {
                std::string nestedKey = "nestedKey" + std::to_string(i);
                request.add_nested_keys(nestedKey);
            }
            LOG(INFO) << request.nested_keys().size();
            std::shared_ptr<master::OCMetadataManager> ocMetadataManager;
            RETURN_IF_NOT_OK(replicaManager_->GetOcMetadataManager(workerUuid_, ocMetadataManager));
            RETURN_IF_NOT_OK_PRINT_ERROR_MSG(ocMetadataManager->CreateMeta(request, response), "create failed");
        }
        return Status::OK();
    }

    Status GIncreaseRef(int num)
    {
        master::GIncreaseReqPb request;
        master::GIncreaseRspPb response;
        for (int i = 0; i < num; ++i) {
            std::string objectKey = "MigrateMetadataTestId" + std::to_string(i);
            objectKeys_.emplace_back(objectKey);
        }
        request.set_address(hostPort_.ToString());
        *request.mutable_object_keys() = { objectKeys_.begin(), objectKeys_.end() };
        std::shared_ptr<master::OCMetadataManager> ocMetadataManager;
        RETURN_IF_NOT_OK(replicaManager_->GetOcMetadataManager(workerUuid_, ocMetadataManager));
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(ocMetadataManager->GIncreaseRef(request, response), "create failed");
        CHECK_FAIL_RETURN_STATUS(response.failed_object_keys().empty(), K_RUNTIME_ERROR, "increase failed");
        return Status::OK();
    }

    void CheckMigrationMetadata(const std::string &objectKey, bool isMigrationSuccess)
    {
        // Check whether the old node has no data.
        QueryMetaReqPb queryReq;
        QueryMetaRspPb queryRsp;
        queryReq.add_ids(objectKey);
        queryReq.set_address(GetWorkerAddr());
        std::vector<RpcMessage> payloads;
        std::shared_ptr<master::OCMetadataManager> ocMetadataManager;
        DS_ASSERT_OK(replicaManager_->GetOcMetadataManager(workerUuid_, ocMetadataManager));
        DS_ASSERT_OK(ocMetadataManager->QueryMeta(queryReq, queryRsp, payloads));
        if (isMigrationSuccess) {
            ASSERT_EQ(queryRsp.query_metas().size(), 0);
        } else {
            ASSERT_EQ(queryRsp.query_metas().size(), 1);
        }

        // Check whether data exists on the new node.
        payloads.clear();
        DS_ASSERT_OK(workerMasterApi_->QueryMeta(queryReq, 0, queryRsp, payloads));
        if (isMigrationSuccess) {
            ASSERT_EQ(queryRsp.query_metas().size(), 1);
            ASSERT_EQ(queryRsp.query_metas()[0].meta().object_key(), objectKey);
        } else {
            ASSERT_EQ(queryRsp.query_metas().size(), 0);
        }
    }

    /**
     * @brief After the rocksdb version is upgraded, it need to initialize the rocksdb database in advance to avoid
     * abort.
     */
    Status PreInitRocksDB()
    {
        RETURN_IF_NOT_OK(
            Uri::NormalizePathWithUserHomeDir(FLAGS_rocksdb_store_dir, "~/.datasystem/rocksdb", "/master"));
        std::string preInitRocksDir = FLAGS_rocksdb_store_dir + "/pre-start";
        RETURN_IF_NOT_OK(RemoveAll(preInitRocksDir));
        if (!FileExist(preInitRocksDir)) {
            // The permission of ~/.datasystem/rocksdb/object_metadata.
            const int permission = 0700;
            RETURN_IF_NOT_OK(CreateDir(preInitRocksDir, true, permission));
        }
        std::shared_ptr<RocksStore> workerRocks = RocksStore::GetInstance(preInitRocksDir);
        CHECK_FAIL_RETURN_STATUS(workerRocks != nullptr, StatusCode::K_RUNTIME_ERROR, "Rocksdb has been initialized.");
        workerRocks->Close();
        return Status::OK();
    }

    HostPort hostPort_;
    std::shared_ptr<AkSkManager> akSkManager_;
    std::unique_ptr<WorkerRemoteMasterOCApi> workerMasterApi_;
    HostPort workerAddress_;
    std::vector<std::string> objectKeys_;
    uint32_t dataSize_ = 10;
    ObjectLifeState lifeState_ = ObjectLifeState::OBJECT_PUBLISHED;
    uint32_t ttlSecond_ = 50;
    WriteMode writeMode_ = WriteMode::WRITE_BACK_L2_CACHE;
    DataFormat dataFormat_ = DataFormat::BINARY;
    ConsistencyType consistencyType_ = ConsistencyType::CAUSAL;
    std::string accessKey_ = "QTWAOYTTINDUT2QVKYUC";
    std::string secretKey_ = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
    std::unique_ptr<EtcdStore> etcdStore_;
    std::unique_ptr<EtcdClusterManager> etcdCM_;
    std::unique_ptr<ReplicaManager> replicaManager_;
    std::string workerUuid_;
    std::vector<std::string> nestedKeys_;
};

TEST_F(OCMigrateMetadataManagerTest, TestMigrateMetadataSuccess)
{
    Status rc = InitInstance();
    DS_ASSERT_OK(rc);
    DS_ASSERT_OK(CreateMetadata(15));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "hashRing.noNeedToCheckForTest", "100*return"));
    std::shared_ptr<master::OCMetadataManager> ocMetadataManager;
    DS_ASSERT_OK(replicaManager_->GetOcMetadataManager(workerUuid_, ocMetadataManager));
    OCMigrateMetadataManager::MigrateMetaInfo info = { .objectKeys = objectKeys_,
                                                       .destAddr = workerAddress_.ToString() };
    DS_ASSERT_OK(OCMigrateMetadataManager::Instance().StartMigrateMetadataForScaleout(ocMetadataManager, info));
    std::vector<std::string> failedObjectKeys;
    DS_ASSERT_OK(OCMigrateMetadataManager::Instance().GetMigrateMetadataResult(workerUuid_, workerAddress_.ToString(),
                                                                               failedObjectKeys));
    for (auto id : objectKeys_) {
        CheckMigrationMetadata(id, true);
    }
}

TEST_F(OCMigrateMetadataManagerTest, TestMigrateMetadataRetryFailedExit)
{
    Status rc = InitInstance();
    DS_ASSERT_OK(rc);
    DS_ASSERT_OK(CreateMetadata(15));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "hashRing.noNeedToCheckForTest", "100*return"));
    std::shared_ptr<master::OCMetadataManager> ocMetadataManager;
    DS_ASSERT_OK(replicaManager_->GetOcMetadataManager(workerUuid_, ocMetadataManager));
    OCMigrateMetadataManager::MigrateMetaInfo info = { .objectKeys = objectKeys_, .destAddr = "111.111.111.111:9191" };
    DS_ASSERT_NOT_OK(OCMigrateMetadataManager::Instance().MigrateMetaDataWithRetry(ocMetadataManager, info, false));
}

TEST_F(OCMigrateMetadataManagerTest, TestMigrateMetadataOneFailed)
{
    DS_ASSERT_OK(InitInstance());
    DS_ASSERT_OK(CreateMetadata(15));  // meta num is 15
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "hashRing.noNeedToCheckForTest", "100*return"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "master.save_minration_data_failed", "1*return"));
    std::shared_ptr<master::OCMetadataManager> ocMetadataManager;
    DS_ASSERT_OK(replicaManager_->GetOcMetadataManager(workerUuid_, ocMetadataManager));
    OCMigrateMetadataManager::MigrateMetaInfo info = { .objectKeys = objectKeys_,
                                                       .destAddr = workerAddress_.ToString() };
    DS_ASSERT_OK(OCMigrateMetadataManager::Instance().StartMigrateMetadataForScaleout(ocMetadataManager, info));
    std::vector<std::string> failedObjectKeys;
    DS_ASSERT_OK(OCMigrateMetadataManager::Instance().GetMigrateMetadataResult(workerUuid_, workerAddress_.ToString(),
                                                                               failedObjectKeys));

    ASSERT_EQ(static_cast<size_t>(1), failedObjectKeys.size());
    int num = 0;
    for (auto id : objectKeys_) {
        if (num == 0) {
            CheckMigrationMetadata(id, false);
        } else {
            CheckMigrationMetadata(id, true);
        }
        ++num;
    }
    failedObjectKeys.clear();
    OCMigrateMetadataManager::MigrateMetaInfo info1 = { .objectKeys = { objectKeys_[0] },
                                                        .destAddr = workerAddress_.ToString() };
    DS_ASSERT_OK(OCMigrateMetadataManager::Instance().StartMigrateMetadataForScaleout(ocMetadataManager, info1));
    DS_ASSERT_OK(OCMigrateMetadataManager::Instance().GetMigrateMetadataResult(workerUuid_, workerAddress_.ToString(),
                                                                               failedObjectKeys));
    ASSERT_EQ(static_cast<size_t>(0), failedObjectKeys.size());
    CheckMigrationMetadata(objectKeys_[0], true);
}

TEST_F(OCMigrateMetadataManagerTest, TestNestRefMigrateSuccess)
{
    FLAGS_v = 2;  // vlog is 2
    Status rc = InitInstance();
    DS_ASSERT_OK(rc);
    DS_ASSERT_OK(CreateMetadata(15, true));  // obj num is 15;
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "hashRing.noNeedToCheckForTest", "100*return"));
    std::shared_ptr<master::OCMetadataManager> ocMetadataManager;
    DS_ASSERT_OK(replicaManager_->GetOcMetadataManager(workerUuid_, ocMetadataManager));
    HashRange range;
    range.emplace_back(0, UINT32_MAX);
    OCMigrateMetadataManager::MigrateMetaInfo info = { .objectKeys = objectKeys_,
                                                       .destAddr = workerAddress_.ToString(),
                                                       .ranges = range };
    DS_ASSERT_OK(OCMigrateMetadataManager::Instance().StartMigrateMetadataForScaleout(ocMetadataManager, info));
    std::vector<std::string> failedObjectKeys;
    DS_ASSERT_OK(OCMigrateMetadataManager::Instance().GetMigrateMetadataResult(workerUuid_, workerAddress_.ToString(),
                                                                               failedObjectKeys));
    for (const auto &id : nestedKeys_) {
        LOG(INFO) << nestedKeys_.size();
        ASSERT_TRUE(ocMetadataManager->GetNestedRefManager()->CheckIsNoneNestedRefById(id));
    }

    for (auto id : objectKeys_) {
        CheckMigrationMetadata(id, true);
        std::vector<std::string> objKeys;
        ocMetadataManager->GetNestedRefManager()->GetNestedRelationship(id, objKeys);
        ASSERT_TRUE(objKeys.empty());
    }
}

TEST_F(OCMigrateMetadataManagerTest, TestGlobalRefMigrateSuccess)
{
    FLAGS_v = 2;  // vlog is 2
    Status rc = InitInstance();
    DS_ASSERT_OK(rc);
    DS_ASSERT_OK(GIncreaseRef(15));  // obj num is 15;
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "hashRing.noNeedToCheckForTest", "100*return"));
    std::shared_ptr<master::OCMetadataManager> ocMetadataManager;
    DS_ASSERT_OK(replicaManager_->GetOcMetadataManager(workerUuid_, ocMetadataManager));
    auto globalRef = ocMetadataManager->GetGlobalRefTable();
    ASSERT_EQ(globalRef->GetObjectRefCount(), 15);  // object num is 15
    HashRange range;
    range.emplace_back(0, UINT32_MAX);
    OCMigrateMetadataManager::MigrateMetaInfo info = { .objectKeys = objectKeys_,
                                                       .destAddr = workerAddress_.ToString(),
                                                       .ranges = range };
    DS_ASSERT_OK(OCMigrateMetadataManager::Instance().StartMigrateMetadataForScaleout(ocMetadataManager, info));
    std::vector<std::string> failedObjectKeys;
    DS_ASSERT_OK(OCMigrateMetadataManager::Instance().GetMigrateMetadataResult(workerUuid_, workerAddress_.ToString(),
                                                                               failedObjectKeys));
    for (const auto &id : objectKeys_) {
        ASSERT_EQ(globalRef->GetRefWorkerCount(id), 0);
    }
    ASSERT_EQ(globalRef->GetObjectRefCount(), 0);
}

class OCMetaManagerCreateMultiMetaTest : public OCMigrateMetadataManagerTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numEtcd = 1;
        opts.numWorkers = 0;
    }

    Status CreateMultiMetadata(int num)
    {
        CreateMultiMetaReqPb request;
        CreateMultiMetaRspPb response;
        for (int i = 0; i < num; ++i) {
            datasystem::ObjectBaseInfoPb *metadata = request.add_metas();
            std::string objectKey = "MigrateMetadataTestId" + std::to_string(i);
            objectKeys_.emplace_back(objectKey);
            metadata->set_object_key(objectKey);
            metadata->set_data_size(dataSize_);
            request.set_life_state(static_cast<uint32_t>(lifeState_));
            request.set_ttl_second(ttlSecond_);
            request.set_existence(ExistenceOptPb::NX);
            ConfigPb *configPb = request.mutable_config();
            configPb->set_write_mode(static_cast<uint32_t>(writeMode_));
            configPb->set_data_format(static_cast<uint32_t>(dataFormat_));
            configPb->set_consistency_type(static_cast<uint32_t>(consistencyType_));
        }
        request.set_istx(true);
        request.set_address(hostPort_.ToString());
        std::shared_ptr<master::OCMetadataManager> ocMetadataManager;
        RETURN_IF_NOT_OK(replicaManager_->GetOcMetadataManager(workerUuid_, ocMetadataManager));
        RETURN_IF_NOT_OK(ocMetadataManager->CreateMultiMeta(request, response));
        return Status::OK();
    }

    Status CheckMetadata(const std::string &objectKey)
    {
        // Check whether the old node has no data.
        QueryMetaReqPb queryReq;
        QueryMetaRspPb queryRsp;
        queryReq.add_ids(objectKey);
        queryReq.set_address(hostPort_.ToString());
        queryReq.set_sub_timeout(0);
        std::vector<RpcMessage> payloads;
        std::shared_ptr<master::OCMetadataManager> ocMetadataManager;
        RETURN_IF_NOT_OK(replicaManager_->GetOcMetadataManager(workerUuid_, ocMetadataManager));
        RETURN_IF_NOT_OK(ocMetadataManager->QueryMeta(queryReq, queryRsp, payloads));
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(queryRsp.query_metas_size() == 1, K_RUNTIME_ERROR, "not found");
        return Status::OK();
    }

    Status InitInstance()
    {
        RETURN_IF_NOT_OK(datasystem::inject::Set("ocMetaManager.noNeedGetFromLocal", "return()"));
        return InitInstanceBase();
    }

    void TearDown() override
    {
        OCMigrateMetadataManager::Instance().Shutdown();
        etcdCM_.reset();
        etcdStore_.reset();
        ExternalClusterTest::TearDown();
    }

protected:
    std::set<std::string> objectKey_;
};

TEST_F(OCMetaManagerCreateMultiMetaTest, CreateMultiMetaTest)
{
    int metaNum = 6;
    DS_ASSERT_OK(InitInstance());
    DS_ASSERT_OK(CreateMultiMetadata(metaNum));
    for (const auto &objectKey : objectKeys_) {
        DS_ASSERT_OK(CheckMetadata(objectKey));
    }
    DS_ASSERT_OK(CreateMetadata(metaNum));
    for (const auto &objectKey : objectKeys_) {
        DS_ASSERT_OK(CheckMetadata(objectKey));
    }
}

TEST_F(OCMetaManagerCreateMultiMetaTest, CreateMultiMetaAndSetTest)
{
    int metaNum = 6;
    DS_ASSERT_OK(InitInstance());
    inject::Set("OCMetadataManager.createMultiMeta.delay", "1*sleep(10000)");
    int metaNum1 = 3;
    std::thread thread1([&] {
        int sleepTime = 4;
        std::this_thread::sleep_for(std::chrono::seconds(sleepTime));
        ASSERT_EQ(CreateMetadata(metaNum1).GetCode(), K_TRY_AGAIN);
    });
    DS_ASSERT_OK(CreateMultiMetadata(metaNum));
    thread1.join();
}

TEST_F(OCMetaManagerCreateMultiMetaTest, LEVEL1_CreateMultiMetaAndGetTest)
{
    int metaNum = 6;
    DS_ASSERT_OK(InitInstance());
    inject::Set("OCMetadataManager.createMultiMeta.delay", "1*sleep(10000)");
    std::thread thread1([&] {
        int sleepTime = 2;
        std::this_thread::sleep_for(std::chrono::seconds(sleepTime));
        for (const auto &objectKey : objectKeys_) {
            ASSERT_EQ(CheckMetadata(objectKey).GetCode(), K_RUNTIME_ERROR);
        }
    });
    DS_ASSERT_OK(CreateMultiMetadata(metaNum));
    thread1.join();
}

TEST_F(OCMetaManagerCreateMultiMetaTest, CreateAndMultiCreateMetaTest)
{
    int metaNum = 6;
    DS_ASSERT_OK(InitInstance());
    int metaNum1 = 3;
    DS_ASSERT_OK(CreateMetadata(metaNum1));
    ASSERT_EQ(CreateMultiMetadata(metaNum).GetCode(), K_OC_KEY_ALREADY_EXIST);
}

class OCMetaManagerTest : public OCMigrateMetadataManagerTest {
public:
    Status DeleteAllCopyMeta()
    {
        master::DeleteAllCopyMetaReqPb deleteReq;
        master::DeleteAllCopyMetaRspPb deleteRsp;
        *deleteReq.mutable_object_keys() = { objectKeys_.begin(), objectKeys_.end() };
        deleteReq.set_address(hostPort_.ToString());
        deleteReq.set_redirect(true);
        deleteReq.set_need_forward_objs_without_meta(true);
        std::shared_ptr<master::OCMetadataManager> ocMetadataManager;
        RETURN_IF_NOT_OK(replicaManager_->GetOcMetadataManager(workerUuid_, ocMetadataManager));
        ocMetadataManager->DeleteAllCopyMeta(deleteReq, deleteRsp);
        CHECK_FAIL_RETURN_STATUS(deleteRsp.failed_object_keys().empty(), K_RUNTIME_ERROR, "delete failed");
        return Status::OK();
    }
};

TEST_F(OCMetaManagerTest, DeleteMetaRemoveExperied)
{
    DS_ASSERT_OK(datasystem::inject::Set("check.expiredObject", "return()"));
    DS_ASSERT_OK(InitInstance());
    CreateMetadata(10);  // create meta num 10
    std::shared_ptr<master::OCMetadataManager> ocMetadataManager;
    DS_ASSERT_OK(replicaManager_->GetOcMetadataManager(workerUuid_, ocMetadataManager));
    DS_ASSERT_OK(DeleteAllCopyMeta());
    for (const auto &obj : objectKeys_) {
        datasystem::master::MetaForMigrationPb meta;
        std::unordered_map<std::string, std::unordered_set<std::shared_ptr<AsyncElement>>> asyncMap;
        ocMetadataManager->FillMetadataForMigration(obj, meta, asyncMap);
        ASSERT_FALSE(meta.enable_ttl());
    }
}
}  // namespace st
}  // namespace datasystem
