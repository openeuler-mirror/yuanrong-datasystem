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
 * Description: Test migrate metadata manager class.
 */
#include "datasystem/master/object_cache/master_oc_service_impl.h"
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
#include "datasystem/object_cache/object_enum.h"
#include "datasystem/protos/generic_service.pb.h"
#include "datasystem/protos/master_object.pb.h"
#include "datasystem/protos/object_posix.pb.h"
#include "datasystem/kv_cache/kv_client.h"
#include "datasystem/utils/status.h"
#include "datasystem/worker/cluster_manager/etcd_cluster_manager.h"
#include "datasystem/worker/hash_ring/hash_ring_allocator.h"
#include "datasystem/worker/object_cache/worker_master_oc_api.h"
#include "gtest/gtest.h"

DS_DECLARE_string(backend_store_dir);
DS_DECLARE_string(etcd_address);
DS_DECLARE_string(master_address);

using namespace datasystem::master;
using namespace datasystem::worker;
namespace datasystem {
namespace st {
using ObjectTable = SafeTable<ImmutableString, ObjectInterface>;
class OCGiveUpPrimaryTest : public ExternalClusterTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numEtcd = 1;
        opts.numMasters = 0;
        opts.numWorkers = 0;
    }

    uint64_t GetMetaSize(uint64_t dataSize)
    {
        const uint64_t defaultMetaSize = 10;
        return object_cache::WorkerOcServiceCrudCommonApi::CanTransferByShm(dataSize) ? defaultMetaSize : 0;
    }

    Status PreInitRocksDB()
    {
        RETURN_IF_NOT_OK(
            Uri::NormalizePathWithUserHomeDir(FLAGS_backend_store_dir, "~/.datasystem/rocksdb", "/master"));
        std::string preInitRocksDir = FLAGS_backend_store_dir + "/pre-start";
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

    Status InitInstanceBase()
    {
        // immutable string pool should be initiailzed first
        ImmutableStringPool::Instance().Init();
        int stubCacheNum = 100;
        RpcStubCacheMgr::Instance().Init(stubCacheNum);
        // The static members are destructed in the reverse order of their construction,
        // Below call guarantees that the destructor of Env is behind other Rocksdb singletons.
        (void)rocksdb::Env::Default();
        FLAGS_backend_store_dir = GetTestCaseDataDir() + "/rocksdb";
        LOG_IF_ERROR(PreInitRocksDB(), "Failed to initialize the rocksdb database in advance.");
        masterAddr_.ParseString("127.0.0.1:" + std::to_string(GetFreePort()));
        akSkManager_ = std::make_shared<AkSkManager>();
        akSkManager_->SetClientAkSk(accessKey_, secretKey_);
        TimerQueue::GetInstance()->Initialize();
        std::pair<HostPort, HostPort> addrs;
        RETURN_IF_NOT_OK(cluster_->GetEtcdAddrs(0, addrs));
        FLAGS_etcd_address = addrs.first.ToString();
        etcdStore_ = std::make_unique<EtcdStore>(FLAGS_etcd_address);
        FLAGS_master_address = masterAddr_.ToString();
        RETURN_IF_NOT_OK(etcdStore_->Init());
        replicaManager_ = std::make_unique<ReplicaManager>();
        etcdCM_ = std::make_unique<EtcdClusterManager>(masterAddr_, masterAddr_, etcdStore_.get());
        ClusterInfo clusterInfo;
        RETURN_IF_NOT_OK(EtcdClusterManager::ConstructClusterInfoViaEtcd(etcdStore_.get(), clusterInfo));
        RETURN_IF_NOT_OK(etcdCM_->Init(clusterInfo));
        workerUuid_ = etcdCM_->GetLocalWorkerUuid();
        // create worker service;
        uint64_t sharedMemoryBytes = 64 * 1024ul * 1024ul;  // convert mb to bytes.
        RETURN_IF_NOT_OK(datasystem::memory::Allocator::Instance()->Init(sharedMemoryBytes));
        auto objectTable_ = std::make_shared<ObjectTable>();
        LOG(INFO) << "Given etcd address is: " << FLAGS_etcd_address;
        auto evictionManager =
            std::make_shared<object_cache::WorkerOcEvictionManager>(objectTable_, masterAddr_, masterAddr_, nullptr);
        worker1OcServiceImpl_ = std::make_shared<datasystem::object_cache::WorkerOCServiceImpl>(
            masterAddr_, masterAddr_, objectTable_, akSkManager_, evictionManager, nullptr, etcdStore_.get());
        objCacheWorkerMsSvc_ =
            std::make_shared<datasystem::object_cache::MasterWorkerOCServiceImpl>(worker1OcServiceImpl_, akSkManager_);
        ReplicaManagerParam param;
        param.dbRootPath = FLAGS_backend_store_dir;
        param.currWorkerId = workerUuid_;
        param.akSkManager = akSkManager_;
        param.etcdStore = etcdStore_.get();
        param.persistenceApi = nullptr;
        param.etcdCM = etcdCM_.get();
        param.masterAddress = masterAddr_;
        param.masterWorkerService = objCacheWorkerMsSvc_.get();
        param.workerWorkerService = nullptr;
        param.workerWorkerService = nullptr;
        param.isOcEnabled = true;
        RETURN_IF_NOT_OK(replicaManager_->Init(param));
        RETURN_IF_NOT_OK(replicaManager_->AddOrSwitchTo(param.currWorkerId, ReplicaType::Primary));
        etcdCM_->SetWorkerReady();
        objCacheMasterSvc_ =
            std::make_unique<MasterOCServiceImpl>(masterAddr_, nullptr, akSkManager_, replicaManager_.get());
        objCacheMasterSvc_->SetClusterManager(etcdCM_.get());
        RETURN_IF_NOT_OK(objCacheMasterSvc_->Init());
        RETURN_IF_NOT_OK(datasystem::inject::Set("master.cache_invalid_failed", "return(K_OK)"));
        worker1OcServiceImpl_->SetClusterManager(etcdCM_.get());
        RETURN_IF_NOT_OK(worker1OcServiceImpl_->Init());
        SetHealthProbe();
        return Status::OK();
    }

    Status CreateCopyMeta(std::vector<std::string> &objs, std::string workerAddr)
    {
        for (const auto &id : objs) {
            master::CreateCopyMetaReqPb req;
            master::CreateCopyMetaRspPb rsp;
            req.set_object_key(id);
            req.set_address(workerAddr);
            req.set_data_format(static_cast<uint32_t>(DataFormat::BINARY));
            std::shared_ptr<master::OCMetadataManager> ocMetadataManager;
            RETURN_IF_NOT_OK(replicaManager_->GetOcMetadataManager(workerUuid_, ocMetadataManager));
            RETURN_IF_NOT_OK_PRINT_ERROR_MSG(ocMetadataManager->CreateCopyMeta(req, rsp), "create copy failed");
        }
        return Status::OK();
    }

    Status CreateMetadata(std::vector<std::string> &objs, std::string workerAddr, WriteMode mode, int dataSize = 10)
    {
        for (size_t i = 0; i < objs.size(); ++i) {
            CreateMetaReqPb request;
            CreateMetaRspPb response;
            datasystem::ObjectMetaPb *metadata = request.mutable_meta();
            std::string objectKey = "MigrateMetadataTestId" + std::to_string(i);
            objs[i] = objectKey;
            metadata->set_object_key(objectKey);
            metadata->set_data_size(dataSize);
            metadata->set_life_state(static_cast<uint32_t>(ObjectLifeState::OBJECT_PUBLISHED));
            metadata->set_ttl_second(0);
            ObjectMetaPb::ConfigPb *configPb = metadata->mutable_config();
            configPb->set_write_mode(static_cast<uint32_t>(mode));
            configPb->set_data_format(static_cast<uint32_t>(DataFormat::BINARY));
            configPb->set_consistency_type(static_cast<uint32_t>(ConsistencyType::CAUSAL));
            request.set_address(workerAddr);
            std::shared_ptr<master::OCMetadataManager> ocMetadataManager;
            RETURN_IF_NOT_OK(replicaManager_->GetOcMetadataManager(workerUuid_, ocMetadataManager));
            RETURN_IF_NOT_OK_PRINT_ERROR_MSG(ocMetadataManager->CreateMeta(request, response), "create failed");
        }
        return Status::OK();
    }

    Status InitInstance(std::shared_ptr<WorkerLocalMasterOCApi> &workerMasterApi, std::string localWorkerAddr)
    {
        HostPort localAddr;
        RETURN_IF_NOT_OK(localAddr.ParseString(localWorkerAddr));
        workerMasterApi = std::make_shared<WorkerLocalMasterOCApi>(objCacheMasterSvc_.get(), localAddr, akSkManager_);
        RETURN_IF_NOT_OK(workerMasterApi->Init());
        return Status::OK();
    }

    void TearDown() override
    {
        objCacheMasterSvc_->Shutdown();
        objCacheMasterSvc_.reset();
        etcdCM_.reset();
        etcdStore_.reset();
        objCacheWorkerMsSvc_.reset();
        ExternalClusterTest::TearDown();
    }

    HostPort masterAddr_;
    std::shared_ptr<AkSkManager> akSkManager_;
    std::shared_ptr<ObjectTable> objectTable_;
    std::string accessKey_ = "QTWAOYTTINDUT2QVKYUC";
    std::string secretKey_ = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
    std::unique_ptr<EtcdStore> etcdStore_;
    std::unique_ptr<EtcdClusterManager> etcdCM_;
    std::string workerUuid_;
    RandomData random_;
    std::shared_ptr<datasystem::object_cache::MasterWorkerOCServiceImpl> objCacheWorkerMsSvc_;
    std::unique_ptr<MasterOCServiceImpl> objCacheMasterSvc_;
    std::shared_ptr<datasystem::object_cache::WorkerOCServiceImpl> worker1OcServiceImpl_;
    std::unique_ptr<ReplicaManager> replicaManager_;
};

TEST_F(OCGiveUpPrimaryTest, TestGIveUpPrimary)
{
    FLAGS_v = 2;  // vlog is 2
    DS_ASSERT_OK(InitInstanceBase());
    DS_ASSERT_OK(datasystem::inject::Set("process.change.primary.copy", "return(K_OK)"));
    std::string primaryAddr = "127.0.0.1:1";
    std::vector<std::string> objs(40);  // obj num is 40
    CreateMetadata(objs, masterAddr_.ToString(), WriteMode::NONE_L2_CACHE);
    CreateCopyMeta(objs, primaryAddr);
    std::vector<std::string> removeFailedIds;
    std::vector<std::string> needMigrateDataIds;
    std::vector<std::string> needWaitIds;
    RemoveMetaRspPb rsp;
    RemoveMetaReqPb req;
    *req.mutable_ids() = { objs.begin(), objs.end() };
    req.set_address(primaryAddr);
    req.set_cause(RemoveMetaReqPb::Cause::RemoveMetaReqPb_Cause_GIVEUP_PRIMARY);
    req.set_version(UINT64_MAX);
    std::shared_ptr<WorkerLocalMasterOCApi> workerMasterApi;
    DS_ASSERT_OK(InitInstance(workerMasterApi, primaryAddr));
    DS_ASSERT_OK(workerMasterApi->RemoveMeta(req, rsp));
}

TEST_F(OCGiveUpPrimaryTest, TestGIveUpPrimaryFailed)
{
    FLAGS_v = 2;  // vlog is 2
    DS_ASSERT_OK(InitInstanceBase());
    DS_ASSERT_OK(datasystem::inject::Set("process.change.primary.copy", "39*return(K_OK)"));
    DS_ASSERT_OK(datasystem::inject::Set("OCMetadataManager.IsPrimaryCopyWithCopy", "40*return()"));
    std::string primaryAddr = "127.0.0.1:1";
    std::vector<std::string> objs(40);  // obj num is 40
    CreateMetadata(objs, masterAddr_.ToString(), WriteMode::NONE_L2_CACHE);
    CreateCopyMeta(objs, primaryAddr);
    std::vector<std::string> removeFailedIds;
    std::vector<std::string> needMigrateDataIds;
    std::vector<std::string> needWaitIds;
    RemoveMetaRspPb rsp;
    RemoveMetaReqPb req;
    *req.mutable_ids() = { objs.begin(), objs.end() };
    req.set_address(primaryAddr);
    req.set_cause(RemoveMetaReqPb::Cause::RemoveMetaReqPb_Cause_GIVEUP_PRIMARY);
    req.set_version(UINT64_MAX);
    std::shared_ptr<WorkerLocalMasterOCApi> workerMasterApi;
    DS_ASSERT_OK(InitInstance(workerMasterApi, primaryAddr));
    DS_ASSERT_OK(workerMasterApi->RemoveMeta(req, rsp));
    ASSERT_EQ(rsp.failed_ids().size(), 1);
}

TEST_F(OCGiveUpPrimaryTest, TestGiveUpPrimaryFailedRetry)
{
    FLAGS_v = 2;  // vlog is 2
    DS_ASSERT_OK(InitInstanceBase());
    DS_ASSERT_OK(datasystem::inject::Set("process.change.primary.copy", "return(K_OK)"));
    DS_ASSERT_OK(datasystem::inject::Set("SendChangePrimaryCopy.failed", "call()"));
    DS_ASSERT_OK(datasystem::inject::Set("master.RetryForFailedIds.success", "return()"));
    std::string primaryAddr = "127.0.0.1:1";
    std::string location = "127.0.0.1:24132";
    std::vector<std::string> objs(40);  // obj num is 40
    CreateMetadata(objs, primaryAddr, WriteMode::NONE_L2_CACHE);
    CreateCopyMeta(objs, location);
    CreateCopyMeta(objs, masterAddr_.ToString());
    std::vector<std::string> removeFailedIds;
    std::vector<std::string> needMigrateDataIds;
    std::vector<std::string> needWaitIds;
    RemoveMetaRspPb rsp;
    RemoveMetaReqPb req;
    *req.mutable_ids() = { objs.begin(), objs.end() };
    req.set_address(primaryAddr);
    req.set_cause(RemoveMetaReqPb::Cause::RemoveMetaReqPb_Cause_GIVEUP_PRIMARY);
    req.set_version(UINT64_MAX);
    std::shared_ptr<WorkerLocalMasterOCApi> workerMasterApi;
    DS_ASSERT_OK(InitInstance(workerMasterApi, primaryAddr));
    DS_ASSERT_OK(workerMasterApi->RemoveMeta(req, rsp));
    ASSERT_EQ(rsp.failed_ids().size(), 0);
}
}  // namespace st
}  // namespace datasystem