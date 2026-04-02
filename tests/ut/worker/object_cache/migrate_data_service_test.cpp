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
 * Description: Test interface to HashRingHealthCheck
 */

#include "datasystem/worker/object_cache/service/worker_oc_service_migrate_impl.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "securec.h"

#include "ut/common.h"
#include "../../../common/binmock/binmock.h"
#include "datasystem/common/object_cache/shm_guard.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/protos/worker_object.pb.h"
#include "datasystem/utils/status.h"
#include "datasystem/worker/object_cache/worker_oc_spill.h"
#include "datasystem/worker/object_cache/worker_request_manager.h"
#include "eviction_manager_common.h"

DS_DECLARE_string(spill_directory);
DS_DECLARE_uint64(spill_size_limit);
DS_DECLARE_uint32(data_migrate_rate_limit_mb);
DS_DECLARE_uint32(arena_per_tenant);
DS_DECLARE_uint32(max_client_num);

using namespace ::testing;
using namespace datasystem::object_cache;
using namespace datasystem::worker;

namespace datasystem {
namespace ut {

class MigrateDataServiceTest : public CommonTest, public EvictionManagerCommon {
public:
    void SetUp() override
    {
        CommonTest::SetUp();
        Init();
        const uint64_t memSize = 32 * 1024ul * 1024ul;
        FLAGS_arena_per_tenant = 1;
        allocator = datasystem::memory::Allocator::Instance();
        allocator->Init(memSize);
        FLAGS_spill_directory = "./spill" + GetStringUuid();
        FLAGS_spill_size_limit = memSize;
        DS_ASSERT_OK(WorkerOcSpill::Instance()->Init());
    }

    void Init()
    {
        objectTable_ = std::make_shared<ObjectTable>();
        WorkerOcServiceCrudParam param{
            .workerMasterApiManager = nullptr,
            .workerRequestManager = requestManager_,
            .memoryRefTable = nullptr,
            .objectTable = objectTable_,
            .evictionManager = nullptr,
            .workerDevOcManager = nullptr,
            .asyncPersistenceDelManager = nullptr,
            .asyncSendManager = nullptr,
            .asyncRollbackManager = nullptr,
            .metadataSize = 0,
            .persistenceApi = nullptr,
            .etcdCM = nullptr,
        };
        threadPool_ = std::make_shared<ThreadPool>(MEMCOPY_THREAD_NUM);
        impl_ = std::make_shared<WorkerOcServiceMigrateImpl>(param, nullptr, threadPool_, nullptr, "127.0.0.1:18888");
        TimerQueue::GetInstance()->Initialize();
    }

    void SetMemoryAvailable(bool available)
    {
        BINEXPECT_CALL(&WorkerOcServiceMigrateImpl::IsMemoryAvailable, (_, _)).WillRepeatedly(Return(available));
    }

    void SetSpillAvailable(bool available)
    {
        BINEXPECT_CALL(&WorkerOcServiceMigrateImpl::IsSpillAvaialble, (_)).WillRepeatedly(Return(available));
    }

    void SetDiskAvailable(bool available)
    {
        BINEXPECT_CALL(&WorkerOcServiceMigrateImpl::IsDiskAvailable, (_)).WillRepeatedly(Return(available));
    }

    void CreateObjects(const std::string &prefix, uint64_t dataSize, uint32_t count, uint64_t version, bool needCreate,
                       bool needLock, MigrateDataReqPb &req)
    {
        for (uint32_t i = 0; i < count; ++i) {
            std::string objectKey = prefix + std::to_string(i);
            if (needCreate) {
                DS_ASSERT_OK(CreateObject(objectKey, dataSize));
                if (needLock) {
                    std::shared_ptr<SafeObjType> entry;
                    DS_ASSERT_OK(objectTable_->Get(objectKey, entry));
                    DS_ASSERT_OK(entry->WLock());
                }
            }

            auto info = req.add_objects();
            info->set_object_key(objectKey);
            info->set_version(version);
            info->set_data_size(dataSize);
        }
    }

    uint64_t GetMetadatSize() const
    {
        constexpr int alignment = 0x8;
        // Worker set lockId_ = 0(shm_guard), so we need client_nums + 1 bits slot.
        uint64_t metadataSize = FLAGS_max_client_num == 0 ? 0 : FLAGS_max_client_num / alignment + 1;
        metadataSize += sizeof(uint32_t) + sizeof(char);
        auto alignCeiling = [](uintptr_t addr, uintptr_t alignment) {
            return (addr + alignment - 1) & ~(alignment - 1);
        };
        metadataSize = alignCeiling(metadataSize, 0x40);
        return metadataSize;
    }

    std::unordered_map<MetaAddrInfo, std::vector<std::string>> MockGroupObjKeysByMasterHostPort(
        const std::unordered_set<std::string> &objectKeys)
    {
        std::unordered_map<MetaAddrInfo, std::vector<std::string>> result;
        MetaAddrInfo info(HostPort("127.0.0.1:18481"), "");
        std::vector<std::string> vec{ objectKeys.begin(), objectKeys.end() };
        result.emplace(info, std::move(vec));
        return result;
    }

    std::unordered_map<MetaAddrInfo, std::vector<std::string>> MockGroupObjKeysByMasterHostPort2(
        const std::unordered_set<std::string> &objectKeys);

    Status PureQueryMeta(const std::shared_ptr<worker::WorkerMasterOCApi> &api, master::PureQueryMetaReqPb &req,
                         master::PureQueryMetaRspPb &rsp);

protected:
    std::shared_ptr<ThreadPool> threadPool_;
    std::shared_ptr<WorkerOcServiceMigrateImpl> impl_;
    std::shared_ptr<WorkerOcEvictionManager> evictionManager_;
    WorkerRequestManager requestManager_;
};

TEST_F(MigrateDataServiceTest, TestDiskIOError)
{
    SetMemoryAvailable(false);
    SetSpillAvailable(false);
    BINEXPECT_CALL(&memory::Allocator::IsDiskAvailable, ()).WillRepeatedly(Return(false));

    constexpr int size = 100;
    MigrateDataReqPb req;
    for (int i = 0; i < size; ++i) {
        auto objInfo = req.add_objects();
        objInfo->set_object_key("HelloWorld_" + std::to_string(i));
    }

    MigrateDataRspPb rsp;
    std::vector<RpcMessage> payloads;
    ASSERT_EQ(impl_->MigrateData(req, rsp, std::move(payloads)).GetCode(), StatusCode::K_OUT_OF_MEMORY);
    ASSERT_EQ(rsp.success_ids_size(), 0);
    ASSERT_EQ(rsp.fail_ids_size(), size);
}

TEST_F(MigrateDataServiceTest, TestResourcesUnavailable)
{
    SetMemoryAvailable(false);
    SetSpillAvailable(false);
    SetDiskAvailable(false);

    constexpr int size = 100;
    MigrateDataReqPb req;
    for (int i = 0; i < size; ++i) {
        auto objInfo = req.add_objects();
        objInfo->set_object_key("HelloWorld_" + std::to_string(i));
    }

    MigrateDataRspPb rsp;
    std::vector<RpcMessage> payloads;
    ASSERT_EQ(impl_->MigrateData(req, rsp, std::move(payloads)).GetCode(), StatusCode::K_OUT_OF_MEMORY);
    ASSERT_EQ(rsp.success_ids_size(), 0);
    ASSERT_EQ(rsp.fail_ids_size(), size);
}

TEST_F(MigrateDataServiceTest, TestLockNeedMigrateObjects)
{
    uint64_t elderVersion = 0;
    uint64_t nowVersion = 1;
    uint64_t newerVersion = 2;
    uint64_t expireCount = 100;
    uint64_t lockFailCount = 10;
    uint64_t newCreateCount = 40;
    uint64_t existCount = 50;

    MigrateDataReqPb req;
    CreateObjects("Expire_", 1, expireCount, elderVersion, true, false, req);
    CreateObjects("Locked_Failed_", 1, lockFailCount, nowVersion, true, true, req);
    CreateObjects("New_Created_", 1, newCreateCount, nowVersion, false, false, req);
    CreateObjects("Exist_", 1, existCount, newerVersion, true, false, req);

    LockedEntryMap lockedEntries;
    LockedEntryMap needModifyPrimary;
    std::unordered_set<std::string> successIds;
    std::unordered_set<std::string> failedIds;
    impl_->BatchLockForMigrateData(req.objects(), lockedEntries, successIds, failedIds, needModifyPrimary);
    ASSERT_EQ(lockedEntries.size(), newCreateCount + existCount);
    ASSERT_EQ(successIds.size(), expireCount);
    ASSERT_EQ(failedIds.size(), lockFailCount);
}

TEST_F(MigrateDataServiceTest, TestLockNeedMigrateObjectsFailed)
{
    DS_ASSERT_OK(inject::Set("SafeTable.ReserveGetAndLock.return", "1*call()"));
    uint64_t elderVersion = 0;
    uint64_t nowVersion = 1;
    uint64_t newerVersion = 2;
    uint64_t expireCount = 100;
    uint64_t lockFailCount = 10;
    uint64_t newCreateCount = 40;
    uint64_t existCount = 50;

    MigrateDataReqPb req;
    CreateObjects("Expire_", 1, expireCount, elderVersion, true, false, req);
    CreateObjects("Locked_Failed_", 1, lockFailCount, nowVersion, true, true, req);
    CreateObjects("New_Created_", 1, newCreateCount, nowVersion, false, false, req);
    CreateObjects("Exist_", 1, existCount, newerVersion, true, false, req);

    LockedEntryMap lockedEntries;
    LockedEntryMap needModifyPrimary;
    std::unordered_set<std::string> successIds;
    std::unordered_set<std::string> failedIds;
    impl_->BatchLockForMigrateData(req.objects(), lockedEntries, successIds, failedIds, needModifyPrimary);
}

TEST_F(MigrateDataServiceTest, ReplacePrimaryRetryFailed)
{
    Status status(StatusCode::K_RPC_UNAVAILABLE, "");
    const size_t retryTimes = 4;
    BINEXPECT_CALL(&WorkerOcServiceMigrateImpl::ReplacePrimaryOnce, (_, _, _))
        .Times(retryTimes)
        .WillRepeatedly(Return(status));
    std::shared_ptr<WorkerRemoteMasterOCApi> remoteApi =
        std::make_shared<WorkerRemoteMasterOCApi>(HostPort("127.0.0.1:18481"), HostPort("127.0.0.1:18482"), nullptr);
    master::ReplacePrimaryReqPb req;
    master::ReplacePrimaryRspPb rsp;
    DS_ASSERT_NOT_OK(impl_->ReplacePrimaryRetry(remoteApi, req, rsp));
}

TEST_F(MigrateDataServiceTest, DISABLED_TestQueryMetaFromMasterMeetsRPCError)
{
    LOG(INFO) << "Test query objects meta meets rpc error";
    BINEXPECT_CALL((std::unordered_map<MetaAddrInfo, std::vector<std::string>>(EtcdClusterManager::*)(
                       const std::unordered_set<std::string> &))
                       & EtcdClusterManager::GroupObjKeysByMasterHostPort,
                   (_))
        .Times(1)
        .WillRepeatedly(Invoke(this, &MigrateDataServiceTest::MockGroupObjKeysByMasterHostPort));

    Status status(StatusCode::K_RPC_UNAVAILABLE, "");
    BINEXPECT_CALL(&WorkerOcServiceMigrateImpl::PureQueryMetaOnce, (_, _, _)).Times(4).WillRepeatedly(Return(status));

    std::shared_ptr<WorkerRemoteMasterOCApi> remoteApi =
        std::make_shared<WorkerRemoteMasterOCApi>(HostPort("127.0.0.1:18481"), HostPort("127.0.0.1:18482"), nullptr);
    BINEXPECT_CALL(&WorkerOcServiceMigrateImpl::GetWorkerMasterApi, (_)).Times(1).WillRepeatedly(Return(remoteApi));

    MigrateDataReqPb req;
    uint64_t elderVersion = 0;
    uint64_t newCreateCount = 40;
    uint64_t expireCount = 100;
    uint64_t nowVersion = 1;
    CreateObjects("Expire_", 1, expireCount, elderVersion, true, false, req);
    CreateObjects("New_Created_", 1, newCreateCount, nowVersion, false, false, req);
    MigrateDataRspPb rsp;
    std::vector<RpcMessage> payloads;
    ASSERT_EQ(impl_->MigrateData(req, rsp, std::move(payloads)).GetCode(), StatusCode::K_RPC_UNAVAILABLE);
    ASSERT_EQ(rsp.fail_ids_size(), newCreateCount);
    ASSERT_EQ(rsp.success_ids_size(), expireCount);
}

std::unordered_map<MetaAddrInfo, std::vector<std::string>> MigrateDataServiceTest::MockGroupObjKeysByMasterHostPort2(
    const std::unordered_set<std::string> &objectKeys)
{
    std::unordered_map<MetaAddrInfo, std::vector<std::string>> result;
    size_t size = objectKeys.size();
    size_t count = 0;
    size_t batch = 3;
    for (const auto &id : objectKeys) {
        if (count < size / batch) {
            MetaAddrInfo info(HostPort("127.0.0.1:18481"), "");
            result[info].emplace_back(id);
        } else if (count < (size / batch * 2)) {
            MetaAddrInfo info(HostPort("127.0.0.1:18482"), "");
            result[info].emplace_back(id);
        } else {
            MetaAddrInfo info(HostPort("127.0.0.1:18483"), "");
            result[info].emplace_back(id);
        }
        count++;
    }
    return result;
}

size_t gCount = 9000;

Status MigrateDataServiceTest::PureQueryMeta(const std::shared_ptr<worker::WorkerMasterOCApi> &api,
                                             master::PureQueryMetaReqPb &req, master::PureQueryMetaRspPb &rsp)
{
    auto fillMeta = [](const std::string &id, master::PureQueryMetaRspPb &rsp) {
        if (id.find_first_of("Equal_Version") != std::string::npos) {
            auto meta = rsp.add_query_metas();
            meta->mutable_meta()->set_version(1);
            meta->mutable_meta()->set_object_key(id);
        } else if (id.find_first_of("Larger_Version") != std::string::npos) {
            auto meta = rsp.add_query_metas();
            meta->mutable_meta()->set_version(2);
            meta->mutable_meta()->set_object_key(id);
        } else if (id.find_first_of("Smaller_Version") != std::string::npos) {
            auto meta = rsp.add_query_metas();
            meta->mutable_meta()->set_version(0);
            meta->mutable_meta()->set_object_key(id);
        }
    };

    (void)api;
    int size = req.object_keys_size();
    int count = 0;
    RedirectMetaInfo *info = nullptr;
    for (const auto &id : req.object_keys()) {
        if (req.redirect() && count < size / 2) {
            if (info == nullptr) {
                info = rsp.add_info();
                info->set_redirect_meta_address("127.0.0.1:" + std::to_string(gCount++));
            }
            info->add_change_meta_ids(id);
        } else {
            fillMeta(id, rsp);
        }
        ++count;
    }
    return Status::OK();
}

TEST_F(MigrateDataServiceTest, DISABLED_TestQueryMetaFromMasterBasicFunction)
{
    LOG(INFO) << "Test query meta from master basic function";
    BINEXPECT_CALL((std::unordered_map<MetaAddrInfo, std::vector<std::string>>(EtcdClusterManager::*)(
                       const std::unordered_set<std::string> &))
                       & EtcdClusterManager::GroupObjKeysByMasterHostPort,
                   (_))
        .Times(1)
        .WillRepeatedly(Invoke(this, &MigrateDataServiceTest::MockGroupObjKeysByMasterHostPort2));

    BINEXPECT_CALL(&WorkerOcServiceMigrateImpl::PureQueryMetaOnce, (_, _, _))
        .Times(6)
        .WillRepeatedly(Invoke(this, &MigrateDataServiceTest::PureQueryMeta));

    std::shared_ptr<WorkerRemoteMasterOCApi> remoteApi =
        std::make_shared<WorkerRemoteMasterOCApi>(HostPort("127.0.0.1:18481"), HostPort("127.0.0.1:18482"), nullptr);
    BINEXPECT_CALL(&WorkerOcServiceMigrateImpl::GetWorkerMasterApi, (_)).Times(6).WillRepeatedly(Return(remoteApi));

    BINEXPECT_CALL(&WorkerOcServiceCrudCommonApi::GetPrimaryReplicaAddr, (_, _))
        .Times(3)
        .WillRepeatedly(Invoke([](const std::string &srcAddr, HostPort &destAddr) {
            destAddr.ParseString(srcAddr);
            return Status::OK();
        }));

    std::unordered_set<std::string> objectKeys;
    uint64_t count = 300;
    for (size_t i = 0; i < count; ++i) {
        if (i > count / 2) {
            objectKeys.emplace("Absent_ID" + std::to_string(i));
        } else {
            objectKeys.emplace("Equal_Version" + std::to_string(i));
        }
    }
    QueryMetaMap queryMetas;
    std::unordered_set<std::string> failedIds;
    DS_ASSERT_OK(impl_->QueryMasterMetadata(objectKeys, queryMetas, failedIds));
    ASSERT_EQ(queryMetas.size(), count / 2);
    ASSERT_TRUE(failedIds.empty());
}

TEST_F(MigrateDataServiceTest, TestQueryMasterMetadataError)
{
    Status status1(StatusCode::K_RUNTIME_ERROR, "");
    BINEXPECT_CALL(&WorkerOcServiceMigrateImpl::QueryMasterMetadata, (_, _, _))
        .Times(1)
        .WillRepeatedly(Return(status1));

    MigrateDataReqPb req;
    uint64_t elderVersion = 0;
    uint64_t newCreateCount = 40;
    uint64_t expireCount = 100;
    uint64_t nowVersion = 1;
    CreateObjects("Expire_", 1, expireCount, elderVersion, true, false, req);
    CreateObjects("New_Created_", 1, newCreateCount, nowVersion, false, false, req);
    MigrateDataRspPb rsp;
    std::vector<RpcMessage> payloads;
    ASSERT_EQ(impl_->MigrateData(req, rsp, std::move(payloads)).GetCode(), StatusCode::K_RUNTIME_ERROR);
}

TEST_F(MigrateDataServiceTest, TestMigrateDataMeetsOOM)
{
    LOG(INFO) << "Test migrate data meets OOM";
}

TEST_F(MigrateDataServiceTest, TestAllocateAndAssignDataBasicFunction)
{
    BINEXPECT_CALL(&WorkerOcEvictionManager::Add, (_)).Times(1).WillRepeatedly(Return());
    BINEXPECT_CALL(&WorkerOcServiceCrudCommonApi::GetMetadataSize, ())
        .Times(1)
        .WillRepeatedly(Return(GetMetadatSize()));
    // Get offset and size, let all memory zone are 1.
    uint64_t size = 1024ul * 1024ul;
    void *pointer;
    int fd;
    ptrdiff_t offset;
    uint64_t mmapSize;
    DS_ASSERT_OK(datasystem::memory::Allocator::Instance()->AllocateMemory(DEFAULT_TENANT_ID, size, false, pointer, fd,
                                                                           offset, mmapSize));
    (void)memset_s((uint8_t *)pointer - offset, mmapSize, 0xff, mmapSize);

    std::string objectKey = "xxx";
    std::shared_ptr<SafeObjType> entry =
        std::make_shared<SafeObjType>(std::make_unique<object_cache::ObjCacheShmUnit>());
    (*entry)->modeInfo.SetCacheType(CacheType::MEMORY);

    std::vector<uint8_t> data(size, 0);
    std::vector<std::pair<const uint8_t *, uint64_t>> payloads = { { data.data(), data.size() } };
    DS_ASSERT_OK(impl_->AllocateAndAssignData(objectKey, entry, payloads, size, nullptr));
    auto shmUnit = (*entry)->GetShmUnit();
    ShmGuard guard(shmUnit, GetMetadatSize(), shmUnit->GetSize() - GetMetadatSize());
    DS_ASSERT_OK(guard.TryRLatch(true));
}

TEST_F(MigrateDataServiceTest, TestMemoryAvailableForSpill)
{
    LOG(INFO) << "Test CheckResource for SPILL type when memory is available";
    SetMemoryAvailable(true);

    MigrateDataReqPb req;
    req.set_type(MigrateType::SPILL);
    MigrateDataRspPb rsp;
    DS_ASSERT_OK(impl_->CheckResource(req, rsp));
    EXPECT_EQ(rsp.fail_ids_size(), 0);
}

TEST_F(MigrateDataServiceTest, TestOOMForSpill)
{
    LOG(INFO) << "Test CheckResource for SPILL type when oom";
    SetMemoryAvailable(false);

    constexpr uint32_t objectCount = 10;
    constexpr uint64_t dataSize = 1024;
    MigrateDataReqPb req;
    req.set_type(MigrateType::SPILL);
    for (uint32_t i = 0; i < objectCount; ++i) {
        auto objInfo = req.add_objects();
        objInfo->set_object_key("spill_fail_obj_" + std::to_string(i));
        objInfo->set_data_size(dataSize);
    }

    MigrateDataRspPb rsp;
    Status status = impl_->CheckResource(req, rsp);
    EXPECT_EQ(status.GetCode(), StatusCode::K_OUT_OF_MEMORY);
    EXPECT_EQ(rsp.success_ids_size(), 0);
    EXPECT_EQ(rsp.fail_ids_size(), objectCount);
}

TEST_F(MigrateDataServiceTest, TestInvalidMigrateType)
{
    LOG(INFO) << "Test CheckResource with invalid migrate type";

    constexpr int invalidTypeValue = 999;
    MigrateDataReqPb req;
    req.set_type(static_cast<MigrateType>(invalidTypeValue));
    MigrateDataRspPb rsp;
    ASSERT_EQ(impl_->CheckResource(req, rsp).GetCode(), StatusCode::K_INVALID);
}

TEST_F(MigrateDataServiceTest, TestSaveDataWithSpillType)
{
    BINEXPECT_CALL(&WorkerOcEvictionManager::Add, (_)).Times(1).WillRepeatedly(Return());
    std::shared_ptr<SafeObjType> entry =
        std::make_shared<SafeObjType>(std::make_unique<object_cache::ObjCacheShmUnit>());
    MigrateDataReqPb::ObjectInfoPb info;
    info.set_object_key("object1");
    constexpr uint64_t dataSize = 26 * 1024 * 1024;  // 26 MB is larger than memory high water for spill type
    info.set_data_size(dataSize);
    info.add_part_index(0);
    std::vector<RpcMessage> payloads(1);
    std::string data = "1";
    payloads[0].CopyString(data);
    // Will oom, don't spill to disk
    ASSERT_EQ(impl_->SaveDataWithObjectLocked(entry, info, payloads, MigrateType::SPILL, nullptr).GetCode(),
              StatusCode::K_OUT_OF_MEMORY);
    info.set_data_size(1);
    DS_ASSERT_OK(impl_->SaveDataWithObjectLocked(entry, info, payloads, MigrateType::SPILL, nullptr));
}

class MigrateL2DataServiceTest : public MigrateDataServiceTest {};

TEST_F(MigrateL2DataServiceTest, TestMigrateL2Data)
{
    
}
}  // namespace ut
}  // namespace datasystem