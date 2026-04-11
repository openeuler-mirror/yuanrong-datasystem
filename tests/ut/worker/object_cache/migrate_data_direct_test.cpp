/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
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

#include "datasystem/worker/object_cache/service/worker_oc_service_migrate_impl.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <cstdint>
#include <memory>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "ut/common.h"

#include "../../../common/binmock/binmock.h"
#include "datasystem/common/rdma/fast_transport_manager_wrapper.h"
#include "datasystem/protos/worker_object.pb.h"
#include "datasystem/utils/status.h"
#include "datasystem/worker/object_cache/worker_request_manager.h"
#include "eviction_manager_common.h"

DS_DECLARE_uint32(arena_per_tenant);

using namespace ::testing;
using namespace datasystem::object_cache;
using namespace datasystem::worker;

namespace datasystem {
namespace ut {

class MigrateDataDirectTest : public CommonTest, public EvictionManagerCommon {
public:
    MigrateDataDirectTest() = default;
    ~MigrateDataDirectTest() override = default;

    void SetUp() override
    {
        CommonTest::SetUp();
        Init();
        const uint64_t memSize = 32 * 1024UL * 1024UL;
        FLAGS_arena_per_tenant = 1;
        allocator = datasystem::memory::Allocator::Instance();
        allocator->Init(memSize);
        SetMemoryAvailable(true);
        BINEXPECT_CALL(&WorkerOcEvictionManager::Add, (_)).WillRepeatedly(Return());
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
        impl_ =
            std::make_shared<WorkerOcServiceMigrateImpl>(param, nullptr, threadPool_, nullptr, "127.0.0.1:18888");
        TimerQueue::GetInstance()->Initialize();
    }

    void SetMemoryAvailable(bool available)
    {
        BINEXPECT_CALL(&WorkerOcServiceMigrateImpl::IsMemoryAvailable, (_, _)).WillRepeatedly(Return(available));
    }

    static constexpr uint64_t defaultDataSize_ = 16;
    static constexpr uint32_t defaultPort_ = 18888;
    static constexpr uint64_t defaultSegVa_ = 0x1000;
    static constexpr uint64_t secondSegVa_ = 0x2000;
    static constexpr uint64_t waitEventKey_ = 123;

    struct DirectObjectSpec {
        std::string objectKey;
        uint64_t version = 0;
        uint64_t dataSize = 0;
        bool withUrmaInfo = false;
        uint64_t segVa = 0;
        uint64_t segDataOffset = 0;
    };

    void EnableUrma(bool enabled)
    {
        BINEXPECT_CALL(&datasystem::IsUrmaEnabled, ()).WillRepeatedly(Return(enabled));
    }

    master::QueryMetaInfoPb MakeMetaInfo(const std::string &objectKey, uint64_t version, uint64_t dataSize) const
    {
        master::QueryMetaInfoPb metaInfo;
        auto *meta = metaInfo.mutable_meta();
        meta->set_object_key(objectKey);
        meta->set_version(version);
        meta->set_data_size(dataSize);
        meta->set_life_state(static_cast<uint32_t>(ObjectLifeState::OBJECT_SEALED));
        auto *cfg = meta->mutable_config();
        cfg->set_consistency_type(static_cast<uint32_t>(ConsistencyType::CAUSAL));
        cfg->set_write_mode(static_cast<uint32_t>(WriteMode::NONE_L2_CACHE));
        cfg->set_cache_type(static_cast<uint32_t>(CacheType::MEMORY));
        cfg->set_data_format(static_cast<uint32_t>(DataFormat::BINARY));
        return metaInfo;
    }

    void MockQueryMasterMetadataOk(uint64_t metaVersion = 1, uint64_t dataSize = defaultDataSize_)
    {
        BINEXPECT_CALL(&WorkerOcServiceMigrateImpl::QueryMasterMetadata, (_, _, _))
            .Times(1)
            .WillOnce(
            Invoke([this, metaVersion, dataSize](const std::unordered_set<std::string> &objectKeys, QueryMetaMap &metas,
                                                 std::unordered_set<std::string> &failedIds) {
                (void)failedIds;
                for (const auto &id : objectKeys) {
                    metas.emplace(id, MakeMetaInfo(id, metaVersion, dataSize));
                }
                return Status::OK();
            }));
    }

    void MockUrmaReadReturnOnce(const Status &rc, std::vector<uint64_t> eventKeys = {})
    {
        BINEXPECT_CALL(&datasystem::UrmaRead, (_, _, _, _, _, _, _))
            .Times(1)
            .WillOnce(Invoke([rc, eventKeys = std::move(eventKeys)](
                                 const datasystem::UrmaRemoteAddrPb &, const uint64_t &, const uint64_t &,
                                 const uint64_t &, const uint64_t &, const uint64_t &, std::vector<uint64_t> &keys) {
                keys = eventKeys;
                return rc;
            }));
    }

    void MockUrmaReadOkTimes(int times, std::vector<uint64_t> eventKeys = { 1 })
    {
        BINEXPECT_CALL(&datasystem::UrmaRead, (_, _, _, _, _, _, _))
            .Times(times)
            .WillRepeatedly(Invoke([eventKeys = std::move(eventKeys)](const datasystem::UrmaRemoteAddrPb &,
                                                                      const uint64_t &, const uint64_t &,
                                                                      const uint64_t &, const uint64_t &,
                                                                      const uint64_t &, std::vector<uint64_t> &keys) {
                keys = eventKeys;
                return Status::OK();
            }));
    }

    void MockWaitFastTransportEventOnce(const Status &rc)
    {
        BINEXPECT_CALL(&datasystem::WaitFastTransportEvent, (_, _, _)).Times(1).WillOnce(Return(rc));
    }

    void MockWaitFastTransportEventOkTimes(int times)
    {
        BINEXPECT_CALL(&datasystem::WaitFastTransportEvent, (_, _, _))
            .Times(times)
            .WillRepeatedly(Return(Status::OK()));
    }

    void MockReplacePrimaryOk(int expectedNeedSendMasterSize = -1)
    {
        BINEXPECT_CALL(&WorkerOcServiceMigrateImpl::ReplacePrimaryImpl, (_, _, _, _, _))
            .Times(1)
            .WillOnce(
            Invoke([expectedNeedSendMasterSize](const std::string &, const ObjectInfoMap &needSendMasterIds,
                                                const MigrateType &, std::unordered_set<std::string> &successIds,
                                                std::unordered_set<std::string> &failedIds) {
                (void)failedIds;
                if (expectedNeedSendMasterSize >= 0) {
                    EXPECT_EQ(static_cast<int>(needSendMasterIds.size()), expectedNeedSendMasterSize);
                }
                for (const auto &it : needSendMasterIds) {
                    successIds.insert(it.first);
                }
                return Status::OK();
            }));
    }

    MigrateDataDirectReqPb MakeReq(const std::vector<DirectObjectSpec> &objects,
                                   const std::string &workerAddr = "127.0.0.1:18888") const
    {
        MigrateDataDirectReqPb req;
        if (!workerAddr.empty()) {
            req.set_worker_addr(workerAddr);
        }
        for (const auto &spec : objects) {
            auto *obj = req.add_objects();
            obj->set_object_key(spec.objectKey);
            if (spec.version != 0) {
                obj->set_version(spec.version);
            }
            if (spec.dataSize != 0) {
                obj->set_data_size(spec.dataSize);
            }
            if (spec.withUrmaInfo) {
                obj->mutable_urma_info()->mutable_request_address()->set_host("127.0.0.1");
                obj->mutable_urma_info()->mutable_request_address()->set_port(defaultPort_);
                obj->mutable_urma_info()->set_seg_va(spec.segVa == 0 ? defaultSegVa_ : spec.segVa);
                obj->mutable_urma_info()->set_seg_data_offset(spec.segDataOffset);
            }
        }
        return req;
    }

    std::shared_ptr<ThreadPool> threadPool_;
    std::shared_ptr<WorkerOcServiceMigrateImpl> impl_;
    std::shared_ptr<WorkerOcEvictionManager> evictionManager_;
    WorkerRequestManager requestManager_;
};

TEST_F(MigrateDataDirectTest, TestMigrateDataDirectUrmaNotEnabled)
{
    EnableUrma(false);
    auto req = MakeReq({ { .objectKey = "obj1" } });

    MigrateDataDirectRspPb rsp;
    ASSERT_EQ(impl_->MigrateDataDirect(req, rsp).GetCode(), StatusCode::K_RUNTIME_ERROR);
    ASSERT_EQ(rsp.failed_object_keys_size(), 1);
}

TEST_F(MigrateDataDirectTest, TestMigrateDataDirectOOM)
{
    SetMemoryAvailable(false);
    EnableUrma(true);

    auto req = MakeReq({ { .objectKey = "obj1" } });
    MigrateDataDirectRspPb rsp;
    ASSERT_EQ(impl_->MigrateDataDirect(req, rsp).GetCode(), StatusCode::K_OUT_OF_MEMORY);
    ASSERT_EQ(rsp.failed_object_keys_size(), 1);
}

TEST_F(MigrateDataDirectTest, TestMigrateDataDirectSuccess)
{
    EnableUrma(true);
    MockQueryMasterMetadataOk(1, defaultDataSize_);
    MockUrmaReadReturnOnce(Status::OK());
    MockWaitFastTransportEventOnce(Status::OK());
    MockReplacePrimaryOk();

    auto req = MakeReq({ { .objectKey = "obj1", .version = 1, .dataSize = defaultDataSize_, .withUrmaInfo = true } });

    MigrateDataDirectRspPb rsp;
    DS_ASSERT_OK(impl_->MigrateDataDirect(req, rsp));
    ASSERT_EQ(rsp.failed_object_keys_size(), 0);
    ASSERT_GT(rsp.remain_bytes(), 0);

    std::shared_ptr<SafeObjType> entry;
    DS_ASSERT_OK(objectTable_->Get("obj1", entry));
    ASSERT_TRUE((*entry)->GetShmUnit() != nullptr);
}

TEST_F(MigrateDataDirectTest, TestMigrateDataDirectWaitEventFail)
{
    EnableUrma(true);
    MockQueryMasterMetadataOk(1, defaultDataSize_);
    MockUrmaReadReturnOnce(Status::OK(), { waitEventKey_ });
    MockWaitFastTransportEventOnce(Status(StatusCode::K_RUNTIME_ERROR, "wait failed"));
    BINEXPECT_CALL(&WorkerOcServiceMigrateImpl::ReplacePrimaryImpl, (_, _, _, _, _)).Times(0);
    BINEXPECT_CALL(&WorkerOcEvictionManager::Erase, (_)).Times(1).WillRepeatedly(Return());

    auto req = MakeReq({ { .objectKey = "obj1", .version = 1, .dataSize = defaultDataSize_, .withUrmaInfo = true } });

    MigrateDataDirectRspPb rsp;
    DS_ASSERT_NOT_OK(impl_->MigrateDataDirect(req, rsp));
    ASSERT_EQ(rsp.failed_object_keys_size(), 1);
    ASSERT_EQ(rsp.failed_object_keys(0), "obj1");
}

TEST_F(MigrateDataDirectTest, TestMigrateDataDirectPartialFail)
{
    EnableUrma(true);
    MockQueryMasterMetadataOk(1, defaultDataSize_);
    BINEXPECT_CALL(&WorkerOcEvictionManager::Erase, (_)).Times(1).WillRepeatedly(Return());

    int call = 0;
    constexpr int exceptCalls = 2;
    BINEXPECT_CALL(&datasystem::UrmaRead, (_, _, _, _, _, _, _))
        .Times(exceptCalls)
        .WillRepeatedly(
        Invoke([&call](const datasystem::UrmaRemoteAddrPb &, const uint64_t &, const uint64_t &, const uint64_t &,
                       const uint64_t &, const uint64_t &, std::vector<uint64_t> &keys) {
            if (call++ == 0) {
                keys = { 1 };
                return Status::OK();
            }
            keys.clear();
            return Status(StatusCode::K_RUNTIME_ERROR, "urma read failed");
        }));

    MockWaitFastTransportEventOnce(Status::OK());
    MockReplacePrimaryOk(1);

    auto req = MakeReq({
        { .objectKey = "obj1",
          .version = 1,
          .dataSize = defaultDataSize_,
          .withUrmaInfo = true,
          .segVa = defaultSegVa_ },
        { .objectKey = "obj2",
          .version = 1,
          .dataSize = defaultDataSize_,
          .withUrmaInfo = true,
          .segVa = secondSegVa_ },
    });

    MigrateDataDirectRspPb rsp;
    DS_ASSERT_OK(impl_->MigrateDataDirect(req, rsp));
    ASSERT_EQ(rsp.failed_object_keys_size(), 1);
}

TEST_F(MigrateDataDirectTest, TestMigrateDataDirectVersionMismatch)
{
    EnableUrma(true);
    uint64_t metaVersion = 2;
    MockQueryMasterMetadataOk(metaVersion, defaultDataSize_);

    BINEXPECT_CALL(&datasystem::UrmaRead, (_, _, _, _, _, _, _)).Times(0);
    BINEXPECT_CALL(&datasystem::WaitFastTransportEvent, (_, _, _)).Times(0);
    BINEXPECT_CALL(&WorkerOcServiceMigrateImpl::ReplacePrimaryImpl, (_, _, _, _, _)).Times(0);

    auto req = MakeReq({ { .objectKey = "obj1", .version = 1, .dataSize = defaultDataSize_ } });

    MigrateDataDirectRspPb rsp;
    DS_ASSERT_OK(impl_->MigrateDataDirect(req, rsp));
    ASSERT_EQ(rsp.failed_object_keys_size(), 0);
}

TEST_F(MigrateDataDirectTest, TestMigrateDataDirectUrmaReadFail)
{
    EnableUrma(true);
    BINEXPECT_CALL(&WorkerOcEvictionManager::Erase, (_)).WillRepeatedly(Return());

    MockQueryMasterMetadataOk(1, defaultDataSize_);
    MockUrmaReadReturnOnce(Status(StatusCode::K_RUNTIME_ERROR, "urma read failed"));
    BINEXPECT_CALL(&datasystem::WaitFastTransportEvent, (_, _, _)).Times(0);
    BINEXPECT_CALL(&WorkerOcServiceMigrateImpl::ReplacePrimaryImpl, (_, _, _, _, _)).Times(0);

    auto req = MakeReq({ { .objectKey = "obj1", .version = 1, .dataSize = defaultDataSize_ } });

    MigrateDataDirectRspPb rsp;
    ASSERT_EQ(impl_->MigrateDataDirect(req, rsp).GetCode(), StatusCode::K_RUNTIME_ERROR);
    ASSERT_EQ(rsp.failed_object_keys_size(), 1);
}

TEST_F(MigrateDataDirectTest, TestAggregateAllocateSmallObjects)
{
    EnableUrma(true);
    constexpr uint64_t smallSize = 256;
    constexpr int objCount = 3;
    MockQueryMasterMetadataOk(1, smallSize);

    MockUrmaReadOkTimes(objCount);
    MockWaitFastTransportEventOkTimes(objCount);
    MockReplacePrimaryOk(objCount);

    auto req = MakeReq({
        { .objectKey = "obj1", .version = 1, .dataSize = smallSize, .withUrmaInfo = true, .segVa = defaultSegVa_ },
        { .objectKey = "obj2", .version = 1, .dataSize = smallSize, .withUrmaInfo = true, .segVa = secondSegVa_ },
        { .objectKey = "obj3", .version = 1, .dataSize = smallSize, .withUrmaInfo = true, .segVa = defaultSegVa_ },
    });

    MigrateDataDirectRspPb rsp;
    DS_ASSERT_OK(impl_->MigrateDataDirect(req, rsp));
    ASSERT_EQ(rsp.failed_object_keys_size(), 0);
}

TEST_F(MigrateDataDirectTest, TestAggregateAllocateLargeObjects)
{
    EnableUrma(true);
    constexpr uint64_t largeSize = 2 * 1024UL * 1024UL; // 2 MB
    constexpr int objCount = 2;
    MockQueryMasterMetadataOk(1, largeSize);

    MockUrmaReadOkTimes(objCount);
    MockWaitFastTransportEventOkTimes(objCount);
    MockReplacePrimaryOk(objCount);

    auto req = MakeReq({
        { .objectKey = "obj1", .version = 1, .dataSize = largeSize, .withUrmaInfo = true, .segVa = defaultSegVa_ },
        { .objectKey = "obj2", .version = 1, .dataSize = largeSize, .withUrmaInfo = true, .segVa = secondSegVa_ },
    });

    MigrateDataDirectRspPb rsp;
    DS_ASSERT_OK(impl_->MigrateDataDirect(req, rsp));
    ASSERT_EQ(rsp.failed_object_keys_size(), 0);
}

TEST_F(MigrateDataDirectTest, TestAggregateAllocateMixedObjects)
{
    EnableUrma(true);
    constexpr uint64_t smallSize = 512;
    constexpr uint64_t largeSize = 2 * 1024UL * 1024UL; // 2 MB
    constexpr int objCount = 3;
    MockQueryMasterMetadataOk(1, smallSize);

    MockUrmaReadOkTimes(objCount);
    MockWaitFastTransportEventOkTimes(objCount);
    MockReplacePrimaryOk(objCount);

    auto req = MakeReq({
        { .objectKey = "obj1", .version = 1, .dataSize = smallSize, .withUrmaInfo = true, .segVa = defaultSegVa_ },
        { .objectKey = "obj2", .version = 1, .dataSize = largeSize, .withUrmaInfo = true, .segVa = secondSegVa_ },
        { .objectKey = "obj3", .version = 1, .dataSize = smallSize, .withUrmaInfo = true, .segVa = defaultSegVa_ },
    });

    MigrateDataDirectRspPb rsp;
    DS_ASSERT_OK(impl_->MigrateDataDirect(req, rsp));
    ASSERT_EQ(rsp.failed_object_keys_size(), 0);
}

}  // namespace ut
}  // namespace datasystem
