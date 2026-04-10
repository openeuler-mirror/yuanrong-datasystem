/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
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
 * Description: Test WorkerOcServiceImpl.
 */

#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <gmock/gmock.h>

#include "ut/common.h"
#include "../../../common/binmock/binmock.h"
#include "datasystem/common/object_cache/safe_table.h"
#include "datasystem/protos/worker_object.pb.h"
#include "datasystem/worker/cluster_event_type.h"
#include "datasystem/worker/object_cache/obj_cache_shm_unit.h"
#define private public
#include "datasystem/worker/object_cache/worker_oc_service_impl.h"
#undef private

using namespace ::testing;

using namespace datasystem::object_cache;

namespace datasystem {
namespace ut {
class WorkerOcServiceImplTest : public CommonTest {
public:
    void SetUp() override
    {
        CommonTest::SetUp();
        Init();
    }

    void Init()
    {
        objectTable_ = std::make_shared<object_cache::ObjectTable>();
        globalRefTable_ = std::make_shared<ObjectGlobalRefTable<ClientKey>>();
        localAddress_ = HostPort("127.0.0.1:18481");
        evictionManager_ =
            std::make_shared<WorkerOcEvictionManager>(objectTable_, localAddress_, localAddress_, nullptr);
        WorkerOcServiceCrudParam param{
            .workerMasterApiManager = nullptr,
            .workerRequestManager = requestManager_,
            .memoryRefTable = nullptr,
            .objectTable = objectTable_,
            .evictionManager = evictionManager_,
            .workerDevOcManager = nullptr,
            .asyncPersistenceDelManager = nullptr,
            .asyncSendManager = nullptr,
            .asyncRollbackManager = nullptr,
            .metadataSize = 0,
            .persistenceApi = nullptr,
            .etcdCM = nullptr,
        };
        deleteProc_ = std::make_shared<WorkerOcServiceDeleteImpl>(param, nullptr, nullptr, localAddress_, nullptr);
        gRefProc_ =
            std::make_shared<WorkerOcServiceGlobalReferenceImpl>(param, nullptr, globalRefTable_, nullptr, localAddress_);
        impl_ = std::make_shared<WorkerOCServiceImpl>(localAddress_, localAddress_, objectTable_, nullptr,
                                                      evictionManager_,
                                                      nullptr, nullptr, nullptr);
        dataClearImpl_ = std::make_shared<WorkerOcServiceClearDataFlow>(
            objectTable_, globalRefTable_, nullptr, gRefProc_, deleteProc_, nullptr, nullptr,
            localAddress_.ToString());
        impl_->InitServiceImpl();
    }

    void TearDown() override
    {
        RecoverMasterAppRefEvent::GetInstance().RemoveSubscriber(kRecoverMasterAppRefSubscriber);
        dataClearImpl_.reset();
        deleteProc_.reset();
        gRefProc_.reset();
        impl_.reset();
        evictionManager_.reset();
        CommonTest::TearDown();
    }

    void AddObject(const std::string &objectKey, uint64_t version = 1, uint64_t dataSize = 1024)
    {
        auto obj = std::make_unique<ObjCacheShmUnit>();
        obj->SetDataSize(dataSize);
        obj->SetCreateTime(version);
        obj->SetLifeState(ObjectLifeState::OBJECT_SEALED);
        obj->modeInfo.SetWriteMode(WriteMode::NONE_L2_CACHE);
        obj->stateInfo.SetDataFormat(DataFormat::BINARY);
        obj->stateInfo.SetPrimaryCopy(true);
        DS_ASSERT_OK(objectTable_->Insert(objectKey, std::move(obj)));
    }

    void AddWorkerRef(const std::string &objectKey, const std::string &clientId = "client-id")
    {
        std::vector<std::string> objectKeys{ objectKey };
        std::vector<std::string> failIncIds;
        std::vector<std::string> firstIncIds;
        DS_ASSERT_OK(globalRefTable_->GIncreaseRef(ClientKey::Intern(clientId), objectKeys, failIncIds, firstIncIds));
        ASSERT_TRUE(failIncIds.empty());
    }

protected:
    static constexpr const char *kRecoverMasterAppRefSubscriber = "WorkerOcServiceImplTest.RecoverMasterAppRef";

    HostPort localAddress_;
    std::shared_ptr<ObjectTable> objectTable_;
    std::shared_ptr<ObjectGlobalRefTable<ClientKey>> globalRefTable_;
    std::shared_ptr<WorkerOcEvictionManager> evictionManager_;
    WorkerRequestManager requestManager_;
    std::shared_ptr<WorkerOCServiceImpl> impl_;
    std::shared_ptr<WorkerOcServiceGlobalReferenceImpl> gRefProc_;
    std::shared_ptr<WorkerOcServiceDeleteImpl> deleteProc_;
    std::shared_ptr<WorkerOcServiceClearDataFlow> dataClearImpl_;
};

TEST_F(WorkerOcServiceImplTest, TestParallelClearData)
{
    std::vector<std::thread> threads;
    int threadCount = 5;
    int batchCount = 100;

    std::vector<std::string> objKeys{ "key1", "key2" };
    for (int i = 0; i < threadCount; i++) {
        threads.emplace_back([this, &objKeys, batchCount] {
            for (int n = 0; n < batchCount; n++) {
                dataClearImpl_->ClearObject(objKeys);
            }
        });
    }
    for (auto &t : threads) {
        t.join();
    }
    for (const auto &id : objKeys) {
        std::shared_ptr<SafeObjType> entry;
        auto rc = objectTable_->Get(id, entry);
        ASSERT_EQ(rc.GetCode(), K_NOT_FOUND);
    }
}

TEST_F(WorkerOcServiceImplTest, ClearDataImplDispatchesMatchedObjectsToClearAndRebuild)
{
    using GetMatchObjectIdsMethod = Status (WorkerOcServiceClearDataFlow::*)(const ClearDataReqPb &,
                                                                             std::vector<std::string> &);
    using ClearMatchedObjectsMethod =
        void (WorkerOcServiceClearDataFlow::*)(const std::vector<std::string> &, WorkerOcServiceClearDataFlow::ClearDataRetryIds &);
    using RebuildRefForMatchedObjectsMethod =
        void (WorkerOcServiceClearDataFlow::*)(const std::vector<std::string> &, WorkerOcServiceClearDataFlow::ClearDataRetryIds &);

    std::vector<std::string> matchObjIds{ "obj1", "obj2" };
    std::vector<std::string> clearObjIds;
    std::vector<std::string> rebuildObjIds;
    BINEXPECT_CALL((GetMatchObjectIdsMethod) & WorkerOcServiceClearDataFlow::GetMatchObjectIds, (_, _))
        .WillOnce(Invoke([&matchObjIds](const ClearDataReqPb &, std::vector<std::string> &outObjIds) {
            outObjIds = matchObjIds;
            return Status::OK();
        }));
    BINEXPECT_CALL((ClearMatchedObjectsMethod) & WorkerOcServiceClearDataFlow::ClearMatchedObjects, (_, _))
        .WillOnce(Invoke([&clearObjIds](const std::vector<std::string> &objIds,
                                        WorkerOcServiceClearDataFlow::ClearDataRetryIds &) { clearObjIds = objIds; }));
    BINEXPECT_CALL((RebuildRefForMatchedObjectsMethod) & WorkerOcServiceClearDataFlow::RebuildRefForMatchedObjects,
                   (_, _))
        .WillOnce(Invoke([&rebuildObjIds](const std::vector<std::string> &objIds,
                                          WorkerOcServiceClearDataFlow::ClearDataRetryIds &) {
            rebuildObjIds = objIds;
        }));

    WorkerOcServiceClearDataFlow::ClearDataRetryIds retryIds;
    ClearDataReqPb req;
    DS_ASSERT_OK(dataClearImpl_->ClearDataImpl(req, retryIds));
    EXPECT_THAT(clearObjIds, ElementsAreArray(matchObjIds));
    EXPECT_THAT(rebuildObjIds, ElementsAreArray(matchObjIds));
    EXPECT_TRUE(retryIds.Empty());
}

TEST_F(WorkerOcServiceImplTest, ClearDataImplReturnsWhenSelectObjectsFailed)
{
    using GetMatchObjectIdsMethod = Status (WorkerOcServiceClearDataFlow::*)(const ClearDataReqPb &,
                                                                             std::vector<std::string> &);
    using ClearMatchedObjectsMethod =
        void (WorkerOcServiceClearDataFlow::*)(const std::vector<std::string> &, WorkerOcServiceClearDataFlow::ClearDataRetryIds &);
    using RebuildRefForMatchedObjectsMethod =
        void (WorkerOcServiceClearDataFlow::*)(const std::vector<std::string> &, WorkerOcServiceClearDataFlow::ClearDataRetryIds &);

    Status selectFailed(StatusCode::K_RUNTIME_ERROR, "select failed");
    BINEXPECT_CALL((GetMatchObjectIdsMethod) & WorkerOcServiceClearDataFlow::GetMatchObjectIds, (_, _))
        .WillOnce(Return(selectFailed));
    BINEXPECT_CALL((ClearMatchedObjectsMethod) & WorkerOcServiceClearDataFlow::ClearMatchedObjects, (_, _)).Times(0);
    BINEXPECT_CALL((RebuildRefForMatchedObjectsMethod) & WorkerOcServiceClearDataFlow::RebuildRefForMatchedObjects,
                   (_, _))
        .Times(0);

    WorkerOcServiceClearDataFlow::ClearDataRetryIds retryIds;
    ClearDataReqPb req;
    auto rc = dataClearImpl_->ClearDataImpl(req, retryIds);
    EXPECT_EQ(rc.GetCode(), selectFailed.GetCode());
    EXPECT_EQ(rc.GetMsg(), selectFailed.GetMsg());
    EXPECT_TRUE(retryIds.Empty());
}

TEST_F(WorkerOcServiceImplTest, RebuildRefForMatchedObjectsShouldCollectRetryIds)
{
    using IncreaseMasterRefMethod = Status (WorkerOcServiceGlobalReferenceImpl::*)(
        std::function<bool(const std::string &)>, std::vector<std::string> &);

    AddObject("obj1");
    AddObject("obj2");
    AddObject("obj3");
    AddObject("obj4");
    AddWorkerRef("obj1", "client-1");
    AddWorkerRef("obj2", "client-2");
    AddWorkerRef("obj3", "client-3");

    RecoverMasterAppRefEvent::GetInstance().AddSubscriber(
        kRecoverMasterAppRefSubscriber,
        [](std::function<bool(const std::string &)> matchFunc, const std::string &) {
            EXPECT_TRUE(matchFunc("obj1"));
            EXPECT_FALSE(matchFunc("obj2"));
            EXPECT_TRUE(matchFunc("obj3"));
            EXPECT_FALSE(matchFunc("obj4"));
            return Status(StatusCode::K_RUNTIME_ERROR, "recover failed");
        });
    BINEXPECT_CALL((IncreaseMasterRefMethod) & WorkerOcServiceGlobalReferenceImpl::GIncreaseMasterRefWithLock, (_, _))
        .WillOnce(Invoke([](std::function<bool(const std::string &)> matchFunc, std::vector<std::string> &failedIds) {
            EXPECT_TRUE(matchFunc("obj1"));
            EXPECT_TRUE(matchFunc("obj2"));
            EXPECT_TRUE(matchFunc("obj3"));
            EXPECT_FALSE(matchFunc("obj4"));
            failedIds = { "obj2" };
            return Status(StatusCode::K_RUNTIME_ERROR, "increase failed");
        }));

    WorkerOcServiceClearDataFlow::ClearDataRetryIds retryIds;
    dataClearImpl_->RebuildRefForMatchedObjects({ "obj1", "obj2", "obj3", "obj4" }, retryIds);

    EXPECT_THAT(retryIds.increaseFailedIds, UnorderedElementsAre("obj2"));
    EXPECT_THAT(retryIds.recoverAppRefFailedIds, UnorderedElementsAre("obj1", "obj3"));
}

TEST_F(WorkerOcServiceImplTest, ClearDataRetryImplShouldRouteFailedIdsToRetryStages)
{
    using ClearMatchedObjectsMethod =
        void (WorkerOcServiceClearDataFlow::*)(const std::vector<std::string> &, WorkerOcServiceClearDataFlow::ClearDataRetryIds &);
    using RetryIncreaseMasterRefMethod =
        void (WorkerOcServiceClearDataFlow::*)(const std::vector<std::string> &, WorkerOcServiceClearDataFlow::ClearDataRetryIds &);
    using RetryRecoverMasterAppRefMethod =
        void (WorkerOcServiceClearDataFlow::*)(const std::vector<std::string> &, WorkerOcServiceClearDataFlow::ClearDataRetryIds &);

    std::vector<std::string> clearObjIds;
    std::vector<std::string> increaseObjIds;
    std::vector<std::string> recoverObjIds;
    BINEXPECT_CALL((ClearMatchedObjectsMethod) & WorkerOcServiceClearDataFlow::ClearMatchedObjects, (_, _))
        .WillOnce(Invoke([&clearObjIds](const std::vector<std::string> &objIds,
                                        WorkerOcServiceClearDataFlow::ClearDataRetryIds &retryIds) {
            clearObjIds = objIds;
            retryIds.clearFailedIds.emplace("clear-next");
        }));
    BINEXPECT_CALL((RetryIncreaseMasterRefMethod) & WorkerOcServiceClearDataFlow::RetryIncreaseMasterRef, (_, _))
        .WillOnce(Invoke([&increaseObjIds](const std::vector<std::string> &objIds,
                                           WorkerOcServiceClearDataFlow::ClearDataRetryIds &retryIds) {
            increaseObjIds = objIds;
            retryIds.increaseFailedIds.emplace("increase-next");
        }));
    BINEXPECT_CALL((RetryRecoverMasterAppRefMethod) &
                       WorkerOcServiceClearDataFlow::RetryRecoverMasterAppRef,
                   (_, _))
        .WillOnce(Invoke([&recoverObjIds](const std::vector<std::string> &objIds,
                                          WorkerOcServiceClearDataFlow::ClearDataRetryIds &retryIds) {
            recoverObjIds = objIds;
            retryIds.recoverAppRefFailedIds.emplace("recover-next");
        }));

    WorkerOcServiceClearDataFlow::ClearDataRetryIds retryIds;
    retryIds.clearFailedIds = { "clear-1", "clear-2" };
    retryIds.increaseFailedIds = { "increase-1" };
    retryIds.recoverAppRefFailedIds = { "recover-1", "recover-2" };

    WorkerOcServiceClearDataFlow::ClearDataRetryIds nextRetryIds;
    ClearDataReqPb req;
    dataClearImpl_->ClearDataRetryImpl(req, retryIds, nextRetryIds);

    EXPECT_THAT(clearObjIds, UnorderedElementsAre("clear-1", "clear-2"));
    EXPECT_THAT(increaseObjIds, UnorderedElementsAre("increase-1"));
    EXPECT_THAT(recoverObjIds, UnorderedElementsAre("recover-1", "recover-2"));
    EXPECT_THAT(nextRetryIds.clearFailedIds, UnorderedElementsAre("clear-next"));
    EXPECT_THAT(nextRetryIds.increaseFailedIds, UnorderedElementsAre("increase-next"));
    EXPECT_THAT(nextRetryIds.recoverAppRefFailedIds, UnorderedElementsAre("recover-next"));
}

}  // namespace ut
}  // namespace datasystem
