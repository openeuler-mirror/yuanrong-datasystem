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

/**
 * Description: Unit tests for metadata recovery manager.
 */
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "common.h"
#include "../../../common/binmock/binmock.h"
#include "datasystem/master/meta_addr_info.h"
#define private public
#include "datasystem/worker/object_cache/metadata_recovery_manager.h"
#undef private
#include "datasystem/worker/object_cache/obj_cache_shm_unit.h"
#include "datasystem/worker/object_cache/worker_master_oc_api.h"

using namespace ::testing;
using namespace datasystem::object_cache;

namespace datasystem {
namespace ut {
class TestWorkerMasterApiManager : public worker::WorkerMasterApiManagerBase<worker::WorkerMasterOCApi> {
public:
    explicit TestWorkerMasterApiManager(HostPort &workerAddr)
        : WorkerMasterApiManagerBase<worker::WorkerMasterOCApi>(workerAddr, nullptr)
    {
    }

    std::shared_ptr<worker::WorkerMasterOCApi> CreateWorkerMasterApi(const HostPort &masterAddress) override
    {
        auto iter = apiByAddr_.find(masterAddress.ToString());
        return iter == apiByAddr_.end() ? nullptr : iter->second;
    }

    std::shared_ptr<worker::WorkerMasterOCApi> GetWorkerMasterApi(const HostPort &masterAddress) override
    {
        auto iter = apiByAddr_.find(masterAddress.ToString());
        return iter == apiByAddr_.end() ? nullptr : iter->second;
    }

    void SetApi(const HostPort &masterAddress, const std::shared_ptr<worker::WorkerMasterOCApi> &api)
    {
        apiByAddr_[masterAddress.ToString()] = api;
    }

private:
    std::unordered_map<std::string, std::shared_ptr<worker::WorkerMasterOCApi>> apiByAddr_;
};

class TestWorkerMasterOCApi : public worker::WorkerRemoteMasterOCApi {
public:
    TestWorkerMasterOCApi(const HostPort &masterAddr, const HostPort &localAddr)
        : WorkerRemoteMasterOCApi(masterAddr, localAddr, nullptr)
    {
    }

    Status PushMetadataToMaster(master::PushMetaToMasterReqPb &req, master::PushMetaToMasterRspPb &rsp) override
    {
        (void)rsp;
        for (const auto &meta : req.metas()) {
            isRecoveredFlags_.emplace_back(meta.is_recovered());
        }
        batchSizes_.emplace_back(req.metas_size());
        return returnStatus_;
    }

    const std::vector<int> &GetBatchSizes() const
    {
        return batchSizes_;
    }

    const std::vector<bool> &GetIsRecoveredFlags() const
    {
        return isRecoveredFlags_;
    }

private:
    Status returnStatus_{ Status::OK() };
    std::vector<int> batchSizes_;
    std::vector<bool> isRecoveredFlags_;
};

class MetaDataRecoveryManagerTest : public CommonTest {
public:
    void SetUp() override
    {
        CommonTest::SetUp();

        localAddress_ = HostPort("127.0.0.1:18500");
        objectTable_ = std::make_shared<ObjectTable>();
        workerMasterApiManager_ = std::make_shared<TestWorkerMasterApiManager>(localAddress_);
        manager_ = std::make_unique<MetaDataRecoveryManager>(localAddress_, objectTable_, nullptr,
                                                             workerMasterApiManager_);
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
        objectTable_->Insert(objectKey, std::move(obj));
    }

protected:
    HostPort localAddress_;
    std::shared_ptr<ObjectTable> objectTable_;
    std::shared_ptr<TestWorkerMasterApiManager> workerMasterApiManager_;
    std::unique_ptr<MetaDataRecoveryManager> manager_;
};

TEST_F(MetaDataRecoveryManagerTest, RecoverMetadataBatchSizeShouldNotExceed500)
{
    BINEXPECT_CALL((Status (EtcdClusterManager::*)(const HostPort &, bool, bool)) & EtcdClusterManager::CheckConnection,
                   (_, _, _))
        .WillRepeatedly(Return(Status::OK()));

    constexpr size_t totalObjects = 1201;
    std::vector<std::string> objectKeys;
    objectKeys.reserve(totalObjects);
    for (size_t i = 0; i < totalObjects; ++i) {
        auto objectKey = "recovery_obj_" + std::to_string(i);
        AddObject(objectKey);
        objectKeys.emplace_back(std::move(objectKey));
    }

    HostPort masterAddr("127.0.0.1:18501");
    auto workerMasterApi = std::make_shared<TestWorkerMasterOCApi>(masterAddr, localAddress_);
    workerMasterApiManager_->SetApi(masterAddr, workerMasterApi);

    auto result = manager_->SendRecoverRequest(MetaAddrInfo(masterAddr, ""), objectKeys);
    DS_ASSERT_OK(result.status);
    EXPECT_TRUE(result.failedIds.empty());

    const auto &batchSizes = workerMasterApi->GetBatchSizes();
    ASSERT_THAT(batchSizes, ElementsAre(500, 500, 201));
    for (const auto isRecovered : workerMasterApi->GetIsRecoveredFlags()) {
        EXPECT_TRUE(isRecovered);
    }
}

TEST_F(MetaDataRecoveryManagerTest, RecoverMetadataShouldReturnAllFailedIdsWhenMasterUnreachable)
{
    std::vector<std::string> objectKeys{ "obj_failed_1", "obj_failed_2", "obj_failed_3" };
    for (const auto &objectKey : objectKeys) {
        AddObject(objectKey);
    }

    HostPort unreachableMaster("127.0.0.1:18502");
    auto result = manager_->SendRecoverRequest(MetaAddrInfo(unreachableMaster, ""), objectKeys);
    DS_ASSERT_NOT_OK(result.status);
    EXPECT_EQ(result.status.GetCode(), K_RUNTIME_ERROR);
    EXPECT_EQ(result.failedIds.size(), objectKeys.size());
}
}  // namespace ut
}  // namespace datasystem
