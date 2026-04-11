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
 * Description: Test AsyncUpdateLocationManager.
 */
#include "ut/common.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/master/object_cache/oc_metadata_manager.h"
#include "datasystem/worker/object_cache/async_update_location_manager.h"

using namespace datasystem::object_cache;

namespace datasystem {
namespace ut {
class AsyncUpdateLocationTest : public CommonTest {
public:
    ~AsyncUpdateLocationTest() = default;
    void SetUp()
    {
        rocksStore_ = RocksStore::GetInstance(GetTestCaseDataDir() + "/rocksdb");
        objectStore_ = std::make_shared<master::ObjectMetaStore>(rocksStore_.get(), nullptr);
        objectStore_->Init();
        hostPort_.ParseString("127.0.0.1:30001");
        akSkManager_ = std::make_shared<AkSkManager>(0);
    }
    static void AsyncUpdateLocationFunc(UpdateLocationTask &&task)
    {
        LOG(INFO) << "start update location, version is " << task.GetFirstObjectKey();
    };

    std::shared_ptr<RocksStore> rocksStore_;
    std::shared_ptr<master::ObjectMetaStore> objectStore_;
    HostPort hostPort_;
    std::shared_ptr<AkSkManager> akSkManager_;
};

class OCMetadataManagerHelper : public master::OCMetadataManager {
public:
    using OCMetadataManager::OCMetadataManager; // Inherit base class constructors

    Status CallRecoverObjectLocations(
        const std::unordered_map<std::string, std::vector<std::pair<std::string, master::AckState>>> &objLocMap)
    {
        return RecoverObjectLocations(objLocMap);
    }

    Status CallLoadObjectLocations(bool isFromRocksdb,
        std::unordered_map<std::string, std::vector<std::pair<std::string, master::AckState>>> &objLocMap)
    {
        return LoadObjectLocations(isFromRocksdb, objLocMap);
    }

    void InsertObjectKey(const std::string &objectKey)
    {
        master::TbbMetaTable::accessor accessor;
        std::unique_lock<std::shared_timed_mutex> lck(metaTableMutex_);
        (void)metaTable_.insert(accessor, objectKey);
    }

    Status GetObjectKeyAckState(const std::string &objectKey, const std::string &workerAddress,
                                    master::AckState &ackState)
    {
        master::TbbMetaTable::accessor accessor;
        LOG(INFO) << "get the object key " << objectKey;
        std::shared_lock<std::shared_timed_mutex> lck(metaTableMutex_);
        auto found = metaTable_.find(accessor, objectKey);
        if (!found) {
            RETURN_STATUS(K_NOT_FOUND, "not found the object key");
        }
        auto it = accessor->second.locations.find(workerAddress);
        if (it == accessor->second.locations.end()) {
            RETURN_STATUS(K_NOT_FOUND, "not found the worker addr");
        }
        ackState = it->second;
        return Status::OK();
    }
};

TEST_F(AsyncUpdateLocationTest, TestAddTask)
{
    AsyncUpdateLocationManager manager = AsyncUpdateLocationManager();
    manager.Init(AsyncUpdateLocationFunc);
    std::string objectKey = "test_object_key";
    UpdateLocationParam param = {objectKey, 1, 1};
    UpdateLocationTask task = UpdateLocationTask(param);
    Status status = manager.AddTask(std::move(task));
    ASSERT_TRUE(status.IsOk());

    const int maxVersion = 5;
    const int loopCount = 3;
    std::vector<std::thread> threads;
    threads.reserve(maxVersion);
    for (int i = 0; i < maxVersion; i++) {
        threads.emplace_back([&manager, objectKey, i]() {
        for (int j = 0; j < loopCount; ++j) {
            UpdateLocationParam param = {objectKey+std::to_string(i), static_cast<uint64_t>(i) + 1, 1};
            UpdateLocationTask task = UpdateLocationTask(param);
            Status status = manager.AddTask(std::move(task));
            ASSERT_TRUE(status.IsOk());
        }
        });
    }

    for (auto &thread : threads) {
        thread.join();
    }
    std::this_thread::sleep_for(std::chrono::seconds(1));
}

TEST_F(AsyncUpdateLocationTest, AddAndLoadLocations)
{
    auto ocMetaManager = std::make_shared<OCMetadataManagerHelper>(akSkManager_, rocksStore_.get(), nullptr, nullptr,
                                                                     "127.0.0.1:900", nullptr, "locationDbName", false);
    DS_ASSERT_OK(objectStore_->Init());
    std::string key1 = "test_object_key1";
    std::string key2 = "test_object_key2";
    std::string key3 = "test_object_key3";
    std::string worker1 = "127.0.0.1:2001";
    std::string worker2 = "127.0.0.1:2002";
    std::unordered_map<std::string, std::vector<std::pair<std::string, master::AckState>>> objLocMap;

    // validate the load location with AddObjectLocations and UNACK
    std::unordered_map<std::string, std::string> keyLocations = {{key1, worker1}, {key2, worker1}};
    objectStore_->AddObjectLocations(keyLocations, "0");
    inject::Set("OCMetadataManager.GetValidWorkersInHashRing", "return(127.0.0.1:2001)"); // return worker1
    std::this_thread::sleep_for(std::chrono::seconds(1));
    ocMetaManager->CallLoadObjectLocations(true, objLocMap);
    ocMetaManager->InsertObjectKey(key1);
    ocMetaManager->InsertObjectKey(key2);
    ocMetaManager->CallRecoverObjectLocations(objLocMap);
    master::AckState ackState1;
    DS_ASSERT_OK(ocMetaManager->GetObjectKeyAckState(key1, worker1, ackState1));
    ASSERT_TRUE(ackState1 == master::AckState::UNACK);
    master::AckState ackState2;
    DS_ASSERT_OK(ocMetaManager->GetObjectKeyAckState(key2, worker1, ackState2));
    ASSERT_TRUE(ackState2 == master::AckState::UNACK);

    // validate the load location with AddObjectLocation and ACK,
    // this can cover the old version location stored in the rocksdb.
    objLocMap.clear();
    inject::Set("OCMetadataManager.GetValidWorkersInHashRing", "return(127.0.0.1:2002)"); // return worker2
    objectStore_->AddObjectLocation(key3, worker2, "");
    std::this_thread::sleep_for(std::chrono::seconds(1));
    ocMetaManager->CallLoadObjectLocations(true, objLocMap);
    ocMetaManager->InsertObjectKey(key3);
    ocMetaManager->CallRecoverObjectLocations(objLocMap);
    master::AckState ackState3;
    DS_ASSERT_OK(ocMetaManager->GetObjectKeyAckState(key3, worker2, ackState3));
    ASSERT_TRUE(ackState3 == master::AckState::ACK);
}
}  // namespace ut
}  // namespace datasystem