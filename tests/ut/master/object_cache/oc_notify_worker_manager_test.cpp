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
 * Description: Test notify worker manager class.
 */
#include "datasystem/master/object_cache/oc_notify_worker_manager.h"

#include <string>
#include <vector>

#include "common.h"
#include "../../../common/binmock/binmock.h"
#include "datasystem/worker/worker.h"
#include "datasystem/common/signal/signal.h"
#include "datasystem/master/object_cache/oc_metadata_manager.h"

DS_DECLARE_string(rocksdb_store_dir);

using namespace ::testing;
using namespace datasystem::master;
namespace datasystem {
namespace ut {
class OCNotifyWorkerManagerTest : public CommonTest {
public:
    void SetUp()
    {
        rocksStore_ = RocksStore::GetInstance(GetTestCaseDataDir() + "/rocksdb");
        objectStore_ = std::make_shared<ObjectMetaStore>(rocksStore_.get(), nullptr);
        objectStore_->Init();
        hostPort_.ParseString("127.0.0.1:30001");
        akSkManager_ = std::make_shared<AkSkManager>(0);
    }
    std::shared_ptr<RocksStore> rocksStore_;
    std::shared_ptr<ObjectMetaStore> objectStore_;
    HostPort hostPort_;
    std::shared_ptr<AkSkManager> akSkManager_;
};

TEST_F(OCNotifyWorkerManagerTest, DISABLED_TestAsyncSendUpdateObject)
{
    inject::Set("OCNotifyWorkerManager.CheckWorkerIsHealth.worker.unhealthy", "return(K_WORKER_ABNORMAL)");
    inject::Set("OCNotifyWorkerManager.NoNeedRecoveryMeta", "return(K_OK)");
    auto manager = std::make_unique<OCNotifyWorkerManager>(objectStore_, true, akSkManager_, nullptr, nullptr);
    EXPECT_EQ(manager->Init(), Status::OK());
    std::string worker1 = "127.0.0.1:40001";
    std::string worker2 = "127.0.0.1:40002";
    std::string worker3 = "127.0.0.1:40003";
    std::string objectKey = "test0001:127.0.0.1:30001";

    {
        ObjectMeta objectMeta;
        objectMeta.meta.set_object_key(objectKey);
        objectMeta.locations[worker2] = AckState::ACK;
        objectMeta.locations[worker3] = AckState::ACK;
        EXPECT_EQ(manager->AsyncSendUpdateObject(objectKey, worker1, objectMeta), Status::OK());
    }

    {
        ObjectMeta objectMeta;
        objectMeta.meta.set_object_key(objectKey);
        objectMeta.locations[worker1] = AckState::ACK;
        objectMeta.locations[worker2] = AckState::ACK;
        EXPECT_EQ(manager->AsyncSendUpdateObject(objectKey, worker3, objectMeta), Status::OK());
    }

    {
        std::vector<std::pair<std::string, std::string>> result;
        EXPECT_EQ(objectStore_->GetAllFromRocks(ASYNC_WORKER_OP_TABLE, result), Status::OK());
        EXPECT_EQ(result[0].first, worker1 + "_" + objectKey);
        EXPECT_EQ(result[1].first, worker2 + "_" + objectKey);
    }

    manager.reset();
    manager = std::make_unique<OCNotifyWorkerManager>(objectStore_, true, akSkManager_, nullptr, nullptr);
    EXPECT_EQ(manager->Init(), Status::OK());
    {
        ObjectMeta objectMeta;
        objectMeta.meta.set_object_key(objectKey);
        objectMeta.locations[worker1] = AckState::ACK;
        objectMeta.locations[worker3] = AckState::ACK;
        EXPECT_EQ(manager->AsyncSendUpdateObject(objectKey, worker2, objectMeta), Status::OK());
    }

    {
        std::vector<std::pair<std::string, std::string>> result;
        EXPECT_EQ(objectStore_->GetAllFromRocks(ASYNC_WORKER_OP_TABLE, result), Status::OK());
        EXPECT_EQ(result[0].first, worker1 + "_" + objectKey);
        EXPECT_EQ(result[1].first, worker3 + "_" + objectKey);
    }
}

TEST_F(OCNotifyWorkerManagerTest, TestChangePrimaryCopy)
{
    auto ocMetaManager = std::make_shared<master::OCMetadataManager>(akSkManager_, nullptr, nullptr, nullptr,
                                                                     "127.0.0.1:900", nullptr, "dbName");
    auto manager =
        std::make_unique<OCNotifyWorkerManager>(objectStore_, true, akSkManager_, nullptr, ocMetaManager.get());

    BINEXPECT_CALL(&OCNotifyWorkerManager::SendChangePrimaryCopy, (_, _, _)).WillRepeatedly(Return(Status::OK()));
    std::string newPrimaryCopy = "127.0.0.1:902";
    const int argumentIndex = 3;
    BINEXPECT_CALL(&OCMetadataManager::ReselectPrimaryCopy, (_, _, _, _))
        .WillRepeatedly(DoAll(SetArgReferee<argumentIndex>(newPrimaryCopy), Return(Status::OK())));

    std::thread t([] {
        const int timeoutMs = 100;
        std::this_thread::sleep_for(std::chrono::milliseconds(timeoutMs));
        SignalHandler(SIGTERM);
    });
    std::unordered_map<std::string, std::unordered_set<std::string>> input;
    input["127.0.0.1:901"].insert("key1");
    input["127.0.0.1:901"].insert("key2");
    manager->ProcessChangePrimaryCopy(input, false);
    t.join();
    RELEASE_STUBS
}
}  // namespace ut
}  // namespace datasystem
