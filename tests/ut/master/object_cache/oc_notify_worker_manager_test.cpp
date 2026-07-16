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

#include <algorithm>
#include <chrono>
#include <string>
#include <thread>
#include <vector>

#include "ut/common.h"
#include "../../../common/binmock/binmock.h"
#include "datasystem/common/signal/signal.h"
#include "datasystem/common/util/format.h"
#include "datasystem/master/object_cache/oc_metadata_manager.h"

DS_DECLARE_string(rocksdb_store_dir);
DS_DECLARE_string(rocksdb_write_mode);

using namespace ::testing;
using namespace datasystem::master;
namespace datasystem {
namespace master {
class OCNotifyWorkerManagerTest : public ut::CommonTest {
public:
    void SetUp()
    {
        rocksdbWriteMode_ = FLAGS_rocksdb_write_mode;
        FLAGS_rocksdb_write_mode = "sync";
        rocksStore_ = RocksStore::GetInstance(ut::GetTestCaseDataDir() + "/rocksdb");
        objectStore_ = std::make_shared<ObjectMetaStore>(rocksStore_.get(), nullptr);
        objectStore_->Init();
        hostPort_.ParseString("127.0.0.1:30001");
        akSkManager_ = std::make_shared<AkSkManager>(0);
    }

    void TearDown() override
    {
        (void)inject::Clear("master.rocksdb.put");
        objectStore_.reset();
        rocksStore_.reset();
        FLAGS_rocksdb_write_mode = rocksdbWriteMode_;
    }

    std::shared_ptr<RocksStore> rocksStore_;
    std::shared_ptr<ObjectMetaStore> objectStore_;
    HostPort hostPort_;
    std::shared_ptr<AkSkManager> akSkManager_;
    std::string rocksdbWriteMode_;

    std::vector<OCNotifyWorkerManager::AsyncWorkerOpSnapshot> SnapshotWorkerOps(OCNotifyWorkerManager &manager,
                                                                                const std::string &worker)
    {
        return manager.SnapshotAsyncWorkerOps(worker);
    }

    Status ClearSnapshotOps(OCNotifyWorkerManager &manager, const std::string &worker,
                            const std::vector<OCNotifyWorkerManager::AsyncWorkerOpSnapshot> &snapshots)
    {
        return manager.ClearAsyncWorkerOpSnapshots(worker, snapshots);
    }
};

TEST_F(OCNotifyWorkerManagerTest, DISABLED_TestAsyncSendUpdateObject)
{
    inject::Set("OCNotifyWorkerManager.CheckWorkerIsHealth.worker.unhealthy", "return(K_WORKER_ABNORMAL)");
    inject::Set("OCNotifyWorkerManager.NoNeedRecoveryMeta", "return(K_OK)");
    auto manager = std::make_unique<OCNotifyWorkerManager>(objectStore_, true, akSkManager_, nullptr);
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
    manager = std::make_unique<OCNotifyWorkerManager>(objectStore_, true, akSkManager_, nullptr);
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

TEST_F(OCNotifyWorkerManagerTest, TestInsertAsyncWorkerOpReleasesTableLockBeforePersistence)
{
    auto manager = std::make_unique<OCNotifyWorkerManager>(objectStore_, true, akSkManager_, nullptr);
    const std::string worker = "127.0.0.1:40001";
    const std::string objectKey = "test_insert_async_worker_op";
    NotifyWorkerOp op = { .type = NotifyWorkerOpType::CACHE_INVALID };
    constexpr int rocksPutSleepMs = 300;
    constexpr int maxExpectedCheckMs = 100;
    constexpr int pollIntervalMs = 5;
    constexpr int maxPollMs = 1000;

    DS_ASSERT_OK(inject::Set("master.rocksdb.put", FormatString("1*sleep(%d)", rocksPutSleepMs)));
    Status insertRc;
    std::thread insertThread([&] { insertRc = manager->InsertAsyncWorkerOp(worker, objectKey, op); });

    bool observedPendingOp = false;
    int64_t maxCheckMs = 0;
    auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(maxPollMs);
    while (std::chrono::steady_clock::now() < deadline) {
        auto start = std::chrono::steady_clock::now();
        bool exists = manager->CheckExistAsyncWorkerOp(worker, objectKey, NotifyWorkerOpType::CACHE_INVALID);
        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start);
        maxCheckMs = std::max(maxCheckMs, static_cast<int64_t>(elapsed.count()));
        if (exists) {
            observedPendingOp = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(pollIntervalMs));
    }

    insertThread.join();
    DS_ASSERT_OK(inject::Clear("master.rocksdb.put"));
    DS_ASSERT_OK(insertRc);
    ASSERT_TRUE(observedPendingOp);
    EXPECT_LT(maxCheckMs, maxExpectedCheckMs);
}

TEST_F(OCNotifyWorkerManagerTest, TestSnapshotClearKeepsNewerAsyncWorkerOp)
{
    auto manager = std::make_unique<OCNotifyWorkerManager>(objectStore_, true, akSkManager_, nullptr);
    const std::string worker = "127.0.0.1:40001";
    const std::string clearedObjectKey = "snapshot_clear_old_op";
    const std::string newerObjectKey = "snapshot_clear_newer_op";
    NotifyWorkerOp op = { .type = NotifyWorkerOpType::CACHE_INVALID };

    DS_ASSERT_OK(manager->InsertAsyncWorkerOp(worker, clearedObjectKey, op));
    DS_ASSERT_OK(manager->InsertAsyncWorkerOp(worker, newerObjectKey, op));
    auto snapshots = SnapshotWorkerOps(*manager, worker);

    DS_ASSERT_OK(manager->InsertAsyncWorkerOp(worker, newerObjectKey, op));
    DS_ASSERT_OK(ClearSnapshotOps(*manager, worker, snapshots));

    ASSERT_FALSE(manager->CheckExistAsyncWorkerOp(worker, clearedObjectKey, NotifyWorkerOpType::CACHE_INVALID));
    ASSERT_TRUE(manager->CheckExistAsyncWorkerOp(worker, newerObjectKey, NotifyWorkerOpType::CACHE_INVALID));
}

TEST_F(OCNotifyWorkerManagerTest, TestChangePrimaryCopy)
{
    auto ocMetaManager = std::make_shared<OCMetadataManager>(
        akSkManager_, nullptr, nullptr, nullptr, "127.0.0.1:900", nullptr, nullptr, false, HostPort(), "", nullptr,
        "workerId");
    auto manager = std::make_unique<OCNotifyWorkerManager>(objectStore_, true, akSkManager_, ocMetaManager.get());

    BINEXPECT_CALL(&OCNotifyWorkerManager::SendChangePrimaryCopy, (_, _, _)).WillRepeatedly(Return(Status::OK()));
    std::string newPrimaryCopy = "127.0.0.1:902";
    const int argumentIndex = 3;
    BINEXPECT_CALL(&OCMetadataManager::ReselectPrimaryCopy, (_, _, _, _))
        .WillRepeatedly(DoAll(SetArgReferee<argumentIndex>(newPrimaryCopy), Return(Status::OK())));

    std::thread t([] {
        const int timeoutMs = 100;
        std::this_thread::sleep_for(std::chrono::milliseconds(timeoutMs));
        datasystem::g_exitFlag = 1;
        datasystem::g_termSignalCv.notify_all();
    });
    std::unordered_map<std::string, std::unordered_set<std::string>> input;
    input["127.0.0.1:901"].insert("key1");
    input["127.0.0.1:901"].insert("key2");
    manager->ProcessChangePrimaryCopy(input, false);
    t.join();
    RELEASE_STUBS
}
}  // namespace master
}  // namespace datasystem
