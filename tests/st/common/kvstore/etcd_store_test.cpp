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
 * Description: Test interface to ETCD
 */
#include <cassert>
#include <string>
#include <thread>
#include <unordered_map>
#include <sstream>
#include <vector>

#include "gtest/gtest.h"

#include "common.h"
#include "datasystem/common/encrypt/secret_manager.h"
#include "datasystem/common/eventloop/timer_queue.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/kvstore/etcd/etcd_store.h"
#include "datasystem/common/kvstore/etcd/etcd_watch.h"
#include "datasystem/common/kvstore/etcd/grpc_session.h"
#include "datasystem/common/util/ssl_authorization.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/util/random_data.h"
#include "datasystem/object_client.h"
#include "datasystem/object/object_enum.h"
#include "datasystem/utils/sensitive_value.h"
#include "etcd/api/mvccpb/kv.pb.h"

using namespace datasystem;

DS_DECLARE_string(etcd_address);
DS_DECLARE_string(az_name);
DS_DECLARE_bool(enable_etcd_auth);
DS_DECLARE_string(etcd_target_name_override);
DS_DECLARE_string(encrypt_kit);
DS_DECLARE_string(etcd_passphrase_path);

namespace datasystem {
namespace st {
class EtcdStoreTest : public ExternalClusterTest {
protected:
    EtcdStoreTest() : db_(nullptr), tableCreated_(false)
    {
    }
    ~EtcdStoreTest() = default;

    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numEtcd = 1;
        opts.numMasters = 0;
        opts.numWorkers = 0;
    }

    void InitTestEtcdInstance()
    {
        TimerQueue::GetInstance()->Initialize();
        std::string etcdAddress;
        for (size_t i = 0; i < cluster_->GetEtcdNum(); ++i) {
            std::pair<HostPort, HostPort> addrs;
            cluster_->GetEtcdAddrs(i, addrs);

            if (!etcdAddress.empty()) {
                etcdAddress += ",";
            }
            etcdAddress += addrs.first.ToString();
        }
        FLAGS_etcd_address = etcdAddress;
        db_ = std::make_unique<EtcdStore>(etcdAddress);
        if ((db_ != nullptr) && (db_->Init().IsOk())) {
            db_->DropTable(tableName_);
            // We don't check rc here. If table to drop does not exist, it's fine.
            Status rc = db_->CreateTable(tableName_, tablePrefix_);
            if (rc == Status::OK()) {
                tableCreated_ = true;
                rc = db_->Put(defaultTable_, keyForReadOnly_, readOnlyValue_);
            }
        }
    }

    void ReceivedEvents(mvccpb::Event &&event)
    {
        eventCount_++;
        EXPECT_EQ(tablePrefix_ + "/" + watchKey_[eventCount_ - 1], event.kv().key());
        if (event.type() != 1) {
            if (watchValue_[eventCount_ - 1] != "pass") {
                EXPECT_EQ(watchValue_[eventCount_ - 1], event.kv().value());
            }
        }
        LOG(INFO) << "Revision: " << event.kv().mod_revision() << " Count: " << eventCount_ << " Event type "
                  << event.type() << " key " << event.kv().key();
    }

    void ReceivedEvents2(mvccpb::Event &&event)
    {
        eventCount_++;
        auto &key = event.kv().key();
        if (watchKeyMap_.find(key) != watchKeyMap_.end()) {
            ASSERT_EQ(event.type(),  mvccpb::Event::EventType::Event_EventType_DELETE);
            watchKeyMap_.erase(key);
        } else {
            ASSERT_EQ(event.type(),  mvccpb::Event::EventType::Event_EventType_PUT);
            watchKeyMap_.emplace(key, event.type());
        }
    }

    void ReceivedEvents3(mvccpb::Event &&event)
    {
        eventCount_++;
        if (event.type() == mvccpb::Event::EventType::Event_EventType_PUT) {
            watchEvent_.emplace_back(event);
        }
    }

    std::unique_ptr<EtcdStore> db_;
    bool tableCreated_;
    const std::string tableName_ = "table1";
    const std::string tablePrefix_ = "/datasystem/master/inode_table";
    const std::string defaultTable_ = "default";
    const std::string keyForReadOnly_ = "keyForReadOnly_";
    const std::string readOnlyValue_ = "readOnlyValue_";
    std::vector<std::string> watchKey_;
    std::vector<std::string> watchValue_;
    std::unordered_map<std::string, mvccpb::Event::EventType> watchKeyMap_;
    std::vector<mvccpb::Event> watchEvent_;
    int eventCount_ = 0;
};

TEST_F(EtcdStoreTest, TestPutGetKeyValue1)
{
    LOG(INFO) << "Test EtcdStore put/get/delete key/value in a table.";
    InitTestEtcdInstance();
    ASSERT_TRUE(db_ != nullptr && tableCreated_);
    Status rc;
    rc = db_->Put(tableName_, "keyA1", "valueA");
    DS_EXPECT_OK(rc);

    std::string value;
    rc = db_->Get(tableName_, "keyA1", value);
    DS_EXPECT_OK(rc);
    EXPECT_EQ(value, "valueA");

    rc = db_->Delete(tableName_, "keyA1");
    DS_EXPECT_OK(rc);

    rc = db_->Delete(tableName_, "keyA1");
    EXPECT_EQ(rc.GetCode(), K_NOT_FOUND);

    rc = db_->Get(tableName_, "keyA1", value);
    EXPECT_EQ(rc.GetCode(), K_NOT_FOUND);

    db_->Close();  // Execute the close function for test coverage
}

TEST_F(EtcdStoreTest, TestPutLease1)
{
    LOG(INFO) << "Test EtcdStore put with lease in a table.";
    InitTestEtcdInstance();
    ASSERT_TRUE(db_ != nullptr && tableCreated_);

    // Create a leaseID with 10s ttl
    std::atomic<int64_t> leaseID;
    Status rc = db_->GetLeaseIDWithReconnectIfError(5, leaseID);
    DS_EXPECT_OK(rc);

    // Insert a KV pair with leaseID
    std::string key1 = "keyA1";
    std::string value1 = "valueA1";
    rc = db_->PutWithLeaseId(tableName_, key1, value1, leaseID);
    DS_EXPECT_OK(rc);

    // read the KV pair immediately
    std::string value;
    rc = db_->Get(tableName_, key1, value);
    DS_EXPECT_OK(rc);
    EXPECT_EQ(value, value1);

    // wait for lease timeout of 10 sec
    std::this_thread::sleep_for(std::chrono::milliseconds(10000));

    // Read the KV pair again. Reply should have size of 0
    rc = db_->Get(tableName_, key1, value);
    DS_EXPECT_NOT_OK(rc);
}

TEST_F(EtcdStoreTest, LEVEL2_TestPutLease2)
{
    LOG(INFO) << "Test EtcdStore put with lease in a table.";
    InitTestEtcdInstance();
    ASSERT_TRUE(db_ != nullptr && tableCreated_);

    // Insert a KV pair with leaseID
    std::string key1 = "keyA1";

    // Keep the lease alive for every 10s and timeout is 20s
    Status rc = db_->InitKeepAlive(tableName_, key1, false);
    DS_EXPECT_OK(rc);

    // Let the lease setup
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));

    // read the KV pair immediately
    std::string value1;
    rc = db_->Get(tableName_, key1, value1);
    DS_EXPECT_OK(rc);

    // wait for lease timeout of 10 sec
    std::this_thread::sleep_for(std::chrono::milliseconds(10000));

    std::string value;
    // read the KV pair afrer 10s
    rc = db_->Get(tableName_, key1, value);
    DS_EXPECT_OK(rc);
    EXPECT_EQ(value, value1);

    // wait for lease timeout of 20000ms
    std::this_thread::sleep_for(std::chrono::milliseconds(20000));

    // Read the KV pair again. The KV pair should not be deleted due to keep alive operation.
    rc = db_->Get(tableName_, key1, value);
    EXPECT_EQ(value, value1);
    DS_EXPECT_OK(rc);
}

TEST_F(EtcdStoreTest, LEVEL1_TestPutLease3)
{
    LOG(INFO) << "Test EtcdStore put with lease in a table.";
    InitTestEtcdInstance();
    ASSERT_TRUE(db_ != nullptr && tableCreated_);

    // Insert a KV pair with leaseID
    std::string key1 = "keyA1";

    // Keep the lease alive for every 10s and timeout is 20s
    Status rc = db_->InitKeepAlive(tableName_, key1, false);
    DS_EXPECT_OK(rc);

    // Let the lease setup
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));

    // read the KV pair immediately
    std::string value;
    rc = db_->Get(tableName_, key1, value);
    DS_EXPECT_OK(rc);
    auto pos = value.find(";");
    EXPECT_TRUE(pos != std::string::npos);
    EXPECT_EQ(value.substr(pos + 1), "start");

    // wait for lease timeout of 10000ms
    std::this_thread::sleep_for(std::chrono::milliseconds(10000));

    // read the KV pair afrer 10s
    std::string value2;
    rc = db_->Get(tableName_, key1, value2);
    DS_EXPECT_OK(rc);
    pos = value2.find(";");
    EXPECT_TRUE(pos != std::string::npos);
    EXPECT_EQ(value2.substr(pos + 1), "start");
    EXPECT_EQ(value, value2);

    // wait for lease timeout of 20000ms
    std::this_thread::sleep_for(std::chrono::milliseconds(20000));

    // Read the KV pair again. The KV pair should not be deleted due to keep alive operation.
    rc = db_->Get(tableName_, key1, value2);
    DS_EXPECT_OK(rc);
    pos = value2.find(";");
    EXPECT_TRUE(pos != std::string::npos);
    EXPECT_EQ(value2.substr(pos + 1), "start");
    EXPECT_EQ(value, value2);
}

TEST_F(EtcdStoreTest, TestWatchEvents1)
{
    LOG(INFO) << "Test Etcd Watch for put and delete events";
    datasystem::inject::Set("EtcdWatch.RetrieveEventPassively.AvoidEventCompensation", "return");
    InitTestEtcdInstance();
    ASSERT_TRUE(db_ != nullptr && tableCreated_);

    watchKey_.push_back("keyA1");
    watchValue_.push_back("valueA1");
    watchKey_.push_back("keyA1");
    watchValue_.push_back("valueA1");
    watchKey_.push_back("keyA1");
    watchValue_.push_back("valueA1");
    watchKey_.push_back("keyA2");
    watchValue_.push_back("valueA2");
    watchKey_.push_back("keyA3");
    watchValue_.push_back("valueA3");

    LOG(INFO) << "Create a watcher for monitoring events";
    db_->SetEventHandler([this](mvccpb::Event &&event) { ReceivedEvents(std::move(event)); });
    Status rc = db_->WatchEvents(tableName_, "keyA", false, 1);
    DS_EXPECT_OK(rc);

    rc = db_->Put(tableName_, watchKey_[0], watchValue_[0]);
    DS_EXPECT_OK(rc);

    rc = db_->Delete(tableName_, watchKey_[1]);
    DS_EXPECT_OK(rc);

    rc = db_->Put(tableName_, watchKey_[2], watchValue_[2]);
    DS_EXPECT_OK(rc);

    rc = db_->Put(tableName_, watchKey_[3], watchValue_[3]);
    DS_EXPECT_OK(rc);

    rc = db_->Put(tableName_, watchKey_[4], watchValue_[4]);
    DS_EXPECT_OK(rc);

    std::this_thread::sleep_for(std::chrono::milliseconds(5000));
    EXPECT_EQ(eventCount_, 5);
}

TEST_F(EtcdStoreTest, LEVEL1_TestWatchEvents2)
{
    LOG(INFO) << "Test Etcd Watch for lease events with lease timeout";
    datasystem::inject::Set("EtcdWatch.RetrieveEventPassively.AvoidEventCompensation", "return");
    InitTestEtcdInstance();
    ASSERT_TRUE(db_ != nullptr && tableCreated_);

    watchKey_.push_back("keyA1");
    watchValue_.push_back("10");
    watchKey_.push_back("keyA1");
    watchValue_.push_back("10");
    watchKey_.push_back("keyA2");
    watchValue_.push_back("pass");

    LOG(INFO) << "Create a watcher for monitoring events";
    db_->SetEventHandler([this](mvccpb::Event &&event) { ReceivedEvents(std::move(event)); });
    Status rc = db_->WatchEvents(tableName_, "keyA", false, 1);
    DS_EXPECT_OK(rc);

    // Create a leaseID with 10s ttl
    std::atomic<int64_t> leaseID;
    rc = db_->GetLeaseID(10, leaseID);
    DS_EXPECT_OK(rc);

    rc = db_->PutWithLeaseId(tableName_, watchKey_[0], watchValue_[0], leaseID);
    DS_EXPECT_OK(rc);

    // wait for lease timeout of 12 sec
    std::this_thread::sleep_for(std::chrono::milliseconds(12000));

    // lease timeout will trigger delete of key-value pain. Watch will get two events.
    EXPECT_EQ(eventCount_, 2);

    // Set 5 seconds for lease, watch on the key of number 2
    rc = db_->InitKeepAlive(tableName_, watchKey_[2], false);
    DS_EXPECT_OK(rc);

    // Wait more than lease timeout of 10 sec. This time lease will not timeout due to keep alive operation.
    std::this_thread::sleep_for(std::chrono::milliseconds(12000));

    // Put will trigger 3rd event
    EXPECT_EQ(eventCount_, 3);
}

TEST_F(EtcdStoreTest, TestWatchEvents3)
{
    LOG(INFO) << "Test Etcd Watch for put and delete events";
    datasystem::inject::Set("EtcdWatch.RetrieveEventPassively.AvoidEventCompensation", "return");
    InitTestEtcdInstance();
    ASSERT_TRUE(db_ != nullptr && tableCreated_);

    watchKey_.push_back("keyA1");
    watchValue_.push_back("valueA1");
    watchKey_.push_back("keyA1");
    watchValue_.push_back("valueA1");
    ThreadPool threadPool(2);

    threadPool.Submit([this]() {
        // Create a leaseID with 5 sec ttl
        std::atomic<int64_t> leaseID;
        Status rc = db_->GetLeaseID(5, leaseID);
        DS_EXPECT_OK(rc);

        rc = db_->PutWithLeaseId(tableName_, "keyA1", "valueA1", leaseID);
        DS_EXPECT_OK(rc);
    });

    auto fstatus = threadPool.Submit([this]() {
        LOG(INFO) << "Create a watcher for monitoring events";
        db_->SetEventHandler([this](mvccpb::Event &&event) { ReceivedEvents(std::move(event)); });
        Status rrc = db_->WatchEvents(tableName_, "keyA", false, 1);
        EXPECT_EQ(rrc, Status::OK());

        // wait for lease timeout of 6 sec
        std::this_thread::sleep_for(std::chrono::milliseconds(10000));

        EXPECT_LE(eventCount_, 2);  // As "Get+Watch", the number of eventCount_ may be 0,1,2 .

        return Status::OK();
    });

    // To ensure watch is shutdown
    fstatus.wait();
}

TEST_F(EtcdStoreTest, TestKeepAliveFailedDueToNetworkerFailure)
{
    FLAGS_node_timeout_s = 3; // node timeout is 3 s
    datasystem::inject::Set("EtcdStore.LaunchKeepAliveThreads.loopQuickly", "call(0)");
    InitTestEtcdInstance();
    ASSERT_TRUE(db_ != nullptr && tableCreated_);

    watchKey_.push_back("keyA1");
    watchValue_.push_back("pass");
    watchKey_.push_back("keyA1");
    watchValue_.push_back("pass");

    // "return true" means networker failure.
    db_->SetCheckEtcdStateWhenNetworkFailedHandler([]() { return true; });
    LOG(INFO) << "Create a watcher for monitoring events";
    db_->SetEventHandler([this](mvccpb::Event &&event) { ReceivedEvents(std::move(event)); });
    DS_ASSERT_OK(db_->WatchEvents(tableName_, "keyA", false, 1));
    DS_ASSERT_OK(db_->InitKeepAlive(tableName_, watchKey_[0], false));
    sleep(1);  // wait etcd notify new event before shutdown.
    auto externalCluster = dynamic_cast<ExternalCluster *>(cluster_.get());
    inject::Set("etcd.sendrpc", "call(2000)");
    DS_ASSERT_OK(externalCluster->ShutdownEtcds());
    int waitWriteFakeEventTimeS = 5;
    sleep(waitWriteFakeEventTimeS);
    int eventNum = 2;  // event1: put; event2: fake delete
    EXPECT_EQ(eventCount_, eventNum);
}

TEST_F(EtcdStoreTest, LEVEL1_TestRetrieveEvent)
{
    // Ignore Event from watch response
    datasystem::inject::Set("EtcdWatch.StoreEvents.IgnoreEvent", "return");
    datasystem::inject::Set("EtcdWatch.RetrieveEventPassively.RetrieveEventQuickly", "call(10)");
    InitTestEtcdInstance();
    ASSERT_TRUE(db_ != nullptr && tableCreated_);

    int keyNum = 100;
    int eventTypeNum = 2;
    int eventNum = keyNum * eventTypeNum;

    db_->SetEventHandler([this](mvccpb::Event &&event) { ReceivedEvents2(std::move(event)); });
    DS_ASSERT_OK(db_->WatchEvents(tableName_, "", false, 1));

    for (int i = 0; i < keyNum; i++) {
        std::atomic<int64_t> leaseID;
        Status rc = db_->GetLeaseID(5, leaseID);
        DS_EXPECT_OK(rc);
        rc = db_->PutWithLeaseId(tableName_, "key" + std::to_string(i), "value", leaseID);
        DS_EXPECT_OK(rc);
    }

    int waitTime = 10;  // wait all keys expired.
    sleep(waitTime);
    db_->Close();
    ASSERT_LE(eventCount_, eventNum);
    ASSERT_EQ(eventCount_ % eventTypeNum, 0);
    ASSERT_EQ(watchKeyMap_.size(), size_t(0));
}

TEST_F(EtcdStoreTest, TestRetrieveCrossVersionEvent)
{
    // Ignore Event from watch response
    DS_ASSERT_OK(inject::Set("EtcdWatch.StoreEvents.IgnoreEvent", "return"));
    DS_ASSERT_OK(inject::Set("EtcdWatch.RetrieveEventPassively.RetrieveEventQuickly", "call(100)"));
    DS_ASSERT_OK(inject::Set("worker.GenerateFakePutEventIfNeeded.timeout", "call(1000)"));
    DS_ASSERT_OK(inject::Set("EtcdWatch.RetrieveEventPassivelyImpl.preRetrieveEvent", "return()"));
    InitTestEtcdInstance();
    ASSERT_TRUE(db_ != nullptr && tableCreated_);

    db_->SetEventHandler([this](mvccpb::Event &&event) { ReceivedEvents2(std::move(event)); });
    DS_ASSERT_OK(db_->WatchEvents(tableName_, "", false, 1));

    std::atomic<int64_t> leaseID;
    int ttlSec = 10;
    DS_ASSERT_OK(db_->GetLeaseID(ttlSec, leaseID));
    DS_ASSERT_OK(db_->PutWithLeaseId(tableName_, "key", "value1", leaseID));  // version = 1
    DS_ASSERT_OK(db_->PutWithLeaseId(tableName_, "key", "value2", leaseID));  // version = 2

    // We can retrieve events after cleaning injection.
    DS_ASSERT_OK(inject::Clear("EtcdWatch.RetrieveEventPassivelyImpl.preRetrieveEvent"));

    sleep(4);  // Expected to receive a put event after 4s.
    ASSERT_EQ(eventCount_, 1);
}

TEST_F(EtcdStoreTest, LEVEL2_TestRetrieveEventOrder)
{
    // Ignore Event from watch response
    datasystem::inject::Set("EtcdWatch.StoreEvents.IgnoreEvent", "return");
    datasystem::inject::Set("EtcdWatch.RetrieveEventPassively.RetrieveEventQuickly", "call(1000)");
    InitTestEtcdInstance();
    ASSERT_TRUE(db_ != nullptr && tableCreated_);

    int keyNum = 100;
    int eventTypeNum = 2;
    int putEventNum = keyNum;

    db_->SetEventHandler([this](mvccpb::Event &&event) { ReceivedEvents3(std::move(event)); });
    DS_ASSERT_OK(db_->WatchEvents(tableName_, "", false, 1));

    for (int i = 0; i < keyNum; i++) {
        std::atomic<int64_t> leaseID;
        Status rc = db_->GetLeaseID(5, leaseID);
        DS_EXPECT_OK(rc);
        rc = db_->PutWithLeaseId(tableName_, "key" + std::to_string(i), "value", leaseID);
        DS_EXPECT_OK(rc);
    }

    int waitTime = 10;  // wait all keys expired.
    sleep(waitTime);

    ASSERT_EQ(eventCount_ % eventTypeNum, 0);
    ASSERT_LE(eventCount_ / eventTypeNum, putEventNum);
    for (size_t i = 1; i < watchEvent_.size(); i++) {
        if (watchEvent_[i].kv().mod_revision() < watchEvent_[i - 1].kv().mod_revision()) {
            ASSERT_TRUE(false);
        }
    }
}

TEST_F(EtcdStoreTest, TestEtcdShutdownLogic)
{
    LOG(INFO) << "Test EtcdStore and lease Shutdown logic.";
    InitTestEtcdInstance();
    ASSERT_TRUE(db_ != nullptr && tableCreated_);

    // Insert a KV pair with leaseID
    std::string key1 = "keyA1";

    // Keep the lease alive for every 10s and timeout is 20s
    Status rc = db_->InitKeepAlive(tableName_, key1, false);
    DS_EXPECT_OK(rc);

    // Inject grpc status error
    datasystem::inject::Set("CheckGrpcStatus", "call(2)");

    // Drop table test to check if table is deleted
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    rc = db_->DropTable(tableName_);
    DS_EXPECT_OK(rc);

    // Shutdown the rc and check if ok is returned
    rc = db_->Shutdown();
    DS_EXPECT_OK(rc);
}

TEST_F(EtcdStoreTest, TestGetAll)
{
    LOG(INFO) << "Test EtcdStore getall api.";
    InitTestEtcdInstance();
    ASSERT_TRUE(db_ != nullptr && tableCreated_);
    Status rc;
    std::string key1 = "keyA1";
    std::string value1 = "valueA1";
    rc = db_->Put(tableName_, key1, value1);
    DS_EXPECT_OK(rc);

    std::string key2 = "keyA2";
    std::string value2 = "valueA2";
    rc = db_->Put(tableName_, key2, value2);
    DS_EXPECT_OK(rc);

    std::vector<std::pair<std::string, std::string>> outKeyValues;
    rc = db_->GetAll(tableName_, outKeyValues);
    DS_EXPECT_OK(rc);
    EXPECT_EQ(outKeyValues.size(), 2ul);
    EXPECT_EQ(outKeyValues[0].first, key1);
    EXPECT_EQ(outKeyValues[0].second, value1);
    EXPECT_EQ(outKeyValues[1].first, key2);
    EXPECT_EQ(outKeyValues[1].second, value2);
}

TEST_F(EtcdStoreTest, TestRangeSearch)
{
    LOG(INFO) << "Test EtcdStore range search api.";
    InitTestEtcdInstance();
    ASSERT_TRUE(db_ != nullptr && tableCreated_);
    auto putFunc = [this](const std::string &key, const std::string &value) {
        DS_ASSERT_OK(db_->Put(tableName_, key, value));
    };

    std::vector<std::string> keys = { "1110_key1", "1110_key2", "1111_key1", "1111_key2", "1112_key1",
                                      "1112_key2", "1113",      "1113_key1", "1113_key2" };

    for (const auto &key : keys) {
        putFunc(key, "");
    }

    std::vector<std::pair<std::string, std::string>> outKeyValues;
    DS_ASSERT_OK(db_->RangeSearch(tableName_, "1111", "1112", outKeyValues));
    ASSERT_EQ(outKeyValues.size(), 4ul);
    std::set<std::string> orderKeys;
    for (const auto &kv : outKeyValues) {
        orderKeys.emplace(kv.first);
    }
    ASSERT_TRUE(orderKeys.find("1111_key1") != orderKeys.end());
    ASSERT_TRUE(orderKeys.find("1111_key2") != orderKeys.end());
    ASSERT_TRUE(orderKeys.find("1112_key1") != orderKeys.end());
    ASSERT_TRUE(orderKeys.find("1112_key2") != orderKeys.end());
}

TEST_F(EtcdStoreTest, TestPrefixSearch)
{
    LOG(INFO) << "Test EtcdStore prefix search api.";
    InitTestEtcdInstance();
    ASSERT_TRUE(db_ != nullptr && tableCreated_);
    Status rc;
    std::string key1 = "keyA1";
    std::string value1 = "valueA1";
    rc = db_->Put(tableName_, key1, value1);
    DS_EXPECT_OK(rc);

    std::string key2 = "keyA2";
    std::string value2 = "valueA2";
    rc = db_->Put(tableName_, key2, value2);
    DS_EXPECT_OK(rc);

    std::string key3 = "keyB1";
    std::string value3 = "valueB1";
    rc = db_->Put(tableName_, key3, value3);
    DS_EXPECT_OK(rc);

    std::string prefix = "keyA";
    std::vector<std::pair<std::string, std::string>> outKeyValues;
    rc = db_->PrefixSearch(tableName_, prefix, outKeyValues);
    DS_EXPECT_OK(rc);
    EXPECT_EQ(outKeyValues.size(), 2ul);
    EXPECT_EQ(outKeyValues[0].first, key1);
    EXPECT_EQ(outKeyValues[0].second, value1);
    EXPECT_EQ(outKeyValues[1].first, key2);
    EXPECT_EQ(outKeyValues[1].second, value2);
}

TEST_F(EtcdStoreTest, TestTransactionCompareValue)
{
    LOG(INFO) << "Test EtcdStore Transaction.";
    InitTestEtcdInstance();
    ASSERT_TRUE(db_ != nullptr && tableCreated_);
    Status rc;
    std::string key1 = "keyA1";
    std::string value1 = "valueA1";
    rc = db_->Put(tableName_, key1, value1);
    EXPECT_EQ(rc, Status::OK());

    std::string value2 = "valueA2";
    std::string etcdKey1 = tablePrefix_ + "/" + key1;
    Transaction transaction;
    transaction.StartTransaction();
    ASSERT_EQ(transaction.CompareKeyValue(etcdKey1, value1), Status::OK());
    ASSERT_EQ(transaction.Put(etcdKey1, value2), Status::OK());
    ASSERT_EQ(transaction.Commit(), Status::OK());
    std::string res;
    ASSERT_EQ(db_->Get(tableName_, key1, res), Status::OK());
    ASSERT_EQ(res, value2);

    // Transaction failed when prev value is error
    transaction.StartTransaction();
    ASSERT_EQ(transaction.CompareKeyValue(etcdKey1, value1), Status::OK());
    ASSERT_EQ(transaction.Put(etcdKey1, value2), Status::OK());
    ASSERT_NE(transaction.Commit(), Status::OK());

    // Transaction error for an empty kv
    std::string key2 = "keyA2";
    std::string valueKey2 = "valueA2";
    std::string etcdKey2 = tablePrefix_ + "/" + key2;
    transaction.StartTransaction();
    ASSERT_EQ(transaction.CompareKeyValue(etcdKey2, ""), Status::OK());
    ASSERT_EQ(transaction.Put(etcdKey2, valueKey2), Status::OK());
    ASSERT_NE(transaction.Commit(), Status::OK());
}

TEST_F(EtcdStoreTest, TestTransactionCompareKeyVersion)
{
    LOG(INFO) << "Test EtcdStore TransactionCompareKeyVersion.";
    InitTestEtcdInstance();
    ASSERT_TRUE(db_ != nullptr && tableCreated_);
    Status rc;
    std::string key1 = "keyA1";
    std::string value1 = "valueA1";
    int64_t version;
    rc = db_->Put(tableName_, key1, value1, &version);
    EXPECT_EQ(rc, Status::OK());

    std::string value2 = "valueA2";
    std::string etcdKey1 = tablePrefix_ + "/" + key1;
    Transaction transaction;
    transaction.StartTransaction();
    ASSERT_EQ(transaction.CompareKeyVersion(etcdKey1, version), Status::OK());
    ASSERT_EQ(transaction.Put(etcdKey1, value2), Status::OK());
    ASSERT_EQ(transaction.Commit(), Status::OK());
    RangeSearchResult res;
    ASSERT_EQ(db_->Get(tableName_, key1, res), Status::OK());
    ASSERT_EQ(res.value, value2);
    ASSERT_EQ(res.version, version + 1);

    // Transaction failed when prev version is error
    transaction.StartTransaction();
    ASSERT_EQ(transaction.CompareKeyVersion(etcdKey1, version), Status::OK());
    ASSERT_EQ(transaction.Put(etcdKey1, value2), Status::OK());
    ASSERT_NE(transaction.Commit(), Status::OK());

    // Transaction error for an empty kv
    std::string key2 = "keyA2";
    std::string valueKey2 = "valueA2";
    std::string etcdKey2 = tablePrefix_ + "/" + key2;
    transaction.StartTransaction();
    ASSERT_EQ(transaction.CompareKeyVersion(etcdKey2, 0), Status::OK());
    ASSERT_EQ(transaction.Put(etcdKey2, valueKey2), Status::OK());
    ASSERT_EQ(transaction.Commit(), Status::OK());
    ASSERT_EQ(db_->Get(tableName_, key2, res), Status::OK());
    ASSERT_EQ(res.value, valueKey2);
    ASSERT_EQ(res.version, 1);
}

TEST_F(EtcdStoreTest, TestBatchPutKeyValue)
{
    InitTestEtcdInstance();
    ASSERT_TRUE(db_ != nullptr && tableCreated_);
    std::vector<std::string> keys;
    int num = 10;
    std::unordered_map<std::string, EtcdStore::BatchInfoPutToEtcd> metas;
    for (int i = 0; i < num; i++) {
        auto key = "test_key" + std::to_string(i);
        metas[key].etcdKey = key;
        metas[key].meta = "bbbb";
        metas[key].tableName = tableName_;
        keys.emplace_back(key);
    }
    DS_ASSERT_OK(db_->BatchPut(metas));
    for (const auto &key : keys) {
        std::string value;
        DS_ASSERT_OK(db_->Get(tableName_, key, value));
        EXPECT_EQ(value, "bbbb");
    }

    // Use the low-level API, faster but not much error handling
    for (const auto &key : keys) {
        std::string value;
        DS_ASSERT_OK(db_->Delete(tableName_, key));
        DS_ASSERT_NOT_OK(db_->Get(tableName_, key, value));
    }
}

TEST_F(EtcdStoreTest, TestCAS)
{
    LOG(INFO) << "Test EtcdStore TransactionCompareKeyVersion.";
    InitTestEtcdInstance();
    ASSERT_TRUE(db_ != nullptr && tableCreated_);

    Status rc;
    std::string key1 = "keyA1";
    std::string value1 = "valueA1";
    rc = db_->CAS(tableName_, key1,
                  [&value1](const std::string &oldValue, std::unique_ptr<std::string> &newValue, bool &retry) {
                      retry = false;
                      if (oldValue.empty()) {
                          newValue = std::make_unique<std::string>(value1);
                      } else {
                          newValue = std::make_unique<std::string>(oldValue + oldValue);
                      }
                      return Status::OK();
                  });
    ASSERT_EQ(rc, Status::OK());
    RangeSearchResult res;
    ASSERT_EQ(db_->Get(tableName_, key1, res), Status::OK());
    ASSERT_EQ(res.value, value1);
    ASSERT_EQ(res.modRevision, 2);  // mod_revision = 2

    rc = db_->CAS(tableName_, key1,
                  [](const std::string &oldValue, std::unique_ptr<std::string> &newValue, bool &retry) {
                      retry = false;
                      if (oldValue.empty()) {
                          newValue = std::make_unique<std::string>("1");
                      } else {
                          newValue = std::make_unique<std::string>(oldValue + oldValue);
                      }
                      return Status::OK();
                  });
    ASSERT_EQ(rc, Status::OK());
    ASSERT_EQ(db_->Get(tableName_, key1, res), Status::OK());
    ASSERT_EQ(res.value, value1 + value1);
    int64_t revisionIndex3 = 3;
    ASSERT_EQ(res.modRevision, revisionIndex3);
}

TEST_F(EtcdStoreTest, TestCASFuncRetry)
{
    LOG(INFO) << "Test EtcdStore TransactionCompareKeyVersion.";
    InitTestEtcdInstance();
    ASSERT_TRUE(db_ != nullptr && tableCreated_);

    Status rc;
    std::string key1 = "keyA1";
    rc = db_->CAS(tableName_, key1, [](const std::string &, std::unique_ptr<std::string> &, bool &retry) {
        retry = true;
        RETURN_STATUS(K_RUNTIME_ERROR, "Test error.");
    });
    ASSERT_EQ(rc.GetCode(), K_RUNTIME_ERROR);
}

TEST_F(EtcdStoreTest, TestCASCompareValue)
{
    LOG(INFO) << "Test EtcdStore TransactionCompareKeyVersion.";
    InitTestEtcdInstance();
    ASSERT_TRUE(db_ != nullptr && tableCreated_);

    Status rc;
    std::string key1 = "keyA1";
    std::string value1 = "valueA1";
    rc = db_->CAS(tableName_, key1, "", value1);
    ASSERT_EQ(rc, Status::OK());
    RangeSearchResult res;
    ASSERT_EQ(db_->Get(tableName_, key1, res), Status::OK());
    ASSERT_EQ(res.value, value1);
    ASSERT_EQ(res.modRevision, 2);  // mod_revision = 2

    std::string value2 = "valueA2";
    rc = db_->CAS(tableName_, key1, value1, value2);
    ASSERT_EQ(rc, Status::OK());
    ASSERT_EQ(db_->Get(tableName_, key1, res), Status::OK());
    ASSERT_EQ(res.value, value2);
    int64_t revisionIndex3 = 3;
    ASSERT_EQ(res.modRevision, revisionIndex3);

    std::string value3 = "valueA3";
    rc = db_->CAS(tableName_, key1, value3, "test");
    ASSERT_NE(rc, Status::OK());

    rc = db_->CAS(tableName_, key1, "", "test");
    ASSERT_NE(rc, Status::OK());
}

TEST_F(EtcdStoreTest, TestGetEtcdPrefix)
{
    LOG(INFO) << "Test EtcdStore GetEtcdPrefix";
    std::string table_prefix = "AZ1";
    FLAGS_az_name = table_prefix;
    InitTestEtcdInstance();
    std::string prefix;
    DS_ASSERT_OK(db_->GetEtcdPrefix(tableName_, prefix));
    ASSERT_EQ(prefix, "/" + table_prefix + tablePrefix_);
    DS_ASSERT_NOT_OK(db_->GetEtcdPrefix("balabala", prefix));
}

class EtcdSslTest : public ExternalClusterTest {
protected:
    EtcdSslTest()
    {
    }
    ~EtcdSslTest() = default;

    // etcd auth
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numEtcd = 3;
        opts.numMasters = 0;
        opts.numWorkers = 0;
        FLAGS_enable_etcd_auth = true;
        GenerateEncryptedETCDCerts();
    }

    virtual void GenerateEncryptedETCDCerts()
    {
        std::string cmd = "bash ../../../tests/ut/gen_cert.sh";
        std::string cmdRes;
        (void)ExecuteCmd(cmd, cmdRes);
        LOG(INFO) << cmdRes;
    }

    void InitTestEtcdInstance()
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
        FLAGS_etcd_address = etcdAddress;
    }

    void EncryptEtcdCertificate()
    {
        if (FileExist("./certs/etcd.p12")) {
            (void)DeleteFile("./certs/etcd.p12");
        }
        std::string cmd = "mkdir etcd_encrypted_certs; ./encrypt -d certs -o etcd_encrypted_certs";
        std::string cmdRes;
        (void)ExecuteCmd(cmd, cmdRes);
        LOG(INFO) << cmdRes;
        FLAGS_etcd_ca = "./certs/ca.crt";
        FLAGS_etcd_cert = "./certs/etcd-client.crt";
        FLAGS_etcd_key = "./etcd_encrypted_certs/etcd-client.key";
        FLAGS_etcd_target_name_override = "etcd";
    }
};

TEST_F(EtcdSslTest, TestCreateSessionWithTls)
{
    InitTestEtcdInstance();
    std::string stsP12Path = "./certs/etcd.p12";
    std::string pass = "example";
    SensitiveValue passphrase(pass);

    std::string CA;
    SensitiveValue crt;
    SensitiveValue key;
    Status res = ParsePKCS12(stsP12Path, passphrase, CA, crt, key);
    ASSERT_EQ(res.GetCode(), K_OK);
    // create session with TSL, put success!
    std::unique_ptr<GrpcSession<etcdserverpb::KV>> rpcSession_;
    GrpcSession<etcdserverpb::KV>::CreateSessionWithTls(FLAGS_etcd_address, CA, crt, key, "etcd", rpcSession_);
    etcdserverpb::PutRequest req;
    std::string etcdKey = "KEY-grpcSslTest";
    req.set_key(etcdKey);
    req.set_value("value0");
    etcdserverpb::PutResponse rsp;
    res = rpcSession_->SendRpc("Put::etcd_kv_Put", req, rsp, &etcdserverpb::KV::Stub::Put);
    ASSERT_EQ(res.GetCode(), K_OK);
    // get success
    etcdserverpb::RangeRequest rangeReq;
    rangeReq.set_key(etcdKey);
    etcdserverpb::RangeResponse rangeRsp;
    res = rpcSession_->SendRpc("Get::etcd_kv_Range", rangeReq, rangeRsp, &etcdserverpb::KV::Stub::Range);
    ASSERT_EQ(res.GetCode(), K_OK);
    std::string value = rangeRsp.kvs(0).value();
    ASSERT_EQ(value, "value0");

    // create session without TSL, put fail!
    std::unique_ptr<GrpcSession<etcdserverpb::KV>> rpcSession2_;
    rpcSession2_ = GrpcSession<etcdserverpb::KV>::CreateSessionWithoutTls(FLAGS_etcd_address);
    const int timeoutMs = 3'000;
    res = rpcSession2_->SendRpc("Put::etcd_kv_Put", req, rsp, &etcdserverpb::KV::Stub::Put, 0, timeoutMs);
    ASSERT_EQ(res.GetCode(), K_RPC_UNAVAILABLE);
}

class EtcdSslWithPassphraseTest : public EtcdSslTest {
protected:
    virtual void GenerateEncryptedETCDCerts() override
    {
        std::string cmd = "bash ../../../tests/ut/gen_cert_with_passphrase.sh";
        std::string cmdRes;
        (void)ExecuteCmd(cmd, cmdRes);
        LOG(INFO) << cmdRes;
    }
};

TEST_F(EtcdSslWithPassphraseTest, TestCreateSessionWithTls)
{
    FLAGS_etcd_passphrase_path = "./certs/passphrase";
    FLAGS_etcd_ca = "./certs/ca.crt";
    FLAGS_etcd_cert = "./certs/etcd-client.crt";
    FLAGS_etcd_key = "./certs/etcd-client.key";

    InitTestEtcdInstance();

    std::unique_ptr<GrpcSession<etcdserverpb::KV>> rpcSession;
    DS_ASSERT_OK(GrpcSession<etcdserverpb::KV>::ReadEtcdCertAndCreateSession(
        FLAGS_etcd_address, FLAGS_etcd_ca, FLAGS_etcd_cert, FLAGS_etcd_key, "etcd", FLAGS_etcd_passphrase_path, true,
        rpcSession));

    // put success
    etcdserverpb::PutRequest req;
    std::string etcdKey = "KEY-grpcKmcSslTest";
    req.set_key(etcdKey);
    req.set_value("kmcValue");
    etcdserverpb::PutResponse rsp;
    DS_ASSERT_OK(rpcSession->SendRpc("Put::etcd_kv_Put", req, rsp, &etcdserverpb::KV::Stub::Put));
    // get success
    etcdserverpb::RangeRequest rangeReq;
    rangeReq.set_key(etcdKey);
    etcdserverpb::RangeResponse rangeRsp;
    DS_ASSERT_OK(rpcSession->SendRpc("Get::etcd_kv_Range", rangeReq, rangeRsp, &etcdserverpb::KV::Stub::Range));
    std::string value = rangeRsp.kvs(0).value();
    ASSERT_EQ(value, "kmcValue");
}
}  // namespace st
}  // namespace datasystem
