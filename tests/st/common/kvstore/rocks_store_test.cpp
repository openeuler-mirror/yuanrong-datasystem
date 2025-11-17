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
 * Description: Test interface to RocksDB
 */
#include <string>
#include <thread>
#include <unordered_map>

#include <rocksdb/slice_transform.h>

#include "common.h"
#include "datasystem/common/kvstore/rocksdb/rocks_store.h"
#include "datasystem/common/util/file_util.h"
#include "datasystem/common/util/random_data.h"
#include "datasystem/utils/status.h"

using namespace datasystem;
DS_DECLARE_string(rocksdb_write_mode);
namespace datasystem {
namespace st {
class RocksStoreTest : public CommonTest {
protected:
    RocksStoreTest() : db_(nullptr), tableCreated_(false), tableHandle_(nullptr)
    {
    }

    ~RocksStoreTest() override = default;

    // every TEST_F macro will call SetUp when start
    void SetUp() override;

    // every TEST_F macro will call TearDown when end
    void TearDown() override;

    void ReopenRockStore();

    std::shared_ptr<RocksStore> db_;
    bool tableCreated_;
    rocksdb::ColumnFamilyHandle *tableHandle_;
    static RandomData random_;
    // make sure ctest can run test in parallel
    const std::string dbName_ = "./rocksdb-" + random_.GetRandomString(8);
    const std::string tableName_ = "table1";
    const std::string defaultTable_ = "default";
    const std::string keyForReadOnly_ = "keyForReadOnly_";
    const std::string readOnlyValue_ = "readOnlyValue_";
};

RandomData RocksStoreTest::random_;

void RocksStoreTest::SetUp()
{
    FLAGS_rocksdb_write_mode = "sync";
    db_ = RocksStore::GetInstance(dbName_);
    if (db_ != nullptr) {
        rocksdb::ColumnFamilyOptions options;
        db_->DropTable(tableName_);
        // We don't check rc here. If table to drop does not exist, it's fine.
        options.prefix_extractor.reset(rocksdb::NewFixedPrefixTransform(2));
        Status rc = db_->CreateTable(tableName_, options, &tableHandle_);
        if (rc == Status::OK()) {
            tableCreated_ = true;
            rc = db_->Put(defaultTable_, keyForReadOnly_, readOnlyValue_);
        }
    }
}

void RocksStoreTest::TearDown()
{
    if (db_ != nullptr) {
        db_->Close();
        db_ = nullptr;
    }
    DS_ASSERT_OK(RemoveAll(dbName_));
}

void RocksStoreTest::ReopenRockStore()
{
    if (db_ != nullptr) {
        db_->Close();
        db_ = nullptr;
        db_ = RocksStore::GetInstance(dbName_);
        if (db_ != nullptr) {
            auto status = db_->Put(defaultTable_, keyForReadOnly_, readOnlyValue_);
        }
    }
}

TEST_F(RocksStoreTest, TestCreateDb)
{
    LOG(INFO) << "Test RocksStore create/open database.";
    EXPECT_NE(db_, nullptr);
}

TEST_F(RocksStoreTest, TestListTables)
{
    LOG(INFO) << "Test RocksStore list tables.";
    if (db_ != nullptr) {
        Status rc;
        std::vector<std::string> tableNames;
        rc = db_->ListTables(tableNames);
        EXPECT_EQ(rc, Status::OK());
        bool found = false;
        for (auto name : tableNames) {
            if (name == "default") {
                // Found "default" table
                found = true;
                break;
            }
        }
        EXPECT_EQ(found, true);
    } else {
        // Force failure
        EXPECT_TRUE(false);
    }
}

TEST_F(RocksStoreTest, TestCreateTable)
{
    LOG(INFO) << "Test RocksStore create table.";
    if (db_ != nullptr && tableCreated_) {
        std::vector<std::string> tableNames;
        Status rc;
        rc = db_->ListTables(tableNames);
        EXPECT_EQ(rc, Status::OK());
        tableCreated_ = false;
        for (auto name : tableNames) {
            if (name == tableName_) {
                tableCreated_ = true;
                break;
            }
        }
        EXPECT_EQ(tableCreated_, true);
    } else {
        // Force failure
        EXPECT_TRUE(false);
    }
}

TEST_F(RocksStoreTest, TestPutGetKeyValue1)
{
    LOG(INFO) << "Test RocksStore put/get/delete key/value in a table.";
    if (db_ != nullptr && tableCreated_) {
        Status rc;
        rc = db_->Put(tableName_, "keyA1", "valueA");
        EXPECT_EQ(rc, Status::OK());

        std::string value;
        rc = db_->Get(tableName_, "keyA1", value);
        EXPECT_EQ(rc, Status::OK());
        EXPECT_EQ(value, "valueA");

        rc = db_->Delete(tableName_, "keyA1");
        EXPECT_EQ(rc, Status::OK());

        // Use the low-level API, faster but not much error handling
        rocksdb::Status rc2;
        rc2 = db_->Put(tableHandle_, "A1", "valueA");
        EXPECT_TRUE(rc2.ok());

        std::string value2;
        rc2 = db_->Get(tableHandle_, "A1", value2);
        EXPECT_TRUE(rc2.ok());
        EXPECT_EQ(value2, "valueA");

        rc2 = db_->Delete(tableHandle_, "A1");
        EXPECT_TRUE(rc2.ok());
    } else {
        // Force failure
        EXPECT_TRUE(false);
    }
}

TEST_F(RocksStoreTest, TestBatchPutDeleteKeyValue)
{
    LOG(INFO) << "Test RocksStore put/get/delete key/value in a table.";
    if (db_ != nullptr && tableCreated_) {
        Status rc;
        std::vector<std::string> keys;
        std::unordered_map<std::string, std::string> metas;
        int num = 10;
        for (int i = 0; i < num; i++) {
            auto key = "test_key" + std::to_string(i);
            metas.emplace(key, "bbbbb");
            keys.emplace_back(key);
        }
        rc = db_->BatchPut(tableName_, metas);
        EXPECT_EQ(rc, Status::OK());
        for (const auto &key : keys) {
            std::string value;
            rc = db_->Get(tableName_, key, value);
            EXPECT_EQ(rc, Status::OK());
            EXPECT_EQ(value, "bbbbb");
        }

        rc = db_->BatchDelete(tableName_, metas);
        EXPECT_EQ(rc, Status::OK());

        // Use the low-level API, faster but not much error handling
        for (const auto &key : keys) {
            std::string value;
            rc = db_->Get(tableName_, key, value);
            EXPECT_EQ(rc.GetCode(), K_NOT_FOUND);
        }
    }
}

TEST_F(RocksStoreTest, TestBatchPutAndDeleteKeyValue1)
{
    LOG(INFO) << "Test RocksStore put/get/delete key/value in a table.";
    if (db_ != nullptr && tableCreated_) {
        Status rc;
        std::vector<std::string> keys;
        int num = 10;
        std::unordered_map<std::string, std::string> metas;
        for (int i = 0; i < num; i++) {
            auto key = "test_key" + std::to_string(i);
            metas.emplace(key, "bbbbb");
            keys.emplace_back(key);
        }
        rc = db_->BatchPut(tableName_, metas);
        EXPECT_EQ(rc, Status::OK());
        for (const auto &key : keys) {
            std::string value;
            rc = db_->Get(tableName_, key, value);
            EXPECT_EQ(rc, Status::OK());
            EXPECT_EQ(value, "bbbbb");
        }

        // Use the low-level API, faster but not much error handling
        for (const auto &key : keys) {
            std::string value;
            rc = db_->Delete(tableName_, key);
            EXPECT_EQ(rc, Status::OK());
            rc = db_->Get(tableName_, key, value);
            EXPECT_EQ(rc.GetCode(), K_NOT_FOUND);
        }
    }
}

TEST_F(RocksStoreTest, TestPrefixSearch)
{
    LOG(INFO) << "Test RocksStore put/PrefixSearch in a table.";
    if (db_ != nullptr && tableCreated_) {
        std::unordered_map<std::string, std::string> inodeMap;
        inodeMap["1_dir1"] = "2";
        inodeMap["2_file1"] = "3";
        inodeMap["2_file2"] = "4";

        rocksdb::Status rc;
        rc = db_->Put(tableHandle_, "1_dir1", "2");
        EXPECT_TRUE(rc.ok());
        rc = db_->Put(tableHandle_, "2_file1", "3");
        EXPECT_TRUE(rc.ok());
        rc = db_->Put(tableHandle_, "2_file2", "4");
        EXPECT_TRUE(rc.ok());

        std::vector<std::pair<std::string, std::string>> inode1Children;
        db_->PrefixSearch(tableHandle_, "1_", inode1Children);
        for (auto &child : inode1Children) {
            LOG(INFO) << "inode1 child:" << child.second;
            EXPECT_TRUE(inodeMap.find(child.first) != inodeMap.end());
            EXPECT_EQ(child.second, inodeMap[child.first]);
        }
        EXPECT_EQ(inode1Children.size(), static_cast<size_t>(1));

        std::vector<std::pair<std::string, std::string>> inode2Children;
        EXPECT_EQ(db_->PrefixSearch(tableName_, "2_", inode2Children), Status::OK());
        for (auto &child : inode2Children) {
            LOG(INFO) << "inode2 child:" << child.second;
            EXPECT_TRUE(inodeMap.find(child.first) != inodeMap.end());
            EXPECT_EQ(child.second, inodeMap[child.first]);
        }
        EXPECT_EQ(inode2Children.size(), static_cast<size_t>(2));

        // Negative test
        EXPECT_NE(db_->PrefixSearch(tableName_, "1", inode1Children), Status::OK());
        EXPECT_NE(db_->PrefixSearch(defaultTable_, "1_", inode1Children), Status::OK());

    } else {
        // Force failure
        EXPECT_TRUE(false);
    }
}

TEST_F(RocksStoreTest, TestConcurrentWrite)
{
    LOG(INFO) << "Test RocksStore writing concurrently.";
    EXPECT_NE(db_, nullptr);
    if (tableCreated_) {
        int counter = 0;
        std::mutex lck;
        std::thread thread1([&]() {
            Status rc;
            std::lock_guard<std::mutex> lock(lck);
            rc = db_->Put(tableName_, "0_ParallelWrite1", std::to_string(counter));
            EXPECT_EQ(rc, Status::OK());
            counter += 13;
        });
        std::thread thread2([&]() {
            Status rc;
            std::lock_guard<std::mutex> lock(lck);
            rc = db_->Put(tableName_, "0_ParallelWrite2", std::to_string(counter));
            EXPECT_EQ(rc, Status::OK());
            counter += 7;
        });
        thread1.join();
        thread2.join();

        std::vector<std::pair<std::string, std::string>> keyValues;
        Status rc = db_->PrefixSearch(tableName_, "0_", keyValues);
        EXPECT_EQ(rc, Status::OK());
        for (auto &value : keyValues) {
            LOG(INFO) << "(" << value.first << "," << value.second << ")" << '\n';
            auto number = std::stoi(value.second);
            EXPECT_TRUE(number == 0 || number == 7 || number == 13);
        }
    }
}

TEST_F(RocksStoreTest, TestCreateDbTwice)
{
    LOG(INFO) << "Test RocksStore create/open database.";
    EXPECT_NE(db_, nullptr);

    auto db_second = RocksStore::GetInstance(dbName_);
    // Will fail
    EXPECT_EQ(db_second, nullptr);
}

TEST_F(RocksStoreTest, TestDropTable)
{
    LOG(INFO) << "Test RocksStore drop table.";
    if (db_ != nullptr && tableCreated_) {
        Status rc;
        rc = db_->DropTable(tableName_);
        EXPECT_EQ(rc, Status::OK());

        std::vector<std::string> tableNames;
        rc = db_->ListTables(tableNames);
        EXPECT_EQ(rc, Status::OK());
        bool found = false;
        for (auto name : tableNames) {
            if (name == tableName_) {
                found = true;
                break;
            }
        }
        EXPECT_EQ(found, false);
        tableCreated_ = false;
    } else {
        // Force failure
        EXPECT_TRUE(false);
    }
}

TEST_F(RocksStoreTest, TestDropDb)
{
    LOG(INFO) << "Test RocksStore drop database.";
    if (db_ != nullptr) {
        db_->Close();
        db_ = nullptr;
    } else {
        // Force failure
        EXPECT_TRUE(false);
    }
}

TEST_F(RocksStoreTest, TestPrefixDelete)
{
    LOG(INFO) << "Test RocksStore put/PrefixSearch in a table.";
    if (db_ != nullptr && tableCreated_) {
        std::unordered_map<std::string, std::string> inodeMap;
        inodeMap["1_dir1"] = "2";
        inodeMap["2_file1"] = "3";
        inodeMap["2_file2"] = "4";

        rocksdb::Status rc;
        rc = db_->Put(tableHandle_, "1_dir1", "2");
        EXPECT_TRUE(rc.ok());
        rc = db_->Put(tableHandle_, "2_file1", "3");
        EXPECT_TRUE(rc.ok());
        rc = db_->Put(tableHandle_, "2_file2", "4");
        EXPECT_TRUE(rc.ok());

        Status status = db_->PrefixDelete(tableName_, "2_");
        DS_ASSERT_OK(status);

        std::string value;
        status = db_->Get(tableName_, "2_file1", value);
        DS_ASSERT_NOT_OK(status);
        std::string value2;
        status = db_->Get(tableName_, "2_file2", value2);
        DS_ASSERT_NOT_OK(status);
    } else {
        // Force failure
        EXPECT_TRUE(false);
    }
}
}  // namespace st
}  // namespace datasystem
