/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2022. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Description: Test ObjectMeta Storage basic functions.
 */

#include <unistd.h>
#include <cstdint>
#include <filesystem>
#include <list>
#include <memory>
#include <string>
#include <unordered_map>

#include <gtest/gtest.h>

#include "ut/common.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/kvstore/rocksdb/rocks_store.h"
#include "datasystem/common/util/uuid_generator.h"
#include "datasystem/master/object_cache/store/object_meta_store.h"
#include "datasystem/master/object_cache/oc_metadata_manager.h"
#include "datasystem/common/kvstore/etcd/etcd_constants.h"
#include "datasystem/common/util/file_util.h"
#include "datasystem/common/util/hash_algorithm.h"
#include "datasystem/common/util/random_data.h"
#include "datasystem/common/log/log.h"
#include "datasystem/worker/object_cache/worker_oc_service_impl.h"

DS_DECLARE_string(etcd_address);
DS_DECLARE_string(rocksdb_write_mode);

using namespace datasystem::master;
namespace datasystem {
namespace ut {
void MakeObjectMetas(size_t createNum, std::unordered_map<std::string, ObjectMeta> &metas)
{
    for (size_t i = 0; i < createNum; i++) {
        ObjectMeta objectMeta;
        objectMeta.meta.set_object_key(std::to_string(i));
        objectMeta.meta.set_data_size(RandomData().GetRandomUint64());
        std::string address = "127.0.0.1:1000";
        objectMeta.locations[address] = AckState::ACK;
        metas.emplace(objectMeta.meta.object_key(), objectMeta);
    }
}

class ObjectMetaStoreTest : public CommonTest {
public:
    void SetUp()
    {
        // set the random string len == 8
        backStorePath_ = "rocks_objectmeta_store_" + random_.GetRandomString(8);
        rocksStore_ = RocksStore::GetInstance(backStorePath_);
        ObjectMetaStore_ = std::make_unique<ObjectMetaStore>(rocksStore_.get(), nullptr);
        CHECK_EQ(ObjectMetaStore_->Init(), Status::OK());
    }

    void TearDown()
    {
        ObjectMetaStore_.reset();
        DS_ASSERT_OK(RemoveAll(backStorePath_));
    }

    void MakeExistIds(std::list<std::string> &queryIds, const std::unordered_map<std::string, ObjectMeta> &inMetas)
    {
        size_t index = 0;
        for (const auto &meta : inMetas) {
            // will remove 50% nodes
            if (index % 2 == 0) {
                queryIds.emplace_back(meta.first);
            }
            index++;
        }
    }

    Status StoreCreate(std::unordered_map<std::string, ObjectMeta> &inMetas)
    {
        for (auto &meta : inMetas) {
            std::string serializedStr;
            RETURN_IF_NOT_OK(
                ObjectMetaStore_->CreateSerializedStringForMeta(meta.first, meta.second.meta, serializedStr));
            RETURN_IF_NOT_OK(ObjectMetaStore_->CreateOrUpdateMeta(meta.first, serializedStr));
        }
        return Status::OK();
    }

    Status StoreRemove(std::list<std::string> removeIds)
    {
        for (const auto &objectKey : removeIds) {
            RETURN_IF_NOT_OK(this->ObjectMetaStore_->RemoveMeta(objectKey));
        }
        return Status::OK();
    }

    std::string backStorePath_;
    static RandomData random_;
    std::shared_ptr<RocksStore> rocksStore_;
    std::unique_ptr<ObjectMetaStore> ObjectMetaStore_;
};

RandomData ObjectMetaStoreTest::random_;

TEST_F(ObjectMetaStoreTest, TestCreateQueryRemoveMeta)
{
    // Create
    size_t createNum = 10;
    std::unordered_map<std::string, ObjectMeta> inMetas;
    MakeObjectMetas(createNum, inMetas);
    EXPECT_EQ(this->StoreCreate(inMetas), Status::OK());
    // Create same
    EXPECT_EQ(this->StoreCreate(inMetas), Status::OK());
    std::list<std::string> removeIds;
    this->MakeExistIds(removeIds, inMetas);
    // Remove exist
    EXPECT_EQ(this->StoreRemove(removeIds), Status::OK());
    // Remove not exist
    EXPECT_EQ(this->StoreRemove(removeIds), Status::OK());
    sleep(1);
}

// Verify that rocksdb_write_mode=none makes RocksStore::GetInstance return a shell instance
// WITHOUT calling DB::Open (no LOCK file is created) and that subsequent Put/Get calls do not
// crash (they are short-circuited by RETURN_OK_IF_TRUE(disableRocksDB)). This is the contract
// that lets a worker start without RocksDB during rolling restarts without hitting LOCK
// contention (K_KVSTORE_ERROR, exit 255).
class RocksStoreNoneModeTest : public testing::Test {
public:
    void SetUp() override
    {
        // Save the original flag value to restore in TearDown (avoid hardcoding a mode that
        // could differ from the process default and pollute other tests).
        origMode_ = FLAGS_rocksdb_write_mode;
        std::string err;
        ASSERT_TRUE(SetCommandLineOption("rocksdb_write_mode", "none", err)) << err;
        backStorePath_ = "rocks_none_mode_" + random_.GetRandomString(8);
    }

    void TearDown() override
    {
        std::string err;
        SetCommandLineOption("rocksdb_write_mode", origMode_, err);
        // disableRocksDB is process-level; clear it so other tests get a real DB again.
        RocksStore::ResetDisableRocksDBForTest();
        DS_ASSERT_OK(RemoveAll(backStorePath_));
    }

    std::string origMode_;
    std::string backStorePath_;
    static RandomData random_;
};

RandomData RocksStoreNoneModeTest::random_;

TEST_F(RocksStoreNoneModeTest, NoneModeSkipsDbOpenAndDoesNotCreateLockFile)
{
    // none mode -> GetInstance must NOT call DB::Open. The shell instance is returned (non-null)
    // and no RocksDB artifacts (LOCK file, the db directory, or any other file) are created.
    std::shared_ptr<RocksStore> store = RocksStore::GetInstance(backStorePath_);
    ASSERT_NE(store, nullptr);

    // The store directory itself must not exist: DB::Open (with create_if_missing=true) is what
    // creates it, and we skipped it. Asserting the whole path is absent is stronger than just LOCK.
    std::error_code ec;
    bool dirExists = std::filesystem::exists(backStorePath_, ec);
    EXPECT_FALSE(dirExists)
        << "rocksdb_write_mode=none must skip DB::Open, so no db directory should be created";
    // belt-and-suspenders: even if the directory somehow existed, it must be empty.
    if (dirExists) {
        size_t n = std::distance(std::filesystem::directory_iterator(backStorePath_, ec),
                                 std::filesystem::directory_iterator());
        EXPECT_EQ(n, 0u) << "rocksdb_write_mode=none must leave the store dir empty (no LOCK/CURRENT/MANIFEST)";
    }

    // Subsequent data operations must not crash; they are no-ops via disableRocksDB guard.
    EXPECT_EQ(store->CreateTable("t1", rocksdb::ColumnFamilyOptions()), Status::OK());
    EXPECT_EQ(store->Put("t1", "k", "v"), Status::OK());
    std::string val;
    EXPECT_EQ(store->Get("t1", "k", val), Status::OK());
    EXPECT_EQ(store->Delete("t1", "k"), Status::OK());

    // After operations, the directory must STILL not exist (no-op operations must not create it).
    EXPECT_FALSE(std::filesystem::exists(backStorePath_, ec))
        << "no-op Put/Get/Delete in none mode must not create any rocksdb directory";
}

}  // namespace ut
}  // namespace datasystem
