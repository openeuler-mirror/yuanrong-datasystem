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
 * Description: test ExistenceOpt NX handling against stale metadata entries (issue #1312).
 */
#include <atomic>
#include <memory>
#include <string>

#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/util/timer.h"
#include "ut/common.h"

#define private public
#include "datasystem/master/object_cache/oc_metadata_manager.h"
#include "datasystem/master/object_cache/expired_object_manager.h"
#undef private

DS_DECLARE_string(rocksdb_write_mode);

namespace datasystem::master {
namespace {
constexpr char LOCAL_ADDRESS[] = "127.0.0.1:30001";
constexpr char WORKER_ADDRESS[] = "127.0.0.1:30002";
constexpr uint32_t TTL_SECOND = 3600;

class OCMetadataManagerExistenceOptTest : public ut::CommonTest {
public:
    void SetUp() override
    {
        oldWriteMode_ = FLAGS_rocksdb_write_mode;
        FLAGS_rocksdb_write_mode = "sync";
        rocksStore_ = RocksStore::GetInstance(ut::GetTestCaseDataDir() + "/rocksdb");
        akSkManager_ = std::make_shared<AkSkManager>(0);
        manager_ = std::make_unique<OCMetadataManager>(akSkManager_, rocksStore_.get(), nullptr, nullptr,
                                                       LOCAL_ADDRESS, nullptr, nullptr, true, HostPort(),
                                                       LOCAL_ADDRESS, &localExiting_, "workerId");
        DS_ASSERT_OK(manager_->objectStore_->Init());
        manager_->nestedRefManager_ = std::make_unique<OCNestedManager>(manager_->objectStore_, true);
        manager_->notifyWorkerManager_ =
            std::make_unique<OCNotifyWorkerManager>(manager_->objectStore_, true, akSkManager_, manager_.get());
        manager_->expiredObjectManager_ = std::make_unique<ExpiredObjectManager>(LOCAL_ADDRESS, manager_.get());
    }

    void TearDown() override
    {
        (void)inject::Clear("master.cache_invalid_failed");
        manager_.reset();
        rocksStore_.reset();
        FLAGS_rocksdb_write_mode = oldWriteMode_;
    }

    // An entry with live copies: NX must consider it existing while its ttl has not elapsed.
    ObjectMeta MakeEntry(uint64_t version, uint32_t ttlSecond)
    {
        ObjectMeta entry;
        entry.meta.set_version(version);
        entry.meta.set_ttl_second(ttlSecond);
        entry.meta.mutable_config()->set_write_mode(static_cast<uint32_t>(WriteMode::NONE_L2_CACHE));
        entry.locations[std::string(LOCAL_ADDRESS)] = AckState::ACK;
        return entry;
    }

    void SeedTableEntry(const std::string &objectKey, const ObjectMeta &entry)
    {
        TbbMetaTable::accessor accessor;
        auto &shard = manager_->metaShards_[manager_->GetShardIndex(objectKey)];
        bthread::RWLockWrGuard lock(shard.mutex);
        (void)shard.table.insert(accessor, objectKey);
        accessor->second = entry;
    }

    bool HasExpiryTask(const std::string &objectKey)
    {
        auto &expired = *manager_->expiredObjectManager_;
        auto &shard = expired.shards_[expired.GetShardIndex(objectKey)];
        return shard.obj2Timed.find(objectKey) != shard.obj2Timed.end();
    }

    bool HasMeta(const std::string &objectKey)
    {
        TbbMetaTable::const_accessor accessor;
        auto &shard = manager_->metaShards_[manager_->GetShardIndex(objectKey)];
        bthread::RWLockRdGuard lock(shard.mutex);
        return shard.table.find(accessor, objectKey);
    }

    bool GetMetaVersion(const std::string &objectKey, uint64_t &version)
    {
        TbbMetaTable::const_accessor accessor;
        auto &shard = manager_->metaShards_[manager_->GetShardIndex(objectKey)];
        bthread::RWLockRdGuard lock(shard.mutex);
        if (!shard.table.find(accessor, objectKey)) {
            return false;
        }
        version = accessor->second.meta.version();
        return true;
    }

    bool GetExpiryTaskDeadline(const std::string &objectKey, uint64_t &deadline)
    {
        auto &expired = *manager_->expiredObjectManager_;
        auto &shard = expired.shards_[expired.GetShardIndex(objectKey)];
        auto iter = shard.obj2Timed.find(objectKey);
        if (iter == shard.obj2Timed.end()) {
            return false;
        }
        deadline = iter->second->first;
        return true;
    }

protected:
    std::shared_ptr<AkSkManager> akSkManager_;
    std::shared_ptr<RocksStore> rocksStore_;
    std::unique_ptr<OCMetadataManager> manager_;
    std::atomic<bool> localExiting_{ false };
    std::string oldWriteMode_;
};

TEST_F(OCMetadataManagerExistenceOptTest, RejectedCreateMetaKeepsExpiryTaskRegistered)
{
    const std::string objectKey = "ttl-strip-entry";
    const uint64_t version = static_cast<uint64_t>(GetSystemClockTimeStampUs());
    ObjectMeta entry = MakeEntry(version, TTL_SECOND);
    entry.meta.set_object_key(objectKey);
    SeedTableEntry(objectKey, entry);
    DS_ASSERT_OK(manager_->expiredObjectManager_->InsertObject(objectKey, version, TTL_SECOND));

    ObjectMetaPb newMeta;
    newMeta.set_object_key(objectKey);
    newMeta.set_data_size(8);
    newMeta.set_ttl_second(TTL_SECOND);
    newMeta.mutable_config()->set_write_mode(static_cast<uint32_t>(WriteMode::NONE_L2_CACHE));
    newMeta.mutable_config()->set_data_format(static_cast<uint32_t>(DataFormat::BINARY));
    int64_t newVersion = 0;
    bool firstOne = false;

    // NX conflict while the entry is within its ttl must keep the ttl task.
    newMeta.set_existence(ExistenceOptPb::NX);
    auto rc = manager_->CreateMeta(newMeta, WORKER_ADDRESS, {}, newVersion, firstOne);
    EXPECT_EQ(rc.GetCode(), K_OC_KEY_ALREADY_EXIST);
    EXPECT_TRUE(HasExpiryTask(objectKey));

    // A create rejected later by the format check must keep the ttl task as well.
    newMeta.set_existence(ExistenceOptPb::NONE);
    newMeta.mutable_config()->set_write_mode(static_cast<uint32_t>(WriteMode::WRITE_THROUGH_L2_CACHE));
    rc = manager_->CreateMeta(newMeta, WORKER_ADDRESS, {}, newVersion, firstOne);
    EXPECT_EQ(rc.GetCode(), K_INVALID);
    EXPECT_TRUE(HasExpiryTask(objectKey));
}

TEST_F(OCMetadataManagerExistenceOptTest, RejectedUpdateKeepsExpiryTaskAndZeroTtlCancelsIt)
{
    const std::string objectKey = "ttl-strip-update-entry";
    const uint64_t version = static_cast<uint64_t>(GetSystemClockTimeStampUs());
    ObjectMeta entry = MakeEntry(version, TTL_SECOND);
    entry.meta.set_object_key(objectKey);
    SeedTableEntry(objectKey, entry);
    DS_ASSERT_OK(manager_->expiredObjectManager_->InsertObject(objectKey, version, TTL_SECOND));

    UpdateMetaReqPb req;
    req.set_object_key(objectKey);
    req.set_address(WORKER_ADDRESS);
    req.set_ttl_second(0);
    auto *params = req.mutable_binary_format_params();
    params->set_write_mode(static_cast<uint32_t>(WriteMode::NONE_L2_CACHE));
    params->set_data_format(static_cast<uint32_t>(DataFormat::BINARY));
    UpdateMetaRspPb rsp;

    // A format-mismatched update is rejected before the strip and keeps the ttl task.
    params->set_write_mode(static_cast<uint32_t>(WriteMode::WRITE_THROUGH_L2_CACHE));
    auto rc = manager_->UpdateMeta(req, rsp);
    EXPECT_EQ(rc.GetCode(), K_INVALID);
    EXPECT_TRUE(HasExpiryTask(objectKey));

    // A committed update to ttl 0 cancels the old task instead of leaving it armed.
    params->set_write_mode(static_cast<uint32_t>(WriteMode::NONE_L2_CACHE));
    rc = manager_->UpdateMeta(req, rsp);
    EXPECT_EQ(rc.GetCode(), K_OK);
    EXPECT_FALSE(HasExpiryTask(objectKey));
}

TEST_F(OCMetadataManagerExistenceOptTest, CreateMetaTtlDeleteRaceLeavesNoPartialEntry)
{
    const std::string objectKey = "ttl-delete-race-entry";
    auto &expiredShard = manager_->expiredObjectManager_->shards_[
        manager_->expiredObjectManager_->GetShardIndex(objectKey)];
    expiredShard.readyExpiredObjects.insert(objectKey);

    ObjectMetaPb newMeta;
    newMeta.set_object_key(objectKey);
    newMeta.set_data_size(8);
    newMeta.set_ttl_second(TTL_SECOND);
    newMeta.mutable_config()->set_write_mode(static_cast<uint32_t>(WriteMode::NONE_L2_CACHE));
    newMeta.mutable_config()->set_data_format(static_cast<uint32_t>(DataFormat::BINARY));
    int64_t newVersion = 0;
    bool firstOne = false;

    auto rc = manager_->CreateMeta(newMeta, WORKER_ADDRESS, {}, newVersion, firstOne);

    EXPECT_EQ(rc.GetCode(), K_TRY_AGAIN);
    EXPECT_FALSE(HasMeta(objectKey));

    expiredShard.readyExpiredObjects.erase(objectKey);
    auto retryRc = manager_->CreateMeta(newMeta, WORKER_ADDRESS, {}, newVersion, firstOne);
    EXPECT_EQ(retryRc.GetCode(), K_OK);
    EXPECT_TRUE(HasMeta(objectKey));
    EXPECT_TRUE(HasExpiryTask(objectKey));
}

TEST_F(OCMetadataManagerExistenceOptTest, UpdateMetaDuringInflightTtlDeleteReturnsTryAgain)
{
    const std::string objectKey = "ttl-delete-race-update-entry";
    const uint64_t version = static_cast<uint64_t>(GetSystemClockTimeStampUs());
    ObjectMeta entry = MakeEntry(version, TTL_SECOND);
    entry.meta.set_object_key(objectKey);
    SeedTableEntry(objectKey, entry);
    auto &expiredShard =
        manager_->expiredObjectManager_->shards_[manager_->expiredObjectManager_->GetShardIndex(objectKey)];
    expiredShard.readyExpiredObjects.insert(objectKey);

    UpdateMetaReqPb req;
    req.set_object_key(objectKey);
    req.set_address(WORKER_ADDRESS);
    req.set_ttl_second(TTL_SECOND);
    auto *params = req.mutable_binary_format_params();
    params->set_write_mode(static_cast<uint32_t>(WriteMode::NONE_L2_CACHE));
    params->set_data_format(static_cast<uint32_t>(DataFormat::BINARY));
    UpdateMetaRspPb rsp;

    auto rc = manager_->UpdateMeta(req, rsp);

    uint64_t currentVersion = 0;
    EXPECT_EQ(rc.GetCode(), K_TRY_AGAIN);
    ASSERT_TRUE(GetMetaVersion(objectKey, currentVersion));
    EXPECT_EQ(currentVersion, version);
    EXPECT_TRUE(expiredShard.readyExpiredObjects.count(objectKey) > 0);
}

TEST_F(OCMetadataManagerExistenceOptTest, FailedUpdateRestoresOriginalExpiryDeadline)
{
    const std::string objectKey = "ttl-restore-update-entry";
    const uint64_t version = static_cast<uint64_t>(GetSystemClockTimeStampUs());
    ObjectMeta entry = MakeEntry(version, TTL_SECOND);
    entry.meta.set_object_key(objectKey);
    SeedTableEntry(objectKey, entry);
    DS_ASSERT_OK(manager_->expiredObjectManager_->InsertObject(objectKey, version, TTL_SECOND));
    uint64_t seededDeadline = 0;
    ASSERT_TRUE(GetExpiryTaskDeadline(objectKey, seededDeadline));

    DS_ASSERT_OK(inject::Set("master.cache_invalid_failed", "return(K_RUNTIME_ERROR)"));
    UpdateMetaReqPb req;
    req.set_object_key(objectKey);
    req.set_address(WORKER_ADDRESS);
    req.set_ttl_second(60);
    auto *params = req.mutable_binary_format_params();
    params->set_write_mode(static_cast<uint32_t>(WriteMode::NONE_L2_CACHE));
    params->set_data_format(static_cast<uint32_t>(DataFormat::BINARY));
    UpdateMetaRspPb rsp;

    auto rc = manager_->UpdateMeta(req, rsp);

    uint64_t restoredDeadline = 0;
    EXPECT_EQ(rc.GetCode(), K_RUNTIME_ERROR);
    EXPECT_TRUE(GetExpiryTaskDeadline(objectKey, restoredDeadline));
    EXPECT_EQ(restoredDeadline, seededDeadline);
}
}  // namespace
}  // namespace datasystem::master
