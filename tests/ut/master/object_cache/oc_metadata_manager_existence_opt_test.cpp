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
#include <algorithm>
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
                                                       LOCAL_ADDRESS, nullptr, nullptr,
                                                       LOCAL_ADDRESS, &localExiting_, "workerId");
        DS_ASSERT_OK(manager_->objectStore_->Init());
        manager_->nestedRefManager_ = std::make_unique<OCNestedManager>(manager_->objectStore_);
        manager_->notifyWorkerManager_ =
            std::make_unique<OCNotifyWorkerManager>(manager_->objectStore_, true, akSkManager_, manager_.get());
        manager_->expiredObjectManager_ = std::make_unique<ExpiredObjectManager>(LOCAL_ADDRESS, manager_.get());
    }

    void TearDown() override
    {
        (void)inject::Clear("master.cache_invalid_failed");
        (void)inject::Clear("ObjectMetaStore.RemoveObjectLocation");
        (void)inject::Clear("ObjectMetaStore.AddNestedRelationship");
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

TEST_F(OCMetadataManagerExistenceOptTest, CreateMetaRejectsLegacyHashMapFormat)
{
    const std::string objectKey = "legacy-hash-format-create";
    ObjectMetaPb newMeta;
    newMeta.set_object_key(objectKey);
    newMeta.set_data_size(8);
    newMeta.mutable_config()->set_write_mode(static_cast<uint32_t>(WriteMode::NONE_L2_CACHE));
    newMeta.mutable_config()->set_data_format(static_cast<uint32_t>(DataFormat::HASH_MAP));
    int64_t version = 0;
    bool firstOne = false;

    auto rc = manager_->CreateMeta(newMeta, WORKER_ADDRESS, {}, version, firstOne);

    EXPECT_EQ(rc.GetCode(), K_INVALID);
    EXPECT_FALSE(HasMeta(objectKey));
}

TEST_F(OCMetadataManagerExistenceOptTest, CreateCopyMetaRejectsLegacyHashMapFormat)
{
    CreateCopyMetaReqPb request;
    request.set_object_key("legacy-hash-format-copy");
    request.set_address(WORKER_ADDRESS);
    request.set_data_format(static_cast<uint32_t>(DataFormat::HASH_MAP));
    CreateCopyMetaRspPb response;

    auto rc = manager_->CreateCopyMeta(request, response);

    EXPECT_EQ(rc.GetCode(), K_INVALID);
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

TEST_F(OCMetadataManagerExistenceOptTest, NxCreateAllowedWhileKeyQueuedForTtlDelete)
{
    const std::string objectKey = "nx-queued-ttl-delete";
    const uint64_t version = static_cast<uint64_t>(GetSystemClockTimeStampUs());
    ObjectMeta entry = MakeEntry(version, TTL_SECOND);
    entry.meta.set_object_key(objectKey);
    entry.locations.clear();
    entry.locations[std::string(WORKER_ADDRESS)] = AckState::ACK;
    SeedTableEntry(objectKey, entry);
    DS_ASSERT_OK(manager_->expiredObjectManager_->InsertObject(objectKey, version, 0, true));
    ASSERT_TRUE(manager_->expiredObjectManager_->IsExpiredOrDeleting(objectKey));

    ObjectMetaPb newMeta;
    newMeta.set_object_key(objectKey);
    newMeta.set_data_size(8);
    newMeta.set_ttl_second(0);
    newMeta.mutable_config()->set_write_mode(static_cast<uint32_t>(WriteMode::NONE_L2_CACHE));
    newMeta.mutable_config()->set_data_format(static_cast<uint32_t>(DataFormat::BINARY));
    newMeta.set_existence(ExistenceOptPb::NX);
    int64_t newVersion = 0;
    bool firstOne = false;

    auto rc = manager_->CreateMeta(newMeta, WORKER_ADDRESS, {}, newVersion, firstOne);

    EXPECT_EQ(rc.GetCode(), K_OK);
    uint64_t currentVersion = 0;
    ASSERT_TRUE(GetMetaVersion(objectKey, currentVersion));
    EXPECT_GT(currentVersion, version);
    EXPECT_FALSE(HasExpiryTask(objectKey));
}

TEST_F(OCMetadataManagerExistenceOptTest, NxCreateDuringInflightTtlDeleteReturnsTryAgain)
{
    const std::string objectKey = "nx-inflight-ttl-delete";
    const uint64_t version = static_cast<uint64_t>(GetSystemClockTimeStampUs());
    ObjectMeta entry = MakeEntry(version, TTL_SECOND);
    entry.meta.set_object_key(objectKey);
    SeedTableEntry(objectKey, entry);
    auto &expiredShard =
        manager_->expiredObjectManager_->shards_[manager_->expiredObjectManager_->GetShardIndex(objectKey)];
    expiredShard.readyExpiredObjects.insert(objectKey);
    ASSERT_TRUE(manager_->expiredObjectManager_->IsExpiredOrDeleting(objectKey));

    ObjectMetaPb newMeta;
    newMeta.set_object_key(objectKey);
    newMeta.set_data_size(8);
    newMeta.set_ttl_second(TTL_SECOND);
    newMeta.mutable_config()->set_write_mode(static_cast<uint32_t>(WriteMode::NONE_L2_CACHE));
    newMeta.mutable_config()->set_data_format(static_cast<uint32_t>(DataFormat::BINARY));
    newMeta.set_existence(ExistenceOptPb::NX);
    int64_t newVersion = 0;
    bool firstOne = false;

    auto rc = manager_->CreateMeta(newMeta, WORKER_ADDRESS, {}, newVersion, firstOne);

    EXPECT_EQ(rc.GetCode(), K_TRY_AGAIN);
    uint64_t currentVersion = 0;
    ASSERT_TRUE(GetMetaVersion(objectKey, currentVersion));
    EXPECT_EQ(currentVersion, version);
    EXPECT_TRUE(expiredShard.readyExpiredObjects.count(objectKey) > 0);
    expiredShard.readyExpiredObjects.erase(objectKey);
}

TEST_F(OCMetadataManagerExistenceOptTest, MSetDuringInflightTtlDeleteArbitratesBeforeMutation)
{
    // The MSet path (CreateMultiMetaNtx -> CreateMeta(objectKey, ObjectMeta&, ...) -> UpdateMeta) must
    // reject an in-flight ttl delete before mutating metadata; a late rejection would leave the meta
    // advanced to a version pointing at data that was never written.
    const std::string objectKey = "ttl-inflight-mset-entry";
    const uint64_t version = static_cast<uint64_t>(GetSystemClockTimeStampUs());
    ObjectMeta entry = MakeEntry(version, TTL_SECOND);
    entry.meta.set_object_key(objectKey);
    SeedTableEntry(objectKey, entry);
    auto &expiredShard =
        manager_->expiredObjectManager_->shards_[manager_->expiredObjectManager_->GetShardIndex(objectKey)];
    expiredShard.readyExpiredObjects.insert(objectKey);
    ASSERT_TRUE(manager_->expiredObjectManager_->IsExpiredOrDeleting(objectKey));

    ObjectMeta newMeta;
    newMeta.meta.set_object_key(objectKey);
    newMeta.meta.set_data_size(8);
    newMeta.meta.set_ttl_second(TTL_SECOND);
    newMeta.meta.mutable_config()->set_write_mode(static_cast<uint32_t>(WriteMode::NONE_L2_CACHE));
    newMeta.meta.mutable_config()->set_data_format(static_cast<uint32_t>(DataFormat::BINARY));
    newMeta.meta.set_existence(ExistenceOptPb::NX);
    int64_t newVersion = static_cast<int64_t>(GetSystemClockTimeStampUs());
    bool firstOne = false;

    auto rc = manager_->CreateMeta(objectKey, newMeta, WORKER_ADDRESS, newVersion, firstOne);

    EXPECT_EQ(rc.GetCode(), K_TRY_AGAIN);
    uint64_t currentVersion = 0;
    ASSERT_TRUE(GetMetaVersion(objectKey, currentVersion));
    EXPECT_EQ(currentVersion, version);
    {
        TbbMetaTable::const_accessor accessor;
        auto &shard = manager_->metaShards_[manager_->GetShardIndex(objectKey)];
        bthread::RWLockRdGuard lock(shard.mutex);
        ASSERT_TRUE(shard.table.find(accessor, objectKey));
        EXPECT_EQ(accessor->second.locations.size(), size_t(1));
    }
    EXPECT_TRUE(expiredShard.readyExpiredObjects.count(objectKey) > 0);
    expiredShard.readyExpiredObjects.erase(objectKey);
}

TEST_F(OCMetadataManagerExistenceOptTest, MSetInvalidationFailureRestoresOriginalExpiryDeadline)
{
    // A failed MSet overwrite must not install the candidate ttl over the still-live old value.
    const std::string objectKey = "ttl-restore-mset-failure-entry";
    const uint64_t version = static_cast<uint64_t>(GetSystemClockTimeStampUs());
    ObjectMeta entry = MakeEntry(version, TTL_SECOND);
    entry.meta.set_object_key(objectKey);
    SeedTableEntry(objectKey, entry);
    DS_ASSERT_OK(manager_->expiredObjectManager_->InsertObject(objectKey, version, TTL_SECOND));
    uint64_t seededDeadline = 0;
    ASSERT_TRUE(GetExpiryTaskDeadline(objectKey, seededDeadline));

    DS_ASSERT_OK(inject::Set("master.cache_invalid_failed", "return(K_RUNTIME_ERROR)"));
    ObjectMeta newMeta;
    newMeta.meta.set_object_key(objectKey);
    newMeta.meta.set_data_size(8);
    newMeta.meta.set_ttl_second(60);
    newMeta.meta.mutable_config()->set_write_mode(static_cast<uint32_t>(WriteMode::NONE_L2_CACHE));
    newMeta.meta.mutable_config()->set_data_format(static_cast<uint32_t>(DataFormat::BINARY));
    int64_t newVersion = 0;
    bool firstOne = false;

    auto rc = manager_->CreateMeta(objectKey, newMeta, WORKER_ADDRESS, newVersion, firstOne);

    EXPECT_EQ(rc.GetCode(), K_RUNTIME_ERROR);
    uint64_t currentVersion = 0;
    ASSERT_TRUE(GetMetaVersion(objectKey, currentVersion));
    EXPECT_EQ(currentVersion, version);
    uint64_t restoredDeadline = 0;
    EXPECT_TRUE(GetExpiryTaskDeadline(objectKey, restoredDeadline));
    EXPECT_EQ(restoredDeadline, seededDeadline);
}

TEST_F(OCMetadataManagerExistenceOptTest, MSetInvalidationFailureKeepsTtlWhenLocationCleanupFails)
{
    // The ttl rollback must not be bypassed when the best-effort location cleanup itself fails;
    // the caller still sees the original invalidation error, not the cleanup error.
    const std::string objectKey = "ttl-restore-mset-cleanup-failure-entry";
    const uint64_t version = static_cast<uint64_t>(GetSystemClockTimeStampUs());
    ObjectMeta entry = MakeEntry(version, TTL_SECOND);
    entry.meta.set_object_key(objectKey);
    SeedTableEntry(objectKey, entry);
    DS_ASSERT_OK(manager_->expiredObjectManager_->InsertObject(objectKey, version, TTL_SECOND));
    uint64_t seededDeadline = 0;
    ASSERT_TRUE(GetExpiryTaskDeadline(objectKey, seededDeadline));

    DS_ASSERT_OK(inject::Set("master.cache_invalid_failed", "return(K_RUNTIME_ERROR)"));
    DS_ASSERT_OK(inject::Set("ObjectMetaStore.RemoveObjectLocation", "return(K_INVALID)"));
    ObjectMeta newMeta;
    newMeta.meta.set_object_key(objectKey);
    newMeta.meta.set_data_size(8);
    newMeta.meta.set_ttl_second(60);
    newMeta.meta.mutable_config()->set_write_mode(static_cast<uint32_t>(WriteMode::NONE_L2_CACHE));
    newMeta.meta.mutable_config()->set_data_format(static_cast<uint32_t>(DataFormat::BINARY));
    int64_t newVersion = 0;
    bool firstOne = false;

    auto rc = manager_->CreateMeta(objectKey, newMeta, WORKER_ADDRESS, newVersion, firstOne);

    EXPECT_EQ(rc.GetCode(), K_RUNTIME_ERROR);
    uint64_t restoredDeadline = 0;
    EXPECT_TRUE(GetExpiryTaskDeadline(objectKey, restoredDeadline));
    EXPECT_EQ(restoredDeadline, seededDeadline);
}

TEST_F(OCMetadataManagerExistenceOptTest, SetOverwriteInvalidationFailureRestoresOriginalExpiryDeadline)
{
    // A failed single-object Set overwrite must not cancel the still-live old value's ttl cleanup:
    // the candidate request carries ttl 0, so without the rollback the old task would be gone forever.
    const std::string objectKey = "ttl-restore-set-overwrite-failure-entry";
    const uint64_t version = static_cast<uint64_t>(GetSystemClockTimeStampUs());
    ObjectMeta entry = MakeEntry(version, TTL_SECOND);
    entry.meta.set_object_key(objectKey);
    SeedTableEntry(objectKey, entry);
    DS_ASSERT_OK(manager_->expiredObjectManager_->InsertObject(objectKey, version, TTL_SECOND));
    uint64_t seededDeadline = 0;
    ASSERT_TRUE(GetExpiryTaskDeadline(objectKey, seededDeadline));

    DS_ASSERT_OK(inject::Set("master.cache_invalid_failed", "return(K_RUNTIME_ERROR)"));
    ObjectMetaPb newMeta;
    newMeta.set_object_key(objectKey);
    newMeta.set_data_size(8);
    newMeta.set_ttl_second(0);
    newMeta.mutable_config()->set_write_mode(static_cast<uint32_t>(WriteMode::NONE_L2_CACHE));
    newMeta.mutable_config()->set_data_format(static_cast<uint32_t>(DataFormat::BINARY));
    int64_t newVersion = 0;
    bool firstOne = false;

    auto rc = manager_->CreateMeta(newMeta, WORKER_ADDRESS, {}, newVersion, firstOne);

    EXPECT_FALSE(firstOne);
    EXPECT_EQ(rc.GetCode(), K_RUNTIME_ERROR);
    uint64_t currentVersion = 0;
    ASSERT_TRUE(GetMetaVersion(objectKey, currentVersion));
    EXPECT_EQ(currentVersion, version);
    uint64_t restoredDeadline = 0;
    EXPECT_TRUE(GetExpiryTaskDeadline(objectKey, restoredDeadline));
    EXPECT_EQ(restoredDeadline, seededDeadline);
}

TEST_F(OCMetadataManagerExistenceOptTest, SetOverwriteInvalidationFailureKeepsTtlWhenLocationCleanupFails)
{
    // Same as above, but the best-effort location cleanup also fails: the rollback must still run
    // and the caller must see the original invalidation error, not the cleanup error.
    const std::string objectKey = "ttl-restore-set-overwrite-cleanup-failure-entry";
    const uint64_t version = static_cast<uint64_t>(GetSystemClockTimeStampUs());
    ObjectMeta entry = MakeEntry(version, TTL_SECOND);
    entry.meta.set_object_key(objectKey);
    SeedTableEntry(objectKey, entry);
    DS_ASSERT_OK(manager_->expiredObjectManager_->InsertObject(objectKey, version, TTL_SECOND));
    uint64_t seededDeadline = 0;
    ASSERT_TRUE(GetExpiryTaskDeadline(objectKey, seededDeadline));

    DS_ASSERT_OK(inject::Set("master.cache_invalid_failed", "return(K_RUNTIME_ERROR)"));
    DS_ASSERT_OK(inject::Set("ObjectMetaStore.RemoveObjectLocation", "return(K_INVALID)"));
    ObjectMetaPb newMeta;
    newMeta.set_object_key(objectKey);
    newMeta.set_data_size(8);
    newMeta.set_ttl_second(60);
    newMeta.mutable_config()->set_write_mode(static_cast<uint32_t>(WriteMode::NONE_L2_CACHE));
    newMeta.mutable_config()->set_data_format(static_cast<uint32_t>(DataFormat::BINARY));
    int64_t newVersion = 0;
    bool firstOne = false;

    auto rc = manager_->CreateMeta(newMeta, WORKER_ADDRESS, {}, newVersion, firstOne);

    EXPECT_EQ(rc.GetCode(), K_RUNTIME_ERROR);
    uint64_t restoredDeadline = 0;
    EXPECT_TRUE(GetExpiryTaskDeadline(objectKey, restoredDeadline));
    EXPECT_EQ(restoredDeadline, seededDeadline);
}

TEST_F(OCMetadataManagerExistenceOptTest, SetOverwriteNestedRefFailureRestoresExpiryDeadline)
{
    // When the invalidation commits but registering the nested relationship fails, the failed
    // request must not leave the entry without ttl cleanup, and its nested keys must not be
    // registered (they would corrupt the dependency table consumed by dead-object cascades).
    const std::string objectKey = "ttl-restore-set-overwrite-nested-failure-entry";
    const uint64_t version = static_cast<uint64_t>(GetSystemClockTimeStampUs());
    ObjectMeta entry = MakeEntry(version, TTL_SECOND);
    entry.meta.set_object_key(objectKey);
    entry.meta.mutable_config()->set_is_replica(true);
    SeedTableEntry(objectKey, entry);
    DS_ASSERT_OK(manager_->expiredObjectManager_->InsertObject(objectKey, version, TTL_SECOND));
    uint64_t seededDeadline = 0;
    ASSERT_TRUE(GetExpiryTaskDeadline(objectKey, seededDeadline));

    DS_ASSERT_OK(inject::Set("ObjectMetaStore.AddNestedRelationship", "return(K_INVALID)"));
    ObjectMetaPb newMeta;
    newMeta.set_object_key(objectKey);
    newMeta.set_data_size(8);
    newMeta.set_ttl_second(60);
    newMeta.mutable_config()->set_write_mode(static_cast<uint32_t>(WriteMode::NONE_L2_CACHE));
    newMeta.mutable_config()->set_data_format(static_cast<uint32_t>(DataFormat::BINARY));
    newMeta.mutable_config()->set_is_replica(true);
    int64_t newVersion = 0;
    bool firstOne = false;
    std::set<ImmutableString> nestedKeys = { "ttl-restore-set-overwrite-nested-a" };

    auto rc = manager_->CreateMeta(newMeta, WORKER_ADDRESS, nestedKeys, newVersion, firstOne);

    EXPECT_EQ(rc.GetCode(), K_INVALID);
    uint64_t currentVersion = 0;
    ASSERT_TRUE(GetMetaVersion(objectKey, currentVersion));
    EXPECT_EQ(currentVersion, static_cast<uint64_t>(newVersion));
    uint64_t restoredDeadline = 0;
    EXPECT_TRUE(GetExpiryTaskDeadline(objectKey, restoredDeadline));
    EXPECT_EQ(restoredDeadline, seededDeadline);
    EXPECT_TRUE(manager_->nestedRefManager_->CheckIsNotParentId(objectKey));
}

TEST_F(OCMetadataManagerExistenceOptTest, EvictionRemoveMetaOlderVersionSkipsNewerMeta)
{
    const std::string objectKey = "eviction-remove-meta-stale-version";
    constexpr uint64_t staleVersion = 1000;
    constexpr uint64_t currentVersion = 2000;
    ObjectMeta entry;
    entry.meta.set_version(currentVersion);
    entry.meta.set_ttl_second(0);
    entry.meta.mutable_config()->set_write_mode(static_cast<uint32_t>(WriteMode::NONE_L2_CACHE_EVICT));
    entry.locations[std::string(LOCAL_ADDRESS)] = AckState::ACK;
    entry.locations[std::string(WORKER_ADDRESS)] = AckState::ACK;
    SeedTableEntry(objectKey, entry);

    RemoveMetaReqPb request;
    request.set_address(WORKER_ADDRESS);
    request.set_cause(RemoveMetaReqPb::EVICTION);
    request.add_ids(objectKey);
    auto *objectVersion = request.add_id_with_version();
    objectVersion->set_id(objectKey);
    objectVersion->set_version(staleVersion);
    RemoveMetaRspPb response;

    auto rc = manager_->RemoveMetaLocation(request, WORKER_ADDRESS, response);

    DS_ASSERT_OK(rc);
    ASSERT_EQ(response.success_ids_size(), 1);
    EXPECT_EQ(response.success_ids(0), objectKey);
    TbbMetaTable::const_accessor accessor;
    auto &shard = manager_->metaShards_[manager_->GetShardIndex(objectKey)];
    bthread::RWLockRdGuard lock(shard.mutex);
    ASSERT_TRUE(shard.table.find(accessor, objectKey));
    EXPECT_EQ(accessor->second.meta.version(), currentVersion);
    EXPECT_EQ(accessor->second.locations.size(), size_t(2));
}

ObjectMeta SeedTwoLocationEntry(uint64_t currentVersion)
{
    ObjectMeta entry;
    entry.meta.set_version(currentVersion);
    entry.meta.set_ttl_second(0);
    entry.meta.mutable_config()->set_write_mode(static_cast<uint32_t>(WriteMode::NONE_L2_CACHE_EVICT));
    entry.locations[std::string(LOCAL_ADDRESS)] = AckState::ACK;
    entry.locations[std::string(WORKER_ADDRESS)] = AckState::ACK;
    return entry;
}

TEST_F(OCMetadataManagerExistenceOptTest, RemoveMetaLocationStaleVersionKeepsNewerLocation)
{
    // Directly exercises the 3-arg overload used by the delete-notification ack path
    // (oc_notify_worker_manager.cpp): a stale ack version must not erase the location.
    const std::string objectKey = "remove-meta-location-stale-version";
    constexpr uint64_t staleVersion = 1000;
    constexpr uint64_t currentVersion = 2000;
    SeedTableEntry(objectKey, SeedTwoLocationEntry(currentVersion));

    auto rc = manager_->RemoveMetaLocation(objectKey, WORKER_ADDRESS, staleVersion);

    DS_ASSERT_OK(rc);
    TbbMetaTable::const_accessor accessor;
    auto &shard = manager_->metaShards_[manager_->GetShardIndex(objectKey)];
    bthread::RWLockRdGuard lock(shard.mutex);
    ASSERT_TRUE(shard.table.find(accessor, objectKey));
    EXPECT_EQ(accessor->second.meta.version(), currentVersion);
    EXPECT_EQ(accessor->second.locations.size(), size_t(2));
}

TEST_F(OCMetadataManagerExistenceOptTest, RemoveMetaLocationCurrentVersionRemovesLocation)
{
    const std::string objectKey = "remove-meta-location-current-version";
    constexpr uint64_t currentVersion = 2000;
    SeedTableEntry(objectKey, SeedTwoLocationEntry(currentVersion));

    auto rc = manager_->RemoveMetaLocation(objectKey, WORKER_ADDRESS, currentVersion);

    DS_ASSERT_OK(rc);
    TbbMetaTable::const_accessor accessor;
    auto &shard = manager_->metaShards_[manager_->GetShardIndex(objectKey)];
    bthread::RWLockRdGuard lock(shard.mutex);
    ASSERT_TRUE(shard.table.find(accessor, objectKey));
    EXPECT_EQ(accessor->second.locations.size(), size_t(1));
    EXPECT_EQ(accessor->second.locations.count(std::string(LOCAL_ADDRESS)), size_t(1));
}

TEST_F(OCMetadataManagerExistenceOptTest, RemoveMetaLocationMaxVersionBypassesGuard)
{
    // Documents why the ack path must forward the real version: a placeholder
    // UINT64_MAX (the removed default argument) always defeats the guard and erases
    // the location of a newer object.
    const std::string objectKey = "remove-meta-location-max-version";
    constexpr uint64_t currentVersion = 2000;
    SeedTableEntry(objectKey, SeedTwoLocationEntry(currentVersion));

    auto rc = manager_->RemoveMetaLocation(objectKey, WORKER_ADDRESS, UINT64_MAX);

    DS_ASSERT_OK(rc);
    TbbMetaTable::const_accessor accessor;
    auto &shard = manager_->metaShards_[manager_->GetShardIndex(objectKey)];
    bthread::RWLockRdGuard lock(shard.mutex);
    ASSERT_TRUE(shard.table.find(accessor, objectKey));
    EXPECT_EQ(accessor->second.locations.size(), size_t(1));
    EXPECT_EQ(accessor->second.locations.count(std::string(LOCAL_ADDRESS)), size_t(1));
}

TEST_F(OCMetadataManagerExistenceOptTest, TtlDeleteSnapshotSkipsRecreatedNewerVersion)
{
    const std::string objectKey = "ttl-snapshot-outdated-entry";
    constexpr uint64_t queuedVersion = 1000;
    constexpr uint64_t currentVersion = 2000;
    ObjectMeta entry = MakeEntry(currentVersion, TTL_SECOND);
    entry.meta.set_object_key(objectKey);
    SeedTableEntry(objectKey, entry);
    DeleteObjectMediator mediator(std::string(LOCAL_ADDRESS), { { objectKey, true } });
    mediator.SetObjKey2Version({ { objectKey, queuedVersion } });

    DeleteStruct replicas;
    auto rc = manager_->GetMetaInfoAndSetDeleting(objectKey, replicas, mediator);

    DS_ASSERT_OK(rc);
    EXPECT_TRUE(replicas.locations.empty());
    EXPECT_NE(std::find(mediator.GetOutdatedObjs().begin(), mediator.GetOutdatedObjs().end(), objectKey),
              mediator.GetOutdatedObjs().end());
}

TEST_F(OCMetadataManagerExistenceOptTest, ClearMetaInfoSkipsNewerVersionThanSnapshot)
{
    const std::string objectKey = "ttl-clear-version-advanced-entry";
    constexpr uint64_t snapshotVersion = 1000;
    constexpr uint64_t currentVersion = 2000;
    ObjectMeta entry = MakeEntry(snapshotVersion, TTL_SECOND);
    entry.meta.set_object_key(objectKey);
    SeedTableEntry(objectKey, entry);
    DeleteObjectMediator mediator(std::string(LOCAL_ADDRESS), { { objectKey, true } });
    mediator.SetObjKey2Version({ { objectKey, snapshotVersion } });
    DeleteStruct snapshot;
    snapshot.locations.emplace(std::string(LOCAL_ADDRESS));
    snapshot.version = snapshotVersion;
    snapshot.writeMode = static_cast<uint32_t>(WriteMode::NONE_L2_CACHE);
    std::unordered_map<std::string, DeleteStruct> sendAllDelObjs{ { objectKey, snapshot } };
    std::unordered_set<std::string> failedObjects;
    {
        TbbMetaTable::accessor accessor;
        auto &shard = manager_->metaShards_[manager_->GetShardIndex(objectKey)];
        bthread::RWLockWrGuard lock(shard.mutex);
        ASSERT_TRUE(shard.table.find(accessor, objectKey));
        accessor->second.meta.set_version(currentVersion);
    }

    auto rc = manager_->ClearMetaInfo(sendAllDelObjs, false, failedObjects, mediator);

    DS_ASSERT_OK(rc);
    EXPECT_EQ(failedObjects.count(objectKey), size_t(0));
    EXPECT_NE(std::find(mediator.GetOutdatedObjs().begin(), mediator.GetOutdatedObjs().end(), objectKey),
              mediator.GetOutdatedObjs().end());
    TbbMetaTable::const_accessor accessor;
    auto &shard = manager_->metaShards_[manager_->GetShardIndex(objectKey)];
    bthread::RWLockRdGuard lock(shard.mutex);
    ASSERT_TRUE(shard.table.find(accessor, objectKey));
    EXPECT_EQ(accessor->second.meta.version(), currentVersion);
}
}  // namespace
}  // namespace datasystem::master
