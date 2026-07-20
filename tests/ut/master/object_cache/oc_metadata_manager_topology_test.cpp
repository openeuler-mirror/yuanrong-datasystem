/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Test topology-fenced object metadata mutations.
 */
#include <atomic>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>

#include "ut/common.h"

#define private public
#include "datasystem/master/object_cache/oc_metadata_manager.h"
#undef private

DS_DECLARE_string(rocksdb_write_mode);

namespace datasystem::master {
namespace {
constexpr char LOCAL_ADDRESS[] = "127.0.0.1:30001";
constexpr char TARGET_ADDRESS[] = "127.0.0.1:30002";

class OCMetadataManagerTopologyTest : public ut::CommonTest {
public:
    void SetUp() override
    {
        oldWriteMode_ = FLAGS_rocksdb_write_mode;
        FLAGS_rocksdb_write_mode = "sync";
        rocksStore_ = RocksStore::GetInstance(ut::GetTestCaseDataDir() + "/rocksdb");
        akSkManager_ = std::make_shared<AkSkManager>(0);
        localExiting_.store(true);
    }

    void TearDown() override
    {
        rocksStore_.reset();
        FLAGS_rocksdb_write_mode = oldWriteMode_;
    }

    void InsertPrimaryWithCopy(OCMetadataManager &manager, const std::string &objectKey)
    {
        TbbMetaTable::accessor accessor;
        auto &shard = manager.metaShards_[manager.GetShardIndex(objectKey)];
        std::unique_lock<std::shared_timed_mutex> lock(shard.mutex);
        (void)shard.table.insert(accessor, objectKey);
        accessor->second.meta.set_object_key(objectKey);
        accessor->second.meta.set_primary_address(LOCAL_ADDRESS);
        accessor->second.locations[LOCAL_ADDRESS] = AckState::ACK;
        accessor->second.locations[TARGET_ADDRESS] = AckState::ACK;
    }

    std::string oldWriteMode_;
    std::shared_ptr<RocksStore> rocksStore_;
    std::shared_ptr<AkSkManager> akSkManager_;
    std::atomic<bool> localExiting_{ false };
};

TEST_F(OCMetadataManagerTopologyTest, TopologyOperationCanChangePrimaryWhileOrdinaryExitingRequestIsFenced)
{
    OCMetadataManager manager(akSkManager_, rocksStore_.get(), nullptr, nullptr, LOCAL_ADDRESS, nullptr, nullptr, false,
                              HostPort(), LOCAL_ADDRESS, &localExiting_, "workerId");
    DS_ASSERT_OK(manager.objectStore_->Init());
    const std::string objectKey = "topology_primary_handoff";
    InsertPrimaryWithCopy(manager, objectKey);

    EXPECT_EQ(manager.ChangePrimaryCopy(TARGET_ADDRESS, objectKey, "").GetCode(), K_TRY_AGAIN);
    {
        TbbMetaTable::accessor accessor;
        auto &shard = manager.metaShards_[manager.GetShardIndex(objectKey)];
        std::shared_lock<std::shared_timed_mutex> lock(shard.mutex);
        ASSERT_TRUE(shard.table.find(accessor, objectKey));
        EXPECT_EQ(accessor->second.meta.primary_address(), LOCAL_ADDRESS);
    }

    DS_ASSERT_OK(manager.ChangePrimaryCopy(TARGET_ADDRESS, objectKey, "scale-in-operation"));
    {
        TbbMetaTable::accessor accessor;
        auto &shard = manager.metaShards_[manager.GetShardIndex(objectKey)];
        std::shared_lock<std::shared_timed_mutex> lock(shard.mutex);
        ASSERT_TRUE(shard.table.find(accessor, objectKey));
        EXPECT_EQ(accessor->second.meta.primary_address(), TARGET_ADDRESS);
    }
}
}  // namespace
}  // namespace datasystem::master
