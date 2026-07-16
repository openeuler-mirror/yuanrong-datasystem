/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Test topology object metadata migration completion.
 */
#include "datasystem/master/object_cache/oc_migrate_metadata_manager.h"

#include <future>

#include <gtest/gtest.h>

#include "ut/common.h"

namespace datasystem::ut {

class OCMigrateMetadataManagerTest : public CommonTest {
public:
    void VerifyTopologyMigrationRejectsPartialItemFailure()
    {
        master::OCMigrateMetadataManager::MigrateMetaInfo info;
        info.destAddr = "127.0.0.1:1";
        info.operationId = "task-operation-partial-failure";
        const auto key = std::make_pair(info.destAddr, info.operationId);
        std::promise<std::pair<Status, std::vector<std::string>>> result;
        master::TbbFutureThreadTable::accessor accessor;
        migrateManager_.futureThread_.emplace(accessor, key, result.get_future());
        accessor.release();
        result.set_value({ Status::OK(), { "object1" } });

        cluster::CancellationToken cancellation;
        auto rc = migrateManager_.RunTopologyMigration(
            nullptr, info, std::chrono::steady_clock::now() + std::chrono::seconds(1), cancellation);
        EXPECT_EQ(rc.GetCode(), K_TRY_AGAIN);
        EXPECT_TRUE(migrateManager_.futureThread_.empty());
    }

protected:
    master::OCMigrateMetadataManager migrateManager_;
};

TEST_F(OCMigrateMetadataManagerTest, TopologyMigrationRejectsPartialItemFailure)
{
    VerifyTopologyMigrationRejectsPartialItemFailure();
}

}  // namespace datasystem::ut
