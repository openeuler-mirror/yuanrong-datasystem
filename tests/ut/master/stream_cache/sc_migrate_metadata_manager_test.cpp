/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
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
 * Description: Test SCMigrateMetadataManager.
 */

#include <gtest/gtest.h>
#include <memory>
#include "ut/common.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/master/stream_cache/sc_migrate_metadata_manager.h"
#include "../../../common/binmock/binmock.h"
#include "datasystem/utils/status.h"
#include "datasystem/common/log/logging.h"

using namespace datasystem::master;
using namespace ::testing;

namespace datasystem {
namespace ut {

class SCMetadataManagerForMigrationTest : public SCMetadataManager {
public:
    SCMetadataManagerForMigrationTest()
        : SCMetadataManager(HostPort(), nullptr, nullptr, nullptr, nullptr, false, HostPort(), nullptr,
                            "migration-test")
    {
    }

    void MarkMigrating(const std::string &streamName)
    {
        migratingItems_.insert({ streamName, true });
    }
};

class SCMigrateMetadataManagerTest : public CommonTest {
public:
    void SetUp() override
    {
        Logging::GetInstance()->Start("ds_llt", true, 1);
    }

    void MigrateMetaDataWithRetry(SCMigrateMetadataManager::MigrateMetaInfo &info)
    {
        datasystem::inject::Set("SCMigrateMetadataManager.MigrateMetaDataWithRetry.interval", "call(5)");
        BINEXPECT_CALL(&SCMigrateMetadataManager::MigrateMetaData, (_, _))
            .WillRepeatedly(Invoke(this, &SCMigrateMetadataManagerTest::MockMigrateMetaDataFailed));
        HostPort hostPort;
        auto scMetadataManager = std::make_shared<master::SCMetadataManager>(
            hostPort, nullptr, nullptr, nullptr, nullptr, false, HostPort(), nullptr, "");
        DS_ASSERT_OK(migrateManager_.MigrateMetaDataWithRetry(scMetadataManager, info, true));
    }

    Status MockMigrateMetaDataFailed(const std::shared_ptr<master::SCMetadataManager> &scMetadataManager,
                               SCMigrateMetadataManager::MigrateMetaInfo &info)
    {
        (void)scMetadataManager;
        info.failedStreamNames = info.streamNames;
        return Status::OK();
    }

    void MigrateMetaDataWithError(SCMigrateMetadataManager::MigrateMetaInfo &info)
    {
        BINEXPECT_CALL(&SCMigrateMetadataManager::MigrateMetaData, (_, _))
            .WillRepeatedly(Invoke(this, &SCMigrateMetadataManagerTest::MockMigrateMetaDataRetError));
        HostPort hostPort;
        auto scMetadataManager = std::make_shared<master::SCMetadataManager>(
            hostPort, nullptr, nullptr, nullptr, nullptr, false, HostPort(), nullptr, "");
        ASSERT_EQ(migrateManager_.MigrateMetaData(scMetadataManager, info).GetCode(), K_RUNTIME_ERROR);
    }

    Status MockMigrateMetaDataRetError(const std::shared_ptr<master::SCMetadataManager> &scMetadataManager,
                                       SCMigrateMetadataManager::MigrateMetaInfo &info)
    {
        (void)scMetadataManager;
        info.failedStreamNames = info.streamNames;
        return Status{ K_RUNTIME_ERROR, "runtime error" };
    }

    void VerifyTopologyMigrationDeadlineReplay()
    {
        SCMigrateMetadataManager::MigrateMetaInfo info;
        info.destAddr = "127.0.0.1:1";
        info.operationId = "task-operation";
        const auto key = std::make_pair(info.destAddr, info.operationId);
        std::promise<std::pair<Status, std::vector<std::string>>> result;
        TbbFutureThreadTable::accessor accessor;
        migrateManager_.futureThread_.emplace(accessor, key, result.get_future());
        accessor.release();
        cluster::CancellationToken cancellation;
        auto rc = migrateManager_.RunTopologyMigration(nullptr, info, std::chrono::steady_clock::now(), cancellation);
        EXPECT_EQ(rc.GetCode(), K_RPC_DEADLINE_EXCEEDED);
        result.set_value({ Status::OK(), {} });
        rc = migrateManager_.RunTopologyMigration(
            nullptr, info, std::chrono::steady_clock::now() + std::chrono::seconds(1), cancellation);
        EXPECT_TRUE(rc.IsOk()) << rc.ToString();
        EXPECT_TRUE(migrateManager_.futureThread_.empty());
    }

    void VerifyTopologyMigrationRejectsPartialItemFailure()
    {
        SCMigrateMetadataManager::MigrateMetaInfo info;
        info.destAddr = "127.0.0.1:1";
        info.operationId = "task-operation-partial-failure";
        const auto key = std::make_pair(info.destAddr, info.operationId);
        std::promise<std::pair<Status, std::vector<std::string>>> result;
        TbbFutureThreadTable::accessor accessor;
        migrateManager_.futureThread_.emplace(accessor, key, result.get_future());
        accessor.release();
        result.set_value({ Status::OK(), { "stream1" } });

        cluster::CancellationToken cancellation;
        auto rc = migrateManager_.RunTopologyMigration(
            nullptr, info, std::chrono::steady_clock::now() + std::chrono::seconds(1), cancellation);
        EXPECT_EQ(rc.GetCode(), K_TRY_AGAIN);
        EXPECT_TRUE(migrateManager_.futureThread_.empty());
    }

protected:
    SCMigrateMetadataManager migrateManager_;
};

TEST_F(SCMigrateMetadataManagerTest, MigrateLimitedRetry)
{
    SCMigrateMetadataManager::MigrateMetaInfo info;
    info.destAddr = "127.0.0.1:1";
    info.streamNames.emplace_back("stream1");
    info.streamNames.emplace_back("stream2");
    info.streamNames.emplace_back("stream3");
    MigrateMetaDataWithRetry(info);
}

TEST_F(SCMigrateMetadataManagerTest, MigrateMeetError)
{
    SCMigrateMetadataManager::MigrateMetaInfo info;
    info.destAddr = "127.0.0.1:1";
    info.streamNames.emplace_back("stream1");
    info.streamNames.emplace_back("stream2");
    info.streamNames.emplace_back("stream3");
    MigrateMetaDataWithError(info);
}

TEST_F(SCMigrateMetadataManagerTest, TopologyMigrationDeadlineKeepsOwnedFutureForReplay)
{
    VerifyTopologyMigrationDeadlineReplay();
}

TEST_F(SCMigrateMetadataManagerTest, TopologyMigrationRejectsPartialItemFailure)
{
    VerifyTopologyMigrationRejectsPartialItemFailure();
}

TEST_F(SCMigrateMetadataManagerTest, MigrationFailureClearsMovingMarker)
{
    SCMetadataManagerForMigrationTest metadataManager;
    const std::string streamName = "failed-stream";
    metadataManager.MarkMigrating(streamName);
    ASSERT_TRUE(metadataManager.ItemIsMigrating(streamName));
    MetaForSCMigrationPb metadata;
    metadata.mutable_meta()->set_stream_name(streamName);

    metadataManager.HandleMetaDataMigrationFailed(metadata);

    EXPECT_FALSE(metadataManager.ItemIsMigrating(streamName));
}

}  // namespace ut
}  // namespace datasystem
