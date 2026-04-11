/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
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
        std::shared_ptr<master::SCMetadataManager> scMetadataManager =
            std::make_shared<master::SCMetadataManager>(hostPort, nullptr, nullptr, nullptr, nullptr, "");
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
        std::shared_ptr<master::SCMetadataManager> scMetadataManager =
            std::make_shared<master::SCMetadataManager>(hostPort, nullptr, nullptr, nullptr, nullptr, "");
        ASSERT_EQ(migrateManager_.MigrateMetaData(scMetadataManager, info).GetCode(), K_RUNTIME_ERROR);
    }

    Status MockMigrateMetaDataRetError(const std::shared_ptr<master::SCMetadataManager> &scMetadataManager,
                                       SCMigrateMetadataManager::MigrateMetaInfo &info)
    {
        (void)scMetadataManager;
        info.failedStreamNames = info.streamNames;
        return Status{ K_RUNTIME_ERROR, "runtime error" };
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

}  // namespace ut
}  // namespace datasystem
