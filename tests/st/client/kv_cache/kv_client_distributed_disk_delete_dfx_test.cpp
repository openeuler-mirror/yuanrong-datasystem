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
 * Description: Reproduce distributed-disk delete failure during worker restart window.
 */

#include <memory>
#include <string>

#include <gtest/gtest.h>

#include "client/kv_cache/kv_client_scale_common.h"
#include "common.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/util/file_util.h"
#include "datasystem/common/util/format.h"
#include "datasystem/kv_client.h"
#include "datasystem/object/object_enum.h"

namespace datasystem {
namespace st {
namespace {
constexpr int NODE_TIMEOUT_S = 2;
constexpr int NODE_DEAD_TIMEOUT_S = 60;
constexpr int HEARTBEAT_INTERVAL_MS = 500;
}  // namespace

class KVClientDistributedDiskDeleteDfxTest : public KVClientScaleCommon {
public:
    void SetUp() override
    {
        CommonTest::SetUp();
        DS_ASSERT_OK(Init());
        ASSERT_TRUE(cluster_ != nullptr);
        DS_ASSERT_OK(cluster_->StartEtcdCluster());
        externalCluster_ = dynamic_cast<ExternalCluster *>(cluster_.get());
        InitTestEtcdInstance();
    }

    void TearDown() override
    {
        metaClient_.reset();
        primaryClient_.reset();
        restartedClient_.reset();
        ExternalClusterTest::TearDown();
    }

    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numEtcd = 1;
        opts.numWorkers = 2;
        opts.enableDistributedMaster = "true";
        opts.waitWorkerReady = false;
        opts.addNodeTime = SCALE_DOWN_ADD_TIME;
        distributedDiskPath_ = GetTestCaseDataDir() + "/distributed_disk";
        DS_ASSERT_OK(CreateDir(distributedDiskPath_, true));
        opts.workerGflagParams = FormatString(
            " -l2_cache_type=distributed_disk"
            " -distributed_disk_path=%s"
            " -distributed_disk_sync_interval_ms=0"
            " -distributed_disk_sync_batch_bytes=1"
            " -enable_metadata_recovery=false"
            " -enable_reconciliation=false"
            " -enable_l2_cache_fallback=false"
            " -auto_del_dead_node=false"
            " -heartbeat_interval_ms=%d"
            " -node_timeout_s=%d"
            " -node_dead_timeout_s=%d"
            " -v=1",
            distributedDiskPath_, HEARTBEAT_INTERVAL_MS, NODE_TIMEOUT_S, NODE_DEAD_TIMEOUT_S);
    }

protected:
    ExternalCluster *externalCluster_ = nullptr;
    std::shared_ptr<KVClient> metaClient_;
    std::shared_ptr<KVClient> primaryClient_;
    std::shared_ptr<KVClient> restartedClient_;
    std::string distributedDiskPath_;
};

TEST_F(KVClientDistributedDiskDeleteDfxTest, LEVEL1_DeleteFailsWhenPrimaryAddressClearedAfterRestart)
{
    DS_ASSERT_OK(externalCluster_->StartWorkerAndWaitReady({ 0, 1 }));
    WaitAllNodesJoinIntoHashRing(2, 20);

    InitTestKVClient(0, metaClient_);
    InitTestKVClient(1, primaryClient_);

    std::string key;
    DS_ASSERT_OK(metaClient_->GenerateKey("distributed_disk_restart_delete", key));
    std::string value = "value_" + GenRandomString(1024 * 1024);
    SetParam param{ .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE };
    DS_ASSERT_OK(primaryClient_->Set(key, value, param));

    std::string valueToGet;
    DS_ASSERT_OK(primaryClient_->Get(key, valueToGet));
    ASSERT_EQ(valueToGet, value);

    // Restart worker1 inside the restart window. Do not wait for passive scale-down to finish.
    DS_ASSERT_OK(externalCluster_->KillWorker(1));
    auto injectStr =
        " -client_reconnect_wait_s=1 "
        "-inject_actions=SlotRecoveryManager.ExecuteRecoveryTask.BeforeRecoverMeta:sleep(2000)";
    ASSERT_TRUE(cluster_->StartNode(WORKER, 1, injectStr).IsOk());
    ASSERT_TRUE(cluster_->WaitNodeReady(WORKER, 1, 40).IsOk());
    WaitAllNodesJoinIntoHashRing(2, 20);
    InitTestKVClient(1, restartedClient_);
    DS_ASSERT_OK(restartedClient_->Del(key));
}

}  // namespace st
}  // namespace datasystem
