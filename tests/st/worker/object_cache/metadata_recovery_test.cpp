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
 * Description: End-to-end tests for object-cache metadata recovery.
 */

#include <csignal>

#include <chrono>
#include <sstream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "common.h"
#include "client/kv_cache/kv_client_scale_common.h"
#include "datasystem/common/util/file_util.h"

namespace datasystem {
namespace st {
namespace {
constexpr int WAIT_GET_TIMEOUT_MS = 15000;
constexpr int WAIT_GET_INTERVAL_MS = 200;
constexpr uint64_t NODE_TIMEOUT_S = 1;
constexpr uint64_t NODE_DEAD_TIMEOUT_S = 3;
constexpr uint64_t HEARTBEAT_INTERVAL_MS = 500;
constexpr uint32_t OBJECT_COUNT = 64;
}  // namespace

class MetadataRecoveryTest : public KVClientScaleCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numEtcd = 1;
        opts.numWorkers = 2;
        opts.enableDistributedMaster = "true";
        opts.addNodeTime = 0;
        distributedDiskPath_ = testCasePath_ + "/distributed_disk";
        DS_ASSERT_OK(CreateDir(distributedDiskPath_, true));
        std::stringstream ss;
        ss << "-l2_cache_type=distributed_disk "
           << "-distributed_disk_path=" << distributedDiskPath_ << " "
           << "-enable_metadata_recovery=true "
           << "-enable_reconciliation=false "
           << "-heartbeat_interval_ms=" << HEARTBEAT_INTERVAL_MS << " "
           << "-node_timeout_s=" << NODE_TIMEOUT_S << " "
           << "-node_dead_timeout_s=" << NODE_DEAD_TIMEOUT_S << " "
           << "-v=1 "
           << "-enable_l2_cache_fallback=false";
        opts.workerGflagParams = ss.str();
    }

protected:
    bool WaitUntilGetSucceeds(const std::shared_ptr<KVClient> &client, const std::string &key,
                              const std::string &expectedValue) const
    {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(WAIT_GET_TIMEOUT_MS);
        while (std::chrono::steady_clock::now() < deadline) {
            std::string value;
            auto rc = client->Get(key, value);
            if (rc.IsOk()) {
                return value == expectedValue;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(WAIT_GET_INTERVAL_MS));
        }
        return false;
    }

    std::string distributedDiskPath_;
    static constexpr int timeoutMs_ = 5'000;
};

TEST_F(MetadataRecoveryTest, MetadataOwnerRestart)
{
    std::shared_ptr<KVClient> client0;
    InitTestKVClient(0, client0, timeoutMs_);

    SetParam param{ .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE };
    std::vector<std::pair<std::string, std::string>> keyValues;
    for (uint32_t i = 0; i < OBJECT_COUNT; ++i) {
        auto key = "metadata_recovery_" + std::to_string(i);
        auto value = GenRandomString(128);
        DS_ASSERT_OK(client0->Set(key, value, param));
        keyValues.emplace_back(std::move(key), std::move(value));
    }

    DS_ASSERT_OK(cluster_->KillWorker(1));

    DS_ASSERT_OK(cluster_->StartNode(WORKER, 1, ""));
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 1));
    WaitAllNodesJoinIntoHashRing(2, 20);

    std::shared_ptr<KVClient> client1;
    InitTestKVClient(1, client1, timeoutMs_);
    for (const auto &keyValue : keyValues) {
        ASSERT_TRUE(WaitUntilGetSucceeds(client1, keyValue.first, keyValue.second)) << keyValue.first;
    }
}
}  // namespace st
}  // namespace datasystem
