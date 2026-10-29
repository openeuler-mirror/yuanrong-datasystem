/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
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
#include "datasystem/common/util/file_util.h"
#include "client/kv_cache/kv_client_scale_common.h"

namespace datasystem {
namespace st {
namespace {
constexpr int WAIT_GET_TIMEOUT_MS = 20'000;
constexpr int WAIT_GET_INTERVAL_MS = 200;
constexpr uint64_t NODE_TIMEOUT_S = 1;
constexpr uint64_t NODE_DEAD_TIMEOUT_S = 3;
constexpr int S2MS = 1000;
}  // namespace

class MetadataRecoveryTest : public KVClientScaleCommon, public testing::WithParamInterface<bool> {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        const bool useCoordinator = GetParam();
        opts.numEtcd = useCoordinator ? 0 : 1;
        opts.numCoordinators = useCoordinator ? 1 : 0;
        opts.numWorkers = 2;
        opts.enableDistributedMaster = "true";
        std::stringstream ss;
        ss << "-enable_reconciliation=true "
           << "-node_timeout_s=" << NODE_TIMEOUT_S << " "
           << "-node_dead_timeout_s=" << NODE_DEAD_TIMEOUT_S << " "
           << "-v=1 "
           << "-enable_l2_cache_fallback=false";
        opts.workerGflagParams = ss.str();
        if (useCoordinator) {
            opts.coordinatorGflagParams = "-node_dead_timeout_s=" + std::to_string(NODE_DEAD_TIMEOUT_S);
        }
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

    static constexpr int timeoutMs_ = 5'000;
};

TEST_P(MetadataRecoveryTest, MetadataOwnerRestartRecoversMetadataAndHonorsTtl)
{
    std::shared_ptr<KVClient> client0;
    std::shared_ptr<KVClient> client1;
    InitTestKVClient(0, client0, timeoutMs_);
    InitTestKVClient(1, client1, timeoutMs_);

    std::string objKey = client1->GenerateKey("metadata_owner_restart_worker1");
    auto value = GenRandomString(10);
    DS_ASSERT_OK(client0->Set(objKey, value));

    constexpr int ttl = 2;
    datasystem::SetParam param{ .ttlSecond = ttl };
    std::string ttlObjKey = client1->GenerateKey("metadata_owner_restart_ttl_worker1");
    DS_ASSERT_OK(client0->Set(ttlObjKey, value, param));

    client1.reset();
    DS_ASSERT_OK(cluster_->KillWorker(1));
    DS_ASSERT_OK(RemoveAll(cluster_->GetRootDir() + "/worker1/rocksdb"));

    DS_ASSERT_OK(cluster_->StartNode(WORKER, 1, ""));
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 1));
    WaitAllMembersJoinClusterTopology(2, 20);

    InitTestKVClient(1, client1, timeoutMs_);
    ASSERT_TRUE(WaitUntilGetSucceeds(client1, objKey, value)) << objKey;
    std::this_thread::sleep_for(std::chrono::milliseconds((ttl + 1) * S2MS));
    std::string ttlValue;
    ASSERT_EQ(client1->Get(ttlObjKey, ttlValue).GetCode(), K_NOT_FOUND);
}

INSTANTIATE_TEST_SUITE_P(EtcdAndCoordinator, MetadataRecoveryTest, testing::Bool(),
                         [](const testing::TestParamInfo<bool> &info) {
                             return info.param ? "Coordinator" : "Etcd";
                         });
}  // namespace st
}  // namespace datasystem
