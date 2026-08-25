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
 * Description: Validate primary END_LIFE redirect convergence during an active scale-in.
 */
#include <chrono>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "client/object_cache/oc_client_common.h"
#include "common.h"
#include "common_distributed_ext.h"
#include "datasystem/common/log/log.h"
#include "datasystem/kv_client.h"

namespace datasystem {
namespace st {
namespace {
bool FileContainsTokens(const std::filesystem::path &path, const std::vector<std::string> &tokens)
{
    std::ifstream input(path);
    if (!input.is_open()) {
        return false;
    }
    std::string line;
    while (std::getline(input, line)) {
        bool matched = true;
        for (const auto &expectedText : tokens) {
            if (line.find(expectedText) == std::string::npos) {
                matched = false;
                break;
            }
        }
        if (matched) {
            return true;
        }
    }
    return false;
}
}  // namespace

class EvictPrimaryRedirectScaleTest : public OCClientCommon, public CommonDistributedExt {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numEtcd = 1;
        opts.numWorkers = 3;
        opts.enableDistributedMaster = "true";
        opts.waitWorkerReady = true;
        opts.workerGflagParams =
            "-shared_memory_size_mb=32 -node_timeout_s=5 -node_dead_timeout_s=8 -log_monitor=true -v=2";
    }

    void SetUp() override
    {
        CommonTest::SetUp();
        DS_ASSERT_OK(Init());
        ASSERT_TRUE(cluster_ != nullptr);
        DS_ASSERT_OK(cluster_->StartEtcdCluster());
        CommonDistributedExt::InitTestEtcdInstance();
        DS_ASSERT_OK(cluster_->StartWorkers());
        DS_ASSERT_OK(cluster_->WaitUntilClusterReadyOrTimeout(30));
        for (size_t i = 0; i < 3; ++i) {
            DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, i));
        }
        InitTestKVClient(1, client_, 60'000, false, 60'000);
        ObtainTokens();
        InitWorkersInfoMap({ 0, 1, 2 });
    }

    void TearDown() override
    {
        client_.reset();
        ExternalClusterTest::TearDown();
    }

protected:
    BaseCluster *GetCluster() override
    {
        return cluster_.get();
    }

    std::string FindKeyOwnedBy(int workerIndex, const std::string &prefix)
    {
        for (uint32_t i = 0; i < 10'000; ++i) {
            std::string objectKey = prefix + std::to_string(i);
            WorkerEntry owner;
            GetMetaLocationById(objectKey, { 0, 1, 2 }, owner);
            if (owner.index == workerIndex) {
                return objectKey;
            }
        }
        return "";
    }

    bool WorkerLogContains(size_t workerIndex, const std::vector<std::string> &tokens)
    {
        auto logDir =
            std::filesystem::path(cluster_->GetRootDir()) / ("worker" + std::to_string(workerIndex)) / "log";
        std::error_code ec;
        if (!std::filesystem::exists(logDir, ec)) {
            return false;
        }
        for (const auto &entry : std::filesystem::directory_iterator(logDir, ec)) {
            if (ec || !entry.is_regular_file()) {
                continue;
            }
            if (FileContainsTokens(entry.path(), tokens)) {
                return true;
            }
        }
        return false;
    }

    bool WaitWorkerLogContains(size_t workerIndex, const std::vector<std::string> &tokens,
                               std::chrono::seconds timeout)
    {
        auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline) {
            if (WorkerLogContains(workerIndex, tokens)) {
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        return false;
    }

    std::shared_ptr<KVClient> client_;
};

TEST_F(EvictPrimaryRedirectScaleTest, LEVEL1_PrimaryEndLifeConvergesDuringActiveScaleIn)
{
    HostPort leavingWorker;
    DS_ASSERT_OK(cluster_->GetWorkerAddr(0, leavingWorker));
    HostPort worker1;
    HostPort worker2;
    DS_ASSERT_OK(cluster_->GetWorkerAddr(1, worker1));
    DS_ASSERT_OK(cluster_->GetWorkerAddr(2, worker2));
    SetParam param;
    param.writeMode = WriteMode::NONE_L2_CACHE_EVICT;
    const std::string value(8 * 1024 * 1024, 'p');
    for (uint32_t i = 0; i < 2; ++i) {
        auto objectKey = FindKeyOwnedBy(0, "primary_redirect_target_" + std::to_string(i) + "_");
        ASSERT_FALSE(objectKey.empty());
        DS_ASSERT_OK(client_->Set(objectKey, value, param));
    }

    // Any candidate owner returns one injected first-hop redirect. The forwarded request has redirect=false, so the
    // target executes the real delete path instead of consuming its own redirect injection.
    DS_ASSERT_OK(cluster_->SetInjectAction(
        WORKER, 0, "redirect.query.delete", FormatString("100*call(%s)", worker2.ToString())));
    DS_ASSERT_OK(cluster_->SetInjectAction(
        WORKER, 1, "redirect.query.delete", FormatString("100*call(%s)", worker2.ToString())));
    DS_ASSERT_OK(cluster_->SetInjectAction(
        WORKER, 2, "redirect.query.delete", FormatString("100*call(%s)", worker1.ToString())));
    VoluntaryScaleDownInject(0);
    WaitClusterTopologyChange([&leavingWorker](const ClusterTopologyPb &topology) {
        auto member = topology.members().find(leavingWorker.ToString());
        return topology.has_active_batch() && topology.active_batch().type() == TypePb::SCALE_IN
               && member != topology.members().end() && member->second.state() == MembershipPb::LEAVING;
    });

    for (uint32_t i = 0; i < 20; ++i) {
        Status rc = client_->Set("primary_redirect_fill_" + std::to_string(i), value, param);
        if (rc.IsError()) {
            break;
        }
    }
    ASSERT_TRUE(WaitWorkerLogContains(
        1, { "PRIMARY_END_LIFE_DIAG stage=rpc_attempt", "attempt_kind=redirect" }, std::chrono::seconds(15)));
    EXPECT_FALSE(WorkerLogContains(1, { "Force deleting primary END_LIFE" }));

    WaitAllMembersJoinClusterTopology(2);
    DS_ASSERT_OK(client_->Set("primary_redirect_after_scale_in", "ok", param));
}

}  // namespace st
}  // namespace datasystem
