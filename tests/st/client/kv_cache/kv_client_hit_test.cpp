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

#include <gtest/gtest.h>
#include <algorithm>
#include <string>
#include <vector>

#include "common.h"
#include "client/object_cache/oc_client_common.h"
#include "datasystem/common/util/format.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace st {
class KVClientHitTest : public OCClientCommon {
public:
    std::vector<std::string> workerAddress_;

    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        auto workerNum = 3;
        opts.numOBS = 1;
        opts.numWorkers = workerNum;
        opts.numEtcd = 1;
        std::string hostIp = "127.0.0.1";
        opts.workerConfigs.emplace_back(hostIp, GetFreePort());
        for (auto addr : opts.workerConfigs) {
            workerAddress_.emplace_back(addr.ToString());
        }
        opts.workerGflagParams =
            "-log_async=false -log_monitor_interval_ms=1000 -shared_memory_size_mb=1 -skip_authenticate=true -v=1 ";
        opts.enableSpill = true;
        opts.numEtcd = 1;
    }

    void SetUp() override
    {
        ExternalClusterTest::SetUp();
        DS_ASSERT_OK(cluster_->GetWorkerAddr(0, workerAddr_));
    }
    Status StrInResLog(int workerIdx, const std::string &str)
    {
        auto resLog = FormatString("%s/worker%d/log/resource.log", cluster_->GetRootDir(), workerIdx);
        std::ifstream ifs(resLog);
        std::stringstream buffer;
        buffer << ifs.rdbuf();
        CHECK_FAIL_RETURN_STATUS((buffer.str().find(str) != std::string::npos), K_NOT_FOUND, "not find string");
        return Status::OK();
    }

protected:
    HostPort workerAddr_;
    int logInterval = 1;
    int kb800 = 800 * 1024;
};

TEST_F(KVClientHitTest, MemHitAndMiss)
{
    std::shared_ptr<KVClient> client0, client1;
    InitTestKVClient(0, client0, [&](ConnectOptions &opts) { (void)opts; });
    InitTestKVClient(1, client1, [&](ConnectOptions &opts) { (void)opts; });
    cluster_->SetInjectAction(ClusterNodeType::WORKER, 0, "hitinfo.prefix", "call()");
    cluster_->SetInjectAction(ClusterNodeType::WORKER, 1, "hitinfo.prefix", "call()");
    auto key = "mem_hit";
    client0->Set(key, std::string("a", kb800));
    std::string getVal;
    client1->Get(key, getVal);
    client0->Get(key, getVal);
    client0->Get("miss_key", getVal);
    client1->Get("miss_key", getVal);
    // make sure the print of resource log finish
    sleep(logInterval);
    DS_ASSERT_OK(StrInResLog(0, "hit_info:1/0/0/0/1"));
    DS_ASSERT_OK(StrInResLog(1, "hit_info:0/0/0/1/1"));
}

TEST_F(KVClientHitTest, DiskHit)
{
    std::shared_ptr<KVClient> client0, client1;
    InitTestKVClient(0, client0, [&](ConnectOptions &opts) { (void)opts; });
    cluster_->SetInjectAction(ClusterNodeType::WORKER, 0, "hitinfo.prefix", "call()");
    auto key = "disk_hit";
    client0->Set(key, std::string("a", kb800), SetParam{ .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE });
    client0->Set("spill", std::string("a", kb800), SetParam{ .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE });
    std::string getVal;
    client0->Get(key, getVal);
    // make sure the print of resource log finish
    sleep(logInterval);
    DS_ASSERT_OK(StrInResLog(0, "hit_info:0/1/0/0/0"));
}

TEST_F(KVClientHitTest, L2Hit)
{
    std::shared_ptr<KVClient> client0, client1;
    InitTestKVClient(0, client0, [&](ConnectOptions &opts) { (void)opts; });
    cluster_->SetInjectAction(ClusterNodeType::WORKER, 0, "hitinfo.prefix", "call()");
    cluster_->SetInjectAction(ClusterNodeType::WORKER, 0, "set.objectIsInvalidInmem", "call()");
    cluster_->SetInjectAction(ClusterNodeType::WORKER, 0, "set.objectIsInComplete", "call()");
    auto key = "l2_hit";
    client0->Set(key, std::string("a", kb800), SetParam{ .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE });
    std::string getVal;
    client0->Get(key, getVal);
    // make sure the print of resource log finish
    sleep(logInterval);
    DS_ASSERT_OK(StrInResLog(0, "hit_info:0/0/1/0/0"));
}

TEST_F(KVClientHitTest, L2RemoteHit)
{
    std::shared_ptr<KVClient> client0, client1;
    InitTestKVClient(0, client0, [&](ConnectOptions &opts) { (void)opts; });
    InitTestKVClient(1, client1, [&](ConnectOptions &opts) { (void)opts; });
    cluster_->SetInjectAction(ClusterNodeType::WORKER, 0, "hitinfo.prefix", "call()");
    cluster_->SetInjectAction(ClusterNodeType::WORKER, 1, "hitinfo.prefix", "call()");
    cluster_->SetInjectAction(ClusterNodeType::WORKER, 0, "set.objectIsInvalidInmem", "call()");
    cluster_->SetInjectAction(ClusterNodeType::WORKER, 0, "set.objectIsInComplete", "call()");
    auto key = "l2_hit";
    client0->Set(key, std::string("a", kb800), SetParam{ .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE });
    std::string getVal;
    client1->Get(key, getVal);
    // make sure the print of resource log finish
    sleep(logInterval);
    DS_ASSERT_OK(StrInResLog(0, "hit_info:0/0/0/0/0"));
    DS_ASSERT_OK(StrInResLog(1, "hit_info:0/0/0/1/0"));
}

TEST_F(KVClientHitTest, DiskRemoteHit)
{
    std::shared_ptr<KVClient> client0, client1;
    InitTestKVClient(0, client0, [&](ConnectOptions &opts) { (void)opts; });
    InitTestKVClient(1, client1, [&](ConnectOptions &opts) { (void)opts; });
    cluster_->SetInjectAction(ClusterNodeType::WORKER, 0, "hitinfo.prefix", "call()");
    cluster_->SetInjectAction(ClusterNodeType::WORKER, 1, "hitinfo.prefix", "call()");

    auto key = "disk_hit";
    client0->Set(key, std::string("a", kb800), SetParam{ .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE });
    client0->Set("spill", std::string("a", kb800), SetParam{ .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE });
    std::string getVal;
    client1->Get(key, getVal);
    // make sure the print of resource log finish
    sleep(logInterval);
    DS_ASSERT_OK(StrInResLog(0, "hit_info:0/0/0/0/0"));
    DS_ASSERT_OK(StrInResLog(1, "hit_info:0/0/0/1/0"));
}

TEST_F(KVClientHitTest, MultiRemoteHit)
{
    std::shared_ptr<KVClient> client0, client1;
    InitTestKVClient(0, client0, [&](ConnectOptions &opts) { (void)opts; });
    InitTestKVClient(1, client1, [&](ConnectOptions &opts) { (void)opts; });
    cluster_->SetInjectAction(ClusterNodeType::WORKER, 0, "hitinfo.prefix", "call()");
    cluster_->SetInjectAction(ClusterNodeType::WORKER, 1, "hitinfo.prefix", "call()");
    std::vector<std::string> keys;
    const auto keyNum = 10;
    for (auto i = 0; i < keyNum; i++) {
        auto key = FormatString("key_%d", i);
        DS_ASSERT_OK(client0->Set(key, "xxx"));
        std::string getVal;
        DS_ASSERT_OK(client1->Get(key, getVal));
    }
    // make sure the print of resource log finish
    sleep(logInterval);
    DS_ASSERT_OK(StrInResLog(0, "hit_info:0/0/0/0/0"));
    DS_ASSERT_OK(StrInResLog(1, "hit_info:0/0/0/10/0"));
}
};  // namespace st
}  // namespace datasystem
