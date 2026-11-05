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
#include <chrono>
#include <string>
#include <thread>
#include <vector>

#include "common.h"
#include "client/object_cache/oc_client_common.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/log/log_manager.h"
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
        opts.systemAccessKey.clear();
        opts.systemSecretKey.clear();
        opts.tenantAccessKey.clear();
        opts.tenantSecretKey.clear();
        std::string hostIp = "127.0.0.1";
        opts.workerConfigs.emplace_back(hostIp, GetFreePort());
        for (auto addr : opts.workerConfigs) {
            workerAddress_.emplace_back(addr.ToString());
        }
        opts.workerGflagParams = "-log_monitor_interval_ms=1000 -shared_memory_size_mb=1 -v=1 ";
        opts.enableSpill = true;
        opts.numEtcd = 1;
    }

    void SetUp() override
    {
        DS_ASSERT_OK(inject::Set("ObjectClientImpl.ClientWorkerWarmup.skip", "call()"));
        ExternalClusterTest::SetUp();
        DS_ASSERT_OK(cluster_->GetWorkerAddr(0, workerAddr_));
    }

    void TearDown() override
    {
        (void)inject::Clear("ObjectClientImpl.ClientWorkerWarmup.skip");
        ExternalClusterTest::TearDown();
    }
    Status StrInWorkerLog(int workerIdx, const char *logFile, const std::string &str)
    {
        auto logPath = FormatString("%s/worker%d/log/%s", cluster_->GetRootDir(), workerIdx, logFile);
        std::ifstream ifs(logPath);
        std::stringstream buffer;
        buffer << ifs.rdbuf();
        CHECK_FAIL_RETURN_STATUS((buffer.str().find(str) != std::string::npos), K_NOT_FOUND, "not find string");
        return Status::OK();
    }

    Status StrInResLog(int workerIdx, const std::string &str)
    {
        return StrInWorkerLog(workerIdx, "resource.log", str);
    }

    // Access entries go through an async bounded buffer that reliably flushes on graceful worker
    // shutdown (same pattern as the latency_summary access.log assertions).
    Status StrInAccessLog(int workerIdx, const std::string &str)
    {
        constexpr int retryTimes = 50;
        constexpr auto retryInterval = std::chrono::milliseconds(100);
        for (int i = 0; i < retryTimes; ++i) {
            (void)LogManager::DoLogMonitorWrite();
            if (StrInWorkerLog(workerIdx, "access.log", str).IsOk()) {
                return Status::OK();
            }
            std::this_thread::sleep_for(retryInterval);
        }
        return StrInWorkerLog(workerIdx, "access.log", str);
    }

    // Returns the index of the worker whose resource log contains str, or -1 when no worker matches.
    int FindWorkerWithResLog(const std::string &str)
    {
        for (size_t i = 0; i < cluster_->GetWorkerNum(); i++) {
            if (StrInResLog(static_cast<int>(i), str).IsOk()) {
                return static_cast<int>(i);
            }
        }
        return -1;
    }

protected:
    HostPort workerAddr_;
    int logInterval = 1;
    int kb800 = 800 * 1024;

    // Keep routed requests on the same no-auth configuration as the rest of this fixture.
    void InitRoutedKVClient(uint32_t workerIndex, std::shared_ptr<KVClient> &client)
    {
        ConnectOptions options;
        InitConnectOpt(workerIndex, options);
        options.accessKey.clear();
        options.secretKey.Clear();
        options.enableLocalCache = false;
        options.dataPlacementPolicy = DataPlacementPolicy::PREFERRED_META_OWNER;
        client = std::make_shared<KVClient>(options);
        DS_ASSERT_OK(client->Init());
    }
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

TEST_F(KVClientHitTest, PartMiss)
{
    std::shared_ptr<KVClient> client0, client1;
    InitTestKVClient(0, client0, [&](ConnectOptions &opts) { (void)opts; });
    InitTestKVClient(1, client1, [&](ConnectOptions &opts) { (void)opts; });
    cluster_->SetInjectAction(ClusterNodeType::WORKER, 0, "hitinfo.prefix", "call()");
    cluster_->SetInjectAction(ClusterNodeType::WORKER, 1, "hitinfo.prefix", "call()");
    std::vector<std::string> keys;
    std::vector<std::string> vals;
    const auto keyNum = 10;
    for (auto i = 0; i < keyNum; i++) {
        auto key = FormatString("key_%d", i);
        auto missKey = key + "_miss";
        auto remoteKey = key + "_remote";
        DS_ASSERT_OK(client0->Set(key, "xxx"));
        DS_ASSERT_OK(client1->Set(remoteKey, "xxx"));
        keys.emplace_back(key);
        keys.emplace_back(remoteKey);
        keys.emplace_back(missKey);
    }
    DS_ASSERT_OK(client1->Get(keys, vals));
    // make sure the print of resource log finish
    sleep(logInterval);
    DS_ASSERT_OK(StrInResLog(0, "hit_info:0/0/0/0/0"));
    DS_ASSERT_OK(StrInResLog(1, "hit_info:10/0/0/10/10"));
}

TEST_F(KVClientHitTest, RoutedOwnerInlineMemHit)
{
    std::shared_ptr<KVClient> client;
    InitRoutedKVClient(0, client);
    for (size_t i = 0; i < cluster_->GetWorkerNum(); i++) {
        cluster_->SetInjectAction(ClusterNodeType::WORKER, i, "hitinfo.prefix", "call()");
    }
    // PREFERRED_META_OWNER places the data on the key's metadata owner and the routed Get is served
    // inline there. The owner is picked by the client-side ring, so assert whichever worker owns it.
    DS_ASSERT_OK(client->Set("routed_mem_hit", "xxx"));
    std::string getVal;
    DS_ASSERT_OK(client->Get("routed_mem_hit", getVal));
    sleep(logInterval);
    ASSERT_GE(FindWorkerWithResLog("hit_info:1/0/0/0/0"), 0);
    // hit_info alone cannot discriminate the inline path from a Phase-2 replica-read fallback (the
    // remote handler counts the same memHit), so also prove no remote-get was served: stop the
    // workers to flush access.log, then require every worker to be free of remote-get entries.
    client.reset();
    for (size_t i = 0; i < cluster_->GetWorkerNum(); i++) {
        DS_ASSERT_OK(cluster_->ShutdownNode(ClusterNodeType::WORKER, i));
    }
    for (size_t i = 0; i < cluster_->GetWorkerNum(); i++) {
        ASSERT_FALSE(StrInWorkerLog(static_cast<int>(i), "access.log", "DS_POSIX_REMOTE_GET").IsOk());
        ASSERT_FALSE(StrInWorkerLog(static_cast<int>(i), "access.log", "DS_POSIX_REMOTE_MGET").IsOk());
    }
}

TEST_F(KVClientHitTest, RoutedReplicaDirectMemHit)
{
    std::shared_ptr<KVClient> boundClient, routedClient;
    InitTestKVClient(0, boundClient, [&](ConnectOptions &opts) { (void)opts; });
    InitRoutedKVClient(2, routedClient);
    for (size_t i = 0; i < cluster_->GetWorkerNum(); i++) {
        cluster_->SetInjectAction(ClusterNodeType::WORKER, i, "hitinfo.prefix", "call()");
    }
    // Data lives on worker0 (bound writer). The value exceeds the QueryAndGet inline payload budget
    // (QUERY_AND_GET_MAX_TCP_PAYLOAD_SIZE), so the owner never serves it inline and the routed read
    // always reaches the replica via GetObjectRemote, where the data holder counts the memory hit.
    DS_ASSERT_OK(boundClient->Set("routed_replica_hit", std::string("a", kb800)));
    std::string getVal;
    DS_ASSERT_OK(routedClient->Get("routed_replica_hit", getVal));
    sleep(logInterval);
    DS_ASSERT_OK(StrInResLog(0, "hit_info:1/0/0/0/0"));
    // Prove the read traversed GetObjectRemote: an 800KB value can never be served inline, so the
    // memory hit above can only come from the remote-get phase.
    boundClient.reset();
    routedClient.reset();
    DS_ASSERT_OK(cluster_->ShutdownNode(ClusterNodeType::WORKER, 0));
    DS_ASSERT_OK(StrInAccessLog(0, "DS_POSIX_REMOTE_GET"));
}

TEST_F(KVClientHitTest, RoutedDiskHit)
{
    std::shared_ptr<KVClient> boundClient, routedClient;
    InitTestKVClient(0, boundClient, [&](ConnectOptions &opts) { (void)opts; });
    InitRoutedKVClient(2, routedClient);
    for (size_t i = 0; i < cluster_->GetWorkerNum(); i++) {
        cluster_->SetInjectAction(ClusterNodeType::WORKER, i, "hitinfo.prefix", "call()");
    }
    DS_ASSERT_OK(boundClient->Set("routed_disk_hit", std::string("a", kb800),
                                  SetParam{ .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE }));
    DS_ASSERT_OK(boundClient->Set("spill_routed", std::string("a", kb800),
                                  SetParam{ .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE }));
    std::string getVal;
    DS_ASSERT_OK(routedClient->Get("routed_disk_hit", getVal));
    sleep(logInterval);
    DS_ASSERT_OK(StrInResLog(0, "hit_info:0/1/0/0/0"));
}

TEST_F(KVClientHitTest, RoutedBatchRemoteHit)
{
    std::shared_ptr<KVClient> boundClient, routedClient;
    InitTestKVClient(0, boundClient, [&](ConnectOptions &opts) { (void)opts; });
    InitRoutedKVClient(2, routedClient);
    for (size_t i = 0; i < cluster_->GetWorkerNum(); i++) {
        cluster_->SetInjectAction(ClusterNodeType::WORKER, i, "hitinfo.prefix", "call()");
    }
    DS_ASSERT_OK(boundClient->Set("routed_batch_hit_a", std::string("a", kb800),
                                  SetParam{ .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE }));
    DS_ASSERT_OK(boundClient->Set("routed_batch_hit_b", std::string("a", kb800),
                                  SetParam{ .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE }));
    DS_ASSERT_OK(boundClient->Set("spill_routed", std::string("a", kb800),
                                  SetParam{ .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE }));
    std::vector<std::string> keys = { "routed_batch_hit_a", "routed_batch_hit_b" };
    std::vector<std::string> vals;
    DS_ASSERT_OK(routedClient->Get(keys, vals));
    sleep(logInterval);
    // Both keys share the worker0 endpoint, so one BatchGetObjectRemote serves them and each
    // delivered key counts exactly once on the data holder.
    DS_ASSERT_OK(StrInResLog(0, "hit_info:0/2/0/0/0"));
    boundClient.reset();
    routedClient.reset();
    DS_ASSERT_OK(cluster_->ShutdownNode(ClusterNodeType::WORKER, 0));
    DS_ASSERT_OK(StrInAccessLog(0, "DS_POSIX_REMOTE_MGET"));
}

TEST_F(KVClientHitTest, RoutedMiss)
{
    std::shared_ptr<KVClient> client;
    InitRoutedKVClient(0, client);
    for (size_t i = 0; i < cluster_->GetWorkerNum(); i++) {
        cluster_->SetInjectAction(ClusterNodeType::WORKER, i, "hitinfo.prefix", "call()");
    }
    std::string getVal;
    (void)client->Get("routed_miss_key", getVal);
    sleep(logInterval);
    // The miss is counted by the metadata owner that served the QueryAndGet.
    ASSERT_GE(FindWorkerWithResLog("hit_info:0/0/0/0/1"), 0);
}

};  // namespace st
}  // namespace datasystem
