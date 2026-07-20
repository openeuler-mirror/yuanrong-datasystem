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
 * Description: brpc cross-worker KV remote-get coverage.
 */

#include <memory>
#include <string>

#include <gtest/gtest.h>

#include "common.h"
#include "datasystem/common/flags/common_flags.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/kv_client.h"
#include "kv_client_common.h"

namespace datasystem {
namespace st {
namespace {
constexpr uint32_t WORKER_NUM = 2;
constexpr uint32_t WORKER_NUM_RETRY = 3;  // Need 3 workers so the cross-node Get can fetch from a
                                         // replica whose address differs from the primary_address.
constexpr uint64_t VALUE_SIZE = 8UL * 1024 * 1024;
constexpr int32_t CLIENT_TIMEOUT_MS = 60 * 1000;
}  // namespace

class KVClientBrpcRemoteGetTest : public KVClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = WORKER_NUM;
        opts.numEtcd = 1;
        opts.enableDistributedMaster = "false";
        opts.workerGflagParams =
            "-shared_memory_size_mb=512 -v=2 -log_monitor=true -enable_perf_trace_log=true -use_brpc=true";
    }

    void SetUp() override
    {
        restoreUseBrpc_ = std::make_unique<Raii>([oldUseBrpc = FLAGS_use_brpc]() { FLAGS_use_brpc = oldUseBrpc; });
        FLAGS_use_brpc = true;
        ExternalClusterTest::SetUp();
    }

    void TearDown() override
    {
        ExternalClusterTest::TearDown();
        restoreUseBrpc_.reset();
    }

private:
    std::unique_ptr<Raii> restoreUseBrpc_;
};

TEST_F(KVClientBrpcRemoteGetTest, LEVEL1_Get8MBValueAcrossWorkersWithShmEnabled)
{
    std::shared_ptr<KVClient> setterClient;
    std::shared_ptr<KVClient> getterClient;
    InitTestKVClient(0, setterClient, CLIENT_TIMEOUT_MS);
    InitTestKVClient(1, getterClient, CLIENT_TIMEOUT_MS);

    const std::string key = "brpc_remote_get_8mb_" + NewObjectKey();
    const std::string value = GenRandomString(VALUE_SIZE);
    SetParam setParam{ .writeMode = WriteMode::NONE_L2_CACHE, .ttlSecond = 0, .existence = ExistenceOpt::NONE,
                       .cacheType = CacheType::MEMORY };
    DS_ASSERT_OK(setterClient->Set(key, value, setParam));

    std::string valueGet;
    DS_ASSERT_OK(getterClient->Get(key, valueGet));
    ASSERT_EQ(valueGet.size(), value.size());
    ASSERT_EQ(valueGet, value);

    DS_ASSERT_OK(setterClient->Del(key));
}

// Three-worker variant needed so the cross-node Get's remote-fetch address (a replica) can differ
// from the object's primary_address. With only two workers the Set places the primary copy on the
// setter's worker, so a cross-node Get fetches from that same primary and primaryAddress == address,
// which never triggers the retry-to-primary path that exercises issue #783.
class KVClientBrpcRemoteGetRetryTest : public KVClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = WORKER_NUM_RETRY;
        opts.numEtcd = 1;
        opts.enableDistributedMaster = "false";
        // enable_data_replication defaults to true: a cross-node Get caches a replica on the getter
        // worker and the master acks it as a location, so a later Get can fetch from that replica
        // (an address != primary_address) and reach the retry-to-primary path.
        opts.workerGflagParams = "-shared_memory_size_mb=512 -v=2 -log_monitor=true -enable_perf_trace_log=true "
                                 "-use_brpc=true -enable_data_replication=true";
    }

    void SetUp() override
    {
        restoreUseBrpc_ = std::make_unique<Raii>([oldUseBrpc = FLAGS_use_brpc]() { FLAGS_use_brpc = oldUseBrpc; });
        FLAGS_use_brpc = true;
        ExternalClusterTest::SetUp();
    }

    void TearDown() override
    {
        ExternalClusterTest::TearDown();
        restoreUseBrpc_.reset();
    }

private:
    std::unique_ptr<Raii> restoreUseBrpc_;
};

// Issue #783: when RetrieveRemotePayload fails (here via the fail_after_transfer inject on the
// getter worker), the error path used to free the ShmUnit pointer without dropping it from the
// entry. The retry-to-primary then re-entered RetrieveRemotePayload, saw szChanged==false, reused
// the stale unit whose pointer was null, and wrote to a near-null address -> worker SIGSEGV.
// After the fix the error path calls entry->SetShmUnit(nullptr), so the retry reallocates and the
// Get succeeds. Pre-fix this test crashes the getter worker (coredump / cluster unhealthy).
TEST_F(KVClientBrpcRemoteGetRetryTest, LEVEL1_RemoteGetRetryToPrimaryAfterPayloadFailureDoesNotCrash)
{
    std::shared_ptr<KVClient> setterClient;   // binds to worker 0 (holds the primary copy)
    std::shared_ptr<KVClient> replicaClient;   // binds to worker 1 (warms up a replica there)
    std::shared_ptr<KVClient> getterClient;   // binds to worker 2 (performs the cross-node remote get)
    InitTestKVClient(0, setterClient, CLIENT_TIMEOUT_MS);
    InitTestKVClient(1, replicaClient, CLIENT_TIMEOUT_MS);
    InitTestKVClient(2, getterClient, CLIENT_TIMEOUT_MS);

    const std::string key = "brpc_remote_get_retry_primary_" + NewObjectKey();
    const std::string value = GenRandomString(VALUE_SIZE);
    SetParam setParam{ .writeMode = WriteMode::NONE_L2_CACHE, .ttlSecond = 0, .existence = ExistenceOpt::NONE,
                       .cacheType = CacheType::MEMORY };
    DS_ASSERT_OK(setterClient->Set(key, value, setParam));

    // Warm up a replica on worker 1 so it is an acked location with data. This makes the later
    // Get fetch from worker 1 (a replica != primary on worker 0), which is required for the
    // retry-to-primary path in GetObjectFromRemoteWorkerWithoutDump to fire.
    std::string warmupVal;
    DS_ASSERT_OK(replicaClient->Get(key, warmupVal));
    ASSERT_EQ(warmupVal, value);
    // Let the master ack worker 1 as a location before the getter's Get consults SelectObjectLocation.
    std::this_thread::sleep_for(std::chrono::seconds(1));

    // Force the getter (worker 2) to fetch from worker 1 (the replica, not the primary on worker 0).
    // In this non-distributed setup the master logic runs on worker 0 (central master worker), so the
    // inject targets worker 0.
    HostPort worker1Addr;
    DS_ASSERT_OK(cluster_->GetWorkerAddr(1, worker1Addr));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "master.select_location",
                                           std::string("return(") + worker1Addr.ToString() + ")"));

    // On the getter worker (worker 2), fail RetrieveRemotePayload exactly once after the data
    // transfer so its error path runs (nulling the ShmUnit pointer). The retry-to-primary then
    // re-enters RetrieveRemotePayload and must reallocate (post-fix) instead of reusing the null unit.
    DS_ASSERT_OK(cluster_->SetInjectAction(
        WORKER, 2, "WorkerOcServiceGetImpl.RetrieveRemotePayload.fail_after_transfer", "1*call(0)"));

    std::string valueGet;
    DS_ASSERT_OK(getterClient->Get(key, valueGet));
    ASSERT_EQ(valueGet.size(), value.size());
    ASSERT_EQ(valueGet, value);

    DS_ASSERT_OK(setterClient->Del(key));
}

}  // namespace st
}  // namespace datasystem
