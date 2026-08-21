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
 * Description: Reproduce issue #1061 -- a worker-relay Get reports "RPC peer dead" when
 * the owning worker is killed in the race window between CheckEndpoint and the data RPC.
 * Verifies that L2-backed objects fall back to L2 cache and succeed; non-L2 objects keep
 * the original peer-dead fast-fail.
 *
 * Cluster: distributed master + coordinator (no etcd) + OBS L2 cache.
 * The test pre-filters a random key whose metadata owner is NOT the worker we kill, so the
 * metadata query succeeds while the data RPC targets the dying primary worker.
 */

#include <chrono>
#include <map>
#include <string>
#include <thread>
#include <unistd.h>

#include <gtest/gtest.h>

#include "client/object_cache/oc_client_common.h"
#include "common.h"
#include "datasystem/common/util/hash_algorithm.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/kv_client.h"
#include "datasystem/object/object_enum.h"
#include "datasystem/protos/cluster_topology.pb.h"

DS_DECLARE_bool(use_brpc);

namespace datasystem {
namespace st {
namespace {
constexpr uint32_t WORKER_NUM = 3;
// Inject point at worker_oc_service_get_impl.cpp GetObjectFromRemoteOnLock, right before
// the remote-get RPC and after CheckEndpoint has passed.
const std::string INJECT_NAME = "worker.before_GetObjectFromRemoteWorkerAndDump";
}  // namespace

class KVClientPeerDeadL2Fallback : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = WORKER_NUM;
        opts.numEtcd = 0;
        opts.numOBS = 1;
        opts.numCoordinators = 1;
        opts.enableDistributedMaster = "true";
        // oc_io_from_l2cache_need_metadata=false: skip etcd metadata fallback (no etcd in this
        // cluster) and enable the existing GetObjectsWithoutMeta L2-cache path for absent keys.
        // node_timeout_s=60: keep the hash ring stable during the test so the pre-filtered key's
        // metadata owner does not shift while we kill the primary worker.
        opts.workerGflagParams =
            "-oc_io_from_l2cache_need_metadata=false "
            "-enable_l2_cache_fallback=true "
            "-enable_worker_worker_batch_get=false "
            "-node_timeout_s=60 -node_dead_timeout_s=120 "
            "-shared_memory_size_mb=5120 -v=2";
        opts.coordinatorGflagParams = "-node_timeout_s=60 -node_dead_timeout_s=120";
    }

    void SetUp() override
    {
        previousUseBrpc_ = FLAGS_use_brpc;
        FLAGS_use_brpc = true;
        ExternalClusterTest::SetUp();
        WaitForWorkersActive();
        InitTestKVClient(0, client0_);
        InitTestKVClient(1, client1_);
    }

    void TearDown() override
    {
        client0_.reset();
        client1_.reset();
        ExternalClusterTest::TearDown();
        FLAGS_use_brpc = previousUseBrpc_;
    }

    // Block until the named inject point has been hit on the given worker so the test can
    // deterministically kill the peer before the parked RPC is resumed.
    bool WaitForInjectHit(uint32_t idx, const std::string &name, int timeoutS = 15)
    {
        auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(timeoutS);
        while (std::chrono::steady_clock::now() < deadline) {
            uint64_t cnt = 0;
            if (cluster_->GetInjectActionExecuteCount(WORKER, idx, name, cnt).IsOk() && cnt >= 1) {
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        return false;
    }

    /**
     * Find a random key (UUID) whose metadata owner is NOT the given worker.
     * In distributed-master mode the metadata owner is determined by the coordinator hash ring.
     * By pre-filtering we ensure the metadata query succeeds (owner is alive) while the data RPC
     * targets the primary worker we are about to kill.
     */
    std::string FindKeyNotOwnedByWorker(uint32_t workerIndex)
    {
        ClusterTopologyPb topology;
        if (!cluster_->ReadClusterTopology(topology).IsOk()) {
            return "";
        }
        HostPort targetWorker;
        if (!cluster_->GetWorkerAddr(workerIndex, targetWorker).IsOk()) {
            return "";
        }
        const std::string targetAddr = targetWorker.ToString();
        std::map<uint32_t, std::string> tokenWorkers;
        for (const auto &member : topology.members()) {
            if (member.second.state() != MembershipPb::ACTIVE) {
                continue;
            }
            for (const auto token : member.second.tokens()) {
                tokenWorkers.emplace(token, member.first);
            }
        }
        if (tokenWorkers.empty()) {
            return "";
        }
        for (int i = 0; i < 1000; ++i) {
            std::string candidate = NewObjectKey();
            auto owner = tokenWorkers.lower_bound(MurmurHash3_32(candidate));
            if (owner == tokenWorkers.end()) {
                owner = tokenWorkers.begin();
            }
            if (owner->second != targetAddr) {
                return candidate;
            }
        }
        return "";
    }

protected:
    std::shared_ptr<KVClient> client0_;
    std::shared_ptr<KVClient> client1_;
    bool previousUseBrpc_ = false;

private:
    void WaitForWorkersActive()
    {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
        while (std::chrono::steady_clock::now() < deadline) {
            ClusterTopologyPb topology;
            if (cluster_->ReadClusterTopology(topology).IsOk()) {
                int activeCount = 0;
                for (const auto &member : topology.members()) {
                    if (member.second.state() == MembershipPb::ACTIVE) {
                        ++activeCount;
                    }
                }
                if (activeCount >= static_cast<int>(WORKER_NUM)) {
                    return;
                }
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
        }
    }
};

// Reproduce + root-cause lock: w0 relays a remote get to w1; w1 is killed while w0 is parked
// before the data RPC (CheckEndpoint already passed). For WRITE_THROUGH_L2_CACHE the data is
// in L2 (OBS), so Get must fall back to L2 and succeed. Buggy code returns K_RPC_PEER_DEAD
// (peer-dead bypasses L2 at worker_oc_service_get_impl.cpp:2404).
TEST_F(KVClientPeerDeadL2Fallback, LEVEL1_PeerDeadDuringRemoteGetFallsBackToL2)
{
    // Pre-filter: the key's metadata owner must NOT be worker 1 (the worker we kill), so the
    // metadata query succeeds while the data RPC targets the dying primary.
    const std::string key = FindKeyNotOwnedByWorker(1);
    ASSERT_FALSE(key.empty()) << "Could not find a key whose metadata owner is not worker 1";

    const std::string value = GenRandomString(500);
    SetParam param{ .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE };
    DS_ASSERT_OK(client1_->Set(key, value, param));

    // Park w0 right before the remote-get RPC to w1 so w1 can be killed inside the exact
    // race window of issue #1061 (CheckEndpoint passed, RPC not yet issued).
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, INJECT_NAME, "pause"));

    Status rc;
    std::string out;
    Status killRc;
    std::thread getThread([&] { rc = client0_->Get(key, out); });
    {
        // RAII guarantees inject is cleared and thread is joined on scope exit, even if a
        // fatal assertion or FAIL fires before the normal cleanup path.
        Raii cleanup([&] {
            (void)cluster_->ClearInjectAction(WORKER, 0, INJECT_NAME);
            if (getThread.joinable()) {
                getThread.join();
            }
        });

        if (!WaitForInjectHit(0, INJECT_NAME)) {
            FAIL() << "worker 0 remote-get inject not hit within timeout; cannot reproduce peer-dead race";
        }
        killRc = cluster_->KillWorker(1);
        sleep(1);  // let the kernel reap w1's listening port so the RPC returns ECONNREFUSED
        // RAii: ClearInject → join at scope exit; resumes w0 → RPC hits dead w1 → L2 fallback
    }
    DS_ASSERT_OK(killRc);

    ASSERT_EQ(rc, Status::OK()) << rc.ToString();
    ASSERT_EQ(value, out);
}

// Surgical-scope guard: non-L2 objects keep the original peer-dead fast-fail. The fix only
// relaxes the early-return for L2-backed write modes, so NONE_L2_CACHE must still surface
// K_RPC_PEER_DEAD to the caller.
TEST_F(KVClientPeerDeadL2Fallback, LEVEL1_PeerDeadDuringRemoteGetNonL2FastFails)
{
    const std::string key = FindKeyNotOwnedByWorker(1);
    ASSERT_FALSE(key.empty()) << "Could not find a key whose metadata owner is not worker 1";

    const std::string value = GenRandomString(500);
    SetParam param{ .writeMode = WriteMode::NONE_L2_CACHE };
    DS_ASSERT_OK(client1_->Set(key, value, param));

    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, INJECT_NAME, "pause"));

    Status rc;
    std::string out;
    Status killRc;
    std::thread getThread([&] { rc = client0_->Get(key, out); });
    {
        Raii cleanup([&] {
            (void)cluster_->ClearInjectAction(WORKER, 0, INJECT_NAME);
            if (getThread.joinable()) {
                getThread.join();
            }
        });

        if (!WaitForInjectHit(0, INJECT_NAME)) {
            FAIL() << "worker 0 remote-get inject not hit within timeout; cannot reproduce peer-dead race";
        }
        killRc = cluster_->KillWorker(1);
        sleep(1);
    }
    DS_ASSERT_OK(killRc);

    ASSERT_EQ(rc.GetCode(), StatusCode::K_RPC_PEER_DEAD) << rc.ToString();
}

// Flag-gate guard: when FLAGS_enable_l2_cache_fallback=false, even L2-backed objects must
// fast-fail on peer-dead. The fix's gate !(l2Backed && FLAGS_enable_l2_cache_fallback) evaluates
// to true when the flag is off, so the early-return is taken and L2 fallback is never reached.
class KVClientPeerDeadL2FallbackDisabled : public KVClientPeerDeadL2Fallback {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        KVClientPeerDeadL2Fallback::SetClusterSetupOptions(opts);
        opts.workerGflagParams =
            "-oc_io_from_l2cache_need_metadata=false "
            "-enable_l2_cache_fallback=false "
            "-node_timeout_s=60 -node_dead_timeout_s=120 "
            "-shared_memory_size_mb=5120 -v=2";
    }
};

TEST_F(KVClientPeerDeadL2FallbackDisabled, LEVEL1_PeerDeadL2FallbackDisabledFastFails)
{
    const std::string key = FindKeyNotOwnedByWorker(1);
    ASSERT_FALSE(key.empty()) << "Could not find a key whose metadata owner is not worker 1";

    const std::string value = GenRandomString(500);
    SetParam param{ .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE };
    DS_ASSERT_OK(client1_->Set(key, value, param));

    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, INJECT_NAME, "pause"));

    Status rc;
    std::string out;
    Status killRc;
    std::thread getThread([&] { rc = client0_->Get(key, out); });
    {
        Raii cleanup([&] {
            (void)cluster_->ClearInjectAction(WORKER, 0, INJECT_NAME);
            if (getThread.joinable()) {
                getThread.join();
            }
        });

        if (!WaitForInjectHit(0, INJECT_NAME)) {
            FAIL() << "worker 0 remote-get inject not hit within timeout; cannot reproduce peer-dead race";
        }
        killRc = cluster_->KillWorker(1);
        sleep(1);
    }
    DS_ASSERT_OK(killRc);

    ASSERT_EQ(rc.GetCode(), StatusCode::K_RPC_PEER_DEAD) << rc.ToString();
}

// Batch path guard: with enable_worker_worker_batch_get=true, the same peer-dead race must
// also fall back to L2. HandleBatchSubResponsePart2 has the same IsNonRetryableRpcError gate
// that was fixed in parallel with the scalar path.
class KVClientPeerDeadL2FallbackBatchGet : public KVClientPeerDeadL2Fallback {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        KVClientPeerDeadL2Fallback::SetClusterSetupOptions(opts);
        opts.workerGflagParams += " -enable_worker_worker_batch_get=true";
    }
};

TEST_F(KVClientPeerDeadL2FallbackBatchGet, LEVEL1_PeerDeadDuringBatchGetFallsBackToL2)
{
    const std::string key = FindKeyNotOwnedByWorker(1);
    ASSERT_FALSE(key.empty()) << "Could not find a key whose metadata owner is not worker 1";

    const std::string value = GenRandomString(500);
    SetParam param{ .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE };
    DS_ASSERT_OK(client1_->Set(key, value, param));

    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, INJECT_NAME, "pause"));

    Status rc;
    std::string out;
    Status killRc;
    std::thread getThread([&] { rc = client0_->Get(key, out); });
    {
        Raii cleanup([&] {
            (void)cluster_->ClearInjectAction(WORKER, 0, INJECT_NAME);
            if (getThread.joinable()) {
                getThread.join();
            }
        });

        if (!WaitForInjectHit(0, INJECT_NAME)) {
            FAIL() << "worker 0 remote-get inject not hit within timeout; cannot reproduce peer-dead race";
        }
        killRc = cluster_->KillWorker(1);
        sleep(1);
    }
    DS_ASSERT_OK(killRc);

    ASSERT_EQ(rc, Status::OK()) << rc.ToString();
    ASSERT_EQ(value, out);
}

// Mixed batch: one L2-backed and one non-L2 object in the same batch. After peer-dead,
// the L2-backed object must fall back to L2 (value populated) while the non-L2 object
// fast-fails (no value). Verifies per-object gate behavior in HandleBatchSubResponsePart2.
TEST_F(KVClientPeerDeadL2FallbackBatchGet, LEVEL1_PeerDeadBatchMixedL2AndNonL2PerObjectFallback)
{
    const std::string key1 = FindKeyNotOwnedByWorker(1);
    ASSERT_FALSE(key1.empty()) << "Could not find a key whose metadata owner is not worker 1";
    const std::string key2 = FindKeyNotOwnedByWorker(1);
    ASSERT_FALSE(key2.empty()) << "Could not find a key whose metadata owner is not worker 1";
    ASSERT_NE(key1, key2) << "Need two distinct keys for mixed batch test";

    const std::string val1 = GenRandomString(500);
    const std::string val2 = GenRandomString(500);
    DS_ASSERT_OK(client1_->Set(key1, val1, SetParam{ .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE }));
    DS_ASSERT_OK(client1_->Set(key2, val2, SetParam{ .writeMode = WriteMode::NONE_L2_CACHE }));

    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, INJECT_NAME, "pause"));

    Status rc;
    std::vector<std::string> vals;
    Status killRc;
    std::thread getThread([&] { rc = client0_->Get(std::vector<std::string>{key1, key2}, vals); });
    {
        Raii cleanup([&] {
            (void)cluster_->ClearInjectAction(WORKER, 0, INJECT_NAME);
            if (getThread.joinable()) {
                getThread.join();
            }
        });

        if (!WaitForInjectHit(0, INJECT_NAME)) {
            FAIL() << "worker 0 remote-get inject not hit within timeout; cannot reproduce peer-dead race";
        }
        killRc = cluster_->KillWorker(1);
        sleep(1);
    }
    DS_ASSERT_OK(killRc);

    // The batch contains one L2-backed and one non-L2 object targeting the same dead worker.
    // After peer-dead, the per-object gate in HandleBatchSubResponsePart2 must:
    //   - relax for L2-backed → tryGetFromElsewhere=true → L2 fallback → value populated
    //   - fast-fail for non-L2 → tryGetFromElsewhere=false → no value
    // Get(vector<keys>) returns OK for partial success; the non-L2 key's value must be absent.
    ASSERT_EQ(vals.size(), 2u) << "Expected one slot per key";
    bool l2ValueFound = false;
    bool nonL2ValueAbsent = false;
    for (const auto &v : vals) {
        if (v == val1) {
            l2ValueFound = true;
        } else if (v != val2) {
            nonL2ValueAbsent = true;
        }
    }
    ASSERT_TRUE(l2ValueFound) << "L2-backed key must fall back to L2 in mixed batch";
    ASSERT_TRUE(nonL2ValueAbsent) << "non-L2 key must not have its value in mixed batch";
}
}  // namespace st
}  // namespace datasystem
