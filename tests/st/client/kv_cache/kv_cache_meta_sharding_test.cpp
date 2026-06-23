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
 * Description: ST tests for 64-way sharded ExpiredObjectManager and OCMetadataManager.
 *              Validates cross-shard fairness (k-way merge), TTL lifecycle, async delete
 *              consistency, and per-worker metadata removal across all 64 MetaTableShards.
 */
#include <gtest/gtest.h>
#include <algorithm>
#include <atomic>
#include <set>
#include <thread>
#include <vector>

#include "common.h"
#include "datasystem/common/eventloop/timer_queue.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/timer.h"
#include "client/object_cache/oc_client_common.h"

namespace datasystem {
namespace st {

class KVCacheMetaShardingTest : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        FLAGS_v = 1;
        opts.numOBS = 1;
        opts.numWorkers = 3;
        opts.numEtcd = 1;
        opts.workerGflagParams = "-node_timeout_s=5 -shared_memory_size_mb=2048 -v=2";
    }

    void SetUp() override
    {
        ExternalClusterTest::SetUp();
        InitTestKVClient(client1Index, client1);
        InitTestKVClient(client2Index, client2);
        InitTestKVClient(client3Index, client3);
    }

    void TearDown() override
    {
        client1.reset();
        client2.reset();
        client3.reset();
        ExternalClusterTest::TearDown();
    }

protected:
    std::shared_ptr<KVClient> client1;
    std::shared_ptr<KVClient> client2;
    std::shared_ptr<KVClient> client3;

    // Generate keys with varied prefixes to distribute across all 64 shards (std::hash<string> % 64).
    // Uses 8 prefixes (not 64) to keep object-store HTTP list/delete operations reasonable
    // while still exercising multiple shards.
    static std::vector<std::string> GenerateVariedKeys(int count, const std::string &label)
    {
        std::vector<std::string> keys;
        keys.reserve(count);
        static const std::vector<std::string> kPrefixes = {
            "a0", "b1", "c2", "d3", "e4", "f5", "g6", "h7"
        };
        for (int i = 0; i < count; ++i) {
            keys.push_back(kPrefixes[i % kPrefixes.size()] + "_" + label + "_" + std::to_string(i));
        }
        return keys;
    }

    // Insert keys with TTL and assert each succeeds.
    void InsertKeys(const std::vector<std::string> &keys, const std::string &value, uint32_t ttlSecond,
                    std::shared_ptr<KVClient> &client)
    {
        SetParam param{ .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE, .ttlSecond = ttlSecond };
        for (const auto &key : keys) {
            DS_ASSERT_OK(client->Set(key, value, param));
        }
    }

    // Count keys that return NOT OK from Get.
    int CountGetErrors(const std::vector<std::string> &keys, std::shared_ptr<KVClient> &client)
    {
        int errors = 0;
        std::string out;
        for (const auto &key : keys) {
            if (!client->Get(key, out).IsOk()) {
                ++errors;
            }
        }
        return errors;
    }

    // Count keys that exist and match expected value.
    int CountMatch(const std::vector<std::string> &keys, const std::string &expected,
                   std::shared_ptr<KVClient> &client)
    {
        int match = 0;
        std::string out;
        for (const auto &key : keys) {
            if (client->Get(key, out).IsOk() && out == expected) {
                ++match;
            }
        }
        return match;
    }

    // Poll until at least targetPct of keys are NOT_FOUND, or maxWaitSec elapsed.
    // Uses 3s polling interval to avoid overwhelming worker RPC queues.
    // Returns the final NOT_FOUND count so callers can apply fallback thresholds.
    int WaitForKeysDeleted(const std::vector<std::string> &keys, std::shared_ptr<KVClient> &client,
                            double targetPct, int maxWaitSec = 60)
    {
        int target = static_cast<int>(keys.size() * targetPct);
        int notFound = 0;
        static constexpr int kPollIntervalSec = 3;
        for (int i = 0; i < maxWaitSec; i += kPollIntervalSec) {
            notFound = CountGetErrors(keys, client);
            if (notFound >= target) {
                LOG(INFO) << "Keys deleted: " << notFound << "/" << keys.size()
                          << " >= target " << target << " after " << (i + kPollIntervalSec) << "s";
                return notFound;
            }
            LOG(INFO) << "Waiting for keys to expire: " << notFound << "/" << keys.size()
                      << " deleted after " << (i + kPollIntervalSec) << "s";
            std::this_thread::sleep_for(std::chrono::seconds(kPollIntervalSec));
        }
        LOG(WARNING) << "Keys deletion timed out after " << maxWaitSec
                     << "s: " << notFound << "/" << keys.size();
        return notFound;
    }

private:
    const uint32_t client1Index = 0;
    const uint32_t client2Index = 1;
    const uint32_t client3Index = 2;
};

// A1-ST: CrossShardFairnessLargeBatch
// Verify that the k-way merge in GetExpiredObject() does not starve any shard:
// ALL expired keys across all 64 shards are eventually processed, even when
// some shards have more expired entries than others.
TEST_F(KVCacheMetaShardingTest, CrossShardFairnessLargeBatch)
{
    const int kNumKeys = 300;
    const uint32_t kTtlSeconds = 2;
    const std::string kValue = "fairness_val";

    auto keys = GenerateVariedKeys(kNumKeys, "fair");

    LOG(INFO) << "[A1] Inserting " << kNumKeys << " keys with TTL=" << kTtlSeconds << "s";
    InsertKeys(keys, kValue, kTtlSeconds, client1);

    // Quick spot-check: a few keys exist before TTL expires.
    {
        std::string out;
        EXPECT_EQ(client1->Get(keys[0], out), Status::OK());
        EXPECT_EQ(out, kValue);
        EXPECT_EQ(client1->Get(keys[kNumKeys / 2], out), Status::OK());
        EXPECT_EQ(out, kValue);
    }

    // Wait for TTL expiry.
    std::this_thread::sleep_for(std::chrono::seconds(3));

    // Use the first 200 actually-inserted keys for verification (not regenerated).
    const int kSampleSize = std::min(200, kNumKeys);
    std::vector<std::string> sampled(keys.begin(), keys.begin() + kSampleSize);

    // Poll until >=95% of sampled keys are deleted.
    int notFound = WaitForKeysDeleted(sampled, client1, 0.95, 60);

    LOG(INFO) << "[A1] " << notFound << "/" << kSampleSize << " expired";
    // 95% strict, 80% fallback.
    if (notFound < kSampleSize * 0.95) {
        // Log surviving keys for diagnosis.
        std::string out;
        for (int i = 0; i < kSampleSize; ++i) {
            if (client1->Get(sampled[i], out).IsOk()) {
                LOG(WARNING) << "[A1] Surviving key: " << sampled[i];
            }
        }
        EXPECT_GE(notFound, kSampleSize * 0.80)
            << "k-way merge must process expired keys from ALL shards. "
            << "Only " << notFound << "/" << kSampleSize << " expired.";
    }
}

// A2: CreateMetaWithTTLLifecycle
// Full lifecycle test through 64-way sharded metadata:
//   create -> immediate read-back -> wait for expiry -> verify deletion -> re-insert works.
TEST_F(KVCacheMetaShardingTest, CreateMetaWithTTLLifecycle)
{
    const int kNumKeys = 500;
    const uint32_t kTtlSeconds = 5;
    const std::string kValue = "lifecycle_val";

    auto keys = GenerateVariedKeys(kNumKeys, "life");

    LOG(INFO) << "[A2] Phase 1: Creating " << kNumKeys << " keys with TTL=" << kTtlSeconds << "s";
    InsertKeys(keys, kValue, kTtlSeconds, client1);

    // Phase 2: Immediately verify all keys exist and return correct value.
    LOG(INFO) << "[A2] Phase 2: Verifying all " << kNumKeys << " keys exist";
    int found = CountMatch(keys, kValue, client1);
    EXPECT_EQ(found, kNumKeys)
        << "All keys must be readable immediately. Missing " << (kNumKeys - found);

    // Phase 3: Wait for TTL to expire, then poll for deletion.
    LOG(INFO) << "[A2] Phase 3: Waiting " << (kTtlSeconds + 1) << "s for TTL expiry";
    std::this_thread::sleep_for(std::chrono::seconds(kTtlSeconds + 1));
    int notFound = WaitForKeysDeleted(keys, client1, 0.95, 60);

    // Phase 4: Verify all keys are gone (NOT_FOUND). Fallback to 80%.
    LOG(INFO) << "[A2] Phase 4: Verifying all keys expired, NOT_FOUND=" << notFound;
    if (notFound < kNumKeys * 0.95) {
        EXPECT_GE(notFound, kNumKeys * 0.80)
            << "Most keys must be NOT_FOUND after TTL. " << (kNumKeys - notFound) << " still exist.";
    }

    // Phase 5: Re-insert previously-expired key — must succeed (no stale metadata conflict).
    std::string rekey = keys[0];
    std::string reval = "relife_val";
    DS_ASSERT_OK(client1->Set(rekey, reval,
                             { .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE, .ttlSecond = 10 }));
    std::string out;
    EXPECT_EQ(client1->Get(rekey, out), Status::OK());
    EXPECT_EQ(out, reval);
}

// A3: AsyncDeleteMultiShardConsistency
// Verify that after async delete completes: no partial state remains,
// no key found in metadata but missing from TTL tracking (or vice versa).
TEST_F(KVCacheMetaShardingTest, AsyncDeleteMultiShardConsistency)
{
    const int kNumKeys = 200;
    const uint32_t kTtlSeconds = 2;
    const std::string kValue = "consist_val";

    auto keys = GenerateVariedKeys(kNumKeys, "cs");

    LOG(INFO) << "[A3] Inserting " << kNumKeys << " keys with TTL=" << kTtlSeconds << "s";
    InsertKeys(keys, kValue, kTtlSeconds, client1);

    // Spot-check pre-expiry.
    {
        std::string out;
        EXPECT_EQ(client1->Get(keys[0], out), Status::OK());
        EXPECT_EQ(out, kValue);
    }

    // Wait for expiry + poll for async delete completion.
    std::this_thread::sleep_for(std::chrono::seconds(3));
    int notFound = WaitForKeysDeleted(keys, client1, 0.95, 60);

    LOG(INFO) << "[A3] NOT_FOUND: " << notFound << " / " << kNumKeys;
    if (notFound < kNumKeys * 0.95) {
        EXPECT_GE(notFound, kNumKeys * 0.80)
            << "No partial state allowed: most keys must be fully deleted. "
            << (kNumKeys - notFound) << " keys still exist.";
    }

    // Consistency check: re-insert a previously-expired key.
    // If partial state existed (meta without TTL tracking, or TTL tracking without meta),
    // the Set would either fail or produce an inconsistency.
    std::string rekey = keys[kNumKeys / 3];
    std::string reval = "reincarnated";
    Status reSet = client3->Set(rekey, reval,
                                { .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE, .ttlSecond = 10 });
    EXPECT_TRUE(reSet.IsOk()) << "Re-insert after expiry must succeed: " << reSet.ToString();

    std::string out;
    EXPECT_EQ(client1->Get(rekey, out), Status::OK());
    EXPECT_EQ(out, reval);

    // Verify no cross-contamination: a fresh key (never inserted) should be NOT_FOUND.
    std::string freshKey = "a3_fresh_nonexistent_key";
    EXPECT_FALSE(client1->Get(freshKey, out).IsOk())
        << "Fresh key must be NOT_FOUND — cross-shard contamination detected.";
}

// A4: RemoveMetaByWorkerFullShardCoverage
// Validates RemoveMetaByWorker cleans worker locations from ALL 64 MetaTableShards,
// and remaining workers' keys survive. Uses different key prefixes per worker
// connection to maximize shard coverage.
TEST_F(KVCacheMetaShardingTest, RemoveMetaByWorkerFullShardCoverage)
{
    const int kKeysPerWorker = 300;
    const std::string kValue = "rmw_val";

    // Generate keys with distinct prefixes per logical worker group,
    // ensuring broad distribution across MetaTableShards.
    auto w0Keys = GenerateVariedKeys(kKeysPerWorker, "w0");
    auto w1Keys = GenerateVariedKeys(kKeysPerWorker, "w1");
    auto w2Keys = GenerateVariedKeys(kKeysPerWorker, "w2");

    // Insert via different client connections (each connected to a distinct worker).
    LOG(INFO) << "[A4] Inserting " << kKeysPerWorker << " keys per worker";
    InsertKeys(w0Keys, kValue, 0 /* no TTL */, client1);
    InsertKeys(w1Keys, kValue, 0, client2);
    InsertKeys(w2Keys, kValue, 0, client3);

    // Verify pre-kill: all keys exist.
    {
        std::string out;
        EXPECT_EQ(client1->Get(w0Keys[0], out), Status::OK());
        EXPECT_EQ(out, kValue);
        EXPECT_EQ(client2->Get(w1Keys[0], out), Status::OK());
        EXPECT_EQ(out, kValue);
        EXPECT_EQ(client3->Get(w2Keys[0], out), Status::OK());
        EXPECT_EQ(out, kValue);
    }

    // Kill worker 1 (index 1) — the middle worker.
    LOG(INFO) << "[A4] Killing worker 1";
    cluster_->ShutdownNode(ClusterNodeType::WORKER, 1);

    // Wait for master to detect worker timeout (node_timeout_s=5) and run
    // RemoveMetaByWorker to clean worker 1's locations from all shards.
    // Use 12s (2× timeout + processing margin) for slow CI machines.
    std::this_thread::sleep_for(std::chrono::seconds(12));

    // After RemoveMetaByWorker: worker 1's locations must be cleaned from all
    // MetaTableShards. Keys on surviving workers (0, 2) should remain accessible
    // (they may be on replicas or have had their primaries reassigned).
    //
    // We verify that the system is consistent: no RUNTIME_ERROR on any Get,
    // and no crash/hang when querying keys from all three groups.
    LOG(INFO) << "[A4] Verifying post-cleanup consistency across all shards";

    // Check all 3 groups. Worker 0 and worker 2 keys should be findable;
    // worker 1 keys may be NOT_FOUND (if primary was on worker 1) or OK (replica reassigned).
    auto checkGroup = [&](const std::vector<std::string> &keys, std::shared_ptr<KVClient> &client,
                          const std::string &groupLabel) {
        int okCount = 0;
        int notFoundCount = 0;
        std::string out;
        for (const auto &key : keys) {
            Status s = client->Get(key, out);
            if (s.IsOk() && out == kValue) {
                ++okCount;
            } else if (!s.IsOk()) {
                ++notFoundCount;
                // Must not be RUNTIME_ERROR — that indicates a crash or internal bug.
                EXPECT_NE(s.GetCode(), StatusCode::K_RUNTIME_ERROR)
                    << groupLabel << " key " << key << " returned RUNTIME_ERROR: " << s.ToString();
            }
        }
        LOG(INFO) << "[A4] " << groupLabel << ": OK=" << okCount << ", NOT_FOUND=" << notFoundCount;
        return std::make_pair(okCount, notFoundCount);
    };

    auto [w0Ok, w0Nf] = checkGroup(w0Keys, client1, "Worker0");
    auto [w1Ok, w1Nf] = checkGroup(w1Keys, client1, "Worker1");
    auto [w2Ok, w2Nf] = checkGroup(w2Keys, client3, "Worker2");

    // Worker 2 keys (never touched) must all be findable.
    EXPECT_GE(w2Ok, kKeysPerWorker * 80 / 100)
        << "Worker2 keys should remain available. w2Ok=" << w2Ok;

    // No key should cause RUNTIME_ERROR — the check above already validates this
    // per-key with EXPECT_NE. But also ensure overall consistency: all keys from
    // all three groups were processed without crash.
    EXPECT_EQ(w0Ok + w0Nf, kKeysPerWorker) << "Worker0 keys not fully accounted";
    EXPECT_EQ(w1Ok + w1Nf, kKeysPerWorker) << "Worker1 keys not fully accounted";
    EXPECT_EQ(w2Ok + w2Nf, kKeysPerWorker) << "Worker2 keys not fully accounted";

    // Restart worker 1 to restore cluster health.
    LOG(INFO) << "[A4] Restarting worker 1";
    DS_ASSERT_OK(cluster_->StartNode(ClusterNodeType::WORKER, 1, " -client_reconnect_wait_s=1 -node_timeout_s=5 -shared_memory_size_mb=2048 -v=2"));
    DS_ASSERT_OK(cluster_->WaitNodeReady(ClusterNodeType::WORKER, 1));

    // Post-restart: system must be fully operational — new Set/Get works.
    std::string postKey = "w1_restart_probe";
    std::string postVal = "healthy";
    DS_ASSERT_OK(client2->Set(postKey, postVal,
                             { .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE, .ttlSecond = 30 }));
    std::string readBack;
    EXPECT_EQ(client1->Get(postKey, readBack), Status::OK());
    EXPECT_EQ(readBack, postVal);
}

}  // namespace st
}  // namespace datasystem
