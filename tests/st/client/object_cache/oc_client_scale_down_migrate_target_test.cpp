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
 * Description: Reproduction ST for issue #1180: voluntary scale-down must migrate data to the per-key
 * token takeover owner, not to the single address-order successor.
 */

#include <chrono>
#include <cstdint>
#include <functional>
#include <iterator>
#include <limits>
#include <map>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

#include <sys/wait.h>

#include "cluster/topology_token_helper.h"
#include "common.h"
#include "common_distributed_ext.h"
#include "oc_client_common.h"
#include "datasystem/common/util/format.h"
#include "datasystem/protos/cluster_topology.pb.h"

namespace datasystem {
namespace st {
namespace {
constexpr uint32_t WORKER0 = 0;
constexpr uint32_t WORKER1 = 1;
constexpr uint32_t WORKER2 = 2;
constexpr char HASH_KEY_PREFIX[] = "a_key_hash_to_";
constexpr size_t KEY_VALUE_SIZE = 1024;
constexpr size_t KEYS_PER_TOKEN_RANGE = 2;
constexpr size_t CONTROL_KEYS_PER_TOKEN_RANGE = 1;
constexpr size_t CONTROL_KEYS_PER_WORKER = 4;
constexpr size_t MIN_MIGRATED_KEYS = 8;
constexpr size_t MIN_TAKEOVER_OWNERS = 2;
constexpr int POLL_INTERVAL_MS = 100;
constexpr int RING_STABLE_TIMEOUT_MS = 60'000;
constexpr int WORKER_EXIT_TIMEOUT_MS = 60'000;
constexpr int META_CONVERGE_TIMEOUT_MS = 30'000;
constexpr int RING_SETTLE_DELAY_MS = 1'000;
constexpr size_t FILLER_OBJECT_COUNT = 8;
constexpr size_t FILLER_OBJECT_SIZE = 5UL * 1024UL * 1024UL;
constexpr int DRAIN_LIVENESS_EXIT_TIMEOUT_MS = 45'000;

struct HashedKey {
    std::string key;
    uint32_t hash;
    uint32_t workerIndex;
};

std::map<uint32_t, std::string> BuildTokenWorkers(const ClusterTopologyPb &ring)
{
    std::map<uint32_t, std::string> tokenWorkers;
    for (const auto &worker : ring.members()) {
        for (auto token : RebuildTopologyMemberTokens(ring, worker.first, worker.second)) {
            tokenWorkers.emplace(token, worker.first);
        }
    }
    return tokenWorkers;
}

std::string ExpectedOwner(const std::map<uint32_t, std::string> &tokenWorkers, uint32_t hash)
{
    auto iter = tokenWorkers.lower_bound(hash);
    if (iter == tokenWorkers.end()) {
        iter = tokenWorkers.begin();
    }
    return iter->second;
}

std::string JoinStrings(const std::vector<std::string> &values, const std::string &separator = ",")
{
    std::string joined;
    for (const auto &value : values) {
        if (!joined.empty()) {
            joined += separator;
        }
        joined += value;
    }
    return joined;
}

std::vector<std::string> KeysOf(const std::vector<HashedKey> &hashedKeys)
{
    std::vector<std::string> keys;
    keys.reserve(hashedKeys.size());
    for (const auto &hashedKey : hashedKeys) {
        keys.emplace_back(hashedKey.key);
    }
    return keys;
}
}  // namespace

class LEVEL1_OCScaleDownMigrateTargetTest : public OCClientCommon, public CommonDistributedExt {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = 3;
        opts.numEtcd = 0;
        opts.numCoordinators = 1;
        opts.numOBS = 0;
        opts.workerGflagParams = "-shared_memory_size_mb=64 -log_monitor=true -enable_lossless_data_exit_mode=true "
                                 "-node_timeout_s=5 -node_dead_timeout_s=10";
    }

    void SetUp() override
    {
        OCClientCommon::SetUp();
        InitTestClient(WORKER0, client0_);
        InitTestClient(WORKER1, client1_);
        InitTestClient(WORKER2, client2_);
        SetWorkerHashInjection();
    }

    void TearDown() override
    {
        client0_.reset();
        client1_.reset();
        client2_.reset();
        OCClientCommon::TearDown();
    }

protected:
    BaseCluster *GetCluster() override
    {
        return cluster_.get();
    }

    bool WaitFor(std::function<bool()> predicate, int timeoutMs)
    {
        auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeoutMs);
        do {
            if (predicate()) {
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(POLL_INTERVAL_MS));
        } while (std::chrono::steady_clock::now() < deadline);
        return predicate();
    }

    std::string CurrentRingDebugString()
    {
        ClusterTopologyPb ring;
        if (cluster_->ReadClusterTopology(ring).IsOk()) {
            return ring.ShortDebugString();
        }
        return "unavailable";
    }

    bool WaitForRingStable(uint32_t expectedWorkers, int timeoutMs)
    {
        bool stable = WaitFor(
            [&] {
                ClusterTopologyPb ring;
                if (cluster_->ReadClusterTopology(ring).IsError()) {
                    return false;
                }
                if (ring.members_size() != static_cast<int>(expectedWorkers)) {
                    return false;
                }
                for (const auto &member : ring.members()) {
                    if (member.second.state() != MembershipPb::ACTIVE) {
                        return false;
                    }
                }
                return true;
            },
            timeoutMs);
        if (stable) {
            std::this_thread::sleep_for(std::chrono::milliseconds(RING_SETTLE_DELAY_MS));
        }
        return stable;
    }

    void GenKeysHashToWorker(const ClusterTopologyPb &ring, uint32_t workerIndex, size_t perRange,
                             std::vector<HashedKey> &keys,
                             size_t maxKeys = std::numeric_limits<size_t>::max())
    {
        ASSERT_GT(maxKeys, 0);
        const size_t initialSize = keys.size();
        std::map<uint32_t, std::string> tokenWorkers = BuildTokenWorkers(ring);
        HostPort workerAddress;
        DS_ASSERT_OK(cluster_->GetWorkerAddr(workerIndex, workerAddress));
        const std::string workerAddr = workerAddress.ToString();
        for (auto iter = tokenWorkers.begin(); iter != tokenWorkers.end(); ++iter) {
            if (iter->second != workerAddr) {
                continue;
            }
            auto prev = iter == tokenWorkers.begin() ? std::prev(tokenWorkers.end()) : std::prev(iter);
            uint32_t distance = iter->first - prev->first;
            for (uint32_t offset = 1; offset <= distance && offset <= perRange; ++offset) {
                uint32_t hash = iter->first - offset + 1;
                keys.push_back(HashedKey{ HASH_KEY_PREFIX + std::to_string(hash), hash, workerIndex });
                if (keys.size() - initialSize == maxKeys) {
                    return;
                }
            }
        }
        ASSERT_GT(keys.size(), initialSize) << "no key generated for worker " << workerIndex;
    }

    void PutObjects(const std::vector<HashedKey> &keys)
    {
        std::vector<std::shared_ptr<ObjectClient>> clients = { client0_, client1_, client2_ };
        const std::string payload(KEY_VALUE_SIZE, 'x');
        for (const auto &hashedKey : keys) {
            Status rc = clients[hashedKey.workerIndex]->Put(hashedKey.key,
                                                            reinterpret_cast<const uint8_t *>(payload.data()),
                                                            payload.size(), CreateParam{});
            ASSERT_TRUE(rc.IsOk()) << FormatString("Put %s failed: %s", hashedKey.key, rc.ToString());
        }
    }

    std::vector<std::string> CollectAffinityMismatches(const std::vector<HashedKey> &hashedKeys,
                                                       const std::vector<ObjMetaInfo> &metas,
                                                       const std::map<uint32_t, std::string> &preTokens,
                                                       const std::map<uint32_t, std::string> &postTokens) const
    {
        std::vector<std::string> mismatches;
        for (size_t i = 0; i < hashedKeys.size() && i < metas.size(); ++i) {
            const bool migrated = hashedKeys[i].workerIndex == WORKER0;
            const std::string expected = migrated ? ExpectedOwner(postTokens, hashedKeys[i].hash)
                                                  : ExpectedOwner(preTokens, hashedKeys[i].hash);
            if (metas[i].locations != std::vector<std::string>({ expected })) {
                mismatches.emplace_back(FormatString("key=%s hash=%u workerIndex=%u expected=[%s] actual=[%s]",
                                                     hashedKeys[i].key.c_str(), hashedKeys[i].hash,
                                                     hashedKeys[i].workerIndex, expected.c_str(),
                                                     JoinStrings(metas[i].locations).c_str()));
            }
        }
        return mismatches;
    }

    std::shared_ptr<ObjectClient> client0_;
    std::shared_ptr<ObjectClient> client1_;
    std::shared_ptr<ObjectClient> client2_;
};

TEST_F(LEVEL1_OCScaleDownMigrateTargetTest, ScaleDownMigratesDataToTokenTakeoverOwner)
{
    ASSERT_TRUE(WaitForRingStable(3, RING_STABLE_TIMEOUT_MS))
        << "topology never reached 3 active members: " << CurrentRingDebugString();
    ClusterTopologyPb ring;
    DS_ASSERT_OK(cluster_->ReadClusterTopology(ring));
    const std::map<uint32_t, std::string> preTokens = BuildTokenWorkers(ring);
    HostPort worker0Addr;
    DS_ASSERT_OK(cluster_->GetWorkerAddr(WORKER0, worker0Addr));

    std::vector<HashedKey> controlKeys;
    GenKeysHashToWorker(ring, WORKER1, CONTROL_KEYS_PER_TOKEN_RANGE, controlKeys, CONTROL_KEYS_PER_WORKER);
    GenKeysHashToWorker(ring, WORKER2, CONTROL_KEYS_PER_TOKEN_RANGE, controlKeys, CONTROL_KEYS_PER_WORKER);
    // Spread coverage: bucket the generated owner keys by their post-scale-in takeover owner (the pre-exit ring
    // minus the leaving worker's tokens) and escalate sampling until the buckets span both survivors; degenerate
    // token layouts where every range succeeds to one worker proceed with single-owner coverage, and the per-key
    // expected==actual assertions below remain the regression oracle.
    std::map<uint32_t, std::string> expectedPostTokens;
    for (const auto &[token, owner] : preTokens) {
        if (owner != worker0Addr.ToString()) {
            expectedPostTokens.emplace(token, owner);
        }
    }
    std::map<std::string, std::vector<HashedKey>> keysByTakeoverOwner;
    size_t perRange = KEYS_PER_TOKEN_RANGE;
    for (int attempt = 0; attempt < 4; ++attempt) {
        keysByTakeoverOwner.clear();
        std::vector<HashedKey> ownerCandidates;
        GenKeysHashToWorker(ring, WORKER0, perRange, ownerCandidates);
        for (const auto &hashedKey : ownerCandidates) {
            keysByTakeoverOwner[ExpectedOwner(expectedPostTokens, hashedKey.hash)].push_back(hashedKey);
        }
        const size_t perOwnerQuota =
            std::max(KEYS_PER_TOKEN_RANGE, MIN_MIGRATED_KEYS / std::max<size_t>(keysByTakeoverOwner.size(), 1));
        size_t selectable = 0;
        for (const auto &[owner, bucket] : keysByTakeoverOwner) {
            (void)owner;
            selectable += std::min(bucket.size(), perOwnerQuota);
        }
        if (keysByTakeoverOwner.size() >= MIN_TAKEOVER_OWNERS && selectable >= MIN_MIGRATED_KEYS) {
            break;
        }
        perRange *= 2;
    }
    ASSERT_FALSE(keysByTakeoverOwner.empty());
    const bool spreadAvailable = keysByTakeoverOwner.size() >= MIN_TAKEOVER_OWNERS;
    const size_t perOwnerQuota = std::max(KEYS_PER_TOKEN_RANGE, MIN_MIGRATED_KEYS / keysByTakeoverOwner.size());
    std::vector<HashedKey> ownerKeys;
    for (auto &[owner, bucket] : keysByTakeoverOwner) {
        const size_t take = std::min(bucket.size(), perOwnerQuota);
        ownerKeys.insert(ownerKeys.end(), bucket.begin(), bucket.begin() + static_cast<std::ptrdiff_t>(take));
    }
    ASSERT_GE(ownerKeys.size(), MIN_MIGRATED_KEYS);

    std::vector<HashedKey> allKeys = ownerKeys;
    allKeys.insert(allKeys.end(), controlKeys.begin(), controlKeys.end());
    PutObjects(allKeys);

    const std::vector<std::string> keys = KeysOf(allKeys);
    // Single-replica invariant: with no cross-worker reads a Put keeps exactly one location per key, so the
    // locations == {owner} assertions below are the precise data-position oracle.
    std::vector<ObjMetaInfo> metas;
    DS_ASSERT_OK(client1_->GetObjMetaInfo("", keys, metas));
    ASSERT_EQ(metas.size(), allKeys.size());
    for (size_t i = 0; i < allKeys.size(); ++i) {
        const std::string expected = ExpectedOwner(preTokens, allKeys[i].hash);
        ASSERT_EQ(metas[i].locations, std::vector<std::string>({ expected }))
            << FormatString("pre-scale-down key=%s hash=%u expected=[%s] actual=[%s]", keys[i].c_str(),
                            allKeys[i].hash, expected.c_str(), JoinStrings(metas[i].locations).c_str());
    }

    client0_.reset();
    VoluntaryScaleDownInject(static_cast<int>(WORKER0));
    const pid_t leavingPid = cluster_->GetWorkerPid(WORKER0);
    ASSERT_TRUE(WaitFor(
        [&] {
            int status = 0;
            return waitpid(leavingPid, &status, WNOHANG) != 0;
        },
        WORKER_EXIT_TIMEOUT_MS))
        << "WORKER0 did not exit within " << WORKER_EXIT_TIMEOUT_MS << "ms after voluntary scale-down";
    ASSERT_TRUE(WaitForRingStable(2, RING_STABLE_TIMEOUT_MS))
        << "topology never converged to 2 active members after WORKER0 scale-down: " << CurrentRingDebugString();

    ClusterTopologyPb postRing;
    DS_ASSERT_OK(cluster_->ReadClusterTopology(postRing));
    ASSERT_EQ(postRing.members_size(), 2) << postRing.ShortDebugString();
    ASSERT_EQ(postRing.members().count(worker0Addr.ToString()), 0) << postRing.ShortDebugString();
    const std::map<uint32_t, std::string> postTokens = BuildTokenWorkers(postRing);
    std::unordered_set<std::string> takeoverOwners;
    for (const auto &hashedKey : ownerKeys) {
        std::string owner = ExpectedOwner(postTokens, hashedKey.hash);
        ASSERT_NE(owner, worker0Addr.ToString())
            << FormatString("token takeover owner of key=%s hash=%u is the leaving worker", hashedKey.key.c_str(),
                            hashedKey.hash);
        (void)takeoverOwners.emplace(std::move(owner));
    }
    if (spreadAvailable) {
        ASSERT_GE(takeoverOwners.size(), MIN_TAKEOVER_OWNERS)
            << "token takeover owners of migrated keys span only " << takeoverOwners.size() << " worker(s)";
    }

    std::vector<std::string> mismatches{ "location query never produced a complete result set" };
    ASSERT_TRUE(WaitFor(
        [&] {
            std::vector<ObjMetaInfo> latest;
            Status rc = client1_->GetObjMetaInfo("", keys, latest);
            if (rc.IsError() || latest.size() != keys.size()) {
                return false;
            }
            mismatches = CollectAffinityMismatches(allKeys, latest, preTokens, postTokens);
            return mismatches.empty();
        },
        META_CONVERGE_TIMEOUT_MS))
        << "data location violates token-takeover affinity for " << mismatches.size() << " key(s):\n"
        << JoinStrings(mismatches, "\n");
}

// Disabled: with the takeover targets preloaded past the FIRST-stage ratio gate, the source's MigrateData
// connections fail with E112 before the stage ladder is ever consulted (reproduces on unfixed master too);
// the retry-stage liveness contract stays covered by DataMigratorScaleDownTest.GroupedRedirectRetryEscalatesStageAndCompletes.
TEST_F(LEVEL1_OCScaleDownMigrateTargetTest, DISABLED_ScaleDownDrainEscalatesStageWhenTargetsArePartiallyFull)
{
    ASSERT_TRUE(WaitForRingStable(3, RING_STABLE_TIMEOUT_MS))
        << "topology never reached 3 active members: " << CurrentRingDebugString();
    ClusterTopologyPb ring;
    DS_ASSERT_OK(cluster_->ReadClusterTopology(ring));
    const std::map<uint32_t, std::string> preTokens = BuildTokenWorkers(ring);
    HostPort worker0Addr;
    DS_ASSERT_OK(cluster_->GetWorkerAddr(WORKER0, worker0Addr));

    std::vector<HashedKey> fillerKeys;
    std::vector<HashedKey> fillerW1;
    GenKeysHashToWorker(ring, WORKER1, FILLER_OBJECT_COUNT, fillerW1);
    ASSERT_GE(fillerW1.size(), FILLER_OBJECT_COUNT);
    fillerW1.resize(FILLER_OBJECT_COUNT);
    std::vector<HashedKey> fillerW2;
    GenKeysHashToWorker(ring, WORKER2, FILLER_OBJECT_COUNT, fillerW2);
    ASSERT_GE(fillerW2.size(), FILLER_OBJECT_COUNT);
    fillerW2.resize(FILLER_OBJECT_COUNT);
    fillerKeys = fillerW1;
    fillerKeys.insert(fillerKeys.end(), fillerW2.begin(), fillerW2.end());
    const std::string fillerPayload(FILLER_OBJECT_SIZE, 'f');
    std::vector<std::shared_ptr<ObjectClient>> fillerClients = { client0_, client1_, client2_ };
    for (const auto &hashedKey : fillerKeys) {
        Status rc = fillerClients[hashedKey.workerIndex]->Put(hashedKey.key,
                                                              reinterpret_cast<const uint8_t *>(fillerPayload.data()),
                                                              fillerPayload.size(), CreateParam{});
        ASSERT_TRUE(rc.IsOk()) << FormatString("Put filler %s failed: %s", hashedKey.key.c_str(), rc.ToString());
    }

    std::vector<HashedKey> ownerKeys;
    GenKeysHashToWorker(ring, WORKER0, KEYS_PER_TOKEN_RANGE, ownerKeys);
    ASSERT_GE(ownerKeys.size(), MIN_MIGRATED_KEYS);
    PutObjects(ownerKeys);

    client0_.reset();
    VoluntaryScaleDownInject(static_cast<int>(WORKER0));
    const pid_t leavingPid = cluster_->GetWorkerPid(WORKER0);
    ASSERT_TRUE(WaitFor(
        [&] {
            int status = 0;
            return waitpid(leavingPid, &status, WNOHANG) != 0;
        },
        DRAIN_LIVENESS_EXIT_TIMEOUT_MS))
        << "WORKER0 did not exit within " << DRAIN_LIVENESS_EXIT_TIMEOUT_MS
        << "ms after voluntary scale-down; drain lost liveness on partially-full takeover targets";
    ASSERT_TRUE(WaitForRingStable(2, RING_STABLE_TIMEOUT_MS))
        << "topology never converged to 2 active members after WORKER0 scale-down: " << CurrentRingDebugString();

    ClusterTopologyPb postRing;
    DS_ASSERT_OK(cluster_->ReadClusterTopology(postRing));
    ASSERT_EQ(postRing.members_size(), 2) << postRing.ShortDebugString();
    ASSERT_EQ(postRing.members().count(worker0Addr.ToString()), 0) << postRing.ShortDebugString();
    const std::map<uint32_t, std::string> postTokens = BuildTokenWorkers(postRing);

    const std::vector<std::string> keys = KeysOf(ownerKeys);
    std::vector<std::string> mismatches{ "location query never produced a complete result set" };
    ASSERT_TRUE(WaitFor(
        [&] {
            std::vector<ObjMetaInfo> latest;
            Status rc = client1_->GetObjMetaInfo("", keys, latest);
            if (rc.IsError() || latest.size() != keys.size()) {
                return false;
            }
            mismatches = CollectAffinityMismatches(ownerKeys, latest, preTokens, postTokens);
            return mismatches.empty();
        },
        META_CONVERGE_TIMEOUT_MS))
        << "drained data violates token-takeover affinity for " << mismatches.size() << " key(s):\n"
        << JoinStrings(mismatches, "\n");
}
}  // namespace st
}  // namespace datasystem
