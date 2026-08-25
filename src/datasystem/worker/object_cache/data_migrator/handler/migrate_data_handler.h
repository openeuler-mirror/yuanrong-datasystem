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

/**
 * Description: Migrate data handler.
 */
#ifndef DATASYSTEM_MIGRATE_DATA_HANDLER_H
#define DATASYSTEM_MIGRATE_DATA_HANDLER_H

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "datasystem/common/immutable_string/immutable_string.h"
#include "datasystem/worker/object_cache/data_migrator/basic/base_data_unit.h"
#include "datasystem/worker/object_cache/data_migrator/handler/async_resource_releaser.h"
#include "datasystem/worker/object_cache/limiter/data_limiter.h"
#include "datasystem/worker/object_cache/data_migrator/basic/migrate_progress.h"
#include "datasystem/worker/object_cache/data_migrator/strategy/selection_strategy.h"
#include "datasystem/worker/object_cache/data_migrator/transport/migrate_transport.h"
#include "datasystem/worker/object_cache/object_kv.h"
#include "datasystem/worker/object_cache/worker_worker_oc_api.h"

namespace datasystem {
namespace object_cache {

class MigrateDataHandler {
public:
    // SelfHealBusyRate probes the remote after a migrate batch returns an unusable limit rate. The remote's
    // MigrateDataRateLimiter uses a 1-second sliding window; a full batch (= maxBandwidth) saturates
    // it, so availableBandwidth stays 0 until the entry expires (~1s after receipt). The 3-probe
    // backoff sequence MUST span more than 1 second so probe 3 lands after the window drains:
    //   probe 1 at +200~400ms, probe 2 at +600~1200ms, probe 3 at +1400~2200ms.
    // Worst case (all minimum sleeps): 1400ms, giving 400ms margin past the 1s window. Do NOT
    // reduce these values without verifying probe 3 still clears the 1-second sliding window.
    static constexpr uint64_t BUSY_HEAL_BUDGET_MS = 3000;
    static constexpr uint64_t BUSY_HEAL_INITIAL_SLEEP_MS = 200;
    static constexpr uint64_t BUSY_HEAL_MAX_SLEEP_MS = 1000;
    static constexpr uint64_t BUSY_HEAL_BACKOFF_FACTOR = 2;
    static constexpr int BUSY_HEAL_MAX_PROBES = 3;
    static constexpr uint64_t BUSY_HEAL_CANCEL_POLL_MS = 10;
    static constexpr uint64_t BUSY_HEAL_PROBE_TIMEOUT_MS = 2000;
    static constexpr uint64_t SCALE_DOWN_MIN_LIMITER_WAIT_MS = 1000;

    MigrateDataHandler(MigrateType type, const std::string &localAddr,
                       const std::vector<ImmutableString> &needMigrateDataIds, std::shared_ptr<ObjectTable> objectTable,
                       std::shared_ptr<WorkerRemoteWorkerOCApi> remoteApi, std::shared_ptr<SelectionStrategy> strategy,
                       const std::atomic<bool> *stoppingPtr,
                       std::shared_ptr<MigrateProgress> progress = nullptr, bool isRetry = false, uint32_t slotId = 0,
                       std::unordered_map<std::string, double> objectHeats = {},
                       RebalancePolicyFence rebalancePolicyFence = {});

    ~MigrateDataHandler() = default;

    struct MigrateResult {
        std::string address;
        Status status;
        std::unordered_set<ImmutableString> successIds;
        std::unordered_set<ImmutableString> failedIds;
        std::unordered_set<ImmutableString> skipIds;
        std::shared_ptr<SelectionStrategy> strategy;
        std::optional<ProviderUbFailureDetailPb> ubFailureDetail;
        int retryCount = 0;
        // Target's fresh available memory (headroom to high-water) reported by the target in the
        // last batch's MigrateDataRspPb. UINT64_MAX means no batch was sent (e.g. all objects
        // skipped); callers must treat it as "no fresh signal". Surfaced so the rebalance executor
        // can forward it to master for per-batch gap recompute.
        uint64_t targetRemainBytes{ UINT64_MAX };
    };

    /**
     * @brief Migrate object data to remote node.
     * @param[in] isSlotMigration Whether this is a slot migration (no batching).
     * @return Migrate result contains ip address, status, success ids, failed ids and skip ids.
     */
    MigrateResult MigrateDataToRemote(bool isSlotMigration = false);

    /**
     * @brief Set the admission check executed immediately before every transport batch.
     * @param[in] admission Non-blocking check owned by the enclosing DataMigrator.
     */
    void SetSendAdmission(std::function<Status()> admission)
    {
        sendAdmission_ = std::move(admission);
    }

    /**
     * @brief Pretty print the migrate result.
     * @param[in] result Migrate data result.
     * @return migrate result in string.
     */
    static std::string ResultToString(const MigrateResult &result);

    /**
     * @brief Migrate data to remote rpc, if meets RPC error, it would retry.
     * @param[in] api Remote worker api.
     * @param[in] req Migrate data request.
     * @param[in] payloads Need migrate data.
     * @param[out] rsp Migrate data response.
     * @return K_OK if success, the error otherwise.
     */
    Status MigrateDataToRemoteRetry(const std::shared_ptr<WorkerRemoteWorkerOCApi> &api, MigrateDataReqPb &req,
                                    const std::vector<MemView> &payloads, MigrateDataRspPb &rsp);

private:
    /**
     * @brief Spy on remote node remian bytes.
     * @param[in] type The cache type.
     * @return K_OK if success, the error otherwise.
     */
    Status SpyOnRemoteRemainBytes(CacheType type);

    /**
     * @brief Spy on remote node remain bytes through migrate RPC.
     * @param[in] type The cache type.
     * @return K_OK if success, the error otherwise.
     */
    Status SpyOnRemoteRemainBytesByRpc(CacheType type);

    /**
     * @brief Adjust max batch size via size.
     * @param[in] size New size.
     */
    void AdjustMaxBatchSize(uint64_t size);

    /**
     * @brief Indicate the remote node is lack of resources or not.
     * @param[in] cacheType The cache type being migrated.
     * @return True if remote node is lack of resources.
     */
    bool IsRemoteLackResources(CacheType cacheType) const;

    /**
     * @brief Add object data into migrate data list.
     * @param[in] objectKV Object key value.
     * @return K_OK if success, the error otherwise.
     */
    Status AddObjectDataLocked(const ObjectKV &objectKV);

    /**
     * @brief Send data to remote node.
     * @param[in] isSlotMigration Whether this is a slot migration.
     */
    void SendDataToRemote(bool isSlotMigration = false);
    bool PrepareTransportSend();
    bool CheckSendAdmission();
    Status EnsureRateForBatch();
    Status PrepareReleaseTasks();
    void HandleMigrationTransportResponse(const Status &status, MigrateTransport::Response &response);

    /**
     * @brief Update rate from response, or self-heal when rate is zero.
     * @param[in] rate Rate from response. A zero rate triggers bounded self-heal probing.
     * @return K_OK if rate is non-zero or self-heal succeeds, the error otherwise.
     */
    Status TryUpdateRate(uint64_t rate);

    /**
     * @brief Refresh an unusable remote rate with bounded budget and exponential backoff.
     * @param[in] requiredSize Token size required by the pending batch.
     * @return K_OK if rate recovered, the error otherwise (K_NOT_READY or last RPC error).
     */
    Status SelfHealBusyRate(uint64_t requiredSize);

    /**
     * @brief Build the final self-heal status from the probe outcome.
     * @param[in] recovered Whether the refreshed rate can admit the pending batch within the bounded wait.
     * @param[in] rate Final advertised rate.
     * @param[in] probesMade Number of probes executed.
     * @param[in] lastErr Last RPC error (if any) from probing.
     * @return K_OK if rate recovered, the error otherwise (cancelled, K_NOT_READY, or last RPC error).
     */
    Status BuildHealResult(bool recovered, uint64_t rate, int probesMade, const Status &lastErr);

    bool IsRateRecovered(uint64_t rate, uint64_t estimatedWaitMs, uint64_t requiredSize) const;
    uint64_t GetScaleDownMaxLimiterWaitMilliseconds(uint64_t requiredSize) const;

    /**
     * @brief Construct the migrate data result.
     * @param[in] status Migrate data status.
     * @return Migrate data result.
     */
    MigrateResult ConstructResult(Status status) const;

    /**
     * @brief Clear datas and state.
     */
    void Clear();

    /**
     * @brief Indicate current batch is full or not.
     * @return True if current batch is full.
     */
    bool IsFull() const
    {
        constexpr uint64_t maxBatchCount = 300;
        return currBatchSize_ >= maxBatchSize_ || currBatchCount_ >= maxBatchCount;
    }

    /**
     * @brief Split migrate data by cache type.
     * @param[in] memoryDataIds The memory data.
     * @param[in] diskDataIds The disk data.
     */
    void SplitByCacheType(std::vector<std::string> &memoryDataIds, std::vector<std::string> &diskDataIds);

    /**
     * @brief Migrate data for one cache type.
     * @param[in] type The cache type.
     * @param[in] needMigrateDataIds Objects to migrate.
     * @param[in] isSlotMigration Whether this is a slot migration.
     * @return K_OK if success, the error otherwise.
     */
    Status MigrateDataByCacheType(CacheType type, std::vector<std::string> &needMigrateDataIds, bool isSlotMigration);

    /**
     * @brief Prepare remote state before migrating a cache type.
     * @param[in] type The cache type.
     * @param[in] needMigrateDataIds Objects to migrate.
     * @return K_OK if success, the error otherwise.
     */
    Status PrepareRemoteMigration(CacheType type, const std::vector<std::string> &needMigrateDataIds);

    /**
     * @brief Collect one object into the current migrate batch.
     * @param[in] objectKey Object key.
     * @param[in] isSlotMigration Whether this is a slot migration.
     */
    void CollectObjectForMigration(const std::string &objectKey, bool isSlotMigration);

    /**
     * @brief Indicate whether to use fast transport for migration.
     * @return True if fast transport should be used.
     */
    bool ShouldUseFastTransport() const;
    void ApplyRebalancePolicyFence(MigrateDataReqPb &req) const;

    /**
     * @brief Finalize source resources for confirmed and expired migration results.
     * @param[in] successIds Confirmed migrated object ids.
     * @param[in] expiredIds Stale object ids that must be released.
     */
    void ReleaseResources(const std::unordered_set<ImmutableString> &successIds,
                          const std::unordered_set<ImmutableString> &expiredIds);

    /**
     * @brief Demote the source's primary copies to non-primary (local/replica) without erasing data.
     * Called under REBALANCE_KEEP_LOCAL: the objectTable entry is kept; only the PRIMARY_COPY flag is
     * cleared. The master already knows the target is the new primary (via ReplacePrimary with
     * remove_location=false), so the source's local copy becomes a cacheable replica.
     * @param[in] successIds Successfully migrated object ids.
     */
    void DemotePrimaryCopies(const std::unordered_set<ImmutableString> &successIds);

    MigrateType type_;
    std::string localAddr_;
    std::unordered_set<ImmutableString> needMigrateDataIds_;
    std::shared_ptr<ObjectTable> objectTable_;
    std::shared_ptr<WorkerRemoteWorkerOCApi> remoteApi_;

    uint64_t maxBatchSize_;
    uint64_t currBatchSize_;
    uint64_t currBatchCount_;

    DataLimiter limiter_;
    std::shared_ptr<SelectionStrategy> strategy_;
    std::shared_ptr<MigrateProgress> progress_{ nullptr };
    std::shared_ptr<MigrateTransport> transport_;
    bool isRetry_{ false };
    uint32_t slotId_{ 0 };
    // Non-empty only for heat rebalance. The transport reads this immutable batch metadata while sending.
    std::unordered_map<std::string, double> objectHeats_;
    RebalancePolicyFence rebalancePolicyFence_;

    bool selfHealAttempted_{ false };
    Status lastHealStatus_;
    const std::atomic<bool> *stoppingPtr_{ nullptr };

    std::unordered_set<ImmutableString> successIds_;
    std::unordered_set<ImmutableString> failedIds_;
    std::unordered_set<ImmutableString> skipIds_;
    std::vector<std::unique_ptr<BaseDataUnit>> datas_;
    std::vector<AsyncResourceReleaser::PreparedTask> preparedReleaseTasks_;
    std::optional<ProviderUbFailureDetailPb> ubFailureDetail_;
    std::function<Status()> sendAdmission_;
    // Last target remain_bytes received from a real batch send (updated in
    // HandleMigrationTransportResponse; spy/probe paths do not touch it). UINT64_MAX means no
    // batch sent yet. Surfaced via MigrateResult.targetRemainBytes.
    uint64_t lastRemainBytes_{ UINT64_MAX };

    Status lastRc_;
};

}  // namespace object_cache
}  // namespace datasystem

#endif
