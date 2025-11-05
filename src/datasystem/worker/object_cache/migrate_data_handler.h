/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2024. All rights reserved.
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
 * Description: Threead unsafe migrate data implemetation.
 */
#ifndef DATASYSTEM_MIGRATE_DATA_HANDLER_H
#define DATASYSTEM_MIGRATE_DATA_HANDLER_H

#include <cstdint>
#include <ctime>
#include <functional>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <utility>
#include <vector>

#include "datasystem/common/immutable_string/immutable_string.h"
#include "datasystem/common/object_cache/shm_guard.h"
#include "datasystem/common/rpc/mem_view.h"
#include "datasystem/common/rpc/rpc_message.h"
#include "datasystem/common/shared_memory/shm_unit.h"
#include "datasystem/common/util/thread.h"
#include "datasystem/object/object_enum.h"
#include "datasystem/protos/worker_object.pb.h"
#include "datasystem/worker/object_cache/object_kv.h"
#include "datasystem/worker/object_cache/worker_worker_oc_api.h"

namespace datasystem {
namespace object_cache {
class MigrateProgress {
public:
    MigrateProgress(uint64_t count, uint64_t intervalSeconds, std::function<void(double, uint64_t, uint64_t)> callback);

    ~MigrateProgress();

    void Deal(uint64_t count);

private:
    void Process();

    std::atomic<size_t> count_;
    std::atomic<size_t> processedCount_{ 0 };
    uint64_t intervalSeconds_;
    std::atomic<bool> stopFlag_{ false };
    Thread thread_;
    std::mutex mutex_;
    std::condition_variable cv_;
    std::function<void(double, uint64_t, uint64_t)> callback_;
    Timer timer_;
};

class BaseData {
public:
    BaseData(const ImmutableString &objectKey, uint64_t version) : objectKey_(objectKey), version_(version)
    {
    }

    virtual ~BaseData() = default;

    /**
     * @brief Lock data before use.
     * @return K_OK if success, the error otherwise.
     */
    virtual Status LockData()
    {
        return Status::OK();
    }

    /**
     * @brief Get data memory views.
     * @return Memory views.
     */
    virtual std::vector<MemView> GetMemViews() const = 0;

    /**
     * @brief Get data size.
     * @return Data size.
     */
    virtual uint64_t Size() const = 0;

    /**
     * @brief Get object data version.
     * @return Version.
     */
    uint64_t Version() const
    {
        return version_;
    }

    /**
     * @brief Get object key.
     * @return Object key.
     */
    ImmutableString Id() const
    {
        return objectKey_;
    }

private:
    ImmutableString objectKey_;

    uint64_t version_;
};

class ShmData : public BaseData {
public:
    ShmData(const ImmutableString &objectKey, uint64_t version, std::shared_ptr<ShmUnit> unit, size_t dataSize,
            size_t metaSize)
        : BaseData(objectKey, version),
          data_(static_cast<uint8_t *>(unit->GetPointer()) + metaSize),
          size_(dataSize),
          shmGuard_(std::move(unit), dataSize, metaSize),
          needLock_(metaSize != 0)
    {
    }

    /**
     * @brief Lock data before use.
     * @return K_OK if success, the error otherwise.
     */
    Status LockData() override
    {
        return needLock_ ? shmGuard_.TryRLatch() : Status::OK();
    }

    /**
     * @brief Get data memory views.
     * @return Memory views.
     */
    std::vector<MemView> GetMemViews() const override
    {
        std::vector<MemView> result;
        result.emplace_back(data_, size_);
        return result;
    }

    /**
     * @brief Get data size.
     * @return Data size.
     */
    uint64_t Size() const override
    {
        return size_;
    }

private:
    void *data_;
    uint64_t size_;
    ShmGuard shmGuard_;
    bool needLock_;
};

class PayloadData : public BaseData {
public:
    PayloadData(const ImmutableString &objectKey, uint64_t version, std::vector<RpcMessage> payloads, size_t dataSize)
        : BaseData(objectKey, version), payloads_(std::move(payloads)), size_(dataSize)
    {
    }

    /**
     * @brief Get data memory views.
     * @return Memory views.
     */
    std::vector<MemView> GetMemViews() const override
    {
        std::vector<MemView> result;
        for (const auto &payload : payloads_) {
            result.emplace_back(payload.Data(), payload.Size());
        }
        return result;
    }

    /**
     * @brief Get data size.
     * @return Data size.
     */
    uint64_t Size() const override
    {
        return size_;
    }

private:
    std::vector<RpcMessage> payloads_;
    uint64_t size_;
};

class MigrateDataLimiter {
public:
    MigrateDataLimiter(uint64_t rate);

    ~MigrateDataLimiter() = default;

    /**
     * @brief Wait util tokens match the require size.
     * @param[in] requiredSize Required size.
     */
    void WaitAllow(uint64_t requiredSize);

    /**
     * @brief Update rate.
     * @param[in] rate Limit rate from response.
     */
    void UpdateRate(uint64_t rate);

    /**
     * @brief Check if remote is busy.
     * @return True if remote is busy, false otherwise.
     */
    bool IsRemoteBusyNode() const;

private:
    /**
     * @brief Refill tokens.
     */
    void Refill();

    /**
     * @brief Calculate need wait milliseconds via required size.
     * @param[in] requiredSize Required size.
     * @return Need wait milliseconds.
     */
    std::time_t WaitMilliseconds(uint64_t requiredSize);

    uint64_t rate_;

    uint64_t tokens_;

    std::time_t timestamp_;

    mutable std::mutex mtx_;

    std::condition_variable cond_;
};

class MigrateStrategy {
public:
    enum class MigrationStrategyStage {
        /**
         * @brief The first stage of migration, where the migration starts and initial checks are performed.
         * @details In this stage, the migration is allowed only if the available space ratio is above a high threshold
         *          (e.g., 50%) and the node is not leaving.
         */
        FIRST,
        /**
         * @brief The second stage of migration, where the migration continues with a lower available space threshold.
         * @details In this stage, the migration is allowed if the available space ratio is above a lower threshold
         *          (e.g., 20%) and the node is not leaving.
         */
        SECOND,
        /**
         * @brief The third stage of migration, where the migration is allowed regardless of the available space ratio.
         * @details In this stage, the migration is allowed as long as the node is not leaving.
         */
        THIRD,
        /**
         * @brief The final stage of migration, where the migration is always allowed.
         * @details In this stage, the migration is unconditionally allowed to ensure all remaining data is migrated.
         */
        FINAL
    };

    explicit MigrateStrategy(MigrationStrategyStage stage = MigrationStrategyStage::FIRST)
        : currentStage_(stage),
          currentDiskStage_(stage),
          count_(0),
          diskCount_(0)
    {
        visitedAddresses_.clear();
        visitedAddressesForDisk_.clear();
    }

    ~MigrateStrategy() = default;

    /**
     * @brief Checks whether the migration condition is met based on the response and cache type.
     * @param[in] rsp The migration response containing information about the migration status.
     * @param[in] type The cache type (e.g., memory or disk).
     * @return True if the migration condition is satisfied, false otherwise.
     */
    bool CheckCondition(const MigrateDataRspPb &rsp, const CacheType &type);

    /**
     * @brief Updates the node size based on the current state.
     * @param[in] nodeSize The size of the node to be updated.
     */
    void UpdateNodeSize(size_t nodeSize);

    /**
     * @brief Checks if the current worker has been visited and upgrades the migration stage if a full cycle is
     * detected.
     * @param[in] currentWorker The address of the current worker being checked.
     */
    void CheckAndUpgradeStage(const std::string &currentWorker);

private:
    /**
     * @brief Evaluates whether the migration condition for a specific stage is met.
     * @param[in] stage The migration stage to evaluate.
     * @param[in] needScaleDown Indicates whether the node is leaving the cluster.
     * @param[in] isDataMigrationStarted Indicates whether data migration has started.
     * @param[in] availableSpaceRatio The ratio of available space on the target node.
     * @return True if the migration condition for the specified stage is satisfied, false otherwise.
     */
    bool EvaluateStageCondition(MigrationStrategyStage stage, bool needScaleDown, bool isDataMigrationStarted,
                                double availableSpaceRatio);

    /**
     * @brief Increments the migration stage if it is not already in the FINAL stage.
     * @param[in,out] stage The current migration stage to be incremented.
     */
    void IncrementStage(MigrationStrategyStage &stage);

    std::unordered_set<std::string> visitedAddresses_;
    std::unordered_set<std::string> visitedAddressesForDisk_;

    MigrationStrategyStage currentStage_;
    MigrationStrategyStage currentDiskStage_;

    size_t count_;
    size_t diskCount_;
};

class MigrateDataRateLimiter {
public:
    MigrateDataRateLimiter(uint64_t maxBandwidthBytes) : maxBandwidth(maxBandwidthBytes), currentBandwidth(0)
    {
    }

    ~MigrateDataRateLimiter() = default;

    /**
     * @brief Update current bandwidth by sliiding window.
     * @param[in] bytesReceived Bytes received of this reqeust.
     */
    void SlidingWindowUpdateRate(const uint64_t &bytesReceived);

    /**
     * @brief Get available bandwidth.
     * @return Available bandwidth of this node.
     */
    uint64_t GetAvailableBandwidth()
    {
        std::lock_guard<std::shared_timed_mutex> l(mutex_);
        if (currentBandwidth >= maxBandwidth) {
            return 0;
        }
        return maxBandwidth - currentBandwidth;
    }

    /**
     * @brief Get max bandwidth.
     * @return Max bandwidth of this node.
     */
    uint64_t GetMaxBandwidth()
    {
        std::lock_guard<std::shared_timed_mutex> l(mutex_);
        return maxBandwidth;
    }

private:
    struct TimestampedData {
        std::chrono::time_point<std::chrono::steady_clock> timestamp;
        uint64_t bytes;
    };

    std::shared_timed_mutex mutex_;
    std::deque<TimestampedData> window;
    const uint64_t maxBandwidth;
    uint64_t currentBandwidth;
};

class MigrateDataHandler {
public:
    MigrateDataHandler(const std::string &localAddr, const std::vector<ImmutableString> &needMigrateDataIds,
                       std::shared_ptr<ObjectTable> objectTable, std::shared_ptr<WorkerRemoteWorkerOCApi> remoteApi,
                       std::shared_ptr<MigrateProgress> progress = nullptr,
                       const MigrateStrategy migrateDataStrategy = MigrateStrategy());

    ~MigrateDataHandler() = default;

    struct MigrateResult {
        std::string address;
        Status status;
        std::unordered_set<ImmutableString> successIds;
        std::unordered_set<ImmutableString> failedIds;
        std::unordered_set<ImmutableString> skipIds;
        MigrateStrategy migrateDataStrategy;
    };

    /**
     * @brief Migrate object data to remote node.
     * @return Migrate result contains ip address, status, success ids, failed ids and skip ids.
     */
    MigrateResult MigrateDataToRemote();

    /**
     * @brief Pretty print the migrate result.
     * @param[in] result Migrate data result.
     * @return migrate result in string.
     */
    static std::string ResultToString(const MigrateResult &result);

private:
    /**
     * @brief Spy on remote node remian bytes.
     * @param[in] type The cache type.
     * @return K_OK if success, the error otherwise.
     */
    Status SpyOnRemoteRaminBytes(CacheType type);

    /**
     * @brief Adjust max batch size via size.
     * @param[in] size New size.
     */
    void AdjustMaxBatchSize(uint64_t size);

    /**
     * @brief Indicate the remote node is lack of resources or not.
     * @return True if remote node is lack of resources.
     */
    bool IsRemoteLackResources() const;

    /**
     * @brief Add object data into migrate data list.
     * @param[in] objectKV Object key value.
     * @return K_OK if success, the error otherwise.
     */
    Status AddObjectDataLocked(const ObjectKV &objectKV);

    /**
     * @brief Send data to remote node.
     */
    void SendDataToRemote();

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

    /**
     * @brief Try update rate by response for 5 times.
     * @param[in] rate Rate from response.
     * @return K_OK if success, the error otherwise.
     */
    Status TryUpdateRate(uint64_t rate);

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

    std::string localAddr_;

    std::unordered_set<ImmutableString> needMigrateDataIds_;

    std::shared_ptr<ObjectTable> objectTable_;

    std::unordered_set<ImmutableString> successIds_;

    std::unordered_set<ImmutableString> failedIds_;

    std::unordered_set<ImmutableString> skipIds_;

    std::shared_ptr<WorkerRemoteWorkerOCApi> remoteApi_;

    uint64_t maxBatchSize_;

    uint64_t currBatchSize_;

    uint64_t currBatchCount_;

    std::vector<std::unique_ptr<BaseData>> datas_;

    Status lastRc_;

    MigrateDataLimiter limiter_;

    std::shared_ptr<MigrateProgress> progress_{ nullptr };

    MigrateStrategy migrateDataStrategy_;
};
}  // namespace object_cache
}  // namespace datasystem

#endif
