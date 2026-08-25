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
 * Description: Pluggable eviction policy operating on the shared EvictionList.
 *
 * WorkerOcEvictionManager owns a single EvictionStrategy and delegates the
 * touch / decay / candidate-selection concerns to it. The actual eviction ACTION
 * (DELETE / FREE_MEMORY / SPILL / END_LIFE / RETAIN) is decided by the manager's
 * GetObjectNextAction and is strategy-independent; only WHO is selected and HOW
 * heat is maintained differ.
 *
 *  - ClockEvictionStrategy wraps the existing clock/second-chance algorithm with
 *    zero behavior change (decay happens per-scan inside FindEvictCandidate).
 *  - HeatEvictionStrategy uses a periodically-decayed heat counter (per-copy-type
 *    half-life) and threshold-based selection; decay runs before each worker
 *    periodic resource report.
 */
#ifndef DATASYSTEM_WORKER_OBJECT_CACHE_EVICTION_STRATEGY_H
#define DATASYSTEM_WORKER_OBJECT_CACHE_EVICTION_STRATEGY_H

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "datasystem/common/object_cache/object_ref_info.h"
#include "datasystem/utils/status.h"
#include "datasystem/worker/object_cache/eviction_list.h"
#include "datasystem/worker/object_cache/object_kv.h"

namespace datasystem {
namespace object_cache {

enum class EvictionPolicy : uint8_t { CLOCK, HEAT };

/**
 * @brief Immutable policy metadata captured when an eviction candidate is selected.
 *
 * The token follows the candidate through synchronous and asynchronous failure paths, so retry does not consult
 * strategy-global mutable state. Generation/update sequence remain opaque identity fields used for final validation.
 */
struct EvictionCandidate {
    std::string objectKey;
    EvictionPolicy policy{ EvictionPolicy::CLOCK };
    double heat{ 0.0 };
    uint64_t generation{ 0 };
    uint64_t heatUpdateSeq{ 0 };
};

/**
 * @brief Selection state owned by one eviction round.
 *
 * Keeping the batch and the selected immutable token in the caller-owned round makes the strategy safe from hidden
 * cross-call state and prevents a future increase in eviction-pool concurrency from silently introducing races.
 */
struct EvictionRoundState {
    std::vector<EvictionList::Node> candidateBatch;
    size_t nextCandidate{ 0 };
    std::optional<EvictionList::Node> selectedCandidate;

    void ClearCandidateBatch()
    {
        candidateBatch.clear();
        nextCandidate = 0;
        selectedCandidate.reset();
    }
};

struct HeatPolicyConfig {
    double halfLifePrimaryS{ 0.0 };
    double halfLifeLocalS{ 0.0 };
    double threshold{ 0.0 };
    double initialCounter{ 0.0 };
    double maxCounter{ 0.0 };
};

uint8_t ComputeClockAddCounter(
    const std::shared_ptr<ObjectGlobalRefTable<ClientKey>> &globalRefTable, const std::string &objectKey);

class EvictionStrategy {
public:
    virtual ~EvictionStrategy() = default;

    /**
     * @brief First-publish / first-insert touch. Idempotent for an existing key
     *        under the heat strategy.
     */
    virtual void OnAdd(const std::string &objectKey) = 0;

    /**
     * @brief Cache-hit touch (a Get request that hit local shared memory). Heat uses migratableSize to normalize the
     *        access credit; Clock deliberately ignores it.
     */
    virtual void OnCacheHit(const std::string &objectKey, uint64_t migratableSize = 0) = 0;

    /**
     * @brief A Get refilled the object from spill, L2, or another worker.
     *
     * Unlike OnAdd, this must account for the Get that paid the refill cost. Heat uses that size-normalized credit as
     * an admission second chance; Clock preserves its historical Add behavior.
     */
    virtual void OnRefill(const std::string &objectKey, uint64_t migratableSize = 0) = 0;

    /**
     * @brief Select the next eviction candidate key. Does not erase the node.
     */
    virtual Status SelectCandidate(EvictionRoundState &round, EvictionCandidate &candidate) = 0;

    /**
     * @brief Revalidate a selected candidate after its object write lock has been acquired.
     */
    virtual bool ValidateCandidate(EvictionRoundState & /* round */, const EvictionCandidate & /* candidate */)
    {
        return true;
    }

    /**
     * @brief Re-add a candidate that was skipped (e.g. being rebalanced) or whose
     *        eviction failed, so it is not re-selected in the same eviction round.
     * @param[in] counter The carried counter from EvictFailedList (e.g. Q1 for
     *            K_TRY_AGAIN spill failures, READD_COUNTER for other failures).
     *            The clock strategy uses this value; the heat strategy ignores it
     *            and restores the heat captured during candidate selection.
     */
    virtual void ReaddCandidate(const EvictionCandidate &candidate, uint8_t counter) = 0;
};

/**
 * @brief Existing clock/second-chance algorithm. OnAdd/OnCacheHit both refill the
 *        node's curCounter (via EvictionList::Add), and
 *        SelectCandidate is EvictionList::FindEvictCandidate. Behavior is identical
 *        to the pre-strategy code path.
 */
class ClockEvictionStrategy : public EvictionStrategy {
public:
    ClockEvictionStrategy(
        EvictionList &list, const std::shared_ptr<ObjectGlobalRefTable<ClientKey>> &globalRefTable)
        : list_(list), globalRefTable_(globalRefTable)
    {
    }
    ~ClockEvictionStrategy() override = default;
    void OnAdd(const std::string &objectKey) override;
    void OnCacheHit(const std::string &objectKey, uint64_t migratableSize = 0) override;
    void OnRefill(const std::string &objectKey, uint64_t migratableSize = 0) override;
    Status SelectCandidate(EvictionRoundState &round, EvictionCandidate &candidate) override;
    void ReaddCandidate(const EvictionCandidate &candidate, uint8_t counter) override;

private:
    EvictionList &list_;
    // The owning manager outlives the strategy. Keep a reference to its shared_ptr slot so Init() can publish the table
    // after the initial strategy is constructed.
    const std::shared_ptr<ObjectGlobalRefTable<ClientKey>> &globalRefTable_;
};

/**
 * @brief Heat-counter strategy. OnAdd inserts a hot node, OnCacheHit increments heat
 *        and refreshes lastAccess, Decay applies count_new = count_old * 0.5^(dt/T)
 *        with a per-copy-type half-life. SelectCandidate keeps recent-access protection,
 *        ranks low/intermediate-heat candidates by heat per allocator byte, and preserves
 *        exact heat/time ordering above the strong-hot boundary.
 */
class HeatEvictionStrategy : public EvictionStrategy {
public:
    HeatEvictionStrategy(EvictionList &list, std::shared_ptr<ObjectTable> objectTable, HeatPolicyConfig config)
        : list_(list), objectTable_(std::move(objectTable)), config_(config)
    {
    }
    ~HeatEvictionStrategy() override = default;
    void OnAdd(const std::string &objectKey) override;
    void OnCacheHit(const std::string &objectKey, uint64_t migratableSize = 0) override;
    void OnRefill(const std::string &objectKey, uint64_t migratableSize = 0) override;
    Status SelectCandidate(EvictionRoundState &round, EvictionCandidate &candidate) override;
    bool ValidateCandidate(EvictionRoundState &round, const EvictionCandidate &candidate) override;
    void ReaddCandidate(const EvictionCandidate &candidate, uint8_t counter) override;

private:
    bool TryResolveCandidateSize(const std::string &objectKey, uint64_t &migratableSize) const;
    EvictionList::HeatNodeMetadata TryResolveCopyType(const std::string &objectKey) const;

    static constexpr size_t HEAT_CANDIDATE_BATCH_SIZE = 256;
    static constexpr uint64_t HEAT_RECENT_ACCESS_PROTECTION_MS = 100;
    static constexpr double HEAT_IMMEDIATE_EVICTION_THRESHOLD = 2.0;
    static constexpr double HEAT_EXACT_RANKING_THRESHOLD = 4.0;
    static constexpr uint64_t HEAT_ACCESS_UNIT_BYTES = 4 * 1024;
    EvictionList &list_;
    std::shared_ptr<ObjectTable> objectTable_;
    const HeatPolicyConfig config_;
};

std::shared_ptr<EvictionStrategy> MakeEvictionStrategy(
    EvictionPolicy policy, EvictionList &list, const std::shared_ptr<ObjectTable> &objectTable,
    const std::shared_ptr<ObjectGlobalRefTable<ClientKey>> &gRefTable, const HeatPolicyConfig &heatConfig);

HeatPolicyConfig GetCurrentHeatPolicyConfig();

}  // namespace object_cache
}  // namespace datasystem

#endif  // DATASYSTEM_WORKER_OBJECT_CACHE_EVICTION_STRATEGY_H
