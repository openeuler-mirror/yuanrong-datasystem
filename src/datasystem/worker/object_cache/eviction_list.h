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
 * Description: Defines EvictionList Interface.
 */
#ifndef DATASYSTEM_EVICTION_LIST_H
#define DATASYSTEM_EVICTION_LIST_H

#include <atomic>
#include <cstdint>
#include <functional>
#include <list>
#include <limits>
#include <memory>
#include <shared_mutex>
#include <string>
#include <utility>
#include <vector>

#include "datasystem/common/immutable_string/immutable_string.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace object_cache {
constexpr uint8_t Q1 = 1;
constexpr uint8_t Q2 = 2;
constexpr uint8_t Q3 = 3;
constexpr uint8_t READD_COUNTER = 5;

class EvictionList {
public:
    enum class StateKind : uint8_t { CLOCK, HEAT };
    enum class HeatMergeMode : uint8_t { PRESERVE_MAX, ADD_CAPPED };

    struct Node {
        Node() = default;

        Node(const ImmutableString &objKey, uint8_t curCnt) : objectKey(objKey), curCounter(curCnt), maxCounter(curCnt)
        {
        }

        // Constructor used by the heat strategy: a node carrying a heat counter and
        // access/decay timestamps (the clock fields stay zero-initialized and unused).
        Node(const ImmutableString &objKey, double heatVal, uint64_t accessMs, uint64_t delayMs)
            : objectKey(objKey), heat(heatVal), lastAccessMs(accessMs), lastDelayMs(delayMs)
        {
        }

        ImmutableString objectKey;
        uint8_t curCounter{ 0 };
        uint8_t maxCounter{ 0 };
        // Heat-strategy fields. Unused (stay 0) under the clock strategy.
        double heat{ 0.0 };
        uint64_t lastAccessMs{ 0 };
        uint64_t lastDelayMs{ 0 };
        // Internal identity/update tokens carried by snapshots. Callers must treat these as opaque.
        uint64_t generation{ 0 };
        uint64_t heatUpdateSeq{ 0 };
        // Approximate allocator footprint resolved after taking the eviction-list snapshot. This is policy-only
        // metadata: resident list nodes do not carry it, and final candidate validation must not depend on it.
        uint64_t migratableSize{ 0 };
    };

    /**
     * @brief Construct EvictionList.
     */
    EvictionList();

    ~EvictionList() = default;

    /**
     * @brief Add a object to EvictionList.
     * @param[in] objectKey The ID of the object to add.
     * @param[in] counter The counter of the object to add.
     */
    void Add(const std::string &objectKey, uint8_t counter);

    /**
     * @brief Erase a object from EvictionList.
     * @param[in] objectKey The ID of the object to erase.
     * @return Status of the call.
     */
    Status Erase(const std::string &objectKey);

    /**
     * @brief Get the size of EvictionList.
     * @return The size of EvictionList.
     */
    size_t Size();

    /**
     * @brief Find a object that current counter is 0.
     * @param[out] candidateObjKey The ID of the object that current counter is 0.
     * @return Status of the call.
     */
    Status FindEvictCandidate(std::string &candidateObjKey);

    // ---- Heat-strategy surface ----
    // The following methods are only used by the heat eviction strategy. Under the
    // clock strategy they are never called and the heat/lastAccessMs/lastDelayMs Node
    // fields stay zero-initialized.

    using HeatCandidateSizeResolver = std::function<bool(const std::string &, uint64_t &)>;

    struct HeatCandidateOptions {
        uint64_t recentAccessProtectionMs{ 0 };
        double immediateEvictionThreshold{ -std::numeric_limits<double>::infinity() };
        double exactRankingThreshold{ -std::numeric_limits<double>::infinity() };
        HeatCandidateSizeResolver sizeResolver;
    };

    struct HeatNodeMetadata {
        // False means metadata could not be read without waiting (for example, an object writer owns its latch).
        // The decay pass skips such a node instead of misclassifying it as a local copy with a shorter half-life.
        bool resolved{ false };
        bool isPrimary{ false };
        bool includeInPrimaryStats{ false };
        uint64_t primaryBytes{ 0 };
    };
    using HeatMetadataResolver = std::function<HeatNodeMetadata(const std::string &)>;

    struct HeatMaintenanceStats {
        size_t scanned{ 0 };
        size_t applied{ 0 };
        uint64_t coldPrimaryCopyCount{ 0 };
        uint64_t warmPrimaryCopyCount{ 0 };
        uint64_t hotPrimaryCopyCount{ 0 };
        uint64_t totalPrimaryCopyCount{ 0 };
        uint64_t coldPrimaryCopyBytes{ 0 };
        uint64_t warmPrimaryCopyBytes{ 0 };
        uint64_t hotPrimaryCopyBytes{ 0 };
        uint64_t totalPrimaryCopyBytes{ 0 };
    };

    /**
     * @brief Insert a node with an initial heat value (first-publish/first-insert path).
     *        If the key already exists, this is a no-op (heat and timestamps untouched)
     *        so publish and cache-hit paths never double-count.
     * @param[in] objectKey The ID of the object to add.
     * @param[in] initialHeat The initial heat value.
     * @param[in] nowMs Monotonic timestamp (ms) for lastAccess/lastDelay.
     */
    void AddHeatNode(const std::string &objectKey, double initialHeat, uint64_t nowMs);

    /**
     * @brief Insert a Heat node with one size-normalized refill-access credit, or atomically increment an existing
     *        node.
     *
     * A newly inserted node carries the credit before it becomes visible in the eviction list, closing the
     * AddHeatNode/IncrementHeat selection window.
     */
    void RefillHeatNode(const std::string &objectKey, double initialHeat, double accessCredit, double cap,
                        uint64_t nowMs);

    /**
     * @brief Apply a heat snapshot received during migration using target-local monotonic timestamps.
     * @param[in] objectKey The migrated object key.
     * @param[in] heat The source heat snapshot.
     * @param[in] cap Maximum accepted heat.
     * @param[in] nowMs Target-local monotonic timestamp.
     * @param[in] mergeExisting If true, preserve a higher heat already observed by an existing target replica.
     * @return K_OK on success, K_INVALID for invalid heat, or K_NOT_FOUND when no memory eviction node exists.
     */
    Status ApplyMigratedHeat(const std::string &objectKey, double heat, double cap, uint64_t nowMs,
                             bool mergeExisting);

    /**
     * @brief Increment an object's heat on a cache hit (capped at cap) and refresh lastAccessMs.
     *        Inserts a fresh hot node if the key is not yet in the list.
     * @param[in] objectKey The ID of the object that was hit.
     * @param[in] increment Size-normalized heat credit for this successful access.
     * @param[in] cap Maximum heat value (heat is capped here).
     * @param[in] nowMs Monotonic timestamp (ms) for lastAccess.
     */
    void IncrementHeat(const std::string &objectKey, double increment, double cap, uint64_t nowMs);

    /**
     * @brief Decay heat and collect post-decay hot-primary statistics in the same object-resolution pass.
     * @param[in] halfLifePrimaryS Primary-copy half-life in seconds.
     * @param[in] halfLifeLocalS Local-copy half-life in seconds.
     * @param[in] coldThreshold Heat strictly below this value is cold.
     * @param[in] hotThreshold Heat strictly above this value is hot; the inclusive interval between thresholds is warm.
     * @param[in] resolver Resolves copy type and allocator-accounted primary bytes once per snapshot node.
     * @return Maintenance work and hot-primary statistics from successfully applied snapshot nodes.
     */
    HeatMaintenanceStats DecayAllAndCollect(double halfLifePrimaryS, double halfLifeLocalS, double coldThreshold,
                                            double hotThreshold, const HeatMetadataResolver &resolver);

    /**
     * @brief Re-add a candidate that was skipped (e.g. being rebalanced) or whose eviction
     *        failed, restoring the heat captured when it was selected.
     *        Inserts a fresh hot node if the key is not present.
     * @param[in] objectKey The ID of the object to re-add.
     * @param[in] heat Selected heat value to restore.
     * @param[in] nowMs Monotonic timestamp (ms) for lastAccess/lastDelay.
     */
    void ReinsertHot(const std::string &objectKey, double heat, uint64_t nowMs);

    /**
     * @brief Get a node from EvictionList, for test.
     * @param[in] objectKey The ID of the object to get.
     * @param[out] node The node of objectKey.
     * @return Status of the call.
     */
    Status GetObjectInfo(const std::string &objectKey, Node &node);

    /**
     * @brief Get the oldest node from EvictionList, for test.
     * @param[out] node The node of oldest one.
     * @return Status of the call.
     */
    Status GetOldestObjectInfo(Node &node);

    /**
     * @brief Get all nodes from EvictionList, for testing.
     * @param[out] res All nodes in EvictionList.
     * @param[out] oldest The oldest node in EvictionList.
     * @return Status of the call.
     */
    Status GetAllObjectsInfo(std::vector<EvictionList::Node> &res, EvictionList::Node &oldest);

    /**
     * @brief Get a bounded snapshot from the oldest node.
     * @param[in] maxScanCount The maximum number of nodes to copy.
     * @param[out] res The bounded object info snapshot.
     * @return Status of the call.
     */
    Status GetObjectsInfoFromOldest(size_t maxScanCount, std::vector<EvictionList::Node> &res);

    /**
     * @brief Return the coldest Heat candidates, preferring nodes outside the recent-access protection window.
     *
     * Protected nodes remain at the end of the bounded result, so foreground allocation can still make progress.
     * When immediateEvictionThreshold is finite, scan at most 8 * maxCount nodes after the first unprotected node and
     * retain the coldest bounded batch, preferring any object strictly below that threshold. The default disabled
     * value is reserved for explicit diagnostic/control callers that require a globally ordered batch.
     * @param[in] threshold Heat below which an object is preferred.
     * @param[in] maxCount Maximum candidates to return (0 = unlimited).
     * @param[out] res Candidate snapshots in selection order.
     */
    Status GetHeatCandidates(double threshold, size_t maxCount, std::vector<Node> &res,
                             uint64_t recentAccessProtectionMs = 0,
                             double immediateEvictionThreshold = -std::numeric_limits<double>::infinity());

    /**
     * @brief Return Heat candidates using explicit selection/ranking options.
     *
     * Candidates at or below exactRankingThreshold are ranked by heat per allocator byte when sizeResolver is set;
     * candidates above it preserve exact heat/time ordering. The resolver runs after the list lock is released.
     */
    Status GetHeatCandidates(double threshold, size_t maxCount, std::vector<Node> &res,
                             const HeatCandidateOptions &options);

    /**
     * @brief Check that a heat candidate still refers to the same, unmodified node.
     *        Unrelated list insertions do not invalidate a point-in-time candidate; updates to this key and
     *        full-list decay are detected through generation and heatUpdateSeq.
     */
    bool IsHeatSnapshotCurrent(const Node &snapshot);

    /**
     * @brief Remove a node while returning an exact snapshot that can be restored if the surrounding operation fails.
     */
    Status Extract(const std::string &objectKey, Node &snapshot);

    /**
     * @brief Restore a node previously returned by Extract. Existing keys are left untouched.
     * @return K_OK on success/idempotent existing membership; K_RUNTIME_ERROR if allocation fails.
     */
    Status Restore(const Node &snapshot);

    /**
     * @brief Insert an exact policy snapshot or merge it into an existing node.
     *
     * This is the cross-list migration primitive. For Clock, merge preserves the
     * larger valid cur/max counters. For Heat, merge follows heatMergeMode and
     * preserves the newest timestamps. A newly inserted Heat node receives a
     * target-list-local generation.
     * @param[in] snapshot Source policy snapshot converted to the target policy.
     * @param[in] kind Target policy state kind.
     * @param[in] heatCap Maximum Heat value; ignored for Clock state.
     * @param[in] heatMergeMode Preserve the larger value for ordinary restore
     *            paths, or add and cap for Clock-to-Heat migration.
     * @param[out] inserted True when a new membership was created; false when an
     *             existing target membership was merged.
     * @return K_OK on success, K_INVALID for malformed Heat state, or a runtime
     *         error when allocation/insertion fails.
     */
    Status InsertOrMerge(const Node &snapshot, StateKind kind, double heatCap, HeatMergeMode heatMergeMode,
                         bool &inserted);

    /**
     * @brief Check whether a object in EvictionList.
     * @param[in] objectKey The ID of the object to check.
     * @return true if object in EvictionList.
     */
    bool Exist(const std::string &objectKey);

    /**
     * @brief Audit list/index/policy-state membership while external route mutation is quiesced.
     */
    Status ValidateMembership(StateKind kind);

    static constexpr size_t ClockListNodeResidentSizeForTest();
    static constexpr size_t HeatStateResidentSizeForTest();

private:
    struct ListNode {
        ListNode(const ImmutableString &objKey, uint8_t curCnt)
            : objectKey(objKey), curCounter(curCnt), maxCounter(curCnt)
        {
        }

        explicit ListNode(const ImmutableString &objKey) : objectKey(objKey)
        {
        }

        explicit ListNode(const Node &snapshot)
            : objectKey(snapshot.objectKey), curCounter(snapshot.curCounter), maxCounter(snapshot.maxCounter)
        {
        }

        ListNode(const ListNode &) = delete;
        ListNode &operator=(const ListNode &) = delete;

        ImmutableString objectKey;
        std::atomic<uint8_t> curCounter{ 0 };
        std::atomic<uint8_t> maxCounter{ 0 };
    };

    static_assert(std::atomic<uint8_t>::is_always_lock_free, "Clock counters must remain lock-free");

    struct HeatState {
        HeatState(double heatVal, uint64_t accessMs, uint64_t delayMs, uint64_t generationVal)
            : heat(heatVal), lastAccessMs(accessMs), lastDelayMs(delayMs), generation(generationVal)
        {
        }

        explicit HeatState(const Node &snapshot)
            : heat(snapshot.heat),
              lastAccessMs(snapshot.lastAccessMs),
              lastDelayMs(snapshot.lastDelayMs),
              generation(snapshot.generation),
              heatUpdateSeq(snapshot.heatUpdateSeq)
        {
        }

        std::atomic<double> heat{ 0.0 };
        std::atomic<uint64_t> lastAccessMs{ 0 };
        std::atomic<uint64_t> lastDelayMs{ 0 };
        const uint64_t generation{ 0 };
        std::atomic<uint64_t> heatUpdateSeq{ 0 };
    };

    using TBBIndexMap = tbb::concurrent_hash_map<ImmutableString, std::list<ListNode>::iterator>;
    using TBBHeatMap = tbb::concurrent_hash_map<ImmutableString, std::shared_ptr<HeatState>>;

    struct HeatDecayUpdate {
        ImmutableString objectKey;
        uint64_t generation;
        double snapshotHeat;
        uint64_t snapshotLastDelayMs;
        double factor;
        HeatNodeMetadata metadata;
    };

    static Node SnapshotNode(const ListNode &node, const HeatState *heatState = nullptr);
    bool SnapshotHeatNode(const ListNode &node, Node &snapshot) const;
    void TouchHeatNode(const std::string &objectKey, double insertHeat, double increment, double cap, uint64_t nowMs);
    static void IncrementHeatState(HeatState &state, double increment, double cap, uint64_t nowMs);
    bool InsertHeatNode(const std::string &objectKey, double insertHeat, uint64_t nowMs,
                        TBBIndexMap::accessor &accessor);
    Status EraseInternal(const std::string &objectKey, Node *snapshot);
    void SnapshotHeatNodes(std::vector<Node> &snapshot);
    static void BuildHeatDecayUpdates(const std::vector<Node> &snapshot, uint64_t nowMs, double halfLifePrimaryS,
                                      double halfLifeLocalS, const HeatMetadataResolver &resolver,
                                      std::vector<HeatDecayUpdate> &updates);
    HeatMaintenanceStats ApplyHeatDecayUpdates(const std::vector<HeatDecayUpdate> &updates, uint64_t nowMs,
                                               double coldThreshold, double hotThreshold);
    Status InsertClockSnapshot(const Node &snapshot, bool &inserted);
    Status InsertHeatSnapshot(const Node &snapshot, double heatCap, HeatMergeMode mergeMode, bool &inserted);
    Status MergeHeatSnapshot(const Node &snapshot, double heatCap, HeatMergeMode mergeMode);
    static void RankHeatCandidatesByDensity(std::vector<Node> &candidates, uint64_t nowMs,
                                            uint64_t recentAccessProtectionMs, double exactRankingThreshold,
                                            const HeatCandidateSizeResolver &sizeResolver);

    mutable tbb::spin_rw_mutex listMutex_;
    std::list<ListNode> list_;
    std::list<ListNode>::iterator oldest_;
    TBBIndexMap indexTable_;
    // Populated only by the heat strategy. Default clock nodes carry no heat counters, timestamps, or generation.
    TBBHeatMap heatTable_;
    std::atomic<uint64_t> nextGeneration_{ 1 };
};

constexpr size_t EvictionList::ClockListNodeResidentSizeForTest()
{
    return sizeof(ListNode);
}

constexpr size_t EvictionList::HeatStateResidentSizeForTest()
{
    return sizeof(HeatState);
}
}  // namespace object_cache
}  // namespace datasystem

#endif
