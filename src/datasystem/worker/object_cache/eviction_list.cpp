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

#include "datasystem/worker/object_cache/eviction_list.h"

#include <algorithm>
#include <cmath>
#include <exception>
#include <limits>
#include <utility>
#include <vector>

#include "datasystem/common/log/log.h"
#include "datasystem/common/perf/perf_manager.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/timer.h"

namespace datasystem {
namespace object_cache {
namespace {
bool IsHeatCandidateProtected(const EvictionList::Node &node, uint64_t nowMs, uint64_t protectionMs)
{
    return protectionMs > 0 && (nowMs < node.lastAccessMs || nowMs - node.lastAccessMs < protectionMs);
}

bool IsHeatCandidateLess(const EvictionList::Node &left, const EvictionList::Node &right, double threshold,
                         uint64_t nowMs, uint64_t protectionMs)
{
    const bool leftProtected = IsHeatCandidateProtected(left, nowMs, protectionMs);
    const bool rightProtected = IsHeatCandidateProtected(right, nowMs, protectionMs);
    if (leftProtected != rightProtected) {
        return !leftProtected;
    }
    if ((left.heat < threshold) != (right.heat < threshold)) {
        return left.heat < threshold;
    }
    if (left.heat < right.heat) {
        return true;
    }
    if (right.heat < left.heat) {
        return false;
    }
    return left.lastAccessMs < right.lastAccessMs;
}
}  // namespace
namespace {
constexpr size_t K_HEAT_CANDIDATE_SCAN_MULTIPLIER = 8;

void StoreMax(std::atomic<uint64_t> &target, uint64_t value)
{
    auto current = target.load(std::memory_order_relaxed);
    while (current < value && !target.compare_exchange_weak(current, value, std::memory_order_relaxed)) {
        current = target.load(std::memory_order_relaxed);
    }
}

template <typename Compare>
void AddBoundedNode(EvictionList::Node node, size_t maxCount, Compare less, std::vector<EvictionList::Node> &nodes)
{
    if (maxCount == 0) {
        nodes.emplace_back(std::move(node));
        return;
    }
    if (nodes.size() < maxCount) {
        nodes.emplace_back(std::move(node));
        std::push_heap(nodes.begin(), nodes.end(), less);
        return;
    }
    if (less(node, nodes.front())) {
        std::pop_heap(nodes.begin(), nodes.end(), less);
        nodes.back() = std::move(node);
        std::push_heap(nodes.begin(), nodes.end(), less);
    }
}
}  // namespace

EvictionList::EvictionList() : oldest_(list_.end())
{
}

EvictionList::Node EvictionList::SnapshotNode(const ListNode &node, const HeatState *heatState)
{
    Node snapshot;
    snapshot.objectKey = node.objectKey;
    snapshot.maxCounter = node.maxCounter.load(std::memory_order_relaxed);
    // A concurrent merge publishes the monotonically increasing maximum before raising the current counter. Relaxed
    // atomics deliberately avoid cross-field synchronization here, so clamp a potentially newer current sample to
    // the maximum observed by this snapshot and preserve the public curCounter <= maxCounter invariant.
    snapshot.curCounter = std::min(node.curCounter.load(std::memory_order_relaxed), snapshot.maxCounter);
    if (heatState != nullptr) {
        snapshot.heat = heatState->heat.load(std::memory_order_relaxed);
        snapshot.lastAccessMs = heatState->lastAccessMs.load(std::memory_order_relaxed);
        snapshot.lastDelayMs = heatState->lastDelayMs.load(std::memory_order_relaxed);
        snapshot.generation = heatState->generation;
        snapshot.heatUpdateSeq = heatState->heatUpdateSeq.load(std::memory_order_relaxed);
    }
    return snapshot;
}

bool EvictionList::SnapshotHeatNode(const ListNode &node, Node &snapshot) const
{
    TBBHeatMap::const_accessor heatAccessor;
    if (!heatTable_.find(heatAccessor, node.objectKey)) {
        return false;
    }
    snapshot = SnapshotNode(node, heatAccessor->second.get());
    return true;
}

void EvictionList::Add(const std::string &objectKey, uint8_t counter)
{
    PerfPoint point(PerfKey::WORKER_EVICT_LIST_ADD);
    TBBIndexMap::accessor accessor;
    bool inserted = indexTable_.insert(accessor, objectKey);
    if (!inserted) {
        // The key accessor serializes updates to this node, while curCounter is atomic for readers that snapshot it
        // through a const accessor. Updating an existing Clock node therefore does not mutate list structure and must
        // not take the global list write lock: this is the normal cache-hit path, and unrelated keys should progress
        // independently.
        auto &nodePtr = accessor->second;
        auto current = nodePtr->curCounter.load(std::memory_order_relaxed);
        const auto maxCounter = nodePtr->maxCounter.load(std::memory_order_relaxed);
        while (current < maxCounter
               && !nodePtr->curCounter.compare_exchange_weak(current, static_cast<uint8_t>(current + 1),
                                                             std::memory_order_relaxed)) {
            current = nodePtr->curCounter.load(std::memory_order_relaxed);
        }
        accessor.release();
        point.Record();
        return;
    }

    {
        tbb::spin_rw_mutex::scoped_lock wlock(listMutex_, true);
        if (inserted) {
            // Append to the tail (newest end). The clock hand starts at oldest_
            // (oldest object) and advances per successful eviction, so a newly added
            // object at the tail is reached only after a full LRU sweep
            // (list_size / write_rate, ~seconds). The previous emplace(oldest_)
            // inserted the new node right before the clock hand, so the hand wrapped
            // back to it within a single EvictionTask (~ms) and evicted fresh data
            // before cross-node GETs could arrive (issue #750).
            try {
                list_.emplace_back(objectKey, counter);
                if (list_.size() == 1) {
                    oldest_ = list_.begin();
                }
                accessor->second = std::prev(list_.end());
            } catch (...) {
                indexTable_.erase(accessor);
                // Don't propagate the exception (e.g. bad_alloc from emplace_back):
                // callers (eviction task, publish, GET re-access) are not prepared
                // for Add to throw and would crash the worker. The object simply
                // stays out of the eviction list and will be added on the next access.
                return;
            }
        }
    }
    accessor.release();
    point.Record();
}

Status EvictionList::Erase(const std::string &objectKey)
{
    return EraseInternal(objectKey, nullptr);
}

Status EvictionList::Extract(const std::string &objectKey, Node &snapshot)
{
    return EraseInternal(objectKey, &snapshot);
}

Status EvictionList::ValidateMembership(StateKind kind)
{
    std::vector<std::string> keys;
    {
        // Mutators acquire per-key map accessors before listMutex_. Copy list identities first and release the list
        // lock before looking up the maps, otherwise validation creates the inverse list -> map lock order.
        tbb::spin_rw_mutex::scoped_lock readLock(listMutex_, false);
        keys.reserve(list_.size());
        for (const auto &node : list_) {
            keys.emplace_back(node.objectKey);
        }
    }
    CHECK_FAIL_RETURN_STATUS(indexTable_.size() == keys.size(), K_RUNTIME_ERROR,
                             "Eviction list/index membership count mismatch");
    const auto expectedHeatStates = kind == StateKind::HEAT ? keys.size() : 0;
    CHECK_FAIL_RETURN_STATUS(heatTable_.size() == expectedHeatStates, K_RUNTIME_ERROR,
                             "Eviction list policy-state count mismatch");
    for (const auto &key : keys) {
        TBBIndexMap::const_accessor indexAccessor;
        CHECK_FAIL_RETURN_STATUS(indexTable_.find(indexAccessor, key), K_RUNTIME_ERROR,
                                 "Eviction list node is absent from the index");
        TBBHeatMap::const_accessor heatAccessor;
        const bool hasHeat = heatTable_.find(heatAccessor, key);
        CHECK_FAIL_RETURN_STATUS(hasHeat == (kind == StateKind::HEAT), K_RUNTIME_ERROR,
                                 "Eviction list node has mismatched policy state");
    }
    return Status::OK();
}

Status EvictionList::EraseInternal(const std::string &objectKey, Node *snapshot)
{
    PerfPoint point(PerfKey::WORKER_EVICT_LIST_ERASE);
    TBBIndexMap::accessor accessor;
    if (!indexTable_.find(accessor, objectKey)) {
        VLOG(1) << "Object " + objectKey + " does not exist in EvictionList";
        RETURN_STATUS(StatusCode::K_NOT_FOUND, "Object " + objectKey + " does not exist in EvictionList.");
    }

    std::shared_ptr<HeatState> heatState;
    TBBHeatMap::const_accessor heatAccessor;
    if (heatTable_.find(heatAccessor, objectKey)) {
        heatState = heatAccessor->second;
    }
    heatAccessor.release();
    tbb::spin_rw_mutex::scoped_lock wlock(listMutex_, true);
    if (snapshot != nullptr) {
        *snapshot = SnapshotNode(*accessor->second, heatState.get());
    }
    bool reassign = false;
    if (oldest_->objectKey == objectKey) {
        ++oldest_;
        if (oldest_ == list_.end()) {
            reassign = true;
        }
    }
    list_.erase(accessor->second);
    heatTable_.erase(objectKey);
    indexTable_.erase(accessor);
    if (reassign) {
        oldest_ = list_.begin();
    }
    point.Record();
    return Status::OK();
}

Status EvictionList::Restore(const Node &snapshot)
{
    TBBIndexMap::accessor accessor;
    if (!indexTable_.insert(accessor, snapshot.objectKey)) {
        VLOG(1) << "Skip restoring existing eviction node " << snapshot.objectKey;
        return Status::OK();
    }
    if (snapshot.generation != 0) {
        TBBHeatMap::accessor heatAccessor;
        try {
            if (!heatTable_.insert(heatAccessor, snapshot.objectKey)) {
                indexTable_.erase(accessor);
                RETURN_STATUS(K_RUNTIME_ERROR, "Failed to restore duplicate heat eviction state");
            }
            heatAccessor->second = std::make_shared<HeatState>(snapshot);
        } catch (...) {
            // concurrent_hash_map::erase(key) must not re-enter the same bucket while its write accessor is held.
            heatAccessor.release();
            heatTable_.erase(snapshot.objectKey);
            indexTable_.erase(accessor);
            RETURN_STATUS(K_RUNTIME_ERROR, "Failed to restore heat eviction state");
        }
    }
    tbb::spin_rw_mutex::scoped_lock wlock(listMutex_, true);
    try {
        auto newest = list_.emplace(oldest_, snapshot);
        if (list_.size() == 1) {
            oldest_ = newest;
        }
        accessor->second = newest;
    } catch (...) {
        heatTable_.erase(snapshot.objectKey);
        indexTable_.erase(accessor);
        RETURN_STATUS(K_RUNTIME_ERROR, "Failed to restore eviction node");
    }
    return Status::OK();
}

Status EvictionList::InsertOrMerge(const Node &snapshot, StateKind kind, double heatCap, HeatMergeMode heatMergeMode,
                                   bool &inserted)
{
    inserted = false;
    switch (kind) {
        case StateKind::CLOCK:
            return InsertClockSnapshot(snapshot, inserted);
        case StateKind::HEAT:
            return InsertHeatSnapshot(snapshot, heatCap, heatMergeMode, inserted);
        default:
            RETURN_STATUS(StatusCode::K_INVALID, "Unsupported eviction state kind");
    }
}

Status EvictionList::InsertClockSnapshot(const Node &snapshot, bool &inserted)
{
    TBBIndexMap::accessor accessor;
    inserted = indexTable_.insert(accessor, snapshot.objectKey);
    if (!inserted) {
        CHECK_FAIL_RETURN_STATUS(heatTable_.count(snapshot.objectKey) == 0, StatusCode::K_RUNTIME_ERROR,
                                 "Target eviction node contains Heat state while merging Clock state");
        auto &node = *accessor->second;
        const auto maxCounter =
            std::max({ node.maxCounter.load(std::memory_order_relaxed), snapshot.maxCounter, snapshot.curCounter });
        node.maxCounter.store(maxCounter, std::memory_order_relaxed);
        const auto current = node.curCounter.load(std::memory_order_relaxed);
        node.curCounter.store(std::min(maxCounter, std::max(current, snapshot.curCounter)), std::memory_order_relaxed);
        return Status::OK();
    }

    try {
        tbb::spin_rw_mutex::scoped_lock wlock(listMutex_, true);
        list_.emplace_back(snapshot);
        if (list_.size() == 1) {
            oldest_ = list_.begin();
        }
        accessor->second = std::prev(list_.end());
    } catch (const std::exception &e) {
        indexTable_.erase(accessor);
        RETURN_STATUS(StatusCode::K_RUNTIME_ERROR,
                      FormatString("Failed to insert Clock eviction snapshot: %s", e.what()));
    } catch (...) {
        indexTable_.erase(accessor);
        RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, "Failed to insert Clock eviction snapshot");
    }
    return Status::OK();
}

Status EvictionList::InsertHeatSnapshot(const Node &snapshot, double heatCap, HeatMergeMode mergeMode, bool &inserted)
{
    CHECK_FAIL_RETURN_STATUS(std::isfinite(snapshot.heat) && snapshot.heat >= 0.0 && std::isfinite(heatCap)
                                 && heatCap >= 0.0,
                             StatusCode::K_INVALID,
                             "Invalid Heat eviction snapshot");
    const double boundedSnapshotHeat = std::min(snapshot.heat, heatCap);
    TBBIndexMap::accessor accessor;
    inserted = indexTable_.insert(accessor, snapshot.objectKey);
    if (!inserted) {
        return MergeHeatSnapshot(snapshot, heatCap, mergeMode);
    }

    try {
        const auto generation = nextGeneration_.fetch_add(1, std::memory_order_relaxed);
        TBBHeatMap::accessor heatAccessor;
        if (!heatTable_.insert(heatAccessor, snapshot.objectKey)) {
            indexTable_.erase(accessor);
            RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, "Failed to insert Heat eviction state");
        }
        heatAccessor->second =
            std::make_shared<HeatState>(boundedSnapshotHeat, snapshot.lastAccessMs, snapshot.lastDelayMs, generation);
        heatAccessor.release();
        tbb::spin_rw_mutex::scoped_lock wlock(listMutex_, true);
        list_.emplace_back(snapshot.objectKey);
        if (list_.size() == 1) {
            oldest_ = list_.begin();
        }
        accessor->second = std::prev(list_.end());
    } catch (const std::exception &e) {
        heatTable_.erase(snapshot.objectKey);
        indexTable_.erase(accessor);
        RETURN_STATUS(StatusCode::K_RUNTIME_ERROR,
                      FormatString("Failed to insert Heat eviction snapshot: %s", e.what()));
    } catch (...) {
        heatTable_.erase(snapshot.objectKey);
        indexTable_.erase(accessor);
        RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, "Failed to insert Heat eviction snapshot");
    }
    return Status::OK();
}

Status EvictionList::MergeHeatSnapshot(const Node &snapshot, double heatCap, HeatMergeMode mergeMode)
{
    TBBHeatMap::accessor heatAccessor;
    CHECK_FAIL_RETURN_STATUS(heatTable_.find(heatAccessor, snapshot.objectKey), StatusCode::K_RUNTIME_ERROR,
                             "Target eviction node is missing Heat state");
    auto &state = *heatAccessor->second;
    const double boundedSnapshotHeat = std::min(snapshot.heat, heatCap);
    const double currentHeat = std::min(state.heat.load(std::memory_order_relaxed), heatCap);
    const double mergedHeat = mergeMode == HeatMergeMode::ADD_CAPPED
                                  ? (currentHeat >= heatCap - boundedSnapshotHeat ? heatCap
                                                                                 : currentHeat + boundedSnapshotHeat)
                                  : std::max(currentHeat, boundedSnapshotHeat);
    state.heat.store(mergedHeat, std::memory_order_relaxed);
    StoreMax(state.lastAccessMs, snapshot.lastAccessMs);
    StoreMax(state.lastDelayMs, snapshot.lastDelayMs);
    state.heatUpdateSeq.fetch_add(1, std::memory_order_relaxed);
    return Status::OK();
}

size_t EvictionList::Size()
{
    tbb::spin_rw_mutex::scoped_lock rlock(listMutex_, false);
    return list_.size();
}

Status EvictionList::FindEvictCandidate(std::string &candidateObjKey)
{
    PerfPoint point(PerfKey::WORKER_EVICT_LIST_FIND);
    tbb::spin_rw_mutex::scoped_lock wlock(listMutex_, true);
    CHECK_FAIL_RETURN_STATUS(!list_.empty(), StatusCode::K_RUNTIME_ERROR, "EvictionList is empty.");
    while (true) {
        auto current = oldest_->curCounter.load(std::memory_order_relaxed);
        if (current == 0) {
            candidateObjKey = oldest_->objectKey;
            break;
        }
        while (current > 0
               && !oldest_->curCounter.compare_exchange_weak(current, static_cast<uint8_t>(current - 1),
                                                              std::memory_order_relaxed)) {
        }
        if (++oldest_ == list_.end()) {
            oldest_ = list_.begin();
        }
    }
    point.Record();
    return Status::OK();
}

Status EvictionList::GetObjectInfo(const std::string &objectKey, Node &node)
{
    TBBIndexMap::const_accessor readAccessor;
    if (!indexTable_.find(readAccessor, objectKey)) {
        RETURN_STATUS_LOG_ERROR(StatusCode::K_NOT_FOUND, "Object " + objectKey + " does not exist");
    }
    tbb::spin_rw_mutex::scoped_lock rlock(listMutex_, false);
    if (!SnapshotHeatNode(*readAccessor->second, node)) {
        node = SnapshotNode(*readAccessor->second);
    }
    return Status::OK();
}

Status EvictionList::GetOldestObjectInfo(Node &node)
{
    tbb::spin_rw_mutex::scoped_lock rlock(listMutex_, false);
    CHECK_FAIL_RETURN_STATUS(!list_.empty(), StatusCode::K_RUNTIME_ERROR, "EvictionList is empty.");
    if (!SnapshotHeatNode(*oldest_, node)) {
        node = SnapshotNode(*oldest_);
    }
    return Status::OK();
}

Status EvictionList::GetAllObjectsInfo(std::vector<EvictionList::Node> &res, EvictionList::Node &oldest)
{
    tbb::spin_rw_mutex::scoped_lock rlock(listMutex_, false);
    if (list_.empty()) {
        return Status::OK();
    }
    if (!SnapshotHeatNode(*oldest_, oldest)) {
        oldest = SnapshotNode(*oldest_);
    }

    auto node = oldest_;
    while (true) {
        Node snapshot;
        if (!SnapshotHeatNode(*node, snapshot)) {
            snapshot = SnapshotNode(*node);
        }
        res.emplace_back(std::move(snapshot));
        ++node;
        if (node == list_.end()) {
            node = list_.begin();
        }
        if (node == oldest_) {
            break;
        }
    }
    return Status::OK();
}

Status EvictionList::GetObjectsInfoFromOldest(size_t maxScanCount, std::vector<EvictionList::Node> &res)
{
    res.clear();
    if (maxScanCount == 0) {
        return Status::OK();
    }
    // Reserve outside the list read lock to avoid repeated vector growth while holding the lock, reducing the time
    // Evict can be blocked on the write lock.
    res.reserve(maxScanCount);

    tbb::spin_rw_mutex::scoped_lock rlock(listMutex_, false);
    if (list_.empty()) {
        return Status::OK();
    }

    // Rebalance only needs a candidate snapshot near the oldest position, not a full eviction-list copy.
    // Scan at most maxScanCount nodes so lock hold time changes from O(list size) to O(maxScanCount).
    auto node = oldest_;
    size_t scanned = 0;
    while (scanned < maxScanCount) {
        Node snapshot;
        if (!SnapshotHeatNode(*node, snapshot)) {
            snapshot = SnapshotNode(*node);
        }
        res.emplace_back(std::move(snapshot));
        ++scanned;
        ++node;
        if (node == list_.end()) {
            node = list_.begin();
        }
        if (node == oldest_) {
            break;
        }
    }
    return Status::OK();
}

Status EvictionList::GetHeatCandidates(double threshold, size_t maxCount, std::vector<Node> &res,
                                       uint64_t recentAccessProtectionMs, double immediateEvictionThreshold)
{
    HeatCandidateOptions options;
    options.recentAccessProtectionMs = recentAccessProtectionMs;
    options.immediateEvictionThreshold = immediateEvictionThreshold;
    return GetHeatCandidates(threshold, maxCount, res, options);
}

Status EvictionList::GetHeatCandidates(double threshold, size_t maxCount, std::vector<Node> &res,
                                       const HeatCandidateOptions &options)
{
    res.clear();
    if (maxCount == 0) {
        return Status::OK();
    }
    std::vector<Node> immediateCandidates;
    const size_t scanBudget =
        maxCount <= std::numeric_limits<size_t>::max() / K_HEAT_CANDIDATE_SCAN_MULTIPLIER
            ? maxCount * K_HEAT_CANDIDATE_SCAN_MULTIPLIER
            : std::numeric_limits<size_t>::max();
    size_t scanCount = 0;
    bool foundImmediateCandidate = false;
    const uint64_t nowMs = GetSteadyClockTimeStampMs();
    auto less = [threshold, nowMs, &options](const Node &left, const Node &right) {
        return IsHeatCandidateLess(left, right, threshold, nowMs, options.recentAccessProtectionMs);
    };
    if (maxCount > 0) {
        res.reserve(maxCount);
        immediateCandidates.reserve(maxCount);
    }
    {
        tbb::spin_rw_mutex::scoped_lock rlock(listMutex_, false);
        if (list_.empty()) {
            RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, "EvictionList is empty.");
        }
        for (const auto &node : list_) {
            Node snapshot;
            if (SnapshotHeatNode(node, snapshot)) {
                const bool protectedCandidate =
                    IsHeatCandidateProtected(snapshot, nowMs, options.recentAccessProtectionMs);
                // Search a bounded window and keep its coldest batch. This avoids the insertion-order bias of
                // returning the first full batch while capping list work at 8x the batch size. Recent objects retain
                // their admission second chance.
                if (!protectedCandidate && snapshot.heat < options.immediateEvictionThreshold) {
                    foundImmediateCandidate = true;
                    AddBoundedNode(std::move(snapshot), maxCount, less, immediateCandidates);
                } else if (!foundImmediateCandidate) {
                    AddBoundedNode(std::move(snapshot), maxCount, less, res);
                }
            }
            // Eviction passes supply a finite immediate threshold. Bound their shared-lock hold even when the scan
            // window contains no immediately evictable object; callers that request a global ordering keep the
            // default -infinity threshold and intentionally scan the whole list.
            if (std::isfinite(options.immediateEvictionThreshold) && ++scanCount >= scanBudget) {
                break;
            }
        }
    }
    if (!immediateCandidates.empty()) {
        std::sort(immediateCandidates.begin(), immediateCandidates.end(), less);
        res = std::move(immediateCandidates);
    } else {
        std::sort(res.begin(), res.end(), less);
    }
    RankHeatCandidatesByDensity(res, nowMs, options.recentAccessProtectionMs, options.exactRankingThreshold,
                                options.sizeResolver);
    return Status::OK();
}

void EvictionList::RankHeatCandidatesByDensity(std::vector<Node> &candidates, uint64_t nowMs,
                                               uint64_t recentAccessProtectionMs, double exactRankingThreshold,
                                               const HeatCandidateSizeResolver &sizeResolver)
{
    if (!sizeResolver) {
        return;
    }
    const bool hasDensityCandidate =
        std::any_of(candidates.begin(), candidates.end(), [exactRankingThreshold](const Node &node) {
            return node.heat <= exactRankingThreshold;
        });
    if (!hasDensityCandidate) {
        return;
    }
    // Resolve object-table metadata only after GetHeatCandidates has released listMutex_. The callback may take an
    // object row lock, so invoking it during the list scan would invert the normal object/list lock order.
    for (auto &node : candidates) {
        if (node.heat <= exactRankingThreshold) {
            (void)sizeResolver(node.objectKey, node.migratableSize);
        }
    }
    const auto less = [nowMs, recentAccessProtectionMs, exactRankingThreshold](const Node &left, const Node &right) {
        const bool leftProtected = IsHeatCandidateProtected(left, nowMs, recentAccessProtectionMs);
        const bool rightProtected = IsHeatCandidateProtected(right, nowMs, recentAccessProtectionMs);
        if (leftProtected != rightProtected) {
            return !leftProtected;
        }
        const bool leftExact = left.heat > exactRankingThreshold;
        const bool rightExact = right.heat > exactRankingThreshold;
        if (leftExact != rightExact) {
            return !leftExact;
        }
        if (!leftExact && (left.migratableSize > 0) != (right.migratableSize > 0)) {
            return left.migratableSize > 0;
        }
        if (!leftExact && left.migratableSize > 0) {
            const long double leftDensity = static_cast<long double>(left.heat) / left.migratableSize;
            const long double rightDensity = static_cast<long double>(right.heat) / right.migratableSize;
            if (leftDensity < rightDensity) {
                return true;
            }
            if (rightDensity < leftDensity) {
                return false;
            }
        }
        if (left.heat < right.heat) {
            return true;
        }
        if (right.heat < left.heat) {
            return false;
        }
        return left.lastAccessMs < right.lastAccessMs;
    };
    std::sort(candidates.begin(), candidates.end(), less);
}

bool EvictionList::IsHeatSnapshotCurrent(const Node &snapshot)
{
    TBBHeatMap::const_accessor accessor;
    if (!heatTable_.find(accessor, snapshot.objectKey)) {
        return false;
    }
    return accessor->second->generation == snapshot.generation
           && accessor->second->heatUpdateSeq.load(std::memory_order_relaxed) == snapshot.heatUpdateSeq;
}

bool EvictionList::Exist(const std::string &objectKey)
{
    return indexTable_.count(objectKey) > 0;
}

void EvictionList::AddHeatNode(const std::string &objectKey, double initialHeat, uint64_t nowMs)
{
    PerfPoint point(PerfKey::WORKER_EVICT_LIST_ADD);
    TBBIndexMap::accessor accessor;
    if (indexTable_.insert(accessor, objectKey)) {
        try {
            auto generation = nextGeneration_.fetch_add(1, std::memory_order_relaxed);
            TBBHeatMap::accessor heatAccessor;
            if (!heatTable_.insert(heatAccessor, objectKey)) {
                indexTable_.erase(accessor);
                return;
            }
            heatAccessor->second = std::make_shared<HeatState>(initialHeat, nowMs, nowMs, generation);
            heatAccessor.release();
            tbb::spin_rw_mutex::scoped_lock wlock(listMutex_, true);
            auto newest = list_.emplace(oldest_, objectKey);
            if (list_.size() == 1) {
                oldest_ = newest;
            }
            accessor->second = newest;
        } catch (...) {
            heatTable_.erase(objectKey);
            indexTable_.erase(accessor);
            LOG(ERROR) << "Failed to add heat eviction node " << objectKey;
            return;
        }
    }
    // Existing node: leave heat/timestamps untouched (first-insert semantics; a later
    // cache hit will increment heat via IncrementHeat).
    point.Record();
}

Status EvictionList::ApplyMigratedHeat(const std::string &objectKey, double heat, double cap, uint64_t nowMs,
                                       bool mergeExisting)
{
    CHECK_FAIL_RETURN_STATUS(std::isfinite(heat) && heat >= 0.0 && std::isfinite(cap) && cap >= 0.0,
                             StatusCode::K_INVALID, "Invalid migrated object heat");
    TBBHeatMap::accessor accessor;
    CHECK_FAIL_RETURN_STATUS(heatTable_.find(accessor, objectKey), StatusCode::K_NOT_FOUND,
                             "Object " + objectKey + " does not exist in EvictionList");

    auto &node = *accessor->second;
    const double boundedHeat = std::min(heat, cap);
    double currentHeat = node.heat.load(std::memory_order_relaxed);
    if (mergeExisting) {
        while (currentHeat < boundedHeat) {
            if (node.heat.compare_exchange_weak(currentHeat, boundedHeat, std::memory_order_relaxed,
                                                std::memory_order_relaxed)) {
                break;
            }
            currentHeat = node.heat.load(std::memory_order_relaxed);
        }
    } else {
        node.heat.store(boundedHeat, std::memory_order_relaxed);
    }
    // Source steady-clock timestamps are not comparable across hosts. Start decay from the target's local clock.
    node.lastAccessMs.store(nowMs, std::memory_order_relaxed);
    node.lastDelayMs.store(nowMs, std::memory_order_relaxed);
    node.heatUpdateSeq.fetch_add(1, std::memory_order_relaxed);
    return Status::OK();
}

void EvictionList::RefillHeatNode(const std::string &objectKey, double initialHeat, double accessCredit, double cap,
                                  uint64_t nowMs)
{
    TouchHeatNode(objectKey, std::min(initialHeat + accessCredit, cap), accessCredit, cap, nowMs);
}

void EvictionList::IncrementHeat(const std::string &objectKey, double increment, double cap, uint64_t nowMs)
{
    TouchHeatNode(objectKey, cap, increment, cap, nowMs);
}

void EvictionList::TouchHeatNode(const std::string &objectKey, double insertHeat, double increment, double cap,
                                 uint64_t nowMs)
{
    PerfPoint point(PerfKey::WORKER_EVICT_LIST_ADD);
    TBBHeatMap::const_accessor reader;
    if (heatTable_.find(reader, objectKey)) {
        IncrementHeatState(*reader->second, increment, cap, nowMs);
        point.Record();
        return;
    }

    TBBIndexMap::accessor accessor;
    if (indexTable_.insert(accessor, objectKey)) {
        if (InsertHeatNode(objectKey, insertHeat, nowMs, accessor)) {
            point.Record();
        }
        return;
    }
    // Another thread inserted the node after the shared-accessor miss. The write accessor is already held here;
    // update once and return. Existing-key hits normally take the shared-accessor/atomic fast path above.
    TBBHeatMap::accessor heatAccessor;
    if (!heatTable_.find(heatAccessor, objectKey)) {
        return;
    }
    IncrementHeatState(*heatAccessor->second, increment, cap, nowMs);
    point.Record();
}

void EvictionList::IncrementHeatState(HeatState &state, double increment, double cap, uint64_t nowMs)
{
    double heat = state.heat.load(std::memory_order_relaxed);
    while (heat < cap
           && !state.heat.compare_exchange_weak(heat, std::min(heat + increment, cap), std::memory_order_relaxed)) {
        heat = state.heat.load(std::memory_order_relaxed);
    }
    StoreMax(state.lastAccessMs, nowMs);
    state.heatUpdateSeq.fetch_add(1, std::memory_order_relaxed);
}

bool EvictionList::InsertHeatNode(const std::string &objectKey, double insertHeat, uint64_t nowMs,
                                  TBBIndexMap::accessor &accessor)
{
    try {
        auto generation = nextGeneration_.fetch_add(1, std::memory_order_relaxed);
        TBBHeatMap::accessor heatAccessor;
        if (!heatTable_.insert(heatAccessor, objectKey)) {
            indexTable_.erase(accessor);
            return false;
        }
        heatAccessor->second = std::make_shared<HeatState>(insertHeat, nowMs, nowMs, generation);
        heatAccessor.release();
        tbb::spin_rw_mutex::scoped_lock wlock(listMutex_, true);
        auto newest = list_.emplace(oldest_, objectKey);
        if (list_.size() == 1) {
            oldest_ = newest;
        }
        accessor->second = newest;
        return true;
    } catch (...) {
        heatTable_.erase(objectKey);
        indexTable_.erase(accessor);
        LOG(ERROR) << "Failed to insert heat eviction node on access " << objectKey;
        return false;
    }
}

void EvictionList::SnapshotHeatNodes(std::vector<Node> &snapshot)
{
    snapshot.clear();
    struct Identity {
        ImmutableString objectKey;
        uint64_t generation;
    };
    std::vector<Identity> identities;
    {
        tbb::spin_rw_mutex::scoped_lock rlock(listMutex_, false);
        if (list_.empty()) {
            return;
        }
        identities.reserve(list_.size());
        for (const auto &node : list_) {
            Node snapshot;
            if (SnapshotHeatNode(node, snapshot)) {
                identities.push_back({ node.objectKey, snapshot.generation });
            }
        }
    }
    snapshot.reserve(identities.size());
    for (const auto &identity : identities) {
        TBBIndexMap::const_accessor indexAccessor;
        TBBHeatMap::const_accessor heatAccessor;
        if (indexTable_.find(indexAccessor, identity.objectKey)
            && heatTable_.find(heatAccessor, identity.objectKey)
            && heatAccessor->second->generation == identity.generation) {
            snapshot.emplace_back(SnapshotNode(*indexAccessor->second, heatAccessor->second.get()));
        }
    }
}

void EvictionList::BuildHeatDecayUpdates(const std::vector<Node> &snapshot, uint64_t nowMs, double halfLifePrimaryS,
                                         double halfLifeLocalS, const HeatMetadataResolver &resolver,
                                         std::vector<HeatDecayUpdate> &updates)
{
    updates.clear();
    updates.reserve(snapshot.size());
    for (const auto &s : snapshot) {
        HeatNodeMetadata metadata;
        try {
            metadata = resolver(s.objectKey);
        } catch (...) {
            // A transient resolver failure must not make a primary look like a local copy and decay it faster.
            continue;
        }
        if (!metadata.resolved) {
            continue;
        }
        double t = metadata.isPrimary ? halfLifePrimaryS : halfLifeLocalS;
        const uint64_t elapsedMs = nowMs > s.lastDelayMs ? nowMs - s.lastDelayMs : 0;
        double dt = static_cast<double>(elapsedMs) / 1000.0;
        double factor = std::pow(0.5, dt / t);
        updates.push_back({ s.objectKey, s.generation, s.heat, s.lastDelayMs, factor, metadata });
    }
}

EvictionList::HeatMaintenanceStats EvictionList::ApplyHeatDecayUpdates(const std::vector<HeatDecayUpdate> &updates,
                                                                       uint64_t nowMs, double coldThreshold,
                                                                       double hotThreshold)
{
    HeatMaintenanceStats stats;
    stats.scanned = updates.size();
    for (const auto &u : updates) {
        TBBHeatMap::accessor accessor;
        if (heatTable_.find(accessor, u.objectKey)) {
            auto &node = *accessor->second;
            if (node.generation != u.generation
                || node.lastDelayMs.load(std::memory_order_relaxed) != u.snapshotLastDelayMs) {
                continue;
            }
            double currentHeat = node.heat.load(std::memory_order_relaxed);
            double newHeat;
            do {
                newHeat = u.snapshotHeat * u.factor + (currentHeat - u.snapshotHeat);
            } while (!node.heat.compare_exchange_weak(currentHeat, newHeat, std::memory_order_relaxed,
                                                      std::memory_order_relaxed));
            node.lastDelayMs.store(nowMs, std::memory_order_relaxed);
            node.heatUpdateSeq.fetch_add(1, std::memory_order_relaxed);
            stats.applied++;
            if (u.metadata.includeInPrimaryStats) {
                stats.totalPrimaryCopyCount++;
                stats.totalPrimaryCopyBytes += u.metadata.primaryBytes;
                if (newHeat < coldThreshold) {
                    stats.coldPrimaryCopyCount++;
                    stats.coldPrimaryCopyBytes += u.metadata.primaryBytes;
                } else if (newHeat > hotThreshold) {
                    stats.hotPrimaryCopyCount++;
                    stats.hotPrimaryCopyBytes += u.metadata.primaryBytes;
                } else {
                    stats.warmPrimaryCopyCount++;
                    stats.warmPrimaryCopyBytes += u.metadata.primaryBytes;
                }
            }
        }
    }
    return stats;
}

EvictionList::HeatMaintenanceStats EvictionList::DecayAllAndCollect(
    double halfLifePrimaryS, double halfLifeLocalS, double coldThreshold, double hotThreshold,
    const HeatMetadataResolver &resolver)
{
    const uint64_t nowMs = GetSteadyClockTimeStampMs();
    // Snapshot structure identities before taking per-key accessors; mutation paths deliberately use map -> list.
    std::vector<Node> snapshot;
    SnapshotHeatNodes(snapshot);
    if (snapshot.empty()) {
        return {};
    }
    // The resolver may consult objectTable_, so computation runs without listMutex_. The apply helper checks
    // generation and lastDelayMs, preserving post-snapshot hit increments while rejecting recreate/reinsert/decay.
    std::vector<HeatDecayUpdate> updates;
    BuildHeatDecayUpdates(snapshot, nowMs, halfLifePrimaryS, halfLifeLocalS, resolver, updates);
    auto stats = ApplyHeatDecayUpdates(updates, nowMs, coldThreshold, hotThreshold);
    VLOG(1) << "DecayAll finished: scanned=" << stats.scanned << ", applied=" << stats.applied;
    return stats;
}

void EvictionList::ReinsertHot(const std::string &objectKey, double heat, uint64_t nowMs)
{
    PerfPoint point(PerfKey::WORKER_EVICT_LIST_ADD);
    TBBIndexMap::accessor accessor;
    if (indexTable_.insert(accessor, objectKey)) {
        // Erased before re-add (the EvictionTask Erase-then-Readd path): re-insert hot.
        try {
            auto generation = nextGeneration_.fetch_add(1, std::memory_order_relaxed);
            TBBHeatMap::accessor heatAccessor;
            if (!heatTable_.insert(heatAccessor, objectKey)) {
                indexTable_.erase(accessor);
                return;
            }
            heatAccessor->second = std::make_shared<HeatState>(heat, nowMs, nowMs, generation);
            heatAccessor.release();
            tbb::spin_rw_mutex::scoped_lock wlock(listMutex_, true);
            auto newest = list_.emplace(oldest_, objectKey);
            if (list_.size() == 1) {
                oldest_ = newest;
            }
            accessor->second = newest;
        } catch (...) {
            heatTable_.erase(objectKey);
            indexTable_.erase(accessor);
            LOG(ERROR) << "Failed to reinsert heat eviction node " << objectKey;
            return;
        }
    } else {
        // Still present: preserve any concurrent cache-hit increase while restoring at least the selected heat.
        // Updating lastDelay makes any in-flight decay snapshot reject this absolute update.
        TBBHeatMap::accessor heatAccessor;
        if (!heatTable_.find(heatAccessor, objectKey)) {
            return;
        }
        const double currentHeat = heatAccessor->second->heat.load(std::memory_order_relaxed);
        heatAccessor->second->heat.store(std::max(currentHeat, heat), std::memory_order_relaxed);
        heatAccessor->second->lastAccessMs.store(nowMs, std::memory_order_relaxed);
        heatAccessor->second->lastDelayMs.store(nowMs, std::memory_order_relaxed);
        heatAccessor->second->heatUpdateSeq.fetch_add(1, std::memory_order_relaxed);
    }
    point.Record();
}
}  // namespace object_cache
}  // namespace datasystem
