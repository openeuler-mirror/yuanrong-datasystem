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
 * Description: Incremental balanced-ring share bookkeeping for token placement.
 */
#ifndef DATASYSTEM_CLUSTER_ALGORITHM_BALANCED_RING_H
#define DATASYSTEM_CLUSTER_ALGORITHM_BALANCED_RING_H

#include <cstdint>
#include <cstddef>
#include <map>
#include <set>
#include <unordered_set>
#include <vector>

namespace datasystem::cluster {

struct PlacementOwner {
    uint32_t token{ 0 };
    uint32_t memberIndex{ 0 };
};

// Ring size beyond which balanced placement degrades to pure hash. Kept near 2x the seed candidate
// budget: past that, per-take landing error (ring/(2K) against ideal ring/(members*tokens)) exceeds the
// take itself, residual quality collapses back to pure-hash levels, and the dense per-token seed
// overrides keep inflating both planning cost and serialized topology size.
inline constexpr uint32_t BALANCED_PLACEMENT_MAX_RING_TOKENS = 2'000;

struct ArcRecord {
    uint64_t length;
    uint32_t start;
};

// Largest arc first; ties break on the smaller start token so planning stays deterministic.
struct ArcOrder {
    bool operator()(const ArcRecord &left, const ArcRecord &right) const;
};

// Incremental ring accounting under routing semantics: the member of token T owns the arc (prev(T), T]
// (lower_bound routes a key to the first token at or above it). Shares and per-member arcs are updated by
// splitting exactly one arc of the next-token owner, so only two members change per inserted token.
class BalancedRing {
public:
    BalancedRing(const std::vector<PlacementOwner> &owners, std::unordered_set<uint32_t> &occupied);

    ~BalancedRing() = default;

    bool Empty() const;

    // The member admitted last owns nothing yet; TotalMembers counts it while its tokens are placed.
    size_t TotalMembers() const;

    uint32_t HeaviestMember() const;

    // The member's token T owns (prev, T]: taking idealShare means landing at start+idealShare (a prefix of
    // the arc); when no arc is that long, take the whole arc minus one unit by landing at end-1.
    uint32_t TakeTarget(uint32_t member, uint64_t idealShare) const;

    void Insert(uint32_t token, uint32_t owner);

    // Grows the bookkeeping so the next admitted member gets a fresh index and share slot.
    void AdmitNewMember();

    void VerifyShares() const;

private:
    void AddArc(uint32_t member, ArcRecord arc);

    void BuildArcs();

    std::map<uint32_t, uint32_t> ring_;
    std::vector<std::set<ArcRecord, ArcOrder>> memberArcs_;
    std::vector<uint64_t> memberShares_;
    size_t memberCount_{ 0 };
};

}  // namespace datasystem::cluster

#endif  // DATASYSTEM_CLUSTER_ALGORITHM_BALANCED_RING_H
