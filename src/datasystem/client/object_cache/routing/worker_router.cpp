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

#include "datasystem/client/object_cache/routing/worker_router.h"

#include <algorithm>
#include <cstddef>
#include <iterator>
#include <unordered_set>
#include <utility>

#include "datasystem/client/object_cache/routing/broken_filter.h"
#include "datasystem/client/object_cache/routing/routing.h"
#include "datasystem/client/object_cache/routing/state_filter.h"
#include "datasystem/common/util/hash_ring_token.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {
namespace client {
namespace {
constexpr size_t DEFAULT_FILTER_COUNT = 2;
constexpr size_t MAX_ROUTING_TOKENS = 640'000;
}  // namespace

WorkerRouter::WorkerRouter(std::string myHostId, std::vector<std::shared_ptr<IWorkerFilter>> additionalFilters)
    : myHostId_(std::move(myHostId))
{
    filters_.reserve(additionalFilters.size() + DEFAULT_FILTER_COUNT);
    filters_.emplace_back(std::make_shared<StateFilter>(this));
    filters_.emplace_back(std::make_shared<BrokenFilter>());
    filters_.insert(filters_.end(), std::make_move_iterator(additionalFilters.begin()),
                    std::make_move_iterator(additionalFilters.end()));

    auto view = std::make_shared<RingView>();
    view->ring = std::make_shared<::datasystem::ClusterTopologyPb>();
    view->sameNodeWorkers = std::make_shared<std::vector<HostPort>>();
    view->tokenIndex = std::make_shared<TokenIndex>();
    std::atomic_store(&ringView_, std::shared_ptr<const RingView>(std::move(view)));
}

void WorkerRouter::SetHostId(std::string hostId)
{
    myHostId_ = std::move(hostId);
}

Status PreparedClusterTopology::BuildTokenIndex(const ::datasystem::ClusterTopologyPb &ring,
                                                std::shared_ptr<const TokenIndex> &tokenIndex)
{
    const auto tokensPerMember = ring.tokens_per_member();
    CHECK_FAIL_RETURN_STATUS(tokensPerMember > 0 && tokensPerMember <= MAX_HASH_RING_TOKENS_PER_MEMBER, K_INVALID,
                             "invalid hash ring tokens per member");
    size_t tokenMemberCount = 0;
    size_t routableMemberCount = 0;
    for (const auto &entry : ring.members()) {
        if (entry.second.state() != ::datasystem::MembershipPb::INITIAL) {
            ++tokenMemberCount;
        }
        if (entry.second.state() == ::datasystem::MembershipPb::ACTIVE
            || entry.second.state() == ::datasystem::MembershipPb::LEAVING) {
            ++routableMemberCount;
        }
    }
    CHECK_FAIL_RETURN_STATUS(tokenMemberCount <= MAX_ROUTING_TOKENS / tokensPerMember, K_INVALID,
                             "hash ring token count exceeds limit");
    auto idx = std::make_shared<TokenIndex>();
    std::unordered_set<uint32_t> occupied;
    occupied.reserve(routableMemberCount * tokensPerMember);
    for (const auto &entry : ring.members()) {
        if (entry.second.state() != ::datasystem::MembershipPb::ACTIVE
            && entry.second.state() != ::datasystem::MembershipPb::LEAVING) {
            continue;
        }
        HostPort hp;
        CHECK_FAIL_RETURN_STATUS(hp.ParseString(entry.first).IsOk(), K_INVALID, "invalid hash ring member address");
        std::vector<uint32_t> seeds(tokensPerMember);
        for (const auto &overridePb : entry.second.token_seed_overrides()) {
            CHECK_FAIL_RETURN_STATUS(
                overridePb.token_index() < tokensPerMember && overridePb.token_seed() > 0
                    && overridePb.token_seed() < MAX_HASH_RING_TOKEN_SEEDS,
                K_INVALID, "invalid hash ring token seed override");
            seeds[overridePb.token_index()] = overridePb.token_seed();
        }
        std::vector<uint32_t> tokens;
        MakeHashRingTokens(entry.first, seeds, tokens);
        for (uint32_t token : tokens) {
            CHECK_FAIL_RETURN_STATUS(occupied.insert(token).second, K_INVALID, "duplicate hash ring token");
            idx->tokenToWorker.emplace_back(token, static_cast<int>(idx->workers.size()));
        }
        idx->workers.push_back(std::move(hp));
    }
    std::sort(idx->tokenToWorker.begin(), idx->tokenToWorker.end(),
              [](const std::pair<uint32_t, int> &a, const std::pair<uint32_t, int> &b) {
                  return a.first < b.first;
              });
    tokenIndex = std::move(idx);
    return Status::OK();
}

PreparedClusterTopology::PreparedClusterTopology(
    ConstructionToken,
    std::shared_ptr<const ::datasystem::ClusterTopologyPb> topology,
    std::shared_ptr<const TokenIndex> tokenIndex)
    : topology_(std::move(topology)), tokenIndex_(std::move(tokenIndex))
{
}

Status PreparedClusterTopology::Create(::datasystem::ClusterTopologyPb &&ring,
                                       std::unique_ptr<PreparedClusterTopology> &prepared)
{
    std::shared_ptr<const TokenIndex> tokenIndex;
    RETURN_IF_NOT_OK(BuildTokenIndex(ring, tokenIndex));

    auto topology = std::make_shared<const ::datasystem::ClusterTopologyPb>(std::move(ring));
    prepared = std::make_unique<PreparedClusterTopology>(ConstructionToken{}, std::move(topology),
                                                         std::move(tokenIndex));
    return Status::OK();
}

const ::datasystem::ClusterTopologyPb &PreparedClusterTopology::GetTopology() const noexcept
{
    return *topology_;
}

const std::shared_ptr<const TokenIndex> &PreparedClusterTopology::GetTokenIndex() const noexcept
{
    return tokenIndex_;
}

bool WorkerRouter::IsWorkerAvailable(const HostPort &addr) const
{
    return std::all_of(filters_.begin(), filters_.end(),
        [&](const std::shared_ptr<IWorkerFilter> &f) { return f->IsAvailable(addr); });
}

bool WorkerRouter::IsExcluded(const HostPort &addr, const std::vector<HostPort> &exclude) const
{
    return std::any_of(exclude.begin(), exclude.end(),
        [&](const HostPort &e) { return e == addr; });
}

Status WorkerRouter::SelectWorker(const std::string &key, DataPlacementPolicy policy, HostPort &worker,
                                  const std::vector<HostPort> &exclude) const
{
    auto view = std::atomic_load(&ringView_);
    return SelectWorkerFromView(key, policy, worker, exclude, view);
}

Status WorkerRouter::SelectWorkerFromView(const std::string &key, DataPlacementPolicy policy, HostPort &worker,
                                          const std::vector<HostPort> &exclude,
                                          const std::shared_ptr<const RingView> &view) const
{
    const auto &idx = view->tokenIndex;
    uint32_t keyHash = MurmurHash3_32(key);

    if (policy != DataPlacementPolicy::PREFERRED_META_OWNER) {
        const auto &sameNodeWorkers = *view->sameNodeWorkers;
        const size_t sameNodeCount = sameNodeWorkers.size();
        const size_t start = sameNodeCount == 0 ? 0 : keyHash % sameNodeCount;
        for (size_t i = 0; i < sameNodeCount; ++i) {
            const auto &w = sameNodeWorkers[(start + i) % sameNodeCount];
            if (IsExcluded(w, exclude)) {
                continue;
            }
            if (IsWorkerAvailable(w)) {
                worker = w;
                return Status::OK();
            }
        }
        if (policy == DataPlacementPolicy::REQUIRED_SAME_NODE) {
            return Status(K_NO_AVAILABLE_WORKER, "No same-node worker is available");
        }
    }

    // PREFERRED_META_OWNER, or the fallback for PREFERRED_SAME_NODE.
    if (idx->tokenToWorker.empty()) {
        return Status(K_NOT_FOUND, "Hash ring is empty, no routable workers");
    }

    // RoutingSnapshot owns closed token ranges, so an exact token hash belongs to that token's worker.
    auto iter = std::lower_bound(idx->tokenToWorker.begin(), idx->tokenToWorker.end(), keyHash,
        [](const std::pair<uint32_t, int> &token, uint32_t val) { return token.first < val; });
    int start = (iter == idx->tokenToWorker.end()) ? 0 : static_cast<int>(iter - idx->tokenToWorker.begin());

    int total = static_cast<int>(idx->tokenToWorker.size());
    for (int i = 0; i < total; ++i) {
        int slot = (start + i) % total;
        int workerIdx = idx->tokenToWorker[slot].second;
        const HostPort &candidate = idx->workers[workerIdx];
        if (IsExcluded(candidate, exclude)) {
            continue;
        }
        if (IsWorkerAvailable(candidate)) {
            worker = candidate;
            return Status::OK();
        }
    }

    return Status(K_NO_AVAILABLE_WORKER, "All workers filtered or excluded");
}

Status WorkerRouter::SelectWorkers(const std::vector<std::string> &keys, DataPlacementPolicy policy,
                                   std::unordered_map<HostPort, std::vector<std::string>> &groups,
                                   const std::vector<HostPort> &exclude) const
{
    auto view = std::atomic_load(&ringView_);
    if (keys.empty()) {
        groups.clear();
        return Status::OK();
    }
    std::unordered_map<HostPort, std::vector<std::string>> newGroups;
    for (const auto &key : keys) {
        HostPort owner;
        Status s = SelectWorkerFromView(key, policy, owner, exclude, view);
        if (s.IsError()) {
            return s;
        }
        newGroups[owner].push_back(key);
    }
    groups = std::move(newGroups);
    return Status::OK();
}

std::vector<HostPort> WorkerRouter::GetAvailableWorkers() const
{
    auto view = std::atomic_load(&ringView_);
    std::vector<HostPort> result;
    for (const auto &w : view->tokenIndex->workers) {
        if (IsWorkerAvailable(w)) {
            result.push_back(w);
        }
    }
    return result;
}

void WorkerRouter::UpdateHashRing(const PreparedClusterTopology &prepared,
                                  const std::unordered_map<std::string, std::string> &hostIdMap)
{
    const auto &ring = prepared.topology_;

    // Build same-node workers list
    auto sameNode = std::make_shared<std::vector<HostPort>>();
    for (const auto &entry : ring->members()) {
        if (entry.second.state() != ::datasystem::MembershipPb::ACTIVE) {
            continue;
        }
        auto it = hostIdMap.find(entry.first);
        if (!myHostId_.empty() && it != hostIdMap.end() && it->second == myHostId_) {
            HostPort hp;
            if (hp.ParseString(entry.first).IsOk()) {
                sameNode->push_back(std::move(hp));
            }
        }
    }
    // Stable ordering makes same-node placement deterministic across protobuf map iteration orders.
    std::sort(sameNode->begin(), sameNode->end());

    // Build new view (all-or-nothing: readers see consistent ring + index + sameNode)
    auto newView = std::make_shared<RingView>();
    newView->ring = ring;
    newView->sameNodeWorkers = sameNode;
    newView->tokenIndex = prepared.tokenIndex_;

    // Single atomic store — all readers see the new view atomically
    std::atomic_store(&ringView_, std::shared_ptr<const RingView>(std::move(newView)));

    // Notify filters
    for (auto &f : filters_) {
        f->OnHashRingUpdated(*ring);
    }
}

void WorkerRouter::UpdateState(const HostPort &addr, StatusCode status)
{
    for (auto &f : filters_) {
        f->OnWorkerStateChange(addr, status);
    }
}

WorkerRingState WorkerRouter::GetRingState(const HostPort &addr) const
{
    auto view = std::atomic_load(&ringView_);
    std::string addrStr = addr.ToString();
    auto it = view->ring->members().find(addrStr);
    if (it == view->ring->members().end()) {
        return WorkerRingState::UNKNOWN;
    }
    switch (it->second.state()) {
        case ::datasystem::MembershipPb::INITIAL:
            return WorkerRingState::INITIAL;
        case ::datasystem::MembershipPb::JOINING:
            return WorkerRingState::JOINING;
        case ::datasystem::MembershipPb::ACTIVE:
            return WorkerRingState::ACTIVE;
        case ::datasystem::MembershipPb::PRE_LEAVING:
            return WorkerRingState::PRE_LEAVING;
        case ::datasystem::MembershipPb::LEAVING:
            return WorkerRingState::LEAVING;
        case ::datasystem::MembershipPb::FAILED:
            return WorkerRingState::FAILED;
        default:
            return WorkerRingState::UNKNOWN;
    }
}

}  // namespace client
}  // namespace datasystem
