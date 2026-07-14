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

#include "datasystem/client/routing/worker_router.h"

#include <algorithm>
#include <cstddef>
#include <iterator>
#include <utility>

#include "datasystem/client/routing/broken_filter.h"
#include "datasystem/client/routing/state_filter.h"

namespace datasystem {
namespace client {
namespace {
constexpr size_t DEFAULT_FILTER_COUNT = 2;
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
    view->tokenIndex = std::make_shared<RingView::TokenIndex>();
    std::atomic_store(&ringView_, std::shared_ptr<const RingView>(std::move(view)));
}

void WorkerRouter::SetHostId(std::string hostId)
{
    myHostId_ = std::move(hostId);
}

std::shared_ptr<const WorkerRouter::RingView::TokenIndex> WorkerRouter::BuildTokenIndex(
    const ::datasystem::ClusterTopologyPb &ring)
{
    auto idx = std::make_shared<RingView::TokenIndex>();
    for (const auto &entry : ring.members()) {
        if (entry.second.state() != ::datasystem::MembershipPb::ACTIVE
            && entry.second.state() != ::datasystem::MembershipPb::LEAVING) {
            continue;
        }
        if (entry.second.tokens().empty()) {
            continue;
        }
        HostPort hp;
        if (!hp.ParseString(entry.first).IsOk()) {
            continue;
        }
        for (uint32_t token : entry.second.tokens()) {
            idx->tokenToWorker.emplace_back(token, static_cast<int>(idx->workers.size()));
        }
        idx->workers.push_back(std::move(hp));
    }
    std::sort(idx->tokenToWorker.begin(), idx->tokenToWorker.end(),
              [](const std::pair<uint32_t, int> &a, const std::pair<uint32_t, int> &b) {
                  return a.first < b.first;
              });
    return idx;
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

Status WorkerRouter::SelectWorker(const std::string &key, SelectStrategy strategy,
                                  HostPort &worker, const std::vector<HostPort> &exclude) const
{
    auto view = std::atomic_load(&ringView_);
    return SelectWorkerFromView(key, strategy, worker, exclude, view);
}

Status WorkerRouter::SelectWorkerFromView(const std::string &key, SelectStrategy strategy, HostPort &worker,
                                          const std::vector<HostPort> &exclude,
                                          const std::shared_ptr<const RingView> &view) const
{
    const auto &idx = view->tokenIndex;
    uint32_t keyHash = MurmurHash3_32(key);

    if (strategy == SelectStrategy::SAME_NODE_PREFERRED) {
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
        // Fallback to hash ring affinity
    }

    // HASH_RING_AFFINITY (or SAME_NODE_PREFERRED fallback)
    if (idx->tokenToWorker.empty()) {
        return Status(K_NOT_FOUND, "Hash ring is empty, no routable workers");
    }

    auto iter = std::upper_bound(idx->tokenToWorker.begin(), idx->tokenToWorker.end(), keyHash,
        [](uint32_t val, const std::pair<uint32_t, int> &token) { return val < token.first; });
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

Status WorkerRouter::SelectWorkers(const std::vector<std::string> &keys, SelectStrategy strategy,
                                   std::unordered_map<HostPort, std::vector<std::string>> &groups) const
{
    auto view = std::atomic_load(&ringView_);
    if (keys.empty()) {
        groups.clear();
        return Status::OK();
    }
    if (view->tokenIndex->tokenToWorker.empty()) {
        return Status(K_NOT_FOUND, "Hash ring is empty");
    }

    std::unordered_map<HostPort, std::vector<std::string>> newGroups;
    for (const auto &key : keys) {
        HostPort owner;
        Status s = SelectWorkerFromView(key, strategy, owner, {}, view);
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

void WorkerRouter::UpdateHashRing(std::shared_ptr<const ::datasystem::ClusterTopologyPb> ring,
    std::shared_ptr<const std::unordered_map<std::string, std::string>> hostIdMap)
{
    if (ring == nullptr || hostIdMap == nullptr) {
        return;
    }

    // Build same-node workers list
    auto sameNode = std::make_shared<std::vector<HostPort>>();
    for (const auto &entry : ring->members()) {
        if (entry.second.state() != ::datasystem::MembershipPb::ACTIVE) {
            continue;
        }
        auto it = hostIdMap->find(entry.first);
        if (!myHostId_.empty() && it != hostIdMap->end() && it->second == myHostId_) {
            HostPort hp;
            if (hp.ParseString(entry.first).IsOk()) {
                sameNode->push_back(std::move(hp));
            }
        }
    }

    // Build new view (all-or-nothing: readers see consistent ring + index + sameNode)
    auto newView = std::make_shared<RingView>();
    newView->ring = ring;
    newView->sameNodeWorkers = sameNode;
    newView->tokenIndex = BuildTokenIndex(*ring);

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
