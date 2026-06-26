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
 * Description: Immutable R0 routing snapshot.
 */
#ifndef DATASYSTEM_TOPOLOGY_ROUTING_ROUTING_SNAPSHOT_H
#define DATASYSTEM_TOPOLOGY_ROUTING_ROUTING_SNAPSHOT_H

#include <algorithm>
#include <cstddef>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "datasystem/common/util/status_helper.h"
#include "datasystem/topology/routing/placement_types.h"

namespace datasystem {
namespace topology {

struct RoutingOwnerEntry {
    RoutingRange unit;
    std::string ownerWorkerId;
};

struct RoutingRedirectHint {
    RoutingRange unit;
    std::string targetWorkerId;
    HostPort targetAddress;
};

struct RoutingSnapshotFacts {
    std::vector<RoutingRedirectHint> redirectHints;
    std::vector<RoutingRange> localOwnedRanges;
    std::vector<std::string> workerOrder;
    std::unordered_set<std::string> validWorkerIds;
    std::unordered_set<std::string> activeWorkerIds;
};

class RoutingSnapshot {
public:
    RoutingSnapshot() = default;
    ~RoutingSnapshot() = default;

    RoutingSnapshot(int64_t version, std::vector<RoutingOwnerEntry> owners)
        : version_(version), sortedOwners_(std::move(owners))
    {
        Sort();
    }

    RoutingSnapshot(int64_t version, std::vector<RoutingOwnerEntry> owners, std::vector<RoutingRedirectHint> hints)
        : version_(version), sortedOwners_(std::move(owners)), redirectHints_(std::move(hints))
    {
        Sort();
    }

    RoutingSnapshot(int64_t version, std::vector<RoutingOwnerEntry> owners, RoutingSnapshotFacts facts)
        : version_(version),
          sortedOwners_(std::move(owners)),
          redirectHints_(std::move(facts.redirectHints)),
          localOwnedRanges_(std::move(facts.localOwnedRanges)),
          workerOrder_(std::move(facts.workerOrder)),
          validWorkerIds_(std::move(facts.validWorkerIds)),
          activeWorkerIds_(std::move(facts.activeWorkerIds))
    {
        Sort();
    }

    /**
     * @brief Get the snapshot version.
     * @return Version carried by this immutable snapshot.
     */
    int64_t Version() const
    {
        return version_;
    }

    /**
     * @brief Check whether this snapshot has no routing entries.
     * @return True if no owner entry is available.
     */
    bool Empty() const
    {
        return sortedOwners_.empty();
    }

    /**
     * @brief Locate owner entry for an object hash.
     * @param[in] objectHash Hash value of the business key.
     * @param[out] entry Matching owner entry.
     * @return K_OK if found, K_NOT_READY if snapshot is empty, K_NOT_FOUND if no range contains the hash.
     *
     * Request threads only read this immutable value object. This method must not perform repository/backend IO,
     * CAS/List/Watch, task scan, migration, recovery, cleanup, or success-path logging.
     */
    Status Locate(uint32_t objectHash, RoutingOwnerEntry &entry) const
    {
        CHECK_FAIL_RETURN_STATUS(!sortedOwners_.empty(), K_NOT_READY, "Routing snapshot is empty.");
        const RoutingOwnerEntry *found = FindEntry(sortedOwners_, objectHash);
        CHECK_FAIL_RETURN_STATUS(found != nullptr, K_NOT_FOUND, "No routing owner contains the object hash.");
        entry = *found;
        return Status::OK();
    }

    /**
     * @brief Find the redirect hint covering a given object hash.
     * @param[in] objectHash Hash value of the business key.
     * @param[out] hint Matching redirect hint.
     * @return True if a hint covers the hash (including wrapped ranges), false otherwise.
     */
    bool FindRedirectHint(uint32_t objectHash, RoutingRedirectHint &hint) const
    {
        const RoutingRedirectHint *found = FindEntry(redirectHints_, objectHash);
        if (found != nullptr) {
            hint = *found;
            return true;
        }
        return false;
    }

    /**
     * @brief Return immutable owner entries for tests and snapshot publishers.
     * @return Sorted owner entries.
     */
    const std::vector<RoutingOwnerEntry> &Owners() const
    {
        return sortedOwners_;
    }

    /**
     * @brief Return immutable redirect hints for tests and snapshot publishers.
     * @return Sorted redirect hint entries.
     */
    const std::vector<RoutingRedirectHint> &RedirectHints() const
    {
        return redirectHints_;
    }

    /**
     * @brief Return local worker owned hash ranges.
     * @return Local owned hash ranges.
     */
    const std::vector<RoutingRange> &LocalOwnedRanges() const
    {
        return localOwnedRanges_;
    }

    /**
     * @brief Return valid worker ids in the current topology.
     * @return Valid worker ids.
     */
    const std::unordered_set<std::string> &ValidWorkerIds() const
    {
        return validWorkerIds_;
    }

    /**
     * @brief Return active worker ids in the current topology.
     * @return Active worker ids.
     */
    const std::unordered_set<std::string> &ActiveWorkerIds() const
    {
        return activeWorkerIds_;
    }

    /**
     * @brief Return worker ids in standby lookup order.
     * @return Ordered worker ids.
     */
    const std::vector<std::string> &WorkerOrder() const
    {
        return workerOrder_;
    }

    /**
     * @brief Return the next worker id after the input worker id in topology order.
     * @param[in] workerId Current worker id.
     * @param[out] nextWorkerId Next worker id.
     * @return K_OK if found, K_RUNTIME_ERROR if no standby worker exists.
     */
    Status GetStandbyWorkerId(const std::string &workerId, std::string &nextWorkerId) const
    {
        CHECK_FAIL_RETURN_STATUS(workerOrder_.size() > 1, K_RUNTIME_ERROR, "Standby worker not found.");
        auto iter = std::find(workerOrder_.begin(), workerOrder_.end(), workerId);
        CHECK_FAIL_RETURN_STATUS(iter != workerOrder_.end(), K_NOT_FOUND, "Worker is not found in topology order.");
        for (size_t i = 1; i < workerOrder_.size(); ++i) {
            const auto &candidate =
                workerOrder_[(static_cast<size_t>(iter - workerOrder_.begin()) + i) % workerOrder_.size()];
            if (candidate != workerId) {
                nextWorkerId = candidate;
                return Status::OK();
            }
        }
        RETURN_STATUS(K_RUNTIME_ERROR, "Standby worker not found.");
    }

private:
    /**
     * @brief Find the entry whose RoutingRange covers objectHash via binary search + wrapped-front fallback.
     * @param[in] entries Entries sorted by unit.rangeEnd (owners or redirect hints).
     * @param[in] objectHash Hash value of the business key.
     * @return Pointer to the matching entry, or nullptr if none covers the hash.
     *
     * Entries are sorted by rangeEnd, so lower_bound by rangeEnd lands on the first entry whose end is >= the hash.
     * A wrapped range (begin > end) at the front is checked last because it spans the 0 boundary.
     */
    template <typename Entry>
    static const Entry *FindEntry(const std::vector<Entry> &entries, uint32_t objectHash)
    {
        auto it = std::lower_bound(entries.begin(), entries.end(), objectHash,
                                   [](const Entry &entry, uint32_t hash) { return entry.unit.rangeEnd < hash; });
        if (it != entries.end() && it->unit.Contains(objectHash)) {
            return &(*it);
        }
        if (!entries.empty() && entries.front().unit.IsWrapped() && entries.front().unit.Contains(objectHash)) {
            return &entries.front();
        }
        return nullptr;
    }

    /**
     * @brief Sort owner entries and redirect hints by range end to enable binary search.
     *
     * Must be called once at construction after the entries are populated, so that Locate / FindRedirectHint can use
     * lower_bound lookups. Sorting here keeps the published snapshot immutable and ready for concurrent reads.
     */
    void Sort()
    {
        std::sort(sortedOwners_.begin(), sortedOwners_.end(),
                  [](const auto &lhs, const auto &rhs) { return lhs.unit.rangeEnd < rhs.unit.rangeEnd; });
        std::sort(redirectHints_.begin(), redirectHints_.end(),
                  [](const auto &lhs, const auto &rhs) { return lhs.unit.rangeEnd < rhs.unit.rangeEnd; });
    }

    int64_t version_ = -1;
    std::vector<RoutingOwnerEntry> sortedOwners_;
    std::vector<RoutingRedirectHint> redirectHints_;
    std::vector<RoutingRange> localOwnedRanges_;
    std::vector<std::string> workerOrder_;
    std::unordered_set<std::string> validWorkerIds_;
    std::unordered_set<std::string> activeWorkerIds_;
};

}  // namespace topology
}  // namespace datasystem
#endif  // DATASYSTEM_TOPOLOGY_ROUTING_ROUTING_SNAPSHOT_H
