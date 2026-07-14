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
 * Description: Immutable validated cluster topology snapshot.
 */
#ifndef DATASYSTEM_CLUSTER_MODEL_TOPOLOGY_SNAPSHOT_H
#define DATASYSTEM_CLUSTER_MODEL_TOPOLOGY_SNAPSHOT_H

#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "datasystem/cluster/model/topology_types.h"
#include "datasystem/utils/status.h"

namespace datasystem::cluster {

class HashAlgorithm;

class TopologySnapshot final {
public:
    /**
     * @brief Validate state and build address/id/token indexes.
     * @param[in] state Domain topology state to validate and consume.
     * @param[in] authorityRevision Revision of the authoritative exact read.
     * @param[in] canonicalDigest Digest of canonical topology bytes.
     * @param[out] snapshot New snapshot; unchanged on failure.
     * @return K_OK on success; K_INVALID for illegal state or evidence.
     */
    static Status Create(TopologyState state, int64_t authorityRevision, std::string canonicalDigest,
                         std::shared_ptr<const TopologySnapshot> &snapshot);

    /**
     * @brief Destroy immutable storage and indexes.
     */
    ~TopologySnapshot() = default;

    /**
     * @brief Disable copying immutable Snapshot storage.
     */
    TopologySnapshot(const TopologySnapshot &) = delete;

    /**
     * @brief Disable copy assignment of immutable Snapshot storage.
     */
    TopologySnapshot &operator=(const TopologySnapshot &) = delete;

    /**
     * @brief Return the topology version.
     * @return Authoritative monotonic version.
     */
    uint64_t Version() const noexcept;

    /**
     * @brief Return the exact-read revision.
     * @return Non-negative backend revision.
     */
    int64_t AuthorityRevision() const noexcept;

    /**
     * @brief Return the canonical digest.
     * @return Stable snapshot-lifetime reference.
     */
    const std::string &CanonicalDigest() const noexcept;

    /**
     * @brief Check first bootstrap state.
     * @return True after bootstrap committed.
     */
    bool ClusterHasInit() const noexcept;

    /**
     * @brief Return the minimal active batch.
     * @return Stable optional reference.
     */
    const std::optional<ActiveBatch> &GetActiveBatch() const noexcept;

    /**
     * @brief Return canonically ordered members.
     * @return Stable member-vector reference.
     */
    const std::vector<Member> &Members() const noexcept;

    /**
     * @brief Find one member by canonical address.
     * @param[in] address Canonical member address.
     * @param[out] member Stable snapshot-lifetime pointer.
     * @return K_OK or K_NOT_FOUND.
     */
    Status FindMemberByAddress(const std::string &address, const Member *&member) const;

    /**
     * @brief Find one member by exact binary id.
     * @param[in] id Exact 16-byte membership id.
     * @param[out] member Stable snapshot-lifetime pointer.
     * @return K_OK, K_INVALID, or K_NOT_FOUND.
     */
    Status FindMemberById(const std::string &id, const Member *&member) const;

    /**
     * @brief Return committed owners.
     * @return ACTIVE/PRE_LEAVING/LEAVING member pointers.
     */
    const std::vector<const Member *> &CommittedMembers() const noexcept;

private:
    friend class HashAlgorithm;

    /**
     * @brief Construct prevalidated immutable storage.
     * @param[in] state Validated state to consume.
     * @param[in] authorityRevision Non-negative backend revision.
     * @param[in] canonicalDigest Canonical digest to consume.
     */
    TopologySnapshot(TopologyState state, int64_t authorityRevision, std::string canonicalDigest);

    /**
     * @brief Build stable indexes once.
     * @return K_OK or K_INVALID.
     */
    Status BuildIndexes();

    /**
     * @brief Reserve immutable index storage before inserting entries.
     * @param[in] scaleOut Whether a prospective SCALE_OUT ring is required.
     */
    void ReserveIndexes(bool scaleOut);

    /**
     * @brief Validate members and insert immutable index entries.
     * @param[in] scaleOut Whether to index JOINING members in the prospective ring.
     * @return K_OK or K_INVALID.
     */
    Status BuildIndexEntries(bool scaleOut);

    TopologyState state_;
    int64_t authorityRevision_{ 0 };
    std::string canonicalDigest_;
    // Views reference immutable strings in state_.members and avoid duplicating every address/id in the indexes.
    std::unordered_map<std::string_view, size_t> addressIndex_;
    std::unordered_map<std::string_view, size_t> idIndex_;
    std::vector<const Member *> committedMembers_;
    std::vector<std::pair<uint32_t, const Member *>> committedTokenOwners_;
    std::vector<std::pair<uint32_t, const Member *>> prospectiveTokenOwners_;
};

}  // namespace datasystem::cluster

#endif  // DATASYSTEM_CLUSTER_MODEL_TOPOLOGY_SNAPSHOT_H
