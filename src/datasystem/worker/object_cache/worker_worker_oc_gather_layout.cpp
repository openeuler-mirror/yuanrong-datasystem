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

/** Description: Validates worker Batch Get aggregate-gather target layouts. */
#include "datasystem/worker/object_cache/worker_worker_oc_gather_layout.h"

#include <limits>

#include "datasystem/common/shared_memory/shm_unit.h"

namespace datasystem {
namespace object_cache {
namespace {
constexpr uint64_t ALIGNMENT_MASK = 3;

bool AddWouldOverflow(uint64_t lhs, uint64_t rhs)
{
    return lhs > std::numeric_limits<uint64_t>::max() - rhs;
}

bool GetAlignedSize(uint64_t dataSize, uint64_t metadataSize, uint64_t &alignedSize)
{
    if (AddWouldOverflow(dataSize, metadataSize)) {
        return false;
    }
    const uint64_t targetSize = dataSize + metadataSize;
    if (AddWouldOverflow(targetSize, ALIGNMENT_MASK)) {
        return false;
    }
    alignedSize = Align4BitsCeiling(targetSize);
    return true;
}

bool SameHostPort(const HostPortPb &lhs, const HostPortPb &rhs)
{
    return lhs.host() == rhs.host() && lhs.port() == rhs.port();
}

bool SameUrmaDestination(const UrmaRemoteAddrPb &lhs, const UrmaRemoteAddrPb &rhs)
{
    return lhs.seg_va() == rhs.seg_va() && SameHostPort(lhs.request_address(), rhs.request_address())
           && lhs.client_id() == rhs.client_id() && lhs.has_chip_id() == rhs.has_chip_id()
           && (!lhs.has_chip_id() || lhs.chip_id() == rhs.chip_id());
}

bool SameUcpDestination(const UcpRemoteInfoPb &lhs, const UcpRemoteInfoPb &rhs)
{
    return SameHostPort(lhs.remote_ip_addr(), rhs.remote_ip_addr())
           && lhs.remote_worker_addr() == rhs.remote_worker_addr() && lhs.rkey() == rhs.rkey();
}

bool ValidateUrmaSubgroup(const BatchGetObjectRemoteReqPb &request, const AggregateGatherSubgroup &subgroup,
                          uint64_t metadataSize)
{
    const auto &firstRequest = request.requests(subgroup.startIndex);
    if (!firstRequest.has_urma_info() || firstRequest.has_ucp_info()) {
        return false;
    }
    const auto &first = firstRequest.urma_info();
    if (first.seg_data_offset() < metadataSize || AddWouldOverflow(first.seg_data_offset(), subgroup.byteSize)) {
        return false;
    }

    uint64_t expectedOffset = first.seg_data_offset();
    const uint64_t endIndex = subgroup.startIndex + subgroup.requestCount;
    for (uint64_t index = subgroup.startIndex; index < endIndex; ++index) {
        const auto &item = request.requests(index);
        if (!item.has_urma_info() || item.has_ucp_info() || !SameUrmaDestination(first, item.urma_info())
            || item.urma_info().seg_data_offset() != expectedOffset) {
            return false;
        }
        uint64_t alignedSize = 0;
        if (!GetAlignedSize(item.data_size(), metadataSize, alignedSize)
            || AddWouldOverflow(expectedOffset, alignedSize)) {
            return false;
        }
        expectedOffset += alignedSize;
    }
    return expectedOffset == first.seg_data_offset() + subgroup.byteSize;
}

bool ValidateUcpSubgroup(const BatchGetObjectRemoteReqPb &request, const AggregateGatherSubgroup &subgroup,
                         uint64_t metadataSize)
{
    const auto &firstRequest = request.requests(subgroup.startIndex);
    if (!firstRequest.has_ucp_info() || firstRequest.has_urma_info()) {
        return false;
    }
    const auto &first = firstRequest.ucp_info();
    if (first.remote_buf() < metadataSize || AddWouldOverflow(first.remote_buf(), subgroup.byteSize)) {
        return false;
    }

    uint64_t expectedAddress = first.remote_buf();
    const uint64_t endIndex = subgroup.startIndex + subgroup.requestCount;
    for (uint64_t index = subgroup.startIndex; index < endIndex; ++index) {
        const auto &item = request.requests(index);
        if (!item.has_ucp_info() || item.has_urma_info() || !SameUcpDestination(first, item.ucp_info())
            || item.ucp_info().remote_buf() != expectedAddress) {
            return false;
        }
        uint64_t alignedSize = 0;
        if (!GetAlignedSize(item.data_size(), metadataSize, alignedSize)
            || AddWouldOverflow(expectedAddress, alignedSize)) {
            return false;
        }
        expectedAddress += alignedSize;
    }
    return expectedAddress == first.remote_buf() + subgroup.byteSize;
}

bool ValidateSubgroup(const BatchGetObjectRemoteReqPb &request, const AggregateGatherSubgroup &subgroup,
                      uint64_t metadataSize)
{
    const auto &first = request.requests(subgroup.startIndex);
    if (first.has_urma_info()) {
        return ValidateUrmaSubgroup(request, subgroup, metadataSize);
    }
    if (first.has_ucp_info()) {
        return ValidateUcpSubgroup(request, subgroup, metadataSize);
    }
    return false;
}
}  // namespace

bool BuildAggregateGatherPlan(const BatchGetObjectRemoteReqPb &request, uint64_t metadataSize,
                              uint64_t maxObjectSize, uint64_t maxSubgroupBytes, uint64_t maxSubgroupObjects,
                              std::vector<AggregateGatherSubgroup> &subgroups)
{
    subgroups.clear();
    if (!request.allow_aggregate_gather() || request.aggregate_gather_metadata_size() != metadataSize
        || request.requests().empty() || maxSubgroupBytes == 0 || maxSubgroupObjects == 0) {
        return false;
    }

    AggregateGatherSubgroup subgroup;
    for (uint64_t index = 0; index < static_cast<uint64_t>(request.requests_size()); ++index) {
        const auto dataSize = request.requests(index).data_size();
        uint64_t alignedSize = 0;
        if (dataSize > maxObjectSize || !GetAlignedSize(dataSize, metadataSize, alignedSize)
            || alignedSize > maxSubgroupBytes) {
            subgroups.clear();
            return false;
        }
        if (subgroup.requestCount > 0
            && (subgroup.requestCount >= maxSubgroupObjects || alignedSize > maxSubgroupBytes - subgroup.byteSize)) {
            subgroups.emplace_back(subgroup);
            subgroup = AggregateGatherSubgroup{ .startIndex = index };
        } else if (subgroup.requestCount == 0) {
            subgroup.startIndex = index;
        }
        subgroup.requestCount++;
        subgroup.byteSize += alignedSize;
    }
    if (subgroup.requestCount > 0) {
        subgroups.emplace_back(subgroup);
    }

    for (const auto &item : subgroups) {
        if (!ValidateSubgroup(request, item, metadataSize)) {
            subgroups.clear();
            return false;
        }
    }
    return true;
}

bool ShouldUseAggregateGather(const BatchGetObjectRemoteReqPb &request, bool isPipelineRequest,
                              uint64_t metadataSize, uint64_t maxObjectSize, uint64_t maxSubgroupBytes,
                              uint64_t maxSubgroupObjects, std::vector<AggregateGatherSubgroup> &subgroups)
{
    subgroups.clear();
    return !isPipelineRequest
           && BuildAggregateGatherPlan(request, metadataSize, maxObjectSize, maxSubgroupBytes, maxSubgroupObjects,
                                       subgroups);
}

void SetAggregateGatherAttestation(bool allRequestsUseAggregateMemory, uint64_t metadataSize,
                                   BatchGetObjectRemoteReqPb &request)
{
    const bool allowAggregateGather = allRequestsUseAggregateMemory && request.requests_size() > 1;
    request.set_allow_aggregate_gather(allowAggregateGather);
    if (allowAggregateGather) {
        request.set_aggregate_gather_metadata_size(metadataSize);
    } else {
        request.clear_aggregate_gather_metadata_size();
    }
}

}  // namespace object_cache
}  // namespace datasystem
