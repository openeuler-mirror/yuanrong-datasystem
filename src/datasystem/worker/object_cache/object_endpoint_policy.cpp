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
 * Description: Object-cache endpoint availability policy over narrow topology capabilities.
 */
#include "datasystem/worker/object_cache/object_endpoint_policy.h"

#include <map>
#include <sstream>

#include "datasystem/common/log/log.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/uuid_generator.h"

namespace datasystem::object_cache {
namespace {
constexpr uint32_t ROUTE_FAILURE_LOG_EVERY_N = 100;

bool IsDirectoryLag(const Status &status)
{
    return status.GetCode() == K_NOT_READY || status.GetCode() == K_NOT_FOUND;
}

std::string SummarizeRouteFailureCodes(const std::unordered_map<std::string, Status> &failures)
{
    std::map<StatusCode, size_t> counts;
    for (const auto &failure : failures) {
        ++counts[failure.second.GetCode()];
    }
    std::ostringstream summary;
    bool first = true;
    for (const auto &[code, count] : counts) {
        summary << (first ? "" : ",") << Status::StatusCodeName(code) << ":" << count;
        first = false;
    }
    return summary.str();
}
}  // namespace

ObjectEndpointPolicy::ObjectEndpointPolicy(const worker::MetadataRouteResolver &resolver,
                                           const cluster::MembershipEndpointView &membership)
    : resolver_(resolver), membership_(membership)
{
}

Status ObjectEndpointPolicy::CheckEndpoint(const HostPort &address, bool allowDirectoryLag) const
{
    const std::string endpointAddress = address.ToString();
    cluster::MemberEndpoint endpoint;
    auto rc = membership_.ResolveByAddress(endpointAddress, endpoint);
    if (rc.IsError()) {
        if (allowDirectoryLag && IsDirectoryLag(rc)) {
            return Status::OK();
        }
        RETURN_STATUS(rc.GetCode(), "The node " + endpointAddress + " could not be found in topology membership.");
    }
    CHECK_FAIL_RETURN_STATUS(endpoint.localAvailability != cluster::EndpointAvailability::UNREACHABLE, K_MASTER_TIMEOUT,
                             "Disconnected from remote node " + endpointAddress);
    return Status::OK();
}

Status ObjectEndpointPolicy::CheckMetaOwner(std::string_view key, bool allowDirectoryLag) const
{
    HostPort address;
    RETURN_IF_NOT_OK(resolver_.ResolveOwner(key, address));
    return CheckEndpoint(address, allowDirectoryLag);
}

std::string ObjectEndpointPolicy::GetDiagnosticMemberId(const std::string &address) const
{
    cluster::MemberEndpoint endpoint;
    auto rc = membership_.ResolveByAddress(address, endpoint);
    if (rc.IsError()) {
        VLOG(1) << "CLUSTER_ENDPOINT_DIAGNOSTIC_LOOKUP_FAILED address=" << address
                << " status=" << rc.ToString();
        return "";
    }
    return endpoint.identity.id.size() == UUID_SIZE ? BytesUuidToString(endpoint.identity.id) : endpoint.identity.id;
}

void AppendRouteFailures(worker::MetaOwnerRouteGroups &grouped, const HostPort &failureOwner)
{
    if (grouped.failures.empty()) {
        return;
    }
    LOG_FIRST_AND_EVERY_N(WARNING, ROUTE_FAILURE_LOG_EVERY_N)
        << "CLUSTER_ROUTE_FAILURE component=object_endpoint_policy failure_count=" << grouped.failures.size()
        << " status_counts=" << SummarizeRouteFailureCodes(grouped.failures);
    auto &keys = grouped.groups[failureOwner];
    keys.reserve(keys.size() + grouped.failures.size());
    for (const auto &failure : grouped.failures) {
        keys.emplace_back(failure.first);
    }
}

}  // namespace datasystem::object_cache
