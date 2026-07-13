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

/** Description: Implements batched object metadata access and one-layer redirect handling. */

#include "datasystem/client/transport/metadata/object_metadata_client.h"

#include <deque>
#include <unordered_map>
#include <utility>

#include "datasystem/common/log/log.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {
namespace client {
namespace {
struct RedirectBatch {
    HostPort address;
    ObjectMetadataBatch items;
};

Status ValidateAndResetItems(const ObjectMetadataBatch &items)
{
    CHECK_FAIL_RETURN_STATUS(!items.empty(), K_INVALID, "Metadata query items are empty");
    for (auto *item : items) {
        RETURN_RUNTIME_ERROR_IF_NULL(item);
        CHECK_FAIL_RETURN_STATUS(!item->objectKey.empty(), K_INVALID, "Object key is empty");
        item->status = Status(K_NOT_READY, "Object metadata is not resolved");
        item->location.Clear();
    }
    return Status::OK();
}

Status PartitionInitialResponse(const ObjectMetadataBatch &items, const master::QueryAndGetRspPb &response,
                                ObjectMetadataBatch &localItems,
                                std::vector<RedirectBatch> &redirectBatches)
{
    std::unordered_map<std::string, std::deque<HostPort>> redirectTargets;
    for (const auto &redirectInfo : response.info()) {
        CHECK_FAIL_RETURN_STATUS(redirectInfo.change_meta_ids_size() > 0, K_RUNTIME_ERROR,
                                 "QueryAndGet returned an empty redirect group");
        HostPort address;
        RETURN_IF_NOT_OK(address.ParseString(redirectInfo.redirect_meta_address()));
        for (const auto &objectKey : redirectInfo.change_meta_ids()) {
            redirectTargets[objectKey].emplace_back(address);
        }
    }

    localItems.clear();
    localItems.reserve(items.size());
    std::unordered_map<HostPort, RedirectBatch *> batchesByAddress;
    // The address map keeps pointers into redirectBatches, so prevent vector relocation while grouping.
    redirectBatches.reserve(response.info_size());
    batchesByAddress.reserve(response.info_size());
    for (auto *item : items) {
        auto target = redirectTargets.find(item->objectKey);
        if (target == redirectTargets.end() || target->second.empty()) {
            localItems.push_back(item);
            continue;
        }
        const HostPort address = std::move(target->second.front());
        target->second.pop_front();
        auto inserted = batchesByAddress.emplace(address, nullptr);
        if (inserted.second) {
            redirectBatches.push_back({ address, {} });
            inserted.first->second = &redirectBatches.back();
        }
        inserted.first->second->items.push_back(item);
    }
    for (const auto &target : redirectTargets) {
        CHECK_FAIL_RETURN_STATUS(target.second.empty(), K_RUNTIME_ERROR,
                                 "QueryAndGet redirect key does not match request");
    }
    return Status::OK();
}

void SetBatchError(const ObjectMetadataBatch &items, const Status &status)
{
    for (auto *item : items) {
        item->status = status;
    }
}
}  // namespace

ObjectMetadataClient::ObjectMetadataClient(std::shared_ptr<DataPlaneManager> manager,
                                           std::shared_ptr<DeadlineRetry> retry)
    : manager_(std::move(manager)), retry_(std::move(retry))
{
}

Status ObjectMetadataClient::InvokeQueryAndGet(const HostPort &address, master::QueryAndGetReqPb &request,
                                               master::QueryAndGetRspPb &response)
{
    RETURN_RUNTIME_ERROR_IF_NULL(manager_);
    std::shared_ptr<WorkerRpcClient> rpcClient;
    RETURN_IF_NOT_OK(manager_->GetOrCreateRpcClient(address, rpcClient));
    RETURN_RUNTIME_ERROR_IF_NULL(rpcClient);
    return rpcClient->InvokeQueryAndGet(request, response);
}

Status ObjectMetadataClient::QueryWithRetry(const HostPort &address, const ObjectMetadataBatch &items,
                                            bool allowRedirect, master::QueryAndGetRspPb &response)
{
    RETURN_RUNTIME_ERROR_IF_NULL(retry_);
    CHECK_FAIL_RETURN_STATUS(!items.empty(), K_INVALID, "Metadata query items are empty");
    int64_t backoffMs = 1;
    size_t attempt = 0;
    while (true) {
        ++attempt;
        RETURN_IF_NOT_OK(retry_->CheckDeadline());
        master::QueryAndGetReqPb request;
        for (const auto *item : items) {
            request.add_object_keys(item->objectKey);
        }
        request.set_redirect(allowRedirect);
        response.Clear();
        VLOG(1) << "[TransportGet][Metadata] Query, meta owner: " << address.ToString()
                << ", key count: " << items.size() << ", redirect: " << allowRedirect
                << ", attempt: " << attempt;
        Status rc = InvokeQueryAndGet(address, request, response);
        if (rc.IsError()) {
            if (!retry_->IsRetryableRpcError(rc)) {
                VLOG(1) << "[TransportGet][Metadata] Query failed without retry, meta owner: "
                        << address.ToString() << ", status: " << rc.ToString();
                return rc;
            }
            VLOG(1) << "[TransportGet][Metadata] Retrying query, meta owner: " << address.ToString()
                    << ", status: " << rc.ToString();
            if (rc.GetCode() == K_RPC_UNAVAILABLE && manager_ != nullptr) {
                manager_->Teardown(address);
            }
            RETURN_IF_NOT_OK(retry_->Backoff(backoffMs));
            continue;
        }
        if (!response.meta_is_moving()) {
            return Status::OK();
        }
        VLOG(1) << "[TransportGet][Metadata] Metadata is moving, meta owner: " << address.ToString()
                << ", key count: " << items.size();
        RETURN_IF_NOT_OK(retry_->Backoff(backoffMs));
    }
}

Status ObjectMetadataClient::ApplyLocations(const ObjectMetadataBatch &items,
                                            const master::QueryAndGetRspPb &response) const
{
    CHECK_FAIL_RETURN_STATUS(static_cast<size_t>(response.location_infos_size()) == items.size(),
                             K_RUNTIME_ERROR, "QueryAndGet location count does not match requested keys");
    for (size_t i = 0; i < items.size(); ++i) {
        auto &item = *items[i];
        const auto &location = response.location_infos(static_cast<int>(i));
        CHECK_FAIL_RETURN_STATUS(location.object_key() == item.objectKey, K_RUNTIME_ERROR,
                                 "QueryAndGet location order does not match requested keys");
        if (location.object_locations_size() == 0) {
            item.status = Status(K_NOT_FOUND, "Object was not found");
            continue;
        }
        Status locationStatus = Status::OK();
        for (const auto &address : location.object_locations()) {
            HostPort parsed;
            locationStatus = parsed.ParseString(address);
            if (locationStatus.IsError()) {
                break;
            }
        }
        item.status = locationStatus;
        if (locationStatus.IsOk()) {
            item.location = location;
        }
    }
    return Status::OK();
}

Status ObjectMetadataClient::ResolveRedirectBatch(const HostPort &address, const ObjectMetadataBatch &items)
{
    master::QueryAndGetRspPb response;
    RETURN_IF_NOT_OK(QueryWithRetry(address, items, false, response));
    CHECK_FAIL_RETURN_STATUS(response.info_size() == 0, K_RUNTIME_ERROR,
                             "QueryAndGet returned a second redirect");
    return ApplyLocations(items, response);
}

Status ObjectMetadataClient::QueryAndGet(const HostPort &address, const ObjectMetadataBatch &items)
{
    RETURN_IF_NOT_OK(ValidateAndResetItems(items));

    master::QueryAndGetRspPb response;
    RETURN_IF_NOT_OK(QueryWithRetry(address, items, true, response));

    ObjectMetadataBatch localItems;
    std::vector<RedirectBatch> redirectBatches;
    RETURN_IF_NOT_OK(PartitionInitialResponse(items, response, localItems, redirectBatches));
    VLOG(1) << "[TransportGet][Metadata] Query resolved, meta owner: " << address.ToString()
            << ", local keys: " << localItems.size() << ", redirect groups: " << redirectBatches.size();
    RETURN_IF_NOT_OK(ApplyLocations(localItems, response));

    for (const auto &batch : redirectBatches) {
        VLOG(1) << "[TransportGet][Metadata] Follow redirect, target: " << batch.address.ToString()
                << ", key count: " << batch.items.size();
        Status rc = ResolveRedirectBatch(batch.address, batch.items);
        if (rc.IsError()) {
            VLOG(1) << "[TransportGet][Metadata] Redirect query failed, target: " << batch.address.ToString()
                    << ", key count: " << batch.items.size() << ", status: " << rc.ToString();
            SetBatchError(batch.items, rc);
        }
    }
    return Status::OK();
}
}  // namespace client
}  // namespace datasystem
