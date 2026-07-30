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

/** Description: Implements grouped metadata lookup and batched replica reads. */

#include "datasystem/client/transport/object_read/object_read_flow.h"

#include <algorithm>
#include <chrono>
#include <future>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "datasystem/common/log/log.h"
#include "datasystem/common/log/trace.h"
#include "datasystem/common/rpc/api_deadline.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/thread_pool.h"

namespace datasystem {
namespace client {
namespace {
struct ReadItem {
    HostPort metaOwner;
    ObjectMetadataItem metadata;
    ObjectReadItemResult result;
};

struct MetadataGroup {
    explicit MetadataGroup(const HostPort &owner) : address(owner)
    {
    }

    void Resolve(ObjectMetadataClient &metadata, const Status &dispatchStatus, bool metadataOnly)
    {
        Status status = dispatchStatus;
        if (status.IsOk()) {
            status = metadataOnly ? metadata.QueryMetadata(address, items) : metadata.QueryAndGet(address, items);
        }
        if (status.IsError()) {
            for (auto *item : items) {
                item->status = status;
            }
            LOG(ERROR) << "[TransportGet][Metadata] Metadata group failed, meta owner: " << address.ToString()
                       << ", key count: " << items.size() << ", metadata only: " << metadataOnly
                       << ", status: " << status.ToString();
        }
    }

    HostPort address;
    ObjectMetadataBatch items;
};

AccessTransportKind MergeTransportKind(AccessTransportKind lhs, AccessTransportKind rhs)
{
    return static_cast<AccessTransportKind>(
        std::max(static_cast<uint8_t>(lhs), static_cast<uint8_t>(rhs)));
}

std::vector<MetadataGroup> GroupByMetaOwner(std::vector<ReadItem> &items)
{
    std::vector<MetadataGroup> groups;
    std::unordered_map<HostPort, MetadataGroup *> groupsByOwner;
    // Keep pointers stored in groupsByOwner stable while groups are appended.
    groups.reserve(items.size());
    groupsByOwner.reserve(items.size());
    for (auto &item : items) {
        auto inserted = groupsByOwner.emplace(item.metaOwner, nullptr);
        if (inserted.second) {
            groups.emplace_back(item.metaOwner);
            inserted.first->second = &groups.back();
        }
        inserted.first->second->items.emplace_back(&item.metadata);
    }
    return groups;
}

template <typename Item, typename Task>
void RunTasks(ThreadPool &taskPool, const std::vector<Item *> &items, const Task &task)
{
    if (items.empty()) {
        return;
    }
    if (items.size() == 1) {
        task(*items.front(), Status::OK());
        return;
    }

    const int64_t remainingUs = ApiDeadline::Instance().ApiRemainingUs();
    if (remainingUs <= 0) {
        const Status status(K_RPC_DEADLINE_EXCEEDED, "API deadline exceeded before task dispatch");
        for (auto *item : items) {
            task(*item, status);
        }
        return;
    }
    const auto traceContext = Trace::Instance().GetContext();
    const auto dispatchTime = std::chrono::steady_clock::now();
    std::vector<std::future<void>> futures;
    futures.reserve(items.size());
    for (auto *item : items) {
        futures.emplace_back(taskPool.Submit([task, item, traceContext, remainingUs, dispatchTime]() {
            TraceGuard traceGuard = Trace::Instance().SetTraceContext(traceContext);
            task(*item, InitTimeoutsFromDispatch(remainingUs, dispatchTime));
        }));
    }
    for (auto &future : futures) {
        future.get();
    }
}

Status InitializeItems(const ObjectReadRequest &request, std::vector<ReadItem> &items)
{
    CHECK_FAIL_RETURN_STATUS(!request.items.empty(), K_INVALID, "Object read items are empty");

    std::unordered_set<size_t> requestIndexes;
    items.reserve(request.items.size());
    for (const auto &input : request.items) {
        CHECK_FAIL_RETURN_STATUS(!input.objectKey.empty(), K_INVALID, "Object key is empty");
        CHECK_FAIL_RETURN_STATUS(!input.metaOwner.Empty(), K_INVALID, "Metadata owner is empty");
        CHECK_FAIL_RETURN_STATUS(requestIndexes.insert(input.requestIndex).second, K_INVALID,
                                 "Object read request index is duplicated");
        items.push_back({ input.metaOwner, { input.objectKey },
                          { input.requestIndex, input.objectKey } });
    }
    return Status::OK();
}

void QueryMetadata(ObjectMetadataClient &metadata, ThreadPool &taskPool, std::vector<ReadItem> &items,
                   bool metadataOnly = false)
{
    auto groups = GroupByMetaOwner(items);
    VLOG(1) << "[TransportGet][Flow] Query metadata, key count: " << items.size()
            << ", owner count: " << groups.size() << ", parallel: " << (groups.size() > 1)
            << ", metadata only: " << metadataOnly
            << ", remaining deadline us: " << ApiDeadline::Instance().ApiRemainingUs();
    std::vector<MetadataGroup *> tasks;
    tasks.reserve(groups.size());
    for (auto &group : groups) {
        tasks.emplace_back(&group);
    }
    RunTasks(taskPool, tasks, [&metadata, metadataOnly](MetadataGroup &group, const Status &dispatchStatus) {
        group.Resolve(metadata, dispatchStatus, metadataOnly);
    });
    const auto resolved = std::count_if(items.begin(), items.end(), [](const ReadItem &item) {
        return item.metadata.status.IsOk();
    });
    VLOG(1) << "[TransportGet][Flow] Metadata completed, resolved: " << resolved
            << ", failed: " << items.size() - resolved;
}

void PropagateUnassignedBatchError(const ReplicaReadBatch &requests, const Status &status)
{
    if (status.IsOk()) {
        return;
    }
    for (const auto &request : requests) {
        if (request.result->status.GetCode() == K_NOT_READY) {
            request.result->status = status;
        }
    }
}

void ReadObjects(ReplicaReader &replicas, std::vector<ReadItem> &items,
                 const std::shared_ptr<const TransportReadContext> &context)
{
    ReplicaReadBatch ready;
    ready.reserve(items.size());
    for (auto &item : items) {
        if (item.metadata.status.IsError()) {
            item.result.status = item.metadata.status;
        } else if (item.metadata.inlineData.has_value()) {
            item.result.objectKey = item.metadata.objectKey;
            item.result.data = std::move(*item.metadata.inlineData);
            item.result.status = Status::OK();
        } else {
            ready.push_back({ &item.metadata.location, &item.result, context });
        }
    }
    VLOG(1) << "[TransportGet][Flow] Read data, key count: " << ready.size()
            << ", inline or failed: " << items.size() - ready.size() << ", batch: " << (ready.size() > 1)
            << ", remaining deadline us: " << ApiDeadline::Instance().ApiRemainingUs();
    Status readStatus = Status::OK();
    if (ready.size() == 1) {
        readStatus = replicas.Read(*ready.front().location, *ready.front().result, ready.front().context);
        ready.front().result->status = readStatus;
    } else if (ready.size() > 1) {
        readStatus = replicas.ReadBatch(ready);
        PropagateUnassignedBatchError(ready, readStatus);
    }
    const auto succeeded = std::count_if(items.begin(), items.end(), [](const ReadItem &item) {
        return item.result.status.IsOk();
    });
    VLOG(1) << "[TransportGet][Flow] Data read completed, succeeded: " << succeeded
            << ", failed: " << items.size() - succeeded;
}

Status BuildResult(std::vector<ReadItem> &items, ObjectReadResult &result)
{
    result.items.reserve(items.size());
    for (auto &item : items) {
        result.items.emplace_back(std::move(item.result));
    }

    bool hasSuccess = false;
    for (const auto &item : result.items) {
        if (item.status.IsOk()) {
            hasSuccess = true;
            result.actualKind = MergeTransportKind(result.actualKind, item.data.kind);
        }
    }
    if (hasSuccess) {
        return Status::OK();
    }
    for (const auto &item : result.items) {
        if (item.status.IsError()) {
            return item.status;
        }
    }
    return Status(K_NOT_FOUND, "Cannot get objects from worker");
}
}  // namespace

ObjectReadFlow::ObjectReadFlow(std::shared_ptr<ObjectMetadataClient> metadata,
                               std::shared_ptr<ReplicaReader> replicas, std::shared_ptr<ThreadPool> taskPool)
    : metadata_(std::move(metadata)), replicas_(std::move(replicas)), taskPool_(std::move(taskPool))
{
}

Status ObjectReadFlow::ResolveMetadata(const ObjectReadRequest &request,
                                       std::vector<ObjectMetadataItem> &metadata)
{
    RETURN_RUNTIME_ERROR_IF_NULL(metadata_);
    RETURN_RUNTIME_ERROR_IF_NULL(taskPool_);
    std::vector<ReadItem> items;
    RETURN_IF_NOT_OK(InitializeItems(request, items));
    QueryMetadata(*metadata_, *taskPool_, items, true);
    size_t resultSize = 0;
    for (const auto &input : request.items) {
        resultSize = std::max(resultSize, input.requestIndex + 1);
    }
    metadata.clear();
    metadata.resize(resultSize);
    for (auto &item : items) {
        metadata[item.result.requestIndex] = std::move(item.metadata);
    }
    return Status::OK();
}

Status ObjectReadFlow::Run(const ObjectReadRequest &request, ObjectReadResult &result)
{
    result.Clear();
    RETURN_RUNTIME_ERROR_IF_NULL(metadata_);
    RETURN_RUNTIME_ERROR_IF_NULL(replicas_);
    RETURN_RUNTIME_ERROR_IF_NULL(taskPool_);
    CHECK_FAIL_RETURN_STATUS(request.context != nullptr, K_INVALID, "Transport read context is missing");

    std::vector<ReadItem> items;
    RETURN_IF_NOT_OK(InitializeItems(request, items));
    AddLatencyTickIfEnabled(request.traceEnabled, LatencyTickKey::CLIENT_DIRECT_QUERY_AND_GET_START);
    QueryMetadata(*metadata_, *taskPool_, items);
    AddLatencyTickIfEnabled(request.traceEnabled, LatencyTickKey::CLIENT_DIRECT_QUERY_AND_GET_END);
    AddLatencyTickIfEnabled(request.traceEnabled, LatencyTickKey::CLIENT_DIRECT_GET_DATA_START);
    ReadObjects(*replicas_, items, request.context);
    AddLatencyTickIfEnabled(request.traceEnabled, LatencyTickKey::CLIENT_DIRECT_GET_DATA_END);
    return BuildResult(items, result);
}
}  // namespace client
}  // namespace datasystem
