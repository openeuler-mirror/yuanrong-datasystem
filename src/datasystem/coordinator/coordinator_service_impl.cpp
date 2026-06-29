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

#include "datasystem/coordinator/coordinator_service_impl.h"

#include "datasystem/common/coordinator/key_value_entry.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {
namespace coordinator {
namespace {
Status CheckCoordinatorStore(const std::shared_ptr<CoordinatorStore> &store)
{
    CHECK_FAIL_RETURN_STATUS(store != nullptr, StatusCode::K_NOT_READY, "coordinator store is not bound");
    return Status::OK();
}

void MarkLeader(ResponseHeader *header)
{
    if (header == nullptr) {
        return;
    }
    header->set_is_leader(true);
    header->clear_leader_address();
}

void FillKeyValuePb(const KeyValueEntry &entry, KeyValue *kv)
{
    kv->set_key(entry.key);
    kv->set_value(entry.value);
    kv->set_version(entry.version);
    kv->set_mod_revision(entry.modRevision);
}
}  // namespace

Status CoordinatorServiceImpl::Put(const PutReqPb &req, PutRspPb &rsp)
{
    RETURN_IF_NOT_OK(CheckCoordinatorStore(store_));

    int64_t version = 0;
    int64_t revision = 0;
    RETURN_IF_NOT_OK(store_->Put(req.key(), req.value(), req.ttl(), req.expected_version(), version, revision));
    MarkLeader(rsp.mutable_header());
    rsp.set_version(version);
    rsp.set_revision(revision);
    return Status::OK();
}

Status CoordinatorServiceImpl::Range(const RangeReqPb &req, RangeRspPb &rsp)
{
    RETURN_IF_NOT_OK(CheckCoordinatorStore(store_));

    std::vector<KeyValueEntry> kvs;
    int64_t revision = 0;
    RETURN_IF_NOT_OK(store_->Range(req.key(), req.range_end(), kvs, revision));
    MarkLeader(rsp.mutable_header());
    rsp.set_revision(revision);
    for (const auto &entry : kvs) {
        FillKeyValuePb(entry, rsp.add_kvs());
    }
    return Status::OK();
}

Status CoordinatorServiceImpl::DeleteRange(const DeleteRangeReqPb &req, DeleteRangeRspPb &rsp)
{
    RETURN_IF_NOT_OK(CheckCoordinatorStore(store_));

    int64_t deleted = 0;
    int64_t revision = 0;
    RETURN_IF_NOT_OK(store_->DeleteRange(req.key(), req.range_end(), deleted, revision));
    MarkLeader(rsp.mutable_header());
    rsp.set_deleted(deleted);
    rsp.set_revision(revision);
    return Status::OK();
}

Status CoordinatorServiceImpl::WatchRange(const WatchRangeReqPb &req, WatchRangeRspPb &rsp)
{
    RETURN_IF_NOT_OK(CheckCoordinatorStore(store_));

    int64_t watchId = 0;
    std::vector<KeyValueEntry> initialKvs;
    RETURN_IF_NOT_OK(store_->WatchRange(req.key(), req.range_end(), req.watcher_addr(), watchId, initialKvs));
    MarkLeader(rsp.mutable_header());
    rsp.set_watch_id(watchId);
    for (const auto &entry : initialKvs) {
        FillKeyValuePb(entry, rsp.add_initial_kvs());
    }
    return Status::OK();
}

Status CoordinatorServiceImpl::CancelWatch(const CancelWatchReqPb &req, CancelWatchRspPb &rsp)
{
    RETURN_IF_NOT_OK(CheckCoordinatorStore(store_));

    std::vector<int64_t> watchIds(req.watch_ids().begin(), req.watch_ids().end());
    RETURN_IF_NOT_OK(store_->CancelWatch(req.watcher_addr(), watchIds));
    MarkLeader(rsp.mutable_header());
    return Status::OK();
}

Status CoordinatorServiceImpl::KeepAlive(const KeepAliveReqPb &req, KeepAliveRspPb &rsp)
{
    RETURN_IF_NOT_OK(CheckCoordinatorStore(store_));

    int64_t ttlMs = 0;
    int64_t remainingTtlMs = 0;
    RETURN_IF_NOT_OK(store_->KeepAlive(req.key(), ttlMs, remainingTtlMs));
    MarkLeader(rsp.mutable_header());
    rsp.set_ttl(ttlMs);
    rsp.set_remaining_ttl(remainingTtlMs);
    return Status::OK();
}
}  // namespace coordinator
}  // namespace datasystem
