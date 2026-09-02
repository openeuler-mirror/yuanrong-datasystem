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
 * Description: Fast migrate transport implementation.
 */
#include "datasystem/common/perf/perf_manager.h"
#include "datasystem/worker/object_cache/data_migrator/transport/fast_migrate_transport.h"

#include <algorithm>
#include <cmath>

#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/rdma/rdma_util.h"
#include "datasystem/common/util/request_context.h"
#include "datasystem/common/util/rpc_util.h"

namespace datasystem {
namespace object_cache {
void FastMigrateTransport::ProcessMigrateResponse(const MigrateDataDirectReqPb &reqPb,
                                                  const MigrateDataDirectRspPb &rspPb, const Request &req,
                                                  Response &rsp)
{
    rsp.remainBytes = rspPb.remain_bytes();
    rsp.limitRate = rspPb.limit_rate();
    rsp.failedKeys.insert(rspPb.failed_object_keys().begin(), rspPb.failed_object_keys().end());
    rsp.skipKeys.insert(rspPb.skipped_object_keys().begin(), rspPb.skipped_object_keys().end());
    for (const auto &obj : reqPb.objects()) {
        const auto &key = obj.object_key();
        if (rsp.failedKeys.find(key) == rsp.failedKeys.end()
            && rsp.skipKeys.find(key) == rsp.skipKeys.end()) {
            (void)rsp.successKeys.emplace(key);
        }
    }
    if (req.progress != nullptr) {
        req.progress->Deal(rsp.successKeys.size());
    }
    if (rspPb.has_provider_ub_failure_detail()) {
        rsp.ubFailureDetail = rspPb.provider_ub_failure_detail();
    }
    LOG_IF(WARNING, !rspPb.failed_object_keys().empty()) << FormatString(
        "[Migrate Data] Send %ld objects[%ld bytes] to %s and %ld objects [%s] failed", req.datas->size(),
        req.batchSize, req.api->Address(), rspPb.failed_object_keys_size(), VectorToString(rspPb.failed_object_keys()));
}

void FastMigrateTransport::FillRequestHeader(const Request &req, MigrateDataDirectReqPb &reqPb) const
{
    reqPb.set_worker_addr(req.localAddr);
    reqPb.set_is_slot_migration(req.isSlotMigration);
    reqPb.set_slot_id(req.slotId);
    reqPb.set_is_retry(req.isRetry);
    if (req.rebalancePolicyFence != nullptr) {
        reqPb.set_has_rebalance_policy_fence(true);
        reqPb.set_target_eviction_policy(req.rebalancePolicyFence->targetPolicy);
        reqPb.set_target_eviction_policy_epoch(req.rebalancePolicyFence->targetEpoch);
        reqPb.set_rebalance_task_id(req.rebalancePolicyFence->taskId);
    }
}

Status FastMigrateTransport::AppendObject(const HostPort &localAddress, BaseDataUnit &data, Response &rsp,
                                          MigrateDataDirectReqPb &reqPb, uint64_t &totalDataBytes) const
{
    Status rc = data.LockData();
    if (rc.IsError()) {
        LOG(ERROR) << FormatString("[Migrate Data] Lock object %s failed, it will not be sent!", data.Id());
        (void)rsp.failedKeys.emplace(data.Id());
        return Status::OK();
    }
    if (data.Data() == nullptr) {
        LOG(ERROR) << FormatString("[Migrate Data] Data pointer of object %s is null, it will not be sent!", data.Id());
        (void)rsp.failedKeys.emplace(data.Id());
        return Status::OK();
    }
    auto *objInfo = reqPb.add_objects();
    objInfo->set_object_key(data.Id());
    objInfo->set_version(data.Version());
    objInfo->set_data_size(data.Size());
    totalDataBytes += data.Size();
    return FillRequestUrmaInfo(localAddress, data.Data(), data.Offset(), data.MetaSize(), *objInfo);
}

Status FastMigrateTransport::BuildRequest(const Request &req, Response &rsp, MigrateDataDirectReqPb &reqPb,
                                          uint64_t &totalDataBytes) const
{
    HostPort localAddress;
    RETURN_IF_NOT_OK(localAddress.ParseString(req.localAddr));
    FillRequestHeader(req, reqPb);
    for (const auto &data : *req.datas) {
        RETURN_IF_NOT_OK(AppendObject(localAddress, *data, rsp, reqPb, totalDataBytes));
    }
    return Status::OK();
}

Status FastMigrateTransport::MigrateDataToRemote(const Request &req, Response &rsp)
{
    INJECT_POINT("FastMigrateTransport.MigrateDataToRemote.delay");
    PerfPoint point(PerfKey::WORKER_MIGRATE_DIRECT_REQ_BUILD);
    MigrateDataDirectReqPb reqPb;
    uint64_t totalDataBytes = 0;
    RETURN_IF_NOT_OK(BuildRequest(req, rsp, reqPb, totalDataBytes));

    // Compute rpc timeout from total bytes and assumed bandwidth (10GB/s), capped at 180s.
    int64_t migrateDirectTimeoutMs = CalcMigrateDataDirectTimeoutMs(totalDataBytes);
    VLOG(1) << FormatString("[Migrate Data] MigrateDataToRemote total data size %lu bytes, calculated timeout %ld ms",
                            totalDataBytes, migrateDirectTimeoutMs);

    // 2. Migrate data (single attempt; the outer migration loop handles retry).
    point.RecordAndReset(PerfKey::WORKER_MIGRATE_DIRECT_RPC);
    MigrateDataDirectRspPb rspPb;
    rspPb.Clear();
    GetRequestContext()->reqTimeoutDuration.InitWithPositiveTime(migrateDirectTimeoutMs);
    Status rc = req.api->MigrateDataDirect(reqPb, rspPb);
    point.RecordAndReset(PerfKey::WORKER_MIGRATE_DIRECT_RSP_PROCESS);
    if (rspPb.has_provider_ub_failure_detail()) {
        rsp.ubFailureDetail = rspPb.provider_ub_failure_detail();
    }
    if (rc.IsOk()) {
        ProcessMigrateResponse(reqPb, rspPb, req, rsp);
    }
    return rc;
}

}  // namespace object_cache
}  // namespace datasystem
