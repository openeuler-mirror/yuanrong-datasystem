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
#include "datasystem/worker/object_cache/data_migrator/transport/fast_migrate_transport.h"

#include <algorithm>
#include <cmath>

#include "datasystem/common/rdma/rdma_util.h"
#include "datasystem/common/util/rpc_util.h"

namespace datasystem {
namespace object_cache {
void FastMigrateTransport::ProcessMigrateResponse(const MigrateDataDirectReqPb &reqPb,
                                                  const MigrateDataDirectRspPb &rspPb, const Request &req,
                                                  Response &rsp)
{
    rsp.remainBytes = rspPb.remain_bytes();
    rsp.failedKeys.insert(rspPb.failed_object_keys().begin(), rspPb.failed_object_keys().end());
    for (const auto &obj : reqPb.objects()) {
        const auto &key = obj.object_key();
        if (rsp.failedKeys.find(key) == rsp.failedKeys.end()) {
            (void)rsp.successKeys.emplace(key);
        }
    }
    if (req.progress != nullptr) {
        req.progress->Deal(rsp.successKeys.size());
    }
    LOG_IF(WARNING, !rspPb.failed_object_keys().empty()) << FormatString(
        "[Migrate Data] Send %ld objects[%ld bytes] to %s and %ld objects [%s] failed", req.datas->size(),
        req.batchSize, req.api->Address(), rspPb.failed_object_keys_size(), VectorToString(rspPb.failed_object_keys()));
}

Status FastMigrateTransport::MigrateDataToRemote(const Request &req, Response &rsp)
{
    PerfPoint point(PerfKey::WORKER_MIGRATE_DIRECT_REQ_BUILD);
    HostPort localAddress;
    RETURN_IF_NOT_OK(localAddress.ParseString(req.localAddr));
    // 1. Construct request.
    MigrateDataDirectReqPb reqPb;
    reqPb.set_worker_addr(req.localAddr);
    reqPb.set_is_slot_migration(req.isSlotMigration);
    reqPb.set_slot_id(req.slotId);
    reqPb.set_is_retry(req.isRetry);
    uint64_t totalDataBytes = 0;
    for (const auto &data : *req.datas) {
        Status s = data->LockData();
        if (s.IsError()) {
            LOG(ERROR) << FormatString("[Migrate Data] Lock object %s failed, it will not be sent!", data->Id());
            (void)rsp.failedKeys.emplace(data->Id());
            continue;
        }
        if (data->Data() == nullptr) {
            LOG(ERROR) << FormatString("[Migrate Data] Data pointer of object %s is null, it will not be sent!",
                                       data->Id());
            (void)rsp.failedKeys.emplace(data->Id());
            continue;
        }

        auto *objInfo = reqPb.add_objects();
        objInfo->set_object_key(data->Id());
        objInfo->set_version(data->Version());
        objInfo->set_data_size(data->Size());
        totalDataBytes += data->Size();
        RETURN_IF_NOT_OK(FillRequestUrmaInfo(localAddress, data->Data(), data->Offset(), data->MetaSize(), *objInfo));
    }

    // Compute rpc timeout from total bytes and assumed bandwidth (10GB/s), capped at 180s.
    int64_t migrateDirectTimeoutMs = CalcMigrateDataDirectTimeoutMs(totalDataBytes);
    VLOG(1) << FormatString("[Migrate Data] MigrateDataToRemote total data size %lu bytes, calculated timeout %ld ms",
                            totalDataBytes, migrateDirectTimeoutMs);

    // 2. Migrate data with retry.
    point.RecordAndReset(PerfKey::WORKER_MIGRATE_DIRECT_RPC);
    MigrateDataDirectRspPb rspPb;
    Status rc = RetryOnRPCErrorByCount(maxRetryCount_,
                                       [&]() {
                                           rspPb.Clear();
                                           reqTimeoutDuration.InitWithPositiveTime(migrateDirectTimeoutMs);
                                           return req.api->MigrateDataDirect(reqPb, rspPb);
                                       },
                                       {});
    point.RecordAndReset(PerfKey::WORKER_MIGRATE_DIRECT_RSP_PROCESS);
    if (rc.IsOk()) {
        ProcessMigrateResponse(reqPb, rspPb, req, rsp);
    }
    return rc;
}

}  // namespace object_cache
}  // namespace datasystem
