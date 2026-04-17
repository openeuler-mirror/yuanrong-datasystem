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
#include "datasystem/worker/object_cache/data_migrator/transport/fast_migrate_transport2.h"

#include <algorithm>
#include <cmath>

#include "datasystem/common/util/rpc_util.h"

namespace datasystem {
namespace object_cache {
Status FastMigrateTransport2::MigrateDataToRemote(const Request &req, Response &rsp)
{
    NotifyRemoteGetReqPb reqPb;
    reqPb.set_addr(req.localAddr);
    uint64_t totalDataBytes = 0;
    for (const auto &data : *req.datas) {
        Status s = data->LockData();
        if (s.IsError()) {
            LOG(WARNING) << FormatString("[Migrate Data] Lock object %s failed, it will not be sent!", data->Id());
            (void)rsp.failedKeys.emplace(data->Id());
            continue;
        }
        if (data->Data() == nullptr) {
            LOG(INFO) << FormatString("[Migrate Data] Data pointer of object %s is null, it will not be sent!",
                                      data->Id());
            (void)rsp.failedKeys.emplace(data->Id());
            continue;
        }
        reqPb.add_object_keys(data->Id());
        reqPb.add_versions(data->Version());
        totalDataBytes += data->Size();
    }

    int64_t migrateDirectTimeoutMs = CalcMigrateDataDirectTimeoutMs(totalDataBytes);
    VLOG(1) << FormatString("[Migrate Data] MigrateDataToRemote total data size %lu bytes, calculated timeout %ld ms",
                            totalDataBytes, migrateDirectTimeoutMs);
    NotifyRemoteGetRspPb rspPb;
    Status rc = RetryOnRPCErrorByCount(maxRetryCount_,
                                       [&]() {
                                           rspPb.Clear();
                                           reqTimeoutDuration.InitWithPositiveTime(migrateDirectTimeoutMs);
                                           return req.api->NotifyRemoteGet(reqPb, rspPb);
                                       },
                                       {});
    if (rc.IsOk()) {
        rsp.remainBytes = rspPb.remain_bytes();
        rsp.failedKeys.insert(rspPb.failed_object_keys().begin(), rspPb.failed_object_keys().end());
        for (const auto &key : reqPb.object_keys()) {
            if (rsp.failedKeys.find(key) == rsp.failedKeys.end()) {
                (void)rsp.successKeys.emplace(key);
            }
        }
        if (req.progress != nullptr) {
            req.progress->Deal(rsp.successKeys.size());
        }
        LOG_IF(WARNING, !rspPb.failed_object_keys().empty())
            << FormatString("[Migrate Data] Send %ld objects[%ld bytes] to %s and %ld objects [%s] failed, remain_bytes: %lu",
                            req.datas->size(), req.batchSize, req.api->Address(), rspPb.failed_object_keys_size(),
                            VectorToString(rspPb.failed_object_keys()), rsp.remainBytes);
    } else {
        rsp.remainBytes = 0;
    }
    return rc;
}
}  // namespace object_cache
}  // namespace datasystem
