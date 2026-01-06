/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
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
 * Description: Scale down migrate transport implementation.
 */
#include "datasystem/worker/object_cache/data_migrator/transport/tcp_migrate_transport.h"

#include "datasystem/common/util/rpc_util.h"

namespace datasystem {
namespace object_cache {

Status TcpMigrateTransport::MigrateDataToRemote(const Request &req, Response &rsp)
{
    // 1. Construct request.
    MigrateDataReqPb reqPb;
    reqPb.set_worker_addr(req.localAddr);
    reqPb.set_bytes_send(req.batchSize);
    reqPb.set_type(req.type);
    std::vector<MemView> payloads;
    uint32_t currPartIndex = 0;
    for (const auto &data : *req.datas) {
        // If it is the shm data, we need to lock it first.
        Status s = data->LockData();
        if (s.IsError()) {
            LOG(ERROR) << FormatString("[Migrate Data] Lock object %s failed, it will not be sent!", data->Id());
            (void)rsp.failedKeys.emplace(data->Id());
            continue;
        }

        auto *objInfo = reqPb.add_objects();
        objInfo->set_object_key(data->Id());
        objInfo->set_version(data->Version());
        objInfo->set_data_size(data->Size());
        auto memViews = data->GetMemViews();
        for (uint32_t i = currPartIndex; i < currPartIndex + memViews.size(); ++i) {
            objInfo->add_part_index(i);
        }
        currPartIndex += memViews.size();
        (void)payloads.insert(payloads.end(), std::make_move_iterator(memViews.begin()),
                              std::make_move_iterator(memViews.end()));
    }

    // 2. Migrate data with retry.
    MigrateDataRspPb rspPb;
    Status rc = RetryOnRPCErrorByCount(maxRetryCount_,
                                       [&]() {
                                           rspPb.Clear();
                                           return req.api->MigrateData(reqPb, payloads, rspPb);
                                       },
                                       {});
    if (rc.IsOk()) {
        rsp.remainBytes = rspPb.remain_bytes();
        rsp.successKeys.insert(rspPb.success_ids().begin(), rspPb.success_ids().end());
        rsp.failedKeys.insert(rspPb.fail_ids().begin(), rspPb.fail_ids().end());
        rsp.limitRate = rspPb.limit_rate();
        if (req.progress != nullptr) {
            req.progress->Deal(rspPb.success_ids().size());
        }
        LOG_IF(WARNING, !rspPb.fail_ids().empty()) << FormatString(
            "[Migrate Data] Send %ld objects[%ld bytes] to %s and %ld objects [%s] failed", req.datas->size(),
            req.batchSize, req.api->Address(), rspPb.fail_ids_size(), VectorToString(rspPb.fail_ids()));
    }
    return rc;
}
}  // namespace object_cache
}  // namespace datasystem
