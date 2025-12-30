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

Status TcpMigrateTransport::MigrateDataToRemote(const std::shared_ptr<WorkerRemoteWorkerOCApi> &api,
                                                const std::vector<std::unique_ptr<BaseDataUnit>> &datas,
                                                const std::string &localAddr, const uint64_t &batchSize,
                                                std::shared_ptr<MigrateProgress> progress, uint64_t &remainBytes,
                                                std::unordered_set<ImmutableString> &successKeys,
                                                std::unordered_set<ImmutableString> &failedKeys, uint64_t &limitRate)
{
    // 1. Construct request.
    MigrateDataReqPb req;
    req.set_worker_addr(localAddr);
    req.set_bytes_send(batchSize);
    std::vector<MemView> payloads;
    uint32_t currPartIndex = 0;
    for (const auto &data : datas) {
        // If it is the shm data, we need to lock it first.
        Status s = data->LockData();
        if (s.IsError()) {
            LOG(ERROR) << FormatString("[Migrate Data] Lock object %s failed, it will not be sent!", data->Id());
            (void)failedKeys.emplace(data->Id());
            continue;
        }

        auto *objInfo = req.add_objects();
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
    MigrateDataRspPb rsp;
    Status rc = RetryOnRPCErrorByCount(maxRetryCount_,
                                       [&]() {
                                           rsp.Clear();
                                           return api->MigrateData(req, payloads, rsp);
                                       },
                                       {});
    if (rc.IsOk()) {
        remainBytes = rsp.remain_bytes();
        (void)successKeys.insert(rsp.success_ids().begin(), rsp.success_ids().end());
        (void)failedKeys.insert(rsp.fail_ids().begin(), rsp.fail_ids().end());
        limitRate = rsp.limit_rate();
        if (progress != nullptr) {
            progress->Deal(rsp.success_ids().size());
        }
        LOG_IF(WARNING, !rsp.fail_ids().empty()) << FormatString(
            "[Migrate Data] Send %ld objects[%ld bytes] to %s and %ld objects [%s] failed", datas.size(), batchSize,
            api->Address(), rsp.fail_ids_size(), VectorToString(rsp.fail_ids()));
    }
    return rc;
}
}  // namespace object_cache
}  // namespace datasystem
