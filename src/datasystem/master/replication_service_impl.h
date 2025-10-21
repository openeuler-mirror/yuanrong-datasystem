/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2024. All rights reserved.
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
 * Description: Defines the replica service processing main class.
 */

#include <memory>
#include "datasystem/common/kvstore/rocksdb/replica.h"
#include "datasystem/protos/worker_object.service.rpc.pb.h"
#include "datasystem/master/replica_manager.h"

namespace datasystem {
class ReplicationServiceImpl : public ReplicationService {
public:
    ReplicationServiceImpl(HostPort serverAddress, ReplicaManager *replicaManager,
                           std::shared_ptr<AkSkManager> akSkManager)
        : ReplicationService(serverAddress), replicaManager_(replicaManager), akSkManager_(akSkManager)
    {
    }

    ~ReplicationServiceImpl() = default;

    Status Init() override;

    /**
     * @brief Try incremental synchronization.
     * @param[in] req The rpc request protobuf.
     * @param[out] rsp The rpc response protobuf.
     * @return Status of the call.
     */
    Status TryPSync(const TryPSyncReqPb &req, TryPSyncRspPb &rsp) override;

    /**
     * @brief Push new logs.
     * @param[in] req The rpc request protobuf.
     * @param[out] rsp The rpc response protobuf.
     * @param[in] payloads Payloads.
     * @return Status of the call.
     */
    Status PushNewLogs(const PushNewLogsReqPb &req, PushNewLogsRspPb &rsp, std::vector<RpcMessage> payloads) override;

    /**
     * @brief Fetch log file meta.
     * @param[in] req The rpc request protobuf.
     * @param[out] rsp The rpc response protobuf.
     * @return Status of the call.
     */
    Status FetchMeta(const FetchMetaReqPb &req, FetchMetaRspPb &rsp) override;

    /**
     * @brief Fetch log file.
     * @param[in] req The rpc request protobuf.
     * @param[out] rsp The rpc response protobuf.
     * @param[out] payloads Payloads.
     * @return Status of the call.
     */
    Status FetchFile(const FetchFileReqPb &req, FetchFileRspPb &rsp, std::vector<RpcMessage> &payloads) override;

private:
    ReplicaManager *replicaManager_;
    std::shared_ptr<AkSkManager> akSkManager_;
};
}  // namespace datasystem