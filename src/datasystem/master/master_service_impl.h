/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2022. All rights reserved.
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
 * Description: Implement common remote services on the master.
 */
#ifndef DATASYSTEM_MASTER_MASTER_COMMON_SERVICE_IMPL_H
#define DATASYSTEM_MASTER_MASTER_COMMON_SERVICE_IMPL_H

#include <memory>
#include <string>
#include <vector>

#include "datasystem/common/ak_sk/ak_sk_manager.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/master/node_manager.h"
#include "datasystem/protos/master_heartbeat.service.rpc.pb.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {
class EtcdClusterManager;
namespace master {
class MasterServiceImpl final : public MasterService {
public:
    /**
     * @brief Construct MasterServiceImpl.
     */
    explicit MasterServiceImpl(HostPort serverAddress, std::shared_ptr<AkSkManager> akSkManager);

    ~MasterServiceImpl() override;

    /**
     * @brief Respond to a heartbeat from a node.
     * @param[in] request The rpc request protobuf.
     * @param[out] response The rpc response protobuf.
     * @return Status of the call.
     */
    Status Heartbeat(const HeartbeatReqPb &request, HeartbeatRspPb &response) override;

    Status Init() override;

    /**
     * @brief Setter method for assigning cluster manager
     * @param[in] cm The pointer to etcd cluster manager
     */
    void SetClusterManager(EtcdClusterManager *cm)
    {
        etcdCM_ = cm;
    }

private:
    EtcdClusterManager *etcdCM_{ nullptr };
    std::shared_ptr<AkSkManager> akSkManager_{ nullptr };
};
}  // namespace master
}  // namespace datasystem
#endif  // DATASYSTEM_MASTER_MASTER_COMMON_SERVICE_IMPL_H
