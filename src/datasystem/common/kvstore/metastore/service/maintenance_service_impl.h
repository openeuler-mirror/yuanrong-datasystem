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
 * Description: Maintenance gRPC service implementation for metastore service.
 */
#ifndef DATASYSTEM_COMMON_KVSTORE_METASTORE_SERVICE_MAINTENANCE_SERVICE_IMPL_H
#define DATASYSTEM_COMMON_KVSTORE_METASTORE_SERVICE_MAINTENANCE_SERVICE_IMPL_H

#include <grpcpp/grpcpp.h>

#include "etcd/api/etcdserverpb/rpc.grpc.pb.h"
#include "datasystem/common/kvstore/metastore/manager/kv_manager.h"

namespace datasystem {

class MaintenanceServiceImpl : public etcdserverpb::Maintenance::Service {
public:
    /**
     * @brief Constructor for MaintenanceServiceImpl.
     * @param[in] kvManager Pointer to the KV manager.
     */
    explicit MaintenanceServiceImpl(KVManager *kvManager);
    ~MaintenanceServiceImpl() = default;

    /**
     * @brief Get the status of the member.
     * @param[in] context The server context.
     * @param[in] request The status request.
     * @param[out] response The status response containing member information.
     * @return Status of the operation.
     */
    ::grpc::Status Status(::grpc::ServerContext *context, const ::etcdserverpb::StatusRequest *request,
                          ::etcdserverpb::StatusResponse *response) override;

private:
    KVManager *kvManager_;
};

}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_KVSTORE_METASTORE_SERVICE_MAINTENANCE_SERVICE_IMPL_H
