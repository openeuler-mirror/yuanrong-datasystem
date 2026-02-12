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
 * Description: Lease gRPC service implementation for metastore service.
 */
#ifndef DATASYSTEM_COMMON_KVSTORE_METASTORE_SERVICE_LEASE_SERVICE_IMPL_H
#define DATASYSTEM_COMMON_KVSTORE_METASTORE_SERVICE_LEASE_SERVICE_IMPL_H

#include <grpcpp/grpcpp.h>
#include <memory>

#include "etcd/api/etcdserverpb/rpc.grpc.pb.h"
#include "datasystem/common/kvstore/metastore/manager/lease_manager.h"
#include "datasystem/common/kvstore/metastore/manager/kv_manager.h"

namespace datasystem {

class LeaseServiceImpl : public etcdserverpb::Lease::Service {
public:
    /**
     * @brief Constructor for LeaseServiceImpl.
     * @param[in] leaseManager Pointer to the lease manager.
     * @param[in] kvManager Pointer to the KV manager.
     */
    explicit LeaseServiceImpl(LeaseManager *leaseManager, KVManager *kvManager);
    ~LeaseServiceImpl() = default;

    /**
     * @brief Grant a lease with specified TTL.
     * @param[in] context The server context.
     * @param[in] request The lease grant request containing TTL.
     * @param[out] response The lease grant response containing lease ID.
     * @return Status of the operation.
     */
    ::grpc::Status LeaseGrant(::grpc::ServerContext *context, const ::etcdserverpb::LeaseGrantRequest *request,
                              ::etcdserverpb::LeaseGrantResponse *response) override;

    /**
     * @brief Keep a lease alive by sending periodic keep-alive requests.
     * @param[in] context The server context.
     * @param[in] stream The bidirectional stream for keep-alive requests and responses.
     * @return Status of the operation.
     */
    ::grpc::Status LeaseKeepAlive(::grpc::ServerContext *context,
                                  ::grpc::ServerReaderWriter<::etcdserverpb::LeaseKeepAliveResponse,
                                                             ::etcdserverpb::LeaseKeepAliveRequest> *stream) override;

private:
    LeaseManager *leaseManager_;
    KVManager *kvManager_;

    /**
     * @brief Fill the response header with cluster ID and member ID.
     * @param[out] header The response header to fill.
     */
    void FillResponseHeader(::etcdserverpb::ResponseHeader *header);
};

}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_KVSTORE_METASTORE_SERVICE_LEASE_SERVICE_IMPL_H
