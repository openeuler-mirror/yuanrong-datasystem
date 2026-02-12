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
 * Description: KV gRPC service implementation for metastore service.
 */
#ifndef DATASYSTEM_COMMON_KVSTORE_METASTORE_SERVICE_KV_SERVICE_IMPL_H
#define DATASYSTEM_COMMON_KVSTORE_METASTORE_SERVICE_KV_SERVICE_IMPL_H

#include <grpcpp/grpcpp.h>
#include <memory>

#include "etcd/api/etcdserverpb/rpc.grpc.pb.h"
#include "datasystem/common/kvstore/metastore/manager/kv_manager.h"
#include "datasystem/common/kvstore/metastore/manager/lease_manager.h"
#include "datasystem/common/kvstore/metastore/manager/watch_manager.h"

namespace datasystem {

class KVServiceImpl : public etcdserverpb::KV::Service {
public:
    /**
     * @brief Constructor for KVServiceImpl.
     * @param[in] kvManager Pointer to the KV manager.
     * @param[in] leaseManager Pointer to the lease manager.
     */
    explicit KVServiceImpl(KVManager *kvManager, LeaseManager *leaseManager);
    ~KVServiceImpl() = default;

    /**
     * @brief Put a key-value pair into the store.
     * @param[in] context The server context.
     * @param[in] request The put request containing key, value, and lease info.
     * @param[out] response The put response containing the previous KV if requested.
     * @return Status of the operation.
     */
    ::grpc::Status Put(::grpc::ServerContext *context, const ::etcdserverpb::PutRequest *request,
                       ::etcdserverpb::PutResponse *response) override;

    /**
     * @brief Get key-value pairs from the store by range.
     * @param[in] context The server context.
     * @param[in] request The range request containing key range and options.
     * @param[out] response The range response containing key-value pairs.
     * @return Status of the operation.
     */
    ::grpc::Status Range(::grpc::ServerContext *context, const ::etcdserverpb::RangeRequest *request,
                         ::etcdserverpb::RangeResponse *response) override;

    /**
     * @brief Delete key-value pairs from the store by range.
     * @param[in] context The server context.
     * @param[in] request The delete range request containing key range.
     * @param[out] response The delete range response containing deleted KVs.
     * @return Status of the operation.
     */
    ::grpc::Status DeleteRange(::grpc::ServerContext *context, const ::etcdserverpb::DeleteRangeRequest *request,
                               ::etcdserverpb::DeleteRangeResponse *response) override;

    /**
     * @brief Execute a transaction atomically.
     * @param[in] context The server context.
     * @param[in] request The transaction request containing compares and operations.
     * @param[out] response The transaction response containing operation results.
     * @return Status of the operation.
     */
    ::grpc::Status Txn(::grpc::ServerContext *context, const ::etcdserverpb::TxnRequest *request,
                       ::etcdserverpb::TxnResponse *response) override;

private:
    KVManager *kvManager_;
    LeaseManager *leaseManager_;

    /**
     * @brief Fill the response header with cluster ID and member ID.
     * @param[out] header The response header to fill.
     */
    void FillResponseHeader(::etcdserverpb::ResponseHeader *header);
};

}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_KVSTORE_METASTORE_SERVICE_KV_SERVICE_IMPL_H
