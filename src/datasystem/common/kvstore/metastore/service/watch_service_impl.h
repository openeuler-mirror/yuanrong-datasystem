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
 * Description: Watch gRPC service implementation for metastore service.
 */
#ifndef DATASYSTEM_COMMON_KVSTORE_METASTORE_SERVICE_WATCH_SERVICE_IMPL_H
#define DATASYSTEM_COMMON_KVSTORE_METASTORE_SERVICE_WATCH_SERVICE_IMPL_H

#include <grpcpp/grpcpp.h>
#include <unordered_set>

#include "etcd/api/etcdserverpb/rpc.grpc.pb.h"
#include "datasystem/common/kvstore/metastore/manager/watch_manager.h"
#include "datasystem/common/kvstore/metastore/manager/kv_manager.h"

namespace datasystem {

class WatchServiceImpl : public etcdserverpb::Watch::Service {
public:
    /**
     * @brief Constructor for WatchServiceImpl.
     * @param[in] watchManager Pointer to the watch manager.
     * @param[in] kvManager Pointer to the KV manager.
     */
    explicit WatchServiceImpl(WatchManager *watchManager, KVManager *kvManager);
    ~WatchServiceImpl();

    /**
     * @brief Watch events on keys by sending watch requests and receiving watch responses.
     * @param[in] context The server context.
     * @param[in] stream The bidirectional stream for watch requests and responses.
     * @return Status of the operation.
     */
    ::grpc::Status Watch(
        ::grpc::ServerContext *context,
        ::grpc::ServerReaderWriter<::etcdserverpb::WatchResponse, ::etcdserverpb::WatchRequest> *stream) override;

private:
    WatchManager *watchManager_;
    KVManager *kvManager_;

    /**
     * @brief Fill response header with cluster ID and member ID.
     * @param[out] header The response header to fill.
     */
    void FillResponseHeader(::etcdserverpb::ResponseHeader *header);
};

}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_KVSTORE_METASTORE_SERVICE_WATCH_SERVICE_IMPL_H
