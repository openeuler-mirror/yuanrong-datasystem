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
 * Description: MetaStoreServer - etcd gRPC server implementation.
 */
#ifndef DATASYSTEM_COMMON_KVSTORE_METASTORE_METASTORE_SERVER_H
#define DATASYSTEM_COMMON_KVSTORE_METASTORE_METASTORE_SERVER_H

#include <memory>
#include <vector>

#include <grpcpp/grpcpp.h>

#include "datasystem/common/kvstore/metastore/manager/kv_manager.h"
#include "datasystem/common/kvstore/metastore/manager/lease_manager.h"
#include "datasystem/common/kvstore/metastore/manager/watch_manager.h"
#include "datasystem/common/kvstore/metastore/service/kv_service_impl.h"
#include "datasystem/common/kvstore/metastore/service/lease_service_impl.h"
#include "datasystem/common/kvstore/metastore/service/watch_service_impl.h"
#include "datasystem/common/kvstore/metastore/service/maintenance_service_impl.h"
#include "datasystem/utils/status.h"

namespace datasystem {

class MetaStoreServer {
public:
    MetaStoreServer();
    ~MetaStoreServer();

    /**
     * @brief Start gRPC server
     * @param address The server address (e.g., "0.0.0.0:2379")
     * @return Status of operation
     */
    Status Start(const std::string &address);

    /**
     * @brief Stop gRPC server
     * @return Status of operation
     */
    Status Stop();

    /**
     * @brief Get KV manager
     * @return Pointer to KV manager
     */
    const KVManager *GetKVManager() const
    {
        return &kvManager_;
    }

    /**
     * @brief Get lease manager
     * @return Pointer to lease manager
     */
    const LeaseManager *GetLeaseManager() const
    {
        return &leaseManager_;
    }

    /**
     * @brief Get watch manager
     * @return Pointer to watch manager
     */
    const WatchManager *GetWatchManager() const
    {
        return &watchManager_;
    }

    /**
     * @brief Check if server is running
     * @return true if running
     */
    bool IsRunning() const
    {
        return server_ != nullptr;
    }

private:
    // gRPC configuration constants
    static constexpr int GRPC_MAX_MESSAGE_SIZE_MB = 4;
    static constexpr int GRPC_MAX_MESSAGE_SIZE_BYTES = GRPC_MAX_MESSAGE_SIZE_MB * 1024 * 1024;
    static constexpr int GRPC_PING_INTERVAL_MS = 1000;
    static constexpr int GRPC_KEEPALIVE_TIME_MS = 60000;
    static constexpr int GRPC_KEEPALIVE_TIMEOUT_MS = 20000;
    static constexpr int GRPC_KEEPALIVE_PERMIT_WITHOUT_CALLS = 1;

    std::unique_ptr<grpc::Server> server_;
    KVManager kvManager_;
    LeaseManager leaseManager_;
    WatchManager watchManager_;

    std::unique_ptr<KVServiceImpl> kvService_;
    std::unique_ptr<LeaseServiceImpl> leaseService_;
    std::unique_ptr<WatchServiceImpl> watchService_;
    std::unique_ptr<MaintenanceServiceImpl> maintenanceService_;
};

}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_KVSTORE_METASTORE_METASTORE_SERVER_H
