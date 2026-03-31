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
#include "datasystem/common/kvstore/metastore/metastore_server.h"

#include "datasystem/common/log/log.h"

namespace datasystem {

MetaStoreServer::MetaStoreServer()
{
    // Setup watch callbacks (put/delete key) in KVManager
    kvManager_.SetWatchCallback(
        [this](const std::string &key, const mvccpb::KeyValue &kv, const mvccpb::KeyValue *prevKv) {
            watchManager_.NotifyPut(key, kv, prevKv, kvManager_.CurrentRevision());
        },
        [this](const std::string &key, const mvccpb::KeyValue &kv) {
            watchManager_.NotifyDelete(key, kv, kvManager_.CurrentRevision());
        });

    // Setup lease manager in KVManager
    kvManager_.SetLeaseManager(&leaseManager_);

    // Setup delete key callback in LeaseManager
    // When lease expires and key is deleted, skipLeaseCleanup=true because lease is already managing the cleanup
    leaseManager_.SetDeleteKeyCallback(
        [this](const std::string &key) { return kvManager_.Delete(key, "", nullptr, true); });

    // Create gRPC services
    kvService_ = std::make_unique<KVServiceImpl>(&kvManager_, &leaseManager_);
    leaseService_ = std::make_unique<LeaseServiceImpl>(&leaseManager_, &kvManager_);
    watchService_ = std::make_unique<WatchServiceImpl>(&watchManager_, &kvManager_);
    maintenanceService_ = std::make_unique<MaintenanceServiceImpl>(&kvManager_);
}

MetaStoreServer::~MetaStoreServer()
{
    Stop();
}

Status MetaStoreServer::Start(const std::string &address)
{
    LOG(INFO) << "Starting MetaStore server on " << address;
    // Create gRPC server builder
    grpc::ServerBuilder builder;

    // Configure gRPC server keepalive settings to prevent "too_many_pings" errors
    builder.SetMaxReceiveMessageSize(GRPC_MAX_MESSAGE_SIZE_BYTES);
    builder.SetMaxSendMessageSize(GRPC_MAX_MESSAGE_SIZE_BYTES);
    // Allow client to send pings more frequently
    builder.AddChannelArgument(GRPC_ARG_HTTP2_MIN_RECV_PING_INTERVAL_WITHOUT_DATA_MS, GRPC_PING_INTERVAL_MS);
    builder.AddChannelArgument(GRPC_ARG_KEEPALIVE_TIME_MS, GRPC_KEEPALIVE_TIME_MS);
    builder.AddChannelArgument(GRPC_ARG_KEEPALIVE_TIMEOUT_MS, GRPC_KEEPALIVE_TIMEOUT_MS);
    builder.AddChannelArgument(GRPC_ARG_KEEPALIVE_PERMIT_WITHOUT_CALLS, GRPC_KEEPALIVE_PERMIT_WITHOUT_CALLS);

    builder.AddListeningPort(address, grpc::InsecureServerCredentials());

    // Register services
    builder.RegisterService(kvService_.get());
    builder.RegisterService(leaseService_.get());
    builder.RegisterService(watchService_.get());
    builder.RegisterService(maintenanceService_.get());

    // Build and start server
    server_ = builder.BuildAndStart();
    if (!server_) {
        return Status(StatusCode::K_RUNTIME_ERROR, "Failed to build gRPC server");
    }

    // Start lease expiration check
    leaseManager_.StartExpirationCheck();

    LOG(INFO) << "MetaStore server successfully started on " << address;
    return Status::OK();
}

Status MetaStoreServer::Stop()
{
    if (server_) {
        LOG(INFO) << "Shutting down MetaStore server...";

        // First, shutdown gRPC server to prevent new incoming requests
        // This ensures no new operations can start during shutdown
        server_->Shutdown();
        server_->Wait();

        // Now stop lease expiration check (waits for thread to complete)
        leaseManager_.StopExpirationCheck();

        // Clear callbacks to prevent any stale access during destruction
        kvManager_.SetWatchCallback(nullptr, nullptr);
        leaseManager_.SetDeleteKeyCallback(nullptr);

        server_.reset();
    }

    return Status::OK();
}

}  // namespace datasystem
