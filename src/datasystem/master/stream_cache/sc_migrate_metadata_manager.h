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
 * Description: Migrating Data in Scaling Scenarios for Stream Cache.
 */
#ifndef DATASYSTEM_MASTER_STREAM_CACHE_SC_MIGRATE_METADATA_MANAGER_H
#define DATASYSTEM_MASTER_STREAM_CACHE_SC_MIGRATE_METADATA_MANAGER_H

#include <memory>
#include <shared_mutex>
#include <thread>
#include <unordered_map>

#include <tbb/concurrent_hash_map.h>

#include "datasystem/common/ak_sk/ak_sk_manager.h"
#include "datasystem/common/rpc/rpc_constants.h"
#include "datasystem/common/util/thread_pool.h"
#include "datasystem/master/replica_manager.h"
#include "datasystem/protos/master_stream.stub.rpc.pb.h"
#include "datasystem/worker/cluster_manager/etcd_cluster_manager.h"

namespace datasystem {
#ifdef WITH_TESTS
namespace ut {
class SCMigrateMetadataManagerTest;
}
#endif

namespace master {

class MasterMasterSCApi {
public:
    /**
     * @brief Constructor for the remote version of the api
     * @param[in] hostPort The host port of the target master
     * @param[in] localHostPort The local worker rpc service host port.
     * @param[in] akSkManager Used to do AK/SK authenticate.
     */
    MasterMasterSCApi(const HostPort &hostPort, const HostPort &localHostPort,
                      std::shared_ptr<AkSkManager> akSkManager);

    ~MasterMasterSCApi() = default;

    /**
     * @brief Initialization.
     * @return Status of the call.
     */
    Status Init();

    /**
     * @brief Migrate the metadata
     * @param[in] req The rpc request protobuf.
     * @param[out] resp The rpc request protobuf.
     */
    Status MigrateSCMetadata(MigrateSCMetadataReqPb &req, MigrateSCMetadataRspPb &rsp);

private:
    HostPort destHostPort_;   // The HostPort of the destination node
    HostPort localHostPort_;  // The HostPort of the local node
    std::shared_ptr<AkSkManager> akSkManager_;
    std::unique_ptr<master::MasterSCService_Stub> rpcSession_{ nullptr };  // session to the master rpc service
};

using TbbFutureThreadTable = tbb::concurrent_hash_map<std::pair<std::string, std::string>,
                                                      std::future<std::pair<Status, std::vector<std::string>>>>;

class SCMigrateMetadataManager {
public:
    struct MigrateMetaInfo {
        std::vector<std::string> streamNames;
        std::vector<std::string> failedStreamNames;
        std::string destAddr;
        std::string destDbName;
    };
    SCMigrateMetadataManager(const SCMigrateMetadataManager &other) = delete;
    SCMigrateMetadataManager(SCMigrateMetadataManager &&other) = delete;
    SCMigrateMetadataManager &operator=(const SCMigrateMetadataManager &) = delete;
    SCMigrateMetadataManager &operator=(SCMigrateMetadataManager &&) = delete;

    /**
     * @brief Singleton mode, obtaining instance.
     * @return SCMigrateMetadataManager reference.
     */
    static SCMigrateMetadataManager &Instance();

    ~SCMigrateMetadataManager();

    /**
     * @brief Initialization.
     * @param[in] localHostPort The local worker rpc service host port.
     * @param[in] akSkManager Used to do AK/SK authenticate.
     * @param[in] cm Used to get master of objects.
     * @param[in] replicaManager The replica manager.
     * @return Status of the call.
     */
    Status Init(const HostPort &localHostPort, std::shared_ptr<AkSkManager> akSkManager, EtcdClusterManager *cm,
                ReplicaManager *replicaManager);

    /**
     * @brief Shutdown the oc migrage metadata module.
     */
    void Shutdown();

    /**
     * @brief Migrate data by hash range
     * @param[in] dbName The rocksdb name.
     * @param[in] dest Destination address of the migration.
     * @param[in] destDbName The dest rocksdb name.
     * @param[in] ranges The stream names to be migrated.
     * @param[in] isNetworkRecovery True if under network recovery scenario.
     * @return Status of the call.
     */
    Status MigrateByRanges(const std::string &dbName, const std::string &dest, const std::string &destDbName,
                           const worker::HashRange &ranges, bool isNetworkRecovery);

    /**
     * @brief Starting Data Migration
     * @param[in] scMetadataManager The SCMetadataManager instance.
     * @param[in] info The migrate meta info.
     * @return Status of the call.
     */
    Status StartMigrateMetadataForScaleout(const std::shared_ptr<master::SCMetadataManager> &scMetadataManager,
                                           MigrateMetaInfo &info);

    /**
     * @brief Obtaining the Data Migration Result
     * @param[in] dbName The rocksdb name.
     * @param[in] destination Destination address of the migration.
     * @param[out] failedStreams Failed stream names.
     * @return Status of the call.
     */
    Status GetMigrateMetadataResult(const std::string &dbName, const std::string &destination,
                                    std::vector<std::string> &failedStreams);

private:
    SCMigrateMetadataManager() = default;

    /**
     * @brief Migrate meta with retry on error
     * @param[in] scMetadataManager The SCMetadataManager instance.
     * @param[in] info The migrate meta info.
     * @param[in] isNetworkRecovery True if under network recovery scenario.
     * @return Status of the call.
     */
    Status MigrateMetaDataWithRetry(const std::shared_ptr<master::SCMetadataManager> &scMetadataManager,
                                    MigrateMetaInfo &info, bool isNetworkRecovery);

    /**
     * @brief Migrate meta
     * @param[in] scMetadataManager The SCMetadataManager instance.
     * @param[in] info The migrate meta info.
     * @return Status of the call.
     */
    Status MigrateMetaData(const std::shared_ptr<master::SCMetadataManager> &scMetadataManager, MigrateMetaInfo &info);

    /**
     * @brief Performing Async Data Migration
     * @param[in] scMetadataManager The SCMetadataManager instance.
     * @param[in] info The migrate meta info.
     * @return Status of the call and failed stream names.
     */
    std::pair<Status, std::vector<std::string>> AsyncMigrateMetadata(
        const std::shared_ptr<master::SCMetadataManager> &scMetadataManager, MigrateMetaInfo &info);

    /**
     * @brief Migrate metadata.
     * @param[in] scMetadataManager The SCMetadataManager instance.
     * @param[in] api Rpc channel for send data
     * @param[in] streamNames The stream names to be migrated.
     * @param[out] failedStreams Failed stream names.
     * @return Status of the call.
     */
    Status MigrateMetadataForScaleout(const std::shared_ptr<master::SCMetadataManager> &scMetadataManager,
                                      std::unique_ptr<MasterMasterSCApi> &api,
                                      const std::vector<std::string> &streamNames,
                                      std::vector<std::string> &failedStreams);

    /**
     * @brief Migrating Data in Batches
     * @param[in] scMetadataManager The SCMetadataManager instance.
     * @param[in] api Rpc channel for send data
     * @param[in] req The request message for data migration.
     * @param[out] failedStreams Failed stream names.
     * @return Status of the call.
     */
    Status BatchMigrateMetadata(const std::shared_ptr<master::SCMetadataManager> &scMetadataManager,
                                std::unique_ptr<MasterMasterSCApi> &api, MigrateSCMetadataReqPb &req,
                                std::vector<std::string> &failedStreams);

    /**
     * @brief Handle migration failed streams.
     * @param[in] scMetadataManager The SCMetadataManager instance.
     * @param[in] info The migrate meta info.
     * @param[in] retryCounter The retry counter.
     */
    void HandleMigrationFailed(const std::shared_ptr<master::SCMetadataManager> &scMetadataManager,
                               MigrateMetaInfo &info, std::unordered_map<std::string, int> &retryCounter);

#ifdef WITH_TESTS
    friend class ::datasystem::ut::SCMigrateMetadataManagerTest;
#endif
    
    HostPort localHostPort_;
    std::shared_ptr<AkSkManager> akSkManager_;
    EtcdClusterManager *cm_{ nullptr };
    std::unique_ptr<ThreadPool> threadPool_;
    // tbb::concurrent_hash_map<workerAddr, std::future<std::pair<Result status, Failed stream names>>>
    TbbFutureThreadTable futureThread_;
    std::atomic<bool> exitFlag_{ false };
    ReplicaManager *replicaManager_;
};
}  // namespace master
}  // namespace datasystem
#endif  // DATASYSTEM_MASTER_STREAM_CACHE_SC_MIGRATE_METADATA_MANAGER_H
