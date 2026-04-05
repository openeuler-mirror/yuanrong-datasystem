/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2023. All rights reserved.
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
 * Description: Module responsible for managing global cache deletion.
 */
#ifndef MASTER_OC_MIGRATE_METADATA_MANAGER_H
#define MASTER_OC_MIGRATE_METADATA_MANAGER_H

#include <memory>
#include <shared_mutex>
#include <string>
#include <thread>
#include <unordered_map>

#include <tbb/concurrent_hash_map.h>

#include "datasystem/common/ak_sk/ak_sk_manager.h"
#include "datasystem/common/rpc/rpc_constants.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/thread_pool.h"
#include "datasystem/master/object_cache/oc_global_cache_delete_manager.h"
#include "datasystem/master/replica_manager.h"
#include "datasystem/master/object_cache/master_master_oc_api.h"
#include "datasystem/protos/master_object.pb.h"
#include "datasystem/protos/master_object.stub.rpc.pb.h"
#include "datasystem/worker/cluster_manager/etcd_cluster_manager.h"

namespace datasystem {
namespace master {
using TbbFutureThreadTable = tbb::concurrent_hash_map<std::pair<std::string, std::string>,
                                                      std::future<std::pair<Status, std::vector<std::string>>>>;

class OCMigrateMetadataManager {
public:
    struct MigrateMetaInfo {
        std::vector<std::string> objectKeys = {};
        std::string destAddr = "";
        worker::HashRange ranges = {};
        std::vector<std::string> failedIds = {};
        std::string destDbName = "";
    };

    OCMigrateMetadataManager(const OCMigrateMetadataManager &other) = delete;
    OCMigrateMetadataManager(OCMigrateMetadataManager &&other) = delete;
    OCMigrateMetadataManager &operator=(const OCMigrateMetadataManager &) = delete;
    OCMigrateMetadataManager &operator=(OCMigrateMetadataManager &&) = delete;

    /**
     * @brief Singleton mode, obtaining instance.
     * @return OCMigrateMetadataManager reference.
     */
    static OCMigrateMetadataManager &Instance();

    ~OCMigrateMetadataManager();

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
     * @param[in] ranges The object keys to be migrated.
     * @param[in] isNetworkRecovery True if under network recovery scenario.
     * @return Status of the call.
     */
    Status MigrateByRanges(const std::string &dbName, const std::string &dest, const std::string &destDbNAme,
                           const worker::HashRange &ranges, bool isNetworkRecovery);

    /**
     * @brief Starting Data Migration
     * @param[in] ocMetadataManager The OCMetadataManager instance.
     * @param[in] info Destination address of the migration. need to migrate objKey ranges, the object keys to be
     * migrated..
     * @return Status of the call and failed object keys.
     */
    Status StartMigrateMetadataForScaleout(const std::shared_ptr<master::OCMetadataManager> &ocMetadataManager,
                                           MigrateMetaInfo &info);

    /**
     * @brief Obtaining the Data Migration Result
     * @param[in] dbName The rocksdb name.
     * @param[in] destination Destination address of the migration.
     * @param[out] failedObjectKeys Failed object keys.
     * @return Status of the call.
     */
    Status GetMigrateMetadataResult(const std::string &dbName, const std::string &destination,
                                    std::vector<std::string> &failedObjectKeys);

    /**
     * @brief Migrate meta with retry on error
     * @param[in] ocMetadataManager The OCMetadataManager instance.
     * @param[in] info Destination address of the migration. need to migrate objKey ranges, the object keys to be
     * migrated.
     * @param[in] isNetworkRecovery True if under network recovery scenario.
     * @return Status of the call.
     */
    Status MigrateMetaDataWithRetry(const std::shared_ptr<master::OCMetadataManager> &ocMetadatManager,
                                    MigrateMetaInfo &info, bool isNetworkRecovery);

private:
    OCMigrateMetadataManager() = default;

    /**
     * @brief Migrate meta
     * @param[in] ocMetadataManager The OCMetadataManager instance.
     * @param[in] info Destination address of the migration, need to migrate objKey ranges, the object keys to be
     * migrated. failed object keys.
     * @return Status of the call.
     */
    Status MigrateMetaData(const std::shared_ptr<master::OCMetadataManager> &ocMetadatManager, MigrateMetaInfo &info);

    /**
     * @brief Performing Async Data Migration
     * @param[in] ocMetadataManager The OCMetadataManager instance.
     * @param[in] info The object keys to be migrated, destination address of the migration, need to migrate objKey
     * ranges.
     * @return Status of the call and failed object keys.
     */
    std::pair<Status, std::vector<std::string>> AsyncMigrateMetadata(
        const std::shared_ptr<master::OCMetadataManager> &ocMetadataManager, MigrateMetaInfo &info);

    /**
     * @brief Migrate metadata.
     * @param[in] ocMetadataManager The OCMetadataManager instance.
     * @param[in] api Rpc channel for send data
     * @param[in] info range and objkeys.
     * @param[out] failedObjectKeys Failed object keys.
     * @return Status of the call.
     */
    Status MigrateMetadataForScaleout(const std::shared_ptr<master::OCMetadataManager> &ocMetadataManager,
                                      std::unique_ptr<MasterMasterOCApi> &api, MigrateMetaInfo &info,
                                      std::vector<std::string> &failedIds);

    /**
     * @brief Migrating Data in Batches
     * @param[in] api Rpc channel for send data
     * @param[in] req The request message for data migration.
     * @param[in] ocMetadataManager The OCMetadataManager instance.
     * @param[out] failedObjectKeys Failed object keys.
     * @return Status of the call.
     */
    Status BatchMigrateMetadata(
        std::unique_ptr<MasterMasterOCApi> &api, MigrateMetadataReqPb &req,
        const std::shared_ptr<master::OCMetadataManager> &ocMetadataManager, std::vector<std::string> &failedObjectKeys,
        const std::unordered_map<std::string, std::unordered_set<std::shared_ptr<AsyncElement>>> &asyncMap = {});

    /**
     * @brief Try fill migrate nested object references.
     * @param[in] objectKey Object key.
     * @param[in] objectRefs Nested object refs.
     * @param[out] meta Need fill meta.
     * @return True if fill success.
     */
    bool TryFillObjectNestedRefMeta(const std::string &objectKey, const std::unordered_set<std::string> &objectRefs,
                                    const std::shared_ptr<master::OCMetadataManager> &ocMetadataManager,
                                    MetaForMigrationPb &meta);

    /**
     * @brief Try fill migrate nested object references.
     * @param[in] objectKey Object key.
     * @param[in] objectRefs Object refs.
     * @param[out] meta Need fill meta.
     * @return True if fill success.
     */
    bool TryFillObjectRefMeta(const std::string &objectKey, const std::unordered_set<std::string> &objectRefs,
                              const std::shared_ptr<master::OCMetadataManager> &ocMetadataManager,
                              MetaForMigrationPb &meta);

    /**
     * @brief Try fill migrate nested object references.
     * @param[in] objectKey Object key.
     * @param[in] globalCacheDeletes Global cache delete infos.
     * @param[out] meta Need fill meta.
     * @return True if fill success.
     */
    bool TryFillGlobalCacheDeleteMeta(const std::string &objectKey, const GlobalDeleteInfoMap &globalCacheDeletes,
                                      MetaForMigrationPb &meta);

    /**
     * @brief Try fill migrate nested object references.
     * @param[in] objectKey Object key.
     * @param[in] asyncL2Metas Async l2 elements.
     * @param[out] meta Need fill meta.
     * @return True if fill success.
     */
    bool TryFillAsyncL2Meta(
        const std::string &objectKey,
        const std::unordered_map<std::string, std::unordered_set<std::shared_ptr<AsyncElement>>> &asyncL2Metas,
        MetaForMigrationPb &meta);

    /**
     * @brief FillRemoteClientIds
     * @param[in] ocMetadataManager The OCMetadataManager instance.
     * @param[in] ranges Ranges need to migrate
     * @param[in] destAddr dest address.
     * @param[out] req Req to fill
     */
    void FillRemoteClientIds(const std::shared_ptr<master::OCMetadataManager> &ocMetadataManager,
                             const worker::HashRange &ranges, const std::string &destAddr, MigrateMetadataReqPb &req);

    /**
     * @brief Fill subscribe infos.
     * @param[in] ocMetadataManager The OCMetadataManager instance.
     * @param[in] ranges Ranges need to migrate
     * @param[out] req Req to fill
     */
    void FillSubscribeInfos(const std::shared_ptr<master::OCMetadataManager> &ocMetadataManager,
                            const worker::HashRange &ranges, MigrateMetadataReqPb &req);

    /**
     * @brief Get and fill migrate device meta info.
     * @param[in] ocMetadataManager The OCMetadataManager instance.
     * @param[in] ranges Ranges need to migrate
     * @param[out] req Req to fill
     */
    void GetAndFillMigrateDeviceMetaInfo(const std::shared_ptr<master::OCMetadataManager> &ocMetadataManager,
                                         const worker::HashRange &ranges, MigrateMetadataReqPb &req);

    /**
     * @brief Migrate metadata that cannot be found in meta table.
     * @param[in] ocMetadataManager The OCMetadataManager instance.
     * @param[in] api Rpc channel for send data
     * @param[in] info range and objkeys.
     * @param[out] failedIds Failed ids.
     * @return Status of the call.
     */
    Status MigrateNoMetaInfoForScaleout(const std::shared_ptr<master::OCMetadataManager> &ocMetadataManager,
                                        std::unique_ptr<MasterMasterOCApi> &api, MigrateMetaInfo &info,
                                        std::vector<std::string> &failedIds);

    /**
     * @brief Migrate device metadata.
     * @param[in] ocMetadataManager The OCMetadataManager instance.
     * @param[in] api Rpc channel for send data
     * @param[in] info range and objkeys.
     */
    void MigrateDeviceMetaForScaleout(const std::shared_ptr<master::OCMetadataManager> &ocMetadataManager,
                                      std::unique_ptr<MasterMasterOCApi> &api, MigrateMetaInfo &info);

    HostPort localHostPort_;
    std::shared_ptr<AkSkManager> akSkManager_;
    EtcdClusterManager *cm_{ nullptr };
    std::unique_ptr<ThreadPool> threadPool_;
    // tbb::concurrent_hash_map<workerAddr, std::future<std::pair<Result status, Failed objectkeys>>>
    TbbFutureThreadTable futureThread_;
    std::atomic<bool> exitFlag_{ false };
    ReplicaManager *replicaManager_;
};
}  // namespace master
}  // namespace datasystem
#endif  // MASTER_OC_MIGRATE_METADATA_MANAGER_H
