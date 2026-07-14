/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2024. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Description: The metadata manager holder define.
 */

#ifndef DATASYSTEM_MASTER_METADATA_MANAGER_HOLDER_H
#define DATASYSTEM_MASTER_METADATA_MANAGER_HOLDER_H

#include <memory>
#include <shared_mutex>

#include "datasystem/common/ak_sk/ak_sk_manager.h"
#include "datasystem/common/kvstore/etcd/etcd_store.h"
#include "datasystem/common/kvstore/rocksdb/rocks_store.h"
#include "datasystem/common/l2cache/persistence_api.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/master/object_cache/oc_metadata_manager.h"
#include "datasystem/master/stream_cache/sc_metadata_manager.h"
#include "datasystem/worker/worker_topology_references.h"

namespace datasystem {
namespace object_cache {
class MasterWorkerOCServiceImpl;
class WorkerWorkerOCServiceImpl;
}  // namespace object_cache

struct MetadataManagerHolderParam {
    std::string dbRootPath;
    std::string currWorkerId;
    std::shared_ptr<AkSkManager> akSkManager;
    EtcdStore *etcdStore;
    std::shared_ptr<PersistenceApi> persistenceApi;
    HostPort masterAddress;
    worker::WorkerTopologyReferences *topologyEngine;
    object_cache::MasterWorkerOCServiceImpl *masterWorkerService;
    object_cache::WorkerWorkerOCServiceImpl *workerWorkerService;
    std::shared_ptr<master::RpcSessionManager> rpcSessionManager;
    bool isOcEnabled;
    bool isScEnabled;
};

struct MetadataManager {
    std::shared_ptr<master::OCMetadataManager> oc;
    std::shared_ptr<master::SCMetadataManager> sc;
    void Shutdown();
};

class MetadataManagerHolder {
public:
    MetadataManagerHolder() = default;
    MetadataManagerHolder(const MetadataManagerHolder &) = delete;
    MetadataManagerHolder &operator=(const MetadataManagerHolder &) = delete;
    ~MetadataManagerHolder() = default;

    Status Init(MetadataManagerHolderParam param);

    Status EnsureLocalMetadataManager(const std::string &workerId);

    const std::string &GetCurrentWorkerUuid()
    {
        return currentWorkerId_;
    }

    Status GetOcMetadataManager(std::shared_ptr<master::OCMetadataManager> &ocMetadataManager);

    Status GetScMetadataManager(std::shared_ptr<master::SCMetadataManager> &scMetadataManager);

    bool HaveAsyncMetaRequest();

    void Shutdown();

    Status GetDeviceOcManager(std::shared_ptr<master::MasterDevOcManager> &devOcManager);

    Status InitLocalMetadataForStart(bool isRestart);

    template <typename Func>
    Status ApplyForAllMetaManager(Func &&func)
    {
        Status lastRc;
        std::shared_lock<std::shared_timed_mutex> locker(mutex_);
        MetadataManager metadataManager{ ocMetadataManager_, scMetadataManager_ };
        auto rc = func(currentWorkerId_, metadataManager);
        lastRc = rc.IsError() ? rc : lastRc;
        return lastRc;
    }

protected:
    virtual Status CreateMetaManager(const std::string &workerId, RocksStore *objectRocksStore,
                                     RocksStore *streamRocksStore);

    bool CheckMetaEmpty();

    std::string dbRootPath_;
    std::string currentWorkerId_;
    std::shared_ptr<AkSkManager> akSkManager_;
    EtcdStore *etcdStore_ = nullptr;
    std::shared_ptr<PersistenceApi> persistenceApi_;
    HostPort masterAddress_;
    worker::WorkerTopologyReferences *topologyEngine_ = nullptr;
    object_cache::MasterWorkerOCServiceImpl *masterWorkerService_ = nullptr;
    object_cache::WorkerWorkerOCServiceImpl *workerWorkerService_ = nullptr;
    std::shared_ptr<master::RpcSessionManager> rpcSessionManager_;
    bool isOcEnabled_ = false;
    bool isScEnabled_ = false;
    bool isNewNode_ = false;

    std::shared_ptr<RocksStore> objectRocksStore_;
    std::shared_ptr<RocksStore> streamRocksStore_;

    std::shared_timed_mutex mutex_;
    std::shared_ptr<master::OCMetadataManager> ocMetadataManager_;
    std::shared_ptr<master::SCMetadataManager> scMetadataManager_;
};
}  // namespace datasystem

#endif  // DATASYSTEM_MASTER_METADATA_MANAGER_HOLDER_H
