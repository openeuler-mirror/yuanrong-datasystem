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
 * Description: The metadata manager holder implement.
 */

#include "datasystem/master/metadata_manager_holder.h"

#include "datasystem/common/flags/flags.h"
#include "datasystem/common/kvstore/rocksdb/replica.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/util/locks.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace {
constexpr char OBJECT_META_NAME[] = "object_metadata";
}  // namespace

void MetadataManager::Shutdown()
{
    if (oc != nullptr) {
        oc->Shutdown();
    }
    ShutdownStream();
}

Status MetadataManagerHolder::Init(MetadataManagerHolderParam param)
{
    LOG(INFO) << "Init metadata manager holder.";
    dbRootPath_ = std::move(param.dbRootPath);
    currentWorkerId_ = std::move(param.currWorkerId);
    persistenceApi_ = param.persistenceApi;
    masterAddress_ = param.masterAddress;
    placement_ = param.placement;
    membership_ = param.membership;
    localAddress_ = std::move(param.localAddress);
    isRestart_ = param.isRestart;
    controlBackendAvailableAtStartup_ = param.controlBackendAvailableAtStartup;
    exitRequested_ = param.exitRequested;
    masterWorkerService_ = param.masterWorkerService;
    workerWorkerService_ = param.workerWorkerService;
    rpcSessionManager_ = param.rpcSessionManager;
    isOcEnabled_ = param.isOcEnabled;
    isScEnabled_ = param.isScEnabled;

    if (!isRestart_) {
        isNewNode_ = true;
        LOG(INFO) << "Newly-added node, remove local metadata store if it exists.";
        RETURN_IF_NOT_OK(RemoveMetadataStoresForNewNode());
    } else {
        isNewNode_ = false;
        LOG(INFO) << "Restarted node, try recovery from local metadata store.";
    }
    return Status::OK();
}

Status MetadataManagerHolder::CreateMetaManager(const std::string &workerId, RocksStore *objectRocksStore,
                                                RocksStore *streamRocksStore)
{
    if (ocMetadataManager_ != nullptr || scMetadataManager_ != nullptr) {
        return Status::OK();
    }

    Timer timer;
    double ocElapsed = 0;
    double scElapsed = 0;
    if (isOcEnabled_) {
        auto oc = std::make_shared<master::OCMetadataManager>(
            objectRocksStore, persistenceApi_, masterAddress_.ToString(), placement_,
            membership_, localAddress_, exitRequested_, workerId);
        LOG(INFO) << "Start init OCMetadataManager for " << workerId;
        RETURN_IF_NOT_OK(oc->Init());
        ocElapsed = timer.ElapsedMilliSecond();
        oc->AssignLocalWorker(masterWorkerService_, workerWorkerService_, masterAddress_);
        ocMetadataManager_ = std::move(oc);
    }

    RETURN_IF_NOT_OK(CreateStreamMetadataManager(workerId, streamRocksStore, timer, scElapsed));

    LOG(INFO) << "OCMetadataManager init cost:" << ocElapsed << "ms, SCMetadataManager init cost:" << scElapsed
              << "ms for " << workerId;
    return Status::OK();
}

Status MetadataManagerHolder::GetOcMetadataManager(std::shared_ptr<master::OCMetadataManager> &ocMetadataManager)
{
    std::shared_lock<SharedMutex> locker(mutex_);
    RETURN_RUNTIME_ERROR_IF_NULL(ocMetadataManager_);
    ocMetadataManager = ocMetadataManager_;
    return Status::OK();
}

Status MetadataManagerHolder::GetScMetadataManager(std::shared_ptr<master::SCMetadataManager> &scMetadataManager)
{
    std::shared_lock<SharedMutex> locker(mutex_);
    RETURN_RUNTIME_ERROR_IF_NULL(scMetadataManager_);
    scMetadataManager = scMetadataManager_;
    return Status::OK();
}

Status MetadataManagerHolder::CleanupLocalMetadataForRejoin(const std::string &workerAddr)
{
    HostPort localAddress;
    RETURN_IF_NOT_OK(localAddress.ParseString(workerAddr));
    std::shared_lock<SharedMutex> locker(mutex_);
    if (ocMetadataManager_ != nullptr) {
        RETURN_IF_NOT_OK(ocMetadataManager_->CleanupLocalMetadataForRejoin(workerAddr));
    }
    return CleanupStreamMetadataForRejoin(localAddress);
}

Status MetadataManagerHolder::EnsureLocalMetadataManager(const std::string &workerId)
{
    std::unique_lock<SharedMutex> locker(mutex_);
    if (objectRocksStore_ == nullptr) {
        std::string objectPath = dbRootPath_ + "/" + OBJECT_META_NAME;
        RETURN_IF_NOT_OK(Replica::CreateRocksStoreInstance(objectPath, objectRocksStore_));
        RETURN_IF_NOT_OK(Replica::CreateOcTable(objectRocksStore_.get()));
    }
    RETURN_IF_NOT_OK(PrepareStreamStore());
    return CreateMetaManager(workerId, objectRocksStore_.get(), streamRocksStore_.get());
}

void MetadataManagerHolder::Shutdown()
{
    Timer timer;
    std::shared_lock<SharedMutex> locker(mutex_);
    if (ocMetadataManager_ != nullptr) {
        ocMetadataManager_->Shutdown();
    }
    ShutdownStreamMetadata();
    LOG(INFO) << "Metadata manager holder shutdown, cost:" << timer.ElapsedMilliSecond() << "ms";
}

void MetadataManagerHolder::PrepareForFullClusterShutdown()
{
    std::shared_lock<SharedMutex> locker(mutex_);
    if (ocMetadataManager_ != nullptr) {
        ocMetadataManager_->PrepareForFullClusterShutdown();
    }
}

Status MetadataManagerHolder::GetDeviceOcManager(std::shared_ptr<master::MasterDevOcManager> &devOcManager)
{
    std::shared_ptr<master::OCMetadataManager> ocMetadataManager;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(GetOcMetadataManager(ocMetadataManager), "GetOcMetadataManager failed");
    devOcManager = ocMetadataManager->GetDeviceOcManager();
    return Status::OK();
}

Status MetadataManagerHolder::InitLocalMetadataForStart(bool isRestart)
{
    (void)isRestart;
    const auto &workerId = GetCurrentWorkerUuid();
    LOG(INFO) << "Create local metadata manager for worker:" << workerId
              << ", worker addr:" << masterAddress_.ToString();

    return EnsureLocalMetadataManager(workerId);
}

bool MetadataManagerHolder::CheckMetaEmpty()
{
    std::shared_lock<SharedMutex> locker(mutex_);
    auto oc = ocMetadataManager_;
    auto sc = scMetadataManager_;
    if ((oc != nullptr && !oc->CheckMetaTableEmpty()) || !CheckStreamMetadataEmpty(sc)) {
        return false;
    }
    return true;
}
}  // namespace datasystem
