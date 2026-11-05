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

#include "datasystem/master/metadata_manager_holder.h"

#include "datasystem/common/kvstore/rocksdb/replica.h"
#include "datasystem/common/util/status_helper.h"

#ifdef BUILD_STREAM_CACHE
#include <utility>

#include "datasystem/common/util/timer.h"
#include "datasystem/master/stream_cache/sc_metadata_manager.h"
#include "datasystem/common/log/log.h"
#endif

namespace datasystem {
#ifdef BUILD_STREAM_CACHE
namespace {
constexpr char STREAM_META_NAME[] = "stream_meta_data";
}  // namespace

Status MetadataManagerHolder::RemoveMetadataStoresForNewNode() const
{
    return Replica::RemoveRocksFromFileSystem(dbRootPath_, true);
}

Status MetadataManagerHolder::PrepareStreamStore()
{
    if (streamRocksStore_ == nullptr) {
        std::string streamPath = dbRootPath_ + "/" + STREAM_META_NAME;
        RETURN_IF_NOT_OK(Replica::CreateRocksStoreInstance(streamPath, streamRocksStore_));
        RETURN_IF_NOT_OK(Replica::CreateScTable(streamRocksStore_.get()));
    }
    return Status::OK();
}

Status MetadataManagerHolder::CreateStreamMetadataManager(
    const std::string &workerId, RocksStore *streamRocksStore, Timer &timer, double &elapsed)
{
    if (isScEnabled_) {
        auto sc = std::make_shared<master::SCMetadataManager>(
            masterAddress_, rpcSessionManager_, placement_, membership_, streamRocksStore, workerId);
        LOG(INFO) << "Start init SCMetadataManager for " << workerId;
        timer.Reset();
        RETURN_IF_NOT_OK(sc->Init());
        elapsed = timer.ElapsedMilliSecond();
        scMetadataManager_ = std::move(sc);
    }
    return Status::OK();
}

Status MetadataManagerHolder::CleanupStreamMetadataForRejoin(const HostPort &localAddress)
{
    if (scMetadataManager_ != nullptr) {
        RETURN_IF_NOT_OK(scMetadataManager_->ClearWorkerMetadata(localAddress, true));
    }
    return Status::OK();
}

bool MetadataManagerHolder::CheckStreamMetadataEmpty(
    const std::shared_ptr<master::SCMetadataManager> &snapshot) const
{
    return snapshot == nullptr || snapshot->CheckMetaTableEmpty();
}

void MetadataManager::ShutdownStream()
{
    if (sc != nullptr) {
        sc->Shutdown();
    }
}

void MetadataManagerHolder::ShutdownStreamMetadata() const
{
    if (scMetadataManager_ != nullptr) {
        scMetadataManager_->Shutdown();
    }
}

#else
Status MetadataManagerHolder::RemoveMetadataStoresForNewNode() const
{
    return Replica::RemoveRocksFromFileSystem(dbRootPath_, false);
}

Status MetadataManagerHolder::PrepareStreamStore()
{
    return Status::OK();
}

Status MetadataManagerHolder::CreateStreamMetadataManager(
    const std::string &, RocksStore *, Timer &, double &)
{
    return Status::OK();
}

Status MetadataManagerHolder::CleanupStreamMetadataForRejoin(const HostPort &)
{
    return Status::OK();
}

bool MetadataManagerHolder::CheckStreamMetadataEmpty(
    const std::shared_ptr<master::SCMetadataManager> &) const
{
    return true;
}

void MetadataManager::ShutdownStream()
{
}

void MetadataManagerHolder::ShutdownStreamMetadata() const
{
}

#endif
}  // namespace datasystem
