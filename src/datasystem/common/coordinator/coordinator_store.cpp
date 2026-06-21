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

#include "datasystem/common/coordinator/coordinator_store.h"

#include <utility>
#include <vector>

#include "datasystem/common/util/status_helper.h"

namespace datasystem {
CoordinatorStore::CoordinatorStore(std::shared_ptr<MemoryKvStore> memKvStore,
                                   std::shared_ptr<WatchRegistry> watchRegistry,
                                   std::shared_ptr<WatchDispatcher> watchDispatcher,
                                   std::shared_ptr<TtlManager> ttlManager)
    : memKvStore_(std::move(memKvStore)),
      watchRegistry_(std::move(watchRegistry)),
      watchDispatcher_(std::move(watchDispatcher)),
      ttlManager_(std::move(ttlManager))
{
    BindCallbacks();
    if (watchDispatcher_) {
        watchDispatcher_->Start();
    }
    if (ttlManager_) {
        ttlManager_->Start();
    }
}

CoordinatorStore::~CoordinatorStore()
{
    if (watchDispatcher_) {
        watchDispatcher_->Stop();
    }
    if (ttlManager_) {
        ttlManager_->Stop();
    }
}

void CoordinatorStore::BindCallbacks()
{
    if (memKvStore_ && watchDispatcher_) {
        std::weak_ptr<WatchDispatcher> weakDispatcher = watchDispatcher_;
        memKvStore_->SetMutationCallback([weakDispatcher](std::shared_ptr<WatchEvent> event) {
            auto dispatcher = weakDispatcher.lock();
            if (dispatcher) {
                dispatcher->Enqueue(std::move(event));
            }
        });
    }

    if (ttlManager_ && memKvStore_) {
        std::weak_ptr<MemoryKvStore> weakStore = memKvStore_;
        ttlManager_->SetExpireCallback([weakStore](const std::string &key, int64_t revision, uint64_t ttlGeneration) {
            auto store = weakStore.lock();
            if (!store) {
                return false;
            }
            return store->DeleteIfTtlExpired(key, revision, ttlGeneration);
        });
    }
}

Status CoordinatorStore::CheckInitialized() const
{
    if (!memKvStore_ || !watchRegistry_ || !watchDispatcher_ || !ttlManager_) {
        return Status(StatusCode::K_NOT_READY, "coordinator store is not initialized");
    }
    return Status::OK();
}

Status CoordinatorStore::Put(const std::string &key, const std::string &value, int64_t ttlMs, int64_t expectedVersion,
                             int64_t &version, int64_t &revision)
{
    RETURN_IF_NOT_OK(CheckInitialized());

    uint64_t ttlGeneration = 0;
    RETURN_IF_NOT_OK(memKvStore_->Put(key, value, ttlMs, expectedVersion, version, revision, ttlGeneration));

    if (ttlMs > 0) {
        return ttlManager_->Schedule(key, ttlMs, revision, ttlGeneration);
    }
    return Status::OK();
}

Status CoordinatorStore::Range(const std::string &key, const std::string &rangeEnd, std::vector<KeyValueEntry> &kvs,
                               int64_t &revision)
{
    RETURN_IF_NOT_OK(CheckInitialized());
    memKvStore_->Range(key, rangeEnd, kvs, revision);
    return Status::OK();
}

Status CoordinatorStore::DeleteRange(const std::string &key, const std::string &rangeEnd, int64_t &deleted,
                                     int64_t &revision)
{
    RETURN_IF_NOT_OK(CheckInitialized());

    std::vector<KeyValueEntry> deletedEntries;
    memKvStore_->Delete(key, rangeEnd, deleted, revision, deletedEntries);

    return Status::OK();
}

Status CoordinatorStore::WatchRange(const std::string &key, const std::string &rangeEnd, const std::string &watcherAddr,
                                    int64_t &watchId, std::vector<KeyValueEntry> &initialKvs)
{
    RETURN_IF_NOT_OK(CheckInitialized());

    watchId = watchRegistry_->Register(key, rangeEnd, watcherAddr);
    watchDispatcher_->AddChannel(watchId, watcherAddr);

    int64_t snapshotRevision = 0;
    memKvStore_->Range(key, rangeEnd, initialKvs, snapshotRevision);
    watchDispatcher_->SetSnapshotRevision(watchId, snapshotRevision);
    return Status::OK();
}

Status CoordinatorStore::KeepAlive(const std::string &key, int64_t &ttlMs, int64_t &remainingTtlMs)
{
    RETURN_IF_NOT_OK(CheckInitialized());
    int64_t revision = 0;
    uint64_t ttlGeneration = 0;
    RETURN_IF_NOT_OK(memKvStore_->KeepAlive(key, ttlMs, remainingTtlMs, revision, ttlGeneration));
    return ttlManager_->Schedule(key, ttlMs, revision, ttlGeneration);
}
}  // namespace datasystem
