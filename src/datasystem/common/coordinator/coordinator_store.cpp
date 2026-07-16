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

#include <exception>
#include <utility>
#include <vector>

#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"

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
    Shutdown();
}

void CoordinatorStore::BindCallbacks()
{
    if (memKvStore_ && watchDispatcher_) {
        std::weak_ptr<WatchDispatcher> weakDispatcher = watchDispatcher_;
        memKvStore_->SetMutationCallback([this, weakDispatcher](std::shared_ptr<WatchEvent> event) {
            if (event != nullptr && committedMutationObserver_) {
                try {
                    committedMutationObserver_(event->type, event->entry.key);
                } catch (const std::exception &error) {
                    LOG(ERROR) << "CLUSTER_COMMITTED_MUTATION_OBSERVER_FAILED, error=" << error.what();
                } catch (...) {
                    LOG(ERROR) << "CLUSTER_COMMITTED_MUTATION_OBSERVER_FAILED, unknown error";
                }
            }
            auto dispatcher = weakDispatcher.lock();
            if (dispatcher) {
                try {
                    dispatcher->Enqueue(std::move(event));
                } catch (const std::exception &error) {
                    LOG(ERROR) << "CLUSTER_WATCH_EVENT_ENQUEUE_FAILED, error=" << error.what();
                } catch (...) {
                    LOG(ERROR) << "CLUSTER_WATCH_EVENT_ENQUEUE_FAILED, unknown error";
                }
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
    VLOG(1) << "Put key: " << key << " revision: " << revision;

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
    VLOG(1) << "Range key: " << key << " rangeEnd: " << rangeEnd << ", revision: " << revision;
    return Status::OK();
}

Status CoordinatorStore::DeleteRange(const std::string &key, const std::string &rangeEnd, int64_t &deleted,
                                     int64_t &revision)
{
    RETURN_IF_NOT_OK(CheckInitialized());

    std::vector<KeyValueEntry> deletedEntries;
    memKvStore_->Delete(key, rangeEnd, deleted, revision, deletedEntries);
    VLOG(1) << "DeleteRange key: " << key << " rangeEnd: " << rangeEnd << ", revision: " << revision;

    return Status::OK();
}

Status CoordinatorStore::WatchRange(const std::string &key, const std::string &rangeEnd, const std::string &watcherAddr,
                                    const std::string &registrationId, int64_t &watchId,
                                    std::vector<KeyValueEntry> &initialKvs)
{
    RETURN_IF_NOT_OK(CheckInitialized());
    bool created = false;
    RETURN_IF_NOT_OK(watchRegistry_->Register(key, rangeEnd, watcherAddr, registrationId, watchId, created));
    VLOG(1) << "WatchRange key: " << key << " rangeEnd: " << rangeEnd << ", watchId: " << watchId;
    if (created) {
        watchDispatcher_->AddChannel(watchId, watcherAddr);
    }

    int64_t snapshotRevision = 0;
    memKvStore_->Range(key, rangeEnd, initialKvs, snapshotRevision);
    watchDispatcher_->SetSnapshotRevision(watchId, snapshotRevision);
    return Status::OK();
}

Status CoordinatorStore::CancelWatch(const std::string &watcherAddr, const std::vector<int64_t> &watchIds)
{
    VLOG(1) << "CancelWatch watcherAddr: " << watcherAddr << " watchIds: " << VectorToString(watchIds);
    RETURN_IF_NOT_OK(CheckInitialized());
    Status firstError;
    for (auto watchId : watchIds) {
        auto status = watchRegistry_->Cancel(watchId, watcherAddr);
        if (status.IsOk() || status.GetCode() == K_NOT_FOUND) {
            watchDispatcher_->RemoveChannel(watchId);
        } else if (firstError.IsOk()) {
            firstError = status;
        }
    }
    return firstError;
}

Status CoordinatorStore::KeepAlive(const std::string &key, int64_t &ttlMs, int64_t &remainingTtlMs)
{
    RETURN_IF_NOT_OK(CheckInitialized());
    int64_t revision = 0;
    uint64_t ttlGeneration = 0;
    RETURN_IF_NOT_OK(memKvStore_->KeepAlive(key, ttlMs, remainingTtlMs, revision, ttlGeneration));
    VLOG(1) << "KeepAlive key: " << key << " ttlMs: " << ttlMs << ", remainingTtlMs: " << remainingTtlMs;
    return ttlManager_->Schedule(key, ttlMs, revision, ttlGeneration);
}

void CoordinatorStore::SetCommittedMutationObserver(CommittedMutationObserver observer)
{
    committedMutationObserver_ = std::move(observer);
}

void CoordinatorStore::StopTtl()
{
    if (ttlManager_ != nullptr) {
        ttlManager_->Stop();
    }
}

void CoordinatorStore::Shutdown()
{
    if (shutdown_) {
        return;
    }
    shutdown_ = true;
    StopTtl();
    committedMutationObserver_ = {};
    if (memKvStore_ != nullptr) {
        memKvStore_->SetMutationCallback({});
    }
    if (watchDispatcher_ != nullptr) {
        watchDispatcher_->Stop();
    }
}
}  // namespace datasystem
