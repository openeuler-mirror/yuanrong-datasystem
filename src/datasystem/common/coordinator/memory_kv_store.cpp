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

#include "datasystem/common/coordinator/memory_kv_store.h"

#include <mutex>

namespace datasystem {
Status MemoryKvStore::Put(const std::string &key, const std::string &value, int64_t ttlMs, int64_t expectedVersion,
                          int64_t &version, int64_t &revision, uint64_t &ttlGeneration)
{
    std::shared_ptr<WatchEvent> event;

    {
        std::unique_lock lock(mutex_);

        auto it = data_.find(key);
        bool exists = (it != data_.end());

        if (expectedVersion != 0) {
            if (!exists) {
                return Status(StatusCode::K_NOT_FOUND, "key not found for CAS");
            }
            if (it->second.version != expectedVersion) {
                return Status(StatusCode::K_INVALID, "version mismatch");
            }
        }

        int64_t newRevision = revision_.fetch_add(1, std::memory_order_relaxed) + 1;

        ValueEntry *stored = nullptr;
        if (exists) {
            auto &entry = it->second;
            entry.version++;
            entry.value = value;
            entry.modRevision = newRevision;
            entry.ttlMs = ttlMs;
            entry.ttlGeneration++;
            stored = &entry;
        } else {
            ValueEntry entry{ value, 1, newRevision, newRevision, ttlMs, 1 };
            auto result = data_.emplace(key, std::move(entry));
            stored = &result.first->second;
        }

        version = stored->version;
        revision = newRevision;
        ttlGeneration = stored->ttlGeneration;

        if (mutationCallback_) {
            event = std::make_shared<WatchEvent>();
            event->type = WatchEvent::Type::PUT;
            event->entry = KeyValueEntry{ key, stored->value, stored->version, stored->modRevision };
            event->revision = newRevision;
        }
    }

    if (mutationCallback_) {
        mutationCallback_(std::move(event));
    }
    return Status::OK();
}

void MemoryKvStore::Range(const std::string &key, const std::string &rangeEnd, std::vector<KeyValueEntry> &kvs,
                          int64_t &revision)
{
    std::shared_lock lock(mutex_);
    revision = revision_.load(std::memory_order_relaxed);

    if (rangeEnd.empty()) {
        auto it = data_.find(key);
        if (it != data_.end()) {
            kvs.push_back(KeyValueEntry{ it->first, it->second.value, it->second.version, it->second.modRevision });
        }
    } else {
        for (auto it = data_.lower_bound(key); it != data_.end() && it->first < rangeEnd; ++it) {
            kvs.push_back(KeyValueEntry{ it->first, it->second.value, it->second.version, it->second.modRevision });
        }
    }
}

void MemoryKvStore::Delete(const std::string &key, const std::string &rangeEnd, int64_t &deleted, int64_t &revision,
                           std::vector<KeyValueEntry> &deletedEntries)
{
    std::vector<std::shared_ptr<WatchEvent>> events;

    {
        std::unique_lock lock(mutex_);

        std::vector<std::map<std::string, ValueEntry>::iterator> toDelete;
        if (rangeEnd.empty()) {
            auto it = data_.find(key);
            if (it != data_.end()) {
                toDelete.push_back(it);
            }
        } else {
            for (auto it = data_.lower_bound(key); it != data_.end() && it->first < rangeEnd; ++it) {
                toDelete.push_back(it);
            }
        }

        if (toDelete.empty()) {
            deleted = 0;
            revision = revision_.load(std::memory_order_relaxed);
            return;
        }

        int64_t newRevision = revision_.fetch_add(1, std::memory_order_relaxed) + 1;

        for (auto &it : toDelete) {
            KeyValueEntry kve{ it->first, it->second.value, it->second.version, it->second.modRevision };
            deletedEntries.push_back(kve);

            if (mutationCallback_) {
                auto event = std::make_shared<WatchEvent>();
                event->type = WatchEvent::Type::DELETE;
                event->entry = kve;
                event->revision = newRevision;
                events.push_back(std::move(event));
            }

            data_.erase(it);
        }

        deleted = static_cast<int64_t>(deletedEntries.size());
        revision = newRevision;
    }

    if (mutationCallback_) {
        for (auto &event : events) {
            mutationCallback_(std::move(event));
        }
    }
}

bool MemoryKvStore::DeleteIfTtlExpired(const std::string &key, int64_t revision, uint64_t ttlGeneration)
{
    std::shared_ptr<WatchEvent> event;
    {
        std::unique_lock lock(mutex_);
        auto it = data_.find(key);
        if (it == data_.end() || it->second.modRevision != revision) {
            return false;
        }
        if (it->second.ttlMs <= 0 || it->second.ttlGeneration != ttlGeneration) {
            return false;
        }

        int64_t deleteRevision = revision_.fetch_add(1, std::memory_order_relaxed) + 1;
        KeyValueEntry kve{ it->first, it->second.value, it->second.version, it->second.modRevision };
        if (mutationCallback_) {
            event = std::make_shared<WatchEvent>();
            event->type = WatchEvent::Type::DELETE;
            event->entry = kve;
            event->revision = deleteRevision;
        }
        data_.erase(it);
    }

    if (mutationCallback_) {
        mutationCallback_(std::move(event));
    }
    return true;
}

Status MemoryKvStore::KeepAlive(const std::string &key, int64_t &ttlMs, int64_t &remainingTtlMs, int64_t &revision,
                                uint64_t &ttlGeneration)
{
    std::unique_lock lock(mutex_);
    auto it = data_.find(key);
    if (it == data_.end() || it->second.ttlMs <= 0) {
        return Status(StatusCode::K_NOT_FOUND, "key has no TTL");
    }

    auto &entry = it->second;
    entry.ttlGeneration++;
    ttlMs = entry.ttlMs;
    remainingTtlMs = entry.ttlMs;
    revision = entry.modRevision;
    ttlGeneration = entry.ttlGeneration;
    return Status::OK();
}

int64_t MemoryKvStore::CurrentRevision() const
{
    return revision_.load(std::memory_order_relaxed);
}

void MemoryKvStore::SetMutationCallback(std::function<void(std::shared_ptr<WatchEvent>)> callback)
{
    mutationCallback_ = std::move(callback);
}
}  // namespace datasystem
