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
 * Description: Manages N same-host workers' independent shm connections. See
 * shm-and-heartbeat design §4.1/§4.3.2/§4.6.
 */
#include "datasystem/client/transport/shm_connection_pool.h"

#include <utility>
#include <vector>

namespace datasystem {
namespace client {
ShmConnectionPool::ShmConnectionPool(std::string sdkHostId, ShmConnector connector)
    : sdkHostId_(std::move(sdkHostId)), connector_(std::move(connector))
{
}

ShmConnectionPool::~ShmConnectionPool()
{
    ReleaseAll();
}

const ShmEntry *ShmConnectionPool::GetOrCreateShm(const HostPort &workerAddr)
{
    const std::string key = workerAddr.ToString();
    // Fast path: an entry already exists and is valid — return it under a shared (read) lock.
    {
        EntryMap::const_accessor accessor;
        if (entries_.find(accessor, key) && accessor->second.IsValid()) {
            return &accessor->second;
        }
    }
    // Build OUTSIDE the per-key write lock so concurrent callers for the same worker are not
    // serialized on the (slow, hundreds-of-ms) handshake (review fix #6). If the connector throws,
    // `fresh` is not yet in the map and its socketFd is closed by ~ShmFd — no half-built residue
    // (review fix #1).
    ShmEntry fresh;
    fresh.workerAddr = workerAddr;
    Status rc = connector_(workerAddr, fresh);
    if (rc.IsError()) {
        return nullptr;
    }
    // Insert-or-reuse under the write lock: insert() returns true only for the winner. A loser
    // (another thread built concurrently) discards its entry; ~ShmFd closes the duplicate socketFd.
    EntryMap::accessor accessor;
    bool inserted = entries_.insert(accessor, key);
    if (inserted) {
        accessor->second = std::move(fresh);
    }
    // fresh (if not moved) is destroyed here; its ShmFd closes the duplicate fd.
    return &accessor->second;
}

void ShmConnectionPool::ReleaseShm(const HostPort &workerAddr)
{
    const std::string key = workerAddr.ToString();
    EntryMap::accessor accessor;
    if (!entries_.find(accessor, key)) {
        return; // idempotent: nothing pooled for this worker.
    }
    // Erase while we still hold the accessor; the ShmFd inside the erased entry closes the socketFd
    // in its destructor. mmapRegion ownership lives in MmapManager (which does the actual munmap via
    // the shm_id path); the pool does not munmap here to avoid double-free with ClearByShmId.
    entries_.erase(accessor);
}

void ShmConnectionPool::ReleaseAll()
{
    // Snapshot keys, then release each: releasing erases from the map, and iterating a
    // concurrent_hash_map while mutating is unsafe, so we collect keys first.
    std::vector<std::string> keys;
    keys.reserve(entries_.size());
    for (const auto &kv : entries_) {
        keys.emplace_back(kv.first);
    }
    for (const auto &k : keys) {
        HostPort addr;
        if (addr.ParseString(k).IsOk()) {
            ReleaseShm(addr);
        } else {
            EntryMap::accessor accessor;
            if (entries_.find(accessor, k)) {
                entries_.erase(accessor); // ShmFd destructor closes the socketFd.
            }
        }
    }
}

const ShmEntry *ShmConnectionPool::GetShmEntry(const HostPort &workerAddr) const
{
    const std::string key = workerAddr.ToString();
    EntryMap::const_accessor accessor;
    if (!entries_.find(accessor, key)) {
        return nullptr;
    }
    return accessor->second.IsValid() ? &accessor->second : nullptr;
}

size_t ShmConnectionPool::Size() const
{
    return entries_.size();
}
}  // namespace client
}  // namespace datasystem
