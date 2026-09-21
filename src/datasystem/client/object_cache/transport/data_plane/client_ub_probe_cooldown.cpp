/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

#include "datasystem/client/object_cache/transport/data_plane/client_ub_probe_cooldown.h"

#include <iterator>
#include <limits>
#include <mutex>

namespace datasystem::client {
namespace {
constexpr uint64_t TIMESTAMP_HALF_RANGE = uint64_t{ 1 } << (std::numeric_limits<uint64_t>::digits - 1);

bool IsBeforeDeadline(uint64_t observedMs, uint64_t deadlineMs)
{
    // Serial-number comparison preserves the one-second deadline across uint64_t timestamp wrap.
    const uint64_t remainingMs = deadlineMs - observedMs;
    return remainingMs != 0 && remainingMs < TIMESTAMP_HALF_RANGE;
}
}  // namespace

bool ClientUbProbeCooldown::TryAcquire(const HostPort &destination, ClientUbProbeScope scope,
                                       uint64_t generation, uint64_t observedMs)
{
    if (destination.Empty()) {
        return false;
    }
    std::lock_guard<bthread::Mutex> lock(mutex_);
    auto &entries = scope == ClientUbProbeScope::LOCAL_NODE ? localNodeEntries_ : remoteWorkerEntries_;
    auto [iter, inserted] = entries.try_emplace(destination);
    auto &entry = iter->second;
    if (!inserted && generation < entry.generation) {
        return false;
    }
    if (inserted || entry.generation != generation) {
        entry.generation = generation;
        entry.suppressUntilMs = observedMs + COOLDOWN_MS;
        return true;
    }
    if (IsBeforeDeadline(observedMs, entry.suppressUntilMs)) {
        return false;
    }
    entry.suppressUntilMs = observedMs + COOLDOWN_MS;
    return true;
}

void ClientUbProbeCooldown::Reconcile(const std::unordered_set<HostPort> &destinations)
{
    std::lock_guard<bthread::Mutex> lock(mutex_);
    localNodeEntries_.reserve(destinations.size());
    remoteWorkerEntries_.reserve(destinations.size());
    auto eraseRemoved = [&destinations](Entries &entries) {
        for (auto iter = entries.begin(); iter != entries.end();) {
            iter = destinations.count(iter->first) == 0 ? entries.erase(iter) : std::next(iter);
        }
    };
    eraseRemoved(localNodeEntries_);
    eraseRemoved(remoteWorkerEntries_);
}
}  // namespace datasystem::client
