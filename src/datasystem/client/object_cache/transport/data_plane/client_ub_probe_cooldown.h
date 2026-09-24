/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
#ifndef DATASYSTEM_CLIENT_UB_PROBE_COOLDOWN_H
#define DATASYSTEM_CLIENT_UB_PROBE_COOLDOWN_H

#include <cstdint>
#include <unordered_map>
#include <unordered_set>

#include <bthread/mutex.h>

#include "datasystem/common/util/net_util.h"

namespace datasystem::client {
enum class ClientUbProbeScope : uint8_t { LOCAL_NODE = 0, REMOTE_WORKER = 1 };

class ClientUbProbeCooldown {
public:
    static constexpr uint64_t COOLDOWN_MS = 1'000;

    ~ClientUbProbeCooldown() = default;

    /**
     * @brief Admit the first probe for one destination and suppress the same scope for one second.
     * @param[in] destination Data-plane destination that produced the CQE.
     * @param[in] scope Whether the evidence requires a local-node or remote-Worker probe.
     * @param[in] generation Destination generation captured when the operation was submitted.
     * @param[in] observedMs Monotonic timestamp at the CQE observation point.
     * @return true for the first observation and the first observation after each cooldown.
     */
    bool TryAcquire(const HostPort &destination, ClientUbProbeScope scope, uint64_t generation,
                    uint64_t observedMs);

    /** @brief Remove cooldown state for destinations no longer present in the Worker snapshot. */
    void Reconcile(const std::unordered_set<HostPort> &destinations);

private:
    struct Entry {
        uint64_t generation = 0;
        uint64_t suppressUntilMs = 0;
    };
    using Entries = std::unordered_map<HostPort, Entry>;

    bthread::Mutex mutex_;
    Entries localNodeEntries_;
    Entries remoteWorkerEntries_;
};
}  // namespace datasystem::client

#endif  // DATASYSTEM_CLIENT_UB_PROBE_COOLDOWN_H
