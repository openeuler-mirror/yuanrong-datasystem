/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

#ifndef DATASYSTEM_CLUSTER_UB_HEALTH_UB_HEALTH_LEASE_SYNC_H
#define DATASYSTEM_CLUSTER_UB_HEALTH_UB_HEALTH_LEASE_SYNC_H

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "datasystem/cluster/coordination_backend/coordination_backend.h"
#include "datasystem/common/object_cache/peer_ub_admission.h"
#include "datasystem/common/util/thread.h"

namespace datasystem::cluster {
class UbHealthLeaseSync {
public:
    using SummaryProvider = std::function<UbHealthSummary()>;
    using IncarnationProvider = std::function<Status(std::string &)>;
    using SnapshotConsumer = std::function<void(const std::vector<UbHealthSummary> &)>;
    using LeasePublisher = std::function<Status(const std::string &, const std::string &, const std::string &)>;
    using SnapshotLoader =
        std::function<Status(const std::string &, std::vector<std::pair<std::string, std::string>> &)>;

    struct Config {
        std::string tableName;
        HostPort self;
        IncarnationProvider incarnationProvider;
        SummaryProvider provider;
        SnapshotConsumer consumer;
        std::chrono::milliseconds interval = std::chrono::seconds(1);
    };

    UbHealthLeaseSync(ICoordinationBackend &backend, Config config);
    UbHealthLeaseSync(LeasePublisher publisher, SnapshotLoader loader, Config config);
    ~UbHealthLeaseSync();

    Status Start();
    void Stop();
    Status SyncOnce();

private:
    void Run();

    LeasePublisher publisher_;
    SnapshotLoader loader_;
    std::string tableName_;
    HostPort self_;
    IncarnationProvider incarnationProvider_;
    SummaryProvider provider_;
    SnapshotConsumer consumer_;
    std::chrono::milliseconds interval_;
    std::atomic<bool> started_{ false };
    std::atomic<bool> stopping_{ false };
    std::mutex mutex_;
    std::mutex syncMutex_;
    std::condition_variable cv_;
    Thread thread_;
    std::unordered_map<std::string, UbHealthSummary> lastSummaries_;
};
}  // namespace datasystem::cluster

#endif  // DATASYSTEM_CLUSTER_UB_HEALTH_UB_HEALTH_LEASE_SYNC_H
