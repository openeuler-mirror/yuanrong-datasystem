/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Standalone exact cluster topology Observer.
 */
#include "datasystem/cluster/runtime/topology_observer.h"

#include <exception>

#include "datasystem/cluster/model/topology_diagnostics.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/util/random_data.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem::cluster {
namespace {
constexpr uint32_t POLL_MIN_MS = 900;
constexpr uint32_t POLL_MAX_MS = 1100;
}

TopologyObserver::TopologyObserver(TopologyReader &reader, const TopologyKeyHelper &keys)
    : reader_(reader), keys_(keys)
{
}

TopologyObserver::~TopologyObserver()
{
    LOG_IF_ERROR(Shutdown(std::chrono::steady_clock::time_point::max()),
                 "Shut down cluster topology Observer during destruction");
}

Status TopologyObserver::Start()
{
    std::lock_guard<std::mutex> lock(stateMutex_);
    CHECK_FAIL_RETURN_STATUS(!started_, K_INVALID, "cluster topology Observer already started");
    RETURN_IF_NOT_OK(ReloadInitialSnapshot());
    return StartStateThread();
}

Status TopologyObserver::ReloadInitialSnapshot()
{
    auto readStatus = Reload();
    if (readStatus.IsError()) {
        RecordReadFailure(readStatus);
        return readStatus;
    }
    std::shared_ptr<const TopologySnapshot> snapshot;
    if (snapshots_.Load(snapshot).IsOk()) {
        diagnostics_.topologyVersion = snapshot->Version();
        diagnostics_.lastError.clear();
    }
    return Status::OK();
}

Status TopologyObserver::StartStateThread()
{
    started_ = true;
    stopping_ = false;
    threadExited_ = false;
    diagnostics_.running = true;
    try {
        stateThread_ = Thread(&TopologyObserver::Run, this);
        stateThread_.set_name("cluster-obs");
    } catch (const std::exception &error) {
        started_ = false;
        threadExited_ = true;
        diagnostics_.running = false;
        RETURN_STATUS(K_RUNTIME_ERROR, std::string("start cluster topology Observer failed: ") + error.what());
    }
    LOG(INFO) << "CLUSTER_LIFECYCLE cluster=" << keys_.ClusterName() << " role=observer state=ready";
    return Status::OK();
}

Status TopologyObserver::GetSnapshot(std::shared_ptr<const TopologySnapshot> &snapshot) const
{
    return snapshots_.Load(snapshot);
}

void TopologyObserver::RecordReadFailure(const Status &status)
{
    ++diagnostics_.readFailures;
    diagnostics_.lastError = status.ToString();
}

Status TopologyObserver::Reload()
{
    std::shared_ptr<const TopologySnapshot> snapshot;
    std::shared_ptr<const TopologySnapshot> current;
    if (snapshots_.Load(current).IsOk() && current->AuthorityRevision() > 0) {
        bool unchanged = false;
        RETURN_IF_NOT_OK(reader_.ReadIfChanged(OBSERVER_READ_TIMEOUT_MS, *current, snapshot, unchanged));
        RETURN_OK_IF_TRUE(unchanged);
    } else {
        RETURN_IF_NOT_OK(reader_.ReadTopologyOnly(OBSERVER_READ_TIMEOUT_MS, snapshot));
    }
    SnapshotUpdateOutcome outcome;
    auto rc = snapshots_.Publish(snapshot, outcome);
    bool newlyPublished = rc.IsOk() && outcome == SnapshotUpdateOutcome::PUBLISHED;
    if (rc.IsError() && outcome == SnapshotUpdateOutcome::VERSION_GAP) {
        rc = snapshots_.PublishAfterFullRebuild(snapshot);
        newlyPublished = rc.IsOk();
    }
    RETURN_IF_NOT_OK(rc);
    VLOG(TOPOLOGY_VERBOSE_LOG_LEVEL)
        << "CLUSTER_RING cluster=" << keys_.ClusterName() << " version=" << snapshot->Version()
        << " digest_prefix=" << TopologyDiagnosticPrefix(snapshot->CanonicalDigest()) << " status=published";
    if (newlyPublished) {
        const auto activeBatch = snapshot->GetActiveBatch();
        const auto batchType =
            activeBatch.has_value() ? TopologyChangeTypeName(activeBatch->type) : "none";
        const auto batchEpoch =
            activeBatch.has_value() ? activeBatch->epoch : TOPOLOGY_NO_ACTIVE_BATCH_EPOCH;
        LOG(INFO) << "CLUSTER_RING cluster=" << keys_.ClusterName() << " role=observer status=published"
                  << " version=" << snapshot->Version()
                  << " authority_revision=" << snapshot->AuthorityRevision()
                  << " digest_prefix=" << TopologyDiagnosticPrefix(snapshot->CanonicalDigest())
                  << " batch_type=" << batchType << " batch_epoch=" << batchEpoch
                  << " member_count=" << snapshot->Members().size()
                  << " active_count=" << snapshot->ActiveMembers().size()
                  << " failed_count=" << snapshot->FailedMembers().size()
                  << " " << TopologyRingViewsForLog(*snapshot);
    }
    return Status::OK();
}

void TopologyObserver::Run()
{
    RandomData random;
    std::unique_lock<std::mutex> lock(stateMutex_);
    while (!stateCv_.wait_for(lock, std::chrono::milliseconds(random.GetRandomUint32(POLL_MIN_MS, POLL_MAX_MS)),
                              [this] { return stopping_; })) {
        lock.unlock();
        auto rc = Reload();
        lock.lock();
        if (rc.IsError()) {
            RecordReadFailure(rc);
            continue;
        }
        std::shared_ptr<const TopologySnapshot> snapshot;
        if (snapshots_.Load(snapshot).IsOk()) {
            diagnostics_.topologyVersion = snapshot->Version();
            diagnostics_.lastError.clear();
        }
    }
    threadExited_ = true;
    diagnostics_.running = false;
    stateCv_.notify_all();
}

Status TopologyObserver::Shutdown(std::chrono::steady_clock::time_point deadline)
{
    std::unique_lock<std::mutex> lock(stateMutex_);
    if (!started_) {
        return Status::OK();
    }
    stopping_ = true;
    LOG(INFO) << "CLUSTER_LIFECYCLE cluster=" << keys_.ClusterName() << " role=observer state=stopping";
    diagnostics_.stopping = true;
    stateCv_.notify_all();
    if (!stateCv_.wait_until(lock, deadline, [this] { return threadExited_; })) {
        ++diagnostics_.shutdownTimeouts;
        RETURN_STATUS(K_RPC_DEADLINE_EXCEEDED, "cluster topology Observer shutdown deadline exceeded");
    }
    if (stateThread_.joinable()) {
        stateThread_.join();
    }
    started_ = false;
    stopping_ = false;
    diagnostics_.stopping = false;
    return Status::OK();
}

TopologyObserverDiagnostics TopologyObserver::GetDiagnostics() const
{
    std::lock_guard<std::mutex> lock(stateMutex_);
    return diagnostics_;
}

}  // namespace datasystem::cluster
