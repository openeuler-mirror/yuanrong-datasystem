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
#include "datasystem/cluster/runtime/topology_role_watch_plan.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem::cluster {
namespace {
constexpr auto OBSERVER_IDLE_WAIT = std::chrono::seconds(1);
constexpr int TOPOLOGY_WATCH_EVENT_LOG_INTERVAL = 1'024;
}

TopologyObserver::TopologyObserver(ICoordinationBackend &backend, TopologyReader &reader, const TopologyKeyHelper &keys)
    : backend_(backend), reader_(reader), keys_(keys)
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
    RETURN_IF_NOT_OK(dispatcher_.Start());
    RETURN_IF_NOT_OK(RegisterWatchEvents());
    RETURN_IF_NOT_OK(ReloadInitialSnapshot());
    return StartStateThread();
}

void TopologyObserver::InstallEventHandler()
{
    backend_.SetEventHandler([this](CoordinationEvent &&event) {
        LOG_FIRST_AND_EVERY_N(INFO, TOPOLOGY_WATCH_EVENT_LOG_INTERVAL)
            << "CLUSTER_WATCH_EVENT cluster=" << keys_.ClusterName() << " role=observer event="
            << event.ToString();
        auto rc = dispatcher_.SubmitCoordination(std::move(event));
        if (rc.IsError()) {
            LOG(WARNING) << "CLUSTER_WATCH_QUEUE cluster=" << keys_.ClusterName()
                         << " role=observer action=submit_failed status=" << rc.ToString();
        }
    });
}

void TopologyObserver::CleanupAfterStartFailure(const char *cleanupMessage)
{
    dispatcher_.ShutdownIngress();
    LOG_IF_ERROR(backend_.ShutdownEventSources(), cleanupMessage);
    backend_.SetEventHandler(ICoordinationBackend::EventHandler{});
}

Status TopologyObserver::RegisterWatchEvents()
{
    std::vector<WatchKey> watches;
    RETURN_IF_NOT_OK(TopologyRoleWatchPlan::Build(TopologyRuntimeRole::OBSERVER, "", keys_, 0, watches));
    InstallEventHandler();
    auto watchStatus = backend_.WatchEvents(watches);
    if (watchStatus.IsError()) {
        CleanupAfterStartFailure("Shut down topology Observer event sources after Start failure");
        return watchStatus;
    }
    LOG(INFO) << "CLUSTER_WATCH cluster=" << keys_.ClusterName() << " role=observer scope_count=" << watches.size()
              << " revision=0 status=registered";
    return Status::OK();
}

Status TopologyObserver::ReloadInitialSnapshot()
{
    auto readStatus = Reload(true);
    if (readStatus.IsError()) {
        RecordReadFailure(readStatus);
        CleanupAfterStartFailure("Shut down topology Observer event sources after initial read failure");
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
        CleanupAfterStartFailure("Shut down topology Observer event sources after thread Start failure");
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

Status TopologyObserver::Reload(bool fullRebuildAllowed)
{
    std::shared_ptr<const TopologySnapshot> snapshot;
    RETURN_IF_NOT_OK(reader_.Read(OBSERVER_READ_TIMEOUT_MS, snapshot));
    SnapshotUpdateOutcome outcome;
    auto rc = snapshots_.Publish(snapshot, outcome);
    bool newlyPublished = rc.IsOk() && outcome == SnapshotUpdateOutcome::PUBLISHED;
    if (rc.IsError() && outcome == SnapshotUpdateOutcome::VERSION_GAP && fullRebuildAllowed) {
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
                  << " failed_count=" << snapshot->FailedMembers().size();
    }
    return Status::OK();
}

void TopologyObserver::Run()
{
    while (true) {
        {
            std::lock_guard<std::mutex> lock(stateMutex_);
            if (stopping_) {
                break;
            }
        }
        RuntimeEvent event;
        auto rc = dispatcher_.WaitPop(std::chrono::steady_clock::now() + OBSERVER_IDLE_WAIT, event);
        if (rc.GetCode() == K_RPC_DEADLINE_EXCEEDED) {
            rc = Reload(true);
        } else if (rc.IsOk()) {
            if (dispatcher_.ConsumeResyncRequired()) {
                LOG(WARNING) << "CLUSTER_WATCH cluster=" << keys_.ClusterName()
                             << " role=observer scope=topology status=resync queued_events="
                             << dispatcher_.GetStats().queueDepth;
            }
            rc = Reload(true);
        }
        if (rc.IsError()) {
            std::lock_guard<std::mutex> lock(stateMutex_);
            if (stopping_) {
                break;
            }
            RecordReadFailure(rc);
            continue;
        }
        std::shared_ptr<const TopologySnapshot> snapshot;
        if (snapshots_.Load(snapshot).IsOk()) {
            std::lock_guard<std::mutex> lock(stateMutex_);
            diagnostics_.topologyVersion = snapshot->Version();
            diagnostics_.lastError.clear();
        }
    }
    std::lock_guard<std::mutex> lock(stateMutex_);
    threadExited_ = true;
    diagnostics_.running = false;
    stoppedCv_.notify_all();
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
    dispatcher_.ShutdownIngress();
    const auto eventSourceStatus = backend_.ShutdownEventSources();
    backend_.SetEventHandler(ICoordinationBackend::EventHandler{});
    if (!stoppedCv_.wait_until(lock, deadline, [this] { return threadExited_; })) {
        ++diagnostics_.shutdownTimeouts;
        RETURN_STATUS(K_RPC_DEADLINE_EXCEEDED, "cluster topology Observer shutdown deadline exceeded");
    }
    lock.unlock();
    if (stateThread_.joinable()) {
        stateThread_.join();
    }
    lock.lock();
    started_ = false;
    stopping_ = false;
    diagnostics_.stopping = false;
    if (eventSourceStatus.IsError()) {
        return eventSourceStatus;
    }
    return Status::OK();
}

TopologyObserverDiagnostics TopologyObserver::GetDiagnostics() const
{
    std::lock_guard<std::mutex> lock(stateMutex_);
    auto diagnostics = diagnostics_;
    diagnostics.resyncRequired = dispatcher_.GetStats().resyncRequired;
    return diagnostics;
}

}  // namespace datasystem::cluster
