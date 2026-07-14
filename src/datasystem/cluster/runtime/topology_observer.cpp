/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Standalone exact cluster topology Observer.
 */
#include "datasystem/cluster/runtime/topology_observer.h"

#include <exception>

#include "datasystem/cluster/runtime/topology_role_watch_plan.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem::cluster {
namespace {
constexpr auto OBSERVER_IDLE_WAIT = std::chrono::seconds(1);
constexpr size_t DIGEST_DIAGNOSTIC_PREFIX_SIZE = 12;
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
    std::vector<WatchKey> watches;
    RETURN_IF_NOT_OK(TopologyRoleWatchPlan::Build(TopologyRuntimeRole::OBSERVER, "", keys_, 0, watches));
    backend_.SetEventHandler(
        [this](CoordinationEvent &&event) { (void)dispatcher_.SubmitCoordination(std::move(event)); });
    auto watchStatus = backend_.WatchEvents(watches);
    if (watchStatus.IsError()) {
        dispatcher_.ShutdownIngress();
        LOG_IF_ERROR(backend_.ShutdownEventSources(), "Shut down topology Observer event sources after Start failure");
        backend_.SetEventHandler(ICoordinationBackend::EventHandler{});
        return watchStatus;
    }
    LOG(INFO) << "CLUSTER_WATCH cluster=" << keys_.ClusterName() << " role=observer scope_count=" << watches.size()
              << " revision=0 status=registered";
    auto readStatus = Reload(true);
    if (readStatus.IsError()) {
        RecordReadFailure(readStatus);
        dispatcher_.ShutdownIngress();
        LOG_IF_ERROR(backend_.ShutdownEventSources(),
                     "Shut down topology Observer event sources after initial read failure");
        backend_.SetEventHandler(ICoordinationBackend::EventHandler{});
        return readStatus;
    } else {
        std::shared_ptr<const TopologySnapshot> snapshot;
        if (snapshots_.Load(snapshot).IsOk()) {
            diagnostics_.topologyVersion = snapshot->Version();
            diagnostics_.lastError.clear();
        }
    }
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
        dispatcher_.ShutdownIngress();
        LOG_IF_ERROR(backend_.ShutdownEventSources(),
                     "Shut down topology Observer event sources after thread Start failure");
        backend_.SetEventHandler(ICoordinationBackend::EventHandler{});
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
    if (rc.IsError() && outcome == SnapshotUpdateOutcome::VERSION_GAP && fullRebuildAllowed) {
        rc = snapshots_.PublishAfterFullRebuild(snapshot);
    }
    RETURN_IF_NOT_OK(rc);
    VLOG(1) << "CLUSTER_RING cluster=" << keys_.ClusterName() << " version=" << snapshot->Version()
            << " digest_prefix=" << snapshot->CanonicalDigest().substr(0, DIGEST_DIAGNOSTIC_PREFIX_SIZE)
            << " status=published";
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
                             << " role=observer scope=topology status=resync";
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
