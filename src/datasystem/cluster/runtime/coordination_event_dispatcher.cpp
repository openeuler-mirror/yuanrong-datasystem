/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Bounded callback-to-role cluster runtime event queue.
 */
#include "datasystem/cluster/runtime/coordination_event_dispatcher.h"

#include <utility>

#include "datasystem/common/log/log.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem::cluster {

CoordinationEventDispatcher::CoordinationEventDispatcher(size_t capacity) : capacity_(capacity)
{
}

Status CoordinationEventDispatcher::Start()
{
    std::lock_guard<std::mutex> lock(mutex_);
    CHECK_FAIL_RETURN_STATUS(capacity_ > 0 && !accepting_, K_INVALID, "invalid or already started runtime queue");
    queue_.clear();
    coordinationByKey_.clear();
    resyncRequired_ = false;
    stats_.queueDepth = 0;
    started_ = true;
    accepting_ = true;
    return Status::OK();
}

void CoordinationEventDispatcher::ShutdownIngress()
{
    std::lock_guard<std::mutex> lock(mutex_);
    accepting_ = false;
    cv_.notify_all();
}

Status CoordinationEventDispatcher::Submit(RuntimeEvent event, bool allowCoalesce)
{
    std::lock_guard<std::mutex> lock(mutex_);
    if (!accepting_) {
        ++stats_.ignoredAfterStop;
        RETURN_STATUS(K_NOT_READY, "cluster runtime event ingress stopped");
    }
    if (allowCoalesce) {
        auto &incoming = std::get<CoordinationEvent>(event.payload);
        auto pending = coordinationByKey_.find(incoming.key);
        if (pending != coordinationByKey_.end()) {
            auto *doorbell = std::get_if<CoordinationEvent>(&pending->second->payload);
            CHECK_FAIL_RETURN_STATUS(doorbell != nullptr, K_RUNTIME_ERROR,
                                     "cluster runtime coordination index is inconsistent");
            *doorbell = std::move(incoming);
            ++stats_.submitted;
            ++stats_.coalesced;
            return Status::OK();
        }
    }
    if (queue_.size() >= capacity_) {
        ++stats_.overflow;
        resyncRequired_ = resyncRequired_ || allowCoalesce;
        LOG(WARNING) << "CLUSTER_WATCH_QUEUE action=overflow queue_depth=" << queue_.size()
                     << " capacity=" << capacity_ << " allow_coalesce=" << allowCoalesce
                     << " overflow=" << stats_.overflow << " resync_required=" << resyncRequired_;
        RETURN_STATUS(K_TRY_AGAIN, "cluster runtime event queue overflow");
    }
    queue_.emplace_back(std::move(event));
    if (allowCoalesce) {
        const auto &doorbell = std::get<CoordinationEvent>(queue_.back().payload);
        coordinationByKey_.emplace(doorbell.key, std::prev(queue_.end()));
    }
    ++stats_.submitted;
    stats_.queueDepth = queue_.size();
    cv_.notify_one();
    return Status::OK();
}

Status CoordinationEventDispatcher::SubmitCoordination(CoordinationEvent &&event)
{
    return Submit(RuntimeEvent{ RuntimeEventPayload{ std::move(event) } }, true);
}

Status CoordinationEventDispatcher::SubmitCompletion(TopologyCallbackCompletion completion)
{
    return Submit(RuntimeEvent{ RuntimeEventPayload{ std::move(completion) } }, false);
}

Status CoordinationEventDispatcher::WaitPop(std::chrono::steady_clock::time_point deadline, RuntimeEvent &event)
{
    std::unique_lock<std::mutex> lock(mutex_);
    if (queue_.empty() && !cv_.wait_until(lock, deadline, [this] { return !queue_.empty() || !accepting_; })) {
        RETURN_STATUS(K_RPC_DEADLINE_EXCEEDED, "cluster runtime event wait expired");
    }
    CHECK_FAIL_RETURN_STATUS(!queue_.empty(), K_NOT_READY, "cluster runtime event queue stopped");
    event = std::move(queue_.front());
    if (auto *doorbell = std::get_if<CoordinationEvent>(&event.payload)) {
        coordinationByKey_.erase(doorbell->key);
    }
    queue_.pop_front();
    stats_.queueDepth = queue_.size();
    return Status::OK();
}

bool CoordinationEventDispatcher::ConsumeResyncRequired() noexcept
{
    std::lock_guard<std::mutex> lock(mutex_);
    return std::exchange(resyncRequired_, false);
}

CoordinationEventDispatcherStats CoordinationEventDispatcher::GetStats() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    auto stats = stats_;
    stats.resyncRequired = resyncRequired_;
    return stats;
}

}  // namespace datasystem::cluster
