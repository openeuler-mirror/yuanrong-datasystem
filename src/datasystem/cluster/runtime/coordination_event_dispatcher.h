/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Bounded callback-to-role cluster runtime event queue.
 */
#ifndef DATASYSTEM_CLUSTER_RUNTIME_COORDINATION_EVENT_DISPATCHER_H
#define DATASYSTEM_CLUSTER_RUNTIME_COORDINATION_EVENT_DISPATCHER_H

#include <chrono>
#include <condition_variable>
#include <list>
#include <mutex>
#include <unordered_map>
#include <variant>

#include "datasystem/cluster/coordination_backend/coordination_backend.h"
#include "datasystem/cluster/executor/topology_task_executor.h"

namespace datasystem::cluster {

using RuntimeEventPayload = std::variant<CoordinationEvent, TopologyCallbackCompletion>;
struct RuntimeEvent {
    RuntimeEventPayload payload;
};
struct CoordinationEventDispatcherStats {
    size_t queueDepth{ 0 };
    uint64_t submitted{ 0 };
    uint64_t coalesced{ 0 };
    uint64_t overflow{ 0 };
    uint64_t ignoredAfterStop{ 0 };
    bool resyncRequired{ false };
};

/**
 * @brief Bounded queue owned and consumed by one role state thread.
 */
class CoordinationEventDispatcher final {
public:
    /**
     * @brief Construct a bounded queue.
     * @param[in] capacity Hard event capacity.
     */
    explicit CoordinationEventDispatcher(size_t capacity);

    /**
     * @brief Destroy after the role thread stops.
     */
    ~CoordinationEventDispatcher() = default;

    /**
     * @brief Disable copying a queue owner.
     */
    CoordinationEventDispatcher(const CoordinationEventDispatcher &) = delete;

    /**
     * @brief Disable copy assignment of a queue owner.
     */
    CoordinationEventDispatcher &operator=(const CoordinationEventDispatcher &) = delete;

    /**
     * @brief Start accepting once.
     * @return K_OK or K_INVALID.
     */
    Status Start();

    /**
     * @brief Stop ingress and wake the role thread.
     */
    void ShutdownIngress();

    /**
     * @brief Non-blockingly enqueue/coalesce a watch doorbell.
     * @param[in] event Move-only coordination event consumed as a doorbell.
     * @return K_OK on enqueue or coalescing; K_TRY_AGAIN on overflow; K_NOT_READY after shutdown.
     */
    Status SubmitCoordination(CoordinationEvent &&event);

    /**
     * @brief Non-blockingly enqueue a callback completion without dropping it silently.
     * @param[in] completion Move-only callback completion to enqueue.
     * @return K_OK on enqueue; K_TRY_AGAIN on overflow; K_NOT_READY after shutdown.
     */
    Status SubmitCompletion(TopologyCallbackCompletion completion);

    /**
     * @brief Pop by a monotonic deadline.
     * @param[in] deadline Absolute monotonic wait deadline.
     * @param[out] event Next queued event; unchanged when no event is returned.
     * @return K_OK on success; K_RPC_DEADLINE_EXCEEDED on timeout; K_NOT_READY after shutdown and drain.
     */
    Status WaitPop(std::chrono::steady_clock::time_point deadline, RuntimeEvent &event);

    /**
     * @brief Atomically consume the sticky full-resync request.
     * @return True when a resync request was pending.
     */
    bool ConsumeResyncRequired() noexcept;

    /**
     * @brief Return no-IO bounded queue statistics.
     * @return Current queue statistics.
     */
    CoordinationEventDispatcherStats GetStats() const;

private:
    /**
     * @brief Apply lifecycle, coalescing, and capacity rules to one runtime event.
     * @param[in] event Move-only event to enqueue.
     * @param[in] allowCoalesce Whether an equivalent coordination doorbell may be replaced.
     * @return K_OK on enqueue or coalescing; a lifecycle or capacity status otherwise.
     */
    Status Submit(RuntimeEvent event, bool allowCoalesce);
    const size_t capacity_;
    // Protects queue_, coordinationByKey_, started_, accepting_, resyncRequired_, and stats_.
    mutable std::mutex mutex_;
    // Uses mutex_ and signals changes to queue_ or accepting_.
    std::condition_variable cv_;
    std::list<RuntimeEvent> queue_;
    std::unordered_map<std::string, std::list<RuntimeEvent>::iterator> coordinationByKey_;
    bool started_{ false };
    bool accepting_{ false };
    bool resyncRequired_{ false };
    CoordinationEventDispatcherStats stats_;
};

}  // namespace datasystem::cluster

#endif  // DATASYSTEM_CLUSTER_RUNTIME_COORDINATION_EVENT_DISPATCHER_H
