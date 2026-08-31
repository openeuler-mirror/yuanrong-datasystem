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

#include "datasystem/client/routing/hash_ring_refresher.h"

#include <algorithm>
#include <utility>

#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/log/trace.h"
#include "datasystem/common/util/uuid_generator.h"

namespace datasystem {
namespace client {
namespace {
constexpr int TOPOLOGY_PUBLISH_FAILURE_LOG_EVERY_N = 10;

int64_t SteadyNowMs()
{
    return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

HashRingRefresher::TimedFetchRpc AdaptFetchRpc(HashRingRefresher::FetchRpc fetchRpc)
{
    if (!fetchRpc) {
        return {};
    }
    return [fetch = std::move(fetchRpc)](const HostPort &workerAddr, uint64_t currentVersion,
                                         ::datasystem::ClusterTopologyPb &ring, std::string &masterAddress,
                                         uint64_t &newVersion, bool &changed,
                                         std::unordered_map<std::string, std::string> &hostIdMap, int32_t) {
        return fetch(workerAddr, currentVersion, ring, masterAddress, newVersion, changed, hostIdMap);
    };
}

void LogTopologyPublishFailure(const HostPort &worker, uint64_t requestedVersion, uint64_t responseVersion,
                               uint64_t currentVersion, const Status &status)
{
    LOG_FIRST_EVERY_N(WARNING, TOPOLOGY_PUBLISH_FAILURE_LOG_EVERY_N)
        << "[Routing] Reject hash ring refresh from " << worker.ToString()
        << ", requested version: " << requestedVersion << ", response version: " << responseVersion
        << ", current version: " << currentVersion << ", status: " << status.ToString();
}
}  // namespace

HashRingRefresher::HashRingRefresher(std::shared_ptr<WorkerRouter> router, FetchRpc fetchRpc,
                                     RingUpdateHook ringUpdateHook, WaitFn waitFn)
    : HashRingRefresher(std::move(router), AdaptFetchRpc(std::move(fetchRpc)), std::move(ringUpdateHook),
                        std::move(waitFn))
{
}

HashRingRefresher::HashRingRefresher(std::shared_ptr<WorkerRouter> router, TimedFetchRpc fetchRpc,
                                     RingUpdateHook ringUpdateHook, WaitFn waitFn)
    : router_(std::move(router)),
      fetchRpc_(std::move(fetchRpc)),
      ringUpdateHook_(std::move(ringUpdateHook)),
      waitFn_(std::move(waitFn))
{
    if (waitFn_ == nullptr) {
        waitFn_ = [](std::condition_variable &cv, std::unique_lock<std::mutex> &lock,
                     std::chrono::milliseconds duration, const std::function<bool()> &wakePredicate) {
            cv.wait_for(lock, duration, wakePredicate);
        };
    }
}

HashRingRefresher::~HashRingRefresher()
{
    Stop();
}

Status HashRingRefresher::InitialFetch(const HostPort &initialWorkerAddr)
{
    RETURN_RUNTIME_ERROR_IF_NULL(router_);
    CHECK_FAIL_RETURN_STATUS(static_cast<bool>(fetchRpc_), K_INVALID, "Hash ring fetch callback must be set");
    CHECK_FAIL_RETURN_STATUS(!initialWorkerAddr.Empty(), K_INVALID, "Initial worker address must not be empty");
    currentVersion_.store(0);
    {
        std::lock_guard<std::mutex> lock(workerListMutex_);
        workerList_.clear();
        workerList_.push_back(initialWorkerAddr);
        nextWorkerIndex_ = 0;
    }
    return DoRefresh(false);
}

Status HashRingRefresher::StartPeriodicRefresh(int64_t intervalMs)
{
    CHECK_FAIL_RETURN_STATUS(intervalMs > 0, K_INVALID, "Hash ring refresh interval must be positive");
    Stop();
    intervalMs_ = intervalMs;
    running_.store(true);
    refreshThread_ = std::thread(&HashRingRefresher::RefreshLoop, this);
    return Status::OK();
}

void HashRingRefresher::Stop()
{
    bool wasRunning = false;
    {
        std::lock_guard<std::mutex> lock(cvMutex_);
        wasRunning = running_.exchange(false);
        forceRefresh_.store(false, std::memory_order_release);
        forceRefreshDeadlineMs_.store(0, std::memory_order_release);
    }
    if (!wasRunning) {
        return;
    }
    cv_.notify_all();
    if (refreshThread_.joinable()) {
        refreshThread_.join();
    }
}

bool HashRingRefresher::ForceRefresh()
{
    const auto nowMs = SteadyNowMs();
    const auto requestedDeadlineMs = nowMs + FORCED_REFRESH_WINDOW_MS;
    auto deadlineMs = forceRefreshDeadlineMs_.load(std::memory_order_acquire);
    bool newWindow = false;
    while (deadlineMs < requestedDeadlineMs) {
        if (forceRefreshDeadlineMs_.compare_exchange_weak(deadlineMs, requestedDeadlineMs, std::memory_order_acq_rel)) {
            newWindow = deadlineMs <= nowMs;
            break;
        }
    }
    if (!newWindow) {
        return false;
    }
    {
        std::lock_guard<std::mutex> lock(cvMutex_);
        forceRefresh_.store(true, std::memory_order_release);
    }
    cv_.notify_all();
    return true;
}

Status HashRingRefresher::DoRefresh(bool stopAware)
{
    // Copy worker list under lock to avoid data race with InitialFetch
    std::vector<HostPort> workers;
    size_t startIndex = 0;
    {
        std::lock_guard<std::mutex> lock(workerListMutex_);
        workers = workerList_;
        if (!workers.empty()) {
            startIndex = nextWorkerIndex_ % workers.size();
            nextWorkerIndex_ = (startIndex + 1) % workers.size();
        }
    }

    const auto probeCount = stopAware ? std::min(workers.size(), MAX_BACKGROUND_PROBES_PER_ROUND) : workers.size();
    bool reachedWorker = false;
    for (size_t offset = 0; offset < probeCount; ++offset) {
        if (stopAware && !running_.load(std::memory_order_acquire)) {
            break;
        }
        const auto &worker = workers[(startIndex + offset) % workers.size()];
        ::datasystem::ClusterTopologyPb ring;
        std::string masterAddress;
        uint64_t newVersion = 0;
        bool changed = false;
        std::unordered_map<std::string, std::string> hostIdMap;

        const uint64_t requestedVersion = currentVersion_.load(std::memory_order_acquire);
        const auto timeoutMs = stopAware ? BACKGROUND_REFRESH_RPC_TIMEOUT_MS : 0;
        Status st = fetchRpc_(worker, requestedVersion, ring, masterAddress, newVersion, changed, hostIdMap, timeoutMs);
        if (st.IsError()) {
            LOG(WARNING) << "[Routing] Skip failed hash ring refresh from " << worker.ToString()
                         << ", requested version: " << requestedVersion << ", status: " << st.ToString();
            continue;
        }
        reachedWorker = true;

        if (changed) {
            if (newVersion < requestedVersion) {
                LOG(WARNING) << "Ignore stale hash ring response from " << worker.ToString()
                             << ", requested version: " << requestedVersion
                             << ", response version: " << newVersion;
                continue;
            }
            auto publish = PublishHashRing(newVersion, std::move(ring), std::move(hostIdMap));
            if (publish.IsError()) {
                LogTopologyPublishFailure(worker, requestedVersion, newVersion,
                                          currentVersion_.load(std::memory_order_acquire), publish);
            }
            return publish;
        }
    }
    if (reachedWorker) {
        return Status::OK();
    }
    return Status(K_NOT_FOUND, "No reachable worker for hash ring refresh");
}

Status HashRingRefresher::PublishHashRing(uint64_t newVersion, ::datasystem::ClusterTopologyPb &&ring,
                                          std::unordered_map<std::string, std::string> &&hostIdMap)
{
    std::unique_ptr<PreparedClusterTopology> prepared;
    RETURN_IF_NOT_OK(PreparedClusterTopology::Create(std::move(ring), prepared));
    const auto &topology = prepared->GetTopology();
    if (ringUpdateHook_) {
        RETURN_IF_NOT_OK(ringUpdateHook_(newVersion, topology, hostIdMap));
    }
    currentVersion_.store(newVersion, std::memory_order_release);
    UpdateWorkerList(topology);
    router_->UpdateHashRing(*prepared, hostIdMap);
    return Status::OK();
}

void HashRingRefresher::UpdateWorkerList(const ::datasystem::ClusterTopologyPb &ring)
{
    std::vector<HostPort> updatedWorkers;
    updatedWorkers.reserve(ring.members_size());
    for (const auto &entry : ring.members()) {
        if (entry.second.state() != ::datasystem::MembershipPb::ACTIVE) {
            continue;
        }
        HostPort worker;
        if (worker.ParseString(entry.first).IsOk()) {
            updatedWorkers.emplace_back(std::move(worker));
        }
    }
    if (updatedWorkers.empty()) {
        return;
    }
    std::sort(updatedWorkers.begin(), updatedWorkers.end());
    std::lock_guard<std::mutex> lock(workerListMutex_);
    workerList_ = std::move(updatedWorkers);
    nextWorkerIndex_ %= workerList_.size();
}

void HashRingRefresher::RefreshLoop()
{
    while (running_.load()) {
        TraceGuard traceGuard = Trace::Instance().SetTraceNewID("HashRingRefresh;" + GetStringUuid());
        forceRefresh_.exchange(false, std::memory_order_acq_rel);
        DoRefresh(true);

        auto deadlineMs = forceRefreshDeadlineMs_.load(std::memory_order_acquire);
        INJECT_POINT_NO_RETURN("HashRingRefresher.RefreshLoop.afterDeadlineRead");
        const auto nowMs = SteadyNowMs();
        if (deadlineMs <= nowMs && deadlineMs != 0) {
            (void)forceRefreshDeadlineMs_.compare_exchange_strong(deadlineMs, 0, std::memory_order_acq_rel);
        }
        const bool retryForcedRefresh = deadlineMs > nowMs;
        std::unique_lock<std::mutex> lock(cvMutex_);
        INJECT_POINT_NO_RETURN("HashRingRefresher.RefreshLoop.beforeWait");
        const auto waitMs =
            retryForcedRefresh ? std::min(FORCED_REFRESH_RETRY_INTERVAL_MS, deadlineMs - nowMs) : intervalMs_;
        waitFn_(cv_, lock, std::chrono::milliseconds(waitMs), [this] {
            const bool ready = !running_.load() || forceRefresh_.load(std::memory_order_acquire);
            INJECT_POINT_NO_RETURN("HashRingRefresher.RefreshLoop.afterWaitPredicateRead");
            return ready;
        });
    }
}

}  // namespace client
}  // namespace datasystem
