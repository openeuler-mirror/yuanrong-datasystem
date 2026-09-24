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

#include "datasystem/client/object_cache/routing/hash_ring_refresher.h"

#include <algorithm>
#include <exception>
#include <utility>

#include "datasystem/common/ak_sk/hasher.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/log/trace.h"
#include "datasystem/common/util/status_helper.h"
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
    LOG_EVERY_N(WARNING, TOPOLOGY_PUBLISH_FAILURE_LOG_EVERY_N)
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
    {
        std::lock_guard<std::mutex> lock(workerListMutex_);
        currentVersion_.store(0);
        workerList_.clear();
        hostIdsDigest_.clear();
        workerList_.push_back(initialWorkerAddr);
        nextWorkerIndex_ = 0;
    }
    return DoRefreshSafely(false);
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

std::vector<HostPort> HashRingRefresher::BeginRefreshRound(size_t &startIndex)
{
    std::vector<HostPort> workers;
    startIndex = 0;
    {
        std::lock_guard<std::mutex> lock(workerListMutex_);
        workers = workerList_;
        if (!workers.empty()) {
            startIndex = nextWorkerIndex_ % workers.size();
            nextWorkerIndex_ = (startIndex + 1) % workers.size();
        }
    }
    {
        std::lock_guard<std::mutex> lock(lowerVersionObsMutex_);
        ++refreshRound_;
        for (auto iter = lowerVersionObs_.begin(); iter != lowerVersionObs_.end();) {
            iter = iter->second.round + 1 < refreshRound_ ? lowerVersionObs_.erase(iter) : ++iter;
        }
    }
    return workers;
}

Status HashRingRefresher::DoRefresh(bool stopAware)
{
    size_t startIndex = 0;
    const auto workers = BeginRefreshRound(startIndex);
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
        Status status = fetchRpc_(worker, requestedVersion, ring, masterAddress, newVersion, changed, hostIdMap,
                                  timeoutMs);
        if (status.IsError()) {
            LOG(WARNING) << "[Routing] Skip failed hash ring refresh from " << worker.ToString()
                         << ", requested version: " << requestedVersion << ", status: " << status.ToString();
            continue;
        }
        reachedWorker = true;
        if (!changed) {
            continue;
        }
        if (newVersion < requestedVersion) {
            Status publish;
            if (TryPublishEpochReset(worker, requestedVersion, newVersion, ring, hostIdMap, publish)) {
                return publish;
            }
            LOG(WARNING) << "Ignore stale hash ring response from " << worker.ToString()
                         << ", requested version: " << requestedVersion << ", response version: " << newVersion;
            continue;
        }
        auto publish = PublishHashRing(newVersion, std::move(ring), std::move(hostIdMap), false);
        if (publish.IsError()) {
            LogTopologyPublishFailure(worker, requestedVersion, newVersion,
                                      currentVersion_.load(std::memory_order_acquire), publish);
        }
        return publish;
    }
    return reachedWorker ? Status::OK() : Status(K_NOT_FOUND, "No reachable worker for hash ring refresh");
}

Status HashRingRefresher::DoRefreshSafely(bool stopAware)
{
    try {
        return DoRefresh(stopAware);
    } catch (const std::exception &error) {
        LOG(ERROR) << "Hash ring refresh callback threw: " << error.what();
        return Status(K_RUNTIME_ERROR, "Hash ring refresh callback threw");
    } catch (...) {
        LOG(ERROR) << "Hash ring refresh callback threw";
        return Status(K_RUNTIME_ERROR, "Hash ring refresh callback threw");
    }
}

bool HashRingRefresher::TryPublishEpochReset(const HostPort &worker, uint64_t requestedVersion, uint64_t newVersion,
                                             ::datasystem::ClusterTopologyPb &ring,
                                             std::unordered_map<std::string, std::string> &hostIdMap,
                                             Status &result)
{
    const auto digest = BuildRingDigest(ring);
    if (digest.empty() || !RecordLowerVersionAndCheckConfirmation(worker, newVersion, digest)) {
        return false;
    }
    LOG(WARNING) << "[Routing] Accept epoch-reset hash ring from " << worker.ToString()
                 << ", previous version: " << requestedVersion << ", reset version: " << newVersion;
    {
        std::lock_guard<std::mutex> lock(lowerVersionObsMutex_);
        lowerVersionObs_.clear();
    }
    result = PublishHashRing(newVersion, std::move(ring), std::move(hostIdMap), true);
    if (result.IsError()) {
        LogTopologyPublishFailure(worker, requestedVersion, newVersion,
                                  currentVersion_.load(std::memory_order_acquire), result);
    }
    return true;
}

std::string HashRingRefresher::BuildRingDigest(const ::datasystem::ClusterTopologyPb &ring)
{
    std::vector<std::string> activeAddresses;
    for (const auto &[address, member] : ring.members()) {
        if (member.state() == ::datasystem::MembershipPb::ACTIVE) {
            activeAddresses.emplace_back(address);
        }
    }
    std::sort(activeAddresses.begin(), activeAddresses.end());
    std::string digest;
    for (const auto &address : activeAddresses) {
        digest += address;
        digest += ',';
    }
    return digest;
}

std::string HashRingRefresher::GetHostIdsDigest(uint64_t version)
{
    std::lock_guard<std::mutex> lock(workerListMutex_);
    return version == currentVersion_.load(std::memory_order_acquire) ? hostIdsDigest_ : "";
}

bool HashRingRefresher::RecordLowerVersionAndCheckConfirmation(const HostPort &worker, uint64_t newVersion,
                                                               const std::string &digest)
{
    const auto workerKey = worker.ToString();
    std::lock_guard<std::mutex> lock(lowerVersionObsMutex_);
    for (const auto &[address, observation] : lowerVersionObs_) {
        if (address != workerKey && observation.version == newVersion && observation.digest == digest) {
            return true;
        }
    }
    lowerVersionObs_[workerKey] = LowerVersionObservation{ newVersion, digest, refreshRound_ };
    return false;
}

Status HashRingRefresher::PublishHashRing(uint64_t newVersion, ::datasystem::ClusterTopologyPb &&ring,
                                          std::unordered_map<std::string, std::string> &&hostIdMap,
                                          bool epochResetConfirmed)
{
    std::unique_ptr<PreparedClusterTopology> prepared;
    RETURN_IF_NOT_OK(PreparedClusterTopology::Create(std::move(ring), prepared));
    std::string hostIdsDigest;
    Hasher hasher;
    RETURN_IF_NOT_OK(hasher.GetStringMapSha256Hex(hostIdMap, hostIdsDigest));
    const auto &topology = prepared->GetTopology();
    if (ringUpdateHook_) {
        RETURN_IF_NOT_OK(ringUpdateHook_(newVersion, topology, hostIdMap, epochResetConfirmed));
    }
    router_->UpdateHashRing(*prepared, hostIdMap);
    UpdateWorkerList(topology);
    {
        std::lock_guard<std::mutex> lock(workerListMutex_);
        hostIdsDigest_ = std::move(hostIdsDigest);
        currentVersion_.store(newVersion, std::memory_order_release);
    }
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
        TraceGuard traceGuard = Trace::Instance().SetTraceNewID(Trace::GenerateComponentTraceId("HashRingRefresh"));
        forceRefresh_.exchange(false, std::memory_order_acq_rel);
        (void)DoRefreshSafely(true);

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
