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

/**
 * Description: URMA send lane lease and pool helpers.
 */

#ifndef DATASYSTEM_COMMON_RDMA_URMA_SEND_LANE_H
#define DATASYSTEM_COMMON_RDMA_URMA_SEND_LANE_H

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <iterator>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

namespace datasystem {
class UrmaJetty;

/** @brief UB NUMA source-chip selection granularity configured by the worker. */
enum class UbNumaRrType : uint32_t {
    DISABLED = 0,
    PER_LOGICAL_WRITE = 1,
    PER_POST = 2,
};

/** @brief UB NUMA source-chip policy configured by the worker. */
enum class UbNumaSrcChipPolicy : uint32_t {
    ROUND_ROBIN = 0,
    ROUND_ROBIN_WITH_AFFINITY = 1,
};

class UrmaSendLaneLease {
public:
    enum class SettleAction : uint8_t { NONE = 0, RELEASE = 1, RETIRE = 2 };

    struct TimeoutInfo {
        uint64_t requestId = 0;
        std::string remoteAddress;
        std::string remoteInstanceId;
        uint64_t timeoutTimestampMs = 0;
    };

    explicit UrmaSendLaneLease(std::shared_ptr<UrmaJetty> jetty, uint64_t requestIdFloor = 0)
        : jetty_(std::move(jetty)), requestIdFloor_(requestIdFloor)
    {
    }

    ~UrmaSendLaneLease() = default;

    UrmaSendLaneLease(const UrmaSendLaneLease &) = delete;
    UrmaSendLaneLease &operator=(const UrmaSendLaneLease &) = delete;
    UrmaSendLaneLease(UrmaSendLaneLease &&) = delete;
    UrmaSendLaneLease &operator=(UrmaSendLaneLease &&) = delete;

    // A lane tracks provider WRs, not business events. Add before the provider post so a
    // completion that races with the post return cannot release the Jetty early.
    void AddWr()
    {
        pendingWrs_.fetch_add(1, std::memory_order_relaxed);
    }

    SettleAction CompleteWr()
    {
        return MarkWrSettled(false);
    }

    // Cancel a WR only when the provider did not accept it (for example, a single-WR
    // post failure or the part of a gather chain after bad_wr).
    SettleAction CancelWr()
    {
        return MarkWrSettled(false);
    }

    // Mark the request as failed without consuming a submitted WR count. Standalone logical transfers use
    // this when a later chunk cannot be submitted after earlier chunks were posted. A worker-
    // to-worker Batch Get passes an externally owned lease and intentionally does not call this
    // for object-level WR creation or post failures; those failures settle through release.
    SettleAction RequestRetire()
    {
        retireRequested_.store(true, std::memory_order_release);
        if (sealed_.load(std::memory_order_acquire) && pendingWrs_.load(std::memory_order_acquire) == 0) {
            return TrySettleLane();
        }
        return SettleAction::NONE;
    }

    // A fatal CQE or Jetty async error retires the whole lane immediately. Its outstanding
    // WRs are subsequently owned by the provider Jetty flush lifecycle, never by Event.
    SettleAction Retire()
    {
        retireRequested_.store(true, std::memory_order_release);
        return TrySettleLane();
    }

    // A timed-out business request may return its Jetty to the reusable pool while provider WRs
    // are still pending. This is only legal after Seal has closed the producer side. The resource
    // transfers the remaining count to per-Jetty orphan accounting before publishing the release;
    // laneSettled_ prevents a second pool release.
    SettleAction ForceRelease()
    {
        if (!sealed_.load(std::memory_order_acquire) || retireRequested_.load(std::memory_order_acquire)) {
            return SettleAction::NONE;
        }
        const auto action = TrySettleLane();
        if (action == SettleAction::RELEASE) {
            forceReleased_.store(true, std::memory_order_release);
        }
        return action;
    }

    SettleAction Seal()
    {
        sealed_.store(true, std::memory_order_release);
        if (pendingWrs_.load(std::memory_order_acquire) == 0) {
            return TrySettleLane();
        }
        return SettleAction::NONE;
    }

    std::shared_ptr<UrmaJetty> GetJetty() const
    {
        return jetty_;
    }

    uint32_t GetPendingWrCount() const
    {
        return pendingWrs_.load(std::memory_order_acquire);
    }

    bool IsSettled() const
    {
        return laneSettled_.load();
    }

    bool IsSealed() const
    {
        return sealed_.load(std::memory_order_acquire);
    }

    bool IsRetireRequested() const
    {
        return retireRequested_.load(std::memory_order_acquire);
    }

    bool IsForceReleased() const
    {
        return forceReleased_.load(std::memory_order_acquire);
    }

    // Only the first timed-out Event of a multi-WR lane records the timeout context. A timeout
    // racing with Seal is safe because both paths retry force release after publishing their state.
    bool TryMarkTimedOut(TimeoutInfo timeoutInfo)
    {
        std::lock_guard<std::mutex> lock(timeoutMutex_);
        if (timedOut_.load(std::memory_order_acquire)) {
            return false;
        }
        timeoutInfo_ = std::move(timeoutInfo);
        timedOut_.store(true, std::memory_order_release);
        return true;
    }

    bool IsTimedOut() const
    {
        return timedOut_.load(std::memory_order_acquire);
    }

    bool GetTimeoutInfo(TimeoutInfo &timeoutInfo) const
    {
        if (!IsTimedOut()) {
            return false;
        }
        std::lock_guard<std::mutex> lock(timeoutMutex_);
        timeoutInfo = timeoutInfo_;
        return true;
    }

    uint64_t GetRequestIdFloor() const
    {
        return requestIdFloor_;
    }

    bool OwnsRequestId(uint64_t requestId) const
    {
        return !requestIdGenerationCheckEnabled_.load(std::memory_order_acquire) || requestId >= requestIdFloor_;
    }

    bool IsRequestIdGenerationCheckEnabled() const
    {
        return requestIdGenerationCheckEnabled_.load(std::memory_order_acquire);
    }

    // Pipeline H2D currently truncates request ids. Keep that compile-time path on its legacy
    // local-id accounting until it adopts a non-wrapping 64-bit completion token.
    void DisableRequestIdGenerationCheck()
    {
        requestIdGenerationCheckEnabled_.store(false, std::memory_order_release);
    }

private:
    SettleAction MarkWrSettled(bool retire)
    {
        if (retire) {
            retireRequested_.store(true, std::memory_order_release);
        }
        uint32_t pending = pendingWrs_.load(std::memory_order_acquire);
        while (pending > 0) {
            if (pendingWrs_.compare_exchange_weak(pending, pending - 1, std::memory_order_acq_rel,
                                                  std::memory_order_acquire)) {
                if (pending == 1 && sealed_.load(std::memory_order_acquire)) {
                    return TrySettleLane();
                }
                return SettleAction::NONE;
            }
        }
        return SettleAction::NONE;
    }

    SettleAction TrySettleLane()
    {
        bool expected = false;
        if (!laneSettled_.compare_exchange_strong(expected, true, std::memory_order_acq_rel,
                                                  std::memory_order_acquire)) {
            return SettleAction::NONE;
        }
        return retireRequested_.load(std::memory_order_acquire) ? SettleAction::RETIRE : SettleAction::RELEASE;
    }

    std::shared_ptr<UrmaJetty> jetty_;
    std::atomic<uint32_t> pendingWrs_{ 0 };
    std::atomic<bool> sealed_{ false };
    std::atomic<bool> retireRequested_{ false };
    std::atomic<bool> laneSettled_{ false };
    std::atomic<bool> forceReleased_{ false };
    mutable std::mutex timeoutMutex_;
    std::atomic<bool> timedOut_{ false };
    TimeoutInfo timeoutInfo_;
    const uint64_t requestIdFloor_;
    std::atomic<bool> requestIdGenerationCheckEnabled_{ true };
};

class SendJettyPool {
public:
    struct Stats {
        size_t poolSize = 0;
        size_t idleCount = 0;
        size_t inUseCount = 0;
    };

    SendJettyPool() = default;
    ~SendJettyPool() = default;

    SendJettyPool(const SendJettyPool &) = delete;
    SendJettyPool &operator=(const SendJettyPool &) = delete;
    SendJettyPool(SendJettyPool &&) = delete;
    SendJettyPool &operator=(SendJettyPool &&) = delete;

    void Clear()
    {
        jettys_.clear();
        idleIndices_.clear();
    }

    void Add(std::shared_ptr<UrmaJetty> jetty)
    {
        idleIndices_.push_back(jettys_.size());
        jettys_.push_back(std::move(jetty));
    }

    bool PopIdle(std::shared_ptr<UrmaJetty> &jetty)
    {
        while (!idleIndices_.empty()) {
            const auto idx = idleIndices_.back();
            idleIndices_.pop_back();
            if (idx >= jettys_.size()) {
                continue;
            }
            jetty = jettys_[idx];
            return true;
        }
        return false;
    }

    bool Release(const std::shared_ptr<UrmaJetty> &jetty)
    {
        auto iter = std::find_if(jettys_.begin(), jettys_.end(), [&jetty](const auto &pooled) {
            return pooled.get() == jetty.get();
        });
        if (iter == jettys_.end()) {
            return false;
        }
        const auto idx = static_cast<size_t>(std::distance(jettys_.begin(), iter));
        if (std::find(idleIndices_.begin(), idleIndices_.end(), idx) != idleIndices_.end()) {
            return true;
        }
        idleIndices_.push_back(idx);
        return true;
    }

    bool Remove(const std::shared_ptr<UrmaJetty> &jetty)
    {
        for (size_t i = 0; i < jettys_.size(); ++i) {
            if (jettys_[i].get() != jetty.get()) {
                continue;
            }
            const auto lastIdx = jettys_.size() - 1;
            idleIndices_.erase(std::remove(idleIndices_.begin(), idleIndices_.end(), i), idleIndices_.end());
            if (i != lastIdx) {
                jettys_[i] = std::move(jettys_[lastIdx]);
                std::replace(idleIndices_.begin(), idleIndices_.end(), lastIdx, i);
            }
            jettys_.pop_back();
            return true;
        }
        return false;
    }

    Stats GetStats() const
    {
        Stats stats;
        stats.poolSize = jettys_.size();
        stats.idleCount = idleIndices_.size();
        stats.inUseCount = stats.poolSize > stats.idleCount ? stats.poolSize - stats.idleCount : 0;
        return stats;
    }

private:
    std::vector<std::shared_ptr<UrmaJetty>> jettys_;
    std::vector<size_t> idleIndices_;
};
}  // namespace datasystem
#endif  // DATASYSTEM_COMMON_RDMA_URMA_SEND_LANE_H
