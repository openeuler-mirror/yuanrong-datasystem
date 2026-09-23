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

/** Description: Vendor-neutral UB port-health contracts. */

#ifndef DATASYSTEM_COMMON_OBJECT_CACHE_UB_PORT_HEALTH_H
#define DATASYSTEM_COMMON_OBJECT_CACHE_UB_PORT_HEALTH_H

#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "datasystem/utils/status.h"

namespace datasystem {

constexpr uint64_t UB_PORT_HEALTH_UNKNOWN_EPOCH = 0;
constexpr uint64_t UB_PORT_HEALTH_FIRST_EPOCH = 1;
constexpr size_t UB_HEALTH_INCARNATION_LOG_PREFIX_LENGTH = 12;
constexpr std::chrono::milliseconds UB_PORT_HEALTH_PROVIDER_QUERY_INTERVAL{ 1'000 };
constexpr std::chrono::milliseconds UB_REMOTE_PORT_HEALTH_QUERY_INTERVAL{ 1'000 };
constexpr uint64_t UB_REMOTE_PORT_HEALTH_RETRY_MIN_MS = 1'000;
constexpr uint64_t UB_REMOTE_PORT_HEALTH_RETRY_MAX_MS = 30'000;
constexpr std::chrono::milliseconds UB_PORT_HEALTH_REFRESH_WAIT_TIMEOUT{ 1'000 };

std::string FormatUbHealthIncarnationPrefix(const uint8_t *incarnation, size_t size);
std::string FormatUbHealthIncarnationPrefix(const std::string &incarnation);

enum class UbPortState : uint8_t { UNKNOWN = 0, GOOD = 1, BAD = 2 };

enum class UbPortHealthOwner : uint8_t { UNKNOWN = 0, CLIENT = 1, WORKER = 2 };

struct UbPortStatus {
    uint32_t portIndex = 0;
    UbPortState state = UbPortState::UNKNOWN;
};

struct UbPortHealthSnapshot {
    // A successful query produces one unique entry per port and keeps these counts equal to the corresponding entries.
    std::vector<UbPortStatus> ports;
    bool valid = false;
    uint32_t totalPortCount = 0;
    uint32_t goodPortCount = 0;
    uint32_t badPortCount = 0;
    uint32_t unknownPortCount = 0;
    uint64_t healthEpoch = UB_PORT_HEALTH_UNKNOWN_EPOCH;
    bool verificationPending = false;
    Status lastQueryStatus;
};

struct UbPortHealthSummary {
    // valid=false is the explicit UNKNOWN state used by new peers; absence is represented by std::optional.
    bool valid = false;
    uint32_t totalPortCount = 0;
    uint32_t badPortCount = 0;
    uint64_t healthEpoch = UB_PORT_HEALTH_UNKNOWN_EPOCH;
    bool verificationPending = false;
};

// Local Monitor snapshots preserve the last confirmed counts while refresh is pending.
inline bool IsLocalUbNodeIsolated(const std::shared_ptr<const UbPortHealthSnapshot> &snapshot)
{
    return snapshot != nullptr && snapshot->valid && snapshot->totalPortCount != 0
           && snapshot->badPortCount == snapshot->totalPortCount;
}

inline bool IsSameUbPortHealth(const UbPortHealthSummary &lhs, const UbPortHealthSummary &rhs)
{
    return lhs.valid == rhs.valid && lhs.totalPortCount == rhs.totalPortCount && lhs.badPortCount == rhs.badPortCount
           && lhs.healthEpoch == rhs.healthEpoch && lhs.verificationPending == rhs.verificationPending;
}

inline bool IsSameUbPortHealth(const std::optional<UbPortHealthSummary> &lhs,
                               const std::optional<UbPortHealthSummary> &rhs)
{
    return lhs.has_value() == rhs.has_value() && (!lhs.has_value() || IsSameUbPortHealth(*lhs, *rhs));
}

inline bool HasKnownUbPortHealth(const UbPortHealthSummary &summary)
{
    return summary.valid && summary.healthEpoch != UB_PORT_HEALTH_UNKNOWN_EPOCH && summary.totalPortCount != 0
           && summary.badPortCount <= summary.totalPortCount;
}

// A pending observation preserves the current admission state; it can neither establish nor clear isolation.
inline bool ShouldIsolateForUbPortHealth(const UbPortHealthSummary &summary)
{
    return HasKnownUbPortHealth(summary) && !summary.verificationPending
           && summary.badPortCount == summary.totalPortCount;
}

inline bool ShouldRecoverFromUbIsolation(const UbPortHealthSummary &summary)
{
    return HasKnownUbPortHealth(summary) && !summary.verificationPending
           && summary.badPortCount < summary.totalPortCount;
}

inline bool CanApplyPassiveUbRecovery(const UbPortHealthSummary &current,
                                      const UbPortHealthSummary &incoming)
{
    if (!ShouldRecoverFromUbIsolation(incoming)) {
        return false;
    }
    return incoming.healthEpoch > current.healthEpoch
           || (incoming.healthEpoch == current.healthEpoch && current.verificationPending
               && incoming.valid == current.valid && incoming.totalPortCount == current.totalPortCount
               && incoming.badPortCount == current.badPortCount);
}

enum class UbPortHealthEvidenceSource : uint8_t {
    PASSIVE_SUMMARY = 0,
    QUERY_RESPONSE = 1,
    PASSIVE_RECOVERY = 2
};

inline bool CanUpdateRemoteUbAdmission(UbPortHealthEvidenceSource source, const UbPortHealthSummary &summary)
{
    if (!HasKnownUbPortHealth(summary) || summary.verificationPending) {
        return false;
    }
    return source == UbPortHealthEvidenceSource::QUERY_RESPONSE
           || (source == UbPortHealthEvidenceSource::PASSIVE_RECOVERY
               && summary.badPortCount < summary.totalPortCount);
}

class IUbPortStatusProvider {
public:
    virtual ~IUbPortStatusProvider() = default;

    /**
     * Query one complete provider observation synchronously. The provider owns its native context; the caller owns
     * portStatus. Implementations must leave portStatus unchanged on failure and must not expose provider-native types.
     */
    virtual Status QueryPortStatus(std::vector<UbPortStatus> &portStatus) = 0;
};

class IUbPortHealthObserver {
public:
    virtual ~IUbPortHealthObserver() = default;

    // Invoked outside Monitor locks. The summary reference is valid only for the duration of the callback. The callback
    // must not synchronously stop or destroy its Monitor.
    virtual void OnUbPortHealthChanged(const UbPortHealthSummary &summary) = 0;
};

/**
 * Owns the latest immutable snapshot and periodic query lifecycle. The monitor shares ownership of provider but only
 * weakly references observer. It drains background work and callbacks before Stop returns and never invokes provider
 * concurrently. Provider access, including refreshes requested by RPC handlers, remains serialized and rate-limited.
 */
class UbPortHealthMonitor {
public:
    explicit UbPortHealthMonitor(std::shared_ptr<IUbPortStatusProvider> provider,
                                 std::weak_ptr<IUbPortHealthObserver> observer = {},
                                 UbPortHealthOwner owner = UbPortHealthOwner::UNKNOWN);
    ~UbPortHealthMonitor();

    static std::unique_ptr<UbPortHealthMonitor> CreateForTest(std::shared_ptr<IUbPortStatusProvider> provider,
                                                               std::chrono::milliseconds queryInterval,
                                                               std::weak_ptr<IUbPortHealthObserver> observer = {},
                                                               UbPortHealthOwner owner = UbPortHealthOwner::UNKNOWN);

    UbPortHealthMonitor(const UbPortHealthMonitor &) = delete;
    UbPortHealthMonitor &operator=(const UbPortHealthMonitor &) = delete;
    UbPortHealthMonitor(UbPortHealthMonitor &&) = delete;
    UbPortHealthMonitor &operator=(UbPortHealthMonitor &&) = delete;

    Status Start();
    void Stop();
    Status EnsureFresh(std::chrono::milliseconds maxAge);
    // Never waits for a provider query. Stale/in-flight facts are returned as pending and refresh is coalesced.
    Status ReadSummaryForQuery(std::chrono::milliseconds maxAge, UbPortHealthSummary &summary);
    Status AddObserver(std::weak_ptr<IUbPortHealthObserver> observer);
    // The background query publishes pending and completed observations even when the port facts stay unchanged.
    void TriggerRefresh();
    std::shared_ptr<const UbPortHealthSnapshot> GetSnapshot() const;
    std::optional<UbPortHealthSummary> GetSummary() const;

private:
    UbPortHealthMonitor(std::shared_ptr<IUbPortStatusProvider> provider,
                        std::weak_ptr<IUbPortHealthObserver> observer, std::chrono::milliseconds queryInterval,
                        UbPortHealthOwner owner);

    class Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_OBJECT_CACHE_UB_PORT_HEALTH_H
