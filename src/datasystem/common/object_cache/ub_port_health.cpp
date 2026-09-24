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

#include "datasystem/common/object_cache/ub_port_health.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <cstddef>
#include <exception>
#include <iterator>
#include <limits>
#include <sstream>
#include <thread>
#include <utility>

#include "datasystem/common/log/log.h"
#include "datasystem/common/util/uuid_generator.h"

#include <bthread/condition_variable.h>
#include <bthread/mutex.h>

#include "datasystem/common/log/log.h"

namespace datasystem {
namespace {
using Clock = std::chrono::steady_clock;
constexpr size_t HEX_CHAR_COUNT_PER_BYTE = 2;
constexpr size_t HEX_HIGH_NIBBLE_SHIFT = 4;
constexpr uint8_t HEX_LOW_NIBBLE_MASK = 0x0f;
constexpr int UB_PORT_HEALTH_QUERY_FAILURE_LOG_RATE = 100;
constexpr char HEX_DIGITS[] = "0123456789abcdef";
constexpr int PORT_QUERY_LOG_RATE = 60;

std::string EncodeHexPrefix(const uint8_t *bytes, size_t size)
{
    std::string encoded(size * HEX_CHAR_COUNT_PER_BYTE, '\0');
    for (size_t i = 0; i < size; ++i) {
        const auto outputOffset = i * HEX_CHAR_COUNT_PER_BYTE;
        encoded[outputOffset] = HEX_DIGITS[bytes[i] >> HEX_HIGH_NIBBLE_SHIFT];
        encoded[outputOffset + 1] = HEX_DIGITS[bytes[i] & HEX_LOW_NIBBLE_MASK];
    }
    return encoded;
}

bool HasSamePortObservation(const UbPortHealthSnapshot &lhs, const UbPortHealthSnapshot &rhs)
{
    if (!lhs.valid || !rhs.valid || lhs.ports.size() != rhs.ports.size()) {
        return false;
    }
    return std::equal(lhs.ports.begin(), lhs.ports.end(), rhs.ports.begin(),
                      [](const UbPortStatus &left, const UbPortStatus &right) {
                          return left.portIndex == right.portIndex && left.state == right.state;
                      });
}

UbPortHealthSummary ToSummary(const UbPortHealthSnapshot &snapshot)
{
    return { snapshot.valid, snapshot.totalPortCount, snapshot.badPortCount, snapshot.healthEpoch,
             snapshot.verificationPending };
}

bool HasSameSummary(const UbPortHealthSummary &lhs, const UbPortHealthSummary &rhs)
{
    return lhs.valid == rhs.valid && lhs.totalPortCount == rhs.totalPortCount
           && lhs.badPortCount == rhs.badPortCount && lhs.healthEpoch == rhs.healthEpoch
           && lhs.verificationPending == rhs.verificationPending;
}

bool HasSameLoggedSnapshot(const UbPortHealthSnapshot &lhs, const UbPortHealthSnapshot &rhs)
{
    return lhs.valid == rhs.valid && lhs.totalPortCount == rhs.totalPortCount
           && lhs.goodPortCount == rhs.goodPortCount && lhs.badPortCount == rhs.badPortCount
           && lhs.unknownPortCount == rhs.unknownPortCount && lhs.healthEpoch == rhs.healthEpoch
           && lhs.verificationPending == rhs.verificationPending
           && lhs.lastQueryStatus.GetCode() == rhs.lastQueryStatus.GetCode()
           && lhs.ports.size() == rhs.ports.size()
           && std::equal(lhs.ports.begin(), lhs.ports.end(), rhs.ports.begin(),
                         [](const UbPortStatus &left, const UbPortStatus &right) {
                             return left.portIndex == right.portIndex && left.state == right.state;
                         });
}

const char *PortStateName(UbPortState state)
{
    switch (state) {
        case UbPortState::GOOD:
            return "GOOD";
        case UbPortState::BAD:
            return "BAD";
        case UbPortState::UNKNOWN:
        default:
            return "UNKNOWN";
    }
}

const char *PortHealthOwnerName(UbPortHealthOwner owner)
{
    switch (owner) {
        case UbPortHealthOwner::CLIENT:
            return "client";
        case UbPortHealthOwner::WORKER:
            return "worker";
        case UbPortHealthOwner::UNKNOWN:
        default:
            return "unknown";
    }
}

std::string FormatPortStatus(const std::vector<UbPortStatus> &ports)
{
    std::ostringstream stream;
    for (size_t i = 0; i < ports.size(); ++i) {
        if (i != 0) {
            stream << ',';
        }
        stream << ports[i].portIndex << ':' << PortStateName(ports[i].state);
    }
    return stream.str();
}

Status BuildSnapshot(const std::vector<UbPortStatus> &portStatus,
                     const std::shared_ptr<const UbPortHealthSnapshot> &previous,
                     UbPortHealthSnapshot &snapshot)
{
    if (portStatus.empty()) {
        return Status(K_INVALID, "UB port status is empty");
    }
    if (portStatus.size() > std::numeric_limits<uint32_t>::max()) {
        return Status(K_OUT_OF_RANGE, "UB port count exceeds uint32_t range");
    }

    UbPortHealthSnapshot candidate;
    candidate.ports = portStatus;
    std::sort(candidate.ports.begin(), candidate.ports.end(),
              [](const UbPortStatus &lhs, const UbPortStatus &rhs) { return lhs.portIndex < rhs.portIndex; });

    for (size_t i = 0; i < candidate.ports.size(); ++i) {
        const auto &port = candidate.ports[i];
        if (i != 0 && candidate.ports[i - 1].portIndex == port.portIndex) {
            return Status(K_INVALID, "UB port status contains a duplicate port index");
        }
        switch (port.state) {
            case UbPortState::GOOD:
                ++candidate.goodPortCount;
                break;
            case UbPortState::BAD:
                ++candidate.badPortCount;
                break;
            case UbPortState::UNKNOWN:
                ++candidate.unknownPortCount;
                break;
            default:
                return Status(K_INVALID, "UB port status contains an invalid state");
        }
    }
    if (candidate.unknownPortCount != 0) {
        return Status(K_INVALID, "UB port status contains an unknown state");
    }

    candidate.totalPortCount = static_cast<uint32_t>(candidate.ports.size());
    candidate.valid = true;
    candidate.verificationPending = false;
    candidate.lastQueryStatus = Status::OK();
    if (previous != nullptr && HasSamePortObservation(*previous, candidate)) {
        candidate.healthEpoch = previous->healthEpoch;
    } else if (previous != nullptr && previous->valid) {
        if (previous->healthEpoch == std::numeric_limits<uint64_t>::max()) {
            return Status(K_OUT_OF_RANGE, "UB port health epoch exhausted");
        }
        candidate.healthEpoch = previous->healthEpoch + 1;
    } else {
        candidate.healthEpoch = UB_PORT_HEALTH_FIRST_EPOCH;
    }
    snapshot = std::move(candidate);
    return Status::OK();
}

bool NeedsPeriodicQuery(const std::shared_ptr<const UbPortHealthSnapshot> &snapshot)
{
    return snapshot != nullptr
           && (snapshot->verificationPending || (snapshot->valid && snapshot->badPortCount != 0));
}
}  // namespace

std::string FormatUbHealthIncarnationPrefix(const uint8_t *incarnation, size_t size)
{
    if (incarnation == nullptr || size == 0) {
        return {};
    }
    if (size == UUID_SIZE) {
        std::array<char, UUID_STRING_BUFFER_SIZE> printableUuid{};
        auto rc = BytesUuidToString(incarnation, size, printableUuid.data(), printableUuid.size());
        if (rc.IsOk()) {
            return std::string(printableUuid.data(), UB_HEALTH_INCARNATION_LOG_PREFIX_LENGTH);
        }
    }
    const auto bytesToEncode = std::min(size, UB_HEALTH_INCARNATION_LOG_PREFIX_LENGTH / HEX_CHAR_COUNT_PER_BYTE);
    return EncodeHexPrefix(incarnation, bytesToEncode);
}

std::string FormatUbHealthIncarnationPrefix(const std::string &incarnation)
{
    std::vector<uint8_t> bytes;
    bytes.reserve(incarnation.size());
    std::transform(incarnation.begin(), incarnation.end(), std::back_inserter(bytes),
                   [](char value) { return static_cast<uint8_t>(value); });
    return FormatUbHealthIncarnationPrefix(bytes.data(), bytes.size());
}

class UbPortHealthMonitor::Impl {
public:
    Impl(std::shared_ptr<IUbPortStatusProvider> provider, std::weak_ptr<IUbPortHealthObserver> observer,
          std::chrono::milliseconds queryInterval, UbPortHealthOwner owner)
        : provider_(std::move(provider)), queryInterval_(queryInterval), owner_(owner)
    {
        if (!observer.expired()) {
            observers_.emplace_back(std::move(observer));
        }
    }

    ~Impl()
    {
        Stop();
    }

    Status Start()
    {
        std::lock_guard<bthread::Mutex> lock(mutex_);
        if (state_ == State::RUNNING) {
            return Status::OK();
        }
        if (state_ == State::STOPPING) {
            return Status(K_NOT_READY, "UB port health monitor is stopping");
        }
        if (provider_ == nullptr) {
            return Status(K_INVALID, "UB port status provider is null");
        }
        if (queryInterval_ <= std::chrono::milliseconds::zero()) {
            return Status(K_INVALID, "UB port health query interval must be positive");
        }

        state_ = State::RUNNING;
        refreshRequested_ = true;
        try {
            worker_ = std::thread([this] { Run(); });
        } catch (const std::exception &e) {
            state_ = State::STOPPED;
            refreshRequested_ = false;
            return Status(K_RUNTIME_ERROR, std::string("Failed to start UB port health monitor: ") + e.what());
        } catch (...) {
            state_ = State::STOPPED;
            refreshRequested_ = false;
            return Status(K_RUNTIME_ERROR, "Failed to start UB port health monitor");
        }
        return Status::OK();
    }

    void Stop()
    {
        std::thread worker;
        {
            std::unique_lock<bthread::Mutex> lock(mutex_);
            if (state_ == State::STOPPED) {
                return;
            }
            if (state_ == State::STOPPING) {
                while (state_ == State::STOPPING) {
                    stateCv_.wait(lock);
                }
                return;
            }
            state_ = State::STOPPING;
            cv_.notify_all();
            worker = std::move(worker_);
        }
        if (worker.joinable()) {
            worker.join();
        }
        {
            std::lock_guard<bthread::Mutex> lock(mutex_);
            refreshRequested_ = false;
            verificationRequested_ = false;
            queryInFlight_ = false;
            hasQueryStarted_ = false;
            hasSuccessfulQuery_ = false;
            triggerPending_.store(false, std::memory_order_release);
        }
        ClearStoppedSnapshot();
        {
            std::lock_guard<bthread::Mutex> lock(mutex_);
            state_ = State::STOPPED;
        }
        stateCv_.notify_all();
        cv_.notify_all();
    }

    Status EnsureFresh(std::chrono::milliseconds maxAge)
    {
        if (maxAge < std::chrono::milliseconds::zero()) {
            return Status(K_INVALID, "UB port health max age must not be negative");
        }

        std::unique_lock<bthread::Mutex> lock(mutex_);
        if (state_ != State::RUNNING) {
            return state_ == State::STOPPING ? Status(K_SHUTTING_DOWN, "UB port health monitor is stopping")
                                             : Status(K_NOT_READY, "UB port health monitor is not running");
        }
        if (std::this_thread::get_id() == worker_.get_id()) {
            return Status(K_NOT_READY, "UB port health observer cannot synchronously refresh its Monitor");
        }

        const uint64_t targetCompletion = completedQueries_ + 1;
        if (!queryInFlight_) {
            auto snapshot = std::atomic_load(&snapshot_);
            const bool fresh = hasSuccessfulQuery_ && snapshot != nullptr && snapshot->valid
                               && !snapshot->verificationPending && Clock::now() - lastSuccessfulQuery_ <= maxAge;
            if (fresh) {
                return Status::OK();
            }
            refreshRequested_ = true;
            verificationRequested_ = true;
            cv_.notify_all();
        }
        const auto deadline = Clock::now() + UB_PORT_HEALTH_REFRESH_WAIT_TIMEOUT;
        while (state_ == State::RUNNING && completedQueries_ < targetCompletion) {
            const auto remaining = std::chrono::duration_cast<std::chrono::microseconds>(deadline - Clock::now());
            if (remaining <= std::chrono::microseconds::zero()) {
                return Status(K_RPC_DEADLINE_EXCEEDED, "Timed out waiting for UB port health refresh");
            }
            cv_.wait_for(lock, std::max<int64_t>(1, remaining.count()));
        }
        if (state_ != State::RUNNING) {
            return Status(K_SHUTTING_DOWN, "UB port health monitor stopped while refreshing");
        }
        return lastQueryStatus_;
    }

    Status ReadSummaryForQuery(std::chrono::milliseconds maxAge, UbPortHealthSummary &summary)
    {
        if (maxAge < std::chrono::milliseconds::zero()) {
            return Status(K_INVALID, "UB port health max age must not be negative");
        }
        std::lock_guard<bthread::Mutex> lock(mutex_);
        if (state_ != State::RUNNING) {
            return Status(K_NOT_READY, "UB port health monitor is not running");
        }
        auto snapshot = std::atomic_load(&snapshot_);
        const bool fresh = hasSuccessfulQuery_ && snapshot != nullptr && snapshot->valid
                           && !snapshot->verificationPending && !queryInFlight_ && !verificationRequested_
                           && Clock::now() - lastSuccessfulQuery_ <= maxAge;
        if (!fresh && !queryInFlight_) {
            refreshRequested_ = true;
            verificationRequested_ = true;
            cv_.notify_all();
        }
        if (snapshot == nullptr) {
            return Status(K_NOT_READY, "UB port health snapshot is not available");
        }
        summary = ToSummary(*snapshot);
        summary.verificationPending = summary.verificationPending || !fresh;
        return Status::OK();
    }

    Status AddObserver(std::weak_ptr<IUbPortHealthObserver> observer)
    {
        auto target = observer.lock();
        if (target == nullptr) {
            return Status(K_INVALID, "UB port health observer is expired");
        }
        std::lock_guard<bthread::Mutex> lock(mutex_);
        if (state_ == State::STOPPING) {
            return Status(K_SHUTTING_DOWN, "UB port health monitor is stopping");
        }
        for (auto iter = observers_.begin(); iter != observers_.end();) {
            auto current = iter->lock();
            if (current == target) {
                return Status::OK();
            }
            iter = current == nullptr ? observers_.erase(iter) : std::next(iter);
        }
        observers_.emplace_back(std::move(observer));
        if (state_ == State::RUNNING) {
            refreshRequested_ = true;
            verificationRequested_ = true;
            cv_.notify_all();
        }
        return Status::OK();
    }

    void TriggerRefresh()
    {
        if (triggerPending_.exchange(true, std::memory_order_acq_rel)) {
            return;
        }
        std::lock_guard<bthread::Mutex> lock(mutex_);
        if (state_ != State::RUNNING) {
            triggerPending_.store(false, std::memory_order_release);
            return;
        }
        refreshRequested_ = true;
        verificationRequested_ = true;
        cv_.notify_all();
    }

    std::shared_ptr<const UbPortHealthSnapshot> GetSnapshot() const
    {
        return std::atomic_load(&snapshot_);
    }

    std::optional<UbPortHealthSummary> GetSummary() const
    {
        auto snapshot = GetSnapshot();
        return snapshot == nullptr ? std::nullopt : std::optional<UbPortHealthSummary>{ ToSummary(*snapshot) };
    }

private:
    enum class State { STOPPED, RUNNING, STOPPING };

    void ClearStoppedSnapshot() noexcept
    {
        std::atomic_store(&snapshot_, std::shared_ptr<const UbPortHealthSnapshot>{});
        try {
            NotifyObserver({});
        } catch (const std::exception &error) {
            LOG(ERROR) << "Failed to notify stopped UB port health Monitor: " << error.what();
        } catch (...) {
            LOG(ERROR) << "Failed to notify stopped UB port health Monitor";
        }
    }

    void Run()
    {
        std::unique_lock<bthread::Mutex> lock(mutex_);
        while (state_ == State::RUNNING) {
            auto snapshot = std::atomic_load(&snapshot_);
            if (!refreshRequested_ && !NeedsPeriodicQuery(snapshot)) {
                while (state_ == State::RUNNING && !refreshRequested_
                       && !NeedsPeriodicQuery(std::atomic_load(&snapshot_))) {
                    cv_.wait(lock);
                }
                continue;
            }

            const auto now = Clock::now();
            if (hasQueryStarted_ && now < lastQueryStarted_ + queryInterval_) {
                const auto queryDeadline = lastQueryStarted_ + queryInterval_;
                while (state_ == State::RUNNING && Clock::now() < queryDeadline) {
                    const auto remaining =
                        std::chrono::duration_cast<std::chrono::microseconds>(queryDeadline - Clock::now());
                    cv_.wait_for(lock, std::max<int64_t>(1, remaining.count()));
                }
                continue;
            }

            refreshRequested_ = false;
            triggerPending_.store(false, std::memory_order_release);
            const bool publishVerificationPending = verificationRequested_;
            verificationRequested_ = false;
            queryInFlight_ = true;
            hasQueryStarted_ = true;
            lastQueryStarted_ = Clock::now();
            lock.unlock();
            Status queryStatus;
            bool iterationThrew = false;
            try {
                if (publishVerificationPending) {
                    PublishVerificationPending();
                }
                queryStatus = QueryAndPublish();
            } catch (const std::exception &e) {
                iterationThrew = true;
                queryStatus = Status(K_RUNTIME_ERROR,
                                     std::string("UB port health monitor iteration threw: ") + e.what());
            } catch (...) {
                iterationThrew = true;
                queryStatus = Status(K_RUNTIME_ERROR, "UB port health monitor iteration threw an unknown exception");
            }
            lock.lock();
            FinishQueryLocked(queryStatus, iterationThrew);
        }
    }

    void FinishQueryLocked(const Status &queryStatus, bool iterationThrew)
    {
        queryInFlight_ = false;
        refreshRequested_ = refreshRequested_ || iterationThrew;
        verificationRequested_ = verificationRequested_ || iterationThrew;
        lastQueryStatus_ = queryStatus;
        if (queryStatus.IsOk()) {
            hasSuccessfulQuery_ = true;
            lastSuccessfulQuery_ = Clock::now();
        }
        ++completedQueries_;
        cv_.notify_all();
    }

    void NotifyObserver(const UbPortHealthSummary &summary)
    {
        std::vector<std::shared_ptr<IUbPortHealthObserver>> targets;
        {
            std::lock_guard<bthread::Mutex> lock(mutex_);
            for (auto iter = observers_.begin(); iter != observers_.end();) {
                auto target = iter->lock();
                if (target != nullptr) {
                    targets.emplace_back(std::move(target));
                    ++iter;
                } else {
                    iter = observers_.erase(iter);
                }
            }
        }
        for (const auto &observer : targets) {
            try {
                observer->OnUbPortHealthChanged(summary);
            } catch (const std::exception &e) {
                LOG(ERROR) << "UB port health observer threw: " << e.what();
            } catch (...) {
                LOG(ERROR) << "UB port health observer threw an unknown exception";
            }
        }
    }

    void PublishVerificationPending()
    {
        auto previous = std::atomic_load(&snapshot_);
        if (previous == nullptr || !previous->valid || previous->verificationPending) {
            return;
        }
        auto next = *previous;
        next.verificationPending = true;
        auto published = std::make_shared<const UbPortHealthSnapshot>(std::move(next));
        std::atomic_store(&snapshot_, published);
        NotifyObserver(ToSummary(*published));
    }

    Status QueryAndPublish()
    {
        std::vector<UbPortStatus> portStatus;
        Status queryStatus;
        try {
            queryStatus = provider_->QueryPortStatus(portStatus);
        } catch (const std::exception &e) {
            queryStatus = Status(K_RUNTIME_ERROR, std::string("UB port status provider threw: ") + e.what());
        } catch (...) {
            queryStatus = Status(K_RUNTIME_ERROR, "UB port status provider threw an unknown exception");
        }

        auto previous = std::atomic_load(&snapshot_);
        UbPortHealthSnapshot next;
        if (queryStatus.IsOk()) {
            const auto &epochBase = previous != nullptr && previous->valid ? previous : lastConfirmedSnapshot_;
            queryStatus = BuildSnapshot(portStatus, epochBase, next);
        }
        if (queryStatus.IsError()) {
            if (previous != nullptr) {
                next = *previous;
            }
            next.verificationPending = true;
            next.lastQueryStatus = queryStatus;
        }
        LOG_EVERY_N(INFO, PORT_QUERY_LOG_RATE)
            << "[PORT_QUERY_LOG] owner=" << PortHealthOwnerName(owner_) << " bad=" << next.badPortCount
            << " total=" << next.totalPortCount << " valid=" << next.valid
            << " pending=" << next.verificationPending << " health_epoch=" << next.healthEpoch
            << " query_status_code=" << queryStatus.GetCode();

        const auto published = std::make_shared<const UbPortHealthSnapshot>(std::move(next));
        return PublishQuerySnapshot(previous, published, queryStatus);
    }

    Status PublishQuerySnapshot(const std::shared_ptr<const UbPortHealthSnapshot> &previous,
                                const std::shared_ptr<const UbPortHealthSnapshot> &published, const Status &queryStatus)
    {
        {
            std::lock_guard<bthread::Mutex> lock(mutex_);
            if (state_ != State::RUNNING) {
                return Status(K_SHUTTING_DOWN, "UB port health monitor stopped during query");
            }
            std::atomic_store(&snapshot_, published);
        }
        if (queryStatus.IsOk()) {
            lastConfirmedSnapshot_ = published;
        } else {
            LOG_EVERY_N(WARNING, UB_PORT_HEALTH_QUERY_FAILURE_LOG_RATE)
                << "UB_PORT_HEALTH action=query_failed owner=" << PortHealthOwnerName(owner_)
                << " status_code=" << queryStatus.GetCode() << " status=" << queryStatus;
        }
        if (!lastLoggedSnapshot_.has_value() || !HasSameLoggedSnapshot(*lastLoggedSnapshot_, *published)) {
            LOG(INFO) << "UB_PORT_HEALTH action=snapshot_changed owner=" << PortHealthOwnerName(owner_)
                      << " valid=" << published->valid << " pending=" << published->verificationPending
                      << " bad=" << published->badPortCount << " total=" << published->totalPortCount
                      << " health_epoch=" << published->healthEpoch
                      << " query_status_code=" << published->lastQueryStatus.GetCode()
                      << " ports=" << FormatPortStatus(published->ports);
            lastLoggedSnapshot_ = *published;
        }
        const auto nextSummary = ToSummary(*published);
        const bool summaryChanged = previous == nullptr || !HasSameSummary(ToSummary(*previous), nextSummary);
        if (summaryChanged) {
            NotifyObserver(nextSummary);
        }
        return queryStatus;
    }

    std::shared_ptr<IUbPortStatusProvider> provider_;
    std::vector<std::weak_ptr<IUbPortHealthObserver>> observers_;
    const std::chrono::milliseconds queryInterval_;
    const UbPortHealthOwner owner_;
    mutable std::shared_ptr<const UbPortHealthSnapshot> snapshot_;
    // Preserve the existing epoch fence across Stop/Start without exposing stopped facts to callers.
    std::shared_ptr<const UbPortHealthSnapshot> lastConfirmedSnapshot_;
    std::optional<UbPortHealthSnapshot> lastLoggedSnapshot_;

    mutable bthread::Mutex mutex_;
    bthread::ConditionVariable cv_;
    bthread::ConditionVariable stateCv_;
    std::thread worker_;
    State state_{ State::STOPPED };
    bool refreshRequested_{ false };
    std::atomic<bool> triggerPending_{ false };
    bool verificationRequested_{ false };
    bool queryInFlight_{ false };
    bool hasQueryStarted_{ false };
    bool hasSuccessfulQuery_{ false };
    uint64_t completedQueries_{ 0 };
    Clock::time_point lastQueryStarted_;
    Clock::time_point lastSuccessfulQuery_;
    Status lastQueryStatus_;
};

UbPortHealthMonitor::UbPortHealthMonitor(std::shared_ptr<IUbPortStatusProvider> provider,
                                         std::weak_ptr<IUbPortHealthObserver> observer, UbPortHealthOwner owner)
    : UbPortHealthMonitor(std::move(provider), std::move(observer), UB_PORT_HEALTH_PROVIDER_QUERY_INTERVAL, owner)
{
}

UbPortHealthMonitor::UbPortHealthMonitor(std::shared_ptr<IUbPortStatusProvider> provider,
                                         std::weak_ptr<IUbPortHealthObserver> observer,
                                         std::chrono::milliseconds queryInterval, UbPortHealthOwner owner)
    : impl_(std::make_unique<Impl>(std::move(provider), std::move(observer), queryInterval, owner))
{
}

UbPortHealthMonitor::~UbPortHealthMonitor() = default;

std::unique_ptr<UbPortHealthMonitor> UbPortHealthMonitor::CreateForTest(
    std::shared_ptr<IUbPortStatusProvider> provider, std::chrono::milliseconds queryInterval,
    std::weak_ptr<IUbPortHealthObserver> observer, UbPortHealthOwner owner)
{
    return std::unique_ptr<UbPortHealthMonitor>(
        new UbPortHealthMonitor(std::move(provider), std::move(observer), queryInterval, owner));
}

Status UbPortHealthMonitor::Start()
{
    return impl_->Start();
}

void UbPortHealthMonitor::Stop()
{
    impl_->Stop();
}

Status UbPortHealthMonitor::EnsureFresh(std::chrono::milliseconds maxAge)
{
    return impl_->EnsureFresh(maxAge);
}

Status UbPortHealthMonitor::ReadSummaryForQuery(std::chrono::milliseconds maxAge, UbPortHealthSummary &summary)
{
    return impl_->ReadSummaryForQuery(maxAge, summary);
}

Status UbPortHealthMonitor::AddObserver(std::weak_ptr<IUbPortHealthObserver> observer)
{
    return impl_->AddObserver(std::move(observer));
}

void UbPortHealthMonitor::TriggerRefresh()
{
    impl_->TriggerRefresh();
}

std::shared_ptr<const UbPortHealthSnapshot> UbPortHealthMonitor::GetSnapshot() const
{
    return impl_->GetSnapshot();
}

std::optional<UbPortHealthSummary> UbPortHealthMonitor::GetSummary() const
{
    return impl_->GetSummary();
}
}  // namespace datasystem
