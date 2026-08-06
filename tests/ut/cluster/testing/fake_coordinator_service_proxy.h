/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Thread-safe in-memory Coordinator proxy for cluster composition tests.
 */
#ifndef DATASYSTEM_TESTS_UT_CLUSTER_TESTING_FAKE_COORDINATOR_SERVICE_PROXY_H
#define DATASYSTEM_TESTS_UT_CLUSTER_TESTING_FAKE_COORDINATOR_SERVICE_PROXY_H

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "datasystem/common/coordinator/coordinator_service_proxy.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem::cluster::testing {

class FakeCoordinatorServiceProxy final : public ICoordinatorServiceProxy {
public:
    struct WatchCall {
        std::string key;
        std::string rangeEnd;
        std::string watcherAddress;
        int64_t watchId{ 0 };
    };

    FakeCoordinatorServiceProxy() = default;
    ~FakeCoordinatorServiceProxy() override = default;

    Status Init() override
    {
        return Status::OK();
    }

    Status Put(const std::string &key, const std::string &value, int64_t ttlMs, int64_t expectedVersion,
               int64_t &version, int64_t &revision, int32_t, std::string *coordinatorId,
               const std::string &expectedCoordinatorId, int64_t expectedModRevision) override
    {
        std::unique_lock<std::mutex> lock(mutex_);
        ++putAttempts_;
        putCv_.notify_all();
        if (blockNextPut_) {
            blockNextPut_ = false;
            putBlocked_ = true;
            putCv_.notify_all();
            putCv_.wait(lock, [this] { return releasePut_; });
            putBlocked_ = false;
            releasePut_ = false;
        }
        if (nextPutStatus_.IsError()) {
            const auto status = nextPutStatus_;
            nextPutStatus_ = Status::OK();
            return status;
        }
        if (!expectedCoordinatorId.empty() && expectedCoordinatorId != coordinatorId_) {
            RETURN_STATUS(K_NOT_READY, "Coordinator identity changed");
        }
        auto iter = entries_.find(key);
        const int64_t currentVersion = iter == entries_.end() ? 0 : iter->second.version;
        const int64_t currentRevision = iter == entries_.end() ? 0 : iter->second.revision;
        if (expectedModRevision != COORDINATOR_NO_MOD_REVISION_CHECK
            && expectedModRevision != currentRevision) {
            RETURN_STATUS(K_TRY_AGAIN, "Coordinator key modification revision changed");
        }
        if (expectedVersion != COORDINATOR_NO_VERSION_CHECK && expectedVersion != currentVersion) {
            RETURN_STATUS(K_TRY_AGAIN, "Coordinator key version changed");
        }
        version = currentVersion + 1;
        revision = ++revision_;
        entries_[key] = { value, version, revision, ttlMs };
        ObserveCoordinatorId(coordinatorId);
        return Status::OK();
    }

    Status Range(const std::string &key, const std::string &rangeEnd, std::vector<KeyValueEntry> &kvs,
                 int64_t &revision, int32_t, std::string *coordinatorId) override
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (key == nextRangeFailureKey_ && nextRangeFailureCode_ != K_OK && rangeFailureCount_ > 0) {
            const auto failureCode = nextRangeFailureCode_;
            if (--rangeFailureCount_ == 0) {
                nextRangeFailureKey_.clear();
                nextRangeFailureCode_ = K_OK;
            }
            return Status(failureCode, "injected Coordinator Range failure");
        }
        kvs.clear();
        AppendRange(key, rangeEnd, kvs);
        revision = revision_;
        ObserveCoordinatorId(coordinatorId);
        return Status::OK();
    }

    Status DeleteRange(const std::string &key, const std::string &rangeEnd, int64_t &deleted, int64_t &revision,
                       int32_t, int64_t expectedModRevision) override
    {
        std::lock_guard<std::mutex> lock(mutex_);
        CHECK_FAIL_RETURN_STATUS(expectedModRevision == COORDINATOR_NO_MOD_REVISION_CHECK || rangeEnd.empty(),
                                 K_INVALID, "modification revision fence requires exact delete");
        auto current = entries_.find(key);
        if (expectedModRevision != COORDINATOR_NO_MOD_REVISION_CHECK && current != entries_.end()
            && current->second.revision != expectedModRevision) {
            RETURN_STATUS(K_TRY_AGAIN, "Coordinator membership incarnation changed");
        }
        deleted = 0;
        auto iter = entries_.lower_bound(key);
        while (iter != entries_.end() && MatchesRange(iter->first, key, rangeEnd)) {
            iter = entries_.erase(iter);
            ++deleted;
        }
        if (deleted > 0) {
            ++revision_;
        }
        revision = revision_;
        return Status::OK();
    }

    Status WatchRange(const std::string &key, const std::string &rangeEnd, const std::string &watcherAddr,
                      const std::string &, int64_t &watchId, std::vector<KeyValueEntry> &initialKvs, int32_t,
                      std::string *coordinatorId) override
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (key == nextWatchFailureKey_ && nextWatchFailureCode_ != K_OK) {
            const auto failureCode = nextWatchFailureCode_;
            nextWatchFailureKey_.clear();
            nextWatchFailureCode_ = K_OK;
            return Status(failureCode, "injected Coordinator WatchRange failure");
        }
        watchId = nextWatchId_++;
        initialKvs.clear();
        AppendRange(key, rangeEnd, initialKvs);
        watchCalls_.push_back({ key, rangeEnd, watcherAddr, watchId });
        ObserveCoordinatorId(coordinatorId);
        return Status::OK();
    }

    Status CancelWatch(const std::string &, const std::vector<int64_t> &watchIds, const std::string &, int32_t) override
    {
        std::lock_guard<std::mutex> lock(mutex_);
        cancelledWatchIds_.insert(cancelledWatchIds_.end(), watchIds.begin(), watchIds.end());
        return Status::OK();
    }

    Status KeepAlive(const std::string &key, int64_t &ttlMs, int64_t &remainingTtlMs, int32_t,
                     std::string *coordinatorId, const std::string &expectedCoordinatorId, int64_t expectedModRevision,
                     const std::vector<std::string> & = {}) override
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!expectedCoordinatorId.empty() && expectedCoordinatorId != coordinatorId_) {
            RETURN_STATUS(K_NOT_READY, "Coordinator identity changed");
        }
        auto iter = entries_.find(key);
        CHECK_FAIL_RETURN_STATUS(iter != entries_.end(), K_NOT_FOUND, "Coordinator lease key is absent");
        CHECK_FAIL_RETURN_STATUS(expectedModRevision == COORDINATOR_NO_MOD_REVISION_CHECK
                                     || expectedModRevision == iter->second.revision,
                                 K_TRY_AGAIN, "Coordinator membership incarnation changed");
        ttlMs = iter->second.ttlMs;
        remainingTtlMs = ttlMs;
        ObserveCoordinatorId(coordinatorId);
        return Status::OK();
    }

    Status CAS(const std::string &key, const CasProcessFunc &process, int64_t &version, int64_t &revision) override
    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto iter = entries_.find(key);
        const std::string oldValue = iter == entries_.end() ? "" : iter->second.value;
        std::unique_ptr<std::string> newValue;
        bool retry = false;
        RETURN_IF_NOT_OK(process(oldValue, newValue, retry));
        if (retry) {
            RETURN_STATUS(K_TRY_AGAIN, "scripted CAS retry");
        }
        if (newValue != nullptr) {
            const int64_t currentVersion = iter == entries_.end() ? 0 : iter->second.version;
            version = currentVersion + 1;
            revision = ++revision_;
            entries_[key] = { *newValue, version, revision, 0 };
        } else {
            version = iter == entries_.end() ? 0 : iter->second.version;
            revision = revision_;
        }
        return Status::OK();
    }

    Status GetCoordinatorId(std::string &coordinatorId, int32_t) override
    {
        std::lock_guard<std::mutex> lock(mutex_);
        coordinatorId = coordinatorId_;
        return Status::OK();
    }

    Status ReportTopologyRecoveryCandidate(const coordinator::ReportTopologyRecoveryCandidateReqPb &request,
                                           coordinator::ReportTopologyRecoveryCandidateRspPb &response,
                                           int32_t) override
    {
        std::lock_guard<std::mutex> lock(mutex_);
        recoveryRequests_.push_back(request);
        response.set_result(coordinator::ReportTopologyRecoveryCandidateRspPb::ACCEPTED);
        const bool requestPayload = requireRecoveryPayload_ && request.canonical_topology().empty();
        response.set_recovery_state(requestPayload ? coordinator::COORDINATOR_RECOVERING
                                                   : coordinator::COORDINATOR_READY);
        response.set_payload_required(requestPayload);
        return Status::OK();
    }

    Status GetClusterRawSnapshot(const coordinator::GetClusterRawSnapshotReqPb &,
                                 coordinator::GetClusterRawSnapshotRspPb &, int32_t) override
    {
        return Status(K_RUNTIME_ERROR, "FakeCoordinatorServiceProxy does not implement GetClusterRawSnapshot");
    }

    void GetObservedCoordinatorId(std::string &coordinatorId) const override
    {
        std::lock_guard<std::mutex> lock(mutex_);
        coordinatorId = coordinatorId_;
    }

    Status PutRaw(std::string key, std::string value)
    {
        int64_t version = 0;
        int64_t revision = 0;
        return Put(key, value, 0, COORDINATOR_NO_VERSION_CHECK, version, revision, 0, nullptr, "",
                   COORDINATOR_NO_MOD_REVISION_CHECK);
    }

    void FailNextRangeForKey(std::string key, StatusCode code)
    {
        FailRangeForKeyTimes(std::move(key), code, 1);
    }

    void FailNextPut(Status status)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        nextPutStatus_ = std::move(status);
    }

    void BlockNextPut()
    {
        std::lock_guard<std::mutex> lock(mutex_);
        blockNextPut_ = true;
        putBlocked_ = false;
        releasePut_ = false;
    }

    bool WaitUntilPutBlocked(std::chrono::steady_clock::time_point deadline)
    {
        std::unique_lock<std::mutex> lock(mutex_);
        return putCv_.wait_until(lock, deadline, [this] { return putBlocked_; });
    }

    void ReleaseBlockedPut()
    {
        std::lock_guard<std::mutex> lock(mutex_);
        releasePut_ = true;
        putCv_.notify_all();
    }

    void FailRangeForKeyTimes(std::string key, StatusCode code, uint32_t count)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        nextRangeFailureKey_ = std::move(key);
        nextRangeFailureCode_ = code;
        rangeFailureCount_ = count;
    }

    void ClearRangeFailures()
    {
        std::lock_guard<std::mutex> lock(mutex_);
        nextRangeFailureKey_.clear();
        nextRangeFailureCode_ = K_OK;
        rangeFailureCount_ = 0;
    }

    void FailNextWatchForKey(std::string key, StatusCode code)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        nextWatchFailureKey_ = std::move(key);
        nextWatchFailureCode_ = code;
    }

    void RequireRecoveryPayload()
    {
        std::lock_guard<std::mutex> lock(mutex_);
        requireRecoveryPayload_ = true;
    }

    size_t RecoveryRequestCount() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return recoveryRequests_.size();
    }

    coordinator::ReportTopologyRecoveryCandidateReqPb RecoveryRequestAt(size_t index) const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return recoveryRequests_.at(index);
    }

    std::vector<WatchCall> WatchCalls() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return watchCalls_;
    }

    size_t CancelledWatchCount() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return cancelledWatchIds_.size();
    }

private:
    struct Entry {
        std::string value;
        int64_t version{ 0 };
        int64_t revision{ 0 };
        int64_t ttlMs{ 0 };
    };

    static bool MatchesRange(const std::string &candidate, const std::string &key, const std::string &rangeEnd)
    {
        return rangeEnd.empty() ? candidate == key : candidate >= key && candidate < rangeEnd;
    }

    void AppendRange(const std::string &key, const std::string &rangeEnd, std::vector<KeyValueEntry> &kvs) const
    {
        auto iter = entries_.lower_bound(key);
        while (iter != entries_.end() && MatchesRange(iter->first, key, rangeEnd)) {
            kvs.push_back({ iter->first, iter->second.value, iter->second.version, iter->second.revision });
            ++iter;
        }
    }

    void ObserveCoordinatorId(std::string *coordinatorId) const
    {
        if (coordinatorId != nullptr) {
            *coordinatorId = coordinatorId_;
        }
    }

    // Protects entries_, revision/watch identity allocation, and captured calls.
    mutable std::mutex mutex_;
    std::condition_variable putCv_;
    std::map<std::string, Entry> entries_;
    std::vector<WatchCall> watchCalls_;
    std::vector<int64_t> cancelledWatchIds_;
    std::vector<coordinator::ReportTopologyRecoveryCandidateReqPb> recoveryRequests_;
    int64_t revision_{ 1 };
    int64_t nextWatchId_{ 1 };
    size_t putAttempts_{ 0 };
    std::string coordinatorId_{ "coordinator-test" };
    std::string nextRangeFailureKey_;
    StatusCode nextRangeFailureCode_{ K_OK };
    uint32_t rangeFailureCount_{ 0 };
    Status nextPutStatus_{ Status::OK() };
    bool blockNextPut_{ false };
    bool putBlocked_{ false };
    bool releasePut_{ false };
    std::string nextWatchFailureKey_;
    StatusCode nextWatchFailureCode_{ K_OK };
    bool requireRecoveryPayload_{ false };
};

}  // namespace datasystem::cluster::testing

#endif  // DATASYSTEM_TESTS_UT_CLUSTER_TESTING_FAKE_COORDINATOR_SERVICE_PROXY_H
