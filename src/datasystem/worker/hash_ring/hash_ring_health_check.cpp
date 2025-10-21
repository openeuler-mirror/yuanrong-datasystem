/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2023. All rights reserved.
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
 * Description: The hash ring health check.
 */

#include "datasystem/worker/hash_ring/hash_ring_health_check.h"

#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <unordered_set>

#include <google/protobuf/util/message_differencer.h>

#include "datasystem/common/log/log.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/log/trace.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/utils/status.h"
#include "datasystem/worker/hash_ring/hash_ring.h"
#include "datasystem/worker/hash_ring/hash_ring_event.h"
#include "datasystem/worker/hash_ring/hash_ring_tools.h"

DS_DECLARE_uint32(node_dead_timeout_s);
DS_DECLARE_uint32(add_node_wait_time_s);
DS_DECLARE_bool(enable_hash_ring_self_healing);

namespace datasystem {
namespace worker {
const uint32_t SEC_TO_MS = 1000;

HashRingHealthCheck::HashRingHealthCheck(HashRing *hashRing) : hashRing_(hashRing)
{
    policies_.emplace_back(
        std::bind(&HashRingHealthCheck::CheckScaleUpInfo, this, std::placeholders::_1, std::placeholders::_2));
    policies_.emplace_back(std::bind(&HashRingHealthCheck::CheckVoluntaryScaleDownInfo, this, std::placeholders::_1,
                                     std::placeholders::_2));
    policies_.emplace_back(
        std::bind(&HashRingHealthCheck::CheckPassiveScaleDownInfo, this, std::placeholders::_1, std::placeholders::_2));
    policies_.emplace_back(
        std::bind(&HashRingHealthCheck::CheckWorkerInJoiningState, this, std::placeholders::_1, std::placeholders::_2));
    policies_.emplace_back(
        std::bind(&HashRingHealthCheck::CheckWorkerInLeavingState, this, std::placeholders::_1, std::placeholders::_2));
    policies_.emplace_back(
        std::bind(&HashRingHealthCheck::CheckWorkerInInitialState, this, std::placeholders::_1, std::placeholders::_2));
    policies_.emplace_back(std::bind(&HashRingHealthCheck::CheckPassiveScaleDownNotStart, this, std::placeholders::_1,
                                     std::placeholders::_2));
    policies_.emplace_back(std::bind(&HashRingHealthCheck::CheckScaleDownInfoStandbyWorker, this, std::placeholders::_1,
                                     std::placeholders::_2));
}

HashRingHealthCheck::~HashRingHealthCheck()
{
    if (!checkThread_) {
        return;
    }
    exitFlag_ = true;
    checkThread_->join();
}

void HashRingHealthCheck::Init()
{
    checkThread_ = std::make_unique<Thread>(&HashRingHealthCheck::Run, this);
    checkThread_->set_name("HashRingHealthCheck");
}

void HashRingHealthCheck::Run()
{
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    LOG(INFO) << "Start HashRing health check thread.";
    const uint32_t intervalMs = 100;  // short interval for quick shutdown.
    const int retryCount = 2;         // retry at least twice before auto-fix
    int64_t ringFixIntervalMs = std::max(FLAGS_node_dead_timeout_s, FLAGS_add_node_wait_time_s) * SEC_TO_MS * 3;
    int64_t ringRetryIntervalMs = ringFixIntervalMs / retryCount;
    auto checkInterval = std::min(ringRetryIntervalMs, ringFixIntervalMs);
    Timer timer;
    while (!exitFlag_) {
        INJECT_POINT("worker.HashRingHealthCheck", [&](uint32_t interval) {
            ringRetryIntervalMs = interval;
            ringFixIntervalMs = ringRetryIntervalMs * retryCount;
            checkInterval = std::min(ringRetryIntervalMs, ringFixIntervalMs);
        });
        std::this_thread::sleep_for(std::chrono::milliseconds(intervalMs));
        auto elapsed = static_cast<int64_t>(GetLastUpdateElapsedMs());
        int64_t checkRetryRemainTime = ringRetryIntervalMs - elapsed;
        int64_t checkFixRemainTime = ringFixIntervalMs - elapsed;
        if (timer.ElapsedMilliSecond() >= checkInterval) {
            LOG_IF_ERROR(DoHealthCheck(checkRetryRemainTime <= 0, checkFixRemainTime <= 0), "DoHealthCheck failed");
            auto remainTime = std::max<int64_t>(std::max(checkRetryRemainTime, checkFixRemainTime), 0);
            if (remainTime == 0) {
                checkInterval = std::min(ringRetryIntervalMs, ringFixIntervalMs);
                ResetUpdateTime();
            } else {
                checkInterval = std::min(checkInterval, remainTime);
            }
            timer.Reset();
        }
    }
    LOG(INFO) << "Terminating HashRing health check thread.";
}

Status HashRingHealthCheck::DoHealthCheck(bool checkRetry, bool checkFix)
{
    RETURN_RUNTIME_ERROR_IF_NULL(hashRing_);
    HashRingPb ring;
    std::string ringStr;
    int64_t version;
    RETURN_IF_NOT_OK(TryGetAndParseHashRingPb(ring, ringStr, version));

    bool changed = IsChanged(ringStr, version);
    LOG(INFO) << "DoHealthCheck checkRetry:" << checkRetry << ", checkFix:" << checkFix << ", changed:" << changed;

    // the new version will be processed in HashRing::HandleRingEvent, skip and wait for next check
    if (changed) {
        LOG(INFO) << "ring has changed to version " << version << ", pass.";
        return Status::OK();
    }

    if (checkRetry) {
        hashRing_->RestoreScalingTaskIfNeeded(false);
    }

    if (checkFix) {
        bool autoFix = FLAGS_enable_hash_ring_self_healing;
        HashRingPb newRing = ring;
        if (!CheckHashRing(autoFix, newRing)) {
            return Status::OK();
        }
        (void)ClearWorkerMapIfNeed(newRing);
        RETURN_IF_NOT_OK(hashRing_->etcdStore_->CAS(
            ETCD_RING_PREFIX, "",
            [&newRing, &ring](const std::string &oldValue, std::unique_ptr<std::string> &newValue, bool & /* retry */) {
                HashRingPb oldRing;
                if (!oldRing.ParseFromString(oldValue)) {
                    LOG(WARNING) << "Failed to parse HashRingPb from string. give up and wait for next time if needed.";
                    return Status::OK();
                }
                google::protobuf::util::MessageDifferencer differencer;
                differencer.set_repeated_field_comparison(google::protobuf::util::MessageDifferencer::AS_SET);
                if (!differencer.Compare(oldRing, ring)) {
                    VLOG(1) << "ring has changed. give up and wait for next time if needed.";
                    return Status::OK();
                }

                newValue = std::make_unique<std::string>(newRing.SerializeAsString());
                return Status::OK();
            }));
    }

    return Status::OK();
}

bool HashRingHealthCheck::IsChanged(const std::string &value, int64_t version)
{
    std::unique_lock<std::mutex> lock(mutex_);
    return value != preHashRingStr_ || preVersion_ != version;
}

Status HashRingHealthCheck::TryGetAndParseHashRingPb(HashRingPb &ring, std::string &value, int64_t &version)
{
    int maxRetryCount = 3;
    int retryIntervalMs = std::min<int>(60000, FLAGS_node_dead_timeout_s * SEC_TO_MS);
    INJECT_POINT("worker.TryGetAndParseHashRingPb.interval", [&retryIntervalMs](int interval) {
        retryIntervalMs = interval;
        return Status::OK();
    });
    bool failed = true;
    std::string errInfo;
    do {
        RangeSearchResult res;
        Status rc = hashRing_->etcdStore_->Get(ETCD_RING_PREFIX, "", res);
        if (rc.IsOk()) {
            value = std::move(res.value);
            version = res.modRevision;
            if (ring.ParseFromString(value)) {
                failed = false;
                break;
            }
            errInfo = "cannot parse HashRingPb";
        } else if (rc.GetCode() != StatusCode::K_NOT_FOUND) {
            return rc;
        } else {
            errInfo = "HashRing not found in etcd";
        }
        maxRetryCount--;
        if (maxRetryCount == 0) {
            break;
        }
        LOG(ERROR) << errInfo << ", retry after " << retryIntervalMs << "ms";
        std::this_thread::sleep_for(std::chrono::milliseconds(retryIntervalMs));
    } while (true);

    if (!failed) {
        return Status::OK();
    }
    LOG(ERROR) << "HashRingHealthCheck: " << errInfo;
    if (FLAGS_enable_hash_ring_self_healing) {
        LOG(INFO) << "try init empty hash ring.";
        RETURN_IF_NOT_OK(hashRing_->etcdStore_->CAS(
            ETCD_RING_PREFIX, "",
            [](const std::string &oldValue, std::unique_ptr<std::string> &newValue, bool & /* retry */) {
                HashRingPb oldRing;
                if (!oldValue.empty() && oldRing.ParseFromString(oldValue)) {
                    LOG(INFO) << "Parse HashRingPb from string success, give up clear hash ring.";
                    return Status::OK();
                }
                HashRingPb newRing;
                newValue = std::make_unique<std::string>(newRing.SerializeAsString());
                return Status::OK();
            }));
    }
    return Status::OK();
}

void HashRingHealthCheck::UpdateRing(const std::string &hashRingStr, int64_t version)
{
    std::unique_lock<std::mutex> lock(mutex_);
    preHashRingStr_ = hashRingStr;
    preVersion_ = version;
    timer_.Reset();
}

void HashRingHealthCheck::ResetUpdateTime()
{
    std::unique_lock<std::mutex> lock(mutex_);
    timer_.Reset();
}

uint64_t HashRingHealthCheck::GetLastUpdateElapsedMs()
{
    return timer_.ElapsedMilliSecond();
}

bool HashRingHealthCheck::CheckHashRing(bool autoFix, HashRingPb &ring)
{
    HashRingPb oldRing = ring;
    bool hashRingAbnormal = false;
    for (auto &func : policies_) {
        hashRingAbnormal |= func(autoFix, ring);
    }
    google::protobuf::util::MessageDifferencer differencer;
    differencer.set_repeated_field_comparison(google::protobuf::util::MessageDifferencer::AS_SET);
    bool ringChanged = !differencer.Compare(oldRing, ring);
    return hashRingAbnormal && ringChanged;
}

bool HashRingHealthCheck::CheckScaleUpInfo(bool autoFix, HashRingPb &ring)
{
    // Exists scale up task and the src node is not exists in del_node_info.
    bool flag = false;
    std::vector<std::string> workers;
    for (const auto &info : ring.add_node_info()) {
        auto iter = ring.mutable_workers()->find(info.first);
        if (iter == ring.mutable_workers()->end() || iter->second.state() != WorkerPb::JOINING) {
            continue;
        }
        bool isSrcPassiveScaleDown = false;
        for (const auto &range : info.second.changed_ranges()) {
            if (!range.finished() && ring.del_node_info().find(range.workerid()) != ring.del_node_info().end()) {
                isSrcPassiveScaleDown = true;
            }
        }
        if (isSrcPassiveScaleDown) {
            continue;
        }
        flag = true;
        workers.emplace_back(info.first);
    }
    LOG_IF(ERROR, flag) << "HashRingHealthCheck: exists scale up info.";
    if (flag && autoFix) {
        for (const auto &worker : workers) {
            ring.mutable_add_node_info()->erase(worker);
            ring.mutable_workers()->erase(worker);
        }
        LOG(INFO) << "Auto fix ScaleUp new ring is: " << HashRing::SummarizeHashRing(ring);
    }
    return flag;
}

bool HashRingHealthCheck::CheckScaleDownInfoStandbyWorker(bool, HashRingPb &ring)
{
    bool flag = false;
    HashRingPb ringAfterClear = ring;
    ringAfterClear.clear_del_node_info();
    for (const auto &info : ring.del_node_info()) {
        for (auto range : info.second.changed_ranges()) {
            if (ring.workers().find(range.workerid()) != ring.workers().end()) {
                (*ringAfterClear.mutable_del_node_info())[info.first].mutable_changed_ranges()->Add(std::move(range));
            } else {
                flag = true;
            }
        }
    }
    if (flag) {
        LOG(INFO) << "Auto fix Scale down info standby worker not exist, old ring is: "
                  << HashRing::SummarizeHashRing(ring);
        ring = ringAfterClear;
        LOG(INFO) << "Auto fix Scale down info standby worker not exist, new ring is: "
                  << HashRing::SummarizeHashRing(ring);
    }
    return flag;
}

bool HashRingHealthCheck::CheckVoluntaryScaleDownInfo(bool autoFix, HashRingPb &ring)
{
    bool flag = false;
    std::set<std::string> srcWorkers;
    std::set<std::string> dstWorkers;
    for (const auto &info : ring.add_node_info()) {
        bool isSrcPassiveScaleDown = false;
        std::set<std::string> workers;
        for (const auto &range : info.second.changed_ranges()) {
            auto iter = ring.workers().find(range.workerid());
            if (iter == ring.workers().end() || iter->second.state() != WorkerPb::LEAVING
                || !iter->second.need_scale_down()) {
                continue;
            }

            if (!range.finished() && ring.del_node_info().find(range.workerid()) != ring.del_node_info().end()) {
                isSrcPassiveScaleDown = true;
            }
            workers.emplace(range.workerid());
        }
        if (isSrcPassiveScaleDown || workers.empty()) {
            continue;
        }
        flag = true;
        srcWorkers.insert(workers.begin(), workers.end());
        dstWorkers.emplace(info.first);
    }
    LOG_IF(ERROR, flag) << "HashRingHealthCheck: exists voluntary scale down info.";
    if (flag && autoFix) {
        for (const auto &worker : srcWorkers) {
            ring.mutable_workers()->erase(worker);
        }
        for (const auto &worker : dstWorkers) {
            ring.mutable_add_node_info()->erase(worker);
        }
        LOG(INFO) << "Auto fix VoluntaryScaleDown new ring is: " << HashRing::SummarizeHashRing(ring);
    }
    return flag;
}

bool HashRingHealthCheck::CheckPassiveScaleDownInfo(bool autoFix, HashRingPb &ring)
{
    bool flag = ring.del_node_info_size() > 0;
    LOG_IF(ERROR, flag) << "HashRingHealthCheck: exists passive scale down info.";
    if (!flag || !autoFix) {
        return flag;
    }
    std::unordered_set<std::string> workers;
    for (const auto &info : ring.del_node_info()) {
        workers.emplace(info.first);
        ring.mutable_workers()->erase(info.first);
    }
    ring.clear_del_node_info();
    // Check if the scale down node is src of scale up task.
    for (auto &info : *ring.mutable_add_node_info()) {
        for (auto &range : *info.second.mutable_changed_ranges()) {
            if (workers.count(range.workerid()) > 0) {
                range.set_finished(true);
            }
        }
    }
    LOG(INFO) << "Auto fix PassiveScaleDown new ring is: " << HashRing::SummarizeHashRing(ring);
    return flag;
}

bool HashRingHealthCheck::CheckWorkerInJoiningState(bool, HashRingPb &ring)
{
    std::vector<std::string> workers;
    for (const auto &worker : ring.workers()) {
        if (worker.second.state() == WorkerPb::JOINING
            && ring.add_node_info().find(worker.first) == ring.add_node_info().end()) {
            workers.emplace_back(worker.first);
        }
    }
    if (!workers.empty()) {
        LOG(ERROR)
            << "HashRingHealthCheck: worker [" << VectorToString(workers)
            << "] in JOINING state for a long time, but not found in add_node_info, need manual shutdown to fix it.";
    }
    return !workers.empty();
}

bool HashRingHealthCheck::CheckWorkerInLeavingState(bool, HashRingPb &ring)
{
    std::vector<std::string> workers;
    for (const auto &worker : ring.workers()) {
        if (worker.second.state() != WorkerPb::LEAVING) {
            continue;
        }
        bool isSrcNode = false;
        for (const auto &info : ring.add_node_info()) {
            const auto &changedRanges = info.second.changed_ranges();
            auto iter = std::find_if(
                changedRanges.begin(), changedRanges.end(),
                [&worker](const ChangeNodePb::RangePb &range) { return worker.first == range.workerid(); });
            if (iter != changedRanges.end()) {
                isSrcNode = true;
            }
        }
        if (!isSrcNode) {
            workers.emplace_back(worker.first);
        }
    }
    if (!workers.empty()) {
        LOG(ERROR) << "HashRingHealthCheck: worker [" << VectorToString(workers)
                   << "] in LEAVING state for a long time, but not found in add_node_info.";
    }
    return !workers.empty();
}

bool HashRingHealthCheck::CheckWorkerInInitialState(bool, HashRingPb &ring)
{
    std::vector<std::string> workers;
    for (const auto &worker : ring.workers()) {
        if (worker.second.state() == WorkerPb::INITIAL) {
            workers.emplace_back(worker.first);
        }
    }
    if (!workers.empty()) {
        LOG(ERROR) << "HashRingHealthCheck: worker [" << VectorToString(workers)
                   << "] in INITIAL state for a long time.";
    }
    return !workers.empty();
}

bool HashRingHealthCheck::CheckPassiveScaleDownNotStart(bool, HashRingPb &ring)
{
    std::vector<std::string> workers;
    std::unordered_set<std::string> failedWorkers;
    if (hashRing_ != nullptr) {
        HashRingEvent::GetFailedWorkers::GetInstance().NotifyAll(failedWorkers);
    }
    INJECT_POINT("worker.CheckPassiveScaleDownNotStart", [&failedWorkers](const std::string &workerAddr) {
        failedWorkers.insert(workerAddr);
        return true;
    });

    for (const auto &workerAddr : failedWorkers) {
        auto iter = ring.workers().find(workerAddr);
        if (iter == ring.workers().end()) {
            continue;
        }
        if (iter->second.state() == WorkerPb::ACTIVE
            && ring.add_node_info().find(workerAddr) == ring.add_node_info().end()) {
            workers.emplace_back(workerAddr);
        }
    }
    if (!workers.empty()) {
        LOG(ERROR) << "HashRingHealthCheck: worker [" << VectorToString(workers)
                   << "] is ACTIVE in HashRingPb, but in fault state in EtcdClusterManager.";
    }
    return !workers.empty();
}
}  // namespace worker
}  // namespace datasystem
