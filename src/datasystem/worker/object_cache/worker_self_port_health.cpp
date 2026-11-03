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

#include "datasystem/worker/object_cache/worker_self_port_health.h"

#include <exception>
#include <utility>

#include "datasystem/common/log/log.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {
namespace object_cache {

WorkerSelfPortHealth::~WorkerSelfPortHealth()
{
    Stop();
}

void WorkerSelfPortHealth::Attach(std::shared_ptr<PeerUbAdmission> admission, const HostPort &selfWorker)
{
    {
        std::lock_guard<bthread::Mutex> lock(mutex_);
        admission_ = admission;
        selfWorker_ = selfWorker;
    }
    if (admission != nullptr) {
        admission->EnableVerifiedPortHealth();
        std::weak_ptr<WorkerSelfPortHealth> weakSelf = weak_from_this();
        admission->SetSelfPortHealthRefreshTrigger([weakSelf] {
            if (auto self = weakSelf.lock()) {
                self->ReportPortHealthTrigger();
            }
        });
    }
}

Status WorkerSelfPortHealth::Configure(std::shared_ptr<UbPortHealthMonitor> monitor,
                                       std::weak_ptr<IUbPortHealthObserver> secondaryObserver)
{
    CHECK_FAIL_RETURN_STATUS(monitor != nullptr, K_INVALID, "UB port health monitor is null");
    {
        std::lock_guard<bthread::Mutex> lock(mutex_);
        if (monitor_ != nullptr) {
            return Status::OK();
        }
        monitor_ = monitor;
        secondaryObserver_ = std::move(secondaryObserver);
    }
    return monitor->AddObserver(weak_from_this());
}

std::shared_ptr<UbPortHealthMonitor> WorkerSelfPortHealth::GetMonitor() const
{
    std::lock_guard<bthread::Mutex> lock(mutex_);
    return monitor_;
}

void WorkerSelfPortHealth::Stop()
{
    std::lock_guard<bthread::Mutex> lock(mutex_);
    monitor_.reset();
    admission_.reset();
    secondaryObserver_.reset();
}

void WorkerSelfPortHealth::ReportPortHealthTrigger()
{
    auto monitor = GetMonitor();
    if (monitor != nullptr) {
        monitor->TriggerRefresh();
    }
}

std::optional<UbPortHealthSummary> WorkerSelfPortHealth::GetSummary() const
{
    auto monitor = GetMonitor();
    return monitor == nullptr ? std::nullopt : monitor->GetSummary();
}

bool WorkerSelfPortHealth::IsEnabled() const
{
    std::lock_guard<bthread::Mutex> lock(mutex_);
    return monitor_ != nullptr;
}

Status WorkerSelfPortHealth::QueryUbPortHealth(const std::string &expectedWorkerIncarnation,
                                               UbPortHealthSummary &summary)
{
    if (expectedWorkerIncarnation.empty()) {
        return Status(K_INVALID, "Expected Worker incarnation is empty");
    }
    auto monitor = GetMonitor();
    if (monitor == nullptr) {
        return Status(K_NOT_READY, "Worker UB port health monitor is not configured");
    }
    return monitor->ReadSummaryForQuery(std::chrono::milliseconds::zero(), summary);
}

void WorkerSelfPortHealth::OnUbPortHealthChanged(const UbPortHealthSummary &summary)
{
    std::shared_ptr<PeerUbAdmission> admission;
    HostPort selfWorker;
    std::shared_ptr<IUbPortHealthObserver> observer;
    {
        std::lock_guard<bthread::Mutex> lock(mutex_);
        admission = admission_.lock();
        selfWorker = selfWorker_;
        observer = secondaryObserver_.lock();
    }
    // Self admission accepts only confirmed local port facts; an E4 alone only triggers a query.
    if (admission != nullptr && !selfWorker.Empty()) {
        (void)admission->ApplyPortHealth(selfWorker, summary, UbPortHealthEvidenceSource::PASSIVE_SUMMARY);
    }
    if (observer != nullptr) {
        try {
            observer->OnUbPortHealthChanged(summary);
        } catch (const std::exception &e) {
            LOG(ERROR) << "Worker self port health secondary observer threw: " << e.what();
        } catch (...) {
            LOG(ERROR) << "Worker self port health secondary observer threw an unknown exception";
        }
    }
}

}  // namespace object_cache
}  // namespace datasystem
