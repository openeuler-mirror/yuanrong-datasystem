/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
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
 * Description: SharedPageQueueGroup
 */

#include "datasystem/worker/stream_cache/page_queue/shared_page_queue_group.h"
#include <memory>
#include <mutex>
#include <shared_mutex>

#include "datasystem/common/constants.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/utils/status.h"
#include "datasystem/common/iam/tenant_auth_manager.h"

namespace {
static bool ValidateGroupCount(const char *flagname, uint32_t value)
{
    const int32_t minValue = 1;
    const int32_t maxValue = 64;
    if ((value < minValue) || (value > maxValue)) {
        LOG(ERROR) << "The " << flagname << " must be between " << minValue << " and " << maxValue << ".";
        return false;
    }
    return true;
}
}  // namespace

DS_DEFINE_uint32(sc_shared_page_group_count, 4, "the shared page count for each remote worker");
DS_DEFINE_validator(sc_shared_page_group_count, &ValidateGroupCount);

namespace datasystem {
namespace worker {
namespace stream_cache {

SharedPageQueueGroup::SharedPageQueueGroup(HostPort remoteWorker,
                                           std::shared_ptr<WorkerSCAllocateMemory> scAllocateManager,
                                           ClientWorkerSCServiceImpl *scSvc)
    : partCount_(FLAGS_sc_shared_page_group_count),
      remoteWorker_(std::move(remoteWorker)),
      scAllocateManager_(std::move(scAllocateManager)),
      scSvc_(scSvc)
{
}

SharedPageQueueGroup::~SharedPageQueueGroup() = default;

size_t SharedPageQueueGroup::GetPartId(const std::string &streamName) const
{
    return std::hash<std::string>{}(streamName) % partCount_;
}

std::vector<std::string> SharedPageQueueGroup::GetAllSharedPageName()
{
    std::vector<std::string> names;
    std::shared_lock<std::shared_timed_mutex> locker(mutex_);
    for (auto &it : tenantPageQueues_) {
        for (auto &page : it.second) {
            names.emplace_back(page->GetStreamName());
        }
    }
    return names;
}

Status SharedPageQueueGroup::GetSharedPageQueue(const std::string &streamName,
                                                std::shared_ptr<SharedPageQueue> &pageQueue)
{
    auto tenantId = TenantAuthManager::ExtractTenantId(streamName);
    auto realStreamName = TenantAuthManager::ExtractRealObjectKey(streamName);
    std::shared_lock<std::shared_timed_mutex> locker(mutex_);
    auto iter = tenantPageQueues_.find(tenantId);
    if (iter != tenantPageQueues_.end()) {
        auto index = GetPartId(realStreamName);
        pageQueue = iter->second[index];
        return Status::OK();
    }
    RETURN_STATUS(K_NOT_FOUND, FormatString("not found page queue for tenant %s", tenantId));
}

void SharedPageQueueGroup::GetOrCreateSharedPageQueue(const std::string &streamName,
                                                      std::shared_ptr<SharedPageQueue> &pageQueue)
{
    auto rc = GetSharedPageQueue(streamName, pageQueue);
    if (rc.IsOk()) {
        return;
    }
    auto tenantId = TenantAuthManager::ExtractTenantId(streamName);
    auto realStreamName = TenantAuthManager::ExtractRealObjectKey(streamName);
    std::lock_guard<std::shared_timed_mutex> locker(mutex_);
    auto iter = tenantPageQueues_.find(tenantId);
    if (iter == tenantPageQueues_.end()) {
        std::vector<std::shared_ptr<SharedPageQueue>> pageQueues(partCount_);
        for (size_t i = 0; i < partCount_; i++) {
            pageQueues[i] = std::make_shared<SharedPageQueue>(tenantId, remoteWorker_, i, scAllocateManager_, scSvc_);
        }
        auto ret = tenantPageQueues_.emplace(std::move(tenantId), std::move(pageQueues));
        iter = ret.first;
    }
    auto index = GetPartId(realStreamName);
    pageQueue = iter->second[index];
}

Status SharedPageQueueGroup::RemoveSharedPageQueueForTenant(const std::string &tenantId)
{
    std::lock_guard<std::shared_timed_mutex> locker(mutex_);
    auto iter = tenantPageQueues_.find(tenantId);
    if (iter != tenantPageQueues_.end()) {
        tenantPageQueues_.erase(iter);
        return Status::OK();
    }
    RETURN_STATUS(K_NOT_FOUND, FormatString("not found page queue for tenant %s", tenantId));
}

}  // namespace stream_cache
}  // namespace worker
}  // namespace datasystem
