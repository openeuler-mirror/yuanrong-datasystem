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
 * Description: Persistence API implementation for aggregated slot-based storage.
 */

#include "datasystem/common/l2cache/aggregated_persistence_api.h"

#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {
AggregatedPersistenceApi::AggregatedPersistenceApi(std::unique_ptr<StorageClient> storageClient)
    : storageClient_(std::move(storageClient))
{
}

AggregatedPersistenceApi::~AggregatedPersistenceApi() = default;

Status AggregatedPersistenceApi::Init()
{
    CHECK_FAIL_RETURN_STATUS(storageClient_ != nullptr, K_RUNTIME_ERROR, "storageClient_ is nullptr");
    return storageClient_->Init();
}

Status AggregatedPersistenceApi::Save(const std::string &objectKey, uint64_t version, int64_t timeoutMs,
                                      const std::shared_ptr<std::iostream> &body, uint64_t asyncElapse,
                                      WriteMode writeMode)
{
    INJECT_POINT("PersistenceApi.Save.timeout", [&timeoutMs](int timeout) {
        timeoutMs = timeout;
        return Status::OK();
    });
    LOG(INFO) << FormatString("invoke save object to aggregated persistence. objectKey:%s, version %llu", objectKey,
                              version);
    INJECT_POINT("persistence.service.save");
    return storageClient_->Save(objectKey, version, timeoutMs, body, asyncElapse, writeMode);
}

Status AggregatedPersistenceApi::Get(const std::string &objectKey, uint64_t version, int64_t timeoutMs,
                                     std::shared_ptr<std::stringstream> &content)
{
    INJECT_POINT("persistence.service.get", [&content](std::string mockContent) {
        *content.get() << mockContent;
        return Status::OK();
    });
    auto rc = storageClient_->Get(objectKey, version, timeoutMs, content);
    if (rc.GetCode() == StatusCode::K_NOT_FOUND || rc.GetCode() == StatusCode::K_NOT_FOUND_IN_L2CACHE) {
        return GetWithoutVersion(objectKey, timeoutMs, 0, content);
    }
    return rc;
}

Status AggregatedPersistenceApi::GetWithoutVersion(const std::string &objectKey, int64_t timeoutMs, uint64_t minVersion,
                                                   std::shared_ptr<std::stringstream> &content)
{
    return storageClient_->GetWithoutVersion(objectKey, timeoutMs, minVersion, content);
}

Status AggregatedPersistenceApi::Del(const std::string &objectKey, uint64_t maxVerToDelete, bool deleteAllVersion,
                                     uint64_t asyncElapse, const uint64_t *const objectVersion,
                                     bool listIncompleteVersions)
{
    INJECT_POINT("persistence.service.del");
    LOG(INFO) << FormatString("invoke delete object from aggregated persistence. objectKey: %s, max version is %llu",
                              objectKey, maxVerToDelete);
    (void)objectVersion;
    (void)listIncompleteVersions;
    return storageClient_->Delete(objectKey, maxVerToDelete, deleteAllVersion, asyncElapse);
}

Status AggregatedPersistenceApi::PreloadSlot(const std::string &sourceWorkerAddress, uint32_t slotId,
                                             const SlotPreloadCallback &callback)
{
    return storageClient_->PreloadSlot(sourceWorkerAddress, slotId, callback);
}

Status AggregatedPersistenceApi::MergeSlot(const std::string &sourceWorkerAddress, uint32_t slotId)
{
    return storageClient_->MergeSlot(sourceWorkerAddress, slotId);
}

std::string AggregatedPersistenceApi::GetL2CacheRequestSuccessRate() const
{
    if (storageClient_ == nullptr) {
        return "";
    }
    return storageClient_->GetRequestSuccessRate();
}
}  // namespace datasystem
