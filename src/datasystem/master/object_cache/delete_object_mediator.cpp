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
 * Description: Module responsible for provide mediator in master's delete process.
 */

#include "datasystem/master/object_cache/delete_object_mediator.h"
#include "datasystem/master/object_cache/oc_metadata_manager.h"

namespace datasystem {
namespace master {
DeleteObjectMediator::DeleteObjectMediator(const std::string &workerAddress,
                                           const std::unordered_map<std::string, bool> &map)
    : sourceWorker_(workerAddress), reqIdsMap_(map)
{
    lastRc_ = Status::OK();
}

std::vector<std::string> DeleteObjectMediator::GetObjKeys()
{
    std::vector<std::string> ids;
    ids.reserve(reqIdsMap_.size());
    for (const auto &kv : reqIdsMap_) {
        ids.emplace_back(kv.first);
    }
    return ids;
}

void DeleteObjectMediator::SetStatusIfError(const Status &status)
{
    if (status.IsError()) {
        SetStatus(status);
    }
}

void DeleteObjectMediator::SetStatus(const Status &status)
{
    lastRc_ = status;
}

const std::unordered_set<std::string> &DeleteObjectMediator::GetSuccessDelIds() const
{
    return successDelIds_;
}

void DeleteObjectMediator::AddSuccessDelId(const std::string &objKey)
{
    successDelIds_.emplace(objKey);
}

const std::unordered_set<std::string> &DeleteObjectMediator::GetFailedObjs() const
{
    return failedIds_;
}

void DeleteObjectMediator::AddFailedDelId(const std::string &objKey)
{
    failedIds_.emplace(objKey);
}

const std::unordered_set<std::string> &DeleteObjectMediator::GetHashObjsWithoutMeta() const
{
    return hashObjsWithoutMeta_;
}

void DeleteObjectMediator::AddHashObjsWithoutMeta(const std::string &objKey)
{
    hashObjsWithoutMeta_.emplace(objKey);
}

const datasystem::Status &DeleteObjectMediator::GetStatus() const
{
    return lastRc_;
}

const std::unordered_map<std::string, bool> &DeleteObjectMediator::GetReqIdsMap() const
{
    return reqIdsMap_;
}

void DeleteObjectMediator::SetIdsNeedToNotifyWorker(
    std::unordered_map<std::string, DeleteStruct> &&map)
{
    idsNeedToNotifyWorker_ = std::move(map);
}

std::unordered_map<std::string, DeleteStruct> &DeleteObjectMediator::GetIdsNeedToNotifyWorker()
{
    return idsNeedToNotifyWorker_;
}

bool DeleteObjectMediator::CheckNoNeedToNotifyWorker()
{
    for (const auto &kv : idsNeedToNotifyWorker_) {
        if (!kv.second.locations.empty()) {
            return false;
        }
    }
    return true;
}

const std::string &DeleteObjectMediator::GetSourceWorker() const
{
    return sourceWorker_;
}

std::vector<std::string> DeleteObjectMediator::GetNotRefIds()
{
    std::vector<std::string> noRefIds;
    // successDelIds may include some nested children objects but we don't need to send these to worker.
    for (const auto &objectKey : GetObjKeys()) {
        if (successDelIds_.count(objectKey) > 0) {
            noRefIds.emplace_back(objectKey);
        }
    }
    return noRefIds;
}

void DeleteObjectMediator::SetToBeNotifiedNestedRefs(std::vector<std::string> &&nestedRefsIds)
{
    toBeNotifiedNestedRefs_ = std::move(nestedRefsIds);
}

const std::vector<std::string> &DeleteObjectMediator::GetToBeNotifiedNestedRefs() const
{
    return toBeNotifiedNestedRefs_;
}

void DeleteObjectMediator::SetObjKey2Version(std::unordered_map<std::string, int64_t> &&objKey2Version)
{
    objKey2Version_ = std::move(objKey2Version);
}

bool DeleteObjectMediator::CheckIfExpired(const std::string &objKey, int64_t currVersion)
{
    auto iter = objKey2Version_.find(objKey);
    return iter != objKey2Version_.end() && currVersion > iter->second;
}

int64_t DeleteObjectMediator::GetObjectVersionInRequest(const std::string &objKey)
{
    auto iter = objKey2Version_.find(objKey);
    return iter != objKey2Version_.end() ? iter->second : -1;
}

void DeleteObjectMediator::SetOutdatedObj(const std::string &outdatedObj)
{
    outdatedObjs_.emplace_back(outdatedObj);
}

const std::vector<std::string> &DeleteObjectMediator::GetOutdatedObjs() const
{
    return outdatedObjs_;
}
}  // namespace master
}  // namespace datasystem