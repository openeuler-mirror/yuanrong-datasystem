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
 * Description: Declare interface to store object meta in async queue.
 */

#include "datasystem/master/object_cache/store/meta_async_queue.h"

#include <cstddef>
#include <mutex>
#include <shared_mutex>
#include <unistd.h>

#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/kvstore/etcd/etcd_constants.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/log/trace.h"
#include "datasystem/common/util/format.h"

namespace datasystem {
namespace master {
AsyncElement::AsyncElement(const std::string &objectKey, const std::string &table, const std::string &key,
                           const std::string &value, AsyncElement::ReqType reqType)
    : reqType_(reqType),
      objectKey_(objectKey),
      table_(table),
      key_(key),
      value_(value),
      beginTimestamp_(std::chrono::steady_clock::now()),
      traceID_(Trace::Instance().GetTraceID())
{
}

AsyncElement::AsyncElement(const std::string &objectKey, const std::string &table, const std::string &key,
                           const std::string &value, ReqType reqType, uint64_t timestamp, const std::string &traceId)
    : reqType_(reqType),
      objectKey_(objectKey),
      table_(table),
      key_(key),
      value_(value),
      beginTimestamp_(std::chrono::microseconds(timestamp)),
      traceID_(traceId)
{
}

MetaAsyncQueue::~MetaAsyncQueue()
{
    std::lock_guard<std::mutex> l(mtx_);
    emptyCond_.notify_all();
}

void MetaAsyncQueue::AppendAsyncTask(const std::shared_ptr<AsyncElement> &element,
                                     std::shared_ptr<AsyncElement> &eliminatedEle, int &incrCnt)
{
    eliminatedEle = nullptr;
    incrCnt = 0;
    if (element == nullptr) {
        LOG(WARNING) << "input element is nullptr, it should not happen!";
        return;
    }
    element->RequestType() == AsyncElement::ReqType::ADD ? AppendAddTaskInternal(element, eliminatedEle, incrCnt)
                                                         : AppendDelTaskInternal(element, eliminatedEle, incrCnt);
}

void MetaAsyncQueue::AppendAddTaskInternal(const std::shared_ptr<AsyncElement> &element,
                                           std::shared_ptr<AsyncElement> &eliminatedEle, int &incrCnt)
{
    const auto &objectKey = element->ObjectKey();
    std::lock_guard<std::mutex> l(mtx_);
    auto it = objectMap_.find(objectKey);
    if (it != objectMap_.end()) {
        auto &objOps = it->second;
        auto opIt = objOps.find(element);
        if (opIt != objOps.end()) {
            (void)objOps.erase(opIt);
            incrCnt -= 1;
        }
        (void)objOps.emplace(element);
        incrCnt += 1;
    } else {
        (void)objectMap_[objectKey].emplace(element);
        incrCnt += 1;
    }
    eliminatedEle = orderedSet_.Put(element);
    emptyCond_.notify_all();
}

void MetaAsyncQueue::AppendDelTaskInternal(const std::shared_ptr<AsyncElement> &element,
                                           std::shared_ptr<AsyncElement> &eliminatedEle, int &incrCnt)
{
    const auto &objectKey = element->ObjectKey();
    std::lock_guard<std::mutex> l(mtx_);
    auto it = objectMap_.find(objectKey);
    if (it != objectMap_.end()) {
        auto &objOps = it->second;
        auto opIt = objOps.find(element);
        if (opIt != objOps.end()) {
            if ((*opIt)->RequestType() == AsyncElement::ReqType::ADD) {
                (void)objOps.erase(opIt);
                orderedSet_.Remove(element);
                eliminatedEle = nullptr;
                incrCnt -= 1;
                return;
            } else {
                (void)objOps.erase(opIt);
                incrCnt -= 1;
            }
        }
        (void)objOps.emplace(element);
        incrCnt += 1;
    } else {
        (void)objectMap_[objectKey].emplace(element);
        incrCnt += 1;
    }
    eliminatedEle = orderedSet_.Put(element);
    emptyCond_.notify_all();
}

bool MetaAsyncQueue::Poll(std::shared_ptr<AsyncElement> &element, int timeoutMs)
{
    INJECT_POINT("meta_async_queue.poll", []() {
        sleep(1);  // sleep 1s.
        return false;
    });
    std::unique_lock<std::mutex> l(mtx_);
    emptyCond_.wait_for(l, std::chrono::milliseconds(timeoutMs), [this]() { return orderedSet_.Size() != 0; });
    bool ret = orderedSet_.Poll(element);
    if (!ret) {
        return ret;
    }

    orderedSet_.Remove(element);
    auto it = objectMap_.find(element->ObjectKey());
    if (it != objectMap_.end()) {
        auto &opMap = it->second;
        auto opIt = opMap.find(element);
        if (opIt != opMap.end()) {
            (void)opMap.erase(opIt);
        } else {
            LOG(WARNING) << FormatString("%s not exist in op map, it should not happen", element->Key());
        }
        if (opMap.empty()) {
            (void)objectMap_.erase(it);
        }
    } else {
        LOG(WARNING) << FormatString("%s not exist in object map, it should not happen", element->Key());
    }
    return ret;
}

void MetaAsyncQueue::PollMetasByObjectKey(
    std::function<bool(const std::string &)> &&matchFunc,
    std::unordered_map<std::string, std::unordered_set<std::shared_ptr<AsyncElement>>> &outElements, uint64_t &count)
{
    count = 0;
    std::lock_guard<std::mutex> l(mtx_);
    for (auto it = objectMap_.begin(); it != objectMap_.end();) {
        const auto &objectKey = it->first;
        const auto &asynElements = it->second;
        if (!matchFunc(objectKey)) {
            ++it;
            continue;
        }
        (void)outElements.emplace(objectKey, asynElements);
        count += asynElements.size();
        for (const auto &ele : asynElements) {
            LOG_IF(WARNING, !orderedSet_.Remove(ele)) << "Remove not exist element: " << *ele;
        }
        it = objectMap_.erase(it);
    }
}

bool MetaAsyncQueue::CheckIdIsDeleting(const std::string &objKey) const
{
    std::lock_guard<std::mutex> l(mtx_);
    auto it = objectMap_.find(objKey);
    if (it == objectMap_.end()) {
        return false;
    }
    auto elements = it->second;
    for (const auto &ele : elements) {
        if (ele->Table().find(ETCD_META_TABLE_PREFIX) != std::string::npos
            && ele->RequestType() == AsyncElement::ReqType::DEL) {
            return true;
        }
    }
    return false;
}

void MetaAsyncQueue::PollAsyncElementsByObjectKey(const std::string &objectKey,
                                                 std::unordered_set<std::shared_ptr<AsyncElement>> &elements)
{
    std::lock_guard<std::mutex> l(mtx_);
    auto it = objectMap_.find(objectKey);
    if (it == objectMap_.end()) {
        return;
    }
    elements = it->second;
    for (const auto &ele : it->second) {
        LOG_IF(WARNING, !orderedSet_.Remove(ele)) << "Remove not exist element: " << *ele;
    }
    (void)objectMap_.erase(it);
}

size_t MetaAsyncQueue::Size() const
{
    std::lock_guard<std::mutex> l(mtx_);
    return orderedSet_.Size();
}
}  // namespace master
}  // namespace datasystem