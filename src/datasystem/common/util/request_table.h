/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2024. All rights reserved.
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
 * Description: Define the RequestTable class to add, remove, update, and query subscription requests.
 */
#ifndef DATASYSTEM_UTILS_REQUEST_TABLE_H
#define DATASYSTEM_UTILS_REQUEST_TABLE_H

#include <tbb/concurrent_hash_map.h>
#include <cstdint>

#include "datasystem/common/immutable_string/immutable_string.h"
#include "datasystem/common/log/access_recorder.h"
#include "datasystem/common/object_cache/object_base.h"
#include "datasystem/common/rpc/rpc_server_stream_base.h"
#include "datasystem/worker/object_cache/object_kv.h"

namespace datasystem {

template <typename Request>
class RequestTable {
public:
    /**
     * @brief Add request to Worker/MasterRequestManager.
     * @param[in] objectKey The object key.
     * @param[in] request The request that is waiting on the object key.
     * @return Status of the call.
     */
    Status AddRequest(const std::string &objectKey, std::shared_ptr<Request> &request)
    {
        RETURN_RUNTIME_ERROR_IF_NULL(request);
        std::lock_guard<std::shared_timed_mutex> lck(mutex_);
        requestTable_[objectKey].push_back(request);
        return Status::OK();
    }

    /**
     * @brief Check if the object is in getting object.
     * @param[in] objectKey Object key.
     * @return True if object is in getting.
     */
    bool ObjectInRequest(const std::string &objectKey)
    {
        std::shared_lock<std::shared_timed_mutex> lck(mutex_);
        return requestTable_.find(objectKey) != requestTable_.end();
    }

    /**
     * @brief Remove the request from the waiting requests table.
     * @param[in] request The request need to remove.
     */
    void RemoveRequest(std::shared_ptr<Request> &request)
    {
        std::lock_guard<std::shared_timed_mutex> locker(mutex_);
        for (auto &objectKey : request->deduplicatedObjectKeys_) {
            auto iter = requestTable_.find(objectKey);
            if (iter == requestTable_.end()) {
                continue;
            }
            auto &requestsOnObject = iter->second;
            // Erase request from the list.
            auto it = std::find(requestsOnObject.begin(), requestsOnObject.end(), request);
            if (it == requestsOnObject.end()) {
                continue;
            }
            requestsOnObject.erase(it);
            // If the vector is empty, remove the object key from the map.
            if (requestsOnObject.empty()) {
                requestTable_.erase(iter);
            }
        }
    }

    /**
     * @brief Remove the request from the waiting requests table.
     * @param[in] key The key need to remove.
     */
    void EraseSub(const std::string &key)
    {
        std::lock_guard<std::shared_timed_mutex> locker(mutex_);
        (void)requestTable_.erase(key);
    }

    /**
     * @brief Update request info after object sealed.
     * @param[in] objectKey The object key.
     * @param[in] entryParam The object entry parameter.
     * @param[in] lastRc The last error.
     * @param[in] doneRequestCallBack Callback function for replying to a done request.
     * @param[in] specRequset Specifying a request, default is nullptr.
     * @param[in] isUpdateSubRecvEventRequest Indicates whether the request is a subscription request, default is false.
     * For SubRecvEvent request, return immediately as long as there is an object is ready.
     * @return Status of the call.
     */
    template <typename EntryParam>
    Status UpdateRequest(
        const std::string &objectKey, std::shared_ptr<EntryParam> entryParam, Status lastRc,
        std::function<void(std::shared_ptr<Request>)> doneRequestCallBack,
        const std::shared_ptr<Request> &specRequset = nullptr, bool isUpdateSubRecvEventRequest = false,
        std::function<bool(const std::string &objKey, const std::shared_ptr<Request>)> checkOffsetMatch = nullptr)
    {
        std::vector<std::shared_ptr<Request>> requests;
        {
            std::shared_lock<std::shared_timed_mutex> lck(mutex_);
            auto it = requestTable_.find(objectKey);
            RETURN_OK_IF_TRUE(it == requestTable_.end());

            LOG(INFO) << FormatString("Update request for objectKey: %s, status:%s", objectKey, lastRc.ToString());
            // Avoid acquiring locks for both WorkerRequestManager/MasterDevReqManager and xxRequest at the same time.
            requests = it->second;
        }
        std::vector<std::shared_ptr<Request>> completedRequests;
        for (auto &req : requests) {
            std::lock_guard<std::mutex> locker(req->mutex_);
            if (specRequset != nullptr && specRequset != req) {
                continue;
            }
            if (checkOffsetMatch && lastRc.IsOk()) {
                if (!checkOffsetMatch(objectKey, req)) {
                    LOG(INFO) << "param offset and size is not match request, not return to client";
                    continue;
                }
            }
            if (req->objects_.emplace(objectKey, entryParam)) {
                req->SetStatus(lastRc);
                (void)req->numSatisfiedObjects_.fetch_add(1);
            } else {
                continue;
            }

            // If this request is done, prepare to respond.
            if (req->numSatisfiedObjects_ == req->numWaitingObjects_) {
                VLOG(1) << "All object data ready for clientId: " + req->clientId_;
                completedRequests.emplace_back(req);
            } else {
                // For SubRecvEvent request, return immediately as long as there is an object is ready.
                if (isUpdateSubRecvEventRequest && req->numSatisfiedObjects_ >= 1) {
                    VLOG(1) << FormatString("Subscription succeeded for clientId: %d, deviceId: %d", req->clientId_,
                                            req->deviceId_);
                    completedRequests.emplace_back(req);
                }
            }
        }
        for (auto &req : completedRequests) {
            doneRequestCallBack(req);
        }
        return Status::OK();
    }

    /**
     * @brief Get the Requests By Object object
     * @param[in] objKey object key
     * @return std::vector<std::shared_ptr<Request>>
     */
    std::vector<std::shared_ptr<Request>> GetRequestsByObject(const std::string &objKey)
    {
        std::shared_lock<std::shared_timed_mutex> lck(mutex_);
        auto it = requestTable_.find(objKey);
        if (it != requestTable_.end()) {
            return it->second;
        }
        return {};
    }

private:
    std::shared_timed_mutex mutex_;

    // A hash table that maps object key to a vector of requests, which are waiting for objects to be ready.
    std::unordered_map<ImmutableString, std::vector<std::shared_ptr<Request>>> requestTable_;
};

template <typename Req, typename Resp, typename EntryParam>
class UnaryRequest {
public:
    using TbbGetObjsTable = tbb::concurrent_hash_map<ImmutableString, std::shared_ptr<EntryParam>>;
    using Param = EntryParam;

    UnaryRequest(std::vector<std::string> objectKeys,
                 std::shared_ptr<::datasystem::ServerUnaryWriterReader<Resp, Req>> serverApi, std::string clientId,
                 std::shared_ptr<AccessRecorder> accessRecorderPoint)
        : UnaryRequest(std::move(objectKeys), serverApi, clientId, -1, {}, accessRecorderPoint)
    {
    }

    // for device
    UnaryRequest(std::vector<std::string> objectKeys,
                 std::shared_ptr<::datasystem::ServerUnaryWriterReader<Resp, Req>> serverApi, std::string clientId,
                 int32_t deviceId, const Req &requestInfo)
        : UnaryRequest(std::move(objectKeys), serverApi, clientId, deviceId, requestInfo,
                       (std::shared_ptr<AccessRecorder>)nullptr)
    {
    }

    UnaryRequest(std::vector<std::string> objectKeys,
                 std::shared_ptr<::datasystem::ServerUnaryWriterReader<Resp, Req>> serverApi, std::string clientId,
                 int32_t deviceId, const Req &requestInfo, std::string workerIP)
        : UnaryRequest(std::move(objectKeys), serverApi, clientId, deviceId, requestInfo)
    {
        workerIP_ = workerIP;
    }

    UnaryRequest(std::vector<std::string> objectKeys,
                 std::shared_ptr<::datasystem::ServerUnaryWriterReader<Resp, Req>> serverApi, const Req &requestInfo)
        : UnaryRequest(std::move(objectKeys), serverApi, "", -1, requestInfo, (std::shared_ptr<AccessRecorder>)nullptr)
    {
    }

    UnaryRequest(std::vector<std::string> objectKeys,
                 std::shared_ptr<::datasystem::ServerUnaryWriterReader<Resp, Req>> serverApi, std::string clientId,
                 int32_t deviceId, const Req &requestInfo, std::shared_ptr<AccessRecorder> accessRecorderPoint)
        : requestInfo_(requestInfo),
          clientId_(std::move(clientId)),
          deviceId_(deviceId),
          rawObjectKeys_(objectKeys),
          deduplicatedObjectKeys_(objectKeys.begin(), objectKeys.end()),
          serverApi_(std::move(serverApi)),
          numWaitingObjects_(deduplicatedObjectKeys_.size()),
          numSatisfiedObjects_(0),
          timer_(nullptr),
          isReturn_(false),
          accessRecorderPoint_(std::move(accessRecorderPoint))
    {
    }

    bool operator==(const UnaryRequest &other) const
    {
        return (clientId_ == other.clientId_ && rawObjectKeys_ == other.rawObjectKeys_
                && numWaitingObjects_ == other.numWaitingObjects_ && numSatisfiedObjects_ == other.numWaitingObjects_);
    }

    void SetStatus(const Status &status)
    {
        if (status.IsError()) {
            lastRc_ = status;
        }
    }

    void SetOffset(const std::unordered_map<std::string, OffsetInfo> offsetInfos)
    {
        std::lock_guard<std::mutex> locker(mutex_);
        offsetInfos_ = offsetInfos;
    }

    bool IsOffsetAndSizeMatchWithoutLock(const std::string &objKey, const uint64_t dataSize,
                                         const OffsetInfo &paramOffsetInfo)
    {
        auto iter = offsetInfos_.find(objKey);
        if (iter == offsetInfos_.end()) {
            // is incomplete obj entry, can not return to client.
            return !paramOffsetInfo.IsOffsetRead(dataSize);
        } else {
            auto tmpInfo = iter->second;
            tmpInfo.AdjustReadSize(dataSize);
            return tmpInfo == paramOffsetInfo;
        }
    }

    // The rpc request info
    Req requestInfo_;

    // The client id.
    std::string clientId_;

    // The device id.
    int32_t deviceId_;

    // The worker ip.
    std::string workerIP_;

    // The object keys to wait that include in a request.
    std::vector<std::string> rawObjectKeys_;

    // The deduplicated object keys that include in this request.
    std::unordered_set<std::string> deduplicatedObjectKeys_;

    // The object information for the objects in this request used to reply.
    TbbGetObjsTable objects_;

    // The stream_ to send result to client.
    std::shared_ptr<::datasystem::ServerUnaryWriterReader<Resp, Req>> serverApi_;

    // The number of objects waiting for in this request.
    size_t numWaitingObjects_;

    // The number of object requests in this wait request that are already satisfied.
    std::atomic<size_t> numSatisfiedObjects_;

    // The timer_ indicates timeout event, it needs to be canceled if the timeout event is returned in advance.
    std::unique_ptr<TimerQueue::TimerImpl> timer_;

    // The isReturn_ indicates whether the request has been returned.
    std::atomic<bool> isReturn_;

    std::shared_ptr<AccessRecorder> accessRecorderPoint_;

    // save the last error.
    Status lastRc_;

    std::mutex mutex_;

    std::atomic_bool isFinished_{ false };

    std::unordered_map<std::string, OffsetInfo> offsetInfos_;
};
}  // namespace datasystem
#endif  // DATASYSTEM_UTILS_REQUEST_TABLE_H
