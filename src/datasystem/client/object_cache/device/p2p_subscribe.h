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
 * Description: Defines the ascend device manager.
 */
#ifndef DATASYSTEM_CLIENT_OBJECT_CACHE_DEVICE_P2P_SUBSCRIBE_H
#define DATASYSTEM_CLIENT_OBJECT_CACHE_DEVICE_P2P_SUBSCRIBE_H

#include <future>
#include <memory>
#include <mutex>
#include <numeric>
#include <sstream>
#include <string>
#include <vector>
#include <chrono>

#include <tbb/concurrent_hash_map.h>

#include "datasystem/common/log/log.h"
#include "datasystem/client/hetero_cache/device_buffer.h"
#include "datasystem/client/hetero_cache/device_util.h"
#include "datasystem/client/object_cache/client_worker_api.h"
#include "datasystem/client/object_cache/device/device_memory_unit.h"
#include "datasystem/client/object_cache/device/hccl_comm_factory.h"
#include "datasystem/common/device/ascend/acl_pointer_wrapper.h"
#include "datasystem/common/device/ascend/acl_resource_manager.h"
#include "datasystem/common/device/ascend/cann_types.h"
#include "datasystem/common/device/ascend/hccl_comm_wrapper.h"
#include "datasystem/common/util/queue/blocking_queue.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/thread_pool.h"
#include "datasystem/hetero/future.h"
#include "datasystem/object/object_enum.h"
#include "datasystem/utils/status.h"

namespace datasystem {

constexpr uint32_t P2P_THREADPOOL_SIZE = 4;

class StatusPromise {
public:
    Status SetValue(const Status &rc);

    Status GetSharedFuture(std::shared_future<Status> &future);

    ~StatusPromise();

private:
    std::atomic<bool> getFuture_ = false;
    std::atomic<bool> setValue_ = false;
    std::promise<Status> promise_;
    std::shared_future<Status> sharedFuture_;
};

class PromiseWithEvent {
public:
    PromiseWithEvent(const std::string &objectKey);

    virtual ~PromiseWithEvent() = default;

    /**
     * @brief Create acl event and generate future list.
     * @param[in] eventCount The number of event.
     * @param[out] futureVec The list of future.
     * @return The status of call.
     */
    Status CreateEventAndFutureList(size_t eventCount, std::vector<Future> &futureVec);

    /*
     * @brief Create acl event and generate future list.
     */
    void CreateEvent();

    /**
     * @brief Get the aclRtEvent.
     * @return The shared_ptr of AclRtEventWrapper
     */
    const std::shared_ptr<AclRtEventWrapper> &GetEvent();

    /**
     * @brief Destroy an event.
     */
    void DestroyEvent();

    /**
     * @brief Set status to the promise
     * @param[in] rc The value of promise.
     * @return The status of call.
     */
    Status SetPromiseValue(const Status &rc);

private:
    /**
     * @brief If the Event is nullptr, create an Event.
     */
    void CreateEventIfNotExistUnlock();

    std::mutex mutex_;
    std::shared_ptr<StatusPromise> promise_;
    // Store event to record when send/recv finish, and pass event to the future that promise create.
    std::shared_ptr<AclRtEventWrapper> event_ = nullptr;
    std::string objectKey_;
};

class P2PPutRequest : public PromiseWithEvent {
public:
    P2PPutRequest(std::shared_ptr<DeviceBufferInfo> deviceBufferInfo, std::vector<Blob> blobStorage)
        : PromiseWithEvent(deviceBufferInfo->devObjKey),
          bufferInfo_(std::move(deviceBufferInfo)),
          blobStorage_(std::move(blobStorage))
    {
    }
    std::shared_ptr<DeviceBufferInfo> GetBufferInfo()
    {
        return bufferInfo_;
    }

    const std::vector<Blob> &GetBlobsStorage() const
    {
        return blobStorage_;
    }

    size_t GetTotalSize() const
    {
        return std::accumulate(blobStorage_.begin(), blobStorage_.end(), 0ul,
                               [](size_t total, const Blob &info) { return total + info.size; });
    }

    const std::string &GetObjectKey() const
    {
        return bufferInfo_->devObjKey;
    }

private:
    std::shared_ptr<DeviceBufferInfo> bufferInfo_;
    std::vector<Blob> blobStorage_;
};

class P2PGetRequest : public P2PPutRequest {
public:
    P2PGetRequest(std::shared_ptr<DeviceBufferInfo> deviceBufferInfo, std::vector<Blob> blobStorage,
                  std::shared_ptr<DeviceMemoryUnit> memUnit)
        : P2PPutRequest(std::move(deviceBufferInfo), std::move(blobStorage)), devMemUnit_(memUnit)
    {
    }

    ~P2PGetRequest() = default;

    void SetSrcClientId(const std::string &srcClientId)
    {
        srcClientId_ = srcClientId;
    }

    void SetSrcDeviceId(int32_t srcDeviceId)
    {
        srcDeviceId_ = srcDeviceId;
    }

    const std::string &GetSrcClientId() const
    {
        return srcClientId_;
    }

    int32_t GetSrcDeviceId() const
    {
        return srcDeviceId_;
    }

    std::shared_ptr<DeviceMemoryUnit> GetMemUnit()
    {
        return devMemUnit_;
    }

private:
    std::string srcClientId_;
    int32_t srcDeviceId_;
    std::shared_ptr<DeviceMemoryUnit> devMemUnit_;
};

class P2PGetRequestsWrapper {
public:
    P2PGetRequestsWrapper() = delete;

    P2PGetRequestsWrapper(std::vector<std::shared_ptr<P2PGetRequest>> requestList, int64_t prefetchTimeoutMs,
                          int64_t subTimeoutMs)
        : requestList_(std::move(requestList)), prefetchTimeout_(prefetchTimeoutMs), subTimeout_(subTimeoutMs)
    {
        initializationTime_ = std::chrono::system_clock::now();
    }

    P2PGetRequestsWrapper(int64_t prefetchTimeoutMs, int64_t subTimeoutMs)
        : prefetchTimeout_(prefetchTimeoutMs), subTimeout_(subTimeoutMs)
    {
        initializationTime_ = std::chrono::system_clock::now();
    }

    size_t Size()
    {
        return requestList_.size();
    }

    bool IsTimeout()
    {
        auto now = std::chrono::system_clock::now();
        int64_t elapsedTime = std::chrono::duration_cast<std::chrono::milliseconds>(now - initializationTime_).count();
        return elapsedTime >= prefetchTimeout_;
    }

    std::chrono::system_clock::time_point initializationTime_;
    std::vector<std::shared_ptr<P2PGetRequest>> requestList_;
    int64_t prefetchTimeout_;
    int64_t subTimeout_;
    std::string getTraceId_;
};

enum class P2PEventReqType { CREATE, UNCREATE };
enum class P2PAckReqType { GET, PUT };

struct P2PAckReq {
public:
    P2PAckReq(std::shared_ptr<P2PGetRequest> req) : type(P2PAckReqType::GET), p2pGetRequest(req)
    {
    }

    P2PAckReq(std::shared_ptr<P2PPutRequest> req) : type(P2PAckReqType::PUT), p2pPutRequest(req)
    {
    }
    ~P2PAckReq() = default;

    P2PAckReqType type;
    std::shared_ptr<P2PGetRequest> p2pGetRequest = nullptr;
    std::shared_ptr<P2PPutRequest> p2pPutRequest = nullptr;
};

// <objectKey, P2PPutRequest>
using TbbP2PPutRequestTable = tbb::concurrent_hash_map<std::string, std::shared_ptr<P2PPutRequest>>;
using TbbDevMemUnitTable = tbb::concurrent_hash_map<std::string, std::shared_ptr<DeviceMemoryUnit>>;

struct P2PGroupKey {
    int32_t remoteDeviceId;
    std::string remoteClientId;
    bool sameNode;
    bool operator==(const P2PGroupKey &other) const
    {
        return sameNode == other.sameNode && remoteDeviceId == other.remoteDeviceId
               && remoteClientId == other.remoteClientId;
    }
};

class P2PSubscribe : public ClientDeviceCurd {
public:
    P2PSubscribe(int32_t deviceId, std::shared_ptr<object_cache::IClientWorkerApi> workerApi,
                 std::shared_ptr<HcclCommFactory> commFactory, bool enableP2Ptransfer, int32_t timeoutMs = 5000);

    ~P2PSubscribe();

    /**
     * @brief Init the P2PSubscribe object.
     */
    void Init();

    /**
     * @brief Add the device object key to the subscribe queue.
     * @param[in] bufferInfo The info of device buffer.
     * @param[in] blobs The list of blob info.
     * @return The future vector of HcclSend result. You can use the Get() method of the future object corresponding to
     * sendDataList to wait for and access the result of HcclSend.
     */
    std::shared_ptr<P2PPutRequest> AddSubscribe(const std::shared_ptr<DeviceBufferInfo> &bufferInfo,
                                                const std::vector<Blob> &blobs);

    /**
     * @brief Remove the device object key from the subscribe queue in worker.
     * @param[in] devObjectKey The device object key.
     */
    void RemoveSubscribe(const std::string &devObjectKey);

    /**
     * @brief The loop of p2p send.
     */
    void RunP2PSendLoop();

    /**
     * @brief Process p2p send event.
     * @param[in] groupedGetNotification The SubscribeReceiveEventRspPb responses.
     */
    void ProcessP2PSend(
        const std::unordered_map<P2PGroupKey, std::vector<SubscribeReceiveNpuEventPb>> &groupedGetNotification);

    template <typename Request>
    void NofityRequestsWithStatus(const std::vector<std::shared_ptr<Request>> &requests, const Status &rc)
    {
        for (const auto &req : requests) {
            req->SetPromiseValue(rc);
        }
    }

    /**
     * @brief The loop of p2p send.
     */
    void RunP2PRecvLoop();

    /**
     * @brief The loop of ack send.
     */
    void RunP2PAckLoop();

    /**
     * @brief Split the code of RunP2PAckLoop () and process the content of p2pPutRequest.
     * @param[in] p2pAckReq by p2pGetQueue_ pop
     * @param[in] p2pPutRequest p2pAckReq->p2pPutRequest_, However, the loop condition of RunP2PAckLoop () is involved.
     * Therefore, another reference is passed in.
     */
    void P2PAckPut(std::shared_ptr<P2PAckReq> &p2pAckReq);

    /**
     * @brief Split the code of RunP2PAckLoop () and process the content of p2pGetRequest.
     * @param[in] p2pGetRequest p2pAckReq->p2pGetRequest_
     */
    void P2PAckGet(std::shared_ptr<P2PGetRequest> &p2pGetRequest);
    /**
     * @brief Get the Data Info object.
     * @param[in] objectKey The device object key.
     * @param[in] putRequest The request of put.
     * @return true if get data info success.
     */
    bool GetPutRequest(const std::string &objectKey, std::shared_ptr<P2PPutRequest> &putRequest);

    /**
     * @brief Process the p2p get event.
     * @param[in] p2pGetRequests The vector of p2p get request
     * @return Status of the call
     */
    Status ProcessP2PGet(const std::vector<std::shared_ptr<P2PGetRequest>> &p2pGetRequests);

    /**
     * @brief Process p2p response from the master
     * @param[in] resp proto response from master
     * @param[in] objKeyToP2PRequest mapper from obj key to request
     * @return Status of the call
     */
    Status ProcessP2PResponse(const std::shared_ptr<P2PGetRequestsWrapper> p2pGetRequests, const GetP2PMetaRspPb &resp,
                              std::unordered_map<std::string, std::shared_ptr<P2PGetRequest>> &objKeyToP2PRequest);
    /**
     * @brief Process the p2p get event using wrapper
     * @param[in] p2pGetRequests The wrapper requests of p2p get.
     * @return Status of the call
     */
    Status ProcessP2PGet(const std::shared_ptr<P2PGetRequestsWrapper> p2pGetRequests);

    /**
     * @brief Invoke client to get the given device object from worker.
     * @param[in] buffer The buffer ready to get.
     * @param[out] futureVec The vector of future.
     * @param[out] subTimeoutMs The maximum time of subscriptions.
     * @return Status of call.
     */
    Status AsyncGet(const std::vector<std::shared_ptr<DeviceBuffer>> &buffer, std::vector<Future> &futureVec,
                    int64_t prefetchTimeout, int64_t subTimeoutMs);

    /**
     * @brief Publish device object to datasystem.
     * @param[in] buffer The device buffer ready to publish.
     * @return Status of the result.
     */
    Status PublishDeviceObject(const std::shared_ptr<DeviceBuffer> &buffer);

    /**
     * @brief Gets the list of future in device memory sending, it only work in MOVE lifetime.
     * @param[out] futureVec The deviceid to which data belongs.
     * @return Status of the result.
     */
    Status GetSendStatus(const std::string &objectKey, std::vector<Future> &futureVec);

    /**
     * @brief Check the number of comms in the comm_ table. If the number is 0, stop the Close fault detection loop
     * Moniter(). If the number of communication domains is greater than one, a fault detection cycle is initiated
     */
    void CommRefCheckMoreThanOne();

    /**
     * @brief Start fault detection cycle
     */
    void StartMonitorThread();

    /**
     * @brief Set the interrupt flag of the thread to true.
     */
    void SetThreadInterruptFlag2True();

    /**
     * @brief Process sub p2p response.
     * @param[in] subResp proto response from master
     * @param[in] p2PGetRequest The p2p get request.
     * @return Status of the call
     */
    Status ProcessSingleResponse(const DeviceObjectMetaRspPb &subResp,
                                 const std::shared_ptr<P2PGetRequest> &p2pGetRequest);

private:
    /**
     * @brief Cycles for fault detection
     */
    void MonitorLoop();
    /**
     * @brief Suspend fault detection cycle
     */
    void StopMonitorThread();
    /**
     * @brief For destruction, release the fault detection loop.
     */
    void ReleaseMonitorThread();

    /**
     * @brief Remove communication when opposite end is destroyed.
     * @param[in] npuEvent The event pb which describe device and client id.
     */
    void ProcessHcclCommDestroy(const SubscribeReceiveNpuEventPb &npuEvent);

    void ProcessP2PRecv(const std::unordered_map<P2PGroupKey, std::vector<DeviceObjectMetaRspPb>> &groupedSubResp,
                        const std::unordered_map<std::string, std::shared_ptr<P2PGetRequest>> &objKeyToP2PRequest,
                        std::set<std::string> &finishedList);

    std::atomic<bool> interruptFlag_{ false };
    int32_t deviceId_;
    std::unique_ptr<ThreadPool> threadPool_;
    std::shared_ptr<HcclCommFactory> commFactory_;

    TbbP2PPutRequestTable objKey2PutReqTable_;
    TbbDevMemUnitTable devMemUnitTable_;

    BlockingQueue<std::shared_ptr<P2PGetRequestsWrapper>> p2pGetQueue_;
    BlockingQueue<std::shared_ptr<P2PAckReq>> p2pAckQueue_;

    // Detection of Circulating Markers
    std::atomic<bool> monitorRun_;
    // User expected timeout notification time transferred by the client during P2P initialization.
    int32_t connectTimeOutMS_;
    bool clientEnableP2Ptransfer_ = false;
};
}  // namespace datasystem

namespace std {

template <>
struct hash<datasystem::P2PGroupKey> {
    size_t operator()(const datasystem::P2PGroupKey &other) const;
};

template <>
struct equal_to<datasystem::P2PGroupKey> {
    bool operator()(const datasystem::P2PGroupKey &lhs, const datasystem::P2PGroupKey &rhs) const;
};

}  // namespace std

#endif
