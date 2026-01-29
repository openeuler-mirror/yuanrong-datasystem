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

#include "datasystem/client/object_cache/device/p2p_subscribe.h"

#include <algorithm>
#include <chrono>
#include <future>
#include <memory>
#include <mutex>
#include <string>

#include "datasystem/client/hetero_cache/device_buffer.h"
#include "datasystem/common/device/ascend/acl_pipeline_p2p_task.h"
#include "datasystem/common/device/ascend/acl_pointer_wrapper.h"
#include "datasystem/common/device/ascend/acl_resource_manager.h"
#include "datasystem/common/device/ascend/hccl_comm_wrapper.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/perf/perf_manager.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/hetero/future.h"
#include "datasystem/object/object_enum.h"
#include "datasystem/protos/p2p_subscribe.pb.h"
#include "datasystem/utils/status.h"

namespace datasystem {
const uint32_t ONE_SECOND_MS = 1000;

P2PSubscribe::P2PSubscribe(int32_t deviceId, std::shared_ptr<object_cache::IClientWorkerApi> workerApi,
                           std::shared_ptr<HcclCommFactory> commFactory, bool enableP2Ptransfer, int32_t timeoutMs)
    : ClientDeviceCurd(std::move(workerApi)),
      interruptFlag_(false),
      deviceId_(deviceId),
      commFactory_(std::move(commFactory)),
      connectTimeOutMS_(timeoutMs),
      clientEnableP2Ptransfer_(enableP2Ptransfer)
{
}

P2PSubscribe::~P2PSubscribe()
{
    interruptFlag_ = true;
    ReleaseMonitorThread();
    p2pGetQueue_.Abort();
    p2pAckQueue_.Abort();
    commFactory_->ShutDown();
    threadPool_.reset();
    objKey2PutReqTable_.clear();
}

void P2PSubscribe::Init()
{
    threadPool_ = std::make_unique<ThreadPool>(P2P_THREADPOOL_SIZE);
    threadPool_->Execute([this]() { RunP2PSendLoop(); });
    threadPool_->Execute([this]() { RunP2PRecvLoop(); });
    threadPool_->Execute([this]() { RunP2PAckLoop(); });
    // The monitor thread is disabled by default and is enabled when the communication domain is established.
    monitorRun_ = false;
}

void P2PSubscribe::RunP2PSendLoop()
{
    std::unordered_set<StatusCode> ignoreLogCodes = { K_OK, K_RPC_DEADLINE_EXCEEDED, K_NOT_FOUND };
    Timer lastGetTimer;
    bool first = true;
    while (!interruptFlag_) {
        TraceGuard traceGuard = Trace::Instance().SetTraceNewID(GetStringUuid());
        SubscribeReceiveEventRspPb resp;
        PerfPoint point(PerfKey::CLIENT_P2P_PUB_NEXT_EVENT);
        Status rc = clientWorkerApi_->SubscribeReceiveEvent(deviceId_, resp);
        if (rc.IsError() || resp.npuevents().empty()) {
            if (ignoreLogCodes.find(rc.GetCode()) == ignoreLogCodes.end()) {
                LOG(WARNING) << " SubscribeReceiveEvent failed, " << rc.ToString();
            }
            continue;
        }
        point.RecordAndReset(PerfKey::CLIENT_P2P_PUB_HANDLE_EVENT);
        std::unordered_map<P2PGroupKey, std::vector<SubscribeReceiveNpuEventPb>> groupedGetNotification;
        for (auto &npuEvent : resp.npuevents()) {
            auto eventType = npuEvent.event_type();
            if (eventType == SubscribeEventTypePb::LIFECYCLE_EXIT_NOTIFICATION) {
                LOG(INFO) << "Device object life cycle exit: " << npuEvent.object_key();
                (void)devMemUnitTable_.erase(npuEvent.object_key());
                (void)objKey2PutReqTable_.erase(npuEvent.object_key());
            } else if (eventType == SubscribeEventTypePb::GET_NOTIFICATION) {
                P2PGroupKey groupKey{ .remoteDeviceId = npuEvent.dst_device_id(),
                                      .remoteClientId = npuEvent.dst_client_id(),
                                      .sameNode = npuEvent.is_same_node() };
                groupedGetNotification[groupKey].emplace_back(std::move(npuEvent));
            } else if (eventType == SubscribeEventTypePb::COMM_DESTROY_NOTIFICATION) {
                ProcessHcclCommDestroy(npuEvent);
            } else if (eventType == SubscribeEventTypePb::SUBSCRIBE_CANCEL_NOTIFICATION) {
                LOG(INFO) << "Event subscription is canceled.";
                interruptFlag_ = true;
                break;
            } else {
                LOG(ERROR) << "Invalid event type " << eventType;
            }
        }
        if (!groupedGetNotification.empty()) {
            if (!first) {
                auto elapsedMs = static_cast<uint64_t>(lastGetTimer.ElapsedMicroSecond() * ONE_SECOND_MS);
                PerfPoint::RecordElapsed(PerfKey::CLIENT_P2P_PUB_NEXT_GET_WAIT, elapsedMs);
            }
            point.RecordAndReset(PerfKey::CLIENT_P2P_PUB_HANDLE_GET_NOTIFICATION);
            ProcessP2PSend(groupedGetNotification);
            lastGetTimer.Reset();
            first = false;
        }
    }
}

void P2PSubscribe::ProcessP2PSend(
    const std::unordered_map<P2PGroupKey, std::vector<SubscribeReceiveNpuEventPb>> &groupedGetNotification)
{
    for (auto &kv : groupedGetNotification) {
        PerfPoint point(PerfKey::CLIENT_P2P_PUB_GET_COMM_AND_SUBMIT);
        const auto &recvClientId = kv.first.remoteClientId;
        auto recvDeviceId = kv.first.remoteDeviceId;
        bool isSameNode = kv.first.sameNode;
        auto &npuEvents = kv.second;
        std::vector<std::string> objectKeys;
        std::transform(npuEvents.begin(), npuEvents.end(), std::back_inserter(objectKeys),
                       [](const SubscribeReceiveNpuEventPb &npuEvent) { return npuEvent.object_key(); });
        LOG(INFO) << FormatString("Get send event from npuId: %s;%d, keys: [%s]", recvClientId, recvDeviceId,
                                  VectorToString(objectKeys));

        StartMonitorThread();
        std::shared_ptr<CommWrapperBase> comm;
        Status rc = commFactory_->GetOrCreateHcclComm(P2PEventType::SEND, deviceId_, recvClientId, recvDeviceId,
                                                      isSameNode, clientEnableP2Ptransfer_, comm);
        if (rc.IsError()) {
            LOG(ERROR) << "ObjectKeys: " << VectorToString(objectKeys) << ",  GetOrCreateHcclComm failed, "
                       << rc.ToString();
            return;
        }
        CommRefCheckMoreThanOne();

        // Register a callback function to be executed after the communication domain is ready
        Timer timer;
        auto traceId = Trace::Instance().GetTraceID();
        comm->AddReadyCallback([this, npuEvents = std::move(npuEvents), comm, traceId, timer]() {
            comm->Execute([this, npuEvents, comm, traceId, timer]() {
                auto elapsedMs = static_cast<uint64_t>(timer.ElapsedMicroSecond() * ONE_SECOND_MS);
                PerfPoint::RecordElapsed(PerfKey::CLIENT_P2P_PUB_SUBMIT_DELAY, elapsedMs);
                PerfPoint::RecordElapsed(PerfKey::CLIENT_P2P_PUB_SUBMIT_KEY_COUNT, npuEvents.size());
                PerfPoint point(PerfKey::CLIENT_P2P_PUB_PIPELINE_PREPARE);
                TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceId);
                point.RecordAndReset(PerfKey::CLIENT_P2P_PUB_PIPELINE_SUBMIT_ALL);
                for (const auto &npuEvent : npuEvents) {
                    std::shared_ptr<P2PPutRequest> putRequest;
                    const auto &objectKey = npuEvent.object_key();
                    auto found = GetPutRequest(objectKey, putRequest);
                    if (!found) {
                        LOG(ERROR) << FormatString("Can't find %s P2PPutRequest info", objectKey);
                        continue;
                    }
                    PerfPoint::RecordElapsed(PerfKey::CLIENT_P2P_PUB_SUBMIT_KEY_SIZE, putRequest->GetTotalSize());
                    putRequest->CreateEvent();
                    size_t srcOffset = npuEvent.src_offset();
                    size_t length = npuEvent.length();
                    std::vector<Blob> blobs = putRequest->GetBlobsStorage();
                    // Calculate minimum size from all blobs
                    size_t minSize = std::min_element(blobs.begin(), blobs.end(), [](const Blob &a, const Blob &b) {
                                         return a.size < b.size;
                                     })->size;
                    // Execute if receiver expects only partial data
                    if (srcOffset > 0 || length < minSize) {
                        VLOG(1) << "Adjusting data info parameters: srcOffset=" << srcOffset << ", length=" << length
                                << ", minSize=" << minSize;
                        for (auto &blob : blobs) {
                            blob.pointer = static_cast<void *>(static_cast<uint8_t *>(blob.pointer) + srcOffset);
                            blob.size = npuEvent.length();
                        }
                    }
                    LOG(INFO) << "Start submit send task for object key:" << objectKey;
                    acl::P2PSendTask sendTask{ .srcBuffers = putRequest->GetBlobsStorage(),
                                               .totalSize = putRequest->GetTotalSize(),
                                               .comm = comm,
                                               .event = putRequest->GetEvent() };
                    auto rc = comm->SubmitPipelineTask(std::move(sendTask));
                    if (rc.IsError()) {
                        LOG(ERROR) << FormatString(
                            "Submitted P2P send task execution failed for object: %s, error msg: [%s]", objectKey,
                            rc.GetMsg());
                        LOG_IF_ERROR(putRequest->SetPromiseValue(rc), "promise set value failed.");
                        return;
                    }
                    std::shared_ptr<P2PAckReq> ackReq = std::make_shared<P2PAckReq>(putRequest);
                    p2pAckQueue_.Push(ackReq);
                }
                point.RecordAndReset(PerfKey::CLIENT_P2P_PUB_PIPELINE_OTHER);
            });
        });
    }
}

void P2PSubscribe::RunP2PRecvLoop()
{
    std::queue<std::shared_ptr<P2PGetRequestsWrapper>> remainTaskQueue;  // to be optimized for scheduling
    Timer lastGetTimer;
    bool first = true;
    while (!interruptFlag_) {
        std::shared_ptr<P2PGetRequestsWrapper> p2pGetRequests;
        if (p2pGetQueue_.Pop(p2pGetRequests).IsError() || p2pGetRequests == nullptr) {
            continue;
        }
        if (p2pGetRequests->Size() == 0) {
            continue;
        }
        TraceGuard traceGuard = Trace::Instance().SetTraceNewID(p2pGetRequests->getTraceId_);
        if (!first) {
            auto elapsedMs = static_cast<uint64_t>(lastGetTimer.ElapsedMicroSecond() * ONE_SECOND_MS);
            PerfPoint::RecordElapsed(PerfKey::CLIENT_P2P_SUB_NEXT_GET_DELAY, elapsedMs);
        }
        PerfPoint point(PerfKey::CLIENT_PROCESS_P2PGET);
        Status rc = ProcessP2PGet(p2pGetRequests);
        LOG_IF_ERROR(rc, "ProcessP2PGet failed.");
        lastGetTimer.Reset();
        first = false;
    }
}

void P2PSubscribe::P2PAckPut(std::shared_ptr<P2PAckReq> &p2pAckReq)
{
    std::shared_ptr<P2PPutRequest> p2pPutRequest = p2pAckReq->p2pPutRequest;
    if (p2pPutRequest->GetEvent() == nullptr) {
        return;
    }
    auto rc = p2pPutRequest->GetEvent()->QueryEventStatus();
    if (rc.IsOk()) {
        const auto &objId = p2pPutRequest->GetBufferInfo()->devObjKey;
        if (p2pPutRequest->GetBufferInfo()->lifetimeType == LifetimeType::MOVE) {
            LOG_IF_ERROR(p2pPutRequest->SetPromiseValue(rc),
                         FormatString("ObjectKey: %s promise set value failed.", objId));
        }
        p2pPutRequest->DestroyEvent();
        VLOG(1) << FormatString("Object key %s completed the sending procedure", objId);
    } else if (rc.GetCode() == K_ACL_ERROR) {
        p2pAckQueue_.Push(p2pAckReq);
    } else {
        const auto &bufferInfo = p2pPutRequest->GetBufferInfo();
        LOG(ERROR) << FormatString("HCCL Event Error of ObjectKey %s, rc:%s", bufferInfo->devObjKey, rc.ToString());
    }
}

void P2PSubscribe::P2PAckGet(std::shared_ptr<P2PGetRequest> &p2pGetRequest)
{
    const auto &bufferInfo = p2pGetRequest->GetBufferInfo();
    const auto &objectKey = bufferInfo->devObjKey;
    auto &srcClientId = p2pGetRequest->GetSrcClientId();
    auto srcDeviceId = p2pGetRequest->GetSrcDeviceId();
    if (bufferInfo->cacheLocation) {
        AddSubscribe(bufferInfo, p2pGetRequest->GetBlobsStorage());
        (void)devMemUnitTable_.insert(std::make_pair(objectKey, p2pGetRequest->GetMemUnit()));
    }
    VLOG(1) << FormatString("Object key %s completed the receiving procedure", objectKey);
    LOG_IF_ERROR(p2pGetRequest->SetPromiseValue(Status::OK()), "promise set value failed.");
    AckRecvFinishReqPb req;
    req.set_object_key(objectKey);
    req.set_src_client_id(srcClientId);
    req.set_src_device_id(srcDeviceId);
    req.set_cache_location(bufferInfo->cacheLocation);
    req.set_dst_client_id(clientWorkerApi_->clientId_);
    req.set_dst_device_id(deviceId_);
    auto rc = clientWorkerApi_->AckRecvFinish(req);
    LOG_IF_ERROR(rc, FormatString("ObjectKey : %s AckRecvFinish failed.", objectKey));
}

void P2PSubscribe::RunP2PAckLoop()
{
    LOG(INFO) << "RunP2PAckLoop starts";
    while (!interruptFlag_) {
        std::shared_ptr<P2PAckReq> p2pAckReq;
        if (p2pAckQueue_.Pop(p2pAckReq).IsError() || p2pAckReq == nullptr) {
            continue;
        }
        TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
        if (p2pAckReq->type == P2PAckReqType::GET) {
            auto &p2pGetRequest = p2pAckReq->p2pGetRequest;
            if (p2pGetRequest->GetEvent() == nullptr) {
                LOG(ERROR) << "p2pGetRequest have not event_";
                continue;
            }
            auto rc = p2pGetRequest->GetEvent()->QueryEventStatus();
            if (rc.IsOk()) {
                P2PAckGet(p2pGetRequest);
            } else if (rc.GetCode() == K_ACL_ERROR) {
                p2pAckQueue_.Push(p2pAckReq);
            } else {
                const auto &bufferInfo = p2pGetRequest->GetBufferInfo();
                LOG(ERROR) << FormatString("HCCL Event Error of ObjectKey %s, rc:%s", bufferInfo->devObjKey,
                                           rc.ToString());
            }
        } else if (p2pAckReq->type == P2PAckReqType::PUT) {
            P2PAckPut(p2pAckReq);
        } else {
            LOG(ERROR) << "RunP2PAckLoop() other type error";
        }
    }
    LOG(INFO) << "RunP2PAckLoop() finish";
}

Status P2PSubscribe::ProcessP2PGet(const std::shared_ptr<P2PGetRequestsWrapper> p2pGetRequests)
{
    PerfPoint point(PerfKey::CLIENT_P2P_SUB_PREPARE);
    if (p2pGetRequests->IsTimeout()) {
        for (auto &request : p2pGetRequests->requestList_) {
            request->SetPromiseValue(Status(K_NOT_FOUND, "p2p meta data get timeout"));
        }
        LOG(ERROR) << "ProcessP2PGet timeout, abort remaining requests";
        return Status(K_NOT_FOUND, "p2p meta data get timeout");
    }
    std::vector<std::shared_ptr<DeviceBufferInfo>> bufferInfoList;
    std::vector<DeviceBlobList> blobStorageList;
    std::unordered_map<std::string, std::shared_ptr<P2PGetRequest>> objKeyToP2PRequest;
    std::stringstream allKeys;
    bool first = true;
    for (size_t i = 0; i < p2pGetRequests->Size(); i++) {
        auto &p2pGetRequest = p2pGetRequests->requestList_[i];
        const auto &bufferInfo = p2pGetRequest->GetBufferInfo();
        const auto &objectKey = bufferInfo->devObjKey;
        const auto &blobStorage = p2pGetRequest->GetBlobsStorage();
        bufferInfoList.emplace_back(bufferInfo);
        blobStorageList.emplace_back(DeviceBlobList{ .blobs = blobStorage, .deviceIdx = -1 });
        (void)objKeyToP2PRequest.emplace(objectKey, p2pGetRequest);
        if (!first) {
            allKeys << ", ";
        }
        allKeys << objectKey;
        first = false;
    }
    LOG(INFO) << FormatString("Start to P2PGet keys: [%s]", allKeys.str());
    GetP2PMetaRspPb resp;
    auto now = std::chrono::system_clock::now();
    int64_t elapsedTime =
        std::chrono::duration_cast<std::chrono::milliseconds>(now - p2pGetRequests->initializationTime_).count();
    auto subTimeout = elapsedTime > p2pGetRequests->subTimeout_ ? 0 : p2pGetRequests->subTimeout_ - elapsedTime;
    point.RecordAndReset(PerfKey::CLIENT_P2P_SUB_GETMETA);
    auto ret = clientWorkerApi_->GetP2PMeta(bufferInfoList, blobStorageList, resp, subTimeout);
    if (ret.IsError()) {
        LOG(ERROR) << "GetP2PMeta error,msg:" << ret.GetMsg();
        if (ret.GetCode() == K_RPC_DEADLINE_EXCEEDED) {
            ret = Status(K_NOT_FOUND, "can't find objects");
        }
        for (auto &it : p2pGetRequests->requestList_) {
            it->SetPromiseValue(ret);
        }
        return ret;
    }
    point.RecordAndReset(PerfKey::CLIENT_P2P_SUB_RESP);
    return ProcessP2PResponse(p2pGetRequests, resp, objKeyToP2PRequest);
}

void P2PSubscribe::ProcessP2PRecv(
    const std::unordered_map<P2PGroupKey, std::vector<DeviceObjectMetaRspPb>> &groupedSubResp,
    const std::unordered_map<std::string, std::shared_ptr<P2PGetRequest>> &objKeyToP2PRequest,
    std::set<std::string> &finishedList)
{
    for (auto &kv : groupedSubResp) {
        PerfPoint point(PerfKey::CLIENT_P2P_SUB_GET_COMM_AND_SUBMIT);
        auto &srcClientId = kv.first.remoteClientId;
        auto srcDeviceId = kv.first.remoteDeviceId;
        auto isSameNode = kv.first.sameNode;
        auto &respList = kv.second;
        std::shared_ptr<CommWrapperBase> comm;
        StartMonitorThread();
        auto rc = commFactory_->GetOrCreateHcclComm(P2PEventType::RECV, deviceId_, srcClientId, srcDeviceId, isSameNode,
                                                    clientEnableP2Ptransfer_, comm);
        std::set<std::string> objectKeys;
        std::transform(respList.begin(), respList.end(), std::inserter(objectKeys, objectKeys.end()),
                       [](const DeviceObjectMetaRspPb &resp) { return resp.object_key(); });
        if (rc.IsError()) {
            LOG(ERROR) << "GetOrCreateHcclComm failed:" << rc.ToString() << VectorToString(objectKeys);
            continue;
        }
        finishedList.insert(objectKeys.cbegin(), objectKeys.cend());
        CommRefCheckMoreThanOne();

        // Register a callback function to be executed after the communication domain is ready
        Timer timer;
        auto traceId = Trace::Instance().GetTraceID();
        comm->AddReadyCallback([this, respList = std::move(respList), comm, objKeyToP2PRequest, srcClientId,
                                srcDeviceId, traceId, timer]() {
            comm->Execute([this, respList, comm, objKeyToP2PRequest, srcClientId, srcDeviceId, traceId,
                           timer]() mutable {
                auto elapsedMs = static_cast<uint64_t>(timer.ElapsedMicroSecond() * ONE_SECOND_MS);
                PerfPoint::RecordElapsed(PerfKey::CLIENT_P2P_SUB_SUBMIT_DELAY, elapsedMs);
                PerfPoint::RecordElapsed(PerfKey::CLIENT_P2P_SUB_SUBMIT_KEY_COUNT, respList.size());
                PerfPoint point(PerfKey::CLIENT_P2P_SUB_PIPELINE_PREPARE);
                TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceId);
                std::vector<std::shared_ptr<P2PGetRequest>> requests;
                size_t maxObjectSize = 0;
                for (const auto &resp : respList) {
                    const auto &objectKey = resp.object_key();
                    auto iter = objKeyToP2PRequest.find(objectKey);
                    if (iter == objKeyToP2PRequest.end()) {
                        LOG(ERROR) << "object key:" << objectKey << " not found in objKeyToP2PRequest";
                        continue;
                    }
                    auto &getRequest = iter->second;
                    requests.emplace_back(getRequest);
                    maxObjectSize = std::max<size_t>(maxObjectSize, getRequest->GetTotalSize());
                }
                point.RecordAndReset(PerfKey::CLIENT_P2P_SUB_PIPELINE_SUBMIT_ALL);
                for (const auto &p2pGetRequest : requests) {
                    PerfPoint::RecordElapsed(PerfKey::CLIENT_P2P_SUB_SUBMIT_KEY_SIZE, p2pGetRequest->GetTotalSize());
                    const auto &objectKey = p2pGetRequest->GetObjectKey();
                    auto blobStorage = p2pGetRequest->GetBlobsStorage();
                    LOG(INFO) << FormatString("Start submit recv task for object key: %s, to comm: %s", objectKey,
                                              comm->GetCommId());
                    acl::P2PRecvTask recvTask{ .destBuffers = blobStorage,
                                               .totalSize = p2pGetRequest->GetTotalSize(),
                                               .comm = comm,
                                               .event = p2pGetRequest->GetEvent() };
                    auto rc = comm->SubmitPipelineTask(std::move(recvTask));
                    if (rc.IsError()) {
                        LOG(ERROR) << FormatString(
                            "Submitted P2P receive task execution failed for object: %s, error msg: [%s]", objectKey,
                            rc.GetMsg());
                        LOG_IF_ERROR(p2pGetRequest->SetPromiseValue(rc), "promise set value failed.");
                        continue;
                    }
                    p2pGetRequest->SetSrcClientId(srcClientId);
                    p2pGetRequest->SetSrcDeviceId(srcDeviceId);
                    std::shared_ptr<P2PAckReq> req = std::make_shared<P2PAckReq>(p2pGetRequest);
                    p2pAckQueue_.Push(req);
                }
                point.RecordAndReset(PerfKey::CLIENT_P2P_SUB_PIPELINE_OTHER);
            });
        });
    }
}

Status P2PSubscribe::ProcessP2PResponse(
    const std::shared_ptr<P2PGetRequestsWrapper> p2pGetRequests, const GetP2PMetaRspPb &resp,
    std::unordered_map<std::string, std::shared_ptr<P2PGetRequest>> &objKeyToP2PRequest)
{
    std::set<std::string> finishedList;
    std::unordered_map<P2PGroupKey, std::vector<DeviceObjectMetaRspPb>> groupedSubResp;
    std::stringstream respKeys;
    bool first = true;
    for (auto &subResp : resp.dev_obj_resp_meta()) {
        // in case of unavailable object key, all of them should be traced
        const auto &objectKey = subResp.object_key();
        if (objKeyToP2PRequest.find(objectKey) == objKeyToP2PRequest.end()) {
            LOG(ERROR) << FormatString("ObjectKey %s not found in objKeyToP2PRequest", objectKey);
            continue;
        }
        auto p2pGetRequest = objKeyToP2PRequest[objectKey];
        auto respCode = static_cast<StatusCode>(subResp.error().error_code());
        if (respCode != StatusCode::K_OK) {
            auto rc = Status(respCode, subResp.error().error_msg());
            p2pGetRequest->SetPromiseValue(rc);
            finishedList.emplace(objectKey);
            continue;
        }
        P2PGroupKey groupKey{ .remoteDeviceId = subResp.src_device_id(),
                              .remoteClientId = subResp.src_client_id(),
                              .sameNode = subResp.is_same_node() };
        groupedSubResp[groupKey].emplace_back(std::move(subResp));
        if (!first) {
            respKeys << ", ";
        }
        respKeys << objectKey;
        first = false;
    }
    VLOG(1) << FormatString("Start processing the keys that were successfully queried, keys:[%s]", respKeys.str());
    if (!groupedSubResp.empty()) {
        ProcessP2PRecv(groupedSubResp, objKeyToP2PRequest, finishedList);
    }
    std::stringstream retryKeys;
    first = true;
    auto remainTasks =
        std::make_shared<P2PGetRequestsWrapper>(p2pGetRequests->prefetchTimeout_, p2pGetRequests->subTimeout_);
    for (size_t i = 0; i < p2pGetRequests->Size(); i++) {
        const auto &objectKey = p2pGetRequests->requestList_[i]->GetBufferInfo()->devObjKey;
        if (finishedList.find(objectKey) == finishedList.end()) {
            remainTasks->requestList_.emplace_back(std::move(p2pGetRequests->requestList_[i]));
            if (!first) {
                retryKeys << ", ";
            }
            retryKeys << objectKey;
            first = false;
        }
        remainTasks->initializationTime_ = p2pGetRequests->initializationTime_;
        remainTasks->getTraceId_ = p2pGetRequests->getTraceId_;
    }
    if (remainTasks->Size() > 0) {
        VLOG(1) << FormatString("Re-adding unfinished keys to p2pGetQueue_，keys: [%s]", retryKeys.str());
        p2pGetQueue_.Push(remainTasks);
    }
    return Status::OK();
}

// no trial p2p get
Status P2PSubscribe::ProcessP2PGet(const std::vector<std::shared_ptr<P2PGetRequest>> &p2pGetRequests)
{
    std::queue<std::shared_ptr<P2PGetRequestsWrapper>> remainP2PTask;
    auto p2pWrapper = std::make_shared<P2PGetRequestsWrapper>(p2pGetRequests, 0, 0);
    return this->ProcessP2PGet(p2pWrapper);
}

bool P2PSubscribe::GetPutRequest(const std::string &objectKey, std::shared_ptr<P2PPutRequest> &putRequest)
{
    TbbP2PPutRequestTable::const_accessor acc;
    auto found = objKey2PutReqTable_.find(acc, objectKey);
    if (found) {
        putRequest = acc->second;
    }
    return found;
}

std::shared_ptr<P2PPutRequest> P2PSubscribe::AddSubscribe(const std::shared_ptr<DeviceBufferInfo> &bufferInfo,
                                                          const std::vector<Blob> &blobs)
{
    auto putRequest = std::make_shared<P2PPutRequest>(bufferInfo, blobs);
    (void)objKey2PutReqTable_.insert(std::make_pair(bufferInfo->devObjKey, putRequest));
    return putRequest;
}

Status P2PSubscribe::PublishDeviceObject(const std::shared_ptr<DeviceBuffer> &buffer)
{
    auto &bufferInfo = buffer->bufferInfo_;
    TbbP2PPutRequestTable::accessor acc;
    if (objKey2PutReqTable_.find(acc, bufferInfo->devObjKey)) {
        //  if the buffer pointers are the same, then it's a retry,skip and return ok
        TbbDevMemUnitTable::accessor buffAcc;
        if (!devMemUnitTable_.find(buffAcc, bufferInfo->devObjKey)) {
            LOG(ERROR) << "The ID already exists, but devMemUnitTable_ don't have entry, need to check";
            RETURN_STATUS_LOG_ERROR(K_RUNTIME_ERROR,
                                    FormatString("The ID already exists,ID:%s", bufferInfo->devObjKey));
        }
        auto &storageBlobVec = buffAcc->second->GetBlobsStorage();
        auto &newBlobVec = buffer->GetDeviceMemUnit()->GetBlobsStorage();
        auto sameDataPtr =
            std::equal(storageBlobVec.begin(), storageBlobVec.end(), newBlobVec.begin(), newBlobVec.end(),
                       [](const Blob &a, const Blob &b) { return a.pointer == b.pointer; });
        if (sameDataPtr) {
            return Status::OK();
        }
        RETURN_STATUS_LOG_ERROR(K_RUNTIME_ERROR, FormatString("The ID already exists, and the data to be deposited is "
                                                              "inconsistent with the existing.ID:%s",
                                                              bufferInfo->devObjKey));
    }
    auto devMemUnit = buffer->GetDeviceMemUnit();
    auto putRequest = AddSubscribe(bufferInfo, devMemUnit->GetBlobsStorage());
    LOG(INFO) << "PutP2PMeta to worker, objectKey: " << buffer->GetObjectKey();
    auto rc = clientWorkerApi_->PutP2PMeta(bufferInfo, devMemUnit->GetBlobsStorage());
    INJECT_POINT("PublishDeviceObject.PutP2PMeta.Timeout", [&rc]() {
        rc = Status(StatusCode::K_RUNTIME_ERROR, "timeout");
        return Status::OK();
    });
    if (rc.IsError()) {
        LOG_IF_ERROR(putRequest->SetPromiseValue(rc), "promise set value failed.");
        LOG(ERROR) << FormatString("Fail to PutP2PMeta devObjectKey %s, rc:%s", bufferInfo->devObjKey, rc.ToString());
        RemoveSubscribe(bufferInfo->devObjKey);
        return rc;
    }
    (void)devMemUnitTable_.insert(std::make_pair(bufferInfo->devObjKey, devMemUnit));
    return Status::OK();
}

Status P2PSubscribe::GetSendStatus(const std::string &objectKey, std::vector<Future> &futureVec)
{
    TbbP2PPutRequestTable::const_accessor acc;
    if (objKey2PutReqTable_.find(acc, objectKey)) {
        auto &putRequest = acc->second;
        return putRequest->CreateEventAndFutureList(putRequest->GetBlobsStorage().size(), futureVec);
    }
    RETURN_STATUS(K_NOT_FOUND, FormatString("The objectKey [ %s ] is not found in this client.", objectKey));
}

void P2PSubscribe::RemoveSubscribe(const std::string &devObjectKey)
{
    (void)devMemUnitTable_.erase(devObjectKey);
    (void)objKey2PutReqTable_.erase(devObjectKey);
}

void P2PSubscribe::ProcessHcclCommDestroy(const SubscribeReceiveNpuEventPb &npuEvent)
{
    auto dstClientId = npuEvent.dst_client_id();
    auto dstDeviceId = npuEvent.dst_device_id();
    LOG(INFO) << FormatString("Get a event to destroy the HcclComm with the remote client %s;%d", dstClientId,
                              dstDeviceId);
    auto sendCommId = HcclCommFactory::GetHcclCommKey(P2PEventType::SEND, deviceId_, dstClientId, dstDeviceId);
    commFactory_->DestroyHcclComm(sendCommId);
    auto recvCommId = HcclCommFactory::GetHcclCommKey(P2PEventType::RECV, deviceId_, dstClientId, dstDeviceId);
    commFactory_->DestroyHcclComm(recvCommId);
}

void P2PSubscribe::MonitorLoop()
{
    while (monitorRun_ && !interruptFlag_) {
        {
            auto hcclCommVec = commFactory_->GetAllHcclComm();
            for (const auto &comm : hcclCommVec) {
                auto rc = comm->CheckHealth(connectTimeOutMS_);
                if (rc.IsError()) {
                    LOG(ERROR) << FormatString("Hccl comm health check failed, %s", rc.ToString());
                    (void)commFactory_->DelComm(comm->GetCommId());
                }
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(ONE_SECOND_MS));
        CommRefCheckMoreThanOne();
    }
}

void P2PSubscribe::StartMonitorThread()
{
    if (monitorRun_ == false && !interruptFlag_) {
        monitorRun_.store(true, std::memory_order_release);
        threadPool_->Execute([this]() { MonitorLoop(); });
        LOG(INFO) << "MonitorThread Start";
    }
}

void P2PSubscribe::StopMonitorThread()
{
    monitorRun_.store(false, std::memory_order_release);
    LOG(INFO) << "MonitorThread Stop";
}

void P2PSubscribe::ReleaseMonitorThread()
{
    LOG(INFO) << "ReleaseMonitorThread";
    StopMonitorThread();
}

void P2PSubscribe::CommRefCheckMoreThanOne()
{
    if (commFactory_->GetHcclCommSize() > 0 && !interruptFlag_) {
        StartMonitorThread();
    } else {
        LOG(INFO) << commFactory_->GetHcclCommSize() << " End Hccl Health Moniter Loop";
        StopMonitorThread();
    }
}

Status P2PSubscribe::AsyncGet(const std::vector<std::shared_ptr<DeviceBuffer>> &bufferList,
                              std::vector<Future> &futureVec, int64_t prefetchTimeout, int64_t subTimeoutMs)
{
    INJECT_POINT("P2PSubscribe.AsyncGet.timeout", [&prefetchTimeout](int64_t timeoutMs) {
        prefetchTimeout = timeoutMs;
        return Status::OK();
    });
    auto p2pRequestsWrapper = std::make_shared<P2PGetRequestsWrapper>(prefetchTimeout, subTimeoutMs);
    for (auto &buffer : bufferList) {
        std::string devObjKey = buffer->bufferInfo_->devObjKey;
        TbbP2PPutRequestTable::accessor acc;
        if (objKey2PutReqTable_.find(acc, devObjKey)) {
            LOG(INFO) << "Get key " << devObjKey
                      << " found locally in put request table, performing on-device D2D copy directly.";
            std::shared_ptr<P2PPutRequest> putRequest = acc->second;
            const std::vector<Blob> blobsInPut = putRequest->GetBlobsStorage();
            std::vector<Blob> blobsInGet = buffer->GetDevBlobList();
            for (size_t i = 0; i < blobsInPut.size(); i++) {
                auto adjustedPtr =
                    static_cast<void *>(static_cast<uint8_t *>(blobsInPut[i].pointer) + buffer->bufferInfo_->srcOffset);
                RETURN_IF_NOT_OK(deviceImpl_->MemCopyD2D(blobsInGet[i].pointer, blobsInGet[i].size,
                    static_cast<void *>(adjustedPtr), blobsInGet[i].size));
            }
            auto promise = std::make_shared<PromiseWithEvent>(devObjKey);
            promise->CreateEventAndFutureList(0, futureVec);
            promise->SetPromiseValue(Status::OK());
            continue;
        }
        auto getRequest =
            std::make_shared<P2PGetRequest>(buffer->bufferInfo_, buffer->GetDevBlobList(), buffer->GetDeviceMemUnit());
        p2pRequestsWrapper->requestList_.emplace_back(getRequest);
        RETURN_IF_NOT_OK(getRequest->CreateEventAndFutureList(buffer->GetDevBlobList().size(), futureVec));
    }
    p2pRequestsWrapper->subTimeout_ = subTimeoutMs;
    p2pRequestsWrapper->getTraceId_ = Trace::Instance().GetTraceID();
    p2pGetQueue_.Push(p2pRequestsWrapper);
    return Status::OK();
}

void P2PSubscribe::SetThreadInterruptFlag2True()
{
    interruptFlag_ = true;
}

Status StatusPromise::SetValue(const Status &rc)
{
    CHECK_FAIL_RETURN_STATUS(!setValue_, K_RUNTIME_ERROR, "Promise already satisfied.");
    setValue_ = true;
    promise_.set_value(rc);
    return Status::OK();
}

Status StatusPromise::GetSharedFuture(std::shared_future<Status> &future)
{
    if (!getFuture_) {
        sharedFuture_ = promise_.get_future();
        getFuture_ = true;
    }
    future = sharedFuture_;

    return Status::OK();
}

StatusPromise::~StatusPromise()
{
    if (!setValue_) {
        Status rc = { K_RUNTIME_ERROR, "StatusPromise destruct without set value." };
        setValue_ = true;
        promise_.set_value(std::move(rc));
    }
}

PromiseWithEvent::PromiseWithEvent(const std::string &objectKey)
{
    promise_ = std::make_shared<StatusPromise>();
    objectKey_ = objectKey;
}

Status PromiseWithEvent::CreateEventAndFutureList(size_t eventCount, std::vector<Future> &futureVec)
{
    std::lock_guard<std::mutex> lock(mutex_);
    (void)eventCount;
    std::shared_future<Status> future;
    RETURN_IF_NOT_OK(promise_->GetSharedFuture(future));
    if (eventCount > 0) {
        CreateEventIfNotExistUnlock();
        futureVec.emplace_back(Future(future, event_, objectKey_));
    } else {
        futureVec.emplace_back(Future(future, nullptr, objectKey_));
    }

    return Status::OK();
}

const std::shared_ptr<AclRtEventWrapper> &PromiseWithEvent::GetEvent()
{
    std::lock_guard<std::mutex> lock(mutex_);
    if (event_ == nullptr) {
        std::shared_ptr<AclRtEventWrapper> event;
        LOG_IF_ERROR(AclRtEventWrapper::Create(event), "Create event error");
        event_ = event;
    }
    return event_;
}

void PromiseWithEvent::DestroyEvent()
{
    std::lock_guard<std::mutex> lock(mutex_);
    event_ = nullptr;
}

Status PromiseWithEvent::SetPromiseValue(const Status &rc)
{
    std::lock_guard<std::mutex> lock(mutex_);
    return promise_->SetValue(rc);
}

void PromiseWithEvent::CreateEvent()
{
    std::lock_guard<std::mutex> lock(mutex_);
    CreateEventIfNotExistUnlock();
}

void PromiseWithEvent::CreateEventIfNotExistUnlock()
{
    if (event_ != nullptr) {
        return;
    }
    std::shared_ptr<AclRtEventWrapper> event;
    LOG_IF_ERROR(AclRtEventWrapper::Create(event), "Create event error");
    event_ = event;
}
}  // namespace datasystem

namespace std {
size_t hash<datasystem::P2PGroupKey>::operator()(const datasystem::P2PGroupKey &key) const
{
    auto val1 = std::hash<bool>()(key.sameNode);
    auto val2 = std::hash<int32_t>()(key.remoteDeviceId);
    auto val3 = std::hash<std::string>()(key.remoteClientId);
    return val1 ^ val2 ^ val3;
}

bool equal_to<datasystem::P2PGroupKey>::operator()(const datasystem::P2PGroupKey &lhs,
                                                   const datasystem::P2PGroupKey &rhs) const
{
    return lhs == rhs;
}
}  // namespace std
