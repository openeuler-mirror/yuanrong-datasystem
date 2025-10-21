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
 * Description: Urma manager for urma context, jfce, jfs, jfr, jfc queues, etc.
 */
#include "datasystem/common/rdma/urma_manager.h"

#include "datasystem/common/log/log.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/rdma/rdma_util.h"
#include "datasystem/common/rdma/urma_manager_wrapper.h"
#include "datasystem/common/rpc/rpc_constants.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/thread_local.h"
#include "datasystem/common/perf/perf_manager.h"

DS_DECLARE_uint32(urma_poll_size);
DS_DECLARE_uint32(urma_connection_size);
DS_DECLARE_bool(urma_event_mode);

namespace datasystem {
constexpr uint32_t DEFAULT_TOKEN = 0xACFE;
UrmaManager &UrmaManager::Instance()
{
    static UrmaManager manager;
    return manager;
}

UrmaManager::UrmaManager()
{
    VLOG(RPC_LOG_LEVEL) << "UrmaManager::UrmaManager()";
    urmaToken_.token = DEFAULT_TOKEN;

    registerSegmentFlag_.bs.token_policy = URMA_TOKEN_PLAIN_TEXT;
    registerSegmentFlag_.bs.cacheable = URMA_NON_CACHEABLE;
    registerSegmentFlag_.bs.access =
        URMA_ACCESS_LOCAL_WRITE | URMA_ACCESS_REMOTE_READ | URMA_ACCESS_REMOTE_WRITE | URMA_ACCESS_REMOTE_ATOMIC;
    registerSegmentFlag_.bs.reserved = 0;

    importSegmentFlag_.bs.cacheable = URMA_NON_CACHEABLE;
    importSegmentFlag_.bs.access =
        URMA_ACCESS_LOCAL_WRITE | URMA_ACCESS_REMOTE_READ | URMA_ACCESS_REMOTE_WRITE | URMA_ACCESS_REMOTE_ATOMIC;
    importSegmentFlag_.bs.mapping = URMA_SEG_NOMAP;
    importSegmentFlag_.bs.reserved = 0;
    localSegmentMap_ = std::make_unique<SegmentMap>();
    remoteDeviceMap_ = std::make_unique<RemoteDeviceMap>();
}

UrmaManager::~UrmaManager()
{
    Stop();
    VLOG(RPC_LOG_LEVEL) << "UrmaManager::~UrmaManager()";
    remoteDeviceMap_.reset();
    localSegmentMap_.reset();
    urmaJfrVec_.clear();
    urmaJfsVec_.clear();
    urmaJfc_.reset();
    UrmaDeleteJfce();
    UrmaDeleteContext();
    UrmaUninit();
    VLOG(RPC_LOG_LEVEL) << "UrmaManager::~UrmaManager() done";
}

Status UrmaManager::Stop()
{
    serverStop_ = true;
    if (serverEventThread_ && serverEventThread_->joinable()) {
        LOG(INFO) << "Waiting for Event thread to exit";
        serverEventThread_->join();
        serverEventThread_.reset();
    }
    return Status::OK();
}

Status UrmaManager::Init(const std::string &host)
{
    LOG(INFO) << "UrmaManager::Init(host = " << host << ")";
    std::string deviceName;
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(GetDevNameFromLocalIp(host, deviceName) == 0, K_INVALID,
                                         "Invalid ip address to get device name");
    RETURN_IF_NOT_OK(EthToRdmaDevName(deviceName, deviceName));
    LOG(INFO) << "deviceName = " << deviceName;
    RETURN_IF_NOT_OK(UrmaInit());
    urma_device_t *urmaDevice = nullptr;
    RETURN_IF_NOT_OK(UrmaGetDeviceByName(deviceName, urmaDevice));
    RETURN_IF_NOT_OK(UrmaQueryDevice(urmaDevice));
    int eidIndex = -1;
    RETURN_IF_NOT_OK(GetEidIndex(urmaDevice, eidIndex));
    RETURN_IF_NOT_OK(UrmaCreateContext(urmaDevice, eidIndex));
    RETURN_IF_NOT_OK(UrmaCreateJfce());
    RETURN_IF_NOT_OK(UrmaCreateJfc(urmaJfc_));
    if (IsEventModeEnabled()) {
        RETURN_IF_NOT_OK(UrmaRearmJfc(urmaJfc_));
    }
    urmaJfsVec_.resize(FLAGS_urma_connection_size);
    urmaJfrVec_.resize(FLAGS_urma_connection_size);
    for (uint i = 0; i < FLAGS_urma_connection_size; ++i) {
        RETURN_IF_NOT_OK(UrmaCreateJfs(urmaJfc_, urmaJfsVec_[i]));
        RETURN_IF_NOT_OK(UrmaCreateJfr(urmaJfc_, urmaJfrVec_[i]));
    }
    serverEventThread_ = std::make_unique<std::thread>(&UrmaManager::ServerEventHandleThreadMain, this);
    return Status::OK();
}

Status UrmaManager::UrmaInit()
{
    LOG(INFO) << "UrmaManager::UrmaInit()";
    urma_init_attr_t urmaInitAttribute = { 0 };
    urmaInitAttribute.uasid = 0;
    urma_status_t ret = urma_init(&urmaInitAttribute);
    if (ret != URMA_SUCCESS) {
        RETURN_STATUS_LOG_ERROR(K_URMA_ERROR, FormatString("Failed to urma init, ret = %d", ret));
    }
    LOG(INFO) << "urma init success";
    return Status::OK();
}

Status UrmaManager::UrmaUninit()
{
    LOG(INFO) << "UrmaManager::UrmaUninit()";
    urma_status_t ret = urma_uninit();
    if (ret != URMA_SUCCESS) {
        RETURN_STATUS_LOG_ERROR(K_URMA_ERROR, FormatString("Failed to urma uninit, ret = %d", ret));
    }
    LOG(INFO) << "urma uninit success";
    return Status::OK();
}

Status UrmaManager::UrmaGetDeviceByName(const std::string &deviceName, urma_device_t *&urmaDevice)
{
    LOG(INFO) << "UrmaManager::UrmaGetDeviceByName()";
    urmaDevice = urma_get_device_by_name(const_cast<char *>(deviceName.c_str()));
    if (urmaDevice) {
        LOG(INFO) << "urma get device by name success";
        return Status::OK();
    }
    RETURN_STATUS_LOG_ERROR(K_URMA_ERROR, FormatString("Failed to urma get device by name, errno = %d", errno));
}

Status UrmaManager::UrmaQueryDevice(urma_device_t *&urmaDevice)
{
    LOG(INFO) << "UrmaManager::UrmaQueryDevice()";
    urma_status_t ret = urma_query_device(urmaDevice, &urmaDeviceAttribute_);
    if (ret != URMA_SUCCESS) {
        RETURN_STATUS_LOG_ERROR(K_URMA_ERROR, FormatString("Failed to urma query device, ret = %d", ret));
    }
    LOG(INFO) << "urma query device success";
    return Status::OK();
}

Status UrmaManager::UrmaGetEidList(urma_device_t *&urmaDevice, urma_eid_info_t *&eidList, uint32_t &eidCount)
{
    LOG(INFO) << "UrmaManager::UrmaGetEidList()";
    eidList = urma_get_eid_list(urmaDevice, &eidCount);
    if (eidList) {
        LOG(INFO) << "urma get eid list success";
        return Status::OK();
    }
    RETURN_STATUS_LOG_ERROR(K_URMA_ERROR, FormatString("Failed to urma get eid list, errno = %d", errno));
}

Status UrmaManager::GetEidIndex(urma_device_t *&urmaDevice, int &eidIndex)
{
    LOG(INFO) << "UrmaManager::GetEidIndex()";
    urma_eid_info_t *eidList = nullptr;
    uint32_t eidCount = 0;
    eidIndex = -1;

    RETURN_IF_NOT_OK(UrmaGetEidList(urmaDevice, eidList, eidCount));

    Raii freeEidList([&eidList]() {
        if (eidList) {
            urma_free_eid_list(eidList);
        }
    });

    if (eidCount > 0) {
        eidIndex = eidList[0].eid_index;
        return Status::OK();
    }
    RETURN_STATUS_LOG_ERROR(K_URMA_ERROR, "Failed to get eid index for device");
}

Status UrmaManager::UrmaCreateContext(urma_device_t *&urmaDevice, const uint32_t eidIndex)
{
    LOG(INFO) << "UrmaManager::UrmaCreateContext()";
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(!urmaContext_, K_DUPLICATED,
                                         "Failed to urma create context, context already exist");

    urmaContext_ = urma_create_context(urmaDevice, eidIndex);
    if (urmaContext_) {
        LOG(INFO) << "urma create context success";
        return Status::OK();
    }
    RETURN_STATUS_LOG_ERROR(K_URMA_ERROR, FormatString("Failed to urma create context, errno = %d", errno));
}

Status UrmaManager::UrmaDeleteContext()
{
    LOG(INFO) << "UrmaManager::UrmaDeleteContext()";
    if (urmaContext_) {
        urma_status_t ret = urma_delete_context(urmaContext_);
        if (ret != URMA_SUCCESS) {
            RETURN_STATUS_LOG_ERROR(K_URMA_ERROR, FormatString("Failed to urma delete context, ret = %d", ret));
        }
        LOG(INFO) << "urma delete context success";
        urmaContext_ = nullptr;
    }
    return Status::OK();
}

Status UrmaManager::UrmaCreateJfce()
{
    LOG(INFO) << "UrmaManager::UrmaCreateJfce()";
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(!urmaJfce_, K_DUPLICATED, "Failed to urma create jfce, jfce already exist");

    urmaJfce_ = urma_create_jfce(urmaContext_);
    if (urmaJfce_) {
        LOG(INFO) << "urma create jfce success";
        return Status::OK();
    }
    RETURN_STATUS_LOG_ERROR(K_URMA_ERROR, FormatString("Failed to urma create jfce, errno = %d", errno));
}

Status UrmaManager::UrmaDeleteJfce()
{
    LOG(INFO) << "UrmaManager::UrmaDeleteJfce()";
    if (urmaJfce_) {
        urma_status_t ret = urma_delete_jfce(urmaJfce_);
        if (ret != URMA_SUCCESS) {
            RETURN_STATUS_LOG_ERROR(K_URMA_ERROR, FormatString("Failed to urma delete jfce, ret = %d", ret));
        }
        LOG(INFO) << "urma delete jfce success";
        urmaJfce_ = nullptr;
    }
    return Status::OK();
}

Status UrmaManager::UrmaCreateJfc(custom_unique_ptr<urma_jfc_t> &out)
{
    LOG(INFO) << "UrmaManager::UrmaCreateJfc()";
    urma_jfc_cfg_t jfcConfig;
    jfcConfig.depth = urmaDeviceAttribute_.dev_cap.max_jfc_depth;
    jfcConfig.flag.value = 0;
    jfcConfig.jfce = urmaJfce_;
    jfcConfig.user_ctx = 0;

    out = MakeCustomUnique<urma_jfc_t>(urma_create_jfc(urmaContext_, &jfcConfig), [](urma_jfc_t *p) {
        std::stringstream oss;
        oss << "urma_delete_jfc ... ";
        auto ret = urma_delete_jfc(p);
        if (ret == URMA_SUCCESS) {
            oss << "success";
        } else {
            oss << FormatString("failed. ret = %d", ret);
        }
        LOG(INFO) << oss.str();
    });
    if (out) {
        LOG(INFO) << "urma create jfc success";
        return Status::OK();
    }
    RETURN_STATUS_LOG_ERROR(K_URMA_ERROR, FormatString("Failed to urma create jfc, errno = %d", errno));
}

Status UrmaManager::UrmaRearmJfc(const custom_unique_ptr<urma_jfc_t> &jfc)
{
    LOG(INFO) << "UrmaManager::UrmaRearmJfc()";
    urma_status_t ret = urma_rearm_jfc(jfc.get(), false);
    if (ret != URMA_SUCCESS) {
        RETURN_STATUS_LOG_ERROR(K_URMA_ERROR, FormatString("Failed to urma rearm jfc, ret = %d", ret));
    }
    LOG(INFO) << "urma rearm jfc success";
    return Status::OK();
}

Status UrmaManager::UrmaCreateJfs(const custom_unique_ptr<urma_jfc_t> &jfc, custom_unique_ptr<urma_jfs_t> &out)
{
    LOG(INFO) << "UrmaManager::UrmaCreateJfs()";

    urma_jfs_cfg_t jfsConfig;
    jfsConfig.depth = JETTY_SIZE_;
    jfsConfig.trans_mode = URMA_TM_RM;
    jfsConfig.priority = URMA_MAX_PRIORITY; /* Highest priority */
    jfsConfig.max_sge = 1;
    jfsConfig.max_inline_data = 0;
    jfsConfig.rnr_retry = URMA_TYPICAL_RNR_RETRY;
    jfsConfig.err_timeout = URMA_TYPICAL_ERR_TIMEOUT;
    jfsConfig.jfc = jfc.get();
    jfsConfig.user_ctx = 0;

    out = MakeCustomUnique<urma_jfs_t>(urma_create_jfs(urmaContext_, &jfsConfig), [](urma_jfs_t *p) {
        std::stringstream oss;
        oss << "urma_delete_jfs ... ";
        auto ret = urma_delete_jfs(p);
        if (ret == URMA_SUCCESS) {
            oss << "success";
        } else {
            oss << FormatString("failed. ret = %d", ret);
        }
        LOG(INFO) << oss.str();
    });
    if (out) {
        LOG(INFO) << "urma create jfs success";
        return Status::OK();
    }
    RETURN_STATUS_LOG_ERROR(K_URMA_ERROR, FormatString("Failed to urma create jfs, errno = %d", errno));
}

Status UrmaManager::UrmaCreateJfr(const custom_unique_ptr<urma_jfc_t> &jfc, custom_unique_ptr<urma_jfr_t> &out)
{
    LOG(INFO) << "UrmaManager::UrmaCreateJfr()";

    urma_jfr_cfg_t jfrConfig;
    jfrConfig.depth = JETTY_SIZE_;
    jfrConfig.flag.bs.tag_matching = URMA_NO_TAG_MATCHING;
    jfrConfig.trans_mode = URMA_TM_RM;
    jfrConfig.min_rnr_timer = URMA_TYPICAL_MIN_RNR_TIMER;
    jfrConfig.jfc = jfc.get();
    jfrConfig.token_value = urmaToken_;
    jfrConfig.id = 0;
    jfrConfig.max_sge = 1;

    out = MakeCustomUnique<urma_jfr_t>(urma_create_jfr(urmaContext_, &jfrConfig), [](urma_jfr_t *p) {
        std::stringstream oss;
        oss << "urma_delete_jfr ... ";
        auto ret = urma_delete_jfr(p);
        if (ret == URMA_SUCCESS) {
            oss << "success";
        } else {
            oss << FormatString("failed. ret = %d", ret);
        }
        LOG(INFO) << oss.str();
    });
    if (out) {
        LOG(INFO) << "urma create jfr success";
        return Status::OK();
    }
    RETURN_STATUS_LOG_ERROR(K_URMA_ERROR, FormatString("Failed to urma create jfr, errno = %d", errno));
}

bool UrmaManager::IsEventModeEnabled()
{
    return FLAGS_urma_event_mode;
}

std::string UrmaManager::GetEid()
{
    return EidToStr(urmaContext_->eid);
};

uint64_t UrmaManager::GetUasid()
{
    return urmaContext_->uasid;
};

std::vector<uint32_t> UrmaManager::GetJfrIds()
{
    std::vector<uint32_t> results;
    for (auto &urmaJfr : urmaJfrVec_) {
        results.emplace_back(urmaJfr->jfr_id.id);
    }
    return results;
};

Status UrmaManager::RegisterSegment(const uint64_t &segAddress, const uint64_t &segSize)
{
    SegmentMap::ConstAccessor constAccessor;
    RETURN_IF_NOT_OK(GetOrRegisterSegment(segAddress, segSize, constAccessor));
    return Status::OK();
}

Status UrmaManager::GetSegmentInfo(const uint64_t &segAddress, const uint64_t &segSize, uint64_t &segVA,
                                   uint64_t &segLen, uint32_t &segFlag, uint32_t &segTokenId)
{
    SegmentMap::ConstAccessor constAccessor;
    RETURN_IF_NOT_OK(GetOrRegisterSegment(segAddress, segSize, constAccessor));
    auto &localSegment = constAccessor.entry->data.segment_;
    segVA = localSegment->seg.ubva.va;
    segLen = localSegment->seg.len;
    segFlag = localSegment->seg.attr.value;
    segTokenId = localSegment->seg.token_id;
    return Status::OK();
}

Status UrmaManager::GetOrRegisterSegment(const uint64_t &segAddress, const uint64_t &segSize,
                                         SegmentMap::ConstAccessor &constAccessor)
{
    std::shared_lock<std::shared_timed_mutex> l(localMapMutex_);
    if (!localSegmentMap_->Find(constAccessor, segAddress)) {
        SegmentMap::Accessor accessor;
        if (localSegmentMap_->Insert(accessor, segAddress)) {
            urma_seg_cfg_t segmentConfig;
            segmentConfig.va = segAddress;
            segmentConfig.len = segSize;
            segmentConfig.token_value = urmaToken_;
            segmentConfig.flag = registerSegmentFlag_;
            segmentConfig.user_ctx = (uint64_t)NULL;
            segmentConfig.iova = 0;
            PerfPoint point(PerfKey::URMA_REGISTER_SEGMENT);
            auto *segment = urma_register_seg(urmaContext_, &segmentConfig);
            point.Record();
            if (segment == nullptr) {
                localSegmentMap_->BlockingErase(accessor);
                return Status(K_RUNTIME_ERROR, FormatString("Failed to register segment, address %llu, size %llu.",
                                                            segAddress, segSize));
            }
            accessor.entry->data.Set(segment, true);
        }
        accessor.Release();
        // Switch to const accessor so it does not block the others.
        CHECK_FAIL_RETURN_STATUS(localSegmentMap_->Find(constAccessor, segAddress), K_RUNTIME_ERROR,
                                 "Failed to operate on local segment map.");
    }
    return Status::OK();
}

Status UrmaManager::ServerEventHandleThreadMain()
{
    // Run this method until serverStop is called.
    while (!serverStop_.load()) {
        std::vector<uint64_t> successCompletedReqs;
        std::vector<uint64_t> failedCompletedReqs;
        Status rc = PollJfcWait(urmaJfc_, MAX_POLL_JFC_TRY_CNT, successCompletedReqs, failedCompletedReqs,
                                FLAGS_urma_poll_size);

        // push it into request set
        // we do not need lock for finishedRequests_ as its accessed only by single thread
        if (successCompletedReqs.size()) {
            finishedRequests_.insert(successCompletedReqs.begin(), successCompletedReqs.end());
        }
        if (failedCompletedReqs.size()) {
            finishedRequests_.insert(failedCompletedReqs.begin(), failedCompletedReqs.end());
            failedRequests_.insert(failedCompletedReqs.begin(), failedCompletedReqs.end());
        }
        // notify threads waiting on any finishedRequests
        CheckAndNotify();
    }
    return Status::OK();
}

Status UrmaManager::CheckAndNotify()
{
    // if no finished requests, no need to notify
    if (finishedRequests_.empty()) {
        return Status::OK();
    }

    // Iterate through the finishedRequests_ set and notify request threads
    for (auto it = finishedRequests_.begin(); it != finishedRequests_.end();) {
        auto requestId = *it;
        std::shared_ptr<Event> event;
        // Get the event for request Id
        if (GetEvent(requestId, event).IsOk()) {
            if (failedRequests_.count(requestId)) {
                event->set_failed();
                failedRequests_.erase(requestId);
            }
            // Notify everyone who are waiting on the event
            event->notify_all();
            // delete the event and
            VLOG(1) << "[UrmaEventHandler] Notifying the request id: " << requestId;
            // remove request id from finishedRequests_ set
            // we dont need lock for finishedRequests_ as its accessed only by single thread
            it = finishedRequests_.erase(it);
        } else {
            ++it;
        }
    }

    return Status::OK();
}

void UrmaManager::DeleteEvent(uint64_t requestId)
{
    std::unique_lock<std::shared_timed_mutex> lock(eventMapMutex_);
    eventMap_.erase(requestId);
}

Status UrmaManager::GetEvent(uint64_t requestId, std::shared_ptr<Event> &event)
{
    std::shared_lock<std::shared_timed_mutex> lock(eventMapMutex_);
    auto eventItr = eventMap_.find(requestId);
    if (eventItr != eventMap_.end()) {
        event = eventItr->second;
        return Status::OK();
    }
    // can happen if event is not yet inserted by sender thread
    // so dont log the status
    RETURN_STATUS(K_NOT_FOUND, FormatString("Request id %d doesnt exist in event map", requestId));
}

Status UrmaManager::CreateEvent(uint64_t requestId, std::shared_ptr<Event> &event)
{
    std::unique_lock<std::shared_timed_mutex> lock(eventMapMutex_);
    auto result = eventMap_.emplace(requestId, std::make_shared<Event>(requestId));
    if (result.second) {
        event = result.first->second;
        return Status::OK();
    } else {
        // If this happens that means requestId is duplicated
        RETURN_STATUS_LOG_ERROR(K_DUPLICATED, FormatString("Request id %d already exists in event map", requestId));
    }
}

Status UrmaManager::WaitToFinish(uint64_t requestId, int64_t timeoutMs)
{
    PerfPoint point(PerfKey::URMA_WAIT_TO_FINISH);
    if (timeoutMs < 0) {
        RETURN_STATUS_LOG_ERROR(K_RPC_DEADLINE_EXCEEDED, FormatString("timedout waiting for request: %d", requestId_));
    }
    std::shared_ptr<Event> event;
    RETURN_IF_NOT_OK(GetEvent(requestId, event));
    // use this unique request id as key to wait
    // wait until timeout

    Raii deleteEvent([this, &requestId]() { DeleteEvent(requestId); });

    VLOG(1) << "[UrmaEventHandler] Started waiting for the request id: " << requestId;
    RETURN_IF_NOT_OK(event->wait_for(std::chrono::milliseconds(timeoutMs)));
    if (event->is_failed()) {
        return Status(K_URMA_ERROR, FormatString("Polling failed with an error for requestId: %d", requestId));
    }
    VLOG(1) << "[UrmaEventHandler] Done waiting for the request id: " << requestId;
    return Status::OK();
}

Status UrmaManager::CheckCompletionRecordStatus(urma_cr_t completeRecords[], int count,
                                                std::vector<uint64_t> &successCompletedReqs,
                                                std::vector<uint64_t> &failedCompletedReqs)
{
    INJECT_POINT("UrmaManager.CheckCompletionRecordStatus", [&completeRecords](int index) {
        completeRecords[index].status = URMA_CR_REM_ACCESS_ABORT_ERR;
        return Status::OK();
    });
    for (int i = 0; i < count; i++) {
        if (completeRecords[i].status == URMA_CR_SUCCESS) {
            VLOG(1) << "[UrmaEventHandler] Got event with request id: " << completeRecords[i].user_ctx;
            successCompletedReqs.push_back(completeRecords[i].user_ctx);
        } else {
            LOG(ERROR) << FormatString("Failed to poll jfc requestId: %d CR.status: %d", completeRecords[i].user_ctx,
                                       completeRecords[i].status);
            failedCompletedReqs.push_back(completeRecords[i].user_ctx);
        }
    }

    if (failedCompletedReqs.size()) {
        RETURN_STATUS(K_URMA_ERROR, "Failed to poll jfc");
    }
    return Status::OK();
}

Status UrmaManager::PollJfcWait(const custom_unique_ptr<urma_jfc_t> &jfc, const uint64_t maxTryCount,
                                std::vector<uint64_t> &successCompletedReqs, std::vector<uint64_t> &failedCompletedReqs,
                                const uint64_t numPollCRS)
{
    auto urmaJfc = jfc.get();
    urma_cr_t completeRecords[numPollCRS];
    urma_jfc_t *ev_jfc = nullptr;
    int cnt;

    if (IsEventModeEnabled()) {
        // wait for the event
        cnt = urma_wait_jfc(urmaJfce_, 1, RPC_POLL_TIME, &ev_jfc);
        if (cnt < 0 || cnt > 1 || (cnt == 1 && urmaJfc != ev_jfc)) {
            // This is error case
            // cnt can be 0 or 1 and jfc should match
            RETURN_STATUS_LOG_ERROR(K_URMA_ERROR, FormatString("Failed to wait jfc, ret = %d", cnt));
        } else if (cnt == 0) {
            // not found any event, just retry
            return Status::OK();
        }

        // Got the event, now get CR for the event
        // Event mode can poll one CR at a time
        cnt = urma_poll_jfc(urmaJfc, numPollCRS, &completeRecords[0]);
        INJECT_POINT("UrmaManager.CheckCompletionRecordStatus", [&completeRecords](int index) {
            completeRecords[0].status = URMA_CR_REM_ACCESS_ABORT_ERR;
            return Status::OK();
        });
        if (cnt < 0) {
            // this is error case
            // cnt can be 0 or 1
            RETURN_STATUS_LOG_ERROR(K_URMA_ERROR, FormatString("Failed to poll jfc, ret = %d, CR.status = %d", cnt,
                                                               completeRecords[0].status));
        } else if (cnt > 0) {
            // Ack the event and rearm jfc to process next event
            uint32_t ack_cnt = 1;
            urma_ack_jfc((urma_jfc_t **)&ev_jfc, &ack_cnt, 1);
            auto status = urma_rearm_jfc(urmaJfc, false);
            if (status != URMA_SUCCESS) {
                RETURN_STATUS_LOG_ERROR(K_URMA_ERROR, FormatString("Failed to rearm jfc, status = %d", status));
            }
            return CheckCompletionRecordStatus(completeRecords, cnt, successCompletedReqs, failedCompletedReqs);
        }
        return Status::OK();
    }

    // trys maxTryCount times to get an event
    for (uint64_t i = 0; i < maxTryCount; ++i) {
        cnt = urma_poll_jfc(urmaJfc, numPollCRS, completeRecords);
        if (cnt == 0) {
            // if there is ntg to poll, just sleep for 10us
            usleep(10);
        } else if (cnt < 0) {
            RETURN_STATUS_LOG_ERROR(K_URMA_ERROR, FormatString("Failed to poll jfc, ret = %d", cnt));
        } else if (cnt > 0) {
            return CheckCompletionRecordStatus(completeRecords, cnt, successCompletedReqs, failedCompletedReqs);
        }
        if (serverStop_.load()) {
            RETURN_STATUS_LOG_ERROR(K_URMA_ERROR, FormatString("Worker exiting"));
        }
    }
    RETURN_STATUS(K_TRY_AGAIN, FormatString("No Event present in JFC"));
}

Status UrmaManager::ImportRemoteJfr(const RpcChannel::UrmaInfo &urmaInfo)
{
    PerfPoint point1(PerfKey::URMA_CONNECT_WITH_REMOTE_DEVICE);
    const std::string remoteDeviceId = urmaInfo.localAddress_.ToString();
    std::shared_lock<std::shared_timed_mutex> l(remoteMapMutex_);
    // Insert or update the import jfr (in case the sending worker restarts)
    RemoteDeviceMap::Accessor accessor;
    auto res = remoteDeviceMap_->Insert(accessor, remoteDeviceId);
    auto &device = accessor.entry->data;
    if (!res) {
        // Existing entry exists, and we will update the imported jfr (assuming the sending worker restarted)
        // First of all, release the existing imported jfr
        device.Clear();
    }
    device.urmaInfo_ = urmaInfo;
    // Now we import a new jfr
    urma_rjfr_t remoteJfr;
    urma_eid_t eid;
    auto rc = StrToEid(urmaInfo.eid, eid);
    if (rc.IsError()) {
        remoteDeviceMap_->BlockingErase(accessor);
        return rc;
    }
    remoteJfr.jfr_id.eid = eid;
    remoteJfr.jfr_id.uasid = urmaInfo.uasid;
    remoteJfr.trans_mode = URMA_TM_RM;
    std::vector<urma_target_jetty_t *> tjfrs;
    for (uint i = 0; i < FLAGS_urma_connection_size; ++i) {
        remoteJfr.jfr_id.id = urmaInfo.jfr_ids[i];
        PerfPoint point1a(PerfKey::URMA_IMPORT_JFR);
        auto *tjfr = urma_import_jfr(urmaContext_, &remoteJfr, &urmaToken_);
        point1a.Record();
        if (tjfr == nullptr) {
            remoteDeviceMap_->BlockingErase(accessor);
            return Status(K_RUNTIME_ERROR, FormatString("Failed to import jfr, %s", urmaInfo.ToString()));
        }
        PerfPoint point1b(PerfKey::URMA_ADVISE_JFR);
        if (urma_advise_jfr(urmaJfsVec_[i].get(), tjfr) != URMA_SUCCESS) {
            (void)urma_unimport_jfr(tjfr);
            RETURN_STATUS_LOG_ERROR(K_URMA_ERROR, FormatString("Failed to advise jfr"));
        }
        point1b.Record();
        tjfrs.emplace_back(tjfr);
    }
    device.SetJfrs(tjfrs);
    accessor.Release();
    point1.Record();
    return Status::OK();
}

Status UrmaManager::ImportSegAndWritePayload(const UrmaImportSegmentPb &urmaInfo, const uint64_t &localSegAddress,
                                             const uint64_t &localSegSize, const uint64_t &localObjectAddress,
                                             const uint64_t &readOffset, const uint64_t &readSize,
                                             const uint64_t &metaDataSize, bool blocking, std::vector<uint64_t> &keys)
{
    // Note that the returned keys only contain the new key(s).
    keys.clear();
    PerfPoint point(PerfKey::URMA_IMPORT_AND_WRITE_PAYLOAD);
    const HostPort requestAddress(urmaInfo.request_address().host(), urmaInfo.request_address().port());
    const std::string remoteDeviceId = requestAddress.ToString();
    PerfPoint point1(PerfKey::URMA_CONNECT_WITH_REMOTE_DEVICE);
    std::shared_lock<std::shared_timed_mutex> l(remoteMapMutex_);
    RemoteDeviceMap::ConstAccessor constAccessor;
    // The comm layer (zmq) has already exchanged the jfr, and we should be able to locate the entry.
    auto res = remoteDeviceMap_->Find(constAccessor, remoteDeviceId);
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(res, K_RUNTIME_ERROR,
                                         FormatString("Failed to find jfr from %s", remoteDeviceId));
    point1.Record();
    PerfPoint point2(PerfKey::URMA_IMPORT_REMOTE_SEGMENT);
    SegmentMap::ConstAccessor remoteSegAccessor;
    RETURN_IF_NOT_OK(constAccessor.entry->data.GetOrImportRemoteSeg(urmaInfo, remoteSegAccessor));
    point2.Record();

    PerfPoint point3(PerfKey::URMA_REGISTER_LOCAL_SEGMENT);
    SegmentMap::ConstAccessor localSegAccessor;
    RETURN_IF_NOT_OK(GetOrRegisterSegment(localSegAddress, localSegSize, localSegAccessor));
    point3.Record();

    // Write payload
    urma_jfs_wr_flag_t flag = { 0 };
    flag.bs.complete_enable = 1;

    PerfPoint point4(PerfKey::URMA_TOTAL_WRITE);
    uint64_t writtenSize = 0;
    uint64_t remainSize = readSize;
    while (remainSize > 0) {
        const uint64_t writeSize = std::min(remainSize, (uint64_t)urmaDeviceAttribute_.dev_cap.max_write_size);
        const uint64_t key = requestId_.fetch_add(1);
        const uint64_t index = key % FLAGS_urma_connection_size;
        urma_jfs_t *urmaJfs = urmaJfsVec_[index].get();
        urma_target_jetty_t *importJfr = constAccessor.entry->data.importJfrs_[index].get();
        PerfPoint point4a(PerfKey::URMA_WRITE);
        urma_status_t ret =
            urma_write(urmaJfs, importJfr, remoteSegAccessor.entry->data.segment_.get(),
                       localSegAccessor.entry->data.segment_.get(),
                       urmaInfo.seg_va() + urmaInfo.seg_data_offset() + readOffset + writtenSize,
                       localObjectAddress + readOffset + metaDataSize + writtenSize, writeSize, flag, key);
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
            ret == URMA_SUCCESS, K_RUNTIME_ERROR,
            FormatString("Failed to urma write object with key = %zu, ret = %d", key, ret));
        point4a.Record();
        keys.emplace_back(key);

        remainSize -= writeSize;
        writtenSize += writeSize;

        std::shared_ptr<Event> event;
        RETURN_IF_NOT_OK(CreateEvent(key, event));
    }
    point4.Record();
    // If it is blocking wait, we will wait for the write to finish here.
    if (blocking) {
        auto remainingTime = []() { return reqTimeoutDuration.CalcRealRemainingTime(); };
        auto errorHandler = [](Status &status) { return status; };
        RETURN_IF_NOT_OK(WaitUrmaEvent(keys, remainingTime, errorHandler));
        keys.clear();
    }
    return Status::OK();
}

urma_target_seg_t *UrmaManager::ImportSegment(urma_seg_t &remoteSegment)
{
    return urma_import_seg(urmaContext_, &remoteSegment, &urmaToken_, 0, importSegmentFlag_);
}

Status UrmaManager::UnimportSegment(const HostPort &remoteAddress, const uint64_t segmentAddress)
{
    RemoteDeviceMap::Accessor accessor;
    if (remoteDeviceMap_->Find(accessor, remoteAddress.ToString())) {
        RETURN_IF_NOT_OK(accessor.entry->data.UnimportRemoteSeg(segmentAddress));
        return Status::OK();
    }
    RETURN_STATUS(K_NOT_FOUND, "Cannot unimport jfr, jfr is not imported");
}

Status UrmaManager::RemoveRemoteDevice(const HostPort &remoteAddress)
{
    RemoteDeviceMap::Accessor accessor;
    if (remoteDeviceMap_->Find(accessor, remoteAddress.ToString())) {
        remoteDeviceMap_->BlockingErase(accessor);
        return Status::OK();
    }
    RETURN_STATUS(K_NOT_FOUND, FormatString("Cannot remove RemoteDevice, RemoteDevice for %s does not exist",
                                            remoteAddress.ToString()));
}

std::string UrmaManager::EidToStr(const urma_eid_t &eid)
{
    return std::string(reinterpret_cast<const char *>(eid.raw), URMA_EID_SIZE);
}

Status UrmaManager::StrToEid(const std::string &eid, urma_eid_t &out)
{
    CHECK_FAIL_RETURN_STATUS(eid.size() == URMA_EID_SIZE, K_RUNTIME_ERROR,
                             FormatString("Eid size mismatch, expected: %d, actual: %d", URMA_EID_SIZE, eid.size()));
    auto rc = memcpy_s(out.raw, URMA_EID_SIZE, eid.data(), eid.size());
    CHECK_FAIL_RETURN_STATUS(rc == EOK, K_RUNTIME_ERROR,
                             FormatString("Unable to copy %d bytes, rc = %d, errno = %d", URMA_EID_SIZE, rc, errno));
    return Status::OK();
}

Status UrmaManager::ExchangeJfr(const UrmaHandshakeReqPb &req, UrmaHandshakeRspPb &rsp)
{
    if (UrmaManager::IsUrmaEnabled()) {
        auto &mgr = UrmaManager::Instance();
        // Register the incoming jfr.
        RpcChannel::UrmaInfo urmaInfo;
        urmaInfo.eid = req.eid();
        urmaInfo.uasid = req.uasid();
        for (auto jfrId : req.jfr_ids()) {
            urmaInfo.jfr_ids.emplace_back(jfrId);
        }
        urmaInfo.localAddress_ = HostPort(req.address().host(), req.address().port());
        LOG(INFO) << urmaInfo.ToString();
        LOG_IF_ERROR(mgr.ImportRemoteJfr(urmaInfo), "Error in import incoming jfr");
        // Return our own jfr.
        rsp.set_eid(mgr.GetEid());
        rsp.set_uasid(mgr.GetUasid());
        for (auto jfrId : mgr.GetJfrIds()) {
            rsp.add_jfr_ids(jfrId);
        }
    }
    return Status::OK();
}

Segment::~Segment()
{
    Clear();
}

void Segment::Set(urma_target_seg_t *seg, const bool local)
{
    Clear();
    local_ = local;
    segment_ = MakeCustomUnique<urma_target_seg_t>(seg, [this](urma_target_seg_t *p) {
        if (p) {
            if (local_) {
                urma_status_t ret = urma_unregister_seg(p);
                LOG_IF(ERROR, ret != URMA_SUCCESS) << "Failed to unregister segment, ret = " << ret;
            } else {
                urma_status_t ret = urma_unimport_seg(p);
                LOG_IF(ERROR, ret != URMA_SUCCESS) << "Failed to unimport segment, ret = " << ret;
            }
        }
    });
}

void Segment::Clear()
{
    segment_.reset();
}

RemoteDevice::~RemoteDevice()
{
    Clear();
}

void RemoteDevice::SetJfrs(std::vector<urma_target_jetty_t *> &jetties)
{
    Clear();
    for (auto &jetty : jetties) {
        importJfrs_.emplace_back(MakeCustomUnique<urma_target_jetty_t>(jetty, [](urma_target_jetty_t *p) {
            if (p) {
                auto ret = urma_unimport_jfr(p);
                LOG_IF(ERROR, ret != URMA_SUCCESS) << "Failed to unimport jfr, ret = " << ret;
            }
        }));
    }
}

Status RemoteDevice::GetOrImportRemoteSeg(const UrmaImportSegmentPb &importSegmentInfo,
                                          SegmentMap::ConstAccessor &constAccessor)
{
    if (!remoteSegments_.Find(constAccessor, importSegmentInfo.seg_va())) {
        SegmentMap::Accessor accessor;
        if (remoteSegments_.Insert(accessor, importSegmentInfo.seg_va())) {
            urma_seg_t remoteSegment;
            urma_eid_t eid;
            auto rc = UrmaManager::StrToEid(urmaInfo_.eid, eid);
            if (rc.IsError()) {
                remoteSegments_.BlockingErase(accessor);
                return rc;
            }
            remoteSegment.ubva.eid = eid;
            remoteSegment.ubva.uasid = urmaInfo_.uasid;
            remoteSegment.ubva.va = importSegmentInfo.seg_va();
            remoteSegment.len = importSegmentInfo.seg_len();
            remoteSegment.attr.value = importSegmentInfo.seg_flag();
            remoteSegment.token_id = importSegmentInfo.seg_token_id();
            auto *segment = UrmaManager::Instance().ImportSegment(remoteSegment);
            if (segment == nullptr) {
                remoteSegments_.BlockingErase(accessor);
                return Status(K_RUNTIME_ERROR,
                              FormatString("Failed to import segment from device with eid %s, seg_va = %zu.",
                                           urmaInfo_.eid, importSegmentInfo.seg_va()));
            }
            accessor.entry->data.Set(segment, false);
        }
        accessor.Release();
        // Switch to const accessor so it does not block the others.
        CHECK_FAIL_RETURN_STATUS(remoteSegments_.Find(constAccessor, importSegmentInfo.seg_va()), K_RUNTIME_ERROR,
                                 "Failed to operate on remote segment map.");
    }
    return Status::OK();
}

void RemoteDevice::Clear()
{
    importJfrs_.clear();
}

Status RemoteDevice::UnimportRemoteSeg(const uint64_t segmentAddress)
{
    SegmentMap::Accessor accessor;
    if (remoteSegments_.Find(accessor, segmentAddress)) {
        remoteSegments_.BlockingErase(accessor);
        return Status::OK();
    }
    RETURN_STATUS(K_NOT_FOUND, "Cannot unimport remote segment, remote segment is not imported");
}

}  // namespace datasystem
