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
 * Description: Urma resource manager.
 */

#include "datasystem/common/rdma/urma_resource.h"

#include <cerrno>
#include <sstream>
#include <utility>

#include <ub/umdk/urma/urma_opcode.h>

#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/log/trace.h"
#include "datasystem/common/perf/perf_manager.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/rdma/urma_dlopen_util.h"
#include "datasystem/utils/status.h"

DS_DECLARE_bool(urma_event_mode);

namespace datasystem {
const uint32_t JETTY_SIZE = 256;

UrmaContext::~UrmaContext()
{
    if (raw_ == nullptr) {
        return;
    }
    const auto ret = ds_urma_delete_context(raw_);
    LOG_IF(WARNING, ret != URMA_SUCCESS) << "Failed to delete context, ret = " << ret;
    LOG(INFO) << "urma delete context success";
    raw_ = nullptr;
}

Status UrmaContext::Create(urma_device_t *device, uint32_t eidIndex, std::unique_ptr<UrmaContext> &context)
{
    LOG(INFO) << "urma_create_context with eidIndex:" << eidIndex;
    CHECK_FAIL_RETURN_STATUS(device != nullptr, K_INVALID, "URMA device is null");
    auto *raw = ds_urma_create_context(device, eidIndex);
    if (raw == nullptr) {
        RETURN_STATUS(K_URMA_ERROR, FormatString("Failed to urma create context, errno = %d", errno));
    }
    context = std::make_unique<UrmaContext>(raw);
    LOG(INFO) << "urma create context success";
    return Status::OK();
}

UrmaJfce::~UrmaJfce()
{
    if (raw_ == nullptr) {
        return;
    }
    const auto ret = ds_urma_delete_jfce(raw_);
    LOG_IF(WARNING, ret != URMA_SUCCESS) << "Failed to delete jfce, ret = " << ret;
    LOG(INFO) << "urma delete jfce success";
    raw_ = nullptr;
}

Status UrmaJfce::Create(urma_context_t *context, std::unique_ptr<UrmaJfce> &jfce)
{
    LOG(INFO) << "urma_create_jfce";
    CHECK_FAIL_RETURN_STATUS(context != nullptr, K_INVALID, "URMA context is null");
    auto *raw = ds_urma_create_jfce(context);
    if (raw == nullptr) {
        RETURN_STATUS(K_URMA_ERROR, FormatString("Failed to urma create jfce, errno = %d", errno));
    }
    jfce = std::make_unique<UrmaJfce>(raw);
    LOG(INFO) << "urma create jfce success";
    return Status::OK();
}

UrmaJfc::~UrmaJfc()
{
    if (raw_ == nullptr) {
        return;
    }
    const auto ret = ds_urma_delete_jfc(raw_);
    LOG_IF(ERROR, ret != URMA_SUCCESS) << "Failed to urma delete jfc, ret = " << ret;
    LOG(INFO) << "urma delete jfc success";
    raw_ = nullptr;
}

Status UrmaJfc::Create(urma_context_t *context, const urma_device_attr_t &deviceAttr, urma_jfce_t *jfce,
                       std::unique_ptr<UrmaJfc> &jfc)
{
    LOG(INFO) << "urma create jfc";
    CHECK_FAIL_RETURN_STATUS(context != nullptr, K_INVALID, "URMA context is null");

    urma_jfc_cfg_t jfcConfig;
    jfcConfig.depth = deviceAttr.dev_cap.max_jfc_depth;
    jfcConfig.flag.value = 0;
    if (GetUrmaMode() == UrmaMode::IB) {
        jfcConfig.jfce = jfce;
    } else if (GetUrmaMode() == UrmaMode::UB) {
        jfcConfig.jfce = nullptr;
    } else {
        RETURN_STATUS(K_INVALID, "Invalid Urma mode");
    }
    jfcConfig.user_ctx = 0;
    jfcConfig.ceqn = 0;

    auto *raw = ds_urma_create_jfc(context, &jfcConfig);
    if (raw == nullptr) {
        RETURN_STATUS_LOG_ERROR(K_URMA_ERROR, FormatString("Failed to urma create jfc, errno = %d", errno));
    }
    jfc = std::make_unique<UrmaJfc>(raw);
    return Status::OK();
}

Status UrmaJfc::Rearm() const
{
    LOG(INFO) << "urma rearm jfc";
    CHECK_FAIL_RETURN_STATUS(raw_ != nullptr, K_INVALID, "URMA jfc is null");
    const auto ret = ds_urma_rearm_jfc(raw_, false);
    if (ret != URMA_SUCCESS) {
        RETURN_STATUS_LOG_ERROR(K_URMA_ERROR, FormatString("Failed to urma rearm jfc, ret = %d", ret));
    }
    LOG(INFO) << "urma rearm jfc success";
    return Status::OK();
}

UrmaJfs::~UrmaJfs()
{
    if (raw_ == nullptr) {
        return;
    }
    std::stringstream oss;
    oss << "delete jfs id " << raw_->jfs_id.id;
    oss << " with index " << index_;
    oss << ", valid: " << (valid_.load() ? "true" : "false") << " ";
    const auto ret = ds_urma_delete_jfs(raw_);
    if (ret == URMA_SUCCESS) {
        oss << "success";
    } else {
        oss << FormatString("failed. ret = %d", ret);
    }
    LOG(INFO) << oss.str();
}

Status UrmaJfs::Create(urma_context_t *context, urma_jfc_t *jfc, uint8_t priority, size_t index,
                       std::shared_ptr<UrmaJfs> &jfs)
{
    urma_jfs_cfg_t jfsConfig;
    jfsConfig.depth = JETTY_SIZE;
    jfsConfig.trans_mode = URMA_TM_RM;
    jfsConfig.priority = priority;
    const auto maxSge = 13;
    jfsConfig.max_sge = maxSge;
    jfsConfig.max_inline_data = 0;
    jfsConfig.rnr_retry = URMA_TYPICAL_RNR_RETRY;
    jfsConfig.err_timeout = URMA_TYPICAL_ERR_TIMEOUT;
    jfsConfig.jfc = jfc;
    jfsConfig.user_ctx = 0;
    jfsConfig.flag.value = 0;
#ifdef URMA_OVER_UB
    jfsConfig.flag.bs.multi_path = 1;
#endif

    urma_jfs_t *raw = ds_urma_create_jfs(context, &jfsConfig);
    if (raw == nullptr) {
        RETURN_STATUS_LOG_ERROR(K_URMA_ERROR, FormatString("Failed to urma create jfs, errno = %d", errno));
    }
    jfs = std::make_shared<UrmaJfs>(raw, index);
    LOG(INFO) << "urma create jfs id " << jfs->GetJfsId() << " with index " << jfs->GetJfsIndex() << " success";
    return Status::OK();
}

Status UrmaJfs::ModifyToError()
{
    urma_jfs_attr_t attr;
    attr.mask = JFS_STATE;
    attr.state = URMA_JETTY_STATE_ERROR;
    auto ret = ds_urma_modify_jfs(raw_, &attr);
    if (ret != URMA_SUCCESS) {
        return Status(K_URMA_ERROR, FormatString("Failed to set jfs error, ret = %d", ret));
    }
    return Status::OK();
}

UrmaJfr::~UrmaJfr()
{
    if (raw_ == nullptr) {
        return;
    }
    std::stringstream oss;
    oss << "delete jfr id " << raw_->jfr_id.id << " ";
    const auto ret = ds_urma_delete_jfr(raw_);
    if (ret == URMA_SUCCESS) {
        oss << "success";
    } else {
        oss << FormatString("failed. ret = %d", ret);
    }
    LOG(INFO) << oss.str();
    raw_ = nullptr;
}

Status UrmaJfr::Create(urma_context_t *context, urma_jfc_t *jfc, urma_token_t urmaToken, std::unique_ptr<UrmaJfr> &jfr)
{
    urma_jfr_cfg_t jfrConfig;
    jfrConfig.depth = JETTY_SIZE;
    jfrConfig.flag.value = 0;
    jfrConfig.flag.bs.tag_matching = URMA_NO_TAG_MATCHING;
    jfrConfig.trans_mode = URMA_TM_RM;
    jfrConfig.min_rnr_timer = URMA_TYPICAL_MIN_RNR_TIMER;
    jfrConfig.jfc = jfc;
    jfrConfig.token_value = urmaToken;
    jfrConfig.id = 0;
    jfrConfig.max_sge = 1;
    jfrConfig.user_ctx = (uint64_t)NULL;

    urma_jfr_t *raw = ds_urma_create_jfr(context, &jfrConfig);
    if (raw == nullptr) {
        RETURN_STATUS_LOG_ERROR(K_URMA_ERROR, FormatString("Failed to urma create jfr, errno = %d", errno));
    }
    jfr = std::make_unique<UrmaJfr>(raw);
    LOG(INFO) << "urma create jfr id " << jfr->Raw()->jfr_id.id << " success";
    return Status::OK();
}

UrmaTargetJfr::~UrmaTargetJfr()
{
    if (raw_ == nullptr) {
        return;
    }
    const auto ret = ds_urma_unimport_jfr(raw_);
    LOG_IF(ERROR, ret != URMA_SUCCESS) << "Failed to unimport jfr, ret = " << ret;
    raw_ = nullptr;
}

Status UrmaTargetJfr::Import(urma_context_t *context, urma_rjfr_t *remoteJfr, urma_token_t urmaToken,
                             std::unique_ptr<UrmaTargetJfr> &tjfr)
{
    PerfPoint point(PerfKey::URMA_IMPORT_JFR);
    auto *rawTjfr = ds_urma_import_jfr(context, remoteJfr, &urmaToken);
    point.Record();
    if (rawTjfr == nullptr) {
        RETURN_STATUS(K_RUNTIME_ERROR, "Failed to import jfr");
    }
    tjfr = std::make_unique<UrmaTargetJfr>(rawTjfr);
    return Status::OK();
}

UrmaLocalSegment::~UrmaLocalSegment()
{
    if (raw_ == nullptr) {
        return;
    }
    const auto ret = ds_urma_unregister_seg(raw_);
    LOG_IF(ERROR, ret != URMA_SUCCESS) << "Failed to unregister segment, ret = " << ret;
    raw_ = nullptr;
}

Status UrmaLocalSegment::Register(urma_context_t *context, uint64_t segAddress, uint64_t segSize,
                                  urma_token_t urmaToken, urma_reg_seg_flag_t registerSegmentFlag,
                                  std::unique_ptr<UrmaLocalSegment> &segment)
{
    urma_seg_cfg_t segmentConfig;
    segmentConfig.va = segAddress;
    segmentConfig.len = segSize;
    segmentConfig.token_value = urmaToken;
    segmentConfig.flag = registerSegmentFlag;
    segmentConfig.user_ctx = (uint64_t)NULL;
    segmentConfig.iova = 0;
    segmentConfig.token_id = nullptr;

    PerfPoint point(PerfKey::URMA_REGISTER_SEGMENT);
    auto *rawSegment = ds_urma_register_seg(context, &segmentConfig);
    point.Record();
    if (rawSegment == nullptr) {
        RETURN_STATUS(K_RUNTIME_ERROR,
                      FormatString("Failed to register segment, address %llu, size %llu.", segAddress, segSize));
    }
    auto tokenId = rawSegment->token_id != nullptr ? rawSegment->token_id->token_id : 0;
    auto tokenId2 = rawSegment->seg.token_id;
    LOG(INFO) << "register segment success, token_id:" << tokenId << ", token_id2:" << tokenId2;
    segment = std::make_unique<UrmaLocalSegment>(rawSegment);
    return Status::OK();
}

UrmaRemoteSegment::~UrmaRemoteSegment()
{
    if (raw_ == nullptr) {
        return;
    }
    const auto ret = ds_urma_unimport_seg(raw_);
    LOG_IF(ERROR, ret != URMA_SUCCESS) << "Failed to unimport segment, ret = " << ret;
    raw_ = nullptr;
}

Status UrmaRemoteSegment::Import(urma_context_t *context, urma_token_t urmaToken,
                                 urma_import_seg_flag_t importSegmentFlag, urma_seg_t &remoteSegment,
                                 std::unique_ptr<UrmaRemoteSegment> &segment)
{
    auto *rawSegment = ds_urma_import_seg(context, &remoteSegment, &urmaToken, 0, importSegmentFlag);
    CHECK_FAIL_RETURN_STATUS(rawSegment != nullptr, K_RUNTIME_ERROR, "Failed to import segment.");
    segment = std::make_unique<UrmaRemoteSegment>(rawSegment);
    return Status::OK();
}

const UrmaJfrInfo &UrmaConnection::GetUrmaJfrInfo() const
{
    return urmaJfrInfo_;
}

std::shared_ptr<UrmaJfs> UrmaConnection::GetJfs() const
{
    std::lock_guard<std::mutex> lock(jfsMutex_);
    return jfs_.lock();
}

void UrmaConnection::SetJfs(std::shared_ptr<UrmaJfs> jfs)
{
    std::lock_guard<std::mutex> lock(jfsMutex_);
    jfs_ = std::move(jfs);
}

urma_target_jetty_t *UrmaConnection::GetTargetJfr() const
{
    return tjfr_ == nullptr ? nullptr : tjfr_->Raw();
}

Status UrmaConnection::GetRemoteSeg(uint64_t segVa, UrmaRemoteSegmentMap::const_accessor &accessor) const
{
    if (!tsegs_.find(accessor, segVa)) {
        RETURN_STATUS(K_NOT_FOUND, FormatString("Remote segment is not found, segment VA: %lu", segVa));
    }
    auto *segment = accessor->second == nullptr ? nullptr : accessor->second->Raw();
    CHECK_FAIL_RETURN_STATUS(segment != nullptr, K_RUNTIME_ERROR,
                             FormatString("Remote segment entry is empty, segment VA: %lu", segVa));
    return Status::OK();
}

Status UrmaConnection::ImportRemoteSeg(const UrmaImportSegmentPb &importSegmentInfo, urma_context_t *context,
                                       urma_token_t urmaToken, urma_import_seg_flag_t importSegmentFlag)
{
    UrmaRemoteSegmentMap::accessor accessor;
    const auto segVa = importSegmentInfo.seg().va();
    if (tsegs_.find(accessor, segVa)) {
        return Status::OK();
    }
    if (!tsegs_.insert(accessor, segVa)) {
        return Status::OK();
    }

    bool needErase = true;
    Raii eraseSegment([this, &accessor, &needErase]() {
        if (needErase) {
            tsegs_.erase(accessor);
        }
    });

    UrmaSeg remoteSegment;
    RETURN_IF_NOT_OK(remoteSegment.FromProto(importSegmentInfo.seg()));
    LOG(INFO) << "import remote seg info: " << remoteSegment.ToString() << ", client_id:" << urmaJfrInfo_.clientId;
    RETURN_IF_NOT_OK(
        UrmaRemoteSegment::Import(context, urmaToken, importSegmentFlag, remoteSegment.raw, accessor->second));
    needErase = false;
    return Status::OK();
}

Status UrmaConnection::UnimportRemoteSeg(uint64_t segmentAddress)
{
    UrmaRemoteSegmentMap::accessor accessor;
    if (!tsegs_.find(accessor, segmentAddress)) {
        RETURN_STATUS(K_NOT_FOUND, "Cannot unimport remote segment, remote segment is not imported");
    }
    tsegs_.erase(accessor);
    return Status::OK();
}

void UrmaConnection::Clear()
{
    {
        std::lock_guard<std::mutex> lock(jfsMutex_);
        jfs_.reset();
    }
    tjfr_.reset();
    tsegs_.clear();
    urmaJfrInfo_ = UrmaJfrInfo();
}

Status UrmaResource::Init(urma_device_t *device, uint32_t eidIndex, uint32_t connectionSize)
{
    Clear();
    CHECK_FAIL_RETURN_STATUS(device != nullptr, K_INVALID, "URMA device is null");
    urma_status_t ret = ds_urma_query_device(device, &urmaDeviceAttribute_);
    if (ret != URMA_SUCCESS) {
        RETURN_STATUS(K_URMA_ERROR, FormatString("Failed to urma query device, ret = %d", ret));
    }
    LOG(INFO) << "urma query device success with dev type:" << device->type;

    uint8_t priority = 0;
    uint32_t sl = 0;
    bool foundPriority = GetJfsPriorityInfoForCTP(priority, sl);
    jfsPriority_ = priority;
    LOG(INFO) << "UrmaResource CTP priority=" << static_cast<uint32_t>(priority) << ", SL=" << sl
              << ", useDefaultPriority=" << !foundPriority;

    RETURN_IF_NOT_OK(UrmaContext::Create(device, eidIndex, context_));
    RETURN_IF_NOT_OK(UrmaJfce::Create(context_->Raw(), jfce_));
    RETURN_IF_NOT_OK(UrmaJfc::Create(context_->Raw(), urmaDeviceAttribute_, jfce_->Raw(), jfc_));
    if (FLAGS_urma_event_mode) {
        RETURN_IF_NOT_OK(jfc_->Rearm());
    }

    auto *context = context_->Raw();
    auto *jfc = jfc_->Raw();
    connectionSize_ = connectionSize;
    jfsLists_.reserve(connectionSize);
    jfrLists_.reserve(connectionSize);
    for (uint32_t i = 0; i < connectionSize; ++i) {
        std::shared_ptr<UrmaJfs> jfs;
        std::unique_ptr<UrmaJfr> jfr;
        RETURN_IF_NOT_OK(UrmaJfs::Create(context, jfc, priority, i, jfs));
        RETURN_IF_NOT_OK(UrmaJfr::Create(context, jfc, urmaToken_, jfr));
        jfsLists_.emplace_back(std::move(jfs));
        jfrLists_.emplace_back(std::move(jfr));
    }
    constexpr uint32_t threadCount = 1;
    deleteJfsThread_ = std::make_unique<ThreadPool>(0, threadCount, "RecreateJfs");
    return Status::OK();
}

urma_context_t *UrmaResource::GetContext() const
{
    return context_ == nullptr ? nullptr : context_->Raw();
}

urma_jfce_t *UrmaResource::GetJfce() const
{
    return jfce_ == nullptr ? nullptr : jfce_->Raw();
}

urma_jfc_t *UrmaResource::GetJfc() const
{
    return jfc_ == nullptr ? nullptr : jfc_->Raw();
}

bool UrmaResource::GetJfsPriorityInfoForCTP(uint8_t &priority, uint32_t &sl) const
{
    constexpr uint8_t defaultPriorityForCTP = 6;
    constexpr uint32_t defaultSLForCTP = 6;
    urma_tp_type_en tpTypeEn;
    tpTypeEn.value = 0;
    tpTypeEn.bs.ctp = 1;

    for (uint32_t i = 0; i <= URMA_MAX_PRIORITY; ++i) {
        auto &priorityInfo = urmaDeviceAttribute_.dev_cap.priority_info[i];
        VLOG(1) << "Checking priority " << i << " with tp_type: " << priorityInfo.tp_type.value
                << " expect tp_type: " << tpTypeEn.value;
        if (priorityInfo.tp_type.value == tpTypeEn.value) {
            priority = i;
            sl = priorityInfo.SL;
            return true;
        }
    }
    // Older URMA versions may not populate priority_info, so fall back
    // to the default priority and SL for CTP.
    priority = defaultPriorityForCTP;
    sl = defaultSLForCTP;
    return false;
}

void UrmaResource::Clear()
{
    {
        std::lock_guard<std::mutex> jfsLock(jfsListMutex_);
        jfsLists_.clear();
    }
    {
        std::lock_guard<std::mutex> pendingDeleteLock(pendingDeleteMutex_);
        pendingDeleteJfs_.clear();
    }
    jfrLists_.clear();
    localJfsIndex_ = 0;
    connectionSize_ = 0;
    jfsPriority_ = 0;
    jfc_.reset();
    jfce_.reset();
    context_.reset();
}

Status UrmaResource::GetNextJfs(std::shared_ptr<UrmaJfs> &jfs)
{
    INJECT_POINT("worker.GetNextJfs", [this](size_t index) {
        localJfsIndex_ = index;
        return Status::OK();
    });
    std::lock_guard<std::mutex> lock(jfsListMutex_);
    CHECK_FAIL_RETURN_STATUS(connectionSize_ > 0, K_RUNTIME_ERROR, "No JFS is available because connectionSize is 0");

    for (size_t i = 0; i < connectionSize_; ++i) {
        const auto index = localJfsIndex_.fetch_add(1) % connectionSize_;
        const auto &candidate = jfsLists_[index];
        if (candidate != nullptr && candidate->IsValid()) {
            jfs = candidate;
            return Status::OK();
        }
    }

    RETURN_STATUS(K_RUNTIME_ERROR, "No valid JFS is available");
}

UrmaJfr *UrmaResource::GetJfr(size_t index) const
{
    if (index >= jfrLists_.size() || jfrLists_[index] == nullptr) {
        return nullptr;
    }
    return jfrLists_[index].get();
}

Status UrmaResource::AsyncModifyJfsToError(std::shared_ptr<UrmaJfs> jfs)
{
    CHECK_FAIL_RETURN_STATUS(jfs != nullptr, K_RUNTIME_ERROR, "Failed to modify JFS to error because JFS is null");
    const auto index = jfs->GetJfsIndex();
    const auto jfsId = jfs->GetJfsId();
    CHECK_FAIL_RETURN_STATUS(index < connectionSize_, K_RUNTIME_ERROR,
                             FormatString("Index out of range for marking JFS to recreate: %u", index));
    {
        std::lock_guard<std::mutex> lock(jfsListMutex_);
        CHECK_FAIL_RETURN_STATUS(jfsLists_[index] == jfs, K_RUNTIME_ERROR,
                                 FormatString("Skip modifying stale JFS at index %u with id %u", index, jfsId));
    }
    if (!jfs->MarkInvalid()) {
        LOG(INFO) << "JFS at index " << index << " with id " << jfsId << " is already invalid";
        return Status::OK();
    }
    LOG(INFO) << "Mark JFS id " << jfsId << " at index " << index << " to invalid";
    auto traceId = Trace::Instance().GetTraceID();
    deleteJfsThread_->Execute([this, jfs, traceId]() {
        auto traceGuard = Trace::Instance().SetTraceNewID(traceId);
        LOG_IF_ERROR(RecreateJfsAfterError(jfs), "RecreateJfsAfterError failed");
    });
    return Status::OK();
}

Status UrmaResource::RecreateJfsAfterError(const std::shared_ptr<UrmaJfs> &jfs)
{
    CHECK_FAIL_RETURN_STATUS(jfs != nullptr, K_RUNTIME_ERROR, "JFS is null when recreating after error");

    const auto index = jfs->GetJfsIndex();
    const auto jfsId = jfs->GetJfsId();
    LOG(INFO) << "Try modify jfs id " << jfsId << " at index " << index << " to state URMA_JETTY_STATE_ERROR";

    auto rc = jfs->ModifyToError();
    CHECK_FAIL_RETURN_STATUS(rc.IsOk(), K_RUNTIME_ERROR,
                             FormatString("Failed to modify jfs index %u with id %u to error state: %s", index, jfsId,
                                          rc.ToString().c_str()));
    auto traceId = Trace::Instance().GetTraceID();
    {
        std::lock_guard<std::mutex> pendingDeleteLock(pendingDeleteMutex_);
        pendingDeleteJfs_[jfsId] = { jfs, traceId };
    }

    CHECK_FAIL_RETURN_STATUS(context_ != nullptr, K_RUNTIME_ERROR, "URMA context is null when recreating jfs");
    CHECK_FAIL_RETURN_STATUS(jfc_ != nullptr, K_RUNTIME_ERROR, "URMA jfc is null when recreating jfs");
    std::shared_ptr<UrmaJfs> newJfs;
    RETURN_IF_NOT_OK(UrmaJfs::Create(context_->Raw(), jfc_->Raw(), jfsPriority_, index, newJfs));
    auto newJfsId = newJfs->GetJfsId();
    {
        std::lock_guard<std::mutex> jfsLock(jfsListMutex_);
        CHECK_FAIL_RETURN_STATUS(jfsLists_[index] == jfs, K_RUNTIME_ERROR,
                                 FormatString("Skip replacing stale JFS at index %u with id %u", index, jfsId));
        jfsLists_[index] = std::move(newJfs);
    }
    LOG(INFO) << "Recreated jfs at index " << index << ", old id " << jfsId << ", new id " << newJfsId;
    return Status::OK();
}

void UrmaResource::AsyncDeleteJfs(uint32_t jfsId)
{
    deleteJfsThread_->Submit([this, jfsId]() {
        std::shared_ptr<UrmaJfs> jfs;
        std::string traceId;
        {
            std::lock_guard<std::mutex> lock(pendingDeleteMutex_);
            auto iter = pendingDeleteJfs_.find(jfsId);
            if (iter == pendingDeleteJfs_.end()) {
                LOG(WARNING) << "JFS with id " << jfsId << " does not exist in pendingDeleteJfs_";
                return;
            }
            jfs = std::move(iter->second.jfs);
            traceId = std::move(iter->second.traceId);
            pendingDeleteJfs_.erase(iter);
        }

        auto traceGuard = Trace::Instance().SetTraceNewID(traceId);
        // Reset outside the critical section so the UrmaJfs destructor runs after the mutex is released.
        LOG(INFO) << "Remove JFS with id " << jfsId << " from pendingDeleteJfs_ ";
        jfs.reset();
    });
}

}  // namespace datasystem
