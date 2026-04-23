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

#include "securec.h"

#include <ub/umdk/urma/urma_opcode.h>

#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/log/trace.h"
#include "datasystem/common/os_transport_pipeline/os_transport_pipeline_worker_api.h"
#include "datasystem/common/perf/perf_manager.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/rdma/urma_dlopen_util.h"
#include "datasystem/utils/status.h"

DS_DECLARE_bool(urma_event_mode);

namespace datasystem {
namespace {
constexpr uint32_t K_URMA_WARNING_LOG_EVERY_N = 100;
}

std::atomic<uint32_t> UrmaJfs::counter_{ 0 };
std::atomic<uint32_t> UrmaJfr::counter_{ 0 };

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

Status UrmaContext::ChangeBondingBalanceMode() const
{
#ifdef BONDP_USER_CTL_BONDING
    LOG(INFO) << "Try change binding mode balance";
    CHECK_FAIL_RETURN_STATUS(raw_ != nullptr, K_INVALID, "URMA context is null");

    bondp_set_bonding_mode_in_t mode{ .bonding_mode = BONDP_BONDING_MODE_BALANCE,
                                      .bonding_level = BONDP_BONDING_LEVEL_PORT };
    urma_user_ctl_in_t in{ .addr = reinterpret_cast<uint64_t>(&mode),
                           .len = sizeof(mode),
                           .opcode = BONDP_USER_CTL_SET_BONDING_MODE };
    urma_user_ctl_out_t out;
    (void)memset_s(&out, sizeof(out), 0, sizeof(out));

    const auto ret = ds_urma_user_ctl(raw_, &in, &out);
    if (ret != URMA_SUCCESS) {
        RETURN_STATUS(K_URMA_ERROR, FormatString("Failed to set bonding balance mode, ret = %d", ret));
    }
#endif
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

    urma_jfc_cfg_t jfcConfig{};
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

std::weak_ptr<UrmaConnection> UrmaEvent::GetConnection() const
{
    auto jfs = jfs_.lock();
    if (jfs) {
        return jfs->GetConnection();
    }
    return {};
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
    if (resource_ != nullptr) {
        resource_->UnregisterJfs(GetJfsId(), this);
    }
    if (raw_ == nullptr) {
        return;
    }
    std::stringstream oss;
    oss << "delete jfs id " << raw_->jfs_id.id;
    oss << ", valid: " << (valid_.load() ? "true" : "false") << " ";
    const auto ret = ds_urma_delete_jfs(raw_);
    if (ret == URMA_SUCCESS) {
        oss << "success";
    } else {
        oss << FormatString("failed. ret = %d", ret);
    }
    counter_.fetch_sub(1);
    oss << ". jfs count: " << counter_.load();
    LOG(INFO) << oss.str();
}

Status UrmaJfs::Create(UrmaResource &resource, std::shared_ptr<UrmaJfs> &jfs)
{
    urma_jfs_cfg_t jfsConfig{};
    jfsConfig.depth = JETTY_SIZE;
    jfsConfig.trans_mode = URMA_TM_RM;
    jfsConfig.priority = resource.GetJfsPriority();
    const auto maxSge = 13;
    jfsConfig.max_sge = maxSge;
    jfsConfig.max_inline_data = 0;
    jfsConfig.rnr_retry = URMA_TYPICAL_RNR_RETRY;
    jfsConfig.err_timeout = URMA_TYPICAL_ERR_TIMEOUT;
    jfsConfig.jfc = resource.GetJfc();
    jfsConfig.user_ctx = 0;
    jfsConfig.flag.value = 0;
#ifdef URMA_OVER_UB
    jfsConfig.flag.bs.multi_path = 1;
#endif

    urma_jfs_t *raw = ds_urma_create_jfs(resource.GetContext(), &jfsConfig);
    if (raw == nullptr) {
        RETURN_STATUS_LOG_ERROR(K_URMA_ERROR, FormatString("Failed to urma create jfs, errno = %d", errno));
    }
    jfs = std::make_shared<UrmaJfs>(raw, &resource);
    LOG(INFO) << "urma create jfs id " << jfs->GetJfsId() << " success. jfs count: " << counter_.load();
    return Status::OK();
}

Status UrmaJfs::ModifyToError()
{
    urma_jfs_attr_t attr{};
    attr.mask = JFS_STATE;
    attr.state = URMA_JETTY_STATE_ERROR;
    auto ret = ds_urma_modify_jfs(raw_, &attr);
    if (ret != URMA_SUCCESS) {
        return Status(K_URMA_ERROR, FormatString("Failed to set jfs error, ret = %d", ret));
    }
    return Status::OK();
}

void UrmaJfs::BindConnection(const std::shared_ptr<UrmaConnection> &connection)
{
    std::lock_guard<std::mutex> lock(connectionMutex_);
    connection_ = connection;
}

std::weak_ptr<UrmaConnection> UrmaJfs::GetConnection() const
{
    std::lock_guard<std::mutex> lock(connectionMutex_);
    return connection_;
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
    counter_.fetch_sub(1);
    oss << ". jfr count: " << counter_.load();
    LOG(INFO) << oss.str();
    raw_ = nullptr;
}

Status UrmaJfr::Create(const UrmaResource &resource, std::unique_ptr<UrmaJfr> &jfr)
{
    urma_jfr_cfg_t jfrConfig{};
    jfrConfig.depth = JETTY_SIZE;
    jfrConfig.flag.value = 0;
    jfrConfig.flag.bs.tag_matching = URMA_NO_TAG_MATCHING;
    jfrConfig.trans_mode = URMA_TM_RM;
    jfrConfig.min_rnr_timer = URMA_TYPICAL_MIN_RNR_TIMER;
    jfrConfig.jfc = resource.GetJfc();
    jfrConfig.token_value = resource.GetUrmaToken();
    jfrConfig.id = 0;
    jfrConfig.max_sge = 1;
    jfrConfig.user_ctx = (uint64_t)NULL;

    urma_jfr_t *raw = ds_urma_create_jfr(resource.GetContext(), &jfrConfig);
    if (raw == nullptr) {
        RETURN_STATUS_LOG_ERROR(K_URMA_ERROR, FormatString("Failed to urma create jfr, errno = %d", errno));
    }
    jfr = std::make_unique<UrmaJfr>(raw);
    LOG(INFO) << "urma create jfr id " << jfr->Raw()->jfr_id.id << " success. jfr count: " << counter_.load();
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
    INJECT_POINT("urma.import_jfr");
    PerfPoint point(PerfKey::URMA_IMPORT_JFR);
    auto *rawTjfr = ds_urma_import_jfr(context, remoteJfr, &urmaToken);
    point.Record();
    if (rawTjfr == nullptr) {
        RETURN_STATUS(K_URMA_CONNECT_FAILED, "Failed to import jfr");
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
    urma_seg_cfg_t segmentConfig{};
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
        RETURN_STATUS(K_URMA_ERROR,
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
    CHECK_FAIL_RETURN_STATUS(rawSegment != nullptr, K_URMA_ERROR, "Failed to import segment.");
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
    return jfs_;
}

Status UrmaConnection::ReCreateJfs(UrmaResource &resource, const std::shared_ptr<UrmaJfs> &failedJfs)
{
    if (failedJfs == nullptr) {
        LOG_FIRST_AND_EVERY_N(WARNING, K_URMA_WARNING_LOG_EVERY_N)
            << "[URMA_RECREATE_JFS_SKIP] failedJfs is null, remoteAddress=" << urmaJfrInfo_.localAddress.ToString()
            << ", remoteInstanceId=" << urmaJfrInfo_.uniqueInstanceId;
        return Status::OK();
    }
    // failedJfs must come from the UrmaEvent that reported the failure.
    // connection->jfs_ may already have been swapped to a newly recreated JFS
    // by another thread. If we read jfs_ here and call MarkInvalid() on it,
    // we may incorrectly invalidate the new healthy JFS instead of the failed one.
    {
        std::lock_guard<std::mutex> lock(jfsMutex_);
        // Keep MarkInvalid() and the recreate decision under jfsMutex_.
        // The same JFS may concurrently receive multiple CQE error-9 events:
        // only one thread is allowed to mark it invalid and recreate jfs_,
        // while other threads must wait here and observe the recreated state.
        if (!failedJfs->MarkInvalid()) {
            LOG_FIRST_AND_EVERY_N(WARNING, K_URMA_WARNING_LOG_EVERY_N)
                << "[URMA_RECREATE_JFS_SKIP] JFS " << failedJfs->GetJfsId()
                << " is already invalid, remoteAddress=" << urmaJfrInfo_.localAddress.ToString()
                << ", remoteInstanceId=" << urmaJfrInfo_.uniqueInstanceId;
            return Status::OK();
        }
        LOG_FIRST_AND_EVERY_N(WARNING, K_URMA_WARNING_LOG_EVERY_N)
            << "[URMA_RECREATE_JFS] Mark JFS " << failedJfs->GetJfsId()
            << " invalid and recreate, remoteAddress=" << urmaJfrInfo_.localAddress.ToString()
            << ", remoteInstanceId=" << urmaJfrInfo_.uniqueInstanceId;
        CHECK_FAIL_RETURN_STATUS(jfs_ != nullptr, K_RUNTIME_ERROR, "JFS already cleared for connection");
        if (jfs_.get() != failedJfs.get()) {
            LOG_FIRST_AND_EVERY_N(WARNING, K_URMA_WARNING_LOG_EVERY_N)
                << "[URMA_RECREATE_JFS_SKIP] JFS " << failedJfs->GetJfsId()
                << " is invalid but connection points to another JFS, remoteAddress="
                << urmaJfrInfo_.localAddress.ToString() << ", remoteInstanceId=" << urmaJfrInfo_.uniqueInstanceId;
            return Status::OK();
        }
        std::shared_ptr<UrmaJfs> newJfs;
        RETURN_IF_NOT_OK_APPEND_MSG(resource.CreateJfs(newJfs), "Failed to recreate JFS");
        newJfs->BindConnection(shared_from_this());
        jfs_ = std::move(newJfs);
        LOG_FIRST_AND_EVERY_N(WARNING, K_URMA_WARNING_LOG_EVERY_N)
            << "[URMA_RECREATE_JFS] connection switched to newJfsId=" << jfs_->GetJfsId()
            << ", remoteAddress=" << urmaJfrInfo_.localAddress.ToString()
            << ", remoteInstanceId=" << urmaJfrInfo_.uniqueInstanceId;
    }
    return resource.AsyncModifyJfsToError(failedJfs);
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

Status UrmaResource::Init(urma_device_t *device, uint32_t eidIndex, bool isBondingDevice)
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
    if (isBondingDevice) {
        LOG_IF_ERROR(context_->ChangeBondingBalanceMode(), "Failed to change bonding balance mode");
    }
    RETURN_IF_NOT_OK(UrmaJfce::Create(context_->Raw(), jfce_));
    RETURN_IF_NOT_OK(UrmaJfc::Create(context_->Raw(), urmaDeviceAttribute_, jfce_->Raw(), jfc_));
    if (FLAGS_urma_event_mode) {
        RETURN_IF_NOT_OK(jfc_->Rearm());
    }

    constexpr uint32_t threadCount = 1;
    deleteJfsThread_ = std::make_unique<ThreadPool>(0, threadCount, "RetireJfs");
    RETURN_IF_NOT_OK(OsXprtPipln::InitOsPiplnH2DEnv(context_->Raw(), jfc_->Raw(), jfce_->Raw()));
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
    OsXprtPipln::UnInitOsPiplnH2DEnv();
    {
        std::lock_guard<std::mutex> lock(jfsRegistryMutex_);
        jfsRegistry_.clear();
    }
    {
        std::lock_guard<std::mutex> pendingDeleteLock(pendingDeleteMutex_);
        pendingDeleteJfs_.clear();
    }
    jfsPriority_ = 0;
    jfc_.reset();
    jfce_.reset();
    context_.reset();
}

Status UrmaResource::CreateJfs(std::shared_ptr<UrmaJfs> &jfs)
{
    CHECK_FAIL_RETURN_STATUS(context_ != nullptr, K_RUNTIME_ERROR, "URMA context is null when creating JFS");
    CHECK_FAIL_RETURN_STATUS(jfc_ != nullptr, K_RUNTIME_ERROR, "URMA jfc is null when creating JFS");
    RETURN_IF_NOT_OK(UrmaJfs::Create(*this, jfs));
    RETURN_IF_NOT_OK(RegisterJfs(jfs));
    return Status::OK();
}

Status UrmaResource::CreateJfr(std::unique_ptr<UrmaJfr> &jfr)
{
    CHECK_FAIL_RETURN_STATUS(context_ != nullptr, K_RUNTIME_ERROR, "URMA context is null when creating JFR");
    CHECK_FAIL_RETURN_STATUS(jfc_ != nullptr, K_RUNTIME_ERROR, "URMA jfc is null when creating JFR");
    return UrmaJfr::Create(*this, jfr);
}

Status UrmaResource::AsyncModifyJfsToError(std::shared_ptr<UrmaJfs> jfs)
{
    CHECK_FAIL_RETURN_STATUS(jfs != nullptr, K_RUNTIME_ERROR, "Failed to modify JFS to error because JFS is null");
    auto traceId = Trace::Instance().GetTraceID();
    deleteJfsThread_->Execute([this, jfs, traceId]() {
        auto traceGuard = Trace::Instance().SetTraceNewID(traceId);
        LOG_IF_ERROR(RetireJfsToError(jfs), "RetireJfsToError failed");
    });
    return Status::OK();
}

Status UrmaResource::RetireJfsToError(const std::shared_ptr<UrmaJfs> &jfs)
{
    CHECK_FAIL_RETURN_STATUS(jfs != nullptr, K_RUNTIME_ERROR, "JFS is null when retiring to error");

    const auto jfsId = jfs->GetJfsId();
    LOG(INFO) << "Try modify jfs id " << jfsId << " to state URMA_JETTY_STATE_ERROR";

    RETURN_IF_NOT_OK_APPEND_MSG(jfs->ModifyToError(),
                                FormatString("Failed to modify jfs with id %u to error state", jfsId));
    auto traceId = Trace::Instance().GetTraceID();
    {
        std::lock_guard<std::mutex> pendingDeleteLock(pendingDeleteMutex_);
        pendingDeleteJfs_[jfsId] = { jfs, traceId };
    }
    LOG(INFO) << "Retired jfs id " << jfsId << " to pending delete";
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

Status UrmaResource::RegisterJfs(const std::shared_ptr<UrmaJfs> &jfs)
{
    CHECK_FAIL_RETURN_STATUS(jfs != nullptr, K_RUNTIME_ERROR, "Cannot register null JFS");
    const auto jfsId = jfs->GetJfsId();
    std::lock_guard<std::mutex> lock(jfsRegistryMutex_);
    jfsRegistry_[jfsId] = jfs;
    LOG(INFO) << "[UrmaResource] Registered JFS " << jfsId << " in registry";
    return Status::OK();
}

void UrmaResource::UnregisterJfs(uint32_t jfsId, const UrmaJfs *expected)
{
    std::lock_guard<std::mutex> lock(jfsRegistryMutex_);
    auto it = jfsRegistry_.find(jfsId);
    if (it == jfsRegistry_.end()) {
        return;
    }
    if (expected != nullptr) {
        auto locked = it->second.lock();
        if (locked && locked.get() != expected) {
            return;  // Different JFS object registered under the same id; do not remove.
        }
    }
    jfsRegistry_.erase(it);
    LOG(INFO) << "[UrmaResource] Unregistered JFS " << jfsId << " from registry";
}

Status UrmaResource::GetJfsById(uint32_t jfsId, std::shared_ptr<UrmaJfs> &jfs)
{
    std::lock_guard<std::mutex> lock(jfsRegistryMutex_);
    auto it = jfsRegistry_.find(jfsId);
    if (it == jfsRegistry_.end()) {
        RETURN_STATUS(K_NOT_FOUND, FormatString("JFS %u not found in registry", jfsId));
    }
    jfs = it->second.lock();
    if (jfs == nullptr) {
        jfsRegistry_.erase(it);
        RETURN_STATUS(K_NOT_FOUND, FormatString("JFS %u expired in registry", jfsId));
    }
    return Status::OK();
}

Status UrmaResource::GetAnyValidJfs(std::shared_ptr<UrmaJfs> &jfs)
{
    std::lock_guard<std::mutex> lock(jfsRegistryMutex_);
    for (auto it = jfsRegistry_.begin(); it != jfsRegistry_.end();) {
        jfs = it->second.lock();
        if (jfs == nullptr) {
            it = jfsRegistry_.erase(it);
            continue;
        }
        if (jfs->IsValid()) {
            return Status::OK();
        }
        ++it;
    }
    RETURN_STATUS(K_NOT_FOUND, "No valid JFS found in registry");
}

}  // namespace datasystem
