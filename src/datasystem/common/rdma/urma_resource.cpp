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
#include <ub/umdk/urma/urma_ubagg.h>

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
constexpr const char *URMA_ERROR_SUGGEST = "check URMA";
}  // namespace

std::atomic<uint32_t> UrmaJfr::counter_{ 0 };
std::atomic<uint32_t> UrmaJetty::counter_{ 0 };

const uint32_t JETTY_SIZE = 256;
const uint32_t SHARED_JFR_DEPTH = 32;
const uint32_t RECV_JETTY_JFS_DEPTH = 32;

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
    auto jetty = jetty_.lock();
    if (jetty) {
        return jetty->GetConnection();
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

Status UrmaJfr::Create(const UrmaResource &resource, uint32_t depth, std::shared_ptr<UrmaJfr> &jfr)
{
    urma_jfr_cfg_t jfrConfig{};
    jfrConfig.depth = depth;
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
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
        raw != nullptr, K_URMA_ERROR,
        FormatString("[URMA_JFR]: call urma_create_jfr failed, errno = %d, suggest: %s", errno, URMA_ERROR_SUGGEST));
    jfr = std::make_shared<UrmaJfr>(raw);
    LOG(INFO) << "urma create jfr id " << jfr->Raw()->jfr_id.id << " success. jfr count: " << counter_.load();
    return Status::OK();
}

UrmaJetty::~UrmaJetty()
{
    if (resource_ != nullptr && raw_ != nullptr) {
        resource_->UnregisterJetty(GetJettyId(), this);
    }
    if (raw_ == nullptr) {
        return;
    }
    std::stringstream oss;
    oss << "delete jetty id " << raw_->jetty_id.id;
    oss << ", valid: " << (valid_.load() ? "true" : "false") << " ";
    const auto ret = ds_urma_delete_jetty(raw_);
    if (ret == URMA_SUCCESS) {
        oss << "success";
    } else {
        oss << FormatString("failed. ret = %d", ret);
    }
    counter_.fetch_sub(1);
    oss << ". jetty count: " << counter_.load();
    LOG(INFO) << oss.str();
    raw_ = nullptr;
    sharedJfr_.reset();
}

Status UrmaJetty::Create(UrmaResource &resource, JettyType jettyType, std::shared_ptr<UrmaJetty> &jetty)
{
    const bool isSendJetty = (jettyType == JettyType::SEND);
    std::shared_ptr<UrmaJfr> sharedJfr;
    if (isSendJetty) {
        RETURN_IF_NOT_OK_APPEND_MSG(resource.GetOrCreateSharedJettyJfr(sharedJfr),
                                    "Failed to get context-level shared JFR for Jetty");
    } else {
        RETURN_IF_NOT_OK_APPEND_MSG(UrmaJfr::Create(resource, JETTY_SIZE, sharedJfr),
                                    "Failed to create dedicated JFR for recv Jetty");
    }

    urma_jfs_cfg_t jfsConfig{};
    jfsConfig.depth = isSendJetty ? JETTY_SIZE : RECV_JETTY_JFS_DEPTH;
    jfsConfig.trans_mode = URMA_TM_RM;
    jfsConfig.priority = resource.GetJfsPriority();
    const auto maxSge = 13;
    jfsConfig.max_sge = maxSge;
    jfsConfig.max_inline_data = 0;
    jfsConfig.rnr_retry = URMA_TYPICAL_RNR_RETRY;
    jfsConfig.err_timeout = 0;
    jfsConfig.jfc = resource.GetJfc();
    jfsConfig.user_ctx = 0;
    jfsConfig.flag.value = 0;
#ifdef URMA_OVER_UB
    jfsConfig.flag.bs.multi_path = 1;
#endif

    urma_jetty_cfg_t jettyConfig{};
    jettyConfig.flag.value = 0;
    jettyConfig.flag.bs.share_jfr = URMA_SHARE_JFR;
    jettyConfig.jfs_cfg = jfsConfig;
    jettyConfig.shared.jfr = sharedJfr->Raw();
    jettyConfig.shared.jfc = resource.GetJfc();
    jettyConfig.user_ctx = 0;

    urma_jetty_t *raw = ds_urma_create_jetty(resource.GetContext(), &jettyConfig);
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(raw != nullptr, K_URMA_ERROR,
                                         FormatString("[URMA_JETTY]: call urma_create_jetty failed, errno = %d, "
                                                      "suggest: %s",
                                                      errno, URMA_ERROR_SUGGEST));
    jetty = std::make_shared<UrmaJetty>(raw, sharedJfr, &resource);
    LOG(INFO) << "urma create jetty id " << jetty->GetJettyId() << " success. jetty count: " << counter_.load();
    return Status::OK();
}

Status UrmaJetty::ModifyToError()
{
    urma_jetty_attr_t attr{};
    attr.mask = JETTY_STATE;
    attr.state = URMA_JETTY_STATE_ERROR;
    auto ret = ds_urma_modify_jetty(raw_, &attr);
    if (ret != URMA_SUCCESS) {
        return Status(K_URMA_ERROR, FormatString("Failed to set jetty error, ret = %d", ret));
    }
    return Status::OK();
}

void UrmaJetty::BindConnection(const std::shared_ptr<UrmaConnection> &connection)
{
    std::lock_guard<std::mutex> lock(connectionMutex_);
    connection_ = connection;
}

std::weak_ptr<UrmaConnection> UrmaJetty::GetConnection() const
{
    std::lock_guard<std::mutex> lock(connectionMutex_);
    return connection_;
}

UrmaTargetJetty::~UrmaTargetJetty()
{
    if (raw_ == nullptr) {
        return;
    }
    const auto ret = ds_urma_unimport_jetty(raw_);
    LOG_IF(ERROR, ret != URMA_SUCCESS) << "Failed to unimport jetty, ret = " << ret;
    raw_ = nullptr;
}

Status UrmaTargetJetty::Import(urma_context_t *context, urma_rjetty_t *remoteJetty, urma_token_t urmaToken,
                               std::unique_ptr<UrmaTargetJetty> &tjetty)
{
    INJECT_POINT("urma.import_jetty");
    PerfPoint point(PerfKey::URMA_IMPORT_JFR);
    auto *rawTjetty = ds_urma_import_jetty(context, remoteJetty, &urmaToken);
    point.Record();
    CHECK_FAIL_RETURN_STATUS(rawTjetty != nullptr, K_URMA_CONNECT_FAILED,
                             FormatString("[URMA_CONNECT]: call urma_import_jetty failed, errno: %d, suggest: %s",
                                          errno, URMA_ERROR_SUGGEST));
    tjetty = std::make_unique<UrmaTargetJetty>(rawTjetty);
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
    CHECK_FAIL_RETURN_STATUS(
        rawSegment != nullptr, K_URMA_ERROR,
        FormatString("[URMA_CONNECT]: call urma_import_seg failed, errno: %d, suggest: %s", errno, URMA_ERROR_SUGGEST));
    segment = std::make_unique<UrmaRemoteSegment>(rawSegment);
    return Status::OK();
}

const UrmaJfrInfo &UrmaConnection::GetUrmaJfrInfo() const
{
    return urmaJfrInfo_;
}

UrmaConnection::~UrmaConnection()
{
    Clear();
}

std::shared_ptr<UrmaJetty> UrmaConnection::GetJetty() const
{
    std::lock_guard<std::mutex> lock(jettyMutex_);
    return jetty_;
}

Status UrmaConnection::ReCreateJetty(UrmaResource &resource, const std::shared_ptr<UrmaJetty> &failedJetty)
{
    if (failedJetty == nullptr) {
        LOG_FIRST_AND_EVERY_N(WARNING, K_URMA_WARNING_LOG_EVERY_N)
            << "[URMA_RECREATE_JETTY_SKIP] failedJetty is null, remoteAddress=" << urmaJfrInfo_.localAddress.ToString()
            << ", remoteInstanceId=" << urmaJfrInfo_.uniqueInstanceId;
        return Status::OK();
    }
    // failedJetty must come from the UrmaEvent that reported the failure.
    // connection->jetty_ may already have been swapped to a newly recreated Jetty
    // by another thread. If we read jetty_ here and call MarkInvalid() on it,
    // we may incorrectly invalidate the new healthy Jetty instead of the failed one.
    {
        std::lock_guard<std::mutex> lock(jettyMutex_);
        // Keep MarkInvalid() and the recreate decision under jettyMutex_.
        // The same Jetty may concurrently receive multiple CQE error-9 events:
        // only one thread is allowed to mark it invalid and recreate jetty_,
        // while other threads must wait here and observe the recreated state.
        if (!failedJetty->MarkInvalid()) {
            LOG_FIRST_AND_EVERY_N(WARNING, K_URMA_WARNING_LOG_EVERY_N)
                << "[URMA_RECREATE_JETTY_SKIP] Jetty " << failedJetty->GetJettyId()
                << " is already invalid, remoteAddress=" << urmaJfrInfo_.localAddress.ToString()
                << ", remoteInstanceId=" << urmaJfrInfo_.uniqueInstanceId;
            return Status::OK();
        }
        LOG_FIRST_AND_EVERY_N(WARNING, K_URMA_WARNING_LOG_EVERY_N)
            << "[URMA_RECREATE_JETTY] Mark Jetty " << failedJetty->GetJettyId()
            << " invalid and recreate, remoteAddress=" << urmaJfrInfo_.localAddress.ToString()
            << ", remoteInstanceId=" << urmaJfrInfo_.uniqueInstanceId;
        CHECK_FAIL_RETURN_STATUS(jetty_ != nullptr, K_RUNTIME_ERROR, "Jetty already cleared for connection");
        if (jetty_.get() != failedJetty.get()) {
            LOG_FIRST_AND_EVERY_N(WARNING, K_URMA_WARNING_LOG_EVERY_N)
                << "[URMA_RECREATE_JETTY_SKIP] Jetty " << failedJetty->GetJettyId()
                << " is invalid but connection points to another Jetty, remoteAddress="
                << urmaJfrInfo_.localAddress.ToString() << ", remoteInstanceId=" << urmaJfrInfo_.uniqueInstanceId;
            return Status::OK();
        }
        std::shared_ptr<UrmaJetty> newJetty;
        RETURN_IF_NOT_OK_APPEND_MSG(resource.CreateJetty(newJetty), "Failed to recreate Jetty");
        newJetty->BindConnection(shared_from_this());
        jetty_ = std::move(newJetty);
        LOG_FIRST_AND_EVERY_N(WARNING, K_URMA_WARNING_LOG_EVERY_N)
            << "[URMA_RECREATE_JETTY] connection switched to newJettyId=" << jetty_->GetJettyId()
            << ", remoteAddress=" << urmaJfrInfo_.localAddress.ToString()
            << ", remoteInstanceId=" << urmaJfrInfo_.uniqueInstanceId;
    }
    return resource.AsyncModifyJettyToError(failedJetty);
}

Status UrmaConnection::ModifyJettyToError(UrmaResource &resource)
{
    std::shared_ptr<UrmaJetty> jetty;
    {
        std::lock_guard<std::mutex> lock(jettyMutex_);
        CHECK_FAIL_RETURN_STATUS(jetty_ != nullptr, K_RUNTIME_ERROR, "Jetty already cleared for connection");
        jetty = jetty_;
        if (!jetty->MarkInvalid()) {
            LOG_FIRST_AND_EVERY_N(WARNING, K_URMA_WARNING_LOG_EVERY_N)
                << "[URMA_MODIFY_JETTY_TO_ERROR_SKIP] Jetty " << jetty->GetJettyId()
                << " is already invalid, remoteAddress=" << urmaJfrInfo_.localAddress.ToString()
                << ", remoteInstanceId=" << urmaJfrInfo_.uniqueInstanceId;
            return Status::OK();
        }
        LOG_FIRST_AND_EVERY_N(WARNING, K_URMA_WARNING_LOG_EVERY_N)
            << "[URMA_MODIFY_JETTY_TO_ERROR] Mark Jetty " << jetty->GetJettyId()
            << " invalid, remoteAddress=" << urmaJfrInfo_.localAddress.ToString()
            << ", remoteInstanceId=" << urmaJfrInfo_.uniqueInstanceId;
    }
    return resource.AsyncModifyJettyToError(jetty);
}

urma_target_jetty_t *UrmaConnection::GetTargetJetty() const
{
    return tjetty_ == nullptr ? nullptr : tjetty_->Raw();
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
        std::lock_guard<std::mutex> lock(jettyMutex_);
        jetty_.reset();
    }
    tjetty_.reset();
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
    bool foundPriority = GetJettyPriorityInfoForCTP(priority, sl);
    jettyPriority_ = priority;
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
    deleteJettyThread_ = std::make_unique<ThreadPool>(0, threadCount, "RetireJetty");
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

bool UrmaResource::GetJettyPriorityInfoForCTP(uint8_t &priority, uint32_t &sl) const
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
        std::lock_guard<std::mutex> lock(jettyRegistryMutex_);
        jettyRegistry_.clear();
    }
    {
        std::lock_guard<std::mutex> lock(sharedJettyJfrMutex_);
        sharedJettyJfr_.reset();
    }
    {
        std::lock_guard<std::mutex> pendingDeleteLock(pendingDeleteMutex_);
        pendingDeleteJettys_.clear();
    }
    jettyPriority_ = 0;
    jfc_.reset();
    jfce_.reset();
    context_.reset();
}

Status UrmaResource::CreateJetty(std::shared_ptr<UrmaJetty> &jetty, JettyType jettyType)
{
    CHECK_FAIL_RETURN_STATUS(context_ != nullptr, K_RUNTIME_ERROR, "URMA context is null when creating Jetty");
    CHECK_FAIL_RETURN_STATUS(jfc_ != nullptr, K_RUNTIME_ERROR, "URMA jfc is null when creating Jetty");
    RETURN_IF_NOT_OK(UrmaJetty::Create(*this, jettyType, jetty));
    RETURN_IF_NOT_OK(RegisterJetty(jetty));
    return Status::OK();
}

Status UrmaResource::GetOrCreateSharedJettyJfr(std::shared_ptr<UrmaJfr> &jfr)
{
    CHECK_FAIL_RETURN_STATUS(context_ != nullptr, K_RUNTIME_ERROR,
                             "URMA context is null when creating shared Jetty JFR");
    CHECK_FAIL_RETURN_STATUS(jfc_ != nullptr, K_RUNTIME_ERROR, "URMA jfc is null when creating shared Jetty JFR");
    std::lock_guard<std::mutex> lock(sharedJettyJfrMutex_);
    if (sharedJettyJfr_ == nullptr) {
        RETURN_IF_NOT_OK_APPEND_MSG(UrmaJfr::Create(*this, SHARED_JFR_DEPTH, sharedJettyJfr_),
                                    "Failed to create context-level shared Jetty JFR");
        LOG(INFO) << "Created context-level shared Jetty JFR with depth " << SHARED_JFR_DEPTH << ", jfr id "
                  << sharedJettyJfr_->Raw()->jfr_id.id;
    }
    jfr = sharedJettyJfr_;
    return Status::OK();
}

Status UrmaResource::AsyncModifyJettyToError(std::shared_ptr<UrmaJetty> jetty)
{
    CHECK_FAIL_RETURN_STATUS(jetty != nullptr, K_RUNTIME_ERROR,
                             "Failed to modify Jetty to error because Jetty is null");
    auto traceId = Trace::Instance().GetTraceID();
    deleteJettyThread_->Execute([this, jetty, traceId]() {
        auto traceGuard = Trace::Instance().SetTraceNewID(traceId);
        LOG_IF_ERROR(RetireJettyToError(jetty), "RetireJettyToError failed");
    });
    return Status::OK();
}

Status UrmaResource::RetireJettyToError(const std::shared_ptr<UrmaJetty> &jetty)
{
    CHECK_FAIL_RETURN_STATUS(jetty != nullptr, K_RUNTIME_ERROR, "Jetty is null when retiring to error");

    const auto jettyId = jetty->GetJettyId();
    LOG(INFO) << "Try modify jetty id " << jettyId << " to state URMA_JETTY_STATE_ERROR";

    RETURN_IF_NOT_OK_APPEND_MSG(jetty->ModifyToError(),
                                FormatString("Failed to modify jetty with id %u to error state", jettyId));
    auto traceId = Trace::Instance().GetTraceID();
    {
        std::lock_guard<std::mutex> pendingDeleteLock(pendingDeleteMutex_);
        pendingDeleteJettys_[jettyId] = { jetty, traceId };
    }
    LOG(INFO) << "Retired jetty id " << jettyId << " to pending delete";
    INJECT_POINT("urma.ModifyJettyToError");
    return Status::OK();
}

void UrmaResource::AsyncDeleteJetty(uint32_t jettyId)
{
    deleteJettyThread_->Submit([this, jettyId]() {
        std::shared_ptr<UrmaJetty> jetty;
        std::string traceId;
        {
            std::lock_guard<std::mutex> lock(pendingDeleteMutex_);
            auto iter = pendingDeleteJettys_.find(jettyId);
            if (iter == pendingDeleteJettys_.end()) {
                LOG(WARNING) << "Jetty with id " << jettyId << " does not exist in pendingDeleteJettys_";
                return;
            }
            jetty = std::move(iter->second.jetty);
            traceId = std::move(iter->second.traceId);
            pendingDeleteJettys_.erase(iter);
        }

        auto traceGuard = Trace::Instance().SetTraceNewID(traceId);
        LOG(INFO) << "Remove Jetty with id " << jettyId << " from pendingDeleteJettys_ ";
        jetty.reset();
    });
}

Status UrmaResource::RegisterJetty(const std::shared_ptr<UrmaJetty> &jetty)
{
    CHECK_FAIL_RETURN_STATUS(jetty != nullptr, K_RUNTIME_ERROR, "Cannot register null Jetty");
    const auto jettyId = jetty->GetJettyId();
    std::lock_guard<std::mutex> lock(jettyRegistryMutex_);
    jettyRegistry_[jettyId] = jetty;
    LOG(INFO) << "[UrmaResource] Registered Jetty " << jettyId << " in registry";
    return Status::OK();
}

void UrmaResource::UnregisterJetty(uint32_t jettyId, const UrmaJetty *expected)
{
    std::lock_guard<std::mutex> lock(jettyRegistryMutex_);
    auto it = jettyRegistry_.find(jettyId);
    if (it == jettyRegistry_.end()) {
        return;
    }
    if (expected != nullptr) {
        auto locked = it->second.lock();
        if (locked && locked.get() != expected) {
            return;
        }
    }
    jettyRegistry_.erase(it);
    LOG(INFO) << "[UrmaResource] Unregistered Jetty " << jettyId << " from registry";
}

Status UrmaResource::GetJettyById(uint32_t jettyId, std::shared_ptr<UrmaJetty> &jetty)
{
    std::lock_guard<std::mutex> lock(jettyRegistryMutex_);
    auto it = jettyRegistry_.find(jettyId);
    if (it == jettyRegistry_.end()) {
        RETURN_STATUS(K_NOT_FOUND, FormatString("Jetty %u not found in registry", jettyId));
    }
    jetty = it->second.lock();
    if (jetty == nullptr) {
        jettyRegistry_.erase(it);
        RETURN_STATUS(K_NOT_FOUND, FormatString("Jetty %u expired in registry", jettyId));
    }
    return Status::OK();
}

Status UrmaResource::GetAnyValidJetty(std::shared_ptr<UrmaJetty> &jetty)
{
    std::lock_guard<std::mutex> lock(jettyRegistryMutex_);
    for (auto it = jettyRegistry_.begin(); it != jettyRegistry_.end();) {
        jetty = it->second.lock();
        if (jetty == nullptr) {
            it = jettyRegistry_.erase(it);
            continue;
        }
        if (jetty->IsValid()) {
            return Status::OK();
        }
        ++it;
    }
    RETURN_STATUS(K_NOT_FOUND, "No valid Jetty found in registry");
}

}  // namespace datasystem
