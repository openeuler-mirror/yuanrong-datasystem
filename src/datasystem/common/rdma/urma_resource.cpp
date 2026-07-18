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

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <iterator>
#include <sstream>
#include <utility>

#include "securec.h"

#ifndef USE_URMA_MOCK
#include <ub/umdk/urma/urma_opcode.h>
#include <ub/umdk/urma/urma_ubagg.h>
#endif

#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/log/trace.h"
#include "datasystem/common/metrics/kv_metrics.h"
#include "datasystem/common/os_transport_pipeline/os_transport_pipeline_worker_api.h"
#include "datasystem/common/perf/perf_manager.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/no_destructor.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/rdma/urma_dlopen_util.h"
#include "datasystem/utils/status.h"

DS_DECLARE_bool(urma_event_mode);
DS_DECLARE_uint64(urma_max_write_size_mb);
DS_DECLARE_uint32(urma_send_jetty_lane_pool_size);
DS_DECLARE_uint32(urma_send_jetty_lane_refill_extra_size);

namespace datasystem {
namespace {
constexpr uint32_t K_URMA_WARNING_LOG_EVERY_N = 100;
constexpr const char *URMA_ERROR_SUGGEST = "check URMA";

// A quarantined/pending Jetty cannot be destroyed safely when the provider's modify/flush
// contract did not converge. Keep the complete wrapper (including its JFR dependency) alive
// until process exit instead of letting shutdown turn fail-closed into an implicit provider call.
// The refill live-limit bounds how many objects can reach this holder in one resource lifetime.
void RetainUnsafeJettyUntilProcessExit(std::shared_ptr<UrmaJetty> jetty)
{
    static NoDestructor<std::mutex> retainedMutex;
    static NoDestructor<std::vector<std::shared_ptr<UrmaJetty>>> retainedJettys;
    std::lock_guard<std::mutex> lock(*retainedMutex);
    retainedJettys->emplace_back(std::move(jetty));
}

// Defensive fallback for an unexpected last-reference drop outside UrmaResource::Clear. The
// wrapper itself can no longer be retained once its destructor starts, but the raw Jetty and its
// JFR dependency still must not be independently reclaimed after a non-converged lifecycle.
void RetainUnsafeRawJettyUntilProcessExit(urma_jetty_t *raw, std::shared_ptr<UrmaJfr> sharedJfr)
{
    using RetainedRawJetty = std::pair<urma_jetty_t *, std::shared_ptr<UrmaJfr>>;
    static NoDestructor<std::mutex> retainedMutex;
    static NoDestructor<std::vector<RetainedRawJetty>> retainedJettys;
    std::lock_guard<std::mutex> lock(*retainedMutex);
    retainedJettys->emplace_back(raw, std::move(sharedJfr));
}

Status BuildRemoteJetty(const UrmaJfrInfo &info, urma_rjetty_t &remoteJetty)
{
    urma_eid_t eid{};
    CHECK_FAIL_RETURN_STATUS(
        info.eid.size() == URMA_EID_SIZE, K_RUNTIME_ERROR,
        FormatString("Eid size mismatch, expected: %d, actual: %d", URMA_EID_SIZE, info.eid.size()));
    auto rc = memcpy_s(eid.raw, URMA_EID_SIZE, info.eid.data(), info.eid.size());
    CHECK_FAIL_RETURN_STATUS(rc == EOK, K_RUNTIME_ERROR,
                             FormatString("Unable to copy %d bytes, rc = %d, errno = %d", URMA_EID_SIZE, rc, errno));
    remoteJetty.jetty_id.eid = eid;
    remoteJetty.jetty_id.uasid = info.uasid;
    remoteJetty.jetty_id.id = info.jfrId;
    remoteJetty.trans_mode = URMA_TM_RM;
    remoteJetty.type = URMA_JETTY;
    remoteJetty.tp_type = URMA_CTP;
    remoteJetty.flag.value = 0;
    return Status::OK();
}
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

Status UrmaJfc::Create(urma_context_t *context, const urma_device_attr_t &deviceAttr, std::unique_ptr<UrmaJfc> &jfc)
{
    LOG(INFO) << "urma create jfc";
    CHECK_FAIL_RETURN_STATUS(context != nullptr, K_INVALID, "URMA context is null");

    urma_jfc_cfg_t jfcConfig{};
    jfcConfig.depth = deviceAttr.dev_cap.max_jfc_depth;
    jfcConfig.flag.value = 0;
    jfcConfig.jfce = nullptr;
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
    auto jetty = GetJetty().lock();
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
        ReleaseCounter();
        return;
    }
    // A retired Jetty reaches provider delete only through DeleteAfterFlush.  In particular, do
    // not turn a failed modify into an implicit destructor delete: that provider contract is not
    // confirmed and could reintroduce post/delete overlap during shutdown.
    if (lifecycle_.load(std::memory_order_acquire) == LifecycleState::ACTIVE && ActivePostCalls() == 0 &&
        resource_ != nullptr) {
        const auto jettyId = raw_->jetty_id.id;
        const auto ret = ds_urma_delete_jetty(raw_);
        LOG_IF(ERROR, ret != URMA_SUCCESS) << "Failed to delete active Jetty " << jettyId << ", ret=" << ret;
        raw_ = nullptr;
        ReleaseCounter();
    } else {
        LOG(WARNING) << "Fail-closed: retaining raw Jetty " << raw_->jetty_id.id
                     << " because its provider lifecycle did not reach DESTROYED";
        RetainUnsafeRawJettyUntilProcessExit(raw_, std::move(sharedJfr_));
        raw_ = nullptr;
    }
    sharedJfr_.reset();
}

Status UrmaJetty::Create(UrmaResource &resource, JettyType jettyType, std::shared_ptr<UrmaJetty> &jetty)
{
    METRIC_TIMER(metrics::KvMetricId::URMA_JETTY_CREATE_LATENCY);
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
    jfsConfig.priority = resource.GetJettyPriority();
    const auto maxSge = 13;
    jfsConfig.max_sge = maxSge;
    jfsConfig.max_inline_data = 0;
    jfsConfig.rnr_retry = URMA_TYPICAL_RNR_RETRY;
    jfsConfig.err_timeout = 0;
    jfsConfig.jfc = resource.GetJfc();
    jfsConfig.user_ctx = 0;
    jfsConfig.flag.value = 0;
    jfsConfig.flag.bs.multi_path = 1;

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
    jetty = std::make_shared<UrmaJetty>(raw, sharedJfr, &resource, jettyType);
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

void UrmaJetty::PostPermit::Reset()
{
    if (jetty_ != nullptr) {
        jetty_->ReleasePostPermit();
        jetty_.reset();
    }
}

void UrmaJetty::ReleaseCounter()
{
    if (counted_) {
        counted_ = false;
        counter_.fetch_sub(1);
    }
}

UrmaJetty::PostPermit UrmaJetty::TryAcquirePostPermit()
{
    auto state = postGate_.load(std::memory_order_acquire);
    while (!PostGate::IsClosing(state)) {
        if (PostGate::ActivePosts(state) == PostGate::kActiveMask) {
            return {};
        }
        if (postGate_.compare_exchange_weak(state, state + 1, std::memory_order_acq_rel,
                                             std::memory_order_acquire)) {
            // Every public posting entry owns a shared_ptr already; retain it in the permit so
            // the wrapper and raw provider handle live through the complete synchronous call.
            return PostPermit(shared_from_this());
        }
    }
    return {};
}

bool UrmaJetty::BeginRetire()
{
    auto state = postGate_.load(std::memory_order_acquire);
    while (!PostGate::IsClosing(state)) {
        if (postGate_.compare_exchange_weak(state, state | PostGate::kClosing, std::memory_order_acq_rel,
                                             std::memory_order_acquire)) {
            lifecycle_.store(LifecycleState::QUIESCING, std::memory_order_release);
            return true;
        }
    }
    return false;
}

bool UrmaJetty::TryScheduleFinalizer()
{
    auto state = postGate_.load(std::memory_order_acquire);
    while (PostGate::IsClosing(state) && PostGate::IsRetireArmed(state) &&
           !PostGate::IsFinalizerScheduled(state) && PostGate::ActivePosts(state) == 0) {
        if (postGate_.compare_exchange_weak(state, state | PostGate::kFinalizerScheduled, std::memory_order_acq_rel,
                                             std::memory_order_acquire)) {
            return true;
        }
    }
    return false;
}

bool UrmaJetty::ArmRetireFinalizer()
{
    auto state = postGate_.load(std::memory_order_acquire);
    while (PostGate::IsClosing(state) && !PostGate::IsRetireArmed(state)) {
        if (postGate_.compare_exchange_weak(state, state | PostGate::kRetireArmed, std::memory_order_acq_rel,
                                             std::memory_order_acquire)) {
            return TryScheduleFinalizer();
        }
    }
    return false;
}

void UrmaJetty::ReleasePostPermit()
{
    const auto old = postGate_.fetch_sub(1, std::memory_order_acq_rel);
    DCHECK(PostGate::ActivePosts(old) != 0) << "PostPermit underflow";
    if (PostGate::IsClosing(old) && PostGate::ActivePosts(old) == 1 && TryScheduleFinalizer() &&
        resource_ != nullptr) {
        resource_->ScheduleRetireFinalizer(shared_from_this());
    }
    // Normal ACTIVE posts never need the shutdown condition variable. Avoid a process-wide
    // mutex/cache-line touch on the data path; only a closing Jetty can unblock a drain waiter.
    if (PostGate::IsClosing(old) && resource_ != nullptr) {
        resource_->NotifyPostPermitReleased();
    }
}

bool UrmaJetty::BeginModify()
{
    auto expected = LifecycleState::QUIESCING;
    return lifecycle_.compare_exchange_strong(expected, LifecycleState::MODIFYING, std::memory_order_acq_rel);
}

bool UrmaJetty::CompleteModify()
{
    auto expected = LifecycleState::MODIFYING;
    if (!lifecycle_.compare_exchange_strong(expected, LifecycleState::WAIT_FLUSH, std::memory_order_acq_rel)) {
        return false;
    }
    if (!flushSeen_.load(std::memory_order_acquire)) {
        return false;
    }
    expected = LifecycleState::WAIT_FLUSH;
    return lifecycle_.compare_exchange_strong(expected, LifecycleState::DELETE_READY, std::memory_order_acq_rel);
}

bool UrmaJetty::ObserveFlushErrDone()
{
    flushSeen_.store(true, std::memory_order_release);
    auto expected = LifecycleState::WAIT_FLUSH;
    return lifecycle_.compare_exchange_strong(expected, LifecycleState::DELETE_READY, std::memory_order_acq_rel);
}

bool UrmaJetty::BeginDelete()
{
    auto expected = LifecycleState::DELETE_READY;
    return lifecycle_.compare_exchange_strong(expected, LifecycleState::DELETING, std::memory_order_acq_rel);
}

Status UrmaJetty::DeleteAfterFlush()
{
    CHECK_FAIL_RETURN_STATUS(lifecycle_.load(std::memory_order_acquire) == LifecycleState::DELETING, K_RUNTIME_ERROR,
                             "Jetty delete attempted before DELETE_READY");
    CHECK_FAIL_RETURN_STATUS(ActivePostCalls() == 0, K_RUNTIME_ERROR, "Jetty delete attempted with active post");
    const auto ret = ds_urma_delete_jetty(raw_);
    if (ret != URMA_SUCCESS) {
        Quarantine();
        return Status(K_URMA_ERROR, FormatString("Failed to delete jetty, ret = %d", ret));
    }
    raw_ = nullptr;
    lifecycle_.store(LifecycleState::DESTROYED, std::memory_order_release);
    ReleaseCounter();
    return Status::OK();
}

void UrmaJetty::Quarantine()
{
    lifecycle_.store(LifecycleState::QUARANTINED, std::memory_order_release);
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

    std::unique_ptr<char[]> segCtxBuf;
    UrmaSeg urmaSeg;
    urma_seg_t *remoteSegment = nullptr;
    if (importSegmentInfo.has_seg_ctx() && !importSegmentInfo.seg_ctx().seg_blob().empty()) {
        const auto &segBlob = importSegmentInfo.seg_ctx().seg_blob();
        if (segBlob.size() < sizeof(urma_seg_t)) {
            RETURN_STATUS(K_RUNTIME_ERROR, FormatString("Invalid delegated seg blob size=%zu", segBlob.size()));
        }
        segCtxBuf = std::make_unique<char[]>(segBlob.size());
        if (memcpy_s(segCtxBuf.get(), segBlob.size(), segBlob.data(), segBlob.size()) != EOK) {
            RETURN_STATUS(K_RUNTIME_ERROR, "Failed to copy delegated seg blob");
        }
        remoteSegment = reinterpret_cast<urma_seg_t *>(segCtxBuf.get());
        LOG(INFO) << "[URMA_CONNECT] Import remote seg using delegated context, va=" << remoteSegment->ubva.va
                  << ", length=" << segBlob.size() << ", client_id:" << urmaJfrInfo_.clientId;
    } else {
        RETURN_IF_NOT_OK(urmaSeg.FromProto(importSegmentInfo.seg()));
        remoteSegment = &urmaSeg.raw;
        LOG(INFO) << "[URMA_CONNECT] Import remote seg using legacy handshake, va=" << remoteSegment->ubva.va
                  << ", client_id:" << urmaJfrInfo_.clientId;
    }
    LOG(INFO) << "import remote seg info: " << UrmaSeg::ToString(*remoteSegment)
              << ", client_id:" << urmaJfrInfo_.clientId;
    RETURN_IF_NOT_OK(
        UrmaRemoteSegment::Import(context, urmaToken, importSegmentFlag, *remoteSegment, accessor->second));
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
    targetJetty_.reset();
    tsegs_.clear();
    urmaJfrInfo_ = UrmaJfrInfo();
}

UrmaResource::~UrmaResource()
{
    Clear();
}

Status UrmaResource::Init(urma_device_t *device, uint32_t eidIndex, bool isBondingDevice)
{
    Clear();
    shuttingDown_.store(false, std::memory_order_release);
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
    RETURN_IF_NOT_OK(UrmaJfc::Create(context_->Raw(), urmaDeviceAttribute_, jfc_));
    if (FLAGS_urma_event_mode) {
        RETURN_IF_NOT_OK(jfc_->Rearm());
    }

    constexpr uint32_t threadCount = 1;
    deleteJettyThread_ = std::make_unique<ThreadPool>(0, threadCount, "RetireJfs");
    RETURN_IF_NOT_OK(OsXprtPipln::InitOsPiplnRH2DEnv(context_->Raw(), jfc_->Raw(), jfce_->Raw(), JETTY_SIZE));

    // Pre-fill the send Jetty pool to capacity (fail-fast on creation failure).
    RETURN_IF_NOT_OK(PreFillSendJettyPool());

    // Start background refill thread to top up the pool after failures.
    refillStop_.store(false);
    refillNeeded_.store(false);
    refillThread_ = std::make_unique<std::thread>(&UrmaResource::RefillLoop, this);
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

uint64_t UrmaResource::GetMaxWriteSize() const
{
    constexpr uint64_t mbToBytes = 1024ul * 1024ul;
    const uint64_t maxSize = FLAGS_urma_max_write_size_mb * mbToBytes;
    return std::min<uint64_t>(maxSize, urmaDeviceAttribute_.dev_cap.max_write_size);
}

void UrmaResource::Clear()
{
    BeginShutdown();
    // Keep the resource boundary safe even when Clear is invoked directly rather than through
    // UrmaManager::Stop. This is normally an immediate idempotent check after manager shutdown.
    WaitForPostPermitsDrained();
    OsXprtPipln::UnInitOsPiplnRH2DEnv();

    // Stop the background refill thread first so it does not touch the pool during teardown.
    if (refillThread_ != nullptr && refillThread_->joinable()) {
        refillStop_.store(true);
        refillCV_.notify_all();
        refillThread_->join();
        refillThread_.reset();
    }
    {
        std::unique_lock<std::shared_timed_mutex> deleteThreadLock(deleteJettyThreadMutex_);
        deleteJettyThread_.reset();
    }

    {
        std::lock_guard<std::mutex> lock(sharedRecvJettyMutex_);
        sharedRecvJetty_.reset();
    }
    {
        std::lock_guard<std::mutex> lock(jettyRegistryMutex_);
        jettyRegistry_.clear();
    }
    {
        std::lock_guard<std::mutex> lock(sharedJettyJfrMutex_);
        sharedJettyJfr_.reset();
    }
    std::vector<std::shared_ptr<UrmaJetty>> unsafeJettys;
    {
        std::lock_guard<std::mutex> pendingDeleteLock(pendingDeleteMutex_);
        unsafeJettys.reserve(pendingDeleteJettys_.size() + quarantinedJettys_.size());
        for (auto &[id, pending] : pendingDeleteJettys_) {
            (void)id;
            unsafeJettys.emplace_back(std::move(pending.jetty));
        }
        pendingDeleteJettys_.clear();
        for (auto &[id, jetty] : quarantinedJettys_) {
            (void)id;
            unsafeJettys.emplace_back(std::move(jetty));
        }
        quarantinedJettys_.clear();
    }
    for (auto &jetty : unsafeJettys) {
        RetainUnsafeJettyUntilProcessExit(std::move(jetty));
    }
    LOG_IF(WARNING, !unsafeJettys.empty())
        << "Retained " << unsafeJettys.size()
        << " non-converged URMA Jetty wrappers until process exit to avoid unsafe provider cleanup";
    {
        std::lock_guard<std::mutex> lock(jettyPoolMutex_);
        sendJettyPool_.Clear();
    }
    jettyPriority_ = 0;
    jfc_.reset();
    jfce_.reset();
    context_.reset();
}

Status UrmaResource::CreateJetty(std::shared_ptr<UrmaJetty> &jetty, JettyType jettyType)
{
    CHECK_FAIL_RETURN_STATUS(!shuttingDown_.load(std::memory_order_acquire), K_RUNTIME_ERROR,
                             "URMA resource is shutting down");
    CHECK_FAIL_RETURN_STATUS(context_ != nullptr, K_RUNTIME_ERROR, "URMA context is null when creating Jetty");
    CHECK_FAIL_RETURN_STATUS(jfc_ != nullptr, K_RUNTIME_ERROR, "URMA jfc is null when creating Jetty");
    RETURN_IF_NOT_OK(UrmaJetty::Create(*this, jettyType, jetty));
    RETURN_IF_NOT_OK(RegisterJetty(jetty));
    return Status::OK();
}

Status UrmaResource::GetOrCreateSharedRecvJetty(std::shared_ptr<UrmaJetty> &jetty)
{
    jetty.reset();
    CHECK_FAIL_RETURN_STATUS(context_ != nullptr, K_RUNTIME_ERROR,
                             "URMA context is null when creating shared recv Jetty");
    CHECK_FAIL_RETURN_STATUS(jfc_ != nullptr, K_RUNTIME_ERROR, "URMA jfc is null when creating shared recv Jetty");

    std::lock_guard<std::mutex> lock(sharedRecvJettyMutex_);
    if (sharedRecvJetty_ != nullptr && sharedRecvJetty_->IsValid()) {
        jetty = sharedRecvJetty_;
        return Status::OK();
    }
    if (sharedRecvJetty_ != nullptr) {
        LOG(WARNING) << "Discard invalid shared recv Jetty id " << sharedRecvJetty_->GetJettyId();
        sharedRecvJetty_.reset();
    }

    std::shared_ptr<UrmaJetty> created;
    RETURN_IF_NOT_OK_APPEND_MSG(CreateJetty(created, JettyType::RECV), "Failed to create shared recv Jetty");
    LOG(INFO) << "Created shared recv Jetty id " << created->GetJettyId();
    sharedRecvJetty_ = std::move(created);
    jetty = sharedRecvJetty_;
    return Status::OK();
}

Status UrmaResource::ImportTargetJetty(const UrmaJfrInfo &remoteInfo, std::unique_ptr<UrmaTargetJetty> &targetJetty,
                                       urma_jetty_t *localJetty)
{
    LOG(INFO) << "Begin to import target jft.";
    bondp_rjetty_t bondpRemoteJetty{};
    urma_rjetty_t remoteJetty{};
    RETURN_IF_NOT_OK(BuildRemoteJetty(remoteInfo, remoteJetty));
    bondpRemoteJetty.base = remoteJetty;
    bondpRemoteJetty.jetty = localJetty;
    bondpRemoteJetty.base.flag.bs.has_drv_ext = 1;
    Timer timer;
    METRIC_TIMER(metrics::KvMetricId::URMA_IMPORT_JFR);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
        UrmaTargetJetty::Import(GetContext(), &(bondpRemoteJetty.base), GetUrmaToken(), targetJetty),
        FormatString("Failed to import target jetty, remoteInfo: %s", remoteInfo.ToString()));
    LOG_IF(INFO, timer.ElapsedMilliSecond() > 1)
        << "[URMA_CONNECT] Import target jetty elapsed = " << timer.ElapsedMilliSecond() << "ms"
        << ", cpuid: " << sched_getcpu() << ", remoteInfo: " << remoteInfo.ToString();
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
    std::shared_lock<std::shared_timed_mutex> deleteThreadLock(deleteJettyThreadMutex_);
    CHECK_FAIL_RETURN_STATUS(deleteJettyThread_ != nullptr, K_RUNTIME_ERROR,
                             "Failed to modify Jetty to error because delete Jetty thread is stopped");
    deleteJettyThread_->Execute([this, jetty, traceId]() {
        auto traceGuard = Trace::Instance().SetTraceNewID(traceId);
        LOG_IF_ERROR(RetireJettyToError(jetty), "RetireJettyToError failed");
    });
    return Status::OK();
}

void UrmaResource::ScheduleRetireFinalizer(std::shared_ptr<UrmaJetty> jetty)
{
    const auto rc = AsyncModifyJettyToError(jetty);
    if (rc.IsError()) {
        // The gate has already claimed the one finalizer.  Reopening it would violate I4, so keep
        // the Jetty out of service and out of destructor delete until a provider-safe recovery is known.
        QuarantineJetty(jetty);
    }
}

Status UrmaResource::RetireJettyToError(const std::shared_ptr<UrmaJetty> &jetty)
{
    CHECK_FAIL_RETURN_STATUS(jetty != nullptr, K_RUNTIME_ERROR, "Jetty is null when retiring to error");
    if (!jetty->BeginModify()) {
        return Status::OK();
    }

    const auto jettyId = jetty->GetJettyId();
    LOG(INFO) << "Try modify jetty id " << jettyId << " to state URMA_JETTY_STATE_ERROR";

    const auto modifyRc = jetty->ModifyToError();
    if (modifyRc.IsError()) {
        QuarantineJetty(jetty);
        return Status(modifyRc.GetCode(),
                      FormatString("Failed to modify jetty with id %u to error state: %s", jettyId,
                                   modifyRc.ToString()));
    }
    // FLUSH_ERR_DONE can legitimately be delivered before modify returns. flushSeen_ preserves
    // that early notification; the record was installed before the provider call in RetireJettyInternal.
    if (jetty->CompleteModify()) {
        ScheduleDeleteJetty(jetty);
    }
    LOG(INFO) << "Retired jetty id " << jettyId << " and waiting for flush completion";
    INJECT_POINT("urma.ModifyJettyToError");
    return Status::OK();
}

void UrmaResource::AsyncDeleteJetty(uint32_t jettyId)
{
    std::shared_ptr<UrmaJetty> jetty;
    {
        std::lock_guard<std::mutex> lock(pendingDeleteMutex_);
        const auto iter = pendingDeleteJettys_.find(jettyId);
        if (iter == pendingDeleteJettys_.end()) {
            // An old/stale flush has no authority to delete a replacement identity.
            return;
        }
        jetty = iter->second.jetty;
    }
    if (jetty->ObserveFlushErrDone()) {
        ScheduleDeleteJetty(jetty);
    }
}

void UrmaResource::ScheduleDeleteJetty(const std::shared_ptr<UrmaJetty> &jetty)
{
    if (jetty == nullptr || !jetty->BeginDelete()) {
        return;
    }
    std::string traceId;
    {
        std::lock_guard<std::mutex> lock(pendingDeleteMutex_);
        const auto iter = pendingDeleteJettys_.find(jetty->GetJettyId());
        if (iter != pendingDeleteJettys_.end()) {
            traceId = iter->second.traceId;
        }
    }
    std::shared_lock<std::shared_timed_mutex> deleteThreadLock(deleteJettyThreadMutex_);
    if (deleteJettyThread_ == nullptr) {
        QuarantineJetty(jetty);
        return;
    }
    deleteJettyThread_->Submit([this, jetty, traceId = std::move(traceId)]() {
        auto traceGuard = Trace::Instance().SetTraceNewID(traceId);
        const auto jettyId = jetty->GetJettyId();
        const auto rc = jetty->DeleteAfterFlush();
        if (rc.IsError()) {
            QuarantineJetty(jetty);
            return;
        }
        UnregisterJetty(jettyId, jetty.get());
        {
            std::lock_guard<std::mutex> lock(pendingDeleteMutex_);
            pendingDeleteJettys_.erase(jettyId);
        }
        INJECT_POINT_NO_RETURN("urma.SendJettyAsyncDeleteComplete");
    });
}

void UrmaResource::QuarantineJetty(const std::shared_ptr<UrmaJetty> &jetty)
{
    if (jetty == nullptr) {
        return;
    }
    jetty->Quarantine();
    const auto jettyId = jetty->GetJettyId();
    {
        std::lock_guard<std::mutex> lock(pendingDeleteMutex_);
        pendingDeleteJettys_.erase(jettyId);
        quarantinedJettys_[jettyId] = jetty;
    }
    LOG_FIRST_AND_EVERY_N(ERROR, K_URMA_WARNING_LOG_EVERY_N)
        << "Quarantined URMA Jetty " << jettyId
        << "; provider cleanup remains fail-closed and the registry identity stays reserved";
    // FLUSH_ERR_DONE carries only local_id, without a generation. Keep the weak registry identity
    // (backed by quarantinedJettys_) until resource teardown so a new wrapper cannot create an ABA
    // collision with a provider object that was never safely deleted.
}

Status UrmaResource::RegisterJetty(const std::shared_ptr<UrmaJetty> &jetty)
{
    CHECK_FAIL_RETURN_STATUS(jetty != nullptr, K_RUNTIME_ERROR, "Cannot register null Jetty");
    const auto jettyId = jetty->GetJettyId();
    std::lock_guard<std::mutex> lock(jettyRegistryMutex_);
    // BeginShutdown sets shuttingDown_ before taking this same registry lock. Therefore either
    // registration wins and appears in its snapshot, or shutdown wins and registration is rejected.
    CHECK_FAIL_RETURN_STATUS(!shuttingDown_.load(std::memory_order_acquire), K_RUNTIME_ERROR,
                             "Cannot register Jetty while URMA resource is shutting down");
    if (const auto iter = jettyRegistry_.find(jettyId); iter != jettyRegistry_.end()) {
        if (auto registered = iter->second.lock(); registered != nullptr) {
            CHECK_FAIL_RETURN_STATUS(registered.get() == jetty.get(), K_RUNTIME_ERROR,
                                     FormatString("Cannot register a second live Jetty with id %u", jettyId));
            return Status::OK();
        }
    }
    jettyRegistry_[jettyId] = jetty;
    LOG(INFO) << "[UrmaResource] Registered Jetty " << jettyId << " in registry";
    return Status::OK();
}

void UrmaResource::UnregisterJetty(uint32_t jettyId, const UrmaJetty *expected)
{
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
    }
    LOG(INFO) << "[UrmaResource] Unregistered Jetty " << jettyId << " from registry";
    INJECT_POINT_NO_RETURN("urma.SendJettyRegistryUnregister");
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

// ============================================================================
// Process-level send Jetty pool
// ============================================================================

Status UrmaResource::PreFillSendJettyPool()
{
    // Create all Jetties outside the pool lock so driver calls do not block the hot path on
    // re-Init. On any failure the call fails fast; already-created Jetties are released by
    // shared_ptr destruction and the caller (Init) aborts startup.
    std::vector<std::shared_ptr<UrmaJetty>> created;
    created.reserve(FLAGS_urma_send_jetty_lane_pool_size);
    for (uint32_t i = 0; i < FLAGS_urma_send_jetty_lane_pool_size; ++i) {
        std::shared_ptr<UrmaJetty> jetty;
        auto rc = CreateJetty(jetty);
        if (rc.IsError()) {
            LOG(ERROR) << "[URMA_SEND_LANE_POOL] Pre-fill failed, targetCapacity="
                       << FLAGS_urma_send_jetty_lane_pool_size << ", failedIndex=" << i
                       << ", createdCount=" << created.size() << ", rc=" << rc.ToString();
            RETURN_IF_NOT_OK_APPEND_MSG(rc, FormatString("Pre-fill send Jetty pool failed, targetCapacity=%u, "
                                                         "failedIndex=%u, createdCount=%zu",
                                                         FLAGS_urma_send_jetty_lane_pool_size, i, created.size()));
        }
        created.push_back(std::move(jetty));
    }
    {
        std::lock_guard<std::mutex> lock(jettyPoolMutex_);
        for (auto &j : created) {
            sendJettyPool_.Add(std::move(j));
        }
    }
    LOG(INFO) << "[URMA_SEND_LANE_POOL] Pre-filled pool with " << FLAGS_urma_send_jetty_lane_pool_size
              << " send Jetties";
    return Status::OK();
}

void UrmaResource::RefillLoop()
{
    constexpr auto kRefillInterval = std::chrono::milliseconds(50);
    bool retryAfterFailure = false;
    while (!refillStop_.load()) {
        {
            std::unique_lock<std::mutex> lock(refillMutex_);
            if (retryAfterFailure) {
                // A failed CreateJetty must not be retried immediately even if another refill notification
                // arrived while the previous attempt was in progress. Only teardown can interrupt this backoff.
                refillCV_.wait_for(lock, kRefillInterval, [this] { return refillStop_.load(); });
            } else {
                refillCV_.wait_for(lock, kRefillInterval,
                                   [this] { return refillStop_.load() || refillNeeded_.load(); });
            }
            if (refillStop_.load()) {
                break;
            }
            refillNeeded_.store(false);
        }

        SendJettyPool::Stats poolStats;
        const auto deficit = GetSendJettyPoolRefillDeficit(poolStats);
        if (deficit != 0) {
            retryAfterFailure = RefillSendJettyPool(deficit);
        } else {
            retryAfterFailure = false;
        }
    }
}

size_t UrmaResource::GetSendJettyPoolRefillDeficit(SendJettyPool::Stats &poolStats)
{
    {
        std::lock_guard<std::mutex> poolLock(jettyPoolMutex_);
        poolStats = sendJettyPool_.GetStats();
    }
    const auto retiringOrPendingCount = GetRetiringOrPendingJettyCount();
    const auto targetPoolSize = static_cast<size_t>(FLAGS_urma_send_jetty_lane_pool_size);
    const auto liveLimit = targetPoolSize + static_cast<size_t>(FLAGS_urma_send_jetty_lane_refill_extra_size);
    const auto liveAndRetiring = poolStats.poolSize + retiringOrPendingCount;
    if (poolStats.poolSize < targetPoolSize && liveAndRetiring < liveLimit) {
        return std::min(targetPoolSize - poolStats.poolSize, liveLimit - liveAndRetiring);
    }
    VLOG(1) << "[URMA_SEND_LANE_POOL] Refill skipped, poolSize=" << poolStats.poolSize
            << ", idleCount=" << poolStats.idleCount << ", retiringOrPendingCount=" << retiringOrPendingCount
            << ", targetPoolSize=" << targetPoolSize << ", liveLimit=" << liveLimit;
    return 0;
}

bool UrmaResource::RefillSendJettyPool(size_t deficit)
{
    if (shuttingDown_.load(std::memory_order_acquire)) {
        return false;
    }
    std::vector<std::shared_ptr<UrmaJetty>> created;
    for (size_t i = 0; i < deficit; ++i) {
        std::shared_ptr<UrmaJetty> jetty;
        auto rc = CreateSendJettyForRefill(jetty);
        if (rc.IsError()) {
            LOG_FIRST_AND_EVERY_N(ERROR, K_URMA_WARNING_LOG_EVERY_N)
                << "[URMA_SEND_LANE_POOL] Refill CreateJetty failed: " << rc.ToString() << ", created "
                << created.size() << "/" << deficit << ", will retry next tick";
            break;
        }
        created.push_back(std::move(jetty));
    }
    if (created.empty()) {
        return true;
    }
    SendJettyPool::Stats poolStats;
    {
        std::lock_guard<std::mutex> poolLock(jettyPoolMutex_);
        if (shuttingDown_.load(std::memory_order_acquire)) {
            return false;
        }
        for (auto &jetty : created) {
            sendJettyPool_.Add(std::move(jetty));
        }
        poolStats = sendJettyPool_.GetStats();
    }
    for (size_t i = 0; i < created.size(); ++i) {
        INJECT_POINT_NO_RETURN("urma.SendJettyPoolRefillAdded");
    }
    LOG(INFO) << "[URMA_SEND_LANE_POOL] Refilled " << created.size() << " send Jetties, poolSize now "
              << poolStats.poolSize << ", idleCount=" << poolStats.idleCount;
    return created.size() != deficit;
}

Status UrmaResource::CreateSendJettyForRefill(std::shared_ptr<UrmaJetty> &jetty)
{
    INJECT_POINT("urma.RefillCreateSendJetty");
    return CreateJetty(jetty);
}

void UrmaResource::MaybeTriggerRefill()
{
    {
        std::lock_guard<std::mutex> lock(refillMutex_);
        refillNeeded_.store(true);
    }
    refillCV_.notify_one();
}

void UrmaResource::RemoveFromPoolLocked(const std::shared_ptr<UrmaJetty> &jetty)
{
    if (sendJettyPool_.Remove(jetty)) {
        const auto stats = sendJettyPool_.GetStats();
        LOG(INFO) << "[URMA_SEND_LANE_POOL] Removed jetty " << jetty->GetJettyId()
                  << " from pool, poolSize=" << stats.poolSize << ", idleCount=" << stats.idleCount;
        return;
    }
    LOG(WARNING) << "[URMA_SEND_LANE_POOL] Jetty " << jetty->GetJettyId() << " not found in pool during removal";
}

Status UrmaResource::AcquireJetty(std::shared_ptr<UrmaJetty> &jetty)
{
    CHECK_FAIL_RETURN_STATUS(!shuttingDown_.load(std::memory_order_acquire), K_TRY_AGAIN,
                             "URMA resource is shutting down");
    std::lock_guard<std::mutex> lock(jettyPoolMutex_);

    // Pop an idle & valid Jetty. After pre-fill the pool holds only valid Jetties, but invalid
    // entries are skipped defensively (e.g. a Jetty invalidated between release and acquire).
    while (sendJettyPool_.PopIdle(jetty)) {
        if (jetty != nullptr && jetty->IsValid()) {
            const auto stats = sendJettyPool_.GetStats();
            VLOG(1) << "[URMA_SEND_LANE_POOL] Acquired jetty " << jetty->GetJettyId()
                    << ", idleCount=" << stats.idleCount << ", poolSize=" << stats.poolSize;
            return Status::OK();
        }
    }

    // Pool is full but every Jetty is in use. With the 64-thread / 200-pool sizing this should not
    // happen in practice; surface K_TRY_AGAIN so the caller can backpressure.
    const auto stats = sendJettyPool_.GetStats();
    RETURN_STATUS(K_TRY_AGAIN, FormatString("No idle URMA send Jetty in pool, poolSize=%zu, idleCount=%zu, "
                                            "inUseCount=%zu",
                                            stats.poolSize, stats.idleCount, stats.inUseCount));
}

void UrmaResource::ReleaseJetty(const std::shared_ptr<UrmaJetty> &jetty)
{
    if (jetty == nullptr) {
        return;
    }

    std::lock_guard<std::mutex> lock(jettyPoolMutex_);

    if (!jetty->IsValid()) {
        // Jetty was invalidated by ReCreateJetty or RetireJetty and already removed from the pool.
        LOG(INFO) << "[URMA_SEND_LANE_POOL] Releasing invalid jetty " << jetty->GetJettyId()
                  << " (already removed from pool)";
        return;
    }

    if (sendJettyPool_.Release(jetty)) {
        const auto stats = sendJettyPool_.GetStats();
        VLOG(1) << "[URMA_SEND_LANE_POOL] Released jetty " << jetty->GetJettyId() << ", idleCount=" << stats.idleCount;
        return;
    }

    LOG_FIRST_AND_EVERY_N(WARNING, K_URMA_WARNING_LOG_EVERY_N)
        << "[URMA_SEND_LANE_POOL] Jetty " << jetty->GetJettyId() << " not found in pool during release";
}

SendJettyPool::Stats UrmaResource::GetSendJettyPoolStats()
{
    std::lock_guard<std::mutex> lock(jettyPoolMutex_);
    return sendJettyPool_.GetStats();
}

size_t UrmaResource::GetRetiringOrPendingJettyCount()
{
    std::lock_guard<std::mutex> pendingDeleteLock(pendingDeleteMutex_);
    size_t sendJettyCount = 0;
    for (const auto &[id, pending] : pendingDeleteJettys_) {
        (void)id;
        sendJettyCount += pending.jetty != nullptr && pending.jetty->GetType() == JettyType::SEND;
    }
    for (const auto &[id, jetty] : quarantinedJettys_) {
        (void)id;
        sendJettyCount += jetty != nullptr && jetty->GetType() == JettyType::SEND;
    }
    return sendJettyCount;
}

Status UrmaResource::ReCreateJetty(const std::shared_ptr<UrmaJetty> &failedJetty)
{
    return RetireJettyInternal(failedJetty);
}

Status UrmaResource::RetireJetty(const std::shared_ptr<UrmaJetty> &jetty)
{
    return RetireJettyInternal(jetty);
}

Status UrmaResource::RetireJettyInternal(const std::shared_ptr<UrmaJetty> &jetty)
{
    if (jetty == nullptr || !jetty->BeginRetire()) {
        return Status::OK();
    }
    const auto jettyId = jetty->GetJettyId();
    if (jetty->GetType() == JettyType::SEND) {
        std::lock_guard<std::mutex> lock(jettyPoolMutex_);
        RemoveFromPoolLocked(jetty);
    } else {
        // A shared receive Jetty is not a send lane: never route it through send-pool refill.
        std::lock_guard<std::mutex> lock(sharedRecvJettyMutex_);
        if (sharedRecvJetty_.get() == jetty.get()) {
            sharedRecvJetty_.reset();
        }
    }

    // This record is deliberately installed before arming the gate and before provider modify.
    // An early FLUSH_ERR_DONE is retained in the lifecycle object and cannot be lost.
    {
        std::lock_guard<std::mutex> lock(pendingDeleteMutex_);
        pendingDeleteJettys_.emplace(jettyId, PendingDeleteJetty{ jetty, Trace::Instance().GetTraceID() });
    }
    if (jetty->GetType() == JettyType::SEND && !shuttingDown_.load(std::memory_order_acquire)) {
        MaybeTriggerRefill();
    }
    if (jetty->ArmRetireFinalizer()) {
        ScheduleRetireFinalizer(jetty);
    }
    return Status::OK();
}

void UrmaResource::BeginShutdown()
{
    if (shuttingDown_.exchange(true, std::memory_order_acq_rel)) {
        return;
    }
    // Prevent a concurrent refill from registering or publishing a fresh ACTIVE Jetty after the
    // registry snapshot below. RefillSendJettyPool and RegisterJetty recheck shuttingDown_.
    refillStop_.store(true, std::memory_order_release);
    refillCV_.notify_all();
    std::vector<std::shared_ptr<UrmaJetty>> jettys;
    {
        std::lock_guard<std::mutex> lock(jettyRegistryMutex_);
        for (auto &[id, weakJetty] : jettyRegistry_) {
            (void)id;
            if (auto jetty = weakJetty.lock(); jetty != nullptr) {
                jettys.emplace_back(std::move(jetty));
            }
        }
    }
    for (const auto &jetty : jettys) {
        (void)RetireJettyInternal(jetty);
    }
}

void UrmaResource::WaitForPostPermitsDrained()
{
    std::unique_lock<std::mutex> drainLock(postDrainMutex_);
    postDrainCV_.wait(drainLock, [this] {
        std::lock_guard<std::mutex> registryLock(jettyRegistryMutex_);
        for (const auto &[id, weakJetty] : jettyRegistry_) {
            (void)id;
            if (const auto jetty = weakJetty.lock(); jetty != nullptr && jetty->ActivePostCalls() != 0) {
                return false;
            }
        }
        return true;
    });
}

void UrmaResource::NotifyPostPermitReleased()
{
    std::lock_guard<std::mutex> drainLock(postDrainMutex_);
    postDrainCV_.notify_all();
}

}  // namespace datasystem
