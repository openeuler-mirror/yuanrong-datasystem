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

#include "datasystem/common/urma_mock/abi/mock_abi.h"

#include <unistd.h>

#include <cerrno>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <chrono>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/urma_mock/inject/fault_inject.h"
#include "datasystem/common/urma_mock/mock_registry.h"
#include "datasystem/common/urma_mock/objects/mock_context.h"
#include "datasystem/common/urma_mock/objects/mock_device.h"
#include "datasystem/common/urma_mock/objects/mock_jfc.h"
#include "datasystem/common/urma_mock/objects/mock_jetty.h"
#include "datasystem/common/urma_mock/objects/mock_seg.h"
#include "datasystem/common/urma_mock/objects/mock_tjetty.h"
#include "datasystem/common/urma_mock/post_send/post_send_wr.h"
#include "datasystem/common/urma_mock/segment/segment_endpoint_registry.h"
#include "datasystem/common/urma_mock/transport/uds_endpoint_service.h"
#include "datasystem/common/urma_mock/urma_mock_backend.h"

namespace {
constexpr int K_MOCK_DEVICE_TYPE = 1;
constexpr uint32_t K_MOCK_EID_COUNT = 1;
constexpr int K_MOCK_DEVICE_LIMIT = 1024;
constexpr int K_MOCK_WR_DEPTH = 4096;
constexpr uint64_t K_MOCK_MAX_TRANSFER_SIZE = 1024ULL * 1024 * 1024;
constexpr int K_MOCK_PAGE_SIZE = 4096;
constexpr uint32_t K_MOCK_PRIORITY_SERVICE_LEVEL = 0;
constexpr int K_NO_WAIT_MS = 0;
constexpr int K_POLL_SLEEP_MS = 1;
constexpr uint32_t K_DEFAULT_ACK_COUNT = 1;
constexpr uint64_t K_DEFAULT_IMPORT_SEG_SIZE = 4096;
constexpr uint64_t K_INVALID_TOKEN = 0;

datasystem::urma_mock::MockContext *CreateDefaultContextForDirectRegister(
    datasystem::urma_mock::MockUrmaBackend &backend)
{
    (void)backend.Init();
    auto devices = backend.GetDevices();
    if (devices.empty()) {
        return nullptr;
    }
    return backend.CreateContextHandle(devices.front()).second;
}

uint64_t ParseSegmentToken(const std::string &key)
{
    if (key.empty()) {
        return K_INVALID_TOKEN;
    }
    char *end = nullptr;
    errno = 0;
    uint64_t token = std::strtoull(key.c_str(), &end, 10);
    if (errno != 0 || end == key.c_str() || *end != '\0') {
        return K_INVALID_TOKEN;
    }
    return token;
}

bool ShouldReturnFromStatusInject(const std::string &name)
{
#ifdef WITH_TESTS
    auto handle = datasystem::inject::Execute(__LINE__, __FILE__, name);
    return handle.NeedReturn();
#else
    (void)name;
    return false;
#endif
}
}  // namespace

// C ABI entry points. Same signatures as the real ds_urma_* functions in liburma.
// They are thin dispatchers into MockUrmaBackend through the side tables.
// --- init (2) ---

urma_status_t ds_urma_mock_init(const urma_init_attr_t *attr)
{
    (void)attr;
    return datasystem::urma_mock::MockUrmaBackend::Instance().Init() ? URMA_SUCCESS : URMA_E_FAIL;
}

urma_status_t ds_urma_mock_uninit(void)
{
    datasystem::urma_mock::MockUrmaBackend::Instance().Cleanup();
    return URMA_SUCCESS;
}

// --- log (2) ---

urma_status_t ds_urma_mock_register_log_func(urma_log_cb_t log_cb)
{
    (void)log_cb;
    return URMA_SUCCESS;
}

urma_status_t ds_urma_mock_unregister_log_func(void)
{
    return URMA_SUCCESS;
}

// --- device (5) ---

urma_device_t **ds_urma_mock_get_device_list(int *dev_num)
{
    auto &backend = datasystem::urma_mock::MockUrmaBackend::Instance();
    if (!backend.IsInitialized()) {
        if (dev_num) {
            *dev_num = 0;
        }
        return nullptr;
    }
    auto devs = backend.GetDevices();
    if (devs.empty()) {
        if (dev_num) {
            *dev_num = 0;
        }
        return nullptr;
    }
    // We need to return a stable array of urma_device_t* pointers.
    // The dev->privRawDev_ field would have been set in Init; we maintain
    // a side table lookup. For simplicity, allocate a one-shot array.
    auto **out = new urma_device_t *[devs.size() + 1];
    auto &tables = datasystem::urma_mock::Tables();
    std::lock_guard<std::mutex> lock(tables.mu);
    for (size_t i = 0; i < devs.size(); ++i) {
        // Find the raw device in side table by MockDevice* match.
        for (auto &[raw, mockDev] : tables.dev) {
            if (mockDev.get() == devs[i]) {
                out[i] = raw;
                break;
            }
        }
    }
    out[devs.size()] = nullptr;
    if (dev_num) {
        *dev_num = static_cast<int>(devs.size());
    }
    return out;
}

urma_device_t *ds_urma_mock_get_device_by_name(char *name)
{
    auto &backend = datasystem::urma_mock::MockUrmaBackend::Instance();
    auto *dev = backend.GetDeviceByName(name == nullptr ? std::string{} : std::string(name));
    if (dev == nullptr) {
        return nullptr;
    }
    auto &tables = datasystem::urma_mock::Tables();
    std::lock_guard<std::mutex> lock(tables.mu);
    for (auto &[raw, mockDev] : tables.dev) {
        if (mockDev.get() == dev) {
            return raw;
        }
    }
    return nullptr;
}

urma_status_t ds_urma_mock_query_device(urma_device_t *device, urma_device_attr_t *attr)
{
    (void)device;
    if (attr == nullptr) {
        return URMA_E_INVALID;
    }
    std::memset(attr, 0, sizeof(*attr));
    attr->dev_type = K_MOCK_DEVICE_TYPE;
    attr->eid_cnt = K_MOCK_EID_COUNT;
    attr->max_jetty = K_MOCK_DEVICE_LIMIT;
    attr->max_jfc = K_MOCK_DEVICE_LIMIT;
    attr->max_jfr = K_MOCK_DEVICE_LIMIT;
    attr->max_send_wr = K_MOCK_WR_DEPTH;
    attr->max_recv_wr = K_MOCK_WR_DEPTH;
    attr->max_send_sge = K_MOCK_EID_COUNT;
    attr->max_recv_sge = K_MOCK_EID_COUNT;
    attr->max_inline_data = 0;
    attr->max_seg_cnt = K_MOCK_DEVICE_LIMIT;
    attr->page_size = K_MOCK_PAGE_SIZE;
    attr->dev_cap.max_write_size = K_MOCK_MAX_TRANSFER_SIZE;
    attr->dev_cap.max_read_size = K_MOCK_MAX_TRANSFER_SIZE;
    attr->dev_cap.max_send_wr = K_MOCK_WR_DEPTH;
    attr->dev_cap.max_recv_wr = K_MOCK_WR_DEPTH;
    attr->dev_cap.max_jetty = K_MOCK_DEVICE_LIMIT;
    attr->dev_cap.max_jfc = K_MOCK_DEVICE_LIMIT;
    attr->dev_cap.max_jfc_depth = K_MOCK_WR_DEPTH;
    attr->dev_cap.max_jfr = K_MOCK_DEVICE_LIMIT;
    attr->dev_cap.page_size = K_MOCK_PAGE_SIZE;
    attr->dev_cap.priority_info[0].SL = K_MOCK_PRIORITY_SERVICE_LEVEL;
    return URMA_SUCCESS;
}

urma_eid_info_t *ds_urma_mock_get_eid_list(urma_device_t *device, uint32_t *eid_count)
{
    (void)device;
    if (eid_count) {
        *eid_count = K_MOCK_EID_COUNT;
    }
    auto *out = new urma_eid_info_t{};
    std::memset(out, 0, sizeof(*out));
    out->eid = datasystem::urma_mock::StableMockDeviceEid();
    out->eid_index = 0;
    return out;
}

void ds_urma_mock_free_eid_list(urma_eid_info_t *eid_list)
{
    delete eid_list;
}

// --- context (4) ---

urma_context_t *ds_urma_mock_create_context(urma_device_t *device, uint32_t eid_index)
{
    (void)eid_index;
    auto &backend = datasystem::urma_mock::MockUrmaBackend::Instance();
    auto &tables = datasystem::urma_mock::Tables();
    datasystem::urma_mock::MockDevice *dev = nullptr;
    {
        std::lock_guard<std::mutex> lock(tables.mu);
        auto it = tables.dev.find(device);
        if (it != tables.dev.end()) {
            dev = it->second.get();
        }
    }
    if (dev == nullptr) {
        return nullptr;
    }
    return backend.CreateContextHandle(dev).first;
}

urma_status_t ds_urma_mock_delete_context(urma_context_t *context)
{
    auto &backend = datasystem::urma_mock::MockUrmaBackend::Instance();
    auto &tables = datasystem::urma_mock::Tables();
    std::shared_ptr<datasystem::urma_mock::MockContext> ctxShptr;
    datasystem::urma_mock::MockContext *ctx = nullptr;
    {
        std::lock_guard<std::mutex> lock(tables.mu);
        auto it = tables.ctx.find(context);
        if (it != tables.ctx.end()) {
            ctxShptr = it->second;
            ctx = ctxShptr.get();
        }
    }
    if (ctx == nullptr) {
        return URMA_E_INVALID;
    }
    backend.DeleteContext(ctxShptr.get());
    return URMA_SUCCESS;
}

urma_status_t ds_urma_mock_set_context_opt(urma_context_t *context, urma_opt_name_t opt_name, const void *opt_value,
                                           size_t opt_value_size)
{
    (void)context;
    (void)opt_name;
    (void)opt_value;
    (void)opt_value_size;
    return URMA_SUCCESS;
}

urma_status_t ds_urma_mock_user_ctl(urma_context_t *ctx, urma_user_ctl_in_t *in, urma_user_ctl_out_t *out)
{
    (void)ctx;
    (void)in;
    (void)out;
    return URMA_SUCCESS;
}

// --- jfce (2) ---

urma_jfce_t *ds_urma_mock_create_jfce(urma_context_t *context)
{
    auto &backend = datasystem::urma_mock::MockUrmaBackend::Instance();
    auto &tables = datasystem::urma_mock::Tables();
    std::shared_ptr<datasystem::urma_mock::MockContext> ctx;
    {
        std::lock_guard<std::mutex> lock(tables.mu);
        auto it = tables.ctx.find(context);
        if (it != tables.ctx.end()) {
            ctx = it->second;
        }
    }
    if (ctx == nullptr) {
        return nullptr;
    }
    // JFC event queue: a separate handle from the standard jfc. We allocate a
    // minimal jfce that wraps a no-op jfc. In mock mode we keep it as just a
    // shim pointer; the real work happens via the per-jetty sendJfc.
    auto *raw = new urma_jfce_t{};
    raw->jfce_id.id = backend.NextId();
    {
        std::lock_guard<std::mutex> lock(tables.mu);
        tables.jfce[raw] = ctx;
    }
    return raw;
}

urma_status_t ds_urma_mock_delete_jfce(urma_jfce_t *jfce)
{
    auto &tables = datasystem::urma_mock::Tables();
    std::lock_guard<std::mutex> lock(tables.mu);
    auto it = tables.jfce.find(jfce);
    if (it == tables.jfce.end()) {
        return URMA_E_INVALID;
    }
    delete jfce;
    tables.jfce.erase(it);
    return URMA_SUCCESS;
}

// --- jfc (6) ---

urma_jfc_t *ds_urma_mock_create_jfc(urma_context_t *context, const urma_jfc_cfg_t *config)
{
    (void)config;
    auto &backend = datasystem::urma_mock::MockUrmaBackend::Instance();
    auto &tables = datasystem::urma_mock::Tables();
    std::shared_ptr<datasystem::urma_mock::MockContext> ctx;
    {
        std::lock_guard<std::mutex> lock(tables.mu);
        auto it = tables.ctx.find(context);
        if (it != tables.ctx.end()) {
            ctx = it->second;
        }
    }
    if (ctx == nullptr) {
        return nullptr;
    }
    return backend.CreateJfcHandle(ctx).first;
}

urma_status_t ds_urma_mock_delete_jfc(urma_jfc_t *jfc)
{
    auto &backend = datasystem::urma_mock::MockUrmaBackend::Instance();
    auto &tables = datasystem::urma_mock::Tables();
    std::shared_ptr<datasystem::urma_mock::MockJfc> fjfc;
    {
        std::lock_guard<std::mutex> lock(tables.mu);
        fjfc = datasystem::urma_mock::FindMockObject(tables.jfc, jfc);
    }
    if (fjfc == nullptr) {
        return URMA_E_INVALID;
    }
    backend.DeleteJfc(fjfc.get());
    return URMA_SUCCESS;
}

urma_status_t ds_urma_mock_rearm_jfc(urma_jfc_t *jfc, bool enableEvents)
{
    auto &tables = datasystem::urma_mock::Tables();
    std::shared_ptr<datasystem::urma_mock::MockJfc> fjfc;
    {
        std::lock_guard<std::mutex> lock(tables.mu);
        fjfc = datasystem::urma_mock::FindMockObject(tables.jfc, jfc);
    }
    if (fjfc != nullptr) {
        datasystem::urma_mock::MockUrmaBackend::Instance().RearmJfc(fjfc.get(), enableEvents);
    }
    return URMA_SUCCESS;
}

int ds_urma_mock_wait_jfc(urma_jfce_t *jfce, int maxEvents, int timeoutMs, urma_jfc_t **evJfc)
{
    auto &tables = datasystem::urma_mock::Tables();
    if (evJfc != nullptr) {
        *evJfc = nullptr;
    }
    std::shared_ptr<datasystem::urma_mock::MockContext> ctx;
    {
        std::lock_guard<std::mutex> lock(tables.mu);
        auto it = tables.jfce.find(jfce);
        if (it != tables.jfce.end()) {
            ctx = it->second;
        }
    }
    if (ctx == nullptr) {
        return 0;
    }
    const auto deadline = timeoutMs < 0 ? std::chrono::steady_clock::time_point::max()
                                        : std::chrono::steady_clock::now() + std::chrono::milliseconds(timeoutMs);
    do {
        std::vector<std::pair<urma_jfc_t *, std::shared_ptr<datasystem::urma_mock::MockJfc>>> jfcs;
        {
            std::lock_guard<std::mutex> lock(tables.mu);
            if (tables.jfce.find(jfce) == tables.jfce.end()) {
                return 0;
            }
            jfcs.reserve(tables.jfc.size());
            for (auto &item : tables.jfc) {
                auto jfcCtx = item.second == nullptr ? nullptr : item.second->LockContext();
                if (jfcCtx == ctx) {
                    jfcs.emplace_back(item.first, item.second);
                }
            }
        }
        for (auto &[raw, mockJfc] : jfcs) {
            datasystem::urma_mock::MockJfc *readyJfc = nullptr;
            int cnt = mockJfc == nullptr ? 0 : mockJfc->Wait(maxEvents, K_NO_WAIT_MS, &readyJfc);
            if (cnt > 0) {
                if (evJfc != nullptr) {
                    *evJfc = raw;
                }
                return cnt;
            }
        }
        if (timeoutMs == 0) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(K_POLL_SLEEP_MS));
    } while (std::chrono::steady_clock::now() < deadline);
    return 0;
}

int ds_urma_mock_poll_jfc(urma_jfc_t *jfc, int maxCr, urma_cr_t *completeRecords)
{
    auto &tables = datasystem::urma_mock::Tables();
    std::shared_ptr<datasystem::urma_mock::MockJfc> fjfc;
    {
        std::lock_guard<std::mutex> lock(tables.mu);
        fjfc = datasystem::urma_mock::FindMockObject(tables.jfc, jfc);
    }
    if (fjfc == nullptr) {
        return 0;
    }
    return datasystem::urma_mock::MockUrmaBackend::Instance().PollJfc(fjfc.get(), maxCr, completeRecords);
}

void ds_urma_mock_ack_jfc(urma_jfc_t **evJfc, uint32_t *ackCnt, int num)
{
    (void)num;
    if (evJfc == nullptr || *evJfc == nullptr) {
        return;
    }
    auto &tables = datasystem::urma_mock::Tables();
    std::shared_ptr<datasystem::urma_mock::MockJfc> fjfc;
    {
        std::lock_guard<std::mutex> lock(tables.mu);
        fjfc = datasystem::urma_mock::FindMockObject(tables.jfc, *evJfc);
    }
    if (fjfc != nullptr) {
        datasystem::urma_mock::MockUrmaBackend::Instance().AckJfc(fjfc.get(), ackCnt ? *ackCnt : K_DEFAULT_ACK_COUNT);
    }
}

// --- jfr (2) ---

urma_jfr_t *ds_urma_mock_create_jfr(urma_context_t *context, const urma_jfr_cfg_t *config)
{
    (void)config;
    auto &backend = datasystem::urma_mock::MockUrmaBackend::Instance();
    auto &tables = datasystem::urma_mock::Tables();
    std::shared_ptr<datasystem::urma_mock::MockContext> ctxShptr;
    datasystem::urma_mock::MockContext *ctx = nullptr;
    {
        std::lock_guard<std::mutex> lock(tables.mu);
        auto it = tables.ctx.find(context);
        if (it != tables.ctx.end()) {
            ctxShptr = it->second;
            ctx = ctxShptr.get();
        }
    }
    if (ctx == nullptr) {
        return nullptr;
    }
    auto *raw = new urma_jfr_t{};
    raw->jfr_id.id = backend.NextId();
    {
        std::lock_guard<std::mutex> lock(tables.mu);
        tables.jfr[raw] = ctxShptr;  // jfr is essentially a no-op in mock mode
    }
    return raw;
}

urma_status_t ds_urma_mock_delete_jfr(urma_jfr_t *jfr)
{
    auto &tables = datasystem::urma_mock::Tables();
    std::lock_guard<std::mutex> lock(tables.mu);
    auto it = tables.jfr.find(jfr);
    if (it == tables.jfr.end()) {
        return URMA_E_INVALID;
    }
    delete jfr;
    tables.jfr.erase(it);
    return URMA_SUCCESS;
}

// --- jfs legacy ABI (3) ---

urma_jfs_t *ds_urma_mock_create_jfs(urma_context_t *context, const urma_jfs_cfg_t *config)
{
    urma_jetty_cfg_t jettyConfig{};
    if (config != nullptr) {
        jettyConfig.jfs_cfg = *config;
    }
    return reinterpret_cast<urma_jfs_t *>(ds_urma_mock_create_jetty(context, &jettyConfig));
}

urma_status_t ds_urma_mock_delete_jfs(urma_jfs_t *jfs)
{
    return ds_urma_mock_delete_jetty(reinterpret_cast<urma_jetty_t *>(jfs));
}

urma_status_t ds_urma_mock_modify_jfs(urma_jfs_t *jfs, urma_jfs_attr_t *attr)
{
    if (jfs == nullptr) {
        return URMA_E_INVALID;
    }
    auto &tables = datasystem::urma_mock::Tables();
    std::lock_guard<std::mutex> lock(tables.mu);
    auto it = tables.jetty.find(reinterpret_cast<urma_jetty_t *>(jfs));
    if (it == tables.jetty.end()) {
        return URMA_E_INVALID;
    }
    if (attr != nullptr && (attr->mask & JETTY_STATE) != 0) {
        jfs->state = attr->state;
    }
    return URMA_SUCCESS;
}

// --- jetty (3) ---

urma_jetty_t *ds_urma_mock_create_jetty(urma_context_t *context, urma_jetty_cfg_t *config)
{
    (void)config;
    auto &backend = datasystem::urma_mock::MockUrmaBackend::Instance();
    auto &tables = datasystem::urma_mock::Tables();
    std::shared_ptr<datasystem::urma_mock::MockContext> ctxShptr;
    datasystem::urma_mock::MockContext *ctx = nullptr;
    {
        std::lock_guard<std::mutex> lock(tables.mu);
        auto it = tables.ctx.find(context);
        if (it != tables.ctx.end()) {
            ctxShptr = it->second;
            ctx = ctxShptr.get();
        }
    }
    if (ctx == nullptr) {
        return nullptr;
    }
    // Pull a sendJfc from the jfs_cfg if provided; otherwise create one.
    datasystem::urma_mock::MockJfc *sendJfc = nullptr;
    if (config != nullptr && config->jfs_cfg.jfc != nullptr) {
        std::lock_guard<std::mutex> lock(tables.mu);
        auto it = tables.jfc.find(static_cast<urma_jfc_t *>(config->jfs_cfg.jfc));
        if (it != tables.jfc.end()) {
            sendJfc = it->second.get();
        }
    }
    if (sendJfc == nullptr) {
        sendJfc = backend.CreateJfc(ctxShptr);
    }
    auto [rawJetty, jetty] = backend.CreateJettyHandle(ctx, sendJfc);
    if (jetty == nullptr) {
        return nullptr;
    }
    return rawJetty;
}

urma_status_t ds_urma_mock_delete_jetty(urma_jetty_t *jetty)
{
    auto &backend = datasystem::urma_mock::MockUrmaBackend::Instance();
    auto &tables = datasystem::urma_mock::Tables();
    datasystem::urma_mock::MockJetty *fjetty = nullptr;
    {
        std::lock_guard<std::mutex> lock(tables.mu);
        auto it = tables.jetty.find(jetty);
        if (it != tables.jetty.end()) {
            fjetty = it->second.get();
        }
    }
    if (fjetty == nullptr) {
        return URMA_E_INVALID;
    }
    backend.DeleteJetty(fjetty);
    return URMA_SUCCESS;
}

urma_status_t ds_urma_mock_get_rjetty(urma_jetty_t *jetty, urma_rjetty_t **rjetty, uint32_t *length)
{
    if (jetty == nullptr || rjetty == nullptr || length == nullptr) {
        return URMA_E_INVALID;
    }
    auto *raw = new urma_rjetty_t{};
    raw->jetty_id = jetty->jetty_id;
    *rjetty = raw;
    *length = sizeof(*raw);
    return URMA_SUCCESS;
}

void ds_urma_mock_put_rjetty(urma_rjetty_t *rjetty)
{
    delete rjetty;
}

urma_status_t ds_urma_mock_modify_jetty(urma_jetty_t *jetty, urma_jetty_attr_t *attr)
{
    (void)attr;
    (void)ShouldReturnFromStatusInject("urma.ModifyJettyToError");
    auto &tables = datasystem::urma_mock::Tables();
    {
        std::lock_guard<std::mutex> lock(tables.mu);
        auto it = tables.jetty.find(jetty);
        if (it != tables.jetty.end()) {
            return URMA_SUCCESS;
        }
    }
    return URMA_E_INVALID;
}

// --- jfr import legacy ABI (3) ---

urma_target_jetty_t *ds_urma_mock_import_jfr(urma_context_t *context, const urma_rjfr_t *remoteJfr, urma_token_t *token)
{
    (void)remoteJfr;
    if (ShouldReturnFromStatusInject("urma.import_jfr")) {
        return nullptr;
    }
    return ds_urma_mock_import_jetty(context, nullptr, token);
}

urma_status_t ds_urma_mock_advise_jfr(urma_jfs_t *jfs, urma_target_jetty_t *tjfr)
{
    auto &tables = datasystem::urma_mock::Tables();
    std::lock_guard<std::mutex> lock(tables.mu);
    auto jettyIt = tables.jetty.find(reinterpret_cast<urma_jetty_t *>(jfs));
    auto tjettyIt = tables.tjetty.find(tjfr);
    if (jettyIt == tables.jetty.end() || tjettyIt == tables.tjetty.end()) {
        return URMA_E_INVALID;
    }
    jettyIt->second->SetDstTjetty(tjettyIt->second.get());
    return URMA_SUCCESS;
}

urma_status_t ds_urma_mock_unimport_jfr(urma_target_jetty_t *tjfr)
{
    return ds_urma_mock_unimport_jetty(tjfr);
}

// --- seg (4) ---

urma_target_seg_t *ds_urma_mock_register_seg(urma_context_t *context, const urma_seg_cfg_t *config)
{
    if (config == nullptr) {
        return nullptr;
    }
    auto &backend = datasystem::urma_mock::MockUrmaBackend::Instance();
    auto &tables = datasystem::urma_mock::Tables();
    std::shared_ptr<datasystem::urma_mock::MockContext> ctxShptr;
    datasystem::urma_mock::MockContext *ctx = nullptr;
    {
        std::lock_guard<std::mutex> lock(tables.mu);
        auto it = tables.ctx.find(context);
        if (it != tables.ctx.end()) {
            ctxShptr = it->second;
            ctx = ctxShptr.get();
        }
    }
    if (ctx == nullptr) {
        ctx = CreateDefaultContextForDirectRegister(backend);
        if (ctx == nullptr) {
            return nullptr;
        }
    }
    std::string key = std::to_string(config->token_value.value);
    if (config->token_value.value == K_INVALID_TOKEN) {
        // Token 0 means "any" — fall back to a (va,len) key for the call so
        // multiple zero-token segments on the same context don't collide.
        key = std::to_string(config->va) + ":" + std::to_string(config->len);
    }
    // pass config->va so RegisterSeg can adopt the business memfd fd
    // by reverse-engineering it from /proc/self/maps.
    urma_target_seg_t *rawSeg = nullptr;
    auto *seg = backend.RegisterSeg(ctx, config->len, key, config->va, &rawSeg);
    // spin up the UDS listener so peers can HELLO_ACK against us.
    if (seg != nullptr) {
        auto &endpointService = datasystem::urma_mock::UdsEndpointService::Instance();
        endpointService.EnsureListener();
        if (rawSeg != nullptr) {
            endpointService.RegisterSegmentEndpoint(rawSeg->seg, config->token_value.value);
        }
    }
    if (seg == nullptr) {
        return nullptr;
    }
    return rawSeg;
}

urma_target_seg_t *ds_urma_mock_import_seg(urma_context_t *context, urma_seg_t *seg, urma_token_t *token, int flags,
                                           urma_import_seg_flag_t importFlag)
{
    (void)flags;
    (void)importFlag;
    auto &backend = datasystem::urma_mock::MockUrmaBackend::Instance();
    auto &tables = datasystem::urma_mock::Tables();
    std::shared_ptr<datasystem::urma_mock::MockContext> ctxShptr;
    datasystem::urma_mock::MockContext *ctx = nullptr;
    {
        std::lock_guard<std::mutex> lock(tables.mu);
        auto it = tables.ctx.find(context);
        if (it != tables.ctx.end()) {
            ctxShptr = it->second;
            ctx = ctxShptr.get();
        }
    }
    if (ctx == nullptr) {
        return nullptr;
    }
    std::string tok = token ? std::to_string(token->value) : "";
    // Token may be 0 for "import any"; allocate a fresh seg of default size.
    uint64_t size = seg ? seg->len : K_DEFAULT_IMPORT_SEG_SIZE;
    uint64_t remoteVa = seg ? seg->ubva.va : 0;
    if (seg != nullptr) {
        uint64_t tokenValue = token != nullptr ? token->value : seg->token_id;
        auto &endpointService = datasystem::urma_mock::UdsEndpointService::Instance();
        auto ep = endpointService.LookupImportEndpoint(tokenValue, remoteVa);
        if (ep.instanceId.empty()) {
            ep = endpointService.LookupSegmentEndpoint(*seg, tokenValue);
            if (ep.instanceId.empty()) {
                ep = datasystem::urma_mock::SegmentEndpointRegistry::Instance().Lookup(tokenValue, remoteVa);
            }
            if (!ep.instanceId.empty()) {
                endpointService.RegisterImportEndpoint(tokenValue, ep);
            }
        }
    }
    urma_target_seg_t *rawSeg = nullptr;
    auto *newSeg = backend.ImportSeg(ctx, size, tok, tok, remoteVa, &rawSeg);
    if (newSeg == nullptr) {
        return nullptr;
    }
    return rawSeg;
}

urma_status_t ds_urma_mock_get_seg_ctx(urma_target_seg_t *tseg, urma_seg_t **seg, uint32_t *size)
{
    if (tseg == nullptr || seg == nullptr || size == nullptr) {
        return URMA_E_INVALID;
    }
    *seg = &tseg->seg;
    *size = sizeof(tseg->seg);
    return URMA_SUCCESS;
}

void ds_urma_mock_put_seg_ctx(urma_seg_t *seg)
{
    (void)seg;
}

urma_status_t ds_urma_mock_unregister_seg(urma_target_seg_t *seg)
{
    auto &backend = datasystem::urma_mock::MockUrmaBackend::Instance();
    auto &tables = datasystem::urma_mock::Tables();
    std::shared_ptr<datasystem::urma_mock::MockSeg> fseg;
    {
        std::lock_guard<std::mutex> lock(tables.mu);
        fseg = datasystem::urma_mock::FindMockObject(tables.tseg, seg);
    }
    if (fseg == nullptr) {
        return URMA_E_INVALID;
    }
    datasystem::urma_mock::UdsEndpointService::Instance().UnregisterSegmentEndpoint(seg->seg,
                                                                                    ParseSegmentToken(fseg->GetKey()));
    backend.UnregisterSeg(fseg.get());
    return URMA_SUCCESS;
}

urma_status_t ds_urma_mock_unimport_seg(urma_target_seg_t *seg)
{
    auto &backend = datasystem::urma_mock::MockUrmaBackend::Instance();
    auto &tables = datasystem::urma_mock::Tables();
    std::shared_ptr<datasystem::urma_mock::MockSeg> fseg;
    {
        std::lock_guard<std::mutex> lock(tables.mu);
        fseg = datasystem::urma_mock::FindMockObject(tables.tseg, seg);
    }
    if (fseg == nullptr) {
        return URMA_E_INVALID;
    }
    backend.UnimportSeg(fseg.get());
    return URMA_SUCCESS;
}

// --- send (1) ---

urma_status_t ds_urma_mock_post_jetty_send_wr(urma_jetty_t *jetty, urma_jfs_wr_t *wr, urma_jfs_wr_t **badWr)
{
    auto &backend = datasystem::urma_mock::MockUrmaBackend::Instance();
    auto &tables = datasystem::urma_mock::Tables();
    std::shared_ptr<datasystem::urma_mock::MockJetty> fjetty;
    {
        std::lock_guard<std::mutex> lock(tables.mu);
        fjetty = datasystem::urma_mock::FindMockObject(tables.jetty, jetty);
    }
    if (fjetty == nullptr) {
        if (badWr) {
            *badWr = wr;
        }
        return URMA_E_INVALID;
    }
    return backend.PostSendWr(fjetty.get(), wr, badWr);
}

urma_status_t ds_urma_mock_post_jfs_wr(urma_jfs_t *jfs, urma_jfs_wr_t *wr, urma_jfs_wr_t **badWr)
{
    return ds_urma_mock_post_jetty_send_wr(reinterpret_cast<urma_jetty_t *>(jfs), wr, badWr);
}

urma_status_t ds_urma_mock_write(urma_jfs_t *jfs, urma_target_jetty_t *tjfr, urma_target_seg_t *remoteSeg,
                                 urma_target_seg_t *localSeg, uint64_t remoteAddr, uint64_t localAddr, uint64_t length,
                                 urma_jfs_wr_flag_t flag, uint64_t userCtx)
{
    urma_sge_t localSge{ localAddr, static_cast<uint32_t>(length), 0, 0, localSeg, nullptr };
    urma_sge_t remoteSge{ remoteAddr, static_cast<uint32_t>(length), 0, 0, remoteSeg, nullptr };
    urma_jfs_wr_t wr{};
    wr.opcode = URMA_OPC_WRITE;
    wr.flag = flag;
    wr.tjetty = tjfr;
    wr.user_ctx = userCtx;
    wr.rw.src = { &localSge, 1 };
    wr.rw.dst = { &remoteSge, 1 };
    return ds_urma_mock_post_jfs_wr(jfs, &wr, nullptr);
}

urma_status_t ds_urma_mock_read(urma_jfs_t *jfs, urma_target_jetty_t *tjfr, urma_target_seg_t *localSeg,
                                urma_target_seg_t *remoteSeg, uint64_t localAddr, uint64_t remoteAddr, uint64_t length,
                                urma_jfs_wr_flag_t flag, uint64_t userCtx)
{
    urma_sge_t remoteSge{ remoteAddr, static_cast<uint32_t>(length), 0, 0, remoteSeg, nullptr };
    urma_sge_t localSge{ localAddr, static_cast<uint32_t>(length), 0, 0, localSeg, nullptr };
    urma_jfs_wr_t wr{};
    wr.opcode = URMA_OPC_READ;
    wr.flag = flag;
    wr.tjetty = tjfr;
    wr.user_ctx = userCtx;
    wr.rw.src = { &remoteSge, 1 };
    wr.rw.dst = { &localSge, 1 };
    return ds_urma_mock_post_jfs_wr(jfs, &wr, nullptr);
}

// --- jfc wait/ack (cont.) + tjetty (3) ---

urma_target_jetty_t *ds_urma_mock_import_jetty(urma_context_t *context, urma_rjetty_t *remoteJetty, urma_token_t *token)
{
    (void)remoteJetty;
    if (ShouldReturnFromStatusInject("urma.import_jetty")) {
        return nullptr;
    }
    auto &backend = datasystem::urma_mock::MockUrmaBackend::Instance();
    auto &tables = datasystem::urma_mock::Tables();
    std::shared_ptr<datasystem::urma_mock::MockContext> ctxShptr;
    datasystem::urma_mock::MockContext *ctx = nullptr;
    {
        std::lock_guard<std::mutex> lock(tables.mu);
        auto it = tables.ctx.find(context);
        if (it != tables.ctx.end()) {
            ctxShptr = it->second;
            ctx = ctxShptr.get();
        }
    }
    if (ctx == nullptr) {
        return nullptr;
    }
    // Cross-process ST imports Jetty before importing peer segments. Real URMA
    // allows that ordering, so mock creates the target Jetty now and resolves
    // the destination segment lazily when PostSendWr sees the concrete dst SGE.
    // Do not pre-bind a segment by token here: all DS mock URMA segments use the
    // same token, and a full ST process can retain same-token segments from
    // earlier tests or forked children.
    std::string tok = token ? std::to_string(token->value) : "";
    datasystem::urma_mock::MockSeg *remoteSeg = nullptr;
    datasystem::urma_mock::MockJfc *remoteJfc = nullptr;
    // Create a recv jfc on the local ctx for completion delivery.
    remoteJfc = backend.CreateJfc(ctxShptr);
    if (remoteJfc == nullptr) {
        return nullptr;
    }
    auto [rawTjetty, tjt] = backend.ImportJettyHandle(ctx, remoteSeg, remoteJfc, tok);
    if (tjt == nullptr) {
        backend.DeleteJfc(remoteJfc);
        return nullptr;
    }
    // Bind only jetties on the same context that are waiting for a destination. In a
    // multi-process cluster, each import_jetty call is paired with one
    // create_jetty call on the peer; both share the same ctx on the
    // importer side. Walking all jetties in the backend can bind a concurrent
    // import to the wrong context.
    {
        std::lock_guard<std::mutex> lock(tables.mu);
        for (auto &[raw, mockJetty] : tables.jetty) {
            (void)raw;
            if (mockJetty != nullptr && mockJetty->GetContext() == ctx && mockJetty->GetDstTjetty() == nullptr) {
                mockJetty->SetDstTjetty(tjt);
            }
        }
    }
    return rawTjetty;
}

urma_status_t ds_urma_mock_unimport_jetty(urma_target_jetty_t *tjetty)
{
    auto &backend = datasystem::urma_mock::MockUrmaBackend::Instance();
    auto &tables = datasystem::urma_mock::Tables();
    std::shared_ptr<datasystem::urma_mock::MockTjetty> ftjt;
    {
        std::lock_guard<std::mutex> lock(tables.mu);
        ftjt = datasystem::urma_mock::FindMockObject(tables.tjetty, tjetty);
    }
    if (ftjt == nullptr) {
        return URMA_E_INVALID;
    }
    backend.UnimportJetty(ftjt.get());
    return URMA_SUCCESS;
}

// --- async (2) ---

urma_status_t ds_urma_mock_get_async_event(urma_context_t *context, urma_async_event_t *event)
{
    if (context != nullptr && context->async_fd >= 0) {
        uint64_t ready = 0;
        auto ret = read(context->async_fd, &ready, sizeof(ready));
        if (ret < 0 && errno != EAGAIN && errno != EWOULDBLOCK) {
            return URMA_E_FAIL;
        }
    }
    return datasystem::urma_mock::MockUrmaBackend::Instance().GetAsyncEvent(event);
}

void ds_urma_mock_ack_async_event(urma_async_event_t *event)
{
    (void)event;
}

// --- perf (3) ---

urma_status_t ds_urma_mock_start_perf(void)
{
    return URMA_SUCCESS;
}
urma_status_t ds_urma_mock_stop_perf(void)
{
    return URMA_SUCCESS;
}
urma_status_t ds_urma_mock_get_perf_info(char *perfBuf, uint32_t *length)
{
    (void)perfBuf;
    if (length) {
        *length = 0;
    }
    return URMA_SUCCESS;
}
