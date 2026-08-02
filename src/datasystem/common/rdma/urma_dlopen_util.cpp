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

#include "datasystem/common/rdma/urma_dlopen_util.h"

// ============================================================================
// Real URMA path: liburma is linked at compile time via -lurma (no dlopen).
// Each ds_urma_* shim directly forwards to the real URMA symbol.
// ============================================================================
#ifndef USE_URMA_MOCK

#include "datasystem/common/log/log.h"

namespace datasystem {
namespace urma_dlopen {

bool Init()
{
    // Symbols are resolved at link time; nothing to load at runtime.
    return true;
}

bool IsAvailable()
{
    return true;
}

void Cleanup()
{
    // No dlopen handles to close; liburma is linked into the binary.
}

}  // namespace urma_dlopen
}  // namespace datasystem

// Direct-call shims: forward each ds_urma_* to the real URMA symbol.
// The ds_ prefix avoids global symbol conflicts when liburma is linked.

urma_status_t ds_urma_init(const urma_init_attr_t *attr)
{
    return urma_init(const_cast<urma_init_attr_t *>(attr));
}

urma_status_t ds_urma_uninit(void)
{
    return urma_uninit();
}

urma_status_t ds_urma_register_log_func(urma_log_cb_t log_cb)
{
    return urma_register_log_func(log_cb);
}

urma_status_t ds_urma_unregister_log_func(void)
{
    return urma_unregister_log_func();
}

urma_device_t **ds_urma_get_device_list(int *dev_num)
{
    return urma_get_device_list(dev_num);
}

urma_device_t *ds_urma_get_device_by_name(char *name)
{
    return urma_get_device_by_name(name);
}

urma_status_t ds_urma_query_device(urma_device_t *device, urma_device_attr_t *attr)
{
    return urma_query_device(device, attr);
}

urma_eid_info_t *ds_urma_get_eid_list(urma_device_t *device, uint32_t *eid_count)
{
    return urma_get_eid_list(device, eid_count);
}

void ds_urma_free_eid_list(urma_eid_info_t *eid_list)
{
    urma_free_eid_list(eid_list);
}

urma_context_t *ds_urma_create_context(urma_device_t *device, uint32_t eid_index)
{
    return urma_create_context(device, eid_index);
}

urma_status_t ds_urma_delete_context(urma_context_t *context)
{
    return urma_delete_context(context);
}

urma_status_t ds_urma_set_context_opt(urma_context_t *context, urma_opt_name_t opt_name, const void *opt_value,
                                      size_t opt_len)
{
    return urma_set_context_opt(context, opt_name, opt_value, opt_len);
}

urma_status_t ds_urma_user_ctl(urma_context_t *ctx, urma_user_ctl_in_t *in, urma_user_ctl_out_t *out)
{
    return urma_user_ctl(ctx, in, out);
}

urma_jfce_t *ds_urma_create_jfce(urma_context_t *context)
{
    return urma_create_jfce(context);
}

urma_status_t ds_urma_delete_jfce(urma_jfce_t *jfce)
{
    return urma_delete_jfce(jfce);
}

urma_jfc_t *ds_urma_create_jfc(urma_context_t *context, const urma_jfc_cfg_t *config)
{
    return urma_create_jfc(context, const_cast<urma_jfc_cfg_t *>(config));
}

urma_status_t ds_urma_delete_jfc(urma_jfc_t *jfc)
{
    return urma_delete_jfc(jfc);
}

urma_status_t ds_urma_rearm_jfc(urma_jfc_t *jfc, bool enable_events)
{
    return urma_rearm_jfc(jfc, enable_events);
}

urma_jfr_t *ds_urma_create_jfr(urma_context_t *context, const urma_jfr_cfg_t *config)
{
    return urma_create_jfr(context, const_cast<urma_jfr_cfg_t *>(config));
}

urma_status_t ds_urma_delete_jfr(urma_jfr_t *jfr)
{
    return urma_delete_jfr(jfr);
}

urma_jetty_t *ds_urma_create_jetty(urma_context_t *context, urma_jetty_cfg_t *config)
{
    return urma_create_jetty(context, config);
}

urma_status_t ds_urma_delete_jetty(urma_jetty_t *jetty)
{
    return urma_delete_jetty(jetty);
}

urma_status_t ds_urma_modify_jetty(urma_jetty_t *jetty, urma_jetty_attr_t *attr)
{
    return urma_modify_jetty(jetty, attr);
}

urma_target_seg_t *ds_urma_register_seg(urma_context_t *context, const urma_seg_cfg_t *config)
{
    return urma_register_seg(context, const_cast<urma_seg_cfg_t *>(config));
}

int ds_urma_wait_jfc(urma_jfce_t *jfce, int max_events, int timeout_ms, urma_jfc_t **ev_jfc)
{
    return urma_wait_jfc(jfce, max_events, timeout_ms, ev_jfc);
}

int ds_urma_poll_jfc(urma_jfc_t *jfc, int max_cr, urma_cr_t *complete_records)
{
    return urma_poll_jfc(jfc, max_cr, complete_records);
}

void ds_urma_ack_jfc(urma_jfc_t **ev_jfc, uint32_t *ack_cnt, int num)
{
    urma_ack_jfc(ev_jfc, ack_cnt, num);
}

urma_target_jetty_t *ds_urma_import_jetty(urma_context_t *context, urma_rjetty_t *remote_jetty,
                                          urma_token_t *token)
{
    return urma_import_jetty(context, remote_jetty, token);
}

urma_status_t ds_urma_unimport_jetty(urma_target_jetty_t *tjetty)
{
    return urma_unimport_jetty(tjetty);
}

urma_status_t ds_urma_get_rjetty(urma_jetty_t *jetty, urma_rjetty_t **rjetty, uint32_t *length)
{
    return urma_get_rjetty(jetty, rjetty, length);
}

void ds_urma_put_rjetty(urma_rjetty_t *rjetty)
{
    urma_put_rjetty(rjetty);
}

urma_status_t ds_urma_post_jetty_send_wr(urma_jetty_t *jetty, urma_jfs_wr_t *wr, urma_jfs_wr_t **bad_wr)
{
    return urma_post_jetty_send_wr(jetty, wr, bad_wr);
}

urma_target_seg_t *ds_urma_import_seg(urma_context_t *context, urma_seg_t *seg, urma_token_t *token, int flags,
                                      urma_import_seg_flag_t import_flag)
{
    return urma_import_seg(context, seg, token, flags, import_flag);
}

urma_status_t ds_urma_get_seg_ctx(urma_target_seg_t *tseg, urma_seg_t **seg, uint32_t *size)
{
    return urma_get_seg_ctx(tseg, seg, size);
}

void ds_urma_put_seg_ctx(urma_seg_t *seg)
{
    urma_put_seg_ctx(seg);
}

urma_status_t ds_urma_unregister_seg(urma_target_seg_t *seg)
{
    return urma_unregister_seg(seg);
}

urma_status_t ds_urma_unimport_seg(urma_target_seg_t *seg)
{
    return urma_unimport_seg(seg);
}

urma_status_t ds_urma_get_async_event(urma_context_t *context, urma_async_event_t *event)
{
    return urma_get_async_event(context, event);
}

void ds_urma_ack_async_event(urma_async_event_t *event)
{
    urma_ack_async_event(event);
}

urma_status_t ds_urma_start_perf(void)
{
    return urma_start_perf();
}

urma_status_t ds_urma_stop_perf(void)
{
    return urma_stop_perf();
}

urma_status_t ds_urma_get_perf_info(char *perf_buf, uint32_t *length)
{
    return urma_get_perf_info(perf_buf, length);
}

// ============================================================================
// Mock URMA path: dev/CI without liburma.so / RDMA hardware.
// Uses a weak-symbol dispatch table (ds_urma_mock_dlopen_ops) to resolve
// ds_urma_* to in-process mock backend functions instead of calling dlsym.
// ============================================================================
#else  // USE_URMA_MOCK

#include "securec.h"

#include "datasystem/common/log/log.h"

struct UrmaMockDlopenOps {
    void *(*handle)();
    bool (*init)();
    void (*cleanup)();
    void *(*symbol)(const char *name);
};

extern "C" const UrmaMockDlopenOps *ds_urma_mock_dlopen_ops() __attribute__((weak));

namespace {
bool g_init = false;

const UrmaMockDlopenOps *GetMockOps()
{
    if (ds_urma_mock_dlopen_ops == nullptr) {
        LOG(ERROR) << "[UrmaDlopen] URMA_MOCK provider is not linked";
        return nullptr;
    }
    auto *ops = ds_urma_mock_dlopen_ops();
    if (ops == nullptr || ops->handle == nullptr || ops->init == nullptr || ops->cleanup == nullptr
        || ops->symbol == nullptr) {
        LOG(ERROR) << "[UrmaDlopen] URMA_MOCK provider ops are incomplete";
        return nullptr;
    }
    return ops;
}

bool EnsureMockUrmaDlopenInitialized()
{
    if (g_init) {
        return true;
    }
    auto *ops = GetMockOps();
    if (ops == nullptr) {
        return false;
    }
    LOG(INFO) << "[UrmaDlopen] URMA_MOCK mode: using mock dispatch table";
    if (!ops->init()) {
        LOG(ERROR) << "[UrmaDlopen] URMA_MOCK provider init failed";
        return false;
    }
    g_init = true;
    return true;
}

void *LoadUrmaSymbol(const char *name)
{
    if (!EnsureMockUrmaDlopenInitialized()) {
        return nullptr;
    }
    auto *ops = GetMockOps();
    if (ops == nullptr) {
        return nullptr;
    }
    auto *mockSym = ops->symbol(name);
    if (mockSym == nullptr) {
        LOG(ERROR) << "[UrmaDlopen] mock symbol not found: " << name;
    }
    return mockSym;
}

template <typename Fn>
Fn LoadFn(const char *name)
{
    void *sym = LoadUrmaSymbol(name);
    if (!sym) {
        return nullptr;
    }
    Fn fn = nullptr;
    int ret = memcpy_s(&fn, sizeof(Fn), &sym, sizeof(void *));
    if (ret != 0) {
        LOG(ERROR) << "[UrmaDlopen] memcpy_s failed while casting URMA symbol: " << ret;
        return nullptr;
    }
    return fn;
}

template <typename Ret, typename Fn, typename... Args>
Ret CallRet(const char *name, Ret fallback, Args... args)
{
    auto fn = LoadFn<Fn>(name);
    if (!fn) {
        return fallback;
    }
    return fn(args...);
}

template <typename Fn, typename... Args>
void CallVoid(const char *name, Args... args)
{
    auto fn = LoadFn<Fn>(name);
    if (!fn) {
        return;
    }
    fn(args...);
}

template <typename Fn, typename... Args>
void *CallPtr(const char *name, Args... args)
{
    auto fn = LoadFn<Fn>(name);
    if (!fn) {
        return nullptr;
    }
    return fn(args...);
}
}  // namespace

namespace datasystem {
namespace urma_dlopen {

bool Init()
{
    return EnsureMockUrmaDlopenInitialized();
}

bool IsAvailable()
{
    return g_init;
}

void Cleanup()
{
    auto *ops = GetMockOps();
    if (ops != nullptr) {
        ops->cleanup();
    }
    g_init = false;
}

}  // namespace urma_dlopen
}  // namespace datasystem

static constexpr urma_status_t kUrmaDlopenErrorStatus = static_cast<urma_status_t>(-1);

urma_status_t ds_urma_init(const urma_init_attr_t *attr)
{
    return CallRet< urma_status_t, decltype(&ds_urma_init)>("urma_init", kUrmaDlopenErrorStatus,
                                                                              attr);
}

urma_status_t ds_urma_uninit(void)
{
    return CallRet< urma_status_t, decltype(&ds_urma_uninit)>("urma_uninit", kUrmaDlopenErrorStatus);
}

urma_status_t ds_urma_register_log_func(urma_log_cb_t log_cb)
{
    return CallRet< urma_status_t, decltype(&ds_urma_register_log_func)>(
        "urma_register_log_func", kUrmaDlopenErrorStatus, log_cb);
}

urma_status_t ds_urma_unregister_log_func(void)
{
    return CallRet< urma_status_t, decltype(&ds_urma_unregister_log_func)>("urma_unregister_log_func",
                                                                                             kUrmaDlopenErrorStatus);
}

urma_device_t **ds_urma_get_device_list(int *dev_num)
{
    return static_cast<urma_device_t **>(CallPtr<decltype(&ds_urma_get_device_list)>("urma_get_device_list", dev_num));
}

urma_device_t *ds_urma_get_device_by_name(char *name)
{
    return static_cast<urma_device_t *>(
        CallPtr<decltype(&ds_urma_get_device_by_name)>("urma_get_device_by_name", name));
}

urma_status_t ds_urma_query_device(urma_device_t *device, urma_device_attr_t *attr)
{
    return CallRet< urma_status_t, decltype(&ds_urma_query_device)>(
        "urma_query_device", kUrmaDlopenErrorStatus, device, attr);
}

urma_eid_info_t *ds_urma_get_eid_list(urma_device_t *device, uint32_t *eid_count)
{
    return static_cast<urma_eid_info_t *>(
        CallPtr<decltype(&ds_urma_get_eid_list)>("urma_get_eid_list", device, eid_count));
}

void ds_urma_free_eid_list(urma_eid_info_t *eid_list)
{
    CallVoid<decltype(&ds_urma_free_eid_list)>("urma_free_eid_list", eid_list);
}

urma_context_t *ds_urma_create_context(urma_device_t *device, uint32_t eid_index)
{
    return static_cast<urma_context_t *>(
        CallPtr<decltype(&ds_urma_create_context)>("urma_create_context", device, eid_index));
}

urma_status_t ds_urma_delete_context(urma_context_t *context)
{
    return CallRet< urma_status_t, decltype(&ds_urma_delete_context)>(
        "urma_delete_context", kUrmaDlopenErrorStatus, context);
}

urma_status_t ds_urma_set_context_opt(urma_context_t *context, urma_opt_name_t opt_name, const void *opt_value,
                                      size_t opt_len)
{
    return CallRet< urma_status_t, decltype(&ds_urma_set_context_opt)>(
        "urma_set_context_opt", kUrmaDlopenErrorStatus, context, opt_name, opt_value, opt_len);
}

urma_status_t ds_urma_user_ctl(urma_context_t *ctx, urma_user_ctl_in_t *in, urma_user_ctl_out_t *out)
{
    return CallRet< urma_status_t, decltype(&ds_urma_user_ctl)>("urma_user_ctl",
                                                                                  kUrmaDlopenErrorStatus, ctx, in, out);
}

urma_jfce_t *ds_urma_create_jfce(urma_context_t *context)
{
    return static_cast<urma_jfce_t *>(CallPtr<decltype(&ds_urma_create_jfce)>("urma_create_jfce", context));
}

urma_status_t ds_urma_delete_jfce(urma_jfce_t *jfce)
{
    return CallRet< urma_status_t, decltype(&ds_urma_delete_jfce)>("urma_delete_jfce",
                                                                                    kUrmaDlopenErrorStatus, jfce);
}

urma_jfc_t *ds_urma_create_jfc(urma_context_t *context, const urma_jfc_cfg_t *config)
{
    return static_cast<urma_jfc_t *>(CallPtr<decltype(&ds_urma_create_jfc)>("urma_create_jfc", context, config));
}

urma_status_t ds_urma_delete_jfc(urma_jfc_t *jfc)
{
    return CallRet< urma_status_t, decltype(&ds_urma_delete_jfc)>("urma_delete_jfc",
                                                                                   kUrmaDlopenErrorStatus, jfc);
}

urma_status_t ds_urma_rearm_jfc(urma_jfc_t *jfc, bool enable_events)
{
    return CallRet< urma_status_t, decltype(&ds_urma_rearm_jfc)>(
        "urma_rearm_jfc", kUrmaDlopenErrorStatus, jfc, enable_events);
}

urma_jfr_t *ds_urma_create_jfr(urma_context_t *context, const urma_jfr_cfg_t *config)
{
    return static_cast<urma_jfr_t *>(CallPtr<decltype(&ds_urma_create_jfr)>("urma_create_jfr", context, config));
}

urma_status_t ds_urma_delete_jfr(urma_jfr_t *jfr)
{
    return CallRet< urma_status_t, decltype(&ds_urma_delete_jfr)>(
        "urma_delete_jfr", kUrmaDlopenErrorStatus, jfr);
}

urma_jetty_t *ds_urma_create_jetty(urma_context_t *context, urma_jetty_cfg_t *config)
{
    return static_cast<urma_jetty_t *>(CallPtr<decltype(&ds_urma_create_jetty)>("urma_create_jetty", context, config));
}

urma_status_t ds_urma_delete_jetty(urma_jetty_t *jetty)
{
    return CallRet< urma_status_t, decltype(&ds_urma_delete_jetty)>(
        "urma_delete_jetty", kUrmaDlopenErrorStatus, jetty);
}

urma_status_t ds_urma_modify_jetty(urma_jetty_t *jetty, urma_jetty_attr_t *attr)
{
    return CallRet< urma_status_t, decltype(&ds_urma_modify_jetty)>(
        "urma_modify_jetty", kUrmaDlopenErrorStatus, jetty, attr);
}

urma_target_seg_t *ds_urma_register_seg(urma_context_t *context, const urma_seg_cfg_t *config)
{
    return static_cast<urma_target_seg_t *>(
        CallPtr<decltype(&ds_urma_register_seg)>("urma_register_seg", context, config));
}

int ds_urma_wait_jfc(urma_jfce_t *jfce, int max_events, int timeout_ms, urma_jfc_t **ev_jfc)
{
    return CallRet< int, decltype(&ds_urma_wait_jfc)>(
        "urma_wait_jfc", -1, jfce, max_events, timeout_ms, ev_jfc);
}

int ds_urma_poll_jfc(urma_jfc_t *jfc, int max_cr, urma_cr_t *complete_records)
{
    return CallRet< int, decltype(&ds_urma_poll_jfc)>(
        "urma_poll_jfc", -1, jfc, max_cr, complete_records);
}

void ds_urma_ack_jfc(urma_jfc_t **ev_jfc, uint32_t *ack_cnt, int num)
{
    CallVoid<decltype(&ds_urma_ack_jfc)>("urma_ack_jfc", ev_jfc, ack_cnt, num);
}

urma_target_jetty_t *ds_urma_import_jetty(urma_context_t *context, urma_rjetty_t *remote_jetty,
                                          urma_token_t *token)
{
    return static_cast<urma_target_jetty_t *>(
        CallPtr<decltype(&ds_urma_import_jetty)>("urma_import_jetty", context, remote_jetty, token));
}

urma_status_t ds_urma_unimport_jetty(urma_target_jetty_t *tjetty)
{
    return CallRet< urma_status_t, decltype(&ds_urma_unimport_jetty)>(
        "urma_unimport_jetty", kUrmaDlopenErrorStatus, tjetty);
}

urma_status_t ds_urma_get_rjetty(urma_jetty_t *jetty, urma_rjetty_t **rjetty, uint32_t *length)
{
    return CallRet< urma_status_t, decltype(&ds_urma_get_rjetty)>(
        "urma_get_rjetty", kUrmaDlopenErrorStatus, jetty, rjetty, length);
}

void ds_urma_put_rjetty(urma_rjetty_t *rjetty)
{
    CallVoid<decltype(&ds_urma_put_rjetty)>("urma_put_rjetty", rjetty);
}

urma_status_t ds_urma_post_jetty_send_wr(urma_jetty_t *jetty, urma_jfs_wr_t *wr, urma_jfs_wr_t **bad_wr)
{
    return CallRet< urma_status_t, decltype(&ds_urma_post_jetty_send_wr)>(
        "urma_post_jetty_send_wr", kUrmaDlopenErrorStatus, jetty, wr, bad_wr);
}

urma_target_seg_t *ds_urma_import_seg(urma_context_t *context, urma_seg_t *seg, urma_token_t *token, int flags,
                                      urma_import_seg_flag_t import_flag)
{
    return static_cast<urma_target_seg_t *>(
        CallPtr<decltype(&ds_urma_import_seg)>("urma_import_seg", context, seg, token, flags, import_flag));
}

urma_status_t ds_urma_get_seg_ctx(urma_target_seg_t *tseg, urma_seg_t **seg, uint32_t *size)
{
    return CallRet< urma_status_t, decltype(&ds_urma_get_seg_ctx)>(
        "urma_get_seg_ctx", kUrmaDlopenErrorStatus, tseg, seg, size);
}

void ds_urma_put_seg_ctx(urma_seg_t *seg)
{
    CallVoid<decltype(&ds_urma_put_seg_ctx)>("urma_put_seg_ctx", seg);
}

urma_status_t ds_urma_unregister_seg(urma_target_seg_t *seg)
{
    return CallRet< urma_status_t, decltype(&ds_urma_unregister_seg)>("urma_unregister_seg",
                                                                                        kUrmaDlopenErrorStatus, seg);
}

urma_status_t ds_urma_unimport_seg(urma_target_seg_t *seg)
{
    return CallRet< urma_status_t, decltype(&ds_urma_unimport_seg)>("urma_unimport_seg",
                                                                                      kUrmaDlopenErrorStatus, seg);
}

urma_status_t ds_urma_get_async_event(urma_context_t *context, urma_async_event_t *event)
{
    return CallRet< urma_status_t, decltype(&ds_urma_get_async_event)>(
        "urma_get_async_event", kUrmaDlopenErrorStatus, context, event);
}

void ds_urma_ack_async_event(urma_async_event_t *event)
{
    CallVoid<decltype(&ds_urma_ack_async_event)>("urma_ack_async_event", event);
}

urma_status_t ds_urma_start_perf(void)
{
    return CallRet< urma_status_t, decltype(&ds_urma_start_perf)>("urma_start_perf",
                                                                                    kUrmaDlopenErrorStatus);
}

urma_status_t ds_urma_stop_perf(void)
{
    return CallRet< urma_status_t, decltype(&ds_urma_stop_perf)>("urma_stop_perf",
                                                                                   kUrmaDlopenErrorStatus);
}

urma_status_t ds_urma_get_perf_info(char *perf_buf, uint32_t *length)
{
    return CallRet< urma_status_t, decltype(&ds_urma_get_perf_info)>(
        "urma_get_perf_info", kUrmaDlopenErrorStatus, perf_buf, length);
}

#endif  // USE_URMA_MOCK
