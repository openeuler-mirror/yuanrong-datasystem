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

#include <climits>
#include <dlfcn.h>
#include <link.h>

#include "securec.h"

#include "datasystem/common/log/log.h"

namespace {
bool g_init = false;
void *g_urma = nullptr;
void *g_urmaUbAgg = nullptr;

const char *urmaLibs[] = { "liburma.so.0", "liburma.so" };
const char *urmaUbAggLibs[] = { "/usr/lib64/urma/liburma_ubagg.so.0", "/usr/lib64/urma/liburma_ubagg.so" };
enum class UrmaLibType { URMA, UB_AGG };

template <size_t N>
void *TryLoadLib(const char *const (&candidates)[N])
{
    void *handle = nullptr;
    for (size_t i = 0; i < N; ++i) {
        const char *candidate = candidates[i];
        handle = dlopen(candidate, RTLD_LAZY | RTLD_GLOBAL);
        if (handle) {
            char origin[PATH_MAX] = {};
            const bool hasOrigin = (dlinfo(handle, RTLD_DI_ORIGIN, origin) == 0 && origin[0] != '\0');
            LOG(INFO) << "[UrmaDlopen] dlopen succeeded for: " << candidate
                      << (hasOrigin ? std::string(", origin path: ") + origin : "");
            break;
        } else {
            LOG(ERROR) << "[UrmaDlopen] dlopen failed for: " << candidate << " -> " << dlerror();
        }
    }
    return handle;
}

template <UrmaLibType LibType>
void *LoadUrmaSymbol(const char *name)
{
    void *handle = (LibType == UrmaLibType::URMA) ? g_urma : g_urmaUbAgg;
    if (!handle) {
        const char *libName = (LibType == UrmaLibType::URMA) ? "liburma" : "liburma_ubagg";
        LOG(INFO) << "[UrmaDlopen] " << libName << " handle is null before loading symbol: " << name;
        return nullptr;
    }
    void *sym = dlsym(handle, name);
    if (!sym) {
        LOG(ERROR) << "[UrmaDlopen] dlsym failed for " << name << ": " << dlerror();
    }
    return sym;
}

template <typename Fn, UrmaLibType LibType = UrmaLibType::URMA>
Fn LoadFn(const char *name)
{
    void *sym = LoadUrmaSymbol<LibType>(name);
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

template <UrmaLibType LibType, typename Ret, typename Fn, typename... Args>
Ret CallRet(const char *name, Ret fallback, Args... args)
{
    auto fn = LoadFn<Fn, LibType>(name);
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
    if (g_init) {
        return true;
    }
    g_urma = TryLoadLib(urmaLibs);
    if (!g_urma) {
        LOG(ERROR) << "[UrmaDlopen] Failed to load liburma: " << dlerror();
        Cleanup();
        return false;
    }
    g_urmaUbAgg = TryLoadLib(urmaUbAggLibs);
    if (!g_urmaUbAgg) {
        LOG(ERROR) << "[UrmaDlopen] Failed to load liburma_ubagg: " << dlerror();
        Cleanup();
        return false;
    }
    g_init = true;
    return true;
}

bool IsAvailable()
{
    return g_init;
}

void Cleanup()
{
    if (g_urma) {
        dlclose(g_urma);
        g_urma = nullptr;
    }
    if (g_urmaUbAgg) {
        dlclose(g_urmaUbAgg);
        g_urmaUbAgg = nullptr;
    }
    g_init = false;
}

}  // namespace urma_dlopen
}  // namespace datasystem

static constexpr urma_status_t kUrmaDlopenErrorStatus = static_cast<urma_status_t>(-1);

urma_status_t ds_urma_init(const urma_init_attr_t *attr)
{
    return CallRet<UrmaLibType::URMA, urma_status_t, decltype(&ds_urma_init)>("urma_init", kUrmaDlopenErrorStatus,
                                                                              attr);
}

urma_status_t ds_urma_uninit(void)
{
    return CallRet<UrmaLibType::URMA, urma_status_t, decltype(&ds_urma_uninit)>("urma_uninit", kUrmaDlopenErrorStatus);
}

urma_status_t ds_urma_register_log_func(urma_log_cb_t log_cb)
{
    return CallRet<UrmaLibType::URMA, urma_status_t, decltype(&ds_urma_register_log_func)>(
        "urma_register_log_func", kUrmaDlopenErrorStatus, log_cb);
}

urma_status_t ds_urma_unregister_log_func(void)
{
    return CallRet<UrmaLibType::URMA, urma_status_t, decltype(&ds_urma_unregister_log_func)>("urma_unregister_log_func",
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
    return CallRet<UrmaLibType::URMA, urma_status_t, decltype(&ds_urma_query_device)>(
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
    return CallRet<UrmaLibType::URMA, urma_status_t, decltype(&ds_urma_delete_context)>(
        "urma_delete_context", kUrmaDlopenErrorStatus, context);
}

urma_status_t ds_urma_set_context_opt(urma_context_t *context, urma_opt_name_t opt_name, const void *opt_value,
                                      size_t opt_len)
{
    return CallRet<UrmaLibType::URMA, urma_status_t, decltype(&ds_urma_set_context_opt)>(
        "urma_set_context_opt", kUrmaDlopenErrorStatus, context, opt_name, opt_value, opt_len);
}

urma_status_t ds_urma_user_ctl(urma_context_t *ctx, urma_user_ctl_in_t *in, urma_user_ctl_out_t *out)
{
    return CallRet<UrmaLibType::URMA, urma_status_t, decltype(&ds_urma_user_ctl)>("urma_user_ctl",
                                                                                  kUrmaDlopenErrorStatus, ctx, in, out);
}

urma_jfce_t *ds_urma_create_jfce(urma_context_t *context)
{
    return static_cast<urma_jfce_t *>(CallPtr<decltype(&ds_urma_create_jfce)>("urma_create_jfce", context));
}

urma_status_t ds_urma_delete_jfce(urma_jfce_t *jfce)
{
    return CallRet<UrmaLibType::URMA, urma_status_t, decltype(&ds_urma_delete_jfce)>("urma_delete_jfce",
                                                                                     kUrmaDlopenErrorStatus, jfce);
}

urma_jfc_t *ds_urma_create_jfc(urma_context_t *context, const urma_jfc_cfg_t *config)
{
    return static_cast<urma_jfc_t *>(CallPtr<decltype(&ds_urma_create_jfc)>("urma_create_jfc", context, config));
}

urma_status_t ds_urma_delete_jfc(urma_jfc_t *jfc)
{
    return CallRet<UrmaLibType::URMA, urma_status_t, decltype(&ds_urma_delete_jfc)>("urma_delete_jfc",
                                                                                    kUrmaDlopenErrorStatus, jfc);
}

urma_status_t ds_urma_rearm_jfc(urma_jfc_t *jfc, bool enable_events)
{
    return CallRet<UrmaLibType::URMA, urma_status_t, decltype(&ds_urma_rearm_jfc)>(
        "urma_rearm_jfc", kUrmaDlopenErrorStatus, jfc, enable_events);
}

urma_jfs_t *ds_urma_create_jfs(urma_context_t *context, const urma_jfs_cfg_t *config)
{
    return static_cast<urma_jfs_t *>(CallPtr<decltype(&ds_urma_create_jfs)>("urma_create_jfs", context, config));
}

urma_status_t ds_urma_delete_jfs(urma_jfs_t *jfs)
{
    return CallRet<UrmaLibType::URMA, urma_status_t, decltype(&ds_urma_delete_jfs)>("urma_delete_jfs",
                                                                                    kUrmaDlopenErrorStatus, jfs);
}

urma_status_t ds_urma_modify_jfs(urma_jfs_t *jfs, urma_jfs_attr_t *attr)
{
    return CallRet<UrmaLibType::URMA, urma_status_t, decltype(&ds_urma_modify_jfs)>("urma_modify_jfs",
                                                                                    kUrmaDlopenErrorStatus, jfs, attr);
}

urma_jfr_t *ds_urma_create_jfr(urma_context_t *context, const urma_jfr_cfg_t *config)
{
    return static_cast<urma_jfr_t *>(CallPtr<decltype(&ds_urma_create_jfr)>("urma_create_jfr", context, config));
}

urma_status_t ds_urma_delete_jfr(urma_jfr_t *jfr)
{
    return CallRet<UrmaLibType::URMA, urma_status_t, decltype(&ds_urma_delete_jfr)>("urma_delete_jfr",
                                                                                    kUrmaDlopenErrorStatus, jfr);
}

urma_target_seg_t *ds_urma_register_seg(urma_context_t *context, const urma_seg_cfg_t *config)
{
    return static_cast<urma_target_seg_t *>(
        CallPtr<decltype(&ds_urma_register_seg)>("urma_register_seg", context, config));
}

int ds_urma_wait_jfc(urma_jfce_t *jfce, int max_events, int timeout_ms, urma_jfc_t **ev_jfc)
{
    return CallRet<UrmaLibType::URMA, int, decltype(&ds_urma_wait_jfc)>("urma_wait_jfc", -1, jfce, max_events,
                                                                        timeout_ms, ev_jfc);
}

int ds_urma_poll_jfc(urma_jfc_t *jfc, int max_cr, urma_cr_t *complete_records)
{
    return CallRet<UrmaLibType::URMA, int, decltype(&ds_urma_poll_jfc)>("urma_poll_jfc", -1, jfc, max_cr,
                                                                        complete_records);
}

void ds_urma_ack_jfc(urma_jfc_t **ev_jfc, uint32_t *ack_cnt, int num)
{
    CallVoid<decltype(&ds_urma_ack_jfc)>("urma_ack_jfc", ev_jfc, ack_cnt, num);
}

urma_target_jetty_t *ds_urma_import_jfr(urma_context_t *context, const urma_rjfr_t *remote_jfr, urma_token_t *token)
{
    return static_cast<urma_target_jetty_t *>(
        CallPtr<decltype(&ds_urma_import_jfr)>("urma_import_jfr", context, remote_jfr, token));
}

urma_status_t ds_urma_advise_jfr(urma_jfs_t *jfs, urma_target_jetty_t *tjfr)
{
    return CallRet<UrmaLibType::URMA, urma_status_t, decltype(&ds_urma_advise_jfr)>("urma_advise_jfr",
                                                                                    kUrmaDlopenErrorStatus, jfs, tjfr);
}

urma_status_t ds_urma_unimport_jfr(urma_target_jetty_t *tjfr)
{
    return CallRet<UrmaLibType::URMA, urma_status_t, decltype(&ds_urma_unimport_jfr)>("urma_unimport_jfr",
                                                                                      kUrmaDlopenErrorStatus, tjfr);
}

urma_status_t ds_urma_write(urma_jfs_t *jfs, urma_target_jetty_t *tjfr, urma_target_seg_t *remote_seg,
                            urma_target_seg_t *local_seg, uint64_t remote_addr, uint64_t local_addr, uint64_t length,
                            urma_jfs_wr_flag_t flag, uint64_t user_ctx)
{
    return CallRet<UrmaLibType::URMA, urma_status_t, decltype(&ds_urma_write)>(
        "urma_write", kUrmaDlopenErrorStatus, jfs, tjfr, remote_seg, local_seg, remote_addr, local_addr, length, flag,
        user_ctx);
}

urma_status_t ds_urma_write_affinity(urma_jfs_t *jfs, urma_target_jetty_t *tjfr, urma_target_seg_t *remote_seg,
                                     urma_target_seg_t *local_seg, uint64_t remote_addr, uint64_t local_addr,
                                     uint64_t length, urma_jfs_wr_flag_t flag, uint64_t user_ctx, uint32_t src_chip_id,
                                     uint32_t dst_chip_id)
{
    return CallRet<UrmaLibType::UB_AGG, urma_status_t, decltype(&ds_urma_write_affinity)>(
        "urma_write_affinity", kUrmaDlopenErrorStatus, jfs, tjfr, remote_seg, local_seg, remote_addr, local_addr,
        length, flag, user_ctx, src_chip_id, dst_chip_id);
}

urma_status_t ds_urma_read(urma_jfs_t *jfs, urma_target_jetty_t *tjfr, urma_target_seg_t *local_seg,
                           urma_target_seg_t *remote_seg, uint64_t local_addr, uint64_t remote_addr, uint64_t length,
                           urma_jfs_wr_flag_t flag, uint64_t user_ctx)
{
    return CallRet<UrmaLibType::URMA, urma_status_t, decltype(&ds_urma_read)>("urma_read", kUrmaDlopenErrorStatus, jfs,
                                                                              tjfr, local_seg, remote_seg, local_addr,
                                                                              remote_addr, length, flag, user_ctx);
}

urma_status_t ds_urma_post_jfs_wr(urma_jfs_t *jfs, urma_jfs_wr_t *wr, urma_jfs_wr_t **bad_wr)
{
    return CallRet<UrmaLibType::URMA, urma_status_t, decltype(&ds_urma_post_jfs_wr)>(
        "urma_post_jfs_wr", kUrmaDlopenErrorStatus, jfs, wr, bad_wr);
}

urma_target_seg_t *ds_urma_import_seg(urma_context_t *context, urma_seg_t *seg, urma_token_t *token, int flags,
                                      urma_import_seg_flag_t import_flag)
{
    return static_cast<urma_target_seg_t *>(
        CallPtr<decltype(&ds_urma_import_seg)>("urma_import_seg", context, seg, token, flags, import_flag));
}

urma_status_t ds_urma_unregister_seg(urma_target_seg_t *seg)
{
    return CallRet<UrmaLibType::URMA, urma_status_t, decltype(&ds_urma_unregister_seg)>("urma_unregister_seg",
                                                                                        kUrmaDlopenErrorStatus, seg);
}

urma_status_t ds_urma_unimport_seg(urma_target_seg_t *seg)
{
    return CallRet<UrmaLibType::URMA, urma_status_t, decltype(&ds_urma_unimport_seg)>("urma_unimport_seg",
                                                                                      kUrmaDlopenErrorStatus, seg);
}

urma_status_t ds_urma_get_async_event(urma_context_t *context, urma_async_event_t *event)
{
    return CallRet<UrmaLibType::URMA, urma_status_t, decltype(&ds_urma_get_async_event)>(
        "urma_get_async_event", kUrmaDlopenErrorStatus, context, event);
}

void ds_urma_ack_async_event(urma_async_event_t *event)
{
    CallVoid<decltype(&ds_urma_ack_async_event)>("urma_ack_async_event", event);
}

urma_status_t ds_urma_start_perf(void)
{
    return CallRet<UrmaLibType::URMA, urma_status_t, decltype(&ds_urma_start_perf)>("urma_start_perf",
                                                                                    kUrmaDlopenErrorStatus);
}

urma_status_t ds_urma_stop_perf(void)
{
    return CallRet<UrmaLibType::URMA, urma_status_t, decltype(&ds_urma_stop_perf)>("urma_stop_perf",
                                                                                   kUrmaDlopenErrorStatus);
}

urma_status_t ds_urma_get_perf_info(char *perf_buf, uint32_t *length)
{
    return CallRet<UrmaLibType::URMA, urma_status_t, decltype(&ds_urma_get_perf_info)>(
        "urma_get_perf_info", kUrmaDlopenErrorStatus, perf_buf, length);
}
