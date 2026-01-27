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

#include "datasystem/common/rdma/ucp_dlopen_util.h"

#include <climits>
#include <dlfcn.h>
#include <link.h>

#include "securec.h"

#include "datasystem/common/log/log.h"
#include "datasystem/common/rpc/rpc_constants.h"

namespace {
// File-local state: handles and init flag. Kept in anonymous namespace
// so they have internal linkage and are not visible outside this TU.
void *g_ucp = nullptr;
void *g_ucs = nullptr;
bool g_init = false;

// Candidate library names to try when loading UCP/UCS. Try specific SONAME
// first then the generic name. Arrays are null-terminated so TryLoadLib can
// iterate until nullptr.
const char *ucpLibs[] = { "libucp.so.0", "libucp.so" };
const char *ucsLibs[] = { "libucs.so.0", "libucs.so" };

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
            LOG(INFO) << "[UcpDlopen] dlopen succeeded for: " << candidate
                      << (hasOrigin ? std::string(", origin path: ") + origin : "");
            break;
        } else {
            LOG(ERROR) << "[UcpDlopen] dlopen failed for: " << candidate << " -> " << dlerror();
        }
    }
    return handle;
}

// Load a raw symbol pointer from the already-open libucp handle.
// Returns nullptr and logs on failure. Only used for UCP symbols.
void *LoadUcpSymbol(const char *name)
{
    if (!g_ucp) {
        LOG(INFO) << "[UcpDlopen] libucp handle is null before loading symbol: " << name;
        return nullptr;
    }
    void *sym = dlsym(g_ucp, name);
    if (!sym) {
        LOG(ERROR) << "[UcpDlopen] dlsym failed for " << name << ": " << dlerror();
    }
    return sym;
}

// Load a raw symbol pointer from the already-open libucs handle.
// Returns nullptr and logs on failure. Only used for UCS symbols.
void *LoadUcsSymbol(const char *name)
{
    if (!g_ucs) {
        LOG(INFO) << "[UcpDlopen] libucs handle is null before loading symbol: " << name;
        return nullptr;
    }
    void *sym = dlsym(g_ucs, name);
    if (!sym) {
        LOG(ERROR) << "[UcpDlopen] dlsym failed for " << name << ": " << dlerror();
    }
    return sym;
}

// Helper templates to load and call function pointers safely.
// UCP path: LoadFn / CallRet / CallVoid / CallPtr
//  - LoadFn<Fn>(name): fetch UCP symbol as typed function pointer (or nullptr)
//  - CallRet: call UCP function pointer, return fallback if missing
//  - CallVoid: call UCP void-returning function pointer, no-op if missing
//  - CallPtr: call UCP function pointer that returns pointer, nullptr if missing
// UCS path: LoadFnUcs / CallRetUcs (status string)
//  - LoadFnUcs<Fn>(name): fetch UCS symbol as typed function pointer (or nullptr)
//  - CallRetUcs: call UCS function pointer, return fallback if missing
template <typename Fn>
Fn LoadFn(const char *name)
{
    void *sym = LoadUcpSymbol(name);
    if (!sym) {
        return nullptr;
    }
    Fn fn = nullptr;
    int ret = memcpy_s(&fn, sizeof(Fn), &sym, sizeof(void *));
    if (ret != 0) {
        LOG(ERROR) << "[UcpDlopen] memcpy_s failed while casting UCP symbol: " << ret;
        return nullptr;
    }
    return fn;
}

template <typename Fn>
Fn LoadFnUcs(const char *name)
{
    void *sym = LoadUcsSymbol(name);
    if (!sym) {
        return nullptr;
    }
    Fn fn = nullptr;
    int ret = memcpy_s(&fn, sizeof(Fn), &sym, sizeof(void *));
    if (ret != 0) {
        LOG(ERROR) << "[UcpDlopen] memcpy_s failed while casting UCS symbol: " << ret;
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

template <typename Ret, typename Fn, typename... Args>
Ret CallRetUcs(const char *name, Ret fallback, Args... args)
{
    auto fn = LoadFnUcs<Fn>(name);
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
namespace ucp_dlopen {

bool Init()
{
    if (g_init) {
        return true;
    }

    // Load UCS first (UCP depends on it). No explicit path: rely on
    // dynamic loader (LD_LIBRARY_PATH / rpath / system dirs) and the
    // default SONAMEs in kUcsLibs.
    g_ucs = TryLoadLib(ucsLibs);
    if (!g_ucs) {
        LOG(ERROR) << "[UcpDlopen] Failed to load libucs: " << dlerror();
        Cleanup();
        return false;
    }

    // Load UCP from ld search (default SONAME order in kUcpLibs)
    g_ucp = TryLoadLib(ucpLibs);
    if (!g_ucp) {
        LOG(ERROR) << "[UcpDlopen] Failed to load libucp: " << dlerror();
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
    if (g_ucp) {
        dlclose(g_ucp);
        g_ucp = nullptr;
    }
    if (g_ucs) {
        dlclose(g_ucs);
        g_ucs = nullptr;
    }
    g_init = false;
}

}  // namespace ucp_dlopen
}  // namespace datasystem

// UCP shim functions (resolved via dlsym)
ucs_status_t ucp_config_read(const char *env_prefix, const char *filename, ucp_config_t **config_p)
{
    return CallRet<ucs_status_t, decltype(&ucp_config_read)>("ucp_config_read", UCS_ERR_UNSUPPORTED, env_prefix,
                                                             filename, config_p);
}

void ucp_config_release(ucp_config_t *config)
{
    CallVoid<decltype(&ucp_config_release)>("ucp_config_release", config);
}

ucs_status_t ucp_init_version(unsigned int api_major_version, unsigned int api_minor_version,
                              const ucp_params_t *params, const ucp_config_t *config, ucp_context_h *context_p)
{
    return CallRet<ucs_status_t, decltype(&ucp_init_version)>(
        "ucp_init_version", UCS_ERR_UNSUPPORTED, api_major_version, api_minor_version, params, config, context_p);
}

void ucp_cleanup(ucp_context_h context)
{
    CallVoid<decltype(&ucp_cleanup)>("ucp_cleanup", context);
}

ucs_status_t ucp_worker_create(ucp_context_h context, const ucp_worker_params_t *params, ucp_worker_h *worker_p)
{
    return CallRet<ucs_status_t, decltype(&ucp_worker_create)>("ucp_worker_create", UCS_ERR_UNSUPPORTED, context,
                                                               params, worker_p);
}

ucs_status_t ucp_worker_get_address(ucp_worker_h worker, ucp_address_t **address_p, size_t *address_length_p)
{
    return CallRet<ucs_status_t, decltype(&ucp_worker_get_address)>("ucp_worker_get_address", UCS_ERR_UNSUPPORTED,
                                                                    worker, address_p, address_length_p);
}

void ucp_worker_release_address(ucp_worker_h worker, ucp_address_t *address)
{
    CallVoid<decltype(&ucp_worker_release_address)>("ucp_worker_release_address", worker, address);
}

ucs_status_t ucp_worker_get_efd(ucp_worker_h worker, int *fd_p)
{
    return CallRet<ucs_status_t, decltype(&ucp_worker_get_efd)>("ucp_worker_get_efd", UCS_ERR_UNSUPPORTED, worker,
                                                                fd_p);
}

unsigned ucp_worker_progress(ucp_worker_h worker)
{
    return CallRet<unsigned, decltype(&ucp_worker_progress)>("ucp_worker_progress", 0U, worker);
}

ucs_status_t ucp_worker_arm(ucp_worker_h worker)
{
    return CallRet<ucs_status_t, decltype(&ucp_worker_arm)>("ucp_worker_arm", UCS_ERR_UNSUPPORTED, worker);
}

void ucp_worker_destroy(ucp_worker_h worker)
{
    CallVoid<decltype(&ucp_worker_destroy)>("ucp_worker_destroy", worker);
}

ucs_status_t ucp_mem_map(ucp_context_h context, const ucp_mem_map_params_t *params, ucp_mem_h *memh_p)
{
    return CallRet<ucs_status_t, decltype(&ucp_mem_map)>("ucp_mem_map", UCS_ERR_UNSUPPORTED, context, params, memh_p);
}

ucs_status_t ucp_mem_unmap(ucp_context_h context, ucp_mem_h memh)
{
    return CallRet<ucs_status_t, decltype(&ucp_mem_unmap)>("ucp_mem_unmap", UCS_ERR_UNSUPPORTED, context, memh);
}

ucs_status_t ucp_mem_query(const ucp_mem_h memh, ucp_mem_attr_t *attr)
{
    return CallRet<ucs_status_t, decltype(&ucp_mem_query)>("ucp_mem_query", UCS_ERR_UNSUPPORTED, memh, attr);
}

ucs_status_t ucp_rkey_pack(ucp_context_h context, ucp_mem_h memh, void **rkey_buffer_p, size_t *size_p)
{
    return CallRet<ucs_status_t, decltype(&ucp_rkey_pack)>("ucp_rkey_pack", UCS_ERR_UNSUPPORTED, context, memh,
                                                           rkey_buffer_p, size_p);
}

void ucp_rkey_buffer_release(void *rkey_buffer)
{
    CallVoid<decltype(&ucp_rkey_buffer_release)>("ucp_rkey_buffer_release", rkey_buffer);
}

ucs_status_t ucp_ep_create(ucp_worker_h worker, const ucp_ep_params_t *params, ucp_ep_h *ep_p)
{
    return CallRet<ucs_status_t, decltype(&ucp_ep_create)>("ucp_ep_create", UCS_ERR_UNSUPPORTED, worker, params, ep_p);
}

ucs_status_t ucp_ep_rkey_unpack(ucp_ep_h ep, const void *rkey_buffer, ucp_rkey_h *rkey_p)
{
    return CallRet<ucs_status_t, decltype(&ucp_ep_rkey_unpack)>("ucp_ep_rkey_unpack", UCS_ERR_UNSUPPORTED, ep,
                                                                rkey_buffer, rkey_p);
}

void ucp_rkey_destroy(ucp_rkey_h rkey)
{
    CallVoid<decltype(&ucp_rkey_destroy)>("ucp_rkey_destroy", rkey);
}

void *ucp_ep_close_nbx(ucp_ep_h ep, const ucp_request_param_t *param)
{
    return CallPtr<decltype(&ucp_ep_close_nbx)>("ucp_ep_close_nbx", ep, param);
}

void *ucp_ep_flush_nbx(ucp_ep_h ep, const ucp_request_param_t *param)
{
    return CallPtr<decltype(&ucp_ep_flush_nbx)>("ucp_ep_flush_nbx", ep, param);
}

void *ucp_put_nbx(ucp_ep_h ep, const void *buffer, size_t count, uint64_t remote_addr, ucp_rkey_h rkey,
                  const ucp_request_param_t *param)
{
    return CallPtr<decltype(&ucp_put_nbx)>("ucp_put_nbx", ep, buffer, count, remote_addr, rkey, param);
}

void ucp_request_free(void *request)
{
    CallVoid<decltype(&ucp_request_free)>("ucp_request_free", request);
}

// UCS shim (for status strings)
const char *ucs_status_string(ucs_status_t status)
{
    // Fallback string when symbol is unavailable
    static const char *kMissing = "UCS_STATUS_STRING_UNAVAILABLE";
    return CallRetUcs<const char *, decltype(&ucs_status_string)>("ucs_status_string", kMissing, status);
}
