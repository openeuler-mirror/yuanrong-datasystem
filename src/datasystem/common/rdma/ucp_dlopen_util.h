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

// UCP dlopen wrapper - provides lazy-loaded function wrappers for UCP/UCX

#ifndef DATASYSTEM_COMMON_RDMA_UCP_DLOPEN_UTIL_H
#define DATASYSTEM_COMMON_RDMA_UCP_DLOPEN_UTIL_H

#include <ucp/api/ucp.h>

// Init API - must call before using any UCP functions via dlopen
namespace datasystem {
namespace ucp_dlopen {

bool Init();
bool IsAvailable();
void Cleanup();

}  // namespace ucp_dlopen

// UCX shim functions with datasystem prefix to avoid global symbol conflicts.
ucs_status_t ds_ucp_config_read(const char *env_prefix, const char *filename, ucp_config_t **config_p);
void ds_ucp_config_release(ucp_config_t *config);
ucs_status_t ds_ucp_init(const ucp_params_t *params, const ucp_config_t *config, ucp_context_h *context_p);
void ds_ucp_cleanup(ucp_context_h context);
ucs_status_t ds_ucp_worker_create(ucp_context_h context, const ucp_worker_params_t *params, ucp_worker_h *worker_p);
ucs_status_t ds_ucp_worker_get_address(ucp_worker_h worker, ucp_address_t **address_p, size_t *address_length_p);
void ds_ucp_worker_release_address(ucp_worker_h worker, ucp_address_t *address);
ucs_status_t ds_ucp_worker_get_efd(ucp_worker_h worker, int *fd_p);
unsigned ds_ucp_worker_progress(ucp_worker_h worker);
ucs_status_t ds_ucp_worker_arm(ucp_worker_h worker);
void ds_ucp_worker_destroy(ucp_worker_h worker);
ucs_status_t ds_ucp_mem_map(ucp_context_h context, const ucp_mem_map_params_t *params, ucp_mem_h *memh_p);
ucs_status_t ds_ucp_mem_unmap(ucp_context_h context, ucp_mem_h memh);
ucs_status_t ds_ucp_mem_query(const ucp_mem_h memh, ucp_mem_attr_t *attr);
ucs_status_t ds_ucp_rkey_pack(ucp_context_h context, ucp_mem_h memh, void **rkey_buffer_p, size_t *size_p);
void ds_ucp_rkey_buffer_release(void *rkey_buffer);
ucs_status_t ds_ucp_ep_create(ucp_worker_h worker, const ucp_ep_params_t *params, ucp_ep_h *ep_p);
ucs_status_t ds_ucp_ep_rkey_unpack(ucp_ep_h ep, const void *rkey_buffer, ucp_rkey_h *rkey_p);
void ds_ucp_rkey_destroy(ucp_rkey_h rkey);
void *ds_ucp_ep_close_nbx(ucp_ep_h ep, const ucp_request_param_t *param);
void *ds_ucp_ep_flush_nbx(ucp_ep_h ep, const ucp_request_param_t *param);
void *ds_ucp_put_nbx(ucp_ep_h ep, const void *buffer, size_t count, uint64_t remote_addr, ucp_rkey_h rkey,
					 const ucp_request_param_t *param);
void ds_ucp_request_free(void *request);
ucs_status_t ds_ucp_request_check_status(void *request);
const char *ds_ucs_status_string(ucs_status_t status);
}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_RDMA_UCP_DLOPEN_UTIL_H
