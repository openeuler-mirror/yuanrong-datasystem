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

// URMA dlopen wrapper - provides lazy-loaded function wrappers for liburma

#ifndef DATASYSTEM_COMMON_RDMA_URMA_DLOPEN_UTIL_H
#define DATASYSTEM_COMMON_RDMA_URMA_DLOPEN_UTIL_H

#include <ub/umdk/urma/urma_api.h>
#ifdef URMA_OVER_UB
#include <ub/umdk/urma/urma_ubagg.h>
#endif

// Init API - must call before using any URMA functions via dlopen
namespace datasystem {
namespace urma_dlopen {

bool Init();
bool IsAvailable();
void Cleanup();

}  // namespace urma_dlopen
}  // namespace datasystem

// URMA shim functions with datasystem prefix to avoid global symbol conflicts.
urma_status_t ds_urma_init(const urma_init_attr_t *attr);
urma_status_t ds_urma_uninit(void);
urma_status_t ds_urma_register_log_func(urma_log_cb_t log_cb);
urma_status_t ds_urma_unregister_log_func(void);
urma_device_t **ds_urma_get_device_list(int *dev_num);
urma_device_t *ds_urma_get_device_by_name(char *name);
urma_status_t ds_urma_query_device(urma_device_t *device, urma_device_attr_t *attr);
urma_eid_info_t *ds_urma_get_eid_list(urma_device_t *device, uint32_t *eid_count);
void ds_urma_free_eid_list(urma_eid_info_t *eid_list);
urma_context_t *ds_urma_create_context(urma_device_t *device, uint32_t eid_index);
urma_status_t ds_urma_delete_context(urma_context_t *context);
urma_jfce_t *ds_urma_create_jfce(urma_context_t *context);
urma_status_t ds_urma_delete_jfce(urma_jfce_t *jfce);
urma_jfc_t *ds_urma_create_jfc(urma_context_t *context, const urma_jfc_cfg_t *config);
urma_status_t ds_urma_delete_jfc(urma_jfc_t *jfc);
urma_status_t ds_urma_rearm_jfc(urma_jfc_t *jfc, bool enable_events);
urma_jfs_t *ds_urma_create_jfs(urma_context_t *context, const urma_jfs_cfg_t *config);
urma_status_t ds_urma_delete_jfs(urma_jfs_t *jfs);
urma_status_t ds_urma_modify_jfs(urma_jfs_t *jfs, urma_jfs_attr_t *attr);
urma_jfr_t *ds_urma_create_jfr(urma_context_t *context, const urma_jfr_cfg_t *config);
urma_status_t ds_urma_delete_jfr(urma_jfr_t *jfr);
urma_target_seg_t *ds_urma_register_seg(urma_context_t *context, const urma_seg_cfg_t *config);
int ds_urma_wait_jfc(urma_jfce_t *jfce, int max_events, int timeout_ms, urma_jfc_t **ev_jfc);
int ds_urma_poll_jfc(urma_jfc_t *jfc, int max_cr, urma_cr_t *complete_records);
void ds_urma_ack_jfc(urma_jfc_t **ev_jfc, uint32_t *ack_cnt, int num);
urma_target_jetty_t *ds_urma_import_jfr(urma_context_t *context, const urma_rjfr_t *remote_jfr, urma_token_t *token);
urma_status_t ds_urma_advise_jfr(urma_jfs_t *jfs, urma_target_jetty_t *tjfr);
urma_status_t ds_urma_unimport_jfr(urma_target_jetty_t *tjfr);
urma_status_t ds_urma_write(urma_jfs_t *jfs, urma_target_jetty_t *tjfr, urma_target_seg_t *remote_seg,
                            urma_target_seg_t *local_seg, uint64_t remote_addr, uint64_t local_addr, uint64_t length,
                            urma_jfs_wr_flag_t flag, uint64_t user_ctx);
urma_status_t ds_urma_read(urma_jfs_t *jfs, urma_target_jetty_t *tjfr, urma_target_seg_t *local_seg,
                           urma_target_seg_t *remote_seg, uint64_t local_addr, uint64_t remote_addr, uint64_t length,
                           urma_jfs_wr_flag_t flag, uint64_t user_ctx);
urma_status_t ds_urma_post_jfs_wr(urma_jfs_t *jfs, urma_jfs_wr_t *wr, urma_jfs_wr_t **bad_wr);
urma_target_seg_t *ds_urma_import_seg(urma_context_t *context, urma_seg_t *seg, urma_token_t *token, int flags,
                                      urma_import_seg_flag_t import_flag);
urma_status_t ds_urma_unregister_seg(urma_target_seg_t *seg);
urma_status_t ds_urma_unimport_seg(urma_target_seg_t *seg);

#endif  // DATASYSTEM_COMMON_RDMA_URMA_DLOPEN_UTIL_H
