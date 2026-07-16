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
 * @brief C ABI entry points for the in-process URMA mock backend.
 * These functions mirror the real URMA SDK entry points used by Datasystem. Under URMA mock builds,
 * urma_dlopen_util resolves ds_urma_* wrappers to these symbols instead of calling dlsym on liburma.
 *
 * @note Lock order is MockUrmaBackend::Mu(), then SideTables::mu, then per-object mutexes. C ABI functions only acquire
 * the backend mutex when the delegated backend method needs it.
 */
#ifndef DATASYSTEM_COMMON_URMA_MOCK_ABI_MOCK_ABI_H
#define DATASYSTEM_COMMON_URMA_MOCK_ABI_MOCK_ABI_H

#include "datasystem/common/urma_mock/abi/urma_abi_compat.h"

#ifdef USE_URMA_MOCK

extern "C" {
/**
 * @brief Initialize the mock URMA backend.
 * @param[in] attr Initialization attributes. Currently accepted for ABI compatibility.
 * @return URMA_SUCCESS on success.
 */
urma_status_t ds_urma_mock_init(const urma_init_attr_t *attr);

/**
 * @brief Uninitialize the mock URMA backend.
 * @return URMA_SUCCESS.
 */
urma_status_t ds_urma_mock_uninit(void);

/**
 * @brief Register a URMA log callback.
 * @param[in] log_cb Callback provided by caller.
 * @return URMA_SUCCESS.
 */
urma_status_t ds_urma_mock_register_log_func(urma_log_cb_t log_cb);

/**
 * @brief Unregister the URMA log callback.
 * @return URMA_SUCCESS.
 */
urma_status_t ds_urma_mock_unregister_log_func(void);

/**
 * @brief Get mock URMA devices.
 * @param[out] dev_num Number of devices returned.
 * @return Null-terminated device list owned by the mock backend.
 */
urma_device_t **ds_urma_mock_get_device_list(int *dev_num);

/**
 * @brief Find a mock device by name.
 * @param[in] name Device name.
 * @return Matched device, or nullptr if not found.
 */
urma_device_t *ds_urma_mock_get_device_by_name(char *name);

/**
 * @brief Query mock device attributes.
 * @param[in] device Device returned by ds_urma_mock_get_device_list.
 * @param[out] attr Device attributes.
 * @return URMA_SUCCESS on success.
 */
urma_status_t ds_urma_mock_query_device(urma_device_t *device, urma_device_attr_t *attr);

/**
 * @brief Get EID list for a mock device.
 * @param[in] device Device returned by ds_urma_mock_get_device_list.
 * @param[out] eid_count Number of EIDs returned.
 * @return EID list allocated by the mock backend.
 */
urma_eid_info_t *ds_urma_mock_get_eid_list(urma_device_t *device, uint32_t *eid_count);

/**
 * @brief Free an EID list returned by ds_urma_mock_get_eid_list.
 * @param[in] eid_list EID list to free.
 */
void ds_urma_mock_free_eid_list(urma_eid_info_t *eid_list);

/**
 * @brief Create a mock URMA context.
 * @param[in] device Device that owns the context.
 * @param[in] eid_index EID index. Currently accepted for ABI compatibility.
 * @return Created context, or nullptr on failure.
 */
urma_context_t *ds_urma_mock_create_context(urma_device_t *device, uint32_t eid_index);

/**
 * @brief Delete a mock URMA context.
 * @param[in] context Context to delete.
 * @return URMA_SUCCESS on success.
 */
urma_status_t ds_urma_mock_delete_context(urma_context_t *context);

/**
 * @brief Set a mock context option.
 * @param[in] context Context to configure.
 * @param[in] opt_name Option name.
 * @param[in] opt_value Option value buffer.
 * @param[in] opt_value_size Option value size in bytes.
 * @return URMA_SUCCESS for accepted options.
 */
urma_status_t ds_urma_mock_set_context_opt(urma_context_t *context, urma_opt_name_t opt_name, const void *opt_value,
                                           size_t opt_value_size);

/**
 * @brief Handle mock user control request.
 * @param[in] ctx Context receiving the control request.
 * @param[in] in Control input.
 * @param[out] out Control output.
 * @return URMA_SUCCESS for supported mock requests.
 */
urma_status_t ds_urma_mock_user_ctl(urma_context_t *ctx, urma_user_ctl_in_t *in, urma_user_ctl_out_t *out);

/**
 * @brief Create a mock JFC event handle.
 * @param[in] context Context that owns the event handle.
 * @return Created event handle, or nullptr on failure.
 */
urma_jfce_t *ds_urma_mock_create_jfce(urma_context_t *context);

/**
 * @brief Delete a mock JFC event handle.
 * @param[in] jfce Event handle to delete.
 * @return URMA_SUCCESS.
 */
urma_status_t ds_urma_mock_delete_jfce(urma_jfce_t *jfce);

/**
 * @brief Create a mock completion queue.
 * @param[in] context Context that owns the JFC.
 * @param[in] config JFC configuration.
 * @return Created JFC, or nullptr on failure.
 */
urma_jfc_t *ds_urma_mock_create_jfc(urma_context_t *context, const urma_jfc_cfg_t *config);

/**
 * @brief Delete a mock completion queue.
 * @param[in] jfc JFC to delete.
 * @return URMA_SUCCESS.
 */
urma_status_t ds_urma_mock_delete_jfc(urma_jfc_t *jfc);

/**
 * @brief Rearm mock completion events.
 * @param[in] jfc JFC to rearm.
 * @param[in] enableEvents Whether event notification is enabled.
 * @return URMA_SUCCESS.
 */
urma_status_t ds_urma_mock_rearm_jfc(urma_jfc_t *jfc, bool enableEvents);

/**
 * @brief Wait for mock completion events.
 * @param[in] jfce Event handle.
 * @param[in] maxEvents Maximum events to return.
 * @param[in] timeoutMs Wait timeout in milliseconds.
 * @param[out] evJfc JFC that produced events.
 * @return Number of events returned.
 */
int ds_urma_mock_wait_jfc(urma_jfce_t *jfce, int maxEvents, int timeoutMs, urma_jfc_t **evJfc);

/**
 * @brief Poll mock completion records.
 * @param[in] jfc Completion queue.
 * @param[in] maxCr Maximum records to return.
 * @param[out] completeRecords Output completion records.
 * @return Number of records returned.
 */
int ds_urma_mock_poll_jfc(urma_jfc_t *jfc, int maxCr, urma_cr_t *completeRecords);

/**
 * @brief Acknowledge mock completion events.
 * @param[in] evJfc Event-producing JFC array.
 * @param[in] ackCnt Acknowledgement counts.
 * @param[in] num Number of JFC entries.
 */
void ds_urma_mock_ack_jfc(urma_jfc_t **evJfc, uint32_t *ackCnt, int num);

/**
 * @brief Create a mock receive jetty.
 * @param[in] context Context that owns the JFR.
 * @param[in] config JFR configuration.
 * @return Created JFR, or nullptr on failure.
 */
urma_jfr_t *ds_urma_mock_create_jfr(urma_context_t *context, const urma_jfr_cfg_t *config);

/**
 * @brief Delete a mock receive jetty.
 * @param[in] jfr JFR to delete.
 * @return URMA_SUCCESS.
 */
urma_status_t ds_urma_mock_delete_jfr(urma_jfr_t *jfr);

/**
 * @brief Create a mock send jetty for the legacy JFS ABI.
 * @param[in] context Context that owns the JFS.
 * @param[in] config JFS configuration.
 * @return Created JFS, or nullptr on failure.
 */
urma_jfs_t *ds_urma_mock_create_jfs(urma_context_t *context, const urma_jfs_cfg_t *config);

/**
 * @brief Delete a mock JFS.
 * @param[in] jfs JFS to delete.
 * @return URMA_SUCCESS on success.
 */
urma_status_t ds_urma_mock_delete_jfs(urma_jfs_t *jfs);

/**
 * @brief Modify a mock JFS.
 * @param[in] jfs JFS to modify.
 * @param[in] attr JFS attributes.
 * @return URMA_SUCCESS on success.
 */
urma_status_t ds_urma_mock_modify_jfs(urma_jfs_t *jfs, urma_jfs_attr_t *attr);

/**
 * @brief Create a mock jetty.
 * @param[in] context Context that owns the jetty.
 * @param[in] config Jetty configuration.
 * @return Created jetty, or nullptr on failure.
 */
urma_jetty_t *ds_urma_mock_create_jetty(urma_context_t *context, urma_jetty_cfg_t *config);

/**
 * @brief Delete a mock jetty.
 * @param[in] jetty Jetty to delete.
 * @return URMA_SUCCESS.
 */
urma_status_t ds_urma_mock_delete_jetty(urma_jetty_t *jetty);

/**
 * @brief Get the remote jetty descriptor for a local mock jetty.
 * @param[in] jetty Local jetty.
 * @param[out] rjetty Allocated remote jetty descriptor.
 * @param[out] length Descriptor length.
 * @return URMA_SUCCESS on success.
 */
urma_status_t ds_urma_mock_get_rjetty(urma_jetty_t *jetty, urma_rjetty_t **rjetty, uint32_t *length);

/**
 * @brief Free a remote jetty descriptor returned by ds_urma_mock_get_rjetty.
 * @param[in] rjetty Remote jetty descriptor.
 */
void ds_urma_mock_put_rjetty(urma_rjetty_t *rjetty);

/**
 * @brief Modify a mock jetty.
 * @param[in] jetty Jetty to modify.
 * @param[in] attr Jetty attributes.
 * @return URMA_SUCCESS on success.
 */
urma_status_t ds_urma_mock_modify_jetty(urma_jetty_t *jetty, urma_jetty_attr_t *attr);

/**
 * @brief Import a mock target JFR for the legacy JFS/JFR ABI.
 * @param[in] context Context that imports the target JFR.
 * @param[in] remoteJfr Remote JFR descriptor.
 * @param[in] token Import token.
 * @return Imported target jetty, or nullptr on failure.
 */
urma_target_jetty_t *ds_urma_mock_import_jfr(urma_context_t *context, const urma_rjfr_t *remoteJfr,
                                             urma_token_t *token);

/**
 * @brief Advise a JFS about an imported target JFR.
 * @param[in] jfs Local JFS.
 * @param[in] tjfr Imported target JFR.
 * @return URMA_SUCCESS on success.
 */
urma_status_t ds_urma_mock_advise_jfr(urma_jfs_t *jfs, urma_target_jetty_t *tjfr);

/**
 * @brief Unimport a mock target JFR.
 * @param[in] tjfr Target JFR to unimport.
 * @return URMA_SUCCESS.
 */
urma_status_t ds_urma_mock_unimport_jfr(urma_target_jetty_t *tjfr);

/**
 * @brief Import a mock target jetty.
 * @param[in] context Context that imports the target jetty.
 * @param[in] remoteJetty Remote jetty descriptor.
 * @param[in] token Import token.
 * @return Imported target jetty, or nullptr on failure.
 */
urma_target_jetty_t *ds_urma_mock_import_jetty(urma_context_t *context, urma_rjetty_t *remoteJetty,
                                               urma_token_t *token);

/**
 * @brief Unimport a mock target jetty.
 * @param[in] tjetty Target jetty to unimport.
 * @return URMA_SUCCESS.
 */
urma_status_t ds_urma_mock_unimport_jetty(urma_target_jetty_t *tjetty);

/**
 * @brief Register a mock target segment.
 * @param[in] context Context that owns the segment.
 * @param[in] config Segment configuration.
 * @return Registered target segment, or nullptr on failure.
 */
urma_target_seg_t *ds_urma_mock_register_seg(urma_context_t *context, const urma_seg_cfg_t *config);

/**
 * @brief Import a mock target segment.
 * @param[in] context Context that imports the segment.
 * @param[in] seg Remote segment descriptor.
 * @param[in] token Import token.
 * @param[in] flags Import flags.
 * @param[in] importFlag Import flag structure.
 * @return Imported target segment, or nullptr on failure.
 */
urma_target_seg_t *ds_urma_mock_import_seg(urma_context_t *context, urma_seg_t *seg, urma_token_t *token, int flags,
                                           urma_import_seg_flag_t importFlag);

/**
 * @brief Get segment context from a mock target segment.
 * @param[in] tseg Target segment.
 * @param[out] seg Segment descriptor owned by the target segment.
 * @param[out] size Descriptor byte length.
 * @return URMA_SUCCESS on success.
 */
urma_status_t ds_urma_mock_get_seg_ctx(urma_target_seg_t *tseg, urma_seg_t **seg, uint32_t *size);

/**
 * @brief Release a segment descriptor returned by ds_urma_mock_get_seg_ctx.
 * @param[in] seg Segment descriptor.
 */
void ds_urma_mock_put_seg_ctx(urma_seg_t *seg);

/**
 * @brief Unregister a mock target segment.
 * @param[in] seg Segment to unregister.
 * @return URMA_SUCCESS.
 */
urma_status_t ds_urma_mock_unregister_seg(urma_target_seg_t *seg);

/**
 * @brief Unimport a mock target segment.
 * @param[in] seg Segment to unimport.
 * @return URMA_SUCCESS.
 */
urma_status_t ds_urma_mock_unimport_seg(urma_target_seg_t *seg);

/**
 * @brief Submit a mock jetty send work request.
 * @param[in] jetty Source jetty.
 * @param[in] wr Work request.
 * @param[out] badWr First invalid work request on failure.
 * @return URMA_SUCCESS on success.
 */
urma_status_t ds_urma_mock_post_jetty_send_wr(urma_jetty_t *jetty, urma_jfs_wr_t *wr, urma_jfs_wr_t **badWr);

/**
 * @brief Submit a mock write through the legacy JFS ABI.
 */
urma_status_t ds_urma_mock_write(urma_jfs_t *jfs, urma_target_jetty_t *tjfr, urma_target_seg_t *remoteSeg,
                                 urma_target_seg_t *localSeg, uint64_t remoteAddr, uint64_t localAddr, uint64_t length,
                                 urma_jfs_wr_flag_t flag, uint64_t userCtx);

/**
 * @brief Submit a mock read through the legacy JFS ABI.
 */
urma_status_t ds_urma_mock_read(urma_jfs_t *jfs, urma_target_jetty_t *tjfr, urma_target_seg_t *localSeg,
                                urma_target_seg_t *remoteSeg, uint64_t localAddr, uint64_t remoteAddr, uint64_t length,
                                urma_jfs_wr_flag_t flag, uint64_t userCtx);

/**
 * @brief Submit a mock JFS work request list.
 */
urma_status_t ds_urma_mock_post_jfs_wr(urma_jfs_t *jfs, urma_jfs_wr_t *wr, urma_jfs_wr_t **badWr);

/**
 * @brief Get a mock async event.
 * @param[in] context Context to query.
 * @param[out] event Output async event.
 * @return URMA_SUCCESS when an event is returned.
 */
urma_status_t ds_urma_mock_get_async_event(urma_context_t *context, urma_async_event_t *event);

/**
 * @brief Acknowledge a mock async event.
 * @param[in] event Event returned by ds_urma_mock_get_async_event.
 */
void ds_urma_mock_ack_async_event(urma_async_event_t *event);

/**
 * @brief Start mock performance collection.
 * @return URMA_SUCCESS.
 */
urma_status_t ds_urma_mock_start_perf(void);

/**
 * @brief Stop mock performance collection.
 * @return URMA_SUCCESS.
 */
urma_status_t ds_urma_mock_stop_perf(void);

/**
 * @brief Get mock performance information.
 * @param[out] perfBuf Destination buffer.
 * @param[in,out] length Buffer length on input and written length on output.
 * @return URMA_SUCCESS when arguments are valid.
 */
urma_status_t ds_urma_mock_get_perf_info(char *perfBuf, uint32_t *length);

}  // extern "C"

#endif  // USE_URMA_MOCK

#endif  // DATASYSTEM_COMMON_URMA_MOCK_ABI_MOCK_ABI_H
