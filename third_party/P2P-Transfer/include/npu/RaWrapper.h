/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 */
#ifndef P2P_RA_WRAPPER_H
#define P2P_RA_WRAPPER_H
#include <stddef.h>
#include <unistd.h>
#include "../tools/Status.h"
#include "external/ra.h"

Status RaInit(struct ra_init_config *config);
Status RaDeinit(struct ra_init_config *config);
Status RaSocketListenStart(struct socket_listen_info_t conn[], uint32_t num);
Status RaSocketListenStop(struct socket_listen_info_t conn[], uint32_t num);
Status RaRdevInitV2(struct rdev_init_info init_info, struct rdev rdev_info, void **rdma_handle);
Status RaRdevDeinit(void *rdma_handle, unsigned int notify_type);
Status RaGetSockets(uint32_t role, struct socket_info_t conn[], uint32_t num, uint32_t *connected_num);
Status RaSocketBatchConnect(struct socket_connect_info_t conn[], uint32_t num);
Status RaSocketBatchClose(struct socket_close_info_t conn[], unsigned int num);
Status RaSocketWhiteListAdd(void *socket_handle, struct socket_wlist_info_t white_list[], unsigned int num);
Status RaSocketWhiteListDel(void *socket_handle, struct socket_wlist_info_t white_list[], unsigned int num);
Status RaQpCreateWithAttrs(void *rdev_handle, struct qp_ext_attrs *ext_attrs, void **qp_handle);
Status RaQpDestroy(void *qp_handle);
Status RaSetQpAttrQos(void *qp_handle, struct qos_attr *attr);
Status RaSetQpAttrTimeout(void *qp_handle, uint32_t *timeout);
Status RaSetQpAttrRetryCnt(void *qp_handle, uint32_t *retry_cnt);
Status RaGetQpnByQpHandle(void *qp_handle, uint32_t *qpn);
Status RaMrReg(void *qp_handle, struct mr_info *info);
Status RaMrDeReg(void *qp_handle, struct mr_info *info);
Status RaQpConnectAsync(void *qp_handle, const void *fd_handle);
Status RaGetQpStatus(void *qp_handle, ra_qp_status *status);
Status RaSendWrlistExt(void *qp_handle, struct send_wrlist_data_ext wr[], struct send_wr_rsp op_rsp[],
                       uint32_t send_num, uint32_t *complete_num);
Status RaGetNotifyBaseAddr(void *rdev_handle, unsigned long long *va, unsigned long long *size);
Status RaSendWr(void *qp_handle, struct send_wr *wr, struct send_wr_rsp *op_rsp);
Status RaGetIfaddrs(struct ra_get_ifattr *config, struct interface_info interface_infos[], unsigned int *num);
Status RaGetIfNum(struct ra_get_ifattr *config, unsigned int *num);

#endif  // P2P_RA_WRAPPER_H