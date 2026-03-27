/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 */
#ifndef P2P_RA_WRAPPER_H
#define P2P_RA_WRAPPER_H
#include <stddef.h>
#include <unistd.h>
#include "../tools/Status.h"
#include "external/ra.h"

Status RaInitWrapper(struct ra_init_config *config);
Status RaDeinitWrapper(struct ra_init_config *config);
Status RaSocketListenStartWrapper(struct socket_listen_info_t conn[], uint32_t num);
Status RaSocketListenStopWrapper(struct socket_listen_info_t conn[], uint32_t num);
Status RaRdevInitV2Wrapper(struct rdev_init_info init_info, struct rdev rdev_info, void **rdma_handle, bool &initialized);
Status RaRdevDeinitWrapper(void *rdma_handle, unsigned int notify_type, bool initialized);
Status RaGetSocketsWrapper(uint32_t role, struct socket_info_t conn[], uint32_t num, uint32_t *connected_num);
Status RaSocketBatchConnectWrapper(struct socket_connect_info_t conn[], uint32_t num);
Status RaSocketBatchCloseWrapper(struct socket_close_info_t conn[], unsigned int num);
Status RaSocketWhiteListAddWrapper(void *socket_handle, struct socket_wlist_info_t white_list[], unsigned int num);
Status RaSocketWhiteListDelWrapper(void *socket_handle, struct socket_wlist_info_t white_list[], unsigned int num);
Status RaQpCreateWithAttrsWrapper(void *rdev_handle, struct qp_ext_attrs *ext_attrs, void **qp_handle);
Status RaQpDestroyWrapper(void *qp_handle);
Status RaSetQpAttrQosWrapper(void *qp_handle, struct qos_attr *attr);
Status RaSetQpAttrTimeoutWrapper(void *qp_handle, uint32_t *timeout);
Status RaSetQpAttrRetryCntWrapper(void *qp_handle, uint32_t *retry_cnt);
Status RaGetQpnByQpHandleWrapper(void *qp_handle, uint32_t *qpn);
Status RaMrRegWrapper(void *qp_handle, struct mr_info *info);
Status RaMrDeRegWrapper(void *qp_handle, struct mr_info *info);
Status RaGlobalMrRegWrapper(void *rdma_handle, struct mr_info *info, void **mr_handle);
Status RaGlobalMrDeRegWrapper(void *rdma_handle, void *mr_handle);
Status RaQpConnectAsyncWrapper(void *qp_handle, const void *fd_handle);
Status RaGetQpStatusWrapper(void *qp_handle, ra_qp_status *status);
Status RaSendWrlistExtWrapper(void *qp_handle, struct send_wrlist_data_ext wr[], struct send_wr_rsp op_rsp[],
                       uint32_t send_num, uint32_t *complete_num);
Status RaGetNotifyBaseAddrWrapper(void *rdev_handle, unsigned long long *va, unsigned long long *size);
Status RaSendWrWrapper(void *qp_handle, struct send_wr *wr, struct send_wr_rsp *op_rsp);
Status RaTypicalSendWrWrapper(void *qp_handle, struct send_wr *wr, struct send_wr_rsp *op_rsp);
Status RaGetIfaddrsWrapper(struct ra_get_ifattr *config, struct interface_info interface_infos[], unsigned int *num);
Status RaGetIfNumWrapper(struct ra_get_ifattr *config, unsigned int *num);

#endif  // P2P_RA_WRAPPER_H