
/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
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
#include "npu/RaWrapper.h"
#include <array>
#include <cstring>
#include <chrono>
#include <thread>
#include "tools/npu-error.h"

constexpr int64_t LINK_TIMEOUT_S = 120;

Status RaInit(struct ra_init_config *config)
{
    int32_t ret = 0;
    auto startTime = std::chrono::steady_clock::now();
    auto timeout = std::chrono::seconds(LINK_TIMEOUT_S);
    while (true) {
        ret = ra_init(config);
        if (!ret) {
            break;
        } else if (ret == SOCK_EAGAIN) {
            if ((std::chrono::steady_clock::now() - startTime) >= timeout) {
                return Status::Error(ErrorCode::ACL_ERROR, "Failed to perform ra_init: timed out");
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        } else {
            return Status::Error(ErrorCode::ACL_ERROR, "Failed to perform ra_init: return " + std::to_string(ret));
        }
    }

    return Status::Success();
}

Status RaDeinit(struct ra_init_config *config)
{
    int32_t ret = 0;
    auto startTime = std::chrono::steady_clock::now();
    auto timeout = std::chrono::seconds(LINK_TIMEOUT_S);
    while (true) {
        ret = ra_deinit(config);
        if (!ret) {
            break;
        } else if (ret == SOCK_EAGAIN) {
            if ((std::chrono::steady_clock::now() - startTime) >= timeout) {
                return Status::Error(ErrorCode::ACL_ERROR, "Failed to perform ra_deinit: timed out");
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        } else {
            return Status::Error(ErrorCode::ACL_ERROR, "Failed to perform ra_deinit: return " + std::to_string(ret));
        }
    }

    return Status::Success();
}

Status RaSocketListenStart(struct socket_listen_info_t conn[], uint32_t num)
{
    int32_t ret = 0;
    auto startTime = std::chrono::steady_clock::now();
    auto timeout = std::chrono::seconds(LINK_TIMEOUT_S);
    while (true) {
        ret = ra_socket_listen_start(conn, num);
        if (!ret) {
            break;
        } else if (ret == SOCK_EAGAIN) {
            if ((std::chrono::steady_clock::now() - startTime) >= timeout) {
                return Status::Error(ErrorCode::ACL_ERROR, "Failed to perform ra_socket_listen_start: timed out");
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        } else {
            return Status::Error(ErrorCode::ACL_ERROR,
                                 "Failed to perform ra_socket_listen_start: return " + std::to_string(ret));
        }
    }

    return Status::Success();
}

Status RaSocketListenStop(struct socket_listen_info_t conn[], uint32_t num)
{
    int32_t ret = 0;
    auto startTime = std::chrono::steady_clock::now();
    auto timeout = std::chrono::seconds(LINK_TIMEOUT_S);
    while (true) {
        ret = ra_socket_listen_stop(conn, num);
        if (!ret) {
            break;
        } else if (ret == SOCK_EAGAIN) {
            if ((std::chrono::steady_clock::now() - startTime) >= timeout) {
                return Status::Error(ErrorCode::ACL_ERROR, "Failed to perform ra_socket_listen_stop: timed out");
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        } else {
            return Status::Error(ErrorCode::ACL_ERROR,
                                 "Failed to perform ra_socket_listen_stop: return " + std::to_string(ret));
        }
    }

    return Status::Success();
}

Status RaRdevInitV2(struct rdev_init_info init_info, struct rdev rdev_info, void **rdma_handle)
{
    ACL_CHECK_STATUS(ra_rdev_init_v2(init_info, rdev_info, rdma_handle));
    return Status::Success();
}

Status RaRdevDeinit(void *rdma_handle, unsigned int notify_type)
{
    ACL_CHECK_STATUS(ra_rdev_deinit(rdma_handle, notify_type));
    return Status::Success();
}

Status RaGetSockets(uint32_t role, struct socket_info_t conn[], uint32_t num, uint32_t *connected_num)
{
    ACL_CHECK_STATUS(ra_get_sockets(role, conn, num, connected_num));
    return Status::Success();
}

Status RaSocketBatchConnect(struct socket_connect_info_t conn[], uint32_t num)
{
    int32_t ret = 0;
    auto startTime = std::chrono::steady_clock::now();
    auto timeout = std::chrono::seconds(LINK_TIMEOUT_S);
    while (true) {
        ret = ra_socket_batch_connect(conn, num);
        if (!ret) {
            break;
        } else if (ret == SOCK_EAGAIN) {
            if ((std::chrono::steady_clock::now() - startTime) >= timeout) {
                return Status::Error(ErrorCode::ACL_ERROR, "Failed to perform ra_socket_batch_connect: timed out");
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        } else {
            return Status::Error(ErrorCode::ACL_ERROR,
                                 "Failed to perform ra_socket_batch_connect: return " + std::to_string(ret));
        }
    }

    return Status::Success();
}

Status RaSocketBatchClose(struct socket_close_info_t conn[], unsigned int num)
{
    int32_t ret = 0;
    auto startTime = std::chrono::steady_clock::now();
    auto timeout = std::chrono::seconds(LINK_TIMEOUT_S);
    while (true) {
        ret = ra_socket_batch_close(conn, num);
        if (!ret) {
            break;
        } else if (ret == SOCK_EAGAIN) {
            if ((std::chrono::steady_clock::now() - startTime) >= timeout) {
                return Status::Error(ErrorCode::ACL_ERROR, "Failed to perform ra_socket_batch_close: timed out");
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        } else {
            return Status::Error(ErrorCode::ACL_ERROR,
                                 "Failed to perform ra_socket_batch_close: return " + std::to_string(ret));
        }
    }

    return Status::Success();
}

Status RaSocketWhiteListAdd(void *socket_handle, struct socket_wlist_info_t white_list[], unsigned int num)
{
    ACL_CHECK_STATUS(ra_socket_white_list_add(socket_handle, white_list, num));
    return Status::Success();
}

Status RaSocketWhiteListDel(void *socket_handle, struct socket_wlist_info_t white_list[], unsigned int num)
{
    ACL_CHECK_STATUS(ra_socket_white_list_del(socket_handle, white_list, num));
    return Status::Success();
}

Status RaQpCreateWithAttrs(void *rdev_handle, struct qp_ext_attrs *ext_attrs, void **qp_handle)
{
    ACL_CHECK_STATUS(ra_qp_create_with_attrs(rdev_handle, ext_attrs, qp_handle));
    return Status::Success();
}

Status RaQpDestroy(void *qp_handle)
{
    ACL_CHECK_STATUS(ra_qp_destroy(qp_handle));
    return Status::Success();
}

Status RaSetQpAttrQos(void *qp_handle, struct qos_attr *attr)
{
    ACL_CHECK_STATUS(ra_set_qp_attr_qos(qp_handle, attr));
    return Status::Success();
}

Status RaSetQpAttrTimeout(void *qp_handle, uint32_t *timeout)
{
    ACL_CHECK_STATUS(ra_set_qp_attr_timeout(qp_handle, timeout));
    return Status::Success();
}

Status RaSetQpAttrRetryCnt(void *qp_handle, uint32_t *retry_cnt)
{
    ACL_CHECK_STATUS(ra_set_qp_attr_retry_cnt(qp_handle, retry_cnt));
    return Status::Success();
}

Status RaMrReg(void *qp_handle, struct mr_info *info)
{
    ACL_CHECK_STATUS(ra_mr_reg(qp_handle, info));
    return Status::Success();
}

Status RaMrDeReg(void *qp_handle, struct mr_info *info)
{
    ACL_CHECK_STATUS(ra_mr_dereg(qp_handle, info));
    return Status::Success();
}

Status RaQpConnectAsync(void *qp_handle, const void *fd_handle)
{
    int32_t ret = 0;
    auto startTime = std::chrono::steady_clock::now();
    auto timeout = std::chrono::seconds(LINK_TIMEOUT_S);
    while (true) {
        ret = ra_qp_connect_async(qp_handle, fd_handle);
        if (!ret) {
            break;
        } else if (ret == SOCK_EAGAIN) {
            if ((std::chrono::steady_clock::now() - startTime) >= timeout) {
                return Status::Error(ErrorCode::ACL_ERROR, "Failed to perform ra_qp_connect_async: timed out");
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        } else {
            return Status::Error(ErrorCode::ACL_ERROR,
                                 "Failed to perform ra_qp_connect_async: return " + std::to_string(ret));
        }
    }

    return Status::Success();
}

Status RaGetQpStatus(void *qp_handle, ra_qp_status *status)
{
    ACL_CHECK_STATUS(ra_get_qp_status(qp_handle, status));
    return Status::Success();
}

Status RaSendWrlistExt(void *qp_handle, struct send_wrlist_data_ext wr[], struct send_wr_rsp op_rsp[],
                       uint32_t send_num, uint32_t *complete_num)
{
    ACL_CHECK_STATUS(ra_send_wrlist_ext(qp_handle, wr, op_rsp, send_num, complete_num));
    return Status::Success();
}

Status RaGetNotifyBaseAddr(void *rdev_handle, unsigned long long *va, unsigned long long *size)
{
    int32_t ret = 0;
    auto startTime = std::chrono::steady_clock::now();
    auto timeout = std::chrono::seconds(LINK_TIMEOUT_S);
    while (true) {
        ret = ra_get_notify_base_addr(rdev_handle, va, size);
        if (!ret) {
            break;
        } else if (ret == SOCK_EAGAIN) {
            if ((std::chrono::steady_clock::now() - startTime) >= timeout) {
                return Status::Error(ErrorCode::ACL_ERROR, "Failed to perform ra_get_notify_base_addr: timed out");
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        } else {
            return Status::Error(ErrorCode::ACL_ERROR,
                                 "Failed to perform ra_get_notify_base_addr: return " + std::to_string(ret));
        }
    }

    return Status::Success();
}

Status RaSendWr(void *qp_handle, struct send_wr *wr, struct send_wr_rsp *op_rsp)
{
    int32_t ret = 0;
    auto startTime = std::chrono::steady_clock::now();
    auto timeout = std::chrono::seconds(LINK_TIMEOUT_S);
    while (true) {
        ret = ra_send_wr(qp_handle, wr, op_rsp);
        if (!ret) {
            break;
        } else if ((ret == SOCK_ENOENT) || (ret == SOCK_EAGAIN)
                   || (ret
                       == ROCE_ENOMEM)) {  // Not sure if we should do the last one, or maybe we should do it only once.
                                           // But fixes our error. Or maybe need to have rate limiting somewhere else
            if ((std::chrono::steady_clock::now() - startTime) >= timeout) {
                return Status::Error(ErrorCode::ACL_ERROR, "Failed to perform RaSendWr: timed out");
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        } else {
            return Status::Error(ErrorCode::ACL_ERROR, "Failed to perform RaSendWr: return " + std::to_string(ret));
        }
    }

    return Status::Success();
}

Status RaGetIfaddrs(struct ra_get_ifattr *config, struct interface_info interface_infos[], unsigned int *num)
{
    ACL_CHECK_STATUS(ra_get_ifaddrs(config, interface_infos, num));
    return Status::Success();
}

Status RaGetIfNum(struct ra_get_ifattr *config, unsigned int *num)
{
    ACL_CHECK_STATUS(ra_get_ifnum(config, num));
    return Status::Success();
}