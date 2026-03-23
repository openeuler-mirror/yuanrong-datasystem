/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 */
#ifndef P2P_NPU_ERROR_H
#define P2P_NPU_ERROR_H
#include <iostream>

#include "acl/acl.h"
#include "tools/Status.h"

#define NPU_ERROR(ans)                                                                                          \
    do {                                                                                                        \
        aclError ret = ans;                                                                                     \
        if (ret != ACL_SUCCESS) {                                                                               \
            std::cerr << "[P2P] npuError: \"" << ret << "\"  in " << __FILE__ << ": " << __LINE__ << std::endl; \
            exit(1);                                                                                            \
        }                                                                                                       \
    } while (0)

#define STATUS_ERROR(ans)                                                                                          \
    do {                                                                                                           \
        Status status = (ans);                                                                                     \
        if (!status.IsSuccess()) {                                                                                 \
            std::cerr << "[P2P] npuError: \"" << status << "\"  in " << __FILE__ << ": " << __LINE__ << std::endl; \
            exit(1);                                                                                               \
        }                                                                                                          \
    } while (0)

#define ACL_CHECK_STATUS(ans)                                                                                  \
    do {                                                                                                       \
        aclError ret = (ans);                                                                                  \
        if (ret != ACL_SUCCESS) {                                                                              \
            std::string estr =                                                                                 \
                "aclError: \"" + std::to_string(ret) + "\"  in " + __FILE__ + ": " + std::to_string(__LINE__); \
            return Status::Error(ErrorCode::ACL_ERROR, estr);                                                  \
        }                                                                                                      \
    } while (0)

#define ACL_STATUS(ans) convertStatus((ans), __FILE__, __LINE__)
inline Status convertStatus(aclError ret, const char *file, int line)
{
    if (ret != ACL_SUCCESS) {
        std::string estr = "[P2P] aclError: \"" + std::to_string(ret) + "\"  in " + file + ": " + std::to_string(line);
        return Status::Error(ErrorCode::ACL_ERROR, estr);
    }
    return Status::Success();
}

#define ACL_CHECK(ans)                                                                                          \
    do {                                                                                                        \
        aclError ret = ans;                                                                                     \
        if (ret != ACL_SUCCESS) {                                                                               \
            std::cerr << "[P2P] npuError: \"" << ret << "\"  in " << __FILE__ << ": " << __LINE__ << std::endl; \
            return ret;                                                                                         \
        }                                                                                                       \
    } while (0)

#define ACL_CHECK_HCCL(ans)        \
    do {                           \
        aclError ret = (ans);      \
        if (ret != ACL_SUCCESS) {  \
            return HCCL_E_RUNTIME; \
        }                          \
    } while (0)

#define CHECK_HCCL(ans)            \
    do {                           \
        HcclResult ret = (ans);    \
        if (ret != HCCL_SUCCESS) { \
            return ret;            \
        }                          \
    } while (0)

#define LOG_STATUS(ans)                                   \
    do {                                                  \
        Status status = (ans);                            \
        if (!status.IsSuccess()) {                        \
            std::cerr << "[P2P] " << status << std::endl; \
        }                                                 \
    } while (0)

#define CHECK_STATUS(ans)          \
    do {                           \
        Status status = (ans);     \
        if (!status.IsSuccess()) { \
            return status;         \
        }                          \
    } while (0)

#define CHECK_STATUS_HCCL(ans)                            \
    do {                                                  \
        Status status = (ans);                            \
        if (!status.IsSuccess()) {                        \
            std::cerr << "[P2P] " << status << std::endl; \
            return HCCL_E_INTERNAL;                       \
        }                                                 \
    } while (0)

// Sets message error message and sends message if there is an error
#define SEND_MSG_ERROR(receiver, status, errorPrefix, msgObject)            \
    do {                                                                    \
        if (!(status).IsSuccess()) {                                        \
            (msgObject).set_error_msg((errorPrefix) + (status).ToString()); \
            (msgObject).set_failed(true);                                   \
            (receiver)->SendObject(msgObject);                              \
            return (status);                                                \
        }                                                                   \
    } while (0)

#endif  // P2P_NPU_ERROR_H