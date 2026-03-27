/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 */
#ifndef TEST_ERROR_H
#define TEST_ERROR_H
#include <iostream>
#include "acl/acl.h"

#define NPU_ERROR(ans)                                                                                          \
    do {                                                                                                        \
        aclError ret = ans;                                                                                     \
        if (ret != ACL_SUCCESS) {                                                                               \
            std::cerr << "[P2P] npuError: \"" << ret << "\"  in " << __FILE__ << ": " << __LINE__ << std::endl; \
            return ret;                                                                                         \
        }                                                                                                       \
    } while (0)

#endif  // TEST_ERROR_H