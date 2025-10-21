/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 */
#ifndef P2P_RDMA_ERR_COLL_H
#define P2P_RDMA_ERR_COLL_H
#include <stddef.h>
#include <array>
#include <cstring>
#include "tools/Status.h"
#include "tools/npu-error.h"
#include "acl/acl.h"
#include "experiment/msprof/toolchain/prof_api.h"

constexpr uint32_t MAX_LOCAL_DEVICES = 16;

class RdmaErrCollector {
public:
    ~RdmaErrCollector();

    static Status GetInstance(uint32_t deviceId, std::shared_ptr<RdmaErrCollector> &outCollector);

    static void cleanup()
    {
        std::lock_guard<std::mutex> lock(instanceMutex);
        for (int i = 0; i < MAX_LOCAL_DEVICES; ++i) {
            instances[i].reset();
        }
    }

private:
    static std::shared_ptr<RdmaErrCollector>[MAX_LOCAL_DEVICES] instances;
    static std::mutex instanceMutex;

    explicit RdmaErrCollector(uint32_t deviceId);

    RdmaErrCollector(const RdmaErrCollector &) = delete;
    RdmaErrCollector &operator=(const RdmaErrCollector &) = delete;

    uint32_t deviceId;
    bool running;
};

#endif  // P2P_RDMA_ERR_COLL_H