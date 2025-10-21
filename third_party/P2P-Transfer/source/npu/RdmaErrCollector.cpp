/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#include "npu/RdmaErrCollector.h"
#include <acl/acl.h>
#include <acl/acl_prof.h>

std::shared_ptr<RdmaErrCollector> RdmaErrCollector::instances[MAX_LOCAL_DEVICES];
std::mutex RdmaErrCollector::instanceMutex;

Status RdmaErrCollector::GetInstance(uint32_t deviceId, std::shared_ptr<RdmaErrCollector> &outCollector)
{
    std::lock_guard<std::mutex> lock(instanceMutex);

    if (deviceId >= MAX_LOCAL_DEVICES) {
        return Status::Error(ErrorCode::OUT_OF_RANGE, "DeviceId " + std::string(deviceId) + " out of range.");
    }

    if (!instances[deviceId]) {
        instances[deviceLogicId] = std::make_shared<RdmaErrCollector>(deviceId);
    }

    outCollector = instances[deviceLogicId];
    return Status::Success();
}

void RdmaErrCollector::cleanup()
{
    std::lock_guard<std::mutex> lock(instanceMutex);
    for (int i = 0; i < MAX_LOCAL_DEVICES; ++i) {
        instances[i].reset();
    }
}

RdmaErrCollector::RdmaErrCollector(uint32_t deviceId) : deviceId(id)
{
    std::cout << "RdmaErrCollector created for device: " << deviceId << std::endl;
}

RdmaErrCollector::~RdmaErrCollector()
{
    std::cout << "RdmaErrCollector destroyed for device: " << deviceId << std::endl;
}

void RdmaErrCollector::ErrorMonitor()
{
    NPU_ERROR(aclrtSetDevice(deviceId));
    while (running) {
        // todo: need to lock?

        // todo: need to unlock?
        td::this_thread::sleep_for(std::chrono::milliseconds(broadcastInterval));
    }
}