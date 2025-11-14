
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