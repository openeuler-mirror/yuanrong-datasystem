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
 * Description: mock driver for file device IPC
 */

#include "datasystem/common/os_transport_pipeline/mock_rh2d_driver.h"

#include <fstream>

namespace OsXprtPipln {

using namespace datasystem;

std::string MockRH2DDriver::mockFilePathPrefix = "/tmp/mock.data";

Status MockRH2DDriver::Init()
{
    mockFilePath = mockFilePathPrefix + "." + std::to_string(reqId_);
    return Status::OK();
}

void MockRH2DDriver::SetReqId(uint32_t reqId)
{
    reqId_ = reqId;
}

Status MockRH2DDriver::SubmitIO(void *srcData, size_t srcSize, size_t destOffset)
{
    {
        std::ofstream guaranteeExists(mockFilePath, std::ios::app | std::ios::binary);
    }
    std::fstream outFile(mockFilePath, std::ios::in | std::ios::out | std::ios::binary);
    outFile.seekp(destOffset, std::ios::beg);
    outFile.write((char *)srcData, srcSize);
    if (!outFile.good()) {
        return Status(StatusCode::K_IO_ERROR, "Submit io for offset " + std::to_string(destOffset) + " failed");
    }
    return Status::OK();
}

Status MockRH2DDriver::WaitIO()
{
    return Status::OK();
}

Status MockRH2DDriver::Release()
{
    return Status::OK();
}

}  // namespace OsXprtPipln