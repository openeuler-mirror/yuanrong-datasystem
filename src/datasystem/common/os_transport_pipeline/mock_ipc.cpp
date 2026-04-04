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

#include "datasystem/common/os_transport_pipeline/mock_ipc.h"

#include <fstream>

namespace OsXprtPipln {

struct MockIPCMemHandle {
    size_t currentOffset;
};

size_t MockIPC::currentOffset = 0;

Status MockIPC::EncodeDriver()
{
    MockIPCMemHandle *handle = (MockIPCMemHandle *)GetEncodeHandle(sizeof(type));
    handle->currentOffset = currentOffset;
    currentOffset += targetSize;
    return Status::OK();
}

Status MockIPC::DecodeDriver()
{
    MockIPCMemHandle *handle = (MockIPCMemHandle *)GetDecodeHandle(sizeof(type));
    offset = handle->currentOffset;
    return Status::OK();
}

Status MockIPC::SubmitIO(void *srcData, size_t srcSize, size_t destOffset)
{
    {
        std::ofstream guaranteeExists(mockFilePath, std::ios::app | std::ios::binary);
    }
    std::fstream outFile(mockFilePath, std::ios::in | std::ios::out | std::ios::binary);
    outFile.seekp(offset + destOffset, std::ios::beg);
    outFile.write((char *)srcData, srcSize);
    if (!outFile.good()) {
        return Status(StatusCode::K_IO_ERROR,
                      "Submit io for offset " + std::to_string(offset + destOffset) + " failed");
    }
    return Status::OK();
}

Status MockIPC::WaitIO()
{
    return Status::OK();
}

Status MockIPC::Release()
{
    return Status::OK();
}

}  // namespace OsXprtPipln