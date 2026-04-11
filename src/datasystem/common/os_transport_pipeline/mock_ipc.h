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
 * Description: define mock IPC driver
 */

#ifndef OS_XPRT_PIPLN_MOCK_IPC
#define OS_XPRT_PIPLN_MOCK_IPC

#include "datasystem/common/os_transport_pipeline/chunk_manager.h"

namespace OsXprtPipln {
class MockIPC : public IpcDriver {
public:
    MockIPC(const DevShmInfo &devInfo, const std::string &targetHandle, bool isClient)
        : IpcDriver(devInfo, targetHandle, isClient)
    {
    }
    virtual ~MockIPC()
    {
        Release();
    }

    Status SubmitIO(void *srcData, size_t srcSize, size_t destOffset) override;
    Status EncodeDriver() override;
    Status DecodeDriver() override;
    Status WaitIO() override;
    Status Release() override;

private:
    static size_t currentOffset;
    size_t offset;
    std::string mockFilePath = "/tmp/mock.data";
};
}  // namespace OsXprtPipln

#endif
