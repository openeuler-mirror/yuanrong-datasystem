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

/**
 * Description: Mimic a remote server, prepare a local buffer and provide info
 * needed as if sent by a remote server. Tool for Ucp tests.
 */

#ifndef COMMON_RDMA_MIMIC_REMOTE_SERVER_H
#define COMMON_RDMA_MIMIC_REMOTE_SERVER_H

#include <cstdlib>
#include <string>
#include <memory>
#include <atomic>

#include "ucp/api/ucp.h"
#include "ucp/api/ucp_def.h"
#include "datasystem/common/util/thread.h"

namespace datasystem {

class MimicRemoteServer {
public:
    MimicRemoteServer(ucp_context_h &context);
    ~MimicRemoteServer();

    void InitUcpWorker();
    void InitUcpSegment();

    std::string ReadBuffer(size_t len);

    inline ucp_worker_h GetWorker()
    {
        return worker_;
    }
    inline std::string GetWorkerAddr()
    {
        return localWorkerAddrStr_;
    }
    inline std::string &GetPackedRkey()
    {
        return packedRkey_;
    }
    inline void *GetLocalSegAddr()
    {
        return buffer_;
    }
    inline size_t GetLocalSegSize()
    {
        return buf_size_;
    }

private:
    void StartProgressThread();
    void ProgressLoop();
    void StopProgressThread();
    ucp_context_h context_;

    ucp_worker_h worker_ = nullptr;
    ucp_address_t *localWorkerAddr_ = nullptr;
    std::string localWorkerAddrStr_;

    void *buffer_ = nullptr;
    size_t buf_size_ = 1024;
    ucp_mem_h memH_ = nullptr;

    // utility variables
    std::string packedRkey_;

    std::unique_ptr<datasystem::Thread> progressThread_;
    std::atomic<bool> running_{false}; 
};

}  // namespace datasystem
#endif
