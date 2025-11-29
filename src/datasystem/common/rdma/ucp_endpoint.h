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
 * Description: UcpSegment endpoint, provides method to unpack an rkey and
 * store the unpacked rkey. Managed by UcpWorker.
 */

#ifndef DATASYSTEM_COMMON_RDMA_UCP_ENDPOINT_H
#define DATASYSTEM_COMMON_RDMA_UCP_ENDPOINT_H

#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>

#include "ucp/api/ucp.h"

#include "datasystem/utils/status.h"

namespace datasystem {

class UcpEndpoint {
public:
    UcpEndpoint() = default;

    explicit UcpEndpoint(const ucp_worker_h &localWorker, const std::string &remoteWorkerAddr);
    virtual ~UcpEndpoint();

    /**
     * @brief initialize a UcpEndpoint instance
     * @return Status::OK() if successful, otherwise error message
     */
    virtual Status Init();

    /**
     * @brief Unpack an rkey to this endpoint
     * @param ucpContext Ucp context used in this environment. Passed in by manager
     * @param remoteRkey rkey from remote server
     * @param remoteSegAddr remote memory head pointer where content will be written to
     * @return an unpacked key used for ucp_put_nbx
     */
    virtual Status UnpackRkey(const std::string &remoteRkey);

    /**
     * @brief fetch the endpoint in this instance
     * @return the ucp endpoint object used for ucp_put_nbx
     */
    virtual ucp_ep_h GetEp() const
    {
        return ep_;
    };

    virtual ucp_rkey_h GetUnpackedRkey() const
    {
        return unpackedRkey_;
    }

private:
    void Clean();

    // env variables
    ucp_worker_h worker_;

    // variables for ep creation
    std::string remoteWorkerData_;

    // variables for rkey unpack
    ucp_ep_h ep_ = nullptr;
    ucp_rkey_h unpackedRkey_ = nullptr;
};
}  // namespace datasystem

#endif