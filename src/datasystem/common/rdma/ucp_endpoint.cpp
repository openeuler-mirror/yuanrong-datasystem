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

#include "datasystem/common/rdma/ucp_endpoint.h"

#include "ucp/api/ucp_def.h"

namespace datasystem {

UcpEndpoint::UcpEndpoint(const ucp_worker_h &localWorker, const std::string &remoteWorkerAddr)
    : worker_(localWorker),
      remoteWorkerData_(remoteWorkerAddr)
{
}

UcpEndpoint::~UcpEndpoint()
{
    Clean();
}

Status UcpEndpoint::Init()
{
    ucp_ep_params_t epParams = {};
    epParams.field_mask =
        UCP_EP_PARAM_FIELD_REMOTE_ADDRESS | UCP_EP_PARAM_FIELD_ERR_HANDLING_MODE | UCP_EP_PARAM_FIELD_ERR_HANDLER;
    epParams.address = reinterpret_cast<ucp_address_t *>(remoteWorkerData_.data());

    if (ucp_ep_create(worker_, &epParams, &ep_) != UCS_OK) {
        return Status(K_RDMA_ERROR, "[UcpEndpoint] ucp_ep_create failed");
    }

    return Status::OK();
}

Status UcpEndpoint::UnpackRkey(const std::string &remoteRkey)
{
    if (ucp_ep_rkey_unpack(ep_, remoteRkey.data(), &unpackedRkey_) != UCS_OK) {
        return Status(K_RDMA_ERROR, "[UcpEndpoint] Failed to unpack rkey");
    }

    return Status::OK();
}

void UcpEndpoint::Clean()
{
    // clean up rkey
    if (unpackedRkey_ != nullptr) {
        ucp_rkey_destroy(unpackedRkey_);
        unpackedRkey_ = nullptr;
    }

    // clean up ep_
    if (ep_ != nullptr) {
        ucp_ep_destroy(ep_);
        ep_ = nullptr;
    }
}

}  // namespace datasystem