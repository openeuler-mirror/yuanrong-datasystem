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

#include <mutex>
#include <chrono>
#include <thread>

#include "datasystem/common/log/log.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {

UcpEndpoint::UcpEndpoint(const ucp_worker_h &localWorker, const std::string &remoteWorkerAddr)
    : worker_(localWorker), remoteWorkerData_(remoteWorkerAddr)
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
    epParams.err_mode = UCP_ERR_HANDLING_MODE_PEER;

    static ucp_err_handler_cb_t errCb = [](void *arg, ucp_ep_h ep, ucs_status_t status) {
        (void)arg;
        (void)ep;
        LOG(ERROR) << "[UcpEndpoint] error: " << ucs_status_string(status);
    };
    epParams.err_handler.cb = errCb;
    epParams.err_handler.arg = nullptr;

    ucs_status_t status = ucp_ep_create(worker_, &epParams, &ep_);
    if (status != UCS_OK) {
        LOG(ERROR) << "[UcpEndpoint] ucp_ep_create failed with status " << ucs_status_string(status);
        RETURN_STATUS(K_RDMA_ERROR, "[UcpEndpoint] ucp_ep_create failed");
    }

    return Status::OK();
}

ucp_rkey_h UcpEndpoint::GetOrUnpackRkey(const std::string &remoteRkey)
{
    {
        std::shared_lock readLock(rkeyMutex_);
        if (unpackedRkey_ != nullptr && remoteRkey_ == remoteRkey) {
            return unpackedRkey_;
        }
    }

    {
        std::unique_lock writeLock(rkeyMutex_);
        CleanUnpackedRkey();
        remoteRkey_ = remoteRkey;
        ucs_status_t status = ucp_ep_rkey_unpack(ep_, remoteRkey.data(), &unpackedRkey_);
        if (status != UCS_OK) {
            LOG(ERROR) << "[UcpEndpoint] Failed to unpack rkey with status " << ucs_status_string(status);
        }
        return unpackedRkey_;
    }
}

void UcpEndpoint::CleanUnpackedRkey()
{
    if (unpackedRkey_ != nullptr) {
        ucp_rkey_destroy(unpackedRkey_);
        unpackedRkey_ = nullptr;
    }
}

void UcpEndpoint::Clean()
{
    // clean up rkey
    {
        std::unique_lock writeLock(rkeyMutex_);
        CleanUnpackedRkey();
    }

    // clean up ep_
    if (ep_ != nullptr) {
        ucp_request_param_t param = {};
        param.op_attr_mask = UCP_OP_ATTR_FIELD_FLAGS;
        param.flags = UCP_EP_CLOSE_FLAG_FORCE;
        ucp_ep_close_nbx(ep_, &param);
        ep_ = nullptr;
    }
}

}  // namespace datasystem