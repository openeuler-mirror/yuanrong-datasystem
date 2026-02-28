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

#include "common/rdma/mimic_remote_server.h"
#include "datasystem/common/rdma/ucp_dlopen_util.h"
#include "datasystem/common/log/log.h"

#include <cstdint>
#include <cstring>
#include <poll.h>

namespace datasystem {

MimicRemoteServer::MimicRemoteServer(ucp_context_h &context) : context_(context)
{
}

MimicRemoteServer::~MimicRemoteServer()
{
    StopProgressThread();

    if (localWorkerAddr_) {
        ds_ucp_worker_release_address(worker_, localWorkerAddr_);
        localWorkerAddr_ = nullptr;
    }

    if (memH_) {
        ds_ucp_mem_unmap(context_, memH_);
        memH_ = nullptr;
    }

    if (worker_) {
        ds_ucp_worker_destroy(worker_);
        worker_ = nullptr;
    }

    if (buffer_) {
        buffer_ = nullptr;
    }
}

void MimicRemoteServer::InitUcpWorker()
{
    ucp_worker_params_t workerParams = {};
    workerParams.field_mask = UCP_WORKER_PARAM_FIELD_THREAD_MODE;
    workerParams.thread_mode = UCS_THREAD_MODE_MULTI;

    ds_ucp_worker_create(context_, &workerParams, &worker_);
    size_t workerAddrLen;

    ds_ucp_worker_get_address(worker_, &localWorkerAddr_, &workerAddrLen);

    localWorkerAddrStr_ = std::string(reinterpret_cast<const char *>(localWorkerAddr_), workerAddrLen);

    StartProgressThread();
}

void MimicRemoteServer::StartProgressThread()
{
    if (running_.load()) {
        return;
    }
    running_.store(true);
    progressThread_ = std::make_unique<Thread>(&MimicRemoteServer::ProgressLoop, this);
}

void MimicRemoteServer::StopProgressThread()
{
    if (!running_.load()) {
        return;
    }

    running_.store(false);
    if (progressThread_ && progressThread_->joinable()) {
        progressThread_->join();
        progressThread_.reset();
    }
}

void MimicRemoteServer::ProgressLoop()
{
    int fd;
    ucs_status_t status = ds_ucp_worker_get_efd(worker_, &fd);
    if (status != UCS_OK) {
        LOG(ERROR) << " Failed to get efd. Status: " << ds_ucs_status_string(status);
    }

    while (running_.load()) {
        bool innerBreak = false;
        while (ds_ucp_worker_progress(worker_)) {
            if (!running_.load()) {
                innerBreak = true;
                break;
            }
        };

        if (innerBreak) {
            break;
        }

        status = ds_ucp_worker_arm(worker_);
        if (status == UCS_ERR_BUSY) {
            // meaning there are new jobs in QP again, just restart the while loop
            continue;
        }
        if (status != UCS_OK) {
            LOG(ERROR) << " Failed to arm worker. Status: " << ds_ucs_status_string(status);
        }

        struct pollfd pfd = { fd, POLLIN, 0 };
        int ret = poll(&pfd, 1, 5);
        if (ret < 0 && errno != EINTR) {
            LOG(ERROR) << " Progress thread failed to poll. Error: " << strerror(errno);
        }
    }
}

void MimicRemoteServer::InitUcpSegment()
{
    ucp_mem_map_params_t params = {};
    params.field_mask =
        UCP_MEM_MAP_PARAM_FIELD_ADDRESS | UCP_MEM_MAP_PARAM_FIELD_LENGTH | UCP_MEM_MAP_PARAM_FIELD_FLAGS;
    params.address = NULL;
    params.length = buf_size_;
    params.flags = UCP_MEM_MAP_ALLOCATE;

    ds_ucp_mem_map(context_, &params, &memH_);

    void *rkeyBuffer;
    size_t rkeySize;
    ds_ucp_rkey_pack(context_, memH_, &rkeyBuffer, &rkeySize);

    ucp_mem_attr_t mem_attr = {};
    mem_attr.field_mask = UCP_MEM_ATTR_FIELD_ADDRESS | UCP_MEM_ATTR_FIELD_LENGTH;
    ds_ucp_mem_query(memH_, &mem_attr);

    buffer_ = mem_attr.address;

    packedRkey_ = std::string(static_cast<const char *>(rkeyBuffer), rkeySize);

    ds_ucp_rkey_buffer_release(rkeyBuffer);
}

std::string MimicRemoteServer::ReadBuffer(size_t len)
{
    const char *data = reinterpret_cast<const char *>(buffer_);
    std::string msg(data, len);
    return msg;
}
}  // namespace datasystem