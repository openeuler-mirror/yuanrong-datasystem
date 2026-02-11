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
 * Description: UcpSegment instance that stores local information needed for
 * receiving content, containing local rkey as string, buffer info and buffer-turned
 * ucp_mem_h object (local)
 */

#include "datasystem/common/rdma/ucp_segment.h"

#include <cstring>

#include "datasystem/common/util/status_helper.h"

namespace datasystem {

UcpSegment::UcpSegment(uintptr_t localSegAddr, size_t localSegSize, const ucp_context_h &ucpContext)
    : context_(ucpContext), memBuffer_(reinterpret_cast<void *>(localSegAddr)), memSize_(localSegSize)
{
}

UcpSegment::~UcpSegment()
{
    if (memH_) {
        ds_ucp_mem_unmap(context_, memH_);
    }
}

UcpSegment &UcpSegment::operator=(UcpSegment &&other) noexcept
{
    if (this != &other) {
        if (memH_ && context_) {
            ds_ucp_mem_unmap(context_, memH_);
        }
        context_ = other.context_;
        memBuffer_ = other.memBuffer_;
        memSize_ = other.memSize_;
        memH_ = other.memH_;
        packedRkey_ = std::move(other.packedRkey_);
        other.context_ = nullptr;
        other.memBuffer_ = nullptr;
        other.memH_ = nullptr;
    }
    return *this;
}

Status UcpSegment::Init()
{
    ucp_mem_map_params_t params = {};
    params.field_mask = UCP_MEM_MAP_PARAM_FIELD_ADDRESS | UCP_MEM_MAP_PARAM_FIELD_LENGTH;
    params.address = memBuffer_;
    params.length = memSize_;

    ucs_status_t status = ds_ucp_mem_map(context_, &params, &memH_);
    if (status != UCS_OK) {
        RETURN_STATUS(K_RDMA_ERROR, std::string("[UcpSegment] Failed to map memory: ") + ds_ucs_status_string(status));
    }

    void *rkeyBuffer;
    size_t rkeySize;
    status = ds_ucp_rkey_pack(context_, memH_, &rkeyBuffer, &rkeySize);
    if (status != UCS_OK) {
        LOG(ERROR) << "[UcpSegment] Failed to pack rkey: " << ds_ucs_status_string(status);
        ds_ucp_mem_unmap(context_, memH_);
        RETURN_STATUS(K_RDMA_ERROR, "[UcpSegment] Failed to pack rkey.");
    }

    packedRkey_ = std::string(static_cast<const char *>(rkeyBuffer), rkeySize);

    ds_ucp_rkey_buffer_release(rkeyBuffer);

    return Status::OK();
}
}  // namespace datasystem