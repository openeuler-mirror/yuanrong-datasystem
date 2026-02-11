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
 * Description: Acquire and prepare a local buffer for Ucp communication. Tool
 * for Ucp tests.
 */

#include "common/rdma/prepare_local_server.h"
#include "datasystem/common/rdma/ucp_dlopen_util.h"

namespace datasystem {

PrepareLocalServer::PrepareLocalServer(const ucp_context_h &context, const std::string &data) : context_(context)
{
    size_t alignedSize = (data.size() + 63) & ~63;
    data_.resize(alignedSize);
    std::copy(data.begin(), data.end(), data_.begin());

    ucp_mem_map_params_t params = {};
    params.field_mask =
        UCP_MEM_MAP_PARAM_FIELD_ADDRESS | UCP_MEM_MAP_PARAM_FIELD_LENGTH | UCP_MEM_MAP_PARAM_FIELD_FLAGS;
    params.address = data_.data();
    params.length = alignedSize;
    params.flags = 0;

    ds_ucp_mem_map(context_, &params, &memH_);

    ucp_mem_attr_t mem_attr = {};
    mem_attr.field_mask = UCP_MEM_ATTR_FIELD_ADDRESS;
    ds_ucp_mem_query(memH_, &mem_attr);

    bufferAddr_ = reinterpret_cast<uintptr_t>(mem_attr.address);
    actualSize_ = alignedSize;
    dataSize_ = data.size();
}

PrepareLocalServer::~PrepareLocalServer()
{
    if (memH_ != nullptr) {
        ds_ucp_mem_unmap(context_, memH_);
    }
}

std::string PrepareLocalServer::ReadBuffer()
{
    const char *data = reinterpret_cast<const char *>(bufferAddr_);
    std::string msg(data, actualSize_);
    return msg;
}
}  // namespace datasystem