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

#ifndef DATASYSTEM_COMMON_RDMA_UCP_SEGMENT_H
#define DATASYSTEM_COMMON_RDMA_UCP_SEGMENT_H

#include <atomic>
#include <memory>
#include <string>
#include <cstdint>

#include "ucp/api/ucp.h"
#include "ucp/api/ucp_def.h"

#include "datasystem/utils/status.h"
#include "datasystem/common/util/lock_map.h"

namespace datasystem {

class UcpSegment {
public:
    UcpSegment() = default;

    explicit UcpSegment(uintptr_t localSegAddr, size_t localSegSize, const ucp_context_h &ucpContext);
    virtual ~UcpSegment();

    UcpSegment &operator=(UcpSegment &&other) noexcept;

    /**
     * @brief initialize UcpSegment. Turn inputs into desired format for UCP use
     * @return Status::OK() if successful, otherwise error message
     */
    virtual Status Init();

    /**
     * @brief fetch the packed rkey to send to a remote server
     * @return the packed rkey in the form of string
     */
    virtual const std::string &GetPackedRkey() const
    {
        return packedRkey_;
    }

    /**
     * @brief fetch local buffer header to send to a remote server
     * @return the pointer of local buffer
     */
    virtual void *GetLocalSegAddr() const
    {
        return memBuffer_;
    }

    /**
     * @brief fetch the local buffer size. Normally not used
     * @return the size of the local buffer prepared to receive content from a remote server
     */
    virtual size_t GetLocalSegSize() const
    {
        return memSize_;
    }

private:
    // env variables
    ucp_context_h context_;

    // local-only variables
    void *memBuffer_;
    size_t memSize_;
    ucp_mem_h memH_ = nullptr;

    // utility variables
    std::string packedRkey_;
};

using UcpSegmentMap = LockMap<uint64_t, UcpSegment>;

}  // namespace datasystem

#endif