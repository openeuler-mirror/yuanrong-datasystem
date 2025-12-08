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
 * Description: Create a global ucp context. Tool for Ucp tests.
 */

#ifndef COMMON_RDMA_CREATE_UCP_CONTEXT_H
#define COMMON_RDMA_CREATE_UCP_CONTEXT_H

#include <cstring>

#include "ucp/api/ucp.h"
#include "ucp/api/ucp_def.h"

namespace datasystem {

class CreateUcpContext {
public:
    CreateUcpContext();
    ~CreateUcpContext();

    inline ucp_context_h GetContext() const
    {
        return context_;
    }

private:
    void InitContext();
    ucp_context_h context_;
};

}  // namespace datasystem

#endif