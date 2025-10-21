/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2024. All rights reserved.
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
 * Description: Defines the API manager base class for worker to master API.
 */
#ifndef DATASYSTEM_COMMON_RPC_RPC_STUB_BASE_H
#define DATASYSTEM_COMMON_RPC_RPC_STUB_BASE_H

#include "datasystem/utils/status.h"

namespace datasystem {
class RpcStubBase {
public:
    virtual ~RpcStubBase() = default;

    /**
     * @brief Get init status.
     * @return Status of the call.
     */
    virtual Status GetInitStatus() = 0;
};
}  // namespace datasystem
#endif  // DATASYSTEM_COMMON_RPC_RPC_STUB_BASE_H
