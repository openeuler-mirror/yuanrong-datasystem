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

#include <cstdint>
#include <string>
#include <vector>

#include "ucp/api/ucp.h"
#include "ucp/api/ucp_def.h"

namespace datasystem {

class PrepareLocalServer {
public:
    PrepareLocalServer(const ucp_context_h &context, const std::string &data);

    ~PrepareLocalServer();

    std::string ReadBuffer();

    inline uintptr_t Address() const
    {
        return bufferAddr_;
    }
    inline size_t Size() const
    {
        return dataSize_;
    }

private:
    ucp_context_h context_;
    ucp_mem_h memH_ = nullptr;
    std::vector<char> data_;
    uintptr_t bufferAddr_ = 0;
    size_t actualSize_ = 0;
    size_t dataSize_ = 0;
};

}  // namespace datasystem