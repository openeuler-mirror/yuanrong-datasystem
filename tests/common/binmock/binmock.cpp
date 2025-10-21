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

#include "binmock.h"

namespace testing {
void StubMgr::Add(size_t id, std::shared_ptr<BaseStub> stub)
{
    stubs_.emplace(id, stub);
}

std::shared_ptr<BaseStub> StubMgr::GetStub(size_t id)
{
    auto iter = stubs_.find(id);
    if (iter != stubs_.end()) {
        return iter->second;
    }
    return {};
}

void StubMgr::Clear()
{
    stubs_.clear();
}

StubMgr &StubMgr::Instance()
{
    static StubMgr ins;
    return ins;
}
}  // namespace testing