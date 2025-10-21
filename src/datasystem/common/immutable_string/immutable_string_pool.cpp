/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2023. All rights reserved.
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
 * Description: Immutable string pool implementation.
 */
#include "datasystem/common/immutable_string/immutable_string_pool.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/immutable_string/ref_count_string.h"

namespace datasystem {

ImmutableStringPool &ImmutableStringPool::Instance()
{
    static ImmutableStringPool instance;
    return instance;
}

void ImmutableStringPool::Init()
{
    LOG(INFO) << "ImmutableStringPool init";
}

void ImmutableStringPool::Intern(const std::string &val, RefCountStringHandle &handle)
{
    RefCountString rcStr(val);
    RCStringMap::const_accessor readAccessor;
    (void)pool_.insert(readAccessor, rcStr);
    handle = RefCountStringHandle(readAccessor->first);
}

void ImmutableStringPool::Erase(RefCountStringHandle &handle)
{
    const auto &val = handle.ToRefCountStr();
    RCStringMap::accessor accessor;
    if (pool_.find(accessor, val) && accessor->first.ReleaseDelRef()) {
        (void)pool_.erase(accessor);
    }
}

size_t ImmutableStringPool::Size()
{
    return pool_.size();
}

ImmutableStringPool::~ImmutableStringPool()
{
    if (Size() > 0) {
        LOG(ERROR) << "Some RCString still in pool: " << Size() << " when pool finalize, may cause segment fault.";
    }
}
}  // namespace datasystem