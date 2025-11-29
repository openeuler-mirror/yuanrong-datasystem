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
 * Description: StringPool implementation.
 */

#include "datasystem/common/string_intern/string_pool.h"
#include "datasystem/common/string_intern/key_type.h"
#include "datasystem/common/log/log.h"

namespace datasystem {
namespace intern {

namespace {
// clang-format off
#define KEY_TYPE_DEF(keyType, keyEnum) case KeyType::keyEnum: return #keyType;
std::string GetKeyTypeName(KeyType keyType)
{
    switch (keyType) {
#include "datasystem/common/string_intern/key_type.def"
    }
    return "unknown";
}
#undef KEY_TYPE_DEF
// clang-format on
}  // namespace

#define KEY_TYPE_DEF(keyType, keyEnum)                \
    case KeyType::keyEnum: {                          \
        static StringPool instance(KeyType::keyEnum); \
        return instance;                              \
    }
StringPool &StringPool::Instance(KeyType keyType)
{
    switch (keyType) {
#include "datasystem/common/string_intern/key_type.def"
    }
    static StringPool instance(KeyType::OTHER);
    return instance;
}
#undef KEY_TYPE_DEF

// clang-format off
#define KEY_TYPE_DEF(keyType, keyEnum) StringPool::Instance(KeyType::keyEnum).Init();
void StringPool::InitAll()
{
#include "datasystem/common/string_intern/key_type.def"
}
#undef KEY_TYPE_DEF
// clang-format on

StringPool::~StringPool()
{
    if (Size() > 0) {
        LOG(ERROR) << "Some StringRef of " << GetKeyTypeName(keyType_) << " still in StringPool: " << Size()
                   << " when pool finalize, may cause segment fault.";
    }
}

void StringPool::Init() const
{
    LOG(INFO) << "StringPool init for " << GetKeyTypeName(keyType_);
}

StringPtr StringPool::Intern(const std::string &val)
{
    StringEntity rcStr(val);
    StringEntityMap::const_accessor readAccessor;
    (void)pool_.insert(readAccessor, rcStr);
    return StringPtr(readAccessor->first);
}

void StringPool::Erase(StringPtr &handle)
{
    const auto val = handle.GetEntity();
    StringEntityMap::accessor accessor;
    if (val != nullptr && pool_.find(accessor, *val) && accessor->first.DecDelRef()) {
        (void)pool_.erase(accessor);
    }
}

size_t StringPool::Size() const
{
    return pool_.size();
}
}  // namespace intern
}  // namespace datasystem
