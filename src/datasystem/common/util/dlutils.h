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
 * Description: The wrapper of dl function.
 */

#ifndef DATASYSTEM_COMMON_UTIL_DL_UTILS_H
#define DATASYSTEM_COMMON_UTIL_DL_UTILS_H

#include <dlfcn.h>
#include <string>

#include "datasystem/common/log/log.h"

/*
 * Register plugin method. Generate method name, method std::function object, function pointer and class member
 * variable.
 */
#define REG_METHOD(name, return_type, ...)                        \
    static constexpr const char *name##Name = #name;              \
    using name##FunObj = std::function<return_type(__VA_ARGS__)>; \
    using name##FunPtr = return_type (*)(__VA_ARGS__);            \
    name##FunPtr name##Func_ = nullptr

// dlsym the class member function pointer.
#define DLSYM_FUNC_OBJ(funcName, pluginHandle)                                           \
    do {                                                                                 \
        funcName##Func_ = DlsymWithCast<funcName##FunPtr>(pluginHandle, funcName##Name); \
    } while (false)

namespace datasystem {
// Return the dl error message.
inline std::string GetDlErrorMsg()
{
    const char *result = dlerror();
    return (result == nullptr) ? "Unknown" : result;
}

// dlsym the function pointer by handle.
template <class T>
static T DlsymWithCast(void *handle, const char *symbolName)
{
    T symbol = reinterpret_cast<T>(reinterpret_cast<intptr_t>(dlsym(handle, symbolName)));
    if (symbol == nullptr) {
        LOG(ERROR) << "Dynamically load symbol " << symbolName << " failed: " << GetDlErrorMsg();
    }
    return symbol;
}
}  // namespace datasystem
#endif