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
 * Description: template util.
 */
#ifndef DATASYSTEM_COMMON_UTIL_TEMPLATE_UTIL_H
#define DATASYSTEM_COMMON_UTIL_TEMPLATE_UTIL_H

#include <memory>
#include <type_traits>

namespace datasystem {
// These two template structs enable a compile-time check to ensure that the input type is not a pointer.
template <typename T>
struct is_shared_ptr : std::false_type {};
template <typename T>
struct is_shared_ptr<std::shared_ptr<T>> : std::true_type {};

#define DEFINE_MEMBER_FUNCTION_CHECKER(FuncName) \
template <typename T_##FuncName, typename... Args> \
struct HasMemberFunc_##FuncName { \
    template <typename U> \
    static auto test(int) -> decltype(std::declval<U>().FuncName(std::declval<Args>()...), std::true_type{}); \
    template <typename U> \
    static std::false_type test(...); \
    static constexpr bool value = decltype(test<T_##FuncName>(0))::value; \
};
}  // namespace datasystem
#endif  // DATASYSTEM_COMMON_UTIL_template_UTIL_H
