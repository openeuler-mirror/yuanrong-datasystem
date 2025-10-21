/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2022. All rights reserved.
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
 * Description: bitmask enum class.
 */
#ifndef DATASYSTEM_COMMON_UTIL_BITMASK_ENUM_H
#define DATASYSTEM_COMMON_UTIL_BITMASK_ENUM_H

#include <cstdint>
#include <memory>
#include <type_traits>
#include <utility>

// C++ has a nice enum class functionality where you can define an underlying type for the enum values.
// For example, you can make an enum class with an underlying type of  uint32_t, and use that for bit flags.
//
// However, it does not implement the bitwise ops like &, |, |=, &= etc, so out of the box, it is not usable for
// bitmasks unless you override those operators.
// It would be tedious to manually override the bitwise operators for every time you want to make a bitmask enum.
// This set of template functions provide the override for you, however you need to enable this because we do not want
// to enable these overrides for all enums. std::enable_if and the macro ENABLE_BITMASK_ENUM_OPS toggle the enablement
// of the operator overloads.

#define ENABLE_BITMASK_ENUM_OPS(T) constexpr bool bitmask_ops(T) { return true; }

#define TESTFLAG(lhs, rhs) (((lhs) & (rhs)) == (rhs))
#define SETFLAG(target, source) (target) |= (source)
#define CLEARFLAG(target, source) (target) &= ~(source)
#define INITFLAG(target, source) (target) = (source)

inline uint32_t ClearUint32OddBits(uint32_t bitmap)
{
    uint32_t evenMask = 0x55555555;
    return bitmap &= evenMask;
}

inline uint32_t ClearUint32EvenBits(uint32_t bitmap)
{
    uint32_t oddMask = 0xAAAAAAAA;
    return bitmap &= oddMask;
}

namespace datasystem {
template<typename T>
constexpr bool bitmask_ops(T)
{
    return false;
}

template<typename T>
typename std::enable_if<bitmask_ops(T()), T>::type operator|(T lhs, T rhs)
{
    using enum_under_type = typename std::underlying_type<T>::type;
    return static_cast<T>(static_cast<enum_under_type>(lhs) | static_cast<enum_under_type>(rhs));
}

template<typename T>
typename std::enable_if<bitmask_ops(T()), T>::type operator&(T lhs, T rhs)
{
    using enum_under_type = typename std::underlying_type<T>::type;
    return static_cast<T>(static_cast<enum_under_type>(lhs) & static_cast<enum_under_type>(rhs));
}

template<typename T>
typename std::enable_if<bitmask_ops(T()), T>::type operator^(T lhs, T rhs)
{
    using enum_under_type = typename std::underlying_type<T>::type;
    return static_cast<T>(static_cast<enum_under_type>(lhs) ^ static_cast<enum_under_type>(rhs));
}

template<typename T>
typename std::enable_if<bitmask_ops(T()), T>::type operator~(T lhs)
{
    using enum_under_type = typename std::underlying_type<T>::type;
    return static_cast<T>(~static_cast<enum_under_type>(lhs));
}

template<typename T>
typename std::enable_if<bitmask_ops(T()), T&>::type operator|=(T& lhs, T rhs)
{
    using enum_under_type = typename std::underlying_type<T>::type;
    lhs = static_cast<T>(static_cast<enum_under_type>(lhs) | static_cast<enum_under_type>(rhs));
    return lhs;
}

template<typename T>
typename std::enable_if<bitmask_ops(T()), T&>::type operator&=(T& lhs, T rhs)
{
    using enum_under_type = typename std::underlying_type<T>::type;
    lhs = static_cast<T>(static_cast<enum_under_type>(lhs) & static_cast<enum_under_type>(rhs));
    return lhs;
}

template<typename T>
typename std::enable_if<bitmask_ops(T()), T&>::type operator^=(T& lhs, T rhs)
{
    using enum_under_type = typename std::underlying_type<T>::type;
    lhs = static_cast<T>(static_cast<enum_under_type>(lhs) ^ static_cast<enum_under_type>(rhs));
    return lhs;
}
}  // namespace datasystem
#endif  // DATASYSTEM_COMMON_UTIL_BITMASK_ENUM_H
