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
 * Description: container util.
 */
#ifndef DATASYSTEM_COMMON_UTIL_CONTAINER_UTIL_H
#define DATASYSTEM_COMMON_UTIL_CONTAINER_UTIL_H

#include <algorithm>
#include <map>
#include <set>
#include <vector>

namespace datasystem {
// comparator sorted by value
struct SortByValue {
    template <typename Key, typename Value>
    bool operator()(const std::pair<Key, Value> &l, const std::pair<Key, Value> &r) const
    {
        return l.second < r.second;
    }
};

// erase specific elements
template <typename T, typename PredFn>
typename std::vector<T>::size_type EraseIf(std::vector<T> &vec, PredFn pred) noexcept
{
    auto it = std::remove_if(vec.begin(), vec.end(), pred);
    auto num = std::distance(it, vec.end());
    vec.erase(it, vec.end());
    return num;
}

// check if container has find member method
template <typename Container, typename Value>
auto HasFindMethod(const Container &c, const Value &v) -> decltype(c.find(v), std::true_type{})
{
    return std::true_type{};
}
inline std::false_type HasFindMethod(...)
{
    return std::false_type{};
}
// check if element in container
template <typename Container, typename Value>
bool ContainsKey(const Container &c, const Value &v)
{
    if constexpr (decltype(HasFindMethod(c, v))::value) {
        return c.find(v) != c.end();
    } else {
        return std::find(c.begin(), c.end(), v) != c.end();
    }
}

// get keys from map-like container
template <typename PairsContainer>
auto GetKeysFromPairsContainer(const PairsContainer &c)
{
    using KeyType = std::decay_t<decltype(c.begin()->first)>;
    std::set<KeyType> keys;
    std::for_each(c.begin(), c.end(), [&](auto &pair) { keys.template emplace(pair.first); });
    return keys;
}

// insert/erase an element form a set-like container
template <typename SetType, typename ValueType>
void UpdateSetByCondition(const ValueType &value, bool condition, SetType &s)
{
    if (condition) {
        s.insert(value);
    } else {
        s.erase(value);
    }
}
}  // namespace datasystem
#endif  // DATASYSTEM_COMMON_UTIL_CONTAINER_UTIL_H
