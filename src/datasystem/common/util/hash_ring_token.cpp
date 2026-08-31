/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
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
 * Description: Shared hash-ring token derivation contract.
 */
#include "datasystem/common/util/hash_ring_token.h"

#include <charconv>

#include "datasystem/common/util/hash_algorithm.h"

namespace datasystem {
namespace {
constexpr char TOKEN_SEPARATOR = '#';
constexpr size_t MAX_UINT32_DECIMAL_DIGITS = 10;
constexpr size_t MAX_TOKEN_SUFFIX_BYTES = MAX_UINT32_DECIMAL_DIGITS * 2 + 2;

void InitializeTokenBuffer(const std::string &address, std::string &input)
{
    input.reserve(address.size() + MAX_TOKEN_SUFFIX_BYTES);
    input.assign(address);
    input.push_back(TOKEN_SEPARATOR);
    input.resize(address.size() + MAX_TOKEN_SUFFIX_BYTES);
}

uint32_t MakeTokenWithBuffer(size_t prefixSize, uint32_t index, uint32_t seed, std::string &input)
{
    auto *const begin = input.data();
    auto *const end = begin + input.size();
    auto *next = std::to_chars(begin + prefixSize, end, index).ptr;
    if (seed > 0) {
        *next++ = TOKEN_SEPARATOR;
        next = std::to_chars(next, end, seed).ptr;
    }
    return MurmurHash3_32(reinterpret_cast<const uint8_t *>(begin), static_cast<size_t>(next - begin));
}
}  // namespace

uint32_t MakeHashRingToken(const std::string &address, uint32_t index, uint32_t seed)
{
    std::string input;
    InitializeTokenBuffer(address, input);
    return MakeTokenWithBuffer(address.size() + 1, index, seed, input);
}

void MakeHashRingTokens(const std::string &address, const std::vector<uint32_t> &seeds,
                        std::vector<uint32_t> &tokens)
{
    std::string input;
    InitializeTokenBuffer(address, input);
    const auto prefixSize = address.size() + 1;
    std::vector<uint32_t> rebuilt;
    rebuilt.reserve(seeds.size());
    for (size_t index = 0; index < seeds.size(); ++index) {
        rebuilt.emplace_back(MakeTokenWithBuffer(prefixSize, static_cast<uint32_t>(index), seeds[index], input));
    }
    tokens = std::move(rebuilt);
}

}  // namespace datasystem
