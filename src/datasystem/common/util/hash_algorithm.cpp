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
 * Description: Hash algorithm
 */

#include "datasystem/common/util/hash_algorithm.h"

#include <limits>

#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/util/strings_util.h"

namespace {
static constexpr uint32_t C1 = 0xcc9e2d51;  // chi-squared expirival value from MurmurHash3
static constexpr uint32_t C2 = 0x1b873593;  // chi-squared expirival value from MurmurHash3

static constexpr uint8_t R1 = 15;  // expirival rotate bits from MurmurHash3
static constexpr uint8_t R2 = 13;  // expirival rotate bits from MurmurHash3

static constexpr uint32_t M = 5;           // expirival value from MurmurHash3
static constexpr uint32_t N = 0xe6546b64;  // expirival value from MurmurHash3

// left rotate val by r position
inline uint32_t Rotl(uint32_t val, uint8_t r)
{
    return (val == 0) ? val : ((val << r) | (val >> (32 - r)));  // 32: std::numeric_limits<decltype(val)>::digits
}

// avalanche
inline uint32_t Fmix(uint32_t val)
{
    val ^= val >> 16;    // avalanche expirival value from MurmurHash3
    val *= 0x85ebca6bU;  // avalanche expirival value from MurmurHash3
    val ^= val >> 13;    // avalanche expirival value from MurmurHash3
    val *= 0xc2b2ae35U;  // avalanche expirival value from MurmurHash3
    val ^= val >> 16;    // avalanche expirival value from MurmurHash3
    return val;
}

// treat remain <= 3 bytes as a uint32_t block by converting the byte order. for example: 0x123456 -> 0x00563412
inline uint32_t GetRemain(const uint8_t *remain, uint8_t mod)
{
    uint32_t k = 0;
    int index = static_cast<int>(mod) - 1;
    while (index >= 0) {
        k ^= static_cast<uint32_t>(remain[index]) << index * std::numeric_limits<uint8_t>::digits;
        index--;
    }
    return k;
}
}  // namespace

namespace datasystem {

uint32_t GetNodeRedirectForInject(const uint8_t *data, size_t len)
{
    const uint32_t node0_redirect = 116852666;
    const uint32_t node1_redirect = 457913941;
    const uint32_t node2_redirect = 715827882;
    std::string key(reinterpret_cast<const char *>(data), len);
    const char *prefix = "redirect_test_";
    const int workerNum = 3;
    key.erase(0, strlen(prefix));
    auto i = StrToUnsignedLong(key);
    if (i % workerNum == 0) {
        return node0_redirect;
    } else if (i % workerNum == 1) {
        return node1_redirect;
    } else {
        return node2_redirect;
    }
}

uint32_t MurmurHash3_32(const uint8_t *data, size_t len, uint32_t seed)
{
    if (data == nullptr) {
        return 0;
    }

    INJECT_POINT("add.node.redirect", [data, len]() { return GetNodeRedirectForInject(data, len); });
    uint32_t result = seed;
    uint32_t k;

    // process body: 4 bytes a block
    const uint32_t *block = reinterpret_cast<const uint32_t *>(data);
    const size_t blockNum = len / sizeof(uint32_t);
    for (size_t i = 0; i < blockNum; ++i) {
        k = *(block++);
        k *= C1;
        k = Rotl(k, R1);
        k *= C2;
        result ^= k;
        result = Rotl(result, R2);
        result = result * M + N;
    }

    // process remaining <=3 bytes
    const uint8_t mod = len & 3;
    if (mod != 0) {
        k = GetRemain(reinterpret_cast<const uint8_t *>(block), mod);
        k *= C1;
        k = Rotl(k, R1);
        k *= C2;
        result ^= k;
    }

    // avalanche
    result ^= len;
    INJECT_POINT("MurmurHash3", [data, len, result]() {
        std::string key(reinterpret_cast<const char *>(data), len);
        const char *prefix = "a_key_hash_to_";
        auto preSize = strlen(prefix);
        if (key.substr(0, preSize) == prefix) {
            key.erase(0, preSize);
            return static_cast<uint32_t>(StrToUnsignedLong(key));
        } else {
            return Fmix(result);
        }
    });

    INJECT_POINT("MurmurHash3SpecifyKey", [data, len, result](const std::string &specifyKey, uint32_t resultHash) {
        std::string key(reinterpret_cast<const char *>(data), len);
        if (specifyKey == key) {
            return resultHash;
        } else {
            return Fmix(result);
        }
    });
    return Fmix(result);
}
}  // namespace datasystem