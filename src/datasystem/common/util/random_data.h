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
 * Description: Declare random data generator.
 */

#ifndef DATASYSTEM_COMMON_UTIL_RANDOM_DATA_H
#define DATASYSTEM_COMMON_UTIL_RANDOM_DATA_H

#include <numeric>
#include <random>
#include <set>
#include <string>
#include <thread>

namespace datasystem {
class RandomData {
public:
    explicit RandomData(size_t i) : randomDevice_(i)
    {
    }

    RandomData() : randomDevice_(std::mt19937(std::chrono::system_clock::now().time_since_epoch().count()))
    {
    }

    ~RandomData() = default;

    /**
     * @brief Reset internal randomDevice_ with a seed.
     * @param[in] seed Random generator seed.
     */
    void SetSeed(const int32_t seed)
    {
        randomDevice_ = std::mt19937(seed);
    }

    /**
     * @brief Generate a random uint8.
     * @return Random uint8.
     */
    uint8_t GetRandomUint8();

    /**
     * @brief Generate a random uint32.
     * @return Random uint32.
     */
    uint32_t GetRandomUint32();

    /**
     * @brief Generate a random uint64 in a range [start, end].
     * @param[in] minValue Range start.
     * @param[in] maxValue Range end.
     * @return Random uint32.
     */
    uint32_t GetRandomUint32(uint32_t minValue, uint32_t maxValue);

    /**
     * @brief Generate a random uint64.
     * @return Random uint64.
     */
    uint64_t GetRandomUint64();

    /**
     * @brief Generate a random uint64 in a range [start, end).
     * @param[in] start Range start.
     * @param[in] end Range end.
     * @return Random uint64.
     */
    uint64_t GetRandomUint64(uint64_t start, uint64_t end);

    /**
     * @brief Generate a random int64 in a range [start, end).
     * @param[in] start Range start.
     * @param[in] end Range end.
     * @return Random int64.
     */
    int64_t GetRandomInt64(int64_t start, int64_t end);

    /**
     * @brief Randomly select an index from the container size.
     * @param[in] The size of the container.
     * @return Random size_t of valid index.
     */
    size_t GetRandomIndex(size_t containerSize);

    /**
     * @brief Generate a random string.
     * @param[in] length String length.
     * @return Random string.
     */
    std::string GetRandomString(std::string::size_type length);

    /**
     * @brief Generate a partially random string.
     * @param[in] length String length.
     * @param[in] randomNum Random char number.
     * @return Random string.
     */
    std::string GetPartRandomString(std::string::size_type length, size_t randomNum);
    /**
     * @brief Generate random bytes in a vector of uint8_t.
     * @param[in] length Number of bytes.
     * @return Random bytes.
     */
    std::vector<uint8_t> RandomBytes(size_t length);

    /**
     * @brief Generate a vector of lengths.
     * @param[in] minValue Min length value.
     * @param[in] maxValue Max length value.
     * @param[in] size Number of lengths.
     * @return Random bytes.
     */
    std::vector<size_t> RandomLens(size_t minValue, size_t maxValue, size_t size);

    /**
     * @brief Generate a random seed without using <random> file.
     * @return Random seed.
     */
    static uint64_t GetRandomSeed();

protected:
    std::mt19937 randomDevice_;
};

class HRandomData {
public:
    HRandomData() : randomDevice_(std::mt19937(std::random_device()()))
    {
    }

    /**
     * @brief Generate a random uint32.
     * @return Random uint32.
     */
    uint32_t GetRandomUint32();

    /**
     * @brief Generate a random uint64.
     * @return Random uint64.
     */
    uint64_t GetRandomUint64();

    /**
     * @brief Generate a random uint64 in a range [start, end).
     * @param[in] start Range start.
     * @param[in] end Range end.
     * @return Random uint64.
     */
    uint64_t GetRandomUint64(uint64_t start, uint64_t end);
private:
    std::mt19937 randomDevice_;
};
}  // namespace datasystem
#endif
