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
 * Description: Define random data generator.
 */
#include "datasystem/common/util/random_data.h"

#include <algorithm>
#include <unistd.h>

#include "datasystem/common/log/trace.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/thread_pool.h"
#include "datasystem/common/util/timer.h"

namespace datasystem {
uint8_t RandomData::GetRandomUint8()
{
    thread_local static std::uniform_int_distribution<uint8_t> distribution(0, std::numeric_limits<uint8_t>::max());
    thread_local static auto generator = randomDevice_;
    return distribution(generator);
}

uint32_t RandomData::GetRandomUint32()
{
    thread_local static std::uniform_int_distribution<uint32_t> distribution(0, std::numeric_limits<uint32_t>::max());
    thread_local static auto generator = randomDevice_;
    return distribution(generator);
}

uint64_t RandomData::GetRandomUint64()
{
    thread_local static std::uniform_int_distribution<uint64_t> distribution(0, std::numeric_limits<uint64_t>::max());
    thread_local static auto generator = randomDevice_;
    return distribution(generator);
}

uint64_t RandomData::GetRandomUint64(uint64_t start, uint64_t end)
{
    if (start > end) {
        std::swap(start, end);
    }
    std::uniform_int_distribution<uint64_t> distribution(start, end);
    thread_local static auto generator = randomDevice_;
    return distribution(generator);
}

size_t RandomData::GetRandomIndex(size_t containerSize)
{
    if (containerSize == 0) {
        return 0;
    }
    thread_local static auto generator = randomDevice_;
    std::uniform_int_distribution<size_t> indexes(0, containerSize - 1);
    return indexes(generator);
}

uint64_t RandomData::GetRandomSeed()
{
    static auto hostHash = std::hash<const char *>()(std::getenv("HOSTNAME"));
    static auto pidHash = std::hash<std::size_t>()(getpid());
    static thread_local auto tidHash = std::hash<std::thread::id>()(std::this_thread::get_id());
    uint64_t systemClock = static_cast<uint64_t>(std::chrono::system_clock::now().time_since_epoch().count());
    uint64_t seed = static_cast<uint64_t>(rand()) ^ systemClock ^ hostHash;
    uint64_t highResClock = static_cast<uint64_t>(std::chrono::high_resolution_clock::now().time_since_epoch().count());
    seed += highResClock ^ pidHash;
    uint64_t steadyClock = static_cast<uint64_t>(std::chrono::steady_clock::now().time_since_epoch().count());
    seed += steadyClock ^ tidHash;
    return seed;
}

int32_t FastRand()
{
    static thread_local bool isFirst = true;
    static thread_local uint32_t seed = 123456789;
    static int32_t magicNum1 = 214013;
    static int32_t magicNum2 = 2531011;
    if (isFirst) {
        isFirst = false;
        seed = std::random_device()() % INT32_MAX;
    }
    seed = (static_cast<int64_t>(magicNum1) * seed + magicNum2) % INT32_MAX;

    static uint32_t magicShift = 16;
    static uint32_t magicAnd = 0x7FFF;
    return (seed >> magicShift) & magicAnd;
}

template <typename T>
void GenerateInParallel(const std::string &chars, std::string::size_type length, T &s)
{
    Timer timer;
    const uint64_t maxConcurrency = 8;
    const uint64_t minConcurrency = 1;
    auto numThreads =
        std::min<uint64_t>(maxConcurrency, std::max<uint64_t>(minConcurrency, std::thread::hardware_concurrency()));
    {
        const int addressOffset = 2;
        ThreadPool pool(numThreads);
        for (uint64_t i = 0; i < numThreads; i++) {
            auto avg = length / numThreads + 1;
            auto beg = i * avg;
            auto end = std::min((i + 1) * avg, length);
            auto traceID = Trace::Instance().GetTraceID();
            pool.Execute([beg, end, &s, chars, addressOffset, traceID]() {
                TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceID);
                Timer timer;
                for (auto i = beg; i < end; i++) {
                    if (chars.size() - addressOffset != 0) {
                        s[i] = chars[FastRand() % (chars.size() - addressOffset)];
                    }
                }
                VLOG(3) << FormatString("Process [[%zu], [%zu]) Time: [%.6lf]s", beg, end, timer.ElapsedSecond());
            });
        }
    }
    VLOG(3) << FormatString(" Time: [%.6lf]s", timer.ElapsedSecond());
}

template <typename T>
void GenerateBytes(std::mt19937 &randomDevice, std::string::size_type length, T &s)
{
    static std::string chars =
        "0123456789"
        "abcdefghijklmnopqrstuvwxyz"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ";

    const int addressOffset = 2;
    const int bigLength = 1024u * 1024u;
    static thread_local std::uniform_int_distribution<unsigned short> genInt(0, chars.size() - addressOffset);
    s.resize(length);
    if (length > bigLength) {
        GenerateInParallel(chars, length, s);
    } else {
        for (size_t i = 0; i < length; i++) {
            s[i] = chars[genInt(randomDevice)];
        }
    }
}

template <typename T>
void RandomReplaceBytes(std::mt19937 &randomDevice, std::string::size_type length, T &s, size_t randomNum)
{
    static std::string chars =
        "0123456789"
        "abcdefghijklmnopqrstuvwxyz"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    const int addressOffset = 1;
    size_t count = 0;
    std::uniform_int_distribution<unsigned short> genCharsIndex(0, chars.size() - addressOffset);
    std::uniform_int_distribution<unsigned long> genStrIndex(0, length - addressOffset);
    while (count < randomNum) {
        s[genStrIndex(randomDevice)] = chars[genCharsIndex(randomDevice)];
        count++;
    }
}

std::string RandomData::GetRandomString(std::string::size_type length)
{
    std::string s;
    thread_local static auto generator = randomDevice_;
    GenerateBytes(generator, length, s);
    return s;
}

std::string RandomData::GetPartRandomString(std::string::size_type length, std::string::size_type randomNum)
{
    std::string s(length, 'A');
    thread_local static auto generator = randomDevice_;
    RandomReplaceBytes(generator, length, s, randomNum);
    return s;
}

std::vector<uint8_t> RandomData::RandomBytes(size_t length)
{
    std::vector<uint8_t> s;
    thread_local static auto generator = randomDevice_;
    GenerateBytes(generator, length, s);
    return s;
}

std::vector<size_t> RandomData::RandomLens(size_t minValue, size_t maxValue, size_t size)
{
    if (maxValue < minValue) {
        maxValue = minValue;
    }
    thread_local static auto generator = randomDevice_;
    std::uniform_int_distribution<size_t> pick(minValue, maxValue);
    std::vector<size_t> lens;
    lens.reserve(size);
    for (size_t i = 0; i < size; i++) {
        lens.emplace_back(pick(generator));
    }
    return lens;
}

uint32_t RandomData::GetRandomUint32(uint32_t minValue, uint32_t maxValue)
{
    if (maxValue < minValue) {
        maxValue = minValue;
    }
    thread_local static auto generator = randomDevice_;
    std::uniform_int_distribution<uint32_t> pick(minValue, maxValue);
    return pick(generator);
}

uint32_t HRandomData::GetRandomUint32()
{
    thread_local static std::uniform_int_distribution<uint32_t> distribution(0, std::numeric_limits<uint32_t>::max());
    thread_local static auto generator = randomDevice_;
    return distribution(generator);
}

uint64_t HRandomData::GetRandomUint64()
{
    thread_local static std::uniform_int_distribution<uint64_t> distribution(0, std::numeric_limits<uint64_t>::max());
    thread_local static auto generator = randomDevice_;
    return distribution(generator);
}

uint64_t HRandomData::GetRandomUint64(uint64_t start, uint64_t end)
{
    std::uniform_int_distribution<uint64_t> distribution(start, end);
    thread_local static auto generator = randomDevice_;
    return distribution(generator);
}
}  // namespace datasystem
