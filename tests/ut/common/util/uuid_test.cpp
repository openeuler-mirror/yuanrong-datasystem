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
 * Description: test uuid functions
 */
#include <algorithm>
#include <chrono>
#include <cstring>
#include <iostream>
#include <limits>
#include <mutex>
#include <thread>

#include "ut/common.h"
#include "datasystem/common/util/uuid_generator.h"
#include "datasystem/common/util/wait_post.h"

namespace datasystem {
namespace ut {
class UuidTest : public CommonTest {};

TEST_F(UuidTest, TestStringUuidToBytes)
{
    std::string uuid = GetBytesUuid();
    std::string uuidString = BytesUuidToString(uuid);
    std::string result;
    DS_ASSERT_OK(StringUuidToBytes(uuidString, result));
    EXPECT_EQ(uuid, result);
}

TEST_F(UuidTest, TestUpperCaseStringUuidToBytes)
{
    std::string uuid = GetBytesUuid();
    std::string uuidString = BytesUuidToString(uuid);
    ;
    std::string upperCaseString;
    for (std::string::size_type inputOffset = 0; inputOffset < uuidString.length(); ++inputOffset) {
        char input = std::toupper(uuidString[inputOffset]);
        upperCaseString.append(1, input);
    }
    std::string result;
    DS_ASSERT_OK(StringUuidToBytes(upperCaseString, result));
    EXPECT_EQ(uuid, result);
}

TEST_F(UuidTest, TestGetStringUuidToBuffer)
{
    char uuid[UUID_STRING_BUFFER_SIZE] = {};
    DS_ASSERT_OK(GetStringUuid(uuid, sizeof(uuid)));
    EXPECT_EQ(strlen(uuid), UUID_STRING_SIZE);

    std::string byteUuid;
    DS_ASSERT_OK(StringUuidToBytes(std::string(uuid), byteUuid));
    EXPECT_EQ(BytesUuidToString(byteUuid), std::string(uuid));
}

TEST_F(UuidTest, TestBytesUuidToStringToBuffer)
{
    std::string byteUuid = GetBytesUuid();
    char uuid[UUID_STRING_BUFFER_SIZE] = {};
    DS_ASSERT_OK(BytesUuidToString(reinterpret_cast<const uint8_t *>(byteUuid.data()), byteUuid.size(), uuid,
                                   sizeof(uuid)));

    EXPECT_EQ(BytesUuidToString(byteUuid), std::string(uuid));
}

TEST_F(UuidTest, LEVEL1_GetStringUuidPerformance)
{
    constexpr size_t warmupIterations = 10000;
    constexpr size_t benchmarkIterations = 1000000;
    uint64_t checksum = 0;
    for (size_t i = 0; i < warmupIterations; ++i) {
        checksum += static_cast<uint8_t>(GetStringUuid()[0]);
    }

    const auto begin = std::chrono::steady_clock::now();
    bool valid = true;
    for (size_t i = 0; i < benchmarkIterations; ++i) {
        const std::string uuid = GetStringUuid();
        valid = valid && uuid.size() == 36 && uuid[8] == '-' && uuid[13] == '-' && uuid[18] == '-' && uuid[23] == '-';
        checksum += static_cast<uint8_t>(uuid[0]);
    }
    const auto elapsed = std::chrono::steady_clock::now() - begin;
    const auto elapsedNs = std::chrono::duration_cast<std::chrono::nanoseconds>(elapsed).count();
    std::cout << "UUID_PERF,GetStringUuid,iterations=" << benchmarkIterations << ",total_ns=" << elapsedNs
              << ",ns_per_op=" << elapsedNs / benchmarkIterations << ",checksum=" << checksum << std::endl;
    EXPECT_TRUE(valid);
}

TEST_F(UuidTest, TestDuplicate)
{
    const uint32_t threadNum = 100;
    const uint32_t times = 10000;

    std::vector<std::thread> threads(threadNum);
    Barrier barr(threadNum);
    std::vector<std::string> results;
    results.reserve(threadNum * times);
    std::mutex muxForReuslts;

    for (uint32_t i = 0; i < threadNum; i++) {
        threads[i] = std::thread([&] {
            barr.Wait();
            std::vector<std::string> thread_results;
            thread_results.reserve(times);
            for (uint32_t j = 0; j < times; j++) {
                thread_results.emplace_back(GetBytesUuid());  // put result into local
            }
            std::sort(thread_results.begin(), thread_results.end());
            {
                std::unique_lock<std::mutex> lock{ muxForReuslts };
                results.insert(results.end(), std::make_move_iterator(thread_results.begin()),
                               std::make_move_iterator(thread_results.end()));
                std::inplace_merge(results.begin(), results.begin() + times, results.end());
            }
        });
    }
    for (auto &t : threads) {
        t.join();
    }

    auto it = std::unique(results.begin(), results.end());
    ASSERT_EQ(results.end() - it, 0);
}

TEST_F(UuidTest, IndexUuidGenTest)
{
    uint64_t normalCheckMaxNum = 10000;
    // 4-4-(12) , 4-(4)-(12) , (4)-(4)-12
    // Calibration of data changes near thresholds
    std::vector<uint64_t> criticalValueCheckStat = {0, 999999999990, 9999999999999990};
    for (auto baseNum : criticalValueCheckStat) {
        for (uint64_t i = 0; i < normalCheckMaxNum; i++) {
            uint64_t index = i + baseNum;
            std::string indexUuid;
            DS_ASSERT_OK(IndexUuidGenerator(index, indexUuid));
            char indexUuidBuffer[UUID_STRING_BUFFER_SIZE] = {};
            DS_ASSERT_OK(IndexUuidGenerator(index, indexUuidBuffer, sizeof(indexUuidBuffer)));
            EXPECT_EQ(indexUuid, std::string(indexUuidBuffer));
            std::string byteUuid;
            DS_ASSERT_OK(StringUuidToBytes(indexUuid, byteUuid));
            std::string resultString = BytesUuidToString(byteUuid);
            resultString.erase(std::remove_if(resultString.begin(), resultString.end(),
                                              [](unsigned char c) { return !std::isdigit(c); }),
                               resultString.end());
            uint64_t dataResult = std::stoull(resultString);
            ASSERT_EQ(index, dataResult);
        }
    }

    const uint64_t maxIndex = std::numeric_limits<uint64_t>::max();
    std::string maxIndexUuid;
    DS_ASSERT_OK(IndexUuidGenerator(maxIndex, maxIndexUuid));
    char maxIndexUuidBuffer[UUID_STRING_BUFFER_SIZE] = {};
    DS_ASSERT_OK(IndexUuidGenerator(maxIndex, maxIndexUuidBuffer, sizeof(maxIndexUuidBuffer)));
    EXPECT_EQ(maxIndexUuid, std::string(maxIndexUuidBuffer));
    std::string byteUuid;
    DS_ASSERT_OK(StringUuidToBytes(maxIndexUuid, byteUuid));
    std::string resultString = BytesUuidToString(byteUuid);
    resultString.erase(std::remove_if(resultString.begin(), resultString.end(),
                                      [](unsigned char c) { return !std::isdigit(c); }),
                       resultString.end());
    ASSERT_EQ(maxIndex, std::stoull(resultString));
}
}  // namespace ut
}  // namespace datasystem
