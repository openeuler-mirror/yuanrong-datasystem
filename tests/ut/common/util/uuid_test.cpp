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
}
}  // namespace ut
}  // namespace datasystem
