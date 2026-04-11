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
 * Description: Stream buffer pool test
 */

#include <chrono>
#include <cstdint>
#include <functional>
#include <string>
#include <unordered_map>
#include <utility>

#include "datasystem/worker/stream_cache/buffer_pool.h"
#include "ut/common.h"

using namespace datasystem::worker::stream_cache;
using namespace std::placeholders;
namespace datasystem {
namespace ut {
constexpr static int numDirtyList = 8;
class StreamData : public BaseBufferData {
public:
    explicit StreamData(std::string streamName) : streamName_(std::move(streamName))
    {
    }
    ~StreamData() override = default;
    std::string StreamName() const override
    {
        return streamName_;
    }

    std::string ProducerName() const override
    {
        return streamName_;
    }

    std::string ProducerInstanceId() const override
    {
        return streamName_;
    }

    Status ReleasePage() override
    {
        return Status::OK();
    }

    uint64_t StreamHash() const override
    {
        return std::hash<std::string>{}(streamName_);
    }

    Status GetStreamStatus()
    {
        return Status::OK();
    }

    bool IfEOSReply()
    {
        return false;
    }

private:
    std::string streamName_;
};
class BufferPoolTest : public CommonTest {
public:
    BufferPoolTest() : flushCount_(numDirtyList)
    {
    }
    ~BufferPoolTest() override = default;

    void SetUp() override
    {
        bp_ =
            std::make_unique<BufferPool>(numDirtyList, "BufferPool", std::bind(&BufferPoolTest::FlushFn, this, _1, _2));
        for (auto i = 0; i < numDirtyList; ++i) {
            flushCount_[i] = 0;
        }
        CommonTest::SetUp();
    }
    void TearDown() override
    {
        if (bp_) {
            bp_->Stop();
            bp_.reset();
        }
        CommonTest::TearDown();
    }

    Status FlushFn(int id, PendingFlushList &flushList)
    {
        (void)id;
        for (auto &ele : flushList) {
            auto &datalist = ele.second;
            flushCount_[id] += datalist.size();
            datalist.clear();
        }
        return Status::OK();
    }

protected:
    std::unique_ptr<BufferPool> bp_;
    std::vector<int> flushCount_;

private:
};

TEST_F(BufferPoolTest, TestOneStream)
{
    DS_ASSERT_OK(bp_->Init());
    const std::string streamName(RandomData().GetRandomString(8));
    const int numElements = 1000;
    for (auto i = 0; i < numElements; ++i) {
        auto data = std::make_shared<StreamData>(streamName);
        bp_->Insert(data);
    }
    sleep(1);
    int total = 0;
    for (auto i = 0; i < numDirtyList; ++i) {
        total += flushCount_[i];
    }
    ASSERT_EQ(total, numElements);
}

TEST_F(BufferPoolTest, TestMultiStreams)
{
    DS_ASSERT_OK(bp_->Init());
    const int numStreams = 100;
    std::vector<std::string> streamNames;
    streamNames.reserve(numStreams);
    for (int i = 0; i < numStreams; ++i) {
        streamNames.emplace_back(RandomData().GetRandomString(8));
    }
    const int numElements = 1000;
    auto threadPool = std::make_unique<ThreadPool>(5);
    for (auto i = 0; i < numElements; ++i) {
        for (auto j = 0; j < numStreams; ++j) {
            threadPool->Execute([this, &streamNames, j]() {
                auto data = std::make_shared<StreamData>(streamNames[j]);
                bp_->Insert(data);
            });
        }
    }
    threadPool.reset();
    sleep(1);
    int total = 0;
    for (auto i = 0; i < numDirtyList; ++i) {
        total += flushCount_[i];
    }
    EXPECT_EQ(total, numElements * numStreams);
}
}  // namespace ut
}  // namespace datasystem
