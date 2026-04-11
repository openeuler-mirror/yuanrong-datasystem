/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
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
 * Description: Test meta async queue test.
 */

#include "datasystem/master/object_cache/store/meta_async_queue.h"

#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

#include <gtest/gtest.h>

#include "ut/common.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/common/log/log.h"

using namespace datasystem::master;

namespace datasystem {
namespace ut {

class MetaAsyncQueueTest : public CommonTest {
public:
    std::vector<std::shared_ptr<AsyncElement>> CreateAsyncElements(AsyncElement::ReqType type, size_t count,
                                                                   const std::string &objectPrefix = "objectKey",
                                                                   const std::string &table = "table");
};

std::vector<std::shared_ptr<AsyncElement>> MetaAsyncQueueTest::CreateAsyncElements(AsyncElement::ReqType type,
                                                                                   size_t count,
                                                                                   const std::string &objectPrefix,
                                                                                   const std::string &table)
{
    std::vector<std::shared_ptr<AsyncElement>> ret;
    for (size_t i = 0; i < count; ++i) {
        std::string objectKey = objectPrefix + std::to_string(i);
        ret.emplace_back(std::make_shared<AsyncElement>(objectKey, table, "key", "", type));
    }
    return ret;
}

TEST_F(MetaAsyncQueueTest, TestAsyncElementBasicFunction)
{
    LOG(INFO) << "Test async element basic function";
    // The element compares object key, table and key to determine whether they are the same.
    auto e1 = std::make_shared<AsyncElement>("xxx", "table1", "key1", "", AsyncElement::ReqType::ADD);
    auto e2 = std::make_shared<AsyncElement>("xxx", "table1", "key1", "value", AsyncElement::ReqType::DEL);
    auto e3 = std::make_shared<AsyncElement>("xxx", "table", "key1", "value", AsyncElement::ReqType::DEL);

    std::unordered_set<std::shared_ptr<AsyncElement>> set{ e1, e2, e3 };
    ASSERT_EQ(set.size(), 2);
    ASSERT_EQ(std::hash<std::shared_ptr<AsyncElement>>()(e1), std::hash<std::shared_ptr<AsyncElement>>()(e2));
    ASSERT_NE(std::hash<std::shared_ptr<AsyncElement>>()(e1), std::hash<std::shared_ptr<AsyncElement>>()(e3));
    ASSERT_NE(std::hash<std::shared_ptr<AsyncElement>>()(e2), std::hash<std::shared_ptr<AsyncElement>>()(e3));

    ASSERT_EQ(set.count(e1), 1);
    ASSERT_EQ(set.count(e2), 1);
    ASSERT_EQ(set.count(e3), 1);
}

TEST_F(MetaAsyncQueueTest, TestMetaAsyncQueueAddAndDelete)
{
    LOG(INFO) << "Test meta async queue add and then delete";
    constexpr int size = 100;
    MetaAsyncQueue queue(size);

    // 1. Add op and delete op counteracting each other
    auto addVec = CreateAsyncElements(AsyncElement::ReqType::ADD, size / 2);
    auto delVec = CreateAsyncElements(AsyncElement::ReqType::DEL, size / 2);
    for (size_t i = 0; i < size / 2; ++i) {
        std::shared_ptr<AsyncElement> eliminatedEle;
        int incrCnt = 0;
        queue.AppendAsyncTask(addVec[i], eliminatedEle, incrCnt);
        ASSERT_TRUE(eliminatedEle == nullptr);
        ASSERT_EQ(incrCnt, 1);
    }
    for (size_t i = 0; i < size / 2; ++i) {
        std::shared_ptr<AsyncElement> eliminatedEle;
        int incrCnt = 0;
        queue.AppendAsyncTask(delVec[i], eliminatedEle, incrCnt);
        ASSERT_TRUE(eliminatedEle == nullptr);
        ASSERT_EQ(incrCnt, -1);
    }
    ASSERT_EQ(queue.Size(), 0);
    std::shared_ptr<AsyncElement> ele;
    constexpr int timeoutMs = 100;
    Timer timer;
    ASSERT_FALSE(queue.Poll(ele, timeoutMs));
    auto elapsedMillseconds = timer.ElapsedMilliSecond();
    ASSERT_GT(elapsedMillseconds, 90);

    // 2. Add to various table and test again.
    auto addVec1 = CreateAsyncElements(AsyncElement::ReqType::ADD, size / 2, "object", "table1");
    auto delVec1 = CreateAsyncElements(AsyncElement::ReqType::DEL, size / 2, "object", "table1");
    auto addVec2 = CreateAsyncElements(AsyncElement::ReqType::ADD, size / 2, "object", "table2");
    auto delVec2 = CreateAsyncElements(AsyncElement::ReqType::DEL, size / 2, "object", "table2");
    auto delVec3 = CreateAsyncElements(AsyncElement::ReqType::DEL, size / 2, "object", "table3");

    for (size_t i = 0; i < size / 2; ++i) {
        std::shared_ptr<AsyncElement> eliminatedEle;
        int incrCnt = 0;
        queue.AppendAsyncTask(addVec1[i], eliminatedEle, incrCnt);
        ASSERT_TRUE(eliminatedEle == nullptr);
        ASSERT_EQ(incrCnt, 1);
        queue.AppendAsyncTask(addVec2[i], eliminatedEle, incrCnt);
        ASSERT_TRUE(eliminatedEle == nullptr);
        ASSERT_EQ(incrCnt, 1);
    }

    for (size_t i = 0; i < size / 2; ++i) {
        std::shared_ptr<AsyncElement> eliminatedEle;
        int incrCnt = 0;
        queue.AppendAsyncTask(delVec1[i], eliminatedEle, incrCnt);
        ASSERT_TRUE(eliminatedEle == nullptr);
        ASSERT_EQ(incrCnt, -1);
        queue.AppendAsyncTask(delVec2[i], eliminatedEle, incrCnt);
        ASSERT_TRUE(eliminatedEle == nullptr);
        ASSERT_EQ(incrCnt, -1);
        queue.AppendAsyncTask(delVec3[i], eliminatedEle, incrCnt);
        ASSERT_TRUE(eliminatedEle == nullptr);
        ASSERT_EQ(incrCnt, 1);
    }
    ASSERT_EQ(queue.Size(), size / 2);
    for (size_t i = 0; i < size / 2; ++i) {
        std::shared_ptr<AsyncElement> ele;
        ASSERT_TRUE(queue.Poll(ele, timeoutMs));
        ASSERT_TRUE(ele != nullptr);
        ASSERT_EQ(*ele, *delVec3[i]);
    }
    ASSERT_EQ(queue.Size(), 0);
}

TEST_F(MetaAsyncQueueTest, TestMetaAsyncQueueAppendDuplicateKey)
{
    LOG(INFO) << "Test meta async queue append duplicate keys";
    constexpr int size = 100;
    MetaAsyncQueue queue(size);

    auto verifyFunc = [this, &queue](size_t count, AsyncElement::ReqType op1, AsyncElement::ReqType op2,
                                     bool interleave) {
        auto vec1 = CreateAsyncElements(op1, count);
        auto vec2 = CreateAsyncElements(op2, count);
        if (interleave) {
            for (size_t i = 0; i < count; ++i) {
                std::shared_ptr<AsyncElement> eliminatedEle;
                int incrCnt = 0;
                queue.AppendAsyncTask(vec1[i], eliminatedEle, incrCnt);
                ASSERT_TRUE(eliminatedEle == nullptr);
                ASSERT_EQ(incrCnt, 1);
                queue.AppendAsyncTask(vec2[i], eliminatedEle, incrCnt);
                ASSERT_TRUE(eliminatedEle == nullptr);
                ASSERT_EQ(incrCnt, 0);
            }
        } else {
            for (size_t i = 0; i < count; ++i) {
                std::shared_ptr<AsyncElement> eliminatedEle;
                int incrCnt = 0;
                queue.AppendAsyncTask(vec1[i], eliminatedEle, incrCnt);
                ASSERT_TRUE(eliminatedEle == nullptr);
                ASSERT_EQ(incrCnt, 1);
            }
            for (size_t i = 0; i < count; ++i) {
                std::shared_ptr<AsyncElement> eliminatedEle;
                int incrCnt = 0;
                queue.AppendAsyncTask(vec2[i], eliminatedEle, incrCnt);
                ASSERT_TRUE(eliminatedEle == nullptr);
                ASSERT_EQ(incrCnt, 0);
            }
        }

        ASSERT_EQ(queue.Size(), count);
        for (size_t i = 0; i < count; ++i) {
            std::shared_ptr<AsyncElement> ele;
            ASSERT_TRUE(queue.Poll(ele, 0));
            ASSERT_TRUE(ele != nullptr);
            ASSERT_EQ(*ele, *vec2[i]);
        }
        ASSERT_EQ(queue.Size(), 0);
    };

    // 1. append add op duplicate keys.
    verifyFunc(size / 2, AsyncElement::ReqType::ADD, AsyncElement::ReqType::ADD, false);
    verifyFunc(size / 2, AsyncElement::ReqType::ADD, AsyncElement::ReqType::ADD, true);

    // 2. append del op duplicate keys.
    verifyFunc(size / 2, AsyncElement::ReqType::DEL, AsyncElement::ReqType::DEL, false);
    verifyFunc(size / 2, AsyncElement::ReqType::DEL, AsyncElement::ReqType::DEL, true);

    // 3. append del first and then add.
    verifyFunc(size / 2, AsyncElement::ReqType::DEL, AsyncElement::ReqType::ADD, false);
    verifyFunc(size / 2, AsyncElement::ReqType::DEL, AsyncElement::ReqType::ADD, true);
}

TEST_F(MetaAsyncQueueTest, TestMetaAsyncQueueAppendOrder)
{
    LOG(INFO) << "Test meta async queue append order";
    constexpr int size = 100;
    constexpr int count = size / 5;
    MetaAsyncQueue queue(size);

    constexpr size_t vecCount = 7;
    std::vector<std::vector<std::shared_ptr<AsyncElement>>> vecs(vecCount);

    vecs[0] = CreateAsyncElements(AsyncElement::ReqType::ADD, count, "object_a", "table1");
    vecs[1] = CreateAsyncElements(AsyncElement::ReqType::ADD, count, "object_a", "table2");
    vecs[2] = CreateAsyncElements(AsyncElement::ReqType::DEL, count, "object_b", "table2");
    vecs[3] = CreateAsyncElements(AsyncElement::ReqType::DEL, count, "object_c", "table2");
    // 1. override vecs[1]
    vecs[4] = CreateAsyncElements(AsyncElement::ReqType::ADD, count, "object_a", "table2");
    // 2. counteracting vecs[0]
    vecs[5] = CreateAsyncElements(AsyncElement::ReqType::DEL, count, "object_a", "table1");
    vecs[6] = CreateAsyncElements(AsyncElement::ReqType::ADD, count, "object_c", "table1");

    for (size_t i = 0; i < vecCount; ++i) {
        const auto &vec = vecs[i];
        for (size_t j = 0; j < vec.size(); ++j) {
            std::shared_ptr<AsyncElement> eliminatedEle;
            int incrCnt = 0;
            queue.AppendAsyncTask(vec[j], eliminatedEle, incrCnt);
        }
    }

    constexpr size_t expectSize = 80;
    ASSERT_EQ(queue.Size(), expectSize);

    for (size_t i = 0; i < expectSize; ++i) {
        std::shared_ptr<AsyncElement> ele;
        ASSERT_TRUE(queue.Poll(ele, 0));
        ASSERT_TRUE(ele != nullptr);
        if (i < count) {
            ASSERT_EQ(*ele, *vecs[2][i]);
        } else if (i < count * 2) {
            ASSERT_EQ(*ele, *vecs[3][i - count]);
        } else if (i < count * 3) {
            ASSERT_EQ(*ele, *vecs[4][i - count * 2]);
        } else {
            ASSERT_EQ(*ele, *vecs[6][i - count * 3]);
        }
    }

    ASSERT_EQ(queue.Size(), 0);
}

TEST_F(MetaAsyncQueueTest, TestMetaAsyncQueueEliminate)
{
    LOG(INFO) << "Test meta async queue eliminate";
    constexpr int size = 100;
    constexpr int count = size / 2;
    MetaAsyncQueue queue(size);

    auto vec = CreateAsyncElements(AsyncElement::ReqType::ADD, count, "object_a", "table1");
    auto vec1 = CreateAsyncElements(AsyncElement::ReqType::ADD, count, "object_a", "table2");
    for (size_t i = 0; i < count; ++i) {
        std::shared_ptr<AsyncElement> eliminatedEle;
        int incrCnt = 0;
        queue.AppendAsyncTask(vec[i], eliminatedEle, incrCnt);
        ASSERT_TRUE(eliminatedEle == nullptr);
        ASSERT_EQ(incrCnt, 1);
    }
    for (size_t i = 0; i < count; ++i) {
        std::shared_ptr<AsyncElement> eliminatedEle;
        int incrCnt = 0;
        queue.AppendAsyncTask(vec1[i], eliminatedEle, incrCnt);
        ASSERT_TRUE(eliminatedEle == nullptr);
        ASSERT_EQ(incrCnt, 1);
    }
    // override vec, it would not overflow, so its return value is still null.
    for (size_t i = 0; i < count; ++i) {
        std::shared_ptr<AsyncElement> eliminatedEle;
        int incrCnt = 0;
        queue.AppendAsyncTask(vec[i], eliminatedEle, incrCnt);
        ASSERT_TRUE(eliminatedEle == nullptr);
        ASSERT_EQ(incrCnt, 0);
    }
    ASSERT_EQ(queue.Size(), size);

    // overflow and vec1's element would be eliminated.
    auto vec2 = CreateAsyncElements(AsyncElement::ReqType::DEL, count, "object_b", "table1");
    for (size_t i = 0; i < count; ++i) {
        std::shared_ptr<AsyncElement> eliminatedEle;
        int incrCnt = 0;
        queue.AppendAsyncTask(vec2[i], eliminatedEle, incrCnt);
        ASSERT_TRUE(eliminatedEle != nullptr);
        ASSERT_EQ(incrCnt, 1);
        ASSERT_EQ(*eliminatedEle, *vec1[i]);
    }
    ASSERT_EQ(queue.Size(), size);

    // override half of vec
    auto vec3 = CreateAsyncElements(AsyncElement::ReqType::DEL, count / 2, "object_a", "table1");
    for (size_t i = 0; i < count / 2; ++i) {
        std::shared_ptr<AsyncElement> eliminatedEle;
        int incrCnt = 0;
        queue.AppendAsyncTask(vec3[i], eliminatedEle, incrCnt);
        ASSERT_EQ(incrCnt, -1);
        ASSERT_TRUE(eliminatedEle == nullptr);
    }
    size_t expectSize = count + count / 2;
    ASSERT_EQ(queue.Size(), expectSize);

    for (size_t i = 0; i < expectSize; ++i) {
        std::shared_ptr<AsyncElement> ele;
        ASSERT_TRUE(queue.Poll(ele, 0));
        if (i < count / 2) {
            ASSERT_EQ(*ele, *vec[i + count / 2]);
        } else {
            ASSERT_EQ(*ele, *vec2[i - count / 2]);
        }
    }
    ASSERT_EQ(queue.Size(), 0);
}

TEST_F(MetaAsyncQueueTest, TestMetaAsyncQueuePoll)
{
    LOG(INFO) << "Test meta async queue poll";
    constexpr int size = 100;
    auto queue = std::make_unique<MetaAsyncQueue>(size);
    constexpr int timeoutMs = 10;
    constexpr size_t count = 10;
    Timer timer;
    for (size_t i = 0; i < count; ++i) {
        std::shared_ptr<AsyncElement> ele;
        ASSERT_FALSE(queue->Poll(ele, timeoutMs));
        auto millseconds = timer.ElapsedMilliSecondAndReset();
        ASSERT_GT(millseconds, timeoutMs - 1);
    }

    auto vec = CreateAsyncElements(AsyncElement::ReqType::ADD, count, "object_a", "table1");
    for (size_t i = 0; i < count; ++i) {
        std::shared_ptr<AsyncElement> eliminatedEle;
        int incrCnt = 0;
        queue->AppendAsyncTask(vec[i], eliminatedEle, incrCnt);
        ASSERT_TRUE(eliminatedEle == nullptr);
        ASSERT_EQ(incrCnt, 1);
    }

    double expectMillseconds = 1;
    timer.Reset();
    for (size_t i = 0; i < count; ++i) {
        std::shared_ptr<AsyncElement> ele;
        ASSERT_TRUE(queue->Poll(ele, timeoutMs));
        ASSERT_TRUE(ele != nullptr);
    }
    auto millseconds = timer.ElapsedMilliSecondAndReset();
    ASSERT_LE(millseconds, expectMillseconds);
}

}  // namespace ut
}  // namespace datasystem