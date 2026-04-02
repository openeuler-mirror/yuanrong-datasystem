/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2024. All rights reserved.
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
 * Description: Test of skip list
 */
#include <thread>
#include "ut/common.h"

#include "datasystem/common/util/skip_list.h"
#include "datasystem/common/util/thread_pool.h"

namespace datasystem {
namespace ut {
class SkipListTest : public CommonTest {
protected:
    SkipList<int, int> list;

    void SetUp() override
    {
        // Insert some elements into the list
        list.insert(1, 100);
        list.insert(2, 200);
        list.insert(3, 300);
        list.insert(4, 400);
        list.insert(6, 600);
        list.insert(7, 700);
        list.insert(8, 800);
        list.insert(10, 1000);
    }

    void BatchInsert(int numEle)
    {
        // Insert some elements into the list
        for (int i = 10; i < numEle+10; i++) {
            list.insert(i, i);
        }
    }
};

TEST_F(SkipListTest, TestBasic)
{
    SkipList<int,int> list1;
    ASSERT_TRUE(list1.insert(1,2).IsOk());
    auto itr = list1.find(1);
    ASSERT_TRUE(itr != list1.end());
    EXPECT_EQ(itr->value, 2);
    auto itr1 = list1.erase(1);
    // Last element
    ASSERT_TRUE(itr1 == list1.end());
}

TEST_F(SkipListTest, InsertTest)
{
    // Insert a new element
    ASSERT_TRUE(list.insert(11, 400).IsOk());

    // Check if the element is in the list
    auto itr = list.find(11);
    ASSERT_TRUE(itr != list.end());
    ASSERT_EQ(itr->value, 400);
}

TEST_F(SkipListTest, DuplicateInsertTest)
{
    Status status = list.insert(12, 100);
    ASSERT_TRUE(status.IsOk());
    status = list.insert(12, 100);
    EXPECT_EQ(status.GetCode(), StatusCode::K_DUPLICATED);
}

TEST_F(SkipListTest, FindTest)
{
    // Check if existing elements are in the list
    auto itr = list.find(1);
    ASSERT_TRUE(itr != list.end());
    ASSERT_EQ(itr->value, 100);

    auto itr2 = list.find(2);
    ASSERT_TRUE(itr2 != list.end());
    ASSERT_EQ(itr2->value, 200);

    auto itr3 = list.find(3);
    ASSERT_TRUE(itr3 != list.end());
    ASSERT_EQ(itr3->value, 300);
}

TEST_F(SkipListTest, DISABLED_ParallelUpperBoundAndEraseTest)
{
    // Do all the insertions
    int num_insertions = 600000;
    BatchInsert(num_insertions);
    ThreadPool pool(3);
    // Erase elements parallelly
    auto eraseFut1 = pool.Submit([this, num_insertions]() {
        for (int i = 0; i < num_insertions; i++) {
            if (!(i%2)) {
                LOG(INFO) << "eraseFut1 is erasing "<<i;
                list.erase(i);
            }
        }
        return Status::OK();
    });
    auto eraseFut2 = pool.Submit([this, num_insertions]() {
        for (int i = 0; i < num_insertions; i++) {
            if (i%2) {
                LOG(INFO) << "eraseFut2 is erasing "<<i;
                list.erase(i);
            }
        }
        return Status::OK();
    });
    // Try to get upper bound
    auto upperBoundFut = pool.Submit([this, num_insertions]() {
        for (int i = 0; i < num_insertions; i++) {
            auto itr = list.upper_bound(num_insertions);
            LOG(INFO) << itr->value;
        }
        return Status::OK();
    });

    ASSERT_EQ(eraseFut1.get(), Status::OK());
    ASSERT_EQ(eraseFut2.get(), Status::OK());
    ASSERT_EQ(upperBoundFut.get(), Status::OK());
}

TEST_F(SkipListTest, UpperBoundTest)
{
    // Check if existing elements are in the list
    int bound = 5;
    int exceptValue = 600;
    auto itr1 = list.upper_bound(bound);
    ASSERT_EQ(itr1->value, exceptValue);

    bound = 7;
    exceptValue = 700;
    auto itr2 = list.upper_bound(bound);
    ASSERT_EQ(itr2->value, exceptValue);

    bound = 9;
    exceptValue = 1000;
    auto itr3 = list.upper_bound(bound);
    ASSERT_EQ(itr3->value, exceptValue);
}

TEST_F(SkipListTest, EraseTest)
{
    // Erase an existing element
    auto itr = list.erase(1);
    ASSERT_TRUE(itr != list.end());

    // Check if the element is still in the list
    auto itr1 = list.find(1);
    ASSERT_TRUE(itr1 == list.end());
}

TEST_F(SkipListTest, EraseNonExistentTest)
{
    // Try to erase a non-existent element
    auto itr = list.erase(5);
    ASSERT_EQ(itr, list.end());
}

TEST_F(SkipListTest, IteratorGetTest)
{
    // Try to erase a non-existent element
    int sum = 0, count = 0;
    for (auto itr = list.begin(); itr != list.end(); ++itr) {
        count++;
        sum += itr->value;
    }
    int exceptSum = 4100;
    int exceptCount = 8;
    ASSERT_EQ(sum, exceptSum);
    ASSERT_EQ(count, exceptCount);
}

TEST_F(SkipListTest, IteratorDeleteTest)
{
    // Delete until key 6
    auto cursorEnd = 6 + 1;
    int count = 0, sum = 0;
    auto itEnd = list.upper_bound(cursorEnd);
    for (auto it = list.begin(); it != itEnd;) {
        auto key = it->key;
        count++;
        sum += it->value;
        ++it;
        LOG(INFO) << "Deleting "<<key;
        list.erase(key);
    }
    int exceptSum = 1600;
    int exceptCount = 5;
    ASSERT_EQ(sum, exceptSum);
    ASSERT_EQ(count, exceptCount);
}
} // namespace ut
} // namespace datasystem