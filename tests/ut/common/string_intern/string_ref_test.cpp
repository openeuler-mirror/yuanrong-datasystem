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
 * Description: Testing StringRef.
 */
#include <gtest/gtest.h>
#include <tbb/concurrent_unordered_set.h>
#include <numeric>
#include <thread>
#include <unordered_map>
#include <unordered_set>

#include "common.h"
#include "datasystem/common/string_intern/string_ref.h"
#include "datasystem/common/string_intern/string_pool.h"
#include "datasystem/common/util/thread_pool.h"
#include "datasystem/common/util/uuid_generator.h"

namespace datasystem {
namespace ut {
using ImmutableString = OtherKey;

namespace {
intern::StringPool &GetStringPool()
{
    return intern::StringPool::Instance(intern::KeyType::OTHER);
}
}  // namespace
class StringRefTest : public CommonTest {
public:
    static void CheckImmutableStringEqual(const ImmutableString &im1, const ImmutableString &im2)
    {
        ASSERT_EQ(im1, im2);
        ASSERT_EQ(im1.ToString(), im2.ToString());
        ASSERT_EQ(&im1.ToString(), &im2.ToString());
    }

    static void CheckSetErase(tbb::concurrent_unordered_set<ImmutableString, std::hash<ImmutableString>> &set1,
                              tbb::concurrent_unordered_set<ImmutableString, std::hash<ImmutableString>> &set2)
    {
        set1.unsafe_erase("123");
        set2.unsafe_erase(ImmutableString("123"));
        EXPECT_EQ(GetStringPool().Size(), 1UL);

        set1.unsafe_erase("456");
        set2.unsafe_erase(std::string("456"));
        EXPECT_EQ(GetStringPool().Size(), 0UL);
    }

    template <typename T>
    static void CheckSetErase(T &set1, T &set2)
    {
        set1.erase("123");
        set2.erase(ImmutableString("123"));
        EXPECT_EQ(GetStringPool().Size(), 1UL);

        set1.erase("456");
        set2.erase(std::string("456"));
        EXPECT_EQ(GetStringPool().Size(), 0UL);
    }

    template <typename T>
    static void ImMapCheckMemoryReduce()
    {
        auto key1 = GetStringUuid();

        auto value1 = RandomData().GetRandomUint32();
        auto value2 = RandomData().GetRandomUint32();
        T map1;
        T map2;
        {
            auto im1 = ImmutableString(key1);
            // insert by ImmutableString
            map1[im1] = value1;
            ASSERT_EQ(map1[key1], value1);

            // insert by std::string
            map2[key1] = value2;
            ASSERT_EQ(map2[im1], value2);

            EXPECT_EQ(GetStringPool().Size(), 1UL);
            map1.erase(key1);
            EXPECT_EQ(GetStringPool().Size(), 1UL);
            map2.erase(key1);
        }

        EXPECT_EQ(GetStringPool().Size(), 0UL);
    }

    template <typename T>
    static void ImSetCheckMemoryReduce()
    {
        T set1;
        T set2;
        std::string test1 = "123";
        std::string test2 = "456";

        auto pair = set1.insert(test1);
        ASSERT_TRUE(pair.second);
        pair = set1.insert(ImmutableString(test2));
        ASSERT_TRUE(pair.second);
        pair = set1.insert(ImmutableString(test2));
        ASSERT_FALSE(pair.second);
        // After insert, 2 RefCountString in pool.
        EXPECT_EQ(GetStringPool().Size(), 2UL);
        // find by ImmutableString
        auto iter = set1.find(ImmutableString(test1));
        ASSERT_TRUE(iter != set1.end());
        ASSERT_EQ(*iter, test1);
        // find by std::string
        iter = set1.find(test2);
        ASSERT_TRUE(iter != set1.end());
        ASSERT_EQ(*iter, test2);

        // find by const char*
        iter = set1.find("456");
        ASSERT_TRUE(iter != set1.end());
        ASSERT_EQ(*iter, test2);

        // find by not exist key
        iter = set1.find("789");
        ASSERT_TRUE(iter == set1.end());

        pair = set2.insert(ImmutableString(test1));
        ASSERT_TRUE(pair.second);
        pair = set2.insert(ImmutableString(test2));
        ASSERT_TRUE(pair.second);
        EXPECT_EQ(GetStringPool().Size(), 2UL);

        auto iterInSet1 = set1.find(test1);
        auto iterInSet2 = set2.find(test1);
        ASSERT_EQ(*iterInSet1, *iterInSet2);

        CheckSetErase(set1, set2);
    }
};

TEST_F(StringRefTest, TestConstructor)
{
    std::string test1 = "123";
    std::string test2 = "456";
    char test3[] = "123";
    {
        ImmutableString im1 = ImmutableString(test1);
        ImmutableString im2 = ImmutableString(test1);
        ImmutableString im3 = ImmutableString(test2);
        ImmutableString im4 = ImmutableString("123");
        ImmutableString im5 = ImmutableString("456");
        ImmutableString im6 = ImmutableString(test3);

        LOG(INFO) << "check im1, im2";
        CheckImmutableStringEqual(im1, im2);
        LOG(INFO) << "check im1, im4";
        CheckImmutableStringEqual(im1, im4);
        LOG(INFO) << "check im1, im6";
        CheckImmutableStringEqual(im1, im6);
        LOG(INFO) << "check im3, im5";
        CheckImmutableStringEqual(im3, im5);
        CHECK_NE(im1, im3);
        CHECK_NE(im1.ToString(), im3.ToString());
        EXPECT_EQ(GetStringPool().Size(), 2UL);
    }
    EXPECT_EQ(GetStringPool().Size(), 0UL);
}

TEST_F(StringRefTest, TestBigString)
{
    const size_t strSize = 1024UL * 1024 * 1024;
    const std::string str = RandomData().GetPartRandomString(strSize, 100);
    const size_t imSize = 2;
    std::vector<ImmutableString> imVec;
    imVec.reserve(imSize);
    for (size_t i = 0; i < imSize; i++) {
        LOG(INFO) << "loop: " << i;
        // Need copy once.
        imVec.emplace_back(str);
    }
    EXPECT_EQ(GetStringPool().Size(), 1UL);
    imVec.clear();
    EXPECT_EQ(GetStringPool().Size(), 0UL);
}

TEST_F(StringRefTest, TestDestructorInParallel)
{
    size_t strNum = 2;
    const size_t strLen = 100;
    std::vector<std::string> strVec;
    strVec.reserve(strNum);
    for (size_t i = 0; i < strNum; i++) {
        strVec.emplace_back(RandomData().GetRandomString(strLen));
    }

    const size_t threadNum = 32;
    const size_t loopCnt = 10000;
    auto pool = std::make_unique<ThreadPool>(threadNum);
    for (size_t i = 0; i < threadNum; i++) {
        pool->Execute([&strVec, i, strNum]() {
            for (size_t j = 0; j < loopCnt; j++) {
                ImmutableString im = ImmutableString(strVec[i % strNum]);
            }
        });
    }
    pool.reset();

    EXPECT_EQ(GetStringPool().Size(), 0UL);
}

TEST_F(StringRefTest, TestImInTbbUnorderedSet)
{
    ImSetCheckMemoryReduce<tbb::concurrent_unordered_set<ImmutableString, std::hash<ImmutableString>>>();
}

TEST_F(StringRefTest, TestImInSTLUnorderedSet)
{
    ImSetCheckMemoryReduce<std::unordered_set<ImmutableString>>();
}

TEST_F(StringRefTest, TestImInSTLSet)
{
    ImSetCheckMemoryReduce<std::set<ImmutableString>>();
}

TEST_F(StringRefTest, ImInTbbHashMap)
{
    auto key1 = GetStringUuid();

    auto value1 = RandomData().GetRandomUint32();
    auto value2 = RandomData().GetRandomUint32();

    using MapType = tbb::concurrent_hash_map<ImmutableString, uint32_t>;

    MapType map1;
    MapType map2;

    auto im1 = ImmutableString(key1);
    MapType::accessor ac;
    // insert by ImmutableString
    map1.insert(ac, im1);
    ac->second = value1;
    ac.release();
    map1.find(ac, im1);
    ASSERT_EQ(ac->second, value1);
    ac.release();

    // insert by std::string
    map2.insert(ac, key1);
    ac->second = value2;
    ac.release();
    map2.find(ac, key1);
    ASSERT_EQ(ac->second, value2);
    ac.release();

    EXPECT_EQ(GetStringPool().Size(), 1UL);
}

TEST_F(StringRefTest, ImInUnorderedMapInParrel)
{
    auto key1 = GetStringUuid();
    using MapType = std::unordered_map<ImmutableString, int>;
    MapType map1;
    const size_t minThreadCnt = 10;
    auto pool = std::make_unique<ThreadPool>(minThreadCnt);
    std::shared_timed_mutex mutex;

    for (size_t i = 0; i < minThreadCnt; i++) {
        pool->Execute([&key1, &map1, &mutex]() {
            std::lock_guard<std::shared_timed_mutex> lck(mutex);
            auto iter = map1.find(key1);
            if (iter == map1.end()) {
                map1.emplace(key1, 1);
            } else {
                iter->second = 2;  // change to 2
            }
        });
        pool->Execute([&key1, &map1, &mutex]() {
            std::shared_lock<std::shared_timed_mutex> lck(mutex);
            auto iter = map1.find(key1);
            if (iter != map1.end()) {
                LOG(INFO) << iter->first;
            }
        });
    }
    pool.reset();
}

TEST_F(StringRefTest, ImInSTLHashMap)
{
    using MapType = std::map<ImmutableString, uint32_t>;
    ImMapCheckMemoryReduce<MapType>();
}

TEST_F(StringRefTest, ImInSTLUnorderedMap)
{
    using MapType = std::unordered_map<ImmutableString, uint32_t>;
    ImMapCheckMemoryReduce<MapType>();
}

TEST_F(StringRefTest, BaseTest)
{
    intern::StringPool::InitAll();
    auto s1 = ObjectKey::Intern("abc");
    auto s2 = ObjectKey::Intern("abc");
    auto s3 = ObjectKey::Intern("abcd");
    ASSERT_EQ(s1, s2);
    ASSERT_NE(s1, s3);

    ASSERT_EQ(intern::StringPool::Instance(intern::KeyType::OBJECT_KEY).Size(), 2);  // OBJECT_KEY count 2

    std::unordered_map<ObjectKey, int> map;
    map.emplace(s1, 1);
    ASSERT_EQ(map[s2], 1);

    auto s4 = ObjectKey::Intern("abcde");
    s4 = s3;
    ASSERT_EQ(s3, s4);
    ASSERT_EQ(intern::StringPool::Instance(intern::KeyType::OBJECT_KEY).Size(), 3);  // OBJECT_KEY count 3

    auto c1 = ClientKey::Intern("abc");
    auto c2 = ClientKey::Intern("abcd");
    ASSERT_EQ(intern::StringPool::Instance(intern::KeyType::OBJECT_KEY).Size(), 3);  // OBJECT_KEY count still 3
    ASSERT_EQ(intern::StringPool::Instance(intern::KeyType::CLIENT_KEY).Size(), 2);  // CLIENT_KEY count still 2
}

TEST_F(StringRefTest, TestMove)
{
    auto s1 = ObjectKey::Intern("abc");
    auto s2 = std::move(s1);
    ASSERT_NE(s1, s2);
    ASSERT_TRUE(s1.Size() == 0);
    ASSERT_EQ(s1.ToString(), "");
    ASSERT_EQ(s2.ToString(), "abc");

    auto s3 = ObjectKey::Intern("abcd");
    s3 = std::move(s2);
    ASSERT_EQ(s2.ToString(), "");
    ASSERT_EQ(s3.ToString(), "abc");

    std::string ss1 = "abc";
    std::string ss2 = "abcd";

    ss2 = std::move(ss1);
    ASSERT_EQ(ss1, "");
    ASSERT_EQ(ss2, "abc");
}

TEST_F(StringRefTest, InternAndErase)
{
    std::string key = "hello";
    std::vector<std::thread> threads;
    const size_t threadCnt = 8;
    const size_t testCnt = 10000;

    for (size_t i = 0; i < threadCnt; i++) {
        threads.emplace_back([&key] {
            for (size_t n = 0; n < testCnt; n++) {
                auto k = ObjectKey::Intern(key);
                (void)k;
            }
        });
    }

    for (auto &t : threads) {
        t.join();
    }
}

TEST_F(StringRefTest, SupportAddString)
{
    ObjectKey key = ObjectKey::Intern("hello");
    auto v1 = "world";
    ASSERT_EQ(key + v1, "helloworld");
    ASSERT_EQ(v1 + key, "worldhello");
    ASSERT_EQ(key.ToString(), "hello");

    std::string v2("world");
    ASSERT_EQ(key + v2, "helloworld");
    ASSERT_EQ(v2 + key, "worldhello");
    ASSERT_EQ(key.ToString(), "hello");
}

TEST_F(StringRefTest, TestEmptyString)
{
    ObjectKey key1 = ObjectKey::Intern("");
    ObjectKey key2 = ObjectKey::Intern("");
    ObjectKey key3 = ObjectKey::Intern("123");

    ASSERT_EQ(key1, key2);
    ASSERT_NE(key1, key3);
    ObjectKey key4 = std::move(key3);
    ASSERT_EQ(key1, key3);
    ASSERT_NE(key3, key4);
    ASSERT_EQ(key4.ToString(), "123");
}
}  // namespace ut
}  // namespace datasystem
