#include "datasystem/common/immutable_string/immutable_string.h"

#include <functional>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <gtest/gtest.h>
#include <sys/resource.h>
#include <tbb/concurrent_hash_map.h>
#include <tbb/concurrent_unordered_set.h>

#include "common.h"
#include "datasystem/common/immutable_string/immutable_string_pool.h"
#include "datasystem/common/util/random_data.h"
#include "datasystem/common/util/thread_pool.h"
#include "datasystem/common/util/uuid_generator.h"
#include "datasystem/utils/connection.h"

namespace datasystem {
namespace ut {
class ImmutableStringTest : public CommonTest {
public:
    static void CheckImmutableStringEqual(const ImmutableStringImpl &im1, const ImmutableStringImpl &im2)
    {
        ASSERT_EQ(im1, im2);
        ASSERT_EQ(im1.ToString(), im2.ToString());
        ASSERT_EQ(&im1.ToString(), &im2.ToString());
    }

    static void CheckSetErase(tbb::concurrent_unordered_set<ImmutableStringImpl, std::hash<ImmutableStringImpl>> &set1,
                              tbb::concurrent_unordered_set<ImmutableStringImpl, std::hash<ImmutableStringImpl>> &set2)
    {
        set1.unsafe_erase("123");
        set2.unsafe_erase(ImmutableStringImpl("123"));
        EXPECT_EQ(ImmutableStringPool::Instance().Size(), 1ul);

        set1.unsafe_erase("456");
        set2.unsafe_erase(std::string("456"));
        EXPECT_EQ(ImmutableStringPool::Instance().Size(), 0ul);
    }

    template <typename T>
    static void CheckSetErase(T &set1, T &set2)
    {
        set1.erase("123");
        set2.erase(ImmutableStringImpl("123"));
        EXPECT_EQ(ImmutableStringPool::Instance().Size(), 1ul);

        set1.erase("456");
        set2.erase(std::string("456"));
        EXPECT_EQ(ImmutableStringPool::Instance().Size(), 0ul);
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
            auto im1 = ImmutableStringImpl(key1);
            // insert by ImmutableStringImpl
            map1[im1] = value1;
            ASSERT_EQ(map1[key1], value1);

            // insert by std::string
            map2[key1] = value2;
            ASSERT_EQ(map2[im1], value2);

            EXPECT_EQ(ImmutableStringPool::Instance().Size(), 1ul);
            map1.erase(key1);
            EXPECT_EQ(ImmutableStringPool::Instance().Size(), 1ul);
            map2.erase(key1);
        }

        EXPECT_EQ(ImmutableStringPool::Instance().Size(), 0ul);
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
        pair = set1.insert(ImmutableStringImpl(test2));
        ASSERT_TRUE(pair.second);
        pair = set1.insert(ImmutableStringImpl(test2));
        ASSERT_FALSE(pair.second);
        // After insert, 2 RefCountString in pool.
        EXPECT_EQ(ImmutableStringPool::Instance().Size(), 2ul);
        // find by ImmutableStringImpl
        auto iter = set1.find(ImmutableStringImpl(test1));
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

        pair = set2.insert(ImmutableStringImpl(test1));
        ASSERT_TRUE(pair.second);
        pair = set2.insert(ImmutableStringImpl(test2));
        ASSERT_TRUE(pair.second);
        EXPECT_EQ(ImmutableStringPool::Instance().Size(), 2ul);

        auto iterInSet1 = set1.find(test1);
        auto iterInSet2 = set2.find(test1);
        ASSERT_EQ(*iterInSet1, *iterInSet2);

        CheckSetErase(set1, set2);
    }
};

TEST_F(ImmutableStringTest, TestConstructor)
{
    std::string test1 = "123";
    std::string test2 = "456";
    char test3[] = "123";
    {
        ImmutableStringImpl im1 = ImmutableStringImpl(test1);
        ImmutableStringImpl im2 = ImmutableStringImpl(test1);
        ImmutableStringImpl im3 = ImmutableStringImpl(test2);
        ImmutableStringImpl im4 = ImmutableStringImpl("123");
        ImmutableStringImpl im5 = ImmutableStringImpl("456");
        ImmutableStringImpl im6 = ImmutableStringImpl(test3);

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
        EXPECT_EQ(ImmutableStringPool::Instance().Size(), 2ul);
    }
    EXPECT_EQ(ImmutableStringPool::Instance().Size(), 0ul);
}

TEST_F(ImmutableStringTest, TestBigString)
{
    size_t strSize = 1024ul * 1024 * 1024;
    std::string str = RandomData().GetPartRandomString(strSize, 100);
    size_t imSize = 2;
    std::vector<ImmutableStringImpl> imVec;
    imVec.reserve(imSize);
    for (size_t i = 0; i < imSize; i++) {
        LOG(INFO) << "loop: " << i;
        // Need copy once.
        imVec.emplace_back(str);
    }
    EXPECT_EQ(ImmutableStringPool::Instance().Size(), 1ul);
    imVec.clear();
    EXPECT_EQ(ImmutableStringPool::Instance().Size(), 0ul);
}

TEST_F(ImmutableStringTest, TestDestructorInParallel)
{
    size_t strNum = 2;
    std::vector<std::string> strVec;
    strVec.reserve(strNum);
    for (size_t i = 0; i < strNum; i++) {
        strVec.emplace_back(RandomData().GetRandomString(100));
    }

    size_t threadNum = 32;
    auto pool = std::make_unique<ThreadPool>(threadNum);
    for (size_t i = 0; i < threadNum; i++) {
        pool->Execute([&strVec, i, strNum]() {
            for (int j = 0; j < 10000; j++) {
                ImmutableStringImpl im = ImmutableStringImpl(strVec[i % strNum]);
            }
        });
    }
    pool.reset();

    EXPECT_EQ(ImmutableStringPool::Instance().Size(), 0ul);
}

/**
1. ImmutableStringImpl\const char*\std::string 都能insert、find
2. 重复insert，内存不增加
3. 都erase后，内存能释放
4. 并发场景下，表不加外部锁的情况下能安全的进行(insert\find\erase)，ImmutableStringImpl不被破坏
5. 支持 tbb 和 stl 的所有map/set 类型。
*/
TEST_F(ImmutableStringTest, TestImInTbbUnorderedSet)
{
    ImSetCheckMemoryReduce<tbb::concurrent_unordered_set<ImmutableStringImpl, std::hash<ImmutableStringImpl>>>();
}

TEST_F(ImmutableStringTest, TestImInSTLUnorderedSet)
{
    ImSetCheckMemoryReduce<std::unordered_set<ImmutableStringImpl>>();
}

TEST_F(ImmutableStringTest, TestImInSTLSet)
{
    ImSetCheckMemoryReduce<std::set<ImmutableStringImpl>>();
}

TEST_F(ImmutableStringTest, ImInTbbHashMap)
{
    auto key1 = GetStringUuid();

    auto value1 = RandomData().GetRandomUint32();
    auto value2 = RandomData().GetRandomUint32();

    using MapType = tbb::concurrent_hash_map<ImmutableStringImpl, uint32_t>;

    MapType map1;
    MapType map2;

    auto im1 = ImmutableStringImpl(key1);
    MapType::accessor ac;
    // insert by ImmutableStringImpl
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

    EXPECT_EQ(ImmutableStringPool::Instance().Size(), 1ul);
}

TEST_F(ImmutableStringTest, ImInUnorderedMapInParrel)
{
    auto key1 = GetStringUuid();
    using MapType = std::unordered_map<ImmutableStringImpl, int>;
    MapType map1;
    auto pool = std::make_unique<ThreadPool>(10);
    std::shared_timed_mutex mutex;

    for (int i = 0; i < 10; i++) {
        pool->Execute([&key1, &map1, &mutex]() {
            std::lock_guard<std::shared_timed_mutex> lck(mutex);
            auto iter = map1.find(key1);
            if (iter == map1.end()) {
                map1.emplace(key1, 1);
            } else {
                iter->second = 2;
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

TEST_F(ImmutableStringTest, ImInSTLHashMap)
{
    using MapType = std::map<ImmutableStringImpl, uint32_t>;
    ImMapCheckMemoryReduce<MapType>();
}

TEST_F(ImmutableStringTest, ImInSTLUnorderedMap)
{
    using MapType = std::unordered_map<ImmutableStringImpl, uint32_t>;
    ImMapCheckMemoryReduce<MapType>();
}
}  // namespace ut
}  // namespace datasystem
