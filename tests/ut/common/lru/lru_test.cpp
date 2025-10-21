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
 * Description: Testing LruCache
 */
#include <memory>
#include <string>

#include "common.h"
#include "datasystem/common/lru/lru_cache.h"
#include "datasystem/common/util/bitmask_enum.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {
namespace ut {
class LruTest : public CommonTest {};

// This is a simple test object that we will use for caching.
// Since we will be using the pre-made size policy, we need to derive from the LruSizePolicyBase and adhere to its
// required interface
// The object class is one of the required class template arguments for the Lru_Cache
// Note that this is a derived class following CRTP pattern as per design
class TestObject : public LruSizePolicyObjBase<TestObject> {
public:
    explicit TestObject(std::string data)
        : LruSizePolicyObjBase(0), data_(std::move(data)), evicted_(false), removed_(false){};

    ~TestObject() = default;

    // overload base size policy object
    std::string ToStringImpl() const
    {
        std::string outStr("Evicted: ");
        outStr += (evicted_) ? " true" : "false";
        outStr += " Removed: ";
        outStr += (removed_) ? " true" : "false";
        outStr += " Data: " + data_;
        return outStr;
    };

    // overload base size policy object
    Status EvictImpl(EvictionCtrl evictFlags)
    {
        if (TESTFLAG(evictFlags, EvictionCtrl::EVICT_REMOVE)) {
            removed_ = true;
        } else if (TESTFLAG(evictFlags, EvictionCtrl::EVICT_SOFT)) {
            evicted_ = true;
        } else {
            evicted_ = true;
        }
        return Status::OK();
    };

    // overload base size policy object
    uint32_t SizeImpl() const
    {
        return data_.size();
    };

    bool IsEvicted()
    {
        return evicted_;
    };

    bool IsRemoved()
    {
        return removed_;
    };

private:
    std::string data_;
    bool evicted_;
    bool removed_;
};

// This is a simple test object that we will use for caching.
// Its basically the same as TestObject above except that this one is used with count policy.
// The object class is one of the required class template arguments for the Lru_Cache
// Note that this is a derived class following CRTP pattern as per design
class TestObject2 : public LruCountPolicyObjBase<TestObject2> {
public:
    explicit TestObject2(std::string data) : LruCountPolicyObjBase(0), data_(std::move(data)), evicted_(false){};

    ~TestObject2() = default;

    // overload base size policy object
    std::string ToStringImpl() const
    {
        std::string outStr("Evicted: ");
        outStr += (evicted_) ? " true" : "false";
        outStr += " Data: " + data_;
        return outStr;
    };

    // overload base size policy object
    Status EvictImpl(EvictionCtrl evictFlags)
    {
        (void)evictFlags;
        evicted_ = true;
        return Status::OK();
    };

    bool IsEvicted()
    {
        return evicted_;
    };

private:
    std::string data_;
    bool evicted_;
};

// This test object is used with LRUSharedPtr test to demonstrate cache object ownership and destroy.
// This one has a log message in the destructor for observing when an object gets destroyed by eviction.
class TestObject3 : public LruSizePolicyObjBase<TestObject3> {
public:
    explicit TestObject3(std::string data) : LruSizePolicyObjBase(0), data_(std::move(data)){};

    // Log a diagnostic when this object is actually destroyed so we can observe how the lru eviction has freed it.
    ~TestObject3()
    {
        LOG(INFO) << "Object being destroyed: " << ToStringImpl();
    }

    // overload base size policy object
    std::string ToStringImpl() const
    {
        std::string outStr("Data: ");
        outStr += data_;
        return outStr;
    };

    // overload base size policy object
    Status EvictImpl(EvictionCtrl evictFlags)
    {
        (void)evictFlags;
        // no-op...no special actions happen when this object is evicted
        return Status::OK();
    };

    // overload base size policy object
    uint32_t SizeImpl() const
    {
        return data_.size();
    };

private:
    std::string data_;
};

class TestObject4 : public LruSizePolicyObjBase<TestObject4> {
public:
    enum class ObjState : uint32_t { NONE = 0, PROTECTED = 0x1, EVICTED = 0x10 };
    friend ENABLE_BITMASK_ENUM_OPS(ObjState);

    explicit TestObject4(std::string data) : LruSizePolicyObjBase(0), state_(ObjState::NONE), data_(std::move(data)){};

    // Log a diagnostic when this object is actually destroyed so we can observe how the lru eviction has freed it.
    ~TestObject4() = default;

    // overload base size policy object
    std::string ToStringImpl() const
    {
        std::string outStr("State:");
        if (state_ == ObjState::NONE) {
            outStr += " NONE";
        } else {
            if (TESTFLAG(state_, ObjState::PROTECTED)) {
                outStr += " PROTECTED";
            }
            if (TESTFLAG(state_, ObjState::EVICTED)) {
                outStr += " EVICTED";
            }
        }
        outStr += " Data: " + data_;
        return outStr;
    };

    // overload base size policy object
    Status EvictImpl(EvictionCtrl evictFlags)
    {
        (void)evictFlags;
        std::unique_lock<std::mutex> ulock(objMtx_);
        // We are only allowed to evict this object if it's not in a protected state.
        // If its in a protected state, then we return the special code of try_again, which tells the lru
        // that this eviction is not allowed right now.
        if (!TESTFLAG(state_, ObjState::PROTECTED)) {
            SETFLAG(state_, ObjState::EVICTED);
            data_.clear();
        } else {
            RETURN_STATUS(StatusCode::K_TRY_AGAIN, "Object is protected.");
        }
        return Status::OK();
    };

    // overload base size policy object
    uint32_t SizeImpl() const
    {
        return data_.size();
    };

    void setProtected()
    {
        std::unique_lock<std::mutex> ulock(objMtx_);
        SETFLAG(state_, ObjState::PROTECTED);
    }

private:
    ObjState state_;
    std::string data_;
    std::mutex objMtx_;
};

// The object to cache can be identified by a compound key that consists of 2 int values.
// An object key is one of the classes that must be provided to the Lru_Cache as a template argument
// and it must implement the HashKey interface using CRTP pattern
class TestObjectKey : public HashKey<TestObjectKey> {
public:
    TestObjectKey() = default;
    TestObjectKey(int id, int otherId) : id_(id), otherId_(otherId){};

    ~TestObjectKey() = default;

    // For hash collisions inside the cache, keys need to be able to perform an equality comparison
    bool EqualImpl(const TestObjectKey *cmp) const
    {
        return (this->id_ == cmp->id_ && this->otherId_ == cmp->otherId_);
    }

    // Give a method to fetch the key as a string for debug purposes
    std::string ToString() const
    {
        std::string outStr("[");
        outStr += std::to_string(this->id_) + " " + std::to_string(this->otherId_) + "]";
        return outStr;
    };

    static std::size_t HashImpl(const TestObjectKey *k)
    {
        size_t h1 = std::hash<int>()(k->id_);
        size_t h2 = std::hash<int>()(k->otherId_);
        return h1 ^ (h2 << 1);
    }

    int id_;
    int otherId_;
};

// Test some failures to init objects and construct the cache
TEST_F(LruTest, TestConstruction)
{
    // The LRU cache is templated based on a few classes.  Define those here for convenence, using the TestObject
    // as the cached object (and related key functions).
    // For this test, it will use the simple pre-made size policy for evictions.
    using Lru = LruCache<TestObjectKey, TestObject *, LruSizePolicy>;

    // Try to build a lru cache that does not have any policy
    std::unique_ptr<Lru> lruCache = nullptr;
    Status rc;
    rc = Lru::Builder().Build(&lruCache);
    ASSERT_TRUE(rc.IsError());
    LOG(INFO) << rc.ToString();
    ASSERT_TRUE(lruCache == nullptr);

    // Create a valid one
    auto policy = std::make_unique<LruSizePolicy>();
    rc = Lru::Builder().SetPolicy(std::move(policy)).Build(&lruCache);
    LOG(INFO) << rc.ToString();
    ASSERT_TRUE(rc.IsOk());

    // Try to create it again.  Disallowed if output arg ptr is non-null
    rc = Lru::Builder().Build(&lruCache);
    ASSERT_TRUE(rc.IsError());
    LOG(INFO) << "Expected error case: " << rc.ToString();

    lruCache.reset();

    // Test invalid number of buckets
    policy.reset();
    policy = std::make_unique<LruSizePolicy>();
    rc = Lru::Builder().SetPolicy(std::move(policy)).SetNumBuckets(0).Build(&lruCache);
    ASSERT_TRUE(rc.IsError());
    LOG(INFO) << "Expected error case: " << rc.ToString();
}

// Tests a play by play of lru usage with some lru additions, lru reads, and a few error scenarios.
// This one is using the pre-made size-based eviction policy
TEST_F(LruTest, SizePolicyTest)
{
    // The LRU cache is templated based on a few classes.  Define those here for convenence.
    using Lru = LruCache<TestObjectKey, TestObject *, LruSizePolicy>;

    // Create some objects that we'll use to cache later.
    std::vector<TestObject> objectList;
    objectList.push_back(TestObject("data1"));
    objectList.push_back(TestObject("data2 has some info in it"));
    objectList.push_back(TestObject("data3 is really fun"));
    objectList.push_back(TestObject("data4"));

    // Show the objects to start with. These are not in any cache at this point.
    for (auto i : objectList) {
        LOG(INFO) << i.ToString() << std::endl;
    }

    // Create the policy for the cache.  Using the pre-made size policy as the example for this testcase.
    auto policy = std::make_unique<LruSizePolicy>();
    policy->SetCacheSize(50);  // Cache can hold 50 bytes worth of data

    std::unique_ptr<Lru> lruCache = nullptr;
    Status rc;
    rc = Lru::Builder().SetPolicy(std::move(policy)).Build(&lruCache);

    // Execute the LRU Access method using the first 3 objects.  This will result in adding those objects to the cache.
    // Each object needs a unique key that uniquely identifies that object

    rc = lruCache->Access(TestObjectKey(1, 1), &objectList[0]);
    ASSERT_TRUE(rc.IsOk());

    rc = lruCache->Access(TestObjectKey(5, 9), &objectList[1]);
    ASSERT_TRUE(rc.IsOk());

    rc = lruCache->Access(TestObjectKey(99, 13), &objectList[2]);
    ASSERT_TRUE(rc.IsOk());

    // Now, display the resultant cache
    LOG(INFO) << lruCache->ToString(true) << std::endl;

    // Add a 4th item that will exceed the size of 50, thus causing an eviction before it can be tracked by the cache
    // In this testcase, the chosen eviction is key (1,1 == objectList index 0) as it's the oldest.  Validate this is
    // is true.
    rc = lruCache->Access(TestObjectKey(4, 1), &objectList[3]);
    ASSERT_TRUE(rc.IsOk());
    ASSERT_TRUE(objectList[0].IsEvicted());

    // Show the cache, and show the overall objects to see the evicted one
    LOG(INFO) << lruCache->ToString(true) << "\nOverall object list:\n" << std::endl;
    for (auto i : objectList) {
        LOG(INFO) << i.ToString() << std::endl;
    }

    // Now, let's show a cache-hit access operation (where we already have the meta data and just want to bump our
    // usage in the lru)
    rc = lruCache->Access(TestObjectKey(5, 9), &objectList[1]);
    ASSERT_TRUE(rc.IsOk());

    // No eviction, but now this item should have become the most recently used
    LOG(INFO) << lruCache->ToString(true) << std::endl;

    // Now, let's try a lookup/fetch, where we don't know what the data ptr is and want to fetch it based on key
    TestObject *fetch_obj = nullptr;
    rc = lruCache->Lookup(TestObjectKey(99, 13), &fetch_obj);
    ASSERT_TRUE(rc.IsOk());

    LOG(INFO) << "Fetched object: " << fetch_obj->ToString() << std::endl;
}

// Tests a play by play of lru usage with some lru additions, lru reads, and a few error scenarios.
// This is essentially identical to the previous test, except that is uses the count-based eviction policy
// instead of the size-based policy
TEST_F(LruTest, CountPolicyTest)
{
    // The LRU cache is templated based on a few classes.  Define those here for convenence.
    using Lru = LruCache<TestObjectKey, TestObject2 *, LruCountPolicy>;

    // Create some objects that we'll use to cache later.
    std::vector<TestObject2> objectList;
    objectList.push_back(TestObject2("data1"));
    objectList.push_back(TestObject2("data2 has some info in it"));
    objectList.push_back(TestObject2("data3 is really fun"));
    objectList.push_back(TestObject2("data4"));

    // Show the objects to start with. These are not in any cache at this point.
    for (auto i : objectList) {
        LOG(INFO) << i.ToString() << std::endl;
    }

    // Create the policy for the cache.  Using the pre-made size policy as the example for this testcase.
    auto policy = std::make_unique<LruCountPolicy>();
    policy->SetCacheCount(3);  // cache can only hold 3 objects

    std::unique_ptr<Lru> lruCache = nullptr;
    Status rc;
    rc = Lru::Builder().SetPolicy(std::move(policy)).Build(&lruCache);

    // Execute the LRU Access method using the first 3 objects.  This will result in adding those objects to the cache.
    // Each object needs a unique key that uniquely identifies that object

    rc = lruCache->Access(TestObjectKey(1, 1), &objectList[0]);
    ASSERT_TRUE(rc.IsOk());

    rc = lruCache->Access(TestObjectKey(5, 9), &objectList[1]);
    ASSERT_TRUE(rc.IsOk());

    rc = lruCache->Access(TestObjectKey(99, 13), &objectList[2]);
    ASSERT_TRUE(rc.IsOk());

    // Now, display the resultant cache
    LOG(INFO) << lruCache->ToString(true) << std::endl;

    // Add a 4th item that will exceed the count of 3, thus causing an eviction before it can be tracked by the cache.
    // In this testcase, the chosen eviction is key (1,1 == objectList index 0) as it's the oldest.  Validate this is
    // is true.
    rc = lruCache->Access(TestObjectKey(4, 1), &objectList[3]);
    ASSERT_TRUE(rc.IsOk());
    ASSERT_TRUE(objectList[0].IsEvicted());

    // Show the cache, and show the overall objects to see the evicted one
    LOG(INFO) << lruCache->ToString(true) << "\nOverall object list:\n" << std::endl;
    for (auto i : objectList) {
        LOG(INFO) << i.ToString() << std::endl;
    }

    // Now, let's show a cache-hit access operation (where we already have the meta data and just want to bump our
    // usage in the lru)
    rc = lruCache->Access(TestObjectKey(5, 9), &objectList[1]);
    ASSERT_TRUE(rc.IsOk());

    // No eviction, but now this item should have become the most recently used
    LOG(INFO) << lruCache->ToString(true) << std::endl;

    // Now, let's try a lookup/fetch, where we don't know what the data ptr is and want to fetch it based on key
    TestObject2 *fetch_obj = nullptr;
    rc = lruCache->Lookup(TestObjectKey(99, 13), &fetch_obj);
    ASSERT_TRUE(rc.IsOk());

    LOG(INFO) << "Fetched object: " << fetch_obj->ToString() << std::endl;
}

TEST_F(LruTest, CountPolicyAsyncEvictTest)
{
    // The LRU cache is templated based on a few classes.  Define those here for convenence.
    using Lru = LruCache<TestObjectKey, TestObject2 *, LruCountPolicy>;

    // Create some objects that we'll use to cache later.
    std::vector<TestObject2> objectList;
    for (int i = 0; i < 10; i++) {
        objectList.push_back(TestObject2("data" + std::to_string(i)));
    }
    // Show the objects to start with. These are not in any cache at this point.
    for (auto i : objectList) {
        LOG(INFO) << i.ToString() << std::endl;
    }

    // Create the policy for the cache.  Using the pre-made size policy as the example for this testcase.
    auto policy = std::make_unique<LruCountPolicy>();
    policy->SetCacheCount(10);  // cache can only hold 10 objects
    policy->SetWatermark(60, 80);

    std::unique_ptr<Lru> lruCache = nullptr;
    Status rc;
    rc = Lru::Builder().SetPolicy(std::move(policy)).SetAsyncEvict(true).Build(&lruCache);

    for (int i = 0; i < 8; i++) {
        LOG(INFO) << "Ready to push " << i;
        rc = lruCache->Access(TestObjectKey(i, i), &objectList[i]);
        ASSERT_TRUE(rc.IsOk());
    }
    sleep(1);
    // Now, display the resultant cache
    LOG(INFO) << lruCache->ToString(true) << std::endl;
    ASSERT_TRUE(objectList[0].IsEvicted());
    ASSERT_TRUE(objectList[1].IsEvicted());
}

// Promote with contention and then spin loops of lru access
TEST_F(LruTest, LruConcurrency)
{
    // The LRU cache is templated based on a few classes.  Define those here for convenence.
    using Lru = LruCache<TestObjectKey, TestObject *, LruSizePolicy>;

    int numReaders = 4;
    int numWriters = 4;
    int numReaderLoops = 100;
    int numWriterLoops = 100;
    std::atomic<int> keyId = { 3 };  // Writers start with keyId of 3 (they increment first b4 using it)

    std::vector<std::thread> readerThreads;
    std::vector<std::thread> writerThreads;

    // Create some objects that we'll use to cache later.
    // The first 4 will be the ones the readers loop over
    std::vector<TestObject> objectList;
    objectList.push_back(TestObject("data1data1data1data1data1data1data1data1"));
    objectList.push_back(TestObject("data2data2data2data2data2data2data2data2"));
    objectList.push_back(TestObject("data3data3data3data3data3data3data3data3"));
    objectList.push_back(TestObject("data4data4data4data4data4data4data4data4"));

    // The remaining will be used to add new objects from the writers
    // The amount of objects needed will be so that each writer has a new object to write
    // for each loop iteration.
    for (int i = 0; i < (numWriterLoops * numWriters); ++i) {
        objectList.emplace_back("dataxdataxdataxdataxdataxdataxdataxdatax");
    }

    // Create the policy for the cache.  Using the pre-made size policy as the example for this testcase.
    auto policy = std::make_unique<LruSizePolicy>();
    policy->SetCacheSize(5000);
    std::unique_ptr<Lru> lruCache = nullptr;
    Status rc;
    rc = Lru::Builder().SetPolicy(std::move(policy)).Build(&lruCache);
    ASSERT_TRUE(rc.IsOk());

    // Test objective/setup:
    // The reader threads will continuously drive access against 4 existing keys keeping them hot in the lru.
    // The writer threads will continuously drive inserts of new items into the lru, but they will quickly move to the
    // back of the lru. Eventually, the lru will fill up and we'll get some churn with evictions happening as well,
    // but the top 4 hot items should never be evicted
    // Due to timing, it's a bit difficult to assert the lru locations and status, but at least we cover concurrent
    // lru access here and we can visually inspect the cache after to make sure the 4 items are at the front of the
    // cache and that various evictions have occurred.

    // Populate the cache for the readers.  They will access objects (1,x) where x is 0 to 3
    rc = lruCache->Access(TestObjectKey(1, 0), &objectList[0]);
    ASSERT_TRUE(rc.IsOk());
    rc = lruCache->Access(TestObjectKey(1, 1), &objectList[1]);
    ASSERT_TRUE(rc.IsOk());
    rc = lruCache->Access(TestObjectKey(1, 2), &objectList[2]);
    ASSERT_TRUE(rc.IsOk());
    rc = lruCache->Access(TestObjectKey(1, 3), &objectList[3]);
    ASSERT_TRUE(rc.IsOk());

    // Launch the readers
    for (int i = 0; i < numReaders; ++i) {
        readerThreads.push_back(std::thread([&numReaderLoops, &lruCache, &objectList]() {
            Status rc;
            for (int loops = 0; loops < numReaderLoops; ++loops) {
                int id = loops % 4;
                rc = lruCache->Access(TestObjectKey(1, id), &objectList[id]);
                ASSERT_TRUE(rc.IsOk());
            }
        }));
    }

    // Launch the writers
    for (int i = 0; i < numWriters; ++i) {
        writerThreads.push_back(std::thread([&numWriterLoops, &lruCache, &keyId, &objectList]() {
            Status rc;
            for (int loops = 0; loops < numWriterLoops; ++loops) {
                int newId = ++keyId;
                rc = lruCache->Access(TestObjectKey(1, newId), &objectList[newId]);
                ASSERT_TRUE(rc.IsOk());
            }
        }));
    }

    // Join the readers
    for (int i = 0; i < numReaders; ++i) {
        readerThreads[i].join();
    }

    // Join the writers
    for (int i = 0; i < numWriters; ++i) {
        writerThreads[i].join();
    }

    // This will be a big display, but you can inspect it for correctness
    // We hope that ids, (1,x) (x from 0 to 3) are very close to the front
    LOG(INFO) << lruCache->ToString(true) << std::endl;

    // In this display, we will see the entire list of objects, and many of them
    // will be in the evicted state because the cache could not hold them all and they got punted out
    for (uint32_t i = 0; i < objectList.size(); ++i) {
        LOG(INFO) << "[" << i << "]: " << objectList[i].ToString() << std::endl;
    }
}

// Test a shared_ptr case where the cache owns the object life of the object.  Thus, an eviction will result in
// the shared_ptr ref count reaching 0 and causing the object to be destroyed.
TEST_F(LruTest, LruSharedPtr)
{
    // The LRU cache is templated based on a few classes.  Define those here for convenence.
    using Lru = LruCache<TestObjectKey, std::shared_ptr<TestObject3>, LruSizePolicy>;

    // Create the policy for the cache.  Using the pre-made size policy as the example for this testcase.
    auto policy = std::make_unique<LruSizePolicy>();
    policy->SetCacheSize(50);
    std::unique_ptr<Lru> lruCache = nullptr;
    Status rc;
    rc = Lru::Builder().SetPolicy(std::move(policy)).Build(&lruCache);
    ASSERT_TRUE(rc.IsOk());

    // 3rd and 4th access will result in evicting previous entries.
    // Since this is the shared_ptr version, we expect those shared_ptr's to descope and free the object when the
    // eviction happens (the destructor of TestObject3 logs an entry to view in the output log)
    rc = lruCache->Access(TestObjectKey(1, 0), std::make_shared<TestObject3>("data1data1data1data1data1"));
    ASSERT_TRUE(rc.IsOk());
    rc = lruCache->Access(TestObjectKey(2, 0), std::make_shared<TestObject3>("data2data2data2data2data2"));
    ASSERT_TRUE(rc.IsOk());
    rc = lruCache->Access(TestObjectKey(3, 0), std::make_shared<TestObject3>("data3data3data3data3data3"));
    ASSERT_TRUE(rc.IsOk());
    rc = lruCache->Access(TestObjectKey(4, 0), std::make_shared<TestObject3>("data4data4data4data4data4"));
    ASSERT_TRUE(rc.IsOk());

    // Now, let us cause another eviction.  However, we will first fetch the object that we know is going to be
    // evicted soon.  Fetching the object will bump it's position in the lru, so it will take a two more accesses
    // of new objects before the one we fetched gets booted out.
    std::shared_ptr<TestObject3> fetchedObject = nullptr;
    TestObjectKey findKey(3, 0);
    rc = lruCache->Lookup(findKey, &fetchedObject);
    ASSERT_TRUE(rc.IsOk());
    // there should be 2 owners of the object, ourself and the copy in the cache
    ASSERT_TRUE(fetchedObject.use_count() == 2);
    LOG(INFO) << "Fetched object: " << findKey.ToString() << " : " << fetchedObject->ToString();

    rc = lruCache->Access(TestObjectKey(5, 0), std::make_shared<TestObject3>("data5data5data5data5data5"));
    ASSERT_TRUE(rc.IsOk());
    rc = lruCache->Access(TestObjectKey(6, 0), std::make_shared<TestObject3>("data6data6data6data6data6"));
    ASSERT_TRUE(rc.IsOk());

    // Show the cache
    LOG(INFO) << lruCache->ToString(true) << std::endl;
    // Ensure that we can still access the object we fetched, although now we are the only owner of the object
    // since it has been removed from the cache via eviction.
    ASSERT_TRUE(fetchedObject.use_count() == 1);
    LOG(INFO) << "Fetched object: " << findKey.ToString() << " : " << fetchedObject->ToString();
}

// This testcase will demonstrate the contention against an object where the user wants to read the object,
// but the cache wants to evict it at the same time, by using state handling in the user code of the object.
TEST_F(LruTest, LruReaderContention)
{
    // The LRU cache is templated based on a few classes.  Define those here for convenence.
    using Lru = LruCache<TestObjectKey, std::shared_ptr<TestObject4>, LruSizePolicy>;

    // Create the policy for the cache.  Using the pre-made size policy as the example for this testcase.
    auto policy = std::make_unique<LruSizePolicy>();
    policy->SetCacheSize(50);
    std::unique_ptr<Lru> lruCache = nullptr;
    Status rc;
    rc = Lru::Builder().SetPolicy(std::move(policy)).Build(&lruCache);
    ASSERT_TRUE(rc.IsOk());

    // To start, let's get 2 objects written to the lru cache.
    // The first object called myObj, we will not "move" into the cache, but instead we will share the ownership of it.
    // That is, this user code has a reference to the object, and also the lru cache has a reference to the same object.

    // Secondly, TestObject4 has internal state handling such that we do not want to allow the object to be evicted
    // if it is in some state that does not allow it to be evicted.
    auto myObj = std::make_shared<TestObject4>("data1data1data1data1data1");
    myObj->setProtected();
    rc = lruCache->Access(TestObjectKey(1, 0), myObj);
    ASSERT_TRUE(rc.IsOk());
    rc = lruCache->Access(TestObjectKey(2, 0), std::make_shared<TestObject4>("data2data2data2data2data2"));
    ASSERT_TRUE(rc.IsOk());

    // Now, as per this small cache size, if we add a 3rd object, it will force the eviction of an object.
    // Normally, the oldest object will be myObj(1,0), but in this case, that object will have to remain inside the
    // cache and (2,0) will be evicted instead.
    rc = lruCache->Access(TestObjectKey(3, 0), std::make_shared<TestObject4>("data3data3data3data3data3"));
    ASSERT_TRUE(rc.IsOk());

    // Show the cache
    LOG(INFO) << lruCache->ToString(true) << std::endl;

    // Now, let us fetch the key 3,0 that is now in the cache.  (2,0) got booted out and its destroyed.
    std::shared_ptr<TestObject4> fetchedObject = nullptr;
    TestObjectKey findKey(3, 0);
    rc = lruCache->Lookup(findKey, &fetchedObject);
    ASSERT_TRUE(rc.IsOk());
    ASSERT_TRUE(fetchedObject.use_count() == 2);  // validate that there's 2 users of this object.

    fetchedObject->setProtected();
    fetchedObject.reset();

    // Try to put another on in.  However, the cache is full of objects that cannot be evicted, so this should be
    // a hard failure
    rc = lruCache->Access(TestObjectKey(4, 0), std::make_shared<TestObject4>("data4data4data4data4data4"));
    ASSERT_TRUE(rc.IsError());
}

// Test the lru deletion code
TEST_F(LruTest, LruDelete)
{
    // The LRU cache is templated based on a few classes.  Define those here for convenence.
    using Lru = LruCache<TestObjectKey, TestObject *, LruSizePolicy>;

    // Create some objects that we'll use to cache later.
    std::vector<TestObject> objectList;
    objectList.push_back(TestObject("data1"));
    objectList.push_back(TestObject("data2 has some info in it"));
    objectList.push_back(TestObject("data3 is really fun"));
    objectList.push_back(TestObject("data4"));

    // Show the objects to start with. These are not in any cache at this point.
    for (auto i : objectList) {
        LOG(INFO) << i.ToString() << std::endl;
    }

    // Create the policy for the cache.  Using the pre-made size policy as the example for this testcase.
    auto policy = std::make_unique<LruSizePolicy>();
    policy->SetCacheSize(500);  // Cache big enough to hold all stuff in this testcase

    std::unique_ptr<Lru> lruCache = nullptr;
    Status rc;
    rc = Lru::Builder().SetPolicy(std::move(policy)).Build(&lruCache);

    // Execute the LRU Access method using the first 3 objects.  This will result in adding those objects to the cache.
    // Each object needs a unique key that uniquely identifies that object

    rc = lruCache->Access(TestObjectKey(1, 1), &objectList[0]);
    ASSERT_TRUE(rc.IsOk());

    rc = lruCache->Access(TestObjectKey(5, 9), &objectList[1]);
    ASSERT_TRUE(rc.IsOk());

    rc = lruCache->Access(TestObjectKey(99, 13), &objectList[2]);
    ASSERT_TRUE(rc.IsOk());

    rc = lruCache->Access(TestObjectKey(4, 1), &objectList[3]);
    ASSERT_TRUE(rc.IsOk());

    // Show the cache
    LOG(INFO) << lruCache->ToString(true) << std::endl;

    // Remove one of them
    rc = lruCache->Remove(TestObjectKey(99, 13));
    ASSERT_TRUE(rc.IsOk());
    ASSERT_TRUE(objectList[2].IsRemoved());

    // Show the cache again
    LOG(INFO) << lruCache->ToString(true) << std::endl;

    // Show the objects again.
    for (auto i : objectList) {
        LOG(INFO) << i.ToString() << std::endl;
    }
}

TEST_F(LruTest, LruPolicyInvalidArgs)
{
    LruSizePolicy szPolicy;
    szPolicy.SetCacheSize(0);
    ASSERT_FALSE(szPolicy.AboveHighWatermark());
    ASSERT_FALSE(szPolicy.BelowLowWatermark());

    LruCountPolicy countPolicy;
    countPolicy.SetCacheCount(0);
    ASSERT_FALSE(countPolicy.AboveHighWatermark());
    ASSERT_FALSE(countPolicy.BelowLowWatermark());
}
}  // namespace ut
}  // namespace datasystem
