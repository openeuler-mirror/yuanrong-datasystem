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
#include <memory>
#include <thread>
#include <chrono>

#include "ut/common.h"
#include "datasystem/common/object_cache/safe_object.h"
#include "datasystem/common/immutable_string/immutable_string.h"
#include "datasystem/common/immutable_string/immutable_string_pool.h"
#include "datasystem/common/object_cache/object_base.h"
#include "datasystem/common/object_cache/safe_table.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/thread_pool.h"
#include "datasystem/common/util/uuid_generator.h"

namespace datasystem {
namespace ut {
class SafeObjectTest : public CommonTest {
public:
    SafeObjectTest() : numObjects_(0), numWriterThreads_(0), numReaderThreads_(0), currObjKey_(0)
    {
    }

    void JoinThreads()
    {
        if (!writerThreads_.empty()) {
            for (auto &writerThread : writerThreads_) {
                writerThread.join();
            }
        }

        if (!readerThreads_.empty()) {
            for (auto &readerThread : readerThreads_) {
                readerThread.join();
            }
        }
    }

    // Circular atomic index fetcher.  Gets next id in the sequence, back to 0 if the last one
    // is hit.
    int GetNextObjKey()
    {
        if (currObjKey_.compare_exchange_strong(numObjects_, 0)) {
            return 0;
        } else {
            return currObjKey_.fetch_add(1);
        }
    }

    int GetRandomInt(int min, int max)
    {
        std::random_device rD;
        static thread_local std::mt19937 randGen(rD());
        std::uniform_int_distribution<int> distr(min, max);
        return distr(randGen);
    }

    // Get a unique string id
    std::string NewObjectKey()
    {
        return GetStringUuid();
    }

protected:
    int numObjects_;
    int numWriterThreads_;
    int numReaderThreads_;
    std::atomic<int> currObjKey_;
    std::vector<std::thread> writerThreads_;
    std::vector<std::thread> readerThreads_;
};

// A simple class used for testing (base class)
class BaseObj : public ObjectInterface {
public:
    explicit BaseObj(int bVal) : baseVal_(bVal)
    {
    }

    virtual void print()
    {
        LOG(INFO) << "base print has val: " << baseVal_ << std::endl;
    }

    virtual void mustOverride(int num) = 0;

    Status FreeResources() override
    {
        return Status::OK();
    }

protected:
    int baseVal_;
};

// A simple class used for testing (derived class)
class DerivedObj : public BaseObj {
public:
    DerivedObj(int dVal, int bVal) : BaseObj(bVal), derivedVal_(dVal)
    {
    }

    void print() override
    {
        LOG(INFO) << "derived print.  base has val: " << baseVal_ << " and derived val: " << derivedVal_ << std::endl;
    }

    void mustOverride(int num) override
    {
        LOG(INFO) << "must override: " << num << std::endl;
    }

    void notVirtualFunction()
    {
        LOG(INFO) << "called notVirtualFunction.  base class does not have this";
    }

private:
    int derivedVal_;
};

// Test basic construction and data extraction
TEST_F(SafeObjectTest, TestConstruct1)
{
    using SafeString = SafeObject<std::string>;

    std::string data1("hello1");
    auto data2 = std::make_unique<std::string>("hello2");

    // Deep copy construction.  A safe object is constructed by copying the input data into the object.
    auto objPtr1 = std::make_shared<SafeString>(data1);

    // Move-based construction, where unique_ptr is moved into the SafeString
    auto objPtr2 = std::make_shared<SafeString>(std::move(data2));

    SafeString &safeString1 = *objPtr1;       // Dereference the shared_ptr to get a reference to SafeObject
    std::string &fetchedData = *safeString1;  // Dereference the SafeObject to get a reference to the real data
    LOG(INFO) << "Data is: " << fetchedData;
    // Make some change to the data with string::insert() since we have a reference
    fetchedData.insert(6, " world");
    // Go through dereference again to show the updated data
    LOG(INFO) << "Data is: " << *(*objPtr1);

    SafeString &safeString2 = *objPtr2;  // Dereference the shared_ptr to get a reference to SafeObject
    // Use -> to dereference to a SafeObject ptr then execute string::insert against real object
    safeString2->insert(6, " world");
    LOG(INFO) << "Data is: " << *(*objPtr2);
}

// Test construction of empty SafeObject and then assign data to it
TEST_F(SafeObjectTest, TestConstruct2)
{
    using SafeString = SafeObject<std::string>;

    std::string data1("hello1");
    auto data2 = std::make_unique<std::string>("hello2");

    // Create some SafeObjects that are initially empty except for the lock
    auto objPtr1 = std::make_shared<SafeString>();
    // A different constructor, takes unique pointer, but you can pass in null to achieve the same effect as no-arg
    // version
    auto objPtr2 = std::make_shared<SafeString>(std::unique_ptr<std::string>(nullptr));

    // Then, populate them with a real object.  Typically you would acquire locks first, but this test does not have
    // concurrency for now.
    objPtr1->SetRealObject(data1);
    objPtr2->SetRealObject(std::move(data2));
    LOG(INFO) << "Data is: " << *(*objPtr1);
}

// Write locking
TEST_F(SafeObjectTest, TestLocks1)
{
    using SafeString = SafeObject<std::string>;
    const int numLoops = 10;
    numWriterThreads_ = 10;
    numObjects_ = 2;  // small number of objects to promote frequent contention
    std::vector<std::shared_ptr<SafeString>> safeStrings;

    // Single threaded, create 2 SafeObjects as shared ptrs stored in a vector.
    safeStrings.push_back(std::move(std::make_shared<SafeString>("abc")));
    safeStrings.push_back(std::move(std::make_shared<SafeString>("def")));

    for (int threadId = 0; threadId < numWriterThreads_; ++threadId) {
        writerThreads_.push_back(std::thread([this, threadId, &numLoops, &safeStrings]() {
            // Do numLoops iterations of writes from this thread.
            for (int i = 0; i < numLoops; ++i) {
                int id = GetRandomInt(0, (numObjects_ - 1));
                DS_ASSERT_OK(safeStrings[id]->WLock());
                LOG(INFO) << "Thread[" << threadId << "] got lock and appending an \"a\" to SafeObject[" << id << "]";
                *(*(safeStrings[id])) += "a";
                safeStrings[id]->WUnlock();
            }
        }));
    }

    JoinThreads();

    LOG(INFO) << "SafeObject[0]: " << *(*(safeStrings[0]));
    LOG(INFO) << "SafeObject[1]: " << *(*(safeStrings[1]));
}

// Read locking
TEST_F(SafeObjectTest, TestLocks2)
{
    using SafeString = SafeObject<std::string>;
    const int numLoops = 10;
    numReaderThreads_ = 10;
    numObjects_ = 2;  // small number of objects to promote frequent contention
    std::vector<std::shared_ptr<SafeString>> safeStrings;

    // Single threaded, create 2 SafeObjects as shared ptrs stored in a vector.
    safeStrings.push_back(std::move(std::make_shared<SafeString>("abc")));
    safeStrings.push_back(std::move(std::make_shared<SafeString>("def")));

    for (int threadId = 0; threadId < numReaderThreads_; ++threadId) {
        readerThreads_.push_back(std::thread([this, threadId, &numLoops, &safeStrings]() {
            // Do numLoops iterations of reads from this thread.
            for (int i = 0; i < numLoops; ++i) {
                int id = GetRandomInt(0, (numObjects_ - 1));
                DS_ASSERT_OK(safeStrings[id]->RLock());
                LOG(INFO) << "Thread[" << threadId << "]: " << *(*(safeStrings[id]));
                safeStrings[id]->RUnlock();
            }
        }));
    }

    JoinThreads();
}

// Combined readers and writers
TEST_F(SafeObjectTest, TestLocks3)
{
    using SafeString = SafeObject<std::string>;
    const int numLoops = 100;
    const int numWLoops = 20;
    numReaderThreads_ = 10;
    numWriterThreads_ = 2;  // not as many writers, don't starve the readers
    numObjects_ = 2;        // small number of objects to promote frequent contention
    std::vector<std::shared_ptr<SafeString>> safeStrings;

    // Single threaded, create 2 SafeObjects as shared ptrs stored in a vector.
    safeStrings.push_back(std::move(std::make_shared<SafeString>("abc")));
    safeStrings.push_back(std::move(std::make_shared<SafeString>("def")));

    for (int threadId = 0; threadId < numReaderThreads_; ++threadId) {
        readerThreads_.push_back(std::thread([this, threadId, &numLoops, &safeStrings]() {
            // Do numLoops iterations of reads from this thread.
            for (int i = 0; i < numLoops; ++i) {
                int id = GetRandomInt(0, (numObjects_ - 1));
                DS_ASSERT_OK(safeStrings[id]->RLock());
                LOG(INFO) << "Thread[" << threadId << "]: " << *(*(safeStrings[id]));
                safeStrings[id]->RUnlock();
            }
        }));
    }

    for (int threadId = 0; threadId < numWriterThreads_; ++threadId) {
        writerThreads_.push_back(std::thread([this, threadId, &numWLoops, &safeStrings]() {
            // Do numWLoops iterations of writes from this thread.
            for (int i = 0; i < numWLoops; ++i) {
                int id = GetRandomInt(0, (numObjects_ - 1));
                std::this_thread::sleep_for(std::chrono::microseconds(100));  // slow down the writers a bit
                DS_ASSERT_OK(safeStrings[id]->WLock());
                LOG(INFO) << "Thread[" << threadId << "] got lock and appending an \"a\" to SafeObject[" << id << "]";
                *(*(safeStrings[id])) += "a";
                safeStrings[id]->WUnlock();
            }
        }));
    }

    JoinThreads();
}

// Combined readers and writers
// Use the secondary lock instead of the main lock
TEST_F(SafeObjectTest, TestLocks4)
{
    using SafeString = SafeObject<std::string>;
    const int numLoops = 100;
    const int numWLoops = 20;
    numReaderThreads_ = 10;
    numWriterThreads_ = 2;  // not as many writers, don't starve the readers
    numObjects_ = 2;        // small number of objects to promote frequent contention
    std::vector<std::shared_ptr<SafeString>> safeStrings;

    // Single threaded, create 2 SafeObjects as shared ptrs stored in a vector.
    safeStrings.push_back(std::move(std::make_shared<SafeString>("abc")));
    safeStrings.push_back(std::move(std::make_shared<SafeString>("def")));

    for (int threadId = 0; threadId < numReaderThreads_; ++threadId) {
        readerThreads_.push_back(std::thread([this, threadId, &numLoops, &safeStrings]() {
            // Do numLoops iterations of reads from this thread.
            for (int i = 0; i < numLoops; ++i) {
                int id = GetRandomInt(0, (numObjects_ - 1));
                DS_ASSERT_OK(safeStrings[id]->RLock());
                LOG(INFO) << "Thread[" << threadId << "]: " << *(*(safeStrings[id]));
                safeStrings[id]->RUnlock();
            }
        }));
    }

    for (int threadId = 0; threadId < numWriterThreads_; ++threadId) {
        writerThreads_.push_back(std::thread([this, threadId, &numWLoops, &safeStrings]() {
            // Do numWLoops iterations of writes from this thread.
            for (int i = 0; i < numWLoops; ++i) {
                int id = GetRandomInt(0, (numObjects_ - 1));
                std::this_thread::sleep_for(std::chrono::microseconds(100));  // slow down the writers a bit
                DS_ASSERT_OK(safeStrings[id]->WLock());
                LOG(INFO) << "Thread[" << threadId << "] got lock and appending an \"a\" to SafeObject[" << id << "]";
                *(*(safeStrings[id])) += "a";
                safeStrings[id]->WUnlock();
            }
        }));
    }

    JoinThreads();
}

// Test access a deleted object
TEST_F(SafeObjectTest, TestDelete)
{
    using SafeString = SafeObject<std::string>;

    // 2 shared pointers pointing to same SafeObject
    auto objPtr1 = std::make_shared<SafeString>("abc");
    std::shared_ptr<SafeString> objPtr2 = objPtr1;

    // Delete the internal object and free the shared pointer
    DS_ASSERT_OK(objPtr1->WLock());
    objPtr1->DeleteObject();
    objPtr1->WUnlock();
    objPtr1.reset();

    // Another shared_ptr exists so the use_count is 1 and SafeObject is still exists.
    // However, it was deleted and cannot be used as if it does not exist.
    Status rc = objPtr2->WLock();
    ASSERT_TRUE(rc.GetCode() == StatusCode::K_NOT_FOUND);

    rc = objPtr2->RLock();
    ASSERT_TRUE(rc.GetCode() == StatusCode::K_NOT_FOUND);
}

// Test an attempt to get a write lock that is held already but do not wait for it
TEST_F(SafeObjectTest, TestTryWriteLocks)
{
    using SafeString = SafeObject<std::string>;
    auto objPtr1 = std::make_shared<SafeString>("abc");

    // Get the lock first in write mode
    DS_ASSERT_OK(objPtr1->WLock());

    // Now try to get the lock again with try mode
    Status rc = objPtr1->TryWLock();
    LOG(INFO) << "TryWLock returned: " << rc.ToString();
    ASSERT_TRUE(rc.GetCode() == StatusCode::K_TRY_AGAIN);

    // free the write lock and get the lock in read mode
    objPtr1->WUnlock();
    DS_ASSERT_OK(objPtr1->RLock());

    // Then, try to get the lock with try mode
    rc = objPtr1->TryWLock();
    LOG(INFO) << "TryWLock returned: " << rc.ToString();
    ASSERT_TRUE(rc.GetCode() == StatusCode::K_TRY_AGAIN);
    objPtr1->RUnlock();

    DS_ASSERT_OK(objPtr1->WLock());

    // Now try to get the lock again with try mode
    rc = objPtr1->TryWLock();
    LOG(INFO) << "TryWLock returned: " << rc.ToString();
    ASSERT_TRUE(rc.GetCode() == StatusCode::K_TRY_AGAIN);

    // free the write lock and get the lock in read mode
    objPtr1->WUnlock();
    objPtr1->RLock();

    // Then, try to get the lock with try mode
    rc = objPtr1->TryWLock();
    LOG(INFO) << "TryWLock returned: " << rc.ToString();
    ASSERT_TRUE(rc.GetCode() == StatusCode::K_TRY_AGAIN);

    // Free the read lock and get the lock with try mode
    objPtr1->RUnlock();
    rc = objPtr1->TryWLock();
    LOG(INFO) << "TryWLock returned: " << rc.ToString();
    ASSERT_TRUE(rc.GetCode() == StatusCode::K_OK);

    // Delete the object and free the write lock
    objPtr1->DeleteObject();
    objPtr1->WUnlock();

    // Now try to get the lock again with try mode
    rc = objPtr1->TryWLock();
    LOG(INFO) << "TryWLock returned: " << rc.ToString();
    ASSERT_TRUE(rc.GetCode() == StatusCode::K_NOT_FOUND);
}

// Test an attempt to get a read lock, do not wait when there is a write lock
TEST_F(SafeObjectTest, TestTryReadLocks)
{
    using SafeString = SafeObject<std::string>;
    auto objPtr1 = std::make_shared<SafeString>("abc");

    // Get the lock first in write mode
    DS_ASSERT_OK(objPtr1->WLock());

    // Now try to get a read lock with try mode
    Status rc = objPtr1->TryRLock();
    LOG(INFO) << "TryRLock returned: " << rc.ToString();
    ASSERT_TRUE(rc.GetCode() == StatusCode::K_TRY_AGAIN);

    // free the write lock and get the lock in read mode
    objPtr1->WUnlock();
    objPtr1->RLock();

    // Then, try to get the read lock with try mode
    rc = objPtr1->TryRLock();
    LOG(INFO) << "TryRLock returned: " << rc.ToString();
    ASSERT_TRUE(rc.GetCode() == StatusCode::K_OK);
    objPtr1->RUnlock();
    objPtr1->RUnlock();

    // Get the lock first in write mode
    DS_ASSERT_OK(objPtr1->WLock());

    // Now try to get a read lock with try mode
    rc = objPtr1->TryRLock();
    LOG(INFO) << "TryRLock returned: " << rc.ToString();
    ASSERT_TRUE(rc.GetCode() == StatusCode::K_TRY_AGAIN);

    // free the write lock and get the lock in read mode
    objPtr1->WUnlock();
    objPtr1->RLock();

    // Then, try to get the read lock with try mode
    rc = objPtr1->TryRLock();
    LOG(INFO) << "TryRLock returned: " << rc.ToString();
    ASSERT_TRUE(rc.GetCode() == StatusCode::K_OK);
    objPtr1->RUnlock();

    // free the read lock and get the write lock to delete the object
    objPtr1->RUnlock();
    objPtr1->WLock();
    objPtr1->DeleteObject();

    // Then, try to get the read lock with try mode
    rc = objPtr1->TryRLock();
    LOG(INFO) << "TryRLock returned: " << rc.ToString();
    ASSERT_TRUE(rc.GetCode() == StatusCode::K_TRY_AGAIN);
}

// Test basic inserts and fetches of the SafeTable
TEST_F(SafeObjectTest, TestTableBasic)
{
    const int numKeys = 3;
    using SafeString = SafeObject<std::string>;
    using StringTable = SafeTable<std::string, std::string>;  // string key, string payload
    StringTable safeTable;

    std::vector<std::string> keys;
    // Create some keys
    for (int i = 0; i < numKeys; ++i) {
        keys.push_back(NewObjectKey());
    }

    std::shared_ptr<SafeString> fetchedObj = nullptr;
    Status rc = safeTable.Get(keys[0], fetchedObj);
    LOG(INFO) << "Get returned: " << rc.ToString();
    ASSERT_TRUE(rc.GetCode() == StatusCode::K_NOT_FOUND);

    // Success insert1 followed by duplicate key
    std::string objData1("hello1");
    DS_ASSERT_OK(safeTable.Insert(keys[0], objData1));
    rc = safeTable.Insert(keys[0], objData1);
    LOG(INFO) << "Insert returned: " << rc.ToString();
    ASSERT_TRUE(rc.GetCode() == StatusCode::K_DUPLICATED);

    // Test contains
    std::string notExistKey("not_a_key");
    DS_ASSERT_OK(safeTable.Contains(keys[0]));
    rc = safeTable.Contains(notExistKey);
    ASSERT_TRUE(rc.GetCode() == StatusCode::K_NOT_FOUND);

    // Success insert2 followed by duplicate key
    auto objData2 = std::make_unique<std::string>("hello2");
    DS_ASSERT_OK(safeTable.Insert(keys[1], std::move(objData2)));
    objData2 = std::make_unique<std::string>("hello2");
    rc = safeTable.Insert(keys[1], std::move(objData2));
    LOG(INFO) << "Insert returned: " << rc.ToString();
    ASSERT_TRUE(rc.GetCode() == StatusCode::K_DUPLICATED);

    // Success insert3 followed by duplicate key
    auto objData3 = std::make_shared<SafeString>("hello3");
    DS_ASSERT_OK(safeTable.Insert(keys[2], objData3));
    rc = safeTable.Insert(keys[2], objData3);
    LOG(INFO) << "Insert returned: " << rc.ToString();
    ASSERT_TRUE(rc.GetCode() == StatusCode::K_DUPLICATED);

    // Successful get
    DS_ASSERT_OK(safeTable.Get(keys[1], fetchedObj));
    LOG(INFO) << "Fetched object: " << *(*fetchedObj);
}

TEST_F(SafeObjectTest, ReserveAndCreate)
{
    using SafeString = SafeObject<std::string>;
    using StringTable = SafeTable<std::string, std::string>;  // string key, string payload
    StringTable safeTable;

    std::string key1 = NewObjectKey();
    std::shared_ptr<SafeString> objPtr = nullptr;
    DS_ASSERT_OK(safeTable.ReserveAndLock(key1, objPtr));

    // The object should already be locked. It's empty, but no-one can use this key now or they will block
    Status rc = objPtr->TryWLock(true);
    LOG(INFO) << "TryWLock returned: " << rc.ToString();
    ASSERT_TRUE(rc.GetCode() == StatusCode::K_TRY_AGAIN);

    // Now, populate data at the reserved slot and then free the lock
    auto testData = std::make_unique<std::string>("hello");
    objPtr->SetRealObject(std::move(testData));
    objPtr->WUnlock();

    objPtr.reset();
    rc = safeTable.ReserveAndLock(key1, objPtr);
    LOG(INFO) << "ReserveAndLock returned: " << rc.ToString();
    ASSERT_TRUE(rc.GetCode() == StatusCode::K_DUPLICATED);
}

TEST_F(SafeObjectTest, TestErase)
{
    using SafeString = SafeObject<std::string>;
    using StringTable = SafeTable<std::string, std::string>;  // string key, string payload
    StringTable safeTable;

    std::string key1 = NewObjectKey();
    Status rc = safeTable.Erase(key1);
    LOG(INFO) << "Erase returned: " << rc.ToString();
    ASSERT_TRUE(rc.GetCode() == StatusCode::K_NOT_FOUND);

    std::string objData1("hello1");
    DS_ASSERT_OK(safeTable.Insert(key1, objData1));

    // Get a shared pointer to the object
    std::shared_ptr<SafeString> fetchedObj = nullptr;
    DS_ASSERT_OK(safeTable.Get(key1, fetchedObj));

    // Remove the object from the safe table, but we are still holding a shared pointer to it
    DS_ASSERT_OK(safeTable.Erase(key1));
    ASSERT_EQ(safeTable.GetSize(), 0ul);

    rc = fetchedObj->WLock();  // expect fail, because something else deleted the real data from it
    ASSERT_TRUE(rc.GetCode() == StatusCode::K_NOT_FOUND);

    DS_ASSERT_OK(safeTable.Insert(key1, objData1));

    fetchedObj = nullptr;
    DS_ASSERT_OK(safeTable.Get(key1, fetchedObj));

    DS_ASSERT_OK(safeTable.Erase(key1, *fetchedObj));
    ASSERT_EQ(safeTable.GetSize(), 0ul);

    rc = fetchedObj->WLock();
    ASSERT_TRUE(rc.GetCode() == StatusCode::K_NOT_FOUND);
}

TEST_F(SafeObjectTest, TestIterate1)
{
    const int numKeys = 3;
    using StringTable = SafeTable<std::string, std::string>;  // string key, string payload
    StringTable safeTable;

    // Insert some data
    std::string objData("hello");
    for (int i = 0; i < numKeys; ++i) {
        DS_ASSERT_OK(safeTable.Insert(NewObjectKey(), objData));
    }

    for (auto &kv : safeTable) {
        LOG(INFO) << "key: " << kv.first << " data: " << *(*kv.second);
    }
}

TEST_F(SafeObjectTest, TestIterate2)
{
    const int numKeys = 3;
    using StringTable = SafeTable<std::string, std::string>;  // string key, string payload
    StringTable safeTable;
    numReaderThreads_ = 2;

    std::vector<std::string> keys;
    std::string objData("hello");
    // Create some keys and insert data
    for (int i = 0; i < numKeys; ++i) {
        keys.push_back(NewObjectKey());
        DS_ASSERT_OK(safeTable.Insert(keys[i], objData));
    }

    // Set up an iterator.  This will get a wlock
    // The iterator is scoped because it holds the table hostScope it so that it destroys when we are done
    {
        StringTable::Iterator iter = std::begin(safeTable);

        // Launch some threads and try to do more inserts..should block on the table lock from the iterator
        for (int threadId = 0; threadId < numReaderThreads_; ++threadId) {
            readerThreads_.push_back(std::thread([this, threadId, &safeTable, &keys]() {
                std::string objData("test");
                std::string newKey = NewObjectKey();
                LOG(INFO) << "Try to insert. Should lock on the table lock because iteration in progress.";
                DS_ASSERT_OK(safeTable.Insert(newKey, objData));
                LOG(INFO) << "Insert done now";
            }));
        }

        // Iterate the table
        std::this_thread::sleep_for(std::chrono::seconds(2));  // pause here.  iterator is holding the lock currently
        while (iter != std::end(safeTable)) {
            iter->second->RLock();
            LOG(INFO) << "key: " << iter->first << " data: " << *(*iter->second);
            iter->second->RUnlock();
            std::this_thread::sleep_for(std::chrono::seconds(2));  // we are slow to iterate
            ++iter;
        }
    }

    JoinThreads();
}

TEST_F(SafeObjectTest, TestBaseObj)
{
    using SafeBase = SafeObject<BaseObj>;
    using BaseTable = SafeTable<std::string, BaseObj>;  // string key, base object payload
    BaseTable safeTable;
    std::string key1 = NewObjectKey();

    // base ptr of a derived class
    std::unique_ptr<BaseObj> baseObj = std::make_unique<DerivedObj>(13, 99);
    DS_ASSERT_OK(safeTable.Insert(key1, std::move(baseObj)));

    // Get a shared pointer to the object
    std::shared_ptr<SafeBase> fetchedObj = nullptr;
    DS_ASSERT_OK(safeTable.Get(key1, fetchedObj));

    // Polymorphic call through base class pointer
    fetchedObj->Get()->print();

    // Explicit cast to derived type first, then call derived-only method
    DerivedObj *childClass = SafeBase::GetDerived<DerivedObj>(*fetchedObj);
    childClass->notVirtualFunction();
}

TEST_F(SafeObjectTest, TestIsWLockedByCurrentThread)
{
    using SafeString = SafeObject<std::string>;
    auto objPtr = std::make_shared<SafeString>("abc");

    ThreadPool threadPool(2);
    auto fut = threadPool.Submit([objPtr]() {
        DS_ASSERT_OK(objPtr->WLock());
        ASSERT_EQ(objPtr->IsWLockedByCurrentThread(), true);
        objPtr->WUnlock();
        DS_ASSERT_OK(objPtr->RLock());
        usleep(50000);
        ASSERT_EQ(objPtr->IsWLockedByCurrentThread(), false);
        objPtr->RUnlock();
    });
    auto fut2 = threadPool.Submit([objPtr]() {
        usleep(25000);
        DS_ASSERT_OK(objPtr->WLock());
        ASSERT_EQ(objPtr->IsWLockedByCurrentThread(), true);
        objPtr->WUnlock();
    });

    fut.get();
    fut2.get();
}

TEST_F(SafeObjectTest, TestReadWriteLock)
{
    using SafeBase = SafeObject<BaseObj>;
    using BaseTable = SafeTable<ImmutableString, BaseObj>;  // string key, base object payload
    BaseTable safeTable;
    std::string key = NewObjectKey();
    ImmutableStringPool::Instance().Init();
    intern::StringPool::InitAll();

    std::atomic<bool> running{ true };
    const int printBatch = 100;
    const int threadCount = 3;
    const int delayMs = 10;
    ThreadPool threadPool(threadCount);
    auto f1 = threadPool.Submit([&safeTable, &running, delayMs] {
        int64_t c1 = 0;
        while (running) {
            std::vector<std::string> list;
            for (const auto &item : safeTable) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
                list.emplace_back(item.first);
            }
            c1++;
            if (c1 % printBatch == 0) {
                LOG(INFO) << "iterator loop:" << c1;
                std::this_thread::sleep_for(std::chrono::milliseconds(delayMs));
            }
        }
    });

    auto f2 = threadPool.Submit([&safeTable, &running, &key, delayMs] {
        int64_t c2 = 0;
        while (running) {
            std::shared_ptr<SafeBase> safeObj;
            bool isInsert;
            Status rc = safeTable.ReserveGetAndLock(key, safeObj, isInsert);
            if (rc.IsError()) {
                continue;
            }
            Raii raii([&safeObj] { safeObj->WUnlock(); });
            c2++;
            if (c2 % printBatch == 0) {
                LOG(INFO) << "ReserveGetAndLock loop:" << c2;
                std::this_thread::sleep_for(std::chrono::milliseconds(delayMs));
            }
        }
    });

    auto f3 = threadPool.Submit([&safeTable, &running, &key, delayMs] {
        int64_t c3 = 0;
        while (running) {
            std::shared_ptr<SafeBase> safeObj;
            Status rc = safeTable.Get(key, safeObj);
            if (rc.IsError()) {
                continue;
            }
            rc = safeObj->WLock(true);
            if (rc.IsError()) {
                continue;
            }
            Raii raii([&safeObj] { safeObj->WUnlock(); });
            rc = safeTable.Erase(key, *safeObj);
            ASSERT_TRUE(rc.IsOk());
            c3++;
            if (c3 % printBatch == 0) {
                LOG(INFO) << "Get and erase loop:" << c3;
                std::this_thread::sleep_for(std::chrono::milliseconds(delayMs));
            }
        }
    });
    const int testTime = 10000;  // 10s
    std::this_thread::sleep_for(std::chrono::milliseconds(testTime));
    running = false;
    f1.get();
    f2.get();
    f3.get();
}
}  // namespace ut
}  // namespace datasystem
