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
 * Description: ObjRefTable test.
 */

#include <tbb/concurrent_hash_map.h>
#include <chrono>
#include <thread>

#include "ut/common.h"
#include "datasystem/common/object_cache/object_ref_info.h"
#include "datasystem/common/string_intern/string_ref.h"
#include "datasystem/common/util/random_data.h"
#include "datasystem/common/object_cache/safe_table.h"
#include "datasystem/common/util/thread_pool.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/worker/object_cache/obj_cache_shm_unit.h"
#include "../common/binmock/binmock.h"

namespace datasystem {
namespace ut {
using namespace ::testing;

template <typename F>
void ParallelFor(size_t numOfOps, F f, size_t numOfThreads)
{
    ThreadPool pool(numOfThreads);
    std::vector<std::future<void>> futures;
    for (size_t i = 0; i < numOfThreads; i++) {
        futures.emplace_back(pool.Submit([f, i, numOfOps, numOfThreads]() {
            size_t avg = numOfOps / numOfThreads + 1;
            size_t beg = avg * i;
            size_t end = std::min(avg * (i + 1), numOfOps);
            for (size_t j = beg; j < end; j++) {
                f(j);
            }
        }));
    }
    for (auto &fut : futures) {
        fut.get();
    }
}

class ObjRefTableTest : public CommonTest {
public:
    using ObjectRefInfo = datasystem::object_cache::ObjectRefInfo<std::string>;
    void SetUp() override
    {
        clientIds_.clear();
        objKeys_.clear();
        refClientSets_.clear();
    }

    std::vector<std::string> GenRandomStrs(size_t dataSz, size_t arrSz);

    std::vector<std::pair<uint32_t, uint32_t>> GenMemRefIndexes(size_t numOfClients, size_t numOfObjs);

    std::vector<std::pair<uint32_t, std::vector<uint32_t>>> GenGloalRefIndexes(size_t numOfClients, size_t numOfObjs);

    std::vector<std::string> GetGlobalRefIds(const std::vector<uint32_t> &objIndexes);

    void VerifyMemRefTableMatches();

    void TestMemRefTableUniqAdd();

    void TestMemRefTableUniqRemove();

    void VerifyGlobalRefTableMatches();

    void TestGlobalRefTableAdd();

    void GlobalRefRemoveObjKeys(const std::string &clientId, const std::vector<std::string> &objKeys);

    void TestGlobalRefTableRemove();

protected:
    std::unordered_map<std::string, std::unordered_set<std::string>> refClientSets_;

    std::vector<ClientKey> clientIds_;
    std::vector<std::string> objKeys_;

    using MemRefTable = datasystem::object_cache::SharedMemoryRefTable;
    using GlobalRefTable = datasystem::object_cache::ObjectGlobalRefTable<ImmutableString>;
    using SafeObjType = datasystem::SafeObject<datasystem::ObjectInterface>;
    using ObjectTable = datasystem::SafeTable<std::string, datasystem::ObjectInterface>;

    // MemRefTable Test Related Tables.
    ObjectTable objectTable_;
    MemRefTable memRefTable_;

    // MemRefTable Test Related Indexes.
    std::vector<std::pair<uint32_t, uint32_t>> addOpsIndexes_;
    std::vector<std::pair<uint32_t, uint32_t>> rmOpsIndexes_;

    // GlobalRefTable Test Related Table.
    GlobalRefTable gRefTable_;

    // GlobalRefTable Test Related Indexes.
    std::vector<std::pair<uint32_t, std::vector<uint32_t>>> batchAddOpsIndexes_;
    std::vector<std::pair<uint32_t, std::vector<uint32_t>>> batchRmOpsIndexes_;

    size_t minNumOfAddRmOps_ = 1;
    size_t maxNumOfAddRmOps_ = 10;

    size_t numOfThreads_ = 4;
    size_t numOfClients_ = 32;
    size_t numOfObjs_ = 55;
    size_t idSz_ = 16;
    size_t numOfOps_ = numOfClients_ * numOfObjs_ * 4;

    RandomData randomData_;
};

std::vector<std::string> ObjRefTableTest::GenRandomStrs(size_t dataSz, size_t arrSz)
{
    std::vector<std::string> strs;
    strs.reserve(arrSz);
    for (size_t i = 0; i < arrSz; i++) {
        strs.emplace_back(randomData_.GetRandomString(dataSz));
    }
    return strs;
}

std::vector<std::pair<uint32_t, uint32_t>> ObjRefTableTest::GenMemRefIndexes(size_t numOfClients, size_t numOfObjs)
{
    std::vector<std::pair<uint32_t, uint32_t>> indexes;
    indexes.reserve(numOfOps_);
    for (size_t i = 0; i < numOfOps_; i++) {
        auto clientIndex = randomData_.GetRandomUint32() % numOfClients;
        auto objIndex = randomData_.GetRandomUint32() % numOfObjs;
        indexes.emplace_back(clientIndex, objIndex);
    }
    return indexes;
}

std::vector<std::pair<uint32_t, std::vector<uint32_t>>> ObjRefTableTest::GenGloalRefIndexes(size_t numOfClients,
                                                                                            size_t numOfObjs)
{
    std::vector<std::pair<uint32_t, std::vector<uint32_t>>> indexes;
    indexes.reserve(numOfOps_);
    for (size_t i = 0; i < numOfOps_; i++) {
        auto clientIndex = randomData_.GetRandomUint32() % numOfClients;
        auto choiceSize = randomData_.GetRandomUint32(minNumOfAddRmOps_, maxNumOfAddRmOps_);

        std::vector<uint32_t> choices;
        for (size_t j = 0; j < choiceSize; j++) {
            choices.emplace_back(randomData_.GetRandomUint32() % numOfObjs);
        }
        indexes.emplace_back(clientIndex, std::move(choices));
    }
    return indexes;
}

std::vector<std::string> ObjRefTableTest::GetGlobalRefIds(const std::vector<uint32_t> &objIndexes)
{
    std::vector<std::string> objKeys;
    objKeys.reserve(objIndexes.size());
    std::transform(std::begin(objIndexes), std::end(objIndexes), std::back_inserter(objKeys),
                   [this](uint32_t idx) { return objKeys_[idx]; });
    return objKeys;
}

void ObjRefTableTest::VerifyMemRefTableMatches()
{
    // Client to objects.
    std::unordered_map<std::string, std::unordered_set<ShmKey>> objects;

    // Verify Object Table and Ref Cnt.
    for (const auto &objKeyClients : refClientSets_) {
        const auto &objKey = objKeyClients.first;
        const auto &clientSet = objKeyClients.second;

        std::shared_ptr<SafeObjType> entry;
        DS_ASSERT_OK(objectTable_.Get(objKey, entry));
        DS_ASSERT_OK(entry->RLock());
        ASSERT_TRUE(entry->Get() != nullptr);
        ASSERT_TRUE((*entry)->GetShmUnit() != nullptr);
        ASSERT_EQ((*entry)->GetShmUnit()->GetRefCount(), static_cast<int>(clientSet.size()));
        entry->RUnlock();
        for (const auto &client : clientSet) {
            ASSERT_TRUE(memRefTable_.Contains(ClientKey::Intern(client), ShmKey::Intern(objKey)));
            objects[client].emplace(ShmKey::Intern(objKey));
        }
    }

    // Verify Client Table.
    for (const auto &clientObjects : objects) {
        const auto &clientId = clientObjects.first;
        const auto &objectSet = clientObjects.second;
        std::vector<ShmKey> objKeys;
        memRefTable_.GetClientRefIds(ClientKey::Intern(clientId), objKeys);
        ASSERT_EQ(objKeys.size(), objectSet.size());
        for (const auto &objKey : objKeys) {
            ASSERT_TRUE(objectSet.count(objKey) > 0);
        }
    }
}

void ObjRefTableTest::TestMemRefTableUniqAdd()
{
    ParallelFor(
        numOfOps_,
        [this](size_t j) {
            size_t clientIndex, objIndex;
            std::tie(clientIndex, objIndex) = addOpsIndexes_[j];
            std::shared_ptr<SafeObjType> entry;
            bool isInsert;
            DS_ASSERT_OK(objectTable_.ReserveGetAndLock(objKeys_[objIndex], entry, isInsert));
            if (entry->Get() == nullptr) {
                std::unique_ptr<datasystem::ObjectInterface> objPtr = nullptr;
                // Set ShmUnit.
                auto shmUnit = std::make_shared<ShmUnit>();
                shmUnit->id = ShmKey::Intern(objKeys_[objIndex]);
                auto objShmUnit = std::make_unique<datasystem::object_cache::ObjCacheShmUnit>();
                objShmUnit->SetShmUnit(shmUnit);
                entry->SetRealObject(std::move(objShmUnit));
            }
            auto shmUnit = (*entry)->GetShmUnit();
            memRefTable_.AddShmUnit(clientIds_[clientIndex], shmUnit);
            entry->WUnlock();
        },
        numOfThreads_);

    // Verify atomic count matches semantics of uniq-add.
    for (size_t i = 0; i < numOfOps_; i++) {
        size_t clientIndex, objIndex;
        std::tie(clientIndex, objIndex) = addOpsIndexes_[i];
        refClientSets_[objKeys_[objIndex]].emplace(clientIds_[clientIndex]);
    }

    // Verify Matches.
    VerifyMemRefTableMatches();
}

void ObjRefTableTest::TestMemRefTableUniqRemove()
{
    ParallelFor(
        numOfOps_,
        [this](size_t j) {
            size_t clientIndex, objIndex;
            std::tie(clientIndex, objIndex) = rmOpsIndexes_[j];
            std::shared_ptr<SafeObjType> entry;
            bool isInsert;
            DS_ASSERT_OK(objectTable_.ReserveGetAndLock(objKeys_[objIndex], entry, isInsert));
            if (entry->Get() == nullptr) {
                std::unique_ptr<datasystem::ObjectInterface> objPtr = nullptr;
                auto objShmUnit = std::make_unique<datasystem::object_cache::ObjCacheShmUnit>();
                entry->SetRealObject(std::move(objShmUnit));
            }
            memRefTable_.RemoveShmUnit(clientIds_[clientIndex], ShmKey::Intern(objKeys_[objIndex]));
            entry->WUnlock();
        },
        numOfThreads_);

    for (size_t i = 0; i < numOfOps_; i++) {
        size_t clientIndex, objIndex;
        std::tie(clientIndex, objIndex) = rmOpsIndexes_[i];
        const auto &objKey = objKeys_[objIndex];
        const auto &clientId = clientIds_[clientIndex];
        auto it = refClientSets_.find(objKey);
        if (it != refClientSets_.end()) {
            it->second.erase(clientId);
            if (it->second.empty()) {
                refClientSets_.erase(it);
            }
        }
    }

    // Verify Matches.
    VerifyMemRefTableMatches();
}

void ObjRefTableTest::VerifyGlobalRefTableMatches()
{
    // Verify Ref Cnts.
    std::vector<std::string> groundTruthIds;
    groundTruthIds.reserve(refClientSets_.size());
    std::transform(
        std::begin(refClientSets_), std::end(refClientSets_), std::back_inserter(groundTruthIds),
        [](const std::pair<std::string, std::unordered_set<std::string>> &objClients) { return objClients.first; });
    std::vector<uint32_t> refCnts;
    gRefTable_.GetRefWorkerCounts(groundTruthIds, refCnts);

    // Compare container equality later.
    std::unordered_map<std::string, std::unordered_set<std::string>> refTable;
    gRefTable_.GetAllRef(refTable);

    size_t idx = 0;
    std::unordered_map<std::string, std::unordered_set<std::string>> objects;
    for (const auto &objKeyClients : refClientSets_) {
        const auto &objKey = objKeyClients.first;
        const auto &clientSet = objKeyClients.second;

        ASSERT_EQ(gRefTable_.GetRefWorkerCount(objKey), clientSet.size());

        // Verify correctness of gRefTable_.GetRefWorkerCounts.
        ASSERT_EQ(refCnts[idx], clientSet.size());
        idx++;

        // Compare container equality.
        auto it = refTable.find(objKey);
        ASSERT_TRUE(it != refTable.end());
        auto &clients = it->second;
        ASSERT_EQ(clients, clientSet);

        for (const auto &client : clientSet) {
            objects[client].emplace(objKey);
        }
    }

    // Verify reverse index.
    for (const auto &clientObjects : objects) {
        const auto &clientId = clientObjects.first;
        const auto &objectSet = clientObjects.second;

        std::vector<std::string> objKeys;
        gRefTable_.GetClientRefIds(ClientKey::Intern(clientId), objKeys);
        ASSERT_EQ(objKeys.size(), objectSet.size());
        for (const auto &objKey : objKeys) {
            ASSERT_TRUE(objectSet.count(objKey) > 0);
        }
    }
}

void ObjRefTableTest::TestGlobalRefTableAdd()
{
    ParallelFor(
        numOfOps_,
        [this](size_t j) {
            size_t clientIndex = batchAddOpsIndexes_[j].first;
            auto objKeys = GetGlobalRefIds(batchAddOpsIndexes_[j].second);

            std::vector<std::string> failedIds;
            std::vector<std::string> firstIds;
            gRefTable_.GIncreaseRef(clientIds_[clientIndex], objKeys, failedIds, firstIds);
        },
        numOfThreads_);

    // Verify atomic count matches semantics of uniq-add.
    for (size_t i = 0; i < numOfOps_; i++) {
        size_t clientIndex = batchAddOpsIndexes_[i].first;
        auto objKeys = GetGlobalRefIds(batchAddOpsIndexes_[i].second);
        for (const auto &objKey : objKeys) {
            refClientSets_[objKey].emplace(clientIds_[clientIndex]);
        }
    }

    VerifyGlobalRefTableMatches();
}

void ObjRefTableTest::GlobalRefRemoveObjKeys(const std::string &clientId, const std::vector<std::string> &objKeys)
{
    for (const auto &objKey : objKeys) {
        auto it = refClientSets_.find(objKey);
        if (it != refClientSets_.end()) {
            it->second.erase(clientId);
            if (it->second.empty()) {
                refClientSets_.erase(it);
            }
        }
    }
}

void ObjRefTableTest::TestGlobalRefTableRemove()
{
    ParallelFor(
        numOfOps_,
        [this](size_t j) {
            size_t clientIndex = batchRmOpsIndexes_[j].first;
            auto objKeys = GetGlobalRefIds(batchRmOpsIndexes_[j].second);

            std::vector<std::string> failedIds;
            std::vector<std::string> firstIds;
            gRefTable_.GDecreaseRef(clientIds_[clientIndex], objKeys, failedIds, firstIds);
        },
        numOfThreads_);

    for (size_t i = 0; i < numOfOps_; i++) {
        size_t clientIndex = batchAddOpsIndexes_[i].first;
        const auto &clientId = clientIds_[clientIndex];
        auto objKeys = GetGlobalRefIds(batchAddOpsIndexes_[i].second);
        GlobalRefRemoveObjKeys(clientId, objKeys);
    }

    VerifyGlobalRefTableMatches();
}

TEST_F(ObjRefTableTest, ObjRefInfoUniqBranchTest)
{
    auto clientInfo = std::make_shared<ObjectRefInfo>();
    size_t dataSz = 32;
    auto id = randomData_.GetRandomString(dataSz);
    auto id2 = randomData_.GetRandomString(dataSz);
    ASSERT_EQ(clientInfo->RemoveRef(id), false);

    ASSERT_EQ(clientInfo->AddRef(id), true);
    ASSERT_EQ(clientInfo->AddRef(id), false);

    ASSERT_EQ(clientInfo->Contains(id), true);
    ASSERT_EQ(clientInfo->CheckIsNoneRef(id), false);
    ASSERT_EQ(clientInfo->CheckIsNoneRef(id2), true);
    ASSERT_EQ(clientInfo->CheckIsRefIdsEmpty(), false);

    // Test clientInfo->GetRefIds.
    ASSERT_EQ(clientInfo->AddRef(id2), true);
    std::vector<std::string> objectKeys;
    clientInfo->GetRefIds(objectKeys);
    std::vector<std::string> groundTruth = { id, id2 };
    ASSERT_EQ(groundTruth.size(), objectKeys.size());
    std::sort(begin(objectKeys), end(objectKeys));
    std::sort(begin(groundTruth), end(groundTruth));
    for (size_t i = 0; i < objectKeys.size(); i++) {
        ASSERT_TRUE(objectKeys[i] == groundTruth[i]);
    }

    ASSERT_EQ(clientInfo->RemoveRef(id), true);
    ASSERT_EQ(clientInfo->RemoveRef(id), false);

    ASSERT_EQ(clientInfo->Contains(id), false);
    ASSERT_EQ(clientInfo->RemoveRef(id2), true);

    // Test whether container is empty.
    ASSERT_EQ(clientInfo->CheckIsRefIdsEmpty(), true);
    objectKeys.clear();
    clientInfo->GetRefIds(objectKeys);
    ASSERT_TRUE(objectKeys.empty());
}

TEST_F(ObjRefTableTest, ObjRefInfoRefCntBranchTest)
{
    auto clientInfo = std::make_shared<ObjectRefInfo>(false);
    size_t dataSz = 32;
    auto id = randomData_.GetRandomString(dataSz);

    size_t limit = 50;
    size_t num = randomData_.GetRandomUint32() % limit;
    for (size_t i = 0; i < num; i++) {
        ASSERT_EQ(clientInfo->AddRef(id), true);
    }
    for (size_t i = 0; i < num; i++) {
        ASSERT_EQ(clientInfo->RemoveRef(id), true);
    }
    ASSERT_EQ(clientInfo->RemoveRef(id), false);
}

TEST_F(ObjRefTableTest, ObjRefInfoRefCntMultiIdMultiThread)
{
    auto clientInfo = std::make_shared<ObjectRefInfo>(false);
    int threadNum = 8;
    ThreadPool threadPool(threadNum);
    std::vector<std::future<void>> futures;
    for (int i = 0; i < threadNum; i++) {
        futures.emplace_back(threadPool.Submit([&clientInfo]() {
            for (int loop = 0; loop < 10000; loop++) {
                RandomData randomData_;
                auto objectKey = randomData_.GetRandomString(32);
                size_t limit = 10;
                size_t num = randomData_.GetRandomUint32() % limit + 1;
                for (size_t i = 0; i < num; i++) {
                    ASSERT_EQ(clientInfo->AddRef(objectKey), true);
                    ASSERT_EQ(clientInfo->Contains(objectKey), true);
                    ASSERT_EQ(clientInfo->CheckIsRefIdsEmpty(), false);
                    ASSERT_EQ(clientInfo->CheckIsNoneRef(objectKey), false);
                }
                std::vector<std::string> objectKeys;
                clientInfo->GetRefIds(objectKeys);
                for (size_t i = 0; i < num; i++) {
                    ASSERT_EQ(clientInfo->RemoveRef(objectKey), true);
                }
                ASSERT_EQ(clientInfo->Contains(objectKey), false);
                ASSERT_EQ(clientInfo->RemoveRef(objectKey), false);
                ASSERT_EQ(clientInfo->CheckIsNoneRef(objectKey), true);
            }
        }));
    }
    for (auto &future : futures) {
        future.get();
    }
    ASSERT_EQ(clientInfo->CheckIsRefIdsEmpty(), true);
}

TEST_F(ObjRefTableTest, ObjRefInfoRefCntOneIdMultiThread)
{
    auto clientInfo = std::make_shared<ObjectRefInfo>(false);
    int threadNum = 8;
    ThreadPool threadPool(threadNum);
    std::vector<std::future<void>> futures;
    auto objectKey = randomData_.GetRandomString(32);
    for (int i = 0; i < threadNum; i++) {
        futures.emplace_back(threadPool.Submit([&clientInfo, objectKey]() {
            for (int loop = 0; loop < 10000; loop++) {
                RandomData randomData_;
                size_t limit = 10;
                size_t num = randomData_.GetRandomUint32() % limit + 1;
                for (size_t i = 0; i < num; i++) {
                    ASSERT_EQ(clientInfo->AddRef(objectKey), true);
                    ASSERT_EQ(clientInfo->Contains(objectKey), true);
                    ASSERT_EQ(clientInfo->CheckIsRefIdsEmpty(), false);
                    ASSERT_EQ(clientInfo->CheckIsNoneRef(objectKey), false);
                }
                std::vector<std::string> objectKeys;
                clientInfo->GetRefIds(objectKeys);
                ASSERT_TRUE(objectKeys[0] == objectKey);
                ASSERT_TRUE(objectKeys.size() == 1);
                for (size_t i = 0; i < num; i++) {
                    ASSERT_EQ(clientInfo->RemoveRef(objectKey), true);
                }
            }
        }));
    }
    for (auto &future : futures) {
        future.get();
    }
    ASSERT_EQ(clientInfo->Contains(objectKey), false);
    ASSERT_EQ(clientInfo->RemoveRef(objectKey), false);
    ASSERT_EQ(clientInfo->CheckIsNoneRef(objectKey), true);
    ASSERT_EQ(clientInfo->CheckIsRefIdsEmpty(), true);
}

TEST_F(ObjRefTableTest, ObjRefInfoRefCntMultiIdMultiThread2)
{
    auto clientInfo = std::make_shared<ObjectRefInfo>(true);
    int threadNum = 8;
    ThreadPool threadPool(threadNum);
    std::vector<std::future<void>> futures;
    for (int i = 0; i < threadNum; i++) {
        futures.emplace_back(threadPool.Submit([&clientInfo]() {
            for (int loop = 0; loop < 10000; loop++) {
                RandomData randomData_;
                auto objectKey = randomData_.GetRandomString(32);
                ASSERT_EQ(clientInfo->AddRef(objectKey), true);
                ASSERT_EQ(clientInfo->AddRef(objectKey), false);
                ASSERT_EQ(clientInfo->Contains(objectKey), true);
                ASSERT_EQ(clientInfo->CheckIsRefIdsEmpty(), false);
                ASSERT_EQ(clientInfo->CheckIsNoneRef(objectKey), false);
                ASSERT_EQ(clientInfo->UpdateRefCount(objectKey, 1).GetCode(), StatusCode::K_OK);
                ASSERT_EQ(clientInfo->UpdateRefCount(objectKey, 2).GetCode(), StatusCode::K_DUPLICATED);
                ASSERT_EQ(clientInfo->GetRefCount(objectKey), 1);
                std::vector<std::string> objectKeys;
                clientInfo->GetRefIds(objectKeys);
                ASSERT_TRUE(std::find(objectKeys.begin(), objectKeys.end(), objectKey) != objectKeys.end());
                ASSERT_EQ(clientInfo->RemoveRef(objectKey), true);
                ASSERT_EQ(clientInfo->RemoveRef(objectKey), false);
                ASSERT_EQ(clientInfo->Contains(objectKey), false);
                ASSERT_EQ(clientInfo->CheckIsNoneRef(objectKey), true);
            }
        }));
    }
    for (auto &future : futures) {
        future.get();
    }
    ASSERT_EQ(clientInfo->CheckIsRefIdsEmpty(), true);
}

TEST_F(ObjRefTableTest, ObjRefInfoRefCntOneIdMultiThread2)
{
    auto clientInfo = std::make_shared<ObjectRefInfo>(true);
    int threadNum = 8;
    ThreadPool threadPool(threadNum);
    std::vector<std::future<void>> futures;
    auto objectKey = randomData_.GetRandomString(32);
    for (int i = 0; i < threadNum; i++) {
        futures.emplace_back(threadPool.Submit([&clientInfo, objectKey]() {
            for (int loop = 0; loop < 10000; loop++) {
                clientInfo->AddRef(objectKey);
                clientInfo->Contains(objectKey);
                clientInfo->CheckIsRefIdsEmpty();
                clientInfo->CheckIsNoneRef(objectKey);
                clientInfo->UpdateRefCount(objectKey, 1);
                std::vector<std::string> objectKeys;
                clientInfo->GetRefIds(objectKeys);
                clientInfo->RemoveRef(objectKey);
            }
        }));
    }
    for (auto &future : futures) {
        future.get();
    }
    std::vector<std::string> objectKeys;
    clientInfo->GetRefIds(objectKeys);
    ASSERT_TRUE(objectKeys.size() == 0);
    ASSERT_EQ(clientInfo->Contains(objectKey), false);
    ASSERT_EQ(clientInfo->RemoveRef(objectKey), false);
    ASSERT_EQ(clientInfo->CheckIsNoneRef(objectKey), true);
    ASSERT_EQ(clientInfo->CheckIsRefIdsEmpty(), true);
}

TEST_F(ObjRefTableTest, MemRefTableUniqAddRmTest)
{
    // ClientIds and ObjKeys.
    auto tmp = this->GenRandomStrs(idSz_, numOfClients_ * 2);
    for (auto it = tmp.begin(); it != tmp.end(); it++) {
        clientIds_.emplace_back(ClientKey::Intern(*it));
    }
    objKeys_ = this->GenRandomStrs(idSz_, numOfObjs_ * 2);

    // Add Ops.
    addOpsIndexes_ = GenMemRefIndexes(numOfClients_, numOfObjs_);

    // Remove Ops.
    rmOpsIndexes_ = GenMemRefIndexes(clientIds_.size(), objKeys_.size());

    // Test Multiple clients and objects add.
    TestMemRefTableUniqAdd();

    // Test Multiple clients and objects remove.
    TestMemRefTableUniqRemove();

    // Test obj-level removal of all clients.
    TestMemRefTableUniqAdd();
    rmOpsIndexes_ = GenMemRefIndexes(clientIds_.size(), objKeys_.size());
}

TEST_F(ObjRefTableTest, GlobalRefTableAddRmTest)
{
    // ClientIds and ObjKeys.
    auto tmp = this->GenRandomStrs(idSz_, numOfClients_ * 2);
    for (auto it = tmp.begin(); it != tmp.end(); it++) {
        clientIds_.emplace_back(ClientKey::Intern(*it));
    }
    objKeys_ = this->GenRandomStrs(idSz_, numOfObjs_ * 2);

    // Batch add and remove ops.
    batchAddOpsIndexes_ = this->GenGloalRefIndexes(numOfClients_, numOfObjs_);
    batchRmOpsIndexes_ = this->GenGloalRefIndexes(clientIds_.size(), objKeys_.size());

    // Test Multiple clients and objects add.
    TestGlobalRefTableAdd();

    // Test Multiple clients and objects remove.
    TestGlobalRefTableRemove();
}

TEST_F(ObjRefTableTest, RemoveClientAndDecreaseShmUnit)
{
    std::vector<ShmKey> shmIds;
    auto clientId = ClientKey::Intern(GetStringUuid());
    for (int i = 0; i < 3000; i++) {  // id num is 3000
        std::shared_ptr<SafeObjType> entry;
        bool isInsert;
        auto objId = GetStringUuid();
        DS_ASSERT_OK(objectTable_.ReserveGetAndLock(objId, entry, isInsert));
        if (entry->Get() == nullptr) {
            std::unique_ptr<datasystem::ObjectInterface> objPtr = nullptr;
            // Set ShmUnit.
            auto shmUnit = std::make_shared<ShmUnit>();
            shmUnit->id = ShmKey::Intern(objId);
            auto objShmUnit = std::make_unique<datasystem::object_cache::ObjCacheShmUnit>();
            objShmUnit->SetShmUnit(shmUnit);
            entry->SetRealObject(std::move(objShmUnit));
            shmIds.emplace_back(ShmKey::Intern(objId));
        }
        auto shmUnit = (*entry)->GetShmUnit();
        memRefTable_.AddShmUnit(clientId, shmUnit);
        entry->WUnlock();
    }
    datasystem::inject::Set("RemoveShmUnit", "sleep(500)");
    std::thread t1([this, &clientId] { memRefTable_.RemoveClient(clientId); });

    std::thread t2([this, &shmIds, &clientId] {
        auto num = 0;
        for (auto &id : shmIds) {
            LOG(INFO) << id << "-----------" << num++;
            memRefTable_.RemoveShmUnit(clientId, id);
        }
    });

    t1.join();
    t2.join();
}

TEST_F(ObjRefTableTest, ReconcileClientShmRefsGetMaybeExpiredShmIds)
{
    const uint64_t fakeTickStep = 1000UL;
    uint64_t currentTimeMs = 0;
    ASSERT_TRUE(inject::Set("shm_ref.GetCurrentTimeMs", FormatString("1*return(1000)->1*return(3000)->abort()")));

    auto clientId = ClientKey::Intern(GetStringUuid());
    auto shmId1 = ShmKey::Intern(GetStringUuid());
    auto shmId2 = ShmKey::Intern(GetStringUuid());
    auto shmUnit1 = std::make_shared<ShmUnit>();  // expired at 2000
    auto shmUnit2 = std::make_shared<ShmUnit>();  // expired at 4000
    shmUnit1->id = shmId1;
    shmUnit2->id = shmId2;

    memRefTable_.AddShmUnit(clientId, shmUnit1);
    memRefTable_.AddShmUnit(clientId, shmUnit2);

    std::vector<ShmKey> maybeExpiredShmIds;
    currentTimeMs += fakeTickStep;
    currentTimeMs += fakeTickStep;
    memRefTable_.FlushMaybeExpiredQueue(currentTimeMs);  // flush at 3000
    memRefTable_.ReconcileClientShmRefs(clientId, {}, maybeExpiredShmIds);

    ASSERT_TRUE(memRefTable_.Contains(clientId, shmId1));
    ASSERT_TRUE(memRefTable_.Contains(clientId, shmId2));
    std::unordered_set<ShmKey> maybeSet(maybeExpiredShmIds.begin(), maybeExpiredShmIds.end());
    ASSERT_EQ(maybeSet.count(shmId1), 1);
    ASSERT_EQ(maybeSet.count(shmId2), 0);

    currentTimeMs += fakeTickStep;
    currentTimeMs += fakeTickStep;
    memRefTable_.FlushMaybeExpiredQueue(currentTimeMs);  // flush at 5000
    memRefTable_.ReconcileClientShmRefs(clientId, maybeExpiredShmIds, maybeExpiredShmIds);
    ASSERT_FALSE(memRefTable_.Contains(clientId, shmId1));
    ASSERT_TRUE(memRefTable_.Contains(clientId, shmId2));

    std::unordered_set<ShmKey> maybeSet2(maybeExpiredShmIds.begin(), maybeExpiredShmIds.end());
    ASSERT_EQ(maybeSet2.count(shmId1), 0);
    ASSERT_EQ(maybeSet2.count(shmId2), 1);

    currentTimeMs += fakeTickStep;
    memRefTable_.FlushMaybeExpiredQueue(currentTimeMs);  // flush at 6000
    memRefTable_.ReconcileClientShmRefs(clientId, maybeExpiredShmIds, maybeExpiredShmIds);
    ASSERT_FALSE(memRefTable_.Contains(clientId, shmId1));
    ASSERT_FALSE(memRefTable_.Contains(clientId, shmId2));

    std::unordered_set<ShmKey> maybeSet3(maybeExpiredShmIds.begin(), maybeExpiredShmIds.end());
    ASSERT_EQ(maybeSet3.count(shmId1), 0);
    ASSERT_EQ(maybeSet3.count(shmId2), 0);
}
}  // namespace ut
}  // namespace datasystem
