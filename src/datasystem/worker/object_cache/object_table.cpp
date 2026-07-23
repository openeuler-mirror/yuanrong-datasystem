/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Description: Object-cache-local table with bounded recovery snapshots.
 */
#include "datasystem/worker/object_cache/object_table.h"

#include <array>
#include <atomic>
#include <chrono>
#include <functional>
#include <map>
#include <thread>
#include <utility>

#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/util/locks.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {
namespace object_cache {
namespace {
constexpr size_t RECOVERY_INDEX_SHARD_COUNT = 64;
}  // namespace

class ObjectTable::Impl {
public:
    ~Impl() = default;

    // Recovery keeps only key generations here; the object payload and lifetime still belong to SafeTable.
    struct IndexShard {
        using EntriesByKey = std::map<std::string, uint64_t>;
        using EntriesByGeneration = std::map<uint64_t, EntriesByKey::iterator>;

        mutable WriterPrefRWLock lock;
        EntriesByKey entries;
        EntriesByGeneration entriesByGeneration;
    };

    class IndexInsertRollback {
    public:
        IndexInsertRollback(IndexShard &shard, IndexShard::EntriesByKey::iterator iter, bool active) noexcept
            : shard_(shard), keyIter_(iter), generationIter_(shard.entriesByGeneration.end()), active_(active)
        {
        }

        ~IndexInsertRollback()
        {
            if (active_) {
                if (generationIter_ != shard_.entriesByGeneration.end()) {
                    shard_.entriesByGeneration.erase(generationIter_);
                }
                shard_.entries.erase(keyIter_);
            }
        }

        void SetGenerationIterator(IndexShard::EntriesByGeneration::iterator iter) noexcept
        {
            generationIter_ = iter;
        }

        void Release() noexcept
        {
            active_ = false;
        }

    private:
        IndexShard &shard_;
        IndexShard::EntriesByKey::iterator keyIter_;
        IndexShard::EntriesByGeneration::iterator generationIter_;
        bool active_;
    };

    template <typename InsertFunc>
    Status Insert(const ImmutableString &key, InsertFunc &&insertFunc)
    {
        ReadLock iteratorGuard(&iteratorCoordination_);
        const std::string indexKey(key);  // NOLINT(performance-unnecessary-copy-initialization)
        auto &shard = ShardFor(indexKey);
        WriteLock shardGuard(&shard.lock);
        auto [indexIt, indexInserted] = shard.entries.emplace(indexKey, 0);
        IndexInsertRollback rollback(shard, indexIt, indexInserted);
        if (indexInserted) {
            indexIt->second = NextGeneration();
            auto generationIt = shard.entriesByGeneration.emplace(indexIt->second, indexIt).first;
            rollback.SetGenerationIterator(generationIt);
        }

        Status status = insertFunc();
        if (status.IsError()) {
            return status;
        }
        INJECT_POINT_NO_RETURN("ObjectTable.Insert.AfterTableInsert",
                               [](int delayMs) { std::this_thread::sleep_for(std::chrono::milliseconds(delayMs)); });
        rollback.Release();
        return status;
    }

    IndexShard &ShardFor(const std::string &key)
    {
        return shards_[std::hash<std::string>{}(key) % shards_.size()];
    }

    uint64_t NextGeneration()
    {
        return insertionGeneration_.fetch_add(1, std::memory_order_acq_rel) + 1;
    }

    Table table_;
    // Existing begin()/end() callers expect whole-table traversal to be stable. Recovery snapshots use the sharded
    // index below and do not take this write lock while scanning.
    WriterPrefRWLock iteratorCoordination_;
    std::array<IndexShard, RECOVERY_INDEX_SHARD_COUNT> shards_;
    std::atomic<uint64_t> insertionGeneration_{ 0 };
};

class ObjectTable::Iterator::Impl {
public:
    Impl(ObjectTable::Impl &table, bool isBegin) : iteratorGuard_(isBegin ? &table.iteratorCoordination_ : nullptr)
    {
        if (isBegin) {
            iterator_.reset(new Table::Iterator(table.table_.begin()));
        } else {
            iterator_.reset(new Table::Iterator(table.table_.end()));
        }
    }
    ~Impl() = default;

    WriteLock iteratorGuard_;
    std::unique_ptr<Table::Iterator> iterator_;
};

ObjectTable::Iterator::Iterator(std::unique_ptr<Impl> impl) : impl_(std::move(impl))
{
}

ObjectTable::Iterator::Iterator(Iterator &&other) noexcept = default;

ObjectTable::Iterator &ObjectTable::Iterator::operator=(Iterator &&other) noexcept = default;

ObjectTable::Iterator::~Iterator() = default;

ObjectTable::RecoverySnapshotCursor::RecoverySnapshotCursor() = default;

ObjectTable::Iterator::reference ObjectTable::Iterator::operator*() const
{
    return *(*impl_->iterator_);
}

ObjectTable::Iterator::pointer ObjectTable::Iterator::operator->() const
{
    return impl_->iterator_->operator->();
}

ObjectTable::Iterator &ObjectTable::Iterator::operator++()
{
    ++(*impl_->iterator_);
    return *this;
}

bool operator==(const ObjectTable::Iterator &lhs, const ObjectTable::Iterator &rhs)
{
    return *lhs.impl_->iterator_ == *rhs.impl_->iterator_;
}

bool operator!=(const ObjectTable::Iterator &lhs, const ObjectTable::Iterator &rhs)
{
    return !(lhs == rhs);
}

ObjectTable::ObjectTable() : impl_(std::make_unique<Impl>())
{
}

ObjectTable::~ObjectTable() = default;

Status ObjectTable::Insert(const ImmutableString &key, const ObjectInterface &obj)
{
    (void)key;
    (void)obj;
    return { K_NOT_SUPPORTED, "ObjectInterface is abstract and cannot be copied into ObjectTable" };
}

Status ObjectTable::Insert(const ImmutableString &key, std::unique_ptr<ObjectInterface> objPtr)
{
    return impl_->Insert(key, [&] { return impl_->table_.Insert(key, std::move(objPtr)); });
}

Status ObjectTable::Insert(const ImmutableString &key, std::shared_ptr<SafeObjType> safeObjPtr)
{
    return impl_->Insert(key, [&] { return impl_->table_.Insert(key, std::move(safeObjPtr)); });
}

void ObjectTable::InsertOrGet(const ImmutableString &key, std::shared_ptr<SafeObjType> &safeObjPtr)
{
    ReadLock iteratorGuard(&impl_->iteratorCoordination_);
    const std::string indexKey(key);  // NOLINT(performance-unnecessary-copy-initialization)
    auto &shard = impl_->ShardFor(indexKey);
    WriteLock shardGuard(&shard.lock);
    auto [indexIt, indexInserted] = shard.entries.emplace(indexKey, 0);
    Impl::IndexInsertRollback rollback(shard, indexIt, indexInserted);
    if (indexInserted) {
        indexIt->second = impl_->NextGeneration();
        auto generationIt = shard.entriesByGeneration.emplace(indexIt->second, indexIt).first;
        rollback.SetGenerationIterator(generationIt);
    }

    impl_->table_.InsertOrGet(key, safeObjPtr);
    rollback.Release();
}

Status ObjectTable::ReserveAndLock(const ImmutableString &key, std::shared_ptr<SafeObjType> &safeObjPtr)
{
    CHECK_FAIL_RETURN_STATUS(safeObjPtr == nullptr, K_RUNTIME_ERROR,
                             "Output argument was passed in as not-null reference!");
    safeObjPtr = std::make_shared<SafeObjType>();
    (void)safeObjPtr->WLock(true);
    auto status = Insert(key, safeObjPtr);
    if (status.IsError()) {
        safeObjPtr->WUnlock();
        safeObjPtr.reset();
    }
    return status;
}

Status ObjectTable::ReserveGetAndLock(const ImmutableString &key, std::shared_ptr<SafeObjType> &safeObjPtr,
                                      bool &isInsert, bool returnIfDuplicated, bool lockIfFailed)
{
    INJECT_POINT_NO_RETURN("ObjectTable.ReserveGetAndLock.BeforeTableCall",
                           [](int delayMs) { std::this_thread::sleep_for(std::chrono::milliseconds(delayMs)); });
    INJECT_POINT("SafeTable.ReserveGetAndLock.return", [] { return Status(K_NOT_FOUND, ""); });
    isInsert = false;
    constexpr int maxRetries = 5;
    int numRetries = 0;
    CHECK_FAIL_RETURN_STATUS(safeObjPtr == nullptr, K_RUNTIME_ERROR,
                             "Output argument was passed in as not-null reference!");
    Status status;
    do {
        safeObjPtr = std::make_shared<SafeObjType>();
        (void)safeObjPtr->WLock(true);
        status = Insert(key, safeObjPtr);
        if (status.IsError()) {
            safeObjPtr->WUnlock();
            safeObjPtr.reset();
        } else {
            isInsert = true;
        }
        if (status.GetCode() == K_DUPLICATED) {
            status = lockIfFailed ? GetAndLock(key, safeObjPtr) : Get(key, safeObjPtr);
            if (!returnIfDuplicated || status.IsError() || safeObjPtr->Get() == nullptr
                || safeObjPtr->Get()->stateInfo.IsCacheInvalid()) {
                continue;
            }
            if (lockIfFailed) {
                safeObjPtr->WUnlock();
            }
            status = Status(K_OC_KEY_ALREADY_EXIST, "object[" + std::string(key) + "] already exists in local worker");
            break;
        }
    } while (status.GetCode() == K_NOT_FOUND && numRetries++ < maxRetries);
    if (status.GetCode() == K_NOT_FOUND && numRetries >= maxRetries) {
        status =
            Status(K_WORKER_TIMEOUT, "Max retries hit while trying to reserve " + std::string(key) + " in ObjectTable");
        LOG(ERROR) << status.ToString();
    }
    return status;
}

Status ObjectTable::Get(const ImmutableString &key, std::shared_ptr<SafeObjType> &safeObjPtr) const
{
    auto &shard = impl_->ShardFor(std::string(key));
    ReadLock shardGuard(&shard.lock);
    return impl_->table_.Get(key, safeObjPtr);
}

Status ObjectTable::Contains(const ImmutableString &key) const
{
    auto &shard = impl_->ShardFor(std::string(key));
    ReadLock shardGuard(&shard.lock);
    return impl_->table_.Contains(key);
}

Status ObjectTable::Erase(const ImmutableString &key, SafeObjType &safeObj)
{
    const bool needUnlock = !safeObj.IsWLockedByCurrentThread();
    if (needUnlock) {
        RETURN_IF_NOT_OK(safeObj.WLock(true));
    }
    Raii safeObjUnlock([&safeObj, needUnlock] {
        if (needUnlock) {
            safeObj.WUnlock();
        }
    });
    ReadLock iteratorGuard(&impl_->iteratorCoordination_);
    const std::string indexKey(key);  // NOLINT(performance-unnecessary-copy-initialization)
    auto &shard = impl_->ShardFor(indexKey);
    WriteLock shardGuard(&shard.lock);
    Status status = impl_->table_.Erase(key, safeObj);
    if (status.IsOk()) {
        auto indexIt = shard.entries.find(indexKey);
        if (indexIt != shard.entries.end()) {
            shard.entriesByGeneration.erase(indexIt->second);
            shard.entries.erase(indexIt);
        }
    }
    return status;
}

Status ObjectTable::Erase(const ImmutableString &key)
{
    std::shared_ptr<SafeObjType> safeObjPtr;
    RETURN_IF_NOT_OK(GetAndLock(key, safeObjPtr));
    auto status = Erase(key, *safeObjPtr);
    safeObjPtr->WUnlock();
    return status;
}

Status ObjectTable::GetAndLock(const ImmutableString &key, std::shared_ptr<SafeObjType> &safeObjPtr) const
{
    RETURN_IF_NOT_OK(Get(key, safeObjPtr));
    INJECT_POINT("safe_table.get_and_lock");
    return safeObjPtr->WLock(true);
}

ObjectTable::Iterator ObjectTable::begin()
{
    return Iterator(std::make_unique<Iterator::Impl>(*impl_, true));
}

ObjectTable::Iterator ObjectTable::end()
{
    return Iterator(std::make_unique<Iterator::Impl>(*impl_, false));
}

size_t ObjectTable::GetSize() const
{
    return impl_->table_.GetSize();
}

ObjectTable::RecoverySnapshotCursor ObjectTable::BeginRecoverySnapshot() const
{
    RecoverySnapshotCursor cursor;
    cursor.generation_ = impl_->insertionGeneration_.load(std::memory_order_acquire);
    return cursor;
}

Status ObjectTable::NextRecoverySnapshotBatch(RecoverySnapshotCursor &cursor, size_t visitBudget,
                                              std::vector<std::string> &objectKeys, bool &done) const
{
    CHECK_FAIL_RETURN_STATUS(visitBudget > 0, K_INVALID, "Recovery snapshot visit budget must be greater than zero");
    objectKeys.clear();
    done = false;
    size_t visited = 0;
    while (cursor.shardIndex_ < impl_->shards_.size() && visited < visitBudget) {
        auto &shard = impl_->shards_[cursor.shardIndex_];
        {
            ReadLock shardGuard(&shard.lock);
            auto iter = shard.entriesByGeneration.upper_bound(cursor.lastVisitedGeneration_);
            while (iter != shard.entriesByGeneration.end() && iter->first <= cursor.generation_
                   && visited < visitBudget) {
                objectKeys.emplace_back(iter->second->first);
                cursor.lastVisitedGeneration_ = iter->first;
                ++visited;
                ++iter;
            }
            if (iter == shard.entriesByGeneration.end() || iter->first > cursor.generation_) {
                ++cursor.shardIndex_;
                cursor.lastVisitedGeneration_ = 0;
            }
        }
    }
    done = cursor.shardIndex_ == impl_->shards_.size();
    return Status::OK();
}

Status ObjectTable::GetRecoverySnapshotObject(const RecoverySnapshotCursor &cursor, const ImmutableString &key,
                                              std::shared_ptr<SafeObjType> &safeObjPtr) const
{
    const std::string indexKey(key);  // NOLINT(performance-unnecessary-copy-initialization)
    auto &shard = impl_->ShardFor(indexKey);
    ReadLock shardGuard(&shard.lock);
    auto iter = shard.entries.find(indexKey);
    if (iter == shard.entries.end() || iter->second > cursor.generation_) {
        RETURN_STATUS(K_NOT_FOUND, "Object is not part of the recovery snapshot");
    }
    return impl_->table_.Get(key, safeObjPtr);
}

}  // namespace object_cache
}  // namespace datasystem
