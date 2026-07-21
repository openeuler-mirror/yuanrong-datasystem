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
#ifndef DATASYSTEM_WORKER_OBJECT_CACHE_OBJECT_TABLE_H
#define DATASYSTEM_WORKER_OBJECT_CACHE_OBJECT_TABLE_H

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "datasystem/common/immutable_string/immutable_string.h"
#include "datasystem/common/object_cache/object_base.h"
#include "datasystem/common/object_cache/safe_table.h"

namespace datasystem {
namespace object_cache {

class ObjectTable final {
public:
    using Table = SafeTable<ImmutableString, ObjectInterface>;
    using SafeObjType = SafeObject<ObjectInterface>;

    class Iterator {
    public:
        using iterator_category = std::forward_iterator_tag;
        using value_type = typename Table::Iterator::value_type;
        using pointer = typename Table::Iterator::pointer;
        using reference = typename Table::Iterator::reference;

        Iterator(Iterator &&other) noexcept;
        Iterator &operator=(Iterator &&other) noexcept;
        ~Iterator();

        reference operator*() const;
        pointer operator->() const;
        Iterator &operator++();

        friend bool operator==(const Iterator &lhs, const Iterator &rhs);
        friend bool operator!=(const Iterator &lhs, const Iterator &rhs);

        Iterator(const Iterator &) = delete;
        Iterator &operator=(const Iterator &) = delete;

    private:
        class Impl;
        explicit Iterator(std::unique_ptr<Impl> impl);

        std::unique_ptr<Impl> impl_;
        friend class ObjectTable;
    };

    class RecoverySnapshotCursor {
    public:
        RecoverySnapshotCursor() = default;
        ~RecoverySnapshotCursor() = default;

    private:
        uint64_t generation_{ 0 };
        size_t shardIndex_{ 0 };
        uint64_t lastVisitedGeneration_{ 0 };

        friend class ObjectTable;
    };

    ObjectTable();
    ~ObjectTable();

    Status Insert(const ImmutableString &key, const ObjectInterface &obj);
    Status Insert(const ImmutableString &key, std::unique_ptr<ObjectInterface> objPtr);
    Status Insert(const ImmutableString &key, std::shared_ptr<SafeObjType> safeObjPtr);
    void InsertOrGet(const ImmutableString &key, std::shared_ptr<SafeObjType> &safeObjPtr);
    Status ReserveAndLock(const ImmutableString &key, std::shared_ptr<SafeObjType> &safeObjPtr);
    Status ReserveGetAndLock(const ImmutableString &key, std::shared_ptr<SafeObjType> &safeObjPtr, bool &isInsert,
                             bool returnIfDuplicated = false, bool lockIfFailed = true);
    Status Get(const ImmutableString &key, std::shared_ptr<SafeObjType> &safeObjPtr) const;
    Status Contains(const ImmutableString &key) const;
    Status Erase(const ImmutableString &key, SafeObjType &safeObj);
    Status Erase(const ImmutableString &key);
    Status GetAndLock(const ImmutableString &key, std::shared_ptr<SafeObjType> &safeObjPtr) const;

    Iterator begin();
    Iterator end();
    size_t GetSize() const;

    RecoverySnapshotCursor BeginRecoverySnapshot() const;
    Status NextRecoverySnapshotBatch(RecoverySnapshotCursor &cursor, size_t visitBudget,
                                     std::vector<std::string> &objectKeys, bool &done) const;
    Status GetRecoverySnapshotObject(const RecoverySnapshotCursor &cursor, const ImmutableString &key,
                                     std::shared_ptr<SafeObjType> &safeObjPtr) const;

    ObjectTable(const ObjectTable &) = delete;
    ObjectTable(ObjectTable &&other) noexcept = delete;
    ObjectTable &operator=(const ObjectTable &) = delete;
    ObjectTable &operator=(ObjectTable &&other) noexcept = delete;

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace object_cache
}  // namespace datasystem

#endif  // DATASYSTEM_WORKER_OBJECT_CACHE_OBJECT_TABLE_H
