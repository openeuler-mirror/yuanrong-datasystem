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

#ifndef DATASYSTEM_SAFE_TABLE_H
#define DATASYSTEM_SAFE_TABLE_H

#include <functional>
#include <memory>

#include <tbb/concurrent_hash_map.h>

#include "datasystem/common/log/log.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/object_cache/safe_object.h"
#include "datasystem/common/util/locks.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/thread_local.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/utils/status.h"

namespace datasystem {
static constexpr int DEBUG_LOG_LEVEL = 1;
/**
 * @brief A SafeTable is a key/value storage that operates over SafeObjects.
 *
 * It is a threadsafe hashmap, where the payload data at each key will be a SafeObject that provides additional
 * row/object level locking behaviours through the SafeObject api's. If the caller wishes to control concurrency
 * (locking) against any objects, it may use the SafeObject api's after fetching the SafeObject's from this table.
 *
 * @tparam KeyType The datatype to use as the keys.
 * @tparam ObjType The object type that will be used with the payload SafeObject's.
 */
template <typename KeyType, typename ObjType>
class SafeTable final {
public:
    using SafeObjType = SafeObject<ObjType>;
    using TbbTable = tbb::concurrent_hash_map<KeyType, std::shared_ptr<SafeObjType>>;

    /**
     * @brief Nested class for providing an iterator over the SafeTable.
     * The internal implementation of iteration is not thread-safe, so this iterator provides a table lock to make the
     * iteration safe.
     * Locking:
     * A table lock exists in the parent SafeTable. All SafeTable operations only get this lock in read mode, even
     * insert/erase it uses read mode. This is because the internal implementation of the table is already thread-safe
     * for lookups/insert/erase.  It does not need any external lock protection and there should not be any lock
     * contention for any lookups/insert/erase.
     * However, the iteration is not safe, so the iteration will get the table lock in write mode so that it will
     * exclusively block all other operations while it iterates.
     */
    class Iterator {
    public:
        using iterator_category = std::forward_iterator_tag;
        using value_type = std::pair<const KeyType, std::shared_ptr<SafeObjType>>;
        using pointer = value_type *;
        using reference = value_type &;

        /**
         * @brief Constructor.
         */
        Iterator(TbbTable &tab, const bool isBegin, SafeTable<KeyType, ObjType> *safeTable)
        {
            if (isBegin) {
                parent_ = safeTable;
                parent_->tableLock_.WriteLock();
                iter_ = tab.begin();
                locked_ = true;
                LOG(INFO) << "SafeTable iterator created and base table is locked";
            } else {
                iter_ = tab.end();
                parent_ = safeTable;
                locked_ = false;
            }
        }

        /**
         * @brief Destructor.
         */
        ~Iterator()
        {
            if (locked_) {
                parent_->tableLock_.WriteUnlock();
                LOG(INFO) << "SafeTable iterator destroyed and base table is now unlocked";
            }
        }

        /**
         * @brief Deference operator.
         * @return Reference to the SafeObject at this iterator position.
         */
        reference operator*() const
        {
            return *iter_;
        }

        /**
         * @brief Deference operator.
         * @return Pointer to the SafeObject at this iterator position.
         */
        pointer operator->() const
        {
            return &(*iter_);
        }

        /**
         * @brief ++ operator for forward iteration to the next SafeObject.
         * @return A reference to the iterator.
         */
        Iterator &operator++()
        {
            iter_++;
            return *this;
        }

        /**
         * @brief Operator ++ for forward iteration to the next SafeObject.
         * @return A reference to the iterator.
         */
        const Iterator operator++(int)
        {
            Iterator tmp = *this;
            ++(*this);
            return tmp;
        }

        /**
         * @brief Equality operator.
         * @param[in] a Iterator to use for comparing.
         * @param[in] b Iterator that will compare to the first one.
         * @return True if the operators are equal.
         */
        friend bool operator==(const Iterator &a, const Iterator &b)
        {
            return a.iter_ == b.iter_;
        }

        /**
         * @brief Inequality operator.
         * @param[in] a Iterator to use for comparing.
         * @param[in] b Iterator that will compare to the first one.
         * @return True if the operators are not equal.
         */
        friend bool operator!=(const Iterator &a, const Iterator &b)
        {
            return a.iter_ != b.iter_;
        };

    private:
        typename TbbTable::iterator iter_;     // This class simply wraps over the tbb iterator.
        SafeTable<KeyType, ObjType> *parent_;  // Back pointer to owning table.
        bool locked_;                          // An indicator to track if this iterator is holding the lock.
    };

    /**
     * @brief Constructor.
     */
    SafeTable() = default;

    /**
     * @brief Default destructor.
     */
    ~SafeTable() = default;

    /**
     * @brief Inserts a new object into the SafeTable by copying the data from input object into it.
     * @param[in] key The key for the object being inserted.
     * @param[in] obj A reference to the object that will be copied into the table.
     * @return Status of the call. This is for inserting a new object. If the key already exists in the table, it
     * returns K_DUPLICATED and the existing entry in the table is unchanged.
     */
    Status Insert(const KeyType &key, const ObjType &obj);

    /**
     * @brief Inserts a new object into the SafeTable by moving the unique ptr of the object into the table.
     * @param[in] key The key for the object being inserted.
     * @param[in] objPtr A pointer to the object.  When inserted, ownership of the unique pointer will be moved into the
     * SafeTable.
     * @return Status of the call. This is for inserting a new object. If the key already exists in the table, it
     * returns K_DUPLICATED and the existing entry in the table is unchanged.
     */
    Status Insert(const KeyType &key, std::unique_ptr<ObjType> objPtr);

    /**
     * @brief Inserts a new object into the SafeTable by copying a shared_pointer of the SafeObject into the SafeTable.
     * The use_count on the shared_ptr shall be incremented when the object is inserted and the pointer takes
     * shared ownership of the inserted object.
     * @param[in] key The key for the object being inserted.
     * @param[in] safeObjPtr A shared pointer to the SafeObject to copy into the SafeTable.
     * @return Status of the call. This is for inserting a new object. If the key already exists in the table, it
     * returns K_DUPLICATED and the existing entry in the table is unchanged.
     */
    Status Insert(const KeyType &key, std::shared_ptr<SafeObjType> safeObjPtr);

    /**
     * @brief Inserts a new object into the SafeTable by copying a shared_pointer of the SafeObject into the SafeTable.
     * If insert failed, assign the SafeObject pointer.
     * @param[in] key The key for the object being inserted.
     * @param[in/out] safeObjPtr A shared pointer to the SafeObject to copy into the SafeTable, if the key has been
     * inserted into table, assign safeObjPtr to existing safeObjPtr in table.
     */
    void InsertOrGet(const KeyType &key, std::shared_ptr<SafeObjType> &safeObjPtr);

    /**
     * @brief Inserts a new object into the SafeTable as an empty SafeObject. A key/slot is consumed in the table but
     * the real object of the SafeObject remains null, allowing the user to perform custom initialization of the real
     * object.
     * @param[in] key The key for object being inserted.
     * @param[out] safeObjPtr A shared pointer to the SafeObject that was just inserted, but the real data for this
     * SafeObject remains null. Output argument, must be nullptr when passed in.
     * @note IMPORTANT: The safeObjPtr that is returned will have a write lock currently held on the object.
     * @return Status of the call. This is for inserting a new object. If the key already exists in the table, it
     * returns K_DUPLICATED and the existing entry in the table is unchanged.
     */
    Status ReserveAndLock(const KeyType &key, std::shared_ptr<SafeObjType> &safeObjPtr);

    /**
     * @brief Inserts a new object into the SafeTable as an empty SafeObject. A key/slot is consumed in the table but
     * the real object of the SafeObject remains null, allowing the user to perform custom initialization of the real
     * object.  If the key is already use by the table, instead of returning K_DUPLICATED like ReserveAndLock(),
     * this function will instead fetch the existing entry.
     * @param[in] key The key for object being inserted.
     * @param[out] safeObjPtr A shared pointer to the SafeObject that was just inserted, OR, the fetched SafeObject
     * if it already existed.
     * @param[out] isInsert Indicates whether this object were inserted.
     * @param[in] returnIfDuplicated To determine whether return or not if key is duplicated. State may care about it.
     * @param[in] lockIfFailed When the key is already use by the table, after fetching the existing entry, determine
     * whether lock it or not.
     * @note IMPORTANT: The safeObjPtr that is returned will have a write lock currently held on the object.
     * @return Status of the call.
     */
    Status ReserveGetAndLock(const KeyType &key, std::shared_ptr<SafeObjType> &safeObjPtr, bool &isInsert,
                             bool returnIfDuplicated = false, bool lockIfFailed = true);

    /**
     * @brief Fetches the safe object into a shared pointer. To access the real object, caller must then use the
     * SafeObject api appropriately.
     * @param[in] key The key for identifying the object.
     * @param[out] safeObjPtr A shared pointer to the SafeObject that is fetched. Output argument, must be nullptr when
     * passed in.
     * @return Status of the call. If the key does not exist, K_NOT_FOUND is returned.
     */
    Status Get(const KeyType &key, std::shared_ptr<SafeObjType> &safeObjPtr) const;

    /**
     * @brief A lighter-weight version of Get, but does not return the object, it only checks if it exists or not.
     * @param[in] key The key for identifying the object.
     * @return Status of the call. If the key exists, OK is returned. If not, K_NOT_FOUND is returned.
     */
    Status Contains(const KeyType &key) const;

    /**
     * @brief Locates the object by key and then removes it from the table.
     * @param[in] key The key for identifying the object.
     * @param[in] safeObj The safe object ready to delete.
     * @return Status of the call.
     *         If the key does not exist, K_NOT_FOUND is returned.
     *         If the value is not the same as safeObj, K_INVALID is returned.
     */
    Status Erase(const KeyType &key, SafeObjType &safeObj);

    /**
     * @brief Locates the object by key and then removes it from the table.
     * @param[in] key The key for identifying the object.
     * @return Status of the call. If the key does not exist, K_NOT_FOUND is returned.
     */
    Status Erase(const KeyType &key);

    /**
     * @brief Creates a new iterator and starts iterating at the first SafeObject.
     * This also acquires a table-level write lock to ensure the iterating is an exclusive operation that is
     * threadsafe.
     * @return The iterator is instantiated and returned.
     */
    Iterator begin()
    {
        return Iterator(tbbTable_, true, this);
    }

    /**
     * @brief Creates a new iterator that is at the end position of iteration.
     * This is used to compare with an iterator to determine if the iteration is done or not.
     * @return The iterator (at the end position) is instantiated and returned.
     */
    Iterator end()
    {
        return Iterator(tbbTable_, false, this);
    }

    size_t GetSize() const
    {
        return tbbTable_.size();
    }

    // Disable all copy and move constructors.
    SafeTable(const SafeTable &) = delete;
    SafeTable(SafeTable &&other) noexcept = delete;
    SafeTable &operator=(const SafeTable &) = delete;
    SafeTable &operator=(SafeTable &&other) noexcept = delete;

    /**
     * @brief Fetches the safe object into a shared pointer and then lock it. This function will fetch the existing
     * entry, lock it first before returning it. If the key is not used by the table, this function returns K_NOT_FOUND
     * @param[in] key The key for identifying the object.
     * @param[out] safeObjPtr A shared pointer to the fetched SafeObject.
     * @note IMPORTANT: The safeObjPtr that is returned will have a write lock currently held on the object.
     * @return Status of the call.
     */
    Status GetAndLock(const KeyType &key, std::shared_ptr<SafeObjType> &safeObjPtr);

private:
    TbbTable tbbTable_;           // The implementation of the table itself.
    WriterPrefRWLock tableLock_;  // A lock for use with iteration to make iteration exclusive and threadsafe.

    /**
     * @brief Locates the object by key and then removes it from the table.
     * @param[in] key The key for identifying the object.
     * @param[in] safeObj The safe object ready to delete.
     * @param[in] checkSafeObj The object to erase is already locked.
     * @return Status of the call.
     *         If the key does not exist, K_NOT_FOUND is returned.
     *         If the value is not the same as safeObj, K_INVALID is returned.
     */
    Status Erase(const KeyType &key, SafeObjType &safeObj, bool checkSafeObj);
};

template <typename KeyType, typename ObjType>
Status SafeTable<KeyType, ObjType>::Insert(const KeyType &key, const ObjType &obj)
{
    auto safeObjPtr = std::make_shared<SafeObjType>(obj);
    return Insert(key, std::move(safeObjPtr));
}

template <typename KeyType, typename ObjType>
Status SafeTable<KeyType, ObjType>::Insert(const KeyType &key, std::unique_ptr<ObjType> objPtr)
{
    auto safeObjPtr = std::make_shared<SafeObjType>(std::move(objPtr));
    return Insert(key, std::move(safeObjPtr));
}

template <typename KeyType, typename ObjType>
Status SafeTable<KeyType, ObjType>::Insert(const KeyType &key, std::shared_ptr<SafeObjType> safeObjPtr)
{
    tableLock_.ReadLock();
    if (tbbTable_.emplace(key, std::move(safeObjPtr))) {
        tableLock_.ReadUnlock();
        return Status::OK();
    }
    tableLock_.ReadUnlock();
    RETURN_STATUS(StatusCode::K_DUPLICATED, "Object already exists.");
}

template <typename KeyType, typename ObjType>
void SafeTable<KeyType, ObjType>::InsertOrGet(const KeyType &key, std::shared_ptr<SafeObjType> &safeObjPtr)
{
    typename TbbTable::accessor accessor;
    if (tbbTable_.find(accessor, key)) {
        safeObjPtr = accessor->second;
    } else {
        tableLock_.ReadLock();
        (void)tbbTable_.emplace(key, safeObjPtr);
        tableLock_.ReadUnlock();
    }
}

template <typename KeyType, typename ObjType>
Status SafeTable<KeyType, ObjType>::ReserveAndLock(const KeyType &key, std::shared_ptr<SafeObjType> &safeObjPtr)
{
    CHECK_FAIL_RETURN_STATUS(safeObjPtr == nullptr, StatusCode::K_RUNTIME_ERROR,
                             "Output argument was passed in as not-null reference!");
    // Create empty SafeObject and lock it.
    safeObjPtr = std::make_shared<SafeObjType>();
    (void)safeObjPtr->WLock(true);
    // Insert into the table, leaving the object locked.
    Status rc = Insert(key, safeObjPtr);
    if (rc.IsError()) {
        safeObjPtr->WUnlock();
        safeObjPtr.reset();
    }
    return rc;
}

template <typename KeyType, typename ObjType>
Status SafeTable<KeyType, ObjType>::GetAndLock(const KeyType &key, std::shared_ptr<SafeObjType> &safeObjPtr)
{
    RETURN_IF_NOT_OK(this->Get(key, safeObjPtr));
    INJECT_POINT("safe_table.get_and_lock");
    return (safeObjPtr->WLock(true));
}

template <typename KeyType, typename ObjType>
Status SafeTable<KeyType, ObjType>::ReserveGetAndLock(const KeyType &key, std::shared_ptr<SafeObjType> &safeObjPtr,
                                                      bool &isInsert, bool returnIfDuplicated, bool lockIfFailed)
{
    INJECT_POINT("SafeTable.ReserveGetAndLock.return", []() { return Status(K_NOT_FOUND, ""); });
    isInsert = false;
    const int maxRetries = 5;
    int numRetries = 0;
    CHECK_FAIL_RETURN_STATUS(safeObjPtr == nullptr, K_RUNTIME_ERROR,
                             "Output argument was passed in as not-null reference!");
    // Insert into the table, leaving the object locked.
    Status rc;
    do {
        // Create empty SafeObject and lock it.
        safeObjPtr = std::make_shared<SafeObjType>();
        (void)safeObjPtr->WLock(true);

        rc = this->Insert(key, safeObjPtr);
        if (rc.IsError()) {
            // Failure to insert. Unlock it before we do error handling.
            safeObjPtr->WUnlock();
            safeObjPtr.reset();
        } else {
            isInsert = true;
        }
        // Handle the error code.
        if (rc.GetCode() == K_DUPLICATED) {
            // Since a key exists, attempt to get the existing SafeObject instead.
            if (lockIfFailed) {
                rc = this->GetAndLock(key, safeObjPtr);
            } else {
                rc = this->Get(key, safeObjPtr);
            }
            if (returnIfDuplicated && rc.IsOk() && safeObjPtr->Get() != nullptr
                && !(safeObjPtr->Get()->stateInfo.IsCacheInvalid())) {
                safeObjPtr->WUnlock();
                rc = Status(K_OC_KEY_ALREADY_EXIST, "object[" + std::string(key) + "] already exists in local worker");
                break;
            }
        }
        // A rare timing case where another thread removed the object from the object table after our reserve
        // attempt but before we did Get(). A retry can fix that.
    } while (rc.GetCode() == K_NOT_FOUND && numRetries++ < maxRetries);

    if (rc.GetCode() == K_NOT_FOUND && numRetries >= maxRetries) {
        rc = Status(K_WORKER_TIMEOUT, "Max retries hit while trying to reserve " + std::string(key) + " in SafeTable");
        LOG(ERROR) << rc.ToString();
    }

    return rc;
}

template <typename KeyType, typename ObjType>
Status SafeTable<KeyType, ObjType>::Get(const KeyType &key, std::shared_ptr<SafeObjType> &safeObjPtr) const
{
    // Note: Get() is read-only so we can allow get requests even if the iterator is in progress and holding the write
    // lock on the table. Iterator does not add or remove entries.
    typename TbbTable::const_accessor accessor;
    if (tbbTable_.find(accessor, key)) {
        RETURN_RUNTIME_ERROR_IF_NULL(accessor->second);
        safeObjPtr = accessor->second;
        return Status::OK();
    }
    RETURN_STATUS(StatusCode::K_NOT_FOUND, "Object does not exist.");
}

template <typename KeyType, typename ObjType>
Status SafeTable<KeyType, ObjType>::Contains(const KeyType &key) const
{
    // Note: Contains() is read-only so we can allow it even if the iterator is in progress and holding the write lock
    // on the table. Iterator does not add or remove entries.
    if (tbbTable_.count(key)) {
        return Status::OK();
    }
    RETURN_STATUS(StatusCode::K_NOT_FOUND, "Object does not exist.");
}

template <typename KeyType, typename ObjType>
Status SafeTable<KeyType, ObjType>::Erase(const KeyType &key, SafeObjType &safeObj, bool checkSafeObj)
{
    typename TbbTable::accessor accessor;
    if (!tbbTable_.find(accessor, key)) {
        return { K_NOT_FOUND, "Object does not exist." };
    }
    // Since there can be many users holding a shared_ptr to the same SafeObject that we are trying to erase,
    // there can be a timing issue where an erase happens after another thread has a ptr to the object, but
    // before they call lock on it.
    // Deleting the object from the SafeTable is safe, however the other users still have a pointer to the
    // object that was deleted.
    // The solution is that we must call delete against the object first to mark it as being deleted.
    // Any user with a reference to this shared object is still valid, however the real object is done.
    // An attempt to lock the deleted object will get an error for that user since the object got removed.
    auto objPtr = accessor->second;
    // Compare the SafeObject reference is the same.
    if (checkSafeObj && &safeObj != &(*objPtr)) {
        return { K_INVALID, "The input safeObj is not the same object in SafeTable." };
    }
    if (objPtr->IsWLockedByCurrentThread()) {
        objPtr->DeleteObject();
    } else {
        (void)objPtr->WLock();
        objPtr->DeleteObject();
        objPtr->WUnlock();
    }
    tableLock_.ReadLock();
    (void)tbbTable_.erase(accessor);  // Positional erase using the accessor.
    tableLock_.ReadUnlock();
    return Status::OK();
}

template <typename KeyType, typename ObjType>
Status SafeTable<KeyType, ObjType>::Erase(const KeyType &key)
{
    SafeObjType empty;
    return Erase(key, empty, false);
}

template <typename KeyType, typename ObjType>
Status SafeTable<KeyType, ObjType>::Erase(const KeyType &key, SafeObjType &safeObj)
{
    return Erase(key, safeObj, true);
}
}  // namespace datasystem
#endif  // DATASYSTEM_SAFE_TABLE_H
