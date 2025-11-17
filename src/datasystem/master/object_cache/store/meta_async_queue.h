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
 * Description: Declare interface to store object meta in async queue.
 */
#ifndef DATASYSTEM_MASTER_OBJECT_CACHE_STORE_META_ASYNC_QUEUE_H
#define DATASYSTEM_MASTER_OBJECT_CACHE_STORE_META_ASYNC_QUEUE_H

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <memory>
#include <list>
#include <mutex>
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include "datasystem/common/util/format.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace master {
class AsyncElement;
}
}  // namespace datasystem

namespace std {
template <>
struct hash<::datasystem::master::AsyncElement> {
    std::size_t operator()(const ::datasystem::master::AsyncElement &e) const;
};

template <>
struct hash<std::shared_ptr<::datasystem::master::AsyncElement>> {
    std::size_t operator()(const std::shared_ptr<datasystem::master::AsyncElement> &e) const;
};

template <>
struct equal_to<std::shared_ptr<::datasystem::master::AsyncElement>> {
    bool operator()(const std::shared_ptr<::datasystem::master::AsyncElement> &e1,
                    const std::shared_ptr<::datasystem::master::AsyncElement> &e2) const;
};
}  // namespace std

namespace datasystem {
namespace master {

template <typename E>
class LinkedHashSet {
public:
    explicit LinkedHashSet(size_t capacity) : capacity_(capacity)
    {
    }

    /**
     * @brief Put an element into the set, f the key already exists, updates and moves it to the front.
     * @param[in] element Element
     * @return the oldest value if it is eliminated.
     */
    E Put(const E &element)
    {
        E e;
        // If the key already exists, remove the old value (retain the sequence).
        if (itemMap_.find(element) != itemMap_.end()) {
            itemList_.erase(itemMap_[element]);
        }
        // Insert a new value to the end of the linked list.
        itemList_.push_back(element);
        itemMap_[element] = --itemList_.end();

        // If capacity is exceeded, remove the earliest inserted element (only for LRU logic)
        if (capacity_ > 0 && itemList_.size() > capacity_) {
            auto oldest = itemList_.front();
            (void)itemMap_.erase(oldest);
            e = itemList_.front();
            itemList_.pop_front();
        }
        return e;
    }

    /**
     * @brief Remove an element.
     * @param[in] element Element.
     * @return True if element exist.
     */
    bool Remove(const E &element)
    {
        if (itemMap_.find(element) == itemMap_.end()) {
            return false;
        }
        itemList_.erase(itemMap_[element]);
        itemMap_.erase(element);
        return true;
    }

    /**
     * @brief Poll the oldest element.
     * @param[out] element Oldest element.
     * @return True if set is not empty.
     */
    bool Poll(E &element) const
    {
        if (itemList_.empty()) {
            return false;
        }
        element = itemList_.front();
        return true;
    }

    /**
     * @brief Check if element is in set.
     * @param[in] element Element.
     * @return True if exist.
     */
    bool Contains(const E &element) const
    {
        return itemMap_.find(element) != itemMap_.end();
    }

    /**
     * @brief Get set size.
     * @return Size.
     */
    size_t Size() const
    {
        return itemList_.size();
    }

private:
    std::list<E> itemList_;
    std::unordered_map<E, typename std::list<E>::iterator> itemMap_;
    size_t capacity_;
};

class AsyncElement {
public:
    enum class ReqType : uint8_t { ADD = 0, DEL = 1 };

    AsyncElement(const std::string &objectKey, const std::string &table, const std::string &key,
                 const std::string &value, ReqType reqType);

    AsyncElement(const std::string &objectKey, const std::string &table, const std::string &key,
                 const std::string &value, ReqType reqType, uint64_t timestamp, const std::string &traceId);

    bool operator==(const AsyncElement &other) const
    {
        return objectKey_ == other.objectKey_ && table_ == other.table_ && key_ == other.key_;
    }

    /**
     * @brief Get request type.
     * @return Request type.
     */
    ReqType RequestType() const
    {
        return reqType_;
    }

    /**
     * @brief Get object key.
     * @return Object key.
     */
    const std::string &ObjectKey() const
    {
        return objectKey_;
    }

    /**
     * @brief Get table name.
     * @return Table name.
     */
    const std::string &Table() const
    {
        return table_;
    }

    /**
     * @brief Get key.
     * @return Key.
     */
    const std::string &Key() const
    {
        return key_;
    }

    /**
     * @brief Get value.
     * @return Value.
     */
    const std::string &Value() const
    {
        return value_;
    }

    /**
     * @brief Get begin timestamp.
     * @return Begin timestamp.
     */
    const std::chrono::time_point<std::chrono::steady_clock> &BeginTimestamp() const
    {
        return beginTimestamp_;
    }

    uint64_t BeginTimestampUs() const
    {
        return std::chrono::duration_cast<std::chrono::microseconds>(beginTimestamp_.time_since_epoch()).count();
    }

    /**
     * @brief Get trace id.
     * @return Trace id.
     */
    const std::string &TraceID() const
    {
        return traceID_;
    }

    friend std::ostream &operator<<(std::ostream &os, const AsyncElement &ele)
    {
        auto printStr = FormatString("object key: %s, table: %s, key: %s, type: %d", ele.objectKey_, ele.table_,
                                     ele.key_, static_cast<uint32_t>(ele.reqType_));
        os << printStr;
        return os;
    }

    void SetPostHandler(std::function<Status()> &&postHandler)
    {
        postHandler_ = std::move(postHandler);
    }

    Status ExcutePostHandler()
    {
        if (postHandler_ == nullptr) {
            return Status::OK();
        }
        return postHandler_();
    }

private:
    const ReqType reqType_;
    const std::string objectKey_;
    const std::string table_;
    const std::string key_;
    const std::string value_;
    const std::chrono::time_point<std::chrono::steady_clock> beginTimestamp_;
    const std::string traceID_;
    std::function<Status()> postHandler_ = nullptr;
};

class MetaAsyncQueue {
public:
    explicit MetaAsyncQueue(size_t capacity) : orderedSet_(capacity)
    {
    }

    ~MetaAsyncQueue();

    /**
     * @brief Append async task.
     * @param[in] element Need append element.
     * @param[out] eliminatedEle Eliminated element,
     * @param[out] incrCnt Increase count.
     */
    void AppendAsyncTask(const std::shared_ptr<AsyncElement> &element, std::shared_ptr<AsyncElement> &eliminatedEle,
                         int &incrCnt);

    /**
     * @brief Poll the oldest element.
     * @param[out] element Oldest element.
     * @param[in] timeoutMs Wait timeout in milliseconds.
     * @return True if element is not null.
     */
    bool Poll(std::shared_ptr<AsyncElement> &element, int timeoutMs);

    void PollMetasByObjectKey(
        std::function<bool(const std::string &)> &&matchFunc,
        std::unordered_map<std::string, std::unordered_set<std::shared_ptr<AsyncElement>>> &outElements,
        uint64_t &count);

    void PollAsyncElementsByObjectKey(const std::string &objectKey,
                                     std::unordered_set<std::shared_ptr<AsyncElement>> &elements);

    /**
     * @brief Get async queue size.
     * @return Size.
     */
    size_t Size() const;

    /**
     * @brief Check id is Deleting or not
     * @param[in] obj key
     * @return true if is deleting.
     */
    bool CheckIdIsDeleting(const std::string &objKey) const;

private:
    /**
     * @brief Append add async task.
     * @param[in] element Need append element.
     * @param[out] eliminatedEle Eliminated element,
     * @param[out] incrCnt Increase count.
     */
    void AppendAddTaskInternal(const std::shared_ptr<AsyncElement> &element,
                               std::shared_ptr<AsyncElement> &eliminatedEle, int &incrCnt);

    /**
     * @brief Append del async task.
     * @param[in] element Need append element.
     * @param[out] eliminatedEle Eliminated element,
     * @param[out] incrCnt Increase count.
     */
    void AppendDelTaskInternal(const std::shared_ptr<AsyncElement> &element,
                               std::shared_ptr<AsyncElement> &eliminatedEle, int &incrCnt);

    mutable std::mutex mtx_;
    std::condition_variable emptyCond_;
    LinkedHashSet<std::shared_ptr<AsyncElement>> orderedSet_;
    std::unordered_map<std::string, std::unordered_set<std::shared_ptr<AsyncElement>>> objectMap_;
};

}  // namespace master
}  // namespace datasystem

inline std::size_t std::hash<::datasystem::master::AsyncElement>::operator()(
    const ::datasystem::master::AsyncElement &e) const
{
    // Use std::hash to compute hashes of members and merge these hashes
    std::size_t h1 = std::hash<std::string>()(e.ObjectKey());
    std::size_t h2 = std::hash<std::string>()(e.Key());
    std::size_t h3 = std::hash<std::string>()(e.Table());
    return h1 ^ (h2 << 1) ^ (h3 << 2);
}

inline std::size_t std::hash<std::shared_ptr<datasystem::master::AsyncElement>>::operator()(
    const std::shared_ptr<datasystem::master::AsyncElement> &e) const
{
    return std::hash<::datasystem::master::AsyncElement>()(*e.get());
}

inline bool std::equal_to<std::shared_ptr<::datasystem::master::AsyncElement>>::operator()(
    const std::shared_ptr<::datasystem::master::AsyncElement> &e1,
    const std::shared_ptr<::datasystem::master::AsyncElement> &e2) const
{
    return *e1 == *e2;
}

#endif