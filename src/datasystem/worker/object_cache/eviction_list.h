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
 * Description: Defines EvictionList Interface.
 */
#ifndef DATASYSTEM_EVICTION_LIST_H
#define DATASYSTEM_EVICTION_LIST_H

#include <list>
#include <memory>
#include <shared_mutex>
#include <utility>

#include "datasystem/common/immutable_string/immutable_string.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace object_cache {
constexpr uint8_t Q1 = 1;
constexpr uint8_t Q2 = 2;
constexpr uint8_t Q3 = 3;
constexpr uint8_t READD_COUNTER = 5;

class EvictionList {
public:
    struct Node {
        Node() = default;

        Node(const ImmutableString &objKey, uint8_t curCnt) : objectKey(objKey), curCounter(curCnt), maxCounter(curCnt)
        {
        }

        ImmutableString objectKey;
        uint8_t curCounter;
        uint8_t maxCounter;
    };
    using TBBIndexMap = tbb::concurrent_hash_map<ImmutableString, std::list<Node>::iterator>;

    /**
     * @brief Construct EvictionList.
     */
    EvictionList();

    ~EvictionList() = default;

    /**
     * @brief Add a object to EvictionList.
     * @param[in] objectKey The ID of the object to add.
     * @param[in] counter The counter of the object to add.
     */
    void Add(const std::string &objectKey, uint8_t counter);

    /**
     * @brief Erase a object from EvictionList.
     * @param[in] objectKey The ID of the object to erase.
     * @return Status of the call.
     */
    Status Erase(const std::string &objectKey);

    /**
     * @brief Get the size of EvictionList.
     * @return The size of EvictionList.
     */
    size_t Size();

    /**
     * @brief Find a object that current counter is 0.
     * @param[out] candidateObjKey The ID of the object that current counter is 0.
     * @return Status of the call.
     */
    Status FindEvictCandidate(std::string &candidateObjKey);

    /**
     * @brief Get a node from EvictionList, for test.
     * @param[in] objectKey The ID of the object to get.
     * @param[out] node The node of objectKey.
     * @return Status of the call.
     */
    Status GetObjectInfo(const std::string &objectKey, Node &node);

    /**
     * @brief Get the oldest node from EvictionList, for test.
     * @param[out] node The node of oldest one.
     * @return Status of the call.
     */
    Status GetOldestObjectInfo(Node &node);

    /**
     * @brief Get all nodes from EvictionList, for testing.
     * @param[out] res All nodes in EvictionList.
     * @param[out] oldest The oldest node in EvictionList.
     * @return Status of the call.
     */
    Status GetAllObjectsInfo(std::vector<EvictionList::Node> &res, EvictionList::Node &oldest);

    /**
     * @brief Check whether a object in EvictionList.
     * @param[in] objectKey The ID of the object to check.
     * @return true if object in EvictionList.
     */
    bool Exist(const std::string &objectKey);

private:
    mutable tbb::spin_rw_mutex listMutex_;
    std::list<Node> list_;
    std::list<Node>::iterator oldest_;
    TBBIndexMap indexTable_;
};
}  // namespace object_cache
}  // namespace datasystem

#endif
