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

#ifndef DATASYSTEM_MASTER_OBJECT_CACHE_DEVICE_MASTER_DEV_DEAD_LOCK_MANAGER_H
#define DATASYSTEM_MASTER_OBJECT_CACHE_DEVICE_MASTER_DEV_DEAD_LOCK_MANAGER_H

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <iostream>
#include <map>
#include <mutex>
#include <queue>
#include <set>
#include <thread>
#include <unordered_map>
#include <vector>

#include "datasystem/common/log/log.h"
#include "datasystem/common/util/format.h"
#include <tbb/concurrent_hash_map.h>
#include <tbb/concurrent_unordered_set.h>

namespace datasystem {
namespace master {

using TbbDeadLockDetectionGraph = tbb::concurrent_hash_map<std::string, tbb::concurrent_unordered_set<std::string>>;

class MasterDevDeadLockManager {
public:
    MasterDevDeadLockManager() = default;

    ~MasterDevDeadLockManager() = default;

    /**
     * @brief Checks if there exists a deadlock in the current dependency graph.
     * @return true if a cycle is detected indicating potential deadlock, false otherwise.
     * @note This method acquires a lock to ensure thread-safe graph traversal.
     *       Should be called when adding new dependencies to prevent deadlocks.
     */
    bool IsExistDeadlock();

    /**
     * @brief Adds a dependency edge from one entity to another in the dependency graph.
     * @param from The source entity that depends on the target entity.
     * @param to The target entity that the source entity depends on.
     * @note Duplicate edges are tracked but not added to the graph to maintain efficiency.
     *       This method is thread-safe.
     */
    void AddDependencyEdge(const std::string &from, const std::string &to);

    /**
     * @brief Removes a dependency edge from the graph.
     * @param from The source entity of the dependency edge to remove.
     * @param to The target entity of the dependency edge to remove.
     * @note If the edge exists, it will be removed from both the graph and tracking sets.
     *       This method is thread-safe.
     */
    void RemoveDependencyEdge(const std::string &from, const std::string &to);

private:
    /**
     * @brief Performs depth-first search to detect cycles in the dependency graph.
     * @param node The current node being visited in the DFS traversal.
     * @param parent The parent node from which we reached the current node.
     * @param visited Set of nodes that have been visited in the current DFS path.
     * @return true if a cycle is detected, false otherwise.
     * @note This is a recursive helper function used by IsExistDeadlock().
     */
    bool HasCycle(const std::string &node, const std::string &parent,
                  tbb::concurrent_unordered_set<std::string> &visited);

    // Thread-safe dependency graph for deadlock detection using TBB containers.
    TbbDeadLockDetectionGraph deadLockDetectionGraph_;

    // Protects non-thread-safe STL containers (edgesSet_ and duplicateEdges_)
    std::mutex edgesMutex_;

    // Thread-safe set tracking all unique dependency edges in the graph.
    tbb::concurrent_unordered_set<std::pair<std::string, std::string>> edgesSet_;

    // Thread-safe set tracking duplicate edges for monitoring and debugging.
    tbb::concurrent_unordered_set<std::pair<std::string, std::string>> duplicateEdges_;
};
}  // namespace master
}  // namespace datasystem
#endif