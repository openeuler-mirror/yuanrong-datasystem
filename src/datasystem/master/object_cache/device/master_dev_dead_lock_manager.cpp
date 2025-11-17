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

#include "datasystem/master/object_cache/device/master_dev_dead_lock_manager.h"

namespace datasystem {
namespace master {
bool MasterDevDeadLockManager::HasCycle(const std::string &node, const std::string &parent,
                                        tbb::concurrent_unordered_set<std::string> &visited)
{
    visited.insert(node);
    TbbDeadLockDetectionGraph::const_accessor acc;
    if (deadLockDetectionGraph_.find(acc, node)) {
        for (const std::string &neighbor : acc->second) {
            if (neighbor == parent) {
                continue;
            }
            if (visited.find(neighbor) != visited.end()) {
                return true;
            }
            if (HasCycle(neighbor, node, visited)) {
                // Cycle detected in recursive traversal
                return true;
            }
        }
    }
    return false;
}

bool MasterDevDeadLockManager::IsExistDeadlock()
{
    PerfPoint point(PerfKey::MASTER_IS_EXIST_DEAD_LOCK);
    auto lockGuard = std::lock_guard<std::mutex>(edgesMutex_);

    // Duplicate edges immediately indicate potential deadlock
    if (!duplicateEdges_.empty()) {
        return true;
    }

    // Perform DFS cycle detection on each connected component
    tbb::concurrent_unordered_set<std::string> visited;
    for (const auto &pair : deadLockDetectionGraph_) {
        const std::string &node = pair.first;
        if (visited.find(node) == visited.end()) {
            if (HasCycle(node, "", visited)) {
                return true;
            }
        }
    }
    return false;
}

void MasterDevDeadLockManager::AddDependencyEdge(const std::string &from, const std::string &to)
{
    {
        auto lockGuard = std::lock_guard<std::mutex>(edgesMutex_);
        std::string u = from;
        std::string v = to;
        if (u > v)
            std::swap(u, v);

        // If edge already exists, mark as duplicate and return without adding to graph
        auto edge = std::make_pair(u, v);
        if (edgesSet_.find(edge) != edgesSet_.end()) {
            duplicateEdges_.insert(edge);
            return;
        }

        // Add the new edge to tracking set and dependency graph
        edgesSet_.insert(edge);
    }

    TbbDeadLockDetectionGraph::accessor acc1;
    deadLockDetectionGraph_.insert(acc1, from);
    acc1->second.insert(to);
    TbbDeadLockDetectionGraph::accessor acc2;
    deadLockDetectionGraph_.insert(acc2, to);
    acc2->second.insert(from);
}

void MasterDevDeadLockManager::RemoveDependencyEdge(const std::string &from, const std::string &to)
{
    {
        auto lockGuard = std::lock_guard<std::mutex>(edgesMutex_);

        // Normalize edge representation same as in AddDependencyEdge
        std::string u = from;
        std::string v = to;
        if (u > v)
            std::swap(u, v);

        // If this edge is marked as duplicate, just remove from duplicates
        auto edge = std::make_pair(u, v);
        if (duplicateEdges_.find(edge) != duplicateEdges_.end()) {
            duplicateEdges_.unsafe_erase(edge);
            return;
        }

        // Remove edge from tracking set
        edgesSet_.unsafe_erase(edge);
    }

    // Remove bidirectional edges from the dependency graph
    TbbDeadLockDetectionGraph::accessor acc;
    if (deadLockDetectionGraph_.find(acc, from)) {
        acc->second.unsafe_erase(to);
        if (acc->second.empty()) {
            deadLockDetectionGraph_.erase(acc);
        }
    }
    if (deadLockDetectionGraph_.find(acc, to)) {
        acc->second.unsafe_erase(from);
        if (acc->second.empty()) {
            deadLockDetectionGraph_.erase(acc);
        }
    }
}
}  // namespace master
}  // namespace datasystem