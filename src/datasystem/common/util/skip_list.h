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
 * Description: Skip List Implementation.
 * Based on: https://people.csail.mit.edu/shanir/publications/OPODIS2006-BA.pdf
 */
#ifndef DATASYSTEM_COMMON_UTIL_SKIP_LIST_H
#define DATASYSTEM_COMMON_UTIL_SKIP_LIST_H

#include <vector>
#include <mutex>
#include <random>
#include <limits>
#include <iterator>

#include "datasystem/common/log/log.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {
template <typename K, typename V>
struct Node {
    Node(K key, V value, int topLayer = 1)
        : key(key), value(value), nextList(topLayer, nullptr), topLayer(topLayer), marked(false), fullyLinked(false)
    {
    }
    K key;
    V value;
    std::vector<Node *> nextList;  // list of next nodes one for each layer
    int topLayer;                  // max layers in this node [0, topLayer)
    bool marked;                   // marked for deletion
    bool fullyLinked;              // fully linked after insertion
    std::mutex lock;
};

/**
 * SkipList is a probabilistic data structure that allows
 * O(log n) average complexity for search, insertion, and deletion
⁡ * within an ordered sequence of n elements.
 * this structure can be used as an alternative to linked list
 * to decrease search complexity
 * Below SkipList implementation assumes list will be in ascending order
*/
template <typename K, typename V>
class SkipList {
public:
    SkipList(int maxHeight = 24)
        : maxHeight_(maxHeight),
          lSentinel_(std::numeric_limits<K>::min(), std::numeric_limits<V>::min(), maxHeight_),
          rSentinel_(std::numeric_limits<K>::max(), std::numeric_limits<V>::max(), maxHeight_)
    {
        // Initially left most node is linked to right most node
        // left most node (lSentinel) will have least value and
        // Right most node (rSentinel) will have highest value
        for (int layer = 0; layer < maxHeight_; ++layer) {
            lSentinel_.nextList[layer] = &rSentinel_;
        }
        // They are fully linked in each level
        lSentinel_.fullyLinked = true;
        rSentinel_.fullyLinked = true;
    }

    /**
     * Iterator for SkipList :
     *
     * Its a forward iterator (uni-directional) for the SkipList.
     * Its operations are equivalent to move through 0th level of SkipList
     */
    class Iterator {
    public:
        using iterator_category = std::forward_iterator_tag;
        using difference_type = std::ptrdiff_t;
        using value_type = Node<K, V>;
        using pointer = Node<K, V> *;
        using reference = Node<K, V> &;

        Iterator(pointer ptr) : ptr_(ptr)
        {
        }

        /**
         * @brief Used to get key and value of a node.
         * @return Pair containing key and value for the iterator.
         */
        reference operator*() const
        {
            return std::make_pair(ptr_->key, ptr_->value);
        }

        /**
         * @brief Used to access the node.
         * @return Node pointer.
         */
        pointer operator->()
        {
            return ptr_;
        }

        /**
         * @brief Moves to next node and returns the Iterator
         * @return SkipList Iterator
         */
        Iterator &operator++()
        {
            ptr_ = ptr_->nextList[0];
            return *this;
        }

        /**
         * @brief Checks if two iterators are same
         * @return bool
         */
        friend bool operator==(const Iterator &a, const Iterator &b)
        {
            return a.ptr_ == b.ptr_;
        };

        /**
         * @brief Checks if two iterators are not same
         * @return bool
         */
        friend bool operator!=(const Iterator &a, const Iterator &b)
        {
            return a.ptr_ != b.ptr_;
        };

    private:
        pointer ptr_;
    };

    /**
     * @brief Returns the iterator to first node (node next to lSentinel)
     * @return SkipList Iterator
     */
    Iterator begin()
    {
        return Iterator(lSentinel_.nextList[0]);
    }

    /**
     * @brief Returns the iterator to node next to last node (rSentinel)
     * @return SkipList Iterator
     */
    Iterator end()
    {
        return Iterator(&rSentinel_);
    }

    /**
     * @brief Inserts a key and value into SkipList
     * @param[in] key Key to insert
     * @param[in] value value of the key
     * @return Status of the call (OK or K_DUPLICATED if already exists)
     */
    Status insert(K key, V value)
    {
        int topLayer = randomHeight();
        std::vector<Node<K, V> *> preds(maxHeight_), succs(maxHeight_);
        while (true) {
            // Check if the node already exists
            int levelFound = findNode(key, preds, succs);
            if (levelFound != -1) {
                Node<K, V> *nodeFound = succs[levelFound];
                if (!nodeFound->marked) {
                    // Wait for linking to be complete
                    waitForLinking(nodeFound);
                    // If not marked for delete and linking complete its a duplicate node
                    RETURN_STATUS(StatusCode::K_DUPLICATED, "key is already in the skip list");
                }
                continue;
            }

            // Lock predecessors in each level
            int highestLocked = -1;
            if (!checkAndLockPredsInsert(topLayer, highestLocked, preds, succs)) {
                continue;
            }

            // Create new node and attach predecessors and successor to it at each level
            Node<K, V> *newNode = new Node<K, V>(key, value, topLayer);
            for (int layer = 0; layer < topLayer; layer++) {
                newNode->nextList[layer] = succs[layer];
                preds[layer]->nextList[layer] = newNode;
            }
            newNode->fullyLinked = true;

            // Unlock all predecessors until highest level locked
            unlock(preds, highestLocked);
            return Status::OK();
        }
    }

    /**
     * @brief Erase a key from SkipList
     * @param[in] key Key to insert
     * @return Iterator to next element (or rSentinel if does not exists)
     */
    Iterator erase(K key)
    {
        Node<K, V> *nodeToDelete = nullptr;
        bool isMarked = false;
        int topLayer = -1;
        std::vector<Node<K, V> *> preds(maxHeight_), succs(maxHeight_);
        while (true) {
            // Find if the node exists
            int levelFound = findNode(key, preds, succs);
            if (isMarked || (levelFound != -1 && okToDelete(succs[levelFound], levelFound))) {
                nodeToDelete = succs[levelFound];
                topLayer = nodeToDelete->topLayer;
                if (!checkAndMarkNode(nodeToDelete, isMarked)) {
                    return end();
                }

                // Lock predecessors in each level
                int highestLocked = -1;
                // Data changed we need to repeat the process
                if (!checkAndLockPredsErase(topLayer, highestLocked, preds, succs)) {
                    continue;
                }

                // Remove the node from the list
                for (int layer = topLayer - 1; layer >= 0; layer--) {
                    preds[layer]->nextList[layer] = nodeToDelete->nextList[layer];
                }
                nodeToDelete->lock.unlock();
                delete nodeToDelete;
                succs[levelFound] = nullptr;
                unlock(preds, highestLocked);
                // Return Iterator to next node (level 0 contains lst of all nodes)
                return Iterator(preds[0]->nextList[0]);
            } else {
                return end();
            }
        }
    }

    /**
     * @brief Find a exact match for the key in SkipList
     * @param[in] key Key to insert
     * @return Iterator to the element (or rSentinel if does not exists)
     */
    Iterator find(K key)
    {
        std::vector<Node<K, V> *> preds(maxHeight_), succs(maxHeight_);
        int levelFound = findNode(key, preds, succs);
        if (levelFound == -1) {
            return end();
        }

        if (succs[levelFound]) {
            if (!succs[levelFound]->fullyLinked || succs[levelFound]->marked) {
                return end();
            } else {
                return Iterator(succs[levelFound]);
            }
        }
        return end();
    }

    /**
     * @brief Find next greatest key in the SkipList
     * @param[in] key Key to insert
     * @return Iterator to the element (or rSentinel if does not exists)
     */
    Iterator upper_bound(K key)
    {
        std::vector<Node<K, V> *> preds(maxHeight_), succs(maxHeight_);
        int upperFound = findUpperBoundNode(key, preds, succs);
        if (upperFound == -1) {
            return end();
        }

        if (succs[upperFound]) {
            if (!succs[upperFound]->fullyLinked || succs[upperFound]->marked) {
                return end();
            } else {
                return Iterator(succs[upperFound]);
            }
        }
        return end();
    }

private:
    int maxHeight_;         // User defined max height
    Node<K, V> lSentinel_;  // left most node
    Node<K, V> rSentinel_;  // right most node

    /**
     * @brief Helper function to mark a node to delete
     * @param[in] nodeFound Node to add
     */
    void waitForLinking(Node<K, V> *nodeFound)
    {
        while (!nodeFound->fullyLinked) {
            ;
        }
    }

    /**
     * @brief Helper function to mark a node to delete
     * @param[in] nodeToDelete Node to delete
     * @param[in] isMarked mark the node for deletion
     * @return true if node is marked for deletion false otherwise
     */
    bool checkAndMarkNode(Node<K, V> *nodeToDelete, bool &isMarked)
    {
        if (!isMarked) {
            nodeToDelete->lock.lock();
            if (nodeToDelete->marked) {
                nodeToDelete->lock.unlock();
                return false;
            }
            nodeToDelete->marked = true;
            isMarked = true;
        }
        return true;
    }

    /**
     * @brief Unlocks all Predecessors of a Node after Insertion or Deletion
     * @param[in] vector of predecessors
     * @param[in] highestLocked highest level locked
     * @return Iterator to the element (or rSentinel if does not exists)
     */
    Status unlock(std::vector<Node<K, V> *> &preds, int highestLevelLocked)
    {
        // check the size of preds < highestLocked
        while (highestLevelLocked >= 0) {
            preds[highestLevelLocked--]->lock.unlock();
        }
        return Status::OK();
    }

    /**
     * @brief Find node and its adjacent nodes
     * @param[in] key of node to find
     * @param[out] vector of predecessors
     * @param[out] vector of successors
     * @return level in which node is found in successor array
     */
    int findNode(K key, std::vector<Node<K, V> *> &preds, std::vector<Node<K, V> *> &succs)
    {
        int levelFound = -1;  // Returns -1 if not found
        Node<K, V> *pred = &lSentinel_;

        // Highest level will have less nodes
        // Starting search from high levels then exploring
        // lower levels will decrease complexity to O(log n)
        for (int layer = maxHeight_ - 1; layer >= 0; layer--) {
            pred->lock.lock(); // This lock prevents erase to execute at the same time
            Node<K, V> *curr = pred->nextList[layer];

            // search until key is greater than current node
            while (curr != nullptr && pred != nullptr && !curr->marked && key > curr->key) {
                auto prevPred = pred;
                pred = curr;
                curr = pred->nextList[layer];
                prevPred->lock.unlock(); // Unlock the previous predecessor
                pred->lock.lock(); // Get lock for next one
            }

            // This case happens end the search and let the user retry
            if (curr == nullptr || pred == nullptr) {
                return levelFound;
            }

            // we found the layer that contains the key
            if (levelFound == -1 && !(curr->marked) && curr && key == curr->key) {
                levelFound = layer;
            }

            preds[layer] = pred;
            // When returned Successor array will have the found element
            succs[layer] = curr;
            // if not found in high level, search in lower levels
            pred->lock.unlock(); // unlock at predecessor in the end
        }
        return levelFound;
    }

    /**
     * @brief Find the next largest node and its adjacent nodes
     * @param[in] key of node to find
     * @param[out] vector of predecessors
     * @param[out] vector of successors
     * @return level in which node is found in successor array
     */
    int findUpperBoundNode(K key, std::vector<Node<K, V> *> &preds, std::vector<Node<K, V> *> &succs)
    {
        int levelFound = -1;  // Returns -1 if not found
        Node<K, V> *pred = &lSentinel_;

        // Highest level will have less nodes
        // Starting search from high levels then exploring
        // lower levels will decrease complexity to O(log n)
        for (int layer = maxHeight_ - 1; layer >= 0; layer--) {
            pred->lock.lock(); // This lock prevents erase to execute at the same time
            Node<K, V> *curr = pred->nextList[layer];
            // search until key is less than current node
            while (curr != nullptr && pred != nullptr && !curr->marked && key > curr->key) {
                auto prevPred = pred;
                pred = curr;
                curr = pred->nextList[layer];
                prevPred->lock.unlock(); // Unlock the previous predecessor
                pred->lock.lock(); // Get lock for next one
            }

            // This case happens end the search and let the user retry
            if (curr == nullptr || pred == nullptr) {
                return levelFound;
            }

            // record the level but do not stop the search until
            // all levels are visited
            if (curr->key != rSentinel_.key && !(curr->marked) && key <= curr->key) {
                levelFound = layer;
            }

            preds[layer] = pred;
            // When returned Successor array will have the found element
            succs[layer] = curr;
            pred->lock.unlock(); // unlock at predecessor the end
        }
        return levelFound;
    }

    /**
     * @brief Given the highest layer traverse through layers and lock all predecessors
     * @param[in] topLayer max number of layers in the node
     * @param[out] highestLevelLocked largest layer locked
     * @param[out] vector of predecessors
     * @param[out] vector of successors
     * @return True if valid false if need to repeat
     */
    bool checkAndLockPredsInsert(int topLayer, int &highestLevelLocked, std::vector<Node<K, V> *> &preds,
                                 std::vector<Node<K, V> *> &succs)
    {
        Node<K, V> *InsertPred, *InsertSucc, *prevPred = nullptr;
        bool valid = true;
        for (int layer = 0; valid && (layer < topLayer); ++layer) {
            InsertPred = preds[layer];
            InsertSucc = succs[layer];
            if (InsertPred != prevPred) {
                // We need this lock as we have to change nextList
                InsertPred->lock.lock();
                highestLevelLocked = layer;
                prevPred = InsertPred;
            }
            // Below function is same except for the valid check
            valid = !InsertPred->marked
                    && InsertSucc
                    && !InsertSucc->marked
                    && InsertPred->nextList[layer] == InsertSucc;
        }
        return valid;
    }

    /**
     * @brief Given the highest layer traverse through layers and lock all predecessors
     * @param[in] topLayer max number of layers in the node
     * @param[out] highestLevelLocked largest layer locked
     * @param[out] vector of predecessors
     * @param[out] vector of successors
     * @return True if valid false if need to repeat
     */
    bool checkAndLockPredsErase(int topLayer, int &highestLevelLocked, std::vector<Node<K, V> *> &preds,
                                std::vector<Node<K, V> *> &succs)
    {
        Node<K, V> *ErasePred, *EraseSucc, *prevPred = nullptr;
        bool valid = true;
        for (int layer = 0; valid && (layer < topLayer); ++layer) {
            ErasePred = preds[layer];
            EraseSucc = succs[layer];
            if (ErasePred != prevPred) {
                // We need this lock as we have to change nextList
                ErasePred->lock.lock();
                highestLevelLocked = layer;
                prevPred = ErasePred;
            }
            valid = !ErasePred->marked && EraseSucc && ErasePred->nextList[layer] == EraseSucc;
        }
        return valid;
    }

    /**
     * @brief Checks if node is not marked already for deletion
     * And fully linked
     * @param[in] candidate pointer to node
     * @param[in] levelFound where did we find this node in search
     * @return True if valid false if need to repeat
     */
    bool okToDelete(Node<K, V> *candidate, int levelFound)
    {
        if (!candidate)
            return true;
        return (candidate->fullyLinked && candidate->topLayer == levelFound + 1 && !candidate->marked);
    }

    /**
     * @brief Gets random height for insertion
     * Follows geometric distribution
     * This will ensure nodes at higher level are less than lower levels
     */
    int randomHeight()
    {
        static thread_local std::mt19937 rndNum(std::chrono::system_clock::now().time_since_epoch().count());
        const double prob = 0.5;
        int level = 1;
        while ((rndNum() / static_cast<double>(rndNum.max())) < prob && level < maxHeight_) {
            level++;
        }
        return level;
    }
};
}  // namespace datasystem

#endif