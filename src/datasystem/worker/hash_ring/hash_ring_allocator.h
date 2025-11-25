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
 * Description: Hash ring allocator, responsible for tokens modification algorithm
 */
#ifndef DATASYSTEM_WORKER_HASH_RING_ALLOCATOR_H
#define DATASYSTEM_WORKER_HASH_RING_ALLOCATOR_H

#include <cstdint>
#include <map>
#include <vector>

#include "datasystem/common/util/meta_route_tool.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/protos/hash_ring.pb.h"

namespace datasystem {
namespace worker {
struct SimplePairHash {
public:
    std::size_t operator()(const Range &range) const
    {
        return range.first ^ range.second;
    }
};

struct RangeEqual {
public:
    bool operator()(const Range &range, const Range &range1) const
    {
        return range.first == range1.first && range.second == range1.second;
    }
};

using HashRange = std::vector<Range>;

template <typename Container, typename It>
It LoopPrev(Container &c, It it)
{
    if (it == c.begin()) {
        return std::prev(c.end());
    }

    return std::prev(it);
}

template <typename Container, typename It>
It LoopNext(Container &c, It it)
{
    if (it == c.end() || std::next(it) == c.end()) {
        return c.begin();
    }

    return std::next(it);
}

class HashRingAllocator {
    static constexpr HashPosition RING_MIN = 0;
    static constexpr HashPosition RING_MAX = UINT32_MAX;

    struct RingNode {
        HashPosition token;  // a point on the ring
        std::string nodeId;  // now we use workerIP as physical nodeId
        bool newAdd{ false };

        friend std::ostream &operator<<(std::ostream &os, const RingNode &n)
        {
            os << "[" << n.nodeId << ":" << n.token << "]";
            return os;
        }
    };

public:
    /**
     * @brief Init hash tokens to specifit workers averagely.
     * @param[in] oldRing The old ring that includes worker info.
     * @param[in] sortedWorkers The workers need to be inited, it is sorted by workerId.
     * @return Return the new HashRingPb.
     */
    static HashRingPb GenerateAllHashTokens(HashRingPb oldRing, const std::set<std::string> &sortedWorkers);

    /**
     * @brief Mark addNodeInfo of input workerAddr finished and delete the whole addNodeInfo if all ranges are finished.
     * @param[out] ring The ring.
     */
    static void FinishAddNodeInfoIfNeed(HashRingPb &ring);

    /**
     * @brief Init hash tokens to specifit workers averagely.
     * @param[in] oldRing The old ring that includes worker info.
     */
    explicit HashRingAllocator(const HashRingPb &oldRing);

    /**
     * @brief Add node into a working ring.
     * @param[in] newNodeId The id of new node.
     * @param[in] tokenNum The number of points this node will settle on the ring.
     * @param[out] newTokens The tokens of new worker.
     * @return Status of the call.
     */
    Status AddNode(const std::string &newNodeId, uint32_t tokenNum, std::vector<uint32_t> &newTokens);

    /**
     * @brief Update hash when src node failed.
     * @param[in] worker The failed worker address.
     * @param[out] ring The old ring to update.
     * @param[out] isVoluntaryNodeAddInfoExist worker is voluntary scale down worker and addnodeinfo exist.
     * @return Status of the call.
     */
    Status UpdateHashWhenSrcWorkerFailed(const std::string &worker, HashRingPb &ring,
                                         bool &isVoluntaryNodeAddInfoExist);

#ifdef WITH_TESTS
    /**
     * @brief Add node into a working ring.
     * @param[in] newNodeId The id of new node.
     * @param[in] tokens The tokens to be added.
     * @return Status of the call.
     */
    void AddNode(const std::string &newNodeId, std::initializer_list<uint32_t> tokens);
#endif

    /**
     * @brief Get change ranges of nodes after adding.
     * @param[out] addnodeInfo The change ranges.
     */
    void GetAddNodeInfo(std::map<std::string, ChangeNodePb> &addnodeInfo);

    /**
     * @brief Remove node from a working ring.
     * @param[in] nodeId The node to be removed.
     * @param[out] hashRing The changed hash ring.
     * @return Status of the call.
     */
    Status RemoveNode(const std::string &nodeId, HashRingPb &hashRing);

    /**
     * @brief Remove worker from a working ring automatically.
     * @param[in] workerId The worker to be removed.
     * @param[in] workerUuid The worker's uuid.
     * @param[in] excludeAddrs The exclude worker address.
     * @param[out] hashRing The changed hash ring.
     * @return Status of the call.
     */
    Status RemoveNodeVoluntarily(const std::string &workerId, const std::string &standbyWorkerId, uint32_t hashVal,
                                 const std::unordered_set<std::string> &excludeAddrs, HashRingPb &hashRing);

    /**
     * @brief Print information of this ring.
     */
    void Print() const;

    /**
     * @brief Get ownership of each node on the ring.
     * @return Ownership map.
     */
    std::map<std::string, uint32_t> GetOwnerShip() const;

    static int defaultHashTokenNum;

private:
    /**
     * @brief Insert a node into current ring.
     * @param[in] index Indicate where to insert.
     * @param[in] node The node to be inserted.
     */
    void Insert(uint32_t index, RingNode &&node);

    /**
     * @brief Calculate the position after rewind.
     * @param[in] pos Indicate where to rewind from.
     * @param[in] distance Indicate the size to be rewinded.
     * @return The position after rewind.
     */
    HashPosition Rewind(HashPosition pos, uint32_t distance) const;

    /**
     * @brief Get the previous index.
     * @param[in] index The node index in the ring.
     * @return The previous index.
     */
    uint32_t PrevIdx(uint32_t index) const;

    /**
     * @brief Calculate the distance from its previous node.
     * @param[in] index The node index in the ring.
     * @return The distance from its previous node.
     */
    uint32_t Distance(uint32_t index) const;

    /**
     * @brief Add del node info when voluntary scale down dest node failed
     * @param[in] hashRing hash ring info
     * @param[in] failedId failed worekr address
     */
    void AddDelNodeInfoVoluntaryScaleDestNodeDown(HashRingPb &hashRing, const std::string &failedId);

    /**
     * @brief Search the largest range of specific node.
     * @param[in] nodeId Indicate the node.
     * @param[out] largestNodeIndex The largest range index in the ring.
     * @param[out] largestRange The largest range of specific node.
     */
    void FindLargestRange(const std::string &nodeId, uint32_t &largestNodeIndex, uint32_t &largestRange) const;

    /**
     * @brief process the add_node_info if the faulty node is joining
     * @param[in] node The current processing node
     * @param[out] range The range to be writed into del_node_info
     * @param[out] hashRing The hash ring after modification
     */
    void ProcessAddNodeInfoWhenJoiningNodeRemove(const RingNode &node, ChangeNodePb::RangePb &range,
                                                 HashRingPb &hashRing) const;

    /**
     * @brief remove the recovery task of specific node in del_node_info
     * @param[in] nodeId The processing node
     * @param[in] hashRing The hash ring to be modificated
     * @return The hash ring after modification
     */
    HashRingPb EraseDelNodeInfoTask(const std::string &nodeId, const HashRingPb &ring);

    /**
     * @brief Construct the ChangeNode for the add_node_info or del_node_info
     * @param[in] tokenBegin The begin of the hash range
     * @param[in] tokenEnd The end of the hash range
     * @param[in] workerId The worker of the workerid field
     * @return The data structure of the ChangeNode
     */
    ChangeNodePb::RangePb ConstructChangeNode(uint32_t tokenBegin, uint32_t tokenEnd, const std::string &workerId);

    using AddRangeFunc =
        std::function<void(uint32_t rangeBegin, uint32_t rangeEnd, std::string &nextWorker, RingNode &currNode)>;
    /**
     * @brief Get next node of nodeId in the ring
     * @param[in] nodeId The target node
     * @param[in] addRange The function that used to operate hash ring according to the range and next node
     * @param[in] excludeNodeIds The exclude node ids.
     */
    Status GetNextNode(const std::string &nodeId, const AddRangeFunc &addRange,
                     const std::unordered_set<std::string> &excludeNodeIds = {});

    std::vector<RingNode> ring_;

    std::map<std::string, uint32_t> ownerships_;
};
}  // namespace worker
}  // namespace datasystem
#endif
