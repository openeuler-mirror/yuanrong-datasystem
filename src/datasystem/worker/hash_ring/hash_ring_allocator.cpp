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
#include "datasystem/worker/hash_ring/hash_ring_allocator.h"

#include <algorithm>
#include <functional>

#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/util/container_util.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/worker/hash_ring/hash_ring_tools.h"

namespace datasystem {
namespace worker {
int HashRingAllocator::defaultHashTokenNum = 4;

HashRingPb HashRingAllocator::GenerateAllHashTokens(HashRingPb oldRing, const std::set<std::string> &sortedWorkers)
{
    uint32_t workerNum = sortedWorkers.size();
    uint32_t hashTokenNum = workerNum * static_cast<unsigned int>(defaultHashTokenNum);
    uint32_t step = UINT32_MAX / hashTokenNum;
    std::vector<std::vector<uint32_t>> tokens(workerNum);
    std::vector<uint32_t> ringTokens;
    uint32_t sum = 0;
    for (uint32_t i = 0; i < hashTokenNum; i++) {
        sum += step;
        tokens[i % workerNum].push_back(sum);
        ringTokens.push_back(sum);
    }
    uint32_t i = 0;

    for (const auto &workerAddr : sortedWorkers) {
        oldRing.mutable_workers()->at(workerAddr).set_state(WorkerPb::ACTIVE);
        for (auto hashToken : tokens[i]) {
            oldRing.mutable_workers()->at(workerAddr).mutable_hash_tokens()->Add(std::move(hashToken));
            INJECT_POINT("hashring.RemoveToken", [&](std::string addr, int upper) {
                auto tokens = oldRing.mutable_workers()->at(workerAddr).mutable_hash_tokens();
                if (workerAddr == addr && tokens->size() > upper) {
                    tokens->RemoveLast();
                }
                return oldRing;
            });
        }
        i++;
    }
    oldRing.set_cluster_has_init(true);
    LOG(INFO) << "Generate hash tokens: " << VectorToString(ringTokens);
    return oldRing;
}

HashRingAllocator::HashRingAllocator(const HashRingPb &oldRing)
{
    ring_.reserve(oldRing.workers_size() * defaultHashTokenNum);
    for (auto &worker : oldRing.workers()) {
        if (oldRing.del_node_info().find(worker.first) != oldRing.del_node_info().end()) {
            // the fault node can not undertake new ranges
            continue;
        }
        for (auto token : worker.second.hash_tokens()) {
            bool isNewAdd = worker.second.state() != WorkerPb::ACTIVE && worker.second.state() != WorkerPb::LEAVING;
            ring_.emplace_back(RingNode{ token, worker.first, isNewAdd });
        }
    }

    std::sort(ring_.begin(), ring_.end(), [](const RingNode &l, const RingNode &r) { return l.token < r.token; });
    ownerships_ = GetOwnerShip();
}

#ifdef WITH_TESTS
void HashRingAllocator::AddNode(const std::string &newNodeId, std::initializer_list<uint32_t> tokens)
{
    for (auto token : tokens) {
        ring_.emplace_back(RingNode{ token, newNodeId, true });
    }
    std::sort(ring_.begin(), ring_.end(), [](const RingNode &l, const RingNode &r) { return l.token < r.token; });
}
#endif

Status HashRingAllocator::AddNode(const std::string &newNodeId, uint32_t tokenNum, std::vector<uint32_t> &newTokens)
{
    if (ring_.size() <= 1) {
        return Status(K_RUNTIME_ERROR, "hash ring needs to be initialized first.");
    }

    // Split Algorithm:
    // 1. For each token of the adding node, first search the node which has largest ownership on the ring, then take
    // away its largest range in proportion from the front.
    // 2. The proportion uses to take away is set a bit bigger than 0.5 to keep a smaller variance in most scenarios.
    const double adoptRatio = 0.55;
    uint32_t largestNodeIndex{ 0 };
    uint32_t largestRange{ 0 };
    for (auto i = 0u; i < tokenNum; i++) {
        auto targetSplitNodeId = std::max_element(ownerships_.begin(), ownerships_.end(), SortByValue{})->first;

        FindLargestRange(targetSplitNodeId, largestNodeIndex, largestRange);
        uint32_t adoptLength = static_cast<uint32_t>(adoptRatio * largestRange);
        HashPosition token = Rewind(ring_[largestNodeIndex].token, largestRange - adoptLength);

        VLOG(1) << FormatString("Split range of %s: Add token between %s and %s", targetSplitNodeId,
                                ring_[PrevIdx(largestNodeIndex)], ring_[largestNodeIndex]);

        newTokens.emplace_back(token);
        Insert(largestNodeIndex, RingNode{ token, newNodeId, true });

        ownerships_[targetSplitNodeId] -= adoptLength;
        ownerships_[newNodeId] += adoptLength;
    }

    return Status::OK();
}

void HashRingAllocator::GetAddNodeInfo(std::map<std::string, ChangeNodePb> &addnodeInfo)
{
    for (auto it = ring_.begin(); it != ring_.end(); it++) {
        if (!it->newAdd) {
            continue;
        }

        ChangeNodePb::RangePb range;

        auto tmp = LoopNext(ring_, it);
        while (tmp != it) {
            if (!tmp->newAdd) {
                break;
            }
            tmp = LoopNext(ring_, tmp);
        }

        range.set_workerid(tmp->nodeId);             // next non-newly added node
        range.set_from(LoopPrev(ring_, it)->token);  // the token of prev node
        range.set_end(it->token);                    // newly add token
        range.set_finished(false);
        addnodeInfo[it->nodeId].mutable_changed_ranges()->Add(std::move(range));
    }
}

void HashRingAllocator::ProcessAddNodeInfoWhenJoiningNodeRemove(const RingNode &node, ChangeNodePb::RangePb &range,
                                                                HashRingPb &hashRing) const
{
    auto findRangeByToken = [](auto ranges, uint32_t token) {
        for (auto i = ranges->begin(); i != ranges->end(); i++) {
            if (i->end() == token) {
                return i;
            }
        }
        return ranges->end();
    };

    // if the faulty node is joining, we should process with its non-finished add_node_info
    auto addRangeOfThisNewNode = hashRing.mutable_add_node_info()->find(node.nodeId);
    if (addRangeOfThisNewNode == hashRing.mutable_add_node_info()->end()) {
        return;
    }
    // may need to add to add_node_info if next node is not the source node of add_node_info
    auto changeRanges = addRangeOfThisNewNode->second.mutable_changed_ranges();
    auto rangeOfThisToken = findRangeByToken(changeRanges, node.token);
    if (rangeOfThisToken == changeRanges->end()) {
        LOG(ERROR) << "The range not found in add_node_info. token=" << node.token;
        return;
    }

    if (!rangeOfThisToken->finished()) {
        range.set_lost_all_range(false);
        // if next is not its source and the range not finished, change the owner of this add_node_info range to
        // next node
        if (rangeOfThisToken->workerid() != range.workerid()) {
            (*hashRing.mutable_add_node_info())[range.workerid()].mutable_changed_ranges()->Add(
                ChangeNodePb::RangePb{ *rangeOfThisToken });
        }
    }

    changeRanges->erase(rangeOfThisToken);
}

HashRingPb HashRingAllocator::EraseDelNodeInfoTask(const std::string &nodeId, const HashRingPb &ring)
{
    // erase the finished range.
    HashRingPb ringAfterClear = ring;
    ringAfterClear.clear_del_node_info();
    for (auto &node : ring.del_node_info()) {
        for (auto range : node.second.changed_ranges()) {
            if (range.workerid() != nodeId) {
                (*ringAfterClear.mutable_del_node_info())[node.first].mutable_changed_ranges()->Add(std::move(range));
            }
        }
    }
    for (auto &oldWorkerInDelNodeInfo : ring.del_node_info()) {
        // In the continuous scale down scenario, if the old hashRange can be overridden
        // by the latest scale down node's hashRange, clear the information of old node in etcd directly.
        if (ringAfterClear.del_node_info().find(oldWorkerInDelNodeInfo.first) == ringAfterClear.del_node_info().end()) {
            ringAfterClear.mutable_workers()->erase(oldWorkerInDelNodeInfo.first);
        }
    }
    return ringAfterClear;
}

ChangeNodePb::RangePb HashRingAllocator::ConstructChangeNode(uint32_t tokenBegin, uint32_t tokenEnd,
                                                             const std::string &workerId)
{
    ChangeNodePb::RangePb range;
    range.set_from(tokenBegin);
    range.set_end(tokenEnd);
    // 1. For the add_node_info, the workerid is the source node to send migratory meta data
    // 2. For the del_node_info, the workerid is the node to get meta data from l2cache.
    range.set_workerid(workerId);
    range.set_finished(false);
    range.set_lost_all_range(true);
    return range;
}

Status HashRingAllocator::GetNextNode(const std::string &nodeId, const AddRangeFunc &addRange,
                                      const std::unordered_set<std::string> &excludeNodeIds)
{
    std::function<bool(const RingNode &)> isTarget = [nodeId](const RingNode &node) { return node.nodeId == nodeId; };
    std::function<bool(const RingNode &)> isNext = [&](const RingNode &node) {
        return node.nodeId != nodeId && excludeNodeIds.find(node.nodeId) == excludeNodeIds.end();
    };

    auto ringItr = ring_.begin();
    auto prevItr = ringItr;
    RingNode currProcessingToken;
    while (ringItr != ring_.end()) {
        ringItr = std::find_if(ringItr, ring_.end(), isTarget);
        if (ringItr == ring_.end()) {
            break;
        }
        if (ringItr == ring_.begin() && LoopPrev(ring_, ringItr)->nodeId == nodeId) {
            ++ringItr;
            continue;
        }

        currProcessingToken = *ringItr;
        prevItr = LoopPrev(ring_, ringItr);
        ringItr = std::find_if(ringItr, ring_.end(), isNext);
        if (ringItr == ring_.end()) {
            ringItr = std::find_if(ring_.begin(), ringItr, isNext);
            if (ringItr == ring_.end()) {
                return Status(K_RUNTIME_ERROR, "No available nodes in the ring.");
            }
            addRange(prevItr->token, LoopPrev(ring_, ringItr)->token, ringItr->nodeId, currProcessingToken);
            break;
        }
        addRange(prevItr->token, LoopPrev(ring_, ringItr)->token, ringItr->nodeId, currProcessingToken);
    }
    return Status::OK();
}

void HashRingAllocator::FinishAddNodeInfoIfNeed(HashRingPb &ring)
{
    // check if scale-up migration is complete
    bool isAllNodeFinished = true;
    std::vector<std::string> scaleDownWorkers;
    for (auto &i : ring.add_node_info()) {
        for (auto &range : i.second.changed_ranges()) {
            if (!range.finished()) {
                isAllNodeFinished = false;
                break;
            }
            if (range.from() == range.end() && !range.is_upgrade()) {
                scaleDownWorkers.emplace_back(range.workerid());
            }
        }
    }
    // if scale-up completes, clear add_node_info and set all joining nodes to active.
    if (!isAllNodeFinished) {
        return;
    }
    ring.clear_add_node_info();
    for (auto &worker : *ring.mutable_workers()) {
        if (worker.second.state() == WorkerPb::JOINING) {
            worker.second.set_state(WorkerPb::ACTIVE);
        }
    }
    return;
}

Status HashRingAllocator::UpdateHashWhenSrcWorkerFailed(const std::string &worker, HashRingPb &ring,
                                                        bool &isVoluntaryNodeAddInfoExist)
{
    for (auto &add_node_info : (*ring.mutable_add_node_info())) {
        for (auto &range : (*add_node_info.second.mutable_changed_ranges())) {
            range.set_finished(true);
            // save add node info to check data and meta.
            if (range.workerid() == worker) {
                auto failId = range.workerid();
                auto delRange = (*ring.mutable_del_node_info())[failId].mutable_changed_ranges()->Add();
                delRange->set_lost_all_range(false);
                delRange->set_finished(false);
                delRange->set_workerid(add_node_info.first);
                delRange->set_from(range.from());
                delRange->set_end(range.end());
                if (range.from() == range.end() && !range.is_upgrade()) {
                    // if range == end, is voluntary scale down add node info.
                    isVoluntaryNodeAddInfoExist = true;
                }
            }
        }
    }
    // all add node finished, clear token range.
    FinishAddNodeInfoIfNeed(ring);
    return Status::OK();
}

Status HashRingAllocator::RemoveNode(const std::string &nodeId, HashRingPb &hashRing)
{
    CHECK_FAIL_RETURN_STATUS(ownerships_.size() >= 1, K_RUNTIME_ERROR, "There are not enough nodes.");
    bool faultNodeIsNewNode = hashRing.add_node_info().find(nodeId) != hashRing.add_node_info().end();
    bool oldNodeFaultWhileAdding =
        !hashRing.add_node_info().empty() && hashRing.add_node_info().find(nodeId) == hashRing.add_node_info().end();
    bool nodeIsLeavingAndAddInfoExist = false;

    if (oldNodeFaultWhileAdding) {
        // when src worker failed, if src node is voluntary scale down node, erase add node info
        // if src node is voluntary scale down node, dest node recover data need to migrate.
        UpdateHashWhenSrcWorkerFailed(nodeId, hashRing, nodeIsLeavingAndAddInfoExist);
    }

    AddRangeFunc addRange = [&](uint32_t rangeBegin, uint32_t rangeEnd, std::string &nextWorker, RingNode &currNode) {
        // no matter the faulty node is joining or running, it should pass the range to next node. so write the
        // del_node_info here.
        // del_node_info need not to change owner, its owner can be an existed node or a joining node
        // when voluntary scale down node failed, generate del node info and set lost all range false.
        ChangeNodePb::RangePb range = ConstructChangeNode(rangeBegin, rangeEnd, nextWorker);
        ProcessAddNodeInfoWhenJoiningNodeRemove(currNode, range, hashRing);
        (*hashRing.mutable_del_node_info())[nodeId].mutable_changed_ranges()->Add(std::move(range));
    };
    // when node removes, its ranges will tansfer to next node
    if (!nodeIsLeavingAndAddInfoExist) {
        RETURN_IF_NOT_OK(GetNextNode(nodeId, addRange));
    }
    // if the faulty node has recovery task of other, delete.
    hashRing = EraseDelNodeInfoTask(nodeId, hashRing);
    if (faultNodeIsNewNode) {
        AddDelNodeInfoVoluntaryScaleDestNodeDown(hashRing, nodeId);
        hashRing.mutable_add_node_info()->erase(nodeId);
        FinishAddNodeInfoIfNeed(hashRing);
    }

    auto iter = hashRing.mutable_workers()->find(nodeId);
    if (iter != hashRing.mutable_workers()->end()) {
        if (iter->second.hash_tokens().empty() && iter->second.state() == WorkerPb::LEAVING) {
            if (iter->second.worker_uuid().empty()) {
                hashRing.mutable_workers()->erase(iter);
            } else {
                hashRing.mutable_del_node_info()->insert({ nodeId, {} });
            }
        }
    }
    auto isTarget = [nodeId](const RingNode &node) { return node.nodeId == nodeId; };
    (void)EraseIf(ring_, isTarget);
    ownerships_ = GetOwnerShip();
    return Status::OK();
}

void HashRingAllocator::AddDelNodeInfoVoluntaryScaleDestNodeDown(HashRingPb &hashRing, const std::string &failedId)
{
    std::set<std::string> voluntaryWorkers;
    for (const auto &info : hashRing.workers()) {
        if (info.second.state() == WorkerPb::LEAVING) {
            voluntaryWorkers.emplace(info.first);
        }
    }
    auto iter = hashRing.add_node_info().find(failedId);
    if (iter != hashRing.del_node_info().end()) {
        for (const auto &changeRange : iter->second.changed_ranges()) {
            if (voluntaryWorkers.find(changeRange.workerid()) != voluntaryWorkers.end()) {
                auto range = changeRange;
                range.set_lost_all_range(false);
                (*hashRing.mutable_del_node_info())[failedId].mutable_changed_ranges()->Add(std::move(range));
            }
        }
    }
}

Status HashRingAllocator::RemoveNodeVoluntarily(const std::string &workerId, const std::string &standbyWorkerId,
                                                uint32_t hashVal, const std::unordered_set<std::string> &excludeAddrs,
                                                HashRingPb &hashRing)
{
    // Add hash range migratory information to add_node_info
    AddRangeFunc addRange = [&](uint32_t rangeBegin, uint32_t rangeEnd, std::string &nextWorker,
                                RingNode & /* currNode */) {
        ChangeNodePb::RangePb range = ConstructChangeNode(rangeBegin, rangeEnd, workerId);
        // The key of the add_node_info is the destination node to receive meta data
        (*hashRing.mutable_add_node_info())[nextWorker].mutable_changed_ranges()->Add(std::move(range));
    };

    RETURN_IF_NOT_OK(GetNextNode(workerId, addRange, excludeAddrs));

    if (standbyWorkerId.empty()) {
        // no need to add uuid range
        return Status::OK();
    }

    // Add uuid migratory information to add_node_info, using same hash value represent the uuid migration.
    ChangeNodePb::RangePb range = ConstructChangeNode(hashVal, hashVal, workerId);
    INJECT_POINT("StandbyWorkerNotSame", [&] {
        for (const auto &workerInfo : hashRing.workers()) {
            if (workerInfo.first != hashRing.add_node_info().begin()->first && workerInfo.first != workerId) {
                (*hashRing.mutable_add_node_info())[workerInfo.first].mutable_changed_ranges()->Add(std::move(range));
                break;
            }
        }
        return Status::OK();
    });
    (*hashRing.mutable_add_node_info())[standbyWorkerId].mutable_changed_ranges()->Add(std::move(range));
    return Status::OK();
}

void HashRingAllocator::Print() const
{
    VLOG(1) << "Ring as like: " << VectorToString(ring_);
    VLOG(1) << "OwnerShip: " << MapToString(ownerships_);
}

void HashRingAllocator::Insert(uint32_t index, RingNode &&node)
{
    if (index != 0) {
        ring_.insert(ring_.begin() + index, std::move(node));
        return;
    }

    if (node.token < ring_[0].token) {
        ring_.insert(ring_.begin(), std::move(node));
    } else {
        ring_.emplace_back(std::move(node));
    }
}

HashPosition HashRingAllocator::Rewind(HashPosition pos, uint32_t distance) const
{
    if (pos < distance) {
        return RING_MAX - (distance - pos);
    }

    return pos - distance;
}

uint32_t HashRingAllocator::PrevIdx(uint32_t index) const
{
    if (ring_.size() == 0) {
        LOG(ERROR) << "Get PrevIdx of an empty ring.";
        return 0;
    }

    if (index == 0) {
        return ring_.size() - 1;
    }

    return index - 1;
}

uint32_t HashRingAllocator::Distance(uint32_t index) const
{
    if (index == 0) {
        return (RING_MAX - ring_.rbegin()->token) + (ring_.begin()->token - RING_MIN);
    }

    return ring_[index].token - ring_[index - 1].token;
}

void HashRingAllocator::FindLargestRange(const std::string &nodeId, uint32_t &largestNodeIndex,
                                         uint32_t &largestRange) const
{
    largestNodeIndex = 0;
    largestRange = 0;
    uint32_t tmpRange{ 0 };
    for (auto i = 0u; i < ring_.size(); i++) {
        if (ring_[i].nodeId != nodeId) {
            continue;
        }

        tmpRange = Distance(i);
        if (tmpRange > largestRange) {
            largestRange = tmpRange;
            largestNodeIndex = i;
        }
    }
}

std::map<std::string, uint32_t> HashRingAllocator::GetOwnerShip() const
{
    std::map<std::string, uint32_t> ownerships;
    for (auto i = 0u; i < ring_.size(); i++) {
        ownerships[ring_[i].nodeId] += Distance(i);
    }
    return ownerships;
}

}  // namespace worker
}  // namespace datasystem