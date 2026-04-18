/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
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

#include "datasystem/common/util/numa_util.h"

#include <dirent.h>
#include <linux/mempolicy.h>
#include <sched.h>
#include <sys/syscall.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstdint>
#include <fstream>
#include <limits>
#include <string>
#include <utility>
#include <vector>

#include "datasystem/common/log/log.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"

namespace datasystem {
namespace {
struct NumaNodeInfo {
    int nodeId = -1;
    std::vector<int> cpus;
};

Status GetNumaNodesImpl(std::vector<NumaNodeInfo> &nodeInfos)
{
    constexpr const char *numaSysFsPath = "/sys/devices/system/node";
    DIR *dir = opendir(numaSysFsPath);
    CHECK_FAIL_RETURN_STATUS(dir != nullptr, K_RUNTIME_ERROR,
                             FormatString("Open %s failed: %s", numaSysFsPath, StrErr(errno)));

    std::vector<NumaNodeInfo> entries;
    struct dirent *entry = nullptr;
    while ((entry = readdir(dir)) != nullptr) {
        std::string nodeName(entry->d_name);
        constexpr size_t strLengthOfNode = 4;  // 4 is length of "node".
        if (nodeName.rfind("node", 0) != 0 || nodeName.size() <= strLengthOfNode) {
            continue;
        }
        int nodeId = -1;
        if (!StringToInt(nodeName.substr(strLengthOfNode), nodeId) || nodeId < 0) {
            continue;
        }

        std::ifstream in(std::string(numaSysFsPath) + "/" + nodeName + "/cpulist");
        if (!in.is_open()) {
            continue;
        }
        std::string cpuList;
        std::getline(in, cpuList);
        in.close();
        if (cpuList.empty()) {
            continue;
        }

        std::vector<int> cpus;
        if (!ParseCpuList(cpuList, cpus)) {
            (void)closedir(dir);
            RETURN_STATUS(K_RUNTIME_ERROR, FormatString("Invalid cpulist in %s: %s", nodeName, cpuList));
        }

        std::vector<int> filtered;
        filtered.reserve(cpus.size());
        for (int cpu : cpus) {
            if (cpu < 0 || cpu >= CPU_SETSIZE) {
                continue;
            }
            filtered.push_back(cpu);
        }
        if (!filtered.empty()) {
            entries.push_back({ nodeId, std::move(filtered) });
        }
    }

    (void)closedir(dir);
    std::sort(entries.begin(), entries.end(),
              [](const NumaNodeInfo &lhs, const NumaNodeInfo &rhs) { return lhs.nodeId < rhs.nodeId; });
    nodeInfos = std::move(entries);
    return Status::OK();
}

std::vector<NumaNodeInfo> &GetNumaNodes()
{
    static std::vector<NumaNodeInfo> cachedNodeInfos;
    if (cachedNodeInfos.empty()) {
        auto rc = GetNumaNodesImpl(cachedNodeInfos);
        if (rc.IsError()) {
            LOG_FIRST_N(ERROR, 1) << "GetNumaNodes failed: " << rc.ToString();
        }
    }
    return cachedNodeInfos;
}

Status TouchMemoryByPage(void *pointer, uint64_t size, uint64_t pageSize)
{
    auto *base = static_cast<uint8_t *>(pointer);
    for (uint64_t offset = 0; offset < size; offset += pageSize) {
        volatile uint8_t *touch = reinterpret_cast<volatile uint8_t *>(base + offset);
        *touch = *touch;
    }
    return Status::OK();
}

Status DistributeMemoryAcrossNumaNodeList(void *pointer, size_t size, const std::vector<int> &nodeIds)
{
    CHECK_FAIL_RETURN_STATUS(pointer != nullptr, K_INVALID, "memory pointer is null");
    CHECK_FAIL_RETURN_STATUS(size > 0, K_INVALID, "memory size is zero");
    CHECK_FAIL_RETURN_STATUS(!nodeIds.empty(), K_INVALID, "NUMA node list is empty");
    long pageSizeRaw = getpagesize();
    CHECK_FAIL_RETURN_STATUS(pageSizeRaw > 0, K_RUNTIME_ERROR, "Page size is invalid");
    auto pageSize = static_cast<uint64_t>(pageSizeRaw);
    constexpr int maxNumaNodeCount = 32;
    for (int nodeId : nodeIds) {
        CHECK_FAIL_RETURN_STATUS(
            nodeId >= 0 && nodeId < maxNumaNodeCount, K_INVALID,
            FormatString("NUMA node id %d exceeds supported range [0, %d)", nodeId, maxNumaNodeCount));
    }

    auto begin = reinterpret_cast<uintptr_t>(pointer);
    auto end = begin + static_cast<uint64_t>(size);
    CHECK_FAIL_RETURN_STATUS(end >= begin, K_INVALID, "memory range overflow");
    CHECK_FAIL_RETURN_STATUS(end <= std::numeric_limits<uint64_t>::max() - (pageSize - 1), K_INVALID,
                             "memory range alignment overflow");
    auto alignedBegin = (begin / pageSize) * pageSize;
    auto alignedEnd = ((end + pageSize - 1) / pageSize) * pageSize;
    auto alignedLen = alignedEnd - alignedBegin;
    CHECK_FAIL_RETURN_STATUS(alignedLen > 0, K_INVALID, "aligned memory length is zero");
    constexpr uint64_t interleaveChunkBytes = 1ULL << 30;  // 1GB
    auto *alignedPtr = reinterpret_cast<uint8_t *>(alignedBegin);

#ifdef SYS_mbind
    for (uint64_t chunkOffset = 0, attempt = 0, nextStartNodeIdx = 0; chunkOffset < alignedLen;) {
        const uint64_t chunkSize = std::min(interleaveChunkBytes, alignedLen - chunkOffset);
        const size_t nodeIdx = static_cast<size_t>((nextStartNodeIdx + attempt) % nodeIds.size());
        const unsigned long nodeMask = (1UL << static_cast<unsigned long>(nodeIds[nodeIdx]));
        int rc = syscall(SYS_mbind, alignedPtr + chunkOffset, static_cast<unsigned long>(chunkSize), MPOL_PREFERRED,
                         &nodeMask, static_cast<unsigned long>(maxNumaNodeCount), 0UL);
        if (rc == 0) {
            RETURN_IF_NOT_OK(TouchMemoryByPage(alignedPtr + chunkOffset, chunkSize, pageSize));
            nextStartNodeIdx = (static_cast<uint64_t>(nodeIdx) + 1) % static_cast<uint64_t>(nodeIds.size());
            chunkOffset += interleaveChunkBytes;
            attempt = 0;
            continue;
        }

        ++attempt;
        CHECK_FAIL_RETURN_STATUS(
            attempt < nodeIds.size(), K_RUNTIME_ERROR,
            FormatString("mbind failed after trying all NUMA nodes, chunkOffset=%lu, chunkSize=%lu, last errno=%s",
                         chunkOffset, chunkSize, StrErr(errno)));
    }
#else
    RETURN_STATUS(K_RUNTIME_ERROR, "SYS_mbind is unavailable on current platform");
#endif
    return Status::OK();
}
}  // namespace

bool ParseCpuList(const std::string &cpuListRaw, std::vector<int> &cpus)
{
    const auto cpuList = Trim(cpuListRaw);
    if (cpuList.empty()) {
        return false;
    }
    auto segments = SplitToUniqueStr(cpuList, ",");
    for (const auto &segmentRaw : segments) {
        auto segment = Trim(segmentRaw);
        if (segment.empty()) {
            continue;
        }
        auto dashPos = segment.find('-');
        if (dashPos == std::string::npos) {
            int cpu = 0;
            if (!StringToInt(segment, cpu) || cpu < 0) {
                return false;
            }
            cpus.push_back(cpu);
            continue;
        }
        int beginCpu = 0;
        int endCpu = 0;
        if (!StringToInt(segment.substr(0, dashPos), beginCpu) || !StringToInt(segment.substr(dashPos + 1), endCpu)
            || beginCpu < 0 || endCpu < beginCpu) {
            return false;
        }
        for (int cpu = beginCpu; cpu <= endCpu; ++cpu) {
            cpus.push_back(cpu);
        }
    }
    std::sort(cpus.begin(), cpus.end());
    return !cpus.empty();
}

Status DistributeMemoryAcrossAllNumaNodes(void *pointer, size_t size)
{
    auto &nodeInfos = GetNumaNodes();
    if (nodeInfos.empty()) {
        return Status::OK();
    }
    std::vector<int> nodeIds;
    nodeIds.reserve(nodeInfos.size());
    for (const auto &nodeInfo : nodeInfos) {
        nodeIds.push_back(nodeInfo.nodeId);
    }
    return DistributeMemoryAcrossNumaNodeList(pointer, size, nodeIds);
}

Status DistributeMemoryAcrossAffinityNumaNodes(void *pointer, size_t size)
{
    auto &nodeInfos = GetNumaNodes();
    if (nodeInfos.empty()) {
        return Status::OK();
    }

    cpu_set_t processSet;
    CPU_ZERO(&processSet);
    CHECK_FAIL_RETURN_STATUS(sched_getaffinity(0, sizeof(processSet), &processSet) == 0, K_RUNTIME_ERROR,
                             FormatString("sched_getaffinity failed: %s", StrErr(errno)));

    std::vector<int> affinityNodeIds;
    affinityNodeIds.reserve(nodeInfos.size());
    for (const auto &nodeInfo : nodeInfos) {
        bool hasAffinityCpu = false;
        for (int cpu : nodeInfo.cpus) {
            if (CPU_ISSET(cpu, &processSet)) {
                hasAffinityCpu = true;
                break;
            }
        }
        if (hasAffinityCpu) {
            affinityNodeIds.push_back(nodeInfo.nodeId);
        }
    }

    CHECK_FAIL_RETURN_STATUS(!affinityNodeIds.empty(), K_RUNTIME_ERROR,
                             "No NUMA node intersects with current process affinity cpuset");
    return DistributeMemoryAcrossNumaNodeList(pointer, size, affinityNodeIds);
}

uint8_t NumaIdToChipId(uint8_t numaId, size_t numaCount)
{
    constexpr uint8_t chipId1 = 1;
    constexpr uint8_t chipId2 = 2;
    if (numaId == INVALID_NUMA_ID) {
        return INVALID_CHIP_ID;
    }

    if (numaCount == 0) {
        auto &nodeInfos = GetNumaNodes();
        if (nodeInfos.empty()) {
            return INVALID_CHIP_ID;
        }
        numaCount = nodeInfos.size();
    }

    if (numaId >= static_cast<uint8_t>(numaCount)) {
        return INVALID_CHIP_ID;
    }

    const size_t firstHalfCount = (numaCount + 1) / 2;
    return (numaId < firstHalfCount) ? chipId1 : chipId2;
}
}  // namespace datasystem
