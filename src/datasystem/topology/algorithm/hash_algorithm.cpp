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

/**
 * Description: Hash topology routing and planning algorithm.
 */
#include "datasystem/topology/algorithm/hash_algorithm.h"

#include <algorithm>
#include <limits>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "datasystem/common/util/hash_algorithm.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {
namespace topology {
namespace {

constexpr char HASH_UNIT_TYPE[] = "hash-token";
constexpr char TOKEN_SEED_SEPARATOR[] = "#";
constexpr uint32_t DECIMAL_BASE = 10;
constexpr uint32_t MAX_TOKEN_PROBE_NUM = 1024;

struct TokenOwner {
    uint32_t token{ 0 };
    WorkerId workerId;
};

class HashRoutingState final : public AlgorithmRoutingState {
public:
    std::vector<TokenOwner> owners;
};

bool IsKnownState(WorkerTopologyState state)
{
    switch (state) {
        case WorkerTopologyState::INITIAL:
        case WorkerTopologyState::JOINING:
        case WorkerTopologyState::ACTIVE:
        case WorkerTopologyState::LEAVING:
            return true;
        default:
            return false;
    }
}

Status ParseUint32(const std::string &value, uint32_t &token)
{
    CHECK_FAIL_RETURN_STATUS(!value.empty(), K_INVALID, "hash token is empty");
    uint64_t parsed = 0;
    for (unsigned char ch : value) {
        CHECK_FAIL_RETURN_STATUS(ch >= '0' && ch <= '9', K_INVALID, "hash token is not decimal");
        parsed = parsed * DECIMAL_BASE + static_cast<uint64_t>(ch - '0');
        CHECK_FAIL_RETURN_STATUS(parsed <= std::numeric_limits<uint32_t>::max(), K_INVALID, "hash token overflow");
    }
    token = static_cast<uint32_t>(parsed);
    return Status::OK();
}

void AddDiagnostic(ValidateResult &result, std::string message)
{
    result.valid = false;
    result.diagnostics.emplace_back(std::move(message));
}

Status NormalizeTargetWorkers(const std::vector<WorkerId> &targetWorkerIds, std::vector<WorkerId> &workers)
{
    workers = targetWorkerIds;
    CHECK_FAIL_RETURN_STATUS(!workers.empty(), K_INVALID, "target worker set is empty");
    std::sort(workers.begin(), workers.end());
    for (size_t i = 0; i < workers.size(); ++i) {
        CHECK_FAIL_RETURN_STATUS(!workers[i].empty(), K_INVALID, "target worker id is empty");
        if (i > 0) {
            CHECK_FAIL_RETURN_STATUS(workers[i - 1] != workers[i], K_INVALID, "target worker id is duplicated");
        }
    }
    return Status::OK();
}

uint32_t BuildToken(const WorkerId &workerId, uint32_t tokenIndex, uint32_t probe)
{
    auto seed = workerId + TOKEN_SEED_SEPARATOR + std::to_string(tokenIndex);
    if (probe != 0) {
        seed += TOKEN_SEED_SEPARATOR + std::to_string(probe);
    }
    return MurmurHash3_32(seed);
}

Status GenerateTokens(const WorkerId &workerId, uint32_t tokenNum, std::unordered_set<uint32_t> &occupied,
                      std::vector<uint32_t> &tokens)
{
    tokens.clear();
    tokens.reserve(tokenNum);
    for (uint32_t tokenIndex = 0; tokenIndex < tokenNum; ++tokenIndex) {
        bool allocated = false;
        for (uint32_t probe = 0; probe < MAX_TOKEN_PROBE_NUM; ++probe) {
            auto token = BuildToken(workerId, tokenIndex, probe);
            if (occupied.insert(token).second) {
                tokens.emplace_back(token);
                allocated = true;
                break;
            }
        }
        CHECK_FAIL_RETURN_STATUS(allocated, K_INVALID, "failed to allocate unique hash token");
    }
    return Status::OK();
}

Status BuildOwnerVector(const TopologyDescriptor &topology, std::vector<TokenOwner> &owners)
{
    owners.clear();
    for (const auto &worker : topology.workers) {
        if (worker.state != WorkerTopologyState::ACTIVE) {
            continue;
        }
        for (auto token : worker.tokens) {
            owners.push_back({ token, worker.workerId });
        }
    }
    CHECK_FAIL_RETURN_STATUS(!owners.empty(), K_NOT_READY, "no active hash owner");
    std::sort(owners.begin(), owners.end(),
              [](const TokenOwner &left, const TokenOwner &right) { return left.token < right.token; });
    for (size_t i = 1; i < owners.size(); ++i) {
        CHECK_FAIL_RETURN_STATUS(owners[i - 1].token != owners[i].token, K_INVALID, "duplicated hash token");
    }
    return Status::OK();
}

Status FindOwner(const std::vector<TokenOwner> &owners, uint32_t token, WorkerId &workerId)
{
    CHECK_FAIL_RETURN_STATUS(!owners.empty(), K_NOT_READY, "hash owner list is empty");
    auto iter = std::lower_bound(owners.begin(), owners.end(), token,
                                 [](const TokenOwner &owner, uint32_t target) { return owner.token < target; });
    if (iter == owners.end()) {
        iter = owners.begin();
    }
    workerId = iter->workerId;
    return Status::OK();
}

void AppendRange(std::vector<PlacementRange> &ranges, uint32_t begin, uint32_t end)
{
    if (!ranges.empty() && ranges.back().end != std::numeric_limits<uint32_t>::max()
        && ranges.back().end + 1 == begin) {
        ranges.back().end = end;
        return;
    }
    ranges.push_back({ begin, end });
}

void AddNextTokenCutPoint(uint32_t token, std::vector<uint32_t> &cutPoints)
{
    if (token == std::numeric_limits<uint32_t>::max()) {
        return;
    }
    cutPoints.emplace_back(token + 1);
}

}  // namespace

HashAlgorithm::HashAlgorithm(AlgorithmId algorithmId, uint32_t virtualTokenNum)
    : algorithmId_(std::move(algorithmId)), virtualTokenNum_(virtualTokenNum)
{
}

AlgorithmId HashAlgorithm::GetAlgorithmId() const
{
    return algorithmId_;
}

Status HashAlgorithm::BuildRoutingState(const TopologyDescriptor &snapshot,
                                        std::unique_ptr<AlgorithmRoutingState> &routing) const
{
    routing.reset();
    CHECK_FAIL_RETURN_STATUS(!algorithmId_.empty(), K_INVALID, "hash algorithm id is empty");
    CHECK_FAIL_RETURN_STATUS(snapshot.version >= 0, K_INVALID, "topology version is invalid");
    CHECK_FAIL_RETURN_STATUS(snapshot.clusterHasInit, K_NOT_READY, "topology is not initialized");

    auto state = std::make_unique<HashRoutingState>();
    state->algorithmId = algorithmId_;
    state->topologyVersion = snapshot.version;
    std::unordered_set<WorkerId> workerIds;
    workerIds.reserve(snapshot.workers.size());
    std::unordered_set<uint32_t> activeTokens;
    for (const auto &worker : snapshot.workers) {
        CHECK_FAIL_RETURN_STATUS(!worker.workerId.empty(), K_INVALID, "worker id is empty");
        CHECK_FAIL_RETURN_STATUS(workerIds.insert(worker.workerId).second, K_INVALID, "worker id is duplicated");
        CHECK_FAIL_RETURN_STATUS(IsKnownState(worker.state), K_INVALID, "worker topology state is invalid");
        if (worker.state != WorkerTopologyState::ACTIVE) {
            continue;
        }
        CHECK_FAIL_RETURN_STATUS(!worker.tokens.empty(), K_INVALID, "active worker token list is empty");
        for (auto token : worker.tokens) {
            CHECK_FAIL_RETURN_STATUS(activeTokens.insert(token).second, K_INVALID, "duplicated hash token");
        }
    }
    RETURN_IF_NOT_OK(BuildOwnerVector(snapshot, state->owners));
    routing = std::move(state);
    return Status::OK();
}

Status HashAlgorithm::BuildPlacementUnit(const RouteContext &context, const PlacementPolicyRule &policy,
                                         PlacementUnit &unit) const
{
    unit = {};
    CHECK_FAIL_RETURN_STATUS(!context.objectKey.empty(), K_INVALID, "object key is empty");
    CHECK_FAIL_RETURN_STATUS(policy.algorithmId == algorithmId_, K_INVALID, "placement policy algorithm mismatch");
    unit.algorithmId = algorithmId_;
    unit.unitType = HASH_UNIT_TYPE;
    unit.opaqueUnit = std::to_string(MurmurHash3_32(context.objectKey));
    return Status::OK();
}

Status HashAlgorithm::Route(const AlgorithmRoutingState &routing, const PlacementUnit &unit, LogicalOwner &owner) const
{
    owner = {};
    CHECK_FAIL_RETURN_STATUS(routing.algorithmId == algorithmId_, K_INVALID, "routing state algorithm mismatch");
    CHECK_FAIL_RETURN_STATUS(unit.algorithmId == algorithmId_, K_INVALID, "placement unit algorithm mismatch");
    CHECK_FAIL_RETURN_STATUS(unit.unitType == HASH_UNIT_TYPE, K_INVALID, "placement unit type mismatch");
    const auto *hashState = dynamic_cast<const HashRoutingState *>(&routing);
    CHECK_FAIL_RETURN_STATUS(hashState != nullptr, K_INVALID, "routing state type mismatch");

    uint32_t token = 0;
    RETURN_IF_NOT_OK(ParseUint32(unit.opaqueUnit, token));
    RETURN_IF_NOT_OK(FindOwner(hashState->owners, token, owner.workerId));
    owner.topologyVersion = routing.topologyVersion;
    return Status::OK();
}

Status HashAlgorithm::ValidatePlacement(const TopologyDescriptor &topology, ValidateResult &result) const
{
    result = {};
    result.valid = true;
    if (algorithmId_.empty()) {
        AddDiagnostic(result, "hash algorithm id is empty");
    }
    if (topology.version < 0) {
        AddDiagnostic(result, "topology version is invalid");
    }
    if (!topology.clusterHasInit) {
        AddDiagnostic(result, "topology is not initialized");
    }
    if (topology.workers.empty()) {
        AddDiagnostic(result, "topology worker list is empty");
    }

    bool hasActiveWorker = false;
    std::unordered_set<WorkerId> workerIds;
    std::unordered_set<uint32_t> tokens;
    for (const auto &worker : topology.workers) {
        if (worker.workerId.empty()) {
            AddDiagnostic(result, "worker id is empty");
            continue;
        }
        if (!workerIds.insert(worker.workerId).second) {
            AddDiagnostic(result, "worker id is duplicated");
        }
        if (!IsKnownState(worker.state)) {
            AddDiagnostic(result, "worker topology state is invalid");
        }
        if (worker.state == WorkerTopologyState::ACTIVE) {
            hasActiveWorker = true;
            if (worker.tokens.empty()) {
                AddDiagnostic(result, "active worker token list is empty");
            }
        }
        for (auto token : worker.tokens) {
            if (!tokens.insert(token).second) {
                AddDiagnostic(result, "hash token is duplicated");
            }
        }
    }
    if (!topology.workers.empty() && !hasActiveWorker) {
        AddDiagnostic(result, "topology has no active worker");
    }
    return Status::OK();
}

Status HashAlgorithm::InitPlacement(const PlanInput &input, PlanResult &result) const
{
    result = {};
    CHECK_FAIL_RETURN_STATUS(!algorithmId_.empty(), K_INVALID, "hash algorithm id is empty");
    CHECK_FAIL_RETURN_STATUS(virtualTokenNum_ > 0, K_INVALID, "virtual token num is zero");
    CHECK_FAIL_RETURN_STATUS(input.current.version >= 0, K_INVALID, "current topology version is invalid");

    std::vector<WorkerId> targetWorkers;
    RETURN_IF_NOT_OK(NormalizeTargetWorkers(input.targetWorkerIds, targetWorkers));

    result.algorithmId = algorithmId_;
    result.next.version = input.current.version + 1;
    result.next.clusterHasInit = true;
    result.next.workers.reserve(targetWorkers.size());
    std::unordered_set<uint32_t> occupied;
    occupied.reserve(static_cast<size_t>(virtualTokenNum_) * targetWorkers.size());
    for (const auto &workerId : targetWorkers) {
        TopologyWorker worker;
        worker.workerId = workerId;
        worker.state = WorkerTopologyState::ACTIVE;
        RETURN_IF_NOT_OK(GenerateTokens(workerId, virtualTokenNum_, occupied, worker.tokens));
        result.next.workers.emplace_back(std::move(worker));
    }
    return Status::OK();
}

Status HashAlgorithm::PlanPlacement(const PlanInput &input, PlanResult &result) const
{
    result = {};
    if (input.current.workers.empty() || !input.current.clusterHasInit) {
        return InitPlacement(input, result);
    }

    ValidateResult validation;
    RETURN_IF_NOT_OK(ValidatePlacement(input.current, validation));
    CHECK_FAIL_RETURN_STATUS(validation.valid, K_INVALID, "current topology is invalid");
    CHECK_FAIL_RETURN_STATUS(virtualTokenNum_ > 0, K_INVALID, "virtual token num is zero");

    std::vector<WorkerId> targetWorkers;
    RETURN_IF_NOT_OK(NormalizeTargetWorkers(input.targetWorkerIds, targetWorkers));
    std::unordered_map<WorkerId, const TopologyWorker *> currentWorkers;
    currentWorkers.reserve(input.current.workers.size());
    for (const auto &worker : input.current.workers) {
        currentWorkers.emplace(worker.workerId, &worker);
    }

    result.algorithmId = algorithmId_;
    result.next.version = input.current.version + 1;
    result.next.clusterHasInit = true;
    result.next.workers.reserve(targetWorkers.size());
    std::unordered_set<uint32_t> occupied;
    occupied.reserve(static_cast<size_t>(virtualTokenNum_) * targetWorkers.size());
    for (const auto &workerId : targetWorkers) {
        TopologyWorker worker;
        worker.workerId = workerId;
        worker.state = WorkerTopologyState::ACTIVE;
        auto iter = currentWorkers.find(workerId);
        if (iter != currentWorkers.end() && iter->second->state == WorkerTopologyState::ACTIVE
            && !iter->second->tokens.empty()) {
            worker.tokens = iter->second->tokens;
            for (auto token : worker.tokens) {
                CHECK_FAIL_RETURN_STATUS(occupied.insert(token).second, K_INVALID, "hash token is duplicated");
            }
        } else {
            RETURN_IF_NOT_OK(GenerateTokens(workerId, virtualTokenNum_, occupied, worker.tokens));
        }
        result.next.workers.emplace_back(std::move(worker));
    }
    RETURN_IF_NOT_OK(DiffPlacement(input.current, result.next, result.ownerChanges));
    return Status::OK();
}

Status HashAlgorithm::DiffPlacement(const TopologyDescriptor &from, const TopologyDescriptor &to,
                                    std::vector<OwnerChange> &changes) const
{
    changes.clear();
    ValidateResult validation;
    RETURN_IF_NOT_OK(ValidatePlacement(from, validation));
    CHECK_FAIL_RETURN_STATUS(validation.valid, K_INVALID, "source topology is invalid");
    RETURN_IF_NOT_OK(ValidatePlacement(to, validation));
    CHECK_FAIL_RETURN_STATUS(validation.valid, K_INVALID, "target topology is invalid");

    std::vector<TokenOwner> fromOwners;
    std::vector<TokenOwner> toOwners;
    RETURN_IF_NOT_OK(BuildOwnerVector(from, fromOwners));
    RETURN_IF_NOT_OK(BuildOwnerVector(to, toOwners));

    std::vector<uint32_t> boundaries{ 0 };
    boundaries.reserve(fromOwners.size() + toOwners.size() + 1);
    for (const auto &owner : fromOwners) {
        AddNextTokenCutPoint(owner.token, boundaries);
    }
    for (const auto &owner : toOwners) {
        AddNextTokenCutPoint(owner.token, boundaries);
    }
    std::sort(boundaries.begin(), boundaries.end());
    boundaries.erase(std::unique(boundaries.begin(), boundaries.end()), boundaries.end());

    std::map<std::pair<WorkerId, WorkerId>, std::vector<PlacementRange>> grouped;
    for (size_t i = 0; i < boundaries.size(); ++i) {
        auto begin = boundaries[i];
        auto end = std::numeric_limits<uint32_t>::max();
        if (i + 1 < boundaries.size()) {
            end = boundaries[i + 1] - 1;
        }
        WorkerId fromWorker;
        WorkerId toWorker;
        RETURN_IF_NOT_OK(FindOwner(fromOwners, begin, fromWorker));
        RETURN_IF_NOT_OK(FindOwner(toOwners, begin, toWorker));
        if (fromWorker == toWorker) {
            continue;
        }
        AppendRange(grouped[{ fromWorker, toWorker }], begin, end);
    }

    changes.reserve(grouped.size());
    for (auto &entry : grouped) {
        OwnerChange change;
        change.fromWorkerId = entry.first.first;
        change.toWorkerId = entry.first.second;
        change.ranges = std::move(entry.second);
        changes.emplace_back(std::move(change));
    }
    return Status::OK();
}

}  // namespace topology
}  // namespace datasystem
