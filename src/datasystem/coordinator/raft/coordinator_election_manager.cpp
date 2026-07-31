// Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

/**
 * Description: Sole owner of Coordinator raft bootstrap, election, and membership lifecycles.
 */
#include "datasystem/coordinator/raft/coordinator_election_manager.h"

#include <algorithm>
#include <cerrno>
#include <exception>
#include <limits>
#include <string_view>
#include <utility>
#include <vector>

#include <dirent.h>
#include <sys/stat.h>

#include "datasystem/common/rpc/brpc_factory.h"
#include "datasystem/protos/coordinator.brpc.stub.pb.h"
// brpc headers above override LOG/VLOG/DLOG via butil/logging.h.
// Include DataSystem helpers afterwards to restore the spdlog-based macros.
#include "datasystem/common/ak_sk/hasher.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/coordinator/raft/coordinator_raft_peer.h"

namespace datasystem::coordinator {
namespace {
constexpr char kCoordinatorRaftMetadataDirectory[] = "raft_meta";
constexpr char kCoordinatorRaftMetadataEntity[] = "raft_meta";
constexpr char kCoordinatorRaftLogDirectory[] = "log";
constexpr char kCoordinatorRaftLogMetadataEntity[] = "log_meta";
constexpr char kCoordinatorRaftLogSegmentPrefix[] = "log_";
constexpr char kCoordinatorRaftInProgressLogSegmentPrefix[] = "log_inprogress_";
constexpr size_t kCoordinatorRaftLogIndexWidth = 20;
constexpr size_t kCoordinatorRaftClosedLogIndexCount = 2;
constexpr size_t kSha256HexLength = 64;

struct DirectoryCloser {
    void operator()(DIR *directory) const noexcept
    {
        if (directory != nullptr) {
            (void)closedir(directory);
        }
    }
};

bool StartsWith(std::string_view value, std::string_view prefix)
{
    return value.substr(0, prefix.size()) == prefix;
}

Status FilesystemOperationError(StatusCode statusCode, const char *operation, std::string_view entity,
                                const std::string &path, int errorNumber)
{
    return Status(statusCode, FormatString("%s failed, entity=%s, path=%s, errno=%d, errmsg=%s", operation,
                                           std::string(entity), path, errorNumber, StrErr(errorNumber)));
}

Status CheckRequiredDirectory(const std::string &path, std::string_view description)
{
    struct stat directoryStat{};
    if (lstat(path.c_str(), &directoryStat) != 0) {
        const int errorNumber = errno;
        const auto statusCode = errorNumber == ENOENT ? K_DATA_INCONSISTENCY : K_NOT_READY;
        return FilesystemOperationError(statusCode, "lstat", description, path, errorNumber);
    }
    CHECK_FAIL_RETURN_STATUS(S_ISDIR(directoryStat.st_mode), K_DATA_INCONSISTENCY,
                             std::string(description) + " is not a directory");
    return Status::OK();
}

Status CheckRequiredNonEmptyRegularFile(const std::string &path, std::string_view description)
{
    struct stat fileStat{};
    if (lstat(path.c_str(), &fileStat) != 0) {
        const int errorNumber = errno;
        const auto statusCode = errorNumber == ENOENT ? K_DATA_INCONSISTENCY : K_NOT_READY;
        return FilesystemOperationError(statusCode, "lstat", description, path, errorNumber);
    }
    CHECK_FAIL_RETURN_STATUS(S_ISREG(fileStat.st_mode), K_DATA_INCONSISTENCY,
                             std::string(description) + " is not a regular file");
    CHECK_FAIL_RETURN_STATUS(fileStat.st_size > 0, K_DATA_INCONSISTENCY, std::string(description) + " is empty");
    return Status::OK();
}

bool IsFixedWidthCoordinatorRaftLogIndex(std::string_view value)
{
    return value.size() == kCoordinatorRaftLogIndexWidth && std::all_of(value.begin(), value.end(), [](char character) {
               return character >= '0' && character <= '9';
           });
}

const char *RaftMetadataStateName(RaftMetadataState state) noexcept
{
    switch (state) {
        case RaftMetadataState::ABSENT:
            return "ABSENT";
        case RaftMetadataState::VALID:
            return "VALID";
        case RaftMetadataState::CORRUPT:
            return "CORRUPT";
        case RaftMetadataState::UNKNOWN:
            return "UNKNOWN";
    }
    return "UNKNOWN";
}

const char *RaftStartPlanName(const RaftStartPlan &plan) noexcept
{
    if (std::holds_alternative<BootstrapPlan>(plan)) {
        return "BootstrapPlan";
    }
    if (std::holds_alternative<RecoverPlan>(plan)) {
        return "RecoverPlan";
    }
    return "WaitingToJoinPlan";
}

std::vector<std::string> RaftStartPlanPeers(const RaftStartPlan &plan)
{
    const auto *bootstrap = std::get_if<BootstrapPlan>(&plan);
    return bootstrap == nullptr ? std::vector<std::string>{} : bootstrap->initialPeers;
}

bool IsCoordinatorRaftLogSegment(std::string_view entryName)
{
    const std::string_view inProgressPrefix = kCoordinatorRaftInProgressLogSegmentPrefix;
    if (StartsWith(entryName, inProgressPrefix)) {
        return IsFixedWidthCoordinatorRaftLogIndex(entryName.substr(inProgressPrefix.size()));
    }

    const std::string_view closedPrefix = kCoordinatorRaftLogSegmentPrefix;
    if (!StartsWith(entryName, closedPrefix)) {
        return false;
    }
    const auto indexes = entryName.substr(closedPrefix.size());
    const auto separatorPosition = indexes.find('_');
    return indexes.size() == kCoordinatorRaftClosedLogIndexCount * kCoordinatorRaftLogIndexWidth + 1
           && separatorPosition == kCoordinatorRaftLogIndexWidth
           && IsFixedWidthCoordinatorRaftLogIndex(indexes.substr(0, separatorPosition))
           && IsFixedWidthCoordinatorRaftLogIndex(indexes.substr(separatorPosition + 1));
}

Status CheckCoordinatorRaftLog(const std::string &dataRoot)
{
    const std::string logDirectoryPath = dataRoot + "/" + kCoordinatorRaftLogDirectory;
    RETURN_IF_NOT_OK(CheckRequiredDirectory(logDirectoryPath, "Coordinator Raft log directory"));
    RETURN_IF_NOT_OK(CheckRequiredNonEmptyRegularFile(logDirectoryPath + "/" + kCoordinatorRaftLogMetadataEntity,
                                                      "Coordinator Raft log metadata entity"));

    auto *logDirectoryHandle = opendir(logDirectoryPath.c_str());
    const int openDirectoryErrorNumber = errno;
    std::unique_ptr<DIR, DirectoryCloser> logDirectory(logDirectoryHandle);
    if (logDirectory == nullptr) {
        return FilesystemOperationError(K_NOT_READY, "opendir", "Coordinator Raft log directory", logDirectoryPath,
                                        openDirectoryErrorNumber);
    }

    bool hasValidSegment = false;
    while (true) {
        errno = 0;
        const auto *entry = readdir(logDirectory.get());
        const int readDirectoryErrorNumber = errno;
        if (entry == nullptr) {
            if (readDirectoryErrorNumber != 0) {
                return FilesystemOperationError(K_NOT_READY, "readdir", "Coordinator Raft log directory",
                                                logDirectoryPath, readDirectoryErrorNumber);
            }
            break;
        }
        const std::string entryName = entry->d_name;
        if (!IsCoordinatorRaftLogSegment(entryName)) {
            continue;
        }
        const std::string segmentPath = logDirectoryPath + "/" + entryName;
        struct stat segmentStat{};
        if (lstat(segmentPath.c_str(), &segmentStat) != 0) {
            const int errorNumber = errno;
            return FilesystemOperationError(K_NOT_READY, "lstat", "Coordinator Raft log segment", segmentPath,
                                            errorNumber);
        }
        CHECK_FAIL_RETURN_STATUS(S_ISREG(segmentStat.st_mode), K_DATA_INCONSISTENCY,
                                 "Coordinator Raft log segment is not a regular file");
        CHECK_FAIL_RETURN_STATUS(segmentStat.st_size > 0, K_DATA_INCONSISTENCY,
                                 "Coordinator Raft log segment is empty");
        hasValidSegment = true;
    }
    CHECK_FAIL_RETURN_STATUS(hasValidSegment, K_DATA_INCONSISTENCY,
                             "Coordinator Raft log directory contains no persisted log segment");
    return Status::OK();
}

Status ProbeCoordinatorRaftMetadata(const std::string &dataRoot, RaftMetadataState &metadataState)
{
    metadataState = RaftMetadataState::UNKNOWN;
    struct stat rootStat{};
    if (lstat(dataRoot.c_str(), &rootStat) != 0) {
        const int errorNumber = errno;
        if (errorNumber == ENOENT) {
            metadataState = RaftMetadataState::ABSENT;
            return Status::OK();
        }
        return FilesystemOperationError(K_NOT_READY, "lstat", "Coordinator Raft data root", dataRoot, errorNumber);
    }
    if (!S_ISDIR(rootStat.st_mode)) {
        metadataState = RaftMetadataState::CORRUPT;
        return Status(K_DATA_INCONSISTENCY, "Coordinator Raft data root exists but is not a directory");
    }

    auto *directoryHandle = opendir(dataRoot.c_str());
    const int openDirectoryErrorNumber = errno;
    std::unique_ptr<DIR, DirectoryCloser> directory(directoryHandle);
    if (directory == nullptr) {
        return FilesystemOperationError(K_NOT_READY, "opendir", "Coordinator Raft data root", dataRoot,
                                        openDirectoryErrorNumber);
    }

    bool rootIsEmpty = true;
    while (true) {
        errno = 0;
        const auto *entry = readdir(directory.get());
        const int readDirectoryErrorNumber = errno;
        if (entry == nullptr) {
            if (readDirectoryErrorNumber != 0) {
                return FilesystemOperationError(K_NOT_READY, "readdir", "Coordinator Raft data root", dataRoot,
                                                readDirectoryErrorNumber);
            }
            break;
        }
        const std::string entryName = entry->d_name;
        if (entryName != "." && entryName != "..") {
            rootIsEmpty = false;
        }
    }
    if (rootIsEmpty) {
        metadataState = RaftMetadataState::ABSENT;
        return Status::OK();
    }

    const std::string metadataDirectoryPath = dataRoot + "/" + kCoordinatorRaftMetadataDirectory;
    auto status = CheckRequiredDirectory(metadataDirectoryPath, "Coordinator Raft metadata directory");
    if (status.IsOk()) {
        status = CheckRequiredNonEmptyRegularFile(metadataDirectoryPath + "/" + kCoordinatorRaftMetadataEntity,
                                                  "Coordinator Raft metadata entity");
    }
    if (status.IsOk()) {
        status = CheckCoordinatorRaftLog(dataRoot);
    }
    if (status.IsError()) {
        metadataState =
            status.GetCode() == K_DATA_INCONSISTENCY ? RaftMetadataState::CORRUPT : RaftMetadataState::UNKNOWN;
        return status;
    }

    metadataState = RaftMetadataState::VALID;
    return Status::OK();
}

Status NormalizePeers(const std::vector<std::string> &input, std::vector<std::string> &normalized)
{
    normalized.clear();
    normalized.reserve(input.size());
    for (const auto &candidateAddress : input) {
        braft::PeerId candidate;
        RETURN_IF_NOT_OK(ParseCoordinatorRaftPeer(candidateAddress, candidate));
        auto normalizedCandidate = CoordinatorRaftPeerAddress(candidate);
        CHECK_FAIL_RETURN_STATUS(!normalizedCandidate.empty(), K_INVALID, "Coordinator Raft peer cannot be normalized");
        normalized.emplace_back(std::move(normalizedCandidate));
    }
    std::sort(normalized.begin(), normalized.end());
    normalized.erase(std::unique(normalized.begin(), normalized.end()), normalized.end());
    return Status::OK();
}

Status BuildCandidateDigest(const std::vector<std::string> &peers, std::string &digest)
{
    std::string encoded;
    for (const auto &peer : peers) {
        encoded.append(std::to_string(peer.size()));
        encoded.push_back(':');
        encoded.append(peer);
        encoded.push_back(';');
    }
    return Hasher().GetSha256Hex(encoded, digest);
}

bool IsSha256Hex(std::string_view digest)
{
    return digest.size() == kSha256HexLength && std::all_of(digest.begin(), digest.end(), [](char character) {
               return (character >= '0' && character <= '9') || (character >= 'a' && character <= 'f')
                      || (character >= 'A' && character <= 'F');
           });
}

Status DependencyExceptionStatus(const char *operation, const std::exception *error = nullptr)
{
    std::string message = std::string("Coordinator election manager failed to ") + operation;
    if (error != nullptr) {
        message += ": ";
        message += error->what();
    }
    return Status(K_RUNTIME_ERROR, std::move(message));
}

size_t QuorumSize(size_t peerCount)
{
    return (peerCount / 2) + 1;
}

bool IsRetryableRaftCallbackError(StatusCode code)
{
    return code == K_RPC_UNAVAILABLE || code == K_RPC_DEADLINE_EXCEEDED || code == K_RPC_CANCELLED
           || code == K_NOT_READY || code == K_TRY_AGAIN;
}

void InvokeCallback(const std::function<void()> &callback)
{
    if (!callback) {
        return;
    }
    try {
        callback();
    } catch (const std::exception &error) {
        LOG(ERROR) << kCoordinatorRaftCallbackFailureMarker << ": " << error.what();
    } catch (...) {
        LOG(ERROR) << kCoordinatorRaftCallbackFailureMarker;
    }
}
}  // namespace

CoordinatorElectionManager::NodeHandle::~NodeHandle() noexcept
{
    node.reset();
    if (onDestroyed) {
        onDestroyed();
    }
}

CoordinatorElectionManager::MembershipHandle::~MembershipHandle() noexcept
{
    membership.reset();
    if (onDestroyed) {
        onDestroyed();
    }
}

CoordinatorElectionManager::Dependencies CoordinatorElectionManager::MakeProductionDependencies()
{
    Dependencies dependencies;
    dependencies.probeLocalMetadata = ProbeCoordinatorRaftMetadata;
    dependencies.discoverCandidates = [](const std::shared_ptr<ICoordinatorDiscovery> &discovery,
                                         std::vector<std::string> &candidates) {
        return discovery->GetCoordinators(candidates);
    };
    dependencies.probePeer = [](const std::string &peer, int32_t timeoutMs, RaftBootstrapState &state) {
        BrpcChannelConfig config;
        config.endpoint = peer;
        config.timeout_ms = timeoutMs;
        config.connect_timeout_ms = timeoutMs;
        config.max_retry = 0;
        config.enable_circuit_breaker = false;
        auto channel = BrpcChannelFactory::Create(config);
        CHECK_FAIL_RETURN_STATUS(channel != nullptr, K_RPC_UNAVAILABLE,
                                 "Failed to create Coordinator bootstrap probe channel");

        CoordinatorService_BrpcGenericStub stub(channel.get(), timeoutMs);
        GetRaftBootstrapStateReqPb request;
        request.set_group_id(kCoordinatorRaftGroupId);
        GetRaftBootstrapStateRspPb response;
        RETURN_IF_NOT_OK(stub.GetRaftBootstrapState(request, response));

        if constexpr (sizeof(size_t) < sizeof(uint64_t)) {
            CHECK_FAIL_RETURN_STATUS(response.expected_member_count() <= std::numeric_limits<size_t>::max(), K_INVALID,
                                     "Coordinator bootstrap response expected member count exceeds size_t");
            CHECK_FAIL_RETURN_STATUS(response.candidate_count() <= std::numeric_limits<size_t>::max(), K_INVALID,
                                     "Coordinator bootstrap response candidate count exceeds size_t");
        }
        state.probeReady = response.probe_ready();
        state.groupId = response.group_id();
        state.localPeer = response.local_peer();
        state.expectedMemberCount = static_cast<size_t>(response.expected_member_count());
        switch (response.metadata_state()) {
            case RAFT_METADATA_ABSENT:
                state.metadataState = RaftMetadataState::ABSENT;
                break;
            case RAFT_METADATA_VALID:
                state.metadataState = RaftMetadataState::VALID;
                break;
            case RAFT_METADATA_CORRUPT:
                state.metadataState = RaftMetadataState::CORRUPT;
                break;
            case RAFT_METADATA_UNKNOWN:
                state.metadataState = RaftMetadataState::UNKNOWN;
                break;
            default:
                return Status(K_INVALID, "Coordinator bootstrap response has an unknown metadata state");
        }
        state.candidateCount = static_cast<size_t>(response.candidate_count());
        state.candidateDigest = response.candidate_digest();
        state.committedPeers.assign(response.committed_peers().begin(), response.committed_peers().end());
        switch (response.phase()) {
            case RAFT_BOOTSTRAP_OBSERVING:
                state.phase = RaftBootstrapPhase::OBSERVING;
                break;
            case RAFT_BOOTSTRAP_RETRYING:
                state.phase = RaftBootstrapPhase::RETRYING;
                break;
            case RAFT_BOOTSTRAP_STARTED:
                state.phase = RaftBootstrapPhase::STARTED;
                break;
            case RAFT_BOOTSTRAP_TERMINAL:
                state.phase = RaftBootstrapPhase::TERMINAL;
                break;
            default:
                return Status(K_INVALID, "Coordinator bootstrap response has an unknown phase");
        }
        state.statusCode = response.status_code();
        return Status::OK();
    };
    dependencies.digestCandidates = BuildCandidateDigest;
    dependencies.now = [] { return std::chrono::steady_clock::now(); };
    dependencies.createNode = [](const CoordinatorRaftOptions &options,
                                 const CoordinatorRaftEventCallbacks &callbacks) {
        auto handle = std::make_unique<NodeHandle>();
        handle->node = std::make_unique<CoordinatorRaftNode>(options, callbacks);
        return handle;
    };
    dependencies.startNode = [](NodeHandle &handle, RaftMetadataState metadataState) {
        return handle.node->Start(metadataState);
    };
    dependencies.createMembership = [](const CoordinatorMembershipOptions &options, NodeHandle &node,
                                       const std::shared_ptr<ICoordinatorDiscovery> &discovery) {
        auto handle = std::make_unique<MembershipHandle>();
        handle->membership = std::make_unique<CoordinatorMembershipManager>(options, *node.node, discovery);
        return handle;
    };
    dependencies.startMembership = [](MembershipHandle &handle) { return handle.membership->Start(); };
    dependencies.shutdownMembership = [](MembershipHandle &handle) { return handle.membership->Shutdown(); };
    dependencies.isLeader = [](const NodeHandle &handle) { return handle.node->IsLeader(); };
    dependencies.getLeader = [](const NodeHandle &handle, std::string &leaderAddress) {
        return handle.node->GetLeader(leaderAddress);
    };
    return dependencies;
}

CoordinatorElectionManager::CoordinatorElectionManager(CoordinatorElectionOptions options,
                                                       CoordinatorRaftEventCallbacks callbacks,
                                                       std::shared_ptr<ICoordinatorDiscovery> discovery)
    : CoordinatorElectionManager(std::move(options), std::move(callbacks), std::move(discovery),
                                 MakeProductionDependencies())
{
}

CoordinatorElectionManager::CoordinatorElectionManager(CoordinatorElectionOptions options,
                                                       CoordinatorRaftEventCallbacks callbacks,
                                                       std::shared_ptr<ICoordinatorDiscovery> discovery,
                                                       Dependencies dependencies)
    : options_(std::move(options)),
      callbacks_(std::move(callbacks)),
      discovery_(std::move(discovery)),
      dependencies_(std::move(dependencies))
{
    bootstrapState_.groupId = kCoordinatorRaftGroupId;
    bootstrapState_.localPeer = options_.raftFlags.localAddress;
    bootstrapState_.expectedMemberCount = options_.membershipOptions.expectedMemberCount;
}

CoordinatorElectionManager::~CoordinatorElectionManager() noexcept
{
    (void)Shutdown();
}

Status CoordinatorElectionManager::ValidateStartupInput() const
{
    CHECK_FAIL_RETURN_STATUS(discovery_ != nullptr, K_INVALID, "Coordinator election Discovery must not be null");
    CHECK_FAIL_RETURN_STATUS(options_.membershipOptions.IsValid(), K_INVALID,
                             "Coordinator election membership options are invalid");
    CHECK_FAIL_RETURN_STATUS(!options_.raftFlags.dataDir.empty(), K_INVALID,
                             "Coordinator Raft data directory must not be empty");
    CHECK_FAIL_RETURN_STATUS(options_.raftFlags.heartbeatIntervalMs >= kCoordinatorRaftMinHeartbeatIntervalMs
                                 && options_.raftFlags.heartbeatIntervalMs <= kCoordinatorRaftMaxHeartbeatIntervalMs,
                             K_INVALID, "Coordinator Raft heartbeat interval is outside the supported range");
    CHECK_FAIL_RETURN_STATUS(options_.raftFlags.electionTimeoutMs >= kCoordinatorRaftMinElectionTimeoutMs
                                 && options_.raftFlags.electionTimeoutMs <= kCoordinatorRaftMaxElectionTimeoutMs,
                             K_INVALID, "Coordinator Raft election timeout is outside the supported range");
    CHECK_FAIL_RETURN_STATUS(options_.raftFlags.electionTimeoutMs % options_.raftFlags.heartbeatIntervalMs == 0,
                             K_INVALID,
                             "Coordinator Raft election timeout must be an integer multiple of heartbeat interval");
    const auto electionHeartbeatRatio = options_.raftFlags.electionTimeoutMs / options_.raftFlags.heartbeatIntervalMs;
    CHECK_FAIL_RETURN_STATUS(electionHeartbeatRatio >= kCoordinatorRaftMinElectionHeartbeatRatio
                                 && electionHeartbeatRatio <= kCoordinatorRaftMaxElectionHeartbeatRatio,
                             K_INVALID,
                             "Coordinator Raft election timeout must be between 5 and 10 times heartbeat interval");

    braft::PeerId localPeer;
    RETURN_IF_NOT_OK(ParseCoordinatorRaftPeer(options_.raftFlags.localAddress, localPeer));
    CHECK_FAIL_RETURN_STATUS(CoordinatorRaftPeerAddress(localPeer) == options_.raftFlags.localAddress, K_INVALID,
                             "Coordinator Raft local address must be normalized");
    CHECK_FAIL_RETURN_STATUS(
        dependencies_.probeLocalMetadata && dependencies_.discoverCandidates && dependencies_.probePeer
            && dependencies_.digestCandidates && dependencies_.now && dependencies_.createNode
            && dependencies_.startNode && dependencies_.createMembership && dependencies_.startMembership
            && dependencies_.shutdownMembership && dependencies_.isLeader && dependencies_.getLeader,
        K_INVALID, "Coordinator election manager dependencies are incomplete");
    return Status::OK();
}

Status CoordinatorElectionManager::Start()
{
    RETURN_IF_NOT_OK(ValidateStartupInput());

    std::lock_guard<std::mutex> lock(lifecycleMutex_);
    if (state_ != LifecycleState::CONSTRUCTED || lifecycleOperationInProgress_) {
        return Status(K_INVALID, "Coordinator election manager cannot be started more than once or after shutdown");
    }
    lifecycleOperationInProgress_ = true;
    state_ = LifecycleState::RUNNING;
    try {
        bootstrapThread_ = std::thread([this] { RunBootstrapControl(); });
    } catch (const std::exception &error) {
        state_ = LifecycleState::STOPPED;
        lifecycleOperationInProgress_ = false;
        lifecycleCv_.notify_all();
        return DependencyExceptionStatus("start bootstrap control worker", &error);
    } catch (...) {
        state_ = LifecycleState::STOPPED;
        lifecycleOperationInProgress_ = false;
        lifecycleCv_.notify_all();
        return DependencyExceptionStatus("start bootstrap control worker");
    }
    lifecycleOperationInProgress_ = false;
    lifecycleCv_.notify_all();
    return Status::OK();
}

void CoordinatorElectionManager::RunBootstrapControl() noexcept
{
    Raii exitNotifier([callback = dependencies_.onBootstrapWorkerExit] {
        if (callback) {
            callback();
        }
    });
    RaftBootstrapState localState;
    {
        std::lock_guard<std::mutex> lock(bootstrapMutex_);
        localState = bootstrapState_;
    }
    localState.phase = RaftBootstrapPhase::OBSERVING;
    localState.statusCode = static_cast<int32_t>(K_OK);
    PublishBootstrapState(localState);
    auto status = dependencies_.digestCandidates({}, localState.candidateDigest);
    if (status.IsError()) {
        localState.probeReady = true;
        PublishBootstrapState(std::move(localState));
        RecordBootstrapTerminalStatus(std::move(status));
        return;
    }

    status = dependencies_.probeLocalMetadata(options_.raftFlags.dataDir, localState.metadataState);
    localState.probeReady = true;
    PublishBootstrapState(localState);
    if (status.IsError()) {
        RecordBootstrapTerminalStatus(std::move(status));
        return;
    }
    if (localState.metadataState == RaftMetadataState::VALID) {
        status = StartOwnedComponents(RecoverPlan{}, RaftMetadataState::VALID);
        if (status.IsError() && !IsBootstrapStopRequested()) {
            RecordBootstrapTerminalStatus(std::move(status));
        }
        return;
    }
    if (localState.metadataState != RaftMetadataState::ABSENT) {
        RecordBootstrapTerminalStatus(
            Status(K_DATA_INCONSISTENCY, "Coordinator local Raft metadata state is not bootstrappable"));
        return;
    }

    std::chrono::steady_clock::time_point nextWarningAt{};
    while (!IsBootstrapStopRequested()) {
        std::vector<std::string> normalizedCandidates;
        status = RefreshBootstrapObservation(localState, normalizedCandidates);
        if (status.IsError()) {
            if (status.GetCode() != K_NOT_READY) {
                RecordBootstrapTerminalStatus(std::move(status));
                return;
            }
            localState.phase = RaftBootstrapPhase::RETRYING;
            localState.statusCode = static_cast<int32_t>(status.GetCode());
            PublishBootstrapState(localState);
            WarnBootstrapRetry(status, nextWarningAt);
            if (WaitForBootstrapRetryOrStop()) {
                return;
            }
            continue;
        }

        RaftStartPlan startPlan = RecoverPlan{};
        status = TryBuildStartPlan(localState, normalizedCandidates, startPlan);
        if (IsBootstrapStopRequested()) {
            return;
        }
        if (status.IsOk()) {
            status = StartOwnedComponents(std::move(startPlan), localState.metadataState);
            if (status.IsError() && !IsBootstrapStopRequested()) {
                RecordBootstrapTerminalStatus(std::move(status));
            }
            return;
        }
        localState.phase = RaftBootstrapPhase::RETRYING;
        localState.statusCode = static_cast<int32_t>(status.GetCode());
        PublishBootstrapState(localState);
        WarnBootstrapRetry(status, nextWarningAt);
        if (WaitForBootstrapRetryOrStop()) {
            return;
        }
    }
}

Status CoordinatorElectionManager::RefreshBootstrapObservation(RaftBootstrapState &localState,
                                                               std::vector<std::string> &normalizedCandidates)
{
    std::vector<std::string> discoveredCandidates;
    Status discoveryStatus = dependencies_.discoverCandidates(discovery_, discoveredCandidates);
    if (discoveryStatus.IsError()) {
        localState.probeReady = false;
        localState.phase = RaftBootstrapPhase::RETRYING;
        localState.statusCode = static_cast<int32_t>(K_NOT_READY);
        PublishBootstrapState(localState);
        return Status(K_NOT_READY, "Coordinator Discovery bootstrap observation failed: " + discoveryStatus.ToString());
    }

    const auto normalizeStatus = NormalizePeers(discoveredCandidates, normalizedCandidates);
    if (normalizeStatus.IsError()) {
        localState.probeReady = false;
        localState.phase = RaftBootstrapPhase::RETRYING;
        localState.statusCode = static_cast<int32_t>(K_NOT_READY);
        PublishBootstrapState(localState);
        return Status(K_NOT_READY,
                      "Coordinator Discovery returned an invalid bootstrap candidate: " + normalizeStatus.ToString());
    }
    std::string digest;
    RETURN_IF_NOT_OK(dependencies_.digestCandidates(normalizedCandidates, digest));
    localState.probeReady = true;
    localState.candidateCount = normalizedCandidates.size();
    localState.candidateDigest = std::move(digest);
    localState.phase = RaftBootstrapPhase::OBSERVING;
    localState.statusCode = static_cast<int32_t>(K_OK);
    PublishBootstrapState(localState);
    return Status::OK();
}

Status CoordinatorElectionManager::TryBuildStartPlan(const RaftBootstrapState &localState,
                                                     const std::vector<std::string> &normalizedCandidates,
                                                     RaftStartPlan &startPlan)
{
    std::vector<BootstrapObservation> observations;
    RETURN_IF_NOT_OK(CollectBootstrapObservations(localState, normalizedCandidates, observations));
    return DecideStartPlan(localState, normalizedCandidates, observations, startPlan);
}

Status CoordinatorElectionManager::CollectBootstrapObservations(const RaftBootstrapState &localState,
                                                                const std::vector<std::string> &normalizedCandidates,
                                                                std::vector<BootstrapObservation> &observations) const
{
    observations.clear();
    observations.reserve(normalizedCandidates.size());
    for (const auto &peer : normalizedCandidates) {
        if (IsBootstrapStopRequested()) {
            return Status(K_SHUTTING_DOWN, "Coordinator bootstrap peer observation was cancelled by shutdown");
        }

        BootstrapObservation observation;
        observation.peer = peer;
        if (peer == localState.localPeer) {
            observation.state = localState;
            observation.status = Status::OK();
        } else {
            observation.status = ProbePeerBootstrapState(peer, observation.state);
        }
        observations.emplace_back(std::move(observation));
    }
    return Status::OK();
}

Status CoordinatorElectionManager::DecideStartPlan(const RaftBootstrapState &localState,
                                                   const std::vector<std::string> &normalizedCandidates,
                                                   const std::vector<BootstrapObservation> &observations,
                                                   RaftStartPlan &startPlan) const
{
    struct CommittedConfigVote {
        std::vector<std::string> peers;
        size_t confirmations{ 0 };
    };

    bool observedCommittedConfiguration = false;
    std::vector<CommittedConfigVote> committedConfigVotes;
    for (const auto &observation : observations) {
        if (observation.status.IsError() || observation.state.metadataState != RaftMetadataState::VALID
            || observation.state.committedPeers.empty()) {
            continue;
        }
        observedCommittedConfiguration = true;
        if (!std::binary_search(observation.state.committedPeers.begin(), observation.state.committedPeers.end(),
                                observation.peer)) {
            continue;
        }
        auto vote = std::find_if(committedConfigVotes.begin(), committedConfigVotes.end(),
                                 [&observation](const auto &v) { return v.peers == observation.state.committedPeers; });
        if (vote == committedConfigVotes.end()) {
            committedConfigVotes.emplace_back(CommittedConfigVote{ observation.state.committedPeers, 1 });
        } else {
            ++vote->confirmations;
        }
    }

    const std::vector<std::string> *quorumConfirmedPeers = nullptr;
    for (const auto &vote : committedConfigVotes) {
        if (vote.confirmations < QuorumSize(vote.peers.size())) {
            continue;
        }
        if (quorumConfirmedPeers != nullptr && *quorumConfirmedPeers != vote.peers) {
            return Status(K_NOT_READY,
                          "Coordinator bootstrap peers report quorum-confirmed configurations that conflict");
        }
        quorumConfirmedPeers = &vote.peers;
    }

    if (quorumConfirmedPeers != nullptr) {
        if (std::binary_search(quorumConfirmedPeers->begin(), quorumConfirmedPeers->end(), localState.localPeer)) {
            startPlan = BootstrapPlan{ *quorumConfirmedPeers };
        } else {
            startPlan = WaitingToJoinPlan{};
        }
        return Status::OK();
    }

    Status firstRetryableError;
    Status firstBlockingObservationError;
    std::vector<std::string> bootstrappableCandidates;
    bootstrappableCandidates.reserve(normalizedCandidates.size());
    for (const auto &observation : observations) {
        if (observation.status.IsError()) {
            if (firstRetryableError.IsOk()) {
                firstRetryableError =
                    Status(K_NOT_READY, "Coordinator bootstrap peer observation is not ready for " + observation.peer
                                            + ": " + observation.status.ToString());
            }
            continue;
        }

        const auto &peerState = observation.state;
        if (peerState.metadataState == RaftMetadataState::CORRUPT
            || peerState.metadataState == RaftMetadataState::UNKNOWN) {
            if (firstBlockingObservationError.IsOk()) {
                firstBlockingObservationError =
                    Status(K_NOT_READY, "Coordinator bootstrap peer metadata state is not ready for first bootstrap: "
                                            + observation.peer);
            }
            continue;
        }
        if (peerState.metadataState == RaftMetadataState::VALID) {
            if (peerState.committedPeers.empty() && firstBlockingObservationError.IsOk()) {
                firstBlockingObservationError =
                    Status(K_NOT_READY, "Coordinator bootstrap peer has valid metadata but no committed configuration: "
                                            + observation.peer);
            }
            continue;
        }
        if (!IsSha256Hex(peerState.candidateDigest) || peerState.candidateCount != localState.candidateCount
            || peerState.candidateDigest != localState.candidateDigest) {
            if (firstBlockingObservationError.IsOk()) {
                firstBlockingObservationError =
                    Status(K_NOT_READY,
                           "Coordinator bootstrap candidate observation digest does not match: " + observation.peer);
            }
            continue;
        }
        bootstrappableCandidates.emplace_back(observation.peer);
    }

    const size_t target = options_.membershipOptions.expectedMemberCount;
    if (observedCommittedConfiguration) {
        for (const auto &vote : committedConfigVotes) {
            if (vote.peers.size() < target
                && std::includes(normalizedCandidates.begin(), normalizedCandidates.end(), vote.peers.begin(),
                                 vote.peers.end())
                && std::binary_search(vote.peers.begin(), vote.peers.end(), localState.localPeer)) {
                startPlan = BootstrapPlan{ vote.peers };
                return Status::OK();
            }
        }
        return Status(K_NOT_READY,
                      "Coordinator bootstrap observed committed configuration but no configuration reached quorum");
    }

    if (firstBlockingObservationError.IsError()) {
        return firstBlockingObservationError;
    }
    const size_t bootstrapQuorum = QuorumSize(target);
    if (bootstrappableCandidates.size() < bootstrapQuorum) {
        if (firstRetryableError.IsError()) {
            return firstRetryableError;
        }
        return Status(K_NOT_READY,
                      FormatString("Coordinator bootstrap has %zu bootstrappable candidates but requires quorum %zu",
                                   bootstrappableCandidates.size(), bootstrapQuorum));
    }

    const size_t selectedCount = std::min(target, bootstrappableCandidates.size());
    std::vector<std::string> selected(
        bootstrappableCandidates.begin(),
        bootstrappableCandidates.begin() + static_cast<std::vector<std::string>::difference_type>(selectedCount));
    if (!std::binary_search(selected.begin(), selected.end(), localState.localPeer)) {
        startPlan = WaitingToJoinPlan{};
        return Status::OK();
    }
    startPlan = BootstrapPlan{ std::move(selected) };
    return Status::OK();
}

Status CoordinatorElectionManager::ProbePeerBootstrapState(const std::string &peer, RaftBootstrapState &state) const
{
    RETURN_IF_NOT_OK(dependencies_.probePeer(peer, options_.raftFlags.electionTimeoutMs, state));
    CHECK_FAIL_RETURN_STATUS(state.groupId == kCoordinatorRaftGroupId, K_INVALID,
                             "Coordinator bootstrap response has the wrong group id");
    braft::PeerId responsePeer;
    RETURN_IF_NOT_OK(ParseCoordinatorRaftPeer(state.localPeer, responsePeer));
    const auto normalizedResponsePeer = CoordinatorRaftPeerAddress(responsePeer);
    CHECK_FAIL_RETURN_STATUS(normalizedResponsePeer == state.localPeer && normalizedResponsePeer == peer, K_INVALID,
                             "Coordinator bootstrap response local peer does not match the probed endpoint");
    CHECK_FAIL_RETURN_STATUS(state.expectedMemberCount == options_.membershipOptions.expectedMemberCount, K_INVALID,
                             "Coordinator bootstrap response expected member count does not match");
    if (!state.probeReady) {
        return Status(K_NOT_READY, "Coordinator bootstrap peer probe is not ready");
    }
    std::vector<std::string> normalizedCommittedPeers;
    RETURN_IF_NOT_OK(NormalizePeers(state.committedPeers, normalizedCommittedPeers));
    CHECK_FAIL_RETURN_STATUS(normalizedCommittedPeers == state.committedPeers, K_INVALID,
                             "Coordinator bootstrap response committed peers are not normalized, unique, and sorted");
    CHECK_FAIL_RETURN_STATUS(state.metadataState == RaftMetadataState::VALID || normalizedCommittedPeers.empty(),
                             K_INVALID, "Coordinator bootstrap response has committed peers without valid metadata");
    state.committedPeers = std::move(normalizedCommittedPeers);
    return Status::OK();
}

CoordinatorRaftEventCallbacks CoordinatorElectionManager::BuildManagedCallbacks()
{
    CoordinatorRaftEventCallbacks managed;
    managed.onLeaderStart = [this, callback = callbacks_.onLeaderStart](int64_t term) {
        std::vector<std::string> committedPeers;
        {
            std::lock_guard<std::mutex> lock(bootstrapMutex_);
            committedPeers = bootstrapState_.committedPeers;
        }
        LOG(INFO) << "COORDINATOR_RAFT_LEADER_ELECTED current_addr=" << options_.raftFlags.localAddress
                  << " leader=" << options_.raftFlags.localAddress << " term=" << term
                  << " peers=" << VectorToString(committedPeers);
        if (!callback) {
            return;
        }
        InvokeCallback([callback, term] { callback(term); });
    };
    managed.onLeaderStop = [this, callback = callbacks_.onLeaderStop](Status status) {
        LOG(WARNING) << "COORDINATOR_RAFT_LEADER_STOPPED current_addr=" << options_.raftFlags.localAddress
                     << " status=" << status.ToString();
        if (!callback) {
            return;
        }
        InvokeCallback([callback, status = std::move(status)]() mutable { callback(std::move(status)); });
    };
    managed.onConfigurationCommitted = [this, callback = callbacks_.onConfigurationCommitted](
                                           std::vector<std::string> peers, int64_t index) {
        std::vector<std::string> normalizedPeers;
        const auto status = NormalizePeers(peers, normalizedPeers);
        if (status.IsError() || normalizedPeers.size() != peers.size() || normalizedPeers.empty()) {
            RecordBootstrapTerminalStatus(
                Status(K_DATA_INCONSISTENCY, "Coordinator committed configuration callback is invalid"));
            return;
        }
        LOG(INFO) << "COORDINATOR_RAFT_CONFIGURATION_COMMITTED current_addr=" << options_.raftFlags.localAddress
                  << " peers=" << VectorToString(normalizedPeers) << " index=" << index;
        {
            std::lock_guard<std::mutex> lock(bootstrapMutex_);
            bootstrapState_.committedPeers = normalizedPeers;
        }
        bootstrapCv_.notify_all();
        if (callback) {
            InvokeCallback(
                [callback, peers = std::move(normalizedPeers), index]() mutable { callback(std::move(peers), index); });
        }
    };
    managed.onError = [this, callback = callbacks_.onError](Status status) {
        auto callbackStatus = status;
        if (!IsRetryableRaftCallbackError(status.GetCode())) {
            RecordBootstrapTerminalStatus(std::move(status));
        }
        if (callback) {
            InvokeCallback([callback, callbackStatus]() mutable { callback(std::move(callbackStatus)); });
        }
    };
    managed.onShutdown = [callback = callbacks_.onShutdown] { InvokeCallback(callback); };
    return managed;
}

Status CoordinatorElectionManager::StartOwnedComponents(RaftStartPlan startPlan, RaftMetadataState metadataState)
{
    if (IsBootstrapStopRequested()) {
        return LifecycleInterruptedStatus("start owned components");
    }
    RaftBootstrapState snapshot;
    RETURN_IF_NOT_OK(GetBootstrapState(snapshot));
    const auto planName = RaftStartPlanName(startPlan);
    const auto planPeers = RaftStartPlanPeers(startPlan);
    LOG(INFO) << "COORDINATOR_RAFT_START_PLAN current_addr=" << options_.raftFlags.localAddress
              << " local_peer=" << snapshot.localPeer << " plan=" << planName
              << " metadata_state=" << RaftMetadataStateName(metadataState)
              << " expected_member_count=" << options_.membershipOptions.expectedMemberCount
              << " candidate_count=" << snapshot.candidateCount << " candidate_digest=" << snapshot.candidateDigest
              << " committed_peers=" << VectorToString(snapshot.committedPeers)
              << " plan_peers=" << VectorToString(planPeers);
    CoordinatorRaftOptions raftOptions{ snapshot.localPeer, options_.raftFlags.dataDir,
                                        options_.raftFlags.heartbeatIntervalMs, options_.raftFlags.electionTimeoutMs,
                                        std::move(startPlan) };

    std::unique_ptr<NodeHandle> node;
    Status status;
    node = dependencies_.createNode(raftOptions, BuildManagedCallbacks());
    if (node == nullptr) {
        status = Status(K_RUNTIME_ERROR, "Coordinator election manager failed to create raft node");
    }
    RETURN_IF_NOT_OK(status);
    Status terminalStatus;
    bool terminal = false;
    if (GetBootstrapTerminalStatus(terminalStatus)) {
        return terminalStatus;
    }

    RETURN_IF_NOT_OK(dependencies_.startNode(*node, metadataState));
    if (GetBootstrapTerminalStatus(terminalStatus)) {
        return terminalStatus;
    }
    {
        std::lock_guard<std::mutex> lock(bootstrapMutex_);
        terminal = bootstrapState_.phase == RaftBootstrapPhase::TERMINAL;
        if (terminal) {
            terminalStatus = bootstrapStatus_;
        } else {
            bootstrapState_.metadataState = RaftMetadataState::VALID;
            bootstrapState_.probeReady = true;
        }
    }
    bootstrapCv_.notify_all();
    if (terminal) {
        return terminalStatus;
    }

    bool membershipDisabled = false;
    {
        std::lock_guard<std::mutex> lock(lifecycleMutex_);
        if (state_ != LifecycleState::RUNNING || shutdownInProgress_) {
            return LifecycleInterruptedStatus("finish raft node startup");
        }
        membershipDisabled = membershipStartDisabled_;
    }

    std::unique_ptr<MembershipHandle> membership;
    if (!membershipDisabled) {
        membership = dependencies_.createMembership(options_.membershipOptions, *node, discovery_);
        if (membership == nullptr) {
            status = Status(K_RUNTIME_ERROR, "Coordinator election manager failed to create membership manager");
        }
        RETURN_IF_NOT_OK(status);
        if (GetBootstrapTerminalStatus(terminalStatus)) {
            const auto cleanupStatus = StopOwnedMembership(std::move(membership));
            RecordPendingCleanupStatus(cleanupStatus);
            return terminalStatus;
        }
        status = dependencies_.startMembership(*membership);
        if (status.IsError()) {
            const auto cleanupStatus = StopOwnedMembership(std::move(membership));
            RecordPendingCleanupStatus(cleanupStatus);
            return status;
        }
        if (GetBootstrapTerminalStatus(terminalStatus)) {
            const auto cleanupStatus = StopOwnedMembership(std::move(membership));
            RecordPendingCleanupStatus(cleanupStatus);
            return terminalStatus;
        }
    }

    bool interrupted = false;
    {
        std::lock_guard<std::mutex> lock(lifecycleMutex_);
        interrupted = state_ != LifecycleState::RUNNING || shutdownInProgress_;
        if (!interrupted) {
            std::lock_guard<std::mutex> bootstrapLock(bootstrapMutex_);
            terminal = bootstrapState_.phase == RaftBootstrapPhase::TERMINAL;
            if (terminal) {
                terminalStatus = bootstrapStatus_;
            } else {
                node_ = std::move(node);
                if (!membershipStartDisabled_) {
                    membership_ = std::move(membership);
                }
                bootstrapState_.phase = RaftBootstrapPhase::STARTED;
                bootstrapState_.statusCode = static_cast<int32_t>(K_OK);
            }
        }
    }
    bootstrapCv_.notify_all();
    if (membership != nullptr) {
        const auto cleanupStatus = StopOwnedMembership(std::move(membership));
        RecordPendingCleanupStatus(cleanupStatus);
        if (cleanupStatus.IsError() && !interrupted && !terminal) {
            return cleanupStatus;
        }
    }
    if (interrupted) {
        node.reset();
        return LifecycleInterruptedStatus("finish component startup");
    }
    if (terminal) {
        node.reset();
        return terminalStatus;
    }
    return Status::OK();
}

void CoordinatorElectionManager::PublishBootstrapState(RaftBootstrapState state)
{
    {
        std::lock_guard<std::mutex> lock(bootstrapMutex_);
        if (bootstrapState_.phase == RaftBootstrapPhase::TERMINAL) {
            return;
        }
        state.committedPeers = bootstrapState_.committedPeers;
        bootstrapState_ = std::move(state);
    }
    bootstrapCv_.notify_all();
}

bool CoordinatorElectionManager::GetBootstrapTerminalStatus(Status &status) const
{
    std::lock_guard<std::mutex> lock(bootstrapMutex_);
    if (bootstrapState_.phase != RaftBootstrapPhase::TERMINAL) {
        return false;
    }
    status = bootstrapStatus_;
    return true;
}

void CoordinatorElectionManager::RecordBootstrapTerminalStatus(Status status)
{
    const auto statusCode = status.GetCode();
    std::string localPeer;
    bool firstTerminal = false;
    {
        std::lock_guard<std::mutex> lock(bootstrapMutex_);
        if (bootstrapState_.phase != RaftBootstrapPhase::TERMINAL) {
            bootstrapStatus_ = Status(statusCode, "");
            bootstrapState_.phase = RaftBootstrapPhase::TERMINAL;
            bootstrapState_.statusCode = static_cast<int32_t>(statusCode);
            localPeer = bootstrapState_.localPeer;
            firstTerminal = true;
        }
    }
    bootstrapCv_.notify_all();
    if (firstTerminal) {
        LOG(ERROR) << "COORDINATOR_RAFT_BOOTSTRAP_TERMINAL group_id=" << kCoordinatorRaftGroupId
                   << ", local_peer=" << localPeer << ", phase=TERMINAL"
                   << ", status_code=" << static_cast<uint32_t>(statusCode)
                   << ", status_name=" << Status::StatusCodeName(statusCode);
    }
}

bool CoordinatorElectionManager::WaitForBootstrapRetryOrStop()
{
    std::unique_lock<std::mutex> lock(bootstrapMutex_);
    const auto observedWakeGeneration = bootstrapWakeGeneration_;
    ++bootstrapRetryWaiters_;
    bootstrapCv_.notify_all();
    bootstrapCv_.wait_for(lock, options_.membershipOptions.discoveryRetryInterval, [this, observedWakeGeneration] {
        return bootstrapStopRequested_ || bootstrapWakeGeneration_ != observedWakeGeneration;
    });
    --bootstrapRetryWaiters_;
    bootstrapCv_.notify_all();
    return bootstrapStopRequested_;
}

bool CoordinatorElectionManager::IsBootstrapStopRequested() const
{
    std::lock_guard<std::mutex> lock(bootstrapMutex_);
    return bootstrapStopRequested_;
}

void CoordinatorElectionManager::WarnBootstrapRetry(const Status &status,
                                                    std::chrono::steady_clock::time_point &nextWarningAt) const
{
    std::chrono::steady_clock::time_point now = dependencies_.now();
    if (now < nextWarningAt) {
        return;
    }
    LOG(WARNING) << "Coordinator Raft bootstrap has not converged: " << status;
    nextWarningAt = now + options_.membershipOptions.operationWarningTimeout;
}

Status CoordinatorElectionManager::GetBootstrapState(RaftBootstrapState &state) const
{
    std::lock_guard<std::mutex> lock(bootstrapMutex_);
    state = bootstrapState_;
    return Status::OK();
}

Status CoordinatorElectionManager::StopOwnedMembership(std::unique_ptr<MembershipHandle> membership)
{
    if (membership == nullptr) {
        return Status::OK();
    }

    return dependencies_.shutdownMembership(*membership);
}

void CoordinatorElectionManager::RecordPendingCleanupStatus(const Status &status)
{
    if (status.IsOk()) {
        return;
    }
    std::lock_guard<std::mutex> lock(lifecycleMutex_);
    if (pendingCleanupStatus_.IsOk()) {
        pendingCleanupStatus_ = status;
    }
    if (shutdownInProgress_ && shutdownStatus_.IsOk()) {
        shutdownStatus_ = status;
    }
}

Status CoordinatorElectionManager::LifecycleInterruptedStatus(const char *operation) const
{
    return Status(K_INVALID, std::string("Coordinator election manager cannot ") + operation + " during shutdown");
}

void CoordinatorElectionManager::RecordShutdownCleanupStatusLocked(uint64_t generation, const Status &status)
{
    if (shutdownInProgress_ && shutdownGeneration_ == generation && shutdownStatus_.IsOk() && status.IsError()) {
        shutdownStatus_ = status;
    }
}

Status CoordinatorElectionManager::WaitForShutdownResultLocked(std::unique_lock<std::mutex> &lock)
{
    ++stopMembershipShutdownWaiters_;
    lifecycleCv_.notify_all();
    lifecycleCv_.wait(lock, [this] { return shutdownComplete_; });
    --stopMembershipShutdownWaiters_;
    return shutdownStatus_;
}

Status CoordinatorElectionManager::StopMembership()
{
    std::unique_ptr<MembershipHandle> membership;
    uint64_t generation = 0;
    {
        std::unique_lock<std::mutex> lock(lifecycleMutex_);
        const uint64_t observedShutdownGeneration = shutdownGeneration_;
        const bool shutdownObservedAtEntry = shutdownInProgress_;
        if (lifecycleOperationInProgress_) {
            ++stopMembershipLifecycleWaiters_;
            lifecycleCv_.notify_all();
            lifecycleCv_.wait(lock, [this] { return !lifecycleOperationInProgress_; });
            --stopMembershipLifecycleWaiters_;
        }
        if (shutdownInProgress_) {
            return WaitForShutdownResultLocked(lock);
        }
        if (shutdownComplete_ && (shutdownObservedAtEntry || shutdownGeneration_ != observedShutdownGeneration)) {
            return shutdownStatus_;
        }
        if (membershipStopInProgress_) {
            generation = membershipStopGeneration_;
            ++membershipStopWaiters_;
            lifecycleCv_.notify_all();
            lifecycleCv_.wait(lock, [this, generation] { return completedMembershipStopGeneration_ >= generation; });
            --membershipStopWaiters_;
            return membershipStopStatus_;
        }
        if (membership_ == nullptr) {
            if (state_ == LifecycleState::RUNNING) {
                membershipStartDisabled_ = true;
            }
            return Status::OK();
        }
        membershipStartDisabled_ = true;
        membershipStopInProgress_ = true;
        ++membershipStopGeneration_;
        generation = membershipStopGeneration_;
        membershipStopStatus_ = Status::OK();
        membership = std::move(membership_);
    }

    const auto result = StopOwnedMembership(std::move(membership));
    {
        std::lock_guard<std::mutex> lock(lifecycleMutex_);
        membershipStopStatus_ = result;
        completedMembershipStopGeneration_ = generation;
        if (shutdownInProgress_) {
            RecordShutdownCleanupStatusLocked(shutdownGeneration_, result);
        }
        membershipStopInProgress_ = false;
    }
    lifecycleCv_.notify_all();
    return result;
}

Status CoordinatorElectionManager::Shutdown()
{
    std::thread bootstrapThread;
    std::unique_ptr<MembershipHandle> membership;
    std::unique_ptr<NodeHandle> node;
    uint64_t generation = 0;
    {
        std::unique_lock<std::mutex> lock(lifecycleMutex_);
        if (shutdownComplete_) {
            return shutdownStatus_;
        }
        if (shutdownInProgress_) {
            lifecycleCv_.wait(lock, [this] { return shutdownComplete_; });
            return shutdownStatus_;
        }

        shutdownInProgress_ = true;
        ++shutdownGeneration_;
        generation = shutdownGeneration_;
        shutdownStatus_ = pendingCleanupStatus_;
        state_ = LifecycleState::STOPPING;
        {
            std::lock_guard<std::mutex> bootstrapLock(bootstrapMutex_);
            bootstrapStopRequested_ = true;
        }
        bootstrapCv_.notify_all();
        lifecycleCv_.notify_all();
        lifecycleCv_.wait(lock, [this] { return !lifecycleOperationInProgress_; });
        bootstrapThread = std::move(bootstrapThread_);
    }

    if (bootstrapThread.joinable()) {
        bootstrapThread.join();
    }

    {
        std::unique_lock<std::mutex> lock(lifecycleMutex_);
        lifecycleCv_.wait(lock, [this] { return !membershipStopInProgress_; });
        membership = std::move(membership_);
        node = std::move(node_);
    }
    const auto cleanupResult = StopOwnedMembership(std::move(membership));
    node.reset();

    Status result;
    {
        std::lock_guard<std::mutex> lock(lifecycleMutex_);
        RecordShutdownCleanupStatusLocked(generation, cleanupResult);
        state_ = LifecycleState::STOPPED;
        shutdownComplete_ = true;
        shutdownInProgress_ = false;
        result = shutdownStatus_;
    }
    lifecycleCv_.notify_all();
    return result;
}

bool CoordinatorElectionManager::IsLeader() const
{
    std::lock_guard<std::mutex> lock(lifecycleMutex_);
    return state_ == LifecycleState::RUNNING && node_ != nullptr && dependencies_.isLeader(*node_);
}

Status CoordinatorElectionManager::GetLeader(std::string &leaderAddress) const
{
    leaderAddress.clear();
    std::lock_guard<std::mutex> lock(lifecycleMutex_);
    if (state_ != LifecycleState::RUNNING || node_ == nullptr) {
        return Status(K_NOT_READY, "Coordinator election manager cannot report a leader before raft startup");
    }
    return dependencies_.getLeader(*node_, leaderAddress);
}

}  // namespace datasystem::coordinator
