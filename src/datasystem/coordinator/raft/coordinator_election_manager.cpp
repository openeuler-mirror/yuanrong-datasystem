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
#include <future>
#include <limits>
#include <map>
#include <string_view>
#include <utility>
#include <vector>

#include <dirent.h>
#include <sys/stat.h>
#include <unistd.h>

#include "datasystem/common/rpc/brpc_factory.h"
#include "datasystem/protos/coordinator.brpc.stub.pb.h"
// brpc headers above override LOG/VLOG/DLOG via butil/logging.h.
// Include DataSystem helpers afterwards to restore the spdlog-based macros.
#include "datasystem/common/log/log.h"
#include "datasystem/common/log/trace.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/uuid_generator.h"
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
constexpr char K_COORDINATOR_BOOTSTRAP_TRACE_PREFIX[] = "CoordinatorBootstrap;";
constexpr std::chrono::milliseconds kBootstrapExchangeInterval{ 100 };
constexpr std::chrono::milliseconds kBootstrapRpcTimeout{ 100 };
constexpr std::chrono::seconds K_BOOTSTRAP_OBSERVATION_TTL{ 1 };
constexpr std::chrono::seconds K_BOOTSTRAP_CONSISTENT_VIEW_DELAY{ 1 };

struct CommittedConfigVote {
    std::vector<std::string> peers;
    size_t confirmations{ 0 };
};

void RecordCommittedConfigVote(const std::string &reporter, const std::vector<std::string> &committedPeers,
                               bool &observedCommittedConfiguration, std::vector<CommittedConfigVote> &votes)
{
    if (committedPeers.empty()) {
        return;
    }
    observedCommittedConfiguration = true;
    if (!std::binary_search(committedPeers.begin(), committedPeers.end(), reporter)) {
        return;
    }
    auto vote = std::find_if(votes.begin(), votes.end(),
                             [&committedPeers](const auto &candidate) { return candidate.peers == committedPeers; });
    if (vote == votes.end()) {
        votes.emplace_back(CommittedConfigVote{ committedPeers, 1 });
    } else {
        ++vote->confirmations;
    }
}

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

std::string GetCoordinatorBootstrapTraceId()
{
    auto traceId = Trace::Instance().GetTraceID();
    if (traceId.empty()) {
        traceId = std::string(K_COORDINATOR_BOOTSTRAP_TRACE_PREFIX) + GetStringUuid();
    }
    return traceId;
}

Status FilesystemOperationError(StatusCode statusCode, const char *operation, std::string_view entity,
                                const std::string &path, int errorNumber)
{
    return Status(statusCode, FormatString("%s failed, entity=%s, path=%s, errno=%d, errmsg=%s", operation,
                                           std::string(entity), path, errorNumber, StrErr(errorNumber)));
}

Status CheckOptionalDirectory(const std::string &path, std::string_view description, bool &exists)
{
    exists = false;
    struct stat directoryStat{};
    if (lstat(path.c_str(), &directoryStat) != 0) {
        const int errorNumber = errno;
        if (errorNumber == ENOENT) {
            return Status::OK();
        }
        return FilesystemOperationError(K_NOT_READY, "lstat", description, path, errorNumber);
    }
    CHECK_FAIL_RETURN_STATUS(S_ISDIR(directoryStat.st_mode), K_INVALID,
                             std::string(description) + " is not a directory");
    exists = true;
    return Status::OK();
}

Status CheckOptionalPersistenceFile(const std::string &path, std::string_view description, bool &hasPersistence,
                                    std::vector<std::string> &emptyPersistenceFiles)
{
    struct stat fileStat{};
    if (lstat(path.c_str(), &fileStat) != 0) {
        const int errorNumber = errno;
        if (errorNumber == ENOENT) {
            return Status::OK();
        }
        return FilesystemOperationError(K_NOT_READY, "lstat", description, path, errorNumber);
    }
    CHECK_FAIL_RETURN_STATUS(S_ISREG(fileStat.st_mode), K_INVALID,
                             std::string(description) + " is not a regular file");
    if (fileStat.st_size > 0) {
        hasPersistence = true;
    } else {
        emptyPersistenceFiles.emplace_back(path);
    }
    return Status::OK();
}

Status DeleteEmptyPersistenceFile(const std::string &path)
{
    struct stat fileStat{};
    if (lstat(path.c_str(), &fileStat) != 0) {
        const int errorNumber = errno;
        if (errorNumber == ENOENT) {
            return Status::OK();
        }
        return FilesystemOperationError(K_NOT_READY, "lstat", "empty Coordinator Raft persistence file", path,
                                        errorNumber);
    }
    CHECK_FAIL_RETURN_STATUS(S_ISREG(fileStat.st_mode), K_INVALID,
                             "Empty Coordinator Raft persistence path is not a regular file");
    CHECK_FAIL_RETURN_STATUS(fileStat.st_size == 0, K_NOT_READY,
                             "Coordinator Raft persistence file became non-empty during cleanup");
    if (unlink(path.c_str()) != 0) {
        const int errorNumber = errno;
        if (errorNumber == ENOENT) {
            return Status::OK();
        }
        return FilesystemOperationError(K_NOT_READY, "unlink", "empty Coordinator Raft persistence file", path,
                                        errorNumber);
    }
    LOG(INFO) << "Removed empty Coordinator Raft persistence file, path=" << path;
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

Status CheckCoordinatorRaftLog(const std::string &dataRoot, bool &hasPersistence,
                               std::vector<std::string> &emptyPersistenceFiles)
{
    const std::string logDirectoryPath = dataRoot + "/" + kCoordinatorRaftLogDirectory;
    bool logDirectoryExists = false;
    RETURN_IF_NOT_OK(
        CheckOptionalDirectory(logDirectoryPath, "Coordinator Raft log directory", logDirectoryExists));
    if (!logDirectoryExists) {
        return Status::OK();
    }
    RETURN_IF_NOT_OK(CheckOptionalPersistenceFile(logDirectoryPath + "/" + kCoordinatorRaftLogMetadataEntity,
                                                  "Coordinator Raft log metadata entity", hasPersistence,
                                                  emptyPersistenceFiles));

    auto *logDirectoryHandle = opendir(logDirectoryPath.c_str());
    const int openDirectoryErrorNumber = errno;
    std::unique_ptr<DIR, DirectoryCloser> logDirectory(logDirectoryHandle);
    if (logDirectory == nullptr) {
        return FilesystemOperationError(K_NOT_READY, "opendir", "Coordinator Raft log directory", logDirectoryPath,
                                        openDirectoryErrorNumber);
    }

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
        CHECK_FAIL_RETURN_STATUS(S_ISREG(segmentStat.st_mode), K_INVALID,
                                 "Coordinator Raft log segment is not a regular file");
        if (segmentStat.st_size > 0) {
            hasPersistence = true;
        } else {
            emptyPersistenceFiles.emplace_back(segmentPath);
        }
    }
    return Status::OK();
}

Status ProbeCoordinatorRaftMetadata(const std::string &dataRoot, RaftMetadataState &metadataState)
{
    metadataState = RaftMetadataState::UNKNOWN;
    bool dataRootExists = false;
    auto status = CheckOptionalDirectory(dataRoot, "Coordinator Raft data root", dataRootExists);
    if (status.IsError()) {
        return status;
    }
    if (!dataRootExists) {
        metadataState = RaftMetadataState::ABSENT;
        return Status::OK();
    }

    bool hasPersistence = false;
    std::vector<std::string> emptyPersistenceFiles;
    const std::string metadataDirectoryPath = dataRoot + "/" + kCoordinatorRaftMetadataDirectory;
    bool metadataDirectoryExists = false;
    status =
        CheckOptionalDirectory(metadataDirectoryPath, "Coordinator Raft metadata directory", metadataDirectoryExists);
    if (status.IsError()) {
        return status;
    }
    if (metadataDirectoryExists) {
        status = CheckOptionalPersistenceFile(metadataDirectoryPath + "/" + kCoordinatorRaftMetadataEntity,
                                              "Coordinator Raft metadata entity", hasPersistence,
                                              emptyPersistenceFiles);
        if (status.IsError()) {
            return status;
        }
    }
    status = CheckCoordinatorRaftLog(dataRoot, hasPersistence, emptyPersistenceFiles);
    if (status.IsError()) {
        return status;
    }

    if (hasPersistence) {
        metadataState = RaftMetadataState::VALID;
        return Status::OK();
    }
    for (const auto &emptyPersistenceFile : emptyPersistenceFiles) {
        RETURN_IF_NOT_OK(DeleteEmptyPersistenceFile(emptyPersistenceFile));
    }
    metadataState = RaftMetadataState::ABSENT;
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
    struct BootstrapChannels {
        std::mutex mutex;
        std::map<std::string, std::shared_ptr<brpc::Channel>> byPeer;
    };

    Dependencies dependencies;
    auto bootstrapChannels = std::make_shared<BootstrapChannels>();
    dependencies.probeLocalMetadata = ProbeCoordinatorRaftMetadata;
    dependencies.discoverCandidates = [](const std::shared_ptr<ICoordinatorDiscovery> &discovery,
                                         std::vector<std::string> &candidates) {
        return discovery->GetCoordinators(candidates);
    };
    dependencies.exchangeObservation = [bootstrapChannels](const std::string &peer, int32_t timeoutMs,
                                                           const RaftBootstrapObservationPb &request,
                                                           RaftBootstrapObservationPb &response) {
        std::shared_ptr<brpc::Channel> channel;
        {
            std::lock_guard<std::mutex> lock(bootstrapChannels->mutex);
            auto &cached = bootstrapChannels->byPeer[peer];
            if (cached == nullptr) {
                BrpcChannelConfig config;
                config.endpoint = peer;
                config.timeout_ms = timeoutMs;
                config.connect_timeout_ms = timeoutMs;
                config.max_retry = 0;
                config.enable_circuit_breaker = false;
                cached = BrpcChannelFactory::Create(config);
            }
            channel = cached;
        }
        CHECK_FAIL_RETURN_STATUS(channel != nullptr, K_RPC_UNAVAILABLE,
                                 "Failed to create Coordinator bootstrap observation channel");

        CoordinatorService_BrpcGenericStub stub(channel.get(), timeoutMs);
        return stub.ExchangeBootstrapObservation(request, response);
    };
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
    CHECK_FAIL_RETURN_STATUS(options_.raftFlags.bootstrapWarningIntervalMs > 0, K_INVALID,
                             "Coordinator Raft bootstrap warning interval must be positive");
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
        dependencies_.probeLocalMetadata && dependencies_.discoverCandidates && dependencies_.exchangeObservation
            && dependencies_.now
            && dependencies_.createNode && dependencies_.startNode && dependencies_.createMembership
            && dependencies_.startMembership && dependencies_.shutdownMembership && dependencies_.isLeader
            && dependencies_.getLeader,
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
    TraceGuard traceGuard = Trace::Instance().SetTraceNewID(GetCoordinatorBootstrapTraceId(), true);
    Raii exitNotifier([callback = dependencies_.onBootstrapWorkerExit] {
        if (callback) {
            callback();
        }
    });
    RaftMetadataState metadataState = RaftMetadataState::UNKNOWN;
    auto status = dependencies_.probeLocalMetadata(options_.raftFlags.dataDir, metadataState);
    if (status.IsError()) {
        RecordBootstrapTerminalStatus(std::move(status));
        return;
    }
    if (metadataState == RaftMetadataState::VALID) {
        status = StartOwnedComponents(RecoverPlan{}, RaftMetadataState::VALID);
        if (status.IsError() && !IsBootstrapStopRequested()) {
            RecordBootstrapTerminalStatus(std::move(status));
        }
        return;
    }
    if (metadataState != RaftMetadataState::ABSENT) {
        RecordBootstrapTerminalStatus(
            Status(K_DATA_INCONSISTENCY, "Coordinator local Raft metadata state is not bootstrappable"));
        return;
    }

    std::chrono::steady_clock::time_point nextWarningAt{};
    while (!IsBootstrapStopRequested()) {
        std::vector<std::string> normalizedCandidates;
        status = RefreshBootstrapTargets(normalizedCandidates);
        if (status.IsOk() && options_.bootstrapMode == RaftBootstrapMode::STATIC_INITIAL_PEERS) {
            if (normalizedCandidates.size() != options_.membershipOptions.expectedMemberCount) {
                RecordBootstrapTerminalStatus(
                    Status(K_INVALID, "Coordinator static initial peers do not match expected member count"));
                return;
            }
            status = StartOwnedComponents(BootstrapPlan{ std::move(normalizedCandidates) }, metadataState);
            if (status.IsError() && !IsBootstrapStopRequested()) {
                RecordBootstrapTerminalStatus(std::move(status));
            }
            return;
        }
        if (status.IsOk()) {
            status = ExchangeBootstrapRound(normalizedCandidates);
        }

        RaftStartPlan startPlan = RecoverPlan{};
        if (status.IsOk()) {
            status = TryBuildStartPlan(startPlan);
        }
        if (IsBootstrapStopRequested()) {
            return;
        }
        if (status.IsOk()) {
            status = StartOwnedComponents(std::move(startPlan), metadataState);
            if (status.IsError() && !IsBootstrapStopRequested()) {
                RecordBootstrapTerminalStatus(std::move(status));
            }
            return;
        }
        if (status.GetCode() != K_NOT_READY) {
            RecordBootstrapTerminalStatus(std::move(status));
            return;
        }
        WarnBootstrapRetry(status, nextWarningAt);
        if (WaitForBootstrapRetryOrStop()) {
            return;
        }
    }
}

Status CoordinatorElectionManager::RefreshBootstrapTargets(std::vector<std::string> &normalizedCandidates)
{
    std::vector<std::string> discoveredCandidates;
    Status discoveryStatus = dependencies_.discoverCandidates(discovery_, discoveredCandidates);
    if (discoveryStatus.IsError()) {
        return Status(K_NOT_READY, "Coordinator Discovery bootstrap observation failed: " + discoveryStatus.ToString());
    }

    const auto normalizeStatus = NormalizePeers(discoveredCandidates, normalizedCandidates);
    if (normalizeStatus.IsError()) {
        return Status(K_NOT_READY,
                      "Coordinator Discovery returned an invalid bootstrap candidate: " + normalizeStatus.ToString());
    }
    CHECK_FAIL_RETURN_STATUS(std::binary_search(normalizedCandidates.begin(), normalizedCandidates.end(),
                                                options_.raftFlags.localAddress),
                             K_NOT_READY, "Coordinator Discovery bootstrap view does not contain the local peer");
    return Status::OK();
}

Status CoordinatorElectionManager::ExchangeBootstrapRound(const std::vector<std::string> &normalizedCandidates)
{
    RaftBootstrapObservationPb request;
    std::vector<std::string> probeTargets;
    {
        std::lock_guard<std::mutex> lock(bootstrapMutex_);
        const auto now = dependencies_.now();
        RETURN_IF_NOT_OK(BuildLocalObservationLocked(now, request));
        probeTargets = BuildBootstrapProbeTargetsLocked(normalizedCandidates, now);
    }
    CHECK_FAIL_RETURN_STATUS(!IsBootstrapStopRequested(), K_SHUTTING_DOWN,
                             "Coordinator bootstrap observation exchange was cancelled by shutdown");

    struct ExchangeResult {
        std::string peer;
        Status status;
        RaftBootstrapObservationPb response;
    };
    std::vector<std::future<ExchangeResult>> exchanges;
    exchanges.reserve(probeTargets.size());
    for (const auto &peer : probeTargets) {
        exchanges.emplace_back(std::async(std::launch::async, [this, peer, request] {
            ExchangeResult result{ peer, Status::OK(), {} };
            result.status = dependencies_.exchangeObservation(
                peer, static_cast<int32_t>(kBootstrapRpcTimeout.count()), request, result.response);
            return result;
        }));
    }

    for (auto &exchange : exchanges) {
        auto result = exchange.get();
        if (result.status.IsError()) {
            continue;
        }
        CHECK_FAIL_RETURN_STATUS(result.response.sender_peer() == result.peer, K_INVALID,
                                 "Coordinator bootstrap response sender does not match the target peer");
        std::lock_guard<std::mutex> lock(bootstrapMutex_);
        RETURN_IF_NOT_OK(RecordPeerObservationLocked(result.response, dependencies_.now()));
    }
    return Status::OK();
}

std::vector<std::string> CoordinatorElectionManager::BuildBootstrapProbeTargetsLocked(
    const std::vector<std::string> &normalizedCandidates, std::chrono::steady_clock::time_point now)
{
    std::vector<std::string> activeTargets;
    std::vector<std::string> pendingTargets;
    for (const auto &peer : normalizedCandidates) {
        if (peer == options_.raftFlags.localAddress) {
            continue;
        }
        const auto observation = bootstrapState_.knownPeers.find(peer);
        if (observation != bootstrapState_.knownPeers.end()
            && now - observation->second.lastSeen <= K_BOOTSTRAP_OBSERVATION_TTL) {
            activeTargets.emplace_back(peer);
        } else {
            pendingTargets.emplace_back(peer);
        }
    }

    const auto probeBudget = options_.membershipOptions.expectedMemberCount;
    std::vector<std::string> targets;
    targets.reserve(std::min(probeBudget, activeTargets.size() + pendingTargets.size()));
    for (const auto &peer : activeTargets) {
        if (targets.size() == probeBudget) {
            return targets;
        }
        targets.emplace_back(peer);
    }
    if (pendingTargets.empty()) {
        bootstrapProbeCursor_ = 0;
        return targets;
    }
    const auto start = bootstrapProbeCursor_ % pendingTargets.size();
    for (size_t offset = 0; offset < pendingTargets.size() && targets.size() < probeBudget; ++offset) {
        targets.emplace_back(pendingTargets[(start + offset) % pendingTargets.size()]);
        ++bootstrapProbeCursor_;
    }
    bootstrapProbeCursor_ %= pendingTargets.size();
    return targets;
}

Status CoordinatorElectionManager::TryBuildStartPlan(RaftStartPlan &startPlan)
{
    std::lock_guard<std::mutex> lock(bootstrapMutex_);
    const auto now = dependencies_.now();
    const auto activePeers = BuildActivePeersLocked(now);
    bool decided = false;
    RETURN_IF_NOT_OK(TryBuildCommittedStartPlanLocked(activePeers, startPlan, decided));
    if (decided) {
        return Status::OK();
    }
    RETURN_IF_NOT_OK(TryBuildFreshStartPlanLocked(activePeers, now, startPlan, decided));
    return decided ? Status::OK() : Status(K_NOT_READY, "Coordinator bootstrap plan is not confirmed");
}

Status CoordinatorElectionManager::TryBuildCommittedStartPlanLocked(const std::vector<std::string> &activePeers,
                                                                    RaftStartPlan &startPlan, bool &decided) const
{
    bool observedCommittedConfiguration = false;
    std::vector<CommittedConfigVote> committedConfigVotes;
    RecordCommittedConfigVote(options_.raftFlags.localAddress, bootstrapState_.committedPeers,
                              observedCommittedConfiguration, committedConfigVotes);
    for (const auto &peer : activePeers) {
        if (peer == options_.raftFlags.localAddress) {
            continue;
        }
        const auto iter = bootstrapState_.knownPeers.find(peer);
        RecordCommittedConfigVote(peer, iter->second.committedPeers, observedCommittedConfiguration,
                                  committedConfigVotes);
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
        if (std::binary_search(quorumConfirmedPeers->begin(), quorumConfirmedPeers->end(),
                               options_.raftFlags.localAddress)) {
            startPlan = BootstrapPlan{ *quorumConfirmedPeers };
        } else {
            startPlan = WaitingToJoinPlan{};
        }
        decided = true;
        return Status::OK();
    }
    CHECK_FAIL_RETURN_STATUS(!observedCommittedConfiguration, K_NOT_READY,
                             "Coordinator bootstrap observed committed configuration without member quorum");
    return Status::OK();
}

Status CoordinatorElectionManager::TryBuildFreshStartPlanLocked(const std::vector<std::string> &activePeers,
                                                                std::chrono::steady_clock::time_point now,
                                                                RaftStartPlan &startPlan, bool &decided)
{
    const auto target = options_.membershipOptions.expectedMemberCount;
    if (activePeers.size() != target) {
        if (!bootstrapState_.frozenPlan) {
            bootstrapState_.consistentView.reset();
        }
        return Status(
            K_NOT_READY,
            FormatString("Coordinator bootstrap observes %zu active peers but requires exactly %zu", activePeers.size(),
                         target));
    }
    if (!HasCompleteConsistentViewLocked(activePeers)) {
        if (!bootstrapState_.frozenPlan) {
            bootstrapState_.consistentView.reset();
        }
        return Status(K_NOT_READY, "Coordinator bootstrap peers do not report one complete consistent view");
    }

    if (!bootstrapState_.consistentView || bootstrapState_.consistentView->peers != activePeers) {
        bootstrapState_.consistentView = RaftBootstrapState::ConsistentView{ activePeers, now };
        return Status(K_NOT_READY, "Coordinator bootstrap complete view entered the stability window");
    }
    if (now - bootstrapState_.consistentView->since < K_BOOTSTRAP_CONSISTENT_VIEW_DELAY) {
        return Status(K_NOT_READY, "Coordinator bootstrap complete view is not stable for one second");
    }
    if (!bootstrapState_.frozenPlan) {
        bootstrapState_.frozenPlan = BootstrapPlan{ activePeers };
        bootstrapState_.phase = RaftBootstrapPhase::PROPOSED;
        return Status(K_NOT_READY, "Coordinator bootstrap plan was frozen and awaits peer confirmation");
    }
    CHECK_FAIL_RETURN_STATUS(bootstrapState_.frozenPlan->initialPeers == activePeers, K_DATA_INCONSISTENCY,
                             "Coordinator bootstrap active peers changed after the plan was frozen");
    CHECK_FAIL_RETURN_STATUS(HasMatchingFrozenPlansLocked(activePeers), K_NOT_READY,
                             "Coordinator bootstrap frozen plan is not confirmed by every member");
    startPlan = *bootstrapState_.frozenPlan;
    decided = true;
    return Status::OK();
}

Status CoordinatorElectionManager::ExchangeBootstrapObservation(const RaftBootstrapObservationPb &request,
                                                                RaftBootstrapObservationPb &response)
{
    CHECK_FAIL_RETURN_STATUS(options_.bootstrapMode == RaftBootstrapMode::DISCOVERY_OBSERVATION, K_INVALID,
                             "Coordinator bootstrap observation exchange is disabled for static initial peers");
    {
        std::lock_guard<std::mutex> lock(bootstrapMutex_);
        RETURN_IF_NOT_OK(RecordPeerObservationLocked(request, dependencies_.now()));
        RETURN_IF_NOT_OK(BuildLocalObservationLocked(dependencies_.now(), response));
    }
    return Status::OK();
}

Status CoordinatorElectionManager::RecordPeerObservationLocked(const RaftBootstrapObservationPb &observation,
                                                               std::chrono::steady_clock::time_point now)
{
    std::string sender;
    std::vector<std::string> peers;
    std::vector<std::string> committedPeers;
    RETURN_IF_NOT_OK(NormalizePeerObservation(observation, sender, peers, committedPeers));

    RaftBootstrapPhase phase;
    RETURN_IF_NOT_OK(ParsePeerObservationPhase(observation, sender, peers, phase));
    bootstrapState_.knownPeers[sender] =
        RaftBootstrapState::ReceivedObservation{ std::move(peers), std::move(committedPeers), phase, now };
    return Status::OK();
}

Status CoordinatorElectionManager::NormalizePeerObservation(const RaftBootstrapObservationPb &observation,
                                                            std::string &sender, std::vector<std::string> &peers,
                                                            std::vector<std::string> &committedPeers) const
{
    if constexpr (sizeof(size_t) < sizeof(uint64_t)) {
        CHECK_FAIL_RETURN_STATUS(observation.expected_member_count() <= std::numeric_limits<size_t>::max(), K_INVALID,
                                 "Coordinator bootstrap expected member count exceeds size_t");
    }
    CHECK_FAIL_RETURN_STATUS(
        observation.expected_member_count() == options_.membershipOptions.expectedMemberCount, K_INVALID,
        "Coordinator bootstrap expected member count does not match");

    braft::PeerId senderPeer;
    RETURN_IF_NOT_OK(ParseCoordinatorRaftPeer(observation.sender_peer(), senderPeer));
    sender = CoordinatorRaftPeerAddress(senderPeer);
    CHECK_FAIL_RETURN_STATUS(
        sender == observation.sender_peer() && sender != options_.raftFlags.localAddress, K_INVALID,
        "Coordinator bootstrap sender is invalid");

    const std::vector<std::string> wirePeers(observation.peers().begin(), observation.peers().end());
    RETURN_IF_NOT_OK(NormalizePeers(wirePeers, peers));
    CHECK_FAIL_RETURN_STATUS(peers == wirePeers, K_INVALID,
                             "Coordinator bootstrap peers are not normalized and unique");
    CHECK_FAIL_RETURN_STATUS(peers.size() <= options_.membershipOptions.expectedMemberCount, K_INVALID,
                             "Coordinator bootstrap observation exceeds the expected member count");

    const std::vector<std::string> wireCommittedPeers(observation.committed_peers().begin(),
                                                      observation.committed_peers().end());
    RETURN_IF_NOT_OK(NormalizePeers(wireCommittedPeers, committedPeers));
    CHECK_FAIL_RETURN_STATUS(committedPeers == wireCommittedPeers, K_INVALID,
                             "Coordinator bootstrap committed peers are not normalized and unique");
    CHECK_FAIL_RETURN_STATUS(
        committedPeers.size() <= options_.membershipOptions.expectedMemberCount + 1, K_INVALID,
        "Coordinator bootstrap committed configuration exceeds the supported transition size");
    return Status::OK();
}

Status CoordinatorElectionManager::ParsePeerObservationPhase(const RaftBootstrapObservationPb &observation,
                                                             const std::string &sender,
                                                             const std::vector<std::string> &peers,
                                                             RaftBootstrapPhase &phase) const
{
    switch (observation.phase()) {
        case RAFT_BOOTSTRAP_OBSERVING:
            phase = RaftBootstrapPhase::OBSERVING;
            CHECK_FAIL_RETURN_STATUS(std::binary_search(peers.begin(), peers.end(), sender), K_INVALID,
                                     "Coordinator bootstrap observing view does not contain the sender");
            break;
        case RAFT_BOOTSTRAP_PROPOSED:
            phase = RaftBootstrapPhase::PROPOSED;
            CHECK_FAIL_RETURN_STATUS(
                peers.size() == options_.membershipOptions.expectedMemberCount
                    && std::binary_search(peers.begin(), peers.end(), sender),
                K_INVALID, "Coordinator bootstrap frozen plan is incomplete or excludes the sender");
            break;
        case RAFT_BOOTSTRAP_STARTED:
            phase = RaftBootstrapPhase::STARTED;
            CHECK_FAIL_RETURN_STATUS(
                peers.empty()
                    || (peers.size() == options_.membershipOptions.expectedMemberCount
                        && std::binary_search(peers.begin(), peers.end(), sender)),
                K_INVALID, "Coordinator bootstrap frozen plan is incomplete or excludes the sender");
            break;
        case RAFT_BOOTSTRAP_TERMINAL:
            phase = RaftBootstrapPhase::TERMINAL;
            break;
        default:
            return Status(K_INVALID, "Coordinator bootstrap observation has an unknown phase");
    }
    return Status::OK();
}

Status CoordinatorElectionManager::BuildLocalObservationLocked(std::chrono::steady_clock::time_point now,
                                                               RaftBootstrapObservationPb &observation) const
{
    observation.Clear();
    observation.set_sender_peer(options_.raftFlags.localAddress);
    observation.set_expected_member_count(options_.membershipOptions.expectedMemberCount);
    RaftBootstrapObservationPhasePb phase = RAFT_BOOTSTRAP_OBSERVING;
    const std::vector<std::string> *peers = nullptr;
    auto observedPeers = BuildActivePeersLocked(now);
    switch (bootstrapState_.phase) {
        case RaftBootstrapPhase::OBSERVING:
            peers = &observedPeers;
            break;
        case RaftBootstrapPhase::PROPOSED:
            phase = RAFT_BOOTSTRAP_PROPOSED;
            CHECK_FAIL_RETURN_STATUS(bootstrapState_.frozenPlan.has_value(), K_DATA_INCONSISTENCY,
                                     "Coordinator bootstrap proposed phase has no frozen plan");
            peers = &bootstrapState_.frozenPlan->initialPeers;
            break;
        case RaftBootstrapPhase::STARTED:
            phase = RAFT_BOOTSTRAP_STARTED;
            if (bootstrapState_.frozenPlan) {
                peers = &bootstrapState_.frozenPlan->initialPeers;
            } else {
                peers = nullptr;
            }
            break;
        case RaftBootstrapPhase::TERMINAL:
            phase = RAFT_BOOTSTRAP_TERMINAL;
            peers = &observedPeers;
            break;
    }
    observation.set_phase(phase);
    if (peers != nullptr) {
        for (const auto &peer : *peers) {
            observation.add_peers(peer);
        }
    }
    for (const auto &peer : bootstrapState_.committedPeers) {
        observation.add_committed_peers(peer);
    }
    return Status::OK();
}

std::vector<std::string> CoordinatorElectionManager::BuildActivePeersLocked(
    std::chrono::steady_clock::time_point now) const
{
    std::vector<std::string> activePeers{ options_.raftFlags.localAddress };
    activePeers.reserve(bootstrapState_.knownPeers.size() + 1);
    for (const auto &[peer, observation] : bootstrapState_.knownPeers) {
        if (now - observation.lastSeen <= K_BOOTSTRAP_OBSERVATION_TTL) {
            activePeers.emplace_back(peer);
        }
    }
    std::sort(activePeers.begin(), activePeers.end());
    return activePeers;
}

bool CoordinatorElectionManager::HasCompleteConsistentViewLocked(
    const std::vector<std::string> &activePeers) const
{
    for (const auto &peer : activePeers) {
        if (peer == options_.raftFlags.localAddress) {
            continue;
        }
        const auto iter = bootstrapState_.knownPeers.find(peer);
        if (iter == bootstrapState_.knownPeers.end()
            || iter->second.phase == RaftBootstrapPhase::TERMINAL
            || iter->second.peers != activePeers) {
            return false;
        }
    }
    return true;
}

bool CoordinatorElectionManager::HasMatchingFrozenPlansLocked(const std::vector<std::string> &activePeers) const
{
    for (const auto &peer : activePeers) {
        if (peer == options_.raftFlags.localAddress) {
            continue;
        }
        const auto iter = bootstrapState_.knownPeers.find(peer);
        if (iter == bootstrapState_.knownPeers.end()
            || (iter->second.phase != RaftBootstrapPhase::PROPOSED
                && iter->second.phase != RaftBootstrapPhase::STARTED)
            || iter->second.peers != bootstrapState_.frozenPlan->initialPeers) {
            return false;
        }
    }
    return true;
}

CoordinatorRaftEventCallbacks CoordinatorElectionManager::BuildManagedCallbacks()
{
    const auto callbackTraceId = GetCoordinatorBootstrapTraceId();
    CoordinatorRaftEventCallbacks managed;
    managed.onLeaderStart = [this, callback = callbacks_.onLeaderStart, callbackTraceId](int64_t term) {
        TraceGuard traceGuard = Trace::Instance().SetTraceNewID(callbackTraceId);
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
    managed.onLeaderStop = [this, callback = callbacks_.onLeaderStop, callbackTraceId](Status status) {
        TraceGuard traceGuard = Trace::Instance().SetTraceNewID(callbackTraceId);
        LOG(WARNING) << "COORDINATOR_RAFT_LEADER_STOPPED current_addr=" << options_.raftFlags.localAddress
                     << " status=" << status.ToString();
        if (!callback) {
            return;
        }
        InvokeCallback([callback, status = std::move(status)]() mutable { callback(std::move(status)); });
    };
    managed.onConfigurationCommitted = [this, callback = callbacks_.onConfigurationCommitted, callbackTraceId](
                                           std::vector<std::string> peers, int64_t index) {
        TraceGuard traceGuard = Trace::Instance().SetTraceNewID(callbackTraceId);
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
    managed.onError = [this, callback = callbacks_.onError, callbackTraceId](Status status) {
        TraceGuard traceGuard = Trace::Instance().SetTraceNewID(callbackTraceId);
        auto callbackStatus = status;
        if (!IsRetryableRaftCallbackError(status.GetCode())) {
            RecordBootstrapTerminalStatus(std::move(status));
        }
        if (callback) {
            InvokeCallback([callback, callbackStatus]() mutable { callback(std::move(callbackStatus)); });
        }
    };
    managed.onShutdown = [callback = callbacks_.onShutdown, callbackTraceId] {
        TraceGuard traceGuard = Trace::Instance().SetTraceNewID(callbackTraceId);
        InvokeCallback(callback);
    };
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
              << " local_peer=" << options_.raftFlags.localAddress << " plan=" << planName
              << " metadata_state=" << RaftMetadataStateName(metadataState)
              << " expected_member_count=" << options_.membershipOptions.expectedMemberCount
              << " committed_peers=" << VectorToString(snapshot.committedPeers)
              << " plan_peers=" << VectorToString(planPeers);
    CoordinatorRaftOptions raftOptions{ options_.raftFlags.localAddress, options_.raftFlags.dataDir,
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
            localPeer = options_.raftFlags.localAddress;
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
    ++bootstrapRetryWaiters_;
    bootstrapCv_.notify_all();
    bootstrapCv_.wait_for(lock, kBootstrapExchangeInterval, [this] { return bootstrapStopRequested_; });
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
    nextWarningAt = now + std::chrono::milliseconds(options_.raftFlags.bootstrapWarningIntervalMs);
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
