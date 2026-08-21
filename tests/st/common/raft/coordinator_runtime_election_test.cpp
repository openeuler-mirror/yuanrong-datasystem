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

#include <algorithm>
#include <array>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <filesystem>
#include <future>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <sstream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <butil/at_exit.h>
#include <gtest/gtest.h>

#include "cluster/test_port_allocator.h"
#include "common_test.h"
#include "datasystem/common/flags/common_flags.h"
#include "datasystem/common/rpc/brpc_factory.h"
#include "datasystem/common/signal/signal.h"
#include "datasystem/coordinator/coordinator_runtime.h"
#include "datasystem/coordinator/raft/coordinator_raft_types.h"
#include "datasystem/protos/coordinator.brpc.stub.pb.h"
#include "datasystem/utils/coordinator_discovery.h"
#include "datasystem/utils/status.h"

namespace datasystem::st {
namespace {
using Deadline = std::chrono::steady_clock::time_point;

constexpr char kLoopbackIp[] = "127.0.0.1";
constexpr size_t kBaselineCoordinatorCount = 3;
constexpr size_t kEndpointCapacity = 4;
constexpr size_t kInvalidRuntimeIndex = kEndpointCapacity;
constexpr int32_t kHeartbeatIntervalMs = 50;
constexpr int32_t kElectionTimeoutMs = 300;
constexpr uint32_t kHealthCheckIntervalMs = 50;
constexpr uint32_t kMemberFailureGraceMs = 500;
constexpr uint32_t kDiscoveryRetryIntervalMs = 100;
constexpr uint32_t kBootstrapWarningIntervalMs = 500;
constexpr int32_t kMinimumRpcTimeoutMs = 1;
constexpr int32_t kRpcTimeoutMs = 200;
constexpr std::chrono::milliseconds kPollInterval{ 20 };
constexpr std::chrono::milliseconds kStopIsolationWindow{ 100 };
constexpr std::chrono::milliseconds kBootstrapIsolationWindow{ 3 * kElectionTimeoutMs };
constexpr std::chrono::milliseconds kDiscoveryFailureIsolationWindow{ 2 * kDiscoveryRetryIntervalMs };
constexpr int32_t kQuorumLossElectionTimeoutMultiplier = 2;
constexpr std::chrono::seconds kCaseBudget{ 6 };
constexpr std::chrono::seconds kCtestTimeout{ 8 };
constexpr std::chrono::seconds kExpiredUnregisterAdvance{ 11 };
constexpr std::chrono::seconds kTeardownPortReleaseBudget{ 1 };

static_assert(kBaselineCoordinatorCount < kEndpointCapacity);
static_assert(kElectionTimeoutMs % kHeartbeatIntervalMs == 0);
static_assert(kHealthCheckIntervalMs < kMemberFailureGraceMs);
static_assert(kHealthCheckIntervalMs <= kDiscoveryRetryIntervalMs);
static_assert(kCaseBudget < kCtestTimeout);

std::vector<size_t> BaselineIndexes()
{
    return { 0, 1, 2 };
}

template <typename Predicate>
bool WaitUntil(Predicate &&predicate, Deadline deadline)
{
    while (std::chrono::steady_clock::now() < deadline) {
        if (predicate()) {
            return true;
        }
        const auto nextPoll = std::min(deadline, std::chrono::steady_clock::now() + kPollInterval);
        std::this_thread::sleep_until(nextPoll);
    }
    return predicate();
}

struct StrictDeadlineResult {
    bool satisfiedByDeadline{ false };
    Deadline completionTime{};
};

template <typename Predicate>
StrictDeadlineResult WaitUntilNoLaterThan(Predicate &&predicate, Deadline deadline)
{
    StrictDeadlineResult result;
    while (true) {
        const auto pollStart = std::chrono::steady_clock::now();
        if (pollStart >= deadline) {
            result.completionTime = pollStart;
            return result;
        }

        const bool predicateSatisfied = predicate();
        result.completionTime = std::chrono::steady_clock::now();
        result.satisfiedByDeadline = predicateSatisfied && result.completionTime <= deadline;
        if (predicateSatisfied || result.completionTime >= deadline) {
            return result;
        }
        const auto nextPoll = std::min(deadline, result.completionTime + kPollInterval);
        std::this_thread::sleep_until(nextPoll);
    }
}

bool CanBindLoopbackPort(int port)
{
    const int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        return false;
    }
    constexpr int enabled = 1;
    if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &enabled, sizeof(enabled)) != 0) {
        close(fd);
        return false;
    }

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(static_cast<uint16_t>(port));
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    const bool canBind = bind(fd, reinterpret_cast<const sockaddr *>(&address), sizeof(address)) == 0;
    close(fd);
    return canBind;
}

class CoordinatorDiscoveryMock final : public ICoordinatorDiscovery {
private:
    using TimePoint = std::chrono::steady_clock::time_point;
    using GenerationKey = std::pair<std::string, size_t>;

    struct PendingUnregister {
        size_t generation;
        TimePoint deadline;
    };

    struct CallbackEvent {
        std::string service;
        size_t generation;
        std::string action;
    };

    struct SharedRegistrationState {
        mutable std::mutex mutex;
        std::condition_variable registeredCv;
        std::set<std::string> serviceList;
        std::map<std::string, size_t> activeGeneration;
        std::map<std::string, PendingUnregister> pendingUnregister;
        std::map<GenerationKey, size_t> registerCount;
        std::map<GenerationKey, size_t> unregisterCount;
        std::vector<CallbackEvent> callbackEvents;
        std::chrono::steady_clock::duration timeOffset{};
        size_t registrationBarrierTarget{ 0 };
        bool registrationBarrierTimedOut{ false };
        bool registrationBarrierCancelled{ false };
    };

public:
    CoordinatorDiscoveryMock() = default;
    ~CoordinatorDiscoveryMock() override = default;

    void ShareRegistrationStateFrom(const CoordinatorDiscoveryMock &source)
    {
        sharedState_ = source.sharedState_;
    }

    void SetRegistrationBarrierTarget(size_t target)
    {
        std::lock_guard<std::mutex> lock(sharedState_->mutex);
        sharedState_->registrationBarrierTarget = target;
        sharedState_->registrationBarrierTimedOut = false;
        sharedState_->registrationBarrierCancelled = false;
    }

    void SetCandidates(std::vector<std::string> candidates)
    {
        std::lock_guard<std::mutex> lock(sharedState_->mutex);
        candidateOverride_ = std::move(candidates);
        hasCandidateOverride_ = true;
    }

    void ClearCandidateOverride()
    {
        std::lock_guard<std::mutex> lock(sharedState_->mutex);
        candidateOverride_.clear();
        hasCandidateOverride_ = false;
    }

    Status GetCoordinators(std::vector<std::string> &serviceList) override
    {
        std::lock_guard<std::mutex> lock(sharedState_->mutex);
        ++discoveryQueryCount_;
        RemoveExpiredLocked(NowLocked());
        if (getStatus_.IsError()) {
            serviceList.clear();
            return getStatus_;
        }
        if (hasCandidateOverride_) {
            serviceList = candidateOverride_;
        } else {
            serviceList.assign(sharedState_->serviceList.begin(), sharedState_->serviceList.end());
        }
        return Status::OK();
    }

    void Register(const std::string &service, size_t generation)
    {
        std::unique_lock<std::mutex> lock(sharedState_->mutex);
        ++sharedState_->registerCount[{ service, generation }];
        sharedState_->callbackEvents.push_back(CallbackEvent{ service, generation, "register" });
        const auto active = sharedState_->activeGeneration.find(service);
        if (active != sharedState_->activeGeneration.end() && active->second > generation) {
            return;
        }
        sharedState_->serviceList.insert(service);
        sharedState_->activeGeneration[service] = generation;
        sharedState_->pendingUnregister.erase(service);
        if (sharedState_->registrationBarrierCancelled
            || sharedState_->serviceList.size() >= sharedState_->registrationBarrierTarget) {
            sharedState_->registeredCv.notify_all();
            return;
        }

        const auto deadline = std::chrono::steady_clock::now() + kRegistrationBarrierTimeout;
        if (!sharedState_->registeredCv.wait_until(lock, deadline, [this] {
                return sharedState_->registrationBarrierCancelled
                       || sharedState_->serviceList.size() >= sharedState_->registrationBarrierTarget;
            })) {
            sharedState_->registrationBarrierTimedOut = true;
        }
    }

    void Unregister(const std::string &service, size_t generation)
    {
        std::lock_guard<std::mutex> lock(sharedState_->mutex);
        ++sharedState_->unregisterCount[{ service, generation }];
        sharedState_->callbackEvents.push_back(CallbackEvent{ service, generation, "unregister" });
        const auto active = sharedState_->activeGeneration.find(service);
        if (active != sharedState_->activeGeneration.end() && active->second == generation) {
            sharedState_->pendingUnregister[service] = PendingUnregister{ generation, NowLocked() + kUnregisterDelay };
        }
    }

    void CancelRegistrationBarrier()
    {
        std::lock_guard<std::mutex> lock(sharedState_->mutex);
        sharedState_->registrationBarrierCancelled = true;
        sharedState_->registeredCv.notify_all();
    }

    void AdvanceTimeForTest(std::chrono::seconds delta)
    {
        std::lock_guard<std::mutex> lock(sharedState_->mutex);
        sharedState_->timeOffset += delta;
    }

    void SetGetStatus(Status status)
    {
        std::lock_guard<std::mutex> lock(sharedState_->mutex);
        getStatus_ = std::move(status);
    }

    size_t DiscoveryQueryCount() const
    {
        std::lock_guard<std::mutex> lock(sharedState_->mutex);
        return discoveryQueryCount_;
    }

    std::vector<std::string> RegisteredServicesForDiagnostics() const
    {
        std::lock_guard<std::mutex> lock(sharedState_->mutex);
        return { sharedState_->serviceList.begin(), sharedState_->serviceList.end() };
    }

    Status GetStatusForDiagnostics() const
    {
        std::lock_guard<std::mutex> lock(sharedState_->mutex);
        return getStatus_;
    }

    size_t RegisteredCount() const
    {
        std::lock_guard<std::mutex> lock(sharedState_->mutex);
        return sharedState_->serviceList.size();
    }

    bool RegistrationBarrierTimedOut() const
    {
        std::lock_guard<std::mutex> lock(sharedState_->mutex);
        return sharedState_->registrationBarrierTimedOut;
    }

    size_t RegisterCount(const std::string &service, size_t generation) const
    {
        std::lock_guard<std::mutex> lock(sharedState_->mutex);
        const auto iter = sharedState_->registerCount.find({ service, generation });
        return iter == sharedState_->registerCount.end() ? 0 : iter->second;
    }

    size_t UnregisterCount(const std::string &service, size_t generation) const
    {
        std::lock_guard<std::mutex> lock(sharedState_->mutex);
        const auto iter = sharedState_->unregisterCount.find({ service, generation });
        return iter == sharedState_->unregisterCount.end() ? 0 : iter->second;
    }

    std::vector<std::string> CallbackEvents(const std::string &service) const
    {
        std::lock_guard<std::mutex> lock(sharedState_->mutex);
        std::vector<std::string> events;
        for (const auto &event : sharedState_->callbackEvents) {
            if (event.service == service) {
                events.emplace_back("g" + std::to_string(event.generation) + ":" + event.action);
            }
        }
        return events;
    }

private:
    static constexpr std::chrono::seconds kUnregisterDelay{ 10 };
    static constexpr std::chrono::seconds kRegistrationBarrierTimeout{ 2 };

    TimePoint NowLocked() const
    {
        return std::chrono::steady_clock::now() + sharedState_->timeOffset;
    }

    void RemoveExpiredLocked(TimePoint now)
    {
        for (auto iter = sharedState_->pendingUnregister.begin(); iter != sharedState_->pendingUnregister.end();) {
            if (iter->second.deadline > now) {
                ++iter;
                continue;
            }
            const auto active = sharedState_->activeGeneration.find(iter->first);
            if (active != sharedState_->activeGeneration.end() && active->second == iter->second.generation) {
                sharedState_->serviceList.erase(iter->first);
                sharedState_->activeGeneration.erase(active);
            }
            iter = sharedState_->pendingUnregister.erase(iter);
        }
    }

    std::shared_ptr<SharedRegistrationState> sharedState_{ std::make_shared<SharedRegistrationState>() };
    Status getStatus_;
    std::vector<std::string> candidateOverride_;
    size_t discoveryQueryCount_{ 0 };
    bool hasCandidateOverride_{ false };
};

class CoordinatorRuntimeMock final : public CoordinatorRuntime {
public:
    explicit CoordinatorRuntimeMock(coordinator::CoordinatorRaftFlags flags) : flags_(std::move(flags))
    {
    }

protected:
    coordinator::CoordinatorRaftFlags GetRaftFlags() const override
    {
        return flags_;
    }

private:
    coordinator::CoordinatorRaftFlags flags_;
};

struct RuntimeGeneration {
    RuntimeGeneration(size_t generation, coordinator::CoordinatorRaftFlags flags)
        : generation(generation), runtime(std::make_unique<CoordinatorRuntimeMock>(std::move(flags))),
          result(resultPromise.get_future().share())
    {
    }

    size_t generation;
    std::unique_ptr<CoordinatorRuntimeMock> runtime;
    std::promise<Status> resultPromise;
    std::shared_future<Status> result;
    std::thread thread;
};

struct CompletedGeneration {
    size_t generation;
    Status result;
};

struct RuntimeSlot {
    size_t nextGeneration{ 1 };
    std::unique_ptr<RuntimeGeneration> active;
    std::vector<CompletedGeneration> completed;
};

struct LeaderObservation {
    std::string endpoint;
    size_t leaderIndex{ kInvalidRuntimeIndex };
};

struct BusinessRpcObservation {
    Status status;
    std::string coordinatorId;
    coordinator::ResponseHeader header;
};

struct BootstrapRpcObservation {
    Status status;
    coordinator::GetRaftBootstrapStateRspPb response;
};
}  // namespace

class CoordinatorRuntimeElectionTest : public CommonTest {
public:
    CoordinatorRuntimeElectionTest() : CommonTest(std::to_string(getpid()))
    {
    }

protected:
    void SetUp() override
    {
        CommonTest::SetUp();
        savedExitFlag_ = g_exitFlag;
        g_exitFlag = 0;

        rootDir_ = testCasePath_ + "/coordinator-runtime-election";
        std::error_code error;
        std::filesystem::remove_all(rootDir_, error);
        ASSERT_FALSE(error) << error.message();
        ASSERT_TRUE(std::filesystem::create_directories(rootDir_, error)) << error.message();

        const auto *testInfo = testing::UnitTest::GetInstance()->current_test_info();
        const std::string testName = testInfo == nullptr
                                         ? "unknown"
                                         : std::string(testInfo->test_case_name()) + "." + testInfo->name();
        auto &allocator = TestPortAllocator::Instance();
        allocator.SetOwnerInfo("coordinator_runtime_election_test", testName, rootDir_);
        const std::vector<std::string> roles{ "coordinator_runtime_0", "coordinator_runtime_1",
                                              "coordinator_runtime_2", "coordinator_runtime_3" };
        const auto reserveStatus = allocator.ReserveBatch(roles, portLeases_);
        ASSERT_TRUE(reserveStatus.IsOk()) << reserveStatus.ToString();
        ASSERT_EQ(portLeases_.size(), kEndpointCapacity);

        for (size_t index = 0; index < kEndpointCapacity; ++index) {
            endpoints_[index] = std::string(kLoopbackIp) + ":" + std::to_string(portLeases_[index].Port());
            dataRoots_[index] = rootDir_ + "/raft-data-" + std::to_string(index);
        }
    }

    void TearDown() override
    {
        if (discovery_ != nullptr) {
            discovery_->CancelRegistrationBarrier();
        }
        for (auto &slot : runtimes_) {
            if (slot.active != nullptr && slot.active->runtime != nullptr) {
                EXPECT_TRUE(slot.active->runtime->Stop().IsOk());
            }
        }
        for (size_t index = 0; index < kEndpointCapacity; ++index) {
            auto &slot = runtimes_[index];
            if (slot.active == nullptr) {
                continue;
            }
            auto &generation = *slot.active;
            if (generation.thread.joinable()) {
                generation.thread.join();
            }
            if (generation.result.valid()
                && generation.result.wait_for(std::chrono::seconds::zero()) == std::future_status::ready) {
                try {
                    const auto result = generation.result.get();
                    EXPECT_TRUE(result.IsOk()) << RuntimeLabel(index, generation.generation) << ": " << result.ToString();
                    slot.completed.push_back(CompletedGeneration{ generation.generation, result });
                } catch (const std::exception &error) {
                    ADD_FAILURE() << RuntimeLabel(index, generation.generation)
                                  << " lifecycle future threw: " << error.what();
                } catch (...) {
                    ADD_FAILURE() << RuntimeLabel(index, generation.generation)
                                  << " lifecycle future threw a non-standard exception";
                }
            } else {
                ADD_FAILURE() << RuntimeLabel(index, generation.generation)
                              << " lifecycle future was not ready after join";
            }
            generation.runtime.reset();
            slot.active.reset();
        }

        const auto portDeadline = std::chrono::steady_clock::now() + kTeardownPortReleaseBudget;
        for (size_t index = 0; index < portLeases_.size(); ++index) {
            EXPECT_TRUE(WaitUntil([this, index] { return CanBindLoopbackPort(portLeases_[index].Port()); }, portDeadline))
                << "Coordinator endpoint remained bound during teardown: index=" << index
                << ", endpoint=" << endpoints_[index];
        }

        discovery_.reset();
        TestPortAllocator::Instance().ReleaseAll();
        g_exitFlag = savedExitFlag_;
        if (!testCasePath_.empty()) {
            std::error_code error;
            std::filesystem::remove_all(testCasePath_, error);
            EXPECT_FALSE(error) << error.message();
        }
        CommonTest::TearDown();
    }

    coordinator::CoordinatorRaftFlags MakeRaftFlags(size_t index) const
    {
        return coordinator::CoordinatorRaftFlags{ endpoints_[index],
                                                  dataRoots_[index],
                                                  kHeartbeatIntervalMs,
                                                  kElectionTimeoutMs,
                                                  kDiscoveryRetryIntervalMs,
                                                  kMemberFailureGraceMs,
                                                  kHealthCheckIntervalMs,
                                                  kBootstrapWarningIntervalMs };
    }

    size_t LaunchRuntime(size_t index, size_t expectedMemberCount = kBaselineCoordinatorCount,
                         std::shared_ptr<CoordinatorDiscoveryMock> runtimeDiscovery = nullptr)
    {
        if (index >= kEndpointCapacity || discovery_ == nullptr || runtimes_[index].active != nullptr
            || !CanBindLoopbackPort(portLeases_[index].Port())) {
            return 0;
        }

        auto &slot = runtimes_[index];
        const size_t generationNumber = slot.nextGeneration++;
        slot.active = std::make_unique<RuntimeGeneration>(generationNumber, MakeRaftFlags(index));
        auto &generation = *slot.active;

        auto provider = runtimeDiscovery == nullptr ? discovery_ : std::move(runtimeDiscovery);
        CoordinatorOptions options;
        options.onStart = [provider, endpoint = endpoints_[index], generationNumber] {
            provider->Register(endpoint, generationNumber);
            return Status::OK();
        };
        options.onStop = [provider, endpoint = endpoints_[index], generationNumber] {
            provider->Unregister(endpoint, generationNumber);
            return Status::OK();
        };
        options.coordinatorDiscovery = provider;
        options.expectedMemberCount = static_cast<int>(expectedMemberCount);

        auto *runtime = generation.runtime.get();
        auto *resultPromise = &generation.resultPromise;
        generation.thread = std::thread([runtime, resultPromise, options = std::move(options)]() mutable {
            try {
                resultPromise->set_value(runtime->InitAndRun(options));
            } catch (const std::exception &error) {
                resultPromise->set_value(
                    Status(K_RUNTIME_ERROR, std::string("Coordinator Runtime test thread threw: ") + error.what()));
            } catch (...) {
                resultPromise->set_value(
                    Status(K_RUNTIME_ERROR, "Coordinator Runtime test thread threw a non-standard exception"));
            }
        });
        return generationNumber;
    }

    bool LifecycleCompleted(size_t index) const
    {
        const auto &active = runtimes_[index].active;
        return active != nullptr && active->result.valid()
               && active->result.wait_for(std::chrono::seconds::zero()) == std::future_status::ready;
    }

    bool AllLifecyclesRunning(const std::vector<size_t> &indexes) const
    {
        return std::all_of(indexes.begin(), indexes.end(), [this](size_t index) {
            return runtimes_[index].active != nullptr && !LifecycleCompleted(index);
        });
    }

    bool ObserveUniqueLeader(const std::vector<size_t> &indexes, LeaderObservation &observation,
                             const std::string &requiredLeader = {}, const std::string &forbiddenLeader = {}) const
    {
        observation = {};
        std::string commonLeader;
        size_t leaderCount = 0;
        bool leaderBelongsToObservedSet = false;
        for (const auto index : indexes) {
            const auto &active = runtimes_[index].active;
            if (active == nullptr || LifecycleCompleted(index)) {
                return false;
            }
            std::string leader;
            const auto leaderStatus = active->runtime->GetLeader(leader);
            if (leaderStatus.IsError() || leader.empty()) {
                return false;
            }
            if (commonLeader.empty()) {
                commonLeader = leader;
            } else if (leader != commonLeader) {
                return false;
            }
            if (active->runtime->IsLeader()) {
                ++leaderCount;
                observation.leaderIndex = index;
            }
        }
        for (const auto index : indexes) {
            leaderBelongsToObservedSet = leaderBelongsToObservedSet || endpoints_[index] == commonLeader;
        }
        if (!leaderBelongsToObservedSet || leaderCount != 1 || observation.leaderIndex >= kEndpointCapacity
            || endpoints_[observation.leaderIndex] != commonLeader
            || (!requiredLeader.empty() && commonLeader != requiredLeader)
            || (!forbiddenLeader.empty() && commonLeader == forbiddenLeader)) {
            return false;
        }
        observation.endpoint = std::move(commonLeader);
        return true;
    }

    std::unique_ptr<brpc::Channel> CreateBusinessChannel(size_t index, Deadline deadline) const
    {
        const int64_t remainingMs = std::chrono::duration_cast<std::chrono::milliseconds>(
                                        deadline - std::chrono::steady_clock::now())
                                        .count();
        const auto timeoutMs = static_cast<int32_t>(
            std::clamp<int64_t>(remainingMs, kMinimumRpcTimeoutMs, kRpcTimeoutMs));
        BrpcChannelConfig config;
        config.endpoint = endpoints_[index];
        config.timeout_ms = timeoutMs;
        config.connect_timeout_ms = timeoutMs;
        config.max_retry = 0;
        config.enable_circuit_breaker = false;
        return BrpcChannelFactory::Create(config);
    }

    BusinessRpcObservation CallBusinessRpc(size_t index, Deadline deadline) const
    {
        auto channel = CreateBusinessChannel(index, deadline);
        if (channel == nullptr) {
            BusinessRpcObservation observation;
            observation.status = Status(K_RUNTIME_ERROR, "Failed to create Coordinator business channel");
            lastBusinessStatus_[index] = observation.status.ToString();
            return observation;
        }

        coordinator::CoordinatorService_BrpcGenericStub stub(channel.get());
        coordinator::GetCoordinatorIdReqPb request;
        coordinator::GetCoordinatorIdRspPb response;
        BusinessRpcObservation observation;
        observation.status = stub.GetCoordinatorId(request, response);
        if (observation.status.IsOk()) {
            observation.coordinatorId = response.header().coordinator_id();
            observation.header = response.header();
        }
        lastBusinessStatus_[index] = observation.status.ToString();
        return observation;
    }

    bool IsBusinessServing(const BusinessRpcObservation &observation) const
    {
        return observation.status.IsOk() && observation.header.is_leader()
               && observation.header.serving_state() == coordinator::ResponseHeader::LEADER_SERVING;
    }

    bool BusinessGatesMatchLeader(const std::vector<size_t> &indexes, size_t leaderIndex, Deadline deadline) const
    {
        for (const auto index : indexes) {
            const auto observation = CallBusinessRpc(index, deadline);
            if (index == leaderIndex) {
                if (!IsBusinessServing(observation) || observation.coordinatorId.empty()) {
                    return false;
                }
                continue;
            }
            if (observation.status.IsOk()) {
                if (observation.coordinatorId.empty() || observation.header.is_leader()
                    || observation.header.serving_state() != coordinator::ResponseHeader::FOLLOWER_SERVING
                    || observation.header.leader_address().empty()) {
                    return false;
                }
                continue;
            }
            if (observation.status.GetCode() != K_NOT_READY) {
                return false;
            }
        }
        return true;
    }

    bool ObserveUniqueServingLeader(const std::vector<size_t> &indexes, LeaderObservation &observation,
                                    Deadline deadline, const std::string &requiredLeader = {},
                                    const std::string &forbiddenLeader = {}) const
    {
        return ObserveUniqueLeader(indexes, observation, requiredLeader, forbiddenLeader)
               && BusinessGatesMatchLeader(indexes, observation.leaderIndex, deadline);
    }

    BootstrapRpcObservation CallBootstrapRpc(size_t index, Deadline deadline) const
    {
        BootstrapRpcObservation observation;
        auto channel = CreateBusinessChannel(index, deadline);
        if (channel == nullptr) {
            observation.status = Status(K_RUNTIME_ERROR, "Failed to create Coordinator bootstrap channel");
            lastBootstrapStatus_[index] = observation.status.ToString();
            return observation;
        }

        coordinator::CoordinatorService_BrpcGenericStub stub(channel.get());
        coordinator::GetRaftBootstrapStateReqPb request;
        request.set_group_id(coordinator::kCoordinatorRaftGroupId);
        observation.status = stub.GetRaftBootstrapState(request, observation.response);
        lastBootstrapStatus_[index] = observation.status.ToString();
        return observation;
    }

    bool HasCommittedConfiguration(size_t index, const std::vector<std::string> &expectedPeers,
                                   Deadline deadline, size_t expectedMemberCount = kBaselineCoordinatorCount) const
    {
        const auto observation = CallBootstrapRpc(index, deadline);
        const auto &response = observation.response;
        if (observation.status.IsError() || !response.probe_ready()
            || response.metadata_state() != coordinator::RAFT_METADATA_VALID
            || response.local_peer() != endpoints_[index]
            || response.expected_member_count() != expectedMemberCount
            || response.phase() != coordinator::RAFT_BOOTSTRAP_STARTED
            || response.status_code() != static_cast<int32_t>(K_OK)) {
            return false;
        }

        std::vector<std::string> committedPeers(response.committed_peers().begin(), response.committed_peers().end());
        auto normalizedExpected = expectedPeers;
        std::sort(normalizedExpected.begin(), normalizedExpected.end());
        return committedPeers == normalizedExpected;
    }

    bool HasWaitingBootstrapState(size_t index, size_t expectedCandidateCount, Deadline deadline,
                                  coordinator::RaftMetadataStatePb expectedMetadataState,
                                  coordinator::RaftBootstrapPhasePb expectedPhase, int32_t expectedStatusCode) const
    {
        const auto observation = CallBootstrapRpc(index, deadline);
        const auto &response = observation.response;
        return observation.status.IsOk() && response.probe_ready()
               && response.group_id() == coordinator::kCoordinatorRaftGroupId
               && response.local_peer() == endpoints_[index]
               && response.expected_member_count() == kBaselineCoordinatorCount
               && response.metadata_state() == expectedMetadataState
               && response.candidate_count() == expectedCandidateCount && !response.candidate_digest().empty()
               && response.committed_peers().empty() && response.phase() == expectedPhase
               && response.status_code() == expectedStatusCode;
    }

    bool HasPersistedData(size_t index) const
    {
        std::error_code error;
        const std::filesystem::path dataRoot(dataRoots_[index]);
        return std::filesystem::is_directory(dataRoot, error) && !error && !std::filesystem::is_empty(dataRoot, error)
               && !error;
    }

    bool StopAndJoin(size_t index, Deadline deadline, std::string &error)
    {
        error.clear();
        if (index >= kEndpointCapacity || runtimes_[index].active == nullptr) {
            error = "no active Runtime generation for endpoint index " + std::to_string(index);
            return false;
        }

        auto &slot = runtimes_[index];
        auto &generation = *slot.active;
        const auto stopStatus = generation.runtime->Stop();
        if (stopStatus.IsError()) {
            error = RuntimeLabel(index, generation.generation) + " Stop failed: " + stopStatus.ToString();
        }
        if (!WaitUntil([this, index] { return LifecycleCompleted(index); }, deadline)) {
            if (!error.empty()) {
                error += "; ";
            }
            error += RuntimeLabel(index, generation.generation) + " lifecycle did not stop";
            return false;
        }
        if (generation.thread.joinable()) {
            generation.thread.join();
        }

        Status lifecycleResult;
        try {
            lifecycleResult = generation.result.get();
        } catch (const std::exception &exception) {
            if (!error.empty()) {
                error += "; ";
            }
            error += RuntimeLabel(index, generation.generation) + " future threw: " + exception.what();
            generation.runtime.reset();
            slot.active.reset();
            return false;
        } catch (...) {
            if (!error.empty()) {
                error += "; ";
            }
            error += RuntimeLabel(index, generation.generation) + " future threw a non-standard exception";
            generation.runtime.reset();
            slot.active.reset();
            return false;
        }
        if (lifecycleResult.IsError()) {
            if (!error.empty()) {
                error += "; ";
            }
            error += RuntimeLabel(index, generation.generation) + " returned: " + lifecycleResult.ToString();
        }

        const size_t completedGeneration = generation.generation;
        generation.runtime.reset();
        slot.completed.push_back(CompletedGeneration{ completedGeneration, lifecycleResult });
        slot.active.reset();
        if (!WaitUntil([this, index] { return CanBindLoopbackPort(portLeases_[index].Port()); }, deadline)) {
            if (!error.empty()) {
                error += "; ";
            }
            error += RuntimeLabel(index, completedGeneration) + " endpoint was not released";
        }
        return error.empty();
    }

    bool LifecyclesRemainPendingThroughIsolationWindow(const std::array<size_t, 2> &indexes,
                                                       Deadline caseDeadline) const
    {
        const auto now = std::chrono::steady_clock::now();
        const auto isolationDeadline = now + kStopIsolationWindow;
        if (isolationDeadline > caseDeadline) {
            return false;
        }
        while (std::chrono::steady_clock::now() < isolationDeadline) {
            if (LifecycleCompleted(indexes[0]) || LifecycleCompleted(indexes[1])) {
                return false;
            }
            const auto nextPoll = std::min(isolationDeadline, std::chrono::steady_clock::now() + kPollInterval);
            std::this_thread::sleep_until(nextPoll);
        }
        return !LifecycleCompleted(indexes[0]) && !LifecycleCompleted(indexes[1]);
    }

    std::vector<size_t> FollowersOf(const LeaderObservation &observation,
                                    const std::vector<size_t> &indexes = BaselineIndexes()) const
    {
        std::vector<size_t> followers;
        for (const auto index : indexes) {
            if (index != observation.leaderIndex) {
                followers.emplace_back(index);
            }
        }
        return followers;
    }

    std::vector<size_t> SortedIndexes(std::vector<size_t> indexes) const
    {
        std::sort(indexes.begin(), indexes.end(), [this](size_t lhs, size_t rhs) {
            return endpoints_[lhs] < endpoints_[rhs];
        });
        return indexes;
    }

    std::vector<std::string> EndpointSet(const std::vector<size_t> &indexes) const
    {
        std::vector<std::string> endpointSet;
        endpointSet.reserve(indexes.size());
        for (const auto index : indexes) {
            endpointSet.emplace_back(endpoints_[index]);
        }
        std::sort(endpointSet.begin(), endpointSet.end());
        return endpointSet;
    }

    std::vector<std::string> BaselineEndpointSet() const
    {
        return EndpointSet(BaselineIndexes());
    }

    bool RemainNonServingWithoutConfiguration(const std::vector<size_t> &indexes,
                                               std::chrono::milliseconds isolationWindow, Deadline caseDeadline,
                                               size_t expectedCandidateCount) const
    {
        const auto isolationDeadline = std::chrono::steady_clock::now() + isolationWindow;
        if (isolationDeadline > caseDeadline) {
            return false;
        }
        while (std::chrono::steady_clock::now() < isolationDeadline) {
            for (const auto index : indexes) {
                if (runtimes_[index].active == nullptr || LifecycleCompleted(index)
                    || runtimes_[index].active->runtime->IsLeader()) {
                    return false;
                }
                std::string leader;
                if (runtimes_[index].active->runtime->GetLeader(leader).GetCode() != K_NOT_READY || !leader.empty()) {
                    return false;
                }
                if (CallBusinessRpc(index, caseDeadline).status.GetCode() != K_NOT_READY) {
                    return false;
                }
                const auto bootstrap = CallBootstrapRpc(index, caseDeadline);
                const bool observing = bootstrap.response.phase() == coordinator::RAFT_BOOTSTRAP_OBSERVING
                                       && bootstrap.response.status_code() == static_cast<int32_t>(K_OK);
                const bool retrying = bootstrap.response.phase() == coordinator::RAFT_BOOTSTRAP_RETRYING
                                      && bootstrap.response.status_code() == static_cast<int32_t>(K_NOT_READY);
                if (bootstrap.status.IsError() || !bootstrap.response.probe_ready()
                    || bootstrap.response.candidate_count() != expectedCandidateCount
                    || bootstrap.response.committed_peers_size() != 0 || (!observing && !retrying)) {
                    return false;
                }
            }
            const auto nextPoll = std::min(isolationDeadline, std::chrono::steady_clock::now() + kPollInterval);
            std::this_thread::sleep_until(nextPoll);
        }
        return true;
    }

    void StartBaselineCluster(Deadline deadline, LeaderObservation &observation)
    {
        discovery_ = std::make_shared<CoordinatorDiscoveryMock>();
        discovery_->SetRegistrationBarrierTarget(kBaselineCoordinatorCount);
        const auto indexes = BaselineIndexes();
        for (const auto index : indexes) {
            ASSERT_EQ(LaunchRuntime(index), 1U) << "Failed to launch " << RuntimeLabel(index, 1);
        }
        ASSERT_TRUE(WaitUntil(
            [this, &indexes] {
                return discovery_->RegisteredCount() == kBaselineCoordinatorCount
                       && !discovery_->RegistrationBarrierTimedOut() && AllLifecyclesRunning(indexes);
            },
            deadline))
            << FailureDiagnostics("baseline registration", indexes);
        ASSERT_FALSE(discovery_->RegistrationBarrierTimedOut());
        ASSERT_TRUE(WaitUntil(
            [this, &indexes, &observation, deadline] {
                return ObserveUniqueServingLeader(indexes, observation, deadline)
                       && HasCommittedConfiguration(observation.leaderIndex, BaselineEndpointSet(), deadline);
            },
            deadline))
            << FailureDiagnostics("baseline unique Leader and business gate convergence", indexes);
    }

    void StartTargetQuorumCluster(Deadline deadline, LeaderObservation &observation)
    {
        discovery_ = std::make_shared<CoordinatorDiscoveryMock>();
        discovery_->SetRegistrationBarrierTarget(1);
        const std::vector<size_t> quorumIndexes{ 0, 1 };
        ASSERT_EQ(LaunchRuntime(quorumIndexes[0]), 1U) << "Failed to launch " << RuntimeLabel(quorumIndexes[0], 1);
        ASSERT_TRUE(WaitUntil(
            [this, &quorumIndexes, deadline] {
                return discovery_->RegisteredCount() == 1 && AllLifecyclesRunning({ quorumIndexes[0] })
                       && HasWaitingBootstrapState(quorumIndexes[0], 1, deadline, coordinator::RAFT_METADATA_ABSENT,
                                                   coordinator::RAFT_BOOTSTRAP_RETRYING,
                                                   static_cast<int32_t>(K_NOT_READY));
            },
            deadline))
            << FailureDiagnostics("single-candidate bootstrap wait", { quorumIndexes[0] });

        ASSERT_EQ(LaunchRuntime(quorumIndexes[1]), 1U) << "Failed to launch " << RuntimeLabel(quorumIndexes[1], 1);
        const auto quorumEndpoints = EndpointSet(quorumIndexes);
        ASSERT_TRUE(WaitUntil(
            [this, &quorumIndexes, &quorumEndpoints, &observation, deadline] {
                return discovery_->RegisteredCount() == quorumIndexes.size() && AllLifecyclesRunning(quorumIndexes)
                       && ObserveUniqueServingLeader(quorumIndexes, observation, deadline)
                       && std::all_of(quorumIndexes.begin(), quorumIndexes.end(), [this, &quorumEndpoints, deadline](size_t index) {
                              return HasCommittedConfiguration(index, quorumEndpoints, deadline);
                          });
            },
            deadline))
            << FailureDiagnostics("target-majority bootstrap", quorumIndexes);
    }

    void StartExtraCandidateCluster(Deadline deadline, std::vector<size_t> &selectedIndexes, size_t &waitingIndex,
                                    LeaderObservation &observation)
    {
        discovery_ = std::make_shared<CoordinatorDiscoveryMock>();
        discovery_->SetRegistrationBarrierTarget(kEndpointCapacity);
        const std::vector<size_t> allIndexes{ 0, 1, 2, 3 };
        for (const auto index : allIndexes) {
            ASSERT_EQ(LaunchRuntime(index), 1U) << "Failed to launch " << RuntimeLabel(index, 1);
        }
        ASSERT_TRUE(WaitUntil(
            [this, &allIndexes] {
                return discovery_->RegisteredCount() == kEndpointCapacity
                       && !discovery_->RegistrationBarrierTimedOut() && AllLifecyclesRunning(allIndexes);
            },
            deadline))
            << FailureDiagnostics("extra-candidate registration", allIndexes);

        const auto sortedIndexes = SortedIndexes(allIndexes);
        selectedIndexes.assign(sortedIndexes.begin(), sortedIndexes.begin() + kBaselineCoordinatorCount);
        waitingIndex = sortedIndexes.back();
        const auto selectedEndpoints = EndpointSet(selectedIndexes);
        ASSERT_TRUE(WaitUntil(
            [this, &allIndexes, &selectedIndexes, waitingIndex, &selectedEndpoints, &observation, deadline] {
                return ObserveUniqueLeader(selectedIndexes, observation)
                       && std::all_of(selectedIndexes.begin(), selectedIndexes.end(),
                                      [this, &selectedEndpoints, deadline](size_t index) {
                                          return HasCommittedConfiguration(index, selectedEndpoints, deadline);
                                      })
                       && HasWaitingBootstrapState(waitingIndex, kEndpointCapacity, deadline,
                                                   coordinator::RAFT_METADATA_VALID,
                                                   coordinator::RAFT_BOOTSTRAP_STARTED,
                                                   static_cast<int32_t>(K_OK))
                       && !runtimes_[waitingIndex].active->runtime->IsLeader()
                       && BusinessGatesMatchLeader(allIndexes, observation.leaderIndex, deadline);
            },
            deadline))
            << FailureDiagnostics("deterministic extra-candidate selection", allIndexes);
    }

    std::string RuntimeLabel(size_t index, size_t generation) const
    {
        return "endpoint_index=" + std::to_string(index) + ", endpoint=" + endpoints_[index]
               + ", generation=" + std::to_string(generation);
    }

    std::string FailureDiagnostics(const std::string &phase, const std::vector<size_t> &indexes) const
    {
        std::ostringstream output;
        output << phase << " timed out";
        if (discovery_ != nullptr) {
            const auto candidates = discovery_->RegisteredServicesForDiagnostics();
            output << ", candidates=[";
            for (size_t i = 0; i < candidates.size(); ++i) {
                output << (i == 0 ? "" : ",") << candidates[i];
            }
            output << "], discovery_status={" << discovery_->GetStatusForDiagnostics().ToString() << "}"
                   << ", discovery_queries=" << discovery_->DiscoveryQueryCount()
                   << ", barrier_timed_out=" << (discovery_->RegistrationBarrierTimedOut() ? "true" : "false");
        }
        for (const auto index : indexes) {
            output << "\nindex=" << index << ", endpoint=" << endpoints_[index]
                   << ", data_present=" << (HasPersistedData(index) ? "true" : "false")
                   << ", completed_generations=" << runtimes_[index].completed.size();
            const auto &active = runtimes_[index].active;
            if (active == nullptr) {
                output << ", active_generation=<none>";
            } else {
                output << ", active_generation=" << active->generation
                       << ", lifecycle_completed=" << (LifecycleCompleted(index) ? "true" : "false");
                if (!LifecycleCompleted(index)) {
                    std::string leader;
                    const auto leaderStatus = active->runtime->GetLeader(leader);
                    output << ", is_leader=" << (active->runtime->IsLeader() ? "true" : "false")
                           << ", leader_status={" << leaderStatus.ToString() << "}, leader=" << leader;
                    const auto bootstrap = CallBootstrapRpc(
                        index, std::chrono::steady_clock::now() + std::chrono::milliseconds(kRpcTimeoutMs));
                    output << ", bootstrap_rpc={" << bootstrap.status.ToString() << "}"
                           << ", bootstrap_probe_ready=" << (bootstrap.response.probe_ready() ? "true" : "false")
                           << ", bootstrap_metadata=" << bootstrap.response.metadata_state()
                           << ", bootstrap_phase=" << bootstrap.response.phase()
                           << ", bootstrap_status_code=" << bootstrap.response.status_code()
                           << ", bootstrap_candidate_count=" << bootstrap.response.candidate_count()
                           << ", bootstrap_digest=" << bootstrap.response.candidate_digest()
                           << ", committed=[";
                    for (int peerIndex = 0; peerIndex < bootstrap.response.committed_peers_size(); ++peerIndex) {
                        output << (peerIndex == 0 ? "" : ",") << bootstrap.response.committed_peers(peerIndex);
                    }
                    output << "]";
                } else {
                    try {
                        output << ", lifecycle_result={" << active->result.get().ToString() << "}";
                    } catch (...) {
                        output << ", lifecycle_result=<threw>";
                    }
                }
            }
            output << ", last_business_status={" << lastBusinessStatus_[index]
                   << "}, last_bootstrap_status={" << lastBootstrapStatus_[index] << "}";
        }
        return output.str();
    }

    butil::AtExitManager atExitManager_;
    std::vector<TestPortLease> portLeases_;
    std::array<std::string, kEndpointCapacity> endpoints_;
    std::array<std::string, kEndpointCapacity> dataRoots_;
    std::array<RuntimeSlot, kEndpointCapacity> runtimes_;
    std::shared_ptr<CoordinatorDiscoveryMock> discovery_;
    std::string rootDir_;
    mutable std::array<std::string, kEndpointCapacity> lastBusinessStatus_;
    mutable std::array<std::string, kEndpointCapacity> lastBootstrapStatus_;

private:
    bool savedUseBrpc_{ false };
    sig_atomic_t savedExitFlag_{ 0 };
};

TEST_F(CoordinatorRuntimeElectionTest, OneOfThreeCandidateWaitsWithoutSynchronousStartupFailure)
{
    const auto caseDeadline = std::chrono::steady_clock::now() + kCaseBudget;
    discovery_ = std::make_shared<CoordinatorDiscoveryMock>();
    discovery_->SetRegistrationBarrierTarget(1);
    ASSERT_EQ(LaunchRuntime(0), 1U) << "Failed to launch " << RuntimeLabel(0, 1);
    ASSERT_TRUE(WaitUntil(
        [this, caseDeadline] {
            return discovery_->RegisteredCount() == 1 && AllLifecyclesRunning({ 0 })
                   && HasWaitingBootstrapState(0, 1, caseDeadline, coordinator::RAFT_METADATA_ABSENT,
                                               coordinator::RAFT_BOOTSTRAP_RETRYING,
                                               static_cast<int32_t>(K_NOT_READY));
        },
        caseDeadline))
        << FailureDiagnostics("one-of-three candidate registration", { 0 });

    ASSERT_TRUE(RemainNonServingWithoutConfiguration({ 0 }, kBootstrapIsolationWindow, caseDeadline, 1))
        << FailureDiagnostics("one-of-three candidate isolation", { 0 });
    EXPECT_FALSE(LifecycleCompleted(0));
    EXPECT_FALSE(runtimes_[0].active->runtime->IsLeader());
    EXPECT_LT(std::chrono::steady_clock::now(), caseDeadline);
}

TEST_F(CoordinatorRuntimeElectionTest, TwoOfThreeCandidatesBootstrapAtTargetQuorum)
{
    const auto caseDeadline = std::chrono::steady_clock::now() + kCaseBudget;
    LeaderObservation observation;
    ASSERT_NO_FATAL_FAILURE(StartTargetQuorumCluster(caseDeadline, observation));

    const std::vector<size_t> quorumIndexes{ 0, 1 };
    const auto quorumEndpoints = EndpointSet(quorumIndexes);
    EXPECT_NE(std::find(quorumEndpoints.begin(), quorumEndpoints.end(), observation.endpoint), quorumEndpoints.end());
    for (const auto index : quorumIndexes) {
        EXPECT_TRUE(HasCommittedConfiguration(index, quorumEndpoints, caseDeadline))
            << FailureDiagnostics("two-of-three committed configuration", quorumIndexes);
    }
    EXPECT_TRUE(BusinessGatesMatchLeader(quorumIndexes, observation.leaderIndex, caseDeadline));
    EXPECT_LT(std::chrono::steady_clock::now(), caseDeadline);
}

TEST_F(CoordinatorRuntimeElectionTest, LaterCandidateFillsBootstrapVacancyWithoutRemoval)
{
    const auto caseDeadline = std::chrono::steady_clock::now() + kCaseBudget;
    LeaderObservation initial;
    ASSERT_NO_FATAL_FAILURE(StartTargetQuorumCluster(caseDeadline, initial));
    const std::vector<size_t> bootstrapIndexes{ 0, 1 };
    const auto bootstrapEndpoints = EndpointSet(bootstrapIndexes);

    ASSERT_EQ(LaunchRuntime(2), 1U) << "Failed to launch " << RuntimeLabel(2, 1);
    const auto allIndexes = BaselineIndexes();
    const auto finalEndpoints = BaselineEndpointSet();
    bool bootstrapMembersPreserved = true;
    std::vector<std::vector<std::string>> observedConfigurations{ bootstrapEndpoints };
    LeaderObservation final;
    ASSERT_TRUE(WaitUntil(
        [this, &initial, &allIndexes, &bootstrapEndpoints, &finalEndpoints, &bootstrapMembersPreserved,
         &observedConfigurations, &final, caseDeadline] {
            const auto bootstrap = CallBootstrapRpc(initial.leaderIndex, caseDeadline);
            if (bootstrap.status.IsOk() && bootstrap.response.committed_peers_size() > 0) {
                std::vector<std::string> peers(bootstrap.response.committed_peers().begin(),
                                               bootstrap.response.committed_peers().end());
                if (observedConfigurations.empty() || observedConfigurations.back() != peers) {
                    observedConfigurations.emplace_back(peers);
                }
                bootstrapMembersPreserved = bootstrapMembersPreserved
                                            && std::all_of(bootstrapEndpoints.begin(), bootstrapEndpoints.end(),
                                                           [&peers](const std::string &peer) {
                                                               return std::find(peers.begin(), peers.end(), peer)
                                                                      != peers.end();
                                                           });
            }
            return bootstrapMembersPreserved && ObserveUniqueServingLeader(allIndexes, final, caseDeadline)
                   && std::all_of(allIndexes.begin(), allIndexes.end(),
                                  [this, &finalEndpoints, caseDeadline](size_t index) {
                                      return HasCommittedConfiguration(index, finalEndpoints, caseDeadline);
                                  });
        },
        caseDeadline))
        << FailureDiagnostics("vacancy Add-only convergence", allIndexes);

    ASSERT_TRUE(bootstrapMembersPreserved);
    ASSERT_FALSE(observedConfigurations.empty());
    EXPECT_EQ(observedConfigurations.front(), bootstrapEndpoints);
    EXPECT_EQ(observedConfigurations.back(), finalEndpoints);
    EXPECT_TRUE(BusinessGatesMatchLeader(allIndexes, final.leaderIndex, caseDeadline));
    EXPECT_LT(std::chrono::steady_clock::now(), caseDeadline);
}

TEST_F(CoordinatorRuntimeElectionTest, ExtraCandidateWaitsOutsideFirstExpectedPeers)
{
    const auto caseDeadline = std::chrono::steady_clock::now() + kCaseBudget;
    std::vector<size_t> selectedIndexes;
    size_t waitingIndex = kInvalidRuntimeIndex;
    LeaderObservation observation;
    ASSERT_NO_FATAL_FAILURE(StartExtraCandidateCluster(caseDeadline, selectedIndexes, waitingIndex, observation));

    const auto sortedIndexes = SortedIndexes({ 0, 1, 2, 3 });
    ASSERT_EQ(selectedIndexes,
              (std::vector<size_t>{ sortedIndexes[0], sortedIndexes[1], sortedIndexes[2] }));
    ASSERT_EQ(waitingIndex, sortedIndexes[3]);
    const auto selectedEndpoints = EndpointSet(selectedIndexes);
    for (const auto index : selectedIndexes) {
        EXPECT_TRUE(HasCommittedConfiguration(index, selectedEndpoints, caseDeadline));
    }
    EXPECT_EQ(std::count_if(sortedIndexes.begin(), sortedIndexes.end(), [this](size_t index) {
                  return runtimes_[index].active->runtime->IsLeader();
              }),
              1);
    EXPECT_TRUE(BusinessGatesMatchLeader(sortedIndexes, observation.leaderIndex, caseDeadline));
    EXPECT_TRUE(HasWaitingBootstrapState(waitingIndex, kEndpointCapacity, caseDeadline,
                                         coordinator::RAFT_METADATA_VALID, coordinator::RAFT_BOOTSTRAP_STARTED,
                                         static_cast<int32_t>(K_OK)));
    EXPECT_FALSE(runtimes_[waitingIndex].active->runtime->IsLeader());
    std::string waitingLeader = "stale";
    EXPECT_EQ(runtimes_[waitingIndex].active->runtime->GetLeader(waitingLeader).GetCode(), K_NOT_READY);
    EXPECT_TRUE(waitingLeader.empty());
    EXPECT_EQ(CallBusinessRpc(waitingIndex, caseDeadline).status.GetCode(), K_NOT_READY);
    EXPECT_LT(std::chrono::steady_clock::now(), caseDeadline);
}

TEST_F(CoordinatorRuntimeElectionTest, WaitingCandidateReplacesFailedFollowerAddBeforeRemove)
{
    const auto caseDeadline = std::chrono::steady_clock::now() + kCaseBudget;
    std::vector<size_t> selectedIndexes;
    size_t waitingIndex = kInvalidRuntimeIndex;
    LeaderObservation initial;
    ASSERT_NO_FATAL_FAILURE(StartExtraCandidateCluster(caseDeadline, selectedIndexes, waitingIndex, initial));
    const auto followers = FollowersOf(initial, selectedIndexes);
    ASSERT_EQ(followers.size(), kBaselineCoordinatorCount - 1);
    const size_t failedFollower = followers.front();

    std::string stopError;
    ASSERT_TRUE(StopAndJoin(failedFollower, caseDeadline, stopError)) << stopError;
    std::vector<size_t> finalIndexes;
    for (const auto index : selectedIndexes) {
        if (index != failedFollower) {
            finalIndexes.emplace_back(index);
        }
    }
    finalIndexes.emplace_back(waitingIndex);
    const auto finalEndpoints = EndpointSet(finalIndexes);
    bool removedBeforeAdd = false;
    LeaderObservation final;
    ASSERT_TRUE(WaitUntil(
        [this, &initial, failedFollower, waitingIndex, &finalIndexes, &finalEndpoints, &removedBeforeAdd, &final,
         caseDeadline] {
            const auto bootstrap = CallBootstrapRpc(initial.leaderIndex, caseDeadline);
            if (bootstrap.status.IsOk() && bootstrap.response.committed_peers_size() > 0) {
                std::vector<std::string> peers(bootstrap.response.committed_peers().begin(),
                                               bootstrap.response.committed_peers().end());
                const bool containsWaiting = std::find(peers.begin(), peers.end(), endpoints_[waitingIndex]) != peers.end();
                const bool containsFailed =
                    std::find(peers.begin(), peers.end(), endpoints_[failedFollower]) != peers.end();
                removedBeforeAdd = removedBeforeAdd || (!containsFailed && !containsWaiting);
            }
            // The deterministic Add-before-Remove transition is covered by CoordinatorMembershipManager UTs. This
            // Runtime ST verifies that polling never observes removal before admission and requires the final state.
            return !removedBeforeAdd && ObserveUniqueServingLeader(finalIndexes, final, caseDeadline)
                   && std::all_of(finalIndexes.begin(), finalIndexes.end(),
                                  [this, &finalEndpoints, caseDeadline](size_t index) {
                                      return HasCommittedConfiguration(index, finalEndpoints, caseDeadline);
                                  });
        },
        caseDeadline))
        << FailureDiagnostics("waiting-candidate replacement", { 0, 1, 2, 3 });

    EXPECT_FALSE(removedBeforeAdd);
    EXPECT_NE(std::find(finalEndpoints.begin(), finalEndpoints.end(), endpoints_[waitingIndex]), finalEndpoints.end());
    EXPECT_EQ(std::find(finalEndpoints.begin(), finalEndpoints.end(), endpoints_[failedFollower]), finalEndpoints.end());
    EXPECT_TRUE(BusinessGatesMatchLeader(finalIndexes, final.leaderIndex, caseDeadline));
    EXPECT_LT(std::chrono::steady_clock::now(), caseDeadline);
}

TEST_F(CoordinatorRuntimeElectionTest, InconsistentBootstrapDigestsNeverCreateConfiguration)
{
    const auto caseDeadline = std::chrono::steady_clock::now() + kCaseBudget;
    discovery_ = std::make_shared<CoordinatorDiscoveryMock>();
    discovery_->SetRegistrationBarrierTarget(kBaselineCoordinatorCount);
    const auto indexes = BaselineIndexes();
    const std::array<std::vector<std::string>, kBaselineCoordinatorCount> candidateViews{
        std::vector<std::string>{ endpoints_[0], endpoints_[1] },
        std::vector<std::string>{ endpoints_[1], endpoints_[2] },
        std::vector<std::string>{ endpoints_[0], endpoints_[2] }
    };
    for (const auto index : indexes) {
        auto injectedProvider = std::make_shared<CoordinatorDiscoveryMock>();
        injectedProvider->ShareRegistrationStateFrom(*discovery_);
        injectedProvider->SetCandidates(candidateViews[index]);
        ASSERT_EQ(LaunchRuntime(index, kBaselineCoordinatorCount, std::move(injectedProvider)), 1U)
            << "Failed to launch " << RuntimeLabel(index, 1);
    }
    ASSERT_TRUE(WaitUntil(
        [this, &indexes] {
            return discovery_->RegisteredCount() == kBaselineCoordinatorCount
                   && !discovery_->RegistrationBarrierTimedOut() && AllLifecyclesRunning(indexes);
        },
        caseDeadline))
        << FailureDiagnostics("inconsistent-digest registration", indexes);

    std::set<std::string> digests;
    ASSERT_TRUE(WaitUntil(
        [this, &indexes, &digests, caseDeadline] {
            digests.clear();
            for (const auto index : indexes) {
                const auto bootstrap = CallBootstrapRpc(index, caseDeadline);
                if (bootstrap.status.IsError() || !bootstrap.response.probe_ready()
                    || bootstrap.response.metadata_state() != coordinator::RAFT_METADATA_ABSENT
                    || bootstrap.response.candidate_count() != 2 || bootstrap.response.candidate_digest().empty()
                    || bootstrap.response.committed_peers_size() != 0) {
                    return false;
                }
                digests.emplace(bootstrap.response.candidate_digest());
            }
            return digests.size() == indexes.size();
        },
        caseDeadline))
        << FailureDiagnostics("inconsistent bootstrap digest publication", indexes);
    ASSERT_TRUE(RemainNonServingWithoutConfiguration(indexes, kBootstrapIsolationWindow, caseDeadline, 2))
        << FailureDiagnostics("inconsistent bootstrap digest isolation", indexes);
    EXPECT_EQ(digests.size(), indexes.size());
    EXPECT_LT(std::chrono::steady_clock::now(), caseDeadline);
}

TEST_F(CoordinatorRuntimeElectionTest, RunningClusterSurvivesDiscoveryFailureWithoutMembershipChange)
{
    const auto caseDeadline = std::chrono::steady_clock::now() + kCaseBudget;
    LeaderObservation initial;
    ASSERT_NO_FATAL_FAILURE(StartBaselineCluster(caseDeadline, initial));
    const auto followers = FollowersOf(initial);
    ASSERT_EQ(followers.size(), kBaselineCoordinatorCount - 1);
    const size_t stoppedFollower = followers.front();
    const std::vector<size_t> survivors{ initial.leaderIndex, followers.back() };

    discovery_->SetGetStatus(Status(K_RUNTIME_ERROR, "injected running-cluster Discovery failure"));
    const size_t queryCountBeforeFailure = discovery_->DiscoveryQueryCount();
    std::string stopError;
    const bool stopped = StopAndJoin(stoppedFollower, caseDeadline, stopError);
    const bool discoveryFailureObserved = stopped && WaitUntil(
        [this, &initial, &survivors, queryCountBeforeFailure, caseDeadline] {
            LeaderObservation observation;
            return discovery_->DiscoveryQueryCount() > queryCountBeforeFailure
                   && ObserveUniqueServingLeader(survivors, observation, caseDeadline, initial.endpoint)
                   && HasCommittedConfiguration(initial.leaderIndex, BaselineEndpointSet(), caseDeadline);
        },
        caseDeadline);

    bool membershipRemainedStable = discoveryFailureObserved;
    const auto isolationDeadline = std::min(caseDeadline,
                                            std::chrono::steady_clock::now() + kDiscoveryFailureIsolationWindow);
    while (membershipRemainedStable && std::chrono::steady_clock::now() < isolationDeadline) {
        LeaderObservation observation;
        membershipRemainedStable = ObserveUniqueServingLeader(survivors, observation, caseDeadline, initial.endpoint)
                                   && HasCommittedConfiguration(initial.leaderIndex, BaselineEndpointSet(), caseDeadline);
        const auto nextPoll = std::min(isolationDeadline, std::chrono::steady_clock::now() + kPollInterval);
        std::this_thread::sleep_until(nextPoll);
    }
    discovery_->SetGetStatus(Status::OK());

    ASSERT_TRUE(stopped) << stopError;
    ASSERT_TRUE(discoveryFailureObserved)
        << FailureDiagnostics("running-cluster Discovery failure observation", survivors);
    EXPECT_TRUE(membershipRemainedStable)
        << FailureDiagnostics("running-cluster membership preservation", survivors);
    EXPECT_GT(discovery_->DiscoveryQueryCount(), queryCountBeforeFailure);
    EXPECT_TRUE(BusinessGatesMatchLeader(survivors, initial.leaderIndex, caseDeadline));
    EXPECT_LT(std::chrono::steady_clock::now(), caseDeadline);
}

TEST_F(CoordinatorRuntimeElectionTest, ThreeInProcessRuntimesElectOneServingLeaderAndStopIndependently)
{
    const auto caseDeadline = std::chrono::steady_clock::now() + kCaseBudget;
    LeaderObservation observation;
    ASSERT_NO_FATAL_FAILURE(StartBaselineCluster(caseDeadline, observation));
    ASSERT_EQ(endpoints_[observation.leaderIndex], observation.endpoint);

    const auto followerIndexes = FollowersOf(observation);
    ASSERT_EQ(followerIndexes.size(), kBaselineCoordinatorCount - 1);

    std::string stopError;
    ASSERT_TRUE(StopAndJoin(followerIndexes[0], caseDeadline, stopError)) << stopError;
    EXPECT_EQ(g_exitFlag, 0);
    const std::array<size_t, 2> stillRunning{ followerIndexes[1], observation.leaderIndex };
    ASSERT_TRUE(LifecyclesRemainPendingThroughIsolationWindow(stillRunning, caseDeadline));
    ASSERT_TRUE(BusinessGatesMatchLeader({ stillRunning[0], stillRunning[1] }, observation.leaderIndex, caseDeadline))
        << FailureDiagnostics("post-follower-stop business gate isolation", { stillRunning[0], stillRunning[1] });

    std::vector<std::string> registeredServices;
    const auto registeredStatus = discovery_->GetCoordinators(registeredServices);
    ASSERT_TRUE(registeredStatus.IsOk()) << registeredStatus.ToString();
    EXPECT_EQ(registeredServices.size(), kBaselineCoordinatorCount);
    EXPECT_NE(std::find(registeredServices.begin(), registeredServices.end(), endpoints_[followerIndexes[0]]),
              registeredServices.end());

    ASSERT_TRUE(StopAndJoin(followerIndexes[1], caseDeadline, stopError)) << stopError;
    ASSERT_TRUE(StopAndJoin(observation.leaderIndex, caseDeadline, stopError)) << stopError;
    for (size_t index = 0; index < kBaselineCoordinatorCount; ++index) {
        ASSERT_EQ(runtimes_[index].active, nullptr);
        ASSERT_EQ(runtimes_[index].completed.size(), 1U);
        ASSERT_EQ(runtimes_[index].completed.front().result.GetCode(), K_OK)
            << runtimes_[index].completed.front().result.ToString();
        EXPECT_EQ(discovery_->RegisterCount(endpoints_[index], 1), 1U);
        EXPECT_EQ(discovery_->UnregisterCount(endpoints_[index], 1), 1U);
        EXPECT_EQ(discovery_->CallbackEvents(endpoints_[index]),
                  (std::vector<std::string>{ "g1:register", "g1:unregister" }));
    }
    EXPECT_EQ(g_exitFlag, 0);

    discovery_->AdvanceTimeForTest(kExpiredUnregisterAdvance);
    registeredServices.clear();
    const auto expiredStatus = discovery_->GetCoordinators(registeredServices);
    ASSERT_TRUE(expiredStatus.IsOk()) << expiredStatus.ToString();
    EXPECT_TRUE(registeredServices.empty());
    EXPECT_LT(std::chrono::steady_clock::now(), caseDeadline);
}

TEST_F(CoordinatorRuntimeElectionTest, LeaderFailoverRestartsOldLeaderAsPersistedFollower)
{
    const auto caseDeadline = std::chrono::steady_clock::now() + kCaseBudget;
    const auto allIndexes = BaselineIndexes();
    LeaderObservation initial;
    ASSERT_NO_FATAL_FAILURE(StartBaselineCluster(caseDeadline, initial));

    const auto survivorIndexes = FollowersOf(initial);
    std::string stopError;
    ASSERT_TRUE(StopAndJoin(initial.leaderIndex, caseDeadline, stopError)) << stopError;
    ASSERT_TRUE(HasPersistedData(initial.leaderIndex))
        << "Old Leader data directory was lost before restart: " << dataRoots_[initial.leaderIndex];

    LeaderObservation failover;
    ASSERT_TRUE(WaitUntil(
        [this, &survivorIndexes, &failover, &initial, caseDeadline] {
            return ObserveUniqueServingLeader(survivorIndexes, failover, caseDeadline, {}, initial.endpoint);
        },
        caseDeadline))
        << FailureDiagnostics("Leader failover", survivorIndexes);
    ASSERT_NE(failover.endpoint, initial.endpoint);

    auto restartProvider = std::make_shared<CoordinatorDiscoveryMock>();
    restartProvider->ShareRegistrationStateFrom(*discovery_);
    ASSERT_EQ(LaunchRuntime(initial.leaderIndex, kBaselineCoordinatorCount, restartProvider), 2U)
        << "Failed to restart " << RuntimeLabel(initial.leaderIndex, 2);
    LeaderObservation recovered;
    ASSERT_TRUE(WaitUntil(
        [this, &allIndexes, &recovered, &failover, &initial, caseDeadline] {
            return ObserveUniqueServingLeader(allIndexes, recovered, caseDeadline, failover.endpoint)
                   && HasCommittedConfiguration(initial.leaderIndex, BaselineEndpointSet(), caseDeadline);
        },
        caseDeadline))
        << FailureDiagnostics("old Leader restart convergence", allIndexes);
    EXPECT_EQ(recovered.endpoint, failover.endpoint);
    EXPECT_EQ(restartProvider->DiscoveryQueryCount(), 0U)
        << "Persisted old-Leader generation queried bootstrap Discovery before recovering locally";
    EXPECT_EQ(discovery_->RegisterCount(initial.endpoint, 1), 1U);
    EXPECT_EQ(discovery_->UnregisterCount(initial.endpoint, 1), 1U);
    EXPECT_EQ(discovery_->RegisterCount(initial.endpoint, 2), 1U);
    EXPECT_EQ(discovery_->UnregisterCount(initial.endpoint, 2), 0U);
    EXPECT_EQ(discovery_->CallbackEvents(initial.endpoint),
              (std::vector<std::string>{ "g1:register", "g1:unregister", "g2:register" }));
    ASSERT_NE(runtimes_[initial.leaderIndex].active, nullptr);
    EXPECT_FALSE(runtimes_[initial.leaderIndex].active->runtime->IsLeader());
    const auto restartedGate = CallBusinessRpc(initial.leaderIndex, caseDeadline);
    EXPECT_TRUE(restartedGate.status.IsOk()) << restartedGate.status.ToString();
    EXPECT_FALSE(IsBusinessServing(restartedGate));
    EXPECT_FALSE(restartedGate.header.is_leader());
    EXPECT_EQ(restartedGate.header.serving_state(), coordinator::ResponseHeader::FOLLOWER_SERVING);
    EXPECT_EQ(restartedGate.header.leader_address(), failover.endpoint);
    EXPECT_TRUE(HasCommittedConfiguration(initial.leaderIndex, BaselineEndpointSet(), caseDeadline));
    EXPECT_LT(std::chrono::steady_clock::now(), caseDeadline);
}

TEST_F(CoordinatorRuntimeElectionTest, PersistedFollowerRestartMaintainsUniqueServingLeader)
{
    const auto caseDeadline = std::chrono::steady_clock::now() + kCaseBudget;
    const auto allIndexes = BaselineIndexes();
    LeaderObservation initial;
    ASSERT_NO_FATAL_FAILURE(StartBaselineCluster(caseDeadline, initial));
    const auto followers = FollowersOf(initial);
    ASSERT_EQ(followers.size(), kBaselineCoordinatorCount - 1);
    const size_t stoppedFollower = followers.front();

    std::string stopError;
    ASSERT_TRUE(StopAndJoin(stoppedFollower, caseDeadline, stopError)) << stopError;
    ASSERT_TRUE(HasPersistedData(stoppedFollower))
        << "Follower data directory was lost before restart: " << dataRoots_[stoppedFollower];
    const std::vector<size_t> survivors{ initial.leaderIndex, followers.back() };
    LeaderObservation afterFailure;
    ASSERT_TRUE(WaitUntil(
        [this, &survivors, &afterFailure, &initial, caseDeadline] {
            return ObserveUniqueServingLeader(survivors, afterFailure, caseDeadline, initial.endpoint);
        },
        caseDeadline))
        << FailureDiagnostics("Follower failure isolation", survivors);

    auto restartProvider = std::make_shared<CoordinatorDiscoveryMock>();
    restartProvider->ShareRegistrationStateFrom(*discovery_);
    ASSERT_EQ(LaunchRuntime(stoppedFollower, kBaselineCoordinatorCount, restartProvider), 2U)
        << "Failed to restart " << RuntimeLabel(stoppedFollower, 2);
    LeaderObservation recovered;
    const auto baselineEndpoints = BaselineEndpointSet();
    ASSERT_TRUE(WaitUntil(
        [this, &allIndexes, &recovered, &baselineEndpoints, caseDeadline] {
            return ObserveUniqueServingLeader(allIndexes, recovered, caseDeadline)
                   && std::all_of(allIndexes.begin(), allIndexes.end(),
                                  [this, &baselineEndpoints, caseDeadline](size_t index) {
                                      return HasCommittedConfiguration(index, baselineEndpoints, caseDeadline);
                                  });
        },
        caseDeadline))
        << FailureDiagnostics("Follower restart convergence", allIndexes);
    EXPECT_EQ(restartProvider->DiscoveryQueryCount(), 0U)
        << "Persisted follower generation queried bootstrap Discovery before recovering locally";
    EXPECT_EQ(discovery_->RegisterCount(endpoints_[stoppedFollower], 1), 1U);
    EXPECT_EQ(discovery_->UnregisterCount(endpoints_[stoppedFollower], 1), 1U);
    EXPECT_EQ(discovery_->RegisterCount(endpoints_[stoppedFollower], 2), 1U);
    EXPECT_EQ(discovery_->UnregisterCount(endpoints_[stoppedFollower], 2), 0U);
    EXPECT_EQ(discovery_->CallbackEvents(endpoints_[stoppedFollower]),
              (std::vector<std::string>{ "g1:register", "g1:unregister", "g2:register" }));
    ASSERT_NE(runtimes_[stoppedFollower].active, nullptr);
    EXPECT_TRUE(BusinessGatesMatchLeader(allIndexes, recovered.leaderIndex, caseDeadline));
    for (const auto index : allIndexes) {
        EXPECT_TRUE(HasCommittedConfiguration(index, baselineEndpoints, caseDeadline));
    }
    EXPECT_LT(std::chrono::steady_clock::now(), caseDeadline);
}

TEST_F(CoordinatorRuntimeElectionTest, QuorumLossClosesServingGateUntilOriginalMemberRestarts)
{
    const auto caseDeadline = std::chrono::steady_clock::now() + kCaseBudget;
    LeaderObservation initial;
    ASSERT_NO_FATAL_FAILURE(StartBaselineCluster(caseDeadline, initial));
    const auto followers = FollowersOf(initial);
    ASSERT_EQ(followers.size(), kBaselineCoordinatorCount - 1);

    std::string stopError;
    ASSERT_TRUE(StopAndJoin(followers[0], caseDeadline, stopError)) << stopError;
    ASSERT_TRUE(StopAndJoin(followers[1], caseDeadline, stopError)) << stopError;
    const auto quorumLossStart = std::chrono::steady_clock::now();
    const auto leaderElectionTimeoutMs = MakeRaftFlags(initial.leaderIndex).electionTimeoutMs;
    const auto quorumLossDeadline =
        quorumLossStart
        + std::chrono::milliseconds{ static_cast<int64_t>(kQuorumLossElectionTimeoutMultiplier)
                                     * leaderElectionTimeoutMs };
    ASSERT_LT(quorumLossDeadline, caseDeadline)
        << "Insufficient case budget remains for the configured two-election-timeout quorum-loss bound";
    ASSERT_TRUE(HasPersistedData(followers[0]));
    ASSERT_TRUE(HasPersistedData(followers[1]));

    const auto quorumLossObservation = WaitUntilNoLaterThan(
        [this, &initial, quorumLossDeadline] {
            if (runtimes_[initial.leaderIndex].active == nullptr
                || runtimes_[initial.leaderIndex].active->runtime->IsLeader()) {
                return false;
            }
            std::string reportedLeader;
            const auto leaderStatus =
                runtimes_[initial.leaderIndex].active->runtime->GetLeader(reportedLeader);
            if (leaderStatus.GetCode() != K_NOT_READY || !reportedLeader.empty()) {
                return false;
            }
            return CallBusinessRpc(initial.leaderIndex, quorumLossDeadline).status.GetCode() == K_NOT_READY;
        },
        quorumLossDeadline);
    ASSERT_TRUE(quorumLossObservation.completionTime <= quorumLossDeadline)
        << "Quorum-loss observation completed after its strict deadline by "
        << std::chrono::duration_cast<std::chrono::microseconds>(quorumLossObservation.completionTime
                                                                - quorumLossDeadline)
               .count()
        << " us\n"
        << FailureDiagnostics("quorum loss serving gate closure", { initial.leaderIndex });
    ASSERT_TRUE(quorumLossObservation.satisfiedByDeadline)
        << FailureDiagnostics("quorum loss serving gate closure", { initial.leaderIndex });

    const size_t recoveringMember = followers.front();
    ASSERT_EQ(LaunchRuntime(recoveringMember), 2U)
        << "Failed to restart original member " << RuntimeLabel(recoveringMember, 2);
    const std::vector<size_t> recoveredMajority{ initial.leaderIndex, recoveringMember };
    LeaderObservation recovered;
    ASSERT_TRUE(WaitUntil(
        [this, &recoveredMajority, &recovered, caseDeadline] {
            return ObserveUniqueServingLeader(recoveredMajority, recovered, caseDeadline)
                   && HasCommittedConfiguration(recovered.leaderIndex, BaselineEndpointSet(), caseDeadline);
        },
        caseDeadline))
        << FailureDiagnostics("original-member quorum recovery", recoveredMajority);
    EXPECT_EQ(discovery_->RegisteredCount(), kBaselineCoordinatorCount);
    EXPECT_EQ(runtimes_[followers.back()].active, nullptr);
    EXPECT_LT(std::chrono::steady_clock::now(), caseDeadline);
}

}  // namespace datasystem::st
