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
 * Description: Verify a centralized object-cache master can coordinate an
 * eviction-policy hot update across multiple workers without interrupting reads.
 */

#include <algorithm>
#include <chrono>
#include <csignal>
#include <cstddef>
#include <cstdint>
#include <future>
#include <memory>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

#include "client/object_cache/oc_client_common.h"
#include "common.h"
#include "datasystem/common/ak_sk/signature.h"
#include "datasystem/common/rpc/brpc_factory.h"
#include "datasystem/common/rpc/rpc_options.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/hash_algorithm.h"
#include "datasystem/kv_client.h"
#include "datasystem/protos/master_object.brpc.stub.pb.h"

namespace datasystem {
namespace st {
namespace {
constexpr uint32_t WORKER_COUNT = 3;
constexpr uint32_t OBJECTS_PER_WORKER = 4;
constexpr uint32_t MIGRATION_BATCH_SIZE = 1;
constexpr uint32_t MIN_COHORT_PERCENT = 1;
constexpr uint32_t FULL_COHORT_PERCENT = 100;
constexpr int RPC_TIMEOUT_MS = 5'000;
constexpr int UPDATE_TIMEOUT_MS = 20'000;
constexpr int RESTART_TIMEOUT_S = 30;
constexpr int ASYNC_FILL_TIMEOUT_S = 10;
constexpr int POLL_INTERVAL_MS = 50;
constexpr uint32_t EVICTION_FILL_OBJECT_COUNT = 16;
constexpr size_t EVICTION_FILL_OBJECT_SIZE = 8 * 1024 * 1024;
constexpr uint32_t MASTER_WORKER_INDEX = 0;
constexpr uint32_t FAULT_WORKER_INDEX = 1;
constexpr char AFTER_AUDIT_INJECT[] = "WorkerOcEvictionManager.CommitPolicyUpdate.afterAudit";
constexpr char AFTER_CANCEL_INJECT[] = "WorkerOcEvictionManager.BeginPolicyUpdate.afterCancelRequested";
constexpr char EVICTION_ROUND_INJECT[] = "worker.Evict";
constexpr char REBALANCE_SOURCE_SEND_INJECT[] = "TcpMigrateTransport.MigrateDataToRemote.delay";
constexpr char REBALANCE_SOURCE_DRAIN_INJECT[] = "RebalanceExecutor.PauseAndCheckDrained.afterCancelRequested";
constexpr char REBALANCE_TARGET_ADMISSION_INJECT[] = "WorkerOcServiceMigrateImpl.MigrateData.afterAdmission";
constexpr char REBALANCE_TARGET_DRAIN_INJECT[] =
    "WorkerOcServiceMigrateImpl.PauseIncomingMigrationAdmissionAndCheckDrained.afterPaused";
constexpr char ACCESS_KEY[] = "QTWAOYTTINDUT2QVKYUC";
constexpr char SECRET_KEY[] = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";

struct WorkerObjects {
    std::vector<std::string> keys;
    std::string value;
};
}  // namespace

class LEVEL1_EvictionPolicyHotUpdateTest : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = WORKER_COUNT;
        opts.numEtcd = 1;
        opts.numOBS = 0;
        opts.enableDistributedMaster = "false";
        opts.masterIdx = 0;
        opts.workerGflagParams =
            "-shared_memory_size_mb=64 -log_monitor=true -log_monitor_interval_ms=1000 "
            "-eviction_strategy=clock -enable_memory_rebalance=false -v=1";
        opts.injectActions = "NodeSelector.setInterval:call(200);ResourceManager.setInterval:call(200);"
                             "WorkerOCServer.heatMaintenanceIntervalMs:call(200)";
    }

    void SetUp() override
    {
        ExternalClusterTest::SetUp();
        clients_.reserve(WORKER_COUNT);
        for (uint32_t worker = 0; worker < WORKER_COUNT; ++worker) {
            std::shared_ptr<KVClient> client;
            InitTestKVClient(worker, client);
            clients_.emplace_back(std::move(client));
            HostPort workerAddress;
            DS_ASSERT_OK(cluster_->GetWorkerAddr(worker, workerAddress));
            workerAddresses_.emplace_back(workerAddress.ToString());
            expectedWorkerAddresses_.emplace(workerAddress.ToString());
        }
        DS_ASSERT_OK(InitMasterStub());
    }

    void TearDown() override
    {
        brpcStub_.reset();
        brpcChannel_.reset();
        clients_.clear();
        ExternalClusterTest::TearDown();
    }

protected:
    Status InitMasterStub()
    {
        HostPort masterAddress;
        RETURN_IF_NOT_OK(cluster_->GetMetaServerAddr(masterAddress));
        BrpcChannelConfig config;
        config.endpoint = masterAddress.ToString();
        config.timeout_ms = RPC_TIMEOUT_MS;
        config.connect_timeout_ms = RPC_TIMEOUT_MS;
        brpcChannel_ = BrpcChannelFactory::Create(config);
        CHECK_FAIL_RETURN_STATUS(brpcChannel_ != nullptr, K_RUNTIME_ERROR,
                                 "Failed to create master brpc channel");
        brpcStub_ = std::make_unique<master::MasterOCService_BrpcGenericStub>(brpcChannel_.get(), RPC_TIMEOUT_MS);
        return Status::OK();
    }

    Status StartPolicyUpdate(master::EvictionPolicyPb targetPolicy, uint64_t epoch,
                             master::EvictionPolicyCommandPb command, uint32_t cohortPercent,
                             uint64_t minimumAvailableMemoryBytes = 0, uint64_t maximumSourceObjects = 0,
                             uint64_t deadlineUnixMs = 0)
    {
        master::StartEvictionPolicyUpdateReqPb req;
        auto *update = req.mutable_update();
        update->set_epoch(epoch);
        update->set_target_policy(targetPolicy);
        update->set_migration_batch_size(MIGRATION_BATCH_SIZE);
        update->set_command(command);
        update->set_minimum_available_memory_bytes(minimumAvailableMemoryBytes);
        update->set_maximum_source_objects(maximumSourceObjects);
        update->set_deadline_unix_ms(deadlineUnixMs);
        req.set_cohort_percent(cohortPercent);
        RETURN_IF_NOT_OK(signature_.GenerateSignature(req));

        master::StartEvictionPolicyUpdateRspPb rsp;
        RpcOptions opts;
        opts.SetTimeout(RPC_TIMEOUT_MS);
        RETURN_RUNTIME_ERROR_IF_NULL(brpcStub_);
        return brpcStub_->StartEvictionPolicyUpdate(opts, req, rsp);
    }

    Status ReconnectMasterStub()
    {
        brpcStub_.reset();
        brpcChannel_.reset();
        return InitMasterStub();
    }

    Status GetPolicyUpdateProgress(uint64_t epoch, master::GetEvictionPolicyUpdateProgressRspPb &rsp)
    {
        master::GetEvictionPolicyUpdateProgressReqPb req;
        req.set_epoch(epoch);
        RETURN_IF_NOT_OK(signature_.GenerateSignature(req));

        RpcOptions opts;
        opts.SetTimeout(RPC_TIMEOUT_MS);
        RETURN_RUNTIME_ERROR_IF_NULL(brpcStub_);
        return brpcStub_->GetEvictionPolicyUpdateProgress(opts, req, rsp);
    }

    Status WriteWorkerObjects()
    {
        objects_.clear();
        objects_.reserve(WORKER_COUNT);
        for (uint32_t worker = 0; worker < WORKER_COUNT; ++worker) {
            WorkerObjects objects;
            objects.value = "worker-" + std::to_string(worker) + "-hot-update-value";
            objects.keys.reserve(OBJECTS_PER_WORKER);
            for (uint32_t object = 0; object < OBJECTS_PER_WORKER; ++object) {
                auto key = "policy-hot-update-w" + std::to_string(worker) + "-k" + std::to_string(object);
                RETURN_IF_NOT_OK(clients_[worker]->Set(key, objects.value));
                objects.keys.emplace_back(std::move(key));
            }
            objects_.emplace_back(std::move(objects));
        }
        return Status::OK();
    }

    Status CheckOneObjectReadable(size_t sequence)
    {
        const auto worker = sequence % WORKER_COUNT;
        const auto object = (sequence / WORKER_COUNT) % OBJECTS_PER_WORKER;
        std::string value;
        RETURN_IF_NOT_OK(clients_[worker]->Get(objects_[worker].keys[object], value, RPC_TIMEOUT_MS));
        CHECK_FAIL_RETURN_STATUS(value == objects_[worker].value, K_RUNTIME_ERROR,
                                 "Object value changed during policy update");
        return Status::OK();
    }

    Status CheckAllObjectsReadable()
    {
        for (uint32_t worker = 0; worker < WORKER_COUNT; ++worker) {
            for (const auto &key : objects_[worker].keys) {
                std::string value;
                RETURN_IF_NOT_OK(clients_[worker]->Get(key, value, RPC_TIMEOUT_MS));
                CHECK_FAIL_RETURN_STATUS(value == objects_[worker].value, K_RUNTIME_ERROR,
                                         "Object value changed after policy update");
            }
        }
        return Status::OK();
    }

    Status CheckSurvivingWorkerObjectsReadable(uint32_t excludedWorker)
    {
        for (uint32_t worker = 0; worker < WORKER_COUNT; ++worker) {
            if (worker == excludedWorker) {
                continue;
            }
            for (const auto &key : objects_.at(worker).keys) {
                std::string value;
                RETURN_IF_NOT_OK(clients_.at(worker)->Get(key, value, RPC_TIMEOUT_MS));
                CHECK_FAIL_RETURN_STATUS(value == objects_.at(worker).value, K_RUNTIME_ERROR,
                                         "Surviving worker object changed during policy recovery");
            }
        }
        return Status::OK();
    }

    Status CheckOneSurvivingWorkerObjectReadable(size_t sequence, uint32_t excludedWorker)
    {
        const auto survivingWorkerCount = WORKER_COUNT - 1;
        auto worker = sequence % survivingWorkerCount;
        worker += worker >= excludedWorker;
        const auto object = (sequence / survivingWorkerCount) % OBJECTS_PER_WORKER;
        std::string value;
        RETURN_IF_NOT_OK(clients_.at(worker)->Get(objects_.at(worker).keys.at(object), value, RPC_TIMEOUT_MS));
        CHECK_FAIL_RETURN_STATUS(value == objects_.at(worker).value, K_RUNTIME_ERROR,
                                 "Surviving worker object changed during policy recovery");
        return Status::OK();
    }

    void ReconnectClients()
    {
        for (uint32_t worker = 0; worker < WORKER_COUNT; ++worker) {
            InitTestKVClient(worker, clients_[worker]);
        }
    }

    Status CheckWorkerObjectReadable(uint32_t workerIndex)
    {
        std::string value;
        RETURN_IF_NOT_OK(
            clients_.at(workerIndex)->Get(objects_.at(workerIndex).keys.front(), value, RPC_TIMEOUT_MS));
        CHECK_FAIL_RETURN_STATUS(value == objects_.at(workerIndex).value, K_RUNTIME_ERROR,
                                 "Object value changed during policy update");
        return Status::OK();
    }

    Status CheckFreshWritesReadable(uint64_t epoch)
    {
        for (uint32_t worker = 0; worker < WORKER_COUNT; ++worker) {
            const auto key = "policy-hot-update-after-fault-w" + std::to_string(worker) + "-e"
                             + std::to_string(epoch);
            const auto value = "after-fault-value-" + std::to_string(worker);
            RETURN_IF_NOT_OK(clients_.at(worker)->Set(key, value));
            std::string actual;
            RETURN_IF_NOT_OK(clients_.at(worker)->Get(key, actual, RPC_TIMEOUT_MS));
            CHECK_FAIL_RETURN_STATUS(actual == value, K_RUNTIME_ERROR,
                                     "Fresh object changed after policy recovery");
        }
        return Status::OK();
    }

    Status FillWorkerMemory(uint32_t workerIndex)
    {
        datasystem::SetParam param;
        param.writeMode = datasystem::WriteMode::NONE_L2_CACHE_EVICT;
        const std::string value(EVICTION_FILL_OBJECT_SIZE, 'e');
        for (uint32_t object = 0; object < EVICTION_FILL_OBJECT_COUNT; ++object) {
            const auto key = "policy-hot-update-eviction-fill-" + std::to_string(object);
            RETURN_IF_NOT_OK(clients_.at(workerIndex)->Set(key, value, param));
        }
        return Status::OK();
    }

    Status CreateRebalancePressure(uint32_t workerIndex)
    {
        constexpr uint32_t objectCount = 3;
        datasystem::SetParam param;
        param.writeMode = datasystem::WriteMode::NONE_L2_CACHE_EVICT;
        rebalanceValue_.assign(EVICTION_FILL_OBJECT_SIZE, 'r');
        rebalanceKeys_.clear();
        for (uint32_t object = 0; object < objectCount; ++object) {
            auto key = "policy-hot-update-rebalance-w" + std::to_string(workerIndex) + "-k" + std::to_string(object);
            RETURN_IF_NOT_OK(clients_.at(workerIndex)->Set(key, rebalanceValue_, param));
            rebalanceKeys_.emplace_back(std::move(key));
        }
        return Status::OK();
    }

    Status CheckRebalancePressureReadable(uint32_t sourceWorker)
    {
        for (const auto &key : rebalanceKeys_) {
            std::string value;
            RETURN_IF_NOT_OK(clients_.at(sourceWorker)->Get(key, value, RPC_TIMEOUT_MS));
            CHECK_FAIL_RETURN_STATUS(value == rebalanceValue_, K_RUNTIME_ERROR,
                                     "Rebalanced object changed during policy update");
        }
        return Status::OK();
    }

    Status WaitForInjectExecutionOnAnyTarget(uint32_t sourceWorker, const std::string &name, uint32_t &targetWorker)
    {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(UPDATE_TIMEOUT_MS);
        size_t sequence = 0;
        do {
            for (uint32_t worker = 0; worker < workerAddresses_.size(); ++worker) {
                if (worker == sourceWorker) {
                    continue;
                }
                uint64_t executeCount = 0;
                RETURN_IF_NOT_OK(cluster_->GetInjectActionExecuteCount(WORKER, worker, name, executeCount));
                if (executeCount > 0) {
                    targetWorker = worker;
                    return Status::OK();
                }
            }
            RETURN_IF_NOT_OK(CheckOneObjectReadable(sequence++));
            std::this_thread::sleep_for(std::chrono::milliseconds(POLL_INTERVAL_MS));
        } while (std::chrono::steady_clock::now() < deadline);
        RETURN_STATUS(K_RPC_DEADLINE_EXCEEDED, "Timed out waiting for target migration fault injection");
    }

    const std::unordered_set<std::string> &GetAllWorkerAddresses() const
    {
        return expectedWorkerAddresses_;
    }

    Status WriteObjectsDuringConversion(uint64_t epoch)
    {
        for (uint32_t worker = 0; worker < WORKER_COUNT; ++worker) {
            auto key = "policy-hot-update-live-w" + std::to_string(worker) + "-e" + std::to_string(epoch);
            RETURN_IF_NOT_OK(clients_[worker]->Set(key, objects_[worker].value));
            objects_[worker].keys.emplace_back(std::move(key));
        }
        return Status::OK();
    }

    std::unordered_set<std::string> GetExpectedWorkerAddresses(uint32_t cohortPercent) const
    {
        std::unordered_set<std::string> selected;
        for (const auto &address : expectedWorkerAddresses_) {
            if (MurmurHash3_32(address) % FULL_COHORT_PERCENT < cohortPercent) {
                selected.emplace(address);
            }
        }
        return selected;
    }

    Status FindPartialCohortPercent(uint32_t &cohortPercent) const
    {
        for (uint32_t percent = MIN_COHORT_PERCENT; percent < FULL_COHORT_PERCENT; ++percent) {
            const auto selected = GetExpectedWorkerAddresses(percent);
            if (!selected.empty() && selected.size() < WORKER_COUNT) {
                cohortPercent = percent;
                return Status::OK();
            }
        }
        RETURN_STATUS(K_RUNTIME_ERROR, "Worker address hashes cannot form a non-empty partial cohort");
    }

    Status WaitForInjectExecution(uint32_t workerIndex, const std::string &name, bool checkReads = true)
    {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(UPDATE_TIMEOUT_MS);
        size_t sequence = 0;
        do {
            uint64_t executeCount = 0;
            auto rc = cluster_->GetInjectActionExecuteCount(WORKER, workerIndex, name, executeCount);
            RETURN_IF_NOT_OK(rc);
            if (executeCount > 0) {
                return Status::OK();
            }
            if (checkReads) {
                RETURN_IF_NOT_OK(CheckOneObjectReadable(sequence++));
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(POLL_INTERVAL_MS));
        } while (std::chrono::steady_clock::now() < deadline);
        RETURN_STATUS(K_RPC_DEADLINE_EXCEEDED, "Timed out waiting for policy-update fault injection");
    }

    Status WaitForWorkerFailure(uint64_t epoch, uint32_t workerIndex, StatusCode expectedCode,
                                master::EvictionPolicyUpdatePhasePb expectedPhase,
                                const std::string &expectedReason)
    {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(UPDATE_TIMEOUT_MS);
        master::GetEvictionPolicyUpdateProgressRspPb progress;
        Status lastRc(K_NOT_READY, "No policy update failure observed");
        do {
            progress.Clear();
            lastRc = GetPolicyUpdateProgress(epoch, progress);
            if (lastRc.IsOk()) {
                for (const auto &worker : progress.workers()) {
                    if (worker.address() != workerAddresses_.at(workerIndex)
                        || worker.status() != master::EVICTION_POLICY_WORKER_FAILED) {
                        continue;
                    }
                    CHECK_FAIL_RETURN_STATUS(worker.active_policy() == master::EVICTION_POLICY_CLOCK
                                                 && worker.epoch() == epoch && worker.phase() == expectedPhase
                                                 && worker.failure_code() == static_cast<int32_t>(expectedCode)
                                                 && worker.failure_reason().find(expectedReason) != std::string::npos,
                                             K_RUNTIME_ERROR,
                                             "Worker reported an unexpected policy-update failure");
                    return Status::OK();
                }
            } else if (lastRc.GetCode() != K_NOT_FOUND) {
                return lastRc;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(POLL_INTERVAL_MS));
        } while (std::chrono::steady_clock::now() < deadline);
        RETURN_STATUS(K_RPC_DEADLINE_EXCEEDED,
                      FormatString("Worker %zu did not report the expected failure; last progress: %s",
                                   static_cast<size_t>(workerIndex), progress.ShortDebugString()));
    }

    Status WaitForWorkerStatus(uint64_t epoch, master::EvictionPolicyWorkerStatusPb expectedStatus,
                               master::EvictionPolicyPb expectedActivePolicy,
                               master::EvictionPolicyPb targetPolicy,
                               master::EvictionPolicyCommandPb expectedCommand,
                               uint32_t expectedCohortPercent,
                               const std::unordered_set<std::string> &expectedAddresses,
                               master::GetEvictionPolicyUpdateProgressRspPb &progress, bool &observedConverting,
                               bool &wroteDuringConversion, bool allowTransientFailure = false,
                               uint64_t minimumExpectedObjects = OBJECTS_PER_WORKER,
                               uint32_t excludedReadWorker = WORKER_COUNT)
    {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(UPDATE_TIMEOUT_MS);
        size_t sequence = 0;
        Status lastRc(K_NOT_READY, "No policy update progress observed");
        do {
            progress.Clear();
            lastRc = GetPolicyUpdateProgress(epoch, progress);
            if (lastRc.IsOk()) {
                CHECK_FAIL_RETURN_STATUS(progress.has_rollout()
                                             && progress.rollout().cohort_percent() == expectedCohortPercent
                                             && progress.rollout().update().epoch() == epoch
                                             && progress.rollout().update().target_policy() == targetPolicy
                                             && progress.rollout().update().migration_batch_size()
                                                    == MIGRATION_BATCH_SIZE
                                             && progress.rollout().update().command() == expectedCommand,
                                         K_RUNTIME_ERROR, "Master returned an unexpected policy rollout");
                CHECK_FAIL_RETURN_STATUS(allowTransientFailure || progress.failed_workers() == 0, K_RUNTIME_ERROR,
                                         "A worker failed the eviction policy update");
                observedConverting = observedConverting || progress.converting_workers() > 0;
                if (expectedCommand == master::EVICTION_POLICY_COMMIT_CONVERT && progress.converting_workers() > 0
                    && !wroteDuringConversion) {
                    RETURN_IF_NOT_OK(WriteObjectsDuringConversion(epoch));
                    wroteDuringConversion = true;
                }
                const uint64_t expectedCount =
                    expectedStatus == master::EVICTION_POLICY_WORKER_READY ? progress.ready_workers()
                                                                          : progress.active_workers();
                if (progress.selected_workers() == expectedAddresses.size()
                    && expectedCount == expectedAddresses.size()) {
                    std::unordered_set<std::string> observedAddresses;
                    for (const auto &worker : progress.workers()) {
                        CHECK_FAIL_RETURN_STATUS(worker.status() == expectedStatus, K_RUNTIME_ERROR,
                                                 "Worker reported an unexpected policy update status");
                        CHECK_FAIL_RETURN_STATUS(expectedAddresses.count(worker.address()) == 1
                                                     && observedAddresses.emplace(worker.address()).second,
                                                 K_RUNTIME_ERROR,
                                                 "Progress did not contain every expected worker exactly once");
                        CHECK_FAIL_RETURN_STATUS(worker.active_policy() == expectedActivePolicy
                                                     && worker.epoch() == epoch
                                                     && worker.phase() == master::EVICTION_POLICY_STABLE
                                                     && worker.total_objects() >= minimumExpectedObjects,
                                                 K_RUNTIME_ERROR,
                                                 "Worker reported unexpected policy state");
                        CHECK_FAIL_RETURN_STATUS(
                            expectedStatus != master::EVICTION_POLICY_WORKER_READY
                                || worker.migrated_objects() == 0,
                            K_RUNTIME_ERROR, "PRECHECK modified eviction policy migration state");
                        CHECK_FAIL_RETURN_STATUS(
                            expectedStatus != master::EVICTION_POLICY_WORKER_ACTIVE
                                || worker.migrated_objects() == worker.total_objects(),
                            K_RUNTIME_ERROR, "Worker did not finish eviction policy migration");
                    }
                    CHECK_FAIL_RETURN_STATUS(observedAddresses.size() == expectedAddresses.size(), K_RUNTIME_ERROR,
                                             "Policy progress worker set is incomplete");
                    return Status::OK();
                }
            } else if (lastRc.GetCode() != K_NOT_FOUND) {
                return lastRc;
            }
            if (excludedReadWorker < WORKER_COUNT) {
                RETURN_IF_NOT_OK(CheckOneSurvivingWorkerObjectReadable(sequence++, excludedReadWorker));
            } else {
                RETURN_IF_NOT_OK(CheckOneObjectReadable(sequence++));
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(POLL_INTERVAL_MS));
        } while (std::chrono::steady_clock::now() < deadline);

        RETURN_STATUS(K_RPC_DEADLINE_EXCEEDED,
                      FormatString("Policy update epoch %zu did not converge; last progress: %s",
                                   static_cast<size_t>(epoch), progress.ShortDebugString()));
    }

    Status RunPolicyUpdate(master::EvictionPolicyPb sourcePolicy, master::EvictionPolicyPb targetPolicy,
                           uint64_t epoch, uint32_t initialCohortPercent)
    {
        const auto initialAddresses = GetExpectedWorkerAddresses(initialCohortPercent);
        CHECK_FAIL_RETURN_STATUS(!initialAddresses.empty(), K_RUNTIME_ERROR,
                                 "Initial policy rollout cohort is empty");
        RETURN_IF_NOT_OK(StartPolicyUpdate(targetPolicy, epoch, master::EVICTION_POLICY_PRECHECK,
                                           initialCohortPercent));
        master::GetEvictionPolicyUpdateProgressRspPb progress;
        bool observedConverting = false;
        bool wroteDuringConversion = false;
        RETURN_IF_NOT_OK(WaitForWorkerStatus(
            epoch, master::EVICTION_POLICY_WORKER_READY, sourcePolicy, targetPolicy,
            master::EVICTION_POLICY_PRECHECK, initialCohortPercent, initialAddresses, progress,
            observedConverting, wroteDuringConversion));

        RETURN_IF_NOT_OK(StartPolicyUpdate(targetPolicy, epoch, master::EVICTION_POLICY_COMMIT_CONVERT,
                                           initialCohortPercent));
        RETURN_IF_NOT_OK(WaitForWorkerStatus(
            epoch, master::EVICTION_POLICY_WORKER_ACTIVE, targetPolicy, targetPolicy,
            master::EVICTION_POLICY_COMMIT_CONVERT, initialCohortPercent, initialAddresses, progress,
            observedConverting, wroteDuringConversion));
        if (initialCohortPercent < FULL_COHORT_PERCENT) {
            RETURN_IF_NOT_OK(StartPolicyUpdate(targetPolicy, epoch, master::EVICTION_POLICY_COMMIT_CONVERT,
                                               FULL_COHORT_PERCENT));
            RETURN_IF_NOT_OK(WaitForWorkerStatus(
                epoch, master::EVICTION_POLICY_WORKER_ACTIVE, targetPolicy, targetPolicy,
                master::EVICTION_POLICY_COMMIT_CONVERT, FULL_COHORT_PERCENT, expectedWorkerAddresses_, progress,
                observedConverting, wroteDuringConversion));
        }
        CHECK_FAIL_RETURN_STATUS(observedConverting, K_RUNTIME_ERROR,
                                 "No worker reported the converting phase");
        CHECK_FAIL_RETURN_STATUS(wroteDuringConversion, K_RUNTIME_ERROR,
                                 "No object was written during policy conversion");
        return CheckAllObjectsReadable();
    }

private:
    Signature signature_{ ACCESS_KEY, SensitiveValue(SECRET_KEY) };
    std::shared_ptr<brpc::Channel> brpcChannel_;
    std::unique_ptr<master::MasterOCService_BrpcGenericStub> brpcStub_;
    std::vector<std::shared_ptr<KVClient>> clients_;
    std::vector<WorkerObjects> objects_;
    std::vector<std::string> rebalanceKeys_;
    std::string rebalanceValue_;
    std::vector<std::string> workerAddresses_;
    std::unordered_set<std::string> expectedWorkerAddresses_;
};

class LEVEL1_EvictionPolicyHotUpdateRebalanceTest : public LEVEL1_EvictionPolicyHotUpdateTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        LEVEL1_EvictionPolicyHotUpdateTest::SetClusterSetupOptions(opts);
        opts.workerGflagParams =
            "-shared_memory_size_mb=64 -log_monitor=true -log_monitor_interval_ms=1000 "
            "-eviction_strategy=clock -enable_memory_rebalance=true -rebalance_source_usage_percent=30 "
            "-rebalance_usage_gap_percent=10 -v=1";
    }
};

TEST_F(LEVEL1_EvictionPolicyHotUpdateTest, ClockHeatClockConvergesAcrossAllWorkers)
{
    DS_ASSERT_OK(WriteWorkerObjects());
    DS_ASSERT_OK(CheckAllObjectsReadable());
    uint32_t partialCohortPercent = 0;
    DS_ASSERT_OK(FindPartialCohortPercent(partialCohortPercent));
    DS_ASSERT_OK(RunPolicyUpdate(master::EVICTION_POLICY_CLOCK, master::EVICTION_POLICY_HEAT, 101,
                                 partialCohortPercent));
    DS_ASSERT_OK(RunPolicyUpdate(master::EVICTION_POLICY_HEAT, master::EVICTION_POLICY_CLOCK, 102,
                                 FULL_COHORT_PERCENT));
}

TEST_F(LEVEL1_EvictionPolicyHotUpdateTest, PrecheckAdmissionFailureKeepsOldPolicyAndNextEpochConverges)
{
    constexpr uint64_t rejectedEpoch = 201;
    constexpr uint64_t recoveryEpoch = 202;
    constexpr uint64_t maximumSourceObjects = 1;
    DS_ASSERT_OK(WriteWorkerObjects());
    DS_ASSERT_OK(StartPolicyUpdate(master::EVICTION_POLICY_HEAT, rejectedEpoch, master::EVICTION_POLICY_PRECHECK,
                                   FULL_COHORT_PERCENT, 0, maximumSourceObjects));
    for (uint32_t worker = 0; worker < WORKER_COUNT; ++worker) {
        DS_ASSERT_OK(WaitForWorkerFailure(rejectedEpoch, worker, StatusCode::K_NO_SPACE,
                                         master::EVICTION_POLICY_STABLE,
                                         "source object count exceeds the precheck limit"));
    }
    DS_ASSERT_OK(CheckAllObjectsReadable());
    DS_ASSERT_OK(RunPolicyUpdate(master::EVICTION_POLICY_CLOCK, master::EVICTION_POLICY_HEAT, recoveryEpoch,
                                 FULL_COHORT_PERCENT));
}

TEST_F(LEVEL1_EvictionPolicyHotUpdateTest, AuditMutationRetriesAndConverges)
{
    constexpr uint64_t epoch = 301;
    DS_ASSERT_OK(WriteWorkerObjects());
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, FAULT_WORKER_INDEX, AFTER_AUDIT_INJECT, "1*pause()"));
    DS_ASSERT_OK(StartPolicyUpdate(master::EVICTION_POLICY_HEAT, epoch, master::EVICTION_POLICY_PRECHECK,
                                   FULL_COHORT_PERCENT));
    master::GetEvictionPolicyUpdateProgressRspPb progress;
    bool observedConverting = false;
    bool wroteDuringConversion = false;
    DS_ASSERT_OK(WaitForWorkerStatus(
        epoch, master::EVICTION_POLICY_WORKER_READY, master::EVICTION_POLICY_CLOCK, master::EVICTION_POLICY_HEAT,
        master::EVICTION_POLICY_PRECHECK, FULL_COHORT_PERCENT, GetAllWorkerAddresses(), progress,
        observedConverting, wroteDuringConversion));
    DS_ASSERT_OK(StartPolicyUpdate(master::EVICTION_POLICY_HEAT, epoch, master::EVICTION_POLICY_COMMIT_CONVERT,
                                   FULL_COHORT_PERCENT));
    DS_ASSERT_OK(WaitForInjectExecution(FAULT_WORKER_INDEX, AFTER_AUDIT_INJECT));

    auto getRc = CheckWorkerObjectReadable(FAULT_WORKER_INDEX);
    auto clearRc = cluster_->ClearInjectAction(WORKER, FAULT_WORKER_INDEX, AFTER_AUDIT_INJECT);
    DS_ASSERT_OK(clearRc);
    DS_ASSERT_OK(getRc);

    DS_ASSERT_OK(WaitForWorkerFailure(epoch, FAULT_WORKER_INDEX, StatusCode::K_TRY_AGAIN,
                                     master::EVICTION_POLICY_VERIFYING,
                                     "membership changed after audit"));
    wroteDuringConversion = true;
    DS_ASSERT_OK(WaitForWorkerStatus(
        epoch, master::EVICTION_POLICY_WORKER_ACTIVE, master::EVICTION_POLICY_HEAT, master::EVICTION_POLICY_HEAT,
        master::EVICTION_POLICY_COMMIT_CONVERT, FULL_COHORT_PERCENT, GetAllWorkerAddresses(), progress,
        observedConverting, wroteDuringConversion, true));
    DS_ASSERT_OK(CheckAllObjectsReadable());
}

TEST_F(LEVEL1_EvictionPolicyHotUpdateTest, MasterWorkerCrashAfterAuditRecoversForward)
{
    constexpr uint64_t epoch = 401;
    DS_ASSERT_OK(WriteWorkerObjects());
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, MASTER_WORKER_INDEX, AFTER_AUDIT_INJECT, "1*pause()"));
    DS_ASSERT_OK(StartPolicyUpdate(master::EVICTION_POLICY_HEAT, epoch, master::EVICTION_POLICY_PRECHECK,
                                   FULL_COHORT_PERCENT));
    master::GetEvictionPolicyUpdateProgressRspPb progress;
    bool observedConverting = false;
    bool wroteDuringConversion = false;
    DS_ASSERT_OK(WaitForWorkerStatus(
        epoch, master::EVICTION_POLICY_WORKER_READY, master::EVICTION_POLICY_CLOCK, master::EVICTION_POLICY_HEAT,
        master::EVICTION_POLICY_PRECHECK, FULL_COHORT_PERCENT, GetAllWorkerAddresses(), progress,
        observedConverting, wroteDuringConversion));
    DS_ASSERT_OK(StartPolicyUpdate(master::EVICTION_POLICY_HEAT, epoch, master::EVICTION_POLICY_COMMIT_CONVERT,
                                   FULL_COHORT_PERCENT));
    DS_ASSERT_OK(WaitForInjectExecution(MASTER_WORKER_INDEX, AFTER_AUDIT_INJECT));

    auto *externalCluster = dynamic_cast<ExternalCluster *>(cluster_.get());
    ASSERT_NE(externalCluster, nullptr);
    DS_ASSERT_OK(externalCluster->RestartWorkerAndWaitReadyOneByOne(
        { MASTER_WORKER_INDEX }, SIGKILL, RESTART_TIMEOUT_S));
    DS_ASSERT_OK(ReconnectMasterStub());
    ReconnectClients();

    wroteDuringConversion = true;
    DS_ASSERT_OK(WaitForWorkerStatus(
        epoch, master::EVICTION_POLICY_WORKER_ACTIVE, master::EVICTION_POLICY_HEAT, master::EVICTION_POLICY_HEAT,
        master::EVICTION_POLICY_COMMIT_CONVERT, FULL_COHORT_PERCENT, GetAllWorkerAddresses(), progress,
        observedConverting, wroteDuringConversion, true, 0, MASTER_WORKER_INDEX));
    DS_ASSERT_OK(CheckSurvivingWorkerObjectsReadable(MASTER_WORKER_INDEX));
}

TEST_F(LEVEL1_EvictionPolicyHotUpdateTest, CommitDrainsRunningEvictionBeforeMigration)
{
    constexpr uint64_t epoch = 501;
    DS_ASSERT_OK(WriteWorkerObjects());
    DS_ASSERT_OK(StartPolicyUpdate(master::EVICTION_POLICY_HEAT, epoch, master::EVICTION_POLICY_PRECHECK,
                                   FULL_COHORT_PERCENT));
    master::GetEvictionPolicyUpdateProgressRspPb progress;
    bool observedConverting = false;
    bool wroteDuringConversion = false;
    DS_ASSERT_OK(WaitForWorkerStatus(
        epoch, master::EVICTION_POLICY_WORKER_READY, master::EVICTION_POLICY_CLOCK, master::EVICTION_POLICY_HEAT,
        master::EVICTION_POLICY_PRECHECK, FULL_COHORT_PERCENT, GetAllWorkerAddresses(), progress,
        observedConverting, wroteDuringConversion));

    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, FAULT_WORKER_INDEX, EVICTION_ROUND_INJECT, "1*pause()"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, FAULT_WORKER_INDEX, AFTER_CANCEL_INJECT, "call()"));
    auto fillFuture =
        std::async(std::launch::async, [this]() { return FillWorkerMemory(FAULT_WORKER_INDEX); });

    const auto evictionPauseRc = WaitForInjectExecution(FAULT_WORKER_INDEX, EVICTION_ROUND_INJECT, false);
    Status commitRc(K_NOT_READY, "Eviction round did not reach the pause point");
    Status cancelObservedRc(K_NOT_READY, "Policy update did not request eviction cancellation");
    if (evictionPauseRc.IsOk()) {
        commitRc = StartPolicyUpdate(master::EVICTION_POLICY_HEAT, epoch,
                                     master::EVICTION_POLICY_COMMIT_CONVERT, FULL_COHORT_PERCENT);
        if (commitRc.IsOk()) {
            cancelObservedRc = WaitForInjectExecution(FAULT_WORKER_INDEX, AFTER_CANCEL_INJECT, false);
        }
    }
    const auto clearEvictionRc =
        cluster_->ClearInjectAction(WORKER, FAULT_WORKER_INDEX, EVICTION_ROUND_INJECT);
    const auto clearCancelRc =
        cluster_->ClearInjectAction(WORKER, FAULT_WORKER_INDEX, AFTER_CANCEL_INJECT);
    const auto fillReady = fillFuture.wait_for(std::chrono::seconds(ASYNC_FILL_TIMEOUT_S));
    Status fillRc(K_RPC_DEADLINE_EXCEEDED, "Memory-fill task did not stop after eviction drain");
    if (fillReady == std::future_status::ready) {
        fillRc = fillFuture.get();
    }

    DS_ASSERT_OK(evictionPauseRc);
    DS_ASSERT_OK(commitRc);
    DS_ASSERT_OK(cancelObservedRc);
    DS_ASSERT_OK(clearEvictionRc);
    DS_ASSERT_OK(clearCancelRc);
    ASSERT_TRUE(fillReady == std::future_status::ready);
    EXPECT_TRUE(fillRc.IsOk() || fillRc.GetCode() == K_OUT_OF_MEMORY || fillRc.GetCode() == K_NO_SPACE)
        << fillRc.ToString();

    wroteDuringConversion = true;
    DS_ASSERT_OK(WaitForWorkerStatus(
        epoch, master::EVICTION_POLICY_WORKER_ACTIVE, master::EVICTION_POLICY_HEAT, master::EVICTION_POLICY_HEAT,
        master::EVICTION_POLICY_COMMIT_CONVERT, FULL_COHORT_PERCENT, GetAllWorkerAddresses(), progress,
        observedConverting, wroteDuringConversion, true, 0));
    DS_ASSERT_OK(CheckFreshWritesReadable(epoch));
}

TEST_F(LEVEL1_EvictionPolicyHotUpdateRebalanceTest, SourceRebalanceDrainsBeforePolicyConversion)
{
    constexpr uint64_t epoch = 601;
    constexpr uint32_t sourceWorker = FAULT_WORKER_INDEX;
    DS_ASSERT_OK(WriteWorkerObjects());
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, sourceWorker, REBALANCE_SOURCE_SEND_INJECT, "1*pause()"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, sourceWorker, REBALANCE_SOURCE_DRAIN_INJECT, "call()"));
    DS_ASSERT_OK(CreateRebalancePressure(sourceWorker));
    const auto sendPausedRc = WaitForInjectExecution(sourceWorker, REBALANCE_SOURCE_SEND_INJECT);

    Status precheckRc(K_NOT_READY, "Rebalance source did not reach send pause");
    Status readyRc(K_NOT_READY, "Policy precheck was not submitted");
    Status commitRc(K_NOT_READY, "Policy commit was not submitted");
    Status drainObservedRc(K_NOT_READY, "Source rebalance drain was not observed");
    master::GetEvictionPolicyUpdateProgressRspPb progress;
    bool observedConverting = false;
    bool wroteDuringConversion = false;
    if (sendPausedRc.IsOk()) {
        precheckRc = StartPolicyUpdate(master::EVICTION_POLICY_HEAT, epoch, master::EVICTION_POLICY_PRECHECK,
                                       FULL_COHORT_PERCENT);
    }
    if (precheckRc.IsOk()) {
        readyRc = WaitForWorkerStatus(
            epoch, master::EVICTION_POLICY_WORKER_READY, master::EVICTION_POLICY_CLOCK,
            master::EVICTION_POLICY_HEAT, master::EVICTION_POLICY_PRECHECK, FULL_COHORT_PERCENT,
            GetAllWorkerAddresses(), progress, observedConverting, wroteDuringConversion);
    }
    if (readyRc.IsOk()) {
        commitRc = StartPolicyUpdate(master::EVICTION_POLICY_HEAT, epoch,
                                     master::EVICTION_POLICY_COMMIT_CONVERT, FULL_COHORT_PERCENT);
    }
    if (commitRc.IsOk()) {
        drainObservedRc = WaitForInjectExecution(sourceWorker, REBALANCE_SOURCE_DRAIN_INJECT, false);
    }
    const auto clearSendRc = cluster_->ClearInjectAction(WORKER, sourceWorker, REBALANCE_SOURCE_SEND_INJECT);
    const auto clearDrainRc = cluster_->ClearInjectAction(WORKER, sourceWorker, REBALANCE_SOURCE_DRAIN_INJECT);

    DS_ASSERT_OK(sendPausedRc);
    DS_ASSERT_OK(precheckRc);
    DS_ASSERT_OK(readyRc);
    DS_ASSERT_OK(commitRc);
    DS_ASSERT_OK(drainObservedRc);
    DS_ASSERT_OK(clearSendRc);
    DS_ASSERT_OK(clearDrainRc);
    DS_ASSERT_OK(WaitForWorkerStatus(
        epoch, master::EVICTION_POLICY_WORKER_ACTIVE, master::EVICTION_POLICY_HEAT,
        master::EVICTION_POLICY_HEAT, master::EVICTION_POLICY_COMMIT_CONVERT, FULL_COHORT_PERCENT,
        GetAllWorkerAddresses(), progress, observedConverting, wroteDuringConversion, true, 0));
    DS_ASSERT_OK(CheckRebalancePressureReadable(sourceWorker));
}

TEST_F(LEVEL1_EvictionPolicyHotUpdateRebalanceTest, TargetMigrationDrainsBeforePolicyConversion)
{
    constexpr uint64_t epoch = 602;
    constexpr uint32_t sourceWorker = FAULT_WORKER_INDEX;
    DS_ASSERT_OK(WriteWorkerObjects());
    for (uint32_t worker = 0; worker < WORKER_COUNT; ++worker) {
        if (worker == sourceWorker) {
            continue;
        }
        DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, worker, REBALANCE_TARGET_ADMISSION_INJECT, "1*pause()"));
        DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, worker, REBALANCE_TARGET_DRAIN_INJECT, "call()"));
    }
    DS_ASSERT_OK(CreateRebalancePressure(sourceWorker));
    uint32_t targetWorker = sourceWorker;
    const auto migrationPausedRc =
        WaitForInjectExecutionOnAnyTarget(sourceWorker, REBALANCE_TARGET_ADMISSION_INJECT, targetWorker);
    for (uint32_t worker = 0; worker < WORKER_COUNT; ++worker) {
        if (worker == sourceWorker || worker == targetWorker) {
            continue;
        }
        DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, worker, REBALANCE_TARGET_ADMISSION_INJECT));
        DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, worker, REBALANCE_TARGET_DRAIN_INJECT));
    }

    Status precheckRc(K_NOT_READY, "Rebalance target did not acquire migration admission");
    Status readyRc(K_NOT_READY, "Policy precheck was not submitted");
    Status commitRc(K_NOT_READY, "Policy commit was not submitted");
    Status drainObservedRc(K_NOT_READY, "Target migration drain was not observed");
    master::GetEvictionPolicyUpdateProgressRspPb progress;
    bool observedConverting = false;
    bool wroteDuringConversion = false;
    if (migrationPausedRc.IsOk()) {
        precheckRc = StartPolicyUpdate(master::EVICTION_POLICY_HEAT, epoch, master::EVICTION_POLICY_PRECHECK,
                                       FULL_COHORT_PERCENT);
    }
    if (precheckRc.IsOk()) {
        readyRc = WaitForWorkerStatus(
            epoch, master::EVICTION_POLICY_WORKER_READY, master::EVICTION_POLICY_CLOCK,
            master::EVICTION_POLICY_HEAT, master::EVICTION_POLICY_PRECHECK, FULL_COHORT_PERCENT,
            GetAllWorkerAddresses(), progress, observedConverting, wroteDuringConversion);
    }
    if (readyRc.IsOk()) {
        commitRc = StartPolicyUpdate(master::EVICTION_POLICY_HEAT, epoch,
                                     master::EVICTION_POLICY_COMMIT_CONVERT, FULL_COHORT_PERCENT);
    }
    if (commitRc.IsOk()) {
        drainObservedRc = WaitForInjectExecution(targetWorker, REBALANCE_TARGET_DRAIN_INJECT, false);
    }
    const auto clearAdmissionRc =
        cluster_->ClearInjectAction(WORKER, targetWorker, REBALANCE_TARGET_ADMISSION_INJECT);
    const auto clearDrainRc = cluster_->ClearInjectAction(WORKER, targetWorker, REBALANCE_TARGET_DRAIN_INJECT);

    DS_ASSERT_OK(migrationPausedRc);
    DS_ASSERT_OK(precheckRc);
    DS_ASSERT_OK(readyRc);
    DS_ASSERT_OK(commitRc);
    DS_ASSERT_OK(drainObservedRc);
    DS_ASSERT_OK(clearAdmissionRc);
    DS_ASSERT_OK(clearDrainRc);
    DS_ASSERT_OK(WaitForWorkerStatus(
        epoch, master::EVICTION_POLICY_WORKER_ACTIVE, master::EVICTION_POLICY_HEAT,
        master::EVICTION_POLICY_HEAT, master::EVICTION_POLICY_COMMIT_CONVERT, FULL_COHORT_PERCENT,
        GetAllWorkerAddresses(), progress, observedConverting, wroteDuringConversion, true));
    DS_ASSERT_OK(CheckRebalancePressureReadable(sourceWorker));
}

}  // namespace st
}  // namespace datasystem
