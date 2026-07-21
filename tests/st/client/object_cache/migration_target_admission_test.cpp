/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
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

#include <gtest/gtest.h>
#include <sys/wait.h>

#include <chrono>
#include <cerrno>
#include <csignal>
#include <fstream>
#include <iterator>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "common.h"
#include "datasystem/common/ak_sk/ak_sk_manager.h"
#include "datasystem/common/kvstore/coordination_keys.h"
#include "datasystem/common/rpc/rpc_stub_cache_mgr.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/object/object_enum.h"
#include "datasystem/worker/object_cache/worker_worker_oc_api.h"
#include "oc_client_common.h"

namespace datasystem::st {
namespace {
constexpr int kEventuallyWaitTimeoutMs = 15'000;
constexpr int kEventuallyPollIntervalMs = 100;
constexpr char kMigrationServiceInject[] = "worker.migrate_service.return";
constexpr char kKeepAliveFailureInject[] = "EtcdKeepAlive.SendKeepAliveMessage";
constexpr char kKeepAliveQuickLoopInject[] = "EtcdStore.LaunchKeepAliveThreads.loopQuickly";
constexpr char kLeaseExpiredInject[] = "GetLeaseExpiredMs";
constexpr char kLocalIsolatedInject[] = "WorkerOCServer.AfterMarkLocalIsolated";
constexpr char kBeforeMarkRunningInject[] = "WorkerRecoveryController.BeforeMarkRunning";

template <typename Operation>
void AssertEventuallyOk(Operation operation, const std::string &operationName)
{
    Status rc;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(kEventuallyWaitTimeoutMs);
    do {
        rc = operation();
        if (rc.IsOk()) {
            return;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(kEventuallyPollIntervalMs));
    } while (std::chrono::steady_clock::now() < deadline);
    ASSERT_TRUE(rc.IsOk()) << operationName << " stayed non-OK for " << kEventuallyWaitTimeoutMs
                           << " ms, last status: " << rc.ToString();
}

void ExpectAdmissionRejected(const Status &rc, const std::string &mode, StatusCode expectedCode,
                             const std::string &operation)
{
    ASSERT_EQ(rc.GetCode(), expectedCode) << operation << " status: " << rc.ToString();
    EXPECT_NE(rc.GetMsg().find(mode), std::string::npos) << operation << " status: " << rc.ToString();
}

class MigrationTargetProbe {
public:
    Status Init(BaseCluster *cluster, uint32_t targetIndex, uint32_t sourceIndex)
    {
        RETURN_IF_NOT_OK(cluster->GetWorkerAddr(targetIndex, target_));
        RETURN_IF_NOT_OK(cluster->GetWorkerAddr(sourceIndex, source_));
        akSkManager_ = std::make_shared<AkSkManager>();
        RETURN_IF_NOT_OK(
            akSkManager_->SetClientAkSk("QTWAOYTTINDUT2QVKYUC", "MFyfvK41ba2giqM7**********KGpownRZlmVmHc"));
        RETURN_IF_NOT_OK(RpcStubCacheMgr::Instance().Init(100, source_));
        api_ = std::make_unique<object_cache::WorkerRemoteWorkerOCApi>(target_, source_, akSkManager_);
        return api_->Init();
    }

    Status ExpectRejected(BaseCluster *cluster, uint32_t targetIndex, const std::string &mode, StatusCode expectedCode,
                          const std::string &objectKey)
    {
        return ExpectRejectedOneOf(cluster, targetIndex, { { mode, expectedCode } }, objectKey);
    }

    Status ExpectRejectedOneOf(BaseCluster *cluster, uint32_t targetIndex,
                               const std::vector<std::pair<std::string, StatusCode>> &expectedModes,
                               const std::string &objectKey)
    {
        uint64_t serviceCallsBefore = 0;
        RETURN_IF_NOT_OK(
            cluster->GetInjectActionExecuteCount(WORKER, targetIndex, kMigrationServiceInject, serviceCallsBefore));

        MigrateDataReqPb req;
        auto *object = req.add_objects();
        object->set_object_key(objectKey);
        object->set_version(1);
        object->set_data_size(1);
        object->set_cache_type(static_cast<uint32_t>(CacheType::MEMORY));
        req.set_worker_addr(source_.ToString());
        req.set_type(MigrateType::SCALE_DOWN);
        MigrateDataRspPb rsp;
        auto rc = api_->MigrateDataProbe(req, rsp, 1'000);
        bool matchedMode = false;
        std::string expectedModeDesc;
        for (const auto &[mode, expectedCode] : expectedModes) {
            expectedModeDesc += mode + "/" + std::to_string(static_cast<int>(expectedCode)) + " ";
            if (rc.GetCode() == expectedCode && rc.GetMsg().find(mode) != std::string::npos) {
                matchedMode = true;
                break;
            }
        }
        CHECK_FAIL_RETURN_STATUS(
            matchedMode, K_RUNTIME_ERROR,
            "unexpected migration rejection, expected one of [" + expectedModeDesc + "], actual: " + rc.ToString());
        CHECK_FAIL_RETURN_STATUS(rc.GetMsg().find("MIGRATION_TARGET") != std::string::npos, K_RUNTIME_ERROR,
                                 "migration rejection does not identify the admission kind");
        CHECK_FAIL_RETURN_STATUS(rsp.success_ids().empty(), K_RUNTIME_ERROR,
                                 "rejected migration unexpectedly returned a success id");

        uint64_t serviceCallsAfter = 0;
        RETURN_IF_NOT_OK(
            cluster->GetInjectActionExecuteCount(WORKER, targetIndex, kMigrationServiceInject, serviceCallsAfter));
        CHECK_FAIL_RETURN_STATUS(serviceCallsAfter == serviceCallsBefore, K_RUNTIME_ERROR,
                                 "rejected migration reached the allocation service");
        return Status::OK();
    }

private:
    HostPort target_;
    HostPort source_;
    std::shared_ptr<AkSkManager> akSkManager_;
    std::unique_ptr<object_cache::WorkerRemoteWorkerOCApi> api_;
};
}  // namespace

class MigrationTargetIsolationTest : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.workerGflagParams =
            "-client_reconnect_wait_s=1 -ipc_through_shared_memory=true -node_timeout_s=2 "
            "-node_dead_timeout_s=3 -heartbeat_interval_ms=1000 -auto_del_dead_node=false";
        opts.numWorkers = 2;
        opts.enableDistributedMaster = "true";
        opts.numEtcd = 1;
    }
};

TEST_F(MigrationTargetIsolationTest, LEVEL1_MigrationTargetFiltersIsolatedAndRecoveringWorker)
{
    std::shared_ptr<ObjectClient> client;
    InitTestClient(0, client);
    MigrationTargetProbe probe;
    DS_ASSERT_OK(probe.Init(cluster_.get(), 0, 1));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, kMigrationServiceInject, "call()"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, kLocalIsolatedInject, "call()"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, kKeepAliveFailureInject, "return(K_RPC_UNAVAILABLE)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, kLeaseExpiredInject, "call(1000)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, kKeepAliveQuickLoopInject, "call(0)"));
    bool keepAliveFailureActive = true;
    bool recoveryPauseActive = false;
    Raii clearFaults([&]() {
        if (recoveryPauseActive) {
            LOG_IF_ERROR(cluster_->ClearInjectAction(WORKER, 0, kBeforeMarkRunningInject),
                         "clear migration-target recovery pause");
        }
        if (keepAliveFailureActive) {
            LOG_IF_ERROR(cluster_->ClearInjectAction(WORKER, 0, kKeepAliveFailureInject),
                         "clear migration-target keepalive failure");
            LOG_IF_ERROR(cluster_->ClearInjectAction(WORKER, 0, kLeaseExpiredInject),
                         "clear migration-target lease override");
            LOG_IF_ERROR(cluster_->ClearInjectAction(WORKER, 0, kKeepAliveQuickLoopInject),
                         "clear migration-target quick keepalive loop");
        }
    });

    AssertEventuallyOk(
        [&]() {
            uint64_t count = 0;
            RETURN_IF_NOT_OK(cluster_->GetInjectActionExecuteCount(WORKER, 0, kLocalIsolatedInject, count));
            CHECK_FAIL_RETURN_STATUS(count > 0, K_NOT_READY, "worker has not entered local isolation");
            return Status::OK();
        },
        "migration target enters local isolation");
    DS_ASSERT_OK(
        probe.ExpectRejected(cluster_.get(), 0, "LOCAL_ISOLATED", K_NOT_READY, "migration-target-local-isolated"));

    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, kBeforeMarkRunningInject, "1*pause"));
    recoveryPauseActive = true;
    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, 0, kKeepAliveFailureInject));
    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, 0, kLeaseExpiredInject));
    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, 0, kKeepAliveQuickLoopInject));
    keepAliveFailureActive = false;
    AssertEventuallyOk(
        [&]() {
            uint64_t count = 0;
            RETURN_IF_NOT_OK(cluster_->GetInjectActionExecuteCount(WORKER, 0, kBeforeMarkRunningInject, count));
            CHECK_FAIL_RETURN_STATUS(count > 0, K_NOT_READY, "worker has not entered the recovery evidence gate");
            return Status::OK();
        },
        "migration target enters recovering mode");
    DS_ASSERT_OK(probe.ExpectRejected(cluster_.get(), 0, "RECOVERING", K_NOT_READY, "migration-target-recovering"));

    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, 0, kBeforeMarkRunningInject));
    recoveryPauseActive = false;
    AssertEventuallyOk(
        [&]() {
            std::shared_ptr<Buffer> buffer;
            RETURN_IF_NOT_OK(client->Create(NewObjectKey(), 1, CreateParam{}, buffer));
            return buffer->Publish();
        },
        "worker reopens after migration target recovery evidence completes");
}

TEST_F(MigrationTargetIsolationTest, LEVEL1_ObjectClientRejectsReadWriteDuringIsolationAndRecovering)
{
    std::shared_ptr<ObjectClient> client;
    InitTestClient(0, client);
    const std::string objectKey = NewObjectKey();
    std::shared_ptr<Buffer> buffer;
    DS_ASSERT_OK(client->Create(objectKey, 1, CreateParam{}, buffer));
    DS_ASSERT_OK(buffer->Publish());
    std::shared_ptr<Buffer> localIsolatedWriteBuffer;
    DS_ASSERT_OK(client->Create(NewObjectKey(), 1, CreateParam{}, localIsolatedWriteBuffer));
    std::shared_ptr<Buffer> recoveringWriteBuffer;
    DS_ASSERT_OK(client->Create(NewObjectKey(), 1, CreateParam{}, recoveringWriteBuffer));

    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, kLocalIsolatedInject, "call()"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, kKeepAliveFailureInject, "return(K_RPC_UNAVAILABLE)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, kLeaseExpiredInject, "call(1000)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, kKeepAliveQuickLoopInject, "call(0)"));
    bool keepAliveFailureActive = true;
    bool recoveryPauseActive = false;
    Raii clearFaults([&]() {
        if (recoveryPauseActive) {
            LOG_IF_ERROR(cluster_->ClearInjectAction(WORKER, 0, kBeforeMarkRunningInject),
                         "clear object-client recovery pause");
        }
        if (keepAliveFailureActive) {
            LOG_IF_ERROR(cluster_->ClearInjectAction(WORKER, 0, kKeepAliveFailureInject),
                         "clear object-client keepalive failure");
            LOG_IF_ERROR(cluster_->ClearInjectAction(WORKER, 0, kLeaseExpiredInject),
                         "clear object-client lease override");
            LOG_IF_ERROR(cluster_->ClearInjectAction(WORKER, 0, kKeepAliveQuickLoopInject),
                         "clear object-client quick keepalive loop");
        }
    });

    AssertEventuallyOk(
        [&]() {
            uint64_t count = 0;
            RETURN_IF_NOT_OK(cluster_->GetInjectActionExecuteCount(WORKER, 0, kLocalIsolatedInject, count));
            CHECK_FAIL_RETURN_STATUS(count > 0, K_NOT_READY, "worker has not entered local isolation");
            return Status::OK();
        },
        "object client target enters local isolation");

    std::vector<Optional<Buffer>> getBuffers;
    ExpectAdmissionRejected(client->Get({ objectKey }, 1'000, getBuffers), "LOCAL_ISOLATED", K_NOT_READY,
                            "object get during local isolation");
    ExpectAdmissionRejected(localIsolatedWriteBuffer->Publish(), "LOCAL_ISOLATED", K_NOT_READY,
                            "object publish during local isolation");

    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, kBeforeMarkRunningInject, "1*pause"));
    recoveryPauseActive = true;
    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, 0, kKeepAliveFailureInject));
    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, 0, kLeaseExpiredInject));
    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, 0, kKeepAliveQuickLoopInject));
    keepAliveFailureActive = false;
    AssertEventuallyOk(
        [&]() {
            uint64_t count = 0;
            RETURN_IF_NOT_OK(cluster_->GetInjectActionExecuteCount(WORKER, 0, kBeforeMarkRunningInject, count));
            CHECK_FAIL_RETURN_STATUS(count > 0, K_NOT_READY, "worker has not entered the recovery evidence gate");
            return Status::OK();
        },
        "object client target enters recovering mode");

    getBuffers.clear();
    ExpectAdmissionRejected(client->Get({ objectKey }, 1'000, getBuffers), "RECOVERING", K_NOT_READY,
                            "object get during recovery");
    ExpectAdmissionRejected(recoveringWriteBuffer->Publish(), "RECOVERING", K_NOT_READY,
                            "object publish during recovery");

    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, 0, kBeforeMarkRunningInject));
    recoveryPauseActive = false;
    AssertEventuallyOk(
        [&]() {
            std::vector<Optional<Buffer>> recoveredBuffers;
            RETURN_IF_NOT_OK(client->Get({ objectKey }, 1'000, recoveredBuffers));
            CHECK_FAIL_RETURN_STATUS(!recoveredBuffers.empty() && recoveredBuffers[0], K_NOT_READY,
                                     "object get returned no buffer after recovery");
            return Status::OK();
        },
        "object client reopens after recovery evidence completes");
}

class MigrationTargetOomTest : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = 1;
        opts.numEtcd = 1;
        opts.workerGflagParams = "-shared_memory_size_mb=64";
        opts.injectActions = "NodeSelector.setInterval:call(2000)";
    }
};

TEST_F(MigrationTargetOomTest, LEVEL1_MigrationTargetFiltersOutOfMemoryWorker)
{
    std::shared_ptr<ObjectClient> client;
    InitTestClient(0, client, 60'000, 2'000);
    MigrationTargetProbe probe;
    DS_ASSERT_OK(probe.Init(cluster_.get(), 0, 0));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, kMigrationServiceInject, "call()"));

    std::vector<std::shared_ptr<Buffer>> pressureBuffers;
    constexpr uint64_t pressureChunkSize = 8ULL * 1024 * 1024;
    for (size_t i = 0; i < 6; ++i) {
        std::shared_ptr<Buffer> pressureBuffer;
        DS_ASSERT_OK(client->Create(NewObjectKey(), pressureChunkSize, CreateParam{}, pressureBuffer));
        pressureBuffers.emplace_back(std::move(pressureBuffer));
    }
    std::shared_ptr<Buffer> failedBuffer;
    constexpr uint64_t overflowSize = 4ULL * 1024 * 1024;
    auto pressureRc = client->Create(NewObjectKey(), overflowSize, CreateParam{}, failedBuffer);
    ASSERT_TRUE(pressureRc.GetCode() == K_OUT_OF_MEMORY || pressureRc.GetCode() == K_NOT_READY)
        << pressureRc.ToString();
    DS_ASSERT_OK(probe.ExpectRejectedOneOf(cluster_.get(), 0,
                                           { { "OUT_OF_MEMORY", K_OUT_OF_MEMORY }, { "RECOVERING", K_NOT_READY } },
                                           "migration-target-out-of-memory"));
    std::this_thread::sleep_for(std::chrono::milliseconds(2500));
    DS_ASSERT_OK(probe.ExpectRejected(cluster_.get(), 0, "RECOVERING", K_NOT_READY,
                                      "migration-target-stays-closed-above-low-water"));
    pressureBuffers.clear();
    AssertEventuallyOk(
        [&]() {
            std::shared_ptr<Buffer> recoveredBuffer;
            RETURN_IF_NOT_OK(client->Create(NewObjectKey(), 1, CreateParam{}, recoveredBuffer));
            return recoveredBuffer->Publish();
        },
        "worker reopens after allocator resource evidence completes");
}

class MigrationTargetDrainingTest : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = 2;
        opts.numEtcd = 1;
        opts.enableDistributedMaster = "true";
    }
};

TEST_F(MigrationTargetDrainingTest, LEVEL1_MigrationTargetFiltersDrainingWorker)
{
    auto db = InitTestEtcdInstance();
    ASSERT_NE(db, nullptr);
    HostPort workerAddress;
    DS_ASSERT_OK(cluster_->GetWorkerAddr(0, workerAddress));
    MigrationTargetProbe probe;
    DS_ASSERT_OK(probe.Init(cluster_.get(), 0, 1));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, kMigrationServiceInject, "call()"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "WorkerOCServer.AfterMarkDraining", "pause()"));
    bool drainPauseActive = true;
    Raii clearDrainPause([&]() {
        if (drainPauseActive) {
            LOG_IF_ERROR(cluster_->ClearInjectAction(WORKER, 0, "WorkerOCServer.AfterMarkDraining"),
                         "clear migration-target drain pause");
        }
    });

    ServerProcess *process = nullptr;
    DS_ASSERT_OK(cluster_->GetProcess(WORKER, 0, process));
    ASSERT_NE(process, nullptr);
    ASSERT_TRUE(process->IsProcessAlive());
    const std::string statusPath = cluster_->GetRootDir() + "/worker0/log/worker-status";
    {
        std::ofstream status(statusPath, std::ios::trunc);
        ASSERT_TRUE(status.is_open());
        status << "voluntary scale in\n";
    }
    DS_ASSERT_OK(process->Kill(SIGTERM));
    AssertEventuallyOk(
        [&]() {
            uint64_t count = 0;
            RETURN_IF_NOT_OK(
                cluster_->GetInjectActionExecuteCount(WORKER, 0, "WorkerOCServer.AfterMarkDraining", count));
            CHECK_FAIL_RETURN_STATUS(count > 0, K_NOT_READY, "worker has not published draining mode");
            return Status::OK();
        },
        "migration target enters draining mode");
    ASSERT_TRUE(process->IsProcessAlive());
    DS_ASSERT_OK(probe.ExpectRejected(cluster_.get(), 0, "DRAINING", K_NOT_READY, "migration-target-draining"));

    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, 0, "WorkerOCServer.AfterMarkDraining"));
    drainPauseActive = false;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(15);
    int processStatus = 0;
    bool topologyRemoved = false;
    bool shutdownMarkerSeen = false;
    bool processExited = false;
    while (std::chrono::steady_clock::now() < deadline) {
        std::string topologyBytes;
        if (db->Get(GetTopologyTableName(), "", topologyBytes).IsOk()) {
            ClusterTopologyPb topology;
            ASSERT_TRUE(topology.ParseFromString(topologyBytes));
            topologyRemoved = topology.members().find(workerAddress.ToString()) == topology.members().end();
        }
        std::ifstream status(statusPath);
        std::string marker{ std::istreambuf_iterator<char>(status), std::istreambuf_iterator<char>() };
        shutdownMarkerSeen = shutdownMarkerSeen || marker == "worker_stop_status:ready";
        const auto waitResult = waitpid(process->Pid(), &processStatus, WNOHANG);
        if (waitResult == process->Pid()) {
            processExited = true;
        } else if (waitResult < 0 && errno != EINTR) {
            ADD_FAILURE() << "waitpid failed while observing migration-target drain, errno=" << errno;
            break;
        }
        if (topologyRemoved && shutdownMarkerSeen && processExited) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(kEventuallyPollIntervalMs));
    }
    if (!processExited) {
        LOG_IF_ERROR(process->Kill(SIGKILL), "force cleanup of migration-target drain worker");
    }
    EXPECT_TRUE(topologyRemoved);
    EXPECT_TRUE(shutdownMarkerSeen);
    ASSERT_TRUE(processExited);
    ASSERT_TRUE(WIFEXITED(processStatus));
    EXPECT_EQ(WEXITSTATUS(processStatus), 0);
}
}  // namespace datasystem::st
