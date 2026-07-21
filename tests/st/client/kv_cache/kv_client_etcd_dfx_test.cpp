/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2024. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Description: State client etcd dfx tests.
 */

#include <gtest/gtest.h>
#include <chrono>
#include <memory>
#include <netdb.h>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>
#include "client/kv_cache/kv_client_scale_common.h"
#include "cluster/base_cluster.h"
#include "common.h"
#include "datasystem/common/flags/common_flags.h"  // FLAGS_use_brpc
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/kv_client.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace st {
namespace {
constexpr int kEventuallyWaitTimeoutMs = 15'000;
constexpr int kEventuallyPollIntervalMs = 100;
constexpr char kKeepAliveFailureInject[] = "EtcdKeepAlive.SendKeepAliveMessage";
constexpr char kKeepAliveQuickLoopInject[] = "EtcdStore.LaunchKeepAliveThreads.loopQuickly";
constexpr char kLeaseExpiredInject[] = "GetLeaseExpiredMs";
constexpr char kLocalIsolatedInject[] = "WorkerOCServer.AfterMarkLocalIsolated";
constexpr char kBeforeMarkRunningInject[] = "WorkerRecoveryController.BeforeMarkRunning";

Status TryConnectTcpPort(const HostPort &addr)
{
    struct addrinfo hints = {};
    hints.ai_family = addr.IsIPv6() ? AF_INET6 : AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_NUMERICHOST | AI_NUMERICSERV;

    struct addrinfo *rawResult = nullptr;
    auto port = std::to_string(addr.Port());
    auto ret = getaddrinfo(addr.Host().c_str(), port.c_str(), &hints, &rawResult);
    CHECK_FAIL_RETURN_STATUS(ret == 0, K_RUNTIME_ERROR,
                             FormatString("Resolve address %s failed: %s", addr.ToString(), gai_strerror(ret)));
    std::unique_ptr<struct addrinfo, decltype(&freeaddrinfo)> result(rawResult, freeaddrinfo);

    for (auto *item = result.get(); item != nullptr; item = item->ai_next) {
        auto fd = socket(item->ai_family, item->ai_socktype, item->ai_protocol);
        if (fd < 0) {
            continue;
        }
        auto connRet = connect(fd, item->ai_addr, item->ai_addrlen);
        close(fd);
        if (connRet == 0) {
            return Status::OK();
        }
    }
    RETURN_STATUS(K_NOT_READY, FormatString("Address %s is not listening", addr.ToString()));
}

bool WaitForTcpPortListening(const HostPort &addr, std::chrono::milliseconds timeout)
{
    auto deadline = std::chrono::steady_clock::now() + timeout;
    do {
        if (TryConnectTcpPort(addr).IsOk()) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    } while (std::chrono::steady_clock::now() < deadline);
    return false;
}

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

void ExpectAdmissionRejected(const Status &rc, const std::string &mode, const std::string &operation)
{
    ASSERT_EQ(rc.GetCode(), K_NOT_READY) << operation << " status: " << rc.ToString();
    EXPECT_NE(rc.GetMsg().find(mode), std::string::npos) << operation << " status: " << rc.ToString();
}
}  // namespace

class KVClientEtcdDfxTest : public KVClientScaleCommon {
public:
    void SetUp() override
    {
        CommonTest::SetUp();
        DS_ASSERT_OK(Init());
        ASSERT_TRUE(cluster_ != nullptr);
        DS_ASSERT_OK(cluster_->StartEtcdCluster());
        externalCluster_ = dynamic_cast<ExternalCluster *>(cluster_.get());
    }

    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numEtcd = 1;
        opts.numOBS = 1;
        opts.numWorkers = workerNum_;
        opts.addNodeTime = SCALE_UP_ADD_TIME;
        opts.enableDistributedMaster = enableDistributedMaster_;
        opts.workerGflagParams =
            FormatString(" -v=1 -node_timeout_s=%d -node_dead_timeout_s=%d", timeoutS_, deadTimeoutS_);
        opts.injectActions = "worker.PreShutDown.skip:return(K_OK)";
        opts.waitWorkerReady = false;
    }

protected:
    void BasicPutGetDel(const std::shared_ptr<KVClient> &client)
    {
        SetParam param{ .writeMode = WriteMode::NONE_L2_CACHE };
        std::string keyPrefix = "Level0", value = "test", modifyValue = "test2";
        std::vector<std::string> keys;
        size_t keyNum = 100;
        for (size_t i = 0; i < keyNum; i++) {
            std::string key = keyPrefix + std::to_string(i);
            keys.emplace_back(key);
            DS_ASSERT_OK(client->Set(key, value, param));
        }

        std::vector<std::string> valsToGet;
        DS_ASSERT_OK(client->Get(keys, valsToGet));
        ASSERT_EQ(valsToGet.size(), keyNum);
        for (auto valToGet : valsToGet) {
            ASSERT_EQ(valToGet, value);
        }

        for (auto &key : keys) {
            DS_ASSERT_OK(client->Set(key, modifyValue));
            std::string valToGet;
            DS_ASSERT_OK(client->Get(key, valToGet));
            ASSERT_EQ(valToGet, modifyValue);
        }

        std::vector<std::string> failedKeys;
        DS_ASSERT_OK(client->Del(keys, failedKeys));
        ASSERT_EQ(failedKeys.size(), 0);
    }

    int timeoutS_ = 2;
    int deadTimeoutS_ = 3;
    ExternalCluster *externalCluster_ = nullptr;
    const int maxWaitTimeSec_ = 20;
    const size_t workerNum_ = 5;
    std::string enableDistributedMaster_ = "true";
};

TEST_F(KVClientEtcdDfxTest, LEVEL1_TestEtcdRestart)
{
    if (FLAGS_use_brpc) {
        GTEST_SKIP() << "brpc migration gap; flaky/failing under brpc. Tracked separately.";
    }
    DS_ASSERT_OK(externalCluster_->StartWorkerAndWaitReady({ 0, 1 }));
    DS_ASSERT_OK(externalCluster_->ShutdownEtcds());
    sleep(deadTimeoutS_ + 1);

    std::shared_ptr<KVClient> client;
    InitTestKVClient(0, client);
    SetParam param{ .writeMode = WriteMode::NONE_L2_CACHE };
    std::string key_name = "Level0", value = "test", modifyValue = "test2";
    std::vector<std::string> keys;
    size_t keyNum = 100;
    for (size_t i = 0; i < keyNum; i++) {
        std::string key = key_name + std::to_string(i);
        keys.emplace_back(key);
        DS_ASSERT_OK(client->Set(key, value, param));
    }

    std::vector<std::string> valsToGet;
    DS_ASSERT_OK(client->Get(keys, valsToGet));
    ASSERT_EQ(valsToGet.size(), keyNum);
    for (auto valToGet : valsToGet) {
        ASSERT_EQ(valToGet, value);
    }

    for (auto key : keys) {
        DS_ASSERT_OK(client->Set(key, modifyValue));
        std::string valToGet;
        DS_ASSERT_OK(client->Get(key, valToGet));
        ASSERT_EQ(valToGet, modifyValue);
    }

    DS_ASSERT_OK(externalCluster_->StartEtcdCluster());

    DS_ASSERT_OK(client->Get(keys, valsToGet));
    ASSERT_EQ(valsToGet.size(), keyNum);
    for (auto valToGet : valsToGet) {
        ASSERT_EQ(valToGet, modifyValue);
    }
    client.reset();

    DS_ASSERT_OK(externalCluster_->StartWorkerAndWaitReady({ 2 }));
    WaitTopologyTasksDrained({ 0, 1, 2 });

    int workerIndex2 = 2;
    InitTestKVClient(workerIndex2, client);
    DS_ASSERT_OK(client->Get(keys, valsToGet));
    ASSERT_EQ(valsToGet.size(), keyNum);
    for (auto valToGet : valsToGet) {
        ASSERT_EQ(valToGet, modifyValue);
    }
    client.reset();
}

TEST_F(KVClientEtcdDfxTest, LEVEL1_TestStartingWorkerRejectsClientSetupBeforeReady)
{
    const int workerIndex = 0;
    DS_ASSERT_OK(externalCluster_->StartWorker(
        workerIndex, HostPort(),
        " -inject_actions=test.start.notWait:call(0);worker.PreShutDown.skip:return(K_OK);"
        "master.disableRocksDb:1*call();WorkerRecoveryController.BeforeMarkRunning:1*sleep(6000)"));

    HostPort workerAddress;
    DS_ASSERT_OK(cluster_->GetWorkerAddr(workerIndex, workerAddress));
    ASSERT_TRUE(WaitForTcpPortListening(workerAddress, std::chrono::seconds(5)));
    std::this_thread::sleep_for(std::chrono::milliseconds(2500));

    ConnectOptions blockedOptions;
    InitConnectOpt(workerIndex, blockedOptions, 500);
    auto blockedClient = std::make_shared<KVClient>(blockedOptions);
    auto blockedRc = blockedClient->Init();
    EXPECT_EQ(blockedRc.GetCode(), StatusCode::K_NOT_READY) << blockedRc.ToString();

    WaitAllMembersJoinClusterTopology(1, 20);
    WaitTopologyTasksDrained({ workerIndex }, 20);

    std::shared_ptr<KVClient> readyClient;
    Status readyRc(StatusCode::K_NOT_READY, "ready client init has not been attempted");
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(20);
    do {
        ConnectOptions readyOptions;
        InitConnectOpt(workerIndex, readyOptions, 2000);
        readyClient = std::make_shared<KVClient>(readyOptions);
        readyRc = readyClient->Init();
        if (readyRc.IsOk()) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    } while (std::chrono::steady_clock::now() < deadline);
    DS_ASSERT_OK(readyRc);

    const std::string key = "startup_admission_" + GetStringUuid();
    const std::string value = "ready";
    DS_ASSERT_OK(readyClient->Set(key, value));
    std::string got;
    DS_ASSERT_OK(readyClient->Get(key, got));
    EXPECT_EQ(got, value);
}

TEST_F(KVClientEtcdDfxTest, LEVEL1_KVClientRejectsReadWriteDuringIsolationAndRecovering)
{
    constexpr int workerIndex = 0;
    DS_ASSERT_OK(externalCluster_->StartWorkerAndWaitReady(
        { workerIndex, 1 },
        " -client_reconnect_wait_s=1 -ipc_through_shared_memory=true -heartbeat_interval_ms=1000"
        " -auto_del_dead_node=false"));

    std::shared_ptr<KVClient> client;
    InitTestKVClient(workerIndex, client);
    const std::string key = "kv_admission_" + GetStringUuid();
    const std::string value = "value";
    DS_ASSERT_OK(client->Set(key, value));
    std::string got;
    DS_ASSERT_OK(client->Get(key, got));
    ASSERT_EQ(got, value);

    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, workerIndex, kLocalIsolatedInject, "call()"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, workerIndex, kKeepAliveFailureInject, "return(K_RPC_UNAVAILABLE)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, workerIndex, kLeaseExpiredInject, "call(1000)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, workerIndex, kKeepAliveQuickLoopInject, "call(0)"));
    bool keepAliveFailureActive = true;
    bool recoveryPauseActive = false;
    Raii clearFaults([&]() {
        if (recoveryPauseActive) {
            LOG_IF_ERROR(cluster_->ClearInjectAction(WORKER, workerIndex, kBeforeMarkRunningInject),
                         "clear kv recovery pause");
        }
        if (keepAliveFailureActive) {
            LOG_IF_ERROR(cluster_->ClearInjectAction(WORKER, workerIndex, kKeepAliveFailureInject),
                         "clear kv keepalive failure");
            LOG_IF_ERROR(cluster_->ClearInjectAction(WORKER, workerIndex, kLeaseExpiredInject),
                         "clear kv lease override");
            LOG_IF_ERROR(cluster_->ClearInjectAction(WORKER, workerIndex, kKeepAliveQuickLoopInject),
                         "clear kv quick keepalive loop");
        }
    });

    AssertEventuallyOk(
        [&]() {
            uint64_t count = 0;
            RETURN_IF_NOT_OK(cluster_->GetInjectActionExecuteCount(WORKER, workerIndex, kLocalIsolatedInject, count));
            CHECK_FAIL_RETURN_STATUS(count > 0, K_NOT_READY, "worker has not entered local isolation");
            return Status::OK();
        },
        "kv client target enters local isolation");

    got.clear();
    ExpectAdmissionRejected(client->Get(key, got, 1'000), "LOCAL_ISOLATED", "kv get during local isolation");
    ExpectAdmissionRejected(client->Set("kv_admission_isolated_" + GetStringUuid(), value), "LOCAL_ISOLATED",
                            "kv set during local isolation");

    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, workerIndex, kBeforeMarkRunningInject, "1*pause"));
    recoveryPauseActive = true;
    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, workerIndex, kKeepAliveFailureInject));
    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, workerIndex, kLeaseExpiredInject));
    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, workerIndex, kKeepAliveQuickLoopInject));
    keepAliveFailureActive = false;
    AssertEventuallyOk(
        [&]() {
            uint64_t count = 0;
            RETURN_IF_NOT_OK(
                cluster_->GetInjectActionExecuteCount(WORKER, workerIndex, kBeforeMarkRunningInject, count));
            CHECK_FAIL_RETURN_STATUS(count > 0, K_NOT_READY, "worker has not entered the recovery evidence gate");
            return Status::OK();
        },
        "kv client target enters recovering mode");

    got.clear();
    ExpectAdmissionRejected(client->Get(key, got, 1'000), "RECOVERING", "kv get during recovery");
    ExpectAdmissionRejected(client->Set("kv_admission_recovering_" + GetStringUuid(), value), "RECOVERING",
                            "kv set during recovery");

    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, workerIndex, kBeforeMarkRunningInject));
    recoveryPauseActive = false;
    AssertEventuallyOk(
        [&]() {
            std::string recovered;
            RETURN_IF_NOT_OK(client->Get(key, recovered, 1'000));
            CHECK_FAIL_RETURN_STATUS(recovered == value, K_NOT_READY, "kv value mismatch after recovery");
            return Status::OK();
        },
        "kv client reopens after recovery evidence completes");
}

TEST_F(KVClientEtcdDfxTest, DISABLED_TestWatchEventLost)
{
    DS_ASSERT_OK(externalCluster_->StartWorkerAndWaitReady(
        { 0, 1 }, " -inject_actions=EtcdWatch.RetrieveEventPassively.RetrieveEventQuickly:call(100)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "EtcdWatch.StoreEvents.IgnoreEvent", "return()"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "EtcdWatch.StoreEvents.IgnoreEvent", "return()"));

    DS_ASSERT_OK(externalCluster_->StartWorkerAndWaitReady({ 2 }));

    int waitTime = SCALE_UP_ADD_TIME + 2;
    int workerNum = 3;
    WaitAllMembersJoinClusterTopology(workerNum, waitTime);
    CheckMaster({ 0, 1, 2 });
}

TEST_F(KVClientEtcdDfxTest, TestEtcdCommitFailed)
{
    DS_ASSERT_OK(externalCluster_->StartWorkerAndWaitReady({ 1 }));
    DS_ASSERT_OK(
        externalCluster_->StartWorkerAndWaitReady({ 0 }, " -inject_actions=etcd.txn.commit:1*return(K_TRY_AGAIN)"));
    DS_ASSERT_OK(externalCluster_->StartWorkerAndWaitReady({ 2 }));
}

TEST_F(KVClientEtcdDfxTest, DISABLED_TestErrorMsgWhenEtcdFailed)
{
    DS_ASSERT_OK(cluster_->StartOBS());
    DS_ASSERT_OK(externalCluster_->StartWorkerAndWaitReady({ 0, 1 }));
    DS_ASSERT_OK(externalCluster_->ShutdownEtcds());

    SetParam param{ .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE };
    std::string key_name = "Level0", value = "test";
    std::shared_ptr<KVClient> client;
    InitTestKVClient(0, client);
    auto status = client->Set(key_name, value, param);
    ASSERT_TRUE(status.ToString().find("Put::etcd_kv_Put") != std::string::npos);
}

TEST_F(KVClientEtcdDfxTest, DISABLED_LEVEL1_TestRestartWorkerDuringEtcdCrash)
{
    DS_ASSERT_OK(externalCluster_->StartWorkerAndWaitReady({ 0, 1, 2 }));
    DS_ASSERT_OK(externalCluster_->ShutdownEtcds());
    DS_ASSERT_OK(externalCluster_->RestartWorkerAndWaitReadyOneByOne({ 0 }));

    std::shared_ptr<KVClient> client;
    InitTestKVClient(0, client);
    BasicPutGetDel(client);
    DS_ASSERT_OK(externalCluster_->StartEtcdCluster());
}

// Test the scenario where reconciliation cannot be completed during the restart process
TEST_F(KVClientEtcdDfxTest, LEVEL1_TestRestartWorkerDuringEtcdCrash2)
{
    DS_ASSERT_OK(externalCluster_->StartWorkerAndWaitReady({ 0 }));
    DS_ASSERT_OK(externalCluster_->ShutdownEtcds());
    DS_ASSERT_NOT_OK(externalCluster_->RestartWorkerAndWaitReadyOneByOne({ 0 }));
}

TEST_F(KVClientEtcdDfxTest, LEVEL1_TestScaleUpWorkerDuringEtcdCrash)
{
    DS_ASSERT_OK(externalCluster_->StartWorkerAndWaitReady({ 0, 1, 2 }));
    DS_ASSERT_OK(externalCluster_->ShutdownEtcds());
    int maxWaitTimeSec = 10;
    DS_ASSERT_NOT_OK(externalCluster_->StartWorkerAndWaitReady({ 3 }, maxWaitTimeSec));
    DS_ASSERT_OK(externalCluster_->StartEtcdCluster());
}

class KVClientEtcdDfxTestAdjustNodeTimeout : public KVClientEtcdDfxTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        timeoutS_ = 60;       // 60s
        deadTimeoutS_ = 300;  // 300s
        KVClientEtcdDfxTest::SetClusterSetupOptions(opts);
        opts.disableRocksDB = false;
        opts.injectActions = "GetLeaseRenewIntervalMs:return(1000)";
    }
};

TEST_F(KVClientEtcdDfxTestAdjustNodeTimeout, TestSetHealthProbe)
{
    DS_ASSERT_OK(externalCluster_->StartWorkerAndWaitReady({ 0, 1, 2 }));
    DS_ASSERT_OK(externalCluster_->ShutdownNode(WORKER, 1));
    DS_ASSERT_OK(externalCluster_->StartWorker(1, HostPort(),
                                               " -inject_actions=worker.RunKeepAliveTask:3*return(K_RPC_UNAVAILABLE)"));
    constexpr int recoveryWindowSec = 15;
    DS_ASSERT_OK(externalCluster_->WaitNodeReady(WORKER, 1, recoveryWindowSec));
}

TEST_F(KVClientEtcdDfxTestAdjustNodeTimeout, TestRestartDuringEtcdCrash)
{
    DS_ASSERT_OK(externalCluster_->StartWorkerAndWaitReady({ 0, 1, 2 }));
    WaitTopologyTasksDrained({ 0, 1, 2 });
    DS_ASSERT_OK(externalCluster_->ShutdownEtcds());

    auto val = "val";

    std::vector<std::string> keysPerRestart;
    std::shared_ptr<KVClient> client0;
    InitTestKVClient(0, client0);
    keysPerRestart.emplace_back(client0->GenerateKey());
    ASSERT_NE(keysPerRestart.back(), "");
    keysPerRestart.emplace_back("per_restart");
    std::shared_ptr<KVClient> client1;
    InitTestKVClient(1, client1);
    for (const auto &key : keysPerRestart) {
        DS_ASSERT_OK(client1->Set(key, val));
    }
    client1.reset();

    constexpr int restartFailureWindowSec = 10;
    DS_ASSERT_NOT_OK(externalCluster_->RestartWorkerAndWaitReadyOneByOne({ 1 }, SIGTERM, restartFailureWindowSec));
    DS_ASSERT_OK(externalCluster_->StartEtcdCluster());
    DS_ASSERT_OK(externalCluster_->StartWorkerAndWaitReady({ 1 }));
    InitTestKVClient(1, client1);

    std::vector<std::string> keysPostRestart;
    keysPostRestart.emplace_back(client0->GenerateKey());
    ASSERT_NE(keysPostRestart.back(), "");
    keysPostRestart.emplace_back("post_restart");
    for (const auto &key : keysPostRestart) {
        DS_ASSERT_OK(client1->Set(key, val));
    }

    constexpr int waitReconciliationSec = 5;
    sleep(waitReconciliationSec);

    for (const auto &key : keysPerRestart) {
        std::string valToGet;
        DS_ASSERT_NOT_OK(client0->Get(key, valToGet));
    }

    for (const auto &key : keysPostRestart) {
        std::string valToGet;
        DS_ASSERT_OK(client0->Get(key, valToGet));
        EXPECT_EQ(valToGet, val);
    }
}

class KVClientEtcdDfxCentralizedMasterTest : public KVClientEtcdDfxTest {
public:
    void SetUp() override
    {
        KVClientEtcdDfxTest::enableDistributedMaster_ = "false";
        KVClientEtcdDfxTest::SetUp();
    }
};

TEST_F(KVClientEtcdDfxCentralizedMasterTest, DISABLED_LEVEL1_TestRestartWorkerDuringEtcdCrash)
{
    DS_ASSERT_OK(externalCluster_->StartWorkerAndWaitReady({ 0, 1, 2 }));
    DS_ASSERT_OK(externalCluster_->ShutdownEtcds());
    int maxWaitTimeSec = 10;
    DS_ASSERT_NOT_OK(externalCluster_->RestartWorkerAndWaitReadyOneByOne({ 0 }, maxWaitTimeSec));
    DS_ASSERT_OK(externalCluster_->StartEtcdCluster());
}

}  // namespace st
}  // namespace datasystem
