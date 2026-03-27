/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2022. All rights reserved.
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
 * Description: ListenWorker class test.
 */

#include "datasystem/client/listen_worker.h"
#include <thread>
#include "datasystem/client/client_worker_common_api.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/inject/inject_point.h"

#include "common.h"

using namespace datasystem::client;

namespace datasystem {
namespace st {
class ListenWorkerTest : public ExternalClusterTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        const char *hostIp = "127.0.0.1";
        opts.numWorkers = 1;
        opts.numEtcd = 1;
        int port = GetFreePort();
        opts.workerConfigs.emplace_back(hostIp, port);
        workerHostPort_ = HostPort(hostIp, port);
        opts.workerGflagParams =
            " -shared_memory_size_mb=64 -v=1 -ipc_through_shared_memory=true -node_timeout_s=1 "
            "-heartbeat_interval_ms=500";

        datasystem::inject::Set("ListenWorker.CheckHeartbeat.interval", "call(400)");
        datasystem::inject::Set("ListenWorker.CheckHeartbeat.heartbeat_interval_ms", "call(400)");
        datasystem::inject::Set("ClientWorkerCommonApi.SendHeartbeat.timeoutMs", "call(500)");
    }

    HostPort workerHostPort_;
    int initTimeoutMs = 5000;
    std::string accessKey_ = "QTWAOYTTINDUT2QVKYUC";
    std::string secretKey_ = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
    std::unique_ptr<Signature> signature_ = std::make_unique<Signature>(accessKey_, secretKey_);
};

class ShmWorkerTest : public ListenWorkerTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        const char *hostIp = "127.0.0.1";
        opts.numWorkers = 1;
        opts.numEtcd = 1;
        int port = GetFreePort();
        opts.workerConfigs.emplace_back(hostIp, port);
        workerHostPort_ = HostPort(hostIp, port);
        opts.workerGflagParams =
            " -shared_memory_size_mb=64 -v=1 -ipc_through_shared_memory=true -node_timeout_s=1 "
            "-heartbeat_interval_ms=500 -oc_shm_transfer_threshold_kb=100";
    }
};

TEST_F(ShmWorkerTest, SHM_THRESHOLD)
{
    auto workerApi = std::make_shared<ClientWorkerRemoteCommonApi>(workerHostPort_, RpcCredential(),
                                                              HeartbeatType::UDS_HEARTBEAT, "", signature_.get());
    DS_ASSERT_OK(workerApi->Init(initTimeoutMs, initTimeoutMs));
    auto threshold = 100 * 1024u;
    DS_ASSERT_TRUE(workerApi->shmThreshold_, threshold);
}

TEST_F(ListenWorkerTest, TestUDSHeartbeatCallback)
{
    auto workerApi = std::make_shared<ClientWorkerRemoteCommonApi>(workerHostPort_, RpcCredential(),
                                                              HeartbeatType::UDS_HEARTBEAT, "", signature_.get());
    DS_ASSERT_OK(workerApi->Init(initTimeoutMs, initTimeoutMs));
    bool called = false;

    ListenWorker listenWorker(workerApi, workerApi->heartbeatType_);

    auto callback = [&called]() {
        LOG(INFO) << "callback run";
        called = true;
    };

    listenWorker.AddCallBackFunc(workerApi.get(), callback);
    DS_ASSERT_OK(listenWorker.StartListenWorker(workerApi->socketFd_));
    DS_ASSERT_OK(listenWorker.CheckWorkerAvailable());

    cluster_->ShutdownNodes(WORKER);
    DS_ASSERT_NOT_OK(listenWorker.CheckWorkerAvailable());
    ASSERT_TRUE(called);
}

TEST_F(ListenWorkerTest, LEVEL1_TestUDSHeartbeatReconnect)
{
    auto workerApi = std::make_shared<ClientWorkerRemoteCommonApi>(workerHostPort_, RpcCredential(),
                                                              HeartbeatType::UDS_HEARTBEAT, "", signature_.get());
    DS_ASSERT_OK(workerApi->Init(initTimeoutMs, initTimeoutMs));

    ListenWorker listenWorker(workerApi, workerApi->heartbeatType_);
    DS_ASSERT_OK(listenWorker.StartListenWorker(workerApi->socketFd_));
    DS_ASSERT_OK(listenWorker.CheckWorkerAvailable());

    cluster_->ShutdownNodes(WORKER);
    DS_ASSERT_NOT_OK(listenWorker.CheckWorkerAvailable());
    cluster_->StartWorkers();
    DS_ASSERT_OK(workerApi->Reconnect());
}

TEST_F(ListenWorkerTest, TestRPCHeartheat)
{
    auto workerApi = std::make_shared<ClientWorkerRemoteCommonApi>(workerHostPort_, RpcCredential(),
                                                             HeartbeatType::RPC_HEARTBEAT, "", signature_.get());
    DS_ASSERT_OK(workerApi->Init(initTimeoutMs, initTimeoutMs));
    bool called = false;

    ListenWorker listenWorker(workerApi, workerApi->heartbeatType_);
    auto callback = [&workerApi, &listenWorker, &called]() {
        LOG(INFO) << "Start to run callback";
        workerApi->Reconnect();
        listenWorker.SetWorkerAvailable(true);
        called = true;
        LOG(INFO) << "Run callback finish.";
    };

    DS_ASSERT_OK(listenWorker.StartListenWorker());
    listenWorker.AddCallBackFunc(workerApi.get(), callback);
    DS_ASSERT_OK(listenWorker.CheckWorkerAvailable());
    cluster_->ShutdownNodes(WORKER);
    sleep(2);  // Wait until the client detects that the worker is disconnected and performs callback.
    DS_ASSERT_NOT_OK(listenWorker.CheckWorkerAvailable());
    DS_ASSERT_OK(cluster_->StartWorkers());
    sleep(1);  // Wait until the client is successfully reconnected.
    ASSERT_TRUE(called);
    DS_ASSERT_OK(listenWorker.CheckWorkerAvailable());
}

TEST_F(ListenWorkerTest, LEVEL1_TestRPCHeartheatReconnect)
{
    auto workerApi = std::make_shared<ClientWorkerRemoteCommonApi>(workerHostPort_, RpcCredential(),
                                                             HeartbeatType::RPC_HEARTBEAT, "", signature_.get());
    DS_ASSERT_OK(workerApi->Init(initTimeoutMs, initTimeoutMs));

    cluster_->ShutdownNodes(WORKER);
    sleep(1);  // Wait until the client detects that the worker is disconnected and performs callback.
    DS_ASSERT_OK(cluster_->StartWorkers());
    DS_ASSERT_OK(workerApi->Reconnect());
}

TEST_F(ListenWorkerTest, TestNoHeartbeatTransformRPCHeartbeat)
{
    auto workerApi = std::make_shared<ClientWorkerRemoteCommonApi>(workerHostPort_, RpcCredential(),
                                                             HeartbeatType::NO_HEARTBEAT, "", signature_.get());
    DS_ASSERT_OK(workerApi->Init(initTimeoutMs, initTimeoutMs));

    ASSERT_TRUE(workerApi->heartbeatType_ == HeartbeatType::RPC_HEARTBEAT);
    ListenWorker listenWorker(workerApi, workerApi->heartbeatType_);
    DS_ASSERT_OK(listenWorker.StartListenWorker());
    DS_ASSERT_OK(listenWorker.CheckWorkerAvailable());

    cluster_->ShutdownNodes(WORKER);
    sleep(2);  // Wait until the client detects that the worker is disconnected and performs callback.
    DS_ASSERT_NOT_OK(listenWorker.CheckWorkerAvailable());
}

TEST_F(ListenWorkerTest, TestRemoveCallback)
{
    auto workerApi = std::make_shared<ClientWorkerRemoteCommonApi>(workerHostPort_, RpcCredential(),
                                                             HeartbeatType::UDS_HEARTBEAT, "", signature_.get());
    DS_ASSERT_OK(workerApi->Init(initTimeoutMs, initTimeoutMs));
    ListenWorker listenWorker(workerApi, workerApi->heartbeatType_);

    bool called1 = false;
    bool called2 = false;
    auto callback1 = [&called1]() {
        called1 = true;
        LOG(INFO) << "Run callback1";
    };
    auto callback2 = [&called2]() { called2 = true; };

    auto point1 = std::make_shared<ClientWorkerRemoteCommonApi>(workerHostPort_, RpcCredential(),
                                                          HeartbeatType::UDS_HEARTBEAT, "", signature_.get());
    auto point2 = std::make_shared<ClientWorkerRemoteCommonApi>(workerHostPort_, RpcCredential(),
                                                          HeartbeatType::UDS_HEARTBEAT, "", signature_.get());
    listenWorker.AddCallBackFunc(point1.get(), callback1);
    listenWorker.AddCallBackFunc(point2.get(), callback2);
    listenWorker.StartListenWorker(workerApi->socketFd_);
    listenWorker.RemoveCallBackFunc(point2.get());

    cluster_->ShutdownNodes(WORKER);
    usleep(100'0000);  // Wait for the callback to be executed.
    ASSERT_TRUE(called1);
    ASSERT_FALSE(called2);
}

TEST_F(ListenWorkerTest, DISABLED_TestClientDetectWorkerCrashTwiceWithUDS)
{
    auto workerApi = std::make_shared<ClientWorkerRemoteCommonApi>(workerHostPort_, RpcCredential(),
                                                             HeartbeatType::UDS_HEARTBEAT, "", signature_.get());
    DS_ASSERT_OK(workerApi->Init(initTimeoutMs, initTimeoutMs));

    ASSERT_TRUE(workerApi->heartbeatType_ == HeartbeatType::UDS_HEARTBEAT);
    ListenWorker listenWorker(workerApi, workerApi->heartbeatType_);

    bool called = false, finish = false;
    auto callback = [&workerApi, &listenWorker, &called, &finish]() {
        if (finish) {
            return;
        }
        LOG(INFO) << "Start to run callback";
        called = true;
        Status status;
        do {
            status = workerApi->Reconnect();
            if (status.IsOk()) {
                listenWorker.UpdateSocketFd(workerApi->socketFd_);
                listenWorker.SetWorkerAvailable(true);
            }
        } while (!finish && status.IsError());
        LOG(INFO) << "Finish to run callback";
    };

    listenWorker.AddCallBackFunc(workerApi.get(), callback);
    listenWorker.StartListenWorker(workerApi->socketFd_);

    int workerIndex = 0;
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, workerIndex, "Worker.Heartbeat.startToHeartbeat",
                                           "abort(Mock worker crash)"));

    bool clientRemoved, workerReboot, isWorkerVoluntaryScaleDown;
    int64_t remainTime = 0;
    std::vector<int64_t> releasedWorkerFds;
    std::vector<int64_t> expiredWorkerFds;
    // The worker crashes for the first time.
    DS_ASSERT_NOT_OK(workerApi->SendHeartbeat(workerReboot, clientRemoved, remainTime, isWorkerVoluntaryScaleDown,
                                              releasedWorkerFds, expiredWorkerFds));
    sleep(2);  // Wait until the worker receive the message
    ASSERT_TRUE(called);
    DS_ASSERT_NOT_OK(listenWorker.CheckWorkerAvailable());

    LOG(INFO) << "Start the worker for the first time.";
    DS_ASSERT_OK(cluster_->StartWorkers());
    sleep(2);  // Wait until the callback is executed successfully.
    DS_ASSERT_OK(listenWorker.CheckWorkerAvailable());

    // The worker crashes for the second time.
    cluster_->ShutdownNodes(WORKER);
    sleep(1);  // Wait until the client detects that the worker is disconnected.
    DS_ASSERT_NOT_OK(listenWorker.CheckWorkerAvailable());

    LOG(INFO) << "Start the worker for the second time.";
    DS_ASSERT_OK(cluster_->StartWorkers());
    sleep(2);  // Wait until the callback is executed successfully.
    DS_ASSERT_OK(listenWorker.CheckWorkerAvailable());
    finish = true;
}

class ListenWorkerWithUDSTest : public ExternalClusterTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        const char *hostIp = "127.0.0.1";
        opts.numWorkers = 1;
        opts.numEtcd = 1;
        int port = GetFreePort();
        opts.workerConfigs.emplace_back(hostIp, port);
        workerHostPort_ = HostPort(hostIp, port);
        opts.workerGflagParams = " -shared_memory_size_mb=64 -v=1 -ipc_through_shared_memory=false -node_timeout_s=5";
    }

    HostPort workerHostPort_;
    std::string accessKey_ = "QTWAOYTTINDUT2QVKYUC";
    std::string secretKey_ = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
    std::unique_ptr<Signature> signature_ = std::make_unique<Signature>(accessKey_, secretKey_);
};

TEST_F(ListenWorkerWithUDSTest, TestNoHeartbeat)
{
    auto workerApi = std::make_shared<ClientWorkerRemoteCommonApi>(workerHostPort_, RpcCredential(),
                                                             HeartbeatType::NO_HEARTBEAT, "", signature_.get());
    int timeoutMs = 5000;
    DS_ASSERT_OK(workerApi->Init(timeoutMs, timeoutMs));

    ListenWorker listenWorker(workerApi, workerApi->heartbeatType_);
    DS_ASSERT_OK(listenWorker.StartListenWorker());

    DS_ASSERT_OK(listenWorker.CheckWorkerAvailable());
    cluster_->ShutdownNodes(WORKER);
    sleep(1);  // Simulate a situation in which the client detects that the worker is disconnected.
    DS_ASSERT_OK(listenWorker.CheckWorkerAvailable());
}

TEST_F(ListenWorkerWithUDSTest, TestHeartbeatTimeout)
{
    auto workerApi = std::make_shared<ClientWorkerRemoteCommonApi>(workerHostPort_, RpcCredential(),
                                                             HeartbeatType::RPC_HEARTBEAT, "", signature_.get());
    int timeoutMs = 5000;
    DS_ASSERT_OK(workerApi->Init(timeoutMs, timeoutMs));

    ListenWorker listenWorker(workerApi, workerApi->heartbeatType_);
    DS_ASSERT_OK(listenWorker.StartListenWorker());

    DS_ASSERT_OK(listenWorker.CheckWorkerAvailable());
    cluster_->ShutdownNodes(WORKER);
    std::this_thread::sleep_for(
        std::chrono::seconds(10));  // Simulate a situation in which the client detects that the worker is disconnected.
    DS_ASSERT_NOT_OK(listenWorker.CheckWorkerAvailable());
}

class ListenWorkerSwitchTest : public ExternalClusterTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        const char *hostIp = "127.0.0.1";
        opts.numWorkers = 2;  // worker num is 2
        opts.numEtcd = 1;
        int port = GetFreePort();
        int port1 = GetFreePort();
        opts.workerConfigs.emplace_back(hostIp, port);
        opts.workerConfigs.emplace_back(hostIp, port1);
        workerHostPort_ = HostPort(hostIp, port);
        workerHostPort1_ = HostPort(hostIp, port1);
        opts.workerGflagParams = " -shared_memory_size_mb=64 -v=1 -ipc_through_shared_memory=false -node_timeout_s=5";
    }

    HostPort workerHostPort_;
    HostPort workerHostPort1_;
    std::string accessKey_ = "QTWAOYTTINDUT2QVKYUC";
    std::string secretKey_ = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
    std::unique_ptr<Signature> signature_ = std::make_unique<Signature>(accessKey_, secretKey_);
};

TEST_F(ListenWorkerSwitchTest, SwitchTest)
{
    auto workerApi = std::make_shared<ClientWorkerRemoteCommonApi>(workerHostPort_, RpcCredential(),
                                                             HeartbeatType::RPC_HEARTBEAT, "", signature_.get());
    int timeoutMs = 5000;
    DS_ASSERT_OK(workerApi->Init(timeoutMs, timeoutMs));
    auto asyncSwitchWorkerPool = std::make_shared<ThreadPool>(0, 1);
    datasystem::inject::Set("TrySwitchBackToLocalWorker", "sleep(10000)");
    std::unique_ptr<ListenWorker> listenWorker =
        std::make_unique<ListenWorker>(workerApi, workerApi->heartbeatType_, 0, asyncSwitchWorkerPool.get());
    DS_ASSERT_OK(listenWorker->StartListenWorker());
    DS_ASSERT_OK(listenWorker->CheckWorkerAvailable());
    datasystem::inject::Set("IsIdle", "return()");
    auto workerApi1 = std::make_shared<ClientWorkerRemoteCommonApi>(workerHostPort1_, RpcCredential(),
                                                              HeartbeatType::RPC_HEARTBEAT, "", signature_.get());
    sleep(8);  // wait for 8 s
    listenWorker =
        std::make_unique<ListenWorker>(workerApi1, workerApi1->heartbeatType_, 0, asyncSwitchWorkerPool.get());
}

TEST_F(ListenWorkerSwitchTest, TestStandbyConnectionShutdownAndRestartConcurrently)
{
    auto workerApi = std::make_shared<ClientWorkerRemoteCommonApi>(workerHostPort_, RpcCredential(),
                                                             HeartbeatType::RPC_HEARTBEAT, "", signature_.get());
    int timeoutMs = 5000;
    DS_ASSERT_OK(workerApi->Init(timeoutMs, timeoutMs));
    auto asyncSwitchWorkerPool = std::make_shared<ThreadPool>(0, 1);
    auto listenWorker =
        std::make_shared<ListenWorker>(workerApi, workerApi->heartbeatType_, 0, asyncSwitchWorkerPool.get());
    std::atomic<int> count = 0;
    listenWorker->AddCallBackFunc(this, [&count]() {
        count++;
    });
    datasystem::inject::Set("IsIdle", "return()");
    datasystem::inject::Set("IsReconnectable", "call()");
    datasystem::inject::Set("listen_worker.reboot", "call()");
    datasystem::inject::Set("listen_worker.StartListenWorker", "return()");
    datasystem::inject::Set("listen_worker.TryShutdownStandbyConnection", "1*sleep(2000)");
    DS_ASSERT_OK(listenWorker->StartListenWorker());
    DS_ASSERT_OK(listenWorker->CheckWorkerAvailable());
    sleep(2);  // wait for 2 s
    ASSERT_EQ(count, 0);
    ASSERT_EQ(asyncSwitchWorkerPool->GetWaitingTasksNum(), 0ul);
}

class ListenWorkerRediscoverTest : public ExternalClusterTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        const char *hostIp = "127.0.0.1";
        opts.numWorkers = 2;
        opts.numEtcd = 1;
        int port = GetFreePort();
        int port1 = GetFreePort();
        opts.workerConfigs.emplace_back(hostIp, port);
        opts.workerConfigs.emplace_back(hostIp, port1);
        workerHostPort_ = HostPort(hostIp, port);
        workerHostPort1_ = HostPort(hostIp, port1);
        opts.workerGflagParams = " -shared_memory_size_mb=64 -v=1 -node_timeout_s=1 -heartbeat_interval_ms=500 ";

        datasystem::inject::Set("ListenWorker.CheckHeartbeat.interval", "call(400)");
        datasystem::inject::Set("ListenWorker.CheckHeartbeat.heartbeat_interval_ms", "call(400)");
        datasystem::inject::Set("ClientWorkerCommonApi.SendHeartbeat.timeoutMs", "call(500)");
    }

    HostPort workerHostPort_;
    HostPort workerHostPort1_;
    std::string accessKey_ = "QTWAOYTTINDUT2QVKYUC";
    std::string secretKey_ = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
    std::unique_ptr<Signature> signature_ = std::make_unique<Signature>(accessKey_, secretKey_);
};

TEST_F(ListenWorkerRediscoverTest, TestRediscoverSkippedWhenNotSwitched)
{
    auto workerApi = std::make_shared<ClientWorkerRemoteCommonApi>(workerHostPort_, RpcCredential(),
                                                             HeartbeatType::RPC_HEARTBEAT, "", signature_.get());
    int timeoutMs = 5000;
    DS_ASSERT_OK(workerApi->Init(timeoutMs, timeoutMs));
    auto asyncSwitchWorkerPool = std::make_shared<ThreadPool>(0, 1);
    auto listenWorker =
        std::make_shared<ListenWorker>(workerApi, workerApi->heartbeatType_, 0, asyncSwitchWorkerPool.get());
    listenWorker->SetIsLocalWorker(true);

    std::atomic<int> rediscoverCount{ 0 };
    listenWorker->SetRediscoverHandle([&rediscoverCount]() {
        rediscoverCount++;
        return false;
    });

    DS_ASSERT_OK(listenWorker->StartListenWorker());
    DS_ASSERT_OK(listenWorker->CheckWorkerAvailable());

    cluster_->ShutdownNode(WORKER, 0);
    sleep(3);

    ASSERT_EQ(rediscoverCount.load(), 0) << "Rediscover handle should NOT be called before switch";
}

TEST_F(ListenWorkerRediscoverTest, TestRediscoverSuccessClearsSwitched)
{
    auto workerApi = std::make_shared<ClientWorkerRemoteCommonApi>(workerHostPort_, RpcCredential(),
                                                             HeartbeatType::RPC_HEARTBEAT, "", signature_.get());
    int timeoutMs = 5000;
    DS_ASSERT_OK(workerApi->Init(timeoutMs, timeoutMs));
    auto asyncSwitchWorkerPool = std::make_shared<ThreadPool>(0, 1);
    auto listenWorker =
        std::make_shared<ListenWorker>(workerApi, workerApi->heartbeatType_, 0, asyncSwitchWorkerPool.get());
    listenWorker->SetIsLocalWorker(true);

    std::atomic<bool> rediscoverCalled{ false };
    listenWorker->SetRediscoverHandle([&rediscoverCalled]() {
        rediscoverCalled = true;
        return true;  // Simulate: local worker found at new IP, reconnection succeeded
    });

    DS_ASSERT_OK(listenWorker->StartListenWorker());
    DS_ASSERT_OK(listenWorker->CheckWorkerAvailable());
    listenWorker->SetSwitched();

    cluster_->ShutdownNode(WORKER, 0);
    sleep(3);

    ASSERT_TRUE(rediscoverCalled.load()) << "Rediscover handle should have been called";
    // isSwitched_ is cleared because rediscoverHandle_ returned true.
    // This means TrySwitchBackToLocalWorker() will no longer fire on subsequent heartbeat successes,
    // which is the correct behavior after rediscovery has already reconnected to the new IP.
}

TEST_F(ListenWorkerRediscoverTest, TestRediscoverNotCalledForStandbyWorker)
{
    auto workerApi = std::make_shared<ClientWorkerRemoteCommonApi>(workerHostPort1_, RpcCredential(),
                                                             HeartbeatType::RPC_HEARTBEAT, "", signature_.get());
    int timeoutMs = 5000;
    DS_ASSERT_OK(workerApi->Init(timeoutMs, timeoutMs));
    auto asyncSwitchWorkerPool = std::make_shared<ThreadPool>(0, 1);
    auto listenWorker =
        std::make_shared<ListenWorker>(workerApi, workerApi->heartbeatType_, 1, asyncSwitchWorkerPool.get());
    listenWorker->SetIsLocalWorker(false);  // This is a standby worker listener.

    std::atomic<int> rediscoverCount{ 0 };
    listenWorker->SetRediscoverHandle([&rediscoverCount]() {
        rediscoverCount++;
        return false;
    });

    DS_ASSERT_OK(listenWorker->StartListenWorker());
    DS_ASSERT_OK(listenWorker->CheckWorkerAvailable());
    listenWorker->SetSwitched();

    cluster_->ShutdownNode(WORKER, 1);
    sleep(3);

    ASSERT_EQ(rediscoverCount.load(), 0) << "Rediscover should NOT be called for standby workers";
}
}  // namespace st
}  // namespace datasystem
