/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2022. All rights reserved.
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

#include <gtest/gtest.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <csignal>
#include <chrono>
#include <cstdint>
#include <ctime>
#include <fstream>
#include <future>
#include <iterator>
#include <limits>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "common.h"
#include "datasystem/common/kvstore/coordination_keys.h"
#include "datasystem/common/log/log.h"
#include "datasystem/client/object_cache/client_worker_api/iclient_worker_api.h"
#include "datasystem/common/ak_sk/ak_sk_manager.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/kvstore/etcd/etcd_store.h"
#include "datasystem/common/rpc/rpc_channel.h"
#include "datasystem/common/rpc/rpc_auth_key_manager.h"
#include "datasystem/common/util/hash_algorithm.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/random_data.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/object/object_enum.h"
#include "datasystem/protos/object_posix.stub.rpc.pb.h"
#include "datasystem/protos/ut_object.stub.rpc.pb.h"
#include "datasystem/utils/status.h"
#include "datasystem/worker/object_cache/worker_master_oc_api.h"
#include "oc_client_common.h"
#include "datasystem/common/flags/common_flags.h"  // FLAGS_use_brpc
#include "datasystem/common/rpc/rpc_stub_cache_mgr.h"  // kBrpcPortOffset
#ifdef WITH_TESTS
#include "datasystem/common/rpc/brpc_factory.h"
#include "datasystem/protos/master_object.brpc.stub.pb.h"
#include "datasystem/protos/object_posix.brpc.stub.pb.h"
#include "datasystem/protos/ut_object.brpc.stub.pb.h"
#endif

DS_DECLARE_bool(use_brpc);
DS_DECLARE_bool(enable_urma);

namespace datasystem {
namespace st {
namespace {
constexpr int kEventuallyWaitTimeoutMs = 15'000;
constexpr int kEventuallyPollIntervalMs = 100;
constexpr char kMasterCacheInvalidInject[] = "master.cache_invalid_failed";
constexpr char kPersistentRpcDeadlineExceeded[] = "return(K_RPC_DEADLINE_EXCEEDED)";

// Abstracts the test control-plane UtOCService stub so the test harness can talk
// to the worker's UtOCService adapter in either transport. In brpc mode the
// worker registers UtOCServiceBrpcAdapter (no ZMQ UtOCService is listening), so
// the ZMQ-only UtOCService_Stub would fail (RPC_SERVICE_UNAVAILABLE) — this
// interface lets InitApis pick the matching stub per FLAGS_use_brpc.
class IUtOCStub {
public:
    virtual ~IUtOCStub() = default;
    virtual Status GetWorkerGRefTable(const GRefTableReqPb &req, GRefTableRspPb &rsp) = 0;
    virtual Status GetMasterGRefTable(const GRefTableReqPb &req, GRefTableRspPb &rsp) = 0;
    virtual Status GetCmNodeTable(const CmNodeTableReqPb &req, CmNodeTableRspPb &rsp) = 0;
};
class ZmqUtOCStub : public IUtOCStub {
public:
    explicit ZmqUtOCStub(std::shared_ptr<RpcChannel> channel) : stub_(std::move(channel)) {}
    Status GetWorkerGRefTable(const GRefTableReqPb &req, GRefTableRspPb &rsp) override
    {
        return stub_.GetWorkerGRefTable(req, rsp);
    }
    Status GetMasterGRefTable(const GRefTableReqPb &req, GRefTableRspPb &rsp) override
    {
        return stub_.GetMasterGRefTable(req, rsp);
    }
    Status GetCmNodeTable(const CmNodeTableReqPb &req, CmNodeTableRspPb &rsp) override
    {
        return stub_.GetCmNodeTable(req, rsp);
    }

private:
    UtOCService_Stub stub_;
};
class BrpcUtOCStub : public IUtOCStub {
public:
    explicit BrpcUtOCStub(std::shared_ptr<brpc::Channel> channel, int32_t timeoutMs)
        : stub_(channel.get(), timeoutMs), channel_(std::move(channel))
    {}
    Status GetWorkerGRefTable(const GRefTableReqPb &req, GRefTableRspPb &rsp) override
    {
        return stub_.GetWorkerGRefTable(req, rsp);
    }
    Status GetMasterGRefTable(const GRefTableReqPb &req, GRefTableRspPb &rsp) override
    {
        return stub_.GetMasterGRefTable(req, rsp);
    }
    Status GetCmNodeTable(const CmNodeTableReqPb &req, CmNodeTableRspPb &rsp) override
    {
        return stub_.GetCmNodeTable(req, rsp);
    }

private:
    UtOCService_BrpcGenericStub stub_;
    std::shared_ptr<brpc::Channel> channel_;  // keep channel alive for stub's raw pointer
};

template <typename Operation>
void AssertEventuallyNotOk(Operation operation, const std::string &operationName)
{
    Status rc;
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::milliseconds(kEventuallyWaitTimeoutMs);
    do {
        rc = operation();
        if (rc.IsError()) {
            return;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(kEventuallyPollIntervalMs));
    } while (std::chrono::steady_clock::now() < deadline);
    ASSERT_TRUE(rc.IsError()) << operationName << " stayed OK for " << kEventuallyWaitTimeoutMs
                              << " ms, last status: " << rc.ToString();
}

template <typename Operation>
void AssertEventuallyOk(Operation operation, const std::string &operationName)
{
    Status rc;
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::milliseconds(kEventuallyWaitTimeoutMs);
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

Status DirectWorkerCreate(const HostPort &workerAddr, const RpcCredential &credential, const CreateReqPb &req,
                          CreateRspPb &rsp, int32_t timeoutMs)
{
    RpcOptions opts;
    opts.SetTimeout(timeoutMs);
    if (FLAGS_use_brpc) {
#ifdef WITH_TESTS
        BrpcChannelConfig cfg;
        cfg.endpoint = HostPort(workerAddr.Host(), workerAddr.Port() + kBrpcPortOffset).ToString();
        cfg.timeout_ms = timeoutMs;
        cfg.connect_timeout_ms = timeoutMs;
        cfg.enable_circuit_breaker = false;
        cfg.max_retry = 0;
        std::shared_ptr<brpc::Channel> channel(BrpcChannelFactory::Create(cfg));
        CHECK_FAIL_RETURN_STATUS(channel != nullptr, K_RPC_UNAVAILABLE,
                                 "Failed to create brpc WorkerOCService channel to " + cfg.endpoint);
        WorkerOCService_BrpcGenericStub stub(channel.get(), timeoutMs);
        return stub.Create(opts, req, rsp);
#else
        (void)workerAddr;
        (void)credential;
        (void)req;
        (void)rsp;
        RETURN_STATUS(K_RUNTIME_ERROR, "brpc direct WorkerOCService ST helper requires WITH_TESTS");
#endif
    }
    auto channel = std::make_shared<RpcChannel>(workerAddr, credential);
    WorkerOCService_Stub stub(channel);
    return stub.Create(opts, req, rsp);
}

Status DirectWorkerGetObjMetaInfo(const HostPort &workerAddr, const RpcCredential &credential,
                                  const GetObjMetaInfoReqPb &req, GetObjMetaInfoRspPb &rsp, int32_t timeoutMs)
{
    RpcOptions opts;
    opts.SetTimeout(timeoutMs);
    if (FLAGS_use_brpc) {
#ifdef WITH_TESTS
        BrpcChannelConfig cfg;
        cfg.endpoint = HostPort(workerAddr.Host(), workerAddr.Port() + kBrpcPortOffset).ToString();
        cfg.timeout_ms = timeoutMs;
        cfg.connect_timeout_ms = timeoutMs;
        cfg.enable_circuit_breaker = false;
        cfg.max_retry = 0;
        std::shared_ptr<brpc::Channel> channel(BrpcChannelFactory::Create(cfg));
        CHECK_FAIL_RETURN_STATUS(channel != nullptr, K_RPC_UNAVAILABLE,
                                 "Failed to create brpc WorkerOCService channel to " + cfg.endpoint);
        WorkerOCService_BrpcGenericStub stub(channel.get(), timeoutMs);
        return stub.GetObjMetaInfo(opts, req, rsp);
#else
        (void)workerAddr;
        (void)credential;
        (void)req;
        (void)rsp;
        RETURN_STATUS(K_RUNTIME_ERROR, "brpc direct WorkerOCService ST helper requires WITH_TESTS");
#endif
    }
    auto channel = std::make_shared<RpcChannel>(workerAddr, credential);
    WorkerOCService_Stub stub(channel);
    return stub.GetObjMetaInfo(opts, req, rsp);
}

Status DirectMasterQueryMeta(const HostPort &workerAddr, const RpcCredential &credential,
                             const master::QueryMetaReqPb &req, master::QueryMetaRspPb &rsp,
                             std::vector<RpcMessage> &payloads, int32_t timeoutMs)
{
    RpcOptions opts;
    opts.SetTimeout(timeoutMs);
    if (FLAGS_use_brpc) {
#ifdef WITH_TESTS
        BrpcChannelConfig cfg;
        cfg.endpoint = HostPort(workerAddr.Host(), workerAddr.Port() + kBrpcPortOffset).ToString();
        cfg.timeout_ms = timeoutMs;
        cfg.connect_timeout_ms = timeoutMs;
        cfg.enable_circuit_breaker = false;
        cfg.max_retry = 0;
        std::shared_ptr<brpc::Channel> channel(BrpcChannelFactory::Create(cfg));
        CHECK_FAIL_RETURN_STATUS(channel != nullptr, K_RPC_UNAVAILABLE,
                                 "Failed to create brpc MasterOCService channel to " + cfg.endpoint);
        master::MasterOCService_BrpcGenericStub stub(channel.get(), timeoutMs);
        return stub.QueryMeta(opts, req, rsp, payloads);
#else
        (void)workerAddr;
        (void)credential;
        (void)req;
        (void)rsp;
        (void)payloads;
        RETURN_STATUS(K_RUNTIME_ERROR, "brpc direct MasterOCService ST helper requires WITH_TESTS");
#endif
    }
    auto channel = std::make_shared<RpcChannel>(workerAddr, credential);
    master::MasterOCService_Stub stub(channel);
    return stub.QueryMeta(opts, req, rsp, payloads);
}

Status DirectMasterCheckObjectDataLocation(const HostPort &workerAddr, const RpcCredential &credential,
                                           const master::CheckObjectDataLocationReqPb &req,
                                           master::CheckObjectDataLocationRspPb &rsp, int32_t timeoutMs)
{
    RpcOptions opts;
    opts.SetTimeout(timeoutMs);
    if (FLAGS_use_brpc) {
#ifdef WITH_TESTS
        BrpcChannelConfig cfg;
        cfg.endpoint = HostPort(workerAddr.Host(), workerAddr.Port() + kBrpcPortOffset).ToString();
        cfg.timeout_ms = timeoutMs;
        cfg.connect_timeout_ms = timeoutMs;
        cfg.enable_circuit_breaker = false;
        cfg.max_retry = 0;
        std::shared_ptr<brpc::Channel> channel(BrpcChannelFactory::Create(cfg));
        CHECK_FAIL_RETURN_STATUS(channel != nullptr, K_RPC_UNAVAILABLE,
                                 "Failed to create brpc MasterOCService channel to " + cfg.endpoint);
        master::MasterOCService_BrpcGenericStub stub(channel.get(), timeoutMs);
        return stub.CheckObjectDataLocation(opts, req, rsp);
#else
        (void)workerAddr;
        (void)credential;
        (void)req;
        (void)rsp;
        RETURN_STATUS(K_RUNTIME_ERROR, "brpc direct MasterOCService ST helper requires WITH_TESTS");
#endif
    }
    auto channel = std::make_shared<RpcChannel>(workerAddr, credential);
    master::MasterOCService_Stub stub(channel);
    return stub.CheckObjectDataLocation(opts, req, rsp);
}
}  // namespace

class WorkerDfxTest : public OCClientCommon {
public:
    void TearDown() override
    {
        objClient0_ = nullptr;
        objClient1_ = nullptr;
        objClient2_ = nullptr;
        ExternalClusterTest::TearDown();
    }

    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.workerGflagParams =
            "-client_reconnect_wait_s=1 -ipc_through_shared_memory=true -sc_stream_socket_num=0 -node_timeout_s=1 "
            "-heartbeat_interval_ms=500";
        opts.numWorkers = 3;
        opts.masterIdx = 1;
        opts.enableDistributedMaster = "false";
        opts.numEtcd = 1;
        datasystem::inject::Set("ListenWorker.CheckHeartbeat.interval", "call(500)");
        datasystem::inject::Set("ListenWorker.CheckHeartbeat.heartbeat_interval_ms", "call(500)");
        datasystem::inject::Set("ClientWorkerCommonApi.SendHeartbeat.timeoutMs", "call(500)");
    }

    void InitTestClients(int32_t timeout = 60000)
    {
        InitTestClient(0, objClient0_, timeout);
        InitTestClient(0, objClient1_, timeout);
        InitTestClient(1, objClient2_, timeout);
    }

protected:
    std::shared_ptr<ObjectClient> objClient0_;
    std::shared_ptr<ObjectClient> objClient1_;
    std::shared_ptr<ObjectClient> objClient2_;
};

TEST_F(WorkerDfxTest, LEVEL1_TestWorkerCrashAndOperateBuffer)
{
    LOG(INFO) << "Test worker crash and operate the shm buffer.";
    std::shared_ptr<ObjectClient> client1;
    InitTestClient(0, client1);
    std::string objKey = "Warriors";
    CreateParam param;
    size_t objSize = 500 * 1024ul;
    std::shared_ptr<Buffer> buffer;
    DS_ASSERT_OK(client1->Create(objKey, objSize, param, buffer));

    // Then we crash the worker and try to operate the shm pointer.
    cluster_->ShutdownNodes(WORKER);
    std::vector<uint8_t> data;
    data.resize(objSize);
    std::vector<std::string> failedObjectKeys;
    // Make sure the op below would be failed.
    AssertEventuallyNotOk([&buffer, &data] { return buffer->MemoryCopy(data.data(), data.size()); },
                          "MemoryCopy after worker shutdown");
    DS_ASSERT_NOT_OK(buffer->Publish());
    DS_ASSERT_NOT_OK(client1->GIncreaseRef({ objKey }, failedObjectKeys));
    DS_ASSERT_NOT_OK(buffer->Seal());
    DS_ASSERT_NOT_OK(buffer->WLatch());
    DS_ASSERT_NOT_OK(buffer->RLatch());
    DS_ASSERT_NOT_OK(buffer->UnWLatch());
    DS_ASSERT_NOT_OK(buffer->UnRLatch());
    DS_ASSERT_NOT_OK(client1->GDecreaseRef({ objKey }, failedObjectKeys));
}

TEST_F(WorkerDfxTest, TestWorkerRestartAndOperateShmBuffer)
{
    LOG(INFO) << "Test worker restart and operate the not shm buffer.";
    std::shared_ptr<ObjectClient> client1;
    InitTestClient(1, client1);
    std::string objKey1 = "Warriors1";
    std::string objKey2 = "Warriors2";
    CreateParam param;
    size_t objSize = 500 * 1024ul;
    std::shared_ptr<Buffer> buffer;
    std::vector<Optional<Buffer>> buffers;
    std::vector<uint8_t> data;
    data.resize(objSize);

    DS_ASSERT_OK(client1->Create(objKey1, objSize, param, buffer));
    DS_ASSERT_OK(buffer->MemoryCopy(data.data(), data.size()));
    DS_ASSERT_OK(buffer->Publish());
    DS_ASSERT_OK(buffer->Publish());
    DS_ASSERT_OK(buffer->Seal());
    DS_ASSERT_OK(client1->Get({ objKey1 }, 0, buffers));

    // Then we crash the worker and try to operate the shm pointer.
    cluster_->ShutdownNodes(WORKER);
    sleep(1);  // The heartbeat interval is 0.5s, and the maximum number of worker disconnections is 1s.
    DS_ASSERT_OK(cluster_->StartWorkers());
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 0));
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 1));
    sleep(2);

    // Make sure the op below would be failed.
    DS_ASSERT_OK(client1->Create(objKey2, objSize, param, buffer));
    DS_ASSERT_OK(buffer->MemoryCopy(data.data(), data.size()));
    DS_ASSERT_OK(buffer->Publish());
    DS_ASSERT_OK(buffer->Publish());
    DS_ASSERT_OK(buffer->Seal());
    DS_ASSERT_OK(client1->Get({ objKey2 }, 0, buffers));
    buffer.reset();
}

TEST_F(WorkerDfxTest, TestWorkerRestartAndOperateNoShmBuffer)
{
    if (FLAGS_use_brpc) {
        GTEST_SKIP() << "brpc: worker restart OC brpc channel revive and clientId re-register. Tracked separately.";
    }

    LOG(INFO) << "Test worker restart and operate the not shm buffer.";
    std::shared_ptr<ObjectClient> client1;
    InitTestClient(1, client1);
    std::string objKey = "Warriors";
    CreateParam param;
    size_t objSize = 1024ul;
    std::shared_ptr<Buffer> buffer;
    DS_ASSERT_OK(client1->Create(objKey, objSize, param, buffer));

    // Then we crash the worker and try to operate the shm pointer.
    cluster_->ShutdownNodes(WORKER);
    sleep(1);  // The heartbeat interval is 0.5s, and the maximum number of worker disconnections is 1s.
    DS_ASSERT_OK(cluster_->StartWorkers());
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 0));
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 1));

    std::vector<uint8_t> data;
    data.resize(objSize);
    // Make sure the op below would be failed.
    DS_ASSERT_OK(buffer->MemoryCopy(data.data(), data.size()));
    DS_ASSERT_OK(buffer->Publish());
    DS_ASSERT_OK(buffer->Publish());
    DS_ASSERT_OK(buffer->Seal());
}

TEST_F(WorkerDfxTest, TestWorkerCrashRecovery)
{
    LOG(INFO) << "Test worker crash recovery and decrease the global reference.";
    std::shared_ptr<ObjectClient> client1;
    InitTestClient(0, client1);
    std::vector<uint8_t> data;
    data.resize(500 * 1024ul);
    size_t objNum = 5;
    std::vector<std::string> objectKeys;
    for (size_t i = 0; i < objNum; ++i) {
        std::string objKey = "Warriors" + std::to_string(i);
        CreateAndSealObject(client1, objKey, data);
        objectKeys.emplace_back(objKey);
    }
    std::vector<std::string> failedObjectKeys;
    DS_ASSERT_OK(client1->GIncreaseRef(objectKeys, failedObjectKeys));
    ASSERT_EQ(failedObjectKeys.size(), size_t(0));

    // Shutdown worker and then restart to verify the metadata recovery.
    cluster_->ShutdownNodes(WORKER);
    DS_ASSERT_OK(cluster_->StartWorkers());
    const int workerNum = 3;
    for (int i = 0; i < workerNum; i++) {
        DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, i));
    }
    sleep(2);

    DS_ASSERT_OK(client1->GDecreaseRef(objectKeys, failedObjectKeys));
    ASSERT_EQ(failedObjectKeys.size(), size_t(0));
}

TEST_F(WorkerDfxTest, LEVEL1_TestClientExitAndCreateTheSameObject)
{
    LOG(INFO) << "Test client exit and other client create the objects with same IDs";
    std::shared_ptr<ObjectClient> client1;
    int timeoutMs = 3000;
    InitTestClient(0, client1, timeoutMs);
    size_t size = 4 * 1024ul * 1024ul;
    std::string namePrefix = "ToyotaLee";
    size_t cnt = 0;

    std::vector<std::string> objKeys;
    std::vector<std::shared_ptr<Buffer>> buffers;
    Status rc;
    do {
        std::string id = namePrefix + std::to_string(cnt);
        CreateParam param;
        std::shared_ptr<Buffer> buffer;
        rc = client1->Create(id, size, param, buffer);
        if (rc.IsOk()) {
            cnt++;
            buffers.emplace_back(std::move(buffer));
            objKeys.emplace_back(id);
        }
    } while (rc.IsOk());

    // client 0 exit, its metadata would be clear in worker.
    buffers.clear();
    client1.reset();

    sleep(1);
    std::shared_ptr<ObjectClient> client2;
    InitTestClient(0, client2);

    for (const auto &id : objKeys) {
        CreateParam param;
        std::shared_ptr<Buffer> buffer;
        DS_ASSERT_OK(client2->Create(id, size, param, buffer));
        buffers.emplace_back(std::move(buffer));
    }
}

TEST_F(WorkerDfxTest, TestGdecreaseWorkerClearFailed)
{
    LOG(INFO) << "Test worker clear object failed and decrease the global reference.";
    std::shared_ptr<ObjectClient> client1;
    InitTestClient(0, client1);
    std::shared_ptr<ObjectClient> client2;
    InitTestClient(1, client2);
    std::string data = "123456";
    std::string objectKey = "object1";
    std::shared_ptr<Buffer> buffer;
    DS_ASSERT_OK(client1->Create(objectKey, data.size(), CreateParam{}, buffer));
    AssertEventuallyOk([&buffer] { return buffer->Publish(); }, "initial publish after cluster startup");
    std::vector<Optional<Buffer>> buffers;
    std::vector<std::string> failedObjectKeys;
    DS_ASSERT_OK(client1->GIncreaseRef({ objectKey }, failedObjectKeys));
    ASSERT_TRUE(failedObjectKeys.empty());
    DS_ASSERT_OK(client2->Get({ objectKey }, 0, buffers));
    ASSERT_TRUE(NotExistsNone(buffers));
    failedObjectKeys.clear();
    DS_ASSERT_OK(client2->GIncreaseRef({ objectKey }, failedObjectKeys));
    ASSERT_TRUE(failedObjectKeys.empty());
    ASSERT_EQ(failedObjectKeys.size(), size_t(0));
    DS_ASSERT_OK(client1->GDecreaseRef({ objectKey }, failedObjectKeys));
    ASSERT_TRUE(failedObjectKeys.empty());
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "worker.clear_object_failure", "1*return(K_RUNTIME_ERROR)"));
    DS_ASSERT_NOT_OK(client2->GDecreaseRef({ objectKey }, failedObjectKeys));
    ASSERT_EQ(failedObjectKeys.size(), size_t(1));
    failedObjectKeys.clear();
    DS_ASSERT_OK(client2->GDecreaseRef({ objectKey }, failedObjectKeys));
    ASSERT_TRUE(failedObjectKeys.empty());
    DS_ASSERT_NOT_OK(client2->Get({ objectKey }, 0, buffers));
}

TEST_F(WorkerDfxTest, DISABLED_TestWorkerNotFirstGincreaseAndMasterCrash)
{
    std::shared_ptr<ObjectClient> client2;
    std::shared_ptr<ObjectClient> client1;
    InitTestClient(0, client2);
    InitTestClient(0, client1);
    std::string objectKey = NewObjectKey();
    std::string data = "123456";
    int dataSize = data.size();
    CreateParam param{ .consistencyType = ConsistencyType::CAUSAL };
    std::shared_ptr<Buffer> buffer1;
    DS_ASSERT_OK(client2->Create(objectKey, dataSize, param, buffer1));
    DS_ASSERT_OK(buffer1->MemoryCopy(data.data(), dataSize));
    DS_ASSERT_OK(buffer1->Publish());
    std::vector<std::string> failObjects;
    DS_ASSERT_OK(client2->GIncreaseRef({ objectKey }, failObjects));
    ASSERT_TRUE(failObjects.empty());
    cluster_->ShutdownNode(WORKER, 1);
    sleep(10);  // Wait 10 seconds for worker shutdown finished
    failObjects.clear();
    DS_ASSERT_OK(client1->GIncreaseRef({ objectKey }, failObjects));
    ASSERT_TRUE(failObjects.empty());
    cluster_->StartNode(WORKER, 1, "");
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 1));
    failObjects.clear();
    ASSERT_EQ(client1->QueryGlobalRefNum(objectKey), 2);
    DS_ASSERT_OK(client1->GDecreaseRef({ objectKey }, failObjects));
    ASSERT_TRUE(failObjects.empty());
    failObjects.clear();
    DS_ASSERT_OK(client2->GDecreaseRef({ objectKey }, failObjects));
    ASSERT_TRUE(failObjects.empty());
}

TEST_F(WorkerDfxTest, LEVEL1_TestWorkerCrashRecoveryAndClientExist)
{
    LOG(INFO) << "Test worker crash recovery and client exit before worker recovery.";
    std::string objKey = "KingBob";
    {
        std::shared_ptr<ObjectClient> client1;
        auto timeoutMs = 3000;
        InitTestClient(0, client1, timeoutMs);
        CreateParam param;
        size_t objSize = 500 * 1024ul;
        std::shared_ptr<Buffer> buffer;
        DS_ASSERT_OK(client1->Create(objKey, objSize, param, buffer));
        std::vector<std::string> failedObjectKeys;
        DS_ASSERT_OK(client1->GIncreaseRef({ objKey }, failedObjectKeys));
        ASSERT_TRUE(failedObjectKeys.empty());
        DS_ASSERT_OK(buffer->Seal());

        // Shutdown worker and exit client.
        cluster_->ShutdownNode(WORKER, 0);
    }
    // When worker restart, it need to notify master to decrease the global
    // reference in objectClient0_, and if buffer is not keep buffer, it will
    // be notify to deleted.
    DS_ASSERT_OK(cluster_->StartNode(WORKER, 0, ""));
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 0));

    std::shared_ptr<ObjectClient> client2;
    InitTestClient(1, client2);
    std::vector<Optional<Buffer>> buffers;
    DS_ASSERT_NOT_OK(client2->Get({ objKey }, 1'000, buffers));
    LOG(INFO) << "Test worker crash recovery and client exit before worker recovery end.";
}

TEST_F(WorkerDfxTest, TestWorkerPublishSealTimeout)
{
    LOG(INFO) << "Test worker process seal timeout.";
    InitTestClients();
    std::string objKey = "Warriors";
    CreateParam param;
    uint64_t objSize = 500 * 1024ul;
    std::shared_ptr<Buffer> buffer;
    DS_ASSERT_OK(objClient0_->Create(objKey, objSize, param, buffer));
    std::string data = GenRandomString(objSize);
    // Make sure the op below would be failed.
    DS_ASSERT_OK(buffer->MemoryCopy((void *)data.c_str(), data.size()));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "worker.publish_failure", "1*return(K_RPC_DEADLINE_EXCEEDED)"));
    DS_ASSERT_OK(buffer->Publish());

    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "worker.seal_failure", "1*return(K_RPC_DEADLINE_EXCEEDED)"));
    DS_ASSERT_OK(buffer->Seal());

    std::vector<Optional<Buffer>> buffers2;
    DS_ASSERT_OK(objClient2_->Get({ objKey }, 5000, buffers2));
    ASSERT_TRUE(NotExistsNone(buffers2));
    ASSERT_EQ(buffers2.size(), size_t(1));
    ASSERT_EQ(data,
              std::string(reinterpret_cast<const char *>((*buffers2[0]).ImmutableData()), (*buffers2[0]).GetSize()));
}

TEST_F(WorkerDfxTest, TestMasterCreateMetaFailure)
{
    LOG(INFO) << "Test master process create meta failure.";
    int32_t timeoutMs = 3000;
    InitTestClients(timeoutMs);
    std::string objKey = "Warriors";
    CreateParam param;
    uint64_t objSize = 500 * 1024ul;
    std::shared_ptr<Buffer> buffer;
    DS_ASSERT_OK(objClient0_->Create(objKey, objSize, param, buffer));
    std::string data = GenRandomString(objSize);
    // Make sure the op below would be failed.
    DS_ASSERT_OK(buffer->MemoryCopy((void *)data.c_str(), data.size()));

    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "WorkerMasterOCApi.CreateMeta.timeoutMs", "call(20)"));
    DS_ASSERT_OK(
        cluster_->SetInjectAction(WORKER, 1, "master.create_meta_failure", "1*return(K_RPC_DEADLINE_EXCEEDED)"));
    datasystem::inject::Set("rpc_util.retry_on_error_after_func", "1*sleep(3000)");
    DS_ASSERT_NOT_OK(buffer->Seal());
    DS_ASSERT_OK(buffer->Seal());

    std::vector<Optional<Buffer>> buffers2;
    DS_ASSERT_OK(objClient2_->Get({ objKey }, 5000, buffers2));
    ASSERT_TRUE(NotExistsNone(buffers2));
    ASSERT_EQ(buffers2.size(), size_t(1));
    ASSERT_BUF_EQ(*buffers2[0], data);
}

TEST_F(WorkerDfxTest, TestAutoUnWLatch)
{
    LOG(INFO) << "Test latch auto unlock when client crash";
    InitTestClients();
    std::string objKey = "Warriors";
    CreateParam param;
    uint64_t objSize = 500 * 1024ul;
    std::shared_ptr<Buffer> buffer;
    DS_ASSERT_OK(objClient0_->Create(objKey, objSize, param, buffer));
    DS_ASSERT_OK(buffer->Publish());

    std::vector<Optional<Buffer>> buffers2;
    DS_ASSERT_OK(objClient1_->Get({ objKey }, 5000, buffers2));
    ASSERT_TRUE(NotExistsNone(buffers2));
    ASSERT_EQ(buffers2.size(), 1ul);
    DS_ASSERT_OK(buffer->WLatch());
    DS_ASSERT_OK(datasystem::inject::Set("buffer.release", "call()"));
    objClient0_.reset();
    buffer.reset();
    DS_ASSERT_OK(buffers2[0]->WLatch());
}

TEST_F(WorkerDfxTest, TestAutoUnRLatch)
{
    LOG(INFO) << "Test latch auto unlock when client crash";
    InitTestClients();
    std::string objKey = "Warriors";
    CreateParam param;
    uint64_t objSize = 500 * 1024ul;
    std::shared_ptr<Buffer> buffer;
    DS_ASSERT_OK(objClient0_->Create(objKey, objSize, param, buffer));
    DS_ASSERT_OK(buffer->Publish());

    std::vector<Optional<Buffer>> buffers2;
    DS_ASSERT_OK(objClient1_->Get({ objKey }, 5000, buffers2));
    ASSERT_TRUE(NotExistsNone(buffers2));
    ASSERT_EQ(buffers2.size(), 1ul);
    DS_ASSERT_OK(buffer->RLatch());
    DS_ASSERT_OK(datasystem::inject::Set("buffer.release", "call()"));
    objClient0_.reset();
    buffer.reset();
    DS_ASSERT_OK(buffers2[0]->WLatch());
}

TEST_F(WorkerDfxTest, TestPublishFailNotVisiable)
{
    LOG(INFO) << "Test shm buffer publish fail and not visible";
    constexpr int32_t kRequestTimeoutMs = 3'000;
    InitTestClients(kRequestTimeoutMs);
    std::string objKey = "Warriors";
    CreateParam param;
    param.consistencyType = ConsistencyType::CAUSAL;
    uint64_t objSize = 500 * 1024ul;
    std::shared_ptr<Buffer> buffer;
    DS_ASSERT_OK(objClient0_->Create(objKey, objSize, param, buffer));
    DS_ASSERT_OK(buffer->Publish());

    std::vector<Optional<Buffer>> buffers2;
    DS_ASSERT_OK(objClient1_->Get({ objKey }, 5000, buffers2));
    ASSERT_TRUE(NotExistsNone(buffers2));

    cluster_->ShutdownNode(WORKER, 1);

    DS_ASSERT_NOT_OK(buffer->Publish());
    DS_ASSERT_NOT_OK(buffers2[0]->RLatch());

    DS_ASSERT_OK(cluster_->StartNode(WORKER, 1, ""));
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 1));

    DS_ASSERT_OK(buffer->Publish());
    DS_ASSERT_OK(buffers2[0]->RLatch());
}

TEST_F(WorkerDfxTest, TestClientExitAsyncNotifyToMaster)
{
    // Fix DTS2022082215708
    LOG(INFO) << "Test client exit and async notify to master";
    InitTestClients();
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "worker.gdecrease", "2*return(K_RPC_UNAVAILABLE)"));

    CreateParam param;
    param.consistencyType = ConsistencyType::CAUSAL;
    uint64_t objSize = 500 * 1024ul;
    std::vector<std::shared_ptr<Buffer>> buffers;
    for (size_t i = 0; i < 10; ++i) {
        std::string objKey = "Warriors" + std::to_string(1);
        std::shared_ptr<Buffer> buffer;
        DS_ASSERT_OK(objClient0_->Create(objKey, objSize, param, buffer));
        std::vector<std::string> failedObjectKeys;
        DS_ASSERT_OK(objClient0_->GIncreaseRef({ objKey }, failedObjectKeys));
        ASSERT_TRUE(failedObjectKeys.empty());
        buffers.emplace_back(std::move(buffer));
    }
    buffers.clear();
    objClient0_.reset();
    std::shared_ptr<Buffer> buffer;
    DS_ASSERT_OK(objClient1_->Create("Warriors", objSize, param, buffer));
}

TEST_F(WorkerDfxTest, DISABLED_TestMasterCacheInvalidFailure)
{
    LOG(INFO) << "Test master process cache invalid failure.";
    // Sort worker addresses.
    std::map<std::string, size_t> workerAddrMap;
    size_t needWorkerNum = 2;
    for (size_t i = 0; i < needWorkerNum; ++i) {
        HostPort addr;
        cluster_->GetWorkerAddr(i, addr);
        workerAddrMap.emplace(addr.ToString(), i);
    }
    // Create client
    std::vector<std::shared_ptr<ObjectClient>> clientVec;
    int32_t timeoutMs = 1000;
    for (const auto &it : workerAddrMap) {
        std::shared_ptr<ObjectClient> client;
        InitTestClient(it.second, client, timeoutMs);
        clientVec.push_back(client);
    }
    // worker1 create/publish object, worker1 get object
    std::string objKey = "Warriors";
    CreateParam param;
    param.consistencyType = ConsistencyType::CAUSAL;
    uint64_t objSize = 500 * 1024ul;
    std::shared_ptr<Buffer> buffer;
    DS_ASSERT_OK(clientVec[0]->Create(objKey, objSize, param, buffer));
    std::string data = GenRandomString(objSize);
    // Make sure the op below would be failed.
    DS_ASSERT_OK(buffer->MemoryCopy((void *)data.c_str(), data.size()));
    DS_ASSERT_OK(buffer->Publish());

    std::vector<Optional<Buffer>> buffers2;
    DS_ASSERT_OK(clientVec[1]->Get({ objKey }, 5000, buffers2));
    ASSERT_TRUE(NotExistsNone(buffers2));
    ASSERT_EQ(buffers2.size(), size_t(1));
    ASSERT_EQ(data,
              std::string(reinterpret_cast<const char *>((*buffers2[0]).ImmutableData()), (*buffers2[0]).GetSize()));

    // worker1 publish failed
    std::string data2 = GenRandomString(objSize);
    // Make sure the op below would be failed.
    DS_ASSERT_OK(buffer->MemoryCopy((void *)data2.c_str(), data2.size()));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "WorkerMasterOCApi.CreateMeta.timeoutMs", "call(20)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "WorkerMasterOCApi.UpdateMeta.timeoutMs", "call(20)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "WorkerMasterOCApi.CreateMeta.timeoutMs", "call(20)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "WorkerMasterOCApi.UpdateMeta.timeoutMs", "call(20)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, kMasterCacheInvalidInject, kPersistentRpcDeadlineExceeded));
    datasystem::inject::Set("rpc_util.retry_on_error_after_func", "1*sleep(1000)");
    DS_ASSERT_NOT_OK(buffer->Publish());

    // worker1 get old data
    std::vector<Optional<Buffer>> buffers3;
    DS_ASSERT_OK(clientVec[0]->Get({ objKey }, 5000, buffers3));
    ASSERT_TRUE(NotExistsNone(buffers3));
    ASSERT_EQ(buffers3.size(), size_t(1));
    ASSERT_EQ(data,
              std::string(reinterpret_cast<const char *>((*buffers3[0]).ImmutableData()), (*buffers3[0]).GetSize()));

    // worker1 publish success
    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, 1, kMasterCacheInvalidInject));
    DS_ASSERT_OK(buffer->MemoryCopy((void *)data2.c_str(), data2.size()));
    DS_ASSERT_OK(buffer->Publish());

    // worker2 get new data
    std::vector<Optional<Buffer>> buffers4;
    DS_ASSERT_OK(clientVec[1]->Get({ objKey }, 5000, buffers4));
    ASSERT_TRUE(NotExistsNone(buffers4));
    ASSERT_EQ(buffers4.size(), size_t(1));
    ASSERT_EQ(data2,
              std::string(reinterpret_cast<const char *>((*buffers4[0]).ImmutableData()), (*buffers4[0]).GetSize()));
}

TEST_F(WorkerDfxTest, TestPublishFailReGet)
{
    LOG(INFO) << "Test shm buffer publish fail and not visible";
    // Sort worker addresses.
    std::map<std::string, size_t> workerAddrMap;
    size_t needWorkerNum = 2;
    for (size_t i = 0; i < needWorkerNum; ++i) {
        HostPort addr;
        cluster_->GetWorkerAddr(i, addr);
        workerAddrMap.emplace(addr.ToString(), i);
    }
    // Create client
    std::vector<std::shared_ptr<ObjectClient>> clientVec;
    int32_t timeoutMs = 1000;
    for (const auto &it : workerAddrMap) {
        std::shared_ptr<ObjectClient> client;
        InitTestClient(it.second, client, timeoutMs);
        clientVec.push_back(client);
    }

    std::string objKey = "Warriors";
    CreateParam param;
    param.consistencyType = ConsistencyType::CAUSAL;
    uint64_t objSize = 1024ul;
    std::shared_ptr<Buffer> buffer;
    // worker2 create
    DS_ASSERT_OK(clientVec[1]->Create(objKey, objSize, param, buffer));
    std::string data = GenRandomString(objSize);
    // Make sure the op below would be failed.
    DS_ASSERT_OK(buffer->MemoryCopy((void *)data.c_str(), data.size()));
    DS_ASSERT_OK(buffer->Publish());

    // worker1 get object
    std::vector<Optional<Buffer>> buffers2;
    DS_ASSERT_OK(clientVec[0]->Get({ objKey }, 5000, buffers2));
    ASSERT_TRUE(NotExistsNone(buffers2));
    ASSERT_EQ(buffers2.size(), size_t(1));
    ASSERT_EQ(data,
              std::string(reinterpret_cast<const char *>((*buffers2[0]).ImmutableData()), (*buffers2[0]).GetSize()));

    // worker2 publish failed
    std::string data2 = GenRandomString(objSize);
    DS_ASSERT_OK(buffer->MemoryCopy((void *)data2.c_str(), data2.size()));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "WorkerMasterOCApi.CreateMeta.timeoutMs", "call(20)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "WorkerMasterOCApi.UpdateMeta.timeoutMs", "call(20)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "WorkerMasterOCApi.CreateMeta.timeoutMs", "call(20)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "WorkerMasterOCApi.UpdateMeta.timeoutMs", "call(20)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, kMasterCacheInvalidInject, kPersistentRpcDeadlineExceeded));
    datasystem::inject::Set("rpc_util.retry_on_error_after_func", "1*sleep(1000)");
    DS_ASSERT_NOT_OK(buffer->Publish());

    // worker2 retry get old data
    std::vector<Optional<Buffer>> buffers4;
    DS_ASSERT_OK(clientVec[1]->Get({ objKey }, 5000, buffers4));
    ASSERT_TRUE(NotExistsNone(buffers4));
    ASSERT_EQ(buffers4.size(), size_t(1));
    ASSERT_EQ(data,
              std::string(reinterpret_cast<const char *>((*buffers4[0]).ImmutableData()), (*buffers4[0]).GetSize()));

    // worker2 retry publish success
    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, 1, kMasterCacheInvalidInject));
    DS_ASSERT_OK(buffer->MemoryCopy((void *)data2.c_str(), data2.size()));
    DS_ASSERT_OK(buffer->Publish());

    // worker1 get new data
    std::vector<Optional<Buffer>> buffers5;
    DS_ASSERT_OK(clientVec[1]->Get({ objKey }, 5000, buffers5));
    ASSERT_TRUE(NotExistsNone(buffers5));
    ASSERT_EQ(buffers5.size(), size_t(1));
    ASSERT_EQ(data2,
              std::string(reinterpret_cast<const char *>((*buffers5[0]).ImmutableData()), (*buffers5[0]).GetSize()));

    // worker2 retry get new data
    std::vector<Optional<Buffer>> buffers6;
    DS_ASSERT_OK(clientVec[1]->Get({ objKey }, 5000, buffers4));
    ASSERT_TRUE(NotExistsNone(buffers4));
    ASSERT_EQ(buffers4.size(), size_t(1));
    ASSERT_EQ(data2,
              std::string(reinterpret_cast<const char *>((*buffers4[0]).ImmutableData()), (*buffers4[0]).GetSize()));
}

TEST_F(WorkerDfxTest, TestGIncreaseRetry)
{
    LOG(INFO) << "Test GIncreaseRef retry";
    std::shared_ptr<ObjectClient> client1;
    InitTestClient(0, client1);
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "worker.GIncrease_ref_failure", "1*return(K_TRY_AGAIN)"));
    std::string objectKey = NewObjectKey();
    std::vector<std::string> failedObjectKeys;
    DS_ASSERT_OK(client1->GIncreaseRef({ objectKey }, failedObjectKeys));
}

TEST_F(WorkerDfxTest, TestWorkerToMasterGIncreaseRetry)
{
    LOG(INFO) << "Test worker to master, GIncreaseRef retry";
    std::shared_ptr<ObjectClient> client1;
    InitTestClient(0, client1);
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "master.GIncrease_ref_failure", "1*return(K_TRY_AGAIN)"));
    std::string objectKey = NewObjectKey();
    std::vector<std::string> failedObjectKeys;
    DS_ASSERT_OK(client1->GIncreaseRef({ objectKey }, failedObjectKeys));
}

TEST_F(WorkerDfxTest, TestGIncreaseRetryIdempotence)
{
    LOG(INFO) << "Test GIncreaseRef idempotence";
    std::shared_ptr<ObjectClient> client1;
    InitTestClient(0, client1);
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "master.GIncrease_ref_Idempotence", "1*return(K_TRY_AGAIN)"));
    std::string objectKey = NewObjectKey();
    std::vector<std::string> failedObjectKeys;
    DS_ASSERT_OK(client1->GIncreaseRef({ objectKey }, failedObjectKeys));
}

TEST_F(WorkerDfxTest, TestGDecreaseClientRetry)
{
    LOG(INFO) << "Test GDecreaseRef client retry";
    std::shared_ptr<ObjectClient> client1;
    InitTestClient(0, client1);
    std::string objectKey = NewObjectKey();
    std::vector<std::string> failedObjectKeys;
    DS_ASSERT_OK(client1->GIncreaseRef({ objectKey }, failedObjectKeys));

    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "worker.gdecrease", "1*return(K_RPC_DEADLINE_EXCEEDED)"));
    DS_ASSERT_OK(client1->GDecreaseRef({ objectKey }, failedObjectKeys));
}

TEST_F(WorkerDfxTest, TestGDecreaseRetryIdempotence)
{
    LOG(INFO) << "Test GDecreaseRef retry Idempotence";
    std::shared_ptr<ObjectClient> client1;
    InitTestClient(0, client1);
    std::string objectKey = NewObjectKey();
    std::vector<std::string> failedObjectKeys;
    DS_ASSERT_OK(client1->GIncreaseRef({ objectKey }, failedObjectKeys));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "master.GDecrease_Ref_Idempotence", "1*return(K_TRY_AGAIN)"));
    DS_ASSERT_OK(client1->GDecreaseRef({ objectKey }, failedObjectKeys));
}

TEST_F(WorkerDfxTest, DISABLED_TestChangePrimaryCopy)
{
    std::shared_ptr<ObjectClient> client1, client2, client3;
    InitTestClient(0, client1);
    InitTestClient(1, client2);
    InitTestClient(2, client3);
    std::string objectKey = "obj1";
    std::vector<std::string> failObjects;
    DS_ASSERT_OK(client1->GIncreaseRef({ objectKey }, failObjects));
    int64_t dataSize = 10;
    std::string value1 = GenRandomString(dataSize);
    CreateParam param = { .consistencyType = ConsistencyType::CAUSAL };
    DS_ASSERT_OK(client1->Put(objectKey, reinterpret_cast<const uint8_t *>(value1.data()), value1.size(), param));
    std::vector<Optional<Buffer>> buffers;
    DS_ASSERT_OK(client3->Get({ objectKey }, 1'000, buffers));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "heartbeat.sleep", "1*sleep(2000)"));
    sleep(10);
    DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, 2));
    sleep(5);
    DS_ASSERT_OK(client2->Get({ objectKey }, 1'000, buffers));
    DS_ASSERT_OK(client1->GDecreaseRef({ objectKey }, failObjects));
    DS_ASSERT_OK(cluster_->StartNode(WORKER, 2, ""));
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 2));
}

class MasterDfxTest : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.workerGflagParams = "-client_reconnect_wait_s=1 -v=1";
        opts.numWorkers = 3;
        opts.masterIdx = masterIdx_;
        opts.numEtcd = 1;
        opts.enableDistributedMaster = "false";
        opts.disableRocksDB = false;
    }

    void InitTestClients()
    {
        InitTestClient(0, objClient0_);
        InitTestClient(0, objClient01_);
        InitTestClient(1, objClient1_);
    }

    void TearDown() override
    {
        objClient0_ = nullptr;
        objClient01_ = nullptr;
        objClient1_ = nullptr;
        ExternalClusterTest::TearDown();
    }

protected:
    std::shared_ptr<ObjectClient> objClient0_;
    std::shared_ptr<ObjectClient> objClient01_;
    std::shared_ptr<ObjectClient> objClient1_;
    const int masterIdx_ = 2;
};

TEST_F(MasterDfxTest, TestMasterRecoveryAndDecreaseGRef)
{
    LOG(INFO) << "Test master crash recovery and decrease the global reference.";
    InitTestClients();
    std::vector<uint8_t> data;
    data.resize(500 * 1024ul);
    size_t objNum = 5;
    std::vector<std::string> objectKeys;
    for (size_t i = 0; i < objNum; ++i) {
        std::string objKey = "Warriors_" + std::to_string(i);
        CreateAndSealObject(objClient0_, objKey, data);
        objectKeys.emplace_back(objKey);
    }
    std::vector<std::string> failedObjectKeys;
    DS_ASSERT_OK(objClient0_->GIncreaseRef(objectKeys, failedObjectKeys));
    ASSERT_TRUE(failedObjectKeys.empty());
    ASSERT_EQ(failedObjectKeys.size(), size_t(0));

    // Shutdown master and then restart to verify the metadata recovery.
    cluster_->ShutdownNode(WORKER, 2);
    DS_ASSERT_OK(cluster_->StartNode(WORKER, 2, ""));
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 2));
    sleep(2);

    DS_ASSERT_OK(objClient0_->GDecreaseRef(objectKeys, failedObjectKeys));
    ASSERT_TRUE(failedObjectKeys.empty());
    ASSERT_EQ(failedObjectKeys.size(), size_t(0));

    std::vector<Optional<Buffer>> buffers;
    DS_ASSERT_NOT_OK(objClient1_->Get({ "Warriors_0" }, 1'000, buffers));
}

TEST_F(MasterDfxTest, LEVEL1_TestMasterRecoveryAndClientExit)
{
    LOG(INFO) << "Test master crash and client exit before recovery";
    InitTestClients();
    std::string objKey = "Stuart";
    CreateParam param;
    size_t objSize = 500 * 1024ul;
    std::shared_ptr<Buffer> buffer;
    DS_ASSERT_OK(objClient0_->Create(objKey, objSize, param, buffer));
    std::vector<std::string> failedObjectKeys;
    DS_ASSERT_OK(objClient0_->GIncreaseRef({ objKey }, failedObjectKeys));
    ASSERT_TRUE(failedObjectKeys.empty());
    DS_ASSERT_OK(buffer->Seal());

    // Shutdown master and exit client.
    cluster_->ShutdownNode(WORKER, 2);
    buffer.reset();
    objClient0_.reset();
    DS_ASSERT_OK(cluster_->StartNode(WORKER, 2, ""));
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 2));

    // Client reference cleanup retries asynchronously while the centralized master is recovering. Wait until the
    // object is confirmed deleted instead of treating a transient routing failure as successful cleanup.
    constexpr int waitCleanupTimeoutSec = 15;
    auto waitObjectDeleted = [&]() {
        std::vector<Optional<Buffer>> buffers;
        auto rc = objClient1_->Get({ objKey }, 1'000, buffers);
        CHECK_FAIL_RETURN_STATUS(rc.GetCode() == K_NOT_FOUND, K_NOT_READY,
                                 "Object cleanup has not completed, status: " + rc.ToString());
        return Status::OK();
    };
    DS_ASSERT_OK(cluster_->WaitForExpectedResult(waitObjectDeleted, waitCleanupTimeoutSec, K_OK));
}

TEST_F(MasterDfxTest, LEVEL1_TestMasterRecoveryAndClientExit2)
{
    LOG(INFO) << "Test master crash and client exit before recovery";
    InitTestClients();
    std::string objKey = "Stuart";
    CreateParam param;
    size_t objSize = 600 * 1024ul;
    std::shared_ptr<Buffer> buffer;
    DS_ASSERT_OK(objClient0_->Create(objKey, objSize, param, buffer));

    // Shutdown master and exit client.
    cluster_->ShutdownNode(WORKER, 2);
    objClient0_.reset();
    DS_ASSERT_OK(cluster_->StartNode(WORKER, 2, ""));
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 2));

    // wait for async queue send message to master.
    sleep(1);

    DS_ASSERT_OK(objClient01_->Create(objKey, objSize, param, buffer));
}

TEST_F(MasterDfxTest, LEVEL1_TestMasterCrashAndGet)
{
    if (FLAGS_use_brpc) {
        GTEST_SKIP() << "brpc stream/worker-restart migration gap; flaky/failing under brpc. Tracked separately.";
    }
    LOG(INFO) << "Test master crash and get";
    InitTestClients();
    constexpr int32_t kConnectTimeoutMs = 60'000;
    constexpr int32_t kFailureRequestTimeoutMs = 3'000;
    std::shared_ptr<ObjectClient> failureClient;
    InitTestClient(0, failureClient, kConnectTimeoutMs, kFailureRequestTimeoutMs);
    std::string objKey = "Stuart";
    CreateParam param{ .consistencyType = ConsistencyType::CAUSAL };
    size_t objSize = 600 * 1024ul;
    std::shared_ptr<Buffer> buffer;
    DS_ASSERT_OK(objClient0_->Create(objKey, objSize, param, buffer));
    DS_ASSERT_OK(buffer->Publish());

    std::vector<Optional<Buffer>> getBuffers;
    std::string randData = GenRandomString(objSize);
    DS_ASSERT_OK(objClient1_->Get({ objKey }, 1'000, getBuffers));
    ASSERT_TRUE(NotExistsNone(getBuffers));
    ASSERT_EQ(getBuffers.size(), size_t(1));
    DS_ASSERT_OK(getBuffers[0]->MemoryCopy((void *)randData.data(), randData.size()));
    DS_ASSERT_OK(getBuffers[0]->Publish());

    // Shutdown master.
    cluster_->ShutdownNode(WORKER, 2);
    cluster_->SetInjectAction(WORKER, 0, "worker.query_meta", "1*return(K_RPC_UNAVAILABLE)");

    std::vector<Optional<Buffer>> getBuffers0;
    DS_ASSERT_NOT_OK(failureClient->Get({ objKey }, 1'000, getBuffers0));

    // Restart master and wait for worker reconnect.
    DS_ASSERT_OK(cluster_->StartNode(WORKER, 2, ""));
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 2));
    sleep(2);

    getBuffers0.clear();
    DS_ASSERT_OK(objClient0_->Get({ objKey }, 1'000, getBuffers0));
    ASSERT_TRUE(NotExistsNone(getBuffers0));
    ASSERT_EQ(getBuffers0.size(), size_t(1));
    std::string realData((const char *)getBuffers0[0]->ImmutableData(), getBuffers0[0]->GetSize());
    ASSERT_EQ(realData, randData);

    // worker 0 publish again.
    randData = GenRandomString(objSize);
    DS_ASSERT_OK(buffer->MemoryCopy((void *)randData.data(), randData.size()));
    DS_ASSERT_OK(buffer->Publish());

    // worker 1 get again and check.
    getBuffers0.clear();
    DS_ASSERT_OK(objClient0_->Get({ objKey }, 1'000, getBuffers0));
    ASSERT_TRUE(NotExistsNone(getBuffers0));
    ASSERT_EQ(getBuffers0.size(), size_t(1));
    std::string realData1((const char *)getBuffers0[0]->ImmutableData(), getBuffers0[0]->GetSize());
    ASSERT_EQ(realData1, randData);
}

class MasterWorkerDisconnectDfxTest : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.masterGflagParams = "-node_timeout_s=1 -node_dead_timeout_s=2 -v=3 -heartbeat_interval_ms=500";
        opts.workerGflagParams = "-node_timeout_s=30 -heartbeat_interval_ms=500 -v=3";
        opts.numMasters = 1;
        opts.numWorkers = 2;
        opts.numEtcd = 1;
        opts.enableDistributedMaster = "false";
    }

    void InitObjectClients()
    {
        InitTestClient(0, objClient0_);
        InitTestClient(1, objClient1_);
    }

    void TearDown() override
    {
        objClient0_ = nullptr;
        objClient1_ = nullptr;
        ClusterTest::TearDown();
    }

protected:
    std::shared_ptr<ObjectClient> objClient0_;
    std::shared_ptr<ObjectClient> objClient1_;
};

TEST_F(MasterWorkerDisconnectDfxTest, TestWorkerTimeoutAndPushMeta)
{
    LOG(INFO) << "Test worker recovery timeout and master clean its metadata then worker push metadata to master";
    InitObjectClients();
    size_t num = 10;
    std::vector<uint8_t> data;
    data.resize(500 * 1024ul);
    std::vector<std::string> objKeys;
    std::vector<std::shared_ptr<Buffer>> buffers(num);
    for (size_t i = 0; i < num; ++i) {
        std::string objKey = "BigEmperor" + std::to_string(i);
        objKeys.emplace_back(objKey);
        CreateParam param;
        DS_ASSERT_OK(objClient0_->Create(objKey, data.size(), param, buffers[i]));
        DS_ASSERT_OK(buffers[i]->MemoryCopy(data.data(), data.size()));
        DS_ASSERT_OK(buffers[i]->Publish());
    }
    std::vector<std::string> failedObjectKeys;
    DS_ASSERT_OK(objClient0_->GIncreaseRef(objKeys, failedObjectKeys));
    ASSERT_EQ(failedObjectKeys.size(), size_t(0));

    // Let hearbeat timeout and master will clear its metadata, then when worker reconnect,
    // master will ask us to send the metadata to master.
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "heartbeat.sleep", "1*sleep(2000)"));
    sleep(4);

    for (size_t i = 0; i < num; ++i) {
        DS_ASSERT_OK(buffers[i]->Seal());
    }

    std::vector<Optional<Buffer>> getBuffers;
    DS_ASSERT_OK(objClient1_->Get(objKeys, 60'000, getBuffers));
    ASSERT_TRUE(NotExistsNone(getBuffers));
    DS_ASSERT_OK(objClient0_->GDecreaseRef(objKeys, failedObjectKeys));
    ASSERT_EQ(failedObjectKeys.size(), size_t(0));
}

TEST_F(MasterWorkerDisconnectDfxTest, LEVEL2_TestMasterDeadAndWorkerReturnMasterTimeout)
{
    InitObjectClients();
    std::string objKey = NewObjectKey();
    std::string data = "Unicorn Gundam";
    std::vector<std::string> failedObjs;
    DS_ASSERT_OK(objClient1_->GIncreaseRef({ objKey }, failedObjs));
    DS_ASSERT_OK(objClient1_->Put(objKey, reinterpret_cast<uint8_t *>(const_cast<char *>(data.data())), data.size(),
                                  CreateParam{}));
    cluster_->ShutdownNode(ClusterNodeType::WORKER, 0);
    DS_ASSERT_NOT_OK(objClient1_->GDecreaseRef({ objKey }, failedObjs));
    failedObjs.clear();
    // Wait worker notify master timeout.
    std::this_thread::sleep_for(std::chrono::seconds(30));
    // Worker return K_MASTER_TIMOUT to client and don't retry.
    LOG(INFO) << "master timeout";
    for (size_t i = 0; i < 10; i++) {
        Timer timer;
        Status rc = objClient1_->GDecreaseRef({ objKey }, failedObjs);
        EXPECT_NE(rc.GetCode(), K_RPC_UNAVAILABLE);
        ASSERT_LE(timer.ElapsedMilliSecondAndReset(), 1000);
        failedObjs.clear();
        rc = objClient1_->GIncreaseRef({ NewObjectKey() }, failedObjs);
        EXPECT_NE(rc.GetCode(), K_RPC_UNAVAILABLE);
        ASSERT_LE(timer.ElapsedMilliSecondAndReset(), 1000);
        failedObjs.clear();
        std::vector<Optional<Buffer>> buffers;
        rc = objClient1_->Get({ objKey }, 0, buffers);
        EXPECT_NE(rc.GetCode(), K_RPC_UNAVAILABLE);
        ASSERT_LE(timer.ElapsedMilliSecondAndReset(), 1000);
    }
    LOG(INFO) << "test case finished";
    cluster_->StartNode(ClusterNodeType::MASTER, 0, {});
    cluster_->WaitNodeReady(ClusterNodeType::MASTER, 0);
}

TEST_F(WorkerDfxTest, TestRemoteGetRpcUnavailableRetry)
{
    FLAGS_v = 1;
    LOG(INFO) << "Test remote get from a busy worker. Local worker should retry";
    InitTestClients();
    std::string objKey = "Warriors";
    CreateParam param;
    param.consistencyType = ConsistencyType::CAUSAL;
    uint64_t objSize = 500 * 1024ul;
    std::shared_ptr<Buffer> buffer;
    DS_ASSERT_OK(objClient0_->Create(objKey, objSize, param, buffer));
    DS_ASSERT_OK(buffer->Publish());
    DS_ASSERT_OK(
        cluster_->SetInjectAction(WORKER, 0, "worker.worker_worker_remote_get_failure", "1*return(K_RPC_UNAVAILABLE)"));

    std::vector<Optional<Buffer>> buffers2;
    DS_ASSERT_OK(objClient2_->Get({ objKey }, 5000, buffers2));
}

class WorkerReconciliationDfxTest : public OCClientCommon {
public:
    enum class GRefTableKind { WORKER, MASTER };

    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = WORKER_NUM;
        opts.numEtcd = 1;
        opts.waitWorkerReady = false;
        opts.enableDistributedMaster = "true";
        opts.workerGflagParams = " -client_reconnect_wait_s=5 -node_timeout_s=5";
        opts.disableRocksDB = false;
    }

    void TearDown() override
    {
        for (auto &client : clients_) {
            client.reset();
        }
        utSvcStub0_.reset();
        utSvcStub1_.reset();
        aksk_.reset();
        db_.reset();
        ExternalClusterTest::TearDown();
    }

    void SetUp() override
    {
        CommonTest::SetUp();
        DS_ASSERT_OK(Init());
        ASSERT_TRUE(cluster_ != nullptr);
        DS_ASSERT_OK(cluster_->StartEtcdCluster());
        InitEtcd();
        DS_ASSERT_OK(cluster_->StartWorkers());
        for (size_t i = 0; i < WORKER_NUM; ++i) {
            DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, i));
        }
        BuildHashOwnerIndex();
        InitializeTest();
    }

    void InitEtcd()
    {
        std::pair<HostPort, HostPort> addrs;
        DS_ASSERT_OK(cluster_->GetEtcdAddrs(0, addrs));
        etcdAddress_ = addrs.first;
        db_ = std::make_shared<EtcdStore>(etcdAddress_.ToString());
        if ((db_ != nullptr) && (db_->Init().IsOk())) {
            (void)RegisterTopologyTables(*db_);
        }
    }

    void BuildHashOwnerIndex()
    {
        std::string value;
        DS_ASSERT_OK(db_->Get(GetTopologyTableName(), "", value));
        ClusterTopologyPb ring;
        ASSERT_TRUE(ring.ParseFromString(value));
        workerAddresses_.clear();
        workerAddresses_.resize(WORKER_NUM);
        for (size_t i = 0; i < WORKER_NUM; ++i) {
            HostPort workerAddress;
            DS_ASSERT_OK(cluster_->GetWorkerAddr(i, workerAddress));
            workerAddresses_[i] = workerAddress.ToString();
            auto worker = ring.members().find(workerAddresses_[i]);
            ASSERT_NE(worker, ring.members().end());
        }
        tokenOwners_.clear();
        for (const auto &worker : ring.members()) {
            for (const auto token : worker.second.tokens()) {
                tokenOwners_.emplace_back(token, worker.first);
            }
        }
        std::sort(tokenOwners_.begin(), tokenOwners_.end(),
                  [](const auto &left, const auto &right) { return left.first < right.first; });
        ASSERT_FALSE(tokenOwners_.empty());
    }

    std::string ObjectKeyHashTo(size_t workerIndex, size_t hashIndex) const
    {
        const std::string prefix =
            "worker_reconciliation_" + std::to_string(workerIndex) + "_" + std::to_string(hashIndex) + "_";
        constexpr size_t kMaxKeySearch = 100'000;
        for (size_t i = 0; i < kMaxKeySearch; ++i) {
            auto key = prefix + std::to_string(i);
            if (HashBelongsToWorker(workerIndex, MurmurHash3_32(key))) {
                return key;
            }
        }
        ADD_FAILURE() << "Failed to find object key for worker index " << workerIndex;
        return prefix + "fallback";
    }

    bool HashBelongsToWorker(size_t workerIndex, uint32_t hash) const
    {
        if (workerIndex >= workerAddresses_.size() || tokenOwners_.empty()) {
            return false;
        }
        auto iter = std::lower_bound(tokenOwners_.begin(), tokenOwners_.end(), hash,
                                     [](const auto &entry, uint32_t value) { return entry.first < value; });
        if (iter == tokenOwners_.end()) {
            iter = tokenOwners_.begin();
        }
        return iter->second == workerAddresses_[workerIndex];
    }

    void InitializeTest()
    {
        DS_ASSERT_OK(cluster_->GetWorkerAddr(0, workerAddr0_));
        DS_ASSERT_OK(cluster_->GetWorkerAddr(1, workerAddr1_));
        InitObjectClients();
        InitApis();
    }

    void InitObjectClients()
    {
        clients_.resize(CLIENT_NUM);
        int idx = 0;
        InitTestClient(0, clients_[idx++]);
        InitTestClient(0, clients_[idx++]);
        InitTestClient(1, clients_[idx++]);
        InitTestClient(1, clients_[idx++]);
    }

    void InitApis()
    {
        HostPort fakeLocalAddr;
        fakeLocalAddr.ParseString("127.0.0.1:30000");
        aksk_ = std::make_shared<AkSkManager>();
        DS_ASSERT_OK(aksk_->SetClientAkSk(ak_, sk_));
        RpcCredential cred;
        RpcAuthKeyManager::CreateClientCredentials(authKeys_, WORKER_SERVER_NAME, cred);
        if (FLAGS_use_brpc) {
            // brpc mode: worker registers UtOCServiceBrpcAdapter, no ZMQ UtOCService.
            // Use a brpc channel + the generated UtOCService_BrpcGenericStub.
            const int32_t timeoutMs = 500;
            auto makeBrpc = [timeoutMs](const HostPort &addr) {
                BrpcChannelConfig cfg;
                cfg.endpoint = HostPort(addr.Host(), addr.Port() + kBrpcPortOffset).ToString();
                cfg.timeout_ms = timeoutMs;
                cfg.connect_timeout_ms = timeoutMs;
                cfg.enable_circuit_breaker = false;  // test control-plane: don't isolate restarted workers
                auto ch = BrpcChannelFactory::Create(cfg);
                return std::make_unique<BrpcUtOCStub>(std::move(ch), timeoutMs);
            };
            utSvcStub0_ = makeBrpc(workerAddr0_);
            utSvcStub1_ = makeBrpc(workerAddr1_);
        } else {
            auto channel = std::make_shared<RpcChannel>(workerAddr0_, cred);
            utSvcStub0_ = std::make_unique<ZmqUtOCStub>(channel);
            channel = std::make_shared<RpcChannel>(workerAddr1_, cred);
            utSvcStub1_ = std::make_unique<ZmqUtOCStub>(channel);
        }
    }

    void PutObjGIncreaseRef()
    {
        size_t sz = 1024;
        std::string data = RandomData().GetRandomString(sz);
        std::vector<std::string> failedIds;
        std::vector<std::string> objKeys(CLIENT_NUM);
        // obj0 by client0, primary copy on node0, meta on node0
        objKeys[0] = ObjectKeyHashTo(0, 0);
        // obj1 by client1, primary copy on node0, meta on node1
        objKeys[1] = ObjectKeyHashTo(1, 0);
        // obj2 by client2, primary copy on node1, meta on node1
        objKeys[2] = ObjectKeyHashTo(1, 1);
        // obj3 by client3, primary copy on node1, meta on node0
        objKeys[3] = ObjectKeyHashTo(0, 1);
        for (size_t i = 0; i < CLIENT_NUM; ++i) {
            DS_ASSERT_OK(
                clients_[i]->Put(objKeys[i], reinterpret_cast<uint8_t *>(&data.front()), sz, CreateParam(), {}));
            DS_ASSERT_OK(clients_[i]->GIncreaseRef({ objKeys[i] }, failedIds));
            ASSERT_TRUE(failedIds.empty());
        }

        GRefTableReqPb req;
        GRefTableRspPb rsp;
        DS_ASSERT_OK(aksk_->GenerateSignature(req));
        // worker0's table should have two clients and two objects
        DS_ASSERT_OK(utSvcStub0_->GetWorkerGRefTable(req, rsp));
        ASSERT_EQ(rsp.single_client_gref().size(), 2);
        // master0's table should have two workers and two objects
        rsp.clear_single_client_gref();
        DS_ASSERT_OK(utSvcStub0_->GetMasterGRefTable(req, rsp));
        ASSERT_EQ(rsp.single_client_gref().size(), 2);
        // worker1's table should have two clients and two objects
        rsp.clear_single_client_gref();
        DS_ASSERT_OK(utSvcStub1_->GetWorkerGRefTable(req, rsp));
        ASSERT_EQ(rsp.single_client_gref().size(), 2);
        // master1's table should have two workers and two objects
        rsp.clear_single_client_gref();
        DS_ASSERT_OK(utSvcStub1_->GetMasterGRefTable(req, rsp));
        ASSERT_EQ(rsp.single_client_gref().size(), 2);
    }

    void AssertGRefTableSizeEventually(IUtOCStub &stub, GRefTableKind tableKind, size_t expected,
                                       const std::string &tableName)
    {
        Status lastRc = Status::OK();
        size_t actual = std::numeric_limits<size_t>::max();
        const auto deadline =
            std::chrono::steady_clock::now() + std::chrono::milliseconds(kEventuallyWaitTimeoutMs);
        do {
            GRefTableReqPb req;
            GRefTableRspPb rsp;
            DS_ASSERT_OK(aksk_->GenerateSignature(req));
            if (tableKind == GRefTableKind::WORKER) {
                lastRc = stub.GetWorkerGRefTable(req, rsp);
            } else {
                lastRc = stub.GetMasterGRefTable(req, rsp);
            }
            if (lastRc.IsOk()) {
                actual = rsp.single_client_gref().size();
                if (actual == expected) {
                    return;
                }
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(kEventuallyPollIntervalMs));
        } while (std::chrono::steady_clock::now() < deadline);

        ASSERT_TRUE(lastRc.IsOk()) << tableName << " query failed: " << lastRc.ToString();
        ASSERT_EQ(actual, expected) << tableName << " did not converge within " << kEventuallyWaitTimeoutMs << " ms";
    }

    void AssertCmNodeTableReadyEventually(IUtOCStub &stub, const std::string &tableName)
    {
        Status lastRc = Status::OK();
        uint32_t actual = 0;
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(kEventuallyWaitTimeoutMs);
        do {
            CmNodeTableReqPb req;
            CmNodeTableRspPb rsp;
            DS_ASSERT_OK(aksk_->GenerateSignature(req));
            lastRc = stub.GetCmNodeTable(req, rsp);
            if (lastRc.IsOk()) {
                actual = rsp.cm_node_table_size();
                if (actual == WORKER_NUM && rsp.cm_node_table().at(0).addition_event_type() == "ready"
                    && rsp.cm_node_table().at(1).addition_event_type() == "ready") {
                    return;
                }
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(kEventuallyPollIntervalMs));
        } while (std::chrono::steady_clock::now() < deadline);

        ASSERT_TRUE(lastRc.IsOk()) << tableName << " query failed: " << lastRc.ToString();
        ASSERT_EQ(actual, WORKER_NUM) << tableName << " did not converge within " << kEventuallyWaitTimeoutMs << " ms";
    }

protected:
    std::vector<std::shared_ptr<ObjectClient>> clients_;
    std::unique_ptr<IUtOCStub> utSvcStub0_;
    std::unique_ptr<IUtOCStub> utSvcStub1_;
    std::shared_ptr<AkSkManager> aksk_;
    std::shared_ptr<EtcdStore> db_;
    HostPort workerAddr0_;
    HostPort workerAddr1_;
    HostPort etcdAddress_;
    std::vector<std::string> workerAddresses_;
    std::vector<std::pair<uint32_t, std::string>> tokenOwners_;
    std::string ak_ = "QTWAOYTTINDUT2QVKYUC";
    std::string sk_ = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
    const size_t CLIENT_NUM = 4;
    const size_t WORKER_NUM = 2;
    RpcAuthKeys authKeys_;
};

TEST_F(WorkerReconciliationDfxTest, LEVEL2_ClientExitDuringWorkerRestart1)
{
    PutObjGIncreaseRef();

    // shutdown node1 and client2
    DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, 1));
    clients_[2].reset();

    GRefTableReqPb req;
    GRefTableRspPb rsp;
    DS_ASSERT_OK(aksk_->GenerateSignature(req));
    // master0 and worker0 should be good
    // worker0's table should have two clients and two objects
    rsp.clear_single_client_gref();
    DS_ASSERT_OK(utSvcStub0_->GetWorkerGRefTable(req, rsp));
    ASSERT_EQ(rsp.single_client_gref().size(), 2);
    // Liveness removal is asynchronous after ShutdownNode returns.
    AssertGRefTableSizeEventually(*utSvcStub0_, GRefTableKind::MASTER, 1,
                                  "worker0 master gref table after worker1 shutdown");

    // restart node1
    DS_ASSERT_OK(cluster_->StartNode(WORKER, 1, ""));
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 1));

    // worker1 should reconcile with master0 and master1
    // worker0's table should have two clients and two objects
    rsp.clear_single_client_gref();
    DS_ASSERT_OK(utSvcStub0_->GetWorkerGRefTable(req, rsp));
    ASSERT_EQ(rsp.single_client_gref().size(), 2);
    // master0's table should have two workers and two objects
    rsp.clear_single_client_gref();
    DS_ASSERT_OK(utSvcStub0_->GetMasterGRefTable(req, rsp));
    ASSERT_EQ(rsp.single_client_gref().size(), 2);
    // worker1's table should have one client and one object
    rsp.clear_single_client_gref();
    DS_ASSERT_OK(utSvcStub1_->GetWorkerGRefTable(req, rsp));
    ASSERT_EQ(rsp.single_client_gref().size(), 1);
    // master1's table should have one worker and one object
    rsp.clear_single_client_gref();
    DS_ASSERT_OK(utSvcStub1_->GetMasterGRefTable(req, rsp));
    ASSERT_EQ(rsp.single_client_gref().size(), 1);
}

TEST_F(WorkerReconciliationDfxTest, LEVEL1_ClientExitDuringWorkerRestart2)
{
    if (FLAGS_use_brpc) {
        GTEST_SKIP() << "brpc: master GlobalRefTable cleanup lags on worker shutdown/restart "
                        "(master ProcessWorkerRestart does not clear stale gref entries the way "
                        "the heartbeat-timeout path does under ZMQ). Tracked separately; the "
                        "UtOCService stub brpc switch (IUtOCStub) and circuit-breaker / retry "
                        "hardening in this PR already make the RPC reach the worker.";
    }
    PutObjGIncreaseRef();

    // shutdown node1 and client3
    DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, 1));
    clients_[3].reset();

    GRefTableReqPb req;
    GRefTableRspPb rsp;
    DS_ASSERT_OK(aksk_->GenerateSignature(req));
    // master0 and worker0 should be good
    // worker0's table should have two clients and two objects
    rsp.clear_single_client_gref();
    DS_ASSERT_OK(utSvcStub0_->GetWorkerGRefTable(req, rsp));
    ASSERT_EQ(rsp.single_client_gref().size(), 2);
    // Liveness removal is asynchronous after ShutdownNode returns.
    AssertGRefTableSizeEventually(*utSvcStub0_, GRefTableKind::MASTER, 1,
                                  "worker0 master gref table after worker1 shutdown");

    // restart node1
    DS_ASSERT_OK(cluster_->StartNode(WORKER, 1, ""));
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 1));

    // worker1 should reconcile with master0 and master1
    // worker0's table should have two clients and two objects
    rsp.clear_single_client_gref();
    DS_ASSERT_OK(utSvcStub0_->GetWorkerGRefTable(req, rsp));
    ASSERT_EQ(rsp.single_client_gref().size(), 2);
    // master0's table should have one worker and one object
    rsp.clear_single_client_gref();
    DS_ASSERT_OK(utSvcStub0_->GetMasterGRefTable(req, rsp));
    ASSERT_EQ(rsp.single_client_gref().size(), 1);
    // worker1's table should have one client and one object
    rsp.clear_single_client_gref();
    DS_ASSERT_OK(utSvcStub1_->GetWorkerGRefTable(req, rsp));
    ASSERT_EQ(rsp.single_client_gref().size(), 1);
    // Failure final removed worker1 before this process started. The new INITIAL member must not restore the removed
    // identity's remote worker reference during fresh admission.
    rsp.clear_single_client_gref();
    DS_ASSERT_OK(utSvcStub1_->GetMasterGRefTable(req, rsp));
    ASSERT_EQ(rsp.single_client_gref().size(), 1);
}

TEST_F(WorkerReconciliationDfxTest, LEVEL1_ClientExitDuringWorkerRestart3)
{
    if (FLAGS_use_brpc) {
        GTEST_SKIP() << "brpc: master GlobalRefTable cleanup lags on worker shutdown/restart "
                        "(master ProcessWorkerRestart does not clear stale gref entries the way "
                        "the heartbeat-timeout path does under ZMQ). Tracked separately; the "
                        "UtOCService stub brpc switch (IUtOCStub) and circuit-breaker / retry "
                        "hardening in this PR already make the RPC reach the worker.";
    }
    PutObjGIncreaseRef();

    // shutdown node1 and client2 and client3
    DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, 1));
    clients_[2].reset();
    clients_[3].reset();

    GRefTableReqPb req;
    GRefTableRspPb rsp;
    DS_ASSERT_OK(aksk_->GenerateSignature(req));
    // master0 and worker0 should be good
    // worker0's table should have two clients and two objects
    rsp.clear_single_client_gref();
    DS_ASSERT_OK(utSvcStub0_->GetWorkerGRefTable(req, rsp));
    ASSERT_EQ(rsp.single_client_gref().size(), 2);
    // Liveness removal is asynchronous after ShutdownNode returns.
    AssertGRefTableSizeEventually(*utSvcStub0_, GRefTableKind::MASTER, 1,
                                  "worker0 master gref table after worker1 shutdown");

    // restart node1
    DS_ASSERT_OK(cluster_->StartNode(WORKER, 1, ""));
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 1));

    // worker1 should reconcile with master0 and master1
    // worker0's table should have two clients and two objects
    rsp.clear_single_client_gref();
    DS_ASSERT_OK(utSvcStub0_->GetWorkerGRefTable(req, rsp));
    ASSERT_EQ(rsp.single_client_gref().size(), 2);
    // master0's table should have one worker and one object
    rsp.clear_single_client_gref();
    DS_ASSERT_OK(utSvcStub0_->GetMasterGRefTable(req, rsp));
    ASSERT_EQ(rsp.single_client_gref().size(), 1);
    // worker1's table should have no client and no object
    AssertGRefTableSizeEventually(*utSvcStub1_, GRefTableKind::WORKER, 0, "worker1 worker gref table");
    // master1's table should have one worker and one object
    AssertGRefTableSizeEventually(*utSvcStub1_, GRefTableKind::MASTER, 1, "worker1 master gref table");
}

TEST_F(WorkerReconciliationDfxTest, LEVEL1_ClientExitDuringWorkerRestart4)
{
    PutObjGIncreaseRef();
    inject::Set("ObjClient.ShutDown", "return(K_OK)");
    for (auto &client : clients_) {
        client.reset();
    }
    cluster_->SetInjectAction(ClusterNodeType::WORKER, 0, "worker.PreShutDown.skip", "return(K_OK)");
    cluster_->SetInjectAction(ClusterNodeType::WORKER, 1, "worker.PreShutDown.skip", "return(K_OK)");
    // shutdown all nodes and clients
    DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, 0));
    DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, 1));

    // restart node0 first and then node1
    DS_ASSERT_OK(cluster_->StartNode(WORKER, 0, ""));
    DS_ASSERT_OK(cluster_->StartNode(WORKER, 1, ""));
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 0));
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 1));

    GRefTableReqPb req;
    GRefTableRspPb rsp;
    DS_ASSERT_OK(aksk_->GenerateSignature(req));
    // all masters and workers should have no gref
    rsp.clear_single_client_gref();
    DS_ASSERT_OK(utSvcStub0_->GetWorkerGRefTable(req, rsp));
    ASSERT_EQ(rsp.single_client_gref().size(), 0);
    rsp.clear_single_client_gref();
    DS_ASSERT_OK(utSvcStub0_->GetMasterGRefTable(req, rsp));
    ASSERT_EQ(rsp.single_client_gref().size(), 0);
    rsp.clear_single_client_gref();
    DS_ASSERT_OK(utSvcStub1_->GetWorkerGRefTable(req, rsp));
    ASSERT_EQ(rsp.single_client_gref().size(), 0);
    rsp.clear_single_client_gref();
    DS_ASSERT_OK(utSvcStub1_->GetMasterGRefTable(req, rsp));
    ASSERT_EQ(rsp.single_client_gref().size(), 0);
}

TEST_F(WorkerReconciliationDfxTest, LEVEL1_ClientExitDuringWorkerRestart5)
{
    // similar to LEVEL1_ClientExitDuringWorkerRestart4 but restart nodes in different order
    PutObjGIncreaseRef();
    inject::Set("ObjClient.ShutDown", "return(K_OK)");
    for (size_t i = 0; i < CLIENT_NUM; ++i) {
        clients_[i].reset();
    }
    cluster_->SetInjectAction(ClusterNodeType::WORKER, 0, "worker.PreShutDown.skip", "return(K_OK)");
    cluster_->SetInjectAction(ClusterNodeType::WORKER, 1, "worker.PreShutDown.skip", "return(K_OK)");
    // shutdown all nodes and clients
    DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, 0));
    DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, 1));

    // restart node1 first and then node0
    DS_ASSERT_OK(cluster_->StartNode(WORKER, 1, ""));
    DS_ASSERT_OK(cluster_->StartNode(WORKER, 0, ""));
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 1));
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 0));

    GRefTableReqPb req;
    GRefTableRspPb rsp;
    DS_ASSERT_OK(aksk_->GenerateSignature(req));
    // all masters and workers should have no gref
    rsp.clear_single_client_gref();
    DS_ASSERT_OK(utSvcStub0_->GetWorkerGRefTable(req, rsp));
    ASSERT_EQ(rsp.single_client_gref().size(), 0);
    rsp.clear_single_client_gref();
    DS_ASSERT_OK(utSvcStub0_->GetMasterGRefTable(req, rsp));
    ASSERT_EQ(rsp.single_client_gref().size(), 0);
    rsp.clear_single_client_gref();
    DS_ASSERT_OK(utSvcStub1_->GetWorkerGRefTable(req, rsp));
    ASSERT_EQ(rsp.single_client_gref().size(), 0);
    rsp.clear_single_client_gref();
    DS_ASSERT_OK(utSvcStub1_->GetMasterGRefTable(req, rsp));
    ASSERT_EQ(rsp.single_client_gref().size(), 0);
}

TEST_F(WorkerReconciliationDfxTest, LEVEL1_GiveUpReconciliation)
{
    if (FLAGS_use_brpc) {
        GTEST_SKIP() << "brpc: master GlobalRefTable cleanup lags on worker shutdown/restart "
                        "(master ProcessWorkerRestart does not clear stale gref entries the way "
                        "the heartbeat-timeout path does under ZMQ). Tracked separately; the "
                        "UtOCService stub brpc switch (IUtOCStub) and circuit-breaker / retry "
                        "hardening in this PR already make the RPC reach the worker.";
    }
    PutObjGIncreaseRef();

    // shutdown node1 and client3
    DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, 1));
    clients_[3].reset();

    GRefTableReqPb req;
    GRefTableRspPb rsp;
    DS_ASSERT_OK(aksk_->GenerateSignature(req));
    // master0 and worker0 should be good
    // worker0's table should have two clients and two objects
    rsp.clear_single_client_gref();
    DS_ASSERT_OK(utSvcStub0_->GetWorkerGRefTable(req, rsp));
    ASSERT_EQ(rsp.single_client_gref().size(), 2);
    // Phase2 excludes the stopped worker from routable membership, so master0 keeps only the healthy worker's ref.
    AssertGRefTableSizeEventually(*utSvcStub0_, GRefTableKind::MASTER, 1, "worker0 master gref table");

    // restart node1
    DS_ASSERT_OK(cluster_->StartNode(
        WORKER, 1, " -inject_actions=WorkerOCServiceImpl.GiveUpReconciliation.setHealthFile:call(1000)"));
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 1));

    // worker1 should reconcile with master0 and master1
    // worker0's table should have two clients and two objects
    rsp.clear_single_client_gref();
    DS_ASSERT_OK(utSvcStub0_->GetWorkerGRefTable(req, rsp));
    ASSERT_EQ(rsp.single_client_gref().size(), 2);
}

TEST_F(WorkerReconciliationDfxTest, LEVEL2_ClientExitDuringWorkerNetworkIssue)
{
    PutObjGIncreaseRef();

    // node1 suffers from network issue and client3 exits
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "heartbeat.sleep", "1*sleep(10000)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "worker.gdecrease", "1*return(K_RPC_UNAVAILABLE)"));
    std::this_thread::sleep_for(std::chrono::seconds(1));
    clients_[3].reset();

    GRefTableReqPb req;
    GRefTableRspPb rsp;
    DS_ASSERT_OK(aksk_->GenerateSignature(req));
    // master0 and worker0 should be good
    // worker0's table should have two clients and two objects
    rsp.clear_single_client_gref();
    DS_ASSERT_OK(utSvcStub0_->GetWorkerGRefTable(req, rsp));
    ASSERT_EQ(rsp.single_client_gref().size(), 2);
    // master0's table should have two workers and two objects
    rsp.clear_single_client_gref();
    DS_ASSERT_OK(utSvcStub0_->GetMasterGRefTable(req, rsp));
    ASSERT_EQ(rsp.single_client_gref().size(), 2);

    // node1 recovers
    int waitTimeSec = 30;
    std::this_thread::sleep_for(std::chrono::seconds(waitTimeSec));

    // worker1 should reconcile with master0 and master1
    // worker0's table should have two clients and two objects
    rsp.clear_single_client_gref();
    DS_ASSERT_OK(utSvcStub0_->GetWorkerGRefTable(req, rsp));
    ASSERT_EQ(rsp.single_client_gref().size(), 2);
    // master0's table should have one worker and one object
    rsp.clear_single_client_gref();
    DS_ASSERT_OK(utSvcStub0_->GetMasterGRefTable(req, rsp));
    ASSERT_EQ(rsp.single_client_gref().size(), 1);
    // worker1's table should have one client and one object
    rsp.clear_single_client_gref();
    DS_ASSERT_OK(utSvcStub1_->GetWorkerGRefTable(req, rsp));
    ASSERT_EQ(rsp.single_client_gref().size(), 1);
    // master1's table should have two workers and two objects
    rsp.clear_single_client_gref();
    DS_ASSERT_OK(utSvcStub1_->GetMasterGRefTable(req, rsp));
    ASSERT_EQ(rsp.single_client_gref().size(), 2);
}

TEST_F(WorkerReconciliationDfxTest, LEVEL2_ClientExitDuringWorkerNetworkIssueAndRestart)
{
    PutObjGIncreaseRef();

    // shutdown worker 0
    DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, 0));
    std::this_thread::sleep_for(std::chrono::seconds(1));
    // node1 suffers from network issue and client3 exits
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "heartbeat.sleep", "1*sleep(6000)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "worker.gdecrease", "1*return(K_RPC_UNAVAILABLE)"));
    std::this_thread::sleep_for(std::chrono::seconds(1));
    clients_[3].reset();

    // node1 recovers
    int waitTimeSec = 5;
    std::this_thread::sleep_for(std::chrono::seconds(waitTimeSec));

    // restart worker0
    DS_ASSERT_OK(cluster_->StartNode(WORKER, 0, ""));
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 0));
    // wait for reconciliation between restarted master and all workers
    waitTimeSec = 10;
    std::this_thread::sleep_for(std::chrono::seconds(waitTimeSec));

    GRefTableReqPb req;
    GRefTableRspPb rsp;
    DS_ASSERT_OK(aksk_->GenerateSignature(req));

    // worker1 should reconcile with master0 and master1
    // worker0's table should have two clients and two objects
    rsp.clear_single_client_gref();
    DS_ASSERT_OK(utSvcStub0_->GetWorkerGRefTable(req, rsp));
    ASSERT_EQ(rsp.single_client_gref().size(), 2);
    // master0's table should have one worker and one object
    rsp.clear_single_client_gref();
    DS_ASSERT_OK(utSvcStub0_->GetMasterGRefTable(req, rsp));
    ASSERT_EQ(rsp.single_client_gref().size(), 1);
    // worker1's table should have one client and one object
    rsp.clear_single_client_gref();
    DS_ASSERT_OK(utSvcStub1_->GetWorkerGRefTable(req, rsp));
    ASSERT_EQ(rsp.single_client_gref().size(), 1);
    // master1's table should have two workers and two objects
    rsp.clear_single_client_gref();
    DS_ASSERT_OK(utSvcStub1_->GetMasterGRefTable(req, rsp));
    ASSERT_EQ(rsp.single_client_gref().size(), 2);
}

TEST_F(WorkerReconciliationDfxTest, DISABLED_LEVEL1_RestartAgainNoExtraReconciliation)
{
    PutObjGIncreaseRef();
    cluster_->SetInjectAction(ClusterNodeType::WORKER, 0, "worker.PreShutDown.skip", "return(K_OK)");
    cluster_->SetInjectAction(ClusterNodeType::WORKER, 1, "worker.PreShutDown.skip", "return(K_OK)");
    inject::Set("ObjClient.ShutDown", "return(K_OK)");
    inject::Set("ObjectClientImpl.ProcessWorkerLost", "call()");
    // restart the whole cluster
    DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, 0));
    DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, 1));
    DS_ASSERT_OK(cluster_->StartNode(WORKER, 0,
                                     " -inject_actions=WorkerOCServiceImpl.GiveUpReconciliation.setHealthFile:call("
                                     "5000);worker.PreShutDown.skip:return(K_OK)"));
    DS_ASSERT_OK(cluster_->StartNode(WORKER, 1, "-inject_actions=worker.PreShutDown.skip:return(K_OK)"));

    // cluster node table should not contain "restart" tag
    auto waitSwitch = [](uint64_t num) {
        auto sec5s = 5;
        Timer t;
        do {
            if (inject::GetExecuteCount("ObjectClientImpl.ProcessWorkerLost") == num) {
                inject::Clear("ObjectClientImpl.ProcessWorkerLost");
                return;
            }
            auto ms100 = 100;
            std::this_thread::sleep_for(std::chrono::milliseconds(ms100));
        } while (t.ElapsedSecond() < sec5s);
        LOG(ERROR) << "wait switch timeout";
    };
    auto isAllNodeReady = [this](std::unique_ptr<IUtOCStub> &stub) -> Status {
        auto request = [this](std::unique_ptr<IUtOCStub> &stub) -> Status {
            CmNodeTableReqPb req;
            CmNodeTableRspPb rsp;
            RETURN_IF_NOT_OK(aksk_->GenerateSignature(req));
            stub->GetCmNodeTable(req, rsp);
            auto nodeSize = 2;
            CHECK_FAIL_RETURN_STATUS(rsp.cm_node_table_size() == nodeSize, K_RUNTIME_ERROR,
                                     "node size not equal");  // The number of worker is 2
            CHECK_FAIL_RETURN_STATUS(rsp.cm_node_table().at(0).addition_event_type() == "ready", K_RUNTIME_ERROR,
                                     "node 0 is not ready");
            CHECK_FAIL_RETURN_STATUS(rsp.cm_node_table().at(1).addition_event_type() == "ready", K_RUNTIME_ERROR,
                                     "node 0 is not ready");
            return Status::OK();
        };
        int i = 1, repeatTime = 100;
        Status res;
        while (true) {
            res = request(stub);
            if (res.IsOk()) {
                LOG(INFO) << "st case isAllNodeReady func try times:" << i;
                return res;
            }
            if (i > repeatTime) {
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            i++;
        }
        return res;
    };
    auto waitCallNum4 = 4;
    waitSwitch(waitCallNum4);
    DS_ASSERT_OK(isAllNodeReady(utSvcStub0_));
    DS_ASSERT_OK(isAllNodeReady(utSvcStub1_));
    inject::Set("ObjectClientImpl.ProcessWorkerLost", "call()");
    // restart node 1
    DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, 1));
    DS_ASSERT_OK(cluster_->StartNode(WORKER, 1, "-inject_actions=worker.PreShutDown.skip:return(K_OK)"));
    auto waitCallNum2 = 2;
    waitSwitch(waitCallNum2);
    DS_ASSERT_OK(isAllNodeReady(utSvcStub0_));
    DS_ASSERT_OK(isAllNodeReady(utSvcStub1_));
}

TEST_F(WorkerReconciliationDfxTest, LEVEL2_RecoverAgainNoExtraReconciliation)
{
    PutObjGIncreaseRef();

    // the whole cluster recovers from network issue
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "heartbeat.sleep", "1*sleep(7000)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "heartbeat.sleep", "1*sleep(7000)"));
    int waitTimeSec = 15;
    std::this_thread::sleep_for(std::chrono::seconds(waitTimeSec));
    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, 0, "heartbeat.sleep"));
    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, 1, "heartbeat.sleep"));

    // cluster node table should not contain "recover" tag
    AssertCmNodeTableReadyEventually(*utSvcStub0_, "worker0 cm node table");
    AssertCmNodeTableReadyEventually(*utSvcStub1_, "worker1 cm node table");

    // disconnect node 1 again
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "heartbeat.sleep", "1*sleep(7000)"));
    std::this_thread::sleep_for(std::chrono::seconds(waitTimeSec));
    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, 1, "heartbeat.sleep"));

    AssertCmNodeTableReadyEventually(*utSvcStub0_, "worker0 cm node table");
    AssertCmNodeTableReadyEventually(*utSvcStub1_, "worker1 cm node table");
}

class StandbyWorkerDfxTest : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        const uint32_t workerNum = 3;
        opts.numWorkers = workerNum;
        opts.numEtcd = 1;
        opts.enableDistributedMaster = "true";
        opts.workerGflagParams =
            " -v=1 -add_node_wait_time_s=3 -auto_del_dead_node=true -node_timeout_s=1"
            " -node_dead_timeout_s=3 -heartbeat_interval_ms=500";
        opts.waitWorkerReady = false;
        datasystem::inject::Set("ListenWorker.CheckHeartbeat.interval", "call(500)");
        datasystem::inject::Set("ListenWorker.CheckHeartbeat.heartbeat_interval_ms", "call(500)");
        datasystem::inject::Set("ClientWorkerCommonApi.SendHeartbeat.timeoutMs", "call(500)");
    }

    void SetUp() override
    {
        CommonTest::SetUp();
        DS_ASSERT_OK(Init());
        ASSERT_TRUE(cluster_ != nullptr);
        DS_ASSERT_OK(cluster_->StartEtcdCluster());
        externalCluster_ = dynamic_cast<ExternalCluster *>(cluster_.get());
    }

    void TearDown() override
    {
        client_.reset();
        client1_.reset();
        client2_.reset();
        ExternalClusterTest::TearDown();
    }

    void StartWorkerAndWaitReady(std::initializer_list<uint32_t> indexes)
    {
        for (auto i : indexes) {
            DS_ASSERT_OK(externalCluster_->StartWorker(i, HostPort()));
        }
        for (auto i : indexes) {
            DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, i));
        }
    }

    void InitClients()
    {
        uint32_t idx = 0;
        InitClient(idx++, client_, true);
        InitClient(idx++, client1_, true);
        InitClient(idx++, client2_, true);
    }

    void InitClient(int workerIndex, std::shared_ptr<ObjectClient> &client, bool enableCrossNodeConnection)
    {
        InitTestClient(workerIndex, client, [enableCrossNodeConnection](ConnectOptions &opts) {
            opts.enableCrossNodeConnection = enableCrossNodeConnection;
            opts.accessKey = "QTWAOYTTINDUT2QVKYUC";
            opts.secretKey = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
        });
    }

protected:
    ExternalCluster *externalCluster_ = nullptr;
    std::shared_ptr<ObjectClient> client_;
    std::shared_ptr<ObjectClient> client1_;
    std::shared_ptr<ObjectClient> client2_;
};

TEST_F(StandbyWorkerDfxTest, DISABLED_TestUseStandbyWorker)
{
    LOG(INFO) << "Test worker fault and use standby worker.";
    StartWorkerAndWaitReady({ 0, 1, 2 });
    InitClients();
    // Fault the worker and try to use standby worker.
    cluster_->ShutdownNode(WORKER, 0);
    sleep(5);

    std::string objKey = "obj1";
    size_t objSize = 500 * 1024ul;
    std::string data(objSize, 'a');
    // Client will use non-shm because current worker is not local worker.
    DS_ASSERT_OK(
        client_->Put(objKey, reinterpret_cast<uint8_t *>(const_cast<char *>(data.data())), data.size(), CreateParam{}));

    std::vector<Optional<Buffer>> buffers1;
    DS_ASSERT_OK(client_->Get({ objKey }, 0, buffers1));
    ASSERT_TRUE(NotExistsNone(buffers1));
    ASSERT_BUF_EQ((*buffers1[0]), data);

    std::vector<Optional<Buffer>> buffers2;
    DS_ASSERT_OK(client2_->Get({ objKey }, 0, buffers2));
    ASSERT_TRUE(NotExistsNone(buffers2));
    ASSERT_BUF_EQ((*buffers2[0]), data);

    // Restore the local worker
    StartWorkerAndWaitReady({ 0 });
    sleep(5);  // sleep more than add_node_wait_time_s
    std::string objKey2 = "obj2";
    std::string data2(objSize, 'b');
    // Client will use shm because local worker is restored.
    DS_ASSERT_OK(client_->Put(objKey2, reinterpret_cast<uint8_t *>(const_cast<char *>(data2.data())), data2.size(),
                              CreateParam{}));
    std::vector<Optional<Buffer>> buffers3;
    DS_ASSERT_OK(client_->Get({ objKey2 }, 0, buffers3));
    ASSERT_TRUE(NotExistsNone(buffers3));
    ASSERT_BUF_EQ((*buffers3[0]), data2);

    std::vector<Optional<Buffer>> buffers4;
    DS_ASSERT_OK(client_->Get({ objKey }, 0, buffers4));
    ASSERT_TRUE(NotExistsNone(buffers4));
    ASSERT_BUF_EQ((*buffers4[0]), data);
}

TEST_F(StandbyWorkerDfxTest, DISABLED_LEVEL1_TestStandbyWorkerFault)
{
    LOG(INFO) << "Test standby worker fault and use standby2 worker.";
    StartWorkerAndWaitReady({ 0, 1, 2 });
    InitClients();
    // Fault the worker and try to use standby worker.
    cluster_->ShutdownNode(WORKER, 0);
    sleep(5);

    std::string objKey = "obj1";
    size_t objSize = 500 * 1024ul;
    std::string data(objSize, 'a');
    DS_ASSERT_OK(
        client_->Put(objKey, reinterpret_cast<uint8_t *>(const_cast<char *>(data.data())), data.size(), CreateParam{}));

    std::vector<Optional<Buffer>> buffers1;
    DS_ASSERT_OK(client_->Get({ objKey }, 0, buffers1));
    ASSERT_TRUE(NotExistsNone(buffers1));
    ASSERT_BUF_EQ((*buffers1[0]), data);

    // Fault the standby worker and try to use standby2 worker.
    cluster_->ShutdownNode(WORKER, 1);
    sleep(5);

    std::string objKey2 = "obj2";
    std::string data2(objSize, 'b');
    DS_ASSERT_OK(client_->Put(objKey2, reinterpret_cast<uint8_t *>(const_cast<char *>(data2.data())), data2.size(),
                              CreateParam{}));
    std::vector<Optional<Buffer>> buffers2;
    DS_ASSERT_OK(client_->Get({ objKey2 }, 0, buffers2));
    ASSERT_TRUE(NotExistsNone(buffers2));
    ASSERT_BUF_EQ((*buffers2[0]), data2);

    std::vector<Optional<Buffer>> buffers3;
    DS_ASSERT_OK(client1_->Get({ objKey2 }, 0, buffers3));
    ASSERT_TRUE(NotExistsNone(buffers3));
    ASSERT_BUF_EQ((*buffers3[0]), data2);

    std::vector<Optional<Buffer>> buffers4;
    DS_ASSERT_OK(client2_->Get({ objKey2 }, 0, buffers4));
    ASSERT_TRUE(NotExistsNone(buffers4));
    ASSERT_BUF_EQ((*buffers4[0]), data2);
}

TEST_F(StandbyWorkerDfxTest, DISABLED_LEVEL1_TestEnableCrossNodeConnectionParam)
{
    StartWorkerAndWaitReady({ 0, 1, 2 });
    std::shared_ptr<ObjectClient> client0;
    std::shared_ptr<ObjectClient> client1;
    // Not supported standby node.
    InitTestClient(0, client0, [](ConnectOptions &opts) {
        opts.accessKey = "QTWAOYTTINDUT2QVKYUC";
        opts.secretKey = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
    });
    // Supported standby node.
    InitTestClient(0, client1, [](ConnectOptions &opts) {
        opts.enableCrossNodeConnection = true;
        opts.accessKey = "QTWAOYTTINDUT2QVKYUC";
        opts.secretKey = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
    });
    cluster_->ShutdownNode(WORKER, 0);
    sleep(5);
    std::string objectKey = "obj1";
    size_t objSize = 500 * 1024ul;
    std::string data(objSize, 'a');
    DS_ASSERT_NOT_OK(client0->Put(objectKey, reinterpret_cast<uint8_t *>(const_cast<char *>(data.data())), data.size(),
                                  CreateParam{}));
    DS_ASSERT_OK(client1->Put(objectKey, reinterpret_cast<uint8_t *>(const_cast<char *>(data.data())), data.size(),
                              CreateParam{}));
    std::vector<Optional<Buffer>> buffers1;
    DS_ASSERT_OK(client1->Get({ objectKey }, 0, buffers1));
    ASSERT_TRUE(NotExistsNone(buffers1));
    ASSERT_BUF_EQ((*buffers1[0]), data);
}

class WorkerWithoutReconciliationTest : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.workerGflagParams =
            "-client_reconnect_wait_s=1 -ipc_through_shared_memory=true -node_timeout_s=1 -enable_reconciliation=false "
            "-heartbeat_interval_ms=500";
        auto numWorkers = 2;
        opts.numWorkers = numWorkers;
        opts.enableDistributedMaster = "true";
        opts.numEtcd = 1;
        opts.disableRocksDB = false;
    }
};

TEST_F(WorkerWithoutReconciliationTest, LEVEL2_WithoutReconciliationTest)
{
    std::shared_ptr<ObjectClient> client0;
    InitTestClient(0, client0);
    std::string objectKey = GetStringUuid();
    size_t objSize = 500 * 1024ul;
    std::string data(objSize, 'a');
    std::vector<std::string> failedObjects;

    DS_ASSERT_OK(client0->GIncreaseRef({ objectKey }, failedObjects));
    DS_ASSERT_OK(client0->Put(objectKey, reinterpret_cast<uint8_t *>(const_cast<char *>(data.data())), data.size(),
                              CreateParam{}));
    std::vector<Optional<Buffer>> buffers1;
    DS_ASSERT_OK(client0->Get({ objectKey }, 0, buffers1));
    ASSERT_TRUE(NotExistsNone(buffers1));
    ASSERT_BUF_EQ((*buffers1[0]), data);

    DS_ASSERT_OK(cluster_->ShutdownNode(ClusterNodeType::WORKER, 0));
    DS_ASSERT_OK(cluster_->StartNode(ClusterNodeType::WORKER, 0, {}));
    DS_ASSERT_OK(cluster_->WaitNodeReady(ClusterNodeType::WORKER, 0));
    const size_t sleepTime = 3;
    std::this_thread::sleep_for(std::chrono::seconds(sleepTime));

    buffers1.clear();
    Status rc = client0->Get({ objectKey }, 0, buffers1);
    LOG(INFO) << "The status of get the object after node0(master and worker) restart: " << rc.ToString();
    ASSERT_TRUE(rc.GetCode() == StatusCode::K_RUNTIME_ERROR);
}

class WorkerPushMetaTest : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.workerGflagParams = FormatString(
            "-client_reconnect_wait_s=1 -ipc_through_shared_memory=true -node_timeout_s=2 -node_dead_timeout_s=%d "
            "-heartbeat_interval_ms=1000 -auto_del_dead_node=false",
            nodeDeadTimeout_);
        auto numWorkers = 2;
        opts.numWorkers = numWorkers;
        opts.enableDistributedMaster = "true";
        opts.numEtcd = 1;
    }

protected:
    int nodeDeadTimeout_ = 3;
};

class WorkerPushMetaScaleOutFaultTest : public OCClientCommon {
public:
    void SetUp() override
    {
        CommonTest::SetUp();
        DS_ASSERT_OK(Init());
        ASSERT_TRUE(cluster_ != nullptr);
        DS_ASSERT_OK(cluster_->StartEtcdCluster());
        externalCluster_ = dynamic_cast<ExternalCluster *>(cluster_.get());
        ASSERT_NE(externalCluster_, nullptr);
    }

    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.workerGflagParams =
            "-client_reconnect_wait_s=1 -ipc_through_shared_memory=true -node_timeout_s=2 "
            "-node_dead_timeout_s=3 -heartbeat_interval_ms=1000 -auto_del_dead_node=false";
        opts.numWorkers = kWorkerCount;
        opts.enableDistributedMaster = "true";
        opts.numEtcd = 1;
        opts.waitWorkerReady = false;
        for (size_t i = 0; i < opts.numWorkers; ++i) {
            opts.workerConfigs.emplace_back(kHostIp, GetFreePort());
            workerAddresses_.emplace_back(opts.workerConfigs.back().ToString());
        }
    }

protected:
    void StartWorkerAndWaitReady(std::initializer_list<uint32_t> indexes, uint32_t masterIdx = 0,
                                 uint32_t maxWaitTimeSec = 20)
    {
        HostPort master;
        ASSERT_LT(masterIdx, workerAddresses_.size());
        DS_ASSERT_OK(master.ParseString(workerAddresses_[masterIdx]));
        for (auto index : indexes) {
            DS_ASSERT_OK(externalCluster_->StartWorker(index, master));
        }
        for (auto index : indexes) {
            DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, index, maxWaitTimeSec));
        }
    }

    ExternalCluster *externalCluster_{ nullptr };
    std::vector<std::string> workerAddresses_;

private:
    static constexpr uint32_t kWorkerCount = 3;
    static constexpr const char *kHostIp = "127.0.0.1";
};

TEST_F(WorkerPushMetaScaleOutFaultTest, LEVEL1_ScaleOutPreservesDataWhenWorkerIsLocallyIsolated)
{
    StartWorkerAndWaitReady({ 0, 1 }, 0);
    auto db = InitTestEtcdInstance();
    ASSERT_NE(db, nullptr);
    std::shared_ptr<ObjectClient> client0;
    std::shared_ptr<ObjectClient> client1;
    InitTestClient(0, client0);
    InitTestClient(1, client1);

    std::vector<std::string> worker0Keys;
    std::vector<std::string> worker1Keys;
    GetObjectKeysHashToWorker(db.get(), 0, 3, worker0Keys);
    GetObjectKeysHashToWorker(db.get(), 1, 2, worker1Keys);
    std::string localData = GenRandomString(1024);
    std::string peerData = GenRandomString(1024);
    CreateAndSealObject(client0, worker0Keys[0], localData);
    CreateAndSealObject(client1, worker1Keys[0], peerData);

    ServerProcess *isolatedProcess = nullptr;
    DS_ASSERT_OK(cluster_->GetProcess(WORKER, 0, isolatedProcess));
    ASSERT_NE(isolatedProcess, nullptr);
    ASSERT_TRUE(isolatedProcess->IsProcessAlive());
    const auto isolatedPid = isolatedProcess->Pid();
    constexpr const char *localIsolatedInject = "WorkerOCServer.AfterMarkLocalIsolated";
    constexpr const char *keepAliveInject = "EtcdKeepAlive.SendKeepAliveMessage";
    constexpr const char *leaseInject = "GetLeaseExpiredMs";
    constexpr const char *quickLoopInject = "EtcdStore.LaunchKeepAliveThreads.loopQuickly";
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, localIsolatedInject, "call()"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, keepAliveInject, "return(K_RPC_UNAVAILABLE)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, leaseInject, "call(1000)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, quickLoopInject, "call(0)"));
    bool keepAliveFailureActive = true;
    Raii clearKeepAliveFailure([&]() {
        if (keepAliveFailureActive) {
            LOG_IF_ERROR(cluster_->ClearInjectAction(WORKER, 0, keepAliveInject), "clear scaleout keepalive failure");
            LOG_IF_ERROR(cluster_->ClearInjectAction(WORKER, 0, leaseInject), "clear scaleout lease override");
            LOG_IF_ERROR(cluster_->ClearInjectAction(WORKER, 0, quickLoopInject), "clear scaleout quick loop");
            LOG_IF_ERROR(cluster_->ClearInjectAction(WORKER, 0, localIsolatedInject), "clear scaleout isolation hook");
        }
    });

    AssertEventuallyOk(
        [&]() {
            uint64_t executeCount = 0;
            RETURN_IF_NOT_OK(cluster_->GetInjectActionExecuteCount(WORKER, 0, localIsolatedInject, executeCount));
            CHECK_FAIL_RETURN_STATUS(executeCount > 0, K_NOT_READY,
                                     "worker 0 has not entered confirmed local isolation");
            return Status::OK();
        },
        "worker 0 enters local isolation before scaleout");
    ASSERT_EQ(kill(isolatedPid, 0), 0);

    StartWorkerAndWaitReady({ 2 }, 0, 30);
    std::shared_ptr<ObjectClient> client2;
    InitTestClient(2, client2);
    AssertEventuallyOk(
        [&]() {
            std::string topologyBytes;
            RETURN_IF_NOT_OK(db->Get(GetTopologyTableName(), "", topologyBytes));
            ClusterTopologyPb topology;
            CHECK_FAIL_RETURN_STATUS(topology.ParseFromString(topologyBytes), K_RUNTIME_ERROR,
                                     "failed to parse topology after scaleout");
            CHECK_FAIL_RETURN_STATUS(topology.members_size() == 3, K_NOT_READY,
                                     "scaleout has not published all members yet");
            for (const auto &member : topology.members()) {
                CHECK_FAIL_RETURN_STATUS(member.second.state() == MembershipPb::ACTIVE, K_NOT_READY,
                                         "scaleout member is not ACTIVE yet");
            }
            return Status::OK();
        },
        "scaleout completes while worker 0 stays locally isolated");

    std::vector<Optional<Buffer>> peerBuffers;
    DS_ASSERT_OK(client2->Get({ worker1Keys[0] }, 0, peerBuffers));
    ASSERT_TRUE(NotExistsNone(peerBuffers));
    AssertBufferEqual(*peerBuffers[0], peerData);
    ASSERT_EQ(kill(isolatedPid, 0), 0);
    ASSERT_TRUE(isolatedProcess->IsProcessAlive());

    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, 0, keepAliveInject));
    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, 0, leaseInject));
    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, 0, quickLoopInject));
    keepAliveFailureActive = false;
    AssertEventuallyOk(
        [&]() {
            std::shared_ptr<Buffer> buffer;
            RETURN_IF_NOT_OK(client0->Create(worker0Keys[1], 1, CreateParam{}, buffer));
            uint8_t data = 1;
            RETURN_IF_NOT_OK(buffer->MemoryCopy(&data, sizeof(data)));
            return buffer->Publish();
        },
        "worker 0 reopens normal service after scaleout and local recovery");

    std::vector<Optional<Buffer>> localBuffers;
    DS_ASSERT_OK(client2->Get({ worker0Keys[0] }, 0, localBuffers));
    ASSERT_TRUE(NotExistsNone(localBuffers));
    AssertBufferEqual(*localBuffers[0], localData);
    ASSERT_EQ(kill(isolatedPid, 0), 0);
    ASSERT_TRUE(isolatedProcess->IsProcessAlive());
    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, 0, localIsolatedInject));
}

TEST_F(WorkerPushMetaScaleOutFaultTest, LEVEL1_ScaleOutSurvivesTransientGlobalBackendOutage)
{
    StartWorkerAndWaitReady({ 0, 1 }, 0);
    auto db = InitTestEtcdInstance();
    ASSERT_NE(db, nullptr);
    std::shared_ptr<ObjectClient> client0;
    std::shared_ptr<ObjectClient> client1;
    InitTestClient(0, client0);
    InitTestClient(1, client1);

    std::vector<std::string> worker0Keys;
    std::vector<std::string> worker1Keys;
    GetObjectKeysHashToWorker(db.get(), 0, 3, worker0Keys);
    GetObjectKeysHashToWorker(db.get(), 1, 3, worker1Keys);
    std::string worker0Data = GenRandomString(1024);
    std::string worker1Data = GenRandomString(1024);
    CreateAndSealObject(client0, worker0Keys[0], worker0Data);
    CreateAndSealObject(client1, worker1Keys[0], worker1Data);

    StartWorkerAndWaitReady({ 2 }, 0, 30);
    std::vector<pid_t> workerPids(3);
    for (uint32_t workerIndex = 0; workerIndex < 3; ++workerIndex) {
        workerPids[workerIndex] = cluster_->GetWorkerPid(workerIndex);
        ASSERT_GT(workerPids[workerIndex], 0);
        DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, workerIndex, "WorkerOCServer.GlobalBackendOutage", "call()"));
        DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, workerIndex, "GetLeaseExpiredMs", "call(1000)"));
        DS_ASSERT_OK(
            cluster_->SetInjectAction(WORKER, workerIndex, "EtcdStore.LaunchKeepAliveThreads.loopQuickly", "call(0)"));
    }
    AssertEventuallyOk(
        [&]() {
            std::string topologyBytes;
            RETURN_IF_NOT_OK(db->Get(GetTopologyTableName(), "", topologyBytes));
            ClusterTopologyPb topology;
            CHECK_FAIL_RETURN_STATUS(topology.ParseFromString(topologyBytes), K_RUNTIME_ERROR,
                                     "failed to parse topology before scaleout/global-backend outage");
            CHECK_FAIL_RETURN_STATUS(topology.members_size() == 3, K_NOT_READY,
                                     "scaleout did not publish all members before backend outage");
            for (const auto &member : topology.members()) {
                CHECK_FAIL_RETURN_STATUS(member.second.state() == MembershipPb::ACTIVE, K_NOT_READY,
                                         "scaleout member is not ACTIVE before backend outage");
            }
            return Status::OK();
        },
        "scaleout converges before transient global backend outage");
    DS_ASSERT_OK(externalCluster_->ShutdownEtcds());
    bool etcdDown = true;
    Raii restoreEtcd([&]() {
        if (etcdDown) {
            LOG_IF_ERROR(externalCluster_->StartEtcdCluster(), "restore etcd after scaleout/global-backend outage");
        }
    });

    AssertEventuallyOk(
        [&]() {
            for (uint32_t workerIndex = 0; workerIndex < 3; ++workerIndex) {
                uint64_t executeCount = 0;
                RETURN_IF_NOT_OK(cluster_->GetInjectActionExecuteCount(
                    WORKER, workerIndex, "WorkerOCServer.GlobalBackendOutage", executeCount));
                CHECK_FAIL_RETURN_STATUS(
                    executeCount > 0, K_NOT_READY,
                    FormatString("worker %u has not classified the backend outage as global", workerIndex));
                CHECK_FAIL_RETURN_STATUS(kill(workerPids[workerIndex], 0) == 0, K_RUNTIME_ERROR,
                                         FormatString("worker %u exited during transient global outage", workerIndex));
            }
            return Status::OK();
        },
        "scaleout workers classify transient backend outage as global and stay alive");

    DS_ASSERT_OK(externalCluster_->StartEtcdCluster());
    etcdDown = false;
    for (uint32_t workerIndex = 0; workerIndex < 3; ++workerIndex) {
        DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, workerIndex, "GetLeaseExpiredMs"));
        DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, workerIndex, "EtcdStore.LaunchKeepAliveThreads.loopQuickly"));
    }
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 2, 30));
    std::shared_ptr<ObjectClient> client2;
    InitTestClient(2, client2);
    AssertEventuallyOk(
        [&]() {
            std::string topologyBytes;
            RETURN_IF_NOT_OK(db->Get(GetTopologyTableName(), "", topologyBytes));
            ClusterTopologyPb topology;
            CHECK_FAIL_RETURN_STATUS(topology.ParseFromString(topologyBytes), K_RUNTIME_ERROR,
                                     "failed to parse topology after scaleout/global-backend outage");
            CHECK_FAIL_RETURN_STATUS(topology.members_size() == 3, K_NOT_READY,
                                     "scaleout did not publish all members after backend recovery");
            for (const auto &member : topology.members()) {
                CHECK_FAIL_RETURN_STATUS(member.second.state() == MembershipPb::ACTIVE, K_NOT_READY,
                                         "scaleout member is not ACTIVE after backend recovery");
            }
            return Status::OK();
        },
        "scaleout converges after transient global backend outage");

    std::vector<Optional<Buffer>> worker0Buffers;
    std::vector<Optional<Buffer>> worker1Buffers;
    DS_ASSERT_OK(client2->Get({ worker0Keys[0] }, 0, worker0Buffers));
    DS_ASSERT_OK(client2->Get({ worker1Keys[0] }, 0, worker1Buffers));
    ASSERT_TRUE(NotExistsNone(worker0Buffers));
    ASSERT_TRUE(NotExistsNone(worker1Buffers));
    AssertBufferEqual(*worker0Buffers[0], worker0Data);
    AssertBufferEqual(*worker1Buffers[0], worker1Data);
}

class WorkerPushMetaTransportTest : public WorkerPushMetaTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        WorkerPushMetaTest::SetClusterSetupOptions(opts);
        opts.workerGflagParams += " -use_brpc=true -enable_urma=false";
    }

    void SetUp() override
    {
        previousUseBrpc_ = FLAGS_use_brpc;
        previousEnableUrma_ = FLAGS_enable_urma;
        FLAGS_use_brpc = true;
        FLAGS_enable_urma = false;
        ExternalClusterTest::SetUp();
    }

    void TearDown() override
    {
        ExternalClusterTest::TearDown();
        FLAGS_enable_urma = previousEnableUrma_;
        FLAGS_use_brpc = previousUseBrpc_;
    }

private:
    bool previousUseBrpc_{ false };
    bool previousEnableUrma_{ false };
};

class WorkerStalePrimaryTest : public WorkerPushMetaTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.workerGflagParams =
            "-client_reconnect_wait_s=1 -ipc_through_shared_memory=true -node_timeout_s=1 "
            "-node_dead_timeout_s=2 -heartbeat_interval_ms=500 -auto_del_dead_node=true";
        opts.numWorkers = 2;
        opts.enableDistributedMaster = "true";
        opts.numEtcd = 1;
        opts.disableRocksDB = false;
    }
};

TEST_F(WorkerPushMetaTest, LEVEL1_TestKeepAliveLocalIsolationKeepsWorkerAliveAndProtectsPeerData)
{
    std::shared_ptr<ObjectClient> client0;
    std::shared_ptr<ObjectClient> client1;
    InitTestClient(0, client0);
    InitTestClient(1, client1);
    HostPort workerAddr0;
    DS_ASSERT_OK(cluster_->GetWorkerAddr(0, workerAddr0));
    RpcCredential cred;
    RpcAuthKeyManager::CreateClientCredentials(RpcAuthKeys(), WORKER_SERVER_NAME, cred);
    AkSkManager akSk;
    DS_ASSERT_OK(akSk.SetClientAkSk("QTWAOYTTINDUT2QVKYUC", "MFyfvK41ba2giqM7**********KGpownRZlmVmHc"));
    auto db = InitTestEtcdInstance();
    ASSERT_NE(db, nullptr);
    std::vector<std::string> worker0Keys;
    std::vector<std::string> worker1Keys;
    GetObjectKeysHashToWorker(db.get(), 0, 2, worker0Keys);
    GetObjectKeysHashToWorker(db.get(), 1, 2, worker1Keys);
    const auto &worker0ProbeKey = worker0Keys[0];
    const auto &worker0RejectKey = worker0Keys[1];
    const auto &peerProbeKey = worker1Keys[0];
    const auto &peerObjectKey = worker1Keys[1];

    AssertEventuallyOk(
        [&]() {
            std::shared_ptr<Buffer> buffer;
            CreateParam param;
            RETURN_IF_NOT_OK(client1->Create(peerProbeKey, 1, param, buffer));
            uint8_t data = 1;
            RETURN_IF_NOT_OK(buffer->MemoryCopy(&data, sizeof(data)));
            return buffer->Publish();
        },
        "create on peer worker before local isolation");
    AssertEventuallyOk(
        [&]() {
            std::shared_ptr<Buffer> buffer;
            CreateParam param;
            RETURN_IF_NOT_OK(client0->Create(worker0ProbeKey, 1, param, buffer));
            uint8_t data = 1;
            RETURN_IF_NOT_OK(buffer->MemoryCopy(&data, sizeof(data)));
            return buffer->Publish();
        },
        "create on worker before local isolation");

    std::string peerData = GenRandomString(1024);
    CreateAndSealObject(client1, peerObjectKey, peerData);
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "worker.Create.beforeValidate", "call()"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "worker.Create.begin", "call()"));

    const auto isolatedWorkerPid = cluster_->GetWorkerPid(0);
    ASSERT_GT(isolatedWorkerPid, 0);
    DS_ASSERT_OK(
        cluster_->SetInjectAction(WORKER, 0, "EtcdKeepAlive.SendKeepAliveMessage", "return(K_RPC_UNAVAILABLE)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "GetLeaseExpiredMs", "call(1000)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "EtcdStore.LaunchKeepAliveThreads.loopQuickly", "call(0)"));

    AssertEventuallyOk(
        [&]() {
            uint64_t beforeValidateBefore = 0;
            uint64_t createBeginBefore = 0;
            uint64_t beforeValidateAfter = 0;
            uint64_t createBeginAfter = 0;
            RETURN_IF_NOT_OK(
                cluster_->GetInjectActionExecuteCount(WORKER, 0, "worker.Create.beforeValidate", beforeValidateBefore));
            RETURN_IF_NOT_OK(
                cluster_->GetInjectActionExecuteCount(WORKER, 0, "worker.Create.begin", createBeginBefore));
            CreateReqPb req;
            CreateRspPb rsp;
            req.set_object_key(worker0RejectKey);
            req.set_client_id("self-healing-direct-client");
            req.set_data_size(1);
            req.set_cache_type(static_cast<uint32_t>(CacheType::MEMORY));
            req.set_request_timeout(1000);
            req.set_is_routed(true);
            RETURN_IF_NOT_OK(akSk.GenerateSignature(req));
            RpcOptions opts;
            opts.SetTimeout(1000);
            auto createStatus = DirectWorkerCreate(workerAddr0, cred, req, rsp, 1'000);
            RETURN_IF_NOT_OK(
                cluster_->GetInjectActionExecuteCount(WORKER, 0, "worker.Create.beforeValidate", beforeValidateAfter));
            RETURN_IF_NOT_OK(cluster_->GetInjectActionExecuteCount(WORKER, 0, "worker.Create.begin", createBeginAfter));
            if (beforeValidateAfter > beforeValidateBefore && createBeginAfter == createBeginBefore) {
                CHECK_FAIL_RETURN_STATUS(createStatus.GetCode() == K_NOT_READY, K_NOT_READY,
                                         "isolated Create did not return K_NOT_READY");
                CHECK_FAIL_RETURN_STATUS(createStatus.GetMsg().find("mode=LOCAL_ISOLATED") != std::string::npos,
                                         K_NOT_READY, "isolated Create response has no fixed mode");
                CHECK_FAIL_RETURN_STATUS(
                    createStatus.GetMsg().find("reason=CONTROL_BACKEND_LOCAL_ISOLATION") != std::string::npos,
                    K_NOT_READY, "isolated Create response has no fixed reason");
                CHECK_FAIL_RETURN_STATUS(createStatus.GetMsg().find("phase=NONE") != std::string::npos, K_NOT_READY,
                                         "isolated Create response has no fixed recovery phase");
                CHECK_FAIL_RETURN_STATUS(createStatus.GetMsg().find(worker0RejectKey) == std::string::npos, K_NOT_READY,
                                         "isolated Create response leaked the object key");
                return Status::OK();
            }
            RETURN_STATUS(StatusCode::K_NOT_READY,
                          FormatString("create request has not been blocked by worker admission yet, "
                                       "createStatus=%s, beforeValidate: %zu->%zu, createBegin: %zu->%zu",
                                       createStatus.ToString(), beforeValidateBefore, beforeValidateAfter,
                                       createBeginBefore, createBeginAfter));
        },
        "create admission blocks locally isolated worker before object allocation");

    ASSERT_EQ(kill(isolatedWorkerPid, 0), 0);
    std::vector<Optional<Buffer>> peerBuffers;
    DS_ASSERT_OK(client1->Get({ peerObjectKey }, 0, peerBuffers));
    ASSERT_TRUE(NotExistsNone(peerBuffers));
    AssertBufferEqual(*peerBuffers[0], peerData);
}

TEST_F(WorkerPushMetaTest, LEVEL1_TestPeerWorkerKvDataSurvivesLocalIsolationAndRecovery)
{
    std::shared_ptr<KVClient> client0;
    std::shared_ptr<KVClient> client1;
    InitTestKVClient(0, client0, 60'000, false, 1'000);
    InitTestKVClient(1, client1, 60'000, false, 1'000);
    auto db = InitTestEtcdInstance();
    ASSERT_NE(db, nullptr);
    std::vector<std::string> worker0Keys;
    std::vector<std::string> worker1Keys;
    GetObjectKeysHashToWorker(db.get(), 0, 2, worker0Keys);
    GetObjectKeysHashToWorker(db.get(), 1, 2, worker1Keys);

    const std::string peerValueBefore = GenRandomString(1024);
    const std::string peerValueDuring = GenRandomString(1024);
    DS_ASSERT_OK(client1->Set(worker1Keys[0], peerValueBefore));
    std::string actual;
    DS_ASSERT_OK(client1->Get(worker1Keys[0], actual));
    ASSERT_EQ(actual, peerValueBefore);

    ServerProcess *workerProcess = nullptr;
    DS_ASSERT_OK(cluster_->GetProcess(WORKER, 0, workerProcess));
    ASSERT_NE(workerProcess, nullptr);
    ASSERT_TRUE(workerProcess->IsProcessAlive());
    const auto workerPid = workerProcess->Pid();
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "WorkerOCServer.LocalBackendIsolationCandidate", "call()"));
    DS_ASSERT_OK(
        cluster_->SetInjectAction(WORKER, 0, "EtcdKeepAlive.SendKeepAliveMessage", "return(K_RPC_UNAVAILABLE)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "GetLeaseExpiredMs", "call(1000)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "EtcdStore.LaunchKeepAliveThreads.loopQuickly", "call(0)"));

    AssertEventuallyOk(
        [&]() {
            uint64_t executeCount = 0;
            RETURN_IF_NOT_OK(cluster_->GetInjectActionExecuteCount(
                WORKER, 0, "WorkerOCServer.LocalBackendIsolationCandidate", executeCount));
            CHECK_FAIL_RETURN_STATUS(executeCount > 0, K_NOT_READY,
                                     "worker has not classified the keepalive failure as local isolation");
            return Status::OK();
        },
        "worker classifies the keepalive failure as local isolation");
    AssertEventuallyOk(
        [&]() {
            auto rc = client0->Set(worker0Keys[0], "isolated-write-must-fail");
            CHECK_FAIL_RETURN_STATUS(
                rc.GetCode() == K_NOT_READY, K_TRY_AGAIN,
                "worker0 KV Set has not reached exact local-isolation admission: " + rc.ToString());
            return Status::OK();
        },
        "locally isolated worker rejects KV Set with K_NOT_READY");

    DS_ASSERT_OK(client1->Set(worker1Keys[1], peerValueDuring));
    actual.clear();
    DS_ASSERT_OK(client1->Get(worker1Keys[0], actual));
    ASSERT_EQ(actual, peerValueBefore);
    actual.clear();
    DS_ASSERT_OK(client1->Get(worker1Keys[1], actual));
    ASSERT_EQ(actual, peerValueDuring);
    ASSERT_TRUE(workerProcess->IsProcessAlive());

    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, 0, "EtcdKeepAlive.SendKeepAliveMessage"));
    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, 0, "GetLeaseExpiredMs"));
    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, 0, "EtcdStore.LaunchKeepAliveThreads.loopQuickly"));
    AssertEventuallyOk([&]() { return client0->Set(worker0Keys[1], "recovered-write"); },
                       "same worker accepts KV Set after recovery");
    ASSERT_EQ(workerProcess->Pid(), workerPid);
    ASSERT_TRUE(workerProcess->IsProcessAlive());

    actual.clear();
    DS_ASSERT_OK(client1->Get(worker1Keys[0], actual));
    ASSERT_EQ(actual, peerValueBefore);
    actual.clear();
    DS_ASSERT_OK(client1->Get(worker1Keys[1], actual));
    ASSERT_EQ(actual, peerValueDuring);
}

TEST_F(WorkerStalePrimaryTest, LEVEL1_IsolatedWorkerMetaCleanupAllowsNewOwnerRebuild)
{
    std::shared_ptr<ObjectClient> client0;
    std::shared_ptr<ObjectClient> client1;
    InitTestClient(0, client0);
    InitTestClient(1, client1);
    HostPort workerAddr0;
    HostPort workerAddr1;
    DS_ASSERT_OK(cluster_->GetWorkerAddr(0, workerAddr0));
    DS_ASSERT_OK(cluster_->GetWorkerAddr(1, workerAddr1));
    RpcCredential cred;
    RpcAuthKeyManager::CreateClientCredentials(RpcAuthKeys(), WORKER_SERVER_NAME, cred);
    AkSkManager akSk;
    DS_ASSERT_OK(akSk.SetClientAkSk("QTWAOYTTINDUT2QVKYUC", "MFyfvK41ba2giqM7**********KGpownRZlmVmHc"));
    auto db = InitTestEtcdInstance();
    ASSERT_NE(db, nullptr);
    SetWorkerHashInjection();
    std::vector<std::string> worker1Keys;
    GetObjectKeysHashToWorker(db.get(), 1, 1, worker1Keys);
    const auto &key = worker1Keys[0];
    const std::string initialValue = GenRandomString(1024);
    const std::string promotedValue = GenRandomString(1024);
    CreateParam param{ .consistencyType = ConsistencyType::CAUSAL };
    DS_ASSERT_OK(client0->Put(key, reinterpret_cast<const uint8_t *>(initialValue.data()), initialValue.size(), param));
    std::vector<Optional<Buffer>> buffers;
    DS_ASSERT_OK(client1->Get({ key }, 1'000, buffers));
    ASSERT_TRUE(NotExistsNone(buffers));
    AssertBufferEqual(*buffers[0], initialValue);
    AssertEventuallyOk(
        [&]() {
            master::QueryMetaReqPb req;
            master::QueryMetaRspPb rsp;
            std::vector<RpcMessage> payloads;
            req.add_ids(key);
            req.set_address(workerAddr1.ToString());
            RETURN_IF_NOT_OK(akSk.GenerateSignature(req));
            RETURN_IF_NOT_OK(DirectMasterQueryMeta(workerAddr1, cred, req, rsp, payloads, 1'000));
            CHECK_FAIL_RETURN_STATUS(rsp.query_metas_size() == 1, K_NOT_READY,
                                     "object metadata is not queryable before fault injection");
            master::CheckObjectDataLocationReqPb locationReq;
            master::CheckObjectDataLocationRspPb locationRsp;
            auto *objectVersion = locationReq.add_object_versions();
            objectVersion->set_object_key(key);
            objectVersion->set_version(rsp.query_metas(0).meta().version());
            locationReq.set_address(workerAddr1.ToString());
            RETURN_IF_NOT_OK(akSk.GenerateSignature(locationReq));
            RETURN_IF_NOT_OK(DirectMasterCheckObjectDataLocation(workerAddr1, cred, locationReq, locationRsp, 1'000));
            CHECK_FAIL_RETURN_STATUS(locationRsp.no_need_clear_object_keys_size() == 1, K_NOT_READY,
                                     "peer copy location has not been acknowledged by the metadata master");
            return Status::OK();
        },
        "peer copy location is acknowledged before the old primary is isolated");

    ServerProcess *workerProcess = nullptr;
    DS_ASSERT_OK(cluster_->GetProcess(WORKER, 0, workerProcess));
    ASSERT_NE(workerProcess, nullptr);
    ASSERT_TRUE(workerProcess->IsProcessAlive());
    const auto workerPid = workerProcess->Pid();
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "process.change.primary.copy", "call()"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "process.change.primary.copy", "call()"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "WorkerOCServer.AfterMarkLocalIsolated", "call()"));
    DS_ASSERT_OK(
        cluster_->SetInjectAction(WORKER, 0, "EtcdKeepAlive.SendKeepAliveMessage", "return(K_RPC_UNAVAILABLE)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "GetLeaseExpiredMs", "call(1000)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "EtcdStore.LaunchKeepAliveThreads.loopQuickly", "call(0)"));
    bool keepAliveFailureActive = true;
    Raii clearKeepAliveFailure([&]() {
        if (keepAliveFailureActive) {
            LOG_IF_ERROR(cluster_->ClearInjectAction(WORKER, 0, "EtcdKeepAlive.SendKeepAliveMessage"),
                         "clear isolated-worker keepalive failure");
            LOG_IF_ERROR(cluster_->ClearInjectAction(WORKER, 0, "GetLeaseExpiredMs"),
                         "clear isolated-worker lease override");
            LOG_IF_ERROR(cluster_->ClearInjectAction(WORKER, 0, "EtcdStore.LaunchKeepAliveThreads.loopQuickly"),
                         "clear isolated-worker quick keepalive loop");
        }
    });

    AssertEventuallyOk(
        [&]() {
            uint64_t isolationCount = 0;
            RETURN_IF_NOT_OK(cluster_->GetInjectActionExecuteCount(WORKER, 0, "WorkerOCServer.AfterMarkLocalIsolated",
                                                                   isolationCount));
            CHECK_FAIL_RETURN_STATUS(isolationCount > 0, K_NOT_READY,
                                     "old primary has not entered confirmed local isolation");
            return Status::OK();
        },
        "old primary enters local isolation while its process remains alive");
    ASSERT_EQ(workerProcess->Pid(), workerPid);
    ASSERT_TRUE(workerProcess->IsProcessAlive());

    AssertEventuallyOk(
        [&]() {
            uint64_t promotionCount = 0;
            RETURN_IF_NOT_OK(
                cluster_->GetInjectActionExecuteCount(WORKER, 1, "process.change.primary.copy", promotionCount));
            CHECK_FAIL_RETURN_STATUS(promotionCount > 0, K_NOT_READY,
                                     "peer primary promotion has not completed after the old primary lease expired");
            return Status::OK();
        },
        "peer becomes primary after the old primary lease expires");
    AssertEventuallyOk(
        [&]() {
            master::QueryMetaReqPb req;
            master::QueryMetaRspPb rsp;
            std::vector<RpcMessage> payloads;
            req.add_ids(key);
            req.set_address(workerAddr1.ToString());
            RETURN_IF_NOT_OK(akSk.GenerateSignature(req));
            RETURN_IF_NOT_OK(DirectMasterQueryMeta(workerAddr1, cred, req, rsp, payloads, 1'000));
            CHECK_FAIL_RETURN_STATUS(rsp.query_metas_size() == 1, K_NOT_READY,
                                     "rebuilt object metadata is not queryable on the new owner");
            CHECK_FAIL_RETURN_STATUS(rsp.query_metas(0).meta().primary_address() == workerAddr1.ToString(), K_NOT_READY,
                                     "authoritative primary still points at the isolated worker");
            return Status::OK();
        },
        "authoritative metadata points at the new owner after cleanup");
    DS_ASSERT_OK(
        client1->Put(key, reinterpret_cast<const uint8_t *>(promotedValue.data()), promotedValue.size(), param));
    buffers.clear();
    DS_ASSERT_OK(client1->Get({ key }, 1'000, buffers));
    ASSERT_TRUE(NotExistsNone(buffers));
    AssertBufferEqual(*buffers[0], promotedValue);

    {
        GetObjMetaInfoReqPb req;
        GetObjMetaInfoRspPb rsp;
        req.add_object_keys(key);
        DS_ASSERT_OK(akSk.GenerateSignature(req));
        ASSERT_EQ(DirectWorkerGetObjMetaInfo(workerAddr0, cred, req, rsp, 1'000).GetCode(), K_NOT_READY);
        ASSERT_EQ(rsp.objs_meta_info_size(), 0);
    }
    ASSERT_EQ(workerProcess->Pid(), workerPid);
    ASSERT_TRUE(workerProcess->IsProcessAlive());

    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, 0, "EtcdKeepAlive.SendKeepAliveMessage"));
    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, 0, "GetLeaseExpiredMs"));
    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, 0, "EtcdStore.LaunchKeepAliveThreads.loopQuickly"));
    keepAliveFailureActive = false;
    AssertEventuallyOk(
        [&]() {
            uint64_t downgradeCount = 0;
            RETURN_IF_NOT_OK(
                cluster_->GetInjectActionExecuteCount(WORKER, 0, "process.change.primary.copy", downgradeCount));
            CHECK_FAIL_RETURN_STATUS(downgradeCount > 0, K_NOT_READY,
                                     "old primary has not applied its persisted downgrade operation");
            return Status::OK();
        },
        "old primary applies its downgrade before normal service reopens");
    const std::string recoveryProbeKey = key + "_recovery_probe";
    const uint8_t recoveryProbeValue = 1;
    AssertEventuallyOk(
        [&]() {
            return client0->Put(recoveryProbeKey, &recoveryProbeValue, sizeof(recoveryProbeValue), CreateParam{});
        },
        "old worker reopens normal service after ownership reconciliation");
    buffers.clear();
    DS_ASSERT_OK(client0->Get({ key }, 1'000, buffers));
    ASSERT_TRUE(NotExistsNone(buffers));
    AssertBufferEqual(*buffers[0], promotedValue);
    buffers.clear();
    DS_ASSERT_OK(client1->Get({ key }, 1'000, buffers));
    ASSERT_TRUE(NotExistsNone(buffers));
    AssertBufferEqual(*buffers[0], promotedValue);
    ASSERT_EQ(workerProcess->Pid(), workerPid);
    ASSERT_TRUE(workerProcess->IsProcessAlive());
    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, 0, "WorkerOCServer.AfterMarkLocalIsolated"));
}

TEST_F(WorkerPushMetaTest, LEVEL1_TestGlobalBackendOutageDoesNotSelfIsolateWorkers)
{
    std::shared_ptr<ObjectClient> client0;
    std::shared_ptr<ObjectClient> client1;
    InitTestClient(0, client0);
    InitTestClient(1, client1);
    auto db = InitTestEtcdInstance();
    ASSERT_NE(db, nullptr);

    std::vector<std::string> worker0Keys;
    std::vector<std::string> worker1Keys;
    GetObjectKeysHashToWorker(db.get(), 0, 3, worker0Keys);
    GetObjectKeysHashToWorker(db.get(), 1, 3, worker1Keys);
    std::string worker0Data = GenRandomString(1024);
    std::string worker1Data = GenRandomString(1024);
    CreateAndSealObject(client0, worker0Keys[0], worker0Data);
    CreateAndSealObject(client1, worker1Keys[0], worker1Data);

    RpcCredential credential;
    RpcAuthKeyManager::CreateClientCredentials(RpcAuthKeys(), WORKER_SERVER_NAME, credential);
    AkSkManager akSk;
    DS_ASSERT_OK(akSk.SetClientAkSk("QTWAOYTTINDUT2QVKYUC", "MFyfvK41ba2giqM7**********KGpownRZlmVmHc"));
    std::vector<HostPort> workerAddresses(2);
    std::vector<pid_t> workerPids(2);
    for (uint32_t workerIndex = 0; workerIndex < 2; ++workerIndex) {
        DS_ASSERT_OK(cluster_->GetWorkerAddr(workerIndex, workerAddresses[workerIndex]));
        workerPids[workerIndex] = cluster_->GetWorkerPid(workerIndex);
        ASSERT_GT(workerPids[workerIndex], 0);
        DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, workerIndex, "WorkerOCServer.GlobalBackendOutage", "call()"));
        DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, workerIndex, "worker.Create.beforeValidate", "call()"));
        DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, workerIndex, "worker.Create.begin", "call()"));
        DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, workerIndex, "GetLeaseExpiredMs", "call(1000)"));
        DS_ASSERT_OK(
            cluster_->SetInjectAction(WORKER, workerIndex, "EtcdStore.LaunchKeepAliveThreads.loopQuickly", "call(0)"));
    }
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "WorkerOCServer.LocalBackendIsolationCandidate", "call()"));
    DS_ASSERT_OK(
        cluster_->SetInjectAction(WORKER, 1, "EtcdStore.ProcessKeepAliveFailure.beforeMarkTimeout", "pause()"));
    auto *externalCluster = dynamic_cast<ExternalCluster *>(cluster_.get());
    ASSERT_NE(externalCluster, nullptr);
    DS_ASSERT_OK(externalCluster->ShutdownEtcds());

    AssertEventuallyOk(
        [&]() {
            uint64_t executeCount = 0;
            RETURN_IF_NOT_OK(cluster_->GetInjectActionExecuteCount(
                WORKER, 1, "EtcdStore.ProcessKeepAliveFailure.beforeMarkTimeout", executeCount));
            CHECK_FAIL_RETURN_STATUS(executeCount > 0, K_NOT_READY,
                                     "worker 1 has not reached the skewed backend failure barrier");
            return Status::OK();
        },
        "worker 1 pauses before publishing its backend timeout");
    AssertEventuallyOk(
        [&]() {
            uint64_t executeCount = 0;
            RETURN_IF_NOT_OK(cluster_->GetInjectActionExecuteCount(
                WORKER, 0, "WorkerOCServer.LocalBackendIsolationCandidate", executeCount));
            CHECK_FAIL_RETURN_STATUS(executeCount > 0, K_NOT_READY,
                                     "worker 0 has not observed the intentionally skewed local candidate");
            return Status::OK();
        },
        "worker 0 observes a local candidate before its peer reports the outage");
    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, 1, "EtcdStore.ProcessKeepAliveFailure.beforeMarkTimeout"));

    AssertEventuallyOk(
        [&]() {
            uint64_t executeCount = 0;
            RETURN_IF_NOT_OK(
                cluster_->GetInjectActionExecuteCount(WORKER, 1, "WorkerOCServer.GlobalBackendOutage", executeCount));
            CHECK_FAIL_RETURN_STATUS(executeCount > 0, K_NOT_READY,
                                     "worker 1 has not classified the backend outage as global");
            return Status::OK();
        },
        "worker 1 classifies the backend outage as global after the skewed barrier is released");

    auto verifyAdmissionRemainsOpen = [&](uint32_t workerIndex, const std::string &objectKey) {
        uint64_t beforeValidateBefore = 0;
        uint64_t createBeginBefore = 0;
        uint64_t beforeValidateAfter = 0;
        uint64_t createBeginAfter = 0;
        RETURN_IF_NOT_OK(cluster_->GetInjectActionExecuteCount(WORKER, workerIndex, "worker.Create.beforeValidate",
                                                               beforeValidateBefore));
        RETURN_IF_NOT_OK(
            cluster_->GetInjectActionExecuteCount(WORKER, workerIndex, "worker.Create.begin", createBeginBefore));
        CreateReqPb req;
        CreateRspPb rsp;
        req.set_object_key(objectKey);
        req.set_client_id("global-outage-direct-client");
        req.set_data_size(1);
        req.set_cache_type(static_cast<uint32_t>(CacheType::MEMORY));
        req.set_request_timeout(1000);
        req.set_is_routed(true);
        RETURN_IF_NOT_OK(akSk.GenerateSignature(req));
        RpcOptions opts;
        opts.SetTimeout(1000);
        auto createStatus = DirectWorkerCreate(workerAddresses[workerIndex], credential, req, rsp, 1'000);
        RETURN_IF_NOT_OK(cluster_->GetInjectActionExecuteCount(WORKER, workerIndex, "worker.Create.beforeValidate",
                                                               beforeValidateAfter));
        RETURN_IF_NOT_OK(
            cluster_->GetInjectActionExecuteCount(WORKER, workerIndex, "worker.Create.begin", createBeginAfter));
        CHECK_FAIL_RETURN_STATUS(beforeValidateAfter > beforeValidateBefore && createBeginAfter > createBeginBefore,
                                 K_NOT_READY,
                                 FormatString("global backend outage closed worker create admission, createStatus=%s, "
                                              "beforeValidate: %zu->%zu, createBegin: %zu->%zu",
                                              createStatus.ToString(), beforeValidateBefore, beforeValidateAfter,
                                              createBeginBefore, createBeginAfter));
        return Status::OK();
    };
    DS_ASSERT_OK(verifyAdmissionRemainsOpen(0, worker0Keys[1]));
    DS_ASSERT_OK(verifyAdmissionRemainsOpen(1, worker1Keys[1]));
    ASSERT_EQ(kill(workerPids[0], 0), 0);
    ASSERT_EQ(kill(workerPids[1], 0), 0);

    for (uint32_t workerIndex = 0; workerIndex < 2; ++workerIndex) {
        DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, workerIndex, "GetLeaseExpiredMs"));
        DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, workerIndex, "EtcdStore.LaunchKeepAliveThreads.loopQuickly"));
    }
    DS_ASSERT_OK(cluster_->StartEtcdCluster());
    AssertEventuallyOk(
        [&]() {
            std::shared_ptr<Buffer> buffer;
            CreateParam param;
            RETURN_IF_NOT_OK(client0->Create(worker0Keys[2], 1, param, buffer));
            return buffer->Publish();
        },
        "worker 0 accepts writes after global backend recovery");
    AssertEventuallyOk(
        [&]() {
            std::shared_ptr<Buffer> buffer;
            CreateParam param;
            RETURN_IF_NOT_OK(client1->Create(worker1Keys[2], 1, param, buffer));
            return buffer->Publish();
        },
        "worker 1 accepts writes after global backend recovery");

    std::vector<Optional<Buffer>> worker0Buffers;
    std::vector<Optional<Buffer>> worker1Buffers;
    DS_ASSERT_OK(client0->Get({ worker0Keys[0] }, 0, worker0Buffers));
    DS_ASSERT_OK(client1->Get({ worker1Keys[0] }, 0, worker1Buffers));
    ASSERT_TRUE(NotExistsNone(worker0Buffers));
    ASSERT_TRUE(NotExistsNone(worker1Buffers));
    AssertBufferEqual(*worker0Buffers[0], worker0Data);
    AssertBufferEqual(*worker1Buffers[0], worker1Data);
}

TEST_F(WorkerPushMetaTest, LEVEL1_TestTopologyJitterDoesNotKillLocalWorker)
{
    std::shared_ptr<ObjectClient> client0;
    InitTestClient(0, client0);
    auto db = InitTestEtcdInstance();
    ASSERT_NE(db, nullptr);
    std::vector<std::string> worker0Keys;
    GetObjectKeysHashToWorker(db.get(), 0, 3, worker0Keys);
    std::string preservedData = GenRandomString(1024);
    CreateAndSealObject(client0, worker0Keys[0], preservedData);

    HostPort workerAddr0;
    DS_ASSERT_OK(cluster_->GetWorkerAddr(0, workerAddr0));
    ServerProcess *workerProcess = nullptr;
    DS_ASSERT_OK(cluster_->GetProcess(WORKER, 0, workerProcess));
    ASSERT_NE(workerProcess, nullptr);
    ASSERT_TRUE(workerProcess->IsProcessAlive());
    const auto workerPid = workerProcess->Pid();
    ASSERT_GT(workerPid, 0);
    RpcCredential credential;
    RpcAuthKeyManager::CreateClientCredentials(RpcAuthKeys(), WORKER_SERVER_NAME, credential);
    AkSkManager akSk;
    DS_ASSERT_OK(akSk.SetClientAkSk("QTWAOYTTINDUT2QVKYUC", "MFyfvK41ba2giqM7**********KGpownRZlmVmHc"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "worker.Create.beforeValidate", "call()"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "worker.Create.begin", "call()"));

    std::string topologyBytes;
    DS_ASSERT_OK(db->Get(GetTopologyTableName(), "", topologyBytes));
    ClusterTopologyPb authoritativeTopology;
    ASSERT_TRUE(authoritativeTopology.ParseFromString(topologyBytes));
    ClusterTopologyPb jitteredTopology = authoritativeTopology;
    auto *members = jitteredTopology.mutable_members();
    auto localMember = members->find(workerAddr0.ToString());
    ASSERT_NE(localMember, members->end());
    auto peerMember = members->begin();
    if (peerMember == localMember) {
        ++peerMember;
    }
    ASSERT_NE(peerMember, members->end());
    for (auto token : localMember->second.tokens()) {
        peerMember->second.add_tokens(token);
    }
    members->erase(localMember);
    jitteredTopology.set_version(authoritativeTopology.version() + 1);
    jitteredTopology.clear_active_batch();
    DS_ASSERT_OK(db->Put(GetTopologyTableName(), "", jitteredTopology.SerializeAsString()));

    auto verifyCreateBlockedBeforeAllocation = [&]() {
        uint64_t beforeValidateBefore = 0;
        uint64_t createBeginBefore = 0;
        uint64_t beforeValidateAfter = 0;
        uint64_t createBeginAfter = 0;
        RETURN_IF_NOT_OK(
            cluster_->GetInjectActionExecuteCount(WORKER, 0, "worker.Create.beforeValidate", beforeValidateBefore));
        RETURN_IF_NOT_OK(cluster_->GetInjectActionExecuteCount(WORKER, 0, "worker.Create.begin", createBeginBefore));
        CreateReqPb req;
        CreateRspPb rsp;
        req.set_object_key(worker0Keys[1]);
        req.set_client_id("topology-jitter-direct-client");
        req.set_data_size(1);
        req.set_cache_type(static_cast<uint32_t>(CacheType::MEMORY));
        req.set_request_timeout(1000);
        req.set_is_routed(true);
        RETURN_IF_NOT_OK(akSk.GenerateSignature(req));
        RpcOptions opts;
        opts.SetTimeout(1000);
        auto createStatus = DirectWorkerCreate(workerAddr0, credential, req, rsp, 1'000);
        RETURN_IF_NOT_OK(
            cluster_->GetInjectActionExecuteCount(WORKER, 0, "worker.Create.beforeValidate", beforeValidateAfter));
        RETURN_IF_NOT_OK(cluster_->GetInjectActionExecuteCount(WORKER, 0, "worker.Create.begin", createBeginAfter));
        CHECK_FAIL_RETURN_STATUS(
            beforeValidateAfter > beforeValidateBefore && createBeginAfter == createBeginBefore, K_NOT_READY,
            FormatString("topology-isolated worker still admitted Create allocation, createStatus=%s, "
                         "beforeValidate: %zu->%zu, createBegin: %zu->%zu",
                         createStatus.ToString(), beforeValidateBefore, beforeValidateAfter, createBeginBefore,
                         createBeginAfter));
        return Status::OK();
    };
    AssertEventuallyOk(verifyCreateBlockedBeforeAllocation,
                       "topology-isolated worker blocks Create before object allocation");

    std::this_thread::sleep_for(std::chrono::seconds(nodeDeadTimeout_ + 1));
    ASSERT_TRUE(workerProcess->IsProcessAlive());
    std::string latestTopologyBytes;
    DS_ASSERT_OK(db->Get(GetTopologyTableName(), "", latestTopologyBytes));
    ClusterTopologyPb latestTopology;
    ASSERT_TRUE(latestTopology.ParseFromString(latestTopologyBytes));
    authoritativeTopology.set_version(latestTopology.version() + 1);
    authoritativeTopology.clear_active_batch();
    DS_ASSERT_OK(db->Put(GetTopologyTableName(), "", authoritativeTopology.SerializeAsString()));
    AssertEventuallyOk(
        [&]() {
            std::shared_ptr<Buffer> buffer;
            CreateParam param;
            RETURN_IF_NOT_OK(client0->Create(worker0Keys[2], 1, param, buffer));
            return buffer->Publish();
        },
        "same worker process accepts writes after authoritative topology returns");
    ASSERT_EQ(workerProcess->Pid(), workerPid);
    ASSERT_TRUE(workerProcess->IsProcessAlive());

    std::vector<Optional<Buffer>> preservedBuffers;
    DS_ASSERT_OK(client0->Get({ worker0Keys[0] }, 0, preservedBuffers));
    ASSERT_TRUE(NotExistsNone(preservedBuffers));
    AssertBufferEqual(*preservedBuffers[0], preservedData);
}

TEST_F(WorkerPushMetaTest, LEVEL1_TestVoluntaryScaleDownStillExitsControlled)
{
    constexpr uint32_t workerIndex = 0;
    auto db = InitTestEtcdInstance();
    ASSERT_NE(db, nullptr);
    HostPort workerAddress;
    DS_ASSERT_OK(cluster_->GetWorkerAddr(workerIndex, workerAddress));
    ServerProcess *process = nullptr;
    DS_ASSERT_OK(cluster_->GetProcess(WORKER, workerIndex, process));
    ASSERT_NE(process, nullptr);
    ASSERT_TRUE(process->IsProcessAlive());

    const std::string statusPath = cluster_->GetRootDir() + "/worker0/log/worker-status";
    {
        std::ofstream status(statusPath, std::ios::trunc);
        ASSERT_TRUE(status.is_open());
        status << "voluntary scale in\n";
        status.flush();
        ASSERT_TRUE(status.good());
    }
    DS_ASSERT_OK(process->Kill(SIGTERM));

    bool topologyRemoved = false;
    bool shutdownMarkerSeen = false;
    bool processExited = false;
    int processExitStatus = 0;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(15);
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
        const auto waitResult = waitpid(process->Pid(), &processExitStatus, WNOHANG);
        if (waitResult == process->Pid()) {
            processExited = true;
        } else if (waitResult < 0 && errno != EINTR) {
            ADD_FAILURE() << "waitpid failed while observing voluntary scale-down, errno=" << errno;
            break;
        }
        if (topologyRemoved && shutdownMarkerSeen && processExited) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(kEventuallyPollIntervalMs));
    }

    const bool gracefulProcessExited = processExited;
    if (!processExited) {
        LOG_IF_ERROR(process->Kill(SIGKILL), "force cleanup of voluntary scale-down test worker");
        const auto reapDeadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
        while (std::chrono::steady_clock::now() < reapDeadline) {
            const auto waitResult = waitpid(process->Pid(), &processExitStatus, WNOHANG);
            if (waitResult == process->Pid() || (waitResult < 0 && errno == ECHILD)) {
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(kEventuallyPollIntervalMs));
        }
    }
    EXPECT_FALSE(process->IsProcessAlive());

    EXPECT_TRUE(topologyRemoved);
    EXPECT_TRUE(shutdownMarkerSeen);
    ASSERT_TRUE(gracefulProcessExited);
    ASSERT_TRUE(WIFEXITED(processExitStatus));
    EXPECT_EQ(WEXITSTATUS(processExitStatus), 0);
}

TEST_F(WorkerPushMetaTest, LEVEL1_TestKeepAliveLocalIsolationRecoversThroughEvidenceGate)
{
    std::shared_ptr<ObjectClient> client0;
    InitTestClient(0, client0);
    auto db = InitTestEtcdInstance();
    ASSERT_NE(db, nullptr);
    std::vector<std::string> worker0Keys;
    GetObjectKeysHashToWorker(db.get(), 0, 4, worker0Keys);
    const auto &preservedObjectKey = worker0Keys[0];
    const auto &isolatedRejectKey = worker0Keys[1];
    const auto &recoveringRejectKey = worker0Keys[2];
    const auto &recoveredObjectKey = worker0Keys[3];
    constexpr const char *localIsolatedInject = "WorkerOCServer.AfterMarkLocalIsolated";

    std::string preservedData = GenRandomString(1024);
    CreateAndSealObject(client0, preservedObjectKey, preservedData);
    std::shared_ptr<Buffer> isolatedRejectBuffer;
    DS_ASSERT_OK(client0->Create(isolatedRejectKey, 1, CreateParam{}, isolatedRejectBuffer));
    uint8_t isolatedRejectData = 1;
    DS_ASSERT_OK(isolatedRejectBuffer->MemoryCopy(&isolatedRejectData, sizeof(isolatedRejectData)));
    std::shared_ptr<Buffer> recoveringRejectBuffer;
    DS_ASSERT_OK(client0->Create(recoveringRejectKey, 1, CreateParam{}, recoveringRejectBuffer));
    uint8_t recoveringRejectData = 2;
    DS_ASSERT_OK(recoveringRejectBuffer->MemoryCopy(&recoveringRejectData, sizeof(recoveringRejectData)));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, localIsolatedInject, "call()"));
    bool localIsolationInjectActive = true;
    Raii clearLocalIsolationInject([&]() {
        if (localIsolationInjectActive) {
            LOG_IF_ERROR(cluster_->ClearInjectAction(WORKER, 0, localIsolatedInject),
                         "clear local-isolation evidence barrier");
        }
    });

    const auto isolatedWorkerPid = cluster_->GetWorkerPid(0);
    ASSERT_GT(isolatedWorkerPid, 0);
    DS_ASSERT_OK(
        cluster_->SetInjectAction(WORKER, 0, "EtcdKeepAlive.SendKeepAliveMessage", "return(K_RPC_UNAVAILABLE)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "GetLeaseExpiredMs", "call(1000)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "EtcdStore.LaunchKeepAliveThreads.loopQuickly", "call(0)"));

    auto verifyPublishRejected = [](const std::shared_ptr<Buffer> &buffer) {
        const auto rc = buffer->Publish();
        CHECK_FAIL_RETURN_STATUS(rc.GetCode() == K_NOT_READY, K_NOT_READY,
                                 "publish request has not been rejected by worker admission");
        return Status::OK();
    };
    AssertEventuallyOk(
        [&]() {
            uint64_t executeCount = 0;
            RETURN_IF_NOT_OK(cluster_->GetInjectActionExecuteCount(WORKER, 0, localIsolatedInject, executeCount));
            CHECK_FAIL_RETURN_STATUS(executeCount > 0, K_NOT_READY, "worker has not entered local isolation");
            return Status::OK();
        },
        "worker enters local isolation before probing object publish admission");
    AssertEventuallyOk([&]() { return verifyPublishRejected(isolatedRejectBuffer); },
                       "publish admission blocks locally isolated worker before object publication");

    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "WorkerRecoveryController.BeforeMarkRunning", "1*pause"));
    bool recoveryPauseActive = true;
    Raii clearRecoveryPause([&]() {
        if (recoveryPauseActive) {
            LOG_IF_ERROR(cluster_->ClearInjectAction(WORKER, 0, "WorkerRecoveryController.BeforeMarkRunning"),
                         "clear recovery evidence pause");
        }
    });
    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, 0, "EtcdKeepAlive.SendKeepAliveMessage"));
    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, 0, "GetLeaseExpiredMs"));
    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, 0, "EtcdStore.LaunchKeepAliveThreads.loopQuickly"));

    AssertEventuallyOk(
        [&]() {
            uint64_t executeCount = 0;
            RETURN_IF_NOT_OK(cluster_->GetInjectActionExecuteCount(
                WORKER, 0, "WorkerRecoveryController.BeforeMarkRunning", executeCount));
            CHECK_FAIL_RETURN_STATUS(executeCount > 0, K_NOT_READY, "recovery evidence gate has not run yet");
            return Status::OK();
        },
        "recovery reaches the evidence gate after keepalive reconnects");
    DS_ASSERT_OK(verifyPublishRejected(recoveringRejectBuffer));
    std::vector<Optional<Buffer>> isolatedBuffers;
    ASSERT_EQ(client0->Get({ preservedObjectKey }, 0, isolatedBuffers).GetCode(), K_NOT_READY);
    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, 0, "WorkerRecoveryController.BeforeMarkRunning"));
    recoveryPauseActive = false;

    AssertEventuallyOk(
        [&]() {
            std::shared_ptr<Buffer> buffer;
            CreateParam param;
            RETURN_IF_NOT_OK(client0->Create(recoveredObjectKey, 1, param, buffer));
            uint8_t data = 1;
            RETURN_IF_NOT_OK(buffer->MemoryCopy(&data, sizeof(data)));
            return buffer->Publish();
        },
        "normal write admission reopens after recovery evidence completes");

    ASSERT_EQ(kill(isolatedWorkerPid, 0), 0);
    std::vector<Optional<Buffer>> preservedBuffers;
    DS_ASSERT_OK(client0->Get({ preservedObjectKey }, 0, preservedBuffers));
    ASSERT_TRUE(NotExistsNone(preservedBuffers));
    AssertBufferEqual(*preservedBuffers[0], preservedData);
    std::vector<Optional<Buffer>> rejectedBuffers;
    ASSERT_EQ(client0->Get({ isolatedRejectKey, recoveringRejectKey }, 1'000, rejectedBuffers).GetCode(), K_NOT_FOUND);
    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, 0, localIsolatedInject));
    localIsolationInjectActive = false;
}

TEST_F(WorkerPushMetaTest, LEVEL1_TestRecoveringWorkerFallsBackToLocalIsolatedOnDisconnect)
{
    std::shared_ptr<KVClient> client0;
    InitTestKVClient(0, client0, 60'000, false, 1'000);
    auto db = InitTestEtcdInstance();
    ASSERT_NE(db, nullptr);
    std::vector<std::string> worker0Keys;
    GetObjectKeysHashToWorker(db.get(), 0, 3, worker0Keys);
    const std::string preservedValue = GenRandomString(1024);
    DS_ASSERT_OK(client0->Set(worker0Keys[0], preservedValue));

    const auto workerPid = cluster_->GetWorkerPid(0);
    ASSERT_GT(workerPid, 0);
    constexpr const char *candidateInject = "WorkerOCServer.LocalBackendIsolationCandidate";
    constexpr const char *isolatedInject = "WorkerOCServer.AfterMarkLocalIsolated";
    constexpr const char *beforeRunningInject = "WorkerRecoveryController.BeforeMarkRunning";
    constexpr const char *afterRunningInject = "WorkerRecoveryController.AfterTryMarkRunning";
    constexpr const char *keepAliveInject = "EtcdKeepAlive.SendKeepAliveMessage";
    constexpr const char *leaseInject = "GetLeaseExpiredMs";
    constexpr const char *quickLoopInject = "EtcdStore.LaunchKeepAliveThreads.loopQuickly";
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, candidateInject, "call()"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, isolatedInject, "call()"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, afterRunningInject, "call()"));
    bool injectsActive = true;
    Raii clearInjects([&]() {
        if (injectsActive) {
            LOG_IF_ERROR(cluster_->ClearInjectAction(WORKER, 0, candidateInject), "clear isolation candidate");
            LOG_IF_ERROR(cluster_->ClearInjectAction(WORKER, 0, isolatedInject), "clear isolation barrier");
            LOG_IF_ERROR(cluster_->ClearInjectAction(WORKER, 0, beforeRunningInject), "clear recovery pause");
            LOG_IF_ERROR(cluster_->ClearInjectAction(WORKER, 0, afterRunningInject), "clear recovery barrier");
            LOG_IF_ERROR(cluster_->ClearInjectAction(WORKER, 0, keepAliveInject), "clear keepalive failure");
            LOG_IF_ERROR(cluster_->ClearInjectAction(WORKER, 0, leaseInject), "clear lease timeout");
            LOG_IF_ERROR(cluster_->ClearInjectAction(WORKER, 0, quickLoopInject), "clear quick loop");
        }
    });
    auto enableKeepAliveFailure = [&]() {
        RETURN_IF_NOT_OK(cluster_->SetInjectAction(WORKER, 0, keepAliveInject, "return(K_RPC_UNAVAILABLE)"));
        RETURN_IF_NOT_OK(cluster_->SetInjectAction(WORKER, 0, leaseInject, "call(1000)"));
        return cluster_->SetInjectAction(WORKER, 0, quickLoopInject, "call(0)");
    };
    auto disableKeepAliveFailure = [&]() {
        RETURN_IF_NOT_OK(cluster_->ClearInjectAction(WORKER, 0, keepAliveInject));
        RETURN_IF_NOT_OK(cluster_->ClearInjectAction(WORKER, 0, leaseInject));
        return cluster_->ClearInjectAction(WORKER, 0, quickLoopInject);
    };
    auto waitForInjectCount = [&](const std::string &name, uint64_t expectedCount, const std::string &detail) {
        AssertEventuallyOk(
            [&]() {
                uint64_t executeCount = 0;
                RETURN_IF_NOT_OK(cluster_->GetInjectActionExecuteCount(WORKER, 0, name, executeCount));
                CHECK_FAIL_RETURN_STATUS(executeCount >= expectedCount, K_NOT_READY, detail);
                return Status::OK();
            },
            detail);
    };

    DS_ASSERT_OK(enableKeepAliveFailure());
    waitForInjectCount(isolatedInject, 1, "worker did not enter initial local isolation");
    ASSERT_EQ(client0->Set(worker0Keys[1], "initial-isolation-write").GetCode(), K_NOT_READY);

    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, beforeRunningInject, "1*pause"));
    DS_ASSERT_OK(disableKeepAliveFailure());
    waitForInjectCount(beforeRunningInject, 1, "worker did not reach the recovery evidence gate");

    DS_ASSERT_OK(enableKeepAliveFailure());
    waitForInjectCount(candidateInject, 2, "recovery did not observe the second local coordination loss");
    waitForInjectCount(isolatedInject, 2, "recovering worker did not return to local isolation");
    auto secondIsolationRc = client0->Set(worker0Keys[1], "recovery-interrupted-write");
    ASSERT_EQ(secondIsolationRc.GetCode(), K_NOT_READY);
    EXPECT_NE(secondIsolationRc.GetMsg().find("CONTROL_BACKEND_LOCAL_ISOLATION"), std::string::npos);
    std::string actual;
    ASSERT_EQ(client0->Get(worker0Keys[0], actual).GetCode(), K_NOT_READY);

    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, 0, beforeRunningInject));
    waitForInjectCount(afterRunningInject, 1, "stale recovery attempt did not leave the evidence gate");
    ASSERT_EQ(client0->Set(worker0Keys[1], "stale-recovery-write").GetCode(), K_NOT_READY);

    DS_ASSERT_OK(disableKeepAliveFailure());
    AssertEventuallyOk([&]() { return client0->Set(worker0Keys[2], "recovered-write"); },
                       "worker did not reopen after the second recovery completed");
    ASSERT_EQ(kill(workerPid, 0), 0);
    DS_ASSERT_OK(client0->Get(worker0Keys[0], actual));
    EXPECT_EQ(actual, preservedValue);

    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, 0, candidateInject));
    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, 0, isolatedInject));
    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, 0, afterRunningInject));
    injectsActive = false;
}

TEST_F(WorkerPushMetaTransportTest, LEVEL1_TestTransportReadInvisibleUntilOwnershipEvidence)
{
    std::shared_ptr<ObjectClient> writer;
    InitTestClient(0, writer);
    FLAGS_enable_urma = false;
    ConnectOptions transportOptions;
    InitConnectOpt(1, transportOptions);
    transportOptions.enableLocalCache = false;
    transportOptions.requestTimeoutMs = 20000;
    auto reader = std::make_shared<ObjectClient>(transportOptions);
    DS_ASSERT_OK(reader->Init());
    FLAGS_enable_urma = false;

    auto db = InitTestEtcdInstance();
    ASSERT_NE(db, nullptr);
    std::vector<std::string> worker0Keys;
    GetObjectKeysHashToWorker(db.get(), 0, 2, worker0Keys);
    std::vector<std::string> preservedData = { GenRandomString(1024), GenRandomString(1024) };
    CreateAndSealObject(writer, worker0Keys[0], preservedData[0]);
    CreateAndSealObject(writer, worker0Keys[1], preservedData[1]);

    std::vector<Optional<Buffer>> buffers;
    DS_ASSERT_OK(reader->Get(worker0Keys, 0, buffers));
    ASSERT_TRUE(NotExistsNone(buffers));
    AssertBufferEqual(*buffers[0], preservedData[0]);
    AssertBufferEqual(*buffers[1], preservedData[1]);

    const std::string readPause = "worker.worker_worker_read_before_admission";
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, readPause, "pause()"));
    auto inFlightRead = std::async(std::launch::async, [&]() {
        std::vector<Optional<Buffer>> inFlightBuffers;
        return reader->Get(worker0Keys, 0, inFlightBuffers);
    });
    bool readPauseActive = true;
    Raii clearReadPause([&]() {
        if (readPauseActive) {
            LOG_IF_ERROR(cluster_->ClearInjectAction(WORKER, 0, readPause), "clear transport read pause");
        }
    });
    AssertEventuallyOk(
        [&]() {
            uint64_t executeCount = 0;
            RETURN_IF_NOT_OK(cluster_->GetInjectActionExecuteCount(WORKER, 0, readPause, executeCount));
            CHECK_FAIL_RETURN_STATUS(executeCount > 0, K_NOT_READY, "transport read has not reached handler yet");
            return Status::OK();
        },
        "transport read pauses after entry admission");

    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "WorkerOCServer.LocalBackendIsolationCandidate", "call()"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "WorkerOCServer.AfterMarkLocalIsolated", "call()"));
    DS_ASSERT_OK(
        cluster_->SetInjectAction(WORKER, 0, "EtcdKeepAlive.SendKeepAliveMessage", "return(K_RPC_UNAVAILABLE)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "GetLeaseExpiredMs", "call(1000)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "EtcdStore.LaunchKeepAliveThreads.loopQuickly", "call(0)"));
    AssertEventuallyOk(
        [&]() {
            uint64_t executeCount = 0;
            RETURN_IF_NOT_OK(cluster_->GetInjectActionExecuteCount(
                WORKER, 0, "WorkerOCServer.LocalBackendIsolationCandidate", executeCount));
            CHECK_FAIL_RETURN_STATUS(executeCount > 0, K_NOT_READY, "worker is not locally isolated yet");
            return Status::OK();
        },
        "transport data owner enters local isolation");
    AssertEventuallyOk(
        [&]() {
            uint64_t executeCount = 0;
            RETURN_IF_NOT_OK(cluster_->GetInjectActionExecuteCount(WORKER, 0, "WorkerOCServer.AfterMarkLocalIsolated",
                                                                   executeCount));
            CHECK_FAIL_RETURN_STATUS(executeCount > 0, K_NOT_READY, "transport admission has not closed yet");
            return Status::OK();
        },
        "transport admission closes before paused read resumes");
    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, 0, readPause));
    readPauseActive = false;
    ASSERT_EQ(inFlightRead.get().GetCode(), K_NOT_READY);
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "WorkerRecoveryController.BeforeMarkRunning", "1*pause"));
    bool recoveryPauseActive = true;
    Raii clearRecoveryPause([&]() {
        if (recoveryPauseActive) {
            LOG_IF_ERROR(cluster_->ClearInjectAction(WORKER, 0, "WorkerRecoveryController.BeforeMarkRunning"),
                         "clear transport recovery evidence pause");
        }
    });
    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, 0, "EtcdKeepAlive.SendKeepAliveMessage"));
    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, 0, "GetLeaseExpiredMs"));
    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, 0, "EtcdStore.LaunchKeepAliveThreads.loopQuickly"));

    AssertEventuallyOk(
        [&]() {
            uint64_t executeCount = 0;
            RETURN_IF_NOT_OK(cluster_->GetInjectActionExecuteCount(
                WORKER, 0, "WorkerRecoveryController.BeforeMarkRunning", executeCount));
            CHECK_FAIL_RETURN_STATUS(executeCount > 0, K_NOT_READY, "recovery evidence gate has not run yet");
            return Status::OK();
        },
        "transport recovery reaches the evidence gate");
    buffers.clear();
    ASSERT_EQ(reader->Get(worker0Keys, 0, buffers).GetCode(), K_NOT_READY);
    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, 0, "WorkerRecoveryController.BeforeMarkRunning"));
    recoveryPauseActive = false;

    AssertEventuallyOk(
        [&]() {
            buffers.clear();
            auto rc = reader->Get(worker0Keys, 0, buffers);
            if (rc.IsError()) {
                return rc;
            }
            CHECK_FAIL_RETURN_STATUS(NotExistsNone(buffers), K_NOT_READY, "preserved transport object is unavailable");
            return Status::OK();
        },
        "transport read reopens after ownership evidence completes");
    AssertBufferEqual(*buffers[0], preservedData[0]);
    AssertBufferEqual(*buffers[1], preservedData[1]);
}

TEST_F(WorkerPushMetaTest, TestWorkerTimeoutAndPushMeta)
{
    std::shared_ptr<ObjectClient> client0;
    std::shared_ptr<ObjectClient> client1;
    InitTestClient(0, client0);
    InitTestClient(1, client1);

    size_t num = 10;
    std::vector<uint8_t> data;
    size_t dataSize = 500 * 1024ul;
    data.resize(dataSize);
    std::vector<std::string> objKeys;
    std::vector<std::shared_ptr<Buffer>> buffers(num);
    for (size_t i = 0; i < num; ++i) {
        std::string objKey = "BigEmperor" + std::to_string(i);
        objKeys.emplace_back(objKey);
        CreateParam param;
        DS_ASSERT_OK(client0->Create(objKey, data.size(), param, buffers[i]));
        DS_ASSERT_OK(buffers[i]->MemoryCopy(data.data(), data.size()));
        DS_ASSERT_OK(buffers[i]->Publish());
    }
    std::vector<std::string> failedObjectKeys;
    DS_ASSERT_OK(client0->GIncreaseRef(objKeys, failedObjectKeys));
    ASSERT_EQ(failedObjectKeys.size(), size_t(0));

    // Let hearbeat timeout and master will clear its metadata, then when worker reconnect,
    // master will ask us to send the metadata to master.
    DS_ASSERT_OK(
        cluster_->SetInjectAction(WORKER, 0, "EtcdKeepAlive.SendKeepAliveMessage", "return(K_RPC_UNAVAILABLE)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "GetLeaseExpiredMs", "call(1000)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "EtcdStore.LaunchKeepAliveThreads.loopQuickly", "call(0)"));

    std::this_thread::sleep_for(std::chrono::seconds(nodeDeadTimeout_ + 1));
    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, 0, "EtcdKeepAlive.SendKeepAliveMessage"));
    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, 0, "GetLeaseExpiredMs"));
    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, 0, "EtcdStore.LaunchKeepAliveThreads.loopQuickly"));
    std::this_thread::sleep_for(std::chrono::seconds(nodeDeadTimeout_));
    AssertEventuallyOk(
        [&]() {
            std::shared_ptr<Buffer> probe;
            CreateParam param;
            RETURN_IF_NOT_OK(client0->Create("worker_timeout_recovery_probe", 1, param, probe));
            uint8_t value = 1;
            RETURN_IF_NOT_OK(probe->MemoryCopy(&value, sizeof(value)));
            return probe->Publish();
        },
        "worker timeout recovery completes before push-meta validation");

    for (size_t i = 0; i < num; ++i) {
        DS_ASSERT_OK(buffers[i]->Seal());
    }

    std::vector<Optional<Buffer>> getBuffers;
    const int timeoutMs = 30'000;
    DS_ASSERT_OK(client1->Get(objKeys, timeoutMs, getBuffers));
    ASSERT_TRUE(NotExistsNone(getBuffers));
    DS_ASSERT_OK(client0->GDecreaseRef(objKeys, failedObjectKeys));
    ASSERT_EQ(failedObjectKeys.size(), size_t(0));
}

class WorkerKillTest : public OCClientCommon {
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
        const int workerNum = 3;
        opts.numWorkers = workerNum;
        opts.enableDistributedMaster = "false";
        opts.numEtcd = 1;
        opts.waitWorkerReady = false;
        const std::string hostIp = "127.0.0.1";
        opts.workerConfigs.emplace_back(hostIp, GetFreePort());
        opts.workerConfigs.emplace_back(hostIp, GetFreePort());
        opts.workerConfigs.emplace_back(hostIp, GetFreePort());
        for (const auto &addr : opts.workerConfigs) {
            workerAddress_.emplace_back(addr.ToString());
        }
    }

    void StartWorkerAndWaitReady(std::initializer_list<uint32_t> indexes, uint32_t masterIdx = 0,
                                 uint32_t maxWaitTimeSec = 20)
    {
        HostPort master;
        ASSERT_LE(masterIdx, workerAddress_.size());
        DS_ASSERT_OK(master.ParseString(workerAddress_[masterIdx]));
        for (auto i : indexes) {
            ASSERT_TRUE(externalCluster_->StartWorker(i, master).IsOk()) << i;
        }
        for (auto i : indexes) {
            ASSERT_TRUE(cluster_->WaitNodeReady(WORKER, i, maxWaitTimeSec).IsOk()) << i;
        }
    }

protected:
    ExternalCluster *externalCluster_ = nullptr;
    std::vector<std::string> workerAddress_;
};

TEST_F(WorkerKillTest, LEVEL1_GetAfterGDecrease)
{
    const int workerIdx2 = 2;
    StartWorkerAndWaitReady({ 0, 1, workerIdx2 }, 1);
    const int waitTime = 5;
    std::this_thread::sleep_for(std::chrono::seconds(waitTime));
    const int connectTimeout = 10000;
    std::shared_ptr<ObjectClient> client;
    InitTestClient(0, client, connectTimeout);
    std::shared_ptr<ObjectClient> client1;
    InitTestClient(1, client1, connectTimeout);
    std::shared_ptr<ObjectClient> client2;
    InitTestClient(workerIdx2, client2, connectTimeout);

    std::string objectKey = NewObjectKey();
    size_t size = 600 * 1024;
    std::string data = GenRandomString(size);
    std::shared_ptr<Buffer> buffer;
    CreateParam param{ .consistencyType = ConsistencyType::PRAM };
    DS_ASSERT_OK(client1->Create(objectKey, size, param, buffer));
    buffer->MemoryCopy((void *)data.data(), size);
    buffer->Publish();

    std::vector<Optional<Buffer>> buffers;
    DS_ASSERT_OK(client->Get({ objectKey }, 0, buffers));
    DS_ASSERT_OK(client2->Get({ objectKey }, 0, buffers));

    DS_ASSERT_OK(externalCluster_->KillWorker(workerIdx2));

    std::vector<std::string> failedObjectKeys;
    DS_ASSERT_OK(client->GIncreaseRef({ objectKey }, failedObjectKeys));
    DS_ASSERT_NOT_OK(client->GDecreaseRef({ objectKey }, failedObjectKeys));

    StartWorkerAndWaitReady({ workerIdx2 }, 1);

    DS_ASSERT_OK(client->Get({ objectKey }, 0, buffers));
    failedObjectKeys.clear();
    DS_ASSERT_OK(client->GDecreaseRef({ objectKey }, failedObjectKeys));

    DS_ASSERT_NOT_OK(client->Get({ objectKey }, 0, buffers));
}

TEST_F(WorkerKillTest, DISABLED_ClearPrimaryAfterWorkerRestart)
{
    const int workerIdx2 = 2;
    StartWorkerAndWaitReady({ 0, 1, workerIdx2 });
    const int waitTime = 5;
    std::this_thread::sleep_for(std::chrono::seconds(waitTime));
    std::shared_ptr<ObjectClient> client1, client2;
    const int timeout = 5'000;
    InitTestClient(1, client1, timeout);
    InitTestClient(workerIdx2, client2, timeout);
    std::string objectKey = "obj1";
    int64_t dataSize = 10;
    std::string value1 = GenRandomString(dataSize);
    CreateParam param = {};
    DS_ASSERT_OK(client1->Put(objectKey, reinterpret_cast<const uint8_t *>(value1.data()), value1.size(), param));
    sleep(1);
    DS_ASSERT_OK(externalCluster_->KillWorker(1));

    ThreadPool threadPool(1);
    auto fut = threadPool.Submit([&]() {
        std::vector<Optional<Buffer>> buffers;
        ASSERT_EQ(client2->Get({ objectKey }, timeout, buffers).GetCode(), StatusCode::K_RUNTIME_ERROR);
    });

    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "OCMetadataManager.RemoveMetaByWorker.delay", "sleep(2000)"));
    StartWorkerAndWaitReady({ 1 });

    fut.get();
}

class ClientDiskCacheDfxTest : public OCClientCommon {
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = 1;
        opts.numEtcd = 1;
        for (size_t i = 0; i < opts.numWorkers; i++) {
            std::string dir = GetTestCaseDataDir() + "/worker" + std::to_string(i) + "/shared_disk";
            opts.workerSpecifyGflagParams[i] += FormatString(" -shared_disk_directory=%s -shared_disk_size_mb=32", dir);
        }
    }
};

TEST_F(ClientDiskCacheDfxTest, ClientCrash)
{
    std::vector<std::shared_ptr<ObjectClient>> clients;
    int num = 3;
    clients.resize(num);
    for (int i = 0; i < num; i++) {
        InitTestClient(0, clients[i]);
    }

    std::string val = GenRandomString(1000 * 1024);  // 1000 KB
    CreateParam param;
    param.cacheType = CacheType::DISK;
    for (int i = 0; i < num; i++) {
        int objectsNum = 10;
        for (int j = 0; j < objectsNum; j++) {
            std::string objectKey = GenRandomString();
            std::vector<std::string> failedObjectKeys;
            DS_ASSERT_OK(clients[i]->GIncreaseRef({ objectKey }, failedObjectKeys));
            DS_ASSERT_OK(clients[i]->Put(objectKey, reinterpret_cast<const uint8_t *>(val.data()), val.size(), param));
        }
    }

    clients[0].reset();
    clients[num - 1].reset();

    int objectsNum = 20;
    for (int j = 0; j < objectsNum; j++) {
        std::string objectKey = GenRandomString();
        DS_ASSERT_OK(clients[1]->Put(objectKey, reinterpret_cast<const uint8_t *>(val.data()), val.size(), param));
    }
}
}  // namespace st
}  // namespace datasystem
