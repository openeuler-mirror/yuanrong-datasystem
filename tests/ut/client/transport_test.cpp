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

/** Description: routed worker control connection and data-plane transport unit tests. */

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <functional>
#include <future>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "datasystem/client/transport/data_plane/data_plane_manager.h"
#include "datasystem/client/transport/data_plane/shm_transporter.h"
#include "datasystem/client/transport/data_plane/tcp_transporter.h"
#include "datasystem/client/transport/data_plane/ub_transporter.h"
#include "datasystem/client/transport/rpc/client_request_auth.h"
#include "datasystem/client/transport/rpc/worker_rpc_client.h"
#include "datasystem/client/transport/transport_layer.h"
#include "datasystem/common/rpc/api_deadline.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/thread_local.h"
#include "datasystem/kv_client.h"

namespace datasystem {
namespace client {
namespace {

HostPort MakeAddress(int port)
{
    return HostPort("127.0.0.1", port);
}

std::shared_ptr<ClientRequestAuth> MakeAuth()
{
    return std::make_shared<DefaultClientRequestAuth>("stable-client");
}

class FakeWorkerRpcClient : public WorkerRpcClient {
public:
    explicit FakeWorkerRpcClient(HostPort address = MakeAddress(9000))
        : WorkerRpcClient(std::move(address), MakeAuth())
    {
    }

    Status Init() override
    {
        alive = initStatus.IsOk();
        return initStatus;
    }

    Status QueryObjectSizes(const std::vector<std::string> &, std::vector<uint64_t> &sizes) override
    {
        ++queryCount;
        sizes = objectSizes;
        return queryStatus;
    }

    Status InvokeGet(int64_t, GetReqPb &request, GetRspPb &response, std::vector<RpcMessage> &rpcPayloads,
                     uint32_t &workerVersion) override
    {
        ++invokeCount;
        invokedRequests.push_back(request);
        if (onInvoke) {
            onInvoke();
        }
        if (invokeStatus.IsError()) {
            return invokeStatus;
        }
        workerVersion = version;
        for (int i = 0; i < request.object_keys_size(); ++i) {
            const auto &key = request.object_keys(i);
            const int64_t dataSize = payloadSizes.count(key) == 0 ? 1 : payloadSizes.at(key);
            auto *payloadInfo = response.add_payload_info();
            payloadInfo->set_object_key(key);
            payloadInfo->set_data_size(dataSize);
            if (!request.has_urma_info() || (mixedUbResponse && i == request.object_keys_size() - 1)) {
                RpcMessage payload;
                std::string data(static_cast<size_t>(std::max<int64_t>(dataSize, 0)), 'x');
                RETURN_IF_NOT_OK(payload.CopyBuffer(data.data(), data.size()));
                payloadInfo->add_part_index(invalidFallbackIndex ? rpcPayloads.size() + 1 : rpcPayloads.size());
                rpcPayloads.emplace_back(std::move(payload));
            }
        }
        response.mutable_last_rc()->set_error_code(static_cast<int32_t>(responseCode));
        for (int64_t dataSize : responseObjectDataSizes) {
            response.add_objects()->set_data_size(dataSize);
        }
        if (afterInvoke) {
            afterInvoke();
        }
        return Status::OK();
    }

    bool IsAlive() const override
    {
        return alive;
    }

    void Close() override
    {
        alive = false;
    }

    bool alive = true;
    Status initStatus = Status::OK();
    Status queryStatus = Status::OK();
    Status invokeStatus = Status::OK();
    std::vector<uint64_t> objectSizes;
    std::unordered_map<std::string, int64_t> payloadSizes;
    std::vector<GetReqPb> invokedRequests;
    uint32_t version = 7;
    int queryCount = 0;
    int invokeCount = 0;
    bool mixedUbResponse = false;
    bool invalidFallbackIndex = false;
    StatusCode responseCode = K_OK;
    std::function<void()> onInvoke;
    std::function<void()> afterInvoke;
    std::vector<int64_t> responseObjectDataSizes;
};

class FailingAuth : public ClientRequestAuth {
public:
    Status Authenticate(GetReqPb &) override
    {
        ++getAuthCount;
        return Status(K_INVALID, "authentication failed");
    }

    Status Authenticate(GetObjMetaInfoReqPb &) override
    {
        ++metaAuthCount;
        return Status(K_INVALID, "authentication failed");
    }

    Status UpdateAkSk(const std::string &, SensitiveValue) override
    {
        return Status::OK();
    }

    int getAuthCount = 0;
    int metaAuthCount = 0;
};

class AuthBoundaryWorkerRpcClient : public WorkerRpcClient {
public:
    explicit AuthBoundaryWorkerRpcClient(std::shared_ptr<ClientRequestAuth> auth)
        : WorkerRpcClient(MakeAddress(9001), std::move(auth))
    {
    }

    bool IsAlive() const override
    {
        return true;
    }

    int invokeCount = 0;
    int queryCount = 0;
    int rpcTimeout = 0;
    int queryRpcTimeout = 0;
    GetReqPb invokedRequest;

protected:
    Status DoQueryObjectSizes(const RpcOptions &options, const GetObjMetaInfoReqPb &,
                              GetObjMetaInfoRspPb &) override
    {
        ++queryCount;
        queryRpcTimeout = options.GetTimeout();
        return Status::OK();
    }

    Status DoInvokeGet(const RpcOptions &options, const GetReqPb &request, GetRspPb &,
                       std::vector<RpcMessage> &) override
    {
        ++invokeCount;
        rpcTimeout = options.GetTimeout();
        invokedRequest = request;
        return Status::OK();
    }
};

class FakeTransporter : public IDataTransporter {
public:
    AccessTransportKind Kind() const override
    {
        return kind;
    }

    bool IsAlive() const override
    {
        return alive && rpcClient != nullptr && rpcClient->IsAlive();
    }

    Status Get(const TransportGetRequest &, TransportGetResult &output) override
    {
        output.Clear();
        output.actualKind = kind;
        if (!getStatuses.empty()) {
            Status rc = getStatuses.front();
            getStatuses.erase(getStatuses.begin());
            return rc;
        }
        return Status::OK();
    }

    void CloseDataPlane() override
    {
        ++closeCount;
        alive = false;
        if (onClose) {
            onClose();
        }
    }

    AccessTransportKind kind = AccessTransportKind::TCP;
    std::shared_ptr<WorkerRpcClient> rpcClient;
    bool alive = true;
    int closeCount = 0;
    std::vector<Status> getStatuses;
    std::function<void()> onClose;
};

class FakeDataPlaneManager : public DataPlaneManager {
public:
    FakeDataPlaneManager() : DataPlaneManager(MakeAuth())
    {
    }

    Status CreateWorkerRpcClient(const HostPort &address, std::shared_ptr<WorkerRpcClient> &output) override
    {
        ++rpcBuildCount;
        auto client = std::make_shared<FakeWorkerRpcClient>(address);
        RETURN_IF_NOT_OK(client->Init());
        lastRpcClient = client;
        output = std::move(client);
        return Status::OK();
    }

    Status BuildTransporter(const HostPort &, TransportHint hint,
                            const std::shared_ptr<WorkerRpcClient> &rpcClient,
                            std::shared_ptr<IDataTransporter> &output) override
    {
        ++transportBuildCount;
        rpcClientsSeen.push_back(rpcClient);
        if (!transportBuildStatuses.empty()) {
            Status rc = transportBuildStatuses.front();
            transportBuildStatuses.erase(transportBuildStatuses.begin());
            if (rc.IsError()) {
                return rc;
            }
        }
        auto transporter = std::make_shared<FakeTransporter>();
        transporter->kind = hint == TransportHint::TCP_ONLY ? AccessTransportKind::TCP : AccessTransportKind::UB;
        transporter->rpcClient = rpcClient;
        if (!transporterGetStatuses.empty()) {
            transporter->getStatuses = std::move(transporterGetStatuses.front());
            transporterGetStatuses.erase(transporterGetStatuses.begin());
        }
        lastTransporter = transporter;
        builtTransporters.emplace_back(transporter);
        output = std::move(transporter);
        return Status::OK();
    }

    int rpcBuildCount = 0;
    int transportBuildCount = 0;
    std::shared_ptr<FakeWorkerRpcClient> lastRpcClient;
    std::shared_ptr<FakeTransporter> lastTransporter;
    std::vector<std::shared_ptr<WorkerRpcClient>> rpcClientsSeen;
    std::vector<Status> transportBuildStatuses;
    std::vector<std::vector<Status>> transporterGetStatuses;
    std::vector<std::shared_ptr<FakeTransporter>> builtTransporters;
};

class FakeUbConnection : public UbConnection {
public:
    Status Establish(const HostPort &) override
    {
        alive = true;
        return Status::OK();
    }

    bool IsAlive() const override
    {
        return alive.load();
    }

    void Teardown() override
    {
        if (invokeFinished != nullptr && !invokeFinished->load()) {
            teardownDuringInvoke.store(true);
        }
        alive.store(false);
    }

    std::atomic<bool> alive{ true };
    std::atomic<bool> teardownDuringInvoke{ false };
    std::atomic<bool> *invokeFinished = nullptr;
};

class TestTransportLayer : public TransportLayer {
public:
    explicit TestTransportLayer(std::shared_ptr<DataPlaneManager> dataPlaneManager)
        : TransportLayer(std::move(dataPlaneManager), std::make_shared<TransportAdvisor>())
    {
    }
};

class FakeBufferOwner : public IReceiveBufferOwner {
public:
    explicit FakeBufferOwner(uint64_t size) : data(size)
    {
    }

    std::vector<uint8_t> data;
};

class FakeUbBufferProvider : public IUbReceiveBufferProvider {
public:
    uint64_t MaxGetSize() const override
    {
        return maxGetSize;
    }

    Status Allocate(uint64_t requiredSize, uint8_t *&data, uint64_t &size, UrmaRemoteAddrPb &remoteAddr,
                    std::shared_ptr<IReceiveBufferOwner> &owner) override
    {
        ++allocateCount;
        if (allocateStatus.IsError()) {
            return allocateStatus;
        }
        auto fakeOwner = std::make_shared<FakeBufferOwner>(requiredSize);
        data = fakeOwner->data.data();
        size = fakeOwner->data.size();
        remoteAddr.set_seg_va(reinterpret_cast<uint64_t>(data));
        owner = fakeOwner;
        lastOwner = fakeOwner;
        return Status::OK();
    }

    uint64_t maxGetSize = 16;
    Status allocateStatus = Status::OK();
    int allocateCount = 0;
    std::weak_ptr<FakeBufferOwner> lastOwner;
};

TEST(ClientRequestAuthTest, FillsStableIdentityTenantAndAkSk)
{
    DefaultClientRequestAuth auth("client-1", "access-1", SensitiveValue("secret-1"),
                                  "tenant-1");
    GetReqPb request;
    request.mutable_urma_info()->set_seg_va(123);
    request.set_ub_buffer_size(64);
    ASSERT_TRUE(auth.Authenticate(request).IsOk());
    EXPECT_EQ(request.client_id(), "client-1");
    EXPECT_TRUE(request.token().empty());
    EXPECT_EQ(request.tenant_id(), "tenant-1");
    EXPECT_EQ(request.access_key(), "access-1");
    EXPECT_FALSE(request.signature().empty());
    EXPECT_EQ(request.urma_info().seg_va(), 123u);

    GetObjMetaInfoReqPb metaRequest;
    ASSERT_TRUE(auth.Authenticate(metaRequest).IsOk());
    EXPECT_EQ(metaRequest.tenantid(), "tenant-1");
    EXPECT_EQ(metaRequest.access_key(), "access-1");
    EXPECT_FALSE(metaRequest.signature().empty());
}

TEST(ClientRequestAuthTest, UpdatesCredentialsWithoutChangingClientIdentity)
{
    DefaultClientRequestAuth auth("client-1");
    ASSERT_TRUE(auth.UpdateAkSk("access-2", SensitiveValue("secret-2")).IsOk());
    GetReqPb request;
    ASSERT_TRUE(auth.Authenticate(request).IsOk());
    EXPECT_EQ(request.client_id(), "client-1");
    EXPECT_TRUE(request.token().empty());
    EXPECT_EQ(request.access_key(), "access-2");
}

TEST(ClientRequestAuthTest, ThreadTenantOverridesDefaultTenant)
{
    const std::string previousTenant = g_ContextTenantId;
    Raii restoreTenant([previousTenant]() { g_ContextTenantId = previousTenant; });
    g_ContextTenantId = "thread-tenant";
    DefaultClientRequestAuth auth("client-1", "", {}, "default-tenant");
    GetReqPb request;

    ASSERT_TRUE(auth.Authenticate(request).IsOk());
    EXPECT_EQ(request.tenant_id(), "thread-tenant");
}

TEST(WorkerRpcClientTest, AuthenticationFailureDoesNotSendRpc)
{
    auto auth = std::make_shared<FailingAuth>();
    AuthBoundaryWorkerRpcClient client(auth);
    GetReqPb request;
    GetRspPb response;
    std::vector<RpcMessage> payloads;
    uint32_t workerVersion = 0;

    EXPECT_EQ(client.InvokeGet(100, request, response, payloads, workerVersion).GetCode(), K_INVALID);
    EXPECT_EQ(auth->getAuthCount, 1);
    EXPECT_EQ(client.invokeCount, 0);

    std::vector<uint64_t> objectSizes;
    EXPECT_EQ(client.QueryObjectSizes({ "key" }, objectSizes).GetCode(), K_INVALID);
    EXPECT_EQ(auth->metaAuthCount, 1);
    EXPECT_EQ(client.queryCount, 0);
}

TEST(WorkerRpcClientTest, AuthenticatesFinalGetRequestBeforeRpc)
{
    auto auth = std::make_shared<DefaultClientRequestAuth>("client-1", "access-1", SensitiveValue("secret-1"));
    AuthBoundaryWorkerRpcClient client(auth);
    GetReqPb request;
    request.mutable_urma_info()->set_seg_va(123);
    request.set_ub_buffer_size(64);
    GetRspPb response;
    std::vector<RpcMessage> payloads;
    uint32_t workerVersion = 0;

    ASSERT_TRUE(client.InvokeGet(800, request, response, payloads, workerVersion).IsOk());
    EXPECT_EQ(client.invokeCount, 1);
    EXPECT_EQ(client.rpcTimeout, 800);
    EXPECT_EQ(client.invokedRequest.client_id(), "client-1");
    EXPECT_EQ(client.invokedRequest.access_key(), "access-1");
    EXPECT_FALSE(client.invokedRequest.signature().empty());
    EXPECT_EQ(client.invokedRequest.urma_info().seg_va(), 123u);
    EXPECT_EQ(client.invokedRequest.ub_buffer_size(), 64u);
}

TEST(WorkerRpcClientTest, BoundsRpcTimeoutByApiDeadline)
{
    ApiDeadlineGuard deadline(100);
    auto auth = std::make_shared<DefaultClientRequestAuth>("client-1");
    AuthBoundaryWorkerRpcClient client(auth);
    GetReqPb request;
    GetRspPb response;
    std::vector<RpcMessage> payloads;
    uint32_t workerVersion = 0;

    ASSERT_TRUE(client.InvokeGet(800, request, response, payloads, workerVersion).IsOk());
    EXPECT_GT(client.rpcTimeout, 0);
    EXPECT_LE(client.rpcTimeout, 100);

    std::vector<uint64_t> objectSizes;
    ASSERT_TRUE(client.QueryObjectSizes({ "key" }, objectSizes).IsOk());
    EXPECT_GT(client.queryRpcTimeout, 0);
    EXPECT_LE(client.queryRpcTimeout, 100);
}

TEST(WorkerRpcClientTest, ExpiredApiDeadlineDoesNotSendRpc)
{
    ApiDeadlineGuard deadline(-1, InUs{});
    auto auth = std::make_shared<DefaultClientRequestAuth>("client-1");
    AuthBoundaryWorkerRpcClient client(auth);
    GetReqPb request;
    GetRspPb response;
    std::vector<RpcMessage> payloads;
    uint32_t workerVersion = 0;

    EXPECT_EQ(client.InvokeGet(100, request, response, payloads, workerVersion).GetCode(),
              K_RPC_DEADLINE_EXCEEDED);
    EXPECT_EQ(client.invokeCount, 0);

    std::vector<uint64_t> objectSizes;
    EXPECT_EQ(client.QueryObjectSizes({ "key" }, objectSizes).GetCode(), K_RPC_DEADLINE_EXCEEDED);
    EXPECT_EQ(client.queryCount, 0);
}

TEST(DataPlaneManagerTest, ReusesRpcClientAndTransporterForSameAddress)
{
    FakeDataPlaneManager manager;
    std::shared_ptr<IDataTransporter> first;
    std::shared_ptr<IDataTransporter> second;
    ASSERT_TRUE(manager.GetOrCreate(MakeAddress(1), TransportHint::TCP_ONLY, first).IsOk());
    ASSERT_TRUE(manager.GetOrCreate(MakeAddress(1), TransportHint::TCP_ONLY, second).IsOk());
    EXPECT_EQ(first, second);
    EXPECT_EQ(manager.rpcBuildCount, 1);
    EXPECT_EQ(manager.transportBuildCount, 1);
}

TEST(DataPlaneManagerTest, DifferentAddressesUseIndependentEntries)
{
    FakeDataPlaneManager manager;
    std::shared_ptr<IDataTransporter> first;
    std::shared_ptr<IDataTransporter> second;

    ASSERT_TRUE(manager.GetOrCreate(MakeAddress(10), TransportHint::TCP_ONLY, first).IsOk());
    ASSERT_TRUE(manager.GetOrCreate(MakeAddress(11), TransportHint::TCP_ONLY, second).IsOk());
    EXPECT_NE(first, second);
    EXPECT_EQ(manager.rpcBuildCount, 2);
    EXPECT_EQ(manager.transportBuildCount, 2);
    ASSERT_EQ(manager.rpcClientsSeen.size(), 2u);
    EXPECT_NE(manager.rpcClientsSeen[0], manager.rpcClientsSeen[1]);
}

TEST(DataPlaneManagerTest, TransportKindChangeReusesRpcClient)
{
    FakeDataPlaneManager manager;
    std::shared_ptr<IDataTransporter> tcp;
    std::shared_ptr<IDataTransporter> ub;
    ASSERT_TRUE(manager.GetOrCreate(MakeAddress(2), TransportHint::TCP_ONLY, tcp).IsOk());
    ASSERT_TRUE(manager.GetOrCreate(MakeAddress(2), TransportHint::UB_CANDIDATE, ub).IsOk());
    ASSERT_EQ(manager.rpcClientsSeen.size(), 2u);
    EXPECT_EQ(manager.rpcClientsSeen[0], manager.rpcClientsSeen[1]);
    EXPECT_EQ(manager.rpcBuildCount, 1);
    EXPECT_EQ(manager.transportBuildCount, 2);
}

TEST(DataPlaneManagerTest, ResetDataPlaneKeepsRpcClient)
{
    FakeDataPlaneManager manager;
    std::shared_ptr<IDataTransporter> first;
    std::shared_ptr<IDataTransporter> second;
    HostPort address = MakeAddress(3);
    ASSERT_TRUE(manager.GetOrCreate(address, TransportHint::TCP_ONLY, first).IsOk());
    auto firstFake = std::dynamic_pointer_cast<FakeTransporter>(first);
    manager.ResetDataPlane(address);
    ASSERT_TRUE(manager.GetOrCreate(address, TransportHint::TCP_ONLY, second).IsOk());
    EXPECT_NE(first, second);
    EXPECT_EQ(manager.rpcBuildCount, 1);
    EXPECT_EQ(manager.transportBuildCount, 2);
    ASSERT_NE(firstFake, nullptr);
    EXPECT_EQ(firstFake->closeCount, 1);
}

TEST(DataPlaneManagerTest, DeadRpcClientRebuildsWholeEntry)
{
    FakeDataPlaneManager manager;
    const HostPort address = MakeAddress(12);
    std::shared_ptr<IDataTransporter> first;
    ASSERT_TRUE(manager.GetOrCreate(address, TransportHint::TCP_ONLY, first).IsOk());
    auto firstTransporter = std::dynamic_pointer_cast<FakeTransporter>(first);
    ASSERT_NE(firstTransporter, nullptr);
    ASSERT_NE(manager.lastRpcClient, nullptr);
    manager.lastRpcClient->alive = false;

    std::shared_ptr<IDataTransporter> second;
    ASSERT_TRUE(manager.GetOrCreate(address, TransportHint::TCP_ONLY, second).IsOk());
    EXPECT_NE(first, second);
    EXPECT_EQ(firstTransporter->closeCount, 1);
    EXPECT_EQ(manager.rpcBuildCount, 2);
    EXPECT_EQ(manager.transportBuildCount, 2);
}

TEST(DataPlaneManagerTest, TransportBuildFailureRetainsRpcClient)
{
    FakeDataPlaneManager manager;
    manager.transportBuildStatuses = { Status(K_RUNTIME_ERROR, "build failed"), Status::OK() };
    const HostPort address = MakeAddress(13);
    std::shared_ptr<IDataTransporter> transporter;

    EXPECT_EQ(manager.GetOrCreate(address, TransportHint::TCP_ONLY, transporter).GetCode(), K_RUNTIME_ERROR);
    EXPECT_EQ(transporter, nullptr);
    ASSERT_TRUE(manager.GetOrCreate(address, TransportHint::TCP_ONLY, transporter).IsOk());
    EXPECT_EQ(manager.rpcBuildCount, 1);
    EXPECT_EQ(manager.transportBuildCount, 2);
    ASSERT_EQ(manager.rpcClientsSeen.size(), 2u);
    EXPECT_EQ(manager.rpcClientsSeen[0], manager.rpcClientsSeen[1]);
}

TEST(DataPlaneManagerTest, TeardownRebuildsRpcClient)
{
    FakeDataPlaneManager manager;
    std::shared_ptr<IDataTransporter> transporter;
    HostPort address = MakeAddress(4);
    ASSERT_TRUE(manager.GetOrCreate(address, TransportHint::TCP_ONLY, transporter).IsOk());
    auto firstTransporter = std::dynamic_pointer_cast<FakeTransporter>(transporter);
    ASSERT_NE(firstTransporter, nullptr);
    manager.Teardown(address);
    EXPECT_EQ(firstTransporter->closeCount, 1);
    ASSERT_TRUE(manager.GetOrCreate(address, TransportHint::TCP_ONLY, transporter).IsOk());
    EXPECT_EQ(manager.rpcBuildCount, 2);
    EXPECT_EQ(manager.transportBuildCount, 2);
}

TEST(DataPlaneManagerTest, ShutdownClosesDataPlaneAndRejectsNewRequests)
{
    FakeDataPlaneManager manager;
    const HostPort address = MakeAddress(14);
    std::shared_ptr<IDataTransporter> transporter;
    ASSERT_TRUE(manager.GetOrCreate(address, TransportHint::TCP_ONLY, transporter).IsOk());
    auto cachedTransporter = std::dynamic_pointer_cast<FakeTransporter>(transporter);
    ASSERT_NE(cachedTransporter, nullptr);

    manager.Shutdown();
    EXPECT_EQ(cachedTransporter->closeCount, 1);
    EXPECT_EQ(manager.GetOrCreate(address, TransportHint::TCP_ONLY, transporter).GetCode(), K_SHUTTING_DOWN);
    EXPECT_EQ(transporter, nullptr);
}

TEST(DataPlaneManagerTest, ReconcileRemovesOnlyWorkersAbsentFromSnapshot)
{
    FakeDataPlaneManager manager;
    const HostPort sameHost = MakeAddress(15);
    const HostPort otherHost = MakeAddress(16);
    const HostPort removed = MakeAddress(17);
    std::shared_ptr<IDataTransporter> transporter;
    ASSERT_TRUE(manager.GetOrCreate(sameHost, TransportHint::TCP_ONLY, transporter).IsOk());
    auto sameHostTransporter = transporter;
    ASSERT_TRUE(manager.GetOrCreate(otherHost, TransportHint::TCP_ONLY, transporter).IsOk());
    auto otherHostTransporter = transporter;
    ASSERT_TRUE(manager.GetOrCreate(removed, TransportHint::TCP_ONLY, transporter).IsOk());
    auto removedTransporter = std::dynamic_pointer_cast<FakeTransporter>(transporter);
    ASSERT_NE(removedTransporter, nullptr);

    WorkerSnapshot snapshot;
    snapshot.sameHostAddrs.push_back(sameHost);
    snapshot.otherAddrs.push_back(otherHost);
    manager.ReconcileWithSnapshot(snapshot);

    ASSERT_TRUE(manager.GetOrCreate(sameHost, TransportHint::TCP_ONLY, transporter).IsOk());
    EXPECT_EQ(transporter, sameHostTransporter);
    ASSERT_TRUE(manager.GetOrCreate(otherHost, TransportHint::TCP_ONLY, transporter).IsOk());
    EXPECT_EQ(transporter, otherHostTransporter);
    ASSERT_TRUE(manager.GetOrCreate(removed, TransportHint::TCP_ONLY, transporter).IsOk());
    EXPECT_EQ(removedTransporter->closeCount, 1);
    EXPECT_EQ(manager.rpcBuildCount, 4);
    EXPECT_EQ(manager.transportBuildCount, 4);
}

TEST(DataPlaneManagerTest, ShutdownPublishesStateBeforeSlowDataPlaneCloseCompletes)
{
    FakeDataPlaneManager manager;
    std::shared_ptr<IDataTransporter> transporter;
    ASSERT_TRUE(manager.GetOrCreate(MakeAddress(18), TransportHint::TCP_ONLY, transporter).IsOk());
    auto fakeTransporter = std::dynamic_pointer_cast<FakeTransporter>(transporter);
    ASSERT_NE(fakeTransporter, nullptr);

    std::promise<void> closeStarted;
    auto closeStartedFuture = closeStarted.get_future();
    std::promise<void> allowClose;
    auto allowCloseFuture = allowClose.get_future().share();
    fakeTransporter->onClose = [&closeStarted, allowCloseFuture]() {
        closeStarted.set_value();
        allowCloseFuture.wait();
    };

    std::thread shutdownThread([&manager]() { manager.Shutdown(); });
    closeStartedFuture.wait();
    EXPECT_EQ(manager.GetOrCreate(MakeAddress(19), TransportHint::TCP_ONLY, transporter).GetCode(), K_SHUTTING_DOWN);
    allowClose.set_value();
    shutdownThread.join();
}

TEST(DataPlaneManagerTest, ReconcileReleasesMapLockBeforeSlowDataPlaneClose)
{
    FakeDataPlaneManager manager;
    std::shared_ptr<IDataTransporter> transporter;
    const HostPort removed = MakeAddress(20);
    ASSERT_TRUE(manager.GetOrCreate(removed, TransportHint::TCP_ONLY, transporter).IsOk());
    auto removedTransporter = std::dynamic_pointer_cast<FakeTransporter>(transporter);
    ASSERT_NE(removedTransporter, nullptr);

    std::promise<void> closeStarted;
    auto closeStartedFuture = closeStarted.get_future();
    std::promise<void> allowClose;
    auto allowCloseFuture = allowClose.get_future().share();
    removedTransporter->onClose = [&closeStarted, allowCloseFuture]() {
        closeStarted.set_value();
        allowCloseFuture.wait();
    };

    WorkerSnapshot snapshot;
    std::thread reconcileThread([&manager, &snapshot]() { manager.ReconcileWithSnapshot(snapshot); });
    closeStartedFuture.wait();
    EXPECT_TRUE(manager.GetOrCreate(MakeAddress(21), TransportHint::TCP_ONLY, transporter).IsOk());
    allowClose.set_value();
    reconcileThread.join();
}

TEST(TcpTransporterTest, BuildsRequestAndPreservesPayloadOwnership)
{
    auto rpcClient = std::make_shared<FakeWorkerRpcClient>();
    rpcClient->payloadSizes = { { "a", 3 }, { "b", 5 } };
    std::vector<std::string> keys{ "a", "b" };
    std::vector<ReadParam> reads{ { "a", 1, 2 }, { "b", 2, 3 } };
    TransportGetRequest request(keys, reads, 123, false);
    TransportGetResult result;

    TcpTransporter transporter(rpcClient);
    ASSERT_TRUE(transporter.Get(request, result).IsOk());
    ASSERT_EQ(result.batches.size(), 1u);
    EXPECT_EQ(result.actualKind, AccessTransportKind::TCP);
    EXPECT_EQ(result.batches[0].workerVersion, 7u);
    EXPECT_EQ(result.batches[0].response.payload_info_size(), 2);
    EXPECT_EQ(result.batches[0].rpcPayloads.size(), 2u);
    ASSERT_EQ(rpcClient->invokedRequests.size(), 1u);
    const auto &invokedRequest = rpcClient->invokedRequests[0];
    ASSERT_EQ(invokedRequest.object_keys_size(), 2);
    EXPECT_EQ(invokedRequest.object_keys(0), "a");
    EXPECT_EQ(invokedRequest.object_keys(1), "b");
    EXPECT_TRUE(invokedRequest.no_query_l2cache());
    EXPECT_TRUE(invokedRequest.return_object_index());
    EXPECT_EQ(invokedRequest.sub_timeout(), 110);
    EXPECT_EQ(invokedRequest.request_timeout(), RPC_TIMEOUT);
    EXPECT_EQ(invokedRequest.read_offset_list(1), 2u);
    EXPECT_EQ(invokedRequest.read_size_list(1), 3u);
}

TEST(TcpTransporterTest, PropagatesRpcError)
{
    auto rpcClient = std::make_shared<FakeWorkerRpcClient>();
    rpcClient->invokeStatus = Status(K_RPC_DEADLINE_EXCEEDED, "deadline");
    TcpTransporter transporter(rpcClient);
    std::vector<std::string> keys{ "a" };
    std::vector<ReadParam> reads;
    TransportGetRequest request(keys, reads, 10, true);
    TransportGetResult result;
    EXPECT_EQ(transporter.Get(request, result).GetCode(), K_RPC_DEADLINE_EXCEEDED);
    EXPECT_EQ(rpcClient->invokeCount, 1);
}

TEST(TcpTransporterTest, PreservesPartialBusinessErrorForResponseProcessing)
{
    auto rpcClient = std::make_shared<FakeWorkerRpcClient>();
    rpcClient->responseCode = K_OUT_OF_MEMORY;
    rpcClient->responseObjectDataSizes = { 4, -1 };
    TcpTransporter transporter(rpcClient);
    std::vector<std::string> keys{ "a", "b" };
    std::vector<ReadParam> reads;
    TransportGetRequest request(keys, reads, 10, true);
    TransportGetResult result;

    ASSERT_TRUE(transporter.Get(request, result).IsOk());
    ASSERT_EQ(result.batches.size(), 1u);
    EXPECT_EQ(result.batches[0].response.last_rc().error_code(), K_OUT_OF_MEMORY);
}

TEST(TcpTransporterTest, PropagatesAllFailedOutOfMemory)
{
    auto rpcClient = std::make_shared<FakeWorkerRpcClient>();
    rpcClient->responseCode = K_OUT_OF_MEMORY;
    rpcClient->responseObjectDataSizes = { -1 };
    TcpTransporter transporter(rpcClient);
    std::vector<std::string> keys{ "a" };
    std::vector<ReadParam> reads;
    TransportGetRequest request(keys, reads, 10, true);
    TransportGetResult result;

    EXPECT_EQ(transporter.Get(request, result).GetCode(), K_OUT_OF_MEMORY);
}

TEST(TcpTransporterTest, PreservesNotFoundForResponseProcessing)
{
    auto rpcClient = std::make_shared<FakeWorkerRpcClient>();
    rpcClient->responseCode = K_NOT_FOUND;
    rpcClient->responseObjectDataSizes = { -1 };
    TcpTransporter transporter(rpcClient);
    std::vector<std::string> keys{ "missing" };
    std::vector<ReadParam> reads;
    TransportGetRequest request(keys, reads, 10, true);
    TransportGetResult result;

    ASSERT_TRUE(transporter.Get(request, result).IsOk());
    ASSERT_EQ(result.batches.size(), 1u);
    EXPECT_EQ(result.batches[0].response.last_rc().error_code(), K_NOT_FOUND);
}

TEST(TcpTransporterTest, RejectsInvalidRequestBeforeRpc)
{
    auto rpcClient = std::make_shared<FakeWorkerRpcClient>();
    TcpTransporter transporter(rpcClient);
    std::vector<std::string> keys{ "a" };
    std::vector<ReadParam> mismatchedReads{ { "a", 0, 1 }, { "b", 0, 1 } };
    TransportGetRequest mismatchedRequest(keys, mismatchedReads, 10, true);
    TransportGetResult result;
    EXPECT_EQ(transporter.Get(mismatchedRequest, result).GetCode(), K_INVALID);

    std::vector<ReadParam> reads;
    TransportGetRequest negativeTimeoutRequest(keys, reads, -1, true);
    EXPECT_EQ(transporter.Get(negativeTimeoutRequest, result).GetCode(), K_INVALID);
    EXPECT_EQ(rpcClient->invokeCount, 0);
}

TEST(UbTransporterTest, BuildsZeroCopySpansAndKeepsOwnerAlive)
{
    auto rpcClient = std::make_shared<FakeWorkerRpcClient>();
    rpcClient->objectSizes = { 4, 5 };
    rpcClient->payloadSizes = { { "a", 4 }, { "b", 5 } };
    auto provider = std::make_shared<FakeUbBufferProvider>();
    UbTransporter transporter(rpcClient, std::make_shared<FakeUbConnection>(), provider);
    std::vector<std::string> keys{ "a", "b" };
    std::vector<ReadParam> reads;
    TransportGetRequest request(keys, reads, 10, true);
    TransportGetResult result;

    ASSERT_TRUE(transporter.Get(request, result).IsOk());
    ASSERT_EQ(result.batches.size(), 1u);
    EXPECT_EQ(result.actualKind, AccessTransportKind::UB);
    EXPECT_EQ(result.batches[0].workerVersion, 7u);
    ASSERT_EQ(result.batches[0].externalPayloads.size(), 2u);
    EXPECT_EQ(result.batches[0].externalPayloads[0].payloadInfoIndex, 0u);
    EXPECT_EQ(result.batches[0].externalPayloads[1].payloadInfoIndex, 1u);
    EXPECT_EQ(result.batches[0].externalPayloads[0].size, 4u);
    EXPECT_EQ(result.batches[0].externalPayloads[1].size, 5u);
    ASSERT_EQ(rpcClient->invokedRequests.size(), 1u);
    EXPECT_TRUE(rpcClient->invokedRequests[0].has_urma_info());
    EXPECT_EQ(rpcClient->invokedRequests[0].ub_buffer_size(), 9u);
    EXPECT_FALSE(provider->lastOwner.expired());
    result.Clear();
    EXPECT_TRUE(provider->lastOwner.expired());
}

TEST(UbTransporterTest, SplitsBatchesAndPreservesOriginalIndices)
{
    auto rpcClient = std::make_shared<FakeWorkerRpcClient>();
    rpcClient->objectSizes = { 8, 8, 1 };
    rpcClient->payloadSizes = { { "a", 8 }, { "b", 8 }, { "c", 1 } };
    auto provider = std::make_shared<FakeUbBufferProvider>();
    provider->maxGetSize = 10;
    UbTransporter transporter(rpcClient, std::make_shared<FakeUbConnection>(), provider);
    std::vector<std::string> keys{ "a", "b", "c" };
    std::vector<ReadParam> reads{ { "a", 1, 2 }, { "b", 2, 3 }, { "c", 3, 1 } };
    TransportGetRequest request(keys, reads, 10, true);
    TransportGetResult result;

    ASSERT_TRUE(transporter.Get(request, result).IsOk());
    ASSERT_EQ(result.batches.size(), 2u);
    EXPECT_EQ(result.batches[0].requestIndices, std::vector<size_t>({ 0 }));
    EXPECT_EQ(result.batches[1].requestIndices, std::vector<size_t>({ 1, 2 }));
    EXPECT_EQ(provider->allocateCount, 2);
    ASSERT_EQ(rpcClient->invokedRequests.size(), 2u);
    EXPECT_EQ(rpcClient->invokedRequests[0].object_keys(0), "a");
    EXPECT_EQ(rpcClient->invokedRequests[0].read_offset_list(0), 1u);
    EXPECT_EQ(rpcClient->invokedRequests[1].object_keys(0), "b");
    EXPECT_EQ(rpcClient->invokedRequests[1].object_keys(1), "c");
    EXPECT_EQ(rpcClient->invokedRequests[1].read_offset_list(0), 2u);
    EXPECT_EQ(rpcClient->invokedRequests[1].read_offset_list(1), 3u);
}

TEST(UbTransporterTest, PreservesTcpFallbackInsideUbBatch)
{
    auto rpcClient = std::make_shared<FakeWorkerRpcClient>();
    rpcClient->objectSizes = { 4, 5 };
    rpcClient->payloadSizes = { { "a", 4 }, { "b", 5 } };
    rpcClient->mixedUbResponse = true;
    auto provider = std::make_shared<FakeUbBufferProvider>();
    UbTransporter transporter(rpcClient, std::make_shared<FakeUbConnection>(), provider);
    std::vector<std::string> keys{ "a", "b" };
    std::vector<ReadParam> reads;
    TransportGetRequest request(keys, reads, 10, true);
    TransportGetResult result;

    ASSERT_TRUE(transporter.Get(request, result).IsOk());
    ASSERT_EQ(result.batches.size(), 1u);
    EXPECT_EQ(result.actualKind, AccessTransportKind::TCP);
    EXPECT_EQ(result.batches[0].kind, AccessTransportKind::TCP);
    EXPECT_EQ(result.batches[0].externalPayloads.size(), 1u);
    EXPECT_EQ(result.batches[0].rpcPayloads.size(), 1u);
}

TEST(UbTransporterTest, DeadConnectionRequestsUbReconnectBeforeRpc)
{
    auto rpcClient = std::make_shared<FakeWorkerRpcClient>();
    auto connection = std::make_shared<FakeUbConnection>();
    connection->alive = false;
    auto provider = std::make_shared<FakeUbBufferProvider>();
    UbTransporter transporter(rpcClient, connection, provider);
    std::vector<std::string> keys{ "a" };
    std::vector<ReadParam> reads;
    TransportGetRequest request(keys, reads, 10, true);
    TransportGetResult result;

    EXPECT_EQ(transporter.Get(request, result).GetCode(), K_URMA_NEED_CONNECT);
    EXPECT_EQ(rpcClient->queryCount, 0);
    EXPECT_EQ(rpcClient->invokeCount, 0);
}

TEST(UbTransporterTest, RejectsInvalidRequestBeforeMetadataRpc)
{
    auto rpcClient = std::make_shared<FakeWorkerRpcClient>();
    auto provider = std::make_shared<FakeUbBufferProvider>();
    UbTransporter transporter(rpcClient, std::make_shared<FakeUbConnection>(), provider);
    std::vector<std::string> keys{ "a" };
    std::vector<ReadParam> reads;
    TransportGetRequest request(keys, reads, -1, true);
    TransportGetResult result;

    EXPECT_EQ(transporter.Get(request, result).GetCode(), K_INVALID);
    EXPECT_EQ(rpcClient->queryCount, 0);
    EXPECT_EQ(rpcClient->invokeCount, 0);
}

TEST(UbTransporterTest, PropagatesWorkerUbReconnectResponse)
{
    auto rpcClient = std::make_shared<FakeWorkerRpcClient>();
    rpcClient->objectSizes = { 4 };
    rpcClient->payloadSizes = { { "a", 4 } };
    rpcClient->responseCode = K_URMA_NEED_CONNECT;
    auto provider = std::make_shared<FakeUbBufferProvider>();
    UbTransporter transporter(rpcClient, std::make_shared<FakeUbConnection>(), provider);
    std::vector<std::string> keys{ "a" };
    std::vector<ReadParam> reads;
    TransportGetRequest request(keys, reads, 10, true);
    TransportGetResult result;

    EXPECT_EQ(transporter.Get(request, result).GetCode(), K_URMA_NEED_CONNECT);
    EXPECT_TRUE(result.batches.empty());
}

TEST(UbTransporterTest, CloseDataPlaneWaitsForInflightGet)
{
    auto rpcClient = std::make_shared<FakeWorkerRpcClient>();
    rpcClient->objectSizes = { 4 };
    rpcClient->payloadSizes = { { "a", 4 } };
    auto connection = std::make_shared<FakeUbConnection>();
    std::atomic<bool> invokeFinished{ false };
    connection->invokeFinished = &invokeFinished;
    std::promise<void> invokeStarted;
    auto invokeStartedFuture = invokeStarted.get_future();
    std::promise<void> allowInvoke;
    auto allowInvokeFuture = allowInvoke.get_future().share();
    rpcClient->onInvoke = [&invokeStarted, allowInvokeFuture]() {
        invokeStarted.set_value();
        allowInvokeFuture.wait();
    };
    rpcClient->afterInvoke = [&invokeFinished]() { invokeFinished.store(true); };

    auto provider = std::make_shared<FakeUbBufferProvider>();
    UbTransporter transporter(rpcClient, connection, provider);
    std::vector<std::string> keys{ "a" };
    std::vector<ReadParam> reads;
    TransportGetRequest request(keys, reads, 10, true);
    TransportGetResult result;

    Status getStatus;
    std::thread getThread([&]() { getStatus = transporter.Get(request, result); });
    invokeStartedFuture.wait();
    std::thread closeThread([&]() { transporter.CloseDataPlane(); });
    allowInvoke.set_value();
    getThread.join();
    closeThread.join();
    EXPECT_TRUE(getStatus.IsOk());
    EXPECT_FALSE(connection->teardownDuringInvoke.load());
}

TEST(UbTransporterTest, PreservesPartialBusinessErrorForResponseProcessing)
{
    auto rpcClient = std::make_shared<FakeWorkerRpcClient>();
    rpcClient->objectSizes = { 4, 4 };
    rpcClient->payloadSizes = { { "a", 4 }, { "b", 4 } };
    rpcClient->responseCode = K_OUT_OF_MEMORY;
    rpcClient->responseObjectDataSizes = { 4, -1 };
    auto provider = std::make_shared<FakeUbBufferProvider>();
    UbTransporter transporter(rpcClient, std::make_shared<FakeUbConnection>(), provider);
    std::vector<std::string> keys{ "a", "b" };
    std::vector<ReadParam> reads;
    TransportGetRequest request(keys, reads, 10, true);
    TransportGetResult result;

    ASSERT_TRUE(transporter.Get(request, result).IsOk());
    ASSERT_EQ(result.batches.size(), 1u);
    EXPECT_EQ(result.batches[0].response.last_rc().error_code(), K_OUT_OF_MEMORY);
}

TEST(UbTransporterTest, OversizedObjectCreatesTcpBatchAndMarksOverallTransport)
{
    auto rpcClient = std::make_shared<FakeWorkerRpcClient>();
    rpcClient->objectSizes = { 8, 32, 4 };
    rpcClient->payloadSizes = { { "a", 8 }, { "b", 32 }, { "c", 4 } };
    auto provider = std::make_shared<FakeUbBufferProvider>();
    provider->maxGetSize = 16;
    UbTransporter transporter(rpcClient, std::make_shared<FakeUbConnection>(), provider);
    std::vector<std::string> keys{ "a", "b", "c" };
    std::vector<ReadParam> reads;
    TransportGetRequest request(keys, reads, 10, true);
    TransportGetResult result;

    ASSERT_TRUE(transporter.Get(request, result).IsOk());
    ASSERT_EQ(result.batches.size(), 3u);
    EXPECT_EQ(result.batches[0].kind, AccessTransportKind::UB);
    EXPECT_EQ(result.batches[1].kind, AccessTransportKind::TCP);
    EXPECT_EQ(result.batches[2].kind, AccessTransportKind::UB);
    EXPECT_EQ(result.batches[1].rpcPayloads.size(), 1u);
    EXPECT_EQ(result.actualKind, AccessTransportKind::TCP);
    EXPECT_EQ(provider->allocateCount, 2);
}

TEST(UbTransporterTest, AllocationFailureUsesTcpPayload)
{
    auto rpcClient = std::make_shared<FakeWorkerRpcClient>();
    rpcClient->objectSizes = { 8 };
    rpcClient->payloadSizes = { { "a", 8 } };
    auto provider = std::make_shared<FakeUbBufferProvider>();
    provider->allocateStatus = Status(K_RUNTIME_ERROR, "allocation failed");
    UbTransporter transporter(rpcClient, std::make_shared<FakeUbConnection>(), provider);
    std::vector<std::string> keys{ "a" };
    std::vector<ReadParam> reads;
    TransportGetRequest request(keys, reads, 10, true);
    TransportGetResult result;

    ASSERT_TRUE(transporter.Get(request, result).IsOk());
    EXPECT_EQ(result.actualKind, AccessTransportKind::TCP);
    EXPECT_EQ(result.batches[0].kind, AccessTransportKind::TCP);
    EXPECT_EQ(result.batches[0].rpcPayloads.size(), 1u);
    EXPECT_EQ(provider->allocateCount, 1);
}

TEST(UbTransporterTest, MetadataCountMismatchFallsBackToTcp)
{
    auto rpcClient = std::make_shared<FakeWorkerRpcClient>();
    rpcClient->objectSizes = { 8 };
    rpcClient->payloadSizes = { { "a", 8 }, { "b", 8 } };
    auto provider = std::make_shared<FakeUbBufferProvider>();
    UbTransporter transporter(rpcClient, std::make_shared<FakeUbConnection>(), provider);
    std::vector<std::string> keys{ "a", "b" };
    std::vector<ReadParam> reads;
    TransportGetRequest request(keys, reads, 10, true);
    TransportGetResult result;

    ASSERT_TRUE(transporter.Get(request, result).IsOk());
    ASSERT_EQ(result.batches.size(), 1u);
    EXPECT_EQ(result.actualKind, AccessTransportKind::TCP);
    EXPECT_EQ(result.batches[0].kind, AccessTransportKind::TCP);
    EXPECT_EQ(result.batches[0].rpcPayloads.size(), 2u);
    EXPECT_EQ(provider->allocateCount, 0);
}

TEST(UbTransporterTest, DetectsReceiveBufferOverflow)
{
    auto rpcClient = std::make_shared<FakeWorkerRpcClient>();
    rpcClient->objectSizes = { 4 };
    rpcClient->payloadSizes = { { "a", 5 } };
    auto provider = std::make_shared<FakeUbBufferProvider>();
    UbTransporter transporter(rpcClient, std::make_shared<FakeUbConnection>(), provider);
    std::vector<std::string> keys{ "a" };
    std::vector<ReadParam> reads;
    TransportGetRequest request(keys, reads, 10, true);
    TransportGetResult result;
    EXPECT_EQ(transporter.Get(request, result).GetCode(), K_RUNTIME_ERROR);
}

TEST(UbTransporterTest, RejectsInvalidFallbackPayloadIndex)
{
    auto rpcClient = std::make_shared<FakeWorkerRpcClient>();
    rpcClient->objectSizes = { 4 };
    rpcClient->payloadSizes = { { "a", 4 } };
    rpcClient->mixedUbResponse = true;
    rpcClient->invalidFallbackIndex = true;
    auto provider = std::make_shared<FakeUbBufferProvider>();
    UbTransporter transporter(rpcClient, std::make_shared<FakeUbConnection>(), provider);
    std::vector<std::string> keys{ "a" };
    std::vector<ReadParam> reads;
    TransportGetRequest request(keys, reads, 10, true);
    TransportGetResult result;

    EXPECT_EQ(transporter.Get(request, result).GetCode(), K_RUNTIME_ERROR);
}

TEST(UbTransporterTest, RejectsNegativeExternalPayloadSize)
{
    auto rpcClient = std::make_shared<FakeWorkerRpcClient>();
    rpcClient->objectSizes = { 1 };
    rpcClient->payloadSizes = { { "a", -1 } };
    auto provider = std::make_shared<FakeUbBufferProvider>();
    UbTransporter transporter(rpcClient, std::make_shared<FakeUbConnection>(), provider);
    std::vector<std::string> keys{ "a" };
    std::vector<ReadParam> reads;
    TransportGetRequest request(keys, reads, 10, true);
    TransportGetResult result;

    EXPECT_EQ(transporter.Get(request, result).GetCode(), K_RUNTIME_ERROR);
}

TEST(ShmTransporterTest, RemainsExplicitPlaceholder)
{
    ShmTransporter transporter;
    std::vector<std::string> keys{ "a" };
    std::vector<ReadParam> reads;
    TransportGetRequest request(keys, reads, 10, true);
    TransportGetResult result;
    EXPECT_EQ(transporter.Get(request, result).GetCode(), K_NOT_SUPPORTED);
}

TEST(TransportLayerTest, UrmaReconnectResetsOnlyDataPlaneAndRetriesOnce)
{
    auto manager = std::make_shared<FakeDataPlaneManager>();
    manager->transporterGetStatuses = { { Status(K_URMA_NEED_CONNECT, "reconnect") }, { Status::OK() } };
    TestTransportLayer layer(manager);
    std::vector<std::string> keys{ "a" };
    std::vector<ReadParam> reads;
    TransportGetRequest request(keys, reads, 10, true);
    TransportGetResult result;

    EXPECT_TRUE(layer.Get(MakeAddress(22), request, result).IsOk());
    EXPECT_EQ(manager->rpcBuildCount, 1);
    EXPECT_EQ(manager->transportBuildCount, 2);
    ASSERT_EQ(manager->builtTransporters.size(), 2u);
    EXPECT_EQ(manager->builtTransporters[0]->closeCount, 1);
}

TEST(TransportLayerTest, RpcUnavailableRebuildsCompleteEntryAndRetriesOnce)
{
    auto manager = std::make_shared<FakeDataPlaneManager>();
    manager->transporterGetStatuses = { { Status(K_RPC_UNAVAILABLE, "unavailable") }, { Status::OK() } };
    TestTransportLayer layer(manager);
    std::vector<std::string> keys{ "a" };
    std::vector<ReadParam> reads;
    TransportGetRequest request(keys, reads, 10, true);
    TransportGetResult result;

    EXPECT_TRUE(layer.Get(MakeAddress(23), request, result).IsOk());
    EXPECT_EQ(manager->rpcBuildCount, 2);
    EXPECT_EQ(manager->transportBuildCount, 2);
}

TEST(TransportLayerTest, DoesNotRetrySecondTransportFailure)
{
    auto manager = std::make_shared<FakeDataPlaneManager>();
    manager->transporterGetStatuses = {
        { Status(K_URMA_NEED_CONNECT, "first") }, { Status(K_URMA_NEED_CONNECT, "second") }
    };
    TestTransportLayer layer(manager);
    std::vector<std::string> keys{ "a" };
    std::vector<ReadParam> reads;
    TransportGetRequest request(keys, reads, 10, true);
    TransportGetResult result;

    EXPECT_EQ(layer.Get(MakeAddress(24), request, result).GetCode(), K_URMA_NEED_CONNECT);
    EXPECT_EQ(manager->rpcBuildCount, 1);
    EXPECT_EQ(manager->transportBuildCount, 2);
}

}  // namespace
}  // namespace client
}  // namespace datasystem
