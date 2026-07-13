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

/** Description: Transport RPC connection and data-plane unit tests. */

#include <gtest/gtest.h>

#include <atomic>
#include <cstdint>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "datasystem/client/transport/data_plane/data_plane_manager.h"
#include "datasystem/client/transport/data_plane/shm_transporter.h"
#include "datasystem/client/transport/data_plane/tcp_transporter.h"
#include "datasystem/client/transport/data_plane/ub_transporter.h"
#include "datasystem/client/transport/common/deadline_retry.h"
#include "datasystem/client/transport/data_plane/data_plane_executor.h"
#include "datasystem/client/transport/metadata/object_metadata_client.h"
#include "datasystem/client/transport/object_read/object_read_flow.h"
#include "datasystem/client/transport/object_read/replica_reader.h"
#include "datasystem/client/transport/rpc/worker_rpc_client.h"
#include "datasystem/client/transport/transport_layer.h"
#include "datasystem/common/ak_sk/signature.h"
#include "datasystem/common/rpc/api_deadline.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/thread_pool.h"
#include "datasystem/utils/connection.h"

namespace datasystem {
namespace client {
namespace {

HostPort MakeAddress(int port)
{
    return HostPort("127.0.0.1", port);
}

std::shared_ptr<Signature> MakeSignature()
{
    return std::make_shared<Signature>();
}

void AddLocation(master::QueryAndGetRspPb &response, const std::string &key, const HostPort &address,
                 uint64_t size = 4)
{
    auto *location = response.add_location_infos();
    location->set_object_key(key);
    location->add_object_locations(address.ToString());
    location->set_object_size(size);
}

std::vector<ObjectMetadataItem> MakeMetadataItems(const std::vector<ObjectReadItem> &inputs)
{
    std::vector<ObjectMetadataItem> items;
    items.reserve(inputs.size());
    for (const auto &input : inputs) {
        items.push_back({ input.objectKey });
    }
    return items;
}

ObjectMetadataBatch MakeMetadataBatch(std::vector<ObjectMetadataItem> &items)
{
    ObjectMetadataBatch batch;
    batch.reserve(items.size());
    for (auto &item : items) {
        batch.emplace_back(&item);
    }
    return batch;
}

class FakeWorkerRpcClient : public WorkerRpcClient {
public:
    explicit FakeWorkerRpcClient(HostPort address = MakeAddress(9000))
        : WorkerRpcClient(std::move(address), MakeSignature())
    {
    }

    Status Init() override
    {
        alive = initStatus.IsOk();
        return initStatus;
    }

    Status InvokeGetObject(GetObjectRemoteReqPb &request, GetObjectRemoteRspPb &response,
                           std::vector<RpcMessage> &rpcPayloads) override
    {
        ++getObjectCount;
        getObjectRequests.push_back(request);
        if (onInvoke) {
            onInvoke();
        }
        response.mutable_error()->set_error_code(getObjectResponseCode);
        response.set_data_size(getObjectDataSize);
        response.set_data_source(request.has_urma_info() ? DataTransferSource::DATA_ALREADY_TRANSFERRED
                                                        : DataTransferSource::DATA_IN_PAYLOAD);
        if (getObjectStatus.IsError()) {
            return getObjectStatus;
        }
        if (request.has_urma_info() && request.data_size() != static_cast<uint64_t>(getObjectDataSize)) {
            return Status(K_OC_REMOTE_GET_NOT_ENOUGH, "receive buffer size mismatch");
        }
        if (!request.has_urma_info() && getObjectDataSize > 0) {
            RpcMessage payload;
            std::string data(static_cast<size_t>(getObjectDataSize), 'd');
            RETURN_IF_NOT_OK(payload.CopyBuffer(data.data(), data.size()));
            rpcPayloads.emplace_back(std::move(payload));
        }
        if (afterInvoke) {
            afterInvoke();
        }
        return Status::OK();
    }

    Status InvokeQueryAndGet(master::QueryAndGetReqPb &request, master::QueryAndGetRspPb &response) override
    {
        ++queryAndGetCount;
        queryAndGetRequests.push_back(request);
        if (queryAndGetHandler) {
            return queryAndGetHandler(WorkerAddress(), request, response);
        }
        return queryAndGetStatus;
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
    int getObjectCount = 0;
    int queryAndGetCount = 0;
    Status getObjectStatus = Status::OK();
    StatusCode getObjectResponseCode = K_OK;
    int64_t getObjectDataSize = 4;
    std::vector<GetObjectRemoteReqPb> getObjectRequests;
    Status queryAndGetStatus = Status::OK();
    std::vector<master::QueryAndGetReqPb> queryAndGetRequests;
    std::function<Status(const HostPort &, const master::QueryAndGetReqPb &, master::QueryAndGetRspPb &)>
        queryAndGetHandler;
    std::function<void()> onInvoke;
    std::function<void()> afterInvoke;
};

class AuthBoundaryWorkerRpcClient : public WorkerRpcClient {
public:
    explicit AuthBoundaryWorkerRpcClient(std::shared_ptr<Signature> signature)
        : WorkerRpcClient(MakeAddress(9001), std::move(signature))
    {
    }

    bool IsAlive() const override
    {
        return true;
    }

    int getObjectInvokeCount = 0;
    int metadataInvokeCount = 0;
    int dataRpcTimeout = 0;
    int metadataRpcTimeout = 0;
    GetObjectRemoteReqPb invokedDataRequest;
    master::QueryAndGetReqPb invokedMetadataRequest;

protected:
    Status DoInvokeGetObject(const RpcOptions &options, const GetObjectRemoteReqPb &request, GetObjectRemoteRspPb &,
                             std::vector<RpcMessage> &) override
    {
        ++getObjectInvokeCount;
        dataRpcTimeout = options.GetTimeout();
        invokedDataRequest = request;
        return Status::OK();
    }

    Status DoInvokeQueryAndGet(const RpcOptions &options, const master::QueryAndGetReqPb &request,
                               master::QueryAndGetRspPb &) override
    {
        ++metadataInvokeCount;
        metadataRpcTimeout = options.GetTimeout();
        invokedMetadataRequest = request;
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

    Status Get(const DataGetRequest &, DataGetResult &) override
    {
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
    FakeDataPlaneManager() : DataPlaneManager(MakeSignature(), ConnectOptions{}.fastTransportMemSize)
    {
    }

    Status CreateWorkerRpcClient(const HostPort &address, std::shared_ptr<WorkerRpcClient> &output) override
    {
        ++rpcBuildCount;
        auto client = std::make_shared<FakeWorkerRpcClient>(address);
        client->queryAndGetHandler = queryAndGetHandler;
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
    std::function<Status(const HostPort &, const master::QueryAndGetReqPb &, master::QueryAndGetRspPb &)>
        queryAndGetHandler;
};

class FakeObjectMetadataClient : public ObjectMetadataClient {
public:
    FakeObjectMetadataClient() : ObjectMetadataClient(nullptr, nullptr)
    {
    }

    Status QueryAndGet(const HostPort &address, const ObjectMetadataBatch &items) override
    {
        {
            std::lock_guard<std::mutex> lock(mutex);
            addresses.push_back(address);
            threadIds.push_back(std::this_thread::get_id());
            keyGroups.emplace_back();
            for (const auto *item : items) {
                keyGroups.back().push_back(item->objectKey);
            }
        }
        auto groupStatus = groupStatuses.find(address.ToString());
        if (groupStatus != groupStatuses.end()) {
            return groupStatus->second;
        }
        for (auto *item : items) {
            auto status = itemStatuses.find(item->objectKey);
            item->status = status == itemStatuses.end() ? Status::OK() : status->second;
            item->location.set_object_key(item->objectKey);
            item->location.set_object_size(4);
            item->location.add_object_locations(MakeAddress(90).ToString());
        }
        return Status::OK();
    }

    std::mutex mutex;
    std::vector<HostPort> addresses;
    std::vector<std::vector<std::string>> keyGroups;
    std::vector<std::thread::id> threadIds;
    std::unordered_map<std::string, Status> groupStatuses;
    std::unordered_map<std::string, Status> itemStatuses;
};

class FakeReplicaReader : public ReplicaReader {
public:
    FakeReplicaReader() : ReplicaReader(nullptr, nullptr)
    {
    }

    Status Read(const master::ObjectLocationInfoPb &location, ObjectReadItemResult &result) override
    {
        {
            std::lock_guard<std::mutex> lock(mutex);
            keys.push_back(location.object_key());
            threadIds.push_back(std::this_thread::get_id());
        }
        auto status = itemStatuses.find(location.object_key());
        if (status != itemStatuses.end() && status->second.IsError()) {
            return status->second;
        }
        result.objectKey = location.object_key();
        result.data.kind = location.object_key() == "tcp" ? AccessTransportKind::TCP : AccessTransportKind::UB;
        return Status::OK();
    }

    std::mutex mutex;
    std::vector<std::string> keys;
    std::vector<std::thread::id> threadIds;
    std::unordered_map<std::string, Status> itemStatuses;
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

TEST(WorkerRpcClientTest, AuthenticatesFinalTransportRequestsBeforeRpc)
{
    auto signature = std::make_shared<Signature>("access-1", SensitiveValue("secret-1"));
    AuthBoundaryWorkerRpcClient client(signature);
    GetObjectRemoteReqPb dataRequest;
    dataRequest.set_object_key("key");
    dataRequest.mutable_urma_info()->set_seg_va(123);
    GetObjectRemoteRspPb dataResponse;
    std::vector<RpcMessage> payloads;

    ASSERT_TRUE(client.InvokeGetObject(dataRequest, dataResponse, payloads).IsOk());
    EXPECT_EQ(client.getObjectInvokeCount, 1);
    EXPECT_EQ(client.invokedDataRequest.access_key(), "access-1");
    EXPECT_FALSE(client.invokedDataRequest.signature().empty());
    EXPECT_EQ(client.invokedDataRequest.urma_info().seg_va(), 123u);

    master::QueryAndGetReqPb metadataRequest;
    metadataRequest.add_object_keys("key");
    metadataRequest.set_redirect(true);
    master::QueryAndGetRspPb metadataResponse;
    ASSERT_TRUE(client.InvokeQueryAndGet(metadataRequest, metadataResponse).IsOk());
    EXPECT_EQ(client.metadataInvokeCount, 1);
    EXPECT_EQ(client.invokedMetadataRequest.access_key(), "access-1");
    EXPECT_FALSE(client.invokedMetadataRequest.signature().empty());
    EXPECT_TRUE(client.invokedMetadataRequest.redirect());
}

TEST(WorkerRpcClientTest, BoundsRpcTimeoutByApiDeadline)
{
    ApiDeadlineGuard deadline(100);
    auto signature = std::make_shared<Signature>();
    AuthBoundaryWorkerRpcClient client(signature);
    GetObjectRemoteReqPb dataRequest;
    GetObjectRemoteRspPb dataResponse;
    std::vector<RpcMessage> payloads;

    ASSERT_TRUE(client.InvokeGetObject(dataRequest, dataResponse, payloads).IsOk());
    EXPECT_GT(client.dataRpcTimeout, 0);
    EXPECT_LE(client.dataRpcTimeout, 100);

    master::QueryAndGetReqPb metadataRequest;
    metadataRequest.add_object_keys("key");
    master::QueryAndGetRspPb metadataResponse;
    ASSERT_TRUE(client.InvokeQueryAndGet(metadataRequest, metadataResponse).IsOk());
    EXPECT_EQ(client.metadataInvokeCount, 1);
    EXPECT_GT(client.metadataRpcTimeout, 0);
    EXPECT_LE(client.metadataRpcTimeout, 100);
    EXPECT_EQ(client.invokedMetadataRequest.access_key(), "");
}

TEST(WorkerRpcClientTest, ExpiredApiDeadlineDoesNotSendRpc)
{
    ApiDeadlineGuard deadline(-1, InUs{});
    auto signature = std::make_shared<Signature>();
    AuthBoundaryWorkerRpcClient client(signature);
    GetObjectRemoteReqPb dataRequest;
    GetObjectRemoteRspPb dataResponse;
    std::vector<RpcMessage> payloads;

    EXPECT_EQ(client.InvokeGetObject(dataRequest, dataResponse, payloads).GetCode(), K_RPC_DEADLINE_EXCEEDED);
    EXPECT_EQ(client.getObjectInvokeCount, 0);

    master::QueryAndGetReqPb metadataRequest;
    master::QueryAndGetRspPb metadataResponse;
    EXPECT_EQ(client.InvokeQueryAndGet(metadataRequest, metadataResponse).GetCode(), K_RPC_DEADLINE_EXCEEDED);
    EXPECT_EQ(client.metadataInvokeCount, 0);
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

TEST(ObjectMetadataClientTest, RestoresOrderAcrossPartialRedirectsAndDuplicateKeys)
{
    ApiDeadlineGuard deadline(1000);
    auto manager = std::make_shared<FakeDataPlaneManager>();
    std::vector<HostPort> calls;
    manager->queryAndGetHandler = [&calls](const HostPort &address, const master::QueryAndGetReqPb &request,
                                           master::QueryAndGetRspPb &response) {
        calls.push_back(address);
        if (address == MakeAddress(41)) {
            EXPECT_EQ(request.object_keys_size(), 4);
            AddLocation(response, "a", MakeAddress(51));
            AddLocation(response, "a", MakeAddress(52));
            auto *redirectC = response.add_info();
            redirectC->set_redirect_meta_address(MakeAddress(43).ToString());
            redirectC->add_change_meta_ids("c");
            auto *redirectB = response.add_info();
            redirectB->set_redirect_meta_address(MakeAddress(42).ToString());
            redirectB->add_change_meta_ids("b");
        } else if (address == MakeAddress(42)) {
            EXPECT_EQ(request.object_keys_size(), 1);
            EXPECT_EQ(request.object_keys(0), "b");
            EXPECT_FALSE(request.redirect());
            AddLocation(response, "b", MakeAddress(53));
        } else {
            EXPECT_EQ(address, MakeAddress(43));
            EXPECT_EQ(request.object_keys_size(), 1);
            EXPECT_EQ(request.object_keys(0), "c");
            EXPECT_FALSE(request.redirect());
            AddLocation(response, "c", MakeAddress(54));
        }
        return Status::OK();
    };
    ObjectMetadataClient metadata(manager, std::make_shared<DeadlineRetry>());
    std::vector<ObjectReadItem> inputs{ { 0, "a", MakeAddress(41) }, { 1, "b", MakeAddress(41) },
                                       { 2, "a", MakeAddress(41) }, { 3, "c", MakeAddress(41) } };
    auto results = MakeMetadataItems(inputs);
    auto batch = MakeMetadataBatch(results);

    ASSERT_TRUE(metadata.QueryAndGet(MakeAddress(41), batch).IsOk());
    ASSERT_EQ(results.size(), 4u);
    for (size_t i = 0; i < results.size(); ++i) {
        EXPECT_TRUE(results[i].status.IsOk());
        EXPECT_EQ(results[i].objectKey, inputs[i].objectKey);
    }
    ASSERT_EQ(calls.size(), 3u);
    EXPECT_EQ(calls[0], MakeAddress(41));
    EXPECT_EQ(calls[1], MakeAddress(42));
    EXPECT_EQ(calls[2], MakeAddress(43));
}

TEST(ObjectMetadataClientTest, RejectsPositionalResponseMismatch)
{
    ApiDeadlineGuard deadline(1000);
    auto manager = std::make_shared<FakeDataPlaneManager>();
    manager->queryAndGetHandler = [](const HostPort &, const master::QueryAndGetReqPb &,
                                     master::QueryAndGetRspPb &response) {
        AddLocation(response, "unexpected", MakeAddress(51));
        return Status::OK();
    };
    ObjectMetadataClient metadata(manager, std::make_shared<DeadlineRetry>());
    auto results = MakeMetadataItems({ { 0, "key", MakeAddress(41) } });
    auto batch = MakeMetadataBatch(results);

    EXPECT_EQ(metadata.QueryAndGet(MakeAddress(41), batch).GetCode(), K_RUNTIME_ERROR);
}

TEST(ObjectMetadataClientTest, EmptyLocationsFailOnlyTheirInputItem)
{
    ApiDeadlineGuard deadline(1000);
    auto manager = std::make_shared<FakeDataPlaneManager>();
    manager->queryAndGetHandler = [](const HostPort &, const master::QueryAndGetReqPb &,
                                     master::QueryAndGetRspPb &response) {
        response.add_location_infos()->set_object_key("missing");
        AddLocation(response, "present", MakeAddress(51));
        return Status::OK();
    };
    ObjectMetadataClient metadata(manager, std::make_shared<DeadlineRetry>());
    auto results = MakeMetadataItems({ { 0, "missing", MakeAddress(41) },
                                       { 1, "present", MakeAddress(41) } });
    auto batch = MakeMetadataBatch(results);

    ASSERT_TRUE(metadata.QueryAndGet(MakeAddress(41), batch).IsOk());
    ASSERT_EQ(results.size(), 2u);
    EXPECT_EQ(results[0].status.GetCode(), K_NOT_FOUND);
    EXPECT_TRUE(results[1].status.IsOk());
}

TEST(ObjectMetadataClientTest, RetriesMetaMovingWithTheSameKeyGroup)
{
    ApiDeadlineGuard deadline(1000);
    auto manager = std::make_shared<FakeDataPlaneManager>();
    int invokeCount = 0;
    manager->queryAndGetHandler = [&invokeCount](const HostPort &, const master::QueryAndGetReqPb &request,
                                                 master::QueryAndGetRspPb &response) {
        EXPECT_EQ(request.object_keys_size(), 2);
        if (++invokeCount == 1) {
            response.set_meta_is_moving(true);
        } else {
            AddLocation(response, "a", MakeAddress(51));
            AddLocation(response, "b", MakeAddress(52));
        }
        return Status::OK();
    };
    ObjectMetadataClient metadata(manager, std::make_shared<DeadlineRetry>());
    auto results = MakeMetadataItems({ { 0, "a", MakeAddress(41) }, { 1, "b", MakeAddress(41) } });
    auto batch = MakeMetadataBatch(results);

    ASSERT_TRUE(metadata.QueryAndGet(MakeAddress(41), batch).IsOk());
    EXPECT_EQ(invokeCount, 2);
    ASSERT_EQ(results.size(), 2u);
    EXPECT_TRUE(results[0].status.IsOk());
    EXPECT_TRUE(results[1].status.IsOk());
}

TEST(ObjectMetadataClientTest, RejectsRedirectReturnedByRedirectTarget)
{
    ApiDeadlineGuard deadline(1000);
    auto manager = std::make_shared<FakeDataPlaneManager>();
    manager->queryAndGetHandler = [](const HostPort &address, const master::QueryAndGetReqPb &,
                                     master::QueryAndGetRspPb &response) {
        auto *redirect = response.add_info();
        redirect->set_redirect_meta_address(
            (address == MakeAddress(41) ? MakeAddress(42) : MakeAddress(43)).ToString());
        redirect->add_change_meta_ids("key");
        return Status::OK();
    };
    ObjectMetadataClient metadata(manager, std::make_shared<DeadlineRetry>());
    auto results = MakeMetadataItems({ { 0, "key", MakeAddress(41) } });
    auto batch = MakeMetadataBatch(results);

    ASSERT_TRUE(metadata.QueryAndGet(MakeAddress(41), batch).IsOk());
    ASSERT_EQ(results.size(), 1u);
    EXPECT_EQ(results[0].status.GetCode(), K_RUNTIME_ERROR);
}

TEST(ObjectMetadataClientTest, RebuildsUnavailableSharedRpcConnection)
{
    ApiDeadlineGuard deadline(1000);
    auto manager = std::make_shared<FakeDataPlaneManager>();
    int invokeCount = 0;
    manager->queryAndGetHandler = [&invokeCount](const HostPort &, const master::QueryAndGetReqPb &,
                                                 master::QueryAndGetRspPb &response) {
        if (++invokeCount == 1) {
            return Status(K_RPC_UNAVAILABLE, "unavailable");
        }
        AddLocation(response, "key", MakeAddress(51));
        return Status::OK();
    };
    ObjectMetadataClient metadata(manager, std::make_shared<DeadlineRetry>());
    auto results = MakeMetadataItems({ { 0, "key", MakeAddress(41) } });
    auto batch = MakeMetadataBatch(results);

    ASSERT_TRUE(metadata.QueryAndGet(MakeAddress(41), batch).IsOk());
    EXPECT_EQ(manager->rpcBuildCount, 2);
    ASSERT_EQ(results.size(), 1u);
    EXPECT_TRUE(results[0].status.IsOk());
}

TEST(ObjectMetadataClientTest, MetadataAndDataReuseOneEndpointRpcClient)
{
    ApiDeadlineGuard deadline(1000);
    auto manager = std::make_shared<FakeDataPlaneManager>();
    manager->queryAndGetHandler = [](const HostPort &address, const master::QueryAndGetReqPb &,
                                     master::QueryAndGetRspPb &response) {
        AddLocation(response, "key", address);
        return Status::OK();
    };
    ObjectMetadataClient metadata(manager, std::make_shared<DeadlineRetry>());
    auto results = MakeMetadataItems({ { 0, "key", MakeAddress(41) } });
    auto batch = MakeMetadataBatch(results);
    ASSERT_TRUE(metadata.QueryAndGet(MakeAddress(41), batch).IsOk());
    std::shared_ptr<IDataTransporter> transporter;

    ASSERT_TRUE(manager->GetOrCreate(MakeAddress(41), TransportHint::TCP_ONLY, transporter).IsOk());
    EXPECT_EQ(manager->rpcBuildCount, 1);
    EXPECT_EQ(manager->transportBuildCount, 1);
}

TEST(ObjectReadFlowTest, BatchesOneOwnerOnCallerAndReadsKeysInParallel)
{
    ApiDeadlineGuard deadline(1000);
    auto metadata = std::make_shared<FakeObjectMetadataClient>();
    metadata->itemStatuses.emplace("missing", Status(K_NOT_FOUND, "missing"));
    auto replicas = std::make_shared<FakeReplicaReader>();
    auto taskPool = std::make_shared<ThreadPool>(0, 4, "object_read_test");
    ObjectReadFlow flow(metadata, replicas, taskPool);
    ObjectReadRequest request;
    request.items = { { 0, "ub", MakeAddress(41) }, { 1, "missing", MakeAddress(41) },
                      { 2, "tcp", MakeAddress(41) } };
    ObjectReadResult result;
    const auto callerThread = std::this_thread::get_id();

    ASSERT_TRUE(flow.Run(request, result).IsOk());
    ASSERT_EQ(metadata->keyGroups.size(), 1u);
    EXPECT_EQ(metadata->keyGroups[0], std::vector<std::string>({ "ub", "missing", "tcp" }));
    EXPECT_EQ(metadata->threadIds[0], callerThread);
    ASSERT_EQ(replicas->threadIds.size(), 2u);
    EXPECT_NE(replicas->threadIds[0], callerThread);
    EXPECT_NE(replicas->threadIds[1], callerThread);
    ASSERT_EQ(result.items.size(), 3u);
    EXPECT_TRUE(result.items[0].status.IsOk());
    EXPECT_EQ(result.items[1].status.GetCode(), K_NOT_FOUND);
    EXPECT_TRUE(result.items[2].status.IsOk());
    EXPECT_EQ(result.actualKind, AccessTransportKind::TCP);
}

TEST(ObjectReadFlowTest, QueriesMultipleOwnersInParallelAndPreservesPartialSuccess)
{
    ApiDeadlineGuard deadline(1000);
    auto metadata = std::make_shared<FakeObjectMetadataClient>();
    metadata->groupStatuses.emplace(MakeAddress(42).ToString(), Status(K_INVALID, "invalid group"));
    auto replicas = std::make_shared<FakeReplicaReader>();
    auto taskPool = std::make_shared<ThreadPool>(0, 4, "object_read_test");
    ObjectReadFlow flow(metadata, replicas, taskPool);
    ObjectReadRequest request;
    request.items = { { 7, "good", MakeAddress(41) }, { 3, "bad", MakeAddress(42) } };
    ObjectReadResult result;
    const auto callerThread = std::this_thread::get_id();

    ASSERT_TRUE(flow.Run(request, result).IsOk());
    ASSERT_EQ(metadata->threadIds.size(), 2u);
    EXPECT_NE(metadata->threadIds[0], callerThread);
    EXPECT_NE(metadata->threadIds[1], callerThread);
    ASSERT_EQ(replicas->threadIds.size(), 1u);
    EXPECT_EQ(replicas->threadIds[0], callerThread);
    ASSERT_EQ(result.items.size(), 2u);
    EXPECT_EQ(result.items[0].requestIndex, 7u);
    EXPECT_TRUE(result.items[0].status.IsOk());
    EXPECT_EQ(result.items[1].requestIndex, 3u);
    EXPECT_EQ(result.items[1].status.GetCode(), K_INVALID);
}

TEST(ObjectReadFlowTest, ReturnsFirstInputErrorWhenAllKeysFail)
{
    ApiDeadlineGuard deadline(1000);
    auto metadata = std::make_shared<FakeObjectMetadataClient>();
    metadata->itemStatuses.emplace("first", Status(K_NOT_FOUND, "first"));
    metadata->itemStatuses.emplace("second", Status(K_INVALID, "second"));
    auto replicas = std::make_shared<FakeReplicaReader>();
    ObjectReadFlow flow(metadata, replicas, std::make_shared<ThreadPool>(0, 2, "object_read_test"));
    ObjectReadRequest request;
    request.items = { { 1, "first", MakeAddress(41) }, { 0, "second", MakeAddress(41) } };
    ObjectReadResult result;

    EXPECT_EQ(flow.Run(request, result).GetCode(), K_NOT_FOUND);
}

TEST(TcpTransporterTest, GetUsesGetObjectRemoteAndPreservesPayload)
{
    auto rpcClient = std::make_shared<FakeWorkerRpcClient>();
    rpcClient->getObjectDataSize = 6;
    TcpTransporter transporter(rpcClient);
    DataGetResult result;

    ASSERT_TRUE(transporter.Get({ "key", 6 }, result).IsOk());
    ASSERT_EQ(rpcClient->getObjectRequests.size(), 1u);
    EXPECT_EQ(rpcClient->getObjectRequests[0].object_key(), "key");
    EXPECT_EQ(rpcClient->getObjectRequests[0].data_size(), 6u);
    EXPECT_TRUE(rpcClient->getObjectRequests[0].try_lock());
    ASSERT_EQ(result.rpcPayloads.size(), 1u);
    EXPECT_EQ(result.rpcPayloads[0].Size(), 6u);
    EXPECT_EQ(result.kind, AccessTransportKind::TCP);
}

TEST(TcpTransporterTest, GetPropagatesRpcAndBusinessErrors)
{
    auto rpcClient = std::make_shared<FakeWorkerRpcClient>();
    TcpTransporter transporter(rpcClient);
    DataGetResult result;

    rpcClient->getObjectStatus = Status(K_RPC_DEADLINE_EXCEEDED, "deadline");
    EXPECT_EQ(transporter.Get({ "key", 1 }, result).GetCode(), K_RPC_DEADLINE_EXCEEDED);

    rpcClient->getObjectStatus = Status::OK();
    rpcClient->getObjectResponseCode = K_NOT_FOUND;
    EXPECT_EQ(transporter.Get({ "key", 1 }, result).GetCode(), K_NOT_FOUND);
}

TEST(UbTransporterTest, GetReturnsOwnerBackedExternalBuffer)
{
    auto rpcClient = std::make_shared<FakeWorkerRpcClient>();
    rpcClient->getObjectDataSize = 8;
    auto bufferProvider = std::make_shared<FakeUbBufferProvider>();
    UbTransporter transporter(rpcClient, std::make_shared<FakeUbConnection>(), bufferProvider);
    DataGetResult result;

    ASSERT_TRUE(transporter.Get({ "key", 8 }, result).IsOk());
    ASSERT_EQ(rpcClient->getObjectRequests.size(), 1u);
    EXPECT_TRUE(rpcClient->getObjectRequests[0].has_urma_info());
    EXPECT_TRUE(rpcClient->getObjectRequests[0].try_lock());
    EXPECT_EQ(result.kind, AccessTransportKind::UB);
    EXPECT_EQ(result.externalSize, 8u);
    EXPECT_NE(result.externalData, nullptr);
    EXPECT_NE(result.externalOwner, nullptr);
}

TEST(UbTransporterTest, GetReallocatesOnceForChangedObjectSize)
{
    auto rpcClient = std::make_shared<FakeWorkerRpcClient>();
    rpcClient->getObjectDataSize = 8;
    auto bufferProvider = std::make_shared<FakeUbBufferProvider>();
    UbTransporter transporter(rpcClient, std::make_shared<FakeUbConnection>(), bufferProvider);
    DataGetResult result;

    ASSERT_TRUE(transporter.Get({ "key", 4 }, result).IsOk());
    ASSERT_EQ(rpcClient->getObjectRequests.size(), 2u);
    EXPECT_EQ(rpcClient->getObjectRequests[0].data_size(), 4u);
    EXPECT_EQ(rpcClient->getObjectRequests[1].data_size(), 8u);
    EXPECT_EQ(result.externalSize, 8u);
    EXPECT_EQ(bufferProvider->allocateCount, 2);
}

TEST(UbTransporterTest, DeadConnectionRequestsUbReconnectBeforeRpc)
{
    auto rpcClient = std::make_shared<FakeWorkerRpcClient>();
    auto connection = std::make_shared<FakeUbConnection>();
    connection->alive = false;
    UbTransporter transporter(rpcClient, connection, std::make_shared<FakeUbBufferProvider>());
    DataGetResult result;

    EXPECT_EQ(transporter.Get({ "key", 4 }, result).GetCode(), K_URMA_NEED_CONNECT);
    EXPECT_EQ(rpcClient->getObjectCount, 0);
}

TEST(UbTransporterTest, CloseDataPlaneWaitsForInflightGet)
{
    auto rpcClient = std::make_shared<FakeWorkerRpcClient>();
    rpcClient->getObjectDataSize = 4;
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

    UbTransporter transporter(rpcClient, connection, std::make_shared<FakeUbBufferProvider>());
    DataGetResult result;
    Status getStatus;
    std::thread getThread([&]() { getStatus = transporter.Get({ "key", 4 }, result); });
    invokeStartedFuture.wait();
    std::thread closeThread([&]() { transporter.CloseDataPlane(); });
    allowInvoke.set_value();
    getThread.join();
    closeThread.join();

    EXPECT_TRUE(getStatus.IsOk());
    EXPECT_FALSE(connection->teardownDuringInvoke.load());
}

TEST(ShmTransporterTest, RemainsExplicitPlaceholder)
{
    ShmTransporter transporter;
    DataGetResult result;
    EXPECT_EQ(transporter.Get({ "key", 1 }, result).GetCode(), K_NOT_SUPPORTED);
}

TEST(DataPlaneExecutorTest, UrmaReconnectResetsOnlyDataPlaneAndRetriesOnce)
{
    auto manager = std::make_shared<FakeDataPlaneManager>();
    manager->transporterGetStatuses = { { Status(K_URMA_NEED_CONNECT, "reconnect") }, { Status::OK() } };
    DataPlaneExecutor executor(manager, std::make_shared<TransportAdvisor>());
    DataGetRequest request{ "a", 1 };
    DataGetResult result;

    EXPECT_TRUE(executor.Execute(MakeAddress(22), [&request, &result](IDataTransporter &transporter) {
        return transporter.Get(request, result);
    }).IsOk());
    EXPECT_EQ(manager->rpcBuildCount, 1);
    EXPECT_EQ(manager->transportBuildCount, 2);
    ASSERT_EQ(manager->builtTransporters.size(), 2u);
    EXPECT_EQ(manager->builtTransporters[0]->closeCount, 1);
}

TEST(DataPlaneExecutorTest, RpcUnavailableRebuildsCompleteEntryAndRetriesOnce)
{
    auto manager = std::make_shared<FakeDataPlaneManager>();
    manager->transporterGetStatuses = { { Status(K_RPC_UNAVAILABLE, "unavailable") }, { Status::OK() } };
    DataPlaneExecutor executor(manager, std::make_shared<TransportAdvisor>());
    DataGetRequest request{ "a", 1 };
    DataGetResult result;

    EXPECT_TRUE(executor.Execute(MakeAddress(23), [&request, &result](IDataTransporter &transporter) {
        return transporter.Get(request, result);
    }).IsOk());
    EXPECT_EQ(manager->rpcBuildCount, 2);
    EXPECT_EQ(manager->transportBuildCount, 2);
}

TEST(DataPlaneExecutorTest, DoesNotRetrySecondTransportFailure)
{
    auto manager = std::make_shared<FakeDataPlaneManager>();
    manager->transporterGetStatuses = {
        { Status(K_URMA_NEED_CONNECT, "first") }, { Status(K_URMA_NEED_CONNECT, "second") }
    };
    DataPlaneExecutor executor(manager, std::make_shared<TransportAdvisor>());
    DataGetRequest request{ "a", 1 };
    DataGetResult result;

    EXPECT_EQ(executor.Execute(MakeAddress(24), [&request, &result](IDataTransporter &transporter) {
        return transporter.Get(request, result);
    }).GetCode(), K_URMA_NEED_CONNECT);
    EXPECT_EQ(manager->rpcBuildCount, 1);
    EXPECT_EQ(manager->transportBuildCount, 2);
}

TEST(ReplicaReaderTest, TriesNextLocationWithoutRefreshingMetadata)
{
    ApiDeadlineGuard deadline(1000);
    auto manager = std::make_shared<FakeDataPlaneManager>();
    manager->transporterGetStatuses = { { Status(K_WORKER_PULL_OBJECT_NOT_FOUND, "missing") }, { Status::OK() } };
    auto executor = std::make_shared<DataPlaneExecutor>(manager, std::make_shared<TransportAdvisor>());
    ReplicaReader reader(executor, std::make_shared<DeadlineRetry>());
    master::ObjectLocationInfoPb location;
    location.set_object_key("key");
    location.set_object_size(4);
    location.add_object_locations(MakeAddress(31).ToString());
    location.add_object_locations(MakeAddress(32).ToString());
    ObjectReadItemResult result;

    result.requestIndex = 7;
    ASSERT_TRUE(reader.Read(location, result).IsOk());
    EXPECT_EQ(result.requestIndex, 7u);
    EXPECT_EQ(result.objectKey, "key");
    EXPECT_EQ(manager->transportBuildCount, 2);
}

TEST(ReplicaReaderTest, StopsOnNonRetryableLocationError)
{
    ApiDeadlineGuard deadline(1000);
    auto manager = std::make_shared<FakeDataPlaneManager>();
    manager->transporterGetStatuses = { { Status(K_INVALID, "invalid") } };
    auto executor = std::make_shared<DataPlaneExecutor>(manager, std::make_shared<TransportAdvisor>());
    ReplicaReader reader(executor, std::make_shared<DeadlineRetry>());
    master::ObjectLocationInfoPb location;
    location.set_object_key("key");
    location.set_object_size(4);
    location.add_object_locations(MakeAddress(33).ToString());
    location.add_object_locations(MakeAddress(34).ToString());
    ObjectReadItemResult result;

    EXPECT_EQ(reader.Read(location, result).GetCode(), K_INVALID);
    EXPECT_EQ(manager->transportBuildCount, 1);
}

TEST(ReplicaReaderTest, StartsAnotherRoundWithoutRefreshingMetadata)
{
    ApiDeadlineGuard deadline(1000);
    auto manager = std::make_shared<FakeDataPlaneManager>();
    manager->transporterGetStatuses = { { Status(K_NOT_FOUND, "first") },
                                        { Status(K_RPC_CANCELLED, "second") } };
    auto executor = std::make_shared<DataPlaneExecutor>(manager, std::make_shared<TransportAdvisor>());
    ReplicaReader reader(executor, std::make_shared<DeadlineRetry>());
    master::ObjectLocationInfoPb location;
    location.set_object_key("key");
    location.set_object_size(4);
    location.add_object_locations(MakeAddress(35).ToString());
    location.add_object_locations(MakeAddress(36).ToString());
    ObjectReadItemResult result;

    ASSERT_TRUE(reader.Read(location, result).IsOk());
    EXPECT_EQ(result.objectKey, "key");
    EXPECT_EQ(manager->transportBuildCount, 2);
}

}  // namespace
}  // namespace client
}  // namespace datasystem
