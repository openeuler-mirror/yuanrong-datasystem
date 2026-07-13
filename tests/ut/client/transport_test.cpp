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
#include <cstdlib>
#include <cstring>
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
#include "datasystem/client/transport/object_buffer_internal.h"
#include "datasystem/client/transport/rpc/set_request_builder.h"
#include "datasystem/client/transport/rpc/worker_rpc_client.h"
#include "datasystem/client/transport/transport_layer.h"
#include "datasystem/common/ak_sk/signature.h"
#include "datasystem/common/object_cache/object_base.h"
#include "datasystem/common/object_cache/urma_fallback_tcp_limiter.h"
#include "datasystem/common/rpc/api_deadline.h"
#include "datasystem/common/rpc/mem_view.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/thread_pool.h"
#include "datasystem/utils/connection.h"
#include "datasystem/object/object_buffer.h"

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

    Status InvokeCreate(int64_t, CreateReqPb &request, CreateRspPb &response, uint32_t &workerVersion) override
    {
        ++createInvokeCount;
        invokedCreateRequests.push_back(request);
        if (createInvokeStatus.IsError()) {
            return createInvokeStatus;
        }
        workerVersion = version;
        if (createResponseHasUrmaInfo) {
            auto *urmaInfo = response.mutable_urma_info();
            urmaInfo->set_seg_va(0x1000);
            urmaInfo->set_seg_data_offset(0);
        }
        response.set_metadata_size(createResponseMetadataSize);
        response.set_shm_id("test-shm-id");
        return Status::OK();
    }

    Status InvokeSet(int64_t, PublishReqPb &request, const std::vector<MemView> &payloads,
                     PublishRspPb &response, uint32_t &workerVersion) override
    {
        ++setInvokeCount;
        invokedSetRequests.push_back(request);
        invokedSetPayloadSizes.push_back(payloads.size());
        invokedSetPayloadData.emplace_back();
        for (const auto &payload : payloads) {
            invokedSetPayloadData.back().emplace_back(static_cast<const char *>(payload.Data()), payload.Size());
        }
        if (onSetInvoke) {
            onSetInvoke();
        }
        if (setInvokeStatus.IsError()) {
            return setInvokeStatus;
        }
        workerVersion = version;
        if (afterSetInvoke) {
            afterSetInvoke();
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
    uint32_t version = 1;
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

    // Create/Set fake state
    int createInvokeCount = 0;
    int setInvokeCount = 0;
    Status createInvokeStatus = Status::OK();
    Status setInvokeStatus = Status::OK();
    bool createResponseHasUrmaInfo = false;
    int64_t createResponseMetadataSize = 0;
    StatusCode createResponseCode = K_OK;
    StatusCode setResponseCode = K_OK;
    std::vector<CreateReqPb> invokedCreateRequests;
    std::vector<PublishReqPb> invokedSetRequests;
    std::vector<size_t> invokedSetPayloadSizes;
    std::vector<std::vector<std::string>> invokedSetPayloadData;
    std::function<void()> onSetInvoke;
    std::function<void()> afterSetInvoke;
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
    CreateReqPb invokedCreateRequest;
    PublishReqPb invokedSetRequest;
    int createInvokeCount = 0;
    int setInvokeCount = 0;
    Status createInvokeStatus = Status::OK();
    Status setInvokeStatus = Status::OK();

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

    Status DoInvokeCreate(const RpcOptions &, const CreateReqPb &request, CreateRspPb &) override
    {
        ++createInvokeCount;
        invokedCreateRequest = request;
        return createInvokeStatus;
    }

    Status DoInvokeSet(const RpcOptions &, const PublishReqPb &request, PublishRspPb &,
                       const std::vector<MemView> &) override
    {
        ++setInvokeCount;
        invokedSetRequest = request;
        return setInvokeStatus;
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

    Status Create(const HostPort &workerAddr, const std::string &key, uint64_t size,
                  const TransportCreateParam &, std::shared_ptr<ObjectBuffer> &buffer) override
    {
        ++createCount;
        createdKeys.push_back(key);
        createdSizes.push_back(size);
        if (!createStatuses.empty()) {
            Status rc = createStatuses.front();
            createStatuses.erase(createStatuses.begin());
            return rc;
        }
        // Create a minimal ObjectBuffer for testing
        auto info = std::make_shared<ObjectBufferInfo>();
        info->objectKey = key;
        info->dataSize = size;
        info->metadataSize = 0;
        info->workerAddr = workerAddr;
        info->pointer = static_cast<uint8_t *>(malloc(size + 1));
        memset(info->pointer, 0, size + 1);
        return ObjectBufferInternal::Create(info, buffer);
    }

    Status Set(ObjectBuffer &buffer, const TransportSetParam &param) override
    {
        ++setCount;
        setParams.push_back(param);
        if (!setStatuses.empty()) {
            Status rc = setStatuses.front();
            setStatuses.erase(setStatuses.begin());
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

    // Create/Set fake state
    int createCount = 0;
    int setCount = 0;
    std::vector<Status> createStatuses;
    std::vector<Status> setStatuses;
    std::vector<std::string> createdKeys;
    std::vector<uint64_t> createdSizes;
    std::vector<TransportSetParam> setParams;
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
        if (!transporterSetStatuses.empty()) {
            transporter->setStatuses = std::move(transporterSetStatuses.front());
            transporterSetStatuses.erase(transporterSetStatuses.begin());
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
    std::vector<std::vector<Status>> transporterSetStatuses;
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

class TestUbTransporter : public UbTransporter {
public:
    TestUbTransporter(std::shared_ptr<WorkerRpcClient> rpcClient, std::shared_ptr<UbConnection> connection)
        : UbTransporter(std::move(rpcClient), std::move(connection))
    {
    }

    Status writeStatus = Status::OK();
    int writeCount = 0;

protected:
    Status WritePayload(ObjectBufferInfo &) override
    {
        ++writeCount;
        return writeStatus;
    }
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

TEST(WorkerRpcClientTest, SignsFinalReadRequestsBeforeRpc)
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

TEST(WorkerRpcClientTest, SignsCreateAndSetBeforeRpc)
{
    auto signature = std::make_shared<Signature>("access-1", SensitiveValue("secret-1"));
    AuthBoundaryWorkerRpcClient client(signature);
    uint32_t workerVersion = 0;

    CreateReqPb createRequest;
    createRequest.set_object_key("create-key");
    CreateRspPb createResponse;
    ASSERT_TRUE(client.InvokeCreate(100, createRequest, createResponse, workerVersion).IsOk());
    EXPECT_EQ(client.invokedCreateRequest.access_key(), "access-1");
    EXPECT_FALSE(client.invokedCreateRequest.signature().empty());
    EXPECT_EQ(client.invokedCreateRequest.object_key(), "create-key");

    PublishReqPb publishRequest;
    publishRequest.set_object_key("publish-key");
    PublishRspPb publishResponse;
    std::vector<MemView> payloads;
    ASSERT_TRUE(client.InvokeSet(100, publishRequest, payloads, publishResponse, workerVersion).IsOk());
    EXPECT_EQ(client.invokedSetRequest.access_key(), "access-1");
    EXPECT_FALSE(client.invokedSetRequest.signature().empty());
    EXPECT_EQ(client.invokedSetRequest.object_key(), "publish-key");
}

TEST(WorkerRpcClientTest, RetrySealAlreadySealedIsSuccess)
{
    AuthBoundaryWorkerRpcClient client(MakeSignature());
    client.setInvokeStatus = Status(K_OC_ALREADY_SEALED, "already sealed");
    PublishReqPb request;
    request.set_is_retry(true);
    request.set_is_seal(true);
    PublishRspPb response;
    std::vector<MemView> payloads;
    uint32_t workerVersion = 0;

    EXPECT_TRUE(client.InvokeSet(100, request, payloads, response, workerVersion).IsOk());
    EXPECT_EQ(client.setInvokeCount, 1);

    request.set_is_retry(false);
    EXPECT_EQ(client.InvokeSet(100, request, payloads, response, workerVersion).GetCode(), K_OC_ALREADY_SEALED);
    EXPECT_EQ(client.setInvokeCount, 2);
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

// --- ObjectBuffer tests ---

TEST(ObjectBufferTest, MemoryCopyWritesDataAndGetSizeReflectsCapacity)
{
    auto info = std::make_shared<ObjectBufferInfo>();
    info->objectKey = "test-key";
    info->dataSize = 64;
    info->metadataSize = 0;
    info->workerAddr = MakeAddress(9000);
    info->pointer = static_cast<uint8_t *>(malloc(64 + 1));
    memset(info->pointer, 0, 65);

    std::shared_ptr<ObjectBuffer> buffer;
    ASSERT_TRUE(ObjectBuffer::Create(info, buffer).IsOk());
    EXPECT_EQ(buffer->GetSize(), 64);

    const char data[] = "hello world";
    ASSERT_TRUE(buffer->MemoryCopy(data, sizeof(data)).IsOk());
    EXPECT_EQ(memcmp(buffer->ImmutableData(), data, sizeof(data)), 0);
}

TEST(ObjectBufferTest, MemoryCopyRejectsMissingBackingMemory)
{
    auto info = std::make_shared<ObjectBufferInfo>();
    info->objectKey = "remote-buffer";
    info->dataSize = 4;
    info->metadataSize = 0;
    info->pointer = nullptr;
    info->remoteHostInfo = std::make_shared<RemoteH2DHostInfoPb>();

    std::shared_ptr<ObjectBuffer> buffer;
    ASSERT_TRUE(ObjectBufferInternal::Create(info, buffer).IsOk());
    const char data[] = "abc";
    EXPECT_EQ(buffer->MemoryCopy(data, sizeof(data)).GetCode(), K_INVALID);
}

TEST(ObjectBufferTest, DestructorFreesMallocedMemory)
{
    auto info = std::make_shared<ObjectBufferInfo>();
    info->objectKey = "test-key";
    info->dataSize = 32;
    info->metadataSize = 0;
    info->workerAddr = MakeAddress(9000);
    info->pointer = static_cast<uint8_t *>(malloc(32 + 1));
    memset(info->pointer, 0, 33);

    std::shared_ptr<ObjectBuffer> buffer;
    ASSERT_TRUE(ObjectBufferInternal::Create(info, buffer).IsOk());
    // Buffer is destroyed when shared_ptr goes out of scope -- covered by ASan
    buffer.reset();
    SUCCEED();
}

TEST(ObjectBufferTest, RejectsAllocationSizeOverflow)
{
    auto expectOverflow = [](uint64_t dataSize, uint64_t metadataSize) {
        auto info = std::make_shared<ObjectBufferInfo>();
        info->objectKey = "overflow";
        info->dataSize = dataSize;
        info->metadataSize = metadataSize;
        info->pointer = nullptr;
        std::shared_ptr<ObjectBuffer> buffer;

        EXPECT_EQ(ObjectBufferInternal::Create(info, buffer).GetCode(), K_RUNTIME_ERROR);
        EXPECT_EQ(buffer, nullptr);
    };

    expectOverflow(UINT64_MAX, 0);
    expectOverflow(UINT64_MAX - 1, 1);
}

TEST(ObjectBufferTest, DestructorReleasesUbPoolHandle)
{
    auto storage = std::make_shared<std::vector<uint8_t>>(16);
    std::weak_ptr<std::vector<uint8_t>> weakStorage = storage;
    auto info = std::make_shared<ObjectBufferInfo>();
    info->objectKey = "ub-buffer";
    info->dataSize = storage->size();
    info->metadataSize = 0;
    info->pointer = storage->data();
    info->ubUrmaDataInfo = std::make_shared<UrmaRemoteAddrPb>();
    info->ubGetBufferHandle = std::static_pointer_cast<void>(storage);

    std::shared_ptr<ObjectBuffer> buffer;
    ASSERT_TRUE(ObjectBufferInternal::Create(info, buffer).IsOk());
    storage.reset();
    info.reset();
    EXPECT_FALSE(weakStorage.expired());
    buffer.reset();
    EXPECT_TRUE(weakStorage.expired());
}

TEST(ObjectBufferTest, MoveAssignmentTransfersOwnershipAndData)
{
    auto sourceInfo = std::make_shared<ObjectBufferInfo>();
    sourceInfo->objectKey = "source";
    sourceInfo->dataSize = 8;
    sourceInfo->metadataSize = 0;
    sourceInfo->pointer = static_cast<uint8_t *>(calloc(9, 1));
    auto destinationInfo = std::make_shared<ObjectBufferInfo>();
    destinationInfo->objectKey = "destination";
    destinationInfo->dataSize = 4;
    destinationInfo->metadataSize = 0;
    destinationInfo->pointer = static_cast<uint8_t *>(calloc(5, 1));

    std::shared_ptr<ObjectBuffer> source;
    std::shared_ptr<ObjectBuffer> destination;
    ASSERT_TRUE(ObjectBufferInternal::Create(sourceInfo, source).IsOk());
    ASSERT_TRUE(ObjectBufferInternal::Create(destinationInfo, destination).IsOk());
    const char payload[] = "payload";
    ASSERT_TRUE(source->MemoryCopy(payload, sizeof(payload)).IsOk());

    *destination = std::move(*source);
    EXPECT_EQ(destination->GetSize(), 8);
    EXPECT_EQ(memcmp(destination->ImmutableData(), payload, sizeof(payload)), 0);
    EXPECT_EQ(source->GetSize(), 0);
}

// --- TcpTransporter Create/Set tests ---

TEST(TcpTransporterTest, CreateAllocatesBuffer)
{
    auto rpcClient = std::make_shared<FakeWorkerRpcClient>();
    TcpTransporter transporter(rpcClient);
    std::shared_ptr<ObjectBuffer> buffer;

    TransportCreateParam param;
    param.cacheType = CacheType::MEMORY;
    param.consistencyType = ConsistencyType::PRAM;

    Status rc = transporter.Create(MakeAddress(9000), "my-key", 128, param, buffer);
    ASSERT_TRUE(rc.IsOk()) << rc.ToString();
    ASSERT_NE(buffer, nullptr);
    EXPECT_EQ(buffer->GetSize(), 128);
    EXPECT_NE(buffer->MutableData(), nullptr);
}

TEST(TcpTransporterTest, CreateRejectsInvalidArguments)
{
    TcpTransporter transporter(std::make_shared<FakeWorkerRpcClient>());
    std::shared_ptr<ObjectBuffer> buffer;
    TransportCreateParam param;
    EXPECT_EQ(transporter.Create(MakeAddress(9000), "", 1, param, buffer).GetCode(), K_INVALID);
    EXPECT_EQ(transporter.Create(MakeAddress(9000), "key", 0, param, buffer).GetCode(), K_INVALID);
    param.subTimeoutMs = -1;
    EXPECT_EQ(transporter.Create(MakeAddress(9000), "key", 1, param, buffer).GetCode(), K_INVALID);
    param.subTimeoutMs = 0;
    EXPECT_EQ(transporter.Create(MakeAddress(9000), "key", UINT64_MAX, param, buffer).GetCode(), K_INVALID);
}

TEST(TcpTransporterTest, SetCallsInvokeSet)
{
    auto rpcClient = std::make_shared<FakeWorkerRpcClient>();
    TcpTransporter transporter(rpcClient);

    // Create a buffer first
    std::shared_ptr<ObjectBuffer> buffer;
    TransportCreateParam createParam;
    ASSERT_TRUE(transporter.Create(MakeAddress(9000), "set-key", 64, createParam, buffer).IsOk());

    // Write data
    const char data[] = "test payload";
    ASSERT_TRUE(buffer->MemoryCopy(data, sizeof(data)).IsOk());

    // Set
    TransportSetParam setParam;
    setParam.subTimeoutMs = 500;
    setParam.ttlSecond = 60;
    Status rc = transporter.Set(*buffer, setParam);
    ASSERT_TRUE(rc.IsOk()) << rc.ToString();
    EXPECT_EQ(rpcClient->setInvokeCount, 1);
    EXPECT_EQ(rpcClient->invokedSetPayloadSizes.size(), 1u);
    EXPECT_EQ(rpcClient->invokedSetPayloadSizes[0], 1u);  // one payload
}

TEST(TcpTransporterTest, SetPropagatesRpcError)
{
    auto rpcClient = std::make_shared<FakeWorkerRpcClient>();
    rpcClient->setInvokeStatus = Status(K_RPC_DEADLINE_EXCEEDED, "timeout");
    TcpTransporter transporter(rpcClient);

    std::shared_ptr<ObjectBuffer> buffer;
    TransportCreateParam createParam;
    ASSERT_TRUE(transporter.Create(MakeAddress(9000), "err-key", 64, createParam, buffer).IsOk());

    TransportSetParam setParam;
    EXPECT_EQ(transporter.Set(*buffer, setParam).GetCode(), K_RPC_DEADLINE_EXCEEDED);
    EXPECT_EQ(rpcClient->setInvokeCount, 1);
}

TEST(UbTransporterTest, SetUrmaSuccessPublishesWithoutTcpPayload)
{
    auto rpcClient = std::make_shared<FakeWorkerRpcClient>();
    auto connection = std::make_shared<FakeUbConnection>();
    TestUbTransporter transporter(rpcClient, connection);
    auto info = std::make_shared<ObjectBufferInfo>();
    info->objectKey = "ub-success";
    info->dataSize = 4;
    info->metadataSize = 0;
    info->pointer = static_cast<uint8_t *>(calloc(5, 1));
    info->ubUrmaDataInfo = std::make_shared<UrmaRemoteAddrPb>();
    std::shared_ptr<ObjectBuffer> buffer;
    ASSERT_TRUE(ObjectBufferInternal::Create(info, buffer).IsOk());
    ASSERT_TRUE(buffer->MemoryCopy("data", 4).IsOk());

    ASSERT_TRUE(transporter.Set(*buffer, {}).IsOk());
    EXPECT_EQ(transporter.writeCount, 1);
    ASSERT_EQ(rpcClient->invokedSetPayloadSizes.size(), 1u);
    EXPECT_EQ(rpcClient->invokedSetPayloadSizes[0], 0u);
}

TEST(UbTransporterTest, SetUrmaFailureFallsBackToCorrectTcpPayload)
{
    auto rpcClient = std::make_shared<FakeWorkerRpcClient>();
    auto connection = std::make_shared<FakeUbConnection>();
    TestUbTransporter transporter(rpcClient, connection);
    transporter.writeStatus = Status(K_URMA_ERROR, "write failed");
    auto info = std::make_shared<ObjectBufferInfo>();
    info->objectKey = "ub-fallback";
    info->dataSize = 4;
    info->metadataSize = 3;
    info->pointer = static_cast<uint8_t *>(calloc(8, 1));
    memcpy(info->pointer, "hdr", 3);
    info->ubUrmaDataInfo = std::make_shared<UrmaRemoteAddrPb>();
    std::shared_ptr<ObjectBuffer> buffer;
    ASSERT_TRUE(ObjectBufferInternal::Create(info, buffer).IsOk());
    ASSERT_TRUE(buffer->MemoryCopy("data", 4).IsOk());

    ASSERT_TRUE(transporter.Set(*buffer, {}).IsOk());
    EXPECT_EQ(transporter.writeCount, 1);
    ASSERT_EQ(rpcClient->invokedSetPayloadData.size(), 1u);
    ASSERT_EQ(rpcClient->invokedSetPayloadData[0].size(), 1u);
    EXPECT_EQ(rpcClient->invokedSetPayloadData[0][0], "data");
}

TEST(UbTransporterTest, RejectedFallbackPreservesReconnectStatus)
{
    auto rpcClient = std::make_shared<FakeWorkerRpcClient>();
    TestUbTransporter transporter(rpcClient, std::make_shared<FakeUbConnection>());
    transporter.writeStatus = Status(K_URMA_NEED_CONNECT, "connection lost");
    auto info = std::make_shared<ObjectBufferInfo>();
    info->objectKey = "ub-reconnect";
    info->dataSize = UrmaFallbackTcpLimiter::kMaxSinglePayloadBytes;
    info->metadataSize = 0;
    info->pointer = static_cast<uint8_t *>(calloc(info->dataSize + 1, 1));
    info->ubUrmaDataInfo = std::make_shared<UrmaRemoteAddrPb>();
    std::shared_ptr<ObjectBuffer> buffer;
    ASSERT_TRUE(ObjectBufferInternal::Create(info, buffer).IsOk());

    EXPECT_EQ(transporter.Set(*buffer, {}).GetCode(), K_URMA_NEED_CONNECT);
    EXPECT_EQ(rpcClient->setInvokeCount, 0);
}

TEST(UbTransporterTest, CloseDataPlaneWaitsForInflightSet)
{
    auto rpcClient = std::make_shared<FakeWorkerRpcClient>();
    auto connection = std::make_shared<FakeUbConnection>();
    std::atomic<bool> invokeFinished{ false };
    connection->invokeFinished = &invokeFinished;
    std::promise<void> invokeStarted;
    auto invokeStartedFuture = invokeStarted.get_future();
    std::promise<void> allowInvoke;
    auto allowInvokeFuture = allowInvoke.get_future().share();
    rpcClient->onSetInvoke = [&invokeStarted, allowInvokeFuture]() {
        invokeStarted.set_value();
        allowInvokeFuture.wait();
    };
    rpcClient->afterSetInvoke = [&invokeFinished]() { invokeFinished.store(true); };

    TestUbTransporter transporter(rpcClient, connection);
    auto info = std::make_shared<ObjectBufferInfo>();
    info->objectKey = "ub-inflight";
    info->dataSize = 4;
    info->metadataSize = 0;
    info->pointer = static_cast<uint8_t *>(calloc(5, 1));
    info->ubDataSentByMemoryCopy = true;
    std::shared_ptr<ObjectBuffer> buffer;
    ASSERT_TRUE(ObjectBufferInternal::Create(info, buffer).IsOk());

    Status setStatus;
    std::thread setThread([&]() { setStatus = transporter.Set(*buffer, {}); });
    invokeStartedFuture.wait();
    std::thread closeThread([&]() { transporter.CloseDataPlane(); });
    allowInvoke.set_value();
    setThread.join();
    closeThread.join();
    EXPECT_TRUE(setStatus.IsOk());
    EXPECT_FALSE(connection->teardownDuringInvoke.load());
}

// --- TransportLayer Create/Set tests ---

TEST(TransportLayerTest, CreateDelegatesToTransporter)
{
    auto manager = std::make_shared<FakeDataPlaneManager>();
    TestTransportLayer layer(manager);

    TransportCreateParam param;
    std::shared_ptr<ObjectBuffer> buffer;
    Status rc = layer.Create(MakeAddress(30), "layer-create-key", 256, param, buffer);
    ASSERT_TRUE(rc.IsOk()) << rc.ToString();
    ASSERT_NE(buffer, nullptr);
    EXPECT_EQ(manager->transportBuildCount, 1);
}

TEST(TransportLayerTest, CreateRejectsInvalidRequestBeforeBuildingTransport)
{
    auto manager = std::make_shared<FakeDataPlaneManager>();
    TestTransportLayer layer(manager);
    TransportCreateParam param;
    std::shared_ptr<ObjectBuffer> buffer;

    EXPECT_EQ(layer.Create(MakeAddress(30), "", 256, param, buffer).GetCode(), K_INVALID);
    EXPECT_EQ(layer.Create(MakeAddress(30), "key", UINT64_MAX, param, buffer).GetCode(), K_INVALID);
    EXPECT_EQ(manager->transportBuildCount, 0);
}

TEST(TransportLayerTest, SetRetryOnUrmaNeedConnect)
{
    auto manager = std::make_shared<FakeDataPlaneManager>();
    manager->transporterSetStatuses = {
        { Status(K_URMA_NEED_CONNECT, "reconnect") }, { Status::OK() }
    };
    TestTransportLayer layer(manager);

    // Create first
    TransportCreateParam createParam;
    std::shared_ptr<ObjectBuffer> buffer;
    ASSERT_TRUE(layer.Create(MakeAddress(31), "retry-key", 64, createParam, buffer).IsOk());

    TransportSetParam setParam;
    Status rc = layer.Set(*buffer, setParam);
    EXPECT_TRUE(rc.IsOk()) << rc.ToString();
    EXPECT_GE(manager->rpcBuildCount, 1);
    EXPECT_GE(manager->transportBuildCount, 2);
    ASSERT_GE(manager->builtTransporters.size(), 2u);
    ASSERT_EQ(manager->builtTransporters[1]->setParams.size(), 1u);
    EXPECT_TRUE(manager->builtTransporters[1]->setParams[0].isRetry);
}

TEST(TransportLayerTest, SetRetryOnRpcUnavailable)
{
    auto manager = std::make_shared<FakeDataPlaneManager>();
    manager->transporterSetStatuses = {
        { Status(K_RPC_UNAVAILABLE, "unavailable") }, { Status::OK() }
    };
    TestTransportLayer layer(manager);

    TransportCreateParam createParam;
    std::shared_ptr<ObjectBuffer> buffer;
    ASSERT_TRUE(layer.Create(MakeAddress(32), "rpc-retry-key", 64, createParam, buffer).IsOk());

    TransportSetParam setParam;
    Status rc = layer.Set(*buffer, setParam);
    EXPECT_TRUE(rc.IsOk()) << rc.ToString();
    EXPECT_EQ(manager->rpcBuildCount, 2);       // RPC client rebuilt
    EXPECT_EQ(manager->transportBuildCount, 2);  // transporter rebuilt once
}

TEST(TransportLayerTest, SetDoesNotRetrySecondFailure)
{
    auto manager = std::make_shared<FakeDataPlaneManager>();
    // Inject two failing statuses: first triggers rebuild, second (retry) also fails.
    manager->transporterSetStatuses = {
        { Status(K_URMA_NEED_CONNECT, "first") }, { Status(K_URMA_NEED_CONNECT, "second") }
    };
    TestTransportLayer layer(manager);

    TransportCreateParam createParam;
    std::shared_ptr<ObjectBuffer> buffer;
    ASSERT_TRUE(layer.Create(MakeAddress(33), "no-retry-key", 64, createParam, buffer).IsOk());

    TransportSetParam setParam;
    Status rc = layer.Set(*buffer, setParam);
    EXPECT_EQ(rc.GetCode(), K_URMA_NEED_CONNECT) << rc.ToString();
    EXPECT_GE(manager->transportBuildCount, 2);
}
}  // namespace
}  // namespace client
}  // namespace datasystem
