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
#include <chrono>
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
#include "datasystem/client/transport/rpc/mset_request_builder.h"
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
#include "datasystem/protos/cluster_topology.pb.h"
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

TransportRequestContext MakeRequestContext()
{
    return { "client-1", "token-1", "tenant-1" };
}

TransportCreateParam MakeCreateParam()
{
    TransportCreateParam param;
    param.requestContext = MakeRequestContext();
    return param;
}

TransportSetParam MakeSetParam()
{
    TransportSetParam param;
    param.requestContext = MakeRequestContext();
    return param;
}

std::shared_ptr<ObjectBuffer> MakeTransportBuffer(const HostPort &workerAddr, const std::string &key,
                                                  const std::string &data, const std::string &shmId,
                                                  bool withUrmaInfo = false)
{
    auto info = std::make_shared<ObjectBufferInfo>();
    info->objectKey = key;
    info->dataSize = data.size();
    info->metadataSize = 0;
    info->workerAddr = workerAddr;
    info->shmId = ShmKey::Intern(shmId);
    info->pointer = static_cast<uint8_t *>(calloc(data.size() + 1, 1));
    if (withUrmaInfo) {
        info->ubUrmaDataInfo = std::make_shared<UrmaRemoteAddrPb>();
    }
    std::shared_ptr<ObjectBuffer> buffer;
    if (ObjectBufferInternal::Create(info, buffer).IsError()
        || buffer->MemoryCopy(data.data(), data.size()).IsError()) {
        return nullptr;
    }
    return buffer;
}

std::vector<std::shared_ptr<ObjectBuffer>> MakeTransportBuffers(const HostPort &workerAddr, size_t count)
{
    std::vector<std::shared_ptr<ObjectBuffer>> buffers;
    buffers.reserve(count);
    for (size_t i = 0; i < count; ++i) {
        auto buffer = MakeTransportBuffer(workerAddr, "key-" + std::to_string(i), "data",
                                          "shm-" + std::to_string(i), true);
        if (buffer == nullptr) {
            return {};
        }
        buffers.emplace_back(std::move(buffer));
    }
    return buffers;
}

master::QueryAndGetResultPb *AddLocation(master::QueryAndGetRspPb &response, const std::string &key,
                                         const HostPort &address, uint64_t size = 4)
{
    auto *result = response.add_results();
    auto *location = result->mutable_location();
    location->set_object_key(key);
    location->add_object_locations(address.ToString());
    location->set_object_size(size);
    return result;
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

    Status InvokeQueryAndGet(master::QueryAndGetReqPb &request, master::QueryAndGetRspPb &response,
                             std::vector<RpcMessage> &payloads) override
    {
        ++queryAndGetCount;
        queryAndGetRequests.push_back(request);
        if (queryAndGetHandler) {
            return queryAndGetHandler(WorkerAddress(), request, response, payloads);
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
        static_cast<void>(response);
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

    Status InvokeMultiCreate(int64_t, MultiCreateReqPb &request, MultiCreateRspPb &response,
                             uint32_t &workerVersion) override
    {
        ++multiCreateInvokeCount;
        invokedMultiCreateRequests.push_back(request);
        if (multiCreateInvokeStatus.IsError()) {
            return multiCreateInvokeStatus;
        }
        for (int i = 0; i < request.object_key_size(); ++i) {
            auto *item = response.add_results();
            item->set_shm_id("multi-shm-" + std::to_string(i));
            if (createResponseHasUrmaInfo) {
                item->mutable_urma_info()->set_seg_va(0x1000 + i);
            }
        }
        workerVersion = version;
        return Status::OK();
    }

    Status InvokeMultiSet(int64_t, MultiPublishReqPb &request, const std::vector<MemView> &payloads,
                          MultiPublishRspPb &response, uint32_t &workerVersion) override
    {
        ++multiSetInvokeCount;
        invokedMultiSetRequests.push_back(request);
        invokedMultiSetPayloadData.emplace_back();
        for (const auto &payload : payloads) {
            invokedMultiSetPayloadData.back().emplace_back(static_cast<const char *>(payload.Data()), payload.Size());
        }
        if (onMultiSetInvoke) {
            onMultiSetInvoke();
        }
        if (multiSetInvokeStatus.IsError()) {
            return multiSetInvokeStatus;
        }
        for (const auto &key : multiSetFailedKeys) {
            response.add_failed_object_keys(key);
        }
        response.mutable_last_rc()->set_error_code(multiSetLastCode);
        response.mutable_last_rc()->set_error_msg(multiSetLastMessage);
        workerVersion = version;
        if (afterMultiSetInvoke) {
            afterMultiSetInvoke();
        }
        return Status::OK();
    }

    Status InvokeDecreaseReference(const TransportRequestContext &context, const ShmKey &shmId) override
    {
        ++decreaseReferenceCount;
        decreaseReferenceContexts.push_back(context);
        decreaseReferenceShmIds.push_back(shmId);
        return decreaseReferenceStatus;
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
    std::function<Status(const HostPort &, const master::QueryAndGetReqPb &, master::QueryAndGetRspPb &,
                         std::vector<RpcMessage> &)>
        queryAndGetHandler;
    std::function<void()> onInvoke;
    std::function<void()> afterInvoke;

    // Create/Set fake state
    int createInvokeCount = 0;
    int setInvokeCount = 0;
    int multiCreateInvokeCount = 0;
    int multiSetInvokeCount = 0;
    int decreaseReferenceCount = 0;
    Status createInvokeStatus = Status::OK();
    Status setInvokeStatus = Status::OK();
    Status multiCreateInvokeStatus = Status::OK();
    Status multiSetInvokeStatus = Status::OK();
    Status decreaseReferenceStatus = Status::OK();
    bool createResponseHasUrmaInfo = false;
    int64_t createResponseMetadataSize = 0;
    StatusCode createResponseCode = K_OK;
    StatusCode setResponseCode = K_OK;
    std::vector<CreateReqPb> invokedCreateRequests;
    std::vector<PublishReqPb> invokedSetRequests;
    std::vector<MultiCreateReqPb> invokedMultiCreateRequests;
    std::vector<MultiPublishReqPb> invokedMultiSetRequests;
    std::vector<size_t> invokedSetPayloadSizes;
    std::vector<std::vector<std::string>> invokedSetPayloadData;
    std::vector<std::vector<std::string>> invokedMultiSetPayloadData;
    std::vector<std::string> multiSetFailedKeys;
    StatusCode multiSetLastCode = K_OK;
    std::string multiSetLastMessage;
    std::vector<TransportRequestContext> decreaseReferenceContexts;
    std::vector<ShmKey> decreaseReferenceShmIds;
    std::function<void()> onSetInvoke;
    std::function<void()> afterSetInvoke;
    std::function<void()> onMultiSetInvoke;
    std::function<void()> afterMultiSetInvoke;
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
    int hashRingCount = 0;
    int dataRpcTimeout = 0;
    int metadataRpcTimeout = 0;
    int hashRingRpcTimeout = 0;
    uint64_t hashRingVersion = 0;
    GetObjectRemoteReqPb invokedDataRequest;
    master::QueryAndGetReqPb invokedMetadataRequest;
    GetHashRingReqPb invokedHashRingRequest;
    CreateReqPb invokedCreateRequest;
    PublishReqPb invokedSetRequest;
    MultiCreateReqPb invokedMultiCreateRequest;
    MultiPublishReqPb invokedMultiSetRequest;
    DecreaseReferenceRequest invokedDecreaseReferenceRequest;
    int createInvokeCount = 0;
    int setInvokeCount = 0;
    int multiCreateInvokeCount = 0;
    int multiSetInvokeCount = 0;
    int decreaseReferenceInvokeCount = 0;
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
                               master::QueryAndGetRspPb &, std::vector<RpcMessage> &) override
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

    Status DoInvokeMultiCreate(const RpcOptions &, const MultiCreateReqPb &request,
                               MultiCreateRspPb &) override
    {
        ++multiCreateInvokeCount;
        invokedMultiCreateRequest = request;
        return Status::OK();
    }

    Status DoInvokeMultiSet(const RpcOptions &, const MultiPublishReqPb &request, MultiPublishRspPb &,
                            const std::vector<MemView> &) override
    {
        ++multiSetInvokeCount;
        invokedMultiSetRequest = request;
        return Status::OK();
    }

    Status DoInvokeDecreaseReference(const RpcOptions &, const DecreaseReferenceRequest &request,
                                     DecreaseReferenceResponse &) override
    {
        ++decreaseReferenceInvokeCount;
        invokedDecreaseReferenceRequest = request;
        return Status::OK();
    }

    Status DoInvokeGetHashRing(const RpcOptions &options, const GetHashRingReqPb &request,
                               GetHashRingRspPb &response) override
    {
        ++hashRingCount;
        hashRingRpcTimeout = options.GetTimeout();
        hashRingVersion = request.version();
        invokedHashRingRequest = request;
        response.set_version(request.version() + 1);
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
        info->shmId = ShmKey::Intern("fake-shm-id");
        info->pointer = static_cast<uint8_t *>(malloc(size + 1));
        memset(info->pointer, 0, size + 1);
        return ObjectBufferInternal::Create(info, buffer);
    }

    Status Set(ObjectBuffer &buffer, const TransportSetParam &param) override
    {
        static_cast<void>(buffer);
        ++setCount;
        setParams.push_back(param);
        if (!setStatuses.empty()) {
            Status rc = setStatuses.front();
            setStatuses.erase(setStatuses.begin());
            return rc;
        }
        return Status::OK();
    }

    Status MCreate(const HostPort &workerAddr, const std::vector<std::string> &keys,
                   const std::vector<uint64_t> &sizes, const TransportCreateParam &param,
                   std::vector<std::shared_ptr<ObjectBuffer>> &buffers) override
    {
        ++mCreateCount;
        if (!mCreateStatuses.empty()) {
            Status rc = mCreateStatuses.front();
            mCreateStatuses.erase(mCreateStatuses.begin());
            if (rc.IsError()) {
                return rc;
            }
        }
        for (size_t i = 0; i < keys.size(); ++i) {
            std::shared_ptr<ObjectBuffer> buffer;
            RETURN_IF_NOT_OK(Create(workerAddr, keys[i], sizes[i], param, buffer));
            buffers.emplace_back(std::move(buffer));
        }
        return Status::OK();
    }

    Status MSet(const std::vector<std::shared_ptr<ObjectBuffer>> &, const TransportSetParam &,
                TransportMSetResult &result) override
    {
        ++mSetCount;
        result.actualKind = kind;
        result.publishAttempted = mSetPublishAttempted;
        if (!mSetStatuses.empty()) {
            Status rc = mSetStatuses.front();
            mSetStatuses.erase(mSetStatuses.begin());
            return rc;
        }
        return Status::OK();
    }

    Status Release(const ShmKey &, const TransportRequestContext &context) override
    {
        ++releaseCount;
        releaseContexts.push_back(context);
        return releaseStatus;
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
    bool mSetPublishAttempted = true;
    int closeCount = 0;
    std::vector<Status> getStatuses;
    std::function<void()> onClose;

    // Create/Set fake state
    int createCount = 0;
    int setCount = 0;
    int mCreateCount = 0;
    int mSetCount = 0;
    int releaseCount = 0;
    Status releaseStatus = Status::OK();
    std::vector<Status> createStatuses;
    std::vector<Status> setStatuses;
    std::vector<Status> mCreateStatuses;
    std::vector<Status> mSetStatuses;
    std::vector<std::string> createdKeys;
    std::vector<uint64_t> createdSizes;
    std::vector<TransportSetParam> setParams;
    std::vector<TransportRequestContext> releaseContexts;
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
        if (!transporterMCreateStatuses.empty()) {
            transporter->mCreateStatuses = std::move(transporterMCreateStatuses.front());
            transporterMCreateStatuses.erase(transporterMCreateStatuses.begin());
        }
        if (!transporterMSetStatuses.empty()) {
            transporter->mSetStatuses = std::move(transporterMSetStatuses.front());
            transporterMSetStatuses.erase(transporterMSetStatuses.begin());
        }
        if (!transporterMSetPublishAttempted.empty()) {
            transporter->mSetPublishAttempted = transporterMSetPublishAttempted.front();
            transporterMSetPublishAttempted.erase(transporterMSetPublishAttempted.begin());
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
    std::vector<std::vector<Status>> transporterMCreateStatuses;
    std::vector<std::vector<Status>> transporterMSetStatuses;
    std::vector<bool> transporterMSetPublishAttempted;
    std::vector<std::shared_ptr<FakeTransporter>> builtTransporters;
    std::function<Status(const HostPort &, const master::QueryAndGetReqPb &, master::QueryAndGetRspPb &,
                         std::vector<RpcMessage> &)>
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
            auto inlineKind = inlineKinds.find(item->objectKey);
            if (item->status.IsOk() && inlineKind != inlineKinds.end()) {
                DataGetResult data;
                data.response.mutable_error()->set_error_code(K_OK);
                data.response.set_data_size(4);
                data.kind = inlineKind->second;
                if (data.kind == AccessTransportKind::TCP) {
                    RpcMessage payload;
                    EXPECT_TRUE(payload.CopyString("data").IsOk());
                    data.rpcPayloads.emplace_back(std::move(payload));
                }
                item->inlineData.emplace(std::move(data));
            }
        }
        return Status::OK();
    }

    std::mutex mutex;
    std::vector<HostPort> addresses;
    std::vector<std::vector<std::string>> keyGroups;
    std::vector<std::thread::id> threadIds;
    std::unordered_map<std::string, Status> groupStatuses;
    std::unordered_map<std::string, Status> itemStatuses;
    std::unordered_map<std::string, AccessTransportKind> inlineKinds;
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
    std::vector<Status> writeStatuses;
    int writeCount = 0;
    int writeBatchCount = 0;
    int waitCount = 0;
    int buildMCreateBufferCount = 0;
    std::function<void(int)> afterWait;

protected:
    Status WritePayload(ObjectBufferInfo &) override
    {
        ++writeCount;
        if (!writeStatuses.empty()) {
            Status rc = writeStatuses.front();
            writeStatuses.erase(writeStatuses.begin());
            return rc;
        }
        return writeStatus;
    }

    Status SubmitPayload(ObjectBufferInfo &, bool, std::vector<uint64_t> &eventKeys) override
    {
        if (static_cast<size_t>(writeCount) % GetMSetPipelineDepth() == 0) {
            ++writeBatchCount;
        }
        ++writeCount;
        Status rc = writeStatus;
        if (!writeStatuses.empty()) {
            rc = writeStatuses.front();
            writeStatuses.erase(writeStatuses.begin());
        }
        if (rc.IsOk()) {
            eventKeys.emplace_back(static_cast<uint64_t>(writeCount));
        }
        return rc;
    }

    Status WaitPayloadEvents(std::vector<uint64_t> &) override
    {
        ++waitCount;
        if (afterWait) {
            afterWait(waitCount);
        }
        return Status::OK();
    }

    Status BuildMCreateBuffer(const HostPort &workerAddr, const std::string &key, uint64_t size,
                              const TransportCreateParam &param, const CreateRspPb &response,
                              uint32_t workerVersion, std::shared_ptr<ObjectBuffer> &buffer) override
    {
        ++buildMCreateBufferCount;
        auto info = std::make_shared<ObjectBufferInfo>();
        info->objectKey = key;
        info->dataSize = size;
        info->metadataSize = 0;
        info->workerAddr = workerAddr;
        info->objectMode = ModeInfo(param.consistencyType, param.writeMode, param.cacheType);
        info->ubUrmaDataInfo = std::make_shared<UrmaRemoteAddrPb>(response.urma_info());
        info->pointer = static_cast<uint8_t *>(calloc(size + 1, 1));
        info->shmId = ShmKey::Intern(response.shm_id());
        info->version = workerVersion;
        return ObjectBufferInternal::Create(std::move(info), buffer);
    }
};

class TestTransportLayer : public TransportLayer {
public:
    explicit TestTransportLayer(std::shared_ptr<DataPlaneManager> dataPlaneManager)
        : TransportLayer(std::move(dataPlaneManager), std::make_shared<TransportAdvisor>())
    {
    }
};

class FixedTransportAdvisor : public TransportAdvisor {
public:
    explicit FixedTransportAdvisor(TransportHint hint) : hint_(hint)
    {
    }

    TransportHint GetTransportHint(const HostPort &) const override
    {
        return hint_;
    }

private:
    TransportHint hint_;
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

    Status Allocate(uint64_t requiredSize, UbReceiveBuffer &buffer) override
    {
        ++allocateCount;
        buffer = UbReceiveBuffer{};
        if (allocateStatus.IsError()) {
            return allocateStatus;
        }
        auto fakeOwner = std::make_shared<FakeBufferOwner>(requiredSize);
        buffer.data = fakeOwner->data.data();
        buffer.size = fakeOwner->data.size();
        buffer.remoteAddr.set_seg_va(reinterpret_cast<uint64_t>(buffer.data));
        buffer.owner = fakeOwner;
        buffer.transportInstanceId = "test-instance";
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
    std::vector<RpcMessage> metadataPayloads;
    ASSERT_TRUE(client.InvokeQueryAndGet(metadataRequest, metadataResponse, metadataPayloads).IsOk());
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

    MultiCreateReqPb multiCreateRequest;
    multiCreateRequest.add_object_key("multi-create-key");
    MultiCreateRspPb multiCreateResponse;
    ASSERT_TRUE(client.InvokeMultiCreate(100, multiCreateRequest, multiCreateResponse, workerVersion).IsOk());
    EXPECT_EQ(client.invokedMultiCreateRequest.access_key(), "access-1");
    EXPECT_FALSE(client.invokedMultiCreateRequest.signature().empty());

    MultiPublishReqPb multiSetRequest;
    multiSetRequest.add_object_info()->set_object_key("multi-set-key");
    MultiPublishRspPb multiSetResponse;
    ASSERT_TRUE(client.InvokeMultiSet(100, multiSetRequest, payloads, multiSetResponse, workerVersion).IsOk());
    EXPECT_EQ(client.invokedMultiSetRequest.access_key(), "access-1");
    EXPECT_FALSE(client.invokedMultiSetRequest.signature().empty());

    TransportRequestContext context{ "client-1", "token-1", "tenant-1" };
    ASSERT_TRUE(client.InvokeDecreaseReference(context, ShmKey::Intern("shm-1")).IsOk());
    EXPECT_EQ(client.decreaseReferenceInvokeCount, 1);
    EXPECT_EQ(client.invokedDecreaseReferenceRequest.client_id(), "client-1");
    EXPECT_EQ(client.invokedDecreaseReferenceRequest.object_keys(0), "shm-1");
    EXPECT_EQ(client.invokedDecreaseReferenceRequest.token(), "token-1");
    EXPECT_EQ(client.invokedDecreaseReferenceRequest.tenant_id(), "tenant-1");
    EXPECT_TRUE(client.invokedDecreaseReferenceRequest.is_routed());
    EXPECT_EQ(client.invokedDecreaseReferenceRequest.access_key(), "access-1");
    EXPECT_FALSE(client.invokedDecreaseReferenceRequest.signature().empty());
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
    std::vector<RpcMessage> metadataPayloads;
    ASSERT_TRUE(client.InvokeQueryAndGet(metadataRequest, metadataResponse, metadataPayloads).IsOk());
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
    std::vector<RpcMessage> metadataPayloads;
    EXPECT_EQ(client.InvokeQueryAndGet(metadataRequest, metadataResponse, metadataPayloads).GetCode(),
              K_RPC_DEADLINE_EXCEEDED);
    EXPECT_EQ(client.metadataInvokeCount, 0);
}

TEST(WorkerRpcClientTest, HashRingRefreshSignsRequestAndUsesControlTimeoutOutsideApiDeadline)
{
    ApiDeadlineGuard deadline(-1, InUs{});
    auto signature = std::make_shared<Signature>("access-1", SensitiveValue("secret-1"));
    AuthBoundaryWorkerRpcClient client(signature);
    GetHashRingRspPb response;

    ASSERT_TRUE(client.InvokeGetHashRing(17, response).IsOk());
    EXPECT_EQ(client.hashRingCount, 1);
    EXPECT_EQ(client.hashRingVersion, 17u);
    EXPECT_EQ(client.invokedHashRingRequest.access_key(), "access-1");
    EXPECT_FALSE(client.invokedHashRingRequest.signature().empty());
    EXPECT_GT(client.hashRingRpcTimeout, 0);
    EXPECT_EQ(response.version(), 18u);
}

TEST(WorkerSnapshotTest, BuildsFromEveryTopologyMembershipState)
{
    ::datasystem::ClusterTopologyPb ring;
    const std::vector<::datasystem::MembershipPb::StatePb> states = {
        ::datasystem::MembershipPb::INITIAL,     ::datasystem::MembershipPb::JOINING,
        ::datasystem::MembershipPb::ACTIVE,      ::datasystem::MembershipPb::PRE_LEAVING,
        ::datasystem::MembershipPb::LEAVING,     ::datasystem::MembershipPb::FAILED,
    };
    for (size_t i = 0; i < states.size(); ++i) {
        const auto address = MakeAddress(100 + static_cast<int>(i));
        (*ring.mutable_members())[address.ToString()].set_state(states[i]);
    }

    WorkerSnapshot snapshot;
    ASSERT_TRUE(BuildWorkerSnapshot(42, ring, snapshot).IsOk());
    EXPECT_EQ(snapshot.ringVersion, 42u);
    EXPECT_TRUE(snapshot.sameHostAddrs.empty());
    EXPECT_EQ(snapshot.otherAddrs.size(), states.size());
}

TEST(WorkerSnapshotTest, RejectsMalformedTopologyWithoutChangingOutput)
{
    ::datasystem::ClusterTopologyPb ring;
    (*ring.mutable_members())["malformed-endpoint"].set_state(::datasystem::MembershipPb::ACTIVE);
    WorkerSnapshot snapshot;
    snapshot.ringVersion = 7;
    snapshot.sameHostAddrs.push_back(MakeAddress(110));

    EXPECT_EQ(BuildWorkerSnapshot(8, ring, snapshot).GetCode(), K_INVALID);
    EXPECT_EQ(snapshot.ringVersion, 7u);
    ASSERT_EQ(snapshot.sameHostAddrs.size(), 1u);
    EXPECT_EQ(snapshot.sameHostAddrs.front(), MakeAddress(110));
}

TEST(WorkerSnapshotTest, AcceptsEmptyTopologyAsCleanupAll)
{
    ::datasystem::ClusterTopologyPb ring;
    WorkerSnapshot snapshot;
    snapshot.otherAddrs.push_back(MakeAddress(111));

    ASSERT_TRUE(BuildWorkerSnapshot(9, ring, snapshot).IsOk());
    EXPECT_EQ(snapshot.ringVersion, 9u);
    EXPECT_TRUE(snapshot.Empty());
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

TEST(DataPlaneManagerTest, ReusesRpcClientWithoutCreatingTransporter)
{
    FakeDataPlaneManager manager;
    std::shared_ptr<WorkerRpcClient> first;
    std::shared_ptr<WorkerRpcClient> second;

    ASSERT_TRUE(manager.GetOrCreateRpcClient(MakeAddress(1), first).IsOk());
    ASSERT_TRUE(manager.GetOrCreateRpcClient(MakeAddress(1), second).IsOk());
    EXPECT_EQ(first, second);
    EXPECT_EQ(manager.rpcBuildCount, 1);
    EXPECT_EQ(manager.transportBuildCount, 0);
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

TEST(DataPlaneManagerTest, PublishedSnapshotRejectsAbsentWorkersBeforeCleanup)
{
    FakeDataPlaneManager manager;
    const HostPort live = MakeAddress(22);
    const HostPort removed = MakeAddress(23);
    std::shared_ptr<IDataTransporter> transporter;
    ASSERT_TRUE(manager.GetOrCreate(live, TransportHint::TCP_ONLY, transporter).IsOk());
    auto liveTransporter = transporter;
    ASSERT_TRUE(manager.GetOrCreate(removed, TransportHint::TCP_ONLY, transporter).IsOk());
    auto removedTransporter = std::dynamic_pointer_cast<FakeTransporter>(transporter);
    ASSERT_NE(removedTransporter, nullptr);

    WorkerSnapshot snapshot;
    snapshot.ringVersion = 10;
    snapshot.otherAddrs.push_back(live);
    ASSERT_TRUE(manager.UpdateWorkerSnapshot(snapshot).IsOk());
    EXPECT_EQ(manager.GetOrCreate(removed, TransportHint::TCP_ONLY, transporter).GetCode(), K_NOT_FOUND);
    EXPECT_EQ(removedTransporter->closeCount, 0);

    manager.ReconcileWithSnapshot(snapshot);
    EXPECT_EQ(removedTransporter->closeCount, 1);
    ASSERT_TRUE(manager.GetOrCreate(live, TransportHint::TCP_ONLY, transporter).IsOk());
    EXPECT_EQ(transporter, liveTransporter);
}

TEST(DataPlaneManagerTest, SupersededSnapshotCannotRemoveCurrentWorkers)
{
    FakeDataPlaneManager manager;
    const HostPort live = MakeAddress(24);
    std::shared_ptr<IDataTransporter> transporter;
    ASSERT_TRUE(manager.GetOrCreate(live, TransportHint::TCP_ONLY, transporter).IsOk());
    auto liveTransporter = std::dynamic_pointer_cast<FakeTransporter>(transporter);
    ASSERT_NE(liveTransporter, nullptr);

    WorkerSnapshot latest;
    latest.ringVersion = 12;
    latest.otherAddrs.push_back(live);
    ASSERT_TRUE(manager.UpdateWorkerSnapshot(latest).IsOk());
    WorkerSnapshot superseded;
    superseded.ringVersion = 11;
    manager.ReconcileWithSnapshot(superseded);

    EXPECT_EQ(liveTransporter->closeCount, 0);
    ASSERT_TRUE(manager.GetOrCreate(live, TransportHint::TCP_ONLY, transporter).IsOk());
    EXPECT_EQ(transporter, liveTransporter);
    EXPECT_EQ(manager.UpdateWorkerSnapshot(superseded).GetCode(), K_INVALID);
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
                                           master::QueryAndGetRspPb &response, std::vector<RpcMessage> &) {
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

TEST(ObjectMetadataClientTest, RejectsResultCountMismatchBeforeIndexedAccess)
{
    ApiDeadlineGuard deadline(1000);
    auto manager = std::make_shared<FakeDataPlaneManager>();
    manager->queryAndGetHandler = [](const HostPort &, const master::QueryAndGetReqPb &,
                                     master::QueryAndGetRspPb &, std::vector<RpcMessage> &) {
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
                                     master::QueryAndGetRspPb &response, std::vector<RpcMessage> &) {
        response.add_results()->mutable_location()->set_object_key("missing");
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
                                                 master::QueryAndGetRspPb &response,
                                                 std::vector<RpcMessage> &) {
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
                                     master::QueryAndGetRspPb &response, std::vector<RpcMessage> &) {
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
                                                 master::QueryAndGetRspPb &response,
                                                 std::vector<RpcMessage> &) {
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
                                     master::QueryAndGetRspPb &response, std::vector<RpcMessage> &) {
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

TEST(ObjectMetadataClientTest, TcpInlineDataMovesRpcPayloadIntoMetadataResult)
{
    ApiDeadlineGuard deadline(1000);
    auto manager = std::make_shared<FakeDataPlaneManager>();
    manager->queryAndGetHandler = [](const HostPort &, const master::QueryAndGetReqPb &request,
                                     master::QueryAndGetRspPb &response, std::vector<RpcMessage> &payloads) {
        EXPECT_EQ(request.object_keys_size(), 1);
        EXPECT_TRUE(request.has_data_request());
        EXPECT_TRUE(request.data_request().has_tcp());
        auto *result = AddLocation(response, "key", MakeAddress(51), 6);
        result->mutable_data_result()->add_payload_indexes(0);
        RpcMessage payload;
        RETURN_IF_NOT_OK(payload.CopyString("inline"));
        payloads.emplace_back(std::move(payload));
        return Status::OK();
    };
    ObjectMetadataClient metadata(manager, std::make_shared<DeadlineRetry>(),
                                  std::make_shared<FixedTransportAdvisor>(TransportHint::TCP_ONLY));
    auto results = MakeMetadataItems({ { 0, "key", MakeAddress(41) } });
    auto batch = MakeMetadataBatch(results);

    ASSERT_TRUE(metadata.QueryAndGet(MakeAddress(41), batch).IsOk());
    ASSERT_EQ(results.size(), 1u);
    ASSERT_TRUE(results[0].inlineData.has_value());
    auto &data = *results[0].inlineData;
    EXPECT_EQ(data.kind, AccessTransportKind::TCP);
    EXPECT_EQ(data.response.data_size(), 6);
    EXPECT_EQ(data.response.data_source(), DataTransferSource::DATA_IN_PAYLOAD);
    ASSERT_EQ(data.rpcPayloads.size(), 1u);
    EXPECT_EQ(data.rpcPayloads[0].Size(), 6u);
    EXPECT_EQ(std::string(static_cast<const char *>(data.rpcPayloads[0].Data()), data.rpcPayloads[0].Size()),
              "inline");
}

TEST(ObjectMetadataClientTest, MissingTcpInlineMarkerFallsBackToReplicaRead)
{
    ApiDeadlineGuard deadline(1000);
    auto manager = std::make_shared<FakeDataPlaneManager>();
    manager->queryAndGetHandler = [](const HostPort &, const master::QueryAndGetReqPb &request,
                                     master::QueryAndGetRspPb &response, std::vector<RpcMessage> &) {
        EXPECT_TRUE(request.has_data_request());
        EXPECT_TRUE(request.data_request().has_tcp());
        AddLocation(response, "key", MakeAddress(51), 6);
        return Status::OK();
    };
    ObjectMetadataClient metadata(manager, std::make_shared<DeadlineRetry>(),
                                  std::make_shared<FixedTransportAdvisor>(TransportHint::TCP_ONLY));
    auto results = MakeMetadataItems({ { 0, "key", MakeAddress(41) } });
    auto batch = MakeMetadataBatch(results);

    ASSERT_TRUE(metadata.QueryAndGet(MakeAddress(41), batch).IsOk());
    ASSERT_EQ(results.size(), 1u);
    EXPECT_TRUE(results[0].status.IsOk());
    EXPECT_FALSE(results[0].inlineData.has_value());
    EXPECT_EQ(results[0].location.object_locations(0), MakeAddress(51).ToString());
}

TEST(ObjectMetadataClientTest, RejectsInvalidTcpInlinePayloadIndex)
{
    ApiDeadlineGuard deadline(1000);
    auto manager = std::make_shared<FakeDataPlaneManager>();
    manager->queryAndGetHandler = [](const HostPort &, const master::QueryAndGetReqPb &,
                                     master::QueryAndGetRspPb &response, std::vector<RpcMessage> &payloads) {
        auto *result = AddLocation(response, "key", MakeAddress(51), 6);
        result->mutable_data_result()->add_payload_indexes(1);
        RpcMessage payload;
        RETURN_IF_NOT_OK(payload.CopyString("inline"));
        payloads.emplace_back(std::move(payload));
        return Status::OK();
    };
    ObjectMetadataClient metadata(manager, std::make_shared<DeadlineRetry>(),
                                  std::make_shared<FixedTransportAdvisor>(TransportHint::TCP_ONLY));
    auto results = MakeMetadataItems({ { 0, "key", MakeAddress(41) } });
    auto batch = MakeMetadataBatch(results);

    EXPECT_EQ(metadata.QueryAndGet(MakeAddress(41), batch).GetCode(), K_RUNTIME_ERROR);
}

TEST(ObjectMetadataClientTest, UbInlineDataUsesConfiguredCapacityAndExternalBuffer)
{
    ApiDeadlineGuard deadline(1000);
    auto manager = std::make_shared<FakeDataPlaneManager>();
    auto bufferProvider = std::make_shared<FakeUbBufferProvider>();
    bufferProvider->maxGetSize = 32;
    manager->queryAndGetHandler = [](const HostPort &, const master::QueryAndGetReqPb &request,
                                     master::QueryAndGetRspPb &response, std::vector<RpcMessage> &payloads) {
        EXPECT_TRUE(request.has_data_request());
        EXPECT_TRUE(request.data_request().has_ub());
        const auto &ubRequest = request.data_request().ub();
        EXPECT_EQ(ubRequest.buffer_size(), 16u);
        EXPECT_EQ(ubRequest.urma_instance_id(), "test-instance");
        EXPECT_EQ(ubRequest.buffer_infos_size(), 1);
        EXPECT_TRUE(payloads.empty());
        if (ubRequest.buffer_infos_size() == 1) {
            auto *data = reinterpret_cast<void *>(ubRequest.buffer_infos(0).seg_va());
            std::memcpy(data, "ubdata", 6);
        }
        AddLocation(response, "key", MakeAddress(51), 6)->mutable_data_result();
        return Status::OK();
    };
    auto advisor = std::make_shared<FixedTransportAdvisor>(TransportHint::UB_CANDIDATE);
    ObjectMetadataClient metadata(manager, std::make_shared<DeadlineRetry>(), advisor, bufferProvider, 16);
    auto results = MakeMetadataItems({ { 0, "key", MakeAddress(41) } });
    auto batch = MakeMetadataBatch(results);

    ASSERT_TRUE(metadata.QueryAndGet(MakeAddress(41), batch).IsOk());
    EXPECT_EQ(bufferProvider->allocateCount, 1);
    EXPECT_EQ(manager->rpcBuildCount, 1);
    EXPECT_EQ(manager->transportBuildCount, 1);
    ASSERT_TRUE(results[0].inlineData.has_value());
    auto &data = *results[0].inlineData;
    EXPECT_EQ(data.kind, AccessTransportKind::UB);
    EXPECT_EQ(data.response.data_size(), 6);
    EXPECT_EQ(data.response.data_source(), DataTransferSource::DATA_ALREADY_TRANSFERRED);
    EXPECT_TRUE(data.rpcPayloads.empty());
    EXPECT_EQ(data.externalSize, 6u);
    ASSERT_NE(data.externalData, nullptr);
    EXPECT_EQ(std::string(reinterpret_cast<const char *>(data.externalData), data.externalSize), "ubdata");
    EXPECT_NE(data.externalOwner, nullptr);
}

TEST(ObjectMetadataClientTest, UbCapacityMissReleasesBufferAndFallsBack)
{
    ApiDeadlineGuard deadline(1000);
    auto manager = std::make_shared<FakeDataPlaneManager>();
    auto bufferProvider = std::make_shared<FakeUbBufferProvider>();
    manager->queryAndGetHandler = [](const HostPort &, const master::QueryAndGetReqPb &request,
                                     master::QueryAndGetRspPb &response, std::vector<RpcMessage> &) {
        EXPECT_TRUE(request.has_data_request());
        EXPECT_TRUE(request.data_request().has_ub());
        EXPECT_EQ(request.data_request().ub().buffer_size(), 16u);
        AddLocation(response, "key", MakeAddress(51), 20);
        return Status::OK();
    };
    ObjectMetadataClient metadata(manager, std::make_shared<DeadlineRetry>(),
                                  std::make_shared<FixedTransportAdvisor>(TransportHint::UB_CANDIDATE),
                                  bufferProvider, 16);
    auto results = MakeMetadataItems({ { 0, "key", MakeAddress(41) } });
    auto batch = MakeMetadataBatch(results);

    ASSERT_TRUE(metadata.QueryAndGet(MakeAddress(41), batch).IsOk());
    EXPECT_FALSE(results[0].inlineData.has_value());
    EXPECT_TRUE(bufferProvider->lastOwner.expired());
}

TEST(ObjectMetadataClientTest, UbBufferAllocationFailureQueriesMetadataOnly)
{
    ApiDeadlineGuard deadline(1000);
    auto manager = std::make_shared<FakeDataPlaneManager>();
    auto bufferProvider = std::make_shared<FakeUbBufferProvider>();
    bufferProvider->allocateStatus = Status(K_OUT_OF_MEMORY, "allocation failed");
    manager->queryAndGetHandler = [](const HostPort &, const master::QueryAndGetReqPb &request,
                                     master::QueryAndGetRspPb &response, std::vector<RpcMessage> &) {
        EXPECT_FALSE(request.has_data_request());
        AddLocation(response, "key", MakeAddress(51), 6);
        return Status::OK();
    };
    ObjectMetadataClient metadata(manager, std::make_shared<DeadlineRetry>(),
                                  std::make_shared<FixedTransportAdvisor>(TransportHint::UB_CANDIDATE),
                                  bufferProvider, 16);
    auto results = MakeMetadataItems({ { 0, "key", MakeAddress(41) } });
    auto batch = MakeMetadataBatch(results);

    ASSERT_TRUE(metadata.QueryAndGet(MakeAddress(41), batch).IsOk());
    EXPECT_EQ(bufferProvider->allocateCount, 1);
    EXPECT_EQ(manager->transportBuildCount, 1);
    EXPECT_FALSE(results[0].inlineData.has_value());
}

TEST(ObjectMetadataClientTest, UbConnectionFailureQueriesMetadataOnly)
{
    ApiDeadlineGuard deadline(1000);
    auto manager = std::make_shared<FakeDataPlaneManager>();
    manager->transportBuildStatuses.emplace_back(K_URMA_ERROR, "connect failed");
    auto bufferProvider = std::make_shared<FakeUbBufferProvider>();
    manager->queryAndGetHandler = [](const HostPort &, const master::QueryAndGetReqPb &request,
                                     master::QueryAndGetRspPb &response, std::vector<RpcMessage> &) {
        EXPECT_FALSE(request.has_data_request());
        AddLocation(response, "key", MakeAddress(51), 6);
        return Status::OK();
    };
    ObjectMetadataClient metadata(manager, std::make_shared<DeadlineRetry>(),
                                  std::make_shared<FixedTransportAdvisor>(TransportHint::UB_CANDIDATE),
                                  bufferProvider, 16);
    auto results = MakeMetadataItems({ { 0, "key", MakeAddress(41) } });
    auto batch = MakeMetadataBatch(results);

    ASSERT_TRUE(metadata.QueryAndGet(MakeAddress(41), batch).IsOk());
    EXPECT_EQ(manager->rpcBuildCount, 1);
    EXPECT_EQ(manager->transportBuildCount, 1);
    EXPECT_EQ(bufferProvider->allocateCount, 0);
    EXPECT_FALSE(results[0].inlineData.has_value());
    EXPECT_TRUE(bufferProvider->lastOwner.expired());
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

TEST(ObjectReadFlowTest, InlineDataSkipsReplicaReaderWhileMissesUseSecondPhase)
{
    ApiDeadlineGuard deadline(1000);
    auto metadata = std::make_shared<FakeObjectMetadataClient>();
    metadata->inlineKinds.emplace("inline", AccessTransportKind::TCP);
    auto replicas = std::make_shared<FakeReplicaReader>();
    auto taskPool = std::make_shared<ThreadPool>(0, 4, "object_read_test");
    ObjectReadFlow flow(metadata, replicas, taskPool);
    ObjectReadRequest request;
    request.items = { { 0, "inline", MakeAddress(41) }, { 1, "fallback", MakeAddress(41) } };
    ObjectReadResult result;

    ASSERT_TRUE(flow.Run(request, result).IsOk());
    ASSERT_EQ(replicas->keys.size(), 1u);
    EXPECT_EQ(replicas->keys[0], "fallback");
    ASSERT_EQ(result.items.size(), 2u);
    EXPECT_TRUE(result.items[0].status.IsOk());
    EXPECT_EQ(result.items[0].data.kind, AccessTransportKind::TCP);
    ASSERT_EQ(result.items[0].data.rpcPayloads.size(), 1u);
    EXPECT_EQ(std::string(static_cast<const char *>(result.items[0].data.rpcPayloads[0].Data()),
                          result.items[0].data.rpcPayloads[0].Size()),
              "data");
    EXPECT_TRUE(result.items[1].status.IsOk());
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
    EXPECT_EQ(rpcClient->getObjectRequests[0].read_offset(), 0u);
    EXPECT_EQ(rpcClient->getObjectRequests[0].read_size(), 8u);
    EXPECT_EQ(rpcClient->getObjectRequests[0].data_size(), 8u);
    EXPECT_TRUE(rpcClient->getObjectRequests[0].try_lock());
    EXPECT_EQ(result.kind, AccessTransportKind::UB);
    EXPECT_EQ(result.externalSize, 8u);
    EXPECT_NE(result.externalData, nullptr);
    EXPECT_NE(result.externalOwner, nullptr);
}

TEST(UbTransporterTest, GetKeepsFullReadSentinelWhenUbBufferUnavailable)
{
    auto rpcClient = std::make_shared<FakeWorkerRpcClient>();
    rpcClient->getObjectDataSize = 8;
    auto bufferProvider = std::make_shared<FakeUbBufferProvider>();
    bufferProvider->allocateStatus = Status(K_OUT_OF_MEMORY, "allocate failed");
    UbTransporter transporter(rpcClient, std::make_shared<FakeUbConnection>(), bufferProvider);
    DataGetResult result;

    ASSERT_TRUE(transporter.Get({ "key", 8 }, result).IsOk());
    ASSERT_EQ(rpcClient->getObjectRequests.size(), 1u);
    EXPECT_FALSE(rpcClient->getObjectRequests[0].has_urma_info());
    EXPECT_EQ(rpcClient->getObjectRequests[0].read_size(), 0u);
    EXPECT_EQ(result.kind, AccessTransportKind::TCP);
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
    EXPECT_EQ(rpcClient->getObjectRequests[0].read_size(), 4u);
    EXPECT_EQ(rpcClient->getObjectRequests[0].data_size(), 4u);
    EXPECT_EQ(rpcClient->getObjectRequests[1].read_size(), 8u);
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

TEST(SetRequestBuilderTest, PreservesIdentityTenantAndWriteOptions)
{
    TransportCreateParam createParam = MakeCreateParam();
    createParam.cacheType = CacheType::DISK;
    CreateReqPb createRequest;
    ASSERT_TRUE(BuildCreateRequest("request-key", 64, createParam, createRequest).IsOk());
    EXPECT_EQ(createRequest.client_id(), "client-1");
    EXPECT_EQ(createRequest.token(), "token-1");
    EXPECT_EQ(createRequest.tenant_id(), "tenant-1");
    EXPECT_TRUE(createRequest.is_routed());

    ObjectBufferInfo info;
    info.objectKey = "request-key";
    info.dataSize = 64;
    info.metadataSize = 0;
    info.objectMode = ModeInfo(ConsistencyType::PRAM, WriteMode::WRITE_THROUGH_L2_CACHE, CacheType::DISK);
    TransportSetParam setParam = MakeSetParam();
    setParam.ttlSecond = 60;
    PublishReqPb setRequest;
    Status rc = BuildSetRequest(info, setParam, setRequest);
    ASSERT_TRUE(rc.IsOk()) << rc.ToString();
    EXPECT_EQ(setRequest.client_id(), "client-1");
    EXPECT_EQ(setRequest.token(), "token-1");
    EXPECT_EQ(setRequest.tenant_id(), "tenant-1");
    EXPECT_TRUE(setRequest.is_routed());
    EXPECT_EQ(setRequest.write_mode(), static_cast<uint32_t>(WriteMode::WRITE_THROUGH_L2_CACHE));
}

TEST(TcpTransporterTest, CreateAllocatesBuffer)
{
    auto rpcClient = std::make_shared<FakeWorkerRpcClient>();
    TcpTransporter transporter(rpcClient);
    std::shared_ptr<ObjectBuffer> buffer;

    TransportCreateParam param = MakeCreateParam();
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
    TransportCreateParam param = MakeCreateParam();
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
    TransportCreateParam createParam = MakeCreateParam();
    ASSERT_TRUE(transporter.Create(MakeAddress(9000), "set-key", 64, createParam, buffer).IsOk());

    // Write data
    const char data[] = "test payload";
    ASSERT_TRUE(buffer->MemoryCopy(data, sizeof(data)).IsOk());

    // Set
    TransportSetParam setParam = MakeSetParam();
    setParam.subTimeoutMs = 500;
    setParam.ttlSecond = 60;
    Status rc = transporter.Set(*buffer, setParam);
    ASSERT_TRUE(rc.IsOk()) << rc.ToString();
    EXPECT_EQ(rpcClient->setInvokeCount, 1);
    EXPECT_EQ(rpcClient->invokedSetPayloadSizes.size(), 1u);
    EXPECT_EQ(rpcClient->invokedSetPayloadSizes[0], 1u);  // one payload
    ASSERT_EQ(rpcClient->invokedSetRequests.size(), 1u);
    EXPECT_EQ(rpcClient->invokedSetRequests[0].client_id(), "client-1");
    EXPECT_EQ(rpcClient->invokedSetRequests[0].token(), "token-1");
    EXPECT_EQ(rpcClient->invokedSetRequests[0].tenant_id(), "tenant-1");
}

TEST(TcpTransporterTest, SetPropagatesRpcError)
{
    auto rpcClient = std::make_shared<FakeWorkerRpcClient>();
    rpcClient->setInvokeStatus = Status(K_RPC_DEADLINE_EXCEEDED, "timeout");
    TcpTransporter transporter(rpcClient);

    std::shared_ptr<ObjectBuffer> buffer;
    TransportCreateParam createParam = MakeCreateParam();
    ASSERT_TRUE(transporter.Create(MakeAddress(9000), "err-key", 64, createParam, buffer).IsOk());

    TransportSetParam setParam = MakeSetParam();
    EXPECT_EQ(transporter.Set(*buffer, setParam).GetCode(), K_RPC_DEADLINE_EXCEEDED);
    EXPECT_EQ(rpcClient->setInvokeCount, 1);
}

TEST(MSetRequestBuilderTest, BuildsMultiCreateAndAlignsMixedFallbackPayloads)
{
    MultiCreateReqPb createRequest;
    ASSERT_TRUE(BuildMultiCreateRequest({ "key-a", "key-b" }, { 4, 5 }, MakeCreateParam(), createRequest).IsOk());
    EXPECT_EQ(createRequest.client_id(), "client-1");
    EXPECT_EQ(createRequest.object_key_size(), 2);
    EXPECT_TRUE(createRequest.skip_check_existence());
    EXPECT_TRUE(createRequest.is_routed());

    const HostPort workerAddr = MakeAddress(9000);
    auto ubBuffer = MakeTransportBuffer(workerAddr, "ub-key", "urma", "shm-ub", true);
    auto fallbackBuffer = MakeTransportBuffer(workerAddr, "fallback-key", "tcp", "shm-fallback", true);
    ASSERT_NE(ubBuffer, nullptr);
    ASSERT_NE(fallbackBuffer, nullptr);
    MultiPublishReqPb publishRequest;
    std::vector<MemView> payloads;
    ASSERT_TRUE(BuildMultiPublishRequest({ ubBuffer, fallbackBuffer }, { false, true }, MakeSetParam(),
                                         publishRequest, payloads).IsOk());
    EXPECT_TRUE(publishRequest.is_routed());
    ASSERT_EQ(publishRequest.object_info_size(), 2);
    EXPECT_EQ(publishRequest.object_info(0).object_key(), "fallback-key");
    EXPECT_TRUE(publishRequest.object_info(0).shm_id().empty());
    EXPECT_EQ(publishRequest.object_info(1).object_key(), "ub-key");
    EXPECT_EQ(publishRequest.object_info(1).shm_id(), "shm-ub");
    ASSERT_EQ(payloads.size(), 1u);
    EXPECT_EQ(std::string(static_cast<const char *>(payloads[0].Data()), payloads[0].Size()), "tcp");
}

TEST(MSetRequestBuilderTest, PreservesPartialFailureWithoutFailingWholeBatch)
{
    MultiPublishRspPb response;
    response.add_failed_object_keys("key-b");
    response.mutable_last_rc()->set_error_code(K_OUT_OF_MEMORY);
    response.mutable_last_rc()->set_error_msg("allocation failed");
    TransportMSetResult result;

    EXPECT_TRUE(SetMSetResponseResult(response, 2, AccessTransportKind::UB, result).IsOk());
    ASSERT_EQ(result.failedKeys.size(), 1u);
    EXPECT_EQ(result.failedKeys[0], "key-b");
    EXPECT_EQ(result.lastRc.GetCode(), K_OUT_OF_MEMORY);
    EXPECT_EQ(result.actualKind, AccessTransportKind::UB);
    EXPECT_EQ(SetMSetResponseResult(response, 1, AccessTransportKind::UB, result).GetCode(), K_OUT_OF_MEMORY);
}

TEST(MSetRequestBuilderTest, RejectsInvalidBatchInvariants)
{
    const HostPort worker = MakeAddress(9000);
    auto first = MakeTransportBuffer(worker, "key-a", "data", "shm-a");
    auto duplicate = MakeTransportBuffer(worker, "key-a", "more", "shm-b");
    auto remote = MakeTransportBuffer(MakeAddress(9001), "key-b", "data", "shm-c");
    auto differentMode = MakeTransportBuffer(worker, "key-c", "data", "shm-d");
    ASSERT_NE(first, nullptr);
    ASSERT_NE(duplicate, nullptr);
    ASSERT_NE(remote, nullptr);
    ASSERT_NE(differentMode, nullptr);
    ObjectBufferInternal::GetMutableInfo(*differentMode).objectMode =
        ModeInfo(ConsistencyType::PRAM, WriteMode::WRITE_BACK_L2_CACHE, CacheType::DISK);

    EXPECT_EQ(ValidateMSetRequest({}, MakeSetParam()).GetCode(), K_INVALID);
    EXPECT_EQ(ValidateMSetRequest({ first, duplicate }, MakeSetParam()).GetCode(), K_INVALID);
    EXPECT_EQ(ValidateMSetRequest({ first, remote }, MakeSetParam()).GetCode(), K_INVALID);
    EXPECT_EQ(ValidateMSetRequest({ first, differentMode }, MakeSetParam()).GetCode(), K_INVALID);

    MultiPublishReqPb request;
    std::vector<MemView> payloads;
    EXPECT_EQ(BuildMultiPublishRequest({ first, remote }, { true }, MakeSetParam(), request, payloads).GetCode(),
              K_INVALID);
}

TEST(MSetRequestBuilderTest, RejectsMalformedFailureResponses)
{
    MultiPublishRspPb response;
    response.add_failed_object_keys("key-a");
    response.add_failed_object_keys("key-b");
    TransportMSetResult result;
    EXPECT_EQ(SetMSetResponseResult(response, 1, AccessTransportKind::UB, result).GetCode(), K_RUNTIME_ERROR);

    response.Clear();
    response.add_failed_object_keys("key-a");
    EXPECT_EQ(SetMSetResponseResult(response, 1, AccessTransportKind::UB, result).GetCode(), K_RUNTIME_ERROR);

    response.Clear();
    response.mutable_last_rc()->set_error_code(K_OUT_OF_MEMORY);
    response.mutable_last_rc()->set_error_msg("master failed before reporting per-key failures");
    EXPECT_EQ(SetMSetResponseResult(response, 1, AccessTransportKind::UB, result).GetCode(), K_OUT_OF_MEMORY);
    EXPECT_EQ(result.actualKind, AccessTransportKind::UNKNOWN);
}

TEST(TcpTransporterTest, MCreateAndMSetUseOneMultiPublishRpc)
{
    auto rpcClient = std::make_shared<FakeWorkerRpcClient>();
    TcpTransporter transporter(rpcClient);
    std::vector<std::shared_ptr<ObjectBuffer>> buffers;
    ASSERT_TRUE(transporter.MCreate(MakeAddress(9000), { "key-a", "key-b" }, { 4, 5 }, MakeCreateParam(),
                                    buffers).IsOk());
    ASSERT_EQ(buffers.size(), 2u);
    ASSERT_TRUE(buffers[0]->MemoryCopy("data", 4).IsOk());
    ASSERT_TRUE(buffers[1]->MemoryCopy("value", 5).IsOk());

    TransportMSetResult result;
    ASSERT_TRUE(transporter.MSet(buffers, MakeSetParam(), result).IsOk());
    EXPECT_EQ(rpcClient->multiSetInvokeCount, 1);
    ASSERT_EQ(rpcClient->invokedMultiSetRequests.size(), 1u);
    EXPECT_EQ(rpcClient->invokedMultiSetRequests[0].object_info_size(), 2);
    ASSERT_EQ(rpcClient->invokedMultiSetPayloadData.size(), 1u);
    ASSERT_EQ(rpcClient->invokedMultiSetPayloadData[0].size(), 2u);
    EXPECT_EQ(rpcClient->invokedMultiSetPayloadData[0][0], "data");
    EXPECT_EQ(rpcClient->invokedMultiSetPayloadData[0][1], "value");
    EXPECT_EQ(result.actualKind, AccessTransportKind::TCP);
}

TEST(TcpTransporterTest, MSetMarksWorkerErrorAsAttemptedPublish)
{
    auto rpcClient = std::make_shared<FakeWorkerRpcClient>();
    rpcClient->multiSetLastCode = K_RPC_UNAVAILABLE;
    rpcClient->multiSetLastMessage = "worker returned connection error";
    TcpTransporter transporter(rpcClient);
    std::vector<std::shared_ptr<ObjectBuffer>> buffers;
    ASSERT_TRUE(transporter.MCreate(MakeAddress(9000), { "key-a" }, { 4 }, MakeCreateParam(), buffers).IsOk());
    ASSERT_EQ(buffers.size(), 1u);
    ASSERT_TRUE(buffers[0]->MemoryCopy("data", 4).IsOk());

    TransportMSetResult result;
    EXPECT_EQ(transporter.MSet(buffers, MakeSetParam(), result).GetCode(), K_RPC_UNAVAILABLE);
    EXPECT_TRUE(result.publishAttempted);
    EXPECT_EQ(rpcClient->multiSetInvokeCount, 1);
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

    ASSERT_TRUE(transporter.Set(*buffer, MakeSetParam()).IsOk());
    EXPECT_EQ(transporter.writeCount, 1);
    ASSERT_EQ(rpcClient->invokedSetPayloadSizes.size(), 1u);
    EXPECT_EQ(rpcClient->invokedSetPayloadSizes[0], 0u);
}

TEST(UbTransporterTest, MCreateUsesOneMultiCreateRpc)
{
    auto rpcClient = std::make_shared<FakeWorkerRpcClient>();
    rpcClient->createResponseHasUrmaInfo = true;
    TestUbTransporter transporter(rpcClient, std::make_shared<FakeUbConnection>());
    std::vector<std::shared_ptr<ObjectBuffer>> buffers;

    ASSERT_TRUE(transporter.MCreate(MakeAddress(9000), { "key-a", "key-b" }, { 4, 8 }, MakeCreateParam(),
                                    buffers).IsOk());
    EXPECT_EQ(rpcClient->multiCreateInvokeCount, 1);
    ASSERT_EQ(rpcClient->invokedMultiCreateRequests.size(), 1u);
    EXPECT_EQ(rpcClient->invokedMultiCreateRequests[0].object_key_size(), 2);
    EXPECT_EQ(transporter.buildMCreateBufferCount, 2);
    ASSERT_EQ(buffers.size(), 2u);
    EXPECT_EQ(buffers[0]->GetSize(), 4);
    EXPECT_EQ(buffers[1]->GetSize(), 8);
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

    ASSERT_TRUE(transporter.Set(*buffer, MakeSetParam()).IsOk());
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

    EXPECT_EQ(transporter.Set(*buffer, MakeSetParam()).GetCode(), K_URMA_NEED_CONNECT);
    EXPECT_EQ(rpcClient->setInvokeCount, 0);
}

TEST(UbTransporterTest, MSetUsesUbAndPositionalTcpFallbackInOneRpc)
{
    auto rpcClient = std::make_shared<FakeWorkerRpcClient>();
    TestUbTransporter transporter(rpcClient, std::make_shared<FakeUbConnection>());
    transporter.writeStatuses = { Status::OK(), Status(K_URMA_ERROR, "fallback") };
    const HostPort workerAddr = MakeAddress(9000);
    auto ubBuffer = MakeTransportBuffer(workerAddr, "ub-key", "urma", "shm-ub", true);
    auto fallbackBuffer = MakeTransportBuffer(workerAddr, "fallback-key", "tcp", "shm-fallback", true);
    ASSERT_NE(ubBuffer, nullptr);
    ASSERT_NE(fallbackBuffer, nullptr);

    TransportMSetResult result;
    ASSERT_TRUE(transporter.MSet({ ubBuffer, fallbackBuffer }, MakeSetParam(), result).IsOk());
    EXPECT_EQ(transporter.writeBatchCount, 1);
    EXPECT_EQ(transporter.writeCount, 2);
    ASSERT_EQ(rpcClient->invokedMultiSetRequests.size(), 1u);
    const auto &request = rpcClient->invokedMultiSetRequests[0];
    EXPECT_EQ(request.object_info(0).object_key(), "fallback-key");
    EXPECT_TRUE(request.object_info(0).shm_id().empty());
    EXPECT_EQ(request.object_info(1).object_key(), "ub-key");
    EXPECT_EQ(request.object_info(1).shm_id(), "shm-ub");
    ASSERT_EQ(rpcClient->invokedMultiSetPayloadData[0].size(), 1u);
    EXPECT_EQ(rpcClient->invokedMultiSetPayloadData[0][0], "tcp");
    EXPECT_EQ(result.actualKind, AccessTransportKind::TCP);
}

TEST(UbTransporterTest, RejectedObjectFallbackDoesNotAbortSuccessfulObjects)
{
    auto rpcClient = std::make_shared<FakeWorkerRpcClient>();
    TestUbTransporter transporter(rpcClient, std::make_shared<FakeUbConnection>());
    transporter.writeStatuses = { Status::OK(), Status(K_URMA_ERROR, "large write failed") };
    const HostPort workerAddr = MakeAddress(9000);
    auto successBuffer = MakeTransportBuffer(workerAddr, "success-key", "data", "shm-success", true);
    const std::string largePayload(UrmaFallbackTcpLimiter::kMaxSinglePayloadBytes, 'x');
    auto rejectedBuffer = MakeTransportBuffer(workerAddr, "rejected-key", largePayload, "shm-rejected", true);
    ASSERT_NE(successBuffer, nullptr);
    ASSERT_NE(rejectedBuffer, nullptr);

    TransportMSetResult result;
    Status rc = transporter.MSet({ successBuffer, rejectedBuffer }, MakeSetParam(), result);

    EXPECT_TRUE(rc.IsOk()) << rc.ToString();
    ASSERT_EQ(result.failedKeys.size(), 1u);
    EXPECT_EQ(result.failedKeys[0], "rejected-key");
    EXPECT_EQ(result.lastRc.GetCode(), K_URMA_ERROR);
    EXPECT_EQ(result.actualKind, AccessTransportKind::UB);
    ASSERT_EQ(rpcClient->invokedMultiSetRequests.size(), 1u);
    EXPECT_EQ(rpcClient->invokedMultiSetRequests[0].object_info_size(), 1);
    EXPECT_EQ(rpcClient->invokedMultiSetRequests[0].object_info(0).object_key(), "success-key");
}

TEST(UbTransporterTest, PreservesLocalFailureWhenWorkerAlsoReportsPartialFailure)
{
    auto rpcClient = std::make_shared<FakeWorkerRpcClient>();
    rpcClient->multiSetFailedKeys = { "worker-failed-key" };
    rpcClient->multiSetLastCode = K_OUT_OF_MEMORY;
    rpcClient->multiSetLastMessage = "worker rejected object";
    TestUbTransporter transporter(rpcClient, std::make_shared<FakeUbConnection>());
    transporter.writeStatuses = {
        Status(K_URMA_ERROR, "local UB write failed"), Status::OK(), Status::OK()
    };
    const HostPort workerAddr = MakeAddress(9000);
    const std::string largePayload(UrmaFallbackTcpLimiter::kMaxSinglePayloadBytes, 'x');
    auto localFailed = MakeTransportBuffer(workerAddr, "local-failed-key", largePayload, "shm-local", true);
    auto workerFailed = MakeTransportBuffer(workerAddr, "worker-failed-key", "data", "shm-worker", true);
    auto success = MakeTransportBuffer(workerAddr, "success-key", "data", "shm-success", true);
    ASSERT_NE(localFailed, nullptr);
    ASSERT_NE(workerFailed, nullptr);
    ASSERT_NE(success, nullptr);

    TransportMSetResult result;
    Status rc = transporter.MSet({ localFailed, workerFailed, success }, MakeSetParam(), result);

    EXPECT_TRUE(rc.IsOk()) << rc.ToString();
    ASSERT_EQ(result.failedKeys.size(), 2u);
    EXPECT_EQ(result.failedKeys[0], "local-failed-key");
    EXPECT_EQ(result.failedKeys[1], "worker-failed-key");
    EXPECT_EQ(result.lastRc.GetCode(), K_URMA_ERROR);
    EXPECT_EQ(result.actualKind, AccessTransportKind::UB);
}

TEST(UbTransporterTest, MSetWritesMoreThanOnePipelineBatch)
{
    auto rpcClient = std::make_shared<FakeWorkerRpcClient>();
    TestUbTransporter transporter(rpcClient, std::make_shared<FakeUbConnection>());
    auto buffers = MakeTransportBuffers(MakeAddress(9000), 33);
    ASSERT_EQ(buffers.size(), 33u);

    TransportMSetResult result;
    ASSERT_TRUE(transporter.MSet(buffers, MakeSetParam(), result).IsOk());

    EXPECT_EQ(transporter.writeBatchCount, 2);
    EXPECT_EQ(transporter.writeCount, 33);
    EXPECT_EQ(transporter.waitCount, 33);
    EXPECT_EQ(rpcClient->multiSetInvokeCount, 1);
    for (const auto &buffer : buffers) {
        EXPECT_TRUE(ObjectBufferInternal::GetInfo(*buffer).ubDataSentByMemoryCopy);
    }
}

TEST(UbTransporterTest, MSetPreservesCompletedBatchWhenConnectionDiesBetweenBatches)
{
    auto rpcClient = std::make_shared<FakeWorkerRpcClient>();
    auto connection = std::make_shared<FakeUbConnection>();
    TestUbTransporter transporter(rpcClient, connection);
    transporter.afterWait = [connection](int waitCount) {
        if (waitCount == 32) {
            connection->alive.store(false);
        }
    };
    auto buffers = MakeTransportBuffers(MakeAddress(9000), 33);
    ASSERT_EQ(buffers.size(), 33u);

    TransportMSetResult result;
    Status rc = transporter.MSet(buffers, MakeSetParam(), result);

    EXPECT_EQ(rc.GetCode(), K_URMA_NEED_CONNECT);
    EXPECT_NE(rc.ToString().find("completed=32/33"), std::string::npos);
    EXPECT_EQ(transporter.writeBatchCount, 1);
    EXPECT_EQ(transporter.writeCount, 32);
    EXPECT_EQ(transporter.waitCount, 32);
    EXPECT_EQ(rpcClient->multiSetInvokeCount, 0);
    for (size_t i = 0; i < 32; ++i) {
        EXPECT_TRUE(ObjectBufferInternal::GetInfo(*buffers[i]).ubDataSentByMemoryCopy);
    }
    EXPECT_FALSE(ObjectBufferInternal::GetInfo(*buffers.back()).ubDataSentByMemoryCopy);
}

TEST(UbTransporterTest, PublishFailureMarksEverySubmittedObjectFailed)
{
    auto rpcClient = std::make_shared<FakeWorkerRpcClient>();
    rpcClient->multiSetLastCode = K_OUT_OF_MEMORY;
    rpcClient->multiSetLastMessage = "master rejected the batch";
    TestUbTransporter transporter(rpcClient, std::make_shared<FakeUbConnection>());
    const HostPort workerAddr = MakeAddress(9000);
    auto firstBuffer = MakeTransportBuffer(workerAddr, "key-a", "data", "shm-a", true);
    auto secondBuffer = MakeTransportBuffer(workerAddr, "key-b", "more", "shm-b", true);
    ASSERT_NE(firstBuffer, nullptr);
    ASSERT_NE(secondBuffer, nullptr);

    TransportMSetResult result;
    Status rc = transporter.MSet({ firstBuffer, secondBuffer }, MakeSetParam(), result);

    EXPECT_EQ(rc.GetCode(), K_OUT_OF_MEMORY);
    EXPECT_EQ(result.lastRc.GetCode(), K_OUT_OF_MEMORY);
    EXPECT_EQ(result.actualKind, AccessTransportKind::UNKNOWN);
    ASSERT_EQ(result.failedKeys.size(), 2u);
    EXPECT_EQ(result.failedKeys[0], "key-a");
    EXPECT_EQ(result.failedKeys[1], "key-b");
}

TEST(UbTransporterTest, PublishRpcFailureMarksEverySubmittedObjectFailed)
{
    auto rpcClient = std::make_shared<FakeWorkerRpcClient>();
    rpcClient->multiSetInvokeStatus = Status(K_RPC_UNAVAILABLE, "response lost");
    TestUbTransporter transporter(rpcClient, std::make_shared<FakeUbConnection>());
    const HostPort workerAddr = MakeAddress(9000);
    auto firstBuffer = MakeTransportBuffer(workerAddr, "key-a", "data", "shm-a", true);
    auto secondBuffer = MakeTransportBuffer(workerAddr, "key-b", "more", "shm-b", true);
    ASSERT_NE(firstBuffer, nullptr);
    ASSERT_NE(secondBuffer, nullptr);

    TransportMSetResult result;
    Status rc = transporter.MSet({ firstBuffer, secondBuffer }, MakeSetParam(), result);

    EXPECT_EQ(rc.GetCode(), K_RPC_UNAVAILABLE);
    EXPECT_EQ(result.lastRc.GetCode(), K_RPC_UNAVAILABLE);
    EXPECT_EQ(result.actualKind, AccessTransportKind::UNKNOWN);
    ASSERT_EQ(result.failedKeys.size(), 2u);
    EXPECT_EQ(result.failedKeys[0], "key-a");
    EXPECT_EQ(result.failedKeys[1], "key-b");
}

TEST(UbTransporterTest, MSetDeadConnectionReturnsReconnectStatus)
{
    auto rpcClient = std::make_shared<FakeWorkerRpcClient>();
    auto connection = std::make_shared<FakeUbConnection>();
    connection->alive.store(false);
    TestUbTransporter transporter(rpcClient, connection);
    const HostPort workerAddr = MakeAddress(9000);
    auto firstBuffer = MakeTransportBuffer(workerAddr, "key-a", "data", "shm-a", true);
    auto secondBuffer = MakeTransportBuffer(workerAddr, "key-b", "more", "shm-b", true);
    ASSERT_NE(firstBuffer, nullptr);
    ASSERT_NE(secondBuffer, nullptr);

    TransportMSetResult result;
    Status rc = transporter.MSet({ firstBuffer, secondBuffer }, MakeSetParam(), result);

    EXPECT_EQ(rc.GetCode(), K_URMA_NEED_CONNECT);
    EXPECT_EQ(transporter.writeCount, 0);
    EXPECT_EQ(rpcClient->multiSetInvokeCount, 0);
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
    std::thread setThread([&]() { setStatus = transporter.Set(*buffer, MakeSetParam()); });
    invokeStartedFuture.wait();
    std::thread closeThread([&]() { transporter.CloseDataPlane(); });
    allowInvoke.set_value();
    setThread.join();
    closeThread.join();
    EXPECT_TRUE(setStatus.IsOk());
    EXPECT_FALSE(connection->teardownDuringInvoke.load());
}

// --- TransportLayer Create/Set tests ---

TEST(TransportLayerTest, WorkerSnapshotCleanupIsAsyncAndCoalescesToLatest)
{
    auto manager = std::make_shared<FakeDataPlaneManager>();
    TestTransportLayer layer(manager);
    ASSERT_TRUE(layer.Init().IsOk());

    const HostPort blocker = MakeAddress(27);
    const HostPort survivor = MakeAddress(28);
    const HostPort marker = MakeAddress(29);
    std::shared_ptr<IDataTransporter> transporter;
    ASSERT_TRUE(manager->GetOrCreate(blocker, TransportHint::TCP_ONLY, transporter).IsOk());
    auto blockerTransporter = std::dynamic_pointer_cast<FakeTransporter>(transporter);
    ASSERT_TRUE(manager->GetOrCreate(survivor, TransportHint::TCP_ONLY, transporter).IsOk());
    auto survivorTransporter = std::dynamic_pointer_cast<FakeTransporter>(transporter);
    ASSERT_TRUE(manager->GetOrCreate(marker, TransportHint::TCP_ONLY, transporter).IsOk());
    auto markerTransporter = std::dynamic_pointer_cast<FakeTransporter>(transporter);
    ASSERT_NE(blockerTransporter, nullptr);
    ASSERT_NE(survivorTransporter, nullptr);
    ASSERT_NE(markerTransporter, nullptr);

    std::promise<void> blockerCloseStarted;
    auto blockerCloseStartedFuture = blockerCloseStarted.get_future();
    std::promise<void> allowBlockerClose;
    auto allowBlockerCloseFuture = allowBlockerClose.get_future().share();
    blockerTransporter->onClose = [&blockerCloseStarted, allowBlockerCloseFuture]() {
        blockerCloseStarted.set_value();
        allowBlockerCloseFuture.wait();
    };
    std::promise<void> markerClosed;
    auto markerClosedFuture = markerClosed.get_future();
    markerTransporter->onClose = [&markerClosed]() { markerClosed.set_value(); };

    WorkerSnapshot first;
    first.ringVersion = 1;
    first.otherAddrs = { survivor, marker };
    ASSERT_TRUE(layer.ApplyWorkerSnapshot(first).IsOk());
    if (blockerCloseStartedFuture.wait_for(std::chrono::seconds(2)) != std::future_status::ready) {
        allowBlockerClose.set_value();
        layer.Shutdown();
        FAIL() << "First asynchronous transport reconciliation did not start";
    }

    WorkerSnapshot superseded;
    superseded.ringVersion = 2;
    ASSERT_TRUE(layer.ApplyWorkerSnapshot(superseded).IsOk());
    WorkerSnapshot latest;
    latest.ringVersion = 3;
    latest.otherAddrs = { survivor };
    ASSERT_TRUE(layer.ApplyWorkerSnapshot(latest).IsOk());
    allowBlockerClose.set_value();
    ASSERT_EQ(markerClosedFuture.wait_for(std::chrono::seconds(2)), std::future_status::ready);

    EXPECT_EQ(blockerTransporter->closeCount, 1);
    EXPECT_EQ(markerTransporter->closeCount, 1);
    EXPECT_EQ(survivorTransporter->closeCount, 0);
    ASSERT_TRUE(manager->GetOrCreate(survivor, TransportHint::TCP_ONLY, transporter).IsOk());
    EXPECT_EQ(transporter, survivorTransporter);
    layer.Shutdown();
    EXPECT_EQ(layer.ApplyWorkerSnapshot(latest).GetCode(), K_NOT_READY);
}

TEST(TransportLayerTest, CreateDelegatesToTransporter)
{
    auto manager = std::make_shared<FakeDataPlaneManager>();
    TestTransportLayer layer(manager);

    TransportCreateParam param = MakeCreateParam();
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
    TransportCreateParam param = MakeCreateParam();
    std::shared_ptr<ObjectBuffer> buffer;

    EXPECT_EQ(layer.Create(MakeAddress(30), "", 256, param, buffer).GetCode(), K_INVALID);
    EXPECT_EQ(layer.Create(MakeAddress(30), "key", UINT64_MAX, param, buffer).GetCode(), K_INVALID);
    EXPECT_EQ(manager->transportBuildCount, 0);
}

TEST(TransportLayerTest, SuccessfulSetReleasesAllocationOnce)
{
    auto manager = std::make_shared<FakeDataPlaneManager>();
    TestTransportLayer layer(manager);
    std::shared_ptr<ObjectBuffer> buffer;
    ASSERT_TRUE(layer.Create(MakeAddress(30), "release-key", 64, MakeCreateParam(), buffer).IsOk());

    ASSERT_TRUE(layer.Set(*buffer, MakeSetParam()).IsOk());
    ASSERT_NE(manager->lastTransporter, nullptr);
    EXPECT_EQ(manager->lastTransporter->releaseCount, 1);
    ASSERT_EQ(manager->lastTransporter->releaseContexts.size(), 1u);
    EXPECT_EQ(manager->lastTransporter->releaseContexts[0].clientId, "client-1");
}

TEST(TransportLayerTest, SetRetryOnUrmaNeedConnect)
{
    auto manager = std::make_shared<FakeDataPlaneManager>();
    manager->transporterSetStatuses = {
        { Status(K_URMA_NEED_CONNECT, "reconnect") }, { Status::OK() }
    };
    TestTransportLayer layer(manager);

    // Create first
    TransportCreateParam createParam = MakeCreateParam();
    std::shared_ptr<ObjectBuffer> buffer;
    ASSERT_TRUE(layer.Create(MakeAddress(31), "retry-key", 64, createParam, buffer).IsOk());

    TransportSetParam setParam = MakeSetParam();
    Status rc = layer.Set(*buffer, setParam);
    EXPECT_TRUE(rc.IsOk()) << rc.ToString();
    EXPECT_GE(manager->rpcBuildCount, 1);
    EXPECT_GE(manager->transportBuildCount, 2);
    ASSERT_GE(manager->builtTransporters.size(), 2u);
    ASSERT_EQ(manager->builtTransporters[1]->setParams.size(), 1u);
    EXPECT_TRUE(manager->builtTransporters[1]->setParams[0].isRetry);
    int releaseCount = 0;
    for (const auto &transporter : manager->builtTransporters) {
        releaseCount += transporter->releaseCount;
    }
    EXPECT_EQ(releaseCount, 1);
}

TEST(TransportLayerTest, SetRetryOnRpcUnavailable)
{
    auto manager = std::make_shared<FakeDataPlaneManager>();
    manager->transporterSetStatuses = {
        { Status(K_RPC_UNAVAILABLE, "unavailable") }, { Status::OK() }
    };
    TestTransportLayer layer(manager);

    TransportCreateParam createParam = MakeCreateParam();
    std::shared_ptr<ObjectBuffer> buffer;
    ASSERT_TRUE(layer.Create(MakeAddress(32), "rpc-retry-key", 64, createParam, buffer).IsOk());

    TransportSetParam setParam = MakeSetParam();
    Status rc = layer.Set(*buffer, setParam);
    EXPECT_TRUE(rc.IsOk()) << rc.ToString();
    EXPECT_EQ(manager->rpcBuildCount, 2);       // RPC client rebuilt
    EXPECT_EQ(manager->transportBuildCount, 2);  // transporter rebuilt once
    int releaseCount = 0;
    for (const auto &transporter : manager->builtTransporters) {
        releaseCount += transporter->releaseCount;
    }
    EXPECT_EQ(releaseCount, 1);
}

TEST(TransportLayerTest, SetDoesNotRetrySecondFailure)
{
    auto manager = std::make_shared<FakeDataPlaneManager>();
    // Inject two failing statuses: first triggers rebuild, second (retry) also fails.
    manager->transporterSetStatuses = {
        { Status(K_URMA_NEED_CONNECT, "first") }, { Status(K_URMA_NEED_CONNECT, "second") }
    };
    TestTransportLayer layer(manager);

    TransportCreateParam createParam = MakeCreateParam();
    std::shared_ptr<ObjectBuffer> buffer;
    ASSERT_TRUE(layer.Create(MakeAddress(33), "no-retry-key", 64, createParam, buffer).IsOk());

    TransportSetParam setParam = MakeSetParam();
    Status rc = layer.Set(*buffer, setParam);
    EXPECT_EQ(rc.GetCode(), K_URMA_NEED_CONNECT) << rc.ToString();
    EXPECT_GE(manager->transportBuildCount, 2);
    int releaseCount = 0;
    for (const auto &transporter : manager->builtTransporters) {
        releaseCount += transporter->releaseCount;
    }
    EXPECT_EQ(releaseCount, 1);
}

TEST(TransportLayerTest, MCreateDoesNotReplayAmbiguousRpcFailure)
{
    auto manager = std::make_shared<FakeDataPlaneManager>();
    manager->transporterMCreateStatuses = { { Status(K_RPC_UNAVAILABLE, "response lost") } };
    TestTransportLayer layer(manager);
    std::vector<std::shared_ptr<ObjectBuffer>> buffers;

    EXPECT_EQ(layer.MCreate(MakeAddress(40), { "key-a", "key-b" }, { 4, 4 }, MakeCreateParam(), buffers).GetCode(),
              K_RPC_UNAVAILABLE);
    EXPECT_TRUE(buffers.empty());
    EXPECT_EQ(manager->transportBuildCount, 1);
    ASSERT_EQ(manager->builtTransporters.size(), 1u);
    EXPECT_EQ(manager->builtTransporters[0]->mCreateCount, 1);
}

TEST(TransportLayerTest, MSetDoesNotReplayAmbiguousRpcFailure)
{
    auto manager = std::make_shared<FakeDataPlaneManager>();
    manager->transporterMSetStatuses = { { Status(K_RPC_UNAVAILABLE, "response lost") } };
    TestTransportLayer layer(manager);
    std::vector<std::shared_ptr<ObjectBuffer>> buffers;
    ASSERT_TRUE(layer.MCreate(MakeAddress(40), { "key-a", "key-b" }, { 4, 4 }, MakeCreateParam(), buffers).IsOk());

    TransportMSetResult result;
    EXPECT_EQ(layer.MSet(buffers, MakeSetParam(), result).GetCode(), K_RPC_UNAVAILABLE);
    ASSERT_GE(manager->builtTransporters.size(), 2u);
    EXPECT_EQ(manager->builtTransporters[0]->mSetCount, 1);
    EXPECT_EQ(manager->builtTransporters[1]->mSetCount, 0);
    EXPECT_EQ(manager->builtTransporters[1]->releaseCount, 2);
}

TEST(TransportLayerTest, MSetRetriesRpcFailureBeforePublish)
{
    auto manager = std::make_shared<FakeDataPlaneManager>();
    manager->transporterMSetStatuses = {
        { Status(K_RPC_UNAVAILABLE, "not sent") }, { Status::OK() }
    };
    manager->transporterMSetPublishAttempted = { false, true };
    TestTransportLayer layer(manager);
    std::vector<std::shared_ptr<ObjectBuffer>> buffers;
    ASSERT_TRUE(layer.MCreate(MakeAddress(40), { "key-a", "key-b" }, { 4, 4 }, MakeCreateParam(), buffers).IsOk());

    TransportMSetResult result;
    ASSERT_TRUE(layer.MSet(buffers, MakeSetParam(), result).IsOk());

    EXPECT_EQ(manager->transportBuildCount, 2);
    ASSERT_EQ(manager->builtTransporters.size(), 2u);
    EXPECT_EQ(manager->builtTransporters[0]->mSetCount, 1);
    EXPECT_EQ(manager->builtTransporters[1]->mSetCount, 1);
    EXPECT_EQ(manager->builtTransporters[1]->releaseCount, 2);
}

TEST(TransportLayerTest, MSetRetryOnUrmaNeedConnectRebuildsOnlyDataPlane)
{
    auto manager = std::make_shared<FakeDataPlaneManager>();
    manager->transporterMSetStatuses = {
        { Status(K_URMA_NEED_CONNECT, "reconnect") }, { Status::OK() }
    };
    TestTransportLayer layer(manager);
    std::vector<std::shared_ptr<ObjectBuffer>> buffers;
    ASSERT_TRUE(layer.MCreate(MakeAddress(41), { "key-a", "key-b" }, { 4, 4 }, MakeCreateParam(), buffers).IsOk());
    ASSERT_EQ(buffers.size(), 2u);
    for (const auto &buffer : buffers) {
        auto &info = ObjectBufferInternal::GetMutableInfo(*buffer);
        info.ubUrmaDataInfo = std::make_shared<UrmaRemoteAddrPb>();
        info.ubDataSentByMemoryCopy = true;
    }

    TransportMSetResult result;
    ASSERT_TRUE(layer.MSet(buffers, MakeSetParam(), result).IsOk());

    EXPECT_EQ(manager->rpcBuildCount, 1);
    EXPECT_EQ(manager->transportBuildCount, 2);
    ASSERT_EQ(manager->builtTransporters.size(), 2u);
    EXPECT_EQ(manager->builtTransporters[0]->mSetCount, 1);
    EXPECT_EQ(manager->builtTransporters[1]->mSetCount, 1);
    EXPECT_EQ(manager->builtTransporters[1]->releaseCount, 2);
    for (const auto &buffer : buffers) {
        EXPECT_TRUE(ObjectBufferInternal::GetInfo(*buffer).ubDataSentByMemoryCopy);
    }
}
}  // namespace
}  // namespace client
}  // namespace datasystem
