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

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <future>
#include <limits>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#define private public
#include "datasystem/client/object_cache/object_client_impl.h"
#undef private

#include "datasystem/client/transport/data_plane/data_plane_manager.h"
#include "datasystem/client/transport/data_plane/shm_transporter.h"
#include "datasystem/client/transport/data_plane/tcp_transporter.h"
#include "datasystem/client/transport/data_plane/ub_transporter.h"
#include "datasystem/client/transport/common/deadline_retry.h"
#include "datasystem/client/transport/data_plane/data_plane_executor.h"
#include "datasystem/client/transport/metadata/object_metadata_client.h"
#include "datasystem/client/transport/object_buffer_internal.h"
#include "datasystem/client/transport/object_read/object_read_flow.h"
#include "datasystem/client/transport/object_read/replica_reader.h"
#include "datasystem/client/transport/rpc/exist_request_builder.h"
#include "datasystem/client/transport/rpc/mset_request_builder.h"
#include "datasystem/client/transport/rpc/set_request_builder.h"
#include "datasystem/client/transport/rpc/worker_rpc_client.h"
#include "datasystem/client/transport/transport_layer.h"
#include "datasystem/common/ak_sk/signature.h"
#include "datasystem/common/flags/common_flags.h"
#include "datasystem/common/log/latency_phase.h"
#include "datasystem/common/metrics/kv_metrics.h"
#include "datasystem/common/metrics/metrics.h"
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

void InitBatchGetMetrics()
{
    const bool jsonLogMonitor = FLAGS_json_log_monitor;
    Raii restoreJsonLogMonitor([jsonLogMonitor]() { FLAGS_json_log_monitor = jsonLogMonitor; });
    FLAGS_json_log_monitor = false;
    metrics::ResetKvMetricsForTest();
    ASSERT_TRUE(metrics::InitKvMetrics().IsOk());
}

void ExpectMetricTotal(const std::string &name, uint64_t total)
{
    const std::string expected = "\"name\":\"" + name + "\",\"total\":" + std::to_string(total) + ",";
    EXPECT_NE(metrics::DumpSummaryForTest().find(expected), std::string::npos) << name;
}

void ExpectMetricAbsent(const std::string &name)
{
    const std::string expected = "\"name\":\"" + name + "\"";
    EXPECT_EQ(metrics::DumpSummaryForTest().find(expected), std::string::npos) << name;
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

master::ObjectLocationInfoPb MakeReplicaLocation(const std::string &key, uint64_t size,
                                                 const std::vector<HostPort> &addresses)
{
    master::ObjectLocationInfoPb location;
    location.set_object_key(key);
    location.set_object_size(size);
    for (const auto &address : addresses) {
        location.add_object_locations(address.ToString());
    }
    return location;
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

    Status InvokeBatchGetObject(BatchGetObjectRemoteReqPb &request, BatchGetObjectRemoteRspPb &response,
                                std::vector<RpcMessage> &rpcPayloads) override
    {
        ++batchGetObjectCount;
        batchGetObjectRequests.push_back(request);
        if (onBatchInvoke) {
            onBatchInvoke();
        }
        if (batchGetHandler) {
            Status rc = batchGetHandler(request, response, rpcPayloads);
            if (afterBatchInvoke) {
                afterBatchInvoke();
            }
            return rc;
        }
        for (const auto &itemResponse : batchGetObjectResponses) {
            *response.add_responses() = itemResponse;
        }
        for (const auto &payloadValue : batchGetObjectPayloadValues) {
            RpcMessage payload;
            RETURN_IF_NOT_OK(payload.CopyString(payloadValue));
            rpcPayloads.emplace_back(std::move(payload));
        }
        if (afterBatchInvoke) {
            afterBatchInvoke();
        }
        return batchGetObjectStatus;
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

    Status InvokeExist(int64_t, ExistReqPb &request, ExistRspPb &response) override
    {
        ++existInvokeCount;
        invokedExistRequests.push_back(request);
        if (existInvokeStatus.IsError()) {
            return existInvokeStatus;
        }
        for (int i = 0; i < request.object_keys_size(); ++i) {
            response.add_exists(true);
        }
        return Status::OK();
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
    int existInvokeCount = 0;
    Status getObjectStatus = Status::OK();
    Status existInvokeStatus = Status::OK();
    StatusCode getObjectResponseCode = K_OK;
    int64_t getObjectDataSize = 4;
    std::vector<GetObjectRemoteReqPb> getObjectRequests;
    std::vector<ExistReqPb> invokedExistRequests;
    int batchGetObjectCount = 0;
    Status batchGetObjectStatus = Status::OK();
    std::vector<BatchGetObjectRemoteReqPb> batchGetObjectRequests;
    std::vector<GetObjectRemoteRspPb> batchGetObjectResponses;
    std::vector<std::string> batchGetObjectPayloadValues;
    std::function<Status(BatchGetObjectRemoteReqPb &, BatchGetObjectRemoteRspPb &, std::vector<RpcMessage> &)>
        batchGetHandler;
    Status queryAndGetStatus = Status::OK();
    std::vector<master::QueryAndGetReqPb> queryAndGetRequests;
    std::function<Status(const HostPort &, const master::QueryAndGetReqPb &, master::QueryAndGetRspPb &,
                         std::vector<RpcMessage> &)>
        queryAndGetHandler;
    std::function<void()> onInvoke;
    std::function<void()> afterInvoke;
    std::function<void()> onBatchInvoke;
    std::function<void()> afterBatchInvoke;

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

    AuthBoundaryWorkerRpcClient(std::shared_ptr<Signature> signature, BrpcChannelConfig channelConfig)
        : WorkerRpcClient(MakeAddress(9001), std::move(signature), std::move(channelConfig))
    {
    }

    bool IsAlive() const override
    {
        return true;
    }

    int getObjectInvokeCount = 0;
    int batchGetObjectInvokeCount = 0;
    int metadataInvokeCount = 0;
    int existInvokeCount = 0;
    int hashRingInvokeCount = 0;
    int hashRingCount = 0;
    int dataRpcTimeout = 0;
    int batchGetRpcTimeout = 0;
    int metadataRpcTimeout = 0;
    int existRpcTimeout = 0;
    int hashRingRpcTimeout = 0;
    uint64_t hashRingVersion = 0;
    GetObjectRemoteReqPb invokedDataRequest;
    BatchGetObjectRemoteReqPb invokedBatchGetRequest;
    master::QueryAndGetReqPb invokedMetadataRequest;
    ExistReqPb invokedExistRequest;
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
    Status batchGetInvokeStatus = Status::OK();
    std::vector<std::string> batchGetPayloadValues;

protected:
    Status DoInvokeGetObject(const RpcOptions &options, const GetObjectRemoteReqPb &request, GetObjectRemoteRspPb &,
                             std::vector<RpcMessage> &) override
    {
        ++getObjectInvokeCount;
        dataRpcTimeout = options.GetTimeout();
        invokedDataRequest = request;
        return Status::OK();
    }

    Status DoInvokeBatchGetObject(const RpcOptions &options, const BatchGetObjectRemoteReqPb &request,
                                  BatchGetObjectRemoteRspPb &response, std::vector<RpcMessage> &payloads) override
    {
        ++batchGetObjectInvokeCount;
        batchGetRpcTimeout = options.GetTimeout();
        invokedBatchGetRequest = request;
        for (int i = 0; i < request.requests_size(); ++i) {
            auto *itemResponse = response.add_responses();
            itemResponse->mutable_error()->set_error_code(K_OK);
            itemResponse->set_data_size(request.requests(i).data_size());
            itemResponse->set_data_source(DataTransferSource::DATA_IN_PAYLOAD);
        }
        for (const auto &value : batchGetPayloadValues) {
            RpcMessage payload;
            RETURN_IF_NOT_OK(payload.CopyString(value));
            payloads.emplace_back(std::move(payload));
        }
        return batchGetInvokeStatus;
    }

    Status DoInvokeQueryAndGet(const RpcOptions &options, const master::QueryAndGetReqPb &request,
                               master::QueryAndGetRspPb &, std::vector<RpcMessage> &) override
    {
        ++metadataInvokeCount;
        metadataRpcTimeout = options.GetTimeout();
        invokedMetadataRequest = request;
        return Status::OK();
    }

    Status DoInvokeExist(const RpcOptions &options, const ExistReqPb &request, ExistRspPb &response) override
    {
        ++existInvokeCount;
        existRpcTimeout = options.GetTimeout();
        invokedExistRequest = request;
        for (int i = 0; i < request.object_keys_size(); ++i) {
            response.add_exists(true);
        }
        return Status::OK();
    }

    Status DoInvokeGetHashRing(const RpcOptions &options, const GetHashRingReqPb &request,
                               GetHashRingRspPb &response) override
    {
        ++hashRingInvokeCount;
        ++hashRingCount;
        hashRingRpcTimeout = options.GetTimeout();
        hashRingVersion = request.version();
        invokedHashRingRequest = request;
        response.set_version(request.version() + 1);
        response.set_master_address("127.0.0.1:18888");
        response.set_hash_ring_changed(true);
        auto &worker = (*response.mutable_hash_ring()->mutable_members())["127.0.0.1:18481"];
        worker.set_state(MembershipPb_StatePb_ACTIVE);
        worker.add_tokens(1);
        (*response.mutable_host_id_map())["127.0.0.1:18481"] = "host-a";
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

    Status Get(const DataGetRequest &input, DataGetResult &output) override
    {
        ++getCount;
        getRequests.push_back(input);
        if (getHandler) {
            return getHandler(input, output);
        }
        if (!getStatuses.empty()) {
            Status rc = getStatuses.front();
            getStatuses.erase(getStatuses.begin());
            return rc;
        }
        return Status::OK();
    }

    Status BatchGet(const DataGetBatchRequest &inputs, DataGetBatchResult &outputs) override
    {
        ++batchGetCount;
        batchGetRequests.push_back(inputs);
        outputs.clear();
        if (batchGetHandler) {
            return batchGetHandler(inputs, outputs);
        }
        if (!batchGetStatuses.empty()) {
            Status rc = batchGetStatuses.front();
            batchGetStatuses.erase(batchGetStatuses.begin());
            return rc;
        }
        outputs.resize(inputs.size());
        for (auto &output : outputs) {
            output.status = Status::OK();
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
    int getCount = 0;
    std::vector<DataGetRequest> getRequests;
    std::function<Status(const DataGetRequest &, DataGetResult &)> getHandler;
    std::vector<Status> getStatuses;
    int batchGetCount = 0;
    std::vector<DataGetBatchRequest> batchGetRequests;
    std::vector<Status> batchGetStatuses;
    std::function<Status(const DataGetBatchRequest &, DataGetBatchResult &)> batchGetHandler;
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
        std::lock_guard<std::mutex> lock(mutex);
        ++rpcBuildCount;
        auto client = std::make_shared<FakeWorkerRpcClient>(address);
        client->queryAndGetHandler = queryAndGetHandler;
        if (!existInvokeStatuses.empty()) {
            client->existInvokeStatus = existInvokeStatuses.front();
            existInvokeStatuses.erase(existInvokeStatuses.begin());
        }
        RETURN_IF_NOT_OK(client->Init());
        lastRpcClient = client;
        output = std::move(client);
        return Status::OK();
    }

    Status BuildTransporter(const HostPort &address, TransportHint hint,
                            const std::shared_ptr<WorkerRpcClient> &rpcClient,
                            std::shared_ptr<IDataTransporter> &output) override
    {
        std::lock_guard<std::mutex> lock(mutex);
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
        if (configureTransporter) {
            configureTransporter(address, *transporter);
        }
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
    std::vector<Status> existInvokeStatuses;
    std::vector<std::vector<Status>> transporterGetStatuses;
    std::vector<std::vector<Status>> transporterSetStatuses;
    std::vector<std::vector<Status>> transporterMCreateStatuses;
    std::vector<std::vector<Status>> transporterMSetStatuses;
    std::vector<bool> transporterMSetPublishAttempted;
    std::vector<std::shared_ptr<FakeTransporter>> builtTransporters;
    std::function<Status(const HostPort &, const master::QueryAndGetReqPb &, master::QueryAndGetRspPb &,
                         std::vector<RpcMessage> &)>
        queryAndGetHandler;
    std::function<void(const HostPort &, FakeTransporter &)> configureTransporter;
    std::mutex mutex;
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
    FakeReplicaReader() : ReplicaReader(nullptr, nullptr, nullptr)
    {
    }

    Status Read(const master::ObjectLocationInfoPb &location, ObjectReadItemResult &result) override
    {
        {
            std::lock_guard<std::mutex> lock(mutex);
            unaryKeys.push_back(location.object_key());
            threadIds.push_back(std::this_thread::get_id());
        }
        return FillResult(location, result);
    }

    Status ReadBatch(const ReplicaReadBatch &requests) override
    {
        std::vector<std::string> batch;
        batch.reserve(requests.size());
        for (const auto &request : requests) {
            batch.push_back(request.location->object_key());
        }
        {
            std::lock_guard<std::mutex> lock(mutex);
            batchKeys.emplace_back(std::move(batch));
            threadIds.push_back(std::this_thread::get_id());
        }

        Status firstError(K_NOT_FOUND, "Cannot get objects from worker");
        bool hasSuccess = false;
        for (const auto &request : requests) {
            Status status = FillResult(*request.location, *request.result);
            request.result->status = status;
            if (status.IsOk()) {
                hasSuccess = true;
            } else if (firstError.GetCode() == K_NOT_FOUND
                       && firstError.GetMsg() == "Cannot get objects from worker") {
                firstError = status;
            }
        }
        return hasSuccess ? Status::OK() : firstError;
    }

    Status FillResult(const master::ObjectLocationInfoPb &location, ObjectReadItemResult &result)
    {
        auto status = itemStatuses.find(location.object_key());
        if (status != itemStatuses.end() && status->second.IsError()) {
            return status->second;
        }
        result.objectKey = location.object_key();
        result.data.kind = location.object_key() == "tcp" ? AccessTransportKind::TCP : AccessTransportKind::UB;
        if (resultHandler) {
            resultHandler(location.object_key(), result);
        }
        return Status::OK();
    }

    std::mutex mutex;
    std::vector<std::string> unaryKeys;
    std::vector<std::vector<std::string>> batchKeys;
    std::vector<std::thread::id> threadIds;
    std::unordered_map<std::string, Status> itemStatuses;
    std::function<void(const std::string &, ObjectReadItemResult &)> resultHandler;
};

class ControlledReplicaReader : public ReplicaReader {
public:
    ControlledReplicaReader(std::shared_ptr<DataPlaneExecutor> executor, std::shared_ptr<ThreadPool> taskPool)
        : ReplicaReader(std::move(executor), std::make_shared<DeadlineRetry>(), std::move(taskPool))
    {
    }

    Status CheckDeadline() const override
    {
        ++deadlineCheckCount;
        if (!deadlineStatuses.empty()) {
            Status status = deadlineStatuses.front();
            deadlineStatuses.erase(deadlineStatuses.begin());
            return status;
        }
        return Status::OK();
    }

    Status Backoff(int64_t &) const override
    {
        ++backoffCount;
        return backoffStatus;
    }

    mutable int deadlineCheckCount = 0;
    mutable int backoffCount = 0;
    mutable std::vector<Status> deadlineStatuses;
    Status backoffStatus = Status::OK();
};

class FakeUbConnection : public UbConnection {
public:
    explicit FakeUbConnection(bool supportsPayloadOnlyClientBatchGet = true)
        : supportsPayloadOnlyClientBatchGet(supportsPayloadOnlyClientBatchGet)
    {
    }

    Status Establish(const HostPort &) override
    {
        alive = true;
        return Status::OK();
    }

    bool IsAlive() const override
    {
        return alive.load();
    }

    bool SupportsPayloadOnlyClientBatchGet() const override
    {
        return supportsPayloadOnlyClientBatchGet;
    }

    void Teardown() override
    {
        if (invokeFinished != nullptr && !invokeFinished->load()) {
            teardownDuringInvoke.store(true);
        }
        alive.store(false);
    }

    std::atomic<bool> alive{ true };
    bool supportsPayloadOnlyClientBatchGet;
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

TEST(UbConnectionTest, PayloadOnlyClientBatchGetCapabilityDefaultsToFalse)
{
    UbConnection connection;

    EXPECT_FALSE(connection.SupportsPayloadOnlyClientBatchGet());
    connection.Teardown();
    EXPECT_FALSE(connection.SupportsPayloadOnlyClientBatchGet());
}

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
        allocationAttempts.emplace_back(requiredSize);
        Status status = allocateHandler == nullptr ? allocateStatus : allocateHandler(requiredSize);
        if (status.IsError()) {
            return status;
        }
        auto fakeOwner = std::make_shared<FakeBufferOwner>(requiredSize);
        buffer.data = fakeOwner->data.data();
        buffer.size = fakeOwner->data.size();
        buffer.remoteAddr.set_seg_va(reinterpret_cast<uint64_t>(buffer.data));
        buffer.remoteAddr.set_seg_data_offset(baseSegDataOffset);
        buffer.owner = fakeOwner;
        buffer.transportInstanceId = "test-instance";
        lastOwner = fakeOwner;
        allocationSizes.emplace_back(requiredSize);
        return Status::OK();
    }

    uint64_t maxGetSize = 16;
    Status allocateStatus = Status::OK();
    std::function<Status(uint64_t)> allocateHandler;
    int allocateCount = 0;
    uint64_t baseSegDataOffset = 0;
    std::vector<uint64_t> allocationAttempts;
    std::vector<uint64_t> allocationSizes;
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

    ExistReqPb existRequest;
    existRequest.add_object_keys("key");
    existRequest.set_client_id("client-1");
    existRequest.set_tenant_id("tenant-1");
    ExistRspPb existResponse;
    ASSERT_TRUE(client.InvokeExist(800, existRequest, existResponse).IsOk());
    EXPECT_EQ(client.existInvokeCount, 1);
    EXPECT_EQ(client.existRpcTimeout, 800);
    EXPECT_EQ(client.invokedExistRequest.client_id(), "client-1");
    EXPECT_EQ(client.invokedExistRequest.access_key(), "access-1");
    EXPECT_FALSE(client.invokedExistRequest.signature().empty());
}

TEST(WorkerRpcClientTest, ExistUsesSubTimeoutBelowChannelTimeout)
{
    BrpcChannelConfig channelConfig;
    channelConfig.timeout_ms = 9000;
    AuthBoundaryWorkerRpcClient client(MakeSignature(), channelConfig);
    ExistReqPb request;
    request.add_object_keys("key");
    ExistRspPb response;

    ASSERT_TRUE(client.InvokeExist(1000, request, response).IsOk());

    EXPECT_EQ(client.existRpcTimeout, 1000);
}

TEST(WorkerRpcClientTest, BatchGetSignsAggregateRequestAndPreservesInputOrder)
{
    auto signature = std::make_shared<Signature>("access-1", SensitiveValue("secret-1"));
    AuthBoundaryWorkerRpcClient client(signature);
    client.batchGetPayloadValues = { "first", "second" };
    BatchGetObjectRemoteReqPb request;
    request.add_requests()->set_object_key("first-key");
    request.mutable_requests(0)->set_data_size(3);
    request.add_requests()->set_object_key("second-key");
    request.mutable_requests(1)->set_data_size(5);
    BatchGetObjectRemoteRspPb response;
    std::vector<RpcMessage> payloads;

    ASSERT_TRUE(client.InvokeBatchGetObject(request, response, payloads).IsOk());

    EXPECT_EQ(client.batchGetObjectInvokeCount, 1);
    EXPECT_GT(client.batchGetRpcTimeout, 0);
    EXPECT_EQ(client.invokedBatchGetRequest.access_key(), "access-1");
    EXPECT_FALSE(client.invokedBatchGetRequest.signature().empty());
    ASSERT_EQ(client.invokedBatchGetRequest.requests_size(), 2);
    EXPECT_EQ(client.invokedBatchGetRequest.requests(0).object_key(), "first-key");
    EXPECT_EQ(client.invokedBatchGetRequest.requests(1).object_key(), "second-key");
    EXPECT_TRUE(client.invokedBatchGetRequest.requests(0).access_key().empty());
    EXPECT_TRUE(client.invokedBatchGetRequest.requests(1).access_key().empty());
    ASSERT_EQ(response.responses_size(), 2);
    EXPECT_EQ(response.responses(0).data_size(), 3);
    EXPECT_EQ(response.responses(1).data_size(), 5);
    ASSERT_EQ(payloads.size(), 2u);
    EXPECT_EQ(std::string(static_cast<const char *>(payloads[0].Data()), payloads[0].Size()), "first");
    EXPECT_EQ(std::string(static_cast<const char *>(payloads[1].Data()), payloads[1].Size()), "second");
}

TEST(WorkerRpcClientTest, BatchGetPropagatesRpcErrorsAndPreservesResponsePayloads)
{
    AuthBoundaryWorkerRpcClient client(MakeSignature());
    client.batchGetInvokeStatus = Status(K_RPC_UNAVAILABLE, "batch RPC unavailable");
    client.batchGetPayloadValues = { "response-payload" };
    BatchGetObjectRemoteReqPb request;
    request.add_requests()->set_object_key("key");
    request.mutable_requests(0)->set_data_size(16);
    BatchGetObjectRemoteRspPb response;
    std::vector<RpcMessage> payloads;

    Status status = client.InvokeBatchGetObject(request, response, payloads);

    EXPECT_EQ(status.GetCode(), K_RPC_UNAVAILABLE);
    EXPECT_EQ(client.batchGetObjectInvokeCount, 1);
    ASSERT_EQ(response.responses_size(), 1);
    EXPECT_EQ(response.responses(0).data_size(), 16);
    ASSERT_EQ(payloads.size(), 1u);
    EXPECT_EQ(std::string(static_cast<const char *>(payloads[0].Data()), payloads[0].Size()), "response-payload");
}

TEST(WorkerRpcClientTest, BatchGetRejectsEmptyRequestBeforeRpc)
{
    AuthBoundaryWorkerRpcClient client(MakeSignature());
    BatchGetObjectRemoteReqPb request;
    BatchGetObjectRemoteRspPb response;
    std::vector<RpcMessage> payloads;

    EXPECT_EQ(client.InvokeBatchGetObject(request, response, payloads).GetCode(), K_INVALID);
    EXPECT_EQ(client.batchGetObjectInvokeCount, 0);
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
    auto signature = std::make_shared<Signature>("access-1", SensitiveValue("secret-1"));
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
    EXPECT_EQ(client.invokedMetadataRequest.access_key(), "access-1");

    ExistReqPb existRequest;
    existRequest.add_object_keys("key");
    ExistRspPb existResponse;
    ASSERT_TRUE(client.InvokeExist(1000, existRequest, existResponse).IsOk());
    EXPECT_EQ(client.existInvokeCount, 1);
    EXPECT_GT(client.existRpcTimeout, 0);
    EXPECT_LE(client.existRpcTimeout, 100);
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

    ExistReqPb existRequest;
    ExistRspPb existResponse;
    EXPECT_EQ(client.InvokeExist(100, existRequest, existResponse).GetCode(), K_RPC_DEADLINE_EXCEEDED);
    EXPECT_EQ(client.existInvokeCount, 0);
}

TEST(WorkerRpcClientTest, InvokesGetHashRingWithVersionAndTimeout)
{
    auto signature = std::make_shared<Signature>("access-1", SensitiveValue("secret-1"));
    AuthBoundaryWorkerRpcClient client(signature);
    GetHashRingRspPb ring;

    ASSERT_TRUE(client.InvokeGetHashRing(42, ring).IsOk());
    EXPECT_EQ(client.hashRingInvokeCount, 1);
    EXPECT_EQ(client.invokedHashRingRequest.version(), 42ul);
    EXPECT_FALSE(client.invokedHashRingRequest.signature().empty());
}

TEST(ExistRequestBuilderTest, RejectsEmptyKeys)
{
    std::vector<std::string> keys;
    TransportExistRequest input(keys, false, false, 100, "client", "tenant", SensitiveValue());
    ExistReqPb request;
    EXPECT_EQ(BuildExistRequest(input, request).GetCode(), K_INVALID);
}

TEST(ExistRequestBuilderTest, RejectsQueryL2CacheWithIsLocal)
{
    std::vector<std::string> keys{ "k1" };
    TransportExistRequest input(keys, true, true, 100, "client", "tenant", SensitiveValue());
    ExistReqPb request;
    EXPECT_EQ(BuildExistRequest(input, request).GetCode(), K_INVALID);
}

TEST(ExistRequestBuilderTest, BuildsRequestWithAuthFields)
{
    std::vector<std::string> keys{ "k1", "k2" };
    TransportExistRequest input(keys, true, false, 200, "client-1", "tenant-1", SensitiveValue("token-1"));
    ExistReqPb request;
    ASSERT_TRUE(BuildExistRequest(input, request).IsOk());
    ASSERT_EQ(request.object_keys_size(), 2);
    EXPECT_EQ(request.object_keys(0), "k1");
    EXPECT_EQ(request.object_keys(1), "k2");
    EXPECT_TRUE(request.query_l2cache());
    EXPECT_FALSE(request.is_local());
    EXPECT_EQ(request.client_id(), "client-1");
    EXPECT_EQ(request.tenant_id(), "tenant-1");
    EXPECT_EQ(request.token(), "token-1");
    EXPECT_TRUE(request.is_routed());
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
            EXPECT_TRUE(request.redirect());
            AddLocation(response, "b", MakeAddress(53));
        } else {
            EXPECT_EQ(address, MakeAddress(43));
            EXPECT_EQ(request.object_keys_size(), 1);
            EXPECT_EQ(request.object_keys(0), "c");
            EXPECT_TRUE(request.redirect());
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

TEST(ObjectMetadataClientTest, FollowsTwoRedirects)
{
    ApiDeadlineGuard deadline(1000);
    auto manager = std::make_shared<FakeDataPlaneManager>();
    std::vector<HostPort> calls;
    manager->queryAndGetHandler = [&calls](const HostPort &address, const master::QueryAndGetReqPb &request,
                                           master::QueryAndGetRspPb &response, std::vector<RpcMessage> &) {
        calls.push_back(address);
        EXPECT_TRUE(request.redirect());
        if (address == MakeAddress(43)) {
            AddLocation(response, "key", MakeAddress(51));
        } else {
            auto *redirect = response.add_info();
            redirect->set_redirect_meta_address(
                (address == MakeAddress(41) ? MakeAddress(42) : MakeAddress(43)).ToString());
            redirect->add_change_meta_ids("key");
        }
        return Status::OK();
    };
    ObjectMetadataClient metadata(manager, std::make_shared<DeadlineRetry>());
    auto results = MakeMetadataItems({ { 0, "key", MakeAddress(41) } });
    auto batch = MakeMetadataBatch(results);

    ASSERT_TRUE(metadata.QueryAndGet(MakeAddress(41), batch).IsOk());
    ASSERT_EQ(results.size(), 1u);
    EXPECT_TRUE(results[0].status.IsOk());
    EXPECT_EQ(calls, std::vector<HostPort>({ MakeAddress(41), MakeAddress(42), MakeAddress(43) }));
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

TEST(ObjectReadFlowTest, BatchReadyItemsOnceOnCallerAndPreservesMetadataErrorPosition)
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
    EXPECT_TRUE(replicas->unaryKeys.empty());
    ASSERT_EQ(replicas->batchKeys.size(), 1u);
    EXPECT_EQ(replicas->batchKeys[0], std::vector<std::string>({ "ub", "tcp" }));
    ASSERT_EQ(replicas->threadIds.size(), 1u);
    EXPECT_EQ(replicas->threadIds[0], callerThread);
    ASSERT_EQ(result.items.size(), 3u);
    EXPECT_TRUE(result.items[0].status.IsOk());
    EXPECT_EQ(result.items[1].status.GetCode(), K_NOT_FOUND);
    EXPECT_TRUE(result.items[2].status.IsOk());
    EXPECT_EQ(result.actualKind, AccessTransportKind::TCP);
}

TEST(ObjectReadFlowTest, RecordsDirectReadLatencyTicksWhenEnabled)
{
    Trace::Instance().ClearLatencyTicks();
    Raii clearTicks([]() { Trace::Instance().ClearLatencyTicks(); });
    ApiDeadlineGuard deadline(1000);
    auto metadata = std::make_shared<FakeObjectMetadataClient>();
    auto replicas = std::make_shared<FakeReplicaReader>();
    ObjectReadFlow flow(metadata, replicas, std::make_shared<ThreadPool>(0, 2, "object_read_test"));
    ObjectReadRequest request;
    request.items = { { 0, "key", MakeAddress(41) } };
    request.traceEnabled = true;
    ObjectReadResult result;

    ASSERT_TRUE(flow.Run(request, result).IsOk());
    auto phases = ComputePhaseDurations(Trace::Instance().GetLatencyTicks(),
                                        Trace::Instance().GetLatencyTickCount(),
                                        Trace::Instance().GetLatencyTickDroppedCount());
    EXPECT_NE(phases.Find(LatencySummaryPhase::CLIENT_RPC_DIRECT_QUERY_AND_GET), nullptr);
    EXPECT_NE(phases.Find(LatencySummaryPhase::CLIENT_RPC_DIRECT_GET_DATA), nullptr);
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
    ASSERT_EQ(replicas->unaryKeys.size(), 1u);
    EXPECT_EQ(replicas->unaryKeys[0], "fallback");
    EXPECT_TRUE(replicas->batchKeys.empty());
    ASSERT_EQ(result.items.size(), 2u);
    EXPECT_TRUE(result.items[0].status.IsOk());
    EXPECT_EQ(result.items[0].data.kind, AccessTransportKind::TCP);
    ASSERT_EQ(result.items[0].data.rpcPayloads.size(), 1u);
    EXPECT_EQ(std::string(static_cast<const char *>(result.items[0].data.rpcPayloads[0].Data()),
                          result.items[0].data.rpcPayloads[0].Size()),
              "data");
    EXPECT_TRUE(result.items[1].status.IsOk());
}

TEST(ObjectReadFlowTest, BatchZeroReadyItemsSkipsDataRead)
{
    ApiDeadlineGuard deadline(1000);
    auto metadata = std::make_shared<FakeObjectMetadataClient>();
    metadata->itemStatuses.emplace("first", Status(K_NOT_FOUND, "first"));
    metadata->itemStatuses.emplace("second", Status(K_INVALID, "second"));
    auto replicas = std::make_shared<FakeReplicaReader>();
    ObjectReadFlow flow(metadata, replicas, std::make_shared<ThreadPool>(0, 2, "object_read_test"));
    ObjectReadRequest request;
    request.items = { { 4, "first", MakeAddress(41) }, { 2, "second", MakeAddress(41) } };
    ObjectReadResult result;

    EXPECT_EQ(flow.Run(request, result).GetCode(), K_NOT_FOUND);
    EXPECT_TRUE(replicas->unaryKeys.empty());
    EXPECT_TRUE(replicas->batchKeys.empty());
    ASSERT_EQ(result.items.size(), 2u);
    EXPECT_EQ(result.items[0].requestIndex, 4u);
    EXPECT_EQ(result.items[0].status.GetCode(), K_NOT_FOUND);
    EXPECT_EQ(result.items[1].requestIndex, 2u);
    EXPECT_EQ(result.items[1].status.GetCode(), K_INVALID);
}

TEST(ObjectReadFlowTest, BatchOneReadyItemUsesUnaryReadOnCaller)
{
    ApiDeadlineGuard deadline(1000);
    auto metadata = std::make_shared<FakeObjectMetadataClient>();
    auto replicas = std::make_shared<FakeReplicaReader>();
    ObjectReadFlow flow(metadata, replicas, std::make_shared<ThreadPool>(0, 2, "object_read_test"));
    ObjectReadRequest request;
    request.items = { { 9, "only", MakeAddress(41) } };
    ObjectReadResult result;
    const auto callerThread = std::this_thread::get_id();

    ASSERT_TRUE(flow.Run(request, result).IsOk());
    EXPECT_EQ(replicas->unaryKeys, std::vector<std::string>({ "only" }));
    EXPECT_TRUE(replicas->batchKeys.empty());
    ASSERT_EQ(replicas->threadIds.size(), 1u);
    EXPECT_EQ(replicas->threadIds[0], callerThread);
}

TEST(ObjectReadFlowTest, BatchMixedDataOutcomesKeepInputOrderAndPartialSuccess)
{
    ApiDeadlineGuard deadline(1000);
    auto metadata = std::make_shared<FakeObjectMetadataClient>();
    auto replicas = std::make_shared<FakeReplicaReader>();
    replicas->itemStatuses.emplace("retry", Status(K_NOT_FOUND, "retry"));
    replicas->itemStatuses.emplace("terminal", Status(K_INVALID, "terminal"));
    ObjectReadFlow flow(metadata, replicas, std::make_shared<ThreadPool>(0, 2, "object_read_test"));
    ObjectReadRequest request;
    request.items = { { 5, "retry", MakeAddress(41) }, { 1, "ok", MakeAddress(41) },
                      { 8, "terminal", MakeAddress(41) } };
    ObjectReadResult result;

    ASSERT_TRUE(flow.Run(request, result).IsOk());
    ASSERT_EQ(result.items.size(), 3u);
    EXPECT_EQ(result.items[0].requestIndex, 5u);
    EXPECT_EQ(result.items[0].status.GetCode(), K_NOT_FOUND);
    EXPECT_EQ(result.items[1].requestIndex, 1u);
    EXPECT_TRUE(result.items[1].status.IsOk());
    EXPECT_EQ(result.items[2].requestIndex, 8u);
    EXPECT_EQ(result.items[2].status.GetCode(), K_INVALID);
}

TEST(ObjectClientTransportTest, BatchExternalOwnersMaterializeIntoIndependentSdkBuffers)
{
    auto owner = std::make_shared<FakeBufferOwner>(8);
    std::memcpy(owner->data.data(), "one!two?", owner->data.size());
    std::weak_ptr<IReceiveBufferOwner> weakOwner = owner;
    ConnectOptions options;
    options.host = "127.0.0.1";
    options.port = 31501;
    auto client = std::make_shared<object_cache::ObjectClientImpl>(options);
    auto workerApi = std::make_shared<object_cache::ClientWorkerRemoteApi>(MakeAddress(31501), RpcCredential{});
    workerApi->clientId_ = "materialization-test-client";
    client->workerApi_.emplace_back(workerApi);
    ObjectReadItemResult first;
    first.objectKey = "one";
    first.data.response.set_data_size(4);
    first.data.externalData = owner->data.data();
    first.data.externalSize = 4;
    first.data.externalOwner = owner;
    ObjectReadItemResult second;
    second.objectKey = "two";
    second.data.response.set_data_size(4);
    second.data.externalData = owner->data.data() + 4;
    second.data.externalSize = 4;
    second.data.externalOwner = owner;
    std::shared_ptr<Buffer> firstBuffer;
    std::shared_ptr<Buffer> secondBuffer;

    ASSERT_TRUE(client->MaterializeTransportItem("one", first, firstBuffer).IsOk());
    ASSERT_TRUE(client->MaterializeTransportItem("two", second, secondBuffer).IsOk());
    first = ObjectReadItemResult{};
    second = ObjectReadItemResult{};
    owner.reset();

    ASSERT_FALSE(weakOwner.expired());
    ASSERT_NE(firstBuffer, nullptr);
    ASSERT_NE(secondBuffer, nullptr);
    EXPECT_EQ(std::string(static_cast<const char *>(firstBuffer->ImmutableData()), firstBuffer->GetSize()), "one!");
    EXPECT_EQ(std::string(static_cast<const char *>(secondBuffer->ImmutableData()), secondBuffer->GetSize()), "two?");
    firstBuffer.reset();
    EXPECT_FALSE(weakOwner.expired());
    EXPECT_EQ(std::string(static_cast<const char *>(secondBuffer->ImmutableData()), secondBuffer->GetSize()), "two?");
    secondBuffer.reset();
    EXPECT_TRUE(weakOwner.expired());
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

GetObjectRemoteRspPb MakeBatchGetResponse(StatusCode status, int64_t dataSize, DataTransferSource source);

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

TEST(TcpTransporterTest, BatchGetMetricsCountOnlyMultiObjectRpc)
{
    InitBatchGetMetrics();
    auto rpcClient = std::make_shared<FakeWorkerRpcClient>();
    rpcClient->getObjectDataSize = 1;
    rpcClient->batchGetObjectResponses = {
        MakeBatchGetResponse(K_OK, 1, DataTransferSource::DATA_IN_PAYLOAD),
        MakeBatchGetResponse(K_OK, 1, DataTransferSource::DATA_IN_PAYLOAD)
    };
    rpcClient->batchGetObjectPayloadValues = { "a", "b" };
    TcpTransporter transporter(rpcClient);
    DataGetBatchResult results;

    ASSERT_TRUE(transporter.BatchGet({ { "unary", 1 } }, results).IsOk());
    ASSERT_TRUE(transporter.BatchGet({ { "first", 1 }, { "second", 1 } }, results).IsOk());

    ExpectMetricTotal("client_direct_batch_get_rpc_total", 1);
    ExpectMetricTotal("client_direct_batch_get_object_total", 2);
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
    EXPECT_TRUE(result.rpcPayloads.empty());
}

GetObjectRemoteRspPb MakeBatchGetResponse(StatusCode status, int64_t dataSize,
                                          DataTransferSource source = DataTransferSource::DATA_IN_PAYLOAD)
{
    GetObjectRemoteRspPb response;
    response.mutable_error()->set_error_code(status);
    response.set_data_size(dataSize);
    response.set_data_source(source);
    return response;
}

TEST(TcpTransporterTest, BatchGetPreservesRequestOrderAndPayloadOwnership)
{
    auto rpcClient = std::make_shared<FakeWorkerRpcClient>();
    rpcClient->batchGetObjectResponses = { MakeBatchGetResponse(K_OK, 5), MakeBatchGetResponse(K_OK, 6) };
    rpcClient->batchGetObjectPayloadValues = { "first", "second" };
    TcpTransporter transporter(rpcClient);
    DataGetBatchResult results;

    ASSERT_TRUE(transporter.BatchGet({ { "first-key", 5 }, { "second-key", 6 } }, results).IsOk());
    ASSERT_EQ(rpcClient->batchGetObjectCount, 1);
    ASSERT_EQ(rpcClient->batchGetObjectRequests.size(), 1u);
    ASSERT_EQ(rpcClient->batchGetObjectRequests[0].requests_size(), 2);
    EXPECT_EQ(rpcClient->batchGetObjectRequests[0].requests(0).object_key(), "first-key");
    EXPECT_EQ(rpcClient->batchGetObjectRequests[0].requests(1).object_key(), "second-key");
    EXPECT_TRUE(rpcClient->batchGetObjectRequests[0].requests(0).try_lock());
    EXPECT_TRUE(rpcClient->batchGetObjectRequests[0].requests(1).try_lock());
    ASSERT_EQ(results.size(), 2u);
    EXPECT_TRUE(results[0].status.IsOk());
    EXPECT_TRUE(results[1].status.IsOk());
    EXPECT_EQ(results[0].data.kind, AccessTransportKind::TCP);
    EXPECT_EQ(results[1].data.kind, AccessTransportKind::TCP);
    ASSERT_EQ(results[0].data.rpcPayloads.size(), 1u);
    ASSERT_EQ(results[1].data.rpcPayloads.size(), 1u);
    EXPECT_EQ(std::string(static_cast<const char *>(results[0].data.rpcPayloads[0].Data()),
                          results[0].data.rpcPayloads[0].Size()),
              "first");
    EXPECT_EQ(std::string(static_cast<const char *>(results[1].data.rpcPayloads[0].Data()),
                          results[1].data.rpcPayloads[0].Size()),
              "second");
}

TEST(TcpTransporterTest, BatchGetReturnsBusinessErrorsPerItemWithoutConsumingPayload)
{
    auto rpcClient = std::make_shared<FakeWorkerRpcClient>();
    rpcClient->batchGetObjectResponses = { MakeBatchGetResponse(K_OK, 4),
                                           MakeBatchGetResponse(K_NOT_FOUND, 0),
                                           MakeBatchGetResponse(K_OUT_OF_MEMORY, 0) };
    rpcClient->batchGetObjectPayloadValues = { "data" };
    TcpTransporter transporter(rpcClient);
    DataGetBatchResult results;

    ASSERT_TRUE(transporter.BatchGet({ { "present", 4 }, { "missing", 4 }, { "full", 4 } }, results).IsOk());
    ASSERT_EQ(results.size(), 3u);
    EXPECT_TRUE(results[0].status.IsOk());
    EXPECT_EQ(results[1].status.GetCode(), K_NOT_FOUND);
    EXPECT_EQ(results[2].status.GetCode(), K_OUT_OF_MEMORY);
    ASSERT_EQ(results[0].data.rpcPayloads.size(), 1u);
    EXPECT_TRUE(results[1].data.rpcPayloads.empty());
    EXPECT_TRUE(results[2].data.rpcPayloads.empty());
}

TEST(TcpTransporterTest, BatchGetReturnsRpcFailureWithoutStaleOutputs)
{
    auto rpcClient = std::make_shared<FakeWorkerRpcClient>();
    rpcClient->batchGetObjectStatus = Status(K_RPC_UNAVAILABLE, "worker unavailable");
    TcpTransporter transporter(rpcClient);
    DataGetBatchResult results(1);
    results[0].data.rpcPayloads.emplace_back();

    EXPECT_EQ(transporter.BatchGet({ { "key", 4 }, { "other", 4 } }, results).GetCode(), K_RPC_UNAVAILABLE);
    EXPECT_TRUE(results.empty());
}

TEST(TcpTransporterTest, BatchGetRejectsResponseCountMismatch)
{
    auto rpcClient = std::make_shared<FakeWorkerRpcClient>();
    rpcClient->batchGetObjectResponses = { MakeBatchGetResponse(K_OK, 4) };
    rpcClient->batchGetObjectPayloadValues = { "data" };
    TcpTransporter transporter(rpcClient);
    DataGetBatchResult results;

    EXPECT_EQ(transporter.BatchGet({ { "first", 4 }, { "second", 4 } }, results).GetCode(), K_RUNTIME_ERROR);
    EXPECT_TRUE(results.empty());
}

TEST(TcpTransporterTest, BatchGetRejectsPayloadUnderflow)
{
    auto rpcClient = std::make_shared<FakeWorkerRpcClient>();
    rpcClient->batchGetObjectResponses = { MakeBatchGetResponse(K_OK, 4), MakeBatchGetResponse(K_OK, 5) };
    rpcClient->batchGetObjectPayloadValues = { "first" };
    TcpTransporter transporter(rpcClient);
    DataGetBatchResult results;

    EXPECT_EQ(transporter.BatchGet({ { "first", 4 }, { "second", 5 } }, results).GetCode(), K_RUNTIME_ERROR);
    EXPECT_TRUE(results.empty());
}

TEST(TcpTransporterTest, BatchGetRejectsUnexpectedPayload)
{
    auto rpcClient = std::make_shared<FakeWorkerRpcClient>();
    rpcClient->batchGetObjectResponses = { MakeBatchGetResponse(K_OK, 4), MakeBatchGetResponse(K_NOT_FOUND, 0) };
    rpcClient->batchGetObjectPayloadValues = { "first", "unexpected" };
    TcpTransporter transporter(rpcClient);
    DataGetBatchResult results;

    EXPECT_EQ(transporter.BatchGet({ { "first", 4 }, { "second", 4 } }, results).GetCode(), K_RUNTIME_ERROR);
    EXPECT_TRUE(results.empty());
}

TEST(TcpTransporterTest, BatchGetDelegatesOneItemToUnaryGet)
{
    auto rpcClient = std::make_shared<FakeWorkerRpcClient>();
    rpcClient->getObjectDataSize = 4;
    TcpTransporter transporter(rpcClient);
    DataGetBatchResult results;

    ASSERT_TRUE(transporter.BatchGet({ { "key", 4 } }, results).IsOk());
    EXPECT_EQ(rpcClient->getObjectCount, 1);
    EXPECT_EQ(rpcClient->batchGetObjectCount, 0);
    ASSERT_EQ(results.size(), 1u);
    EXPECT_TRUE(results[0].status.IsOk());
    ASSERT_EQ(results[0].data.rpcPayloads.size(), 1u);
}

TEST(TcpTransporterTest, BatchGetDelegatedUnaryBusinessErrorDoesNotOwnPayload)
{
    auto rpcClient = std::make_shared<FakeWorkerRpcClient>();
    rpcClient->getObjectDataSize = 4;
    rpcClient->getObjectResponseCode = K_NOT_FOUND;
    TcpTransporter transporter(rpcClient);
    DataGetBatchResult results;

    ASSERT_TRUE(transporter.BatchGet({ { "missing", 4 } }, results).IsOk());
    ASSERT_EQ(results.size(), 1u);
    EXPECT_EQ(results[0].status.GetCode(), K_NOT_FOUND);
    EXPECT_TRUE(results[0].data.rpcPayloads.empty());
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

TEST(UbTransporterTest, BatchGetAggregateOneItemPreservesUnaryGetBehaviorAndClearsOutput)
{
    auto rpcClient = std::make_shared<FakeWorkerRpcClient>();
    rpcClient->getObjectDataSize = 8;
    auto bufferProvider = std::make_shared<FakeUbBufferProvider>();
    UbTransporter transporter(rpcClient, std::make_shared<FakeUbConnection>(), bufferProvider);
    DataGetBatchResult results(2);

    ASSERT_TRUE(transporter.BatchGet({ { "key", 8 } }, results).IsOk());

    EXPECT_EQ(rpcClient->getObjectCount, 1);
    EXPECT_EQ(rpcClient->batchGetObjectCount, 0);
    ASSERT_EQ(results.size(), 1u);
    EXPECT_TRUE(results[0].status.IsOk());
    EXPECT_EQ(results[0].data.kind, AccessTransportKind::UB);
    EXPECT_NE(results[0].data.externalOwner, nullptr);
}

TEST(UbTransporterTest, BatchGetAggregateUsesOneAlignedAllocationAndOneRpc)
{
    auto rpcClient = std::make_shared<FakeWorkerRpcClient>();
    rpcClient->batchGetObjectResponses = {
        MakeBatchGetResponse(K_OK, 1, DataTransferSource::DATA_ALREADY_TRANSFERRED),
        MakeBatchGetResponse(K_OK, 17, DataTransferSource::DATA_ALREADY_TRANSFERRED),
        MakeBatchGetResponse(K_OK, 16, DataTransferSource::DATA_ALREADY_TRANSFERRED)
    };
    auto bufferProvider = std::make_shared<FakeUbBufferProvider>();
    bufferProvider->maxGetSize = 64;
    bufferProvider->baseSegDataOffset = 64;
    auto connection = std::make_shared<FakeUbConnection>(true);
    UbTransporter transporter(rpcClient, connection, bufferProvider);
    DataGetBatchResult results;

    ASSERT_TRUE(transporter.BatchGet({ { "one", 1 }, { "two", 17 }, { "three", 16 } }, results).IsOk());

    ASSERT_EQ(bufferProvider->allocationSizes, std::vector<uint64_t>({ 64 }));
    ASSERT_EQ(rpcClient->batchGetObjectRequests.size(), 1u);
    const auto &request = rpcClient->batchGetObjectRequests[0];
    ASSERT_EQ(request.requests_size(), 3);
    EXPECT_EQ(request.urma_instance_id(), "test-instance");
    const std::vector<uint64_t> expectedOffsets{ 0, 16, 48 };
    for (size_t i = 0; i < expectedOffsets.size(); ++i) {
        EXPECT_TRUE(request.requests(static_cast<int>(i)).has_urma_info());
        EXPECT_EQ(request.requests(static_cast<int>(i)).read_offset(), 0u);
        EXPECT_EQ(request.requests(static_cast<int>(i)).read_size(),
                  i == 0 ? 1u : (i == 1 ? 17u : 16u));
        EXPECT_EQ(request.requests(static_cast<int>(i)).urma_info().seg_data_offset(), 64 + expectedOffsets[i]);
        EXPECT_EQ(expectedOffsets[i] % 16, 0u);
    }

    ASSERT_EQ(results.size(), 3u);
    auto owner = results[0].data.externalOwner;
    ASSERT_NE(owner, nullptr);
    auto *base = results[0].data.externalData;
    EXPECT_EQ(results[1].data.externalData, base + 16);
    EXPECT_EQ(results[2].data.externalData, base + 48);
    EXPECT_EQ(results[0].data.externalSize, 1u);
    EXPECT_EQ(results[1].data.externalSize, 17u);
    EXPECT_EQ(results[2].data.externalSize, 16u);
    EXPECT_EQ(results[1].data.externalOwner, owner);
    EXPECT_EQ(results[2].data.externalOwner, owner);

}

TEST(UbTransporterTest, BatchGetMetricsCountNormalMultiObjectRpc)
{
    InitBatchGetMetrics();
    auto rpcClient = std::make_shared<FakeWorkerRpcClient>();
    rpcClient->batchGetObjectResponses = {
        MakeBatchGetResponse(K_OK, 8, DataTransferSource::DATA_ALREADY_TRANSFERRED),
        MakeBatchGetResponse(K_OK, 8, DataTransferSource::DATA_ALREADY_TRANSFERRED)
    };
    auto bufferProvider = std::make_shared<FakeUbBufferProvider>();
    bufferProvider->maxGetSize = 32;
    UbTransporter transporter(rpcClient, std::make_shared<FakeUbConnection>(), bufferProvider);
    DataGetBatchResult results;

    ASSERT_TRUE(transporter.BatchGet({ { "one", 8 }, { "two", 8 } }, results).IsOk());

    EXPECT_EQ(rpcClient->batchGetObjectCount, 1);
    ExpectMetricTotal("client_direct_batch_get_rpc_total", 1);
    ExpectMetricTotal("client_direct_batch_get_object_total", 2);
}

TEST(UbTransporterTest, BatchGetUsesOneTcpBatchWhenWorkerLacksPayloadOnlyCapability)
{
    InitBatchGetMetrics();
    auto rpcClient = std::make_shared<FakeWorkerRpcClient>();
    rpcClient->batchGetObjectResponses = {
        MakeBatchGetResponse(K_OK, 3, DataTransferSource::DATA_IN_PAYLOAD),
        MakeBatchGetResponse(K_OK, 4, DataTransferSource::DATA_IN_PAYLOAD)
    };
    rpcClient->batchGetObjectPayloadValues = { "one", "two2" };
    auto bufferProvider = std::make_shared<FakeUbBufferProvider>();
    UrmaHandshakeRspPb oldWorkerHandshake;
    ASSERT_FALSE(oldWorkerHandshake.supports_payload_only_client_batch_get());
    auto connection = std::make_shared<FakeUbConnection>(oldWorkerHandshake.supports_payload_only_client_batch_get());
    UbTransporter transporter(rpcClient, connection, bufferProvider);
    DataGetBatchResult results(1);

    ASSERT_TRUE(transporter.BatchGet({ { "first", 3 }, { "second", 4 } }, results).IsOk());

    EXPECT_EQ(bufferProvider->allocateCount, 0);
    ASSERT_EQ(rpcClient->batchGetObjectRequests.size(), 1u);
    const auto &request = rpcClient->batchGetObjectRequests.front();
    ASSERT_EQ(request.requests_size(), 2);
    EXPECT_EQ(request.requests(0).object_key(), "first");
    EXPECT_EQ(request.requests(1).object_key(), "second");
    EXPECT_FALSE(request.requests(0).has_urma_info());
    EXPECT_FALSE(request.requests(1).has_urma_info());
    ASSERT_EQ(results.size(), 2u);
    EXPECT_EQ(results[0].data.kind, AccessTransportKind::TCP);
    EXPECT_EQ(results[1].data.kind, AccessTransportKind::TCP);
    ExpectMetricTotal("client_direct_batch_get_rpc_total", 1);
    ExpectMetricTotal("client_direct_batch_get_object_total", 2);
}

TEST(UbTransporterTest, BatchGetMetricsExcludePressureCreatedSingletonRpcs)
{
    InitBatchGetMetrics();
    auto rpcClient = std::make_shared<FakeWorkerRpcClient>();
    rpcClient->batchGetHandler = [](BatchGetObjectRemoteReqPb &request, BatchGetObjectRemoteRspPb &response,
                                    std::vector<RpcMessage> &) {
        for (const auto &itemRequest : request.requests()) {
            *response.add_responses() = MakeBatchGetResponse(
                K_OK, static_cast<int64_t>(itemRequest.data_size()), DataTransferSource::DATA_ALREADY_TRANSFERRED);
        }
        return Status::OK();
    };
    auto bufferProvider = std::make_shared<FakeUbBufferProvider>();
    bufferProvider->maxGetSize = 64;
    int allocationAttempt = 0;
    bufferProvider->allocateHandler = [&allocationAttempt](uint64_t size) {
        ++allocationAttempt;
        return allocationAttempt == 1 || allocationAttempt == 3
                   ? Status(K_OUT_OF_MEMORY, "deterministic pressure at " + std::to_string(size))
                   : Status::OK();
    };
    UbTransporter transporter(rpcClient, std::make_shared<FakeUbConnection>(), bufferProvider);
    DataGetBatchResult results;

    ASSERT_TRUE(transporter.BatchGet(
        { { "one", 8 }, { "two", 8 }, { "three", 8 }, { "four", 8 } }, results).IsOk());

    ASSERT_EQ(rpcClient->batchGetObjectRequests.size(), 3u);
    EXPECT_EQ(rpcClient->batchGetObjectRequests[0].requests_size(), 2);
    EXPECT_EQ(rpcClient->batchGetObjectRequests[1].requests_size(), 1);
    EXPECT_EQ(rpcClient->batchGetObjectRequests[2].requests_size(), 1);
    ExpectMetricTotal("client_direct_batch_get_rpc_total", 1);
    ExpectMetricTotal("client_direct_batch_get_object_total", 2);
    ExpectMetricTotal("client_direct_batch_get_ub_split_total", 2);
}

TEST(BatchGetMetricsTest, FailedTcpAndUbRpcsCountAttemptsAndObjects)
{
    InitBatchGetMetrics();
    auto tcpRpcClient = std::make_shared<FakeWorkerRpcClient>();
    tcpRpcClient->batchGetObjectStatus = Status(K_RPC_UNAVAILABLE, "tcp unavailable");
    TcpTransporter tcp(tcpRpcClient);
    DataGetBatchResult results;

    EXPECT_EQ(tcp.BatchGet({ { "one", 1 }, { "two", 1 } }, results).GetCode(), K_RPC_UNAVAILABLE);
    ExpectMetricTotal("client_direct_batch_get_rpc_total", 1);
    ExpectMetricTotal("client_direct_batch_get_object_total", 2);

    InitBatchGetMetrics();
    auto ubRpcClient = std::make_shared<FakeWorkerRpcClient>();
    ubRpcClient->batchGetObjectStatus = Status(K_RPC_UNAVAILABLE, "ub unavailable");
    auto bufferProvider = std::make_shared<FakeUbBufferProvider>();
    bufferProvider->maxGetSize = 32;
    UbTransporter ub(ubRpcClient, std::make_shared<FakeUbConnection>(), bufferProvider);

    EXPECT_EQ(ub.BatchGet({ { "one", 8 }, { "two", 8 } }, results).GetCode(), K_RPC_UNAVAILABLE);
    ExpectMetricTotal("client_direct_batch_get_rpc_total", 1);
    ExpectMetricTotal("client_direct_batch_get_object_total", 2);
}

TEST(UbTransporterTest, BatchGetFailedAllocationFallbackCountsSubmittedObjects)
{
    InitBatchGetMetrics();
    auto rpcClient = std::make_shared<FakeWorkerRpcClient>();
    rpcClient->batchGetObjectStatus = Status(K_RPC_UNAVAILABLE, "fallback unavailable");
    auto bufferProvider = std::make_shared<FakeUbBufferProvider>();
    bufferProvider->maxGetSize = 32;
    bufferProvider->allocateStatus = Status(K_OUT_OF_MEMORY, "pool exhausted");
    UbTransporter transporter(rpcClient, std::make_shared<FakeUbConnection>(), bufferProvider);
    DataGetBatchResult results;

    ASSERT_TRUE(transporter.BatchGet({ { "one", 8 }, { "two", 8 } }, results).IsOk());

    ASSERT_EQ(results.size(), 2u);
    EXPECT_EQ(results[0].status.GetCode(), K_RPC_UNAVAILABLE);
    EXPECT_EQ(results[1].status.GetCode(), K_RPC_UNAVAILABLE);
    ExpectMetricTotal("client_direct_batch_get_rpc_total", 1);
    ExpectMetricTotal("client_direct_batch_get_object_total", 2);
    ExpectMetricTotal("client_direct_batch_get_ub_split_total", 1);
    ExpectMetricTotal("client_direct_batch_get_tcp_fallback_total", 2);
}

TEST(UbTransporterTest, BatchGetAggregateOwnerOutlivesTransporterAndSiblingResults)
{
    auto rpcClient = std::make_shared<FakeWorkerRpcClient>();
    rpcClient->batchGetObjectResponses = {
        MakeBatchGetResponse(K_OK, 8, DataTransferSource::DATA_ALREADY_TRANSFERRED),
        MakeBatchGetResponse(K_OK, 8, DataTransferSource::DATA_ALREADY_TRANSFERRED)
    };
    auto bufferProvider = std::make_shared<FakeUbBufferProvider>();
    bufferProvider->maxGetSize = 32;
    DataGetResult survivor;
    {
        auto transporter = std::make_unique<UbTransporter>(rpcClient, std::make_shared<FakeUbConnection>(),
                                                           bufferProvider);
        DataGetBatchResult results;
        ASSERT_TRUE(transporter->BatchGet({ { "one", 8 }, { "two", 8 } }, results).IsOk());
        ASSERT_EQ(results.size(), 2u);
        survivor = std::move(results[1].data);
        results.clear();
        transporter.reset();
    }

    EXPECT_FALSE(bufferProvider->lastOwner.expired());
    EXPECT_NE(survivor.externalData, nullptr);
    survivor.externalOwner.reset();
    EXPECT_TRUE(bufferProvider->lastOwner.expired());
}

TEST(UbTransporterTest, BatchGetAggregateSplitsByAlignedMaxGetSizeBeforeAllocation)
{
    auto rpcClient = std::make_shared<FakeWorkerRpcClient>();
    rpcClient->batchGetHandler = [](BatchGetObjectRemoteReqPb &request, BatchGetObjectRemoteRspPb &response,
                                    std::vector<RpcMessage> &) {
        for (const auto &itemRequest : request.requests()) {
            *response.add_responses() = MakeBatchGetResponse(
                K_OK, static_cast<int64_t>(itemRequest.data_size()), DataTransferSource::DATA_ALREADY_TRANSFERRED);
        }
        return Status::OK();
    };
    auto bufferProvider = std::make_shared<FakeUbBufferProvider>();
    bufferProvider->maxGetSize = 64;
    UbTransporter transporter(rpcClient, std::make_shared<FakeUbConnection>(), bufferProvider);
    DataGetBatchResult results;
    DataGetBatchRequest request{ { "one", 8 }, { "two", 8 }, { "three", 8 },
                                 { "four", 8 }, { "five", 8 }, { "six", 8 } };

    ASSERT_TRUE(transporter.BatchGet(request, results).IsOk());

    EXPECT_EQ(bufferProvider->allocationSizes, std::vector<uint64_t>({ 64, 32 }));
    ASSERT_EQ(rpcClient->batchGetObjectRequests.size(), 2u);
    EXPECT_EQ(rpcClient->batchGetObjectRequests[0].requests_size(), 4);
    EXPECT_EQ(rpcClient->batchGetObjectRequests[1].requests_size(), 2);
    ASSERT_EQ(results.size(), request.size());
    for (const auto &result : results) {
        EXPECT_TRUE(result.status.IsOk());
        EXPECT_EQ(result.data.kind, AccessTransportKind::UB);
    }
}

TEST(UbTransporterTest, BatchGetAggregateHandlesZeroSizeInsideMixedBatch)
{
    auto rpcClient = std::make_shared<FakeWorkerRpcClient>();
    rpcClient->getObjectDataSize = 0;
    rpcClient->batchGetObjectResponses = {
        MakeBatchGetResponse(K_OK, 8, DataTransferSource::DATA_ALREADY_TRANSFERRED),
        MakeBatchGetResponse(K_OK, 8, DataTransferSource::DATA_ALREADY_TRANSFERRED)
    };
    auto bufferProvider = std::make_shared<FakeUbBufferProvider>();
    bufferProvider->maxGetSize = 32;
    UbTransporter transporter(rpcClient, std::make_shared<FakeUbConnection>(), bufferProvider);
    DataGetBatchResult results(1);

    ASSERT_TRUE(transporter.BatchGet(
        { { "one", 8 }, { "two", 8 }, { "zero", 0 }, { "three", 8 }, { "four", 8 } }, results).IsOk());

    EXPECT_EQ(rpcClient->batchGetObjectCount, 2);
    ASSERT_EQ(rpcClient->getObjectRequests.size(), 1u);
    EXPECT_EQ(rpcClient->getObjectRequests[0].object_key(), "zero");
    EXPECT_FALSE(rpcClient->getObjectRequests[0].has_urma_info());
    ASSERT_EQ(results.size(), 5u);
    EXPECT_EQ(results[2].data.kind, AccessTransportKind::TCP);
}

TEST(UbTransporterTest, BatchGetAggregateHandlesOverMaxSizeInsideMixedBatch)
{
    auto rpcClient = std::make_shared<FakeWorkerRpcClient>();
    rpcClient->getObjectDataSize = 8;
    rpcClient->batchGetObjectResponses = {
        MakeBatchGetResponse(K_OK, 8, DataTransferSource::DATA_ALREADY_TRANSFERRED),
        MakeBatchGetResponse(K_OK, 8, DataTransferSource::DATA_ALREADY_TRANSFERRED)
    };
    auto bufferProvider = std::make_shared<FakeUbBufferProvider>();
    bufferProvider->maxGetSize = 32;
    UbTransporter transporter(rpcClient, std::make_shared<FakeUbConnection>(), bufferProvider);
    DataGetBatchResult results;

    ASSERT_TRUE(transporter.BatchGet(
        { { "one", 8 }, { "two", 8 }, { "large", 64 }, { "three", 8 }, { "four", 8 } }, results).IsOk());

    EXPECT_EQ(rpcClient->batchGetObjectCount, 2);
    ASSERT_EQ(rpcClient->getObjectRequests.size(), 1u);
    EXPECT_EQ(rpcClient->getObjectRequests[0].object_key(), "large");
    EXPECT_FALSE(rpcClient->getObjectRequests[0].has_urma_info());
    EXPECT_EQ(results.size(), 5u);
}

TEST(UbTransporterTest, BatchGetAggregateClearsStaleOutputBeforeEarlyValidationError)
{
    auto rpcClient = std::make_shared<FakeWorkerRpcClient>();
    auto bufferProvider = std::make_shared<FakeUbBufferProvider>();
    UbTransporter transporter(rpcClient, std::make_shared<FakeUbConnection>(), bufferProvider);
    DataGetBatchResult results(2);

    EXPECT_EQ(transporter.BatchGet({ { "valid", 8 }, { "", 8 } }, results).GetCode(), K_INVALID);
    EXPECT_TRUE(results.empty());
    EXPECT_EQ(bufferProvider->allocateCount, 0);
    EXPECT_EQ(rpcClient->getObjectCount, 0);
    EXPECT_EQ(rpcClient->batchGetObjectCount, 0);
}

TEST(UbTransporterTest, BatchGetAggregateRejectsAlignmentAndTotalSizeOverflowBeforeIo)
{
    auto rpcClient = std::make_shared<FakeWorkerRpcClient>();
    auto bufferProvider = std::make_shared<FakeUbBufferProvider>();
    bufferProvider->maxGetSize = std::numeric_limits<uint64_t>::max();
    UbTransporter transporter(rpcClient, std::make_shared<FakeUbConnection>(), bufferProvider);
    DataGetBatchResult results(1);

    EXPECT_EQ(transporter.BatchGet({ { "overflow", std::numeric_limits<uint64_t>::max() }, { "other", 1 } },
                                   results).GetCode(),
              K_INVALID);
    EXPECT_TRUE(results.empty());
    EXPECT_EQ(bufferProvider->allocateCount, 0);
    EXPECT_EQ(rpcClient->batchGetObjectCount, 0);

    const uint64_t largestAligned = std::numeric_limits<uint64_t>::max() - 15;
    EXPECT_EQ(transporter.BatchGet({ { "large", largestAligned }, { "overflow", 16 } }, results).GetCode(),
              K_INVALID);
    EXPECT_TRUE(results.empty());
    EXPECT_EQ(bufferProvider->allocateCount, 0);
    EXPECT_EQ(rpcClient->batchGetObjectCount, 0);
}

TEST(UbTransporterTest, BatchGetAggregateRejectsRemoteOffsetOverflowBeforeRpc)
{
    auto rpcClient = std::make_shared<FakeWorkerRpcClient>();
    auto bufferProvider = std::make_shared<FakeUbBufferProvider>();
    bufferProvider->maxGetSize = 32;
    bufferProvider->baseSegDataOffset = std::numeric_limits<uint64_t>::max() - 8;
    UbTransporter transporter(rpcClient, std::make_shared<FakeUbConnection>(), bufferProvider);
    DataGetBatchResult results;

    EXPECT_EQ(transporter.BatchGet({ { "one", 8 }, { "two", 8 } }, results).GetCode(), K_INVALID);
    EXPECT_TRUE(results.empty());
    EXPECT_EQ(bufferProvider->allocateCount, 1);
    EXPECT_EQ(rpcClient->batchGetObjectCount, 0);
}

TEST(UbTransporterTest, BatchGetAggregateParsesDirectPayloadAndSizeChangeInOrder)
{
    auto rpcClient = std::make_shared<FakeWorkerRpcClient>();
    rpcClient->batchGetObjectResponses = {
        MakeBatchGetResponse(K_OK, 8, DataTransferSource::DATA_ALREADY_TRANSFERRED),
        MakeBatchGetResponse(K_OK, 7, DataTransferSource::DATA_IN_PAYLOAD),
        MakeBatchGetResponse(K_OC_REMOTE_GET_NOT_ENOUGH, 12, DataTransferSource::DATA_ALREADY_TRANSFERRED)
    };
    rpcClient->batchGetObjectPayloadValues = { "payload" };
    auto bufferProvider = std::make_shared<FakeUbBufferProvider>();
    bufferProvider->maxGetSize = 48;
    UbTransporter transporter(rpcClient, std::make_shared<FakeUbConnection>(), bufferProvider);
    DataGetBatchResult results;

    ASSERT_TRUE(transporter.BatchGet({ { "direct", 8 }, { "fallback", 8 }, { "changed", 8 } }, results).IsOk());

    ASSERT_EQ(results.size(), 3u);
    EXPECT_TRUE(results[0].status.IsOk());
    EXPECT_EQ(results[0].data.kind, AccessTransportKind::UB);
    EXPECT_NE(results[0].data.externalOwner, nullptr);
    EXPECT_TRUE(results[1].status.IsOk());
    EXPECT_EQ(results[1].data.kind, AccessTransportKind::TCP);
    ASSERT_EQ(results[1].data.rpcPayloads.size(), 1u);
    EXPECT_EQ(std::string(static_cast<const char *>(results[1].data.rpcPayloads[0].Data()),
                          results[1].data.rpcPayloads[0].Size()),
              "payload");
    EXPECT_EQ(results[2].status.GetCode(), K_OC_REMOTE_GET_NOT_ENOUGH);
    EXPECT_EQ(results[2].data.response.data_size(), 12);
    EXPECT_EQ(results[2].data.externalOwner, nullptr);
}

TEST(UbTransporterTest, BatchGetAggregateRejectsResponseCountMismatch)
{
    auto rpcClient = std::make_shared<FakeWorkerRpcClient>();
    rpcClient->batchGetObjectResponses = {
        MakeBatchGetResponse(K_OK, 8, DataTransferSource::DATA_ALREADY_TRANSFERRED)
    };
    auto bufferProvider = std::make_shared<FakeUbBufferProvider>();
    bufferProvider->maxGetSize = 32;
    UbTransporter transporter(rpcClient, std::make_shared<FakeUbConnection>(), bufferProvider);
    DataGetBatchResult results;

    EXPECT_EQ(transporter.BatchGet({ { "one", 8 }, { "two", 8 } }, results).GetCode(), K_RUNTIME_ERROR);
    EXPECT_TRUE(results.empty());
}

TEST(UbTransporterTest, BatchGetAggregateRejectsPayloadCountMismatchAndInvalidSource)
{
    auto rpcClient = std::make_shared<FakeWorkerRpcClient>();
    auto bufferProvider = std::make_shared<FakeUbBufferProvider>();
    bufferProvider->maxGetSize = 32;
    UbTransporter transporter(rpcClient, std::make_shared<FakeUbConnection>(), bufferProvider);
    DataGetBatchResult results;

    rpcClient->batchGetObjectResponses = {
        MakeBatchGetResponse(K_OK, 8, DataTransferSource::DATA_IN_PAYLOAD),
        MakeBatchGetResponse(K_OK, 8, DataTransferSource::DATA_ALREADY_TRANSFERRED)
    };
    EXPECT_EQ(transporter.BatchGet({ { "one", 8 }, { "two", 8 } }, results).GetCode(), K_RUNTIME_ERROR);
    EXPECT_TRUE(results.empty());

    rpcClient->batchGetObjectResponses = {
        MakeBatchGetResponse(K_OK, 8, DataTransferSource::DATA_DELAY_TRANSFER),
        MakeBatchGetResponse(K_OK, 8, DataTransferSource::DATA_ALREADY_TRANSFERRED)
    };
    rpcClient->batchGetObjectPayloadValues = { "unexpected" };
    EXPECT_EQ(transporter.BatchGet({ { "one", 8 }, { "two", 8 } }, results).GetCode(), K_RUNTIME_ERROR);
    EXPECT_TRUE(results.empty());
}

TEST(UbTransporterTest, BatchGetAllocationPressureBisectsByAlignedBytesAndPreservesOrder)
{
    auto rpcClient = std::make_shared<FakeWorkerRpcClient>();
    rpcClient->batchGetHandler = [](BatchGetObjectRemoteReqPb &request, BatchGetObjectRemoteRspPb &response,
                                    std::vector<RpcMessage> &) {
        for (const auto &itemRequest : request.requests()) {
            *response.add_responses() = MakeBatchGetResponse(
                K_OK, static_cast<int64_t>(itemRequest.data_size()), DataTransferSource::DATA_ALREADY_TRANSFERRED);
        }
        return Status::OK();
    };
    auto bufferProvider = std::make_shared<FakeUbBufferProvider>();
    bufferProvider->maxGetSize = 128;
    bufferProvider->allocateHandler = [](uint64_t size) {
        return size == 128 ? Status(K_OUT_OF_MEMORY, "aggregate allocation pressure") : Status::OK();
    };
    UbTransporter transporter(rpcClient, std::make_shared<FakeUbConnection>(), bufferProvider);
    DataGetBatchResult results;

    ASSERT_TRUE(transporter.BatchGet({ { "one", 1 }, { "two", 1 }, { "three", 1 }, { "four", 80 } },
                                     results).IsOk());

    EXPECT_EQ(bufferProvider->allocationAttempts, std::vector<uint64_t>({ 128, 80, 48 }));
    ASSERT_EQ(rpcClient->batchGetObjectRequests.size(), 2u);
    EXPECT_EQ(rpcClient->batchGetObjectRequests[0].requests_size(), 1);
    EXPECT_EQ(rpcClient->batchGetObjectRequests[1].requests_size(), 3);
    ASSERT_EQ(results.size(), 4u);
    for (size_t i = 0; i < results.size(); ++i) {
        EXPECT_TRUE(results[i].status.IsOk());
        EXPECT_EQ(results[i].data.kind, AccessTransportKind::UB);
        EXPECT_EQ(results[i].data.response.data_size(), i == 3 ? 80 : 1);
    }
}

TEST(UbTransporterTest, BatchGetAllocationPressureKeepsSuccessfulHalfWhileOtherHalfSplitsAgain)
{
    auto rpcClient = std::make_shared<FakeWorkerRpcClient>();
    rpcClient->batchGetHandler = [](BatchGetObjectRemoteReqPb &request, BatchGetObjectRemoteRspPb &response,
                                    std::vector<RpcMessage> &) {
        for (const auto &itemRequest : request.requests()) {
            *response.add_responses() = MakeBatchGetResponse(
                K_OK, static_cast<int64_t>(itemRequest.data_size()), DataTransferSource::DATA_ALREADY_TRANSFERRED);
        }
        return Status::OK();
    };
    auto bufferProvider = std::make_shared<FakeUbBufferProvider>();
    bufferProvider->maxGetSize = 160;
    bufferProvider->allocateHandler = [](uint64_t size) {
        return size > 64 ? Status(K_OUT_OF_MEMORY, "aggregate allocation pressure") : Status::OK();
    };
    UbTransporter transporter(rpcClient, std::make_shared<FakeUbConnection>(), bufferProvider);
    DataGetBatchResult results;

    ASSERT_TRUE(transporter.BatchGet(
        { { "zero", 1 }, { "one", 1 }, { "two", 1 }, { "three", 1 }, { "four", 48 }, { "five", 48 } },
        results).IsOk());

    EXPECT_EQ(bufferProvider->allocationAttempts, std::vector<uint64_t>({ 160, 96, 64, 48, 48 }));
    ASSERT_EQ(results.size(), 6u);
    std::vector<std::string> observedKeys;
    for (const auto &request : rpcClient->batchGetObjectRequests) {
        for (const auto &item : request.requests()) {
            observedKeys.emplace_back(item.object_key());
        }
    }
    EXPECT_EQ(observedKeys, std::vector<std::string>({ "zero", "one", "two", "three", "four", "five" }));
    for (const auto &result : results) {
        EXPECT_TRUE(result.status.IsOk());
        EXPECT_EQ(result.data.kind, AccessTransportKind::UB);
    }
}

TEST(UbTransporterTest, BatchGetAllocationPressureKeepsAttemptsNonIncreasingAcrossInitialChunks)
{
    auto rpcClient = std::make_shared<FakeWorkerRpcClient>();
    rpcClient->batchGetHandler = [](BatchGetObjectRemoteReqPb &request, BatchGetObjectRemoteRspPb &response,
                                    std::vector<RpcMessage> &) {
        for (const auto &itemRequest : request.requests()) {
            *response.add_responses() = MakeBatchGetResponse(
                K_OK, static_cast<int64_t>(itemRequest.data_size()), DataTransferSource::DATA_ALREADY_TRANSFERRED);
        }
        return Status::OK();
    };
    auto bufferProvider = std::make_shared<FakeUbBufferProvider>();
    bufferProvider->maxGetSize = 128;
    bufferProvider->allocateHandler = [](uint64_t size) {
        return size == 128 ? Status(K_OUT_OF_MEMORY, "aggregate allocation pressure") : Status::OK();
    };
    UbTransporter transporter(rpcClient, std::make_shared<FakeUbConnection>(), bufferProvider);
    DataGetBatchResult results;

    ASSERT_TRUE(transporter.BatchGet(
        { { "first", 64 }, { "second", 64 }, { "third", 48 }, { "fourth", 48 } }, results).IsOk());

    EXPECT_EQ(bufferProvider->allocationAttempts, std::vector<uint64_t>({ 128, 64, 64, 48, 48 }));
    EXPECT_TRUE(std::is_sorted(bufferProvider->allocationAttempts.begin(), bufferProvider->allocationAttempts.end(),
                               std::greater<uint64_t>()));
    ASSERT_EQ(results.size(), 4u);
    for (const auto &result : results) {
        EXPECT_TRUE(result.status.IsOk());
        EXPECT_EQ(result.data.kind, AccessTransportKind::UB);
    }
}

TEST(UbTransporterTest, BatchGetAllocationPressureCoalescesOrdinarySingletonChunksWithoutUnaryRpc)
{
    auto rpcClient = std::make_shared<FakeWorkerRpcClient>();
    rpcClient->batchGetObjectResponses = {
        MakeBatchGetResponse(K_OK, 48, DataTransferSource::DATA_IN_PAYLOAD),
        MakeBatchGetResponse(K_OK, 48, DataTransferSource::DATA_IN_PAYLOAD),
        MakeBatchGetResponse(K_OK, 48, DataTransferSource::DATA_IN_PAYLOAD),
        MakeBatchGetResponse(K_OK, 48, DataTransferSource::DATA_IN_PAYLOAD)
    };
    rpcClient->batchGetObjectPayloadValues = { "first", "second", "third", "fourth" };
    auto bufferProvider = std::make_shared<FakeUbBufferProvider>();
    bufferProvider->maxGetSize = 64;
    bufferProvider->allocateStatus = Status(K_OUT_OF_MEMORY, "UB pool exhausted");
    UbTransporter transporter(rpcClient, std::make_shared<FakeUbConnection>(), bufferProvider);
    DataGetBatchResult results;

    ASSERT_TRUE(transporter.BatchGet(
        { { "first", 48 }, { "second", 48 }, { "third", 48 }, { "fourth", 48 } }, results).IsOk());

    EXPECT_EQ(bufferProvider->allocationAttempts, std::vector<uint64_t>({ 48, 48, 48, 48 }));
    EXPECT_EQ(rpcClient->getObjectCount, 0);
    ASSERT_EQ(rpcClient->batchGetObjectRequests.size(), 1u);
    const auto &fallbackRequest = rpcClient->batchGetObjectRequests.front();
    ASSERT_EQ(fallbackRequest.requests_size(), 4);
    for (const auto &request : fallbackRequest.requests()) {
        EXPECT_FALSE(request.has_urma_info());
    }
    ASSERT_EQ(results.size(), 4u);
    for (const auto &result : results) {
        EXPECT_TRUE(result.status.IsOk());
        EXPECT_EQ(result.data.kind, AccessTransportKind::TCP);
    }
}

TEST(UbTransporterTest, BatchGetAllocationPressureBatchesSingletonTcpFallbackAndMapsOriginalIndexes)
{
    InitBatchGetMetrics();
    auto rpcClient = std::make_shared<FakeWorkerRpcClient>();
    rpcClient->batchGetObjectResponses = {
        MakeBatchGetResponse(K_OK, 1, DataTransferSource::DATA_IN_PAYLOAD),
        MakeBatchGetResponse(K_NOT_FOUND, 0, DataTransferSource::DATA_IN_PAYLOAD),
        MakeBatchGetResponse(K_OK, 1, DataTransferSource::DATA_IN_PAYLOAD),
        MakeBatchGetResponse(K_OK, 1, DataTransferSource::DATA_IN_PAYLOAD)
    };
    rpcClient->batchGetObjectPayloadValues = { "a", "c", "d" };
    auto bufferProvider = std::make_shared<FakeUbBufferProvider>();
    bufferProvider->maxGetSize = 64;
    bufferProvider->allocateStatus = Status(K_OUT_OF_MEMORY, "UB pool exhausted");
    UbTransporter transporter(rpcClient, std::make_shared<FakeUbConnection>(), bufferProvider);
    DataGetBatchResult results;

    ASSERT_TRUE(transporter.BatchGet({ { "first", 1 }, { "second", 1 }, { "third", 1 }, { "fourth", 1 } },
                                     results).IsOk());

    EXPECT_EQ(bufferProvider->allocationAttempts, std::vector<uint64_t>({ 64, 32, 32, 16, 16, 16, 16 }));
    EXPECT_TRUE(std::is_sorted(bufferProvider->allocationAttempts.begin(), bufferProvider->allocationAttempts.end(),
                               std::greater<uint64_t>()));
    ASSERT_EQ(rpcClient->batchGetObjectRequests.size(), 1u);
    const auto &fallbackRequest = rpcClient->batchGetObjectRequests.front();
    ASSERT_EQ(fallbackRequest.requests_size(), 4);
    for (int i = 0; i < fallbackRequest.requests_size(); ++i) {
        EXPECT_FALSE(fallbackRequest.requests(i).has_urma_info());
    }
    ASSERT_EQ(results.size(), 4u);
    EXPECT_TRUE(results[0].status.IsOk());
    EXPECT_EQ(results[1].status.GetCode(), K_NOT_FOUND);
    EXPECT_TRUE(results[2].status.IsOk());
    EXPECT_TRUE(results[3].status.IsOk());
    EXPECT_EQ(results[0].data.kind, AccessTransportKind::TCP);
    EXPECT_EQ(results[2].data.kind, AccessTransportKind::TCP);
    EXPECT_EQ(results[3].data.kind, AccessTransportKind::TCP);
    ExpectMetricTotal("client_direct_batch_get_rpc_total", 1);
    ExpectMetricTotal("client_direct_batch_get_object_total", 4);
    ExpectMetricTotal("client_direct_batch_get_ub_split_total", 3);
    ExpectMetricTotal("client_direct_batch_get_tcp_fallback_total", 4);
}

TEST(UbTransporterTest, BatchGetAllocationPressureScopesTcpRpcFailureToFallbackItems)
{
    auto rpcClient = std::make_shared<FakeWorkerRpcClient>();
    rpcClient->batchGetHandler = [](BatchGetObjectRemoteReqPb &request, BatchGetObjectRemoteRspPb &response,
                                    std::vector<RpcMessage> &) {
        if (!request.requests(0).has_urma_info()) {
            return Status(K_RPC_UNAVAILABLE, "TCP fallback unavailable");
        }
        for (const auto &itemRequest : request.requests()) {
            *response.add_responses() = MakeBatchGetResponse(
                K_OK, static_cast<int64_t>(itemRequest.data_size()), DataTransferSource::DATA_ALREADY_TRANSFERRED);
        }
        return Status::OK();
    };
    auto bufferProvider = std::make_shared<FakeUbBufferProvider>();
    bufferProvider->maxGetSize = 128;
    bufferProvider->allocateHandler = [](uint64_t size) {
        return size == 16 || size == 32 || size == 80 || size == 128
                   ? Status(K_OUT_OF_MEMORY, "aggregate allocation pressure")
                   : Status::OK();
    };
    UbTransporter transporter(rpcClient, std::make_shared<FakeUbConnection>(), bufferProvider);
    DataGetBatchResult results;

    ASSERT_TRUE(transporter.BatchGet({ { "fallback-0", 1 }, { "fallback-1", 1 }, { "ub-2", 48 }, { "ub-3", 48 } },
                                     results).IsOk());

    ASSERT_EQ(results.size(), 4u);
    EXPECT_EQ(results[0].status.GetCode(), K_RPC_UNAVAILABLE);
    EXPECT_EQ(results[1].status.GetCode(), K_RPC_UNAVAILABLE);
    EXPECT_TRUE(results[2].status.IsOk());
    EXPECT_TRUE(results[3].status.IsOk());
    EXPECT_EQ(results[2].data.kind, AccessTransportKind::UB);
    EXPECT_EQ(results[3].data.kind, AccessTransportKind::UB);
    ASSERT_GE(rpcClient->batchGetObjectRequests.size(), 3u);
    const auto &fallbackRequest = rpcClient->batchGetObjectRequests.back();
    ASSERT_EQ(fallbackRequest.requests_size(), 2);
    EXPECT_FALSE(fallbackRequest.requests(0).has_urma_info());
    EXPECT_FALSE(fallbackRequest.requests(1).has_urma_info());
}

TEST(UbTransporterTest, BatchGetAllocationPressurePreservesUnarySizeRetryAndMaxPreSplit)
{
    auto rpcClient = std::make_shared<FakeWorkerRpcClient>();
    rpcClient->getObjectDataSize = 24;
    auto bufferProvider = std::make_shared<FakeUbBufferProvider>();
    bufferProvider->maxGetSize = 32;
    UbTransporter transporter(rpcClient, std::make_shared<FakeUbConnection>(), bufferProvider);
    DataGetBatchResult results;

    ASSERT_TRUE(transporter.BatchGet({ { "sentinel", 16 } }, results).IsOk());
    EXPECT_EQ(bufferProvider->allocationAttempts, std::vector<uint64_t>({ 16, 24 }));
    ASSERT_EQ(results.size(), 1u);
    EXPECT_TRUE(results[0].status.IsOk());

    bufferProvider->allocationAttempts.clear();
    rpcClient->getObjectDataSize = 48;
    rpcClient->batchGetHandler = [](BatchGetObjectRemoteReqPb &request, BatchGetObjectRemoteRspPb &response,
                                    std::vector<RpcMessage> &) {
        for (const auto &itemRequest : request.requests()) {
            *response.add_responses() = MakeBatchGetResponse(
                K_OK, static_cast<int64_t>(itemRequest.data_size()), DataTransferSource::DATA_ALREADY_TRANSFERRED);
        }
        return Status::OK();
    };
    ASSERT_TRUE(transporter.BatchGet({ { "oversize", 48 }, { "small-a", 16 }, { "small-b", 16 } }, results).IsOk());
    ASSERT_FALSE(bufferProvider->allocationAttempts.empty());
    EXPECT_TRUE(std::all_of(bufferProvider->allocationAttempts.begin(), bufferProvider->allocationAttempts.end(),
                            [&](uint64_t size) { return size <= bufferProvider->maxGetSize; }));
}

TEST(UbTransporterTest, BatchGetAggregateCloseWaitsForRpcAndResultSetup)
{
    auto rpcClient = std::make_shared<FakeWorkerRpcClient>();
    auto connection = std::make_shared<FakeUbConnection>();
    std::atomic<bool> invokeFinished{ false };
    connection->invokeFinished = &invokeFinished;
    std::promise<void> invokeStarted;
    auto invokeStartedFuture = invokeStarted.get_future();
    std::promise<void> allowInvoke;
    auto allowInvokeFuture = allowInvoke.get_future().share();
    rpcClient->onBatchInvoke = [&invokeStarted, allowInvokeFuture]() {
        invokeStarted.set_value();
        allowInvokeFuture.wait();
    };
    rpcClient->afterBatchInvoke = [&invokeFinished]() { invokeFinished.store(true); };
    rpcClient->batchGetObjectResponses = {
        MakeBatchGetResponse(K_OK, 8, DataTransferSource::DATA_ALREADY_TRANSFERRED),
        MakeBatchGetResponse(K_OK, 8, DataTransferSource::DATA_ALREADY_TRANSFERRED)
    };

    auto bufferProvider = std::make_shared<FakeUbBufferProvider>();
    bufferProvider->maxGetSize = 32;
    UbTransporter transporter(rpcClient, connection, bufferProvider);
    DataGetBatchResult results;
    Status batchStatus;
    std::thread batchThread([&]() { batchStatus = transporter.BatchGet({ { "one", 8 }, { "two", 8 } }, results); });
    if (invokeStartedFuture.wait_for(std::chrono::seconds(2)) != std::future_status::ready) {
        batchThread.join();
        FAIL() << "BatchGet did not invoke the batch RPC";
        return;
    }
    std::thread closeThread([&]() { transporter.CloseDataPlane(); });
    allowInvoke.set_value();
    batchThread.join();
    closeThread.join();

    EXPECT_TRUE(batchStatus.IsOk());
    EXPECT_FALSE(connection->teardownDuringInvoke.load());
    ASSERT_EQ(results.size(), 2u);
    EXPECT_NE(results[0].data.externalOwner, nullptr);
    EXPECT_NE(results[1].data.externalOwner, nullptr);
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

TEST(DataPlaneExecutorTest, BatchGetConnectionRetryMetricsCountBothActualRpcAttempts)
{
    InitBatchGetMetrics();
    auto manager = std::make_shared<FakeDataPlaneManager>();
    int configured = 0;
    manager->configureTransporter = [&configured](const HostPort &, FakeTransporter &transporter) {
        auto rpcClient = std::static_pointer_cast<FakeWorkerRpcClient>(transporter.rpcClient);
        if (++configured == 1) {
            rpcClient->batchGetObjectStatus = Status(K_RPC_UNAVAILABLE, "first attempt unavailable");
        } else {
            rpcClient->batchGetObjectResponses = {
                MakeBatchGetResponse(K_OK, 1, DataTransferSource::DATA_IN_PAYLOAD),
                MakeBatchGetResponse(K_OK, 1, DataTransferSource::DATA_IN_PAYLOAD)
            };
            rpcClient->batchGetObjectPayloadValues = { "a", "b" };
        }
        transporter.batchGetHandler = [rpcClient](const DataGetBatchRequest &inputs, DataGetBatchResult &outputs) {
            TcpTransporter tcp(rpcClient);
            return tcp.BatchGet(inputs, outputs);
        };
    };
    DataPlaneExecutor executor(manager, std::make_shared<TransportAdvisor>());
    DataGetBatchRequest request{ { "one", 1 }, { "two", 1 } };
    DataGetBatchResult results;
    int operationCount = 0;

    ASSERT_TRUE(executor.Execute(MakeAddress(78), [&](IDataTransporter &transporter) {
        ++operationCount;
        return transporter.BatchGet(request, results);
    }).IsOk());

    EXPECT_EQ(operationCount, 2);
    EXPECT_EQ(manager->rpcBuildCount, 2);
    EXPECT_EQ(manager->transportBuildCount, 2);
    ExpectMetricTotal("client_direct_batch_get_rpc_total", 2);
    ExpectMetricTotal("client_direct_batch_get_object_total", 4);
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
    ReplicaReader reader(executor, std::make_shared<DeadlineRetry>(), std::make_shared<ThreadPool>(1));
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
    ReplicaReader reader(executor, std::make_shared<DeadlineRetry>(), std::make_shared<ThreadPool>(1));
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
    ReplicaReader reader(executor, std::make_shared<DeadlineRetry>(), std::make_shared<ThreadPool>(1));
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

TEST(ReplicaReaderTest, BatchSameAddressUsesOneBatchAndPreservesCallerOrder)
{
    ApiDeadlineGuard deadline(1000);
    auto manager = std::make_shared<FakeDataPlaneManager>();
    auto executor = std::make_shared<DataPlaneExecutor>(manager, std::make_shared<TransportAdvisor>());
    ReplicaReader reader(executor, std::make_shared<DeadlineRetry>(), std::make_shared<ThreadPool>(2));
    std::vector<master::ObjectLocationInfoPb> locations(3);
    std::vector<ObjectReadItemResult> results(3);
    ReplicaReadBatch requests;
    for (size_t i = 0; i < locations.size(); ++i) {
        locations[i].set_object_key("key-" + std::to_string(i));
        locations[i].set_object_size(i + 1);
        locations[i].add_object_locations(MakeAddress(40).ToString());
        requests.push_back({ &locations[i], &results[i] });
    }

    ASSERT_TRUE(reader.ReadBatch(requests).IsOk());
    ASSERT_EQ(manager->builtTransporters.size(), 1u);
    const auto &transporter = manager->builtTransporters.front();
    ASSERT_EQ(transporter->batchGetRequests.size(), 1u);
    ASSERT_EQ(transporter->batchGetRequests.front().size(), 3u);
    for (size_t i = 0; i < results.size(); ++i) {
        EXPECT_EQ(transporter->batchGetRequests.front()[i].objectKey, "key-" + std::to_string(i));
        EXPECT_TRUE(results[i].status.IsOk());
        EXPECT_EQ(results[i].objectKey, "key-" + std::to_string(i));
    }
}

TEST(ReplicaReaderTest, BatchDifferentAddressesExecuteConcurrentlyWithDisjointResults)
{
    ApiDeadlineGuard deadline(2000);
    auto manager = std::make_shared<FakeDataPlaneManager>();
    std::promise<void> bothStarted;
    auto bothStartedFuture = bothStarted.get_future();
    std::promise<void> release;
    auto releaseFuture = release.get_future().share();
    std::atomic<int> started{ 0 };
    manager->configureTransporter = [&](const HostPort &, FakeTransporter &transporter) {
        transporter.batchGetHandler = [&](const DataGetBatchRequest &inputs, DataGetBatchResult &outputs) {
            if (started.fetch_add(1) + 1 == 2) {
                bothStarted.set_value();
            }
            releaseFuture.wait();
            outputs.resize(inputs.size());
            for (auto &output : outputs) {
                output.status = Status::OK();
            }
            return Status::OK();
        };
    };
    auto executor = std::make_shared<DataPlaneExecutor>(manager, std::make_shared<TransportAdvisor>());
    ReplicaReader reader(executor, std::make_shared<DeadlineRetry>(), std::make_shared<ThreadPool>(2));
    std::vector<master::ObjectLocationInfoPb> locations = {
        MakeReplicaLocation("a0", 1, { MakeAddress(41) }), MakeReplicaLocation("a1", 1, { MakeAddress(41) }),
        MakeReplicaLocation("b0", 1, { MakeAddress(42) }), MakeReplicaLocation("b1", 1, { MakeAddress(42) })
    };
    std::vector<ObjectReadItemResult> results(locations.size());
    ReplicaReadBatch requests;
    for (size_t i = 0; i < locations.size(); ++i) {
        requests.push_back({ &locations[i], &results[i] });
    }

    auto readFuture = std::async(std::launch::async, [&]() { return reader.ReadBatch(requests); });
    const bool concurrent = bothStartedFuture.wait_for(std::chrono::seconds(2)) == std::future_status::ready;
    release.set_value();
    ASSERT_TRUE(concurrent);
    ASSERT_TRUE(readFuture.get().IsOk());
    EXPECT_EQ(started.load(), 2);
    for (const auto &result : results) {
        EXPECT_TRUE(result.status.IsOk());
    }
}

TEST(ReplicaReaderTest, BatchEndpointFailureDoesNotDiscardPeerSuccess)
{
    ApiDeadlineGuard deadline(1000);
    auto manager = std::make_shared<FakeDataPlaneManager>();
    manager->configureTransporter = [](const HostPort &address, FakeTransporter &transporter) {
        transporter.batchGetHandler = [address](const DataGetBatchRequest &inputs, DataGetBatchResult &outputs) {
            if (address.ToString() == MakeAddress(43).ToString()) {
                return Status(K_INVALID, "endpoint rejected request");
            }
            outputs.resize(inputs.size());
            for (auto &output : outputs) {
                output.status = Status::OK();
            }
            return Status::OK();
        };
    };
    auto executor = std::make_shared<DataPlaneExecutor>(manager, std::make_shared<TransportAdvisor>());
    ReplicaReader reader(executor, std::make_shared<DeadlineRetry>(), std::make_shared<ThreadPool>(2));
    std::vector<master::ObjectLocationInfoPb> locations = {
        MakeReplicaLocation("bad-0", 1, { MakeAddress(43) }), MakeReplicaLocation("bad-1", 1, { MakeAddress(43) }),
        MakeReplicaLocation("good-0", 1, { MakeAddress(44) }), MakeReplicaLocation("good-1", 1, { MakeAddress(44) })
    };
    std::vector<ObjectReadItemResult> results(locations.size());
    ReplicaReadBatch requests;
    for (size_t i = 0; i < locations.size(); ++i) {
        requests.push_back({ &locations[i], &results[i] });
    }

    EXPECT_TRUE(reader.ReadBatch(requests).IsOk());
    EXPECT_EQ(results[0].status.GetCode(), K_INVALID);
    EXPECT_EQ(results[1].status.GetCode(), K_INVALID);
    EXPECT_TRUE(results[2].status.IsOk());
    EXPECT_TRUE(results[3].status.IsOk());
}

TEST(ReplicaReaderTest, BatchRetryableItemsRegroupAtNextReplicaAndSuccessfulPeerLeaves)
{
    InitBatchGetMetrics();
    ApiDeadlineGuard deadline(1000);
    auto manager = std::make_shared<FakeDataPlaneManager>();
    manager->configureTransporter = [](const HostPort &address, FakeTransporter &transporter) {
        if (address.ToString() == MakeAddress(45).ToString()
            || address.ToString() == MakeAddress(46).ToString()) {
            transporter.getStatuses = { Status(K_NOT_FOUND, "first replica missing") };
        }
    };
    auto executor = std::make_shared<DataPlaneExecutor>(manager, std::make_shared<TransportAdvisor>());
    ReplicaReader reader(executor, std::make_shared<DeadlineRetry>(), std::make_shared<ThreadPool>(3));
    std::vector<master::ObjectLocationInfoPb> locations = {
        MakeReplicaLocation("retry-a", 1, { MakeAddress(45), MakeAddress(48) }),
        MakeReplicaLocation("retry-b", 1, { MakeAddress(46), MakeAddress(48) }),
        MakeReplicaLocation("done", 1, { MakeAddress(47) })
    };
    std::vector<ObjectReadItemResult> results(locations.size());
    ReplicaReadBatch requests;
    for (size_t i = 0; i < locations.size(); ++i) {
        requests.push_back({ &locations[i], &results[i] });
    }

    ASSERT_TRUE(reader.ReadBatch(requests).IsOk());
    std::shared_ptr<FakeTransporter> regrouped;
    for (const auto &transporter : manager->builtTransporters) {
        if (!transporter->batchGetRequests.empty()) {
            regrouped = transporter;
        }
    }
    ASSERT_NE(regrouped, nullptr);
    ASSERT_EQ(regrouped->batchGetRequests.size(), 1u);
    ASSERT_EQ(regrouped->batchGetRequests.front().size(), 2u);
    EXPECT_EQ(regrouped->batchGetRequests.front()[0].objectKey, "retry-a");
    EXPECT_EQ(regrouped->batchGetRequests.front()[1].objectKey, "retry-b");
    for (const auto &result : results) {
        EXPECT_TRUE(result.status.IsOk());
    }
    ExpectMetricTotal("client_direct_batch_get_replica_retry_total", 2);
}

TEST(ReplicaReaderTest, BatchSizeChangeRetriesSameReplicaWithUpdatedExpectedSize)
{
    InitBatchGetMetrics();
    ApiDeadlineGuard deadline(1000);
    auto manager = std::make_shared<FakeDataPlaneManager>();
    std::atomic<int> attempts{ 0 };
    manager->configureTransporter = [&](const HostPort &, FakeTransporter &transporter) {
        transporter.batchGetHandler = [&](const DataGetBatchRequest &inputs, DataGetBatchResult &outputs) {
            outputs.resize(inputs.size());
            for (auto &output : outputs) {
                output.status = Status::OK();
            }
            outputs[0].status = Status(K_OC_REMOTE_GET_NOT_ENOUGH, "size changed");
            outputs[0].data.response.set_data_size(8);
            ++attempts;
            return Status::OK();
        };
        transporter.getHandler = [&](const DataGetRequest &input, DataGetResult &) {
            ++attempts;
            EXPECT_EQ(input.expectedSize, 8u);
            return Status::OK();
        };
    };
    auto executor = std::make_shared<DataPlaneExecutor>(manager, std::make_shared<TransportAdvisor>());
    ReplicaReader reader(executor, std::make_shared<DeadlineRetry>(), std::make_shared<ThreadPool>(2));
    std::vector<master::ObjectLocationInfoPb> locations = {
        MakeReplicaLocation("changed", 4, { MakeAddress(49) }),
        MakeReplicaLocation("peer", 4, { MakeAddress(49) })
    };
    std::vector<ObjectReadItemResult> results(locations.size());

    ASSERT_TRUE(reader.ReadBatch({ { &locations[0], &results[0] }, { &locations[1], &results[1] } }).IsOk());
    EXPECT_EQ(attempts.load(), 2);
    EXPECT_TRUE(results[0].status.IsOk());
    EXPECT_TRUE(results[1].status.IsOk());
    ExpectMetricAbsent("client_direct_batch_get_replica_retry_total");
}

TEST(ReplicaReaderTest, BatchUnarySizeChangeRetriesSameReplicaWithUpdatedExpectedSize)
{
    ApiDeadlineGuard deadline(1000);
    auto manager = std::make_shared<FakeDataPlaneManager>();
    std::atomic<int> attempts{ 0 };
    manager->configureTransporter = [&](const HostPort &, FakeTransporter &transporter) {
        transporter.getHandler = [&](const DataGetRequest &input, DataGetResult &output) {
            const int attempt = attempts.fetch_add(1);
            if (attempt == 0) {
                EXPECT_EQ(input.expectedSize, 4u);
                output.response.set_data_size(8);
                return Status(K_OC_REMOTE_GET_NOT_ENOUGH, "size changed");
            }
            EXPECT_EQ(input.expectedSize, 8u);
            return Status::OK();
        };
    };
    auto executor = std::make_shared<DataPlaneExecutor>(manager, std::make_shared<TransportAdvisor>());
    ControlledReplicaReader reader(executor, std::make_shared<ThreadPool>(1));
    auto location = MakeReplicaLocation("changed", 4, { MakeAddress(70) });
    ObjectReadItemResult result;

    ASSERT_TRUE(reader.ReadBatch({ { &location, &result } }).IsOk());
    EXPECT_EQ(attempts.load(), 2);
    EXPECT_EQ(reader.backoffCount, 0);
    EXPECT_TRUE(result.status.IsOk());
}

TEST(ReplicaReaderTest, BatchUnchangedSizeErrorAdvancesReplicaInsteadOfSpinning)
{
    ApiDeadlineGuard deadline(1000);
    auto manager = std::make_shared<FakeDataPlaneManager>();
    manager->configureTransporter = [](const HostPort &address, FakeTransporter &transporter) {
        if (address.ToString() == MakeAddress(50).ToString()) {
            transporter.getHandler = [](const DataGetRequest &input, DataGetResult &output) {
                output.response.set_data_size(input.expectedSize);
                return Status(K_OC_REMOTE_GET_NOT_ENOUGH, "unchanged size");
            };
        }
    };
    auto executor = std::make_shared<DataPlaneExecutor>(manager, std::make_shared<TransportAdvisor>());
    ReplicaReader reader(executor, std::make_shared<DeadlineRetry>(), std::make_shared<ThreadPool>(2));
    auto location = MakeReplicaLocation("key", 4, { MakeAddress(50), MakeAddress(51) });
    ObjectReadItemResult result;

    ASSERT_TRUE(reader.ReadBatch({ { &location, &result } }).IsOk());
    EXPECT_TRUE(result.status.IsOk());
    EXPECT_EQ(manager->transportBuildCount, 2);
}

TEST(ReplicaReaderTest, BatchChunksByObjectCountAndExpectedBytesAndUsesUnaryForSingletons)
{
    ApiDeadlineGuard deadline(2000);
    auto manager = std::make_shared<FakeDataPlaneManager>();
    auto executor = std::make_shared<DataPlaneExecutor>(manager, std::make_shared<TransportAdvisor>());
    ReplicaReader reader(executor, std::make_shared<DeadlineRetry>(), std::make_shared<ThreadPool>(2));
    std::vector<master::ObjectLocationInfoPb> countLocations;
    std::vector<ObjectReadItemResult> countResults(1025);
    ReplicaReadBatch countRequests;
    countLocations.reserve(1025);
    for (size_t i = 0; i < 1025; ++i) {
        countLocations.emplace_back(MakeReplicaLocation("count-" + std::to_string(i), 1, { MakeAddress(52) }));
        countRequests.push_back({ &countLocations.back(), &countResults[i] });
    }
    ASSERT_TRUE(reader.ReadBatch(countRequests).IsOk());
    ASSERT_EQ(manager->builtTransporters.size(), 1u);
    EXPECT_EQ(manager->builtTransporters[0]->batchGetRequests.size(), 1u);
    EXPECT_EQ(manager->builtTransporters[0]->batchGetRequests[0].size(), 1024u);
    EXPECT_EQ(manager->builtTransporters[0]->getCount, 1);

    auto byteManager = std::make_shared<FakeDataPlaneManager>();
    auto byteExecutor = std::make_shared<DataPlaneExecutor>(byteManager, std::make_shared<TransportAdvisor>());
    ReplicaReader byteReader(byteExecutor, std::make_shared<DeadlineRetry>(), std::make_shared<ThreadPool>(2));
    const uint64_t mib = 1024ULL * 1024ULL;
    std::vector<master::ObjectLocationInfoPb> byteLocations = {
        MakeReplicaLocation("large", 60 * mib, { MakeAddress(53) }),
        MakeReplicaLocation("medium", 50 * mib, { MakeAddress(53) }),
        MakeReplicaLocation("small", 1, { MakeAddress(53) })
    };
    std::vector<ObjectReadItemResult> byteResults(3);
    ASSERT_TRUE(byteReader.ReadBatch({ { &byteLocations[0], &byteResults[0] },
                                       { &byteLocations[1], &byteResults[1] },
                                       { &byteLocations[2], &byteResults[2] } }).IsOk());
    ASSERT_EQ(byteManager->builtTransporters.size(), 1u);
    EXPECT_EQ(byteManager->builtTransporters[0]->getCount, 1);
    ASSERT_EQ(byteManager->builtTransporters[0]->batchGetRequests.size(), 1u);
    EXPECT_EQ(byteManager->builtTransporters[0]->batchGetRequests[0].size(), 2u);
}

TEST(ReplicaReaderTest, BatchObjectLargerThanByteCapFormsUnaryChunk)
{
    ApiDeadlineGuard deadline(1000);
    auto manager = std::make_shared<FakeDataPlaneManager>();
    auto executor = std::make_shared<DataPlaneExecutor>(manager, std::make_shared<TransportAdvisor>());
    ReplicaReader reader(executor, std::make_shared<DeadlineRetry>(), std::make_shared<ThreadPool>(2));
    const uint64_t mib = 1024ULL * 1024ULL;
    std::vector<master::ObjectLocationInfoPb> locations = {
        MakeReplicaLocation("oversized", 101 * mib, { MakeAddress(71) }),
        MakeReplicaLocation("small-a", 1, { MakeAddress(71) }),
        MakeReplicaLocation("small-b", 1, { MakeAddress(71) })
    };
    std::vector<ObjectReadItemResult> results(3);

    ASSERT_TRUE(reader.ReadBatch({ { &locations[0], &results[0] },
                                   { &locations[1], &results[1] },
                                   { &locations[2], &results[2] } }).IsOk());
    ASSERT_EQ(manager->builtTransporters.size(), 1u);
    const auto &transporter = manager->builtTransporters[0];
    ASSERT_EQ(transporter->getRequests.size(), 1u);
    EXPECT_EQ(transporter->getRequests[0].objectKey, "oversized");
    ASSERT_EQ(transporter->batchGetRequests.size(), 1u);
    ASSERT_EQ(transporter->batchGetRequests[0].size(), 2u);
    EXPECT_EQ(transporter->batchGetRequests[0][0].objectKey, "small-a");
    EXPECT_EQ(transporter->batchGetRequests[0][1].objectKey, "small-b");
}

TEST(ReplicaReaderTest, BatchMixedItemStatusesTransitionIndependently)
{
    ApiDeadlineGuard deadline(1000);
    auto manager = std::make_shared<FakeDataPlaneManager>();
    manager->configureTransporter = [](const HostPort &address, FakeTransporter &transporter) {
        if (address.ToString() == MakeAddress(72).ToString()) {
            transporter.batchGetHandler = [](const DataGetBatchRequest &inputs, DataGetBatchResult &outputs) {
                EXPECT_EQ(inputs.size(), 3u);
                outputs.resize(3);
                outputs[0].status = Status::OK();
                outputs[1].status = Status(K_NOT_FOUND, "retry next replica");
                outputs[2].status = Status(K_INVALID, "terminal");
                return Status::OK();
            };
        }
    };
    auto executor = std::make_shared<DataPlaneExecutor>(manager, std::make_shared<TransportAdvisor>());
    ReplicaReader reader(executor, std::make_shared<DeadlineRetry>(), std::make_shared<ThreadPool>(2));
    std::vector<master::ObjectLocationInfoPb> locations = {
        MakeReplicaLocation("success", 1, { MakeAddress(72) }),
        MakeReplicaLocation("retry", 1, { MakeAddress(72), MakeAddress(73) }),
        MakeReplicaLocation("terminal", 1, { MakeAddress(72), MakeAddress(74) })
    };
    std::vector<ObjectReadItemResult> results(3);

    ASSERT_TRUE(reader.ReadBatch({ { &locations[0], &results[0] },
                                   { &locations[1], &results[1] },
                                   { &locations[2], &results[2] } }).IsOk());
    EXPECT_TRUE(results[0].status.IsOk());
    EXPECT_TRUE(results[1].status.IsOk());
    EXPECT_EQ(results[2].status.GetCode(), K_INVALID);
    EXPECT_EQ(manager->transportBuildCount, 2);
}

TEST(ReplicaReaderTest, BatchInvalidItemsDoNotCorruptValidPeer)
{
    ApiDeadlineGuard deadline(1000);
    auto manager = std::make_shared<FakeDataPlaneManager>();
    auto executor = std::make_shared<DataPlaneExecutor>(manager, std::make_shared<TransportAdvisor>());
    ReplicaReader reader(executor, std::make_shared<DeadlineRetry>(), std::make_shared<ThreadPool>(1));
    master::ObjectLocationInfoPb emptyLocation;
    auto validLocation = MakeReplicaLocation("valid", 1, { MakeAddress(54) });
    ObjectReadItemResult emptyResult;
    ObjectReadItemResult validResult;

    EXPECT_TRUE(reader.ReadBatch({ { &emptyLocation, &emptyResult }, { nullptr, nullptr },
                                   { &validLocation, &validResult } }).IsOk());
    EXPECT_EQ(emptyResult.status.GetCode(), K_NOT_FOUND);
    EXPECT_TRUE(validResult.status.IsOk());
}

TEST(ReplicaReaderTest, BatchNonRetryableItemTerminatesWithoutTryingNextReplica)
{
    ApiDeadlineGuard deadline(1000);
    auto manager = std::make_shared<FakeDataPlaneManager>();
    manager->configureTransporter = [](const HostPort &address, FakeTransporter &transporter) {
        if (address.ToString() == MakeAddress(59).ToString()) {
            transporter.getStatuses = { Status(K_INVALID, "terminal") };
        }
    };
    auto executor = std::make_shared<DataPlaneExecutor>(manager, std::make_shared<TransportAdvisor>());
    ReplicaReader reader(executor, std::make_shared<DeadlineRetry>(), std::make_shared<ThreadPool>(1));
    auto location = MakeReplicaLocation("key", 1, { MakeAddress(59), MakeAddress(60) });
    ObjectReadItemResult result;

    EXPECT_EQ(reader.ReadBatch({ { &location, &result } }).GetCode(), K_INVALID);
    EXPECT_EQ(result.status.GetCode(), K_INVALID);
    EXPECT_EQ(manager->transportBuildCount, 1);
}

TEST(ReplicaReaderTest, BatchBacksOffOnceAfterAllUnresolvedItemsCompleteReplicaRound)
{
    ApiDeadlineGuard deadline(1000);
    auto manager = std::make_shared<FakeDataPlaneManager>();
    std::unordered_map<std::string, std::shared_ptr<std::atomic<int>>> calls;
    for (int port : { 61, 62, 63, 64 }) {
        calls.emplace(MakeAddress(port).ToString(), std::make_shared<std::atomic<int>>(0));
    }
    manager->configureTransporter = [&](const HostPort &address, FakeTransporter &transporter) {
        auto count = calls.at(address.ToString());
        const bool firstReplica = address.ToString() == MakeAddress(61).ToString()
                                  || address.ToString() == MakeAddress(63).ToString();
        transporter.getHandler = [count, firstReplica](const DataGetRequest &, DataGetResult &) {
            const int invocation = count->fetch_add(1);
            return firstReplica && invocation > 0 ? Status::OK() : Status(K_NOT_FOUND, "round miss");
        };
    };
    auto executor = std::make_shared<DataPlaneExecutor>(manager, std::make_shared<TransportAdvisor>());
    ControlledReplicaReader reader(executor, std::make_shared<ThreadPool>(4));
    std::vector<master::ObjectLocationInfoPb> locations = {
        MakeReplicaLocation("a", 1, { MakeAddress(61), MakeAddress(62) }),
        MakeReplicaLocation("b", 1, { MakeAddress(63), MakeAddress(64) })
    };
    std::vector<ObjectReadItemResult> results(2);

    ASSERT_TRUE(reader.ReadBatch({ { &locations[0], &results[0] }, { &locations[1], &results[1] } }).IsOk());
    EXPECT_EQ(reader.backoffCount, 1);
    EXPECT_EQ(calls.at(MakeAddress(61).ToString())->load(), 2);
    EXPECT_EQ(calls.at(MakeAddress(63).ToString())->load(), 2);
}

TEST(ReplicaReaderTest, BatchDifferingReplicaCountsShareBackoffAfterLongestRoundCompletes)
{
    ApiDeadlineGuard deadline(1000);
    auto manager = std::make_shared<FakeDataPlaneManager>();
    auto shortCalls = std::make_shared<std::atomic<int>>(0);
    auto firstLongCalls = std::make_shared<std::atomic<int>>(0);
    auto secondLongCalls = std::make_shared<std::atomic<int>>(0);
    manager->configureTransporter = [=](const HostPort &address, FakeTransporter &transporter) {
        if (address.ToString() == MakeAddress(75).ToString()) {
            transporter.getHandler = [shortCalls](const DataGetRequest &, DataGetResult &) {
                return shortCalls->fetch_add(1) == 0 ? Status(K_NOT_FOUND, "short round miss") : Status::OK();
            };
        } else if (address.ToString() == MakeAddress(76).ToString()) {
            transporter.getHandler = [firstLongCalls](const DataGetRequest &, DataGetResult &) {
                return firstLongCalls->fetch_add(1) == 0 ? Status(K_NOT_FOUND, "first long miss") : Status::OK();
            };
        } else {
            transporter.getHandler = [secondLongCalls](const DataGetRequest &, DataGetResult &) {
                ++(*secondLongCalls);
                return Status(K_NOT_FOUND, "second long miss");
            };
        }
    };
    auto executor = std::make_shared<DataPlaneExecutor>(manager, std::make_shared<TransportAdvisor>());
    ControlledReplicaReader reader(executor, std::make_shared<ThreadPool>(3));
    std::vector<master::ObjectLocationInfoPb> locations = {
        MakeReplicaLocation("short", 1, { MakeAddress(75) }),
        MakeReplicaLocation("long", 1, { MakeAddress(76), MakeAddress(77) })
    };
    std::vector<ObjectReadItemResult> results(2);

    ASSERT_TRUE(reader.ReadBatch({ { &locations[0], &results[0] }, { &locations[1], &results[1] } }).IsOk());
    EXPECT_EQ(reader.backoffCount, 1);
    EXPECT_EQ(shortCalls->load(), 2);
    EXPECT_EQ(firstLongCalls->load(), 2);
    EXPECT_EQ(secondLongCalls->load(), 1);
}

TEST(ReplicaReaderTest, BatchDeadlineReturnsLastMeaningfulItemError)
{
    ApiDeadlineGuard deadline(1000);
    auto manager = std::make_shared<FakeDataPlaneManager>();
    manager->configureTransporter = [](const HostPort &, FakeTransporter &transporter) {
        transporter.getStatuses = { Status(K_NOT_FOUND, "meaningful miss") };
    };
    auto executor = std::make_shared<DataPlaneExecutor>(manager, std::make_shared<TransportAdvisor>());
    ControlledReplicaReader reader(executor, std::make_shared<ThreadPool>(1));
    reader.deadlineStatuses = { Status::OK(), Status(K_RPC_DEADLINE_EXCEEDED, "deadline") };
    auto location = MakeReplicaLocation("key", 1, { MakeAddress(65) });
    ObjectReadItemResult result;

    Status status = reader.ReadBatch({ { &location, &result } });
    EXPECT_EQ(status.GetCode(), K_NOT_FOUND);
    EXPECT_EQ(result.status.GetCode(), K_NOT_FOUND);
    EXPECT_EQ(result.status.GetMsg(), "meaningful miss");
    EXPECT_EQ(reader.backoffCount, 1);
}

TEST(ReplicaReaderTest, BatchDeadlineBeforeFirstAttemptReturnsDeadlineNotSyntheticError)
{
    InitBatchGetMetrics();
    ApiDeadlineGuard deadline(1000);
    auto manager = std::make_shared<FakeDataPlaneManager>();
    auto executor = std::make_shared<DataPlaneExecutor>(manager, std::make_shared<TransportAdvisor>());
    ControlledReplicaReader reader(executor, std::make_shared<ThreadPool>(1));
    reader.deadlineStatuses = { Status(K_RPC_DEADLINE_EXCEEDED, "deadline before data") };
    auto location = MakeReplicaLocation("key", 1, { MakeAddress(66) });
    ObjectReadItemResult result;

    Status status = reader.ReadBatch({ { &location, &result } });
    EXPECT_EQ(status.GetCode(), K_RPC_DEADLINE_EXCEEDED);
    EXPECT_EQ(result.status.GetCode(), K_RPC_DEADLINE_EXCEEDED);
    EXPECT_EQ(manager->transportBuildCount, 0);
    ExpectMetricAbsent("client_direct_batch_get_replica_retry_total");
}

TEST(ReplicaReaderTest, BatchSingleReplicaBackoffDoesNotCountReplicaRetry)
{
    InitBatchGetMetrics();
    ApiDeadlineGuard deadline(1000);
    auto manager = std::make_shared<FakeDataPlaneManager>();
    manager->transporterGetStatuses = { { Status(K_NOT_FOUND, "round exhausted") } };
    auto executor = std::make_shared<DataPlaneExecutor>(manager, std::make_shared<TransportAdvisor>());
    ControlledReplicaReader reader(executor, std::make_shared<ThreadPool>(1));
    reader.backoffStatus = Status(K_RPC_DEADLINE_EXCEEDED, "deadline during backoff");
    auto location = MakeReplicaLocation("single", 1, { MakeAddress(79) });
    ObjectReadItemResult result;

    EXPECT_EQ(reader.ReadBatch({ { &location, &result } }).GetCode(), K_NOT_FOUND);
    EXPECT_EQ(reader.backoffCount, 1);
    ExpectMetricAbsent("client_direct_batch_get_replica_retry_total");
}

TEST(ReplicaReaderTest, BatchNonPositiveSizeChangeAdvancesWithoutUnsignedConversion)
{
    ApiDeadlineGuard deadline(1000);
    auto manager = std::make_shared<FakeDataPlaneManager>();
    manager->configureTransporter = [](const HostPort &address, FakeTransporter &transporter) {
        if (address.ToString() == MakeAddress(67).ToString()) {
            transporter.batchGetHandler = [](const DataGetBatchRequest &inputs, DataGetBatchResult &outputs) {
                outputs.resize(inputs.size());
                outputs[0].status = Status(K_OC_REMOTE_GET_NOT_ENOUGH, "zero size");
                outputs[0].data.response.set_data_size(0);
                outputs[1].status = Status(K_OC_REMOTE_GET_NOT_ENOUGH, "negative size");
                outputs[1].data.response.set_data_size(-1);
                return Status::OK();
            };
        } else {
            transporter.batchGetHandler = [](const DataGetBatchRequest &inputs, DataGetBatchResult &outputs) {
                EXPECT_EQ(inputs[0].expectedSize, 4u);
                EXPECT_EQ(inputs[1].expectedSize, 4u);
                outputs.resize(inputs.size());
                for (auto &output : outputs) {
                    output.status = Status::OK();
                }
                return Status::OK();
            };
        }
    };
    auto executor = std::make_shared<DataPlaneExecutor>(manager, std::make_shared<TransportAdvisor>());
    ReplicaReader reader(executor, std::make_shared<DeadlineRetry>(), std::make_shared<ThreadPool>(2));
    std::vector<master::ObjectLocationInfoPb> locations = {
        MakeReplicaLocation("zero", 4, { MakeAddress(67), MakeAddress(68) }),
        MakeReplicaLocation("negative", 4, { MakeAddress(67), MakeAddress(68) })
    };
    std::vector<ObjectReadItemResult> results(2);

    ASSERT_TRUE(reader.ReadBatch({ { &locations[0], &results[0] }, { &locations[1], &results[1] } }).IsOk());
    EXPECT_TRUE(results[0].status.IsOk());
    EXPECT_TRUE(results[1].status.IsOk());
}

TEST(ReplicaReaderTest, BatchSameEndpointChunksRunSequentiallyInOneTask)
{
    ApiDeadlineGuard deadline(1000);
    auto manager = std::make_shared<FakeDataPlaneManager>();
    std::mutex callsMutex;
    std::vector<std::pair<std::string, std::thread::id>> calls;
    manager->configureTransporter = [&](const HostPort &, FakeTransporter &transporter) {
        transporter.batchGetHandler = [&](const DataGetBatchRequest &inputs, DataGetBatchResult &outputs) {
            std::lock_guard<std::mutex> lock(callsMutex);
            calls.emplace_back("batch", std::this_thread::get_id());
            outputs.resize(inputs.size());
            for (auto &output : outputs) {
                output.status = Status::OK();
            }
            return Status::OK();
        };
        transporter.getHandler = [&](const DataGetRequest &, DataGetResult &) {
            std::lock_guard<std::mutex> lock(callsMutex);
            calls.emplace_back("get", std::this_thread::get_id());
            return Status::OK();
        };
    };
    auto executor = std::make_shared<DataPlaneExecutor>(manager, std::make_shared<TransportAdvisor>());
    ReplicaReader reader(executor, std::make_shared<DeadlineRetry>(), std::make_shared<ThreadPool>(4));
    std::vector<master::ObjectLocationInfoPb> locations;
    std::vector<ObjectReadItemResult> results(1025);
    ReplicaReadBatch requests;
    locations.reserve(1025);
    for (size_t i = 0; i < 1025; ++i) {
        locations.emplace_back(MakeReplicaLocation("key-" + std::to_string(i), 1, { MakeAddress(69) }));
        requests.push_back({ &locations.back(), &results[i] });
    }

    ASSERT_TRUE(reader.ReadBatch(requests).IsOk());
    ASSERT_EQ(calls.size(), 2u);
    EXPECT_EQ(calls[0].first, "batch");
    EXPECT_EQ(calls[1].first, "get");
    EXPECT_EQ(calls[0].second, calls[1].second);
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
