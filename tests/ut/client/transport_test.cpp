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
#include <cerrno>
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

#include <spawn.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>

extern char **environ;

#define private public
#include "datasystem/client/object_cache/object_client_impl.h"
#undef private

#include "datasystem/client/transport/data_plane/data_plane_manager.h"
#include "datasystem/client/listen_worker.h"
#include "datasystem/client/transport/data_plane/shm_transporter.h"
#include "datasystem/client/transport/data_plane/tcp_transporter.h"
#include "datasystem/client/transport/data_plane/ub_transporter.h"
#include "datasystem/client/transport/data_plane/shm_send_buffer_owner.h"
#include "datasystem/client/transport/common/deadline_retry.h"
#include "datasystem/client/transport/data_plane/data_plane_executor.h"
#include "datasystem/client/transport/metadata/object_metadata_client.h"
#include "datasystem/client/transport/object_buffer_internal.h"
#include "datasystem/client/transport/object_read/object_read_flow.h"
#include "datasystem/client/transport/object_read/replica_reader.h"
#include "datasystem/client/transport/worker_snapshot.h"
#include "datasystem/client/transport/rpc/exist_request_builder.h"
#include "datasystem/client/transport/rpc/mset_request_builder.h"
#include "datasystem/client/transport/rpc/set_request_builder.h"
#include "datasystem/client/transport/rpc/worker_rpc_client.h"
#include "datasystem/client/transport/transport_layer.h"
#include "datasystem/common/ak_sk/signature.h"
#include "datasystem/common/flags/common_flags.h"
#include "datasystem/common/inject/inject_point.h"
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

#ifdef USE_URMA
DS_DECLARE_bool(enable_urma);
#endif

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

class FakeMmapTableEntry : public IMmapTableEntry {
public:
    FakeMmapTableEntry() : IMmapTableEntry(-1, 1)
    {
    }

    Status Init(bool, const std::string &) override
    {
        return Status::OK();
    }
};

TransportRequestContext MakeRequestContext()
{
    return { "client-1", "token-1", "tenant-1" };
}

std::shared_ptr<const TransportReadContext> MakeReadContext()
{
    static const auto context = [] {
        auto value = std::make_shared<TransportReadContext>();
        value->requestContext = MakeRequestContext();
        value->subTimeoutMs = 1000;
        return value;
    }();
    return context;
}

TEST(DeadlineRetryTest, AdmissionFailureSkipsBackoff)
{
    constexpr int64_t kDeadlineMs = 1'000;
    constexpr int64_t kBackoffMs = 128;
    ApiDeadlineGuard deadline(kDeadlineMs);
    int checkCount = 0;
    DeadlineRetry retry([&checkCount]() {
        ++checkCount;
        return Status(K_RPC_PEER_DEAD, "Bound worker is dead");
    });
    int64_t backoffMs = kBackoffMs;

    auto rc = retry.Backoff(backoffMs);

    EXPECT_EQ(rc.GetCode(), K_RPC_PEER_DEAD);
    EXPECT_EQ(checkCount, 1);
}

TEST(DeadlineRetryTest, RechecksAdmissionAfterBackoff)
{
    constexpr int64_t kDeadlineMs = 1'000;
    ApiDeadlineGuard deadline(kDeadlineMs);
    int checkCount = 0;
    DeadlineRetry retry([&checkCount]() {
        ++checkCount;
        return checkCount == 1 ? Status::OK() : Status(K_RPC_PEER_DEAD, "Bound worker died during backoff");
    });
    int64_t backoffMs = 0;

    auto rc = retry.Backoff(backoffMs);

    EXPECT_EQ(rc.GetCode(), K_RPC_PEER_DEAD);
    EXPECT_EQ(checkCount, 2);
}

ReplicaReadRequest MakeReplicaReadRequest(const master::ObjectLocationInfoPb *location, ObjectReadItemResult *result)
{
    return { location, result, MakeReadContext() };
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

QueryAndGetResultPb *AddLocation(QueryAndGetRspPb &response, const std::string &key,
                                 const HostPort &address, uint64_t size = 4, uint64_t topologyVersion = 0)
{
    auto *result = response.add_results();
    auto *location = result->mutable_location();
    location->set_object_key(key);
    location->add_object_locations(address.ToString());
    location->set_object_size(size);
    location->set_topology_version(topologyVersion);
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

Status MakeStaleSnapshotStatus(const HostPort &address)
{
    return Status(K_NOT_READY, "Worker endpoint is absent from latest transport snapshot: " + address.ToString());
}

Status MakeWorkerDrainingStatus()
{
    return Status(K_NOT_READY, "Worker is draining for ScaleIn");
}

std::shared_ptr<Routing> MakeSingleWorkerRouting(const HostPort &address)
{
    auto router = std::make_shared<WorkerRouter>("");
    auto ring = std::make_shared<::datasystem::ClusterTopologyPb>();
    ring->set_tokens_per_member(1);
    auto &worker = (*ring->mutable_members())[address.ToString()];
    worker.set_state(::datasystem::MembershipPb::ACTIVE);
    auto hostIdMap = std::make_shared<std::unordered_map<std::string, std::string>>();
    std::unique_ptr<PreparedClusterTopology> prepared;
    auto status = PreparedClusterTopology::Create(std::move(*ring), prepared);
    if (status.IsError()) {
        ADD_FAILURE() << status.ToString();
        return nullptr;
    }
    router->UpdateHashRing(*prepared, *hostIdMap);

    auto fetch = [](const HostPort &, uint64_t, ::datasystem::ClusterTopologyPb &, std::string &, uint64_t &, bool &,
                    std::unordered_map<std::string, std::string> &) { return Status::OK(); };
    auto refresher = std::make_shared<HashRingRefresher>(router, fetch);
    auto routing = std::make_shared<Routing>(router, refresher);
    routing->initialized_.store(true);
    return routing;
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
        response.set_data_source(request.has_urma_info() && !getObjectResponseInPayload
                                     ? DataTransferSource::DATA_ALREADY_TRANSFERRED
                                     : DataTransferSource::DATA_IN_PAYLOAD);
        if (getObjectStatus.IsError()) {
            return getObjectStatus;
        }
        if (request.has_urma_info() && request.data_size() != static_cast<uint64_t>(getObjectDataSize)) {
            return Status(K_OC_REMOTE_GET_NOT_ENOUGH, "receive buffer size mismatch");
        }
        if ((!request.has_urma_info() || getObjectResponseInPayload) && getObjectDataSize > 0) {
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

    Status InvokeQueryAndGet(QueryAndGetReqPb &request, QueryAndGetRspPb &response,
                             std::vector<RpcMessage> &payloads, bool *rpcDispatched = nullptr) override
    {
        if (rpcDispatched != nullptr) {
            *rpcDispatched = true;
        }
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
        response.set_store_fd(createResponseStoreFd);
        response.set_mmap_size(createResponseMmapSize);
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
        // Default: one result per requested key. If multiCreateResultCount >= 0, return that many
        // (used to test the count-mismatch Release branch in ShmTransporter::MCreate).
        int resultCount = multiCreateResultCount < 0 ? request.object_key_size() : multiCreateResultCount;
        for (int i = 0; i < resultCount; ++i) {
            auto *item = response.add_results();
            item->set_shm_id("multi-shm-" + std::to_string(i));
            item->set_store_fd(createResponseStoreFd);
            item->set_mmap_size(createResponseMmapSize);
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
    bool getObjectResponseInPayload = false;
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
    std::vector<QueryAndGetReqPb> queryAndGetRequests;
    std::function<Status(const HostPort &, const QueryAndGetReqPb &, QueryAndGetRspPb &,
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
    int multiCreateResultCount = -1;  // <0 = per request key count; >=0 = fixed (mismatch test)
    int multiSetInvokeCount = 0;
    int decreaseReferenceCount = 0;
    Status createInvokeStatus = Status::OK();
    Status setInvokeStatus = Status::OK();
    Status multiCreateInvokeStatus = Status::OK();
    Status multiSetInvokeStatus = Status::OK();
    Status decreaseReferenceStatus = Status::OK();
    bool createResponseHasUrmaInfo = false;
    int64_t createResponseMetadataSize = 0;
    int32_t createResponseStoreFd = 0;
    uint64_t createResponseMmapSize = 0;
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

class BlockingFdWorkerRpcClient final : public FakeWorkerRpcClient {
public:
    Status InvokeGetClientFd(GetClientFdReqPb &, GetClientFdRspPb &) override
    {
        entered_.set_value();
        releaseFuture_.wait();
        return Status(K_RPC_UNAVAILABLE, "GetClientFd interrupted");
    }

    std::future<void> EnteredFuture()
    {
        return entered_.get_future();
    }

    void Release()
    {
        if (!released_.exchange(true)) {
            release_.set_value();
        }
    }

private:
    std::promise<void> entered_;
    std::promise<void> release_;
    std::shared_future<void> releaseFuture_{ release_.get_future().share() };
    std::atomic<bool> released_{ false };
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
    int clientGetInvokeCount = 0;
    int shmHeartbeatInvokeCount = 0;
    int getSocketPathInvokeCount = 0;
    std::atomic<int> shmDisconnectInvokeCount{ 0 };
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
    int shmHeartbeatRpcTimeout = 0;
    int shmDisconnectRpcTimeout = 0;
    uint64_t hashRingVersion = 0;
    GetObjectRemoteReqPb invokedDataRequest;
    GetReqPb invokedClientGetRequest;
    HeartbeatReqPb invokedShmHeartbeatRequest;
    DisconnectClientReqPb invokedShmDisconnectRequest;
    BatchGetObjectRemoteReqPb invokedBatchGetRequest;
    QueryAndGetReqPb invokedMetadataRequest;
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
    bool shmHeartbeatVoluntaryScaleDown = false;
    bool shmHeartbeatClientRemoved = false;
    std::function<void()> beforeShmHeartbeatReturn;

protected:
    Status DoInvokeGetSocketPath(const RpcOptions &, const GetSocketPathReqPb &, GetSocketPathRspPb &) override
    {
        ++getSocketPathInvokeCount;
        return Status(K_RUNTIME_ERROR, "unexpected shared-memory reconnect");
    }

    Status DoInvokeGetObject(const RpcOptions &options, const GetObjectRemoteReqPb &request, GetObjectRemoteRspPb &,
                             std::vector<RpcMessage> &) override
    {
        ++getObjectInvokeCount;
        dataRpcTimeout = options.GetTimeout();
        invokedDataRequest = request;
        return Status::OK();
    }

    Status DoInvokeClientGet(const RpcOptions &, const GetReqPb &request, GetRspPb &response,
                             std::vector<RpcMessage> &) override
    {
        ++clientGetInvokeCount;
        invokedClientGetRequest = request;
        auto *info = response.add_objects();
        info->set_object_index(0);
        info->set_store_fd(11);
        response.mutable_last_rc()->set_error_code(K_OK);
        return Status::OK();
    }

    Status DoInvokeShmHeartbeat(const RpcOptions &options, const HeartbeatReqPb &request,
                                HeartbeatRspPb &response) override
    {
        ++shmHeartbeatInvokeCount;
        shmHeartbeatRpcTimeout = options.GetTimeout();
        invokedShmHeartbeatRequest = request;
        response.set_worker_start_id("worker-start");
        response.set_is_voluntary_scale_down(shmHeartbeatVoluntaryScaleDown);
        response.set_client_removed(shmHeartbeatClientRemoved);
        if (beforeShmHeartbeatReturn) {
            beforeShmHeartbeatReturn();
        }
        return Status::OK();
    }

    Status DoInvokeDisconnectShmClient(const RpcOptions &options, const DisconnectClientReqPb &request,
                                       DisconnectClientRspPb &) override
    {
        ++shmDisconnectInvokeCount;
        shmDisconnectRpcTimeout = options.GetTimeout();
        invokedShmDisconnectRequest = request;
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

    Status DoInvokeQueryAndGet(const RpcOptions &options, const QueryAndGetReqPb &request,
                               QueryAndGetRspPb &, std::vector<RpcMessage> &) override
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
        response.mutable_hash_ring()->set_tokens_per_member(1);
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

    Status Set(ObjectBuffer &buffer, const TransportSetParam &param, TransportSetResult *result = nullptr) override
    {
        ++setCount;
        if (result != nullptr) {
            result->publishAttempted = true;
        }
        setParams.push_back(param);
        const auto &info = ObjectBufferInternal::GetInfo(buffer);
        setPayloads.emplace_back(info.pointer == nullptr
                                     ? std::string()
                                     : std::string(reinterpret_cast<const char *>(info.pointer + info.metadataSize),
                                                   info.dataSize));
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
        result.workerAutoRelease = mSetWorkerAutoRelease;
        result.failedKeys = mSetFailedKeys;
        if (!mSetStatuses.empty()) {
            Status rc = mSetStatuses.front();
            mSetStatuses.erase(mSetStatuses.begin());
            return rc;
        }
        return Status::OK();
    }

    Status Release(const ShmKey &shmId, const TransportRequestContext &context) override
    {
        ++releaseCount;
        releasedShmIds.push_back(shmId);
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
    bool mSetWorkerAutoRelease = false;
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
    std::vector<std::string> setPayloads;
    std::vector<std::string> mSetFailedKeys;
    std::vector<ShmKey> releasedShmIds;
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
        if (!rpcBuildStatuses.empty()) {
            Status rc = rpcBuildStatuses.front();
            rpcBuildStatuses.erase(rpcBuildStatuses.begin());
            if (rc.IsError()) {
                return rc;
            }
        }
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
                            TransportPhaseLatencyRecorder *recorder,
                            std::shared_ptr<IDataTransporter> &output) override
    {
        std::lock_guard<std::mutex> lock(mutex);
        ++transportBuildCount;
        transportBuildTraceEnabled.push_back(recorder != nullptr);
        rpcClientsSeen.push_back(rpcClient);
        if (!transportBuildStatuses.empty()) {
            Status rc = transportBuildStatuses.front();
            transportBuildStatuses.erase(transportBuildStatuses.begin());
            if (rc.IsError()) {
                return rc;
            }
        }
        auto transporter = std::make_shared<FakeTransporter>();
        transporter->kind = hint == TransportHint::SHM_CANDIDATE
                                ? AccessTransportKind::SHM
                                : (hint == TransportHint::TCP_ONLY ? AccessTransportKind::TCP
                                                                  : AccessTransportKind::UB);
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
    std::vector<bool> transportBuildTraceEnabled;
    std::vector<Status> rpcBuildStatuses;
    std::vector<Status> transportBuildStatuses;
    std::vector<Status> existInvokeStatuses;
    std::vector<std::vector<Status>> transporterGetStatuses;
    std::vector<std::vector<Status>> transporterSetStatuses;
    std::vector<std::vector<Status>> transporterMCreateStatuses;
    std::vector<std::vector<Status>> transporterMSetStatuses;
    std::vector<bool> transporterMSetPublishAttempted;
    std::vector<std::shared_ptr<FakeTransporter>> builtTransporters;
    std::function<Status(const HostPort &, const QueryAndGetReqPb &, QueryAndGetRspPb &,
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

    Status QueryAndGet(const HostPort &address, const ObjectMetadataBatch &items,
                       std::shared_ptr<const TransportReadContext>) override
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
        if (queryAndGetHandler) {
            auto status = queryAndGetHandler(address, items);
            if (status.IsError()) {
                return status;
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
    std::function<Status(const HostPort &, const ObjectMetadataBatch &)> queryAndGetHandler;
};

class FakeReplicaReader : public ReplicaReader {
public:
    FakeReplicaReader() : ReplicaReader(nullptr, nullptr, nullptr)
    {
    }

    Status Read(const master::ObjectLocationInfoPb &location, ObjectReadItemResult &result,
                std::shared_ptr<const TransportReadContext> context, bool traceEnabled) override
    {
        EXPECT_NE(context, nullptr);
        {
            std::lock_guard<std::mutex> lock(mutex);
            unaryKeys.push_back(location.object_key());
            threadIds.push_back(std::this_thread::get_id());
            traceDecisions.push_back(traceEnabled);
        }
        return FillResult(location, result);
    }

    Status ReadBatch(const ReplicaReadBatch &requests, bool traceEnabled) override
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
            traceDecisions.push_back(traceEnabled);
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
        if (statusHandler) {
            Status status = statusHandler(location.object_key());
            if (status.IsError()) {
                return status;
            }
        }
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
    std::vector<bool> traceDecisions;
    std::unordered_map<std::string, Status> itemStatuses;
    std::function<Status(const std::string &)> statusHandler;
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
        : UbTransporter(std::move(rpcClient), std::move(connection)), testRpcClient_(rpcClient)
    {
    }

    std::shared_ptr<WorkerRpcClient> testRpcClient_;

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

    Status SubmitPayload(ObjectBufferInfo &, bool, std::vector<uint64_t> &eventKeys,
                         UrmaWriteFailure *) override
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

    Status WaitPayloadEvents(std::vector<uint64_t> &, UrmaWriteFailure *) override
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
        // Mirror UbTransporter::BuildMCreateBuffer: attach owner for routed ref release.
        info->receiveBufferOwner = std::make_shared<ShmSendBufferOwner>(
            testRpcClient_, info->shmId, param.requestContext, std::weak_ptr<ThreadPool>{}, nullptr);
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

    void SetObjectRead(std::unique_ptr<ObjectReadFlow> objectRead)
    {
        objectRead_ = std::move(objectRead);
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
    explicit FakeBufferOwner(uint64_t size, bool managesWorkerReference = false)
        : data(size), managesWorkerReference(managesWorkerReference)
    {
    }

    bool ManagesWorkerReference() const override
    {
        return managesWorkerReference;
    }

    Status CheckAlive() const override
    {
        return alive ? Status::OK() : Status(K_BUFFER_DEPRECATED, "Fake buffer owner is no longer alive");
    }

    std::vector<uint8_t> data;
    bool managesWorkerReference;
    bool alive = true;
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

    QueryAndGetReqPb metadataRequest;
    metadataRequest.add_object_keys("key");
    metadataRequest.mutable_data_request()->mutable_tcp();
    QueryAndGetRspPb metadataResponse;
    std::vector<RpcMessage> metadataPayloads;
    ASSERT_TRUE(client.InvokeQueryAndGet(metadataRequest, metadataResponse, metadataPayloads).IsOk());
    EXPECT_EQ(client.metadataInvokeCount, 1);
    EXPECT_EQ(client.invokedMetadataRequest.access_key(), "access-1");
    EXPECT_FALSE(client.invokedMetadataRequest.signature().empty());
    EXPECT_TRUE(client.invokedMetadataRequest.data_request().has_tcp());

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

TEST(WorkerRpcClientTest, ClientGetUsesSignedWorkerOcServiceRequest)
{
    ApiDeadlineGuard deadline(1000);
    auto signature = std::make_shared<Signature>("access-1", SensitiveValue("secret-1"));
    AuthBoundaryWorkerRpcClient client(signature);
    GetReqPb request;
    request.set_client_id("endpoint-client");
    request.set_tenant_id("tenant-1");
    request.add_object_keys("key");
    GetRspPb response;
    std::vector<RpcMessage> payloads;

    ASSERT_TRUE(client.InvokeClientGet(request, response, payloads).IsOk());
    EXPECT_EQ(client.clientGetInvokeCount, 1);
    EXPECT_EQ(client.invokedClientGetRequest.client_id(), "endpoint-client");
    EXPECT_EQ(client.invokedClientGetRequest.tenant_id(), "tenant-1");
    EXPECT_EQ(client.invokedClientGetRequest.access_key(), "access-1");
    EXPECT_FALSE(client.invokedClientGetRequest.signature().empty());
    EXPECT_EQ(client.getObjectInvokeCount, 0);
    EXPECT_EQ(client.batchGetObjectInvokeCount, 0);
}

TEST(WorkerRpcClientTest, ShmMaintenanceHeartbeatUsesSignedWorkerServiceRequest)
{
    auto signature = std::make_shared<Signature>("access-1", SensitiveValue("secret-1"));
    AuthBoundaryWorkerRpcClient client(signature);
    HeartbeatReqPb request;
    request.set_client_id("endpoint-client");
    request.set_token("token-1");
    request.add_released_worker_fds(17);
    HeartbeatRspPb response;

    ASSERT_TRUE(client.InvokeShmHeartbeat(request, response).IsOk());
    EXPECT_EQ(client.shmHeartbeatInvokeCount, 1);
    EXPECT_GT(client.shmHeartbeatRpcTimeout, 0);
    EXPECT_LE(client.shmHeartbeatRpcTimeout, 1000);
    EXPECT_EQ(client.invokedShmHeartbeatRequest.client_id(), "endpoint-client");
    EXPECT_EQ(client.invokedShmHeartbeatRequest.token(), "token-1");
    EXPECT_EQ(client.invokedShmHeartbeatRequest.released_worker_fds(0), 17);
    EXPECT_EQ(client.invokedShmHeartbeatRequest.access_key(), "access-1");
    EXPECT_FALSE(client.invokedShmHeartbeatRequest.signature().empty());
}

TEST(ShmConnectionTest, VoluntaryScaleDownDoesNotReconnectSharedMemory)
{
    auto rpcClient = std::make_shared<AuthBoundaryWorkerRpcClient>(MakeSignature());
    rpcClient->shmHeartbeatVoluntaryScaleDown = true;
    rpcClient->shmHeartbeatClientRemoved = true;
    auto releasePool = std::make_shared<ThreadPool>(0, 1, "scale_in_disconnect_test");
    ShmConnection connection(MakeAddress(9001), rpcClient, releasePool);
    auto fdChannel = std::make_shared<ShmFdChannel>(rpcClient, ShmFd(), false, "endpoint-client");
    auto mmapManager = std::make_shared<MmapManager>(fdChannel, false);
    auto session = std::shared_ptr<ShmSession>(new ShmSession(
        MakeAddress(9001), rpcClient, fdChannel, mmapManager, "endpoint-client", "worker-start", 1,
        releasePool, MakeRequestContext(), true, connection.scaleInDraining_));
    rpcClient->beforeShmHeartbeatReturn = [&connection, session] {
        session->Close(false);
        connection.Invalidate(session);
    };
    connection.session_ = session;

    session->RunMaintenance();
    rpcClient->beforeShmHeartbeatReturn = nullptr;
    std::shared_ptr<ShmSession> acquired;
    const Status rc = connection.Acquire(MakeRequestContext(), acquired);

    EXPECT_TRUE(IsWorkerDrainingForScaleIn(rc));
    EXPECT_EQ(rpcClient->shmHeartbeatInvokeCount, 1);
    EXPECT_EQ(rpcClient->getSocketPathInvokeCount, 0);
    EXPECT_EQ(acquired, nullptr);
    releasePool.reset();
    EXPECT_EQ(rpcClient->shmDisconnectInvokeCount.load(), 1);
}

TEST(WorkerRpcClientTest, ShmDisconnectUsesBoundedSignedWorkerServiceRequest)
{
    BrpcChannelConfig config;
    config.timeout_ms = 60'000;
    auto signature = std::make_shared<Signature>("access-1", SensitiveValue("secret-1"));
    AuthBoundaryWorkerRpcClient client(signature, config);
    DisconnectClientReqPb request;
    request.set_client_id("endpoint-client");
    request.set_token("token-1");
    DisconnectClientRspPb response;

    ASSERT_TRUE(client.InvokeDisconnectShmClient(request, response).IsOk());
    EXPECT_EQ(client.shmDisconnectInvokeCount.load(), 1);
    EXPECT_GT(client.shmDisconnectRpcTimeout, 0);
    EXPECT_LE(client.shmDisconnectRpcTimeout, 1000);
    EXPECT_EQ(client.invokedShmDisconnectRequest.client_id(), "endpoint-client");
    EXPECT_EQ(client.invokedShmDisconnectRequest.token(), "token-1");
    EXPECT_EQ(client.invokedShmDisconnectRequest.access_key(), "access-1");
    EXPECT_FALSE(client.invokedShmDisconnectRequest.signature().empty());
}

TEST(ShmFdChannelTest, CloseDoesNotWaitForInFlightGetClientFdRpc)
{
    int sockets[2] = { INVALID_SHM_FD, INVALID_SHM_FD };
    ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, sockets), 0);
    Raii closePeer([&sockets]() {
        if (sockets[1] != INVALID_SHM_FD) {
            RETRY_ON_EINTR(close(sockets[1]));
        }
    });
    auto rpcClient = std::make_shared<BlockingFdWorkerRpcClient>();
    auto entered = rpcClient->EnteredFuture();
    auto channel =
        std::make_shared<ShmFdChannel>(rpcClient, ShmFd(sockets[0]), false, "endpoint-client");
    sockets[0] = INVALID_SHM_FD;

    auto getFuture = std::async(std::launch::async, [channel]() {
        std::vector<int> clientFds;
        return channel->GetClientFd({ 17 }, clientFds, "tenant-1");
    });
    Raii releaseRpc([rpcClient]() { rpcClient->Release(); });
    ASSERT_EQ(entered.wait_for(std::chrono::seconds(1)), std::future_status::ready);

    auto closeFuture = std::async(std::launch::async, [channel]() { channel->Close(); });
    EXPECT_EQ(closeFuture.wait_for(std::chrono::seconds(1)), std::future_status::ready);

    rpcClient->Release();
    EXPECT_EQ(getFuture.get().GetCode(), K_RPC_UNAVAILABLE);
    closeFuture.get();
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

TEST(WorkerRpcClientTest, RecordsRoutedSetRpcTotalLatency)
{
    constexpr uint64_t staleCommUs = std::numeric_limits<uint64_t>::max();
    Trace::Instance().ClearLatencyTicks();
    Trace::Instance().SetLastRpcCommUs(0);
    Raii clearTrace([] {
        Trace::Instance().ClearLatencyTicks();
        Trace::Instance().SetLastRpcCommUs(0);
    });
    Trace::Instance().AddLatencyTick(LatencyTickKey::CLIENT_SET_START);

    AuthBoundaryWorkerRpcClient client(MakeSignature());
    uint32_t workerVersion = 0;
    CreateReqPb createRequest;
    CreateRspPb createResponse;
    createResponse.add_latency_phase_us(static_cast<uint32_t>(LatencySummaryPhase::WORKER_PROCESS_CREATE));
    createResponse.add_latency_phase_us(1);
    createResponse.set_latency_tick_dropped_count(1);
    Trace::Instance().SetLastRpcCommUs(staleCommUs);
    ASSERT_TRUE(client.InvokeCreate(100, createRequest, createResponse, workerVersion).IsOk());

    PublishReqPb publishRequest;
    PublishRspPb publishResponse;
    publishResponse.add_latency_phase_us(static_cast<uint32_t>(LatencySummaryPhase::WORKER_PROCESS_PUBLISH));
    publishResponse.add_latency_phase_us(1);
    publishResponse.set_latency_tick_dropped_count(1);
    std::vector<MemView> payloads;
    Trace::Instance().SetLastRpcCommUs(staleCommUs);
    ASSERT_TRUE(client.InvokeSet(100, publishRequest, payloads, publishResponse, workerVersion).IsOk());

    const auto &phases = Trace::Instance().GetDownstreamPhases();
    ASSERT_EQ(phases.count, 2u);
    EXPECT_EQ(phases.entries[0].phase, LatencySummaryPhase::CLIENT_RPC_CREATE_TOTAL);
    EXPECT_NE(phases.entries[0].durationUs, staleCommUs);
    EXPECT_EQ(phases.entries[1].phase, LatencySummaryPhase::CLIENT_RPC_PUBLISH_TOTAL);
    EXPECT_NE(phases.entries[1].durationUs, staleCommUs);
    EXPECT_EQ(phases.tickDroppedCount, 0u);
    EXPECT_EQ(Trace::Instance().ConsumeLastRpcCommUs(), 0u);
}

TEST(WorkerRpcClientTest, SkipsRoutedRpcLatencyWithoutActiveTrace)
{
    constexpr uint64_t staleCommUs = std::numeric_limits<uint64_t>::max();
    Trace::Instance().ClearLatencyTicks();
    Trace::Instance().SetLastRpcCommUs(staleCommUs);
    Raii clearTrace([] {
        Trace::Instance().ClearLatencyTicks();
        Trace::Instance().SetLastRpcCommUs(0);
    });

    AuthBoundaryWorkerRpcClient client(MakeSignature());
    CreateReqPb request;
    CreateRspPb response;
    uint32_t workerVersion = 0;
    ASSERT_TRUE(client.InvokeCreate(100, request, response, workerVersion).IsOk());

    EXPECT_EQ(Trace::Instance().GetDownstreamPhases().count, 0u);
    EXPECT_EQ(Trace::Instance().ConsumeLastRpcCommUs(), 0u);
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

    QueryAndGetReqPb metadataRequest;
    metadataRequest.add_object_keys("key");
    QueryAndGetRspPb metadataResponse;
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

    QueryAndGetReqPb metadataRequest;
    QueryAndGetRspPb metadataResponse;
    std::vector<RpcMessage> metadataPayloads;
    bool metadataRpcDispatched = true;
    EXPECT_EQ(
        client.InvokeQueryAndGet(metadataRequest, metadataResponse, metadataPayloads, &metadataRpcDispatched).GetCode(),
        K_RPC_DEADLINE_EXCEEDED);
    EXPECT_EQ(client.metadataInvokeCount, 0);
    EXPECT_FALSE(metadataRpcDispatched);

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
    std::unordered_map<std::string, std::string> emptyHostIdMap;
    ASSERT_TRUE(BuildWorkerSnapshot(42, ring, emptyHostIdMap, "", snapshot).IsOk());
    EXPECT_EQ(snapshot.ringVersion, 42u);
    EXPECT_TRUE(snapshot.shmCandidateAddrs.empty());
    EXPECT_EQ(snapshot.remoteTransportAddrs.size(), states.size());
}

TEST(WorkerSnapshotTest, RejectsMalformedTopologyWithoutChangingOutput)
{
    ::datasystem::ClusterTopologyPb ring;
    (*ring.mutable_members())["malformed-endpoint"].set_state(::datasystem::MembershipPb::ACTIVE);
    WorkerSnapshot snapshot;
    snapshot.ringVersion = 7;
    snapshot.shmCandidateAddrs.push_back(MakeAddress(110));

    EXPECT_EQ(
        BuildWorkerSnapshot(8, ring, std::unordered_map<std::string, std::string>{}, "", snapshot).GetCode(),
        K_INVALID);
    EXPECT_EQ(snapshot.ringVersion, 7u);
    ASSERT_EQ(snapshot.shmCandidateAddrs.size(), 1u);
    EXPECT_EQ(snapshot.shmCandidateAddrs.front(), MakeAddress(110));
}

TEST(WorkerSnapshotTest, AcceptsEmptyTopologyAsCleanupAll)
{
    ::datasystem::ClusterTopologyPb ring;
    WorkerSnapshot snapshot;
    snapshot.remoteTransportAddrs.push_back(MakeAddress(111));

    ASSERT_TRUE(
        BuildWorkerSnapshot(9, ring, std::unordered_map<std::string, std::string>{}, "", snapshot).IsOk());
    EXPECT_EQ(snapshot.ringVersion, 9u);
    EXPECT_TRUE(snapshot.Empty());
}

// canPartition=true: eligible same-host workers use SHM; all others use a remote transport.
TEST(WorkerSnapshotTest, PartitionsByHostIdWhenSdkHostIdAndHostIdMapPresent)
{
    ::datasystem::ClusterTopologyPb ring;
    const HostPort sameA = MakeAddress(200);
    const HostPort sameB = MakeAddress(201);
    const HostPort crossC = MakeAddress(202);
    (*ring.mutable_members())[sameA.ToString()].set_state(::datasystem::MembershipPb::ACTIVE);
    (*ring.mutable_members())[sameB.ToString()].set_state(::datasystem::MembershipPb::ACTIVE);
    (*ring.mutable_members())[crossC.ToString()].set_state(::datasystem::MembershipPb::ACTIVE);

    std::unordered_map<std::string, std::string> hostIdMap = {
        { sameA.ToString(), "node05" },
        { sameB.ToString(), "node05" },
        { crossC.ToString(), "node06" },
    };
    WorkerSnapshot snapshot;
    ASSERT_TRUE(BuildWorkerSnapshot(11, ring, hostIdMap, "node05", snapshot).IsOk());
    EXPECT_EQ(snapshot.ringVersion, 11u);
    EXPECT_EQ(snapshot.shmCandidateAddrs.size(), 2u);
    EXPECT_EQ(snapshot.remoteTransportAddrs.size(), 1u);
    // same-host and other are disjoint
    EXPECT_NE(std::find(snapshot.shmCandidateAddrs.begin(), snapshot.shmCandidateAddrs.end(), sameA),
              snapshot.shmCandidateAddrs.end());
    EXPECT_NE(std::find(snapshot.shmCandidateAddrs.begin(), snapshot.shmCandidateAddrs.end(), sameB),
              snapshot.shmCandidateAddrs.end());
    EXPECT_EQ(snapshot.remoteTransportAddrs.front(), crossC);
}

#ifdef USE_URMA_MOCK
TEST(WorkerSnapshotTest, DrainingSameHostWorkersSelectUbWithoutShmAttempt)
{
    const bool enableUrma = FLAGS_enable_urma;
    Raii restoreEnableUrma([enableUrma]() { FLAGS_enable_urma = enableUrma; });
    FLAGS_enable_urma = true;

    ::datasystem::ClusterTopologyPb ring;
    const HostPort active = MakeAddress(210);
    const HostPort preLeaving = MakeAddress(211);
    const HostPort leaving = MakeAddress(212);
    (*ring.mutable_members())[active.ToString()].set_state(::datasystem::MembershipPb::ACTIVE);
    (*ring.mutable_members())[preLeaving.ToString()].set_state(::datasystem::MembershipPb::PRE_LEAVING);
    (*ring.mutable_members())[leaving.ToString()].set_state(::datasystem::MembershipPb::LEAVING);

    const std::unordered_map<std::string, std::string> hostIdMap = {
        { active.ToString(), "node05" },
        { preLeaving.ToString(), "node05" },
        { leaving.ToString(), "node05" },
    };
    WorkerSnapshot snapshot;
    ASSERT_TRUE(BuildWorkerSnapshot(12, ring, hostIdMap, "node05", snapshot).IsOk());
    ASSERT_EQ(snapshot.shmCandidateAddrs.size(), 1u);
    EXPECT_EQ(snapshot.shmCandidateAddrs.front(), active);
    ASSERT_EQ(snapshot.remoteTransportAddrs.size(), 2u);
    EXPECT_NE(std::find(snapshot.remoteTransportAddrs.begin(), snapshot.remoteTransportAddrs.end(), preLeaving),
              snapshot.remoteTransportAddrs.end());
    EXPECT_NE(std::find(snapshot.remoteTransportAddrs.begin(), snapshot.remoteTransportAddrs.end(), leaving),
              snapshot.remoteTransportAddrs.end());

    TransportAdvisor advisor;
    advisor.SetShmCandidateWorkers(snapshot.shmCandidateAddrs);
    EXPECT_EQ(advisor.GetTransportHint(active), TransportHint::SHM_CANDIDATE);
    EXPECT_EQ(advisor.GetTransportHint(preLeaving), TransportHint::UB_CANDIDATE);
    EXPECT_EQ(advisor.GetTransportHint(leaving), TransportHint::UB_CANDIDATE);

    FLAGS_enable_urma = false;
    EXPECT_EQ(advisor.GetTransportHint(preLeaving), TransportHint::TCP_ONLY);
    EXPECT_EQ(advisor.GetTransportHint(leaving), TransportHint::TCP_ONLY);

    (*ring.mutable_members())[preLeaving.ToString()].set_state(::datasystem::MembershipPb::ACTIVE);
    (*ring.mutable_members())[leaving.ToString()].set_state(::datasystem::MembershipPb::ACTIVE);
    ASSERT_TRUE(BuildWorkerSnapshot(13, ring, hostIdMap, "node05", snapshot).IsOk());
    advisor.SetShmCandidateWorkers(snapshot.shmCandidateAddrs);
    EXPECT_EQ(advisor.GetTransportHint(preLeaving), TransportHint::SHM_CANDIDATE);
    EXPECT_EQ(advisor.GetTransportHint(leaving), TransportHint::SHM_CANDIDATE);
}
#endif

// A worker whose hostId differs from sdkHostId must not be an SHM candidate.
TEST(WorkerSnapshotTest, WorkerInRingButMismatchedHostIdUsesRemoteTransport)
{
    ::datasystem::ClusterTopologyPb ring;
    const HostPort w = MakeAddress(300);
    (*ring.mutable_members())[w.ToString()].set_state(::datasystem::MembershipPb::ACTIVE);

    std::unordered_map<std::string, std::string> hostIdMap = { { w.ToString(), "node06" } };
    WorkerSnapshot snapshot;
    ASSERT_TRUE(BuildWorkerSnapshot(12, ring, hostIdMap, "node05", snapshot).IsOk());
    EXPECT_TRUE(snapshot.shmCandidateAddrs.empty());
    EXPECT_EQ(snapshot.remoteTransportAddrs.size(), 1u);
    EXPECT_EQ(snapshot.remoteTransportAddrs.front(), w);
}

// sdkHostId empty or hostIdMap empty → canPartition=false, every worker uses remote transport.
TEST(WorkerSnapshotTest, NoPartitionWhenSdkHostIdOrHostIdMapEmpty)
{
    ::datasystem::ClusterTopologyPb ring;
    const HostPort w = MakeAddress(400);
    (*ring.mutable_members())[w.ToString()].set_state(::datasystem::MembershipPb::ACTIVE);
    std::unordered_map<std::string, std::string> hostIdMap = { { w.ToString(), "node05" } };

    WorkerSnapshot snapA;
    ASSERT_TRUE(BuildWorkerSnapshot(13, ring, hostIdMap, "", snapA).IsOk());
    EXPECT_TRUE(snapA.shmCandidateAddrs.empty());
    EXPECT_EQ(snapA.remoteTransportAddrs.size(), 1u);

    WorkerSnapshot snapB;
    ASSERT_TRUE(BuildWorkerSnapshot(14, ring, std::unordered_map<std::string, std::string>{}, "node05", snapB).IsOk());
    EXPECT_TRUE(snapB.shmCandidateAddrs.empty());
    EXPECT_EQ(snapB.remoteTransportAddrs.size(), 1u);
}

// A ring member missing from hostIdMap uses remote transport.
TEST(WorkerSnapshotTest, RingMemberMissingFromHostIdMapUsesRemoteTransport)
{
    ::datasystem::ClusterTopologyPb ring;
    const HostPort known = MakeAddress(500);
    const HostPort unknown = MakeAddress(501);
    (*ring.mutable_members())[known.ToString()].set_state(::datasystem::MembershipPb::ACTIVE);
    (*ring.mutable_members())[unknown.ToString()].set_state(::datasystem::MembershipPb::ACTIVE);

    std::unordered_map<std::string, std::string> hostIdMap = { { known.ToString(), "node05" } };
    WorkerSnapshot snapshot;
    ASSERT_TRUE(BuildWorkerSnapshot(15, ring, hostIdMap, "node05", snapshot).IsOk());
    EXPECT_EQ(snapshot.shmCandidateAddrs.size(), 1u);
    EXPECT_EQ(snapshot.shmCandidateAddrs.front(), known);
    EXPECT_EQ(snapshot.remoteTransportAddrs.size(), 1u);
    EXPECT_EQ(snapshot.remoteTransportAddrs.front(), unknown);
}

// ResolveSdkHostId decides whether the sdk adopts the bound (initial) worker's hostId when its own
// host_id is unresolved. A cross-node bound worker's hostId must NOT be adopted, or the whole remote
// host is misclassified as same-host and cross-node Gets time out on the SHM/UDS path.
TEST(ResolveSdkHostIdTest, DoesNotAdoptCrossNodeBoundWorkerHostId)
{
    const HostPort bound = MakeAddress(501);
    std::unordered_map<std::string, std::string> hostIdMap = { { bound.ToString(), "remoteHost" } };
    // Bound worker NOT confirmed same-host: keep empty so cross-node workers use remote transport and
    // GetTransportHint selects UB/TCP instead of SHM. This is the fix.
    EXPECT_TRUE(ResolveSdkHostId(/*boundWorkerIsLocal=*/false, bound, hostIdMap).empty());
}

TEST(ResolveSdkHostIdTest, RemoteFallbackWorkerDoesNotSelectShmTransport)
{
    const HostPort remoteFallback = MakeAddress(503);
    ::datasystem::ClusterTopologyPb ring;
    (*ring.mutable_members())[remoteFallback.ToString()].set_state(::datasystem::MembershipPb::ACTIVE);
    std::unordered_map<std::string, std::string> hostIdMap = {
        { remoteFallback.ToString(), "remoteHost" }
    };

    const auto sdkHostId =
        ResolveSdkHostId(/*boundWorkerIsLocal=*/false, remoteFallback, hostIdMap);
    WorkerSnapshot snapshot;
    ASSERT_TRUE(BuildWorkerSnapshot(16, ring, hostIdMap, sdkHostId, snapshot).IsOk());
    ASSERT_TRUE(snapshot.shmCandidateAddrs.empty());
    ASSERT_EQ(snapshot.remoteTransportAddrs.size(), 1u);
    EXPECT_EQ(snapshot.remoteTransportAddrs.front(), remoteFallback);

    TransportAdvisor advisor;
    advisor.SetShmCandidateWorkers(snapshot.shmCandidateAddrs);
    EXPECT_NE(advisor.GetTransportHint(remoteFallback), TransportHint::SHM_CANDIDATE);
}

TEST(ResolveSdkHostIdTest, AdoptsGenuineLocalBoundWorkerHostId)
{
    const HostPort bound = MakeAddress(502);
    std::unordered_map<std::string, std::string> hostIdMap = { { bound.ToString(), "localHost" } };
    EXPECT_EQ(ResolveSdkHostId(/*boundWorkerIsLocal=*/true, bound, hostIdMap), "localHost");
    // No host_id_map entry for the bound worker: nothing to adopt, stay empty.
    EXPECT_TRUE(
        ResolveSdkHostId(/*boundWorkerIsLocal=*/true, bound, std::unordered_map<std::string, std::string>{})
            .empty());
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

#ifdef USE_URMA
constexpr char INIT_POLICY_CHILD_ENV[] = "DATASYSTEM_UT_DATA_PLANE_INIT_POLICY_CHILD";

std::string BuildInitPolicyChildMarker(const char *mode, pid_t parentPid)
{
    return std::string(mode) + ":" + std::to_string(parentPid);
}

bool IsInitPolicyChild(const char *mode)
{
    const char *marker = std::getenv(INIT_POLICY_CHILD_ENV);
    return marker != nullptr && marker == BuildInitPolicyChildMarker(mode, getppid());
}

void RunInitPolicyTestInFreshProcess(const char *testName, const char *mode)
{
    const std::string filter = std::string("--gtest_filter=DataPlaneManagerTest.") + testName;
    const std::string markerPrefix = std::string(INIT_POLICY_CHILD_ENV) + "=";
    const std::string marker = markerPrefix + BuildInitPolicyChildMarker(mode, getpid());
    constexpr char gtestRepeatEnvPrefix[] = "GTEST_REPEAT=";
    std::vector<std::string> childEnvironment;
    for (char **env = environ; env != nullptr && *env != nullptr; ++env) {
        if (std::strncmp(*env, markerPrefix.c_str(), markerPrefix.size()) != 0
            && std::strncmp(*env, gtestRepeatEnvPrefix, sizeof(gtestRepeatEnvPrefix) - 1) != 0) {
            childEnvironment.emplace_back(*env);
        }
    }
    childEnvironment.emplace_back(marker);

    std::vector<char *> childEnvironmentPointers;
    childEnvironmentPointers.reserve(childEnvironment.size() + 1);
    for (auto &entry : childEnvironment) {
        childEnvironmentPointers.emplace_back(const_cast<char *>(entry.c_str()));
    }
    childEnvironmentPointers.emplace_back(nullptr);

    std::vector<char *> arguments{ const_cast<char *>("/proc/self/exe"), const_cast<char *>(filter.c_str()),
                                   const_cast<char *>("--gtest_also_run_disabled_tests"),
                                   const_cast<char *>("--gtest_repeat=1"), nullptr };
    pid_t pid = -1;
    const int spawnRc = posix_spawn(&pid, "/proc/self/exe", nullptr, nullptr, arguments.data(),
                                    childEnvironmentPointers.data());
    ASSERT_EQ(spawnRc, 0) << std::strerror(spawnRc);

    int status = 0;
    pid_t waitRc;
    do {
        waitRc = waitpid(pid, &status, 0);
    } while (waitRc == -1 && errno == EINTR);
    ASSERT_EQ(waitRc, pid) << std::strerror(errno);
    ASSERT_TRUE(WIFEXITED(status));
    EXPECT_EQ(WEXITSTATUS(status), 0);
}

TEST(DataPlaneManagerTest, TransportNeutralInitDoesNotActivateUbRuntime)
{
    constexpr char mode[] = "transport-neutral";
    if (!IsInitPolicyChild(mode)) {
        RunInitPolicyTestInFreshProcess("TransportNeutralInitDoesNotActivateUbRuntime", mode);
        return;
    }

    FLAGS_enable_urma = false;
    ASSERT_TRUE(inject::Set("FastTransportManager.Initialize", "return(0)").IsOk());
    DataPlaneManager manager(MakeSignature(), ConnectOptions{}.fastTransportMemSize, {}, nullptr, false, 64, nullptr,
                             false);
    EXPECT_TRUE(manager.Init().IsOk());
    EXPECT_FALSE(IsUrmaEnabled());
}

TEST(DataPlaneManagerTest, DefaultInitStillActivatesUbRuntime)
{
    constexpr char mode[] = "default-eager";
    if (!IsInitPolicyChild(mode)) {
        RunInitPolicyTestInFreshProcess("DefaultInitStillActivatesUbRuntime", mode);
        return;
    }

    FLAGS_enable_urma = false;
    ASSERT_TRUE(inject::Set("FastTransportManager.Initialize", "return(0)").IsOk());
    DataPlaneManager manager(MakeSignature(), ConnectOptions{}.fastTransportMemSize);
    const Status rc = manager.Init();
    EXPECT_EQ(rc.GetCode(), K_URMA_ERROR);
    EXPECT_FALSE(IsUrmaEnabled());
}

TEST(DataPlaneManagerTest, ShmFirstInitAllowsOptionalUbRuntimeFailure)
{
    constexpr char mode[] = "shm-first-optional-ub";
    if (!IsInitPolicyChild(mode)) {
        RunInitPolicyTestInFreshProcess("ShmFirstInitAllowsOptionalUbRuntimeFailure", mode);
        return;
    }

    FLAGS_enable_urma = false;
    ASSERT_TRUE(inject::Set("FastTransportManager.Initialize", "return(0)").IsOk());
    DataPlaneManager manager(MakeSignature(), ConnectOptions{}.fastTransportMemSize, {}, nullptr, false, 64, nullptr,
                             true, true);
    EXPECT_TRUE(manager.Init().IsOk());
    EXPECT_FALSE(IsUrmaEnabled());
}

TEST(DataPlaneManagerTest, DirectPipelineForcesUbRuntimeInitialization)
{
    constexpr char mode[] = "direct-pipeline";
    if (!IsInitPolicyChild(mode)) {
        RunInitPolicyTestInFreshProcess("DirectPipelineForcesUbRuntimeInitialization", mode);
        return;
    }

    FLAGS_enable_urma = false;
    ASSERT_TRUE(inject::Set("FastTransportManager.Initialize", "return(0)").IsOk());
    DataPlaneManager manager(MakeSignature(), ConnectOptions{}.fastTransportMemSize, {}, nullptr, true, 64, nullptr,
                             false);
    const Status rc = manager.Init();
    EXPECT_EQ(rc.GetCode(), K_URMA_ERROR);
    EXPECT_FALSE(IsUrmaEnabled());
}

TEST(DataPlaneManagerTest, UbRuntimeIsNotPublishedBeforeInitializationCompletes)
{
    constexpr char mode[] = "publish-after-init";
    if (!IsInitPolicyChild(mode)) {
        RunInitPolicyTestInFreshProcess("UbRuntimeIsNotPublishedBeforeInitializationCompletes", mode);
        return;
    }

    FLAGS_enable_urma = false;
    ASSERT_TRUE(inject::Set("FastTransportManager.Initialize", "sleep(200)->return(0)").IsOk());
    DataPlaneManager manager(MakeSignature(), ConnectOptions{}.fastTransportMemSize);
    auto init = std::async(std::launch::async, [&manager]() { return manager.Init(); });
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    EXPECT_FALSE(IsUrmaEnabled());
    EXPECT_TRUE(init.get().IsOk());
    EXPECT_TRUE(IsUrmaEnabled());
}
#endif

TEST(DataPlaneManagerTest, ShmCandidateDoesNotDependOnInitialWorkerShmCapability)
{
    ApiDeadlineGuard deadline(1000);
    DataPlaneManager manager(MakeSignature(), ConnectOptions{}.fastTransportMemSize);
    ASSERT_TRUE(manager.Init().IsOk());
    std::shared_ptr<IDataTransporter> first;
    std::shared_ptr<IDataTransporter> second;

    ASSERT_TRUE(manager.GetOrCreate(MakeAddress(1010), TransportHint::SHM_CANDIDATE, first).IsOk());
    ASSERT_NE(first, nullptr);
    EXPECT_EQ(first->Kind(), AccessTransportKind::SHM);
    ASSERT_TRUE(manager.GetOrCreate(MakeAddress(1010), TransportHint::SHM_CANDIDATE, second).IsOk());
    EXPECT_EQ(second, first);
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

TEST(DataPlaneManagerTest, ShmToUbFallbackRebuildsSingleTransporterOnDemand)
{
    FakeDataPlaneManager manager;
    std::shared_ptr<IDataTransporter> firstShm;
    std::shared_ptr<IDataTransporter> ub;
    std::shared_ptr<IDataTransporter> secondShm;
    const HostPort address = MakeAddress(2026);

    ASSERT_TRUE(manager.GetOrCreate(address, TransportHint::SHM_CANDIDATE, firstShm).IsOk());
    ASSERT_TRUE(manager.GetOrCreate(address, TransportHint::UB_CANDIDATE, ub).IsOk());
    ASSERT_TRUE(manager.GetOrCreate(address, TransportHint::SHM_CANDIDATE, secondShm).IsOk());

    EXPECT_NE(firstShm, secondShm);
    EXPECT_NE(firstShm, ub);
    EXPECT_EQ(manager.rpcBuildCount, 1);
    EXPECT_EQ(manager.transportBuildCount, 3);
}

TEST(DataPlaneManagerTest, StaleShmHintDoesNotReplaceUbAfterScaleInDrain)
{
    FakeDataPlaneManager manager;
    std::shared_ptr<IDataTransporter> shm;
    std::shared_ptr<IDataTransporter> ub;
    std::shared_ptr<IDataTransporter> staleShm;
    std::shared_ptr<IDataTransporter> reusedUb;
    const HostPort address = MakeAddress(2027);

    ASSERT_TRUE(manager.GetOrCreate(address, TransportHint::SHM_CANDIDATE, shm).IsOk());
    manager.MarkShmDraining(address);
    ASSERT_TRUE(manager.GetOrCreate(address, TransportHint::UB_CANDIDATE, ub).IsOk());
    const Status staleHintStatus = manager.GetOrCreate(address, TransportHint::SHM_CANDIDATE, staleShm);
    ASSERT_TRUE(manager.GetOrCreate(address, TransportHint::UB_CANDIDATE, reusedUb).IsOk());

    EXPECT_TRUE(IsWorkerDrainingForScaleIn(staleHintStatus));
    EXPECT_EQ(staleShm, nullptr);
    EXPECT_EQ(reusedUb, ub);
    EXPECT_EQ(manager.rpcBuildCount, 1);
    EXPECT_EQ(manager.transportBuildCount, 2);
}

TEST(DataPlaneManagerTest, ScaleInShmRejectionSurvivesEndpointTeardown)
{
    FakeDataPlaneManager manager;
    std::shared_ptr<IDataTransporter> shm;
    std::shared_ptr<IDataTransporter> ub;
    std::shared_ptr<IDataTransporter> staleShm;
    const HostPort address = MakeAddress(2028);

    ASSERT_TRUE(manager.GetOrCreate(address, TransportHint::SHM_CANDIDATE, shm).IsOk());
    manager.MarkShmDraining(address);
    manager.Teardown(address);
    ASSERT_TRUE(manager.GetOrCreate(address, TransportHint::UB_CANDIDATE, ub).IsOk());
    const Status staleHintStatus = manager.GetOrCreate(address, TransportHint::SHM_CANDIDATE, staleShm);

    EXPECT_TRUE(IsWorkerDrainingForScaleIn(staleHintStatus));
    EXPECT_EQ(staleShm, nullptr);
    EXPECT_EQ(manager.rpcBuildCount, 2);
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

TEST(DataPlaneManagerTest, DataPlaneLeaseBlocksResetUntilReleased)
{
    FakeDataPlaneManager manager;
    const HostPort address = MakeAddress(31);
    std::unique_ptr<DataPlaneManager::DataPlaneLease> lease;
    ASSERT_TRUE(manager.AcquireDataPlaneLease(address, TransportHint::UB_CANDIDATE, lease).IsOk());
    ASSERT_NE(lease, nullptr);
    auto leasedTransporter = std::dynamic_pointer_cast<FakeTransporter>(lease->GetTransporter());
    ASSERT_NE(leasedTransporter, nullptr);
    EXPECT_EQ(lease->GetRpcClient(), manager.lastRpcClient);

    std::promise<void> resetStarted;
    auto resetStartedFuture = resetStarted.get_future();
    auto resetFuture = std::async(std::launch::async, [&]() {
        resetStarted.set_value();
        manager.ResetDataPlane(address);
    });
    resetStartedFuture.wait();
    EXPECT_EQ(resetFuture.wait_for(std::chrono::milliseconds(20)), std::future_status::timeout);
    EXPECT_EQ(leasedTransporter->closeCount, 0);

    lease.reset();
    EXPECT_EQ(resetFuture.wait_for(std::chrono::seconds(1)), std::future_status::ready);
    resetFuture.get();
    EXPECT_EQ(leasedTransporter->closeCount, 1);
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
    snapshot.shmCandidateAddrs.push_back(sameHost);
    snapshot.remoteTransportAddrs.push_back(otherHost);
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
    snapshot.remoteTransportAddrs.push_back(live);
    ASSERT_TRUE(manager.UpdateWorkerSnapshot(snapshot).IsOk());
    EXPECT_EQ(manager.GetOrCreate(removed, TransportHint::TCP_ONLY, transporter).GetCode(), K_NOT_READY);
    EXPECT_EQ(removedTransporter->closeCount, 0);

    manager.ReconcileWithSnapshot(snapshot);
    EXPECT_EQ(removedTransporter->closeCount, 1);
    ASSERT_TRUE(manager.GetOrCreate(live, TransportHint::TCP_ONLY, transporter).IsOk());
    EXPECT_EQ(transporter, liveTransporter);
}

TEST(DataPlaneManagerTest, NewerLocationSnapshotAdmitsWorkerMissingFromClientSnapshot)
{
    constexpr uint64_t clientTopologyVersion = 10;
    constexpr uint64_t locationTopologyVersion = 11;
    FakeDataPlaneManager manager;
    const HostPort newWorker = MakeAddress(24);
    WorkerSnapshot snapshot;
    snapshot.ringVersion = clientTopologyVersion;
    snapshot.remoteTransportAddrs.push_back(MakeAddress(23));
    ASSERT_TRUE(manager.UpdateWorkerSnapshot(snapshot).IsOk());

    std::shared_ptr<IDataTransporter> transporter;
    EXPECT_TRUE(manager.GetOrCreateForDataLocation(newWorker, TransportHint::TCP_ONLY, locationTopologyVersion,
                                                  transporter)
                    .IsOk());
    EXPECT_NE(transporter, nullptr);
    EXPECT_EQ(manager.rpcBuildCount, 1);
    EXPECT_EQ(manager.transportBuildCount, 1);
}

TEST(DataPlaneManagerTest, OldLocationSnapshotDoesNotAdmitMissingWorker)
{
    constexpr uint64_t legacyTopologyVersion = 0;
    constexpr uint64_t clientTopologyVersion = 10;
    constexpr uint64_t oldTopologyVersion = clientTopologyVersion - 1;
    FakeDataPlaneManager manager;
    const HostPort newWorker = MakeAddress(24);
    WorkerSnapshot snapshot;
    snapshot.ringVersion = clientTopologyVersion;
    snapshot.remoteTransportAddrs.push_back(MakeAddress(23));
    ASSERT_TRUE(manager.UpdateWorkerSnapshot(snapshot).IsOk());

    std::shared_ptr<IDataTransporter> transporter;
    for (uint64_t version : { legacyTopologyVersion, oldTopologyVersion, clientTopologyVersion }) {
        SCOPED_TRACE(version);
        EXPECT_EQ(manager.GetOrCreateForDataLocation(newWorker, TransportHint::TCP_ONLY, version, transporter)
                      .GetCode(),
                  K_NOT_READY);
        EXPECT_EQ(transporter, nullptr);
    }
    EXPECT_EQ(manager.rpcBuildCount, 0);
    EXPECT_EQ(manager.transportBuildCount, 0);
}

TEST(DataPlaneManagerTest, SnapshotAdvanceRevokesNewLocationAdmissionDuringBuild)
{
    constexpr uint64_t clientTopologyVersion = 10;
    constexpr uint64_t locationTopologyVersion = 11;
    FakeDataPlaneManager manager;
    const HostPort existingWorker = MakeAddress(23);
    const HostPort newWorker = MakeAddress(24);
    WorkerSnapshot snapshot;
    snapshot.ringVersion = clientTopologyVersion;
    snapshot.remoteTransportAddrs.push_back(existingWorker);
    ASSERT_TRUE(manager.UpdateWorkerSnapshot(snapshot).IsOk());
    manager.configureTransporter = [&](const HostPort &, FakeTransporter &) {
        WorkerSnapshot advanced;
        advanced.ringVersion = locationTopologyVersion;
        advanced.remoteTransportAddrs.push_back(existingWorker);
        EXPECT_TRUE(manager.UpdateWorkerSnapshot(advanced).IsOk());
    };

    std::shared_ptr<IDataTransporter> transporter;
    EXPECT_EQ(manager.GetOrCreateForDataLocation(newWorker, TransportHint::TCP_ONLY, locationTopologyVersion,
                                                transporter)
                  .GetCode(),
              K_NOT_READY);
    EXPECT_EQ(transporter, nullptr);
    EXPECT_EQ(manager.rpcBuildCount, 1);
    EXPECT_EQ(manager.transportBuildCount, 1);
    ASSERT_NE(manager.lastTransporter, nullptr);
    EXPECT_EQ(manager.lastTransporter->closeCount, 0);
    DataPlaneManager::EntryMap::const_accessor accessor;
    EXPECT_FALSE(manager.entries_.find(accessor, newWorker.ToString()));
}

TEST(DataPlaneManagerTest, OlderReconcilePreservesLocationAdmittedEndpointUntilSnapshotCatchesUp)
{
    constexpr uint64_t clientTopologyVersion = 10;
    constexpr uint64_t locationTopologyVersion = 11;
    FakeDataPlaneManager manager;
    const HostPort existingWorker = MakeAddress(23);
    const HostPort newWorker = MakeAddress(24);
    WorkerSnapshot snapshot;
    snapshot.ringVersion = clientTopologyVersion;
    snapshot.remoteTransportAddrs.push_back(existingWorker);
    ASSERT_TRUE(manager.UpdateWorkerSnapshot(snapshot).IsOk());

    std::shared_ptr<IDataTransporter> transporter;
    ASSERT_TRUE(manager.GetOrCreateForDataLocation(newWorker, TransportHint::TCP_ONLY, locationTopologyVersion,
                                                  transporter)
                    .IsOk());
    auto admittedTransporter = std::dynamic_pointer_cast<FakeTransporter>(transporter);
    ASSERT_NE(admittedTransporter, nullptr);
    manager.ReconcileWithSnapshot(snapshot);

    EXPECT_EQ(admittedTransporter->closeCount, 0);
    ASSERT_TRUE(manager.GetOrCreateForDataLocation(newWorker, TransportHint::TCP_ONLY, locationTopologyVersion,
                                                  transporter)
                    .IsOk());
    EXPECT_EQ(transporter, admittedTransporter);
    EXPECT_EQ(manager.transportBuildCount, 1);

    snapshot.ringVersion = locationTopologyVersion;
    ASSERT_TRUE(manager.UpdateWorkerSnapshot(snapshot).IsOk());
    manager.ReconcileWithSnapshot(snapshot);
    EXPECT_EQ(admittedTransporter->closeCount, 1);
    EXPECT_EQ(manager.GetOrCreateForDataLocation(newWorker, TransportHint::TCP_ONLY, locationTopologyVersion,
                                                transporter)
                  .GetCode(),
              K_NOT_READY);
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
    latest.remoteTransportAddrs.push_back(live);
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

TEST(ObjectMetadataClientTest, RejectsResultCountMismatchBeforeIndexedAccess)
{
    ApiDeadlineGuard deadline(1000);
    auto manager = std::make_shared<FakeDataPlaneManager>();
    manager->queryAndGetHandler = [](const HostPort &, const QueryAndGetReqPb &,
                                     QueryAndGetRspPb &, std::vector<RpcMessage> &) {
        return Status::OK();
    };
    ObjectMetadataClient metadata(manager, std::make_shared<DeadlineRetry>());
    auto results = MakeMetadataItems({ { 0, "key", MakeAddress(41) } });
    auto batch = MakeMetadataBatch(results);

    EXPECT_EQ(metadata.QueryAndGet(MakeAddress(41), batch, nullptr).GetCode(), K_RUNTIME_ERROR);
}

TEST(ObjectMetadataClientTest, EmptyLocationsFailOnlyTheirInputItem)
{
    ApiDeadlineGuard deadline(1000);
    auto manager = std::make_shared<FakeDataPlaneManager>();
    manager->queryAndGetHandler = [](const HostPort &, const QueryAndGetReqPb &,
                                     QueryAndGetRspPb &response, std::vector<RpcMessage> &) {
        response.add_results()->mutable_location()->set_object_key("missing");
        AddLocation(response, "present", MakeAddress(51));
        return Status::OK();
    };
    ObjectMetadataClient metadata(manager, std::make_shared<DeadlineRetry>());
    auto results = MakeMetadataItems({ { 0, "missing", MakeAddress(41) },
                                       { 1, "present", MakeAddress(41) } });
    auto batch = MakeMetadataBatch(results);

    ASSERT_TRUE(metadata.QueryAndGet(MakeAddress(41), batch, nullptr).IsOk());
    ASSERT_EQ(results.size(), 2u);
    EXPECT_EQ(results[0].status.GetCode(), K_NOT_FOUND);
    EXPECT_TRUE(results[1].status.IsOk());
}

TEST(ObjectMetadataClientTest, DoesNotReportDeadlineExpiredBeforeAccess)
{
    ApiDeadlineGuard deadline(100, InUs{});
    auto manager = std::make_shared<FakeDataPlaneManager>();
    size_t invokeCount = 0;
    manager->queryAndGetHandler = [&invokeCount](const HostPort &, const QueryAndGetReqPb &,
                                                 QueryAndGetRspPb &, std::vector<RpcMessage> &) {
        ++invokeCount;
        return Status::OK();
    };
    size_t failureCount = 0;
    ObjectMetadataClient metadata(manager, std::make_shared<DeadlineRetry>(), nullptr, nullptr, 0,
                                  [&failureCount](const HostPort &, const Status &) { ++failureCount; });
    auto results = MakeMetadataItems({ { 0, "key", MakeAddress(41) } });
    auto batch = MakeMetadataBatch(results);
    std::this_thread::sleep_for(std::chrono::milliseconds(1));

    EXPECT_EQ(metadata.QueryAndGet(MakeAddress(41), batch, nullptr).GetCode(), K_RPC_DEADLINE_EXCEEDED);
    EXPECT_EQ(invokeCount, 0u);
    EXPECT_EQ(failureCount, 0u);
}

TEST(ObjectMetadataClientTest, ConnectionFailureRequestsRerouteWithoutFixedOwnerRetry)
{
    ApiDeadlineGuard deadline(1000);
    auto manager = std::make_shared<FakeDataPlaneManager>();
    manager->rpcBuildStatuses.emplace_back(K_RPC_UNAVAILABLE, "unavailable before dispatch");
    std::vector<std::pair<HostPort, Status>> failures;
    ObjectMetadataClient metadata(
        manager, std::make_shared<DeadlineRetry>(), nullptr, nullptr, 0,
        [&failures](const HostPort &address, const Status &status) { failures.emplace_back(address, status); });
    auto results = MakeMetadataItems({ { 0, "key", MakeAddress(41) } });
    auto batch = MakeMetadataBatch(results);

    const auto rc = metadata.QueryAndGet(MakeAddress(41), batch, nullptr);

    EXPECT_TRUE(IsTransportSnapshotStaleLocation(rc));
    EXPECT_EQ(manager->rpcBuildCount, 1);
    ASSERT_EQ(failures.size(), 1u);
    EXPECT_EQ(failures[0].first, MakeAddress(41));
    EXPECT_EQ(failures[0].second.GetCode(), K_RPC_UNAVAILABLE);
}

TEST(ObjectMetadataClientTest, DispatchedDeadlineRequestsRerouteWithoutFixedOwnerRetry)
{
    ApiDeadlineGuard deadline(1000);
    auto manager = std::make_shared<FakeDataPlaneManager>();
    int invokeCount = 0;
    manager->queryAndGetHandler = [&invokeCount](const HostPort &, const QueryAndGetReqPb &,
                                                 QueryAndGetRspPb &, std::vector<RpcMessage> &) {
        ++invokeCount;
        return Status(K_RPC_DEADLINE_EXCEEDED, "metadata owner deadline");
    };
    std::vector<std::pair<HostPort, Status>> failures;
    ObjectMetadataClient metadata(
        manager, std::make_shared<DeadlineRetry>(), nullptr, nullptr, 0,
        [&failures](const HostPort &address, const Status &status) { failures.emplace_back(address, status); });
    auto results = MakeMetadataItems({ { 0, "key", MakeAddress(41) } });
    auto batch = MakeMetadataBatch(results);

    const auto rc = metadata.QueryAndGet(MakeAddress(41), batch, nullptr);

    EXPECT_TRUE(IsTransportSnapshotStaleLocation(rc));
    EXPECT_EQ(invokeCount, 1);
    ASSERT_EQ(failures.size(), 1u);
    EXPECT_EQ(failures[0].first, MakeAddress(41));
    EXPECT_EQ(failures[0].second.GetCode(), K_RPC_DEADLINE_EXCEEDED);
}

TEST(ObjectMetadataClientTest, PeerDeadTearsDownWithoutRetry)
{
    ApiDeadlineGuard deadline(1000);
    auto manager = std::make_shared<FakeDataPlaneManager>();
    int invokeCount = 0;
    manager->queryAndGetHandler = [&invokeCount](const HostPort &, const QueryAndGetReqPb &,
                                                 QueryAndGetRspPb &, std::vector<RpcMessage> &) {
        ++invokeCount;
        return Status(K_RPC_PEER_DEAD, "peer dead");
    };
    ObjectMetadataClient metadata(manager, std::make_shared<DeadlineRetry>());
    auto results = MakeMetadataItems({ { 0, "key", MakeAddress(41) } });
    auto batch = MakeMetadataBatch(results);

    EXPECT_TRUE(IsTransportSnapshotStaleLocation(metadata.QueryAndGet(MakeAddress(41), batch, nullptr)));
    EXPECT_EQ(invokeCount, 1);
    EXPECT_EQ(manager->rpcBuildCount, 1);

    manager->queryAndGetHandler = [](const HostPort &, const QueryAndGetReqPb &,
                                     QueryAndGetRspPb &response, std::vector<RpcMessage> &) {
        AddLocation(response, "key", MakeAddress(51));
        return Status::OK();
    };
    ASSERT_TRUE(metadata.QueryAndGet(MakeAddress(41), batch, nullptr).IsOk());
    EXPECT_EQ(manager->rpcBuildCount, 2);
}

TEST(ObjectMetadataClientTest, MetadataAndDataReuseOneEndpointRpcClient)
{
    ApiDeadlineGuard deadline(1000);
    auto manager = std::make_shared<FakeDataPlaneManager>();
    manager->queryAndGetHandler = [](const HostPort &address, const QueryAndGetReqPb &,
                                     QueryAndGetRspPb &response, std::vector<RpcMessage> &) {
        AddLocation(response, "key", address);
        return Status::OK();
    };
    ObjectMetadataClient metadata(manager, std::make_shared<DeadlineRetry>());
    auto results = MakeMetadataItems({ { 0, "key", MakeAddress(41) } });
    auto batch = MakeMetadataBatch(results);
    ASSERT_TRUE(metadata.QueryAndGet(MakeAddress(41), batch, nullptr).IsOk());
    std::shared_ptr<IDataTransporter> transporter;

    ASSERT_TRUE(manager->GetOrCreate(MakeAddress(41), TransportHint::TCP_ONLY, transporter).IsOk());
    EXPECT_EQ(manager->rpcBuildCount, 1);
    EXPECT_EQ(manager->transportBuildCount, 1);
}

TEST(ObjectMetadataClientTest, TcpInlineDataMovesRpcPayloadIntoMetadataResult)
{
    ApiDeadlineGuard deadline(1000);
    auto manager = std::make_shared<FakeDataPlaneManager>();
    manager->queryAndGetHandler = [](const HostPort &, const QueryAndGetReqPb &request,
                                     QueryAndGetRspPb &response, std::vector<RpcMessage> &payloads) {
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

    ASSERT_TRUE(metadata.QueryAndGet(MakeAddress(41), batch, nullptr).IsOk());
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
    manager->queryAndGetHandler = [](const HostPort &, const QueryAndGetReqPb &request,
                                     QueryAndGetRspPb &response, std::vector<RpcMessage> &) {
        EXPECT_TRUE(request.has_data_request());
        EXPECT_TRUE(request.data_request().has_tcp());
        AddLocation(response, "key", MakeAddress(51), 6);
        return Status::OK();
    };
    ObjectMetadataClient metadata(manager, std::make_shared<DeadlineRetry>(),
                                  std::make_shared<FixedTransportAdvisor>(TransportHint::TCP_ONLY));
    auto results = MakeMetadataItems({ { 0, "key", MakeAddress(41) } });
    auto batch = MakeMetadataBatch(results);

    ASSERT_TRUE(metadata.QueryAndGet(MakeAddress(41), batch, nullptr).IsOk());
    ASSERT_EQ(results.size(), 1u);
    EXPECT_TRUE(results[0].status.IsOk());
    EXPECT_FALSE(results[0].inlineData.has_value());
    EXPECT_EQ(results[0].location.object_locations(0), MakeAddress(51).ToString());
}

TEST(ObjectMetadataClientTest, QueryAndGetCopiesLocationTopologyVersion)
{
    constexpr uint64_t locationTopologyVersion = 11;
    ApiDeadlineGuard deadline(1000);
    auto manager = std::make_shared<FakeDataPlaneManager>();
    manager->queryAndGetHandler = [](const HostPort &, const QueryAndGetReqPb &,
                                     QueryAndGetRspPb &response, std::vector<RpcMessage> &) {
        AddLocation(response, "key", MakeAddress(51), 4, locationTopologyVersion);
        return Status::OK();
    };
    ObjectMetadataClient metadata(manager, std::make_shared<DeadlineRetry>(),
                                  std::make_shared<FixedTransportAdvisor>(TransportHint::TCP_ONLY));
    auto results = MakeMetadataItems({ { 0, "key", MakeAddress(41) } });
    auto batch = MakeMetadataBatch(results);

    ASSERT_TRUE(metadata.QueryAndGet(MakeAddress(41), batch, nullptr).IsOk());
    ASSERT_EQ(results.size(), 1u);
    EXPECT_EQ(results[0].location.topology_version(), locationTopologyVersion);
}

TEST(ObjectMetadataClientTest, RejectsInvalidTcpInlinePayloadIndex)
{
    ApiDeadlineGuard deadline(1000);
    auto manager = std::make_shared<FakeDataPlaneManager>();
    manager->queryAndGetHandler = [](const HostPort &, const QueryAndGetReqPb &,
                                     QueryAndGetRspPb &response, std::vector<RpcMessage> &payloads) {
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

    EXPECT_EQ(metadata.QueryAndGet(MakeAddress(41), batch, nullptr).GetCode(), K_RUNTIME_ERROR);
}

TEST(ObjectMetadataClientTest, UbInlineDataUsesConfiguredCapacityAndExternalBuffer)
{
    ApiDeadlineGuard deadline(1000);
    auto manager = std::make_shared<FakeDataPlaneManager>();
    auto bufferProvider = std::make_shared<FakeUbBufferProvider>();
    bufferProvider->maxGetSize = 32;
    manager->queryAndGetHandler = [](const HostPort &, const QueryAndGetReqPb &request,
                                     QueryAndGetRspPb &response, std::vector<RpcMessage> &payloads) {
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

    ASSERT_TRUE(metadata.QueryAndGet(MakeAddress(41), batch, nullptr).IsOk());
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

#ifdef USE_URMA
TEST(ObjectMetadataClientTest, ShmPreparationFailureTriesUbBeforeTcp)
{
    ApiDeadlineGuard deadline(1000);
    const bool enableUrma = FLAGS_enable_urma;
    Raii restoreEnableUrma([enableUrma]() { FLAGS_enable_urma = enableUrma; });
    FLAGS_enable_urma = true;
    auto manager = std::make_shared<FakeDataPlaneManager>();
    auto bufferProvider = std::make_shared<FakeUbBufferProvider>();
    manager->queryAndGetHandler = [](const HostPort &, const QueryAndGetReqPb &request,
                                     QueryAndGetRspPb &response, std::vector<RpcMessage> &) {
        EXPECT_TRUE(request.data_request().has_ub());
        AddLocation(response, "key", MakeAddress(51), 6);
        return Status::OK();
    };
    ObjectMetadataClient metadata(manager, std::make_shared<DeadlineRetry>(),
                                  std::make_shared<FixedTransportAdvisor>(TransportHint::SHM_CANDIDATE),
                                  bufferProvider, 16);
    auto results = MakeMetadataItems({ { 0, "key", MakeAddress(41) } });
    auto batch = MakeMetadataBatch(results);
    auto readContext = std::make_shared<TransportReadContext>();

    ASSERT_TRUE(metadata.QueryAndGet(MakeAddress(41), batch, readContext).IsOk());
    EXPECT_EQ(manager->transportBuildCount, 2);
    EXPECT_EQ(bufferProvider->allocateCount, 1);
}
#endif

TEST(ObjectMetadataClientTest, UbCapacityMissReleasesBufferAndFallsBack)
{
    ApiDeadlineGuard deadline(1000);
    auto manager = std::make_shared<FakeDataPlaneManager>();
    auto bufferProvider = std::make_shared<FakeUbBufferProvider>();
    manager->queryAndGetHandler = [](const HostPort &, const QueryAndGetReqPb &request,
                                     QueryAndGetRspPb &response, std::vector<RpcMessage> &) {
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

    ASSERT_TRUE(metadata.QueryAndGet(MakeAddress(41), batch, nullptr).IsOk());
    EXPECT_FALSE(results[0].inlineData.has_value());
    EXPECT_TRUE(bufferProvider->lastOwner.expired());
}

TEST(ObjectMetadataClientTest, UbBufferAllocationFailureFallsBackToTcp)
{
    ApiDeadlineGuard deadline(1000);
    auto manager = std::make_shared<FakeDataPlaneManager>();
    auto bufferProvider = std::make_shared<FakeUbBufferProvider>();
    bufferProvider->allocateStatus = Status(K_OUT_OF_MEMORY, "allocation failed");
    manager->queryAndGetHandler = [](const HostPort &, const QueryAndGetReqPb &request,
                                     QueryAndGetRspPb &response, std::vector<RpcMessage> &) {
        EXPECT_TRUE(request.has_data_request());
        EXPECT_TRUE(request.data_request().has_tcp());
        AddLocation(response, "key", MakeAddress(51), 6);
        return Status::OK();
    };
    ObjectMetadataClient metadata(manager, std::make_shared<DeadlineRetry>(),
                                  std::make_shared<FixedTransportAdvisor>(TransportHint::UB_CANDIDATE),
                                  bufferProvider, 16);
    auto results = MakeMetadataItems({ { 0, "key", MakeAddress(41) } });
    auto batch = MakeMetadataBatch(results);

    ASSERT_TRUE(metadata.QueryAndGet(MakeAddress(41), batch, nullptr).IsOk());
    EXPECT_EQ(bufferProvider->allocateCount, 1);
    EXPECT_EQ(manager->transportBuildCount, 1);
    EXPECT_FALSE(results[0].inlineData.has_value());
}

TEST(ObjectMetadataClientTest, UbConnectionFailureFallsBackToTcp)
{
    ApiDeadlineGuard deadline(1000);
    auto manager = std::make_shared<FakeDataPlaneManager>();
    manager->transportBuildStatuses.emplace_back(K_URMA_ERROR, "connect failed");
    auto bufferProvider = std::make_shared<FakeUbBufferProvider>();
    manager->queryAndGetHandler = [](const HostPort &, const QueryAndGetReqPb &request,
                                     QueryAndGetRspPb &response, std::vector<RpcMessage> &) {
        EXPECT_TRUE(request.has_data_request());
        EXPECT_TRUE(request.data_request().has_tcp());
        AddLocation(response, "key", MakeAddress(51), 6);
        return Status::OK();
    };
    ObjectMetadataClient metadata(manager, std::make_shared<DeadlineRetry>(),
                                  std::make_shared<FixedTransportAdvisor>(TransportHint::UB_CANDIDATE),
                                  bufferProvider, 16);
    auto results = MakeMetadataItems({ { 0, "key", MakeAddress(41) } });
    auto batch = MakeMetadataBatch(results);

    ASSERT_TRUE(metadata.QueryAndGet(MakeAddress(41), batch, nullptr).IsOk());
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
    request.context = MakeReadContext();
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
    EXPECT_EQ(replicas->traceDecisions, std::vector<bool>({ false }));
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
    request.context = MakeReadContext();
    request.items = { { 0, "key", MakeAddress(41) } };
    request.traceEnabled = true;
    ObjectReadResult result;

    ASSERT_TRUE(flow.Run(request, result).IsOk());
    auto phases = ComputePhaseDurations(Trace::Instance().GetLatencyTicks(),
                                        Trace::Instance().GetLatencyTickCount(),
                                        Trace::Instance().GetLatencyTickDroppedCount());
    EXPECT_NE(phases.Find(LatencySummaryPhase::CLIENT_RPC_DIRECT_QUERY_AND_GET), nullptr);
    EXPECT_NE(phases.Find(LatencySummaryPhase::CLIENT_RPC_DIRECT_GET_DATA), nullptr);
    EXPECT_EQ(replicas->traceDecisions, std::vector<bool>({ true }));
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
    request.context = MakeReadContext();
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
    request.context = MakeReadContext();
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
    request.context = MakeReadContext();
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
    request.context = MakeReadContext();
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

TEST(ObjectClientTransportTest, ReadTransportRoundPreservesMixedItemStatusesWhenAggregateIsStale)
{
    ApiDeadlineGuard deadline(1000);
    const auto ownerAddress = MakeAddress(41);
    const auto staleReplicaAddress = MakeAddress(90);
    auto metadata = std::make_shared<FakeObjectMetadataClient>();
    auto replicas = std::make_shared<FakeReplicaReader>();
    replicas->itemStatuses.emplace("stale", MakeStaleSnapshotStatus(staleReplicaAddress));
    replicas->itemStatuses.emplace("missing", Status(K_NOT_FOUND, "missing"));
    auto transportLayer = std::make_unique<TestTransportLayer>(std::make_shared<FakeDataPlaneManager>());
    transportLayer->SetObjectRead(
        std::make_unique<ObjectReadFlow>(metadata, replicas, std::make_shared<ThreadPool>(0, 2, "object_read_test")));

    ConnectOptions options;
    options.host = "127.0.0.1";
    options.port = 31501;
    auto client = std::make_shared<object_cache::ObjectClientImpl>(options);
    auto workerApi = std::make_shared<object_cache::ClientWorkerRemoteApi>(MakeAddress(31501));
    workerApi->clientId_ = "transport-round-test-client";
    client->workerApi_.emplace_back(workerApi);
    client->transportLayer_ = std::move(transportLayer);
    std::atomic_store(&client->routing_, MakeSingleWorkerRouting(ownerAddress));

    const std::vector<std::string> objectKeys{ "stale", "missing" };
    std::vector<std::shared_ptr<Buffer>> buffers(objectKeys.size());
    std::vector<Status> itemStatuses(objectKeys.size(), Status(K_NOT_READY, "pending"));
    AccessTransportKind actualKind = AccessTransportKind::SHM;
    Status transportStatus;

    ASSERT_TRUE(client
                    ->ReadTransportRound(objectKeys, false, 1000, false, buffers, itemStatuses, actualKind,
                                         transportStatus)
                    .IsOk());
    EXPECT_TRUE(IsTransportSnapshotStaleLocation(transportStatus));
    ASSERT_EQ(itemStatuses.size(), 2u);
    EXPECT_TRUE(IsTransportSnapshotStaleLocation(itemStatuses[0]));
    EXPECT_EQ(itemStatuses[1].GetCode(), K_NOT_FOUND);
    ASSERT_EQ(metadata->keyGroups.size(), 1u);
    EXPECT_EQ(metadata->keyGroups[0], objectKeys);
    ASSERT_EQ(replicas->batchKeys.size(), 1u);
    EXPECT_EQ(replicas->batchKeys[0], objectKeys);
}

TEST(ObjectClientTransportTest, DrainingLocationRetriesOnlyPendingKeys)
{
    ApiDeadlineGuard deadline(1000);
    const auto ownerAddress = MakeAddress(41);
    auto metadata = std::make_shared<FakeObjectMetadataClient>();
    auto replicas = std::make_shared<FakeReplicaReader>();
    std::atomic<int> drainingAttempts{ 0 };
    replicas->statusHandler = [&drainingAttempts](const std::string &key) {
        if (key == "draining" && drainingAttempts.fetch_add(1) == 0) {
            return MakeWorkerDrainingStatus();
        }
        return Status::OK();
    };
    replicas->resultHandler = [](const std::string &, ObjectReadItemResult &result) {
        result.data.kind = AccessTransportKind::TCP;
        result.data.response.set_data_size(4);
        result.data.response.set_data_source(DataTransferSource::DATA_IN_PAYLOAD);
        RpcMessage payload;
        ASSERT_TRUE(payload.CopyString("data").IsOk());
        result.data.rpcPayloads.emplace_back(std::move(payload));
    };
    auto transportLayer = std::make_unique<TestTransportLayer>(std::make_shared<FakeDataPlaneManager>());
    transportLayer->SetObjectRead(
        std::make_unique<ObjectReadFlow>(metadata, replicas, std::make_shared<ThreadPool>(0, 2, "object_read_test")));

    ConnectOptions options;
    options.host = "127.0.0.1";
    options.port = 31501;
    auto client = std::make_shared<object_cache::ObjectClientImpl>(options);
    auto workerApi = std::make_shared<object_cache::ClientWorkerRemoteApi>(MakeAddress(31501));
    workerApi->clientId_ = "draining-retry-test-client";
    client->workerApi_.emplace_back(workerApi);
    client->transportLayer_ = std::move(transportLayer);
    std::atomic_store(&client->routing_, MakeSingleWorkerRouting(ownerAddress));

    const std::vector<std::string> objectKeys{ "draining", "stable" };
    std::vector<std::shared_ptr<Buffer>> buffers(objectKeys.size());
    ASSERT_TRUE(client->GetFromTransportLayer(objectKeys, buffers, false, 1000, false).IsOk());
    ASSERT_EQ(metadata->keyGroups.size(), 2u);
    EXPECT_EQ(metadata->keyGroups[0], objectKeys);
    EXPECT_EQ(metadata->keyGroups[1], std::vector<std::string>({ "draining" }));
    EXPECT_EQ(replicas->batchKeys, std::vector<std::vector<std::string>>({ objectKeys }));
    EXPECT_EQ(replicas->unaryKeys, std::vector<std::string>({ "draining" }));
    EXPECT_NE(buffers[0], nullptr);
    EXPECT_NE(buffers[1], nullptr);
}

TEST(ObjectClientTransportTest, PeerDeadMetadataOwnerRetriesRoutedRead)
{
    ApiDeadlineGuard deadline(1000);
    const auto ownerAddress = MakeAddress(41);
    const auto refreshedOwnerAddress = MakeAddress(42);
    ConnectOptions options;
    options.host = "127.0.0.1";
    options.port = 31501;
    auto client = std::make_shared<object_cache::ObjectClientImpl>(options);
    auto workerApi = std::make_shared<object_cache::ClientWorkerRemoteApi>(MakeAddress(31501));
    workerApi->clientId_ = "peer-dead-metadata-retry-test-client";
    client->workerApi_.emplace_back(workerApi);
    std::atomic_store(&client->routing_, MakeSingleWorkerRouting(ownerAddress));
    auto metadata = std::make_shared<FakeObjectMetadataClient>();
    std::atomic<int> metadataAttempts{ 0 };
    metadata->queryAndGetHandler = [&metadataAttempts, &client, &refreshedOwnerAddress](
                                       const HostPort &, const ObjectMetadataBatch &) {
        if (metadataAttempts.fetch_add(1) == 0) {
            std::atomic_store(&client->routing_, MakeSingleWorkerRouting(refreshedOwnerAddress));
            return Status(K_RPC_PEER_DEAD, "metadata owner is dead");
        }
        return Status::OK();
    };
    auto replicas = std::make_shared<FakeReplicaReader>();
    replicas->resultHandler = [](const std::string &, ObjectReadItemResult &result) {
        result.data.kind = AccessTransportKind::TCP;
        result.data.response.set_data_size(4);
        result.data.response.set_data_source(DataTransferSource::DATA_IN_PAYLOAD);
        RpcMessage payload;
        ASSERT_TRUE(payload.CopyString("data").IsOk());
        result.data.rpcPayloads.emplace_back(std::move(payload));
    };
    auto transportLayer = std::make_unique<TestTransportLayer>(std::make_shared<FakeDataPlaneManager>());
    transportLayer->SetObjectRead(
        std::make_unique<ObjectReadFlow>(metadata, replicas, std::make_shared<ThreadPool>(0, 2, "object_read_test")));

    client->transportLayer_ = std::move(transportLayer);

    const std::vector<std::string> objectKeys{ "peer-dead" };
    std::vector<std::shared_ptr<Buffer>> buffers(objectKeys.size());
    ASSERT_TRUE(client->GetFromTransportLayer(objectKeys, buffers, false, 1000, false).IsOk());
    EXPECT_EQ(metadataAttempts.load(), 2);
    EXPECT_EQ(metadata->keyGroups.size(), 2u);
    EXPECT_EQ(metadata->addresses, std::vector<HostPort>({ ownerAddress, refreshedOwnerAddress }));
    EXPECT_EQ(replicas->unaryKeys, objectKeys);
    EXPECT_NE(buffers[0], nullptr);
}

TEST(ObjectClientTransportTest, DrainingLocationStopsAfterThreeFastRetries)
{
    ApiDeadlineGuard deadline(1000);
    const auto ownerAddress = MakeAddress(41);
    auto metadata = std::make_shared<FakeObjectMetadataClient>();
    auto replicas = std::make_shared<FakeReplicaReader>();
    replicas->statusHandler = [](const std::string &) { return MakeWorkerDrainingStatus(); };
    auto transportLayer = std::make_unique<TestTransportLayer>(std::make_shared<FakeDataPlaneManager>());
    transportLayer->SetObjectRead(
        std::make_unique<ObjectReadFlow>(metadata, replicas, std::make_shared<ThreadPool>(0, 2, "object_read_test")));

    ConnectOptions options;
    options.host = "127.0.0.1";
    options.port = 31501;
    auto client = std::make_shared<object_cache::ObjectClientImpl>(options);
    auto workerApi = std::make_shared<object_cache::ClientWorkerRemoteApi>(MakeAddress(31501));
    workerApi->clientId_ = "draining-budget-test-client";
    client->workerApi_.emplace_back(workerApi);
    client->transportLayer_ = std::move(transportLayer);
    std::atomic_store(&client->routing_, MakeSingleWorkerRouting(ownerAddress));

    const std::vector<std::string> objectKeys{ "draining" };
    std::vector<std::shared_ptr<Buffer>> buffers(objectKeys.size());
    Status rc = client->GetFromTransportLayer(objectKeys, buffers, false, 1000, false);
    EXPECT_EQ(rc.GetCode(), K_NOT_READY);
    EXPECT_NE(rc.GetMsg().find("Worker is draining for ScaleIn"), std::string::npos);
    EXPECT_EQ(metadata->keyGroups.size(), 4u);
    EXPECT_EQ(replicas->unaryKeys.size(), 4u);
    EXPECT_EQ(buffers[0], nullptr);
}

TEST(ObjectClientTransportTest, AlternatingRefreshableErrorsKeepConsumedRetryBudgets)
{
    ApiDeadlineGuard deadline(1000);
    const auto ownerAddress = MakeAddress(41);
    auto metadata = std::make_shared<FakeObjectMetadataClient>();
    auto replicas = std::make_shared<FakeReplicaReader>();
    std::atomic<int> attempts{ 0 };
    replicas->statusHandler = [&attempts, &ownerAddress](const std::string &) {
        return attempts.fetch_add(1) % 2 == 0 ? MakeWorkerDrainingStatus() : MakeStaleSnapshotStatus(ownerAddress);
    };
    auto transportLayer = std::make_unique<TestTransportLayer>(std::make_shared<FakeDataPlaneManager>());
    transportLayer->SetObjectRead(
        std::make_unique<ObjectReadFlow>(metadata, replicas, std::make_shared<ThreadPool>(0, 2, "object_read_test")));

    ConnectOptions options;
    options.host = "127.0.0.1";
    options.port = 31501;
    auto client = std::make_shared<object_cache::ObjectClientImpl>(options);
    auto workerApi = std::make_shared<object_cache::ClientWorkerRemoteApi>(MakeAddress(31501));
    workerApi->clientId_ = "alternating-refresh-budget-test-client";
    client->workerApi_.emplace_back(workerApi);
    client->transportLayer_ = std::move(transportLayer);
    std::atomic_store(&client->routing_, MakeSingleWorkerRouting(ownerAddress));

    const std::vector<std::string> objectKeys{ "alternating" };
    std::vector<std::shared_ptr<Buffer>> buffers(objectKeys.size());
    Status rc = client->GetFromTransportLayer(objectKeys, buffers, false, 1000, false);
    EXPECT_EQ(rc.GetCode(), K_NOT_READY);
    EXPECT_EQ(attempts.load(), 7);
    EXPECT_EQ(metadata->keyGroups.size(), 7u);
    EXPECT_EQ(replicas->unaryKeys.size(), 7u);
    EXPECT_EQ(buffers[0], nullptr);
}

TEST(ObjectClientTransportTest, StaleLocationStopsAfterFiveRetries)
{
    ApiDeadlineGuard deadline(1000);
    const auto ownerAddress = MakeAddress(41);
    auto metadata = std::make_shared<FakeObjectMetadataClient>();
    auto replicas = std::make_shared<FakeReplicaReader>();
    replicas->statusHandler = [&ownerAddress](const std::string &) { return MakeStaleSnapshotStatus(ownerAddress); };
    auto transportLayer = std::make_unique<TestTransportLayer>(std::make_shared<FakeDataPlaneManager>());
    transportLayer->SetObjectRead(
        std::make_unique<ObjectReadFlow>(metadata, replicas, std::make_shared<ThreadPool>(0, 2, "object_read_test")));

    ConnectOptions options;
    options.host = "127.0.0.1";
    options.port = 31501;
    auto client = std::make_shared<object_cache::ObjectClientImpl>(options);
    auto workerApi = std::make_shared<object_cache::ClientWorkerRemoteApi>(MakeAddress(31501));
    workerApi->clientId_ = "stale-budget-test-client";
    client->workerApi_.emplace_back(workerApi);
    client->transportLayer_ = std::move(transportLayer);
    std::atomic_store(&client->routing_, MakeSingleWorkerRouting(ownerAddress));

    const std::vector<std::string> objectKeys{ "stale" };
    std::vector<std::shared_ptr<Buffer>> buffers(objectKeys.size());
    Status rc = client->GetFromTransportLayer(objectKeys, buffers, false, 1000, false);
    EXPECT_EQ(rc.GetCode(), K_RPC_UNAVAILABLE);
    EXPECT_FALSE(IsTransportSnapshotStaleLocation(rc)) << rc.ToString();
    EXPECT_NE(rc.GetMsg().find(STALE_TRANSPORT_SNAPSHOT_MESSAGE), std::string::npos) << rc.ToString();
    EXPECT_EQ(metadata->keyGroups.size(), 6u);
    EXPECT_EQ(replicas->unaryKeys.size(), 6u);
    EXPECT_EQ(buffers[0], nullptr);
}

TEST(ObjectClientTransportTest, FirstStaleLocationRetryUsesZeroBackoff)
{
    constexpr uint8_t firstRetry = 0;
    constexpr uint8_t secondRetry = 1;
    constexpr int64_t immediateBackoffMs = 0;
    constexpr int64_t staleBackoffMs = 20;
    constexpr int64_t drainingBackoffMs = 1;

    EXPECT_EQ(SelectLocationRefreshBackoffMs(false, firstRetry, staleBackoffMs), immediateBackoffMs);
    EXPECT_EQ(SelectLocationRefreshBackoffMs(false, secondRetry, staleBackoffMs), staleBackoffMs);
    EXPECT_EQ(SelectLocationRefreshBackoffMs(true, firstRetry, drainingBackoffMs), drainingBackoffMs);
}

TEST(ObjectClientTransportTest, StaleLocationSlowReadDeadlineReturnsPublicDeadline)
{
    ApiDeadlineGuard deadline(10);
    const auto ownerAddress = MakeAddress(41);
    auto metadata = std::make_shared<FakeObjectMetadataClient>();
    auto replicas = std::make_shared<FakeReplicaReader>();
    replicas->statusHandler = [&ownerAddress](const std::string &) {
        std::this_thread::sleep_for(std::chrono::milliseconds(25));
        return MakeStaleSnapshotStatus(ownerAddress);
    };
    auto transportLayer = std::make_unique<TestTransportLayer>(std::make_shared<FakeDataPlaneManager>());
    transportLayer->SetObjectRead(
        std::make_unique<ObjectReadFlow>(metadata, replicas, std::make_shared<ThreadPool>(0, 2, "object_read_test")));

    ConnectOptions options;
    options.host = "127.0.0.1";
    options.port = 31501;
    auto client = std::make_shared<object_cache::ObjectClientImpl>(options);
    auto workerApi = std::make_shared<object_cache::ClientWorkerRemoteApi>(MakeAddress(31501));
    workerApi->clientId_ = "stale-slow-read-deadline-test-client";
    client->workerApi_.emplace_back(workerApi);
    client->transportLayer_ = std::move(transportLayer);
    std::atomic_store(&client->routing_, MakeSingleWorkerRouting(ownerAddress));

    const std::vector<std::string> objectKeys{ "stale" };
    std::vector<std::shared_ptr<Buffer>> buffers(objectKeys.size());
    Status rc = client->GetFromTransportLayer(objectKeys, buffers, false, 1000, false);
    EXPECT_EQ(rc.GetCode(), K_RPC_DEADLINE_EXCEEDED) << rc.ToString();
    EXPECT_FALSE(IsTransportSnapshotStaleLocation(rc)) << rc.ToString();
    EXPECT_NE(rc.GetMsg().find(STALE_TRANSPORT_SNAPSHOT_MESSAGE), std::string::npos) << rc.ToString();
    EXPECT_EQ(metadata->keyGroups.size(), 1u);
    EXPECT_EQ(replicas->unaryKeys.size(), 1u);
    EXPECT_EQ(buffers[0], nullptr);
}

TEST(ObjectClientTransportTest, BatchStaleLocationBudgetReturnsPublicAvailability)
{
    ApiDeadlineGuard deadline(1000);
    const auto ownerAddress = MakeAddress(41);
    auto metadata = std::make_shared<FakeObjectMetadataClient>();
    auto replicas = std::make_shared<FakeReplicaReader>();
    replicas->statusHandler = [&ownerAddress](const std::string &) { return MakeStaleSnapshotStatus(ownerAddress); };
    auto transportLayer = std::make_unique<TestTransportLayer>(std::make_shared<FakeDataPlaneManager>());
    transportLayer->SetObjectRead(
        std::make_unique<ObjectReadFlow>(metadata, replicas, std::make_shared<ThreadPool>(0, 2, "object_read_test")));

    ConnectOptions options;
    options.host = "127.0.0.1";
    options.port = 31501;
    auto client = std::make_shared<object_cache::ObjectClientImpl>(options);
    auto workerApi = std::make_shared<object_cache::ClientWorkerRemoteApi>(MakeAddress(31501));
    workerApi->clientId_ = "batch-stale-budget-test-client";
    client->workerApi_.emplace_back(workerApi);
    client->transportLayer_ = std::move(transportLayer);
    std::atomic_store(&client->routing_, MakeSingleWorkerRouting(ownerAddress));

    const std::vector<std::string> objectKeys{ "stale-a", "stale-b" };
    std::vector<std::shared_ptr<Buffer>> buffers(objectKeys.size());
    Status rc = client->GetFromTransportLayer(objectKeys, buffers, false, 1000, false);
    EXPECT_EQ(rc.GetCode(), K_RPC_UNAVAILABLE) << rc.ToString();
    EXPECT_FALSE(IsTransportSnapshotStaleLocation(rc)) << rc.ToString();
    EXPECT_NE(rc.GetMsg().find(STALE_TRANSPORT_SNAPSHOT_MESSAGE), std::string::npos) << rc.ToString();
    EXPECT_EQ(metadata->keyGroups.size(), 6u);
    EXPECT_EQ(replicas->batchKeys.size(), 6u);
    EXPECT_EQ(buffers[0], nullptr);
    EXPECT_EQ(buffers[1], nullptr);
}

TEST(ObjectClientTransportTest, BatchStaleLocationSlowReadDeadlineReturnsPublicDeadline)
{
    ApiDeadlineGuard deadline(10);
    const auto ownerAddress = MakeAddress(41);
    auto metadata = std::make_shared<FakeObjectMetadataClient>();
    auto replicas = std::make_shared<FakeReplicaReader>();
    replicas->statusHandler = [&ownerAddress](const std::string &) {
        std::this_thread::sleep_for(std::chrono::milliseconds(25));
        return MakeStaleSnapshotStatus(ownerAddress);
    };
    auto transportLayer = std::make_unique<TestTransportLayer>(std::make_shared<FakeDataPlaneManager>());
    transportLayer->SetObjectRead(
        std::make_unique<ObjectReadFlow>(metadata, replicas, std::make_shared<ThreadPool>(0, 2, "object_read_test")));

    ConnectOptions options;
    options.host = "127.0.0.1";
    options.port = 31501;
    auto client = std::make_shared<object_cache::ObjectClientImpl>(options);
    auto workerApi = std::make_shared<object_cache::ClientWorkerRemoteApi>(MakeAddress(31501));
    workerApi->clientId_ = "batch-stale-slow-read-deadline-test-client";
    client->workerApi_.emplace_back(workerApi);
    client->transportLayer_ = std::move(transportLayer);
    std::atomic_store(&client->routing_, MakeSingleWorkerRouting(ownerAddress));

    const std::vector<std::string> objectKeys{ "stale-a", "stale-b" };
    std::vector<std::shared_ptr<Buffer>> buffers(objectKeys.size());
    Status rc = client->GetFromTransportLayer(objectKeys, buffers, false, 1000, false);
    EXPECT_EQ(rc.GetCode(), K_RPC_DEADLINE_EXCEEDED) << rc.ToString();
    EXPECT_FALSE(IsTransportSnapshotStaleLocation(rc)) << rc.ToString();
    EXPECT_NE(rc.GetMsg().find(STALE_TRANSPORT_SNAPSHOT_MESSAGE), std::string::npos) << rc.ToString();
    EXPECT_EQ(metadata->keyGroups.size(), 1u);
    EXPECT_EQ(replicas->batchKeys.size(), 1u);
    EXPECT_EQ(buffers[0], nullptr);
    EXPECT_EQ(buffers[1], nullptr);
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
    auto workerApi = std::make_shared<object_cache::ClientWorkerRemoteApi>(MakeAddress(31501));
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

TEST(ObjectClientTransportTest, RoutedShmBufferUsesTargetSessionLockId)
{
    constexpr uint32_t TARGET_SESSION_LOCK_ID = 17;
    constexpr uint64_t METADATA_SIZE = 64;
    constexpr uint64_t DATA_SIZE = 4;
    auto owner = std::make_shared<FakeBufferOwner>(METADATA_SIZE + DATA_SIZE, true);
    std::copy_n("data", DATA_SIZE, owner->data.data() + METADATA_SIZE);
    ConnectOptions options;
    options.host = "127.0.0.1";
    options.port = 31501;
    auto client = std::make_shared<object_cache::ObjectClientImpl>(options);
    auto workerApi = std::make_shared<object_cache::ClientWorkerRemoteApi>(MakeAddress(31501));
    workerApi->clientId_ = "materialization-test-client";
    client->workerApi_.emplace_back(workerApi);
    ObjectReadItemResult item;
    item.objectKey = "routed-shm";
    item.data.response.set_data_size(DATA_SIZE);
    item.data.externalData = owner->data.data();
    item.data.externalSize = DATA_SIZE;
    item.data.externalOwner = owner;
    ExternalBufferMeta meta;
    meta.metadataSize = METADATA_SIZE;
    meta.shmId = ShmKey::Intern("target-worker-shm");
    meta.lockId = TARGET_SESSION_LOCK_ID;
    meta.workerAddr = MakeAddress(31502);
    item.data.externalMeta = meta;
    std::shared_ptr<Buffer> buffer;

    ASSERT_TRUE(client->MaterializeTransportItem(item.objectKey, item, buffer).IsOk());
    ASSERT_NE(buffer, nullptr);
    ASSERT_NE(buffer->bufferInfo_, nullptr);
    EXPECT_TRUE(buffer->bufferInfo_->useSessionLockId);
    EXPECT_EQ(buffer->bufferInfo_->sessionLockId, TARGET_SESSION_LOCK_ID);
    EXPECT_EQ(std::string(static_cast<const char *>(buffer->ImmutableData()), DATA_SIZE), "data");
}

TEST(ObjectClientTransportTest, RoutedPublishReplaysScaleDownOnRemainingWorker)
{
    const HostPort leavingWorker = MakeAddress(31511);
    const HostPort remainingWorker = MakeAddress(31512);
    auto manager = std::make_shared<FakeDataPlaneManager>();
    manager->configureTransporter = [leavingWorker](const HostPort &address, FakeTransporter &transporter) {
        if (address == leavingWorker) {
            transporter.setStatuses.emplace_back(K_SCALE_DOWN, "Worker is exiting now");
        }
    };
    ConnectOptions options;
    options.host = leavingWorker.Host();
    options.port = leavingWorker.Port();
    object_cache::ObjectClientImpl client(options);
    auto workerApi = std::make_shared<object_cache::ClientWorkerRemoteApi>(leavingWorker);
    workerApi->clientId_ = "routed-publish-scale-down-test";
    client.workerApi_.emplace_back(workerApi);
    client.enableLocalCache_ = false;
    client.transportLayer_ = std::make_unique<TestTransportLayer>(manager);
    std::atomic_store(&client.routing_, MakeSingleWorkerRouting(remainingWorker));
    auto info = std::make_shared<ObjectBufferInfo>();
    info->objectKey = "routed-publish-scale-down";
    info->workerAddr = leavingWorker;
    info->dataSize = 4;
    info->pointer = static_cast<uint8_t *>(malloc(5));
    ASSERT_NE(info->pointer, nullptr);
    std::memcpy(info->pointer, "data", info->dataSize);
    info->isRoutedWrite = true;
    ScopedRequestContext requestContext;
    ApiDeadlineGuard deadline(1'000);

    ASSERT_TRUE(client.PublishRoutedBuffer(info, { "nested" }, true).IsOk());

    ASSERT_EQ(manager->builtTransporters.size(), 2U);
    const auto &leaving = manager->builtTransporters[0];
    const auto &remaining = manager->builtTransporters[1];
    EXPECT_EQ(leaving->rpcClient->WorkerAddress(), leavingWorker);
    EXPECT_EQ(remaining->rpcClient->WorkerAddress(), remainingWorker);
    EXPECT_EQ(leaving->setCount, 1);
    EXPECT_EQ(remaining->createCount, 1);
    EXPECT_EQ(remaining->setCount, 1);
    EXPECT_EQ(remaining->setPayloads, std::vector<std::string>({ "data" }));
    EXPECT_TRUE(remaining->setParams.front().isSeal);
    EXPECT_EQ(remaining->setParams.front().nestedKeys, std::unordered_set<std::string>({ "nested" }));
    EXPECT_TRUE(info->isSeal);
    free(info->pointer);
    info->pointer = nullptr;
}

TEST(ObjectClientTransportTest, RoutedReplayKeepsBufferUsableAfterSourceWorkerRemoval)
{
    const HostPort leavingWorker = MakeAddress(31513);
    const HostPort remainingWorker = MakeAddress(31514);
    auto manager = std::make_shared<FakeDataPlaneManager>();
    manager->configureTransporter = [leavingWorker](const HostPort &address, FakeTransporter &transporter) {
        if (address == leavingWorker) {
            transporter.setStatuses.emplace_back(K_SCALE_DOWN, "Worker is exiting now");
        }
    };
    ConnectOptions options;
    options.host = leavingWorker.Host();
    options.port = leavingWorker.Port();
    auto client = std::make_shared<object_cache::ObjectClientImpl>(options);
    auto workerApi = std::make_shared<object_cache::ClientWorkerRemoteApi>(leavingWorker);
    workerApi->clientId_ = "routed-publish-rebind-test";
    workerApi->SetHealthy(true);
    client->workerApi_.emplace_back(workerApi);
    client->listenWorker_.resize(object_cache::ObjectClientImpl::STANDBY2_WORKER + 1);
    client->listenWorker_[object_cache::ObjectClientImpl::LOCAL_WORKER] = std::make_shared<client::ListenWorker>(
        workerApi, HeartbeatType::NO_HEARTBEAT, object_cache::ObjectClientImpl::LOCAL_WORKER, nullptr);
    client->enableLocalCache_ = false;
    client->transportLayer_ = std::make_unique<TestTransportLayer>(manager);
    std::atomic_store(&client->routing_, MakeSingleWorkerRouting(remainingWorker));
    bool initNeedsCompletion = false;
    ASSERT_TRUE(client->clientStateManager_->ProcessInit(initNeedsCompletion).IsOk());
    client->clientStateManager_->CompleteHandler(false, initNeedsCompletion);
    auto info = std::make_shared<ObjectBufferInfo>();
    info->objectKey = "routed-publish-rebind";
    info->workerAddr = leavingWorker;
    info->shmId = ShmKey::Intern("leaving-worker-shm");
    info->dataSize = 4;
    info->pointer = static_cast<uint8_t *>(malloc(info->dataSize + 1));
    ASSERT_NE(info->pointer, nullptr);
    std::memcpy(info->pointer, "data", info->dataSize);
    info->isRoutedWrite = true;
    auto owner = std::make_shared<FakeBufferOwner>(info->dataSize, true);
    info->receiveBufferOwner = owner;
    std::shared_ptr<Buffer> buffer;
    ASSERT_TRUE(Buffer::CreateBuffer(info, client, buffer).IsOk());

    ASSERT_TRUE(buffer->Publish().IsOk());
    owner->alive = false;
    ASSERT_TRUE(buffer->MemoryCopy("next", 4).IsOk());
    ASSERT_TRUE(buffer->Publish().IsOk());
    ASSERT_TRUE(buffer->Seal().IsOk());

    ASSERT_EQ(manager->builtTransporters.size(), 2U);
    EXPECT_EQ(manager->builtTransporters[0]->setCount, 1);
    EXPECT_EQ(manager->builtTransporters[1]->createCount, 3);
    EXPECT_EQ(manager->builtTransporters[1]->setPayloads, std::vector<std::string>({ "data", "next", "next" }));
    buffer.reset();
    free(info->pointer);
    info->pointer = nullptr;
}

TEST(ObjectClientTransportTest, RoutedMultiPublishReplaysScaleDownOnRemainingWorker)
{
    const HostPort leavingWorker = MakeAddress(31521);
    const HostPort remainingWorker = MakeAddress(31522);
    auto manager = std::make_shared<FakeDataPlaneManager>();
    manager->configureTransporter = [leavingWorker](const HostPort &address, FakeTransporter &transporter) {
        if (address == leavingWorker) {
            transporter.mSetStatuses.emplace_back(K_SCALE_DOWN, "Worker is exiting now");
        }
    };
    ConnectOptions options;
    options.host = leavingWorker.Host();
    options.port = leavingWorker.Port();
    object_cache::ObjectClientImpl client(options);
    auto workerApi = std::make_shared<object_cache::ClientWorkerRemoteApi>(leavingWorker);
    workerApi->clientId_ = "routed-multi-publish-scale-down-test";
    client.workerApi_.emplace_back(workerApi);
    client.enableLocalCache_ = false;
    client.transportLayer_ = std::make_unique<TestTransportLayer>(manager);
    std::atomic_store(&client.routing_, MakeSingleWorkerRouting(remainingWorker));
    std::vector<std::shared_ptr<ObjectBufferInfo>> infos;
    for (const auto &value : { std::string("first"), std::string("second") }) {
        auto info = std::make_shared<ObjectBufferInfo>();
        info->objectKey = value;
        info->workerAddr = leavingWorker;
        info->dataSize = value.size();
        info->pointer = static_cast<uint8_t *>(malloc(value.size() + 1));
        ASSERT_NE(info->pointer, nullptr);
        std::memcpy(info->pointer, value.data(), value.size());
        info->isRoutedWrite = true;
        infos.emplace_back(std::move(info));
    }
    ScopedRequestContext requestContext;
    ApiDeadlineGuard deadline(1'000);
    size_t failedCount = 0;

    ASSERT_TRUE(client.ProcessRoutedMSetGroup(leavingWorker, infos, failedCount).IsOk());
    ASSERT_TRUE(client.ProcessRoutedMSetGroup(leavingWorker, infos, failedCount).IsOk());
    ASSERT_TRUE(client.PublishRoutedBuffer(infos.front(), {}, true).IsOk());

    ASSERT_EQ(manager->builtTransporters.size(), 2U);
    const auto &leaving = manager->builtTransporters[0];
    const auto &remaining = manager->builtTransporters[1];
    EXPECT_EQ(leaving->mSetCount, 1);
    EXPECT_EQ(remaining->rpcClient->WorkerAddress(), remainingWorker);
    EXPECT_EQ(remaining->createCount, 5);
    EXPECT_EQ(remaining->setCount, 5);
    EXPECT_EQ(remaining->setPayloads, std::vector<std::string>({ "first", "second", "first", "second", "first" }));
    EXPECT_TRUE(remaining->setParams.back().isSeal);
    EXPECT_EQ(failedCount, 0U);
    for (auto &info : infos) {
        free(info->pointer);
        info->pointer = nullptr;
    }
}

TEST(ObjectClientTransportTest, ConnectOptionsPolicyControlsWritePlacement)
{
    ConnectOptions options;
    options.host = "127.0.0.1";
    options.port = 31501;
    options.dataPlacementPolicy = datasystem::DataPlacementPolicy::PREFERRED_META_OWNER;

    object_cache::ObjectClientImpl client(options);

    ASSERT_TRUE(client.InitDataPlacementPolicy().IsOk());
    EXPECT_EQ(client.dataPlacementPolicy_, DataPlacementPolicy::PREFERRED_META_OWNER);
}

TEST(ObjectClientTransportTest, LocalCacheSetRouteSkipsQuarantinedBoundWorker)
{
    const auto boundWorker = MakeAddress(31501);
    const auto healthyWorker = MakeAddress(31502);
    ConnectOptions options;
    options.host = boundWorker.Host();
    options.port = boundWorker.Port();
    object_cache::ObjectClientImpl client(options);
    auto workerApi = std::make_shared<object_cache::ClientWorkerRemoteApi>(boundWorker);
    workerApi->clientId_ = "local-cache-write-target-test";
    client.workerApi_.emplace_back(workerApi);
    client.listenWorker_.resize(object_cache::ObjectClientImpl::STANDBY2_WORKER + 1);
    client.listenWorker_[object_cache::ObjectClientImpl::LOCAL_WORKER] = std::make_shared<client::ListenWorker>(
        workerApi, HeartbeatType::NO_HEARTBEAT, object_cache::ObjectClientImpl::LOCAL_WORKER, nullptr);
    client.enableLocalCache_ = true;
    client.dataPlacementPolicy_ = DataPlacementPolicy::PREFERRED_SAME_NODE;
    client.ubHealthFilter_ = std::make_shared<client::UbHealthFilter>();
    ASSERT_TRUE(client.ubHealthFilter_->ReportWriteTargetFailure(
        boundWorker, Status(K_URMA_ERROR, "remote ack timeout"), std::nullopt,
        URMA_REMOTE_ACK_TIMEOUT_STATUS));
    auto routing = MakeSingleWorkerRouting(healthyWorker);
    std::atomic_store(&client.routing_, routing);
    workerApi->SetHealthy(true);

    object_cache::ObjectClientImpl::SetRouteContext route;
    ASSERT_TRUE(client.SelectSetRoute("rerouted", {}, route).IsOk());

    EXPECT_EQ(route.worker, healthyWorker);
    EXPECT_EQ(route.directWorkerApi, nullptr);
}

TEST(ObjectClientTransportTest, LocalCacheSetRouteKeepsHealthyShmPathForUbQuarantinedBoundWorker)
{
    const auto boundWorker = MakeAddress(31503);
    const auto healthyWorker = MakeAddress(31504);
    ConnectOptions options;
    options.host = boundWorker.Host();
    options.port = boundWorker.Port();
    object_cache::ObjectClientImpl client(options);
    auto workerApi = std::make_shared<object_cache::ClientWorkerRemoteApi>(boundWorker);
    workerApi->clientId_ = "local-cache-shm-write-target-test";
    workerApi->shmEnableType_ = ShmEnableType::UDS;
    client.workerApi_.emplace_back(workerApi);
    client.listenWorker_.resize(object_cache::ObjectClientImpl::STANDBY2_WORKER + 1);
    client.listenWorker_[object_cache::ObjectClientImpl::LOCAL_WORKER] = std::make_shared<client::ListenWorker>(
        workerApi, HeartbeatType::NO_HEARTBEAT, object_cache::ObjectClientImpl::LOCAL_WORKER, nullptr);
    client.enableLocalCache_ = true;
    client.dataPlacementPolicy_ = DataPlacementPolicy::PREFERRED_SAME_NODE;
    client.ubHealthFilter_ = std::make_shared<client::UbHealthFilter>();
    ASSERT_TRUE(client.ubHealthFilter_->ReportWriteTargetFailure(
        boundWorker, Status(K_URMA_ERROR, "remote ack timeout"), std::nullopt,
        URMA_REMOTE_ACK_TIMEOUT_STATUS));
    auto routing = MakeSingleWorkerRouting(healthyWorker);
    std::atomic_store(&client.routing_, routing);
    workerApi->SetHealthy(true);

    object_cache::ObjectClientImpl::SetRouteContext route;
    ASSERT_TRUE(client.SelectSetRoute("local-shm", {}, route).IsOk());

    EXPECT_EQ(route.worker, boundWorker);
    EXPECT_EQ(route.directWorkerApi, workerApi);

    object_cache::ObjectClientImpl::SetRouteContext retryRoute;
    ASSERT_TRUE(client.SelectSetRoute("retry", { boundWorker }, retryRoute).IsOk());

    EXPECT_EQ(retryRoute.worker, healthyWorker);
    EXPECT_EQ(retryRoute.directWorkerApi, nullptr);
}

TEST(ObjectClientTransportTest, ConnectOptionsPolicyDefaultsToPreferredSameNode)
{
    ConnectOptions options;
    options.host = "127.0.0.1";
    options.port = 31501;

    object_cache::ObjectClientImpl client(options);

    ASSERT_TRUE(client.InitDataPlacementPolicy().IsOk());
    EXPECT_EQ(client.dataPlacementPolicy_, DataPlacementPolicy::PREFERRED_SAME_NODE);
}

TEST(ObjectClientTransportTest, RejectsInvalidWritePlacementPolicyFromConnectOptions)
{
    ConnectOptions options;
    options.host = "127.0.0.1";
    options.port = 31501;
    options.dataPlacementPolicy = static_cast<datasystem::DataPlacementPolicy>(255);

    object_cache::ObjectClientImpl client(options);

    EXPECT_EQ(client.InitDataPlacementPolicy().GetCode(), K_INVALID);
}

TEST(ObjectClientTransportTest, ShutdownWaitsForAsyncWorkerSwitchTasks)
{
    ConnectOptions options;
    options.host = "127.0.0.1";
    options.port = 31501;
    object_cache::ObjectClientImpl client(options);
    client.asyncSwitchWorkerPool_ = std::make_shared<ThreadPool>(0, 1, "switch_shutdown_test");
    client.asyncSwitchWorkerPoolHandle_ = client.asyncSwitchWorkerPool_.get();
    client.enableLocalCache_ = true;
    client.enableCrossNodeConnection_ = true;
    auto workerApi = std::make_shared<object_cache::ClientWorkerRemoteApi>(MakeAddress(31501));
    workerApi->clientId_ = "switch-shutdown-test-client";
    client.workerApi_.emplace_back(workerApi);

    std::promise<void> taskStarted;
    std::promise<void> releaseTask;
    auto releaseFuture = releaseTask.get_future().share();
    client.asyncSwitchWorkerPool_->Execute([&taskStarted, releaseFuture]() {
        taskStarted.set_value();
        releaseFuture.wait();
    });
    taskStarted.get_future().wait();
    EXPECT_TRUE(client.SubmitUnavailableWorkerSwitch(workerApi));
    EXPECT_TRUE(client.SubmitUrmaDataPlaneSwitch(object_cache::ObjectClientImpl::LOCAL_WORKER, workerApi));
    {
        std::lock_guard<std::mutex> lock(client.asyncSwitchWorkerMutex_);
        EXPECT_EQ(client.unavailableWorkerSwitchPending_.size(), 1U);
    }

    bool initNeedsCompletion = false;
    ASSERT_TRUE(client.clientStateManager_->ProcessInit(initNeedsCompletion).IsOk());
    client.clientStateManager_->CompleteHandler(false, initNeedsCompletion);
    auto shutdownFuture = std::async(std::launch::async, [&client]() {
        bool shutdownNeedsCompletion = false;
        auto rc = client.ShutDown(shutdownNeedsCompletion, true);
        client.clientStateManager_->CompleteHandler(rc.IsError(), shutdownNeedsCompletion);
        return rc;
    });

    EXPECT_EQ(shutdownFuture.wait_for(std::chrono::milliseconds(100)), std::future_status::timeout);
    releaseTask.set_value();
    EXPECT_TRUE(shutdownFuture.get().IsOk());
    EXPECT_EQ(client.asyncSwitchWorkerPool_, nullptr);
    EXPECT_EQ(client.asyncSwitchWorkerPoolHandle_, nullptr);
    EXPECT_TRUE(client.unavailableWorkerSwitchPending_.empty());
}

TEST(ObjectClientTransportTest, DirectGetRecoveryFailureForcesRingRefreshWithoutEagerSwitch)
{
    ConnectOptions options;
    options.host = "127.0.0.1";
    options.port = 31501;
    object_cache::ObjectClientImpl client(options);
    client.enableCrossNodeConnection_ = true;
    client.asyncSwitchWorkerPool_ = std::make_shared<ThreadPool>(0, 1, "direct_get_refresh_test");
    client.asyncSwitchWorkerPoolHandle_ = client.asyncSwitchWorkerPool_.get();
    auto workerApi = std::make_shared<object_cache::ClientWorkerRemoteApi>(MakeAddress(31501));
    client.workerApi_.emplace_back(workerApi);
    auto routing = MakeSingleWorkerRouting(MakeAddress(31501));
    std::atomic_store(&client.routing_, routing);

    client.HandleDirectGetFailure(workerApi, Status(K_RPC_DEADLINE_EXCEEDED, "request timed out"));

    EXPECT_GT(routing->refresher_->forceRefreshDeadlineMs_.load(), 0);
    std::lock_guard<std::mutex> lock(client.asyncSwitchWorkerMutex_);
    EXPECT_TRUE(client.unavailableWorkerSwitchPending_.empty());
}

TEST(ObjectClientTransportTest, DirectGetNonRecoveryFailureDoesNotForceRingRefresh)
{
    ConnectOptions options;
    options.host = "127.0.0.1";
    options.port = 31501;
    object_cache::ObjectClientImpl client(options);
    auto workerApi = std::make_shared<object_cache::ClientWorkerRemoteApi>(MakeAddress(31501));
    client.workerApi_.emplace_back(workerApi);
    auto routing = MakeSingleWorkerRouting(MakeAddress(31501));
    std::atomic_store(&client.routing_, routing);

    client.HandleDirectGetFailure(workerApi, Status(K_NOT_FOUND, "missing object"));

    EXPECT_EQ(routing->refresher_->forceRefreshDeadlineMs_.load(), 0);
}

TEST(ObjectClientTransportTest, SetCreateMetadataOwnerUnavailableForcesRingRefreshWithoutReplay)
{
    ConnectOptions options;
    options.host = "127.0.0.1";
    options.port = 31501;
    object_cache::ObjectClientImpl client(options);
    auto routing = MakeSingleWorkerRouting(MakeAddress(31501));
    std::atomic_store(&client.routing_, routing);
    std::vector<HostPort> excludedWorkers;

    const bool retry = client.HandleSetRouteFailure(
        Status(K_METADATA_OWNER_UNAVAILABLE, "metadata owner is unavailable"),
        object_cache::ObjectClientImpl::SetFailureStage::CREATE, MakeAddress(31501), excludedWorkers);

    EXPECT_FALSE(retry);
    EXPECT_TRUE(excludedWorkers.empty());
    EXPECT_GT(routing->refresher_->forceRefreshDeadlineMs_.load(), 0);
}

TEST(ObjectClientTransportTest, SetCreatePeerDeadForcesRingRefreshAndKeepsSafeRetry)
{
    ConnectOptions options;
    options.host = "127.0.0.1";
    options.port = 31501;
    object_cache::ObjectClientImpl client(options);
    auto routing = MakeSingleWorkerRouting(MakeAddress(31501));
    std::atomic_store(&client.routing_, routing);
    std::vector<HostPort> excludedWorkers;

    const bool retry = client.HandleSetRouteFailure(
        Status(K_RPC_PEER_DEAD, "ingress worker is unavailable"),
        object_cache::ObjectClientImpl::SetFailureStage::CREATE, MakeAddress(31501), excludedWorkers);

    EXPECT_TRUE(retry);
    EXPECT_EQ(excludedWorkers, std::vector<HostPort>({ MakeAddress(31501) }));
    EXPECT_GT(routing->refresher_->forceRefreshDeadlineMs_.load(), 0);
}

TEST(ObjectClientTransportTest, RoutedClientPeerDeadRefreshesRingWithoutBoundWorkerSwitch)
{
    ConnectOptions options;
    options.host = "127.0.0.1";
    options.port = 31501;
    object_cache::ObjectClientImpl client(options);
    client.enableLocalCache_ = false;
    client.enableCrossNodeConnection_ = true;
    client.asyncSwitchWorkerPool_ = std::make_shared<ThreadPool>(0, 1, "routed_peer_dead_no_switch_test");
    client.asyncSwitchWorkerPoolHandle_ = client.asyncSwitchWorkerPool_.get();
    auto workerApi = std::make_shared<object_cache::ClientWorkerRemoteApi>(MakeAddress(31501));
    client.workerApi_.emplace_back(workerApi);
    auto routing = MakeSingleWorkerRouting(MakeAddress(31501));
    std::atomic_store(&client.routing_, routing);

    client.HandleDirectGetFailure(workerApi, Status(K_RPC_PEER_DEAD, "routed worker down"));

    EXPECT_GT(routing->refresher_->forceRefreshDeadlineMs_.load(), 0);
    EXPECT_FALSE(client.SubmitUnavailableWorkerSwitch(workerApi));
    std::lock_guard<std::mutex> lock(client.asyncSwitchWorkerMutex_);
    EXPECT_TRUE(client.unavailableWorkerSwitchPending_.empty());
}

TEST(ObjectClientTransportTest, AuthoritativeRingRemovalQueuesBoundWorkerSwitchOnlyAfterRoutingInit)
{
    ConnectOptions options;
    options.host = "127.0.0.1";
    options.port = 31501;
    object_cache::ObjectClientImpl client(options);
    client.enableLocalCache_ = true;
    client.enableCrossNodeConnection_ = true;
    client.asyncSwitchWorkerPool_ = std::make_shared<ThreadPool>(0, 1, "ring_removal_switch_test");
    client.asyncSwitchWorkerPoolHandle_ = client.asyncSwitchWorkerPool_.get();
    auto workerApi = std::make_shared<object_cache::ClientWorkerRemoteApi>(MakeAddress(31501));
    client.workerApi_.emplace_back(workerApi);

    std::promise<void> taskStarted;
    std::promise<void> releaseTask;
    auto releaseFuture = releaseTask.get_future().share();
    client.asyncSwitchWorkerPool_->Execute([&taskStarted, releaseFuture]() {
        taskStarted.set_value();
        releaseFuture.wait();
    });
    taskStarted.get_future().wait();

    ::datasystem::ClusterTopologyPb ringWithoutBoundWorker;
    (*ringWithoutBoundWorker.mutable_members())[MakeAddress(31502).ToString()].set_state(
        ::datasystem::MembershipPb::ACTIVE);
    client.MaybeSwitchWorkerRemovedFromRing(ringWithoutBoundWorker);
    {
        std::lock_guard<std::mutex> lock(client.asyncSwitchWorkerMutex_);
        EXPECT_TRUE(client.unavailableWorkerSwitchPending_.empty());
    }

    std::atomic_store(&client.routing_, MakeSingleWorkerRouting(MakeAddress(31502)));
    client.MaybeSwitchWorkerRemovedFromRing(ringWithoutBoundWorker);
    client.MaybeSwitchWorkerRemovedFromRing(ringWithoutBoundWorker);
    {
        std::lock_guard<std::mutex> lock(client.asyncSwitchWorkerMutex_);
        EXPECT_EQ(client.unavailableWorkerSwitchPending_.size(), 1U);
    }

    {
        std::lock_guard<std::mutex> lock(client.switchNodeMutex_);
        client.workerApi_[client.currentNode_] =
            std::make_shared<object_cache::ClientWorkerRemoteApi>(MakeAddress(31502));
    }
    releaseTask.set_value();
    client.DrainAsyncSwitchWorkerPool();
    EXPECT_TRUE(client.unavailableWorkerSwitchPending_.empty());
}

TEST(ObjectClientTransportTest, AuthoritativeRingRemovalDoesNotSwitchRoutedClient)
{
    ConnectOptions options;
    options.host = "127.0.0.1";
    options.port = 31501;
    object_cache::ObjectClientImpl client(options);
    client.enableLocalCache_ = false;
    client.enableCrossNodeConnection_ = true;
    client.asyncSwitchWorkerPool_ = std::make_shared<ThreadPool>(0, 1, "routed_ring_no_switch_test");
    client.asyncSwitchWorkerPoolHandle_ = client.asyncSwitchWorkerPool_.get();
    auto workerApi = std::make_shared<object_cache::ClientWorkerRemoteApi>(MakeAddress(31501));
    client.workerApi_.emplace_back(workerApi);
    std::atomic_store(&client.routing_, MakeSingleWorkerRouting(MakeAddress(31502)));
    ::datasystem::ClusterTopologyPb ring;
    (*ring.mutable_members())[MakeAddress(31501).ToString()].set_state(::datasystem::MembershipPb::FAILED);

    client.MaybeSwitchWorkerRemovedFromRing(ring);
    EXPECT_FALSE(client.SubmitUnavailableWorkerSwitch(workerApi));

    std::lock_guard<std::mutex> lock(client.asyncSwitchWorkerMutex_);
    EXPECT_TRUE(client.unavailableWorkerSwitchPending_.empty());
}

TEST(ObjectClientTransportTest, RoutedExistIgnoresBoundWorkerFailFastState)
{
    const auto owner = MakeAddress(31502);
    ConnectOptions options;
    options.host = "127.0.0.1";
    options.port = 31501;
    object_cache::ObjectClientImpl client(options);
    auto workerApi = std::make_shared<object_cache::ClientWorkerRemoteApi>(MakeAddress(31501));
    workerApi->clientId_ = "routed-exist-client";
    client.workerApi_.emplace_back(workerApi);
    client.enableLocalCache_ = false;
    client.enableCrossNodeConnection_ = true;
    client.workerSwitchState_ = object_cache::ObjectClientImpl::WorkerSwitchState::NO_SWITCHABLE_WORKER;
    auto manager = std::make_shared<FakeDataPlaneManager>();
    client.transportLayer_ = std::make_unique<TestTransportLayer>(manager);
    std::atomic_store(&client.routing_, MakeSingleWorkerRouting(owner));
    bool initNeedsCompletion = false;
    ASSERT_TRUE(client.clientStateManager_->ProcessInit(initNeedsCompletion).IsOk());
    client.clientStateManager_->CompleteHandler(false, initNeedsCompletion);
    std::vector<bool> exists;

    ASSERT_TRUE(client.Exist({ "key" }, exists, false, false).IsOk());

    ASSERT_EQ(exists, std::vector<bool>({ true }));
    ASSERT_NE(manager->lastRpcClient, nullptr);
    EXPECT_EQ(manager->lastRpcClient->WorkerAddress(), owner);
    EXPECT_EQ(manager->lastRpcClient->existInvokeCount, 1);
    EXPECT_TRUE(client.unavailableWorkerSwitchPending_.empty());
}

TEST(ObjectClientTransportTest, ActiveBoundWorkerDoesNotQueueAuthoritativeRingSwitch)
{
    ConnectOptions options;
    options.host = "127.0.0.1";
    options.port = 31501;
    object_cache::ObjectClientImpl client(options);
    client.enableLocalCache_ = true;
    client.enableCrossNodeConnection_ = true;
    client.asyncSwitchWorkerPool_ = std::make_shared<ThreadPool>(0, 1, "active_ring_switch_test");
    client.asyncSwitchWorkerPoolHandle_ = client.asyncSwitchWorkerPool_.get();
    auto workerApi = std::make_shared<object_cache::ClientWorkerRemoteApi>(MakeAddress(31501));
    client.workerApi_.emplace_back(workerApi);
    std::atomic_store(&client.routing_, MakeSingleWorkerRouting(MakeAddress(31501)));
    ::datasystem::ClusterTopologyPb ring;
    (*ring.mutable_members())[MakeAddress(31501).ToString()].set_state(::datasystem::MembershipPb::ACTIVE);

    client.MaybeSwitchWorkerRemovedFromRing(ring);

    std::lock_guard<std::mutex> lock(client.asyncSwitchWorkerMutex_);
    EXPECT_TRUE(client.unavailableWorkerSwitchPending_.empty());
}

TEST(ObjectClientTransportTest, NonActiveBoundWorkerQueuesAuthoritativeRingSwitch)
{
    ConnectOptions options;
    options.host = "127.0.0.1";
    options.port = 31501;
    object_cache::ObjectClientImpl client(options);
    client.enableLocalCache_ = true;
    client.enableCrossNodeConnection_ = true;
    client.asyncSwitchWorkerPool_ = std::make_shared<ThreadPool>(0, 1, "non_active_ring_switch_test");
    client.asyncSwitchWorkerPoolHandle_ = client.asyncSwitchWorkerPool_.get();
    auto workerApi = std::make_shared<object_cache::ClientWorkerRemoteApi>(MakeAddress(31501));
    client.workerApi_.emplace_back(workerApi);
    std::atomic_store(&client.routing_, MakeSingleWorkerRouting(MakeAddress(31502)));

    std::promise<void> taskStarted;
    std::promise<void> releaseTask;
    auto releaseFuture = releaseTask.get_future().share();
    client.asyncSwitchWorkerPool_->Execute([&taskStarted, releaseFuture]() {
        taskStarted.set_value();
        releaseFuture.wait();
    });
    taskStarted.get_future().wait();
    ::datasystem::ClusterTopologyPb ring;
    (*ring.mutable_members())[MakeAddress(31501).ToString()].set_state(::datasystem::MembershipPb::FAILED);

    client.MaybeSwitchWorkerRemovedFromRing(ring);
    {
        std::lock_guard<std::mutex> lock(client.asyncSwitchWorkerMutex_);
        EXPECT_EQ(client.unavailableWorkerSwitchPending_.size(), 1U);
    }

    {
        std::lock_guard<std::mutex> lock(client.switchNodeMutex_);
        client.workerApi_[client.currentNode_] =
            std::make_shared<object_cache::ClientWorkerRemoteApi>(MakeAddress(31502));
    }
    releaseTask.set_value();
    client.DrainAsyncSwitchWorkerPool();
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
    request.context = MakeReadContext();
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
    request.context = MakeReadContext();
    request.items = { { 1, "first", MakeAddress(41) }, { 0, "second", MakeAddress(41) } };
    ObjectReadResult result;

    EXPECT_EQ(flow.Run(request, result).GetCode(), K_NOT_FOUND);
}

TEST(ObjectClientTransportTest, TransportMSetParallelMemoryCopyPreservesPayload)
{
    constexpr size_t valueSize = 512 * 1024;
    ConnectOptions options;
    options.host = "127.0.0.1";
    options.port = 31501;
    object_cache::ObjectClientImpl client(options);
    client.memoryCopyThreadPool_ = std::make_shared<ThreadPool>(0, 4, "mset_memcopy_test");
    client.memcpyParallelThreshold_ = 0;
    client.parallismNum_ = 0;

    const std::vector<std::string> values{ std::string(valueSize, 'a'), std::string(valueSize, 'b') };
    object_cache::ObjectClientImpl::MSetRouteGroup group;
    group.worker = MakeAddress(31501);
    group.keys = { "parallel-copy-key-0", "parallel-copy-key-1" };
    group.values = { values[0], values[1] };

    std::vector<std::shared_ptr<ObjectBuffer>> buffers;
    for (size_t i = 0; i < values.size(); ++i) {
        auto info = std::make_shared<ObjectBufferInfo>();
        info->objectKey = group.keys[i];
        info->dataSize = valueSize;
        info->workerAddr = group.worker;
        info->pointer = static_cast<uint8_t *>(calloc(valueSize + 1, 1));
        ASSERT_NE(info->pointer, nullptr);
        std::shared_ptr<ObjectBuffer> buffer;
        ASSERT_TRUE(ObjectBufferInternal::Create(info, buffer).IsOk());
        buffers.emplace_back(std::move(buffer));
    }

    ApiDeadlineGuard deadline(5'000);
    ASSERT_TRUE(client.MemoryCopyTransportMSetBuffers(group, buffers, valueSize * values.size()).IsOk());
    for (size_t i = 0; i < values.size(); ++i) {
        EXPECT_EQ(memcmp(buffers[i]->ImmutableData(), values[i].data(), valueSize), 0);
    }
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

TEST(UbTransporterTest, GetReturnsAllocationErrorWithoutRpcWhenUbBufferUnavailable)
{
    auto rpcClient = std::make_shared<FakeWorkerRpcClient>();
    rpcClient->getObjectDataSize = 8;
    auto bufferProvider = std::make_shared<FakeUbBufferProvider>();
    bufferProvider->allocateStatus = Status(K_OUT_OF_MEMORY, "allocate failed");
    UbTransporter transporter(rpcClient, std::make_shared<FakeUbConnection>(), bufferProvider);
    DataGetResult result;

    EXPECT_EQ(transporter.Get({ "key", 8 }, result).GetCode(), K_URMA_ERROR);
    EXPECT_EQ(rpcClient->getObjectCount, 0);
    EXPECT_TRUE(rpcClient->getObjectRequests.empty());
}

TEST(UbTransporterTest, GetRejectsTcpPayloadResponseForUbRequest)
{
    auto rpcClient = std::make_shared<FakeWorkerRpcClient>();
    rpcClient->getObjectDataSize = 8;
    rpcClient->getObjectResponseInPayload = true;
    auto bufferProvider = std::make_shared<FakeUbBufferProvider>();
    UbTransporter transporter(rpcClient, std::make_shared<FakeUbConnection>(), bufferProvider);
    DataGetResult result;

    EXPECT_EQ(transporter.Get({ "key", 8 }, result).GetCode(), K_URMA_ERROR);
    ASSERT_EQ(rpcClient->getObjectRequests.size(), 1u);
    EXPECT_TRUE(rpcClient->getObjectRequests[0].has_urma_info());
    EXPECT_EQ(result.kind, AccessTransportKind::UNKNOWN);
}

TEST(UbTransporterTest, GetRejectsUnsupportedReceiveSizeWithoutRpc)
{
    auto rpcClient = std::make_shared<FakeWorkerRpcClient>();
    auto bufferProvider = std::make_shared<FakeUbBufferProvider>();
    UbTransporter transporter(rpcClient, std::make_shared<FakeUbConnection>(), bufferProvider);
    DataGetResult result;

    EXPECT_EQ(transporter.Get({ "zero", 0 }, result).GetCode(), K_URMA_ERROR);
    EXPECT_EQ(transporter.Get({ "oversize", bufferProvider->maxGetSize + 1 }, result).GetCode(), K_URMA_ERROR);
    EXPECT_EQ(bufferProvider->allocateCount, 0);
    EXPECT_EQ(rpcClient->getObjectCount, 0);
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

TEST(UbTransporterTest, BatchGetAggregateKeepsPartialSuccessWithZeroSizeItem)
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

    ASSERT_TRUE(transporter
                    .BatchGet({ { "one", 8 }, { "two", 8 }, { "zero", 0 }, { "three", 8 }, { "four", 8 } }, results)
                    .IsOk());

    EXPECT_EQ(rpcClient->batchGetObjectCount, 2);
    EXPECT_EQ(rpcClient->getObjectCount, 0);
    ASSERT_EQ(results.size(), 5u);
    EXPECT_TRUE(results[0].status.IsOk());
    EXPECT_TRUE(results[1].status.IsOk());
    EXPECT_EQ(results[2].status.GetCode(), K_URMA_ERROR);
    EXPECT_EQ(results[2].data.kind, AccessTransportKind::UNKNOWN);
    EXPECT_TRUE(results[3].status.IsOk());
    EXPECT_TRUE(results[4].status.IsOk());
}

TEST(UbTransporterTest, BatchGetAggregateKeepsPartialSuccessWithOverMaxSizeItem)
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

    ASSERT_TRUE(transporter
                    .BatchGet({ { "one", 8 }, { "two", 8 }, { "large", 64 }, { "three", 8 }, { "four", 8 } }, results)
                    .IsOk());

    EXPECT_EQ(rpcClient->batchGetObjectCount, 2);
    EXPECT_EQ(rpcClient->getObjectCount, 0);
    ASSERT_EQ(results.size(), 5u);
    EXPECT_TRUE(results[0].status.IsOk());
    EXPECT_TRUE(results[1].status.IsOk());
    EXPECT_EQ(results[2].status.GetCode(), K_URMA_ERROR);
    EXPECT_EQ(results[2].data.kind, AccessTransportKind::UNKNOWN);
    EXPECT_TRUE(results[3].status.IsOk());
    EXPECT_TRUE(results[4].status.IsOk());
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
    DataGetBatchRequest request{ { "oversize", 48 }, { "small-a", 16 }, { "small-b", 16 } };
    ASSERT_TRUE(transporter.BatchGet(request, results).IsOk());

    EXPECT_EQ(bufferProvider->allocationAttempts, std::vector<uint64_t>({ bufferProvider->maxGetSize }));
    EXPECT_EQ(rpcClient->getObjectCount, 2);
    EXPECT_EQ(rpcClient->batchGetObjectCount, 1);
    ASSERT_EQ(results.size(), request.size());
    EXPECT_EQ(results[0].status.GetCode(), K_URMA_ERROR);
    EXPECT_EQ(results[0].data.kind, AccessTransportKind::UNKNOWN);
    EXPECT_TRUE(results[1].status.IsOk());
    EXPECT_EQ(results[1].data.kind, AccessTransportKind::UB);
    EXPECT_TRUE(results[2].status.IsOk());
    EXPECT_EQ(results[2].data.kind, AccessTransportKind::UB);
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

TEST(ShmTransporterTest, RejectsGetWhenRpcClientAbsent)
{
    ShmTransporter transporter(MakeAddress(9100), std::shared_ptr<WorkerRpcClient>{});
    DataGetResult result;
    EXPECT_EQ(transporter.Get({ "key", 1 }, result).GetCode(), K_RUNTIME_ERROR);
}

TEST(ShmTransporterTest, RejectsInvalidUnaryWorkerOcResponseIndex)
{
    GetRspPb response;
    response.add_objects()->set_object_index(1);
    std::vector<RpcMessage> payloads;

    EXPECT_EQ(ShmTransporter::ValidateShmResponse(response, payloads, 1).GetCode(), K_RUNTIME_ERROR);
}

TEST(ShmTransporterTest, RejectsDuplicateBatchWorkerOcResponseIndexes)
{
    GetRspPb response;
    response.add_objects()->set_object_index(0);
    response.add_objects()->set_object_index(0);
    std::vector<RpcMessage> payloads;

    EXPECT_EQ(ShmTransporter::ValidateShmResponse(response, payloads, 2).GetCode(), K_RUNTIME_ERROR);
}

// Create with store_fd=0 (default) → fallback path: no mmap entry, buffer malloc'd for payload inline.
TEST(ShmTransporterTest, CreateFallsBackToPayloadWhenNoStoreFd)
{
    auto rpc = std::make_shared<FakeWorkerRpcClient>();
    ShmTransporter transporter(MakeAddress(9100), rpc);
    std::shared_ptr<ObjectBuffer> buffer;
    ASSERT_TRUE(transporter.Create(MakeAddress(9100), "k1", 1024, MakeCreateParam(), buffer).IsOk());
    ASSERT_NE(buffer, nullptr);
    const auto &info = ObjectBufferInternal::GetInfo(*buffer);
    EXPECT_EQ(info.mmapEntry, nullptr);  // no mmap → fallback payload inline
    EXPECT_NE(info.pointer, nullptr);    // ObjectBuffer::Init mallocs a local buffer for the payload
    EXPECT_TRUE(info.shmId.Empty());     // store_fd<=0 = no shm region; pure inline payload, no shm_id (#7)
    EXPECT_EQ(rpc->createInvokeCount, 1);
}

// CreateDoesNotResolveTargetFdThroughBoundWorkerState was removed: it asserted the *placeholder* semantics
// (store_fd>0 yet Create does not mmap). Under Component B, store_fd>0 now mmaps the worker region via the
// endpoint-scoped ShmSession (target worker, not the bound worker). That zero-copy path needs a real fd-passing
// session and is covered end-to-end by KVClientTransportSetWithShmTest.RoutedSetUsesShmZeroCopy (ST), not the
// UT mock. The store_fd<=0 fallback (no mmap) is covered by CreateFallsBackToPayloadWhenNoStoreFd above.

// Set on a fallback (non-mmap) buffer sends payload inline (InvokeSet called with non-empty payloads).
TEST(ShmTransporterTest, SetSendsPayloadInlineForFallbackBuffer)
{
    auto rpc = std::make_shared<FakeWorkerRpcClient>();
    ShmTransporter transporter(MakeAddress(9101), rpc);
    std::shared_ptr<ObjectBuffer> buffer;
    ASSERT_TRUE(transporter.Create(MakeAddress(9101), "k2", 64, MakeCreateParam(), buffer).IsOk());
    TransportSetParam sp = MakeSetParam();
    ASSERT_TRUE(transporter.Set(*buffer, sp).IsOk());
    ASSERT_EQ(rpc->setInvokeCount, 1);
    ASSERT_FALSE(rpc->invokedSetPayloadSizes.empty());
    EXPECT_EQ(rpc->invokedSetPayloadSizes.back(), 1u);  // one MemView payload inline
}

// MCreate: worker returns fewer results than requested → client releases the reported shm_ids.
TEST(ShmTransporterTest, MCreateReleasesShmIdsOnResponseCountMismatch)
{
    auto rpc = std::make_shared<FakeWorkerRpcClient>();
    rpc->multiCreateResultCount = 2;  // return 2 results for 3 keys → count mismatch
    ShmTransporter transporter(MakeAddress(9102), rpc);
    std::vector<std::string> keys = { "mk1", "mk2", "mk3" };
    std::vector<uint64_t> sizes = { 64, 64, 64 };
    std::vector<std::shared_ptr<ObjectBuffer>> buffers;
    // Mismatch → K_RUNTIME_ERROR; the 2 reported shm_ids must be released via InvokeDecreaseReference.
    EXPECT_EQ(transporter.MCreate(MakeAddress(9102), keys, sizes, MakeCreateParam(), buffers).GetCode(),
              K_RUNTIME_ERROR);
    EXPECT_TRUE(buffers.empty());
    EXPECT_EQ(rpc->decreaseReferenceCount, 2);
    EXPECT_EQ(rpc->decreaseReferenceShmIds.size(), 2u);
    EXPECT_EQ(rpc->decreaseReferenceShmIds[0], ShmKey::Intern("multi-shm-0"));
    EXPECT_EQ(rpc->decreaseReferenceShmIds[1], ShmKey::Intern("multi-shm-1"));
}

TEST(ShmTransporterTest, CreateReleasesWorkerAllocationWhenLocalBufferSetupFails)
{
    auto rpc = std::make_shared<FakeWorkerRpcClient>();
    ShmTransporter transporter(MakeAddress(9102), rpc);
    std::shared_ptr<ObjectBuffer> buffer;
    ASSERT_TRUE(inject::Set("ShmTransporter.BuildLocalBuffer", "return(K_OUT_OF_MEMORY)").IsOk());
    Raii clearInject([]() { (void)inject::Clear("ShmTransporter.BuildLocalBuffer"); });

    EXPECT_EQ(transporter.Create(MakeAddress(9102), "create-fail", 64, MakeCreateParam(), buffer).GetCode(),
              K_OUT_OF_MEMORY);
    EXPECT_EQ(buffer, nullptr);
    EXPECT_EQ(rpc->decreaseReferenceCount, 1);
    ASSERT_EQ(rpc->decreaseReferenceShmIds.size(), 1u);
    EXPECT_EQ(rpc->decreaseReferenceShmIds[0], ShmKey::Intern("test-shm-id"));
}

TEST(ShmTransporterTest, MCreateReleasesAllWorkerAllocationsWhenLocalBufferSetupFails)
{
    auto rpc = std::make_shared<FakeWorkerRpcClient>();
    ShmTransporter transporter(MakeAddress(9102), rpc);
    std::vector<std::shared_ptr<ObjectBuffer>> buffers;
    ASSERT_TRUE(inject::Set("ShmTransporter.BuildLocalBuffer", "return(K_OUT_OF_MEMORY)").IsOk());
    Raii clearInject([]() { (void)inject::Clear("ShmTransporter.BuildLocalBuffer"); });

    EXPECT_EQ(transporter.MCreate(MakeAddress(9102), { "first", "second" }, { 64, 64 },
                                  MakeCreateParam(), buffers).GetCode(),
              K_OUT_OF_MEMORY);
    EXPECT_TRUE(buffers.empty());
    EXPECT_EQ(rpc->decreaseReferenceCount, 2);
    ASSERT_EQ(rpc->decreaseReferenceShmIds.size(), 2u);
    EXPECT_EQ(rpc->decreaseReferenceShmIds[0], ShmKey::Intern("multi-shm-0"));
    EXPECT_EQ(rpc->decreaseReferenceShmIds[1], ShmKey::Intern("multi-shm-1"));
}

// MCreate success path: worker returns one result per key → buffers built, no Release.
TEST(ShmTransporterTest, MCreateSucceedsBuildsBuffers)
{
    auto rpc = std::make_shared<FakeWorkerRpcClient>();
    ShmTransporter transporter(MakeAddress(9103), rpc);
    std::vector<std::string> keys = { "ok1", "ok2" };
    std::vector<uint64_t> sizes = { 32, 32 };
    std::vector<std::shared_ptr<ObjectBuffer>> buffers;
    ASSERT_TRUE(transporter.MCreate(MakeAddress(9103), keys, sizes, MakeCreateParam(), buffers).IsOk());
    ASSERT_EQ(buffers.size(), keys.size());
    EXPECT_EQ(rpc->decreaseReferenceCount, 0);
}

// Release propagates the worker-side error (InvokeDecreaseReference failure).
TEST(ShmTransporterTest, ReleasePropagatesDecreaseReferenceError)
{
    auto rpc = std::make_shared<FakeWorkerRpcClient>();
    rpc->decreaseReferenceStatus = Status(K_RPC_UNAVAILABLE, "worker gone");
    ShmTransporter transporter(MakeAddress(9104), rpc);
    TransportRequestContext ctx;
    EXPECT_EQ(transporter.Release(ShmKey::Intern("shm-x"), ctx).GetCode(), K_RPC_UNAVAILABLE);
    EXPECT_EQ(rpc->decreaseReferenceCount, 1);
}

// MSet uses a single InvokeMultiSet RPC (not N serial Set calls). Verifies the batch path is taken.
TEST(ShmTransporterTest, MSetUsesSingleInvokeMultiSetNotSerialSet)
{
    auto rpc = std::make_shared<FakeWorkerRpcClient>();
    ShmTransporter transporter(MakeAddress(9200), rpc);
    // Build 3 buffers via Create (store_fd=0 → fallback, mmapEntry null).
    std::vector<std::shared_ptr<ObjectBuffer>> buffers;
    for (int i = 0; i < 3; ++i) {
        std::shared_ptr<ObjectBuffer> buf;
        ASSERT_TRUE(
            transporter.Create(MakeAddress(9200), "mset-k" + std::to_string(i), 64, MakeCreateParam(), buf).IsOk());
        buffers.push_back(buf);
    }
    TransportSetParam sp = MakeSetParam();
    TransportMSetResult result;
    ASSERT_TRUE(transporter.MSet(buffers, sp, result).IsOk());
    EXPECT_EQ(rpc->multiSetInvokeCount, 1);
    EXPECT_EQ(rpc->setInvokeCount, 0);
    ASSERT_EQ(rpc->invokedMultiSetRequests.size(), 1u);
    EXPECT_TRUE(result.lastRc.IsOk());
    // The mock Create returns store_fd=0 (no shm region), so Create falls back to a local payload buffer
    // (mmapEntry null) and MSet publishes inline (TCP). The SHM zero-copy kind is covered by the
    // KVClientTransportSetWithShmTest ST (store_fd>0 + fd-passing).
    EXPECT_EQ(result.actualKind, AccessTransportKind::TCP);
    EXPECT_TRUE(result.publishAttempted);
}

// MSet partial failure: worker reports some failed_object_keys → result.failedKeys filled, return OK.
TEST(ShmTransporterTest, MSetPartialFailureReturnsOkWithFailedKeys)
{
    auto rpc = std::make_shared<FakeWorkerRpcClient>();
    rpc->multiSetFailedKeys = { "mset-p1" };  // 1 of 3 fails
    rpc->multiSetLastCode = K_NOT_FOUND;
    rpc->multiSetLastMessage = "not found";
    ShmTransporter transporter(MakeAddress(9201), rpc);
    std::vector<std::shared_ptr<ObjectBuffer>> buffers;
    for (int i = 0; i < 3; ++i) {
        std::shared_ptr<ObjectBuffer> buf;
        ASSERT_TRUE(
            transporter.Create(MakeAddress(9201), "mset-p" + std::to_string(i), 64, MakeCreateParam(), buf).IsOk());
        // Simulate a SHM-backed buffer: both mmapEntry and shmId must be present for the auto_release path.
        auto &mutableInfo = ObjectBufferInternal::GetMutableInfo(*buf);
        mutableInfo.mmapEntry = std::make_shared<FakeMmapTableEntry>();
        mutableInfo.shmId = ShmKey::Intern("mset-shm-" + std::to_string(i));
        buffers.push_back(buf);
    }
    TransportSetParam sp = MakeSetParam();
    TransportMSetResult result;
    EXPECT_TRUE(transporter.MSet(buffers, sp, result).IsOk());
    ASSERT_EQ(result.failedKeys.size(), 1u);
    EXPECT_EQ(result.failedKeys[0], "mset-p1");
    EXPECT_EQ(result.lastRc.GetCode(), K_NOT_FOUND);
    EXPECT_TRUE(result.publishAttempted);
    ASSERT_EQ(rpc->invokedMultiSetRequests.size(), 1u);
    EXPECT_TRUE(rpc->invokedMultiSetRequests[0].auto_release_memory_ref());
    EXPECT_TRUE(result.workerAutoRelease);
}

// MSet full failure: every key fails + last_rc error → returns the last error.
TEST(ShmTransporterTest, MSetAllFailureReturnsLastError)
{
    auto rpc = std::make_shared<FakeWorkerRpcClient>();
    rpc->multiSetFailedKeys = { "mset-a0", "mset-a1" };
    rpc->multiSetLastCode = K_RPC_UNAVAILABLE;
    rpc->multiSetLastMessage = "worker gone";
    ShmTransporter transporter(MakeAddress(9202), rpc);
    std::vector<std::shared_ptr<ObjectBuffer>> buffers;
    for (int i = 0; i < 2; ++i) {
        std::shared_ptr<ObjectBuffer> buf;
        ASSERT_TRUE(
            transporter.Create(MakeAddress(9202), "mset-a" + std::to_string(i), 64, MakeCreateParam(), buf).IsOk());
        buffers.push_back(buf);
    }
    TransportSetParam sp = MakeSetParam();
    TransportMSetResult result;
    EXPECT_EQ(transporter.MSet(buffers, sp, result).GetCode(), K_RPC_UNAVAILABLE);
    EXPECT_EQ(result.failedKeys.size(), 2u);
    EXPECT_EQ(result.lastRc.GetCode(), K_RPC_UNAVAILABLE);
    EXPECT_TRUE(result.publishAttempted);
}

TEST(ShmTransporterTest, MSetFullFailureWithoutFailedKeysKeepsClientCleanup)
{
    auto rpc = std::make_shared<FakeWorkerRpcClient>();
    rpc->multiSetLastCode = K_OUT_OF_MEMORY;
    rpc->multiSetLastMessage = "master rejected the batch";
    const HostPort workerAddr = MakeAddress(9203);
    ShmTransporter transporter(workerAddr, rpc);
    auto first = MakeTransportBuffer(workerAddr, "mset-f0", "data", "shm-f0");
    auto second = MakeTransportBuffer(workerAddr, "mset-f1", "data", "shm-f1");
    ASSERT_NE(first, nullptr);
    ASSERT_NE(second, nullptr);
    ObjectBufferInternal::GetMutableInfo(*first).mmapEntry = std::make_shared<FakeMmapTableEntry>();
    ObjectBufferInternal::GetMutableInfo(*second).mmapEntry = std::make_shared<FakeMmapTableEntry>();

    TransportMSetResult result;
    Status rc = transporter.MSet({ first, second }, MakeSetParam(), result);

    EXPECT_EQ(rc.GetCode(), K_OUT_OF_MEMORY);
    ASSERT_EQ(rpc->invokedMultiSetRequests.size(), 1u);
    EXPECT_TRUE(rpc->invokedMultiSetRequests[0].auto_release_memory_ref());
    EXPECT_TRUE(result.failedKeys.empty());
    EXPECT_FALSE(result.workerAutoRelease);
}

// Owner-managed (routed SHM zero-copy) MSet buffers must NOT set auto_release_memory_ref: the
// send-side owner releases the worker ref on destruction, so asking the worker to auto-release too
// would double-release and flood worker "shmId not exists" warnings. Guards the routed-MSet
// double-release regression (anyOwnerManaged branch in BuildMultiPublishRequest).
TEST(ShmTransporterTest, MSetOwnerManagedBuffersDoNotAutoRelease)
{
    auto rpc = std::make_shared<FakeWorkerRpcClient>();
    const HostPort workerAddr = MakeAddress(9204);
    ShmTransporter transporter(workerAddr, rpc);
    std::vector<std::shared_ptr<ObjectBuffer>> buffers;
    for (int i = 0; i < 2; ++i) {
        auto buf = MakeTransportBuffer(workerAddr, "mset-own" + std::to_string(i), "data",
                                       "shm-own" + std::to_string(i));
        ASSERT_NE(buf, nullptr);
        auto &mutableInfo = ObjectBufferInternal::GetMutableInfo(*buf);
        // Routed SHM zero-copy buffer: mmapEntry present + a send-side owner that manages the worker ref.
        mutableInfo.mmapEntry = std::make_shared<FakeMmapTableEntry>();
        mutableInfo.receiveBufferOwner = std::make_shared<FakeBufferOwner>(static_cast<uint64_t>(buf->GetSize()), true);
        buffers.push_back(buf);
    }
    TransportMSetResult result;
    ASSERT_TRUE(transporter.MSet(buffers, MakeSetParam(), result).IsOk());
    ASSERT_EQ(rpc->invokedMultiSetRequests.size(), 1u);
    EXPECT_FALSE(rpc->invokedMultiSetRequests[0].auto_release_memory_ref());
    EXPECT_FALSE(result.workerAutoRelease);
}

// A store_fd<=0 Create fallback (worker did not allocate an shm region) must build a pure inline
// payload buffer that carries no worker shm_id. Sending inline payload + a stale shm_id would risk the
// worker dual-path attaching an uninitialized region. Guards the routed-Set fallback regression.
TEST(ShmTransporterTest, CreateFallbackBufferCarriesNoShmId)
{
    auto rpc = std::make_shared<FakeWorkerRpcClient>();
    ShmTransporter transporter(MakeAddress(9205), rpc);
    std::shared_ptr<ObjectBuffer> buf;
    ASSERT_TRUE(transporter.Create(MakeAddress(9205), "fb-k", 64, MakeCreateParam(), buf).IsOk());
    const auto &info = ObjectBufferInternal::GetInfo(*buf);
    EXPECT_TRUE(info.shmId.Empty());              // no worker region -> no shm_id carried
    EXPECT_EQ(info.mmapEntry, nullptr);           // fallback local buffer, no mmap
    EXPECT_EQ(info.receiveBufferOwner, nullptr);  // not owner-managed
}

// The 5 new shm metrics must register without ID collision (InitKvMetrics returns OK). This guards
// against the PR#1558 regression where worker_kv_event IDs collided with client_shm IDs.
// Metric ID collision is verified by the test fixture's InitBatchGetMetrics() (SetUp calls
// InitKvMetrics which returns OK only if all IDs are unique). No separate test needed here.

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

TEST(DataPlaneExecutorTest, NetworkBlipRebuildsCompleteEntryAndRetriesOnce)
{
    auto manager = std::make_shared<FakeDataPlaneManager>();
    manager->transporterGetStatuses = { { Status(K_RPC_NETWORK_BLIP, "network blip") }, { Status::OK() } };
    DataPlaneExecutor executor(manager, std::make_shared<TransportAdvisor>());
    DataGetRequest request{ "a", 1 };
    DataGetResult result;

    EXPECT_TRUE(executor.Execute(MakeAddress(24), [&request, &result](IDataTransporter &transporter) {
        return transporter.Get(request, result);
    }).IsOk());
    EXPECT_EQ(manager->rpcBuildCount, 2);
    EXPECT_EQ(manager->transportBuildCount, 2);
}

TEST(DataPlaneExecutorTest, PeerDeadTearsDownWithoutRetry)
{
    auto manager = std::make_shared<FakeDataPlaneManager>();
    manager->transporterGetStatuses = { { Status(K_RPC_PEER_DEAD, "peer dead") }, { Status::OK() } };
    DataPlaneExecutor executor(manager, std::make_shared<TransportAdvisor>());
    DataGetRequest request{ "a", 1 };
    DataGetResult result;

    EXPECT_EQ(executor.Execute(MakeAddress(25), [&request, &result](IDataTransporter &transporter) {
        return transporter.Get(request, result);
    }).GetCode(), K_RPC_PEER_DEAD);
    EXPECT_EQ(manager->rpcBuildCount, 1);
    EXPECT_EQ(manager->transportBuildCount, 1);
    ASSERT_EQ(manager->builtTransporters.size(), 1u);
    EXPECT_EQ(manager->builtTransporters[0]->getCount, 1);
    EXPECT_EQ(manager->builtTransporters[0]->closeCount, 1);
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

#ifdef USE_URMA_MOCK
TEST(DataPlaneExecutorTest, DrainingShmUsesUrmaMockUbBeforeTcpFallback)
{
    const bool enableUrma = FLAGS_enable_urma;
    Raii restoreEnableUrma([enableUrma]() { FLAGS_enable_urma = enableUrma; });
    FLAGS_enable_urma = true;

    const auto workerAddr = MakeAddress(26);
    auto advisor = std::make_shared<TransportAdvisor>();
    advisor->SetShmCandidateWorkers({ workerAddr });
    auto manager = std::make_shared<FakeDataPlaneManager>();
    manager->transporterGetStatuses = {
        { MakeWorkerDrainingStatus() }, { Status(K_URMA_CONNECT_FAILED, "mock UB connect failed") }, { Status::OK() }
    };
    DataPlaneExecutor executor(manager, advisor);
    DataGetRequest request{ "draining", 1 };
    DataGetResult result;

    EXPECT_TRUE(executor.Execute(workerAddr, [&request, &result](IDataTransporter &transporter) {
        return transporter.Get(request, result);
    }).IsOk());
    ASSERT_EQ(manager->builtTransporters.size(), 3u);
    EXPECT_EQ(manager->builtTransporters[0]->kind, AccessTransportKind::SHM);
    EXPECT_EQ(manager->builtTransporters[1]->kind, AccessTransportKind::UB);
    EXPECT_EQ(manager->builtTransporters[2]->kind, AccessTransportKind::TCP);
    EXPECT_EQ(manager->builtTransporters[0]->getCount, 1);
    EXPECT_EQ(manager->builtTransporters[1]->getCount, 1);
    EXPECT_EQ(manager->builtTransporters[2]->getCount, 1);
}

TEST(DataPlaneExecutorTest, DrainingShmFallbackReusesUbAndRefreshesOncePerPublishedSnapshot)
{
    const bool enableUrma = FLAGS_enable_urma;
    Raii restoreEnableUrma([enableUrma]() { FLAGS_enable_urma = enableUrma; });
    FLAGS_enable_urma = true;

    const auto workerAddr = MakeAddress(28);
    const auto otherWorkerAddr = MakeAddress(29);
    auto advisor = std::make_shared<TransportAdvisor>();
    advisor->SetShmCandidateWorkers({ workerAddr, otherWorkerAddr });
    auto manager = std::make_shared<FakeDataPlaneManager>();
    manager->transporterGetStatuses = {
        { MakeWorkerDrainingStatus() }, { Status::OK() }, { MakeWorkerDrainingStatus() },
        { Status::OK() }, { MakeWorkerDrainingStatus() }, { Status::OK() }
    };
    int refreshCount = 0;
    DataPlaneExecutor executor(manager, advisor,
                               [&refreshCount](const HostPort &, const Status &) { ++refreshCount; });
    DataGetRequest request{ "draining", 1 };
    DataGetResult result;
    auto get = [&request, &result](IDataTransporter &transporter) { return transporter.Get(request, result); };

    EXPECT_TRUE(executor.Execute(workerAddr, get).IsOk());
    EXPECT_TRUE(executor.Execute(workerAddr, get).IsOk());
    EXPECT_EQ(refreshCount, 1);
    ASSERT_EQ(manager->builtTransporters.size(), 2u);
    EXPECT_EQ(manager->builtTransporters[0]->kind, AccessTransportKind::SHM);
    EXPECT_EQ(manager->builtTransporters[0]->getCount, 1);
    EXPECT_EQ(manager->builtTransporters[1]->kind, AccessTransportKind::UB);
    EXPECT_EQ(manager->builtTransporters[1]->getCount, 2);

    EXPECT_TRUE(executor.Execute(otherWorkerAddr, get).IsOk());
    EXPECT_EQ(refreshCount, 1);

    advisor->SetShmCandidateWorkers({ workerAddr, otherWorkerAddr });
    EXPECT_TRUE(executor.Execute(workerAddr, get).IsOk());
    EXPECT_EQ(refreshCount, 2);
    ASSERT_EQ(manager->builtTransporters.size(), 6u);
    for (size_t i = 0; i < manager->builtTransporters.size(); i += 2) {
        EXPECT_EQ(manager->builtTransporters[i]->kind, AccessTransportKind::SHM);
        EXPECT_EQ(manager->builtTransporters[i + 1]->kind, AccessTransportKind::UB);
    }
}

TEST(DataPlaneExecutorTest, DrainingShmDoesNotHideUbApplicationError)
{
    const bool enableUrma = FLAGS_enable_urma;
    Raii restoreEnableUrma([enableUrma]() { FLAGS_enable_urma = enableUrma; });
    FLAGS_enable_urma = true;

    const auto workerAddr = MakeAddress(27);
    auto advisor = std::make_shared<TransportAdvisor>();
    advisor->SetShmCandidateWorkers({ workerAddr });
    auto manager = std::make_shared<FakeDataPlaneManager>();
    manager->transporterGetStatuses = {
        { MakeWorkerDrainingStatus() }, { Status(K_NOT_FOUND, "object not found") }, { Status::OK() }
    };
    DataPlaneExecutor executor(manager, advisor);
    DataGetRequest request{ "missing", 1 };
    DataGetResult result;

    EXPECT_EQ(executor.Execute(workerAddr, [&request, &result](IDataTransporter &transporter) {
        return transporter.Get(request, result);
    }).GetCode(), K_NOT_FOUND);
    ASSERT_EQ(manager->builtTransporters.size(), 2u);
    EXPECT_EQ(manager->builtTransporters[0]->kind, AccessTransportKind::SHM);
    EXPECT_EQ(manager->builtTransporters[1]->kind, AccessTransportKind::UB);
    EXPECT_EQ(manager->builtTransporters[0]->getCount, 1);
    EXPECT_EQ(manager->builtTransporters[1]->getCount, 1);
}
#endif

TEST(ReplicaReaderTest, FreshLocationVersionAdmitsWorkerMissingFromClientSnapshot)
{
    constexpr uint64_t clientTopologyVersion = 10;
    constexpr uint64_t locationTopologyVersion = 11;
    ApiDeadlineGuard deadline(1000);
    auto manager = std::make_shared<FakeDataPlaneManager>();
    WorkerSnapshot snapshot;
    snapshot.ringVersion = clientTopologyVersion;
    snapshot.remoteTransportAddrs.push_back(MakeAddress(30));
    ASSERT_TRUE(manager->UpdateWorkerSnapshot(snapshot).IsOk());
    auto executor = std::make_shared<DataPlaneExecutor>(manager, std::make_shared<TransportAdvisor>());
    ReplicaReader reader(executor, std::make_shared<DeadlineRetry>(), std::make_shared<ThreadPool>(1));
    auto location = MakeReplicaLocation("key", 4, { MakeAddress(31) });
    location.set_topology_version(locationTopologyVersion);
    ObjectReadItemResult result;

    ASSERT_TRUE(reader.Read(location, result, MakeReadContext()).IsOk());
    EXPECT_EQ(manager->rpcBuildCount, 1);
    EXPECT_EQ(manager->transportBuildCount, 1);
}

TEST(ReplicaReaderTest, BatchUsesNewestLocationVersionForSharedEndpoint)
{
    constexpr uint64_t clientTopologyVersion = 10;
    constexpr uint64_t locationTopologyVersion = 11;
    ApiDeadlineGuard deadline(1000);
    auto manager = std::make_shared<FakeDataPlaneManager>();
    WorkerSnapshot snapshot;
    snapshot.ringVersion = clientTopologyVersion;
    snapshot.remoteTransportAddrs.push_back(MakeAddress(30));
    ASSERT_TRUE(manager->UpdateWorkerSnapshot(snapshot).IsOk());
    auto executor = std::make_shared<DataPlaneExecutor>(manager, std::make_shared<TransportAdvisor>());
    ReplicaReader reader(executor, std::make_shared<DeadlineRetry>(), std::make_shared<ThreadPool>(1));
    const HostPort newWorker = MakeAddress(31);
    std::vector<master::ObjectLocationInfoPb> locations = {
        MakeReplicaLocation("new-version", 4, { newWorker }), MakeReplicaLocation("old-version", 4, { newWorker })
    };
    locations.front().set_topology_version(locationTopologyVersion);
    std::vector<ObjectReadItemResult> results(locations.size());

    ASSERT_TRUE(reader.ReadBatch({ MakeReplicaReadRequest(&locations[0], &results[0]),
                                   MakeReplicaReadRequest(&locations[1], &results[1]) })
                    .IsOk());
    ASSERT_NE(manager->lastTransporter, nullptr);
    EXPECT_EQ(manager->lastTransporter->batchGetCount, 1);
    EXPECT_TRUE(results[0].status.IsOk());
    EXPECT_TRUE(results[1].status.IsOk());
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
    ASSERT_TRUE(reader.Read(location, result, MakeReadContext()).IsOk());
    EXPECT_EQ(result.requestIndex, 7u);
    EXPECT_EQ(result.objectKey, "key");
    EXPECT_EQ(manager->transportBuildCount, 2);
}

TEST(ReplicaReaderTest, PeerDeadReplicaTriesNextLocationWithoutRefreshingMetadata)
{
    ApiDeadlineGuard deadline(1000);
    auto manager = std::make_shared<FakeDataPlaneManager>();
    manager->transporterGetStatuses = { { Status(K_RPC_PEER_DEAD, "dead replica") }, { Status::OK() } };
    auto executor = std::make_shared<DataPlaneExecutor>(manager, std::make_shared<TransportAdvisor>());
    ReplicaReader reader(executor, std::make_shared<DeadlineRetry>(), std::make_shared<ThreadPool>(1));
    auto location = MakeReplicaLocation("key", 4, { MakeAddress(33), MakeAddress(34) });
    ObjectReadItemResult result;

    ASSERT_TRUE(reader.Read(location, result, MakeReadContext()).IsOk());
    EXPECT_EQ(result.objectKey, "key");
    EXPECT_EQ(manager->transportBuildCount, 2);
}

TEST(ReplicaReaderTest, PeerDeadReplicaReturnsForMetadataRefreshAfterAllReplicas)
{
    ApiDeadlineGuard deadline(1000);
    auto manager = std::make_shared<FakeDataPlaneManager>();
    manager->transporterGetStatuses = { { Status(K_RPC_PEER_DEAD, "dead replica") } };
    auto executor = std::make_shared<DataPlaneExecutor>(manager, std::make_shared<TransportAdvisor>());
    ReplicaReader reader(executor, std::make_shared<DeadlineRetry>(), std::make_shared<ThreadPool>(1));
    auto location = MakeReplicaLocation("key", 4, { MakeAddress(35) });
    ObjectReadItemResult result;

    Status rc = reader.Read(location, result, MakeReadContext());
    EXPECT_TRUE(IsTransportSnapshotStaleLocation(rc)) << rc.ToString();
    EXPECT_NE(rc.GetMsg().find("dead replica"), std::string::npos);
    EXPECT_EQ(manager->transportBuildCount, 1);
}

TEST(ReplicaReaderTest, StaleTransportSnapshotTriesNextReplica)
{
    ApiDeadlineGuard deadline(1000);
    const auto staleAddress = MakeAddress(80);
    const auto healthyAddress = MakeAddress(81);
    std::unordered_map<HostPort, size_t> admissionChecks;
    auto manager = std::make_shared<FakeDataPlaneManager>();
    auto executor = std::make_shared<DataPlaneExecutor>(manager, std::make_shared<TransportAdvisor>());
    ReplicaReader reader(
        executor, std::make_shared<DeadlineRetry>(), std::make_shared<ThreadPool>(1),
        [&admissionChecks, staleAddress](const HostPort &address) {
            ++admissionChecks[address];
            return address == staleAddress ? MakeStaleSnapshotStatus(address) : Status::OK();
        });
    auto location = MakeReplicaLocation("key", 4, { staleAddress, healthyAddress });
    ObjectReadItemResult result;

    ASSERT_TRUE(reader.Read(location, result, MakeReadContext()).IsOk());
    EXPECT_EQ(result.objectKey, "key");
    EXPECT_EQ(admissionChecks[staleAddress], 1u);
    EXPECT_EQ(admissionChecks[healthyAddress], 1u);
    EXPECT_EQ(manager->transportBuildCount, 1);
}

TEST(ReplicaReaderTest, DrainingWorkerTriesNextReplica)
{
    ApiDeadlineGuard deadline(1000);
    auto manager = std::make_shared<FakeDataPlaneManager>();
    manager->transporterGetStatuses = { { MakeWorkerDrainingStatus() }, { Status::OK() } };
    auto executor = std::make_shared<DataPlaneExecutor>(manager, std::make_shared<TransportAdvisor>());
    ReplicaReader reader(executor, std::make_shared<DeadlineRetry>(), std::make_shared<ThreadPool>(1));
    auto location = MakeReplicaLocation("key", 4, { MakeAddress(86), MakeAddress(87) });
    ObjectReadItemResult result;

    ASSERT_TRUE(reader.Read(location, result, MakeReadContext()).IsOk());
    EXPECT_EQ(result.objectKey, "key");
    EXPECT_EQ(manager->transportBuildCount, 2);
}

TEST(ReplicaReaderTest, StaleTransportSnapshotReturnsForMetadataRefreshAfterAllReplicas)
{
    ApiDeadlineGuard deadline(1000);
    const auto staleAddress = MakeAddress(82);
    auto manager = std::make_shared<FakeDataPlaneManager>();
    auto executor = std::make_shared<DataPlaneExecutor>(manager, std::make_shared<TransportAdvisor>());
    ReplicaReader reader(
        executor, std::make_shared<DeadlineRetry>(), std::make_shared<ThreadPool>(1),
        [staleAddress](const HostPort &address) {
            return address == staleAddress ? MakeStaleSnapshotStatus(address) : Status::OK();
        });
    auto location = MakeReplicaLocation("key", 4, { staleAddress });
    ObjectReadItemResult result;

    Status rc = reader.Read(location, result, MakeReadContext());
    EXPECT_TRUE(IsTransportSnapshotStaleLocation(rc));
    EXPECT_EQ(manager->transportBuildCount, 0);
}

TEST(ReplicaReaderTest, EnablesTransportPhaseRecordingOnlyForTracedRequest)
{
    ApiDeadlineGuard deadline(1000);
    auto manager = std::make_shared<FakeDataPlaneManager>();
    auto executor = std::make_shared<DataPlaneExecutor>(manager, std::make_shared<TransportAdvisor>());
    ReplicaReader reader(executor, std::make_shared<DeadlineRetry>(), std::make_shared<ThreadPool>(1));
    auto normalLocation = MakeReplicaLocation("normal", 4, { MakeAddress(33) });
    auto tracedLocation = MakeReplicaLocation("traced", 4, { MakeAddress(34) });
    ObjectReadItemResult normalResult;
    ObjectReadItemResult tracedResult;
    ASSERT_TRUE(reader.Read(normalLocation, normalResult, MakeReadContext()).IsOk());
    ASSERT_TRUE(reader.Read(tracedLocation, tracedResult, MakeReadContext(), true).IsOk());

    EXPECT_EQ(manager->transportBuildTraceEnabled, std::vector<bool>({ false, true }));
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

    EXPECT_EQ(reader.Read(location, result, MakeReadContext()).GetCode(), K_INVALID);
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

    ASSERT_TRUE(reader.Read(location, result, MakeReadContext()).IsOk());
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
        requests.push_back({ &locations[i], &results[i], MakeReadContext() });
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
        requests.push_back({ &locations[i], &results[i], MakeReadContext() });
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
        requests.push_back({ &locations[i], &results[i], MakeReadContext() });
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
        requests.push_back({ &locations[i], &results[i], MakeReadContext() });
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

    ASSERT_TRUE(reader.ReadBatch({ MakeReplicaReadRequest(&locations[0], &results[0]),
                                   MakeReplicaReadRequest(&locations[1], &results[1]) })
                    .IsOk());
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

    ASSERT_TRUE(reader.ReadBatch({ MakeReplicaReadRequest(&location, &result) }).IsOk());
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

    ASSERT_TRUE(reader.ReadBatch({ MakeReplicaReadRequest(&location, &result) }).IsOk());
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
        countRequests.push_back(MakeReplicaReadRequest(&countLocations.back(), &countResults[i]));
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
    ASSERT_TRUE(byteReader.ReadBatch({ MakeReplicaReadRequest(&byteLocations[0], &byteResults[0]),
                                       MakeReplicaReadRequest(&byteLocations[1], &byteResults[1]),
                                       MakeReplicaReadRequest(&byteLocations[2], &byteResults[2]) })
                    .IsOk());
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

    ASSERT_TRUE(reader.ReadBatch({ MakeReplicaReadRequest(&locations[0], &results[0]),
                                   MakeReplicaReadRequest(&locations[1], &results[1]),
                                   MakeReplicaReadRequest(&locations[2], &results[2]) })
                    .IsOk());
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

    ASSERT_TRUE(reader.ReadBatch({ MakeReplicaReadRequest(&locations[0], &results[0]),
                                   MakeReplicaReadRequest(&locations[1], &results[1]),
                                   MakeReplicaReadRequest(&locations[2], &results[2]) })
                    .IsOk());
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

    EXPECT_TRUE(reader.ReadBatch({ MakeReplicaReadRequest(&emptyLocation, &emptyResult),
                                   MakeReplicaReadRequest(nullptr, nullptr),
                                   MakeReplicaReadRequest(&validLocation, &validResult) })
                    .IsOk());
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

    EXPECT_EQ(reader.ReadBatch({ MakeReplicaReadRequest(&location, &result) }).GetCode(), K_INVALID);
    EXPECT_EQ(result.status.GetCode(), K_INVALID);
    EXPECT_EQ(manager->transportBuildCount, 1);
}

TEST(ReplicaReaderTest, BatchStaleTransportSnapshotAdvancesReplica)
{
    ApiDeadlineGuard deadline(1000);
    const auto staleAddress = MakeAddress(83);
    const auto healthyAddress = MakeAddress(84);
    std::unordered_map<HostPort, size_t> admissionChecks;
    auto manager = std::make_shared<FakeDataPlaneManager>();
    auto executor = std::make_shared<DataPlaneExecutor>(manager, std::make_shared<TransportAdvisor>());
    ReplicaReader reader(
        executor, std::make_shared<DeadlineRetry>(), std::make_shared<ThreadPool>(1),
        [&admissionChecks, staleAddress](const HostPort &address) {
            ++admissionChecks[address];
            return address == staleAddress ? MakeStaleSnapshotStatus(address) : Status::OK();
        });
    auto location = MakeReplicaLocation("key", 1, { staleAddress, healthyAddress });
    ObjectReadItemResult result;

    ASSERT_TRUE(reader.ReadBatch({ MakeReplicaReadRequest(&location, &result) }).IsOk());
    EXPECT_TRUE(result.status.IsOk());
    EXPECT_EQ(admissionChecks[staleAddress], 1u);
    EXPECT_EQ(admissionChecks[healthyAddress], 1u);
    EXPECT_EQ(manager->transportBuildCount, 1);
}

TEST(ReplicaReaderTest, BatchDrainingWorkerAdvancesReplica)
{
    ApiDeadlineGuard deadline(1000);
    auto manager = std::make_shared<FakeDataPlaneManager>();
    manager->configureTransporter = [](const HostPort &address, FakeTransporter &transporter) {
        if (address.ToString() == MakeAddress(88).ToString()) {
            transporter.getStatuses = { MakeWorkerDrainingStatus() };
        }
    };
    auto executor = std::make_shared<DataPlaneExecutor>(manager, std::make_shared<TransportAdvisor>());
    ReplicaReader reader(executor, std::make_shared<DeadlineRetry>(), std::make_shared<ThreadPool>(1));
    auto location = MakeReplicaLocation("key", 1, { MakeAddress(88), MakeAddress(89) });
    ObjectReadItemResult result;

    ASSERT_TRUE(reader.ReadBatch({ MakeReplicaReadRequest(&location, &result) }).IsOk());
    EXPECT_TRUE(result.status.IsOk());
    EXPECT_EQ(manager->transportBuildCount, 2);
}

TEST(ReplicaReaderTest, BatchStaleTransportSnapshotCompletesForMetadataRefreshAfterAllReplicas)
{
    ApiDeadlineGuard deadline(1000);
    const auto staleAddress = MakeAddress(85);
    auto manager = std::make_shared<FakeDataPlaneManager>();
    auto executor = std::make_shared<DataPlaneExecutor>(manager, std::make_shared<TransportAdvisor>());
    ReplicaReader reader(
        executor, std::make_shared<DeadlineRetry>(), std::make_shared<ThreadPool>(1),
        [staleAddress](const HostPort &address) {
            return address == staleAddress ? MakeStaleSnapshotStatus(address) : Status::OK();
        });
    auto location = MakeReplicaLocation("key", 1, { staleAddress });
    ObjectReadItemResult result;

    Status rc = reader.ReadBatch({ MakeReplicaReadRequest(&location, &result) });
    EXPECT_TRUE(IsTransportSnapshotStaleLocation(rc));
    EXPECT_TRUE(IsTransportSnapshotStaleLocation(result.status));
    EXPECT_EQ(manager->transportBuildCount, 0);
}

TEST(ReplicaReaderTest, BatchPeerDeadReplicaCompletesForMetadataRefreshAfterAllReplicas)
{
    ApiDeadlineGuard deadline(1000);
    auto manager = std::make_shared<FakeDataPlaneManager>();
    manager->configureTransporter = [](const HostPort &, FakeTransporter &transporter) {
        transporter.getHandler = [](const DataGetRequest &, DataGetResult &) {
            return Status(K_RPC_PEER_DEAD, "dead replica");
        };
    };
    auto executor = std::make_shared<DataPlaneExecutor>(manager, std::make_shared<TransportAdvisor>());
    ReplicaReader reader(executor, std::make_shared<DeadlineRetry>(), std::make_shared<ThreadPool>(1));
    auto location = MakeReplicaLocation("key", 1, { MakeAddress(90) });
    ObjectReadItemResult result;

    Status rc = reader.ReadBatch({ MakeReplicaReadRequest(&location, &result) });
    EXPECT_TRUE(IsTransportSnapshotStaleLocation(rc)) << rc.ToString();
    EXPECT_TRUE(IsTransportSnapshotStaleLocation(result.status)) << result.status.ToString();
    EXPECT_NE(result.status.GetMsg().find("dead replica"), std::string::npos);
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

    ASSERT_TRUE(reader.ReadBatch({ MakeReplicaReadRequest(&locations[0], &results[0]),
                                   MakeReplicaReadRequest(&locations[1], &results[1]) })
                    .IsOk());
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

    ASSERT_TRUE(reader.ReadBatch({ MakeReplicaReadRequest(&locations[0], &results[0]),
                                   MakeReplicaReadRequest(&locations[1], &results[1]) })
                    .IsOk());
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

    Status status = reader.ReadBatch({ MakeReplicaReadRequest(&location, &result) });
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

    Status status = reader.ReadBatch({ MakeReplicaReadRequest(&location, &result) });
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

    EXPECT_EQ(reader.ReadBatch({ MakeReplicaReadRequest(&location, &result) }).GetCode(), K_NOT_FOUND);
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

    ASSERT_TRUE(reader.ReadBatch({ MakeReplicaReadRequest(&locations[0], &results[0]),
                                   MakeReplicaReadRequest(&locations[1], &results[1]) })
                    .IsOk());
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
        requests.push_back({ &locations.back(), &results[i], MakeReadContext() });
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

TEST(ObjectBufferTest, DestructorFreesLocalPayloadWithWorkerShmId)
{
    auto info = std::make_shared<ObjectBufferInfo>();
    info->objectKey = "routed-local-payload";
    info->dataSize = 32;
    info->metadataSize = 0;
    info->workerAddr = MakeAddress(9000);
    info->pointer = nullptr;
    info->shmId = ShmKey::Intern("worker-allocation-id");

    std::shared_ptr<ObjectBuffer> buffer;
    ASSERT_TRUE(ObjectBufferInternal::Create(info, buffer).IsOk());
    ASSERT_NE(info->pointer, nullptr);
    buffer.reset();
    EXPECT_EQ(info->pointer, nullptr);
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
    EXPECT_FALSE(publishRequest.auto_release_memory_ref());
    ASSERT_EQ(publishRequest.object_info_size(), 2);
    EXPECT_EQ(publishRequest.object_info(0).object_key(), "fallback-key");
    EXPECT_TRUE(publishRequest.object_info(0).shm_id().empty());
    EXPECT_EQ(publishRequest.object_info(1).object_key(), "ub-key");
    EXPECT_EQ(publishRequest.object_info(1).shm_id(), "shm-ub");
    ASSERT_EQ(payloads.size(), 1u);
    EXPECT_EQ(std::string(static_cast<const char *>(payloads[0].Data()), payloads[0].Size()), "tcp");
}

TEST(MSetRequestBuilderTest, EnablesWorkerAutoReleaseOnlyForPureShmOrUbBatch)
{
    const HostPort workerAddr = MakeAddress(9000);
    auto first = MakeTransportBuffer(workerAddr, "key-a", "data-a", "shm-a", true);
    auto second = MakeTransportBuffer(workerAddr, "key-b", "data-b", "shm-b", true);
    ASSERT_NE(first, nullptr);
    ASSERT_NE(second, nullptr);
    MultiPublishReqPb request;
    std::vector<MemView> payloads;

    ASSERT_TRUE(
        BuildMultiPublishRequest({ first, second }, { false, false }, MakeSetParam(), request, payloads).IsOk());
    EXPECT_TRUE(request.auto_release_memory_ref());
    EXPECT_TRUE(payloads.empty());

    ObjectBufferInternal::GetMutableInfo(*second).shmId = ShmKey();
    ASSERT_TRUE(
        BuildMultiPublishRequest({ first, second }, { false, false }, MakeSetParam(), request, payloads).IsOk());
    EXPECT_FALSE(request.auto_release_memory_ref());
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
    EXPECT_FALSE(request.auto_release_memory_ref());
    EXPECT_EQ(request.object_info(0).object_key(), "fallback-key");
    EXPECT_TRUE(request.object_info(0).shm_id().empty());
    EXPECT_EQ(request.object_info(1).object_key(), "ub-key");
    EXPECT_EQ(request.object_info(1).shm_id(), "shm-ub");
    ASSERT_EQ(rpcClient->invokedMultiSetPayloadData[0].size(), 1u);
    EXPECT_EQ(rpcClient->invokedMultiSetPayloadData[0][0], "tcp");
    EXPECT_EQ(result.actualKind, AccessTransportKind::TCP);
    EXPECT_FALSE(result.workerAutoRelease);
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
    ASSERT_EQ(rpcClient->invokedMultiSetRequests.size(), 1u);
    EXPECT_TRUE(rpcClient->invokedMultiSetRequests[0].auto_release_memory_ref());
    EXPECT_TRUE(result.workerAutoRelease);
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
    ASSERT_EQ(rpcClient->invokedMultiSetRequests.size(), 1u);
    EXPECT_TRUE(rpcClient->invokedMultiSetRequests[0].auto_release_memory_ref());
    EXPECT_TRUE(result.workerAutoRelease);
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
    EXPECT_FALSE(result.workerAutoRelease);
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
    first.remoteTransportAddrs = { survivor, marker };
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
    latest.remoteTransportAddrs = { survivor };
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

TEST(TransportLayerTest, SetPeerDeadTearsDownWithoutRetry)
{
    auto manager = std::make_shared<FakeDataPlaneManager>();
    manager->transporterSetStatuses = { { Status(K_RPC_PEER_DEAD, "peer dead") }, { Status::OK() } };
    TestTransportLayer layer(manager);

    TransportCreateParam createParam = MakeCreateParam();
    std::shared_ptr<ObjectBuffer> buffer;
    ASSERT_TRUE(layer.Create(MakeAddress(35), "peer-dead-key", 64, createParam, buffer).IsOk());

    TransportSetParam setParam = MakeSetParam();
    Status rc = layer.Set(*buffer, setParam);
    EXPECT_EQ(rc.GetCode(), K_RPC_PEER_DEAD) << rc.ToString();
    int setCount = 0;
    int releaseCount = 0;
    for (const auto &transporter : manager->builtTransporters) {
        setCount += transporter->setCount;
        releaseCount += transporter->releaseCount;
    }
    EXPECT_EQ(setCount, 1);
    EXPECT_EQ(releaseCount, 1);
    ASSERT_GE(manager->builtTransporters.size(), 2u);
    EXPECT_EQ(manager->builtTransporters[0]->closeCount, 1);
    EXPECT_EQ(manager->builtTransporters[1]->releaseCount, 1);
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

// MCreate replays an ambiguous (K_RPC_UNAVAILABLE, response lost) failure: it rebuilds the RPC +
// data plane and retries once, consistent with Create and Set (Component C). A lost response may
// leave the worker with partial allocations, reclaimed by the expired-fds reconciler (the same
// fallback Create relies on). MCreate has no publish step, so unlike MSet it always replays.
TEST(TransportLayerTest, MCreateReplaysAmbiguousRpcFailure)
{
    auto manager = std::make_shared<FakeDataPlaneManager>();
    manager->transporterMCreateStatuses = { { Status(K_RPC_UNAVAILABLE, "response lost") } };
    TestTransportLayer layer(manager);
    std::vector<std::shared_ptr<ObjectBuffer>> buffers;

    EXPECT_TRUE(layer.MCreate(MakeAddress(40), { "key-a", "key-b" }, { 4, 4 }, MakeCreateParam(), buffers).IsOk());
    EXPECT_FALSE(buffers.empty());
    EXPECT_EQ(manager->transportBuildCount, 2);  // initial build + rebuild after Teardown
    ASSERT_EQ(manager->builtTransporters.size(), 2u);
    EXPECT_EQ(manager->builtTransporters[0]->mCreateCount, 1);  // first attempt on the original transporter
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

TEST(TransportLayerTest, MSetWorkerAutoReleaseSkipsSuccessfulObjectClientReleases)
{
    auto manager = std::make_shared<FakeDataPlaneManager>();
    manager->configureTransporter = [](const HostPort &, FakeTransporter &transporter) {
        transporter.mSetWorkerAutoRelease = true;
    };
    TestTransportLayer layer(manager);
    std::vector<std::shared_ptr<ObjectBuffer>> buffers;
    ASSERT_TRUE(layer.MCreate(MakeAddress(42), { "key-a", "key-b" }, { 4, 4 }, MakeCreateParam(), buffers).IsOk());

    TransportMSetResult result;
    ASSERT_TRUE(layer.MSet(buffers, MakeSetParam(), result).IsOk());

    ASSERT_NE(manager->lastTransporter, nullptr);
    EXPECT_TRUE(result.workerAutoRelease);
    EXPECT_EQ(manager->lastTransporter->releaseCount, 0);
}

TEST(TransportLayerTest, MSetWorkerAutoReleaseKeepsClientCleanupForFailedObjects)
{
    auto manager = std::make_shared<FakeDataPlaneManager>();
    manager->configureTransporter = [](const HostPort &, FakeTransporter &transporter) {
        transporter.mSetWorkerAutoRelease = true;
        transporter.mSetFailedKeys = { "key-b" };
    };
    TestTransportLayer layer(manager);
    std::vector<std::shared_ptr<ObjectBuffer>> buffers;
    ASSERT_TRUE(layer.MCreate(MakeAddress(43), { "key-a", "key-b" }, { 4, 4 }, MakeCreateParam(), buffers).IsOk());

    TransportMSetResult result;
    ASSERT_TRUE(layer.MSet(buffers, MakeSetParam(), result).IsOk());

    ASSERT_NE(manager->lastTransporter, nullptr);
    ASSERT_EQ(manager->lastTransporter->releasedShmIds.size(), 1u);
    EXPECT_EQ(manager->lastTransporter->releasedShmIds[0].ToString(), "fake-shm-id");
    ASSERT_EQ(result.failedKeys.size(), 1u);
    EXPECT_EQ(result.failedKeys[0], "key-b");
}

// Regression: UbTransporter::Create must attach a ShmSendBufferOwner (ManagesWorkerReference=true)
// so Buffer::Release releases the shmId via the routed worker's RPC client, not LOCAL_WORKER.
// Without it, DecreaseReference goes to LOCAL_WORKER, causing "shmId not exists" warnings when
// the LOCAL_WORKER is not the worker that allocated the shmId (enableLocalCache=false + UB route).
//
// Directly construct and destroy ShmSendBufferOwner to verify the owner mechanism: destructor
// triggers InvokeDecreaseReference on the routed rpcClient (not LOCAL_WORKER).
TEST(UbTransporterTest, ShmSendBufferOwnerReleasesOnRoutedWorker)
{
    auto rpcClient = std::make_shared<FakeWorkerRpcClient>();
    auto context = MakeRequestContext();
    auto pool = std::make_shared<ThreadPool>(0, 1, "test_release");
    {
        auto owner = std::make_shared<ShmSendBufferOwner>(
            rpcClient, ShmKey::Intern("test-shm-id"), context, pool, nullptr);
        EXPECT_TRUE(owner->ManagesWorkerReference());
        EXPECT_EQ(rpcClient->decreaseReferenceCount, 0);
    }
    // Owner destroyed: async Release via pool → InvokeDecreaseReference on rpcClient.
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    EXPECT_EQ(rpcClient->decreaseReferenceCount, 1);
    ASSERT_FALSE(rpcClient->decreaseReferenceShmIds.empty());
    EXPECT_EQ(rpcClient->decreaseReferenceShmIds[0].ToString(), "test-shm-id");
}
}  // namespace
}  // namespace client
}  // namespace datasystem
