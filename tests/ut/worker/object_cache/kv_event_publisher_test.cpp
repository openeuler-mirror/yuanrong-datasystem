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

/**
 * Description: Test KV event publisher.
 */

// brpc headers must precede datasystem logging headers because brpc uses streamable CHECK_* macros.
#include <brpc/controller.h>

#include "datasystem/worker/object_cache/kv_event/kv_event_publisher.h"

#include <chrono>
#include <functional>
#include <gtest/gtest.h>
#include <memory>
#include <new>
#include <nlohmann/json.hpp>
#include <set>
#include <string_view>
#include <thread>
#include <unistd.h>

#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/worker/object_cache/kv_event/zmq_lite.h"
#include "ut/common.h"
#include "datasystem/worker/object_cache/worker_oc_eviction_manager.h"
#include "datasystem/worker/object_cache/obj_cache_shm_unit.h"
#include "datasystem/worker/object_cache/service/worker_oc_service_crud_common_api.h"
#include "datasystem/worker/object_cache/service/worker_oc_service_multi_publish_impl.h"
#include "tests/ut/worker/object_cache/test_metadata_route.h"

namespace datasystem {
namespace ut {
namespace {
using object_cache::BuildKvEventConfigFromJsonString;
using object_cache::KvEventPublisher;
using object_cache::ObjCacheShmUnit;
using object_cache::ObjectGlobalRefTable;
using object_cache::ObjectKV;
using object_cache::SafeObjType;
using object_cache::WorkerOcEvictionManager;
using object_cache::WorkerOcServiceMultiPublishImpl;
using object_cache::WorkerOcServiceCrudCommonApi;
using object_cache::WorkerOcServiceCrudParam;
using object_cache::WorkerRequestManager;
using object_cache::kKvEventMediumCpu;

constexpr int K_TEST_RECV_TIMEOUT_MS = 200;
constexpr int K_TEST_PUBLISH_RETRIES = 20;

class KvEventPublisherTest : public CommonTest {
};

class TestCrudApi : public WorkerOcServiceCrudCommonApi {
public:
    using WorkerOcServiceCrudCommonApi::WorkerOcServiceCrudCommonApi;

    void PublishStoredForTest(const std::string &objectKey, const std::string &medium) const
    {
        PublishKvStoredEvent(kvEventPublisher_, objectKey, medium);
    }
};

std::string TestEndpoint()
{
    auto now = std::chrono::steady_clock::now().time_since_epoch().count();
    return "ipc:///tmp/yuanrong_kv_event_publisher_test_" + std::to_string(getpid()) + "_" + std::to_string(now);
}

std::string IpcPath(const std::string &endpoint)
{
    const std::string prefix = "ipc://";
    return endpoint.substr(prefix.size());
}

std::string VllmPoolKey(const std::string &chunkHash, const std::string &tenantId = "")
{
    const std::string tenantPrefix = tenantId.empty() ? "" : tenantId + "$";
    return tenantPrefix +
           "Qwen3@pcp0@dcp0@head_or_tp_rank:0@pp_rank:0@group:0@cache_role:kv@cache_family:c1@" + chunkHash;
}

uint64_t ReadBigEndianUint64(const ZmqLiteMessage &msg)
{
    const auto *bytes = static_cast<const uint8_t *>(msg.Data());
    uint64_t value = 0;
    for (size_t i = 0; i < sizeof(uint64_t); ++i) {
        value = (value << 8) | bytes[i];
    }
    return value;
}

bool RecvBatch(ZmqLiteSocket &subscriber, nlohmann::json &payload)
{
    ZmqLiteMessage topicFrame;
    if (subscriber.RecvMsg(topicFrame, ZmqLiteRecvFlags::NONE).IsError()) {
        return false;
    }
    EXPECT_TRUE(topicFrame.Empty());
    EXPECT_TRUE(topicFrame.More());

    ZmqLiteMessage seqFrame;
    if (subscriber.RecvMsg(seqFrame, ZmqLiteRecvFlags::NONE).IsError()) {
        return false;
    }
    EXPECT_EQ(seqFrame.Size(), sizeof(uint64_t));
    EXPECT_GT(ReadBigEndianUint64(seqFrame), 0U);
    EXPECT_TRUE(seqFrame.More());

    ZmqLiteMessage payloadFrame;
    if (subscriber.RecvMsg(payloadFrame, ZmqLiteRecvFlags::NONE).IsError()) {
        return false;
    }
    EXPECT_FALSE(payloadFrame.More());
    payload = nlohmann::json::from_msgpack(static_cast<const uint8_t *>(payloadFrame.Data()),
                                           static_cast<const uint8_t *>(payloadFrame.Data()) + payloadFrame.Size());
    return true;
}

void WithPublisherAndSubscriber(object_cache::KvEventConfig config,
                                const std::function<void(const std::shared_ptr<KvEventPublisher> &)> &action,
                                nlohmann::json &payload, bool warmup = false, size_t expectedEventCount = 1)
{
    auto endpoint = TestEndpoint();
    (void)unlink(IpcPath(endpoint).c_str());

    config.enabled = true;
    config.bindEndpoint = endpoint;
    config.modelName = "Qwen3";
    config.backendId = "worker-0";
    config.tenantId = "default-tenant";
    config.blockSize = 128;
    config.dpRank = 2;
    config.queueCapacity = 32;
    auto publisher = std::make_shared<KvEventPublisher>(config);
    ASSERT_TRUE(publisher->Enabled());

    ZmqLiteContext subscriberContext;
    DS_ASSERT_OK(subscriberContext.Init());
    auto subscriber = subscriberContext.CreateZmqSocket(ZmqLiteSocketType::SUB);
    ASSERT_TRUE(subscriber.IsValid());
    DS_ASSERT_OK(subscriber.SetLinger(0));
    DS_ASSERT_OK(subscriber.SetRcvtimeo(K_TEST_RECV_TIMEOUT_MS));
    DS_ASSERT_OK(subscriber.SubscribeAll());
    DS_ASSERT_OK(subscriber.Connect(endpoint));
    std::this_thread::sleep_for(std::chrono::milliseconds(20));

    if (warmup) {
        nlohmann::json warmupPayload;
        for (int i = 0; i < K_TEST_PUBLISH_RETRIES && warmupPayload.is_null(); ++i) {
            publisher->PublishStored(VllmPoolKey("1", "tenant-a"), "memory");
            (void)RecvBatch(subscriber, warmupPayload);
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        ASSERT_FALSE(warmupPayload.is_null());
    }

    action(publisher);
    for (int i = 0; i < K_TEST_PUBLISH_RETRIES
         && (payload.is_null() || payload[1].size() < expectedEventCount); ++i) {
        nlohmann::json batchPayload;
        if (RecvBatch(subscriber, batchPayload)) {
            if (payload.is_null()) {
                payload = std::move(batchPayload);
            } else {
                for (const auto &event : batchPayload[1]) {
                    payload[1].push_back(event);
                }
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }

    subscriber = ZmqLiteSocket();
    subscriberContext.Close(false);
    (void)unlink(IpcPath(endpoint).c_str());
}

void PublishStoredAndReceive(object_cache::KvEventConfig config, nlohmann::json &payload)
{
    WithPublisherAndSubscriber(config,
                               [](const std::shared_ptr<KvEventPublisher> &publisher) {
                                   publisher->PublishStored(VllmPoolKey("2a", "tenant-a"), "memory");
                               },
                               payload, true);
}

TEST_F(KvEventPublisherTest, EmptyConfigDisablesPublisher)
{
    auto config = BuildKvEventConfigFromJsonString("");

    ASSERT_FALSE(config.enabled);
}

TEST_F(KvEventPublisherTest, ParsesJsonConfig)
{
    auto config = BuildKvEventConfigFromJsonString(R"({
        "bind_endpoint": "tcp://127.0.0.1:15557",
        "model_name": "Qwen3",
        "backend_id": "worker-0",
        "tenant_id": "tenant-a",
        "additional_salt": "salt",
        "lora_name": "adapter",
        "block_size": 128,
        "dp_rank": 2,
        "emit_legacy_compat_fields": false,
        "queue_capacity": 1024
    })");

    ASSERT_TRUE(config.enabled);
    ASSERT_EQ(config.bindEndpoint, "tcp://127.0.0.1:15557");
    ASSERT_EQ(config.modelName, "Qwen3");
    ASSERT_EQ(config.backendId, "worker-0");
    ASSERT_EQ(config.tenantId, "tenant-a");
    ASSERT_EQ(config.additionalSalt, "salt");
    ASSERT_EQ(config.loraName, "adapter");
    ASSERT_EQ(config.blockSize, 128U);
    ASSERT_EQ(config.dpRank, 2U);
    ASSERT_FALSE(config.emitLegacyCompatFields);
    ASSERT_EQ(config.queueCapacity, 1024U);
}

TEST_F(KvEventPublisherTest, LegacyCompatFieldsDefaultToEnabledAndCanBeDisabled)
{
    auto config = BuildKvEventConfigFromJsonString(R"({
        "bind_endpoint": "tcp://127.0.0.1:15557",
        "backend_id": "worker-0"
    })");

    ASSERT_TRUE(config.enabled);
    ASSERT_TRUE(config.emitLegacyCompatFields);

    nlohmann::json payloadWithDefault;
    PublishStoredAndReceive(config, payloadWithDefault);
    ASSERT_FALSE(payloadWithDefault.is_null());
    const auto &eventWithDefault = payloadWithDefault[1][0];
    ASSERT_EQ(eventWithDefault["type"], "BlockStored");
    ASSERT_EQ(eventWithDefault["block_hashes"][0], 42U);
    ASSERT_TRUE(eventWithDefault["parent_block_hash"].is_null());

    auto disabledConfig = BuildKvEventConfigFromJsonString(R"({
        "bind_endpoint": "tcp://127.0.0.1:15557",
        "backend_id": "worker-0",
        "emit_legacy_compat_fields": false
    })");
    ASSERT_TRUE(disabledConfig.enabled);
    ASSERT_FALSE(disabledConfig.emitLegacyCompatFields);

    nlohmann::json payloadWithoutLegacy;
    PublishStoredAndReceive(disabledConfig, payloadWithoutLegacy);
    ASSERT_FALSE(payloadWithoutLegacy.is_null());
    const auto &eventWithoutLegacy = payloadWithoutLegacy[1][0];
    ASSERT_EQ(eventWithoutLegacy["event_type"], "stored");
    ASSERT_EQ(eventWithoutLegacy["seq_hashes"][0], 42U);
    ASSERT_TRUE(eventWithoutLegacy["parent_hash"].is_null());
    ASSERT_FALSE(eventWithoutLegacy.contains("type"));
    ASSERT_FALSE(eventWithoutLegacy.contains("block_hashes"));
    ASSERT_FALSE(eventWithoutLegacy.contains("parent_block_hash"));
}

TEST_F(KvEventPublisherTest, InvalidJsonConfigDisablesPublisher)
{
    ASSERT_FALSE(BuildKvEventConfigFromJsonString("{").enabled);
    ASSERT_FALSE(BuildKvEventConfigFromJsonString("[]").enabled);
    ASSERT_FALSE(BuildKvEventConfigFromJsonString(R"({"backend_id":"worker-0"})").enabled);
    ASSERT_FALSE(BuildKvEventConfigFromJsonString(R"({
        "bind_endpoint":"tcp://127.0.0.1:15557",
        "backend_id":"worker-0",
        "queue_capacity":0
    })").enabled);
    ASSERT_FALSE(BuildKvEventConfigFromJsonString(R"({
        "bind_endpoint":"tcp://127.0.0.1:15557",
        "backend_id":"worker-0",
        "dp_rank":"bad"
    })").enabled);
}

TEST_F(KvEventPublisherTest, InvalidFieldTypeLogIncludesFieldName)
{
    testing::internal::CaptureStderr();
    auto config = BuildKvEventConfigFromJsonString(R"({
        "bind_endpoint":"tcp://127.0.0.1:15557",
        "backend_id":"worker-0",
        "dp_rank":"bad"
    })");
    auto output = testing::internal::GetCapturedStderr();

    ASSERT_FALSE(config.enabled);
    ASSERT_NE(output.find("Invalid kv_events_config field type for key 'dp_rank'"), std::string::npos) << output;
}

TEST_F(KvEventPublisherTest, ParsesTenantPrefixedPoolKey)
{
    auto parsed = KvEventPublisher::ParseKey(VllmPoolKey("2a", "tenant-a"), "default-tenant");

    ASSERT_TRUE(parsed.has_value());
    ASSERT_EQ(parsed->tenantId, "tenant-a");
    ASSERT_EQ(parsed->seqHash, 42U);
}

TEST_F(KvEventPublisherTest, StripsNormalizedSuffix)
{
    auto parsed = KvEventPublisher::ParseKey(VllmPoolKey("2a", "tenant-a") + "__0123456789abcdef",
                                             "default-tenant");

    ASSERT_TRUE(parsed.has_value());
    ASSERT_EQ(parsed->tenantId, "tenant-a");
    ASSERT_EQ(parsed->seqHash, 42U);
}

TEST_F(KvEventPublisherTest, ParsesVllmAscendPoolKey)
{
    const std::string key =
        "DeepSeek-V4-Flash-w8a8-mtp@pcp0@dcp0@head_or_tp_rank:0@pp_rank:0@group:5@cache_role:kv@"
        "cache_family:c1@434a963790185be7e82efef8b389a062df13340a2a05a15c54d98744cd9f9a381b57de29fa2eca"
        "3e5334783b505cc8d30573d6bf824c8d8ca446f9031a516919345c87";

    auto parsed = KvEventPublisher::ParseKey(key, "default-tenant");

    ASSERT_TRUE(parsed.has_value());
    ASSERT_EQ(parsed->seqHash, 0x031a516919345c87U);
}

TEST_F(KvEventPublisherTest, ParsesVllmAscendLayerPoolKey)
{
    const std::string key =
        "DeepSeek-V4-Flash-w8a8-mtp@pcp0@dcp0@head_or_tp_rank:0@group:5@cache_role:kv@cache_family:c1@"
        "434a963790185be7e82efef8b389a062df13340a2a05a15c54d98744cd9f9a381b57de29fa2eca3e5334783b"
        "505cc8d30573d6bf824c8d8ca446f9031a516919345c87@17";

    auto parsed = KvEventPublisher::ParseKey(key, "default-tenant");

    ASSERT_TRUE(parsed.has_value());
    ASSERT_EQ(parsed->seqHash, 0x031a516919345c87U);
}

TEST_F(KvEventPublisherTest, ParsesNormalizedVllmAscendPoolKey)
{
    const std::string key =
        "DeepSeek-V4-Flash-w8a8-mtp@pcp0@dcp0@head_or_tp_rank:0@pp_rank:0@group:5@cache_role:kv@"
        "cache_family:c1@434a963790185be7e82efef8b389a062df13340a2a05a15c54d98744cd9f9a381b57de29fa2eca"
        "3e5334783b505cc8d30573d6bf824c8d8ca446f9031a516919345c87__82a48dce35b581d1";

    auto parsed = KvEventPublisher::ParseKey(key, "default-tenant");

    ASSERT_TRUE(parsed.has_value());
    ASSERT_EQ(parsed->seqHash, 0x031a516919345c87U);
}

TEST_F(KvEventPublisherTest, RejectsUnparseableKeys)
{
    ASSERT_FALSE(KvEventPublisher::ParseKey("", "default-tenant").has_value());
    ASSERT_FALSE(KvEventPublisher::ParseKey("12345", "default-tenant").has_value());
    ASSERT_FALSE(KvEventPublisher::ParseKey("0x2a", "default-tenant").has_value());
    ASSERT_FALSE(KvEventPublisher::ParseKey("tenant-a$123abc", "default-tenant").has_value());
    ASSERT_FALSE(KvEventPublisher::ParseKey("tenant-a$model@0x2a", "default-tenant").has_value());
}

TEST_F(KvEventPublisherTest, DisabledPublisherIsNoOp)
{
    object_cache::KvEventConfig config;
    config.enabled = false;
    KvEventPublisher publisher(config);

    publisher.PublishStored(VllmPoolKey("2a", "tenant-a"), "memory");
    publisher.PublishRemoved(VllmPoolKey("2a", "tenant-a"), "memory");

    ASSERT_FALSE(publisher.Enabled());
    auto stats = publisher.GetStats();
    ASSERT_EQ(stats.publishedBatches, 0U);
    ASSERT_EQ(stats.publishedEvents, 0U);
    ASSERT_EQ(stats.droppedEvents, 0U);
    ASSERT_EQ(stats.skippedUnparsedKeys, 0U);
}

TEST_F(KvEventPublisherTest, NullPublisherHelpersAcceptNonOwningMedium)
{
    const std::shared_ptr<KvEventPublisher> publisher;
    constexpr char medium[] = { 'c', 'p', 'u' };
    const std::string objectKey = VllmPoolKey("2a", "tenant-a");

    PublishKvStoredEvent(publisher, objectKey, std::string_view(medium, sizeof(medium)));
    PublishKvRemovedEvent(publisher, objectKey, std::string_view(medium, sizeof(medium)));
}

TEST_F(KvEventPublisherTest, ZmqSocketDestructorClosesAndUnregistersSocket)
{
    ZmqLiteContext context;
    DS_ASSERT_OK(context.Init());
    {
        auto socket = context.CreateZmqSocket(ZmqLiteSocketType::PUB);
        ASSERT_TRUE(socket.IsValid());
        DS_ASSERT_OK(socket.SetLinger(0));
        ASSERT_EQ(context.GetOpenSocketCount(), 1U);
    }

    ASSERT_EQ(context.GetOpenSocketCount(), 0U);
    context.Close(false);
}

TEST_F(KvEventPublisherTest, ZmqSocketMoveAssignmentClosesPreviousSocket)
{
    ZmqLiteContext context;
    DS_ASSERT_OK(context.Init());
    auto target = context.CreateZmqSocket(ZmqLiteSocketType::PUB);
    auto source = context.CreateZmqSocket(ZmqLiteSocketType::SUB);
    ASSERT_TRUE(target.IsValid());
    ASSERT_TRUE(source.IsValid());
    DS_ASSERT_OK(target.SetLinger(0));
    DS_ASSERT_OK(source.SetLinger(0));
    void *sourceHandle = source.GetHandle();
    ASSERT_EQ(context.GetOpenSocketCount(), 2U);

    target = std::move(source);

    ASSERT_FALSE(source.IsValid());
    ASSERT_EQ(target.GetHandle(), sourceHandle);
    ASSERT_EQ(context.GetOpenSocketCount(), 1U);
    target = ZmqLiteSocket();
    ASSERT_EQ(context.GetOpenSocketCount(), 0U);
    context.Close(false);
}

TEST_F(KvEventPublisherTest, EvictionManagerKeepsPublisherAlive)
{
    auto objectTable = std::make_shared<object_cache::ObjectTable>();
    HostPort localAddress("127.0.0.1:18480");
    auto evictionManager = std::make_shared<WorkerOcEvictionManager>(objectTable, localAddress, localAddress,
                                                                     GetTestMetadataRoute());
    auto publisher = std::make_shared<KvEventPublisher>(object_cache::KvEventConfig{});
    std::weak_ptr<KvEventPublisher> publisherObserver = publisher;

    evictionManager->SetKvEventPublisher(publisher);
    publisher.reset();
    ASSERT_FALSE(publisherObserver.expired());

    evictionManager.reset();
    ASSERT_TRUE(publisherObserver.expired());
}

TEST_F(KvEventPublisherTest, ClearObjectPublishesRemovedCpuEvent)
{
    object_cache::KvEventConfig config;
    nlohmann::json payload;
    WithPublisherAndSubscriber(
        config,
        [](const std::shared_ptr<KvEventPublisher> &publisher) {
            auto objectTable = std::make_shared<object_cache::ObjectTable>();
            WorkerRequestManager requestManager;
            HostPort localAddress("127.0.0.1:18481");
            auto evictionManager = std::make_shared<WorkerOcEvictionManager>(
                objectTable, localAddress, localAddress, GetTestMetadataRoute());
            WorkerOcServiceCrudParam param{
                .workerMasterApiManager = nullptr,
                .workerRequestManager = requestManager,
                .memoryRefTable = nullptr,
                .objectTable = objectTable,
                .evictionManager = evictionManager,
                .workerDevOcManager = nullptr,
                .asyncPersistenceDelManager = nullptr,
                .asyncSendManager = nullptr,
                .kvEventPublisher = publisher,
                .metadataSize = 0,
                .persistenceApi = nullptr,
                .metadataRouteResolver = &GetTestMetadataRoute(),
                .endpointPolicy = nullptr,
                .exitRequested = nullptr,
                .allowDirectoryLag = false,
            };
            WorkerOcServiceCrudCommonApi crudApi(param);

            auto obj = std::make_unique<ObjCacheShmUnit>();
            obj->SetDataSize(128);
            obj->SetCreateTime(1);
            obj->SetLifeState(ObjectLifeState::OBJECT_PUBLISHED);
            obj->modeInfo.SetWriteMode(WriteMode::NONE_L2_CACHE);
            obj->stateInfo.SetDataFormat(DataFormat::BINARY);
            obj->stateInfo.SetPrimaryCopy(true);
            obj->stateInfo.SetCacheInvalid(false);
            auto objectKey = VllmPoolKey("2a", "tenant-a");
            DS_ASSERT_OK(objectTable->Insert(objectKey, std::move(obj)));

            std::shared_ptr<SafeObjType> entry;
            DS_ASSERT_OK(objectTable->GetAndLock(objectKey, entry));
            Raii unlock([&entry]() { entry->WUnlock(); });
            ObjectKV objectKV(objectKey, *entry);
            DS_ASSERT_OK(crudApi.ClearObject(objectKV));
        },
        payload, true);

    ASSERT_FALSE(payload.is_null());
    ASSERT_TRUE(payload.is_array());
    ASSERT_EQ(payload.size(), 3U);
    ASSERT_TRUE(payload[1].is_array());
    ASSERT_GE(payload[1].size(), 1U);
    const auto &event = payload[1][0];
    ASSERT_EQ(event["event_type"], "removed");
    ASSERT_EQ(event["type"], "BlockRemoved");
    ASSERT_EQ(event["tenant_id"], "tenant-a");
    ASSERT_EQ(event["backend_id"], "worker-0");
    ASSERT_EQ(event["medium"], "cpu");
    ASSERT_EQ(event["dp_rank"], 2U);
    ASSERT_EQ(event["seq_hashes"][0], 42U);
    ASSERT_EQ(event["block_hashes"][0], 42U);
}

TEST_F(KvEventPublisherTest, CrudApiPublishesStoredCpuEvent)
{
    object_cache::KvEventConfig config;
    nlohmann::json payload;
    WithPublisherAndSubscriber(
        config,
        [](const std::shared_ptr<KvEventPublisher> &publisher) {
            auto objectTable = std::make_shared<object_cache::ObjectTable>();
            WorkerRequestManager requestManager;
            HostPort localAddress("127.0.0.1:18482");
            auto evictionManager = std::make_shared<WorkerOcEvictionManager>(
                objectTable, localAddress, localAddress, GetTestMetadataRoute());
            WorkerOcServiceCrudParam param{
                .workerMasterApiManager = nullptr,
                .workerRequestManager = requestManager,
                .memoryRefTable = nullptr,
                .objectTable = objectTable,
                .evictionManager = evictionManager,
                .workerDevOcManager = nullptr,
                .asyncPersistenceDelManager = nullptr,
                .asyncSendManager = nullptr,
                .kvEventPublisher = publisher,
                .metadataSize = 0,
                .persistenceApi = nullptr,
                .metadataRouteResolver = &GetTestMetadataRoute(),
                .endpointPolicy = nullptr,
                .exitRequested = nullptr,
                .allowDirectoryLag = false,
            };
            TestCrudApi crudApi(param);
            crudApi.PublishStoredForTest(VllmPoolKey("2a", "tenant-a"), kKvEventMediumCpu);
        },
        payload, true);

    ASSERT_FALSE(payload.is_null());
    ASSERT_TRUE(payload.is_array());
    ASSERT_EQ(payload.size(), 3U);
    ASSERT_TRUE(payload[1].is_array());
    ASSERT_GE(payload[1].size(), 1U);
    const auto &event = payload[1][0];
    ASSERT_EQ(event["event_type"], "stored");
    ASSERT_EQ(event["type"], "BlockStored");
    ASSERT_EQ(event["tenant_id"], "tenant-a");
    ASSERT_EQ(event["backend_id"], "worker-0");
    ASSERT_EQ(event["medium"], "cpu");
    ASSERT_EQ(event["dp_rank"], 2U);
    ASSERT_EQ(event["seq_hashes"][0], 42U);
    ASSERT_EQ(event["block_hashes"][0], 42U);
}

TEST_F(KvEventPublisherTest, MultiPublishUpdatePublishesStoredCpuEventForEachKey)
{
    object_cache::KvEventConfig config;
    nlohmann::json payload;
    WithPublisherAndSubscriber(
        config,
        [](const std::shared_ptr<KvEventPublisher> &publisher) {
            auto objectTable = std::make_shared<object_cache::ObjectTable>();
            WorkerRequestManager requestManager;
            HostPort localAddress("127.0.0.1:18483");
            auto evictionManager = std::make_shared<WorkerOcEvictionManager>(
                objectTable, localAddress, localAddress, GetTestMetadataRoute());
            auto globalRefTable = std::make_shared<ObjectGlobalRefTable<ClientKey>>();
            auto akSkManager = std::make_shared<AkSkManager>(0);
            DS_ASSERT_OK(evictionManager->Init(globalRefTable, akSkManager));
            WorkerOcServiceCrudParam param{
                .workerMasterApiManager = nullptr,
                .workerRequestManager = requestManager,
                .memoryRefTable = nullptr,
                .objectTable = objectTable,
                .evictionManager = evictionManager,
                .workerDevOcManager = nullptr,
                .asyncPersistenceDelManager = nullptr,
                .asyncSendManager = nullptr,
                .kvEventPublisher = publisher,
                .metadataSize = 0,
                .persistenceApi = nullptr,
                .metadataRouteResolver = &GetTestMetadataRoute(),
                .endpointPolicy = nullptr,
                .exitRequested = nullptr,
                .allowDirectoryLag = false,
            };
            auto memCpyThreadPool = std::make_shared<ThreadPool>(1);
            auto notifyThreadPool = std::make_shared<ThreadPool>(1);
            WorkerOcServiceMultiPublishImpl multiPublish(param, memCpyThreadPool, notifyThreadPool, akSkManager,
                                                         localAddress);

            const std::vector<std::string> objectKeys{
                VllmPoolKey("2a", "tenant-a"),
                VllmPoolKey("2b", "tenant-a"),
            };
            std::vector<std::shared_ptr<SafeObjType>> entries;
            entries.reserve(objectKeys.size());
            for (const auto &objectKey : objectKeys) {
                auto obj = std::make_unique<ObjCacheShmUnit>();
                obj->SetDataSize(128);
                obj->SetLifeState(ObjectLifeState::OBJECT_INVALID);
                obj->modeInfo.SetWriteMode(WriteMode::NONE_L2_CACHE);
                obj->stateInfo.SetDataFormat(DataFormat::BINARY);
                obj->stateInfo.SetCacheInvalid(true);
                DS_ASSERT_OK(objectTable->Insert(objectKey, std::move(obj)));

                std::shared_ptr<SafeObjType> entry;
                DS_ASSERT_OK(objectTable->Get(objectKey, entry));
                entries.emplace_back(std::move(entry));
            }

            const std::vector<uint64_t> versions{ 100, 101 };
            multiPublish.UpdateObjectAfterCreatingMetaForTest(objectKeys, entries, versions, 0);
            for (int i = 0; i < K_TEST_PUBLISH_RETRIES
                 && (notifyThreadPool->GetWaitingTasksNum() != 0 || notifyThreadPool->GetRunningTasksNum() != 0);
                 ++i) {
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }
        },
        payload, true, 2);

    ASSERT_FALSE(payload.is_null());
    ASSERT_TRUE(payload.is_array());
    ASSERT_EQ(payload.size(), 3U);
    ASSERT_TRUE(payload[1].is_array());
    ASSERT_EQ(payload[1].size(), 2U);
    std::set<uint64_t> seqHashes;
    for (const auto &event : payload[1]) {
        ASSERT_EQ(event["event_type"], "stored");
        ASSERT_EQ(event["type"], "BlockStored");
        ASSERT_EQ(event["tenant_id"], "tenant-a");
        ASSERT_EQ(event["backend_id"], "worker-0");
        ASSERT_EQ(event["medium"], "cpu");
        ASSERT_EQ(event["dp_rank"], 2U);
        seqHashes.emplace(event["seq_hashes"][0].get<uint64_t>());
        ASSERT_EQ(event["seq_hashes"][0], event["block_hashes"][0]);
    }
    ASSERT_EQ(seqHashes, (std::set<uint64_t>{ 42U, 43U }));
}

TEST_F(KvEventPublisherTest, EvictionRemovalPathsPublishRemovedCpuEvents)
{
    object_cache::KvEventConfig config;
    nlohmann::json payload;
    WithPublisherAndSubscriber(
        config,
        [](const std::shared_ptr<KvEventPublisher> &publisher) {
            auto objectTable = std::make_shared<object_cache::ObjectTable>();
            HostPort localAddress("127.0.0.1:18484");
            auto evictionManager = std::make_shared<WorkerOcEvictionManager>(
                objectTable, localAddress, localAddress, GetTestMetadataRoute());
            evictionManager->SetKvEventPublisher(publisher);

            const std::vector<std::string> objectKeys{
                VllmPoolKey("2a", "tenant-a"),
                VllmPoolKey("2b", "tenant-a"),
                VllmPoolKey("2c", "tenant-a"),
            };
            for (const auto &objectKey : objectKeys) {
                auto obj = std::make_unique<ObjCacheShmUnit>();
                obj->SetDataSize(128);
                obj->SetCreateTime(1);
                obj->SetLifeState(ObjectLifeState::OBJECT_PUBLISHED);
                obj->modeInfo.SetWriteMode(WriteMode::NONE_L2_CACHE);
                obj->stateInfo.SetDataFormat(DataFormat::BINARY);
                obj->stateInfo.SetPrimaryCopy(true);
                obj->stateInfo.SetCacheInvalid(false);
                DS_ASSERT_OK(objectTable->Insert(objectKey, std::move(obj)));
            }

            std::shared_ptr<SafeObjType> deleteEntry;
            DS_ASSERT_OK(objectTable->GetAndLock(objectKeys[0], deleteEntry));
            Raii deleteUnlock([&deleteEntry]() { deleteEntry->WUnlock(); });
            ObjectKV deleteObjectKV(objectKeys[0], *deleteEntry);
            DS_ASSERT_OK(evictionManager->EvictDeleteObjectForTest(deleteObjectKV));

            std::shared_ptr<SafeObjType> freeEntry;
            DS_ASSERT_OK(objectTable->GetAndLock(objectKeys[1], freeEntry));
            Raii freeUnlock([&freeEntry]() { freeEntry->WUnlock(); });
            ObjectKV freeObjectKV(objectKeys[1], *freeEntry);
            DS_ASSERT_OK(evictionManager->EvictFreeMemoryForTest(freeObjectKV));

            std::shared_ptr<SafeObjType> endLifeEntry;
            DS_ASSERT_OK(objectTable->GetAndLock(objectKeys[2], endLifeEntry));
            Raii endLifeUnlock([&endLifeEntry]() { endLifeEntry->WUnlock(); });
            DS_ASSERT_OK(evictionManager->DeletePrimaryEndLifeLocalForTest(objectKeys[2], endLifeEntry));
            ASSERT_FALSE(objectTable->Contains(objectKeys[2]));
        },
        payload, true, 3);

    ASSERT_FALSE(payload.is_null());
    ASSERT_TRUE(payload.is_array());
    ASSERT_EQ(payload.size(), 3U);
    ASSERT_TRUE(payload[1].is_array());
    ASSERT_EQ(payload[1].size(), 3U);
    std::set<uint64_t> seqHashes;
    for (const auto &event : payload[1]) {
        ASSERT_EQ(event["event_type"], "removed");
        ASSERT_EQ(event["type"], "BlockRemoved");
        ASSERT_EQ(event["tenant_id"], "tenant-a");
        ASSERT_EQ(event["backend_id"], "worker-0");
        ASSERT_EQ(event["medium"], "cpu");
        ASSERT_EQ(event["dp_rank"], 2U);
        seqHashes.emplace(event["seq_hashes"][0].get<uint64_t>());
        ASSERT_EQ(event["seq_hashes"][0], event["block_hashes"][0]);
    }
    ASSERT_EQ(seqHashes, (std::set<uint64_t>{ 42U, 43U, 44U }));
}

TEST_F(KvEventPublisherTest, PublishesStoredEventOverZmq)
{
    object_cache::KvEventConfig config;
    nlohmann::json payload;
    PublishStoredAndReceive(config, payload);

    ASSERT_FALSE(payload.is_null());
    ASSERT_TRUE(payload.is_array());
    ASSERT_EQ(payload.size(), 3U);
    ASSERT_TRUE(payload[1].is_array());
    ASSERT_GE(payload[1].size(), 1U);
    const auto &event = payload[1][0];
    ASSERT_EQ(event["event_type"], "stored");
    ASSERT_EQ(event["type"], "BlockStored");
    ASSERT_EQ(event["tenant_id"], "tenant-a");
    ASSERT_EQ(event["backend_id"], "worker-0");
    ASSERT_EQ(event["medium"], "memory");
    ASSERT_EQ(event["dp_rank"], 2U);
    ASSERT_EQ(event["seq_hashes"][0], 42U);
    ASSERT_EQ(event["block_hashes"][0], 42U);
    ASSERT_TRUE(event["parent_hash"].is_null());
    ASSERT_TRUE(event["parent_block_hash"].is_null());
}

TEST_F(KvEventPublisherTest, TakeBatchAllocationFailureDoesNotTerminateWorker)
{
    constexpr char injectPoint[] = "KvEventPublisher.TakeBatch.beforeMove";
    DS_ASSERT_OK(inject::Set(injectPoint, "1*call()"));
    Raii clearInjectPoint([&] { (void)inject::Clear(injectPoint); });

    object_cache::KvEventConfig config;
    nlohmann::json payload;
    PublishStoredAndReceive(config, payload);

    ASSERT_FALSE(payload.is_null());
    ASSERT_EQ(payload[1].size(), 1U);
    ASSERT_EQ(payload[1][0]["seq_hashes"][0], 42U);
    ASSERT_EQ(inject::GetExecuteCount(injectPoint), 1U);
}
}  // namespace
}  // namespace ut
}  // namespace datasystem
