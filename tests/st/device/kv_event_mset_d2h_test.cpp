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
 * Description: End-to-end test for MSetD2H KV event publishing.
 */
#include "device/dev_test_helper.h"

#include <chrono>
#include <nlohmann/json.hpp>
#include <set>
#include <thread>
#include <unistd.h>

#include "datasystem/common/util/strings_util.h"
#include "datasystem/worker/object_cache/kv_event/zmq_lite.h"

namespace datasystem {
using namespace acl;
namespace st {
namespace {
constexpr int KV_EVENT_RECV_TIMEOUT_MS = 200;
constexpr int KV_EVENT_RECV_RETRIES = 30;
constexpr size_t MB = 1024 * 1024;

std::string IpcPath(const std::string &endpoint)
{
    const std::string prefix = "ipc://";
    return endpoint.substr(prefix.size());
}

std::string VllmPoolKey(const std::string &chunkHash)
{
    return "Qwen3@pcp0@dcp0@head_or_tp_rank:0@pp_rank:0@group:0@cache_role:kv@cache_family:c1@" + chunkHash;
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

bool RecvKvEventBatch(ZmqLiteSocket &subscriber, nlohmann::json &payload)
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
    LOG(INFO) << "Received KV event batch, sequence: " << ReadBigEndianUint64(seqFrame)
              << ", payload: " << payload.dump();
    return true;
}
}  // namespace

class KvEventMSetD2HTest : public DevTestHelper {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = 1;
        opts.workerGflagParams =
            " -v=1 -authorization_enable=true -shared_memory_size_mb=4096 -enable_fallocate=false "
            "-arena_per_tenant=2 -kv_events_config={\"bind_endpoint\":\""
            + endpoint_ + "\",\"backend_id\":\"worker-0\",\"tenant_id\":\"default\",\"dp_rank\":0,"
                          "\"queue_capacity\":1024}";
        opts.enableDistributedMaster = "false";
        opts.numEtcd = 1;
        FLAGS_v = 1;
    }

    void SetUp() override
    {
        const char *ascendRoot = std::getenv("ASCEND_HOME_PATH");
        if (ascendRoot == nullptr) {
            DS_ASSERT_OK(datasystem::inject::Set("NO_USE_FFTS", "call()"));
            DS_ASSERT_OK(datasystem::inject::Set("client.GetOrCreateHcclComm.setIsSameNode", "call(0)"));
            BINEXPECT_CALL(AclDeviceManager::Instance, ()).WillRepeatedly(Return(&managerMock_));
        }
        InitSubscriber();
        ExternalClusterTest::SetUp();
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    void TearDown() override
    {
        if (subscriber_.IsValid()) {
            subscriber_ = ZmqLiteSocket();
        }
        subscriberContext_.Close(false);
        (void)unlink(IpcPath(endpoint_).c_str());
        ExternalClusterTest::TearDown();
    }

protected:
    void InitSubscriber()
    {
        (void)unlink(IpcPath(endpoint_).c_str());
        DS_ASSERT_OK(subscriberContext_.Init());
        subscriber_ = subscriberContext_.CreateZmqSocket(ZmqLiteSocketType::SUB);
        ASSERT_TRUE(subscriber_.IsValid());
        DS_ASSERT_OK(subscriber_.SetLinger(0));
        DS_ASSERT_OK(subscriber_.SetRcvtimeo(KV_EVENT_RECV_TIMEOUT_MS));
        DS_ASSERT_OK(subscriber_.SubscribeAll());
        DS_ASSERT_OK(subscriber_.Connect(endpoint_));
    }

    bool WaitCpuEvents(const std::string &eventType, const std::string &legacyType,
                       const std::set<uint64_t> &expectedSeqHashes)
    {
        std::set<uint64_t> receivedSeqHashes;
        for (int i = 0; i < KV_EVENT_RECV_RETRIES && receivedSeqHashes != expectedSeqHashes; ++i) {
            nlohmann::json payload;
            if (!RecvKvEventBatch(subscriber_, payload)) {
                continue;
            }
            if (!payload.is_array() || payload.size() != 3U || !payload[1].is_array()) {
                continue;
            }
            for (const auto &event : payload[1]) {
                if (event.value("event_type", "") != eventType || event.value("medium", "") != "cpu") {
                    continue;
                }
                EXPECT_EQ(event["type"], legacyType);
                EXPECT_EQ(event["backend_id"], "worker-0");
                auto seqHash = event["seq_hashes"][0].get<uint64_t>();
                LOG(INFO) << "Matched " << eventType << " KV event, seq_hash: " << seqHash
                          << ", current received seq_hashes: " << VectorToString(receivedSeqHashes);
                receivedSeqHashes.emplace(seqHash);
            }
        }
        return receivedSeqHashes == expectedSeqHashes;
    }

    bool WaitAnyCpuEvent(const std::string &eventType, const std::string &legacyType,
                         const std::set<uint64_t> &candidateSeqHashes)
    {
        for (int i = 0; i < KV_EVENT_RECV_RETRIES; ++i) {
            nlohmann::json payload;
            if (!RecvKvEventBatch(subscriber_, payload)) {
                continue;
            }
            if (!payload.is_array() || payload.size() != 3U || !payload[1].is_array()) {
                continue;
            }
            for (const auto &event : payload[1]) {
                if (event.value("event_type", "") != eventType || event.value("medium", "") != "cpu") {
                    continue;
                }
                EXPECT_EQ(event["type"], legacyType);
                EXPECT_EQ(event["backend_id"], "worker-0");
                auto seqHash = event["seq_hashes"][0].get<uint64_t>();
                LOG(INFO) << "Matched " << eventType << " KV event, seq_hash: " << seqHash;
                if (candidateSeqHashes.count(seqHash) > 0) {
                    return true;
                }
            }
        }
        return false;
    }

    bool WaitStoredCpuEvents(const std::set<uint64_t> &expectedSeqHashes)
    {
        return WaitCpuEvents("stored", "BlockStored", expectedSeqHashes);
    }

    bool WaitRemovedCpuEvents(const std::set<uint64_t> &expectedSeqHashes)
    {
        return WaitCpuEvents("removed", "BlockRemoved", expectedSeqHashes);
    }

    bool WaitAnyRemovedCpuEvent(const std::set<uint64_t> &candidateSeqHashes)
    {
        return WaitAnyCpuEvent("removed", "BlockRemoved", candidateSeqHashes);
    }

    const std::string endpoint_ =
        "ipc:///tmp/yuanrong_kv_event_mset_d2h_e2e_" + std::to_string(getpid()) + "_" + GetStringUuid();
    ZmqLiteContext subscriberContext_;
    ZmqLiteSocket subscriber_;
};

class KvEventEvictionTest : public KvEventMSetD2HTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = 1;
        opts.workerGflagParams =
            " -v=1 -authorization_enable=true -shared_memory_size_mb=128 -eviction_reserve_mem_threshold_mb=100 "
            "-enable_fallocate=false -arena_per_tenant=1 -kv_events_config={\"bind_endpoint\":\""
            + endpoint_ + "\",\"backend_id\":\"worker-0\",\"tenant_id\":\"default\",\"dp_rank\":0,"
                          "\"queue_capacity\":1024}";
        opts.injectActions = "evictAction.setDelete:call()";
        opts.enableDistributedMaster = "false";
        opts.numEtcd = 1;
        FLAGS_v = 1;
    }
};

TEST_F(KvEventMSetD2HTest, MSetD2HPublishesStoredCpuEvents)
{
    InitAcl(0);
    std::shared_ptr<HeteroClient> client;
    InitTestHeteroClient(0, client);

    const size_t numOfObjs = 2;
    const size_t blksPerObj = 1;
    const size_t blkSz = 1024;
    std::vector<std::string> objectKeys{ VllmPoolKey("2a"), VllmPoolKey("2b") };
    std::vector<DeviceBlobList> devGetBlobList;
    std::vector<DeviceBlobList> devSetBlobList;
    PrePareDevData(numOfObjs, blksPerObj, blkSz, devGetBlobList, devSetBlobList, 0);

    DS_ASSERT_OK(client->MSetD2H(objectKeys, devSetBlobList));

    ASSERT_TRUE(WaitStoredCpuEvents({ 42U, 43U })) << "Failed to receive MSetD2H stored/cpu KV events.";
}

TEST_F(KvEventMSetD2HTest, DeleteAfterMSetD2HPublishesRemovedCpuEvents)
{
    InitAcl(0);
    std::shared_ptr<HeteroClient> client;
    InitTestHeteroClient(0, client);

    const size_t numOfObjs = 2;
    const size_t blksPerObj = 1;
    const size_t blkSz = 1024;
    std::vector<std::string> objectKeys{ VllmPoolKey("2d"), VllmPoolKey("2e") };
    std::vector<DeviceBlobList> devGetBlobList;
    std::vector<DeviceBlobList> devSetBlobList;
    PrePareDevData(numOfObjs, blksPerObj, blkSz, devGetBlobList, devSetBlobList, 0);

    DS_ASSERT_OK(client->MSetD2H(objectKeys, devSetBlobList));
    std::vector<std::string> failedKeys;
    DS_ASSERT_OK(client->Delete(objectKeys, failedKeys));
    ASSERT_TRUE(failedKeys.empty());

    ASSERT_TRUE(WaitRemovedCpuEvents({ 45U, 46U })) << "Failed to receive MSetD2H delete removed/cpu KV events.";
}

TEST_F(KvEventEvictionTest, DISABLED_EvictionPublishesRemovedCpuEvent)
{
    std::shared_ptr<ObjectClient> client;
    InitTestClient(0, client);

    std::vector<uint8_t> firstData(70 * MB, 'a');
    CreateParam param;
    DS_ASSERT_OK(client->Put(VllmPoolKey("3c"), firstData.data(), firstData.size(), param));
    std::this_thread::sleep_for(std::chrono::seconds(1));

    std::vector<uint8_t> secondData(50 * MB, 'b');
    DS_ASSERT_OK(client->Put(VllmPoolKey("3d"), secondData.data(), secondData.size(), param));

    ASSERT_TRUE(WaitAnyRemovedCpuEvent({ 60U })) << "Failed to receive eviction removed/cpu KV event.";
}
}  // namespace st
}  // namespace datasystem
