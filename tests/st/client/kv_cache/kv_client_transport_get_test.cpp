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

/** Description: Tests KVClient reads through the transport layer (enableLocalCache=false). */

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <future>
#include <map>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "client/object_cache/oc_client_common.h"
#include "common_distributed_ext.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/kvstore/etcd/etcd_store.h"
#include "datasystem/common/log/access_recorder.h"
#include "datasystem/common/util/hash_algorithm.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/thread_pool.h"
#include "datasystem/kv_client.h"
#include "datasystem/protos/cluster_topology.pb.h"

DS_DECLARE_bool(use_brpc);

namespace datasystem {
namespace st {
namespace {
constexpr uint32_t META_OWNER_INDEX = 0;
constexpr uint32_t TRANSPORT_CLIENT_WORKER_INDEX = 1;
constexpr int32_t CLIENT_TIMEOUT_MS = 3'000;
constexpr size_t VALUE_SIZE = 128 * 1024;
constexpr size_t INLINE_DATA_LIMIT = 512 * 1024;
constexpr size_t LARGE_VALUE_SIZE = 8 * 1024 * 1024;
constexpr size_t KEY_SEARCH_LIMIT = 100'000;
constexpr char REAL_ROUTE_KEY_PREFIX[] = "transport_real_route_";
constexpr char UB_GET_SIZE_ENV[] = "DATASYSTEM_UB_GET_DATA_SIZE_BYTES";
constexpr char SKIP_WARMUP_INJECT[] = "ObjectClientImpl.ClientWorkerWarmup.skip";
constexpr char QUERY_AND_GET_INJECT[] = "client.transport.query_and_get";
constexpr char GET_OBJECT_REMOTE_INJECT[] = "client.transport.get_object_remote";
constexpr char BATCH_GET_OBJECT_REMOTE_INJECT[] = "client.transport.batch_get_object_remote";
constexpr char INLINE_READ_FAILURE_INJECT[] = "worker.worker_worker_remote_get_failure";
constexpr char SHM_LATCH_FAIL_INJECT[] = "worker.ShmGuard.TryRLatch.Fail";

struct TransportRpcCounts {
    uint64_t queryAndGet = 0;
    uint64_t getObjectRemote = 0;
};

const char *ExpectedTransport()
{
#ifdef USE_URMA
    return "UB";
#else
    return "TCP";
#endif
}
}  // namespace

class KVClientTransportGetTest : public OCClientCommon, public CommonDistributedExt {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        FLAGS_v = 1;
        opts.numEtcd = 1;
        opts.numWorkers = 3;
        opts.enableDistributedMaster = "true";
        opts.workerGflagParams =
            " -shared_memory_size_mb=512 -ipc_through_shared_memory=false -arena_per_tenant=1 -use_brpc=true";
#ifdef USE_URMA
        opts.workerGflagParams += " -enable_urma=true -enable_transport_fallback=false";
#else
        opts.workerGflagParams += " -enable_urma=false";
#endif
        opts.injectActions = "worker.batch_get_failure_for_keys:call()";
    }

    void SetUp() override
    {
        const char *ubGetSize = std::getenv(UB_GET_SIZE_ENV);
        if (ubGetSize != nullptr) {
            hadPreviousUbGetSize_ = true;
            previousUbGetSize_ = ubGetSize;
        }
        previousUseBrpc_ = FLAGS_use_brpc;
        FLAGS_use_brpc = true;
        DS_ASSERT_OK(inject::Set(SKIP_WARMUP_INJECT, "call()"));
        DS_ASSERT_OK(inject::Set(QUERY_AND_GET_INJECT, "call()"));
        DS_ASSERT_OK(inject::Set(GET_OBJECT_REMOTE_INJECT, "call()"));
        ExternalClusterTest::SetUp();
        CommonDistributedExt::InitTestEtcdInstance();

        etcd_ = OCClientCommon::InitTestEtcdInstance();
        ASSERT_NE(etcd_, nullptr);
        InitTestKVClient(META_OWNER_INDEX, writer_, CLIENT_TIMEOUT_MS);
#ifdef USE_URMA
        SetUbGetSize(INLINE_DATA_LIMIT);
#endif
        InitTransportClient();
    }

    void TearDown() override
    {
        reader_.reset();
        writer_.reset();
        etcd_.reset();
        CommonDistributedExt::etcd_.reset();
        RestoreUbGetSize();
        (void)inject::Clear(SKIP_WARMUP_INJECT);
        (void)inject::Clear(QUERY_AND_GET_INJECT);
        (void)inject::Clear(GET_OBJECT_REMOTE_INJECT);
        ExternalClusterTest::TearDown();
        FLAGS_use_brpc = previousUseBrpc_;
    }

protected:
    BaseCluster *GetCluster() override
    {
        return cluster_.get();
    }

    // enableLocalCache=false reader: the client under test, exercising TransportLayer::Get.
    void InitTransportClient()
    {
        ConnectOptions options;
        InitConnectOpt(TRANSPORT_CLIENT_WORKER_INDEX, options, CLIENT_TIMEOUT_MS);
        options.enableLocalCache = false;
        reader_ = std::make_shared<KVClient>(options);
        DS_ASSERT_OK(reader_->Init());
    }

    void SetUbGetSize(size_t size)
    {
        ASSERT_EQ(setenv(UB_GET_SIZE_ENV, std::to_string(size).c_str(), 1), 0);
    }

    void RestoreUbGetSize()
    {
        if (hadPreviousUbGetSize_) {
            (void)setenv(UB_GET_SIZE_ENV, previousUbGetSize_.c_str(), 1);
        } else {
            (void)unsetenv(UB_GET_SIZE_ENV);
        }
    }

    void GetRealHashKeysToWorker(uint32_t workerIndex, size_t keyCount, std::vector<std::string> &keys)
    {
        ASSERT_NE(etcd_, nullptr);
        std::string value;
        DS_ASSERT_OK(etcd_->Get(GetTopologyTableName(), "", value));
        ClusterTopologyPb ring;
        ASSERT_TRUE(ring.ParseFromString(value));

        HostPort targetWorker;
        DS_ASSERT_OK(cluster_->GetWorkerAddr(workerIndex, targetWorker));
        ASSERT_NE(ring.members().find(targetWorker.ToString()), ring.members().end());
        std::map<uint32_t, std::string> tokenWorkers;
        for (const auto &worker : ring.members()) {
            for (const auto token : worker.second.tokens()) {
                tokenWorkers.emplace(token, worker.first);
            }
        }
        ASSERT_FALSE(tokenWorkers.empty());

        keys.clear();
        for (size_t candidateIndex = 0; candidateIndex < KEY_SEARCH_LIMIT && keys.size() < keyCount; ++candidateIndex) {
            std::string candidate =
                REAL_ROUTE_KEY_PREFIX + std::to_string(workerIndex) + "_" + std::to_string(candidateIndex);
            auto owner = tokenWorkers.upper_bound(MurmurHash3_32(candidate));
            if (owner == tokenWorkers.end()) {
                owner = tokenWorkers.begin();
            }
            if (owner->second == targetWorker.ToString()) {
                keys.emplace_back(std::move(candidate));
            }
        }
        ASSERT_EQ(keys.size(), keyCount);
    }

    void GetRpcCounts(TransportRpcCounts &counts)
    {
        counts.queryAndGet = inject::GetExecuteCount(QUERY_AND_GET_INJECT);
        counts.getObjectRemote = inject::GetExecuteCount(GET_OBJECT_REMOTE_INJECT);
    }

    // Generate N distinct keys without making placement assumptions.
    std::vector<std::string> MakeRandomKeys(size_t count)
    {
        std::vector<std::string> keys;
        keys.reserve(count);
        for (size_t i = 0; i < count; ++i) {
            keys.emplace_back("transport_get_" + std::to_string(i) + "_" + GetStringUuid());
        }
        return keys;
    }

    std::vector<std::string> MakeKeysAcrossMetaOwners(size_t countPerOwner)
    {
        constexpr size_t MAX_CANDIDATES = 10'000;
        const size_t workerCount = cluster_->GetWorkerNum();
        std::vector<std::vector<std::string>> keysByOwner(workerCount);
        size_t selected = 0;
        for (size_t i = 0; i < MAX_CANDIDATES && selected < workerCount * countPerOwner; ++i) {
            std::string key = "transport_get_owner_" + std::to_string(i) + "_" + GetStringUuid();
            WorkerEntry owner;
            GetMetaLocationById(key, { 0, 1, 2 }, owner);
            if (owner.index < 0 || static_cast<size_t>(owner.index) >= workerCount) {
                ADD_FAILURE() << "invalid metadata owner index " << owner.index;
                return {};
            }
            auto &ownerKeys = keysByOwner[owner.index];
            if (ownerKeys.size() < countPerOwner) {
                ownerKeys.emplace_back(std::move(key));
                ++selected;
            }
        }
        EXPECT_EQ(selected, workerCount * countPerOwner);

        std::vector<std::string> keys;
        keys.reserve(selected);
        for (size_t position = 0; position < countPerOwner; ++position) {
            for (auto &ownerKeys : keysByOwner) {
                if (position < ownerKeys.size()) {
                    keys.emplace_back(std::move(ownerKeys[position]));
                }
            }
        }
        return keys;
    }

    std::unique_ptr<EtcdStore> etcd_;
    std::shared_ptr<KVClient> writer_;
    std::shared_ptr<KVClient> reader_;
    bool hadPreviousUbGetSize_ = false;
    std::string previousUbGetSize_;
    bool previousUseBrpc_ = false;
};

TEST_F(KVClientTransportGetTest, InlineHitSkipsSecondPhase)
{
    std::vector<std::string> keys;
    GetRealHashKeysToWorker(META_OWNER_INDEX, 1, keys);
    ASSERT_EQ(keys.size(), 1u);
    const std::string &key = keys.front();
    const std::string value(INLINE_DATA_LIMIT, 's');
    DS_ASSERT_OK(writer_->Set(key, value));

    TransportRpcCounts before;
    GetRpcCounts(before);
    Optional<Buffer> buffer;
    DS_ASSERT_OK(reader_->Get(key, buffer));
    TransportRpcCounts after;
    GetRpcCounts(after);

    ASSERT_TRUE(buffer);
    AssertBufferEqual(*buffer, value);
    ASSERT_EQ(AccessTransportTracker::ToString(), ExpectedTransport());
    ASSERT_EQ(after.queryAndGet, before.queryAndGet + 1);
    ASSERT_EQ(after.getObjectRemote, before.getObjectRemote);
}

// Multi-key batch returns every value in input order.
TEST_F(KVClientTransportGetTest, MultiKeyGetSameOwner)
{
    std::vector<std::string> keys;
    GetRealHashKeysToWorker(META_OWNER_INDEX, 3, keys);
    ASSERT_EQ(keys.size(), 3u);
    std::vector<std::string> values;
    for (size_t i = 0; i < keys.size(); ++i) {
        values.emplace_back(VALUE_SIZE + i * 1024, 'a' + static_cast<char>(i));
        DS_ASSERT_OK(writer_->Set(keys[i], values[i]));
    }

    std::vector<Optional<Buffer>> buffers;
    DS_ASSERT_OK(reader_->Get(keys, buffers));

    ASSERT_EQ(buffers.size(), keys.size());
    for (size_t i = 0; i < buffers.size(); ++i) {
        ASSERT_TRUE(buffers[i]);
        AssertBufferEqual(*buffers[i], values[i]);
    }
    ASSERT_EQ(AccessTransportTracker::ToString(), ExpectedTransport());
}

// Keys spanning multiple meta owners still return values in input order.
TEST_F(KVClientTransportGetTest, MultiKeyGetDifferentOwners)
{
    std::vector<std::string> localOwnerKeys;
    std::vector<std::string> remoteOwnerKeys;
    GetRealHashKeysToWorker(META_OWNER_INDEX, 4, localOwnerKeys);
    GetRealHashKeysToWorker(TRANSPORT_CLIENT_WORKER_INDEX, 4, remoteOwnerKeys);
    ASSERT_EQ(localOwnerKeys.size(), 4u);
    ASSERT_EQ(remoteOwnerKeys.size(), 4u);
    std::vector<std::string> keys;
    keys.reserve(localOwnerKeys.size() + remoteOwnerKeys.size());
    for (size_t i = 0; i < localOwnerKeys.size(); ++i) {
        keys.emplace_back(localOwnerKeys[i]);
        keys.emplace_back(remoteOwnerKeys[i]);
    }
    std::vector<std::string> values;
    for (size_t i = 0; i < keys.size(); ++i) {
        values.emplace_back(VALUE_SIZE, 'a' + static_cast<char>(i % 26));
        DS_ASSERT_OK(writer_->Set(keys[i], values[i]));
    }

    std::vector<Optional<Buffer>> buffers;
    DS_ASSERT_OK(reader_->Get(keys, buffers));

    ASSERT_EQ(buffers.size(), keys.size());
    for (size_t i = 0; i < buffers.size(); ++i) {
        ASSERT_TRUE(buffers[i]) << "missing buffer at position " << i;
        AssertBufferEqual(*buffers[i], values[i]);
    }
    ASSERT_EQ(AccessTransportTracker::ToString(), ExpectedTransport());
}

// Absent object yields empty locations -> K_NOT_FOUND, no data fetch.
TEST_F(KVClientTransportGetTest, ObjectNotFound)
{
    std::vector<std::string> keys;
    GetRealHashKeysToWorker(META_OWNER_INDEX, 1, keys);
    ASSERT_EQ(keys.size(), 1u);
    Optional<Buffer> buffer;
    ASSERT_EQ(reader_->Get(keys.front(), buffer).GetCode(), StatusCode::K_NOT_FOUND);
    ASSERT_FALSE(buffer);
}

TEST_F(KVClientTransportGetTest, NonLocalMetaOwnerFallsBack)
{
    std::vector<std::string> keys;
    GetRealHashKeysToWorker(TRANSPORT_CLIENT_WORKER_INDEX, 1, keys);
    ASSERT_EQ(keys.size(), 1u);
    const std::string &key = keys.front();
    const std::string value(VALUE_SIZE, 'n');
    DS_ASSERT_OK(writer_->Set(key, value));

    TransportRpcCounts before;
    GetRpcCounts(before);
    Optional<Buffer> buffer;
    DS_ASSERT_OK(reader_->Get(key, buffer));
    TransportRpcCounts after;
    GetRpcCounts(after);

    ASSERT_TRUE(buffer);
    AssertBufferEqual(*buffer, value);
    ASSERT_EQ(AccessTransportTracker::ToString(), ExpectedTransport());
    ASSERT_EQ(after.queryAndGet, before.queryAndGet + 1);
    ASSERT_EQ(after.getObjectRemote, before.getObjectRemote + 1);
}

class KVClientTransportGetInlineFailureTest : public KVClientTransportGetTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        KVClientTransportGetTest::SetClusterSetupOptions(opts);
        opts.injectActions += ";" + std::string(INLINE_READ_FAILURE_INJECT) + ":1*return(K_RUNTIME_ERROR)";
    }
};

TEST_F(KVClientTransportGetInlineFailureTest, InlineReadFailureFallsBack)
{
    std::vector<std::string> keys;
    GetRealHashKeysToWorker(META_OWNER_INDEX, 1, keys);
    ASSERT_EQ(keys.size(), 1u);
    const std::string &key = keys.front();
    const std::string value(VALUE_SIZE, 'f');
    DS_ASSERT_OK(writer_->Set(key, value));

    TransportRpcCounts before;
    GetRpcCounts(before);

    Optional<Buffer> buffer;
    Status rc = reader_->Get(key, buffer);

    TransportRpcCounts after;
    GetRpcCounts(after);

    DS_ASSERT_OK(rc);
    ASSERT_TRUE(buffer);
    AssertBufferEqual(*buffer, value);
    ASSERT_EQ(AccessTransportTracker::ToString(), ExpectedTransport());
    ASSERT_EQ(after.queryAndGet, before.queryAndGet + 1);
    ASSERT_EQ(after.getObjectRemote, before.getObjectRemote + 1);
}

// One key's data read fails while the others succeed; overall K_OK with the failed slot empty.
TEST_F(KVClientTransportGetTest, PartialDataFailure)
{
    const std::vector<std::string> keys = { "transport_get_ok0_" + GetStringUuid(), "key2",
                                            "transport_get_ok1_" + GetStringUuid() };
    const std::vector<std::string> values = { std::string(VALUE_SIZE, 'a'), std::string(VALUE_SIZE, 'b'),
                                              std::string(VALUE_SIZE, 'c') };
    for (size_t i = 0; i < keys.size(); ++i) {
        DS_ASSERT_OK(writer_->Set(keys[i], values[i]));
    }

    std::vector<Optional<Buffer>> buffers;
    DS_ASSERT_OK(reader_->Get(keys, buffers));

    ASSERT_EQ(buffers.size(), keys.size());
    ASSERT_TRUE(buffers[0]);
    AssertBufferEqual(*buffers[0], values[0]);
    ASSERT_FALSE(buffers[1]); // "key2" failed and no replica to recover
    ASSERT_TRUE(buffers[2]);
    AssertBufferEqual(*buffers[2], values[2]);
    ASSERT_EQ(AccessTransportTracker::ToString(), ExpectedTransport());
}

// Every key fails; the batch returns the first error in input order. The suite timeout bounds retries.
TEST_F(KVClientTransportGetTest, AllKeysFailReturnFirstError)
{
    const std::vector<std::string> keys = { "key2", "key3" };
    const std::vector<std::string> values = { std::string(VALUE_SIZE, 'a'), std::string(VALUE_SIZE, 'b') };
    for (size_t i = 0; i < keys.size(); ++i) {
        DS_ASSERT_OK(writer_->Set(keys[i], values[i]));
    }

    std::vector<Optional<Buffer>> buffers;
    ASSERT_EQ(reader_->Get(keys, buffers).GetCode(), StatusCode::K_RUNTIME_ERROR);
    ASSERT_EQ(buffers.size(), keys.size());
    for (const auto &b : buffers) {
        ASSERT_FALSE(b);
    }
}

TEST_F(KVClientTransportGetTest, LargeObjectRoundTrip)
{
    std::vector<std::string> keys;
    GetRealHashKeysToWorker(META_OWNER_INDEX, 1, keys);
    ASSERT_EQ(keys.size(), 1u);
    const std::string value(LARGE_VALUE_SIZE, 'L');
    DS_ASSERT_OK(writer_->Set(keys.front(), value));

    Optional<Buffer> buffer;
    DS_ASSERT_OK(reader_->Get(keys.front(), buffer));

    ASSERT_TRUE(buffer);
    AssertBufferEqual(*buffer, value);
    ASSERT_EQ(AccessTransportTracker::ToString(), ExpectedTransport());
}

TEST_F(KVClientTransportGetTest, InlineCapacityLimitFallsBack)
{
#ifdef USE_URMA
    reader_.reset();
    SetUbGetSize(VALUE_SIZE / 2);
    InitTransportClient();
    const size_t valueSize = VALUE_SIZE;
#else
    const size_t valueSize = INLINE_DATA_LIMIT + 1;
#endif
    std::vector<std::string> keys;
    GetRealHashKeysToWorker(META_OWNER_INDEX, 1, keys);
    ASSERT_EQ(keys.size(), 1u);
    const std::string &key = keys.front();
    const std::string value(valueSize, 'L');
    DS_ASSERT_OK(writer_->Set(key, value));

    TransportRpcCounts before;
    GetRpcCounts(before);
    Optional<Buffer> buffer;
    DS_ASSERT_OK(reader_->Get(key, buffer));
    TransportRpcCounts after;
    GetRpcCounts(after);

    ASSERT_TRUE(buffer);
    ASSERT_EQ(buffer->GetSize(), value.size());
    AssertBufferEqual(*buffer, value);
    ASSERT_EQ(AccessTransportTracker::ToString(), ExpectedTransport());
    ASSERT_EQ(after.queryAndGet, before.queryAndGet + 1);
    ASSERT_EQ(after.getObjectRemote, before.getObjectRemote + 1);
}

TEST_F(KVClientTransportGetTest, DirectBatchGetRoundTrips32Keys)
{
    const auto keys = MakeRandomKeys(32);
    std::vector<std::string> values;
    values.reserve(keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        values.emplace_back(VALUE_SIZE + i, 'a' + static_cast<char>(i % 26));
        DS_ASSERT_OK(writer_->Set(keys[i], values[i]));
    }

    std::vector<Optional<Buffer>> buffers;
    DS_ASSERT_OK(reader_->Get(keys, buffers));

    ASSERT_EQ(buffers.size(), keys.size());
    for (size_t i = 0; i < buffers.size(); ++i) {
        ASSERT_TRUE(buffers[i]) << "missing buffer at position " << i;
        AssertBufferEqual(*buffers[i], values[i]);
    }
    ASSERT_EQ(AccessTransportTracker::ToString(), ExpectedTransport());
}

TEST_F(KVClientTransportGetTest, DirectBatchGetPreservesOrderAcrossMetadataOwners)
{
    const auto keys = MakeKeysAcrossMetaOwners(4);
    ASSERT_EQ(keys.size(), cluster_->GetWorkerNum() * 4);
    std::vector<std::string> values;
    values.reserve(keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        values.emplace_back(VALUE_SIZE + i * 17, 'A' + static_cast<char>(i % 26));
        DS_ASSERT_OK(writer_->Set(keys[i], values[i]));
    }

    std::vector<Optional<Buffer>> buffers;
    DS_ASSERT_OK(reader_->Get(keys, buffers));

    ASSERT_EQ(buffers.size(), keys.size());
    for (size_t i = 0; i < buffers.size(); ++i) {
        ASSERT_TRUE(buffers[i]) << "missing buffer at position " << i;
        AssertBufferEqual(*buffers[i], values[i]);
    }
    ASSERT_EQ(AccessTransportTracker::ToString(), ExpectedTransport());
}

TEST_F(KVClientTransportGetTest, U7LargeBatchUsesStableRoutingSnapshotAcrossMetadataOwners)
{
    constexpr size_t keysPerOwner = 32;
    const auto keys = MakeKeysAcrossMetaOwners(keysPerOwner);
    ASSERT_EQ(keys.size(), cluster_->GetWorkerNum() * keysPerOwner);

    std::vector<std::string> values;
    values.reserve(keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        values.emplace_back(VALUE_SIZE + i, 'A' + static_cast<char>(i % 26));
    }
    std::vector<StringView> valueViews;
    valueViews.reserve(values.size());
    for (const auto &value : values) {
        valueViews.emplace_back(value);
    }
    std::vector<std::string> failedKeys;
    DS_ASSERT_OK(writer_->MSet(keys, valueViews, failedKeys));
    ASSERT_TRUE(failedKeys.empty());

    std::vector<Optional<Buffer>> buffers;
    DS_ASSERT_OK(reader_->Get(keys, buffers));
    ASSERT_EQ(buffers.size(), keys.size());
    for (size_t i = 0; i < buffers.size(); ++i) {
        ASSERT_TRUE(buffers[i]) << "missing buffer at position " << i;
        AssertBufferEqual(*buffers[i], values[i]);
    }
    ASSERT_EQ(AccessTransportTracker::ToString(), ExpectedTransport());
}

TEST_F(KVClientTransportGetTest, DirectBatchGetReturnsExistingValuesWithMissingSlots)
{
    auto existingKeys = MakeRandomKeys(3);
    const std::vector<std::string> values = { std::string(VALUE_SIZE, 'x'), std::string(VALUE_SIZE + 1, 'y'),
                                              std::string(VALUE_SIZE + 2, 'z') };
    for (size_t i = 0; i < existingKeys.size(); ++i) {
        DS_ASSERT_OK(writer_->Set(existingKeys[i], values[i]));
    }
    const std::vector<std::string> keys = { existingKeys[0], "missing_" + GetStringUuid(), existingKeys[1],
                                            "missing_" + GetStringUuid(), existingKeys[2] };

    std::vector<Optional<Buffer>> buffers;
    DS_ASSERT_OK(reader_->Get(keys, buffers));

    ASSERT_EQ(buffers.size(), keys.size());
    ASSERT_TRUE(buffers[0]);
    AssertBufferEqual(*buffers[0], values[0]);
    ASSERT_FALSE(buffers[1]);
    ASSERT_TRUE(buffers[2]);
    AssertBufferEqual(*buffers[2], values[1]);
    ASSERT_FALSE(buffers[3]);
    ASSERT_TRUE(buffers[4]);
    AssertBufferEqual(*buffers[4], values[2]);
    ASSERT_EQ(AccessTransportTracker::ToString(), ExpectedTransport());
}

TEST_F(KVClientTransportGetTest, DirectBatchGetAllMissingReturnsNotFound)
{
    const std::vector<std::string> keys = { "missing_first_" + GetStringUuid(),
                                            "missing_second_" + GetStringUuid(),
                                            "missing_third_" + GetStringUuid() };
    std::vector<Optional<Buffer>> buffers;

    ASSERT_EQ(reader_->Get(keys, buffers).GetCode(), StatusCode::K_NOT_FOUND);
    ASSERT_EQ(buffers.size(), keys.size());
    for (const auto &buffer : buffers) {
        ASSERT_FALSE(buffer);
    }
}

TEST_F(KVClientTransportGetTest, DirectBatchGetAllUnavailableReturnsFirstInputError)
{
    const std::vector<std::string> keys = { "key3", "key2" };
    DS_ASSERT_OK(writer_->Set(keys[0], std::string(VALUE_SIZE, 'n')));
    DS_ASSERT_OK(writer_->Set(keys[1], std::string(VALUE_SIZE, 'r')));
    std::vector<Optional<Buffer>> buffers;

    ASSERT_EQ(reader_->Get(keys, buffers).GetCode(), StatusCode::K_WORKER_PULL_OBJECT_NOT_FOUND);
    ASSERT_EQ(buffers.size(), keys.size());
    for (const auto &buffer : buffers) {
        ASSERT_FALSE(buffer);
    }
}

TEST_F(KVClientTransportGetTest, DirectBatchGetRetriesChangedSizesWithoutCorruptingNeighbors)
{
#ifndef USE_URMA
    GTEST_SKIP() << "Direct Batch Get size-change ST requires USE_URMA.";
#else
    constexpr auto WAIT_TIMEOUT = std::chrono::seconds(5);
    constexpr auto POLL_INTERVAL = std::chrono::milliseconds(50);
    const auto keys = MakeRandomKeys(2);
    const std::vector<std::string> initialValues = { std::string(VALUE_SIZE, 'a'),
                                                     std::string(VALUE_SIZE + 1024, 'b') };
    const std::vector<std::string> updatedValues = { std::string(VALUE_SIZE + 8192, 'A'),
                                                     std::string(VALUE_SIZE + 16 * 1024, 'B') };
    for (size_t i = 0; i < keys.size(); ++i) {
        DS_ASSERT_OK(writer_->Set(keys[i], initialValues[i]));
    }

    std::vector<Optional<Buffer>> buffers;
    std::string actualTransport;
    std::promise<Status> getPromise;
    auto getFuture = getPromise.get_future();
    std::thread getThread;
    bool pauseCleared = false;
    Raii cleanup([&]() {
        if (!pauseCleared) {
            (void)inject::Clear(BATCH_GET_OBJECT_REMOTE_INJECT);
        }
        if (getThread.joinable()) {
            getThread.join();
        }
    });

    DS_ASSERT_OK(inject::Set(BATCH_GET_OBJECT_REMOTE_INJECT, "pause()"));
    const uint64_t baselineCount = inject::GetExecuteCount(BATCH_GET_OBJECT_REMOTE_INJECT);

    getThread = std::thread([&]() {
        auto rc = reader_->Get(keys, buffers);
        actualTransport = AccessTransportTracker::ToString();
        getPromise.set_value(std::move(rc));
    });

    bool dataRequestPaused = false;
    const auto deadline = std::chrono::steady_clock::now() + WAIT_TIMEOUT;
    while (std::chrono::steady_clock::now() < deadline) {
        dataRequestPaused = inject::GetExecuteCount(BATCH_GET_OBJECT_REMOTE_INJECT) > baselineCount;
        if (dataRequestPaused || getFuture.wait_for(POLL_INTERVAL) == std::future_status::ready) {
            break;
        }
    }
    ASSERT_TRUE(dataRequestPaused) << "direct Batch Get did not reach the paused client data request";

    for (size_t i = 0; i < keys.size(); ++i) {
        DS_ASSERT_OK(writer_->Set(keys[i], updatedValues[i]));
    }

    const Status clearStatus = inject::Clear(BATCH_GET_OBJECT_REMOTE_INJECT);
    ASSERT_TRUE(clearStatus.IsOk()) << clearStatus.ToString();
    pauseCleared = true;
    getThread.join();
    const Status getStatus = getFuture.get();
    DS_ASSERT_OK(getStatus);
    ASSERT_EQ(buffers.size(), keys.size());
    for (size_t i = 0; i < buffers.size(); ++i) {
        ASSERT_TRUE(buffers[i]) << "missing buffer at position " << i;
        AssertBufferEqual(*buffers[i], updatedValues[i]);
    }
    ASSERT_EQ(actualTransport, ExpectedTransport());
#endif
}

TEST_F(KVClientTransportGetTest, DirectBatchGetConcurrentOverlappingRequests)
{
    constexpr size_t KEY_COUNT = 32;
    constexpr size_t THREAD_COUNT = 4;
    constexpr size_t KEYS_PER_REQUEST = 16;
    constexpr size_t KEY_STRIDE = 4;
    const auto keys = MakeRandomKeys(KEY_COUNT);
    std::unordered_map<std::string, std::string> expected;
    for (size_t i = 0; i < keys.size(); ++i) {
        auto inserted = expected.emplace(keys[i], std::string(VALUE_SIZE + i, 'a' + static_cast<char>(i % 26)));
        DS_ASSERT_OK(writer_->Set(inserted.first->first, inserted.first->second));
    }

    struct GetResult {
        Status status;
        std::vector<std::string> keys;
        std::vector<Optional<Buffer>> buffers;
        std::string transport;
    };
    std::promise<void> ready;
    std::shared_future<void> start(ready.get_future());
    ThreadPool pool(THREAD_COUNT);
    std::vector<std::future<GetResult>> futures;
    for (size_t thread = 0; thread < THREAD_COUNT; ++thread) {
        futures.emplace_back(pool.Submit([&, thread]() {
            GetResult result;
            result.keys.reserve(KEYS_PER_REQUEST);
            for (size_t i = 0; i < KEYS_PER_REQUEST; ++i) {
                result.keys.emplace_back(keys[(thread * KEY_STRIDE + i) % keys.size()]);
            }
            start.wait();
            result.status = reader_->Get(result.keys, result.buffers);
            result.transport = AccessTransportTracker::ToString();
            return result;
        }));
    }
    ready.set_value();

    for (auto &future : futures) {
        auto result = future.get();
        DS_ASSERT_OK(result.status);
        ASSERT_EQ(result.buffers.size(), result.keys.size());
        for (size_t i = 0; i < result.keys.size(); ++i) {
            ASSERT_TRUE(result.buffers[i]) << "missing buffer at position " << i;
            AssertBufferEqual(*result.buffers[i], expected.at(result.keys[i]));
        }
        ASSERT_EQ(result.transport, ExpectedTransport());
    }
}

TEST_F(KVClientTransportGetTest, DirectBatchGetUbBufferSurvivesSiblingReset)
{
#ifndef USE_URMA
    GTEST_SKIP() << "Direct Batch Get UB owner-lifetime ST requires USE_URMA.";
#else
    constexpr size_t KEY_COUNT = 8;
    constexpr size_t SURVIVOR_INDEX = 3;
    const auto keys = MakeRandomKeys(KEY_COUNT);
    std::vector<std::string> values;
    values.reserve(keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        values.emplace_back(VALUE_SIZE + i * 4096, 'A' + static_cast<char>(i));
        DS_ASSERT_OK(writer_->Set(keys[i], values[i]));
    }

    std::vector<Optional<Buffer>> buffers;
    const Status getStatus = reader_->Get(keys, buffers);
    const std::string actualTransport = AccessTransportTracker::ToString();
    DS_ASSERT_OK(getStatus);
    ASSERT_EQ(buffers.size(), keys.size());
    for (size_t i = 0; i < buffers.size(); ++i) {
        ASSERT_TRUE(buffers[i]) << "missing buffer at position " << i;
        AssertBufferEqual(*buffers[i], values[i]);
    }

    Optional<Buffer> survivor = std::move(buffers[SURVIVOR_INDEX]);
    const std::string survivorValue = values[SURVIVOR_INDEX];
    buffers.clear();
    buffers.shrink_to_fit();

    ASSERT_TRUE(survivor);
    AssertBufferEqual(*survivor, survivorValue);
    if (actualTransport != "UB") {
        GTEST_SKIP() << "URMA runtime did not execute UB direct read: " << actualTransport;
    }
#endif
}

TEST_F(KVClientTransportGetTest, DirectBatchGetUbConcurrentOverlappingBatches)
{
#ifndef USE_URMA
    GTEST_SKIP() << "Direct Batch Get UB concurrency ST requires USE_URMA.";
#else
    constexpr size_t KEY_COUNT = 24;
    constexpr size_t THREAD_COUNT = 4;
    constexpr size_t ITERATION_COUNT = 4;
    constexpr size_t KEYS_PER_REQUEST = 12;
    constexpr size_t KEY_STRIDE = 3;
    const auto keys = MakeRandomKeys(KEY_COUNT);
    std::unordered_map<std::string, std::string> expected;
    for (size_t i = 0; i < keys.size(); ++i) {
        auto inserted = expected.emplace(keys[i], std::string(VALUE_SIZE + i * 257, 'a' + static_cast<char>(i % 26)));
        DS_ASSERT_OK(writer_->Set(inserted.first->first, inserted.first->second));
    }

    struct ConcurrentGetResult {
        std::vector<Status> statuses;
        bool contentsMatch = true;
        std::string mismatch;
        std::vector<std::string> transports;
    };
    std::promise<void> ready;
    std::shared_future<void> start(ready.get_future());
    ThreadPool pool(THREAD_COUNT);
    std::vector<std::future<ConcurrentGetResult>> futures;
    for (size_t thread = 0; thread < THREAD_COUNT; ++thread) {
        futures.emplace_back(pool.Submit([&, thread]() {
            ConcurrentGetResult result;
            result.statuses.reserve(ITERATION_COUNT);
            result.transports.reserve(ITERATION_COUNT);
            start.wait();
            for (size_t iteration = 0; iteration < ITERATION_COUNT; ++iteration) {
                std::vector<std::string> requestKeys;
                requestKeys.reserve(KEYS_PER_REQUEST);
                for (size_t i = 0; i < KEYS_PER_REQUEST; ++i) {
                    requestKeys.emplace_back(keys[(thread * KEY_STRIDE + iteration + i) % keys.size()]);
                }
                std::vector<Optional<Buffer>> buffers;
                const Status rc = reader_->Get(requestKeys, buffers);
                result.statuses.emplace_back(rc);
                result.transports.emplace_back(AccessTransportTracker::ToString());
                if (rc.IsError()) {
                    break;
                }
                if (buffers.size() != requestKeys.size()) {
                    result.contentsMatch = false;
                    result.mismatch = "result count does not match request count";
                    break;
                }
                for (size_t i = 0; i < requestKeys.size(); ++i) {
                    const auto &value = expected.at(requestKeys[i]);
                    const void *data = buffers[i] ? buffers[i]->ImmutableData() : nullptr;
                    if (!buffers[i] || buffers[i]->GetSize() != static_cast<int64_t>(value.size())
                        || data == nullptr || std::memcmp(data, value.data(), value.size()) != 0) {
                        result.contentsMatch = false;
                        result.mismatch = "value mismatch at thread " + std::to_string(thread) + ", iteration "
                                          + std::to_string(iteration) + ", position " + std::to_string(i);
                        break;
                    }
                }
                if (!result.contentsMatch) {
                    break;
                }
            }
            return result;
        }));
    }
    ready.set_value();

    std::vector<ConcurrentGetResult> results;
    results.reserve(futures.size());
    size_t ubObservationCount = 0;
    for (auto &future : futures) {
        results.emplace_back(future.get());
        ubObservationCount += static_cast<size_t>(
            std::count(results.back().transports.begin(), results.back().transports.end(), "UB"));
    }
    for (const auto &result : results) {
        for (const auto &status : result.statuses) {
            DS_ASSERT_OK(status);
        }
        ASSERT_EQ(result.statuses.size(), ITERATION_COUNT);
        ASSERT_TRUE(result.contentsMatch) << result.mismatch;
        ASSERT_EQ(result.transports.size(), ITERATION_COUNT);
    }
    if (ubObservationCount == 0) {
        GTEST_SKIP() << "URMA runtime did not execute UB in any concurrent direct-read context.";
    }
    for (const auto &result : results) {
        for (const auto &transport : result.transports) {
            ASSERT_EQ(transport, "UB") << "every concurrent Get context must exercise UB";
        }
    }
#endif
}

// Reproduces issue #749: with enable_local_cache=false the direct Get path goes through
// WorkerWorkerOCServiceImpl::GetObjectRemote, which takes the SHM read latch via ShmGuard::TryRLatch.
// When the latch cannot be acquired (write-side contention during set/eviction/migration under load),
// TryRLatch previously returned K_RUNTIME_ERROR (code=5). Because ReplicaReader treats K_RUNTIME_ERROR as
// a non-retryable location error, every direct Get surfaced code=5 to the client — matching the 100%
// get failure observed in the issue while Set stayed healthy. The latch failure is transient contention,
// so it must return K_TRY_AGAIN to let the reader retry until the API deadline.
class KVClientTransportGetShmLatchTest : public OCClientCommon, public CommonDistributedExt {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numEtcd = 1;
        opts.numWorkers = 3;
        opts.enableDistributedMaster = "true";
        // Enable SHM so the worker GetObjectRemote path exercises ShmGuard::TryRLatch.
        opts.workerGflagParams =
            " -shared_memory_size_mb=512 -ipc_through_shared_memory=true -arena_per_tenant=1 -use_brpc=true"
            " -enable_urma=false";
    }

    void SetUp() override
    {
        previousUseBrpc_ = FLAGS_use_brpc;
        FLAGS_use_brpc = true;
        DS_ASSERT_OK(inject::Set(SKIP_WARMUP_INJECT, "call()"));
        DS_ASSERT_OK(inject::Set(QUERY_AND_GET_INJECT, "call()"));
        DS_ASSERT_OK(inject::Set(GET_OBJECT_REMOTE_INJECT, "call()"));
        ExternalClusterTest::SetUp();
        CommonDistributedExt::InitTestEtcdInstance();

        // Reuse the base class CommonDistributedExt::etcd_ instead of shadowing it, so TearDown resets a
        // single owner. InitTestEtcdInstance is idempotent (early-returns when etcd_ != nullptr).
        ASSERT_NE(etcd_, nullptr);
        InitTestKVClient(META_OWNER_INDEX, writer_, CLIENT_TIMEOUT_MS);
        InitTransportClient();
    }

    void TearDown() override
    {
        reader_.reset();
        writer_.reset();
        etcd_.reset();
        (void)inject::Clear(SKIP_WARMUP_INJECT);
        (void)inject::Clear(QUERY_AND_GET_INJECT);
        (void)inject::Clear(GET_OBJECT_REMOTE_INJECT);
        ClearShmLatchFailureEverywhere();
        ExternalClusterTest::TearDown();
        FLAGS_use_brpc = previousUseBrpc_;
    }

protected:
    BaseCluster *GetCluster() override
    {
        return cluster_.get();
    }

    void InitTransportClient()
    {
        ConnectOptions options;
        InitConnectOpt(TRANSPORT_CLIENT_WORKER_INDEX, options, CLIENT_TIMEOUT_MS);
        options.enableLocalCache = false;
        reader_ = std::make_shared<KVClient>(options);
        DS_ASSERT_OK(reader_->Init());
    }

    // Pin the SHM read-latch to fail on every worker so the direct Get exercises the retryable
    // error path regardless of which worker owns the pulled object's metadata or replicas.
    void PinShmLatchFailureEverywhere()
    {
        for (uint32_t i = 0; i < cluster_->GetWorkerNum(); ++i) {
            DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, i, SHM_LATCH_FAIL_INJECT, "return()"));
        }
    }

    // Symmetric cleanup so neither the test body nor TearDown can forget to clear the inject.
    void ClearShmLatchFailureEverywhere()
    {
        for (uint32_t i = 0; i < cluster_->GetWorkerNum(); ++i) {
            (void)cluster_->ClearInjectAction(WORKER, i, SHM_LATCH_FAIL_INJECT);
        }
    }

    std::shared_ptr<KVClient> writer_;
    std::shared_ptr<KVClient> reader_;
    bool previousUseBrpc_ = false;
};

// Baseline: a direct Get with SHM enabled succeeds and returns the written value.
TEST_F(KVClientTransportGetShmLatchTest, DirectGetSucceedsWithShmEnabled)
{
    const std::string key = "shm_latch_baseline_" + GetStringUuid();
    const std::string value(VALUE_SIZE, 'x');
    DS_ASSERT_OK(writer_->Set(key, value));

    std::vector<Optional<Buffer>> buffers;
    DS_ASSERT_OK(reader_->Get({ key }, buffers));
    ASSERT_EQ(buffers.size(), 1u);
    ASSERT_TRUE(buffers[0]);
    AssertBufferEqual(*buffers[0], value);
}

// Regression: unresolved SHM read-latch contention must surface as a retryable error, never as
// K_RUNTIME_ERROR (code=5). With the latch pinned to fail, the reader retries until the API deadline
// and returns K_RPC_DEADLINE_EXCEEDED; after clearing the inject the same key reads successfully.
TEST_F(KVClientTransportGetShmLatchTest, LatchFailureIsRetryableNotRuntimeError)
{
    const std::string key = "shm_latch_fail_" + GetStringUuid();
    const std::string value(VALUE_SIZE, 'y');
    DS_ASSERT_OK(writer_->Set(key, value));

    // Sanity check that the object is readable before injecting contention.
    {
        std::vector<Optional<Buffer>> buffers;
        DS_ASSERT_OK(reader_->Get({ key }, buffers));
        ASSERT_TRUE(buffers[0]);
        AssertBufferEqual(*buffers[0], value);
    }

    PinShmLatchFailureEverywhere();
    // Single-key Get is required for the K_RPC_DEADLINE_EXCEEDED assertion: ObjectReadFlow::ReadObjects
    // takes its ready.size()==1 branch (ReplicaReader::Read), where K_TRY_AGAIN stays retryable until
    // CheckDeadline/Backoff exhaust the API deadline. A multi-key request would take ReplicaReader::ReadBatch,
    // whose FinishUnresolvedWithDeadline returns lastStatus (K_TRY_AGAIN) under some states — not deadline.
    std::vector<Optional<Buffer>> buffers;
    const auto start = std::chrono::steady_clock::now();
    const Status rc = reader_->Get({ key }, buffers);
    const auto elapsedMs = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start).count();
    // With the latch pinned to fail, TryRLatch returns K_TRY_AGAIN on every replica, the reader retries
    // via CheckDeadline/Backoff until the API deadline, and ReplicaReader::Read returns
    // K_RPC_DEADLINE_EXCEEDED. K_RUNTIME_ERROR here would mean the bug is back. Accepting other retryable
    // codes would mask a regression where the reader stops exhausting the deadline, so pin the final code.
    ASSERT_NE(rc.GetCode(), StatusCode::K_RUNTIME_ERROR)
        << "direct Get must not surface SHM latch contention as K_RUNTIME_ERROR";
    ASSERT_EQ(rc.GetCode(), StatusCode::K_RPC_DEADLINE_EXCEEDED)
        << "expected deadline exhaustion after latch-contention retries, got: " << rc.ToString();
    // Wall-clock guard: the contention Get must exhaust around the API deadline (sourced from
    // requestTimeoutMs_, bounded by CLIENT_TIMEOUT_MS), and must not run far longer if the deadline is
    // ever raised. Cap at 2x CLIENT_TIMEOUT_MS to catch regressions.
    ASSERT_LT(elapsedMs, 2 * CLIENT_TIMEOUT_MS)
        << "latch-contention Get ran " << elapsedMs << "ms, expected near the API deadline";

    ClearShmLatchFailureEverywhere();
    std::vector<Optional<Buffer>> recovered;
    DS_ASSERT_OK(reader_->Get({ key }, recovered));
    ASSERT_EQ(recovered.size(), 1u);
    ASSERT_TRUE(recovered[0]);
    AssertBufferEqual(*recovered[0], value);
}

}  // namespace st
}  // namespace datasystem
