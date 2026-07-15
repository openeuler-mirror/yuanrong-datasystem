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

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "client/object_cache/oc_client_common.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/kvstore/etcd/etcd_store.h"
#include "datasystem/common/log/access_recorder.h"
#include "datasystem/common/util/hash_algorithm.h"
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
constexpr char INLINE_READ_FAILURE_INJECT[] = "worker.worker_worker_remote_get_failure";

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

class KVClientTransportGetTest : public OCClientCommon {
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

        etcd_ = InitTestEtcdInstance();
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
        RestoreUbGetSize();
        (void)inject::Clear(SKIP_WARMUP_INJECT);
        (void)inject::Clear(QUERY_AND_GET_INJECT);
        (void)inject::Clear(GET_OBJECT_REMOTE_INJECT);
        ExternalClusterTest::TearDown();
        FLAGS_use_brpc = previousUseBrpc_;
    }

protected:
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

}  // namespace st
}  // namespace datasystem
