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
#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "client/object_cache/oc_client_common.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/log/access_recorder.h"
#include "datasystem/kv_client.h"

DS_DECLARE_bool(use_brpc);

namespace datasystem {
namespace st {
namespace {
constexpr uint32_t META_OWNER_INDEX = 0;
constexpr uint32_t TRANSPORT_CLIENT_WORKER_INDEX = 1;
constexpr int32_t CLIENT_TIMEOUT_MS = 3'000;
constexpr size_t VALUE_SIZE = 128 * 1024;
constexpr size_t LARGE_VALUE_SIZE = 8 * 1024 * 1024;
constexpr char SKIP_WARMUP_INJECT[] = "ObjectClientImpl.ClientWorkerWarmup.skip";

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
        opts.numWorkers = 3; // worker0/worker1 for routing + worker2 as an extra replica holder
        opts.enableDistributedMaster = "true";
        opts.workerGflagParams =
            " -shared_memory_size_mb=512 -ipc_through_shared_memory=false -arena_per_tenant=1 -use_brpc=true";
#ifdef USE_URMA
        opts.workerGflagParams += " -enable_urma=true -enable_transport_fallback=false";
#else
        opts.workerGflagParams += " -enable_urma=false";
#endif
        // Inject the data-path failure hook at startup. It fires by exact key name ("key2"/"key3"/
        // "key0") inside GetObjectRemoteImpl and is a no-op for any other key, so it is safe to keep
        // enabled for the whole suite and lets the failure cases avoid the worker request-queue
        // timeout seen with runtime SetInjectAction under distributed master.
        opts.injectActions = "worker.batch_get_failure_for_keys:call()";
    }

    void SetUp() override
    {
        previousUseBrpc_ = FLAGS_use_brpc;
        FLAGS_use_brpc = true;
        DS_ASSERT_OK(inject::Set(SKIP_WARMUP_INJECT, "call()"));
        ExternalClusterTest::SetUp();

        // Writer (enableLocalCache=true, gateway path) and reader through the transport layer.
        InitTestKVClient(META_OWNER_INDEX, writer_, CLIENT_TIMEOUT_MS);
        InitTransportClient();
    }

    void TearDown() override
    {
        reader_.reset();
        writer_.reset();
        (void)inject::Clear(SKIP_WARMUP_INJECT);
        ExternalClusterTest::TearDown();
        FLAGS_use_brpc = previousUseBrpc_;
    }

protected:
    // enableLocalCache=false reader: the client under test, exercising TransportLayer::Get.
    void InitTransportClient()
    {
        ConnectOptions options;
        InitConnectOpt(TRANSPORT_CLIENT_WORKER_INDEX, options, CLIENT_TIMEOUT_MS);
        options.enableLocalCache = false;
        reader_ = std::make_shared<KVClient>(options);
        DS_ASSERT_OK(reader_->Init());
    }

    // Generate N distinct random keys; the real hash-ring routing distributes them across workers,
    // so no manual hash-to-worker pinning is needed (client already resolves meta owners).
    std::vector<std::string> MakeRandomKeys(size_t count)
    {
        std::vector<std::string> keys;
        keys.reserve(count);
        for (size_t i = 0; i < count; ++i) {
            keys.emplace_back("transport_get_" + std::to_string(i) + "_" + GetStringUuid());
        }
        return keys;
    }

    std::shared_ptr<KVClient> writer_;
    std::shared_ptr<KVClient> reader_;
    bool previousUseBrpc_ = false;
};

// Single key over the transport layer; data phase runs on the caller thread.
TEST_F(KVClientTransportGetTest, SingleKeyGet)
{
    const std::string key = MakeRandomKeys(1).front();
    const std::string value(VALUE_SIZE, 's');
    DS_ASSERT_OK(writer_->Set(key, value));

    Optional<Buffer> buffer;
    DS_ASSERT_OK(reader_->Get(key, buffer));

    ASSERT_TRUE(buffer);
    AssertBufferEqual(*buffer, value);
    ASSERT_EQ(AccessTransportTracker::ToString(), ExpectedTransport());
}

// Multi-key batch returns every value in input order.
TEST_F(KVClientTransportGetTest, MultiKeyGetSameOwner)
{
    const auto keys = MakeRandomKeys(3);
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
    const auto keys = MakeRandomKeys(8);
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
    const std::string key = MakeRandomKeys(1).front();
    Optional<Buffer> buffer;
    ASSERT_EQ(reader_->Get(key, buffer).GetCode(), StatusCode::K_NOT_FOUND);
    ASSERT_FALSE(buffer);
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

// Large value round-trip exercises the transport receive-buffer allocation and SDK Buffer
// materialization for objects bigger than the typical fast-transport memory unit.
TEST_F(KVClientTransportGetTest, LargeObjectRoundTrip)
{
    const std::string key = MakeRandomKeys(1).front();
    const std::string value(LARGE_VALUE_SIZE, 'L');
    DS_ASSERT_OK(writer_->Set(key, value));

    Optional<Buffer> buffer;
    DS_ASSERT_OK(reader_->Get(key, buffer));

    ASSERT_TRUE(buffer);
    ASSERT_EQ(buffer->GetSize(), value.size());
    AssertBufferEqual(*buffer, value);
    ASSERT_EQ(AccessTransportTracker::ToString(), ExpectedTransport());
}

}  // namespace st
}  // namespace datasystem
