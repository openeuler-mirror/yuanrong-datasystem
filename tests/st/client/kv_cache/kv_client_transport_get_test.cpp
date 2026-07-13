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

/** Description: Tests KVClient reads through the transport layer. */

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "client/object_cache/oc_client_common.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/kvstore/etcd/etcd_store.h"
#include "datasystem/common/log/access_recorder.h"
#include "datasystem/kv_client.h"

DS_DECLARE_bool(use_brpc);

namespace datasystem {
namespace st {
namespace {
constexpr uint32_t META_OWNER_INDEX = 0;
constexpr uint32_t TRANSPORT_CLIENT_WORKER_INDEX = 1;
constexpr size_t VALUE_SIZE = 128 * 1024;
constexpr char META_OWNER_INJECT[] = "client.transport.meta_owner";
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
        opts.numWorkers = 2;
        opts.enableDistributedMaster = "true";
        opts.injectActions = "MurmurHash3:return()";
        opts.workerGflagParams =
            " -shared_memory_size_mb=512 -ipc_through_shared_memory=false -arena_per_tenant=1 -use_brpc=true";
#ifdef USE_URMA
        opts.workerGflagParams += " -enable_urma=true -enable_transport_fallback=false";
#else
        opts.workerGflagParams += " -enable_urma=false";
#endif
    }

    void SetUp() override
    {
        previousUseBrpc_ = FLAGS_use_brpc;
        FLAGS_use_brpc = true;
        DS_ASSERT_OK(inject::Set(SKIP_WARMUP_INJECT, "call()"));
        ExternalClusterTest::SetUp();
        etcd_ = InitTestEtcdInstance();
        ASSERT_NE(etcd_, nullptr);

        DS_ASSERT_OK(cluster_->GetWorkerAddr(META_OWNER_INDEX, metaOwner_));
        DS_ASSERT_OK(inject::Set(META_OWNER_INJECT, "call(" + metaOwner_.ToString() + ")"));

        InitTransportClient();
        InitTestKVClient(META_OWNER_INDEX, client0_);
    }

    void TearDown() override
    {
        client1_.reset();
        client0_.reset();
        etcd_.reset();
        (void)inject::Clear(META_OWNER_INJECT);
        (void)inject::Clear(SKIP_WARMUP_INJECT);
        ExternalClusterTest::TearDown();
        FLAGS_use_brpc = previousUseBrpc_;
    }

protected:
    void InitTransportClient()
    {
        ConnectOptions options;
        InitConnectOpt(TRANSPORT_CLIENT_WORKER_INDEX, options);
        options.enableLocalCache = false;
        client1_ = std::make_shared<KVClient>(options);
        DS_ASSERT_OK(client1_->Init());
    }

    std::unique_ptr<EtcdStore> etcd_;
    HostPort metaOwner_;
    std::shared_ptr<KVClient> client0_;
    std::shared_ptr<KVClient> client1_;
    bool previousUseBrpc_ = false;
};

TEST_F(KVClientTransportGetTest, SingleKeyGet)
{
    const std::string key = GetObjectKeyHashToWorker(etcd_.get(), META_OWNER_INDEX);
    const std::string value(VALUE_SIZE, 's');
    DS_ASSERT_OK(client0_->Set(key, value));

    Optional<Buffer> buffer;
    DS_ASSERT_OK(client1_->Get(key, buffer));

    ASSERT_TRUE(buffer);
    AssertBufferEqual(*buffer, value);
    ASSERT_EQ(AccessTransportTracker::ToString(), ExpectedTransport());
}

TEST_F(KVClientTransportGetTest, MultiKeyGet)
{
    std::vector<std::string> keys;
    GetObjectKeysHashToWorker(etcd_.get(), META_OWNER_INDEX, 2, keys);
    const std::vector<std::string> values = { std::string(VALUE_SIZE, 'a'), std::string(VALUE_SIZE * 2, 'b') };
    for (size_t i = 0; i < keys.size(); ++i) {
        DS_ASSERT_OK(client0_->Set(keys[i], values[i]));
    }

    std::vector<Optional<Buffer>> buffers;
    DS_ASSERT_OK(client1_->Get(keys, buffers));

    ASSERT_EQ(buffers.size(), keys.size());
    for (size_t i = 0; i < buffers.size(); ++i) {
        ASSERT_TRUE(buffers[i]);
        AssertBufferEqual(*buffers[i], values[i]);
    }
    ASSERT_EQ(AccessTransportTracker::ToString(), ExpectedTransport());
}
}  // namespace st
}  // namespace datasystem
