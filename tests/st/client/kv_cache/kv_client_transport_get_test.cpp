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
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "client/object_cache/oc_client_common.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/kvstore/etcd/etcd_constants.h"
#include "datasystem/common/kvstore/etcd/etcd_store.h"
#include "datasystem/common/log/access_recorder.h"
#include "datasystem/common/util/hash_algorithm.h"
#include "datasystem/kv_client.h"
#include "datasystem/protos/hash_ring.pb.h"

DS_DECLARE_bool(use_brpc);

namespace datasystem {
namespace st {
namespace {
constexpr uint32_t META_OWNER_INDEX = 0;
constexpr uint32_t TRANSPORT_CLIENT_WORKER_INDEX = 1;
constexpr size_t VALUE_SIZE = 128 * 1024;
constexpr size_t KEY_SEARCH_LIMIT = 100'000;
constexpr char REAL_ROUTE_KEY_PREFIX[] = "transport_real_route_";
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

        InitTransportClient();
        InitTestKVClient(META_OWNER_INDEX, client0_);
    }

    void TearDown() override
    {
        client1_.reset();
        client0_.reset();
        etcd_.reset();
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

    void GetRealHashKeysToWorker(uint32_t workerIndex, size_t keyCount, std::vector<std::string> &keys)
    {
        ASSERT_NE(etcd_, nullptr);
        std::string value;
        DS_ASSERT_OK(etcd_->Get(ETCD_RING_PREFIX, "", value));
        HashRingPb ring;
        ASSERT_TRUE(ring.ParseFromString(value));

        HostPort targetWorker;
        DS_ASSERT_OK(cluster_->GetWorkerAddr(workerIndex, targetWorker));
        ASSERT_NE(ring.workers().find(targetWorker.ToString()), ring.workers().end());
        std::map<uint32_t, std::string> tokenWorkers;
        for (const auto &worker : ring.workers()) {
            for (const auto token : worker.second.hash_tokens()) {
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

    std::unique_ptr<EtcdStore> etcd_;
    std::shared_ptr<KVClient> client0_;
    std::shared_ptr<KVClient> client1_;
    bool previousUseBrpc_ = false;
};

TEST_F(KVClientTransportGetTest, SingleKeyGet)
{
    std::vector<std::string> keys;
    GetRealHashKeysToWorker(META_OWNER_INDEX, 1, keys);
    const std::string &key = keys.front();
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
    std::vector<std::string> worker0Keys;
    std::vector<std::string> worker1Keys;
    GetRealHashKeysToWorker(META_OWNER_INDEX, 1, worker0Keys);
    GetRealHashKeysToWorker(TRANSPORT_CLIENT_WORKER_INDEX, 1, worker1Keys);
    const std::vector<std::string> keys = { worker0Keys.front(), worker1Keys.front() };
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
