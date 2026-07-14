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

/** Description: Tests the routed KVClient Set transaction. */

#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "client/object_cache/oc_client_common.h"
#include "datasystem/common/ak_sk/ak_sk_manager.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/kvstore/etcd/etcd_constants.h"
#include "datasystem/common/kvstore/etcd/etcd_store.h"
#include "datasystem/common/log/access_recorder.h"
#include "datasystem/common/rpc/rpc_message.h"
#include "datasystem/common/rpc/rpc_stub_cache_mgr.h"
#include "datasystem/common/util/hash_algorithm.h"
#include "datasystem/common/util/request_context.h"
#include "datasystem/kv_client.h"
#include "datasystem/protos/hash_ring.pb.h"
#include "datasystem/worker/object_cache/worker_master_oc_api.h"

DS_DECLARE_bool(use_brpc);

namespace datasystem {
namespace st {
namespace {
constexpr uint32_t WORKER_NUM = 2;
constexpr uint32_t READER_WORKER_INDEX = 0;
constexpr uint32_t ROUTED_CLIENT_WORKER_INDEX = 1;
constexpr size_t VALUE_SIZE = 128 * 1024;
constexpr size_t KEY_SEARCH_LIMIT = 100'000;
constexpr char SKIP_WARMUP_INJECT[] = "ObjectClientImpl.ClientWorkerWarmup.skip";
constexpr char PUBLISH_INJECT[] = "WorkerRpcClient.InvokeSet.beforeRpc";

const char *ExpectedTransport()
{
#ifdef USE_URMA
    return "UB";
#else
    return "TCP";
#endif
}
}  // namespace

class KVClientTransportSetTest : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        FLAGS_v = 1;
        opts.numEtcd = 1;
        opts.numWorkers = WORKER_NUM;
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
        InitRoutedClient();
        InitLocalClient();
        InitTestKVClient(READER_WORKER_INDEX, readerClient_);
        InitMasterApis();
    }

    void TearDown() override
    {
        masterApis_.clear();
        akSkManager_.reset();
        routedClient_.reset();
        localClient_.reset();
        readerClient_.reset();
        etcd_.reset();
        (void)inject::Clear(PUBLISH_INJECT);
        (void)inject::Clear(SKIP_WARMUP_INJECT);
        ExternalClusterTest::TearDown();
        FLAGS_use_brpc = previousUseBrpc_;
    }

protected:
    void InitRoutedClient()
    {
        ConnectOptions options;
        InitConnectOpt(ROUTED_CLIENT_WORKER_INDEX, options);
        options.enableLocalCache = false;
        routedClient_ = std::make_shared<KVClient>(options);
        DS_ASSERT_OK(routedClient_->Init());
    }

    void InitLocalClient()
    {
        ConnectOptions options;
        InitConnectOpt(ROUTED_CLIENT_WORKER_INDEX, options);
        options.enableLocalCache = true;
        localClient_ = std::make_shared<KVClient>(options);
        DS_ASSERT_OK(localClient_->Init());
    }

    void InitMasterApis()
    {
        DS_ASSERT_OK(RpcStubCacheMgr::Instance().Init(100));
        akSkManager_ = std::make_shared<AkSkManager>();
        DS_ASSERT_OK(akSkManager_->SetClientAkSk("QTWAOYTTINDUT2QVKYUC",
                                                "MFyfvK41ba2giqM7**********KGpownRZlmVmHc"));

        HostPort localAddress;
        DS_ASSERT_OK(cluster_->GetWorkerAddr(READER_WORKER_INDEX, localAddress));
        queryAddress_ = localAddress.ToString();
        masterApis_.reserve(WORKER_NUM);
        for (uint32_t i = 0; i < WORKER_NUM; ++i) {
            HostPort workerAddress;
            DS_ASSERT_OK(cluster_->GetWorkerAddr(i, workerAddress));
            auto api =
                std::make_unique<worker::WorkerRemoteMasterOCApi>(workerAddress, localAddress, akSkManager_);
            DS_ASSERT_OK(api->Init());
            masterApis_.emplace_back(std::move(api));
        }
    }

    Status QueryPrimaryWorker(const std::string &key, HostPort &primaryWorker)
    {
        Status lastRc(K_NOT_FOUND, "Object metadata does not exist");
        bool querySucceeded = false;
        for (const auto &api : masterApis_) {
            master::QueryMetaReqPb request;
            master::QueryMetaRspPb response;
            std::vector<RpcMessage> payloads;
            request.add_ids(key);
            request.set_address(queryAddress_);
            GetRequestContext()->reqTimeoutDuration.Reset();
            Status rc = api->QueryMeta(request, 0, response, payloads);
            if (rc.IsError()) {
                if (!querySucceeded) {
                    lastRc = rc;
                }
                continue;
            }
            querySucceeded = true;
            for (const auto &queryMeta : response.query_metas()) {
                if (queryMeta.meta().object_key() == key) {
                    return primaryWorker.ParseString(queryMeta.meta().primary_address());
                }
            }
        }
        if (!querySucceeded) {
            return lastRc;
        }
        lastRc = Status(K_NOT_FOUND, "Object metadata does not exist");
        return lastRc;
    }

    Status FindRouteKeyToWorker(uint32_t workerIndex, const std::string &prefix, std::string &key)
    {
        RETURN_RUNTIME_ERROR_IF_NULL(etcd_);
        std::string value;
        RETURN_IF_NOT_OK(etcd_->Get(ETCD_RING_PREFIX, "", value));
        HashRingPb ring;
        CHECK_FAIL_RETURN_STATUS(ring.ParseFromString(value), K_RUNTIME_ERROR, "Parse hash ring failed");
        HostPort targetWorker;
        RETURN_IF_NOT_OK(cluster_->GetWorkerAddr(workerIndex, targetWorker));
        CHECK_FAIL_RETURN_STATUS(ring.workers().find(targetWorker.ToString()) != ring.workers().end(), K_NOT_FOUND,
                                 "Target worker is absent from hash ring");

        std::map<uint32_t, std::string> tokenWorkers;
        for (const auto &worker : ring.workers()) {
            for (const auto token : worker.second.hash_tokens()) {
                tokenWorkers.emplace(token, worker.first);
            }
        }
        CHECK_FAIL_RETURN_STATUS(!tokenWorkers.empty(), K_NOT_FOUND, "Hash ring has no worker tokens");
        for (size_t i = 0; i < KEY_SEARCH_LIMIT; ++i) {
            std::string candidate = prefix + std::to_string(i);
            auto owner = tokenWorkers.upper_bound(MurmurHash3_32(candidate));
            if (owner == tokenWorkers.end()) {
                owner = tokenWorkers.begin();
            }
            if (owner->second == targetWorker.ToString()) {
                key = std::move(candidate);
                return Status::OK();
            }
        }
        return Status(K_NOT_FOUND, "Unable to find a key for the target worker");
    }

    void AssertValue(const std::string &key, const std::string &expected)
    {
        std::string actual;
        DS_ASSERT_OK(readerClient_->Get(key, actual));
        ASSERT_EQ(actual, expected);
    }

    std::shared_ptr<KVClient> routedClient_;
    std::shared_ptr<KVClient> localClient_;
    std::shared_ptr<KVClient> readerClient_;
    std::shared_ptr<AkSkManager> akSkManager_;
    std::unique_ptr<EtcdStore> etcd_;
    std::vector<std::unique_ptr<worker::WorkerRemoteMasterOCApi>> masterApis_;
    std::string queryAddress_;
    bool previousUseBrpc_ = false;
};

TEST_F(KVClientTransportSetTest, RoutedSetPublishesDataAndMetadata)
{
    std::string key;
    DS_ASSERT_OK(FindRouteKeyToWorker(READER_WORKER_INDEX, "transport_set_normal_", key));
    const std::string value(VALUE_SIZE, 'n');

    DS_ASSERT_OK(routedClient_->Set(key, value));

    ASSERT_EQ(AccessTransportTracker::ToString(), ExpectedTransport());
    AssertValue(key, value);
    HostPort primaryWorker;
    DS_ASSERT_OK(QueryPrimaryWorker(key, primaryWorker));
    HostPort expectedWorker;
    DS_ASSERT_OK(cluster_->GetWorkerAddr(READER_WORKER_INDEX, expectedWorker));
    ASSERT_EQ(primaryWorker, expectedWorker);
}

TEST_F(KVClientTransportSetTest, LocalCacheEnabledSetUsesConnectedWorker)
{
    std::string key;
    DS_ASSERT_OK(FindRouteKeyToWorker(READER_WORKER_INDEX, "transport_set_local_", key));
    const std::string value(VALUE_SIZE, 'l');

    DS_ASSERT_OK(localClient_->Set(key, value));

    AssertValue(key, value);
    HostPort primaryWorker;
    DS_ASSERT_OK(QueryPrimaryWorker(key, primaryWorker));
    HostPort expectedWorker;
    DS_ASSERT_OK(cluster_->GetWorkerAddr(ROUTED_CLIENT_WORKER_INDEX, expectedWorker));
    ASSERT_EQ(primaryWorker, expectedWorker);
}

TEST_F(KVClientTransportSetTest, ScaleDownPublishReroutesWholeTransaction)
{
    std::string key;
    DS_ASSERT_OK(FindRouteKeyToWorker(READER_WORKER_INDEX, "transport_set_scale_down_", key));
    HostPort firstWorker;
    DS_ASSERT_OK(cluster_->GetWorkerAddr(READER_WORKER_INDEX, firstWorker));
    DS_ASSERT_OK(inject::Set(PUBLISH_INJECT, "1*return(K_SCALE_DOWN)"));
    const std::string value(VALUE_SIZE, 's');

    DS_ASSERT_OK(routedClient_->Set(key, value));

    HostPort reroutedWorker;
    DS_ASSERT_OK(QueryPrimaryWorker(key, reroutedWorker));
    ASSERT_NE(reroutedWorker, firstWorker);
    AssertValue(key, value);
}

TEST_F(KVClientTransportSetTest, AmbiguousPublishFailureIsNotReplayedOnAnotherWorker)
{
    std::string key;
    DS_ASSERT_OK(FindRouteKeyToWorker(READER_WORKER_INDEX, "transport_set_publish_failure_", key));
    HostPort firstWorker;
    DS_ASSERT_OK(cluster_->GetWorkerAddr(READER_WORKER_INDEX, firstWorker));
    DS_ASSERT_OK(inject::Set(PUBLISH_INJECT, "2*return(K_RPC_UNAVAILABLE)"));
    const std::string value(VALUE_SIZE, 'f');

    Status rc = routedClient_->Set(key, value);

    ASSERT_EQ(rc.GetCode(), K_RPC_UNAVAILABLE) << rc.ToString();
    HostPort unexpectedPrimary;
    ASSERT_EQ(QueryPrimaryWorker(key, unexpectedPrimary).GetCode(), K_NOT_FOUND);

    DS_ASSERT_OK(routedClient_->Set(key, value));
    HostPort retryWorker;
    DS_ASSERT_OK(QueryPrimaryWorker(key, retryWorker));
    ASSERT_NE(retryWorker, firstWorker);
    AssertValue(key, value);
}
}  // namespace st
}  // namespace datasystem
