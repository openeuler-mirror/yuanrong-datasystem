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
 * Description: End-to-end brpc dead-peer fast-fail tests.
 */
#include <cerrno>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>

#include "common.h"
#include "datasystem/client/object_cache/client_worker_api/client_worker_remote_api.h"
#include "datasystem/common/ak_sk/ak_sk_manager.h"
#include "datasystem/common/ak_sk/signature.h"
#include "datasystem/common/flags/common_flags.h"
#include "datasystem/common/rpc/api_deadline.h"
#include "datasystem/common/rpc/rpc_stub_cache_mgr.h"
#include "datasystem/common/util/file_util.h"
#include "datasystem/common/util/request_context.h"
#include "datasystem/common/util/rpc_util.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/protos/master_object.pb.h"
#include "datasystem/protos/object_posix.pb.h"
#include "datasystem/worker/object_cache/worker_master_oc_api.h"

using datasystem::object_cache::ClientWorkerRemoteApi;
using datasystem::worker::WorkerMasterOCApi;

namespace datasystem {
namespace st {
namespace {
constexpr uint32_t kMasterWorkerIndex = 0;
constexpr uint32_t kPeerWorkerIndex = 1;
constexpr int32_t kRpcBudgetMs = 10'000;
constexpr int64_t kFastFailMaxMs = kRpcBudgetMs / 3;
constexpr int kStubCacheNum = 100;
// Test-only dummy credentials, not valid for any environment. Mirrors the fixtures
// used across the ST suite (ak_sk_manager needs non-empty values to sign requests).
const std::string kAccessKey = "QTWAOYTTINDUT2QVKYUC";
const std::string kSecretKey = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
constexpr char kWorkerBinEnv[] = "DATASYSTEM_WORKER_BIN";

Status UseBazelBuiltWorker()
{
    const char *srcdir = std::getenv("TEST_SRCDIR");
    const char *workspace = std::getenv("TEST_WORKSPACE");
    if (srcdir == nullptr || workspace == nullptr) {
        return Status::OK();
    }
    std::string workerBin = std::string(srcdir) + "/" + workspace + "/src/datasystem/worker/datasystem_worker";
    CHECK_FAIL_RETURN_STATUS(FileExist(workerBin), K_NOT_FOUND, "Cannot find Bazel-built worker: " + workerBin);
    CHECK_FAIL_RETURN_STATUS(setenv(kWorkerBinEnv, workerBin.c_str(), 1) == 0, K_RUNTIME_ERROR,
                             std::string("Set ") + kWorkerBinEnv + " failed: " + std::strerror(errno));
    return Status::OK();
}
}  // namespace

class BrpcDeadPeerFastFailTest : public ExternalClusterTest {
public:
    void SetUp() override
    {
        const char *workerBin = std::getenv(kWorkerBinEnv);
        hadOldWorkerBinOverride_ = workerBin != nullptr;
        oldWorkerBinOverride_ = hadOldWorkerBinOverride_ ? workerBin : "";
        DS_ASSERT_OK(UseBazelBuiltWorker());
        signature_ = std::make_unique<Signature>(kAccessKey, kSecretKey);
        ClusterTest::SetUp();
    }

    void TearDown() override
    {
        ClusterTest::TearDown();
        if (hadOldWorkerBinOverride_) {
            (void)setenv(kWorkerBinEnv, oldWorkerBinOverride_.c_str(), 1);
        } else {
            (void)unsetenv(kWorkerBinEnv);
        }
    }

    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = 2;
        opts.numEtcd = 1;
        opts.masterIdx = static_cast<int32_t>(kMasterWorkerIndex);
        opts.workerGflagParams = " -shared_memory_size_mb=128 -v=1";
    }

protected:
    std::shared_ptr<ClientWorkerRemoteApi> CreateClientWorkerApi(uint32_t workerIndex)
    {
        HostPort workerAddr;
        Status rc = cluster_->GetWorkerAddr(workerIndex, workerAddr);
        if (rc.IsError()) {
            ADD_FAILURE() << rc.ToString();
            return nullptr;
        }
        auto api = std::make_shared<ClientWorkerRemoteApi>(
            workerAddr, RpcCredential(), HeartbeatType::RPC_HEARTBEAT, "", signature_.get());
        rc = api->Init(kRpcBudgetMs, kRpcBudgetMs);
        if (rc.IsError()) {
            ADD_FAILURE() << rc.ToString();
            return nullptr;
        }
        return api;
    }

    std::shared_ptr<WorkerMasterOCApi> CreateWorkerMasterApi()
    {
        HostPort masterAddr;
        HostPort localAddr;
        Status rc = cluster_->GetWorkerAddr(kMasterWorkerIndex, masterAddr);
        if (rc.IsError()) {
            ADD_FAILURE() << rc.ToString();
            return nullptr;
        }
        rc = cluster_->GetWorkerAddr(kPeerWorkerIndex, localAddr);
        if (rc.IsError()) {
            ADD_FAILURE() << rc.ToString();
            return nullptr;
        }

        auto akSkManager = std::make_shared<AkSkManager>(0);
        akSkManager->SetClientAkSk(kAccessKey, kSecretKey);
        rc = RpcStubCacheMgr::Instance().Init(kStubCacheNum, localAddr);
        if (rc.IsError()) {
            ADD_FAILURE() << rc.ToString();
            return nullptr;
        }
        auto api = WorkerMasterOCApi::CreateWorkerMasterOCApi(masterAddr, localAddr, akSkManager);
        rc = api->Init();
        if (rc.IsError()) {
            ADD_FAILURE() << rc.ToString();
            return nullptr;
        }
        return api;
    }

    bool oldUseBrpc_ = true;
    bool hadOldWorkerBinOverride_ = false;
    std::string oldWorkerBinOverride_;
    std::unique_ptr<Signature> signature_;
};

TEST_F(BrpcDeadPeerFastFailTest, ClientWorkerCreateFastFailsAfterWorkerKilled)
{
    auto clientApi = CreateClientWorkerApi(kPeerWorkerIndex);
    ASSERT_NE(clientApi, nullptr);
    DS_ASSERT_OK(cluster_->KillWorker(kPeerWorkerIndex));

    ScopedRequestContext requestCtx;
    ApiDeadlineGuard deadline(kRpcBudgetMs);
    GetRequestContext()->reqTimeoutDuration.Init(kRpcBudgetMs);
    uint32_t version = 0;
    uint64_t metadataSize = 0;
    auto shmUnitInfo = std::make_shared<ShmUnitInfo>();
    std::shared_ptr<UrmaRemoteAddrPb> dummy;

    Timer timer;
    Status status = clientApi->Create(NewObjectKey(), 8, version, metadataSize, shmUnitInfo, dummy);
    int64_t elapsedMs = timer.ElapsedMilliSecond();

    ASSERT_EQ(status.GetCode(), StatusCode::K_RPC_PEER_DEAD) << status.ToString();
    ASSERT_LT(elapsedMs, kFastFailMaxMs) << status.ToString();
}

TEST_F(BrpcDeadPeerFastFailTest, WorkerMasterCreateMetaFastFailsAfterMasterKilled)
{
    auto workerMasterApi = CreateWorkerMasterApi();
    ASSERT_NE(workerMasterApi, nullptr);
    HostPort localAddr;
    DS_ASSERT_OK(cluster_->GetWorkerAddr(kPeerWorkerIndex, localAddr));
    DS_ASSERT_OK(cluster_->KillWorker(kMasterWorkerIndex));

    master::CreateMetaReqPb req;
    master::CreateMetaRspPb rsp;
    req.set_address(localAddr.ToString());
    req.mutable_meta()->set_object_key(NewObjectKey());
    req.mutable_meta()->set_data_size(8);

    ScopedRequestContext requestCtx;
    ApiDeadlineGuard deadline(kRpcBudgetMs);
    GetRequestContext()->reqTimeoutDuration.Init(kRpcBudgetMs);

    Timer timer;
    Status status = workerMasterApi->CreateMeta(req, rsp);
    int64_t elapsedMs = timer.ElapsedMilliSecond();

    ASSERT_EQ(status.GetCode(), StatusCode::K_RPC_PEER_DEAD) << status.ToString();
    ASSERT_LT(elapsedMs, kFastFailMaxMs) << status.ToString();
}
}  // namespace st
}  // namespace datasystem
