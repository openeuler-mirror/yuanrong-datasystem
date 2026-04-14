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

#include <gtest/gtest.h>

#include <memory>
#include <string>

#include "common.h"
#include "datasystem/common/ak_sk/signature.h"
#include "datasystem/common/util/version.h"
#include "datasystem/common/rpc/rpc_auth_keys.h"
#include "datasystem/common/rpc/rpc_auth_key_manager.h"
#include "datasystem/common/rpc/rpc_channel.h"
#include "datasystem/protos/share_memory.pb.h"
#include "datasystem/protos/share_memory.stub.rpc.pb.h"

namespace datasystem {
namespace st {

const std::string WORKER_SERVER_NAME = "worker";

class RegisterClientNonShmRspTest : public ExternalClusterTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = 1;
        opts.numEtcd = 1;
        opts.workerGflagParams = " -shared_memory_size_mb=64 -v=1 -ipc_through_shared_memory=false";
    }

    void SetUp() override
    {
        ExternalClusterTest::SetUp();

        HostPort workerAddr;
        DS_ASSERT_OK(cluster_->GetWorkerAddr(0, workerAddr));

        RpcCredential cred;
        RpcAuthKeyManager::CreateClientCredentials(authKeys_, WORKER_SERVER_NAME, cred);
        channel_ = std::make_shared<RpcChannel>(workerAddr, cred);
        stub_ = std::make_unique<WorkerService_Stub>(channel_);
    }

protected:
    RpcAuthKeys authKeys_;
    std::shared_ptr<RpcChannel> channel_;
    std::unique_ptr<WorkerService_Stub> stub_;
};

TEST_F(RegisterClientNonShmRspTest, RegisterClientReturnsNonShmFields)
{
    const std::string accessKey = "QTWAOYTTINDUT2QVKYUC";
    const SensitiveValue secretKey = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
    Signature signature(accessKey, secretKey);

    RegisterClientReqPb req;
    RegisterClientRspPb rsp;

    req.set_server_fd(-1);
    req.set_version(DATASYSTEM_VERSION);
    req.set_git_hash(GetGitHash());
    req.set_heartbeat_enabled(false);
    req.set_shm_enabled(false);
    req.set_tenant_id("");
    req.set_enable_cross_node(false);
    req.set_pod_name("st");
    req.set_enable_exclusive_connection(false);
    req.set_support_multi_shm_ref_count(false);

    DS_ASSERT_OK(signature.GenerateSignature(req));
    DS_ASSERT_OK(stub_->RegisterClient(req, rsp));

    ASSERT_EQ(rsp.lock_id(), 0u);
    ASSERT_EQ(rsp.store_fd(), -1);
    ASSERT_EQ(rsp.mmap_size(), 0u);
    ASSERT_EQ(rsp.offset(), 0u);
    ASSERT_TRUE(rsp.shm_id().empty());
}

}  // namespace st
}  // namespace datasystem

