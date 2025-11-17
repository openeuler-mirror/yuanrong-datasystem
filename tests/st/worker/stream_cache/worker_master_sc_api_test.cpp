/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2022. All rights reserved.
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

#include "datasystem/worker/stream_cache/worker_master_sc_api.h"

#include "common.h"

using datasystem::worker::stream_cache::WorkerMasterSCApi;

namespace datasystem {
namespace st {
class WorkerMasterSCApiTest : public ExternalClusterTest {
public:
    void SetUp() override
    {
        ClusterTest::SetUp();
        InitWorkerMasterSCApi();
    }

    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = 1;
        opts.numEtcd = 1;
        opts.isStreamCacheCase = true;
    }

protected:
    void InitWorkerMasterSCApi()
    {
        HostPort metaAddr;
        DS_ASSERT_OK(cluster_->GetMetaServerAddr(metaAddr));
        akSkManager_ = std::make_shared<AkSkManager>(0);
        akSkManager_->SetClientAkSk(accessKey_, secretKey_);
        int stubCacheNum = 100;
        RpcStubCacheMgr::Instance().Init(stubCacheNum);
        workerMasterSCApi_ =
            WorkerMasterSCApi::CreateWorkerMasterSCApi(metaAddr, HostPort("127.0.0.1", 18888), akSkManager_);
    }

    std::shared_ptr<WorkerMasterSCApi> workerMasterSCApi_;
    std::string accessKey_ = "QTWAOYTTINDUT2QVKYUC";
    std::string secretKey_ = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
    std::shared_ptr<AkSkManager> akSkManager_;
};

TEST_F(WorkerMasterSCApiTest, TestWorkerMasterDiffIp)
{
    DS_ASSERT_OK(workerMasterSCApi_->Init());
}
}  // namespace st
}  // namespace datasystem
