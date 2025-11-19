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

#include "datasystem/common/rpc/rpc_stub_cache_mgr.h"
#include "datasystem/master/stream_cache/master_worker_sc_api.h"

#include "common.h"

using datasystem::master::MasterWorkerSCApi;

namespace datasystem {
namespace st {
class MasterWorkerSCApiTest : public ExternalClusterTest {
public:
    void SetUp() override
    {
        std::shared_ptr<AkSkManager> akSkManager_ = std::make_shared<AkSkManager>(0);
        akSkManager_->SetClientAkSk(accessKey_, secretKey_);
        ClusterTest::SetUp();
        InitMasterWorkerSCApi();
    }

    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = 1;
        opts.numEtcd = 1;
        opts.isStreamCacheCase = true;
    }

protected:
    void InitMasterWorkerSCApi()
    {
        HostPort metaAddr;
        DS_ASSERT_OK(cluster_->GetMetaServerAddr(metaAddr));
        HostPort localHost("127.0.0.2", 18888);
        int stubCacheNum = 100;
        RpcStubCacheMgr::Instance().Init(stubCacheNum);
        masterWorkerSCApi_ = MasterWorkerSCApi::CreateMasterWorkerSCApi(localHost, localHost, akSkManager_, nullptr);
    }

    std::shared_ptr<MasterWorkerSCApi> masterWorkerSCApi_;
    std::string accessKey_ = "QTWAOYTTINDUT2QVKYUC";
    std::string secretKey_ = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
    std::shared_ptr<AkSkManager> akSkManager_{ nullptr };
};

TEST_F(MasterWorkerSCApiTest, TestMasterWorkerDiffIp)
{
    DS_ASSERT_OK(masterWorkerSCApi_->Init());
}
}  // namespace st
}  // namespace datasystem
