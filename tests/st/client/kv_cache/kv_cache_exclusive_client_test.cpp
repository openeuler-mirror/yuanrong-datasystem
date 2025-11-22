/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
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
 * Description: Test kv client exclusive function
 */

#include "client/object_cache/oc_client_common.h"

namespace datasystem {
namespace st {
class KVCacheExclusiveClientTest : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = 1;
        opts.numEtcd = 1;
        opts.workerGflagParams = "-node_timeout_s=5 -shared_memory_size_mb=2048 -v=2";
    }

    void SetUp() override
    {
        ExternalClusterTest::SetUp();
        InitTestKVClient(0, client1_, timeoutMs_, false, true);
    }

    void TearDown() override
    {
        client1_.reset();
        ExternalClusterTest::TearDown();
    }

protected:
    std::shared_ptr<KVClient> client1_;
    const int timeoutMs_ = 2'000;
};

TEST_F(KVCacheExclusiveClientTest, GetTimeout)
{
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "worker.Get.delay", "sleep(3000)"));
    DS_ASSERT_OK(client1_->Set("key1", "value1"));
    std::string val;
    DS_ASSERT_NOT_OK(client1_->Get("key1", val));
}

}
}