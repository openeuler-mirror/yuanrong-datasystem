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
 * Description: KV client initialization tests.
 */
#include <memory>

#include <gtest/gtest.h>

#include "client/object_cache/oc_client_common.h"
#include "common.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/kv_client.h"
#include "datasystem/utils/connection.h"

namespace datasystem {
namespace st {
class KVClientInitTest : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numEtcd = 1;
        opts.numWorkers = 1;
        opts.workerGflagParams = " -shared_memory_size_mb=100 -client_dead_timeout_s=1 ";
    }

    void TearDown() override
    {
        client_.reset();
        ExternalClusterTest::TearDown();
    }

protected:
    std::shared_ptr<KVClient> client_;
};

TEST_F(KVClientInitTest, FastTransportFailureFallsBack)
{
    ConnectOptions connectOptions;
    InitConnectOpt(0, connectOptions, 5000);
    client_ = std::make_shared<KVClient>(connectOptions);

    DS_ASSERT_OK(inject::Set("FastTransportManager.Initialize", "return(3000)"));

    auto status = client_->Init();
    ASSERT_EQ(status.GetCode(), StatusCode::K_URMA_ERROR);
}
}  // namespace st
}  // namespace datasystem
