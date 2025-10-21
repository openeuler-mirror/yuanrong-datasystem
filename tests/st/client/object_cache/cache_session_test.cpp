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

/**
 * Description:
 */
#include "oc_client_common.h"

namespace datasystem {
namespace st {
class CacheSessionTest : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        int num = 4;
        opts.numWorkers = num;
        opts.workerGflagParams = " -cache_rpc_session=false ";
        opts.masterGflagParams = " -cache_rpc_session=false ";
        opts.numEtcd = 1;
    }
};

TEST_F(CacheSessionTest, TestSessionCount)
{
    // Notice: This testcase just for coverage.
    std::shared_ptr<KVClient> client1;
    std::shared_ptr<KVClient> client2;
    std::shared_ptr<KVClient> client3;
    std::shared_ptr<KVClient> client4;
    InitTestKVClient(0, client1);
    InitTestKVClient(1, client2);
    InitTestKVClient(2, client3);
    InitTestKVClient(3, client4);

    SetParam param{ .writeMode = WriteMode::NONE_L2_CACHE };
    ASSERT_EQ(client1->Set("key1", "value", param), Status::OK());
    ASSERT_EQ(client2->Set("key2", "value", param), Status::OK());
    ASSERT_EQ(client3->Set("key3", "value", param), Status::OK());
    ASSERT_EQ(client4->Set("key4", "value", param), Status::OK());

    std::vector<std::string> vals;
    ASSERT_EQ(client4->Get({ "key1", "key2", "key3", "key4" }, vals), Status::OK());

    std::vector<std::string> failedIds;
    ASSERT_EQ(client4->Del({ "key1", "key2", "key3", "key4" }, failedIds), Status::OK());
    ASSERT_TRUE(failedIds.empty());
}
}  // namespace st
}  // namespace datasystem
