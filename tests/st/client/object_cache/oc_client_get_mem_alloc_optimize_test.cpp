/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2024. All rights reserved.
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
#include <vector>
#include <gtest/gtest.h>
#include "common.h"
#include "datasystem/common/util/wait_post.h"
#include "datasystem/object_cache/object_client.h"
#include "datasystem/utils/status.h"
#include "oc_client_common.h"
#include "datasystem/client/client_worker_common_api.h"

namespace datasystem {
namespace st {
class OCClientGetMemAllocOptimizeTest : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        const char *hostIp = "127.0.0.1";
        opts.numWorkers = 1;
        opts.numEtcd = 1;
        int port = GetFreePort();
        opts.workerConfigs.emplace_back(hostIp, port);
        workerHostPort_ = HostPort(hostIp, port);
        opts.workerGflagParams =
            " -shared_memory_size_mb=64 -v=1 -ipc_through_shared_memory=true"
            " -enable_huge_tlb=true -enable_fallocate=false";
        signature_ = std::make_unique<Signature>(accessKey_, secretKey_);
    }

protected:
    HostPort workerHostPort_;
    std::string accessKey_ = "QTWAOYTTINDUT2QVKYUC";
    std::string secretKey_ = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
    std::unique_ptr<Signature> signature_;
};

TEST_F(OCClientGetMemAllocOptimizeTest, DISABLED_GetLocalTest)
{
    std::shared_ptr<ObjectClient> cliLocal;
    InitTestClient(0, cliLocal);
    std::shared_ptr<ObjectClient> cli2Local;
    InitTestClient(0, cli2Local);
    std::shared_ptr<ObjectClient> cli3Local;
    InitTestClient(0, cli3Local);

    std::string objKey = NewObjectKey();
    std::vector<uint8_t> data = { 65, 66, 67, 68, 69, 70 };
    CreateAndSealObject(cliLocal, objKey, data);

    // get exist id data
    std::vector<Optional<Buffer>> dataList;
    DS_ASSERT_OK(cliLocal->Get({ objKey }, 0, dataList));
    ASSERT_TRUE(NotExistsNone(dataList));
    ASSERT_EQ(dataList[0]->GetSize(), static_cast<int>(data.size()));
    ASSERT_NE(dataList[0]->ImmutableData(), nullptr);
    AssertBufferEqual(*dataList[0], std::string{ 65, 66, 67, 68, 69, 70 });
    std::string notExistObjKey = NewObjectKey();
    std::vector<Optional<Buffer>> dataListNotExist(1);
    DS_ASSERT_NOT_OK(cliLocal->Get({ notExistObjKey }, 0, dataListNotExist));
    int timeoutMs = 1000;
    DS_ASSERT_NOT_OK(cliLocal->Get({ notExistObjKey }, timeoutMs, dataListNotExist));
    std::string notSealObjKey = NewObjectKey();
    CreateObject(cliLocal, notSealObjKey, data);
    std::vector<Optional<Buffer>> dataListNotSeal;
    DS_ASSERT_NOT_OK(cliLocal->Get({ notSealObjKey }, 0, dataListNotSeal));
    // get not seal data in different client
    std::vector<Optional<Buffer>> dataListNotSealDiffCli(1);
    DS_ASSERT_NOT_OK(cli2Local->Get({ notSealObjKey }, 0, dataListNotSealDiffCli));
}

TEST_F(OCClientGetMemAllocOptimizeTest, DISABLED_RegisterClient)
{
    using datasystem::client::ClientWorkerCommonApi;
    auto workerApi = std::make_shared<ClientWorkerCommonApi>(
        workerHostPort_, RpcCredential(), HeartbeatType::RPC_HEARTBEAT, signature_.get(), "", true);
    ASSERT_TRUE(workerApi->IsEnableHugeTlb());
}
}  // namespace st
}  // namespace datasystem