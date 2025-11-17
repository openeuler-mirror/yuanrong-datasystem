/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2023. All rights reserved.
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
 * Description: common class of stream client test
 */

#ifndef DATASYSTEM_UT_SC_CLIENT_COMMON_H
#define DATASYSTEM_UT_SC_CLIENT_COMMON_H

#include "common.h"
#include "datasystem/stream_client.h"
namespace datasystem {
namespace st {
class SCClientCommon : public ExternalClusterTest {
public:
protected:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        for (size_t i = 0; i < opts.numWorkers; ++i) {
            auto port = GetFreePort();
            opts.workerSpecifyGflagParams.emplace(i, FormatString("-sc_worker_worker_direct_port=%d", port));
        }
        opts.isStreamCacheCase = true;
    }

    void InitStreamClient(uint32_t index, std::shared_ptr<StreamClient> &client, int32_t timeoutMs = 60000,
                          bool reportWorkerLost = false)
    {
        HostPort workerAddress;
        ASSERT_TRUE(index < cluster_->GetWorkerNum());
        DS_ASSERT_OK(cluster_->GetWorkerAddr(index, workerAddress));
        LOG(INFO) << "worker index " << index << ": " << workerAddress.ToString();
        ConnectOptions connectOptions;
        connectOptions = { .host = workerAddress.Host(), .port = workerAddress.Port(), .connectTimeoutMs = timeoutMs };
        connectOptions.accessKey = "QTWAOYTTINDUT2QVKYUC";
        connectOptions.secretKey = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
        client = std::make_shared<StreamClient>(connectOptions);
        DS_ASSERT_OK(client->Init(reportWorkerLost));
    }

private:
};
}  // namespace st
}  // namespace datasystem
#endif  // DATASYSTEM_UT_SC_CLIENT_COMMON_H
