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
 * Description: Test ClientWorkerCommonApi class.
 */

#include "datasystem/client/client_worker_common_api.h"
#include "datasystem/client/object_cache/client_worker_api.h"
#include "datasystem/protos/object_posix.stub.rpc.pb.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/shared_memory/shm_unit.h"

#include "common.h"

using namespace datasystem::client;
using namespace datasystem::object_cache;

namespace datasystem {
namespace st {
class ClientCommonWorkerApiTest : public ExternalClusterTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        const char *hostIp = "127.0.0.1";
        opts.numWorkers = 1;
        opts.numEtcd = 1;
        int port = GetFreePort();
        opts.workerConfigs.emplace_back(hostIp, port);
        workerHostPort_ = HostPort(hostIp, port);
        opts.workerGflagParams = " -shared_memory_size_mb=64 -v=1 -ipc_through_shared_memory=true";
        signature_ = std::make_unique<Signature>(accessKey_, secretKey_);
    }
    HostPort workerHostPort_;
    std::string accessKey_ = "QTWAOYTTINDUT2QVKYUC";
    std::string secretKey_ = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
    std::unique_ptr<Signature> signature_;
};

TEST_F(ClientCommonWorkerApiTest, TestGetClientFd)
{
    int timeoutMs = 5000;
    auto clientApi = std::make_shared<ClientWorkerRemoteApi>(workerHostPort_, RpcCredential(),
                                                             HeartbeatType::RPC_HEARTBEAT,    "", signature_.get());
    DS_ASSERT_OK(clientApi->Init(timeoutMs, timeoutMs));
    ASSERT_TRUE(clientApi->shmEnabled_);

    int64_t dataSize = 10;
    uint32_t version;
    uint64_t metaSize = 0;
    auto shmUnitInfo = std::make_shared<ShmUnitInfo>();
    clientApi->Create("objectKey", dataSize, version, metaSize, shmUnitInfo);

    std::vector<int> workerFds{ shmUnitInfo->fd };
    std::vector<int> clientFds;
    DS_ASSERT_OK(clientApi->GetClientFd(workerFds, clientFds, ""));
    ASSERT_TRUE(clientFds.size() == workerFds.size());
    int invalidFd = -1;
    for (auto fd : clientFds) {
        ASSERT_TRUE(fd != invalidFd);
    }
}

TEST_F(ClientCommonWorkerApiTest, TestGetClientFdInvalid)
{
    int timeoutMs = 5000;
    auto clientApi = std::make_shared<ClientWorkerRemoteApi>(
        workerHostPort_, RpcCredential(), HeartbeatType::RPC_HEARTBEAT, "", signature_.get(), "tenant1");
    DS_ASSERT_OK(clientApi->Init(timeoutMs, timeoutMs));
    ASSERT_TRUE(clientApi->shmEnabled_);

    int64_t dataSize = 10;
    uint32_t version;
    uint64_t metaSize = 0;
    auto shmUnitInfo = std::make_shared<ShmUnitInfo>();
    DS_ASSERT_OK(clientApi->Create("objectKey", dataSize, version, metaSize, shmUnitInfo));

    auto clientApi2 = std::make_shared<ClientWorkerRemoteApi>(
        workerHostPort_, RpcCredential(), HeartbeatType::RPC_HEARTBEAT, "", signature_.get(), "tenant2");
    DS_ASSERT_OK(clientApi2->Init(timeoutMs, timeoutMs));
    std::vector<int> workerFds{ shmUnitInfo->fd };
    std::vector<int> clientFds;
    auto rc = clientApi2->GetClientFd(workerFds, clientFds, "");
    DS_ASSERT_NOT_OK(rc);
}

TEST_F(ClientCommonWorkerApiTest, TestGetMultiClientFd)
{
    int timeoutMs = 5000;
    auto clientApi = std::make_shared<ClientWorkerRemoteApi>(workerHostPort_, RpcCredential(),
                                                             HeartbeatType::RPC_HEARTBEAT, "", signature_.get());
    DS_ASSERT_OK(clientApi->Init(timeoutMs, timeoutMs));
    ASSERT_TRUE(clientApi->shmEnabled_);

    int64_t dataSize = 10;
    uint32_t version;
    uint64_t metaSize = 0;
    auto shm1 = std::make_shared<ShmUnitInfo>();
    auto shm2 = std::make_shared<ShmUnitInfo>();
    auto shm3 = std::make_shared<ShmUnitInfo>();
    DS_ASSERT_OK(clientApi->Create("objectKey1", dataSize, version, metaSize, shm1));
    DS_ASSERT_OK(clientApi->Create("objectKey2", dataSize, version, metaSize, shm2));
    DS_ASSERT_OK(clientApi->Create("objectKey3", dataSize, version, metaSize, shm3));

    std::vector<int> workerFds{ shm1->fd, shm2->fd, shm3->fd };
    std::vector<int> clientFds;
    DS_ASSERT_OK(clientApi->GetClientFd(workerFds, clientFds, ""));
    ASSERT_TRUE(clientFds.size() == workerFds.size());
    int invalidFd = -1;
    for (auto fd : clientFds) {
        ASSERT_TRUE(fd != invalidFd);
    }
}
}  // namespace st
}  // namespace datasystem
