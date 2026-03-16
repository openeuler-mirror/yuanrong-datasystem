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
 * Description: State client exist tests.
 */
#include <cstdio>
#include <unistd.h>
#include <memory>
#include <cstdint>
#include <string>

#include <gtest/gtest.h>
#include <tbb/concurrent_hash_map.h>

#include "client/object_cache/oc_client_common.h"
#include "common.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/kv/read_only_buffer.h"
#include "datasystem/kv_client.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/log/log.h"
#include "datasystem/utils/status.h"

DS_DECLARE_bool(log_monitor);

namespace datasystem {
namespace st {
constexpr int WAIT_ASYNC_NOTIFY_WORKER = 300;
class KvNetworkErrorTest : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        const uint32_t numWorkers = 2;
        opts.numEtcd = 1;
        opts.numWorkers = numWorkers;
        opts.workerGflagParams = " -shared_memory_size_mb=100 -authorization_enable=true ";
        opts.systemAccessKey = accessKey_;
        opts.systemSecretKey = secretKey_;
    }

    void SetUp() override
    {
        ExternalClusterTest::SetUp();
        FLAGS_log_monitor = true;
        InitClients();
    }

    void TearDown() override
    {
        client_.reset();
        client1_.reset();
        ExternalClusterTest::TearDown();
    }

    void InitClients()
    {
        InitTestKVClient(0, client_,
                         [&](ConnectOptions &opts) { opts.SetAkSkAuth(accessKey_, secretKey_, tenantId_); });
        InitTestKVClient(0, client1_,
                         [&](ConnectOptions &opts) { opts.SetAkSkAuth(accessKey_, secretKey_, tenantId1_); });
        InitTestKVClient(1, client2_,
                         [&](ConnectOptions &opts) { opts.SetAkSkAuth(accessKey_, secretKey_, tenantId2_); });
    }

    std::shared_ptr<KVClient> client_;
    std::shared_ptr<KVClient> client1_;
    std::shared_ptr<KVClient> client2_;
    std::string tenantId_ = "client0";
    std::string tenantId1_ = "client1";
    std::string tenantId2_ = "client0";

protected:
    std::string accessKey_ = "QTWAOYTTINDUT2QVKYUC";
    std::string secretKey_ = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
};
TEST_F(KvNetworkErrorTest, DISABLED_BlockMasterAndWorker)
{
    HostPort hostPort;
    cluster_->GetWorkerAddr(0, hostPort);
    auto blockCmd =
        "nft add table ip filter"
        "&& nft add chain ip filter input { type filter hook input priority 0 \\;} "
        "&& nft add chain ip filter output { type filter hook output priority 0 \\;} "
        + FormatString(" && nft add rule ip filter input ip protocol tcp tcp dport %d drop ", hostPort.Port())
        + FormatString("&& nft add rule ip filter output ip protocol tcp tcp dport %d drop ", hostPort.Port());
    ;
    auto key = client_->GenerateKey("key1");
    DS_ASSERT_OK(client_->Set(key, "val"));
    ExecuteCmd(blockCmd);
    Raii raii([]() { ExecuteCmd("nft flush ruleset"); });
    std::string val;
    DS_ASSERT_TRUE(client2_->Get(key, val).GetCode(), K_RPC_UNAVAILABLE);
}
}  // namespace st
}  // namespace datasystem