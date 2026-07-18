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
 * Description: ST that validates the end-to-end client-deadline propagation
 * through brpc.  Requires brpc mode (--use_brpc=true on the test binary).
 * Injects a 300ms master-side delay and issues a Set with a 20ms request
 * budget from worker 1 (master on worker 0, non-distributed).
 *
 * If deadline propagation is working (the fix in brpc_service_generator.cpp +
 * baidu_std_protocol_deliver_timeout_ms), the master RPC sees ~20ms and the
 * Set returns K_RPC_DEADLINE_EXCEEDED because the master delay (300ms) far
 * exceeds the propagated budget (20ms).  If propagation is broken (e.g. the
 * 60s default sneaks in), the Set runs the full 300ms master delay and
 * returns OK — the assertion fails, catching the regression.
 *
 * Important: the test forces workers AND the client onto brpc. Without
 * --use_brpc=true at the test-binary level, ZMQ is the default transport
 * and the ST validates the wrong code path (ZMQ does not exercise the
 * brpc_service_generator changes at all).
 */
#include <memory>
#include <string>

#include "common.h"
#include "client/object_cache/oc_client_common.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/kv_client.h"
#include "datasystem/utils/status.h"

DS_DECLARE_bool(use_brpc);

namespace datasystem {
namespace st {

class WorkerCreateMetaDeadlineDiagTest : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numEtcd = 1;
        opts.numWorkers = 2;
        // Non-distributed master: master lives on worker 0. A Set issued via
        // worker 1 whose object key routes to the worker-0 master takes the
        // Remote RPC path (WorkerRemoteMasterOCApi::CreateMeta).
        opts.enableDistributedMaster = "false";
        opts.masterIdx = 0;
        opts.waitWorkerReady = true;
        // Force brpc transport (external_cluster.cpp:1137 reads FLAGS_use_brpc).
        // The ZMQ default would silently skip the generator fix under test.
        FLAGS_use_brpc = true;
        opts.workerGflagParams = "-use_brpc=true -shared_memory_size_mb=1024 -v=2";
    }

    void SetUp() override
    {
        CommonTest::SetUp();
        DS_ASSERT_OK(Init());
        ASSERT_TRUE(cluster_ != nullptr);
        DS_ASSERT_OK(cluster_->StartEtcdCluster());
        DS_ASSERT_OK(cluster_->StartWorkers());
        for (size_t i = 0; i < 2; i++) {
            DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, i));
        }
        // Client on worker 1, request budget 20ms (mirrors production config).
        // FLAGS_use_brpc was set true in SetClusterSetupOptions (called by Init),
        // so the KVClient picks up brpc transport.
        InitTestKVClient(1, client_, 60000, false, 20);
    }

    void TearDown() override
    {
        client_.reset();
        ExternalClusterTest::TearDown();
    }

protected:
    std::shared_ptr<KVClient> client_;
};

// Reproduce: master CreateMeta delayed 300ms + client 20ms request budget.
// With correct deadline propagation the Set must time out at ~20ms.
// Without propagation the master RPC runs with 60s default, Set returns OK
// after the 300ms master delay — and the assertion catches the regression.
TEST_F(WorkerCreateMetaDeadlineDiagTest, SlowMasterSetTimesOutAtClientBudget)
{
    // Inject 300ms delay into the worker-0 master CreateMeta path.
    // Inject 300ms delay at MasterOCServiceImpl::CreateMeta entry (line 146),
    // which is hit by every CreateMeta call including single Set.
    // Note: master.CreateMeta.delay (oc_metadata_manager.cpp:584) is inside
    // CreatePendingMeta() — only reaches the MSET/batch path, NOT single Set.
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "master.CreateMeta.begin", "1*sleep(300)"));

    std::string key = "deadline_diag_" + RandomData().GetRandomString(10);
    std::string val = "v";
    datasystem::SetParam param;
    param.writeMode = datasystem::WriteMode::NONE_L2_CACHE;
    Status rc = client_->Set(key, val, param);
    LOG(INFO) << "Set with 20ms budget against 300ms master delay: rc=" << rc.ToString();

    // If deadline propagation works (brpc_service_generator fix +
    // baidu_std_protocol_deliver_timeout_ms), the 20ms client budget reaches
    // the master RPC and times out before the 300ms master delay completes.
    ASSERT_EQ(rc.GetCode(), StatusCode::K_RPC_DEADLINE_EXCEEDED)
        << "Expected Set to time out at the 20ms client budget, but got "
        << rc.ToString()
        << ". The client deadline may not have propagated to the master RPC "
           "(brpc_service_generator.cntl->timeout_ms() still UNSET_MAGIC_NUM? "
           "baidu_std_protocol_deliver_timeout_ms not enabled?).";

    // Clear the inject so teardown is clean.
    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, 0, "master.CreateMeta.begin"));
}

}  // namespace st
}  // namespace datasystem
