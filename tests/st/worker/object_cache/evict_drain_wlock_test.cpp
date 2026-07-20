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
 * Description: ST that validates the SafeObject W-lock is released across the
 * end-of-life master DeleteAllCopyMeta RPC (fix for issue #19).  Injects a
 * 500ms delay into WorkerRemoteMasterOCApi::DeleteAllCopyMeta on the evicting
 * worker and issues a cross-worker Get during the injected sleep.  If the
 * W-lock is held across the RPC (the old behavior), the Get's RLock blocks
 * for the full sleep duration (~500ms) and the test times out.  If the W-lock
 * is released before the RPC (the fix), the Get succeeds immediately (<200ms).
 *
 * Topology (2 workers, non-distributed master):
 *   Worker 0: data holder.  Client 0 sets large objects here to trigger
 *             eviction under memory pressure (shared_memory_size_mb=32).
 *             Inject worker.DeleteAllCopyMeta on this worker.
 *   Worker 1: master + client 1.  Client 1 issues a cross-worker Get that
 *             pulls from worker 0 via BatchGetObjectRemote.
 *   Master on worker 1 so that worker 0's drain → DeleteAllCopyMeta takes
 *   the remote-RPC path (WorkerRemoteMasterOCApi), which carries the inject.
 */
#include <chrono>
#include <memory>
#include <string>
#include <thread>

#include "common.h"
#include "client/object_cache/oc_client_common.h"
#include "datasystem/kv_client.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace st {

class EvictDrainWLockTest : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numEtcd = 1;
        opts.numWorkers = 2;
        opts.enableDistributedMaster = "false";
        opts.masterIdx = 1;  // master on w1: w0 drain → remote RPC → inject fires.
        opts.waitWorkerReady = true;
        opts.workerGflagParams =
            "-shared_memory_size_mb=32 -enable_worker_worker_batch_get=true"
            " -slow_log_process_slower_than=1 -v=2";
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
        InitTestKVClient(0, client0_, 60000, false, 60000);  // w0: data + eviction.
        InitTestKVClient(1, client1_, 60000, false, 60000);  // w1: cross-worker Get.
    }

    void TearDown() override
    {
        client0_.reset();
        client1_.reset();
        ExternalClusterTest::TearDown();
    }

    // Fill w0 shm with 8MB objects until eviction is triggered.
    void FillMemoryToTriggerEviction(const std::string &targetKey, const std::string &targetVal)
    {
        datasystem::SetParam param;
        param.writeMode = datasystem::WriteMode::NONE_L2_CACHE_EVICT;
        Status rc = client0_->Set(targetKey, targetVal, param);
        VLOG(1) << "Set target key " << targetKey << ": " << rc.ToString();
        const std::string dataStr(8 * 1024 * 1024, 'x');
        for (int i = 0; i < 50; i++) {
            std::string key = "evict_fill_" + std::to_string(i);
            auto status = client0_->Set(key, dataStr, param);
            VLOG(1) << "Set key " << key << ": " << status.ToString();
            if (status.IsError()) {
                break;
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }

protected:
    std::shared_ptr<KVClient> client0_;
    std::shared_ptr<KVClient> client1_;
};

// Inject 500ms in DeleteAllCopyMeta on w0. With fix (WLock released before
// RPC), concurrent cross-worker Get <300ms; without fix it blocks ~500ms.
TEST_F(EvictDrainWLockTest, CrossWorkerGetNotBlockedByEndLifeDrain)
{
    LOG(INFO) << "Test: cross-worker Get while target key is being end-of-life drained.";
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "worker.DeleteAllCopyMeta", "1*sleep(500)"));

    const std::string targetKey = "drain_target_" + RandomData().GetRandomString(10);
    const std::string targetVal(1024, 'y');

    FillMemoryToTriggerEviction(targetKey, targetVal);

    datasystem::Optional<datasystem::ReadOnlyBuffer> buffer;
    auto t0 = std::chrono::steady_clock::now();
    Status rc = client1_->Get(targetKey, buffer);
    auto t1 = std::chrono::steady_clock::now();
    auto elapsedMs = std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();
    LOG(INFO) << "Cross-worker Get during evict drain: rc=" << rc.ToString()
              << ", elapsed=" << elapsedMs << "ms";

    ASSERT_LT(elapsedMs, 300)
        << "Cross-worker Get took " << elapsedMs
        << "ms — WLock may still be held across master RPC. "
           "Expected <300ms (WLock released before 500ms inject).";

    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, 0, "worker.DeleteAllCopyMeta"));
}

}  // namespace st
}  // namespace datasystem
