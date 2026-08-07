/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Description: Regression test for the SHM fd-passing request-timeout bug (T3).
 *
 * Bug: ClientWorkerRemoteCommonApi::GetClientFd bounded the fd-arrival wait by
 * recvClientFdState_.getClientFdTimeoutMs (== connectTimeoutMs, 9s) and re-initialised the in-flight
 * request deadline with that value, so a Get whose fd signal was delayed blocked for up to
 * connectTimeoutMs and returned OK, completely ignoring the client's requestTimeoutMs.
 *
 * Fix: GetClientFd routes the timeout through BoundFdPassingTimeoutMs, which caps it by
 * ApiDeadline::ApiRemainingUs() (derived from requestTimeoutMs). With requestTimeoutMs=20 the fd
 * wait is bounded near the request budget instead of waiting for the long fd-passing timeout.
 *
 * Red/green:
 *   - Unfixed master: Get completes after ~FD_DELAY_MS (2 s) -> latency assertion FAILS.
 *   - Fixed tree:     Get completes near REQUEST_TIMEOUT_MS (20 ms) -> latency assertion PASS.
 *
 * Note: the inject point runs on the SDK client worker's recvPageThread_, which lives in-process in
 * the test binary (the KVClient SDK is a library). So the action is set on the in-process
 * InjectPointManager, not via cluster_->SetInjectAction (which targets the worker process).
 */

#include <cstdint>
#include <string>

#include <gtest/gtest.h>

#include "common.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/kv_client.h"
#include "datasystem/utils/status.h"
#include "kv_client_common.h"

namespace datasystem {
namespace st {

namespace {
// In-process inject point inside ClientWorkerRemoteCommonApi::PostRecvPageFd. The lambda sleeps for
// its int argument, delaying the recvPageNotify signal that RecvFdAfterNotify waits on.
constexpr char RECV_PAGE_FD_INJECT[] = "ClientWorkerCommonApi.RecvPageFd";

// Tight client request timeout (ms). This is the budget the bug ignores.
constexpr int32_t REQUEST_TIMEOUT_MS = 20;

// How long (ms) the inject delays the fd signal. Well above REQUEST_TIMEOUT_MS and well below
// getClientFdTimeoutMs so the buggy build finishes at ~2 s rather than hanging to connect timeout.
constexpr int32_t FD_DELAY_MS = 2'000;

// Upper bound for the fixed build's bounded latency. Even with heavy CI jitter it stays far below
// 1 s. The buggy build (~FD_DELAY_MS) exceeds it, so this threshold separates red from green.
constexpr int64_t MAX_FIXED_LATENCY_MS = 1'000;

// Value size large enough to live in SHM (above any inline threshold) so the same-host Get exercises
// the SHM fd-passing path rather than an inline RPC short-circuit.
constexpr size_t VALUE_SIZE = 1 * 1024ul * 1024ul;
}  // namespace

class KVClientRequestTimeoutFdPassingTest : public KVClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        // Single same-host worker. SHM is on by default (ipc_through_shared_memory=true) and
        // enableLocalCache stays at its ConnectOptions default (true), so a Get takes the legacy
        // SHM fd-passing path: MmapManager::ReceiveAndMmapClientFds -> GetClientFd -> RecvFdAfterNotify.
        opts.numWorkers = 1;
        opts.numEtcd = 1;
        opts.enableDistributedMaster = "false";
        opts.workerGflagParams = " -shared_memory_size_mb=256";
    }
};

// Regression for T3: a Get whose fd-passing is delayed must honor the client's requestTimeoutMs and
// return quickly, not block up to connectTimeoutMs.
TEST_F(KVClientRequestTimeoutFdPassingTest, GetHonorsRequestTimeoutWhenFdPassingIsDelayed)
{
    // Writer sets data and never reads, so it never triggers the read-side fd-passing. Created and
    // exercised before the inject is applied so its Set is not slowed by the fd-delay injection.
    std::shared_ptr<KVClient> writer;
    InitTestKVClient(0, writer, 60000, false, 0);
    const std::string key = ObjectKey();
    const std::string value(VALUE_SIZE, 'a');
    DS_ASSERT_OK(writer->Set(key, value));

    // Delay the fd signal inside this test process's SDK client worker (recvPageThread_).
    DS_ASSERT_OK(inject::InjectPointManager::Instance().SetAction(
        RECV_PAGE_FD_INJECT, "sleep(" + std::to_string(FD_DELAY_MS) + ")"));

    // Fresh reader with a 20 ms request budget. Its first Get has a cold fd (fdProvider_ null) so
    // ReceiveAndMmapClientFds takes the GetClientFd path that the fix bounds by ApiDeadline.
    std::shared_ptr<KVClient> reader;
    InitTestKVClient(0, reader, 60000, false, REQUEST_TIMEOUT_MS);

    Timer timer;
    std::string out;
    const Status rc = reader->Get(key, out, 0);
    const int64_t elapsedMs = timer.ElapsedMilliSecond();

    // Prove the Get actually reached GetClientFd/PostRecvPageFd rather than failing earlier
    // (e.g. at the metadata query), which would make the latency assertion vacuously true.
    const uint64_t injectCount = inject::InjectPointManager::Instance().GetExecuteCount(RECV_PAGE_FD_INJECT);
    DS_ASSERT_OK(inject::InjectPointManager::Instance().ClearAction(RECV_PAGE_FD_INJECT));
    ASSERT_GE(injectCount, 1u) << "fd-passing inject never fired; Get did not reach GetClientFd";

    // The fd map is the source of truth and notify is only the wake-up signal. Depending on the
    // timing, Get may return OK if the fd was already published before the delayed notify runs.
    // The regression contract is latency: fd-passing must be bounded by requestTimeoutMs.
    ASSERT_LT(elapsedMs, MAX_FIXED_LATENCY_MS)
        << "Get ran " << elapsedMs << " ms; expected fd-passing to be bounded near requestTimeoutMs="
        << REQUEST_TIMEOUT_MS << " ms. rc=" << rc.ToString();
    if (rc.IsOk()) {
        ASSERT_EQ(out, value);
    }
    LOG(INFO) << "Get completed within bounded timeout in " << elapsedMs << " ms, rc=" << rc.ToString();
}
}  // namespace st
}  // namespace datasystem
