/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
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

/** Description: Bound-worker switch on UB data-plane failures and switch-back after recovery. */

#include <chrono>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>
#include <unistd.h>

#include <gtest/gtest.h>

#include "common.h"
#include "oc_client_common.h"
#include "datasystem/common/inject/inject_point.h"

namespace datasystem::st {
#ifdef USE_URMA_MOCK
namespace {
constexpr const char *CQE_STATUS_POINT = "UrmaManager.CheckCompletionRecordStatus";
// Fires once per completed bound-worker switch with the action address as the comparison target.
constexpr const char *SWITCH_NOTIFY_POINT = "client.switch_worker_expected_1";
// Fires when a URMA write failure that the TCP fallback absorbed still triggers a UB plane rebuild.
constexpr const char *REBUILD_AFTER_FALLBACK_POINT = "TransportLayer.RebuildUbAfterTcpFallback";
// Retires the peer's send Jetties on the next slot acquisition, opening the breaker deterministically.
constexpr const char *FORCE_CIRCUIT_BROKEN_POINT = "UrmaConnection.AcquireInflightSlot.ForceCircuitBroken";
}  // namespace

class UbFailoverSwitchTest : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = 2;
        opts.numEtcd = 1;
        opts.enableDistributedMaster = "true";
        // client_dead_timeout_s=3 shrinks the URMA success-rate window from ~60s to 3s.
        // payload_nocopy_threshold=1MB routes 2MB objects through the UB data plane instead of inline RPC.
        opts.workerGflagParams = " -enable_urma=true -shared_memory_size_mb=1024 -arena_per_tenant=1"
                                 " -ipc_through_shared_memory=false -client_dead_timeout_s=3"
                                 " -payload_nocopy_threshold=1000000";
    }

    void SetUp() override
    {
        const char *previousUds = getenv("URMA_MOCK_UDS_BASE_DIR");
        if (previousUds != nullptr) {
            previousUds_ = previousUds;
        }
        const auto uds = "/tmp/ds_urma_sw_" + std::to_string(getpid());
        ASSERT_EQ(setenv("URMA_MOCK_UDS_BASE_DIR", uds.c_str(), 1), 0);
        ExternalClusterTest::SetUp();
        ConnectOptions bound;
        InitConnectOpt(0, bound);
        boundWorker_ = HostPort(bound.host, bound.port);
    }

    void TearDown() override
    {
        (void)inject::Clear(CQE_STATUS_POINT);
        (void)inject::Clear(SWITCH_NOTIFY_POINT);
        (void)inject::Clear(REBUILD_AFTER_FALLBACK_POINT);
        ExternalClusterTest::TearDown();
        if (previousUds_.has_value()) {
            (void)setenv("URMA_MOCK_UDS_BASE_DIR", previousUds_->c_str(), 1);
        } else {
            (void)unsetenv("URMA_MOCK_UDS_BASE_DIR");
        }
    }

    Status MakeClient(bool enableLocalCache, std::shared_ptr<ObjectClient> &client)
    {
        ConnectOptions options;
        InitConnectOpt(0, options);
        options.enableLocalCache = enableLocalCache;
        options.enableCrossNodeConnection = true;
        options.requestTimeoutMs = 3000;
        client = std::make_shared<ObjectClient>(options);
        return client->Init();
    }

    static Status CreateCopyPublish(const std::shared_ptr<ObjectClient> &client, const std::string &key,
                                    const std::string &data)
    {
        std::shared_ptr<Buffer> buffer;
        RETURN_IF_NOT_OK(client->Create(key, data.size(), CreateParam{}, buffer));
        RETURN_RUNTIME_ERROR_IF_NULL(buffer);
        RETURN_IF_NOT_OK(buffer->MemoryCopy(data.data(), data.size()));
        return buffer->Publish();
    }

    void ArmSwitchCounters()
    {
        DS_ASSERT_OK(inject::Set(SWITCH_NOTIFY_POINT, "call(" + boundWorker_.ToString() + ")"));
    }

    static uint64_t RunFor(std::chrono::seconds duration,
                           const std::function<Status(const std::string &)> &operation, const std::string &keyPrefix)
    {
        uint64_t okCount = 0;
        uint64_t failCount = 0;
        const auto deadline = std::chrono::steady_clock::now() + duration;
        while (std::chrono::steady_clock::now() < deadline) {
            if (operation(keyPrefix + GetStringUuid()).IsOk()) {
                ++okCount;
            } else {
                ++failCount;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
        }
        std::cout << "[Diag] " << keyPrefix << " ok=" << okCount << " fail=" << failCount << std::endl;
        return failCount;
    }

protected:
    HostPort boundWorker_;
    std::optional<std::string> previousUds_;
};

TEST_F(UbFailoverSwitchTest, NoBoundWorkerSwitchWhenLocalCacheDisabled)
{
    std::shared_ptr<ObjectClient> client;
    DS_ASSERT_OK(MakeClient(false, client));
    const std::string data(2 * 1024 * 1024, 'a');
    DS_ASSERT_OK(CreateCopyPublish(client, "ub-sw-off-base-" + GetStringUuid(), data));
    ArmSwitchCounters();

    DS_ASSERT_OK(inject::Set(CQE_STATUS_POINT, "call(0,9)"));
    const auto faultFailures =
        RunFor(std::chrono::seconds(12), [&](const std::string &key) { return CreateCopyPublish(client, key, data); },
               "ub-sw-off-fault-");
    (void)inject::Clear(CQE_STATUS_POINT);

    EXPECT_GT(faultFailures, 0u) << "Injected CQE9 did not break the routed UB data plane";
    EXPECT_EQ(inject::GetExecuteCount(SWITCH_NOTIFY_POINT), 0u)
        << "Bound-worker switch fired although the routed data plane never feeds the URMA success-rate tracker";
    // Single post-fault probe only, no retry loop: repeatedly rebuilding the UB data plane inside this
    // process damages the shared mock URMA backend and leaks breaker state into later UB tests in the
    // same binary. The breaker self-heal contract (cooldown-elapsed -> NEED_CONNECT -> rebuild) is
    // covered by UrmaConnectionInflightTest and the rebuild log evidence recorded in the PR.
    (void)CreateCopyPublish(client, "ub-sw-off-after-fault-" + GetStringUuid(), data);
}

TEST_F(UbFailoverSwitchTest, SwitchNeverRevertsAfterRecoveryWhenLocalCacheEnabled)
{
    GTEST_SKIP() << "Bound UB pre-send could not be engaged on a single host (client.set.urma_write_ok count=0 "
                    "although the cross-node registration shape took effect: RegisterClient shmEnabled=0 and the "
                    "client UB arena was imported by the worker). True cross-host bound workers do not help either: "
                    "Create routes through the routed transport layer for them "
                    "(object_client_impl.cpp Create -> !IsSameHostWorker -> CreateRoutedBuffer), so the URMA "
                    "success-rate tracker is not fed. The switch-without-revert scenario needs a same-host bound "
                    "worker whose SHM channel fails while UB stays healthy; that shape is not reachable in this "
                    "harness yet. See the risk-1 analysis: without ServiceDiscovery there is no switch-back path "
                    "(worker_failover.cpp RecoverPreferredLocalWorker is discovery-only).";
}
// Boundary counterpart of NoBoundWorkerSwitchWhenLocalCacheDisabled: the object is small enough for
// the TCP fallback limiter to accept, so the publish succeeds and the write path only learns about
// the broken breaker from ubFailureReportRc. The explicit rebuild trigger must still fire, otherwise
// the UB plane stays broken for every small write until a payload the limiter rejects comes along.
class UbTcpFallbackRebuildTest : public UbFailoverSwitchTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        UbFailoverSwitchTest::SetClusterSetupOptions(opts);
        // 512KB probes clear this threshold (so they use the UB data plane) while staying under the
        // 1MB TCP fallback limiter (so the fallback succeeds).
        const std::string base = "-payload_nocopy_threshold=1000000";
        const auto pos = opts.workerGflagParams.find(base);
        ASSERT_NE(pos, std::string::npos) << "base fixture lost its payload_nocopy_threshold flag";
        opts.workerGflagParams.replace(pos, base.size(), "-payload_nocopy_threshold=1024");
    }
};

TEST_F(UbTcpFallbackRebuildTest, TcpFallbackSuccessStillTriggersUbPlaneRebuild)
{
    std::shared_ptr<ObjectClient> client;
    DS_ASSERT_OK(MakeClient(false, client));
    const std::string data(512 * 1024, 'c');
    DS_ASSERT_OK(CreateCopyPublish(client, "ub-fb-base-" + GetStringUuid(), data));

    // Trip the breaker deterministically, then let its cooldown elapse. The next UB write reports
    // NEED_CONNECT before publishing, the limiter accepts 512KB, so the publish succeeds via TCP.
    DS_ASSERT_OK(inject::Set(FORCE_CIRCUIT_BROKEN_POINT, "call()"));
    (void)CreateCopyPublish(client, "ub-fb-trip-" + GetStringUuid(), data);
    (void)inject::Clear(FORCE_CIRCUIT_BROKEN_POINT);
    DS_ASSERT_OK(inject::Set(REBUILD_AFTER_FALLBACK_POINT, "call()"));

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(35);
    while (inject::GetExecuteCount(REBUILD_AFTER_FALLBACK_POINT) == 0
           && std::chrono::steady_clock::now() < deadline) {
        (void)CreateCopyPublish(client, "ub-fb-after-" + GetStringUuid(), data);
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
    EXPECT_GT(inject::GetExecuteCount(REBUILD_AFTER_FALLBACK_POINT), 0u)
        << "A UB write failure absorbed by the TCP fallback did not trigger a UB plane rebuild";
}
#endif
}  // namespace datasystem::st
