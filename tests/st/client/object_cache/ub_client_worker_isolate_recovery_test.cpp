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

/** Description: Client isolates a UB-faulty Worker, then restores access after the Worker port recovery. */

#include <chrono>
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
#include "datasystem/client/object_cache/transport/rpc/worker_rpc_client.h"
#include "datasystem/common/flags/common_flags.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/object_cache/ub_health_summary_codec.h"
#include "datasystem/common/rpc/api_deadline.h"
#include "datasystem/common/ak_sk/signature.h"

namespace datasystem::st {
#ifdef USE_URMA_MOCK
namespace {
constexpr const char *CQE_STATUS_POINT = "UrmaManager.CheckCompletionRecordStatus";
constexpr const char *PORT_STATUS_POINT = "UrmaMock.QueryPortStatus";
constexpr uint32_t TOTAL_MOCK_PORTS = 4;

std::string PortStatusRule(uint32_t badPorts)
{
    return "call(" + std::to_string(TOTAL_MOCK_PORTS) + "," + std::to_string(badPorts) + ")";
}
}  // namespace

class UbClientWorkerIsolateRecoveryTest : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = 1;
        opts.numEtcd = 1;
        opts.enableDistributedMaster = "true";
        opts.workerGflagParams = " -enable_urma=true -shared_memory_size_mb=128 -arena_per_tenant=1"
                                 " -ipc_through_shared_memory=false";
    }

    void SetUp() override
    {
        const char *previousUds = getenv("URMA_MOCK_UDS_BASE_DIR");
        if (previousUds != nullptr) {
            previousUds_ = previousUds;
        }
        const auto uds = "/tmp/ds_urma_iso_" + std::to_string(getpid());
        ASSERT_EQ(setenv("URMA_MOCK_UDS_BASE_DIR", uds.c_str(), 1), 0);
        previousFallback_ = FLAGS_enable_transport_fallback;
        FLAGS_enable_transport_fallback = false;
        ExternalClusterTest::SetUp();
        ConnectOptions options;
        InitConnectOpt(0, options);
        options.enableLocalCache = false;
        options.enableCrossNodeConnection = false;
        options.requestTimeoutMs = 3000;
        client_ = std::make_shared<ObjectClient>(options);
        DS_ASSERT_OK(client_->Init());
        worker_ = HostPort(options.host, options.port);
        auto signature = std::make_shared<Signature>(options.accessKey, options.secretKey);
        rpc_ = std::make_shared<client::WorkerRpcClient>(worker_, std::move(signature));
        DS_ASSERT_OK(rpc_->Init());
    }

    void TearDown() override
    {
        rpc_.reset();
        client_.reset();
        (void)inject::Clear(CQE_STATUS_POINT);
        FLAGS_enable_transport_fallback = previousFallback_;
        ExternalClusterTest::TearDown();
        if (previousUds_.has_value()) {
            (void)setenv("URMA_MOCK_UDS_BASE_DIR", previousUds_->c_str(), 1);
        } else {
            (void)unsetenv("URMA_MOCK_UDS_BASE_DIR");
        }
    }

    Status PutKey(const std::string &key, const std::string &data)
    {
        return client_->Put(key, reinterpret_cast<const uint8_t *>(data.data()), data.size(), CreateParam{});
    }

    Status GetKey(const std::string &key, std::string &value)
    {
        std::vector<Optional<Buffer>> buffers;
        auto rc = client_->Get({ key }, 0, buffers);
        if (rc.IsOk()) {
            if (buffers.size() != 1u || !buffers.front()) {
                return Status(K_RUNTIME_ERROR, "Get succeeded without buffer");
            }
            value.assign(static_cast<const char *>(buffers.front()->MutableData()), buffers.front()->GetSize());
        }
        return rc;
    }

    Status WaitForWorkerPorts(uint32_t badPorts, UbHealthSummary &summary)
    {
        GetHashRingRspPb ring;
        {
            ApiDeadlineGuard requestBudget(5000);
            RETURN_IF_NOT_OK(rpc_->InvokeGetHashRing(0, ring));
        }
        CHECK_FAIL_RETURN_STATUS(ring.has_hash_ring(), K_NOT_READY, "Hash ring is empty");
        const auto incarnation = ring.hash_ring().members().at(worker_.ToString()).id();
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (std::chrono::steady_clock::now() < deadline) {
            ApiDeadlineGuard requestBudget(1000);
            QueryUbPortHealthRspPb response;
            auto status = rpc_->QueryUbPortHealth(incarnation, 1000, response);
            if (status.IsOk() && response.has_health_summary()
                && DecodeUbHealthSummary(response.health_summary(), summary).IsOk()
                && summary.portHealth.has_value() && HasKnownUbPortHealth(*summary.portHealth)
                && !summary.portHealth->verificationPending
                && summary.portHealth->badPortCount == badPorts) {
                return Status::OK();
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
        return Status(K_NOT_READY, "Worker port facts did not converge to bad=" + std::to_string(badPorts));
    }

    bool WaitForClientIsolation(const std::string &key, std::chrono::seconds timeout)
    {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        std::string ignored;
        while (std::chrono::steady_clock::now() < deadline) {
            if (IsUbIsolationDenial(GetKey(key, ignored))) {
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        return false;
    }

    // Only a deterministic UB admission or routing denial proves the client isolated the Worker. A
    // scheduling or network jitter failure surfaces as a timeout class instead and must not be read
    // as isolation, otherwise the recovery assertion below becomes trivially green.
    static bool IsUbIsolationDenial(const Status &status)
    {
        const auto code = status.GetCode();
        if (code == K_URMA_WORKER_UNAVAILABLE || code == K_URMA_DATA_WORKER_UNAVAILABLE
            || code == K_URMA_READ_SOURCE_DENIED) {
            return true;
        }
        // Single-worker ring: the router reports this only after the UB health filter excluded the
        // worker, so the message is part of the contract being asserted.
        return code == K_NO_AVAILABLE_WORKER && status.GetMsg().find("All workers filtered") != std::string::npos;
    }

    bool WaitForClientRecovery(const std::string &key, const std::string &data, std::chrono::seconds timeout)
    {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        std::string value;
        while (std::chrono::steady_clock::now() < deadline) {
            std::string keyToWrite = key + "-recovery-" + GetStringUuid();
            if (PutKey(keyToWrite, data).IsOk() && GetKey(keyToWrite, value).IsOk() && value == data) {
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
        }
        return false;
    }

protected:
    HostPort worker_;
    std::shared_ptr<ObjectClient> client_;
    std::shared_ptr<client::WorkerRpcClient> rpc_;
    std::optional<std::string> previousUds_;
    bool previousFallback_ = true;
};

TEST_F(UbClientWorkerIsolateRecoveryTest, ClientRestoresWorkerAccessAfterUbPortRecovery)
{
    const std::string key = "ub-iso-recovery-" + GetStringUuid();
    const std::string data(1024, 'r');
    DS_ASSERT_OK(PutKey(key, data));
    std::string value;
    DS_ASSERT_OK(GetKey(key, value));
    ASSERT_EQ(value, data);

    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, PORT_STATUS_POINT, PortStatusRule(TOTAL_MOCK_PORTS)));
    bool isolated = false;
    for (int attempt = 0; attempt < 5 && !isolated; ++attempt) {
        DS_ASSERT_OK(inject::Set(CQE_STATUS_POINT, "1*call(0,9)"));
        (void)PutKey(key + "-fault-" + std::to_string(attempt), data);
        isolated = WaitForClientIsolation(key, std::chrono::seconds(5));
    }
    ASSERT_TRUE(isolated) << "Client did not isolate the Worker after injected CQE9 write failures";

    (void)inject::Clear(CQE_STATUS_POINT);
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, PORT_STATUS_POINT, PortStatusRule(3)));
    UbHealthSummary summary;
    DS_ASSERT_OK(WaitForWorkerPorts(3, summary));

    ASSERT_TRUE(WaitForClientRecovery(key, data, std::chrono::seconds(60)))
        << "Client did not restore Worker access after UB port recovery";
}
#endif
}  // namespace datasystem::st
