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

/** Description: Focused client-local sender UB admission tests. */

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <future>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include <bthread/bthread.h>
#include <bthread/condition_variable.h>
#include <bthread/countdown_event.h>
#include <bthread/mutex.h>

#include "datasystem/client/object_cache/routing/ub_health_filter.h"
#include "datasystem/client/object_cache/transport/common/deadline_retry.h"
#include "datasystem/client/object_cache/transport/object_buffer_internal.h"
#include "datasystem/client/object_cache/transport/object_read/replica_reader.h"
#include "datasystem/client/object_cache/transport/transport_layer.h"
#include "datasystem/common/flags/common_flags.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/object_cache/provider_ub_failure_detail.h"
#include "datasystem/common/object_cache/ub_health_summary_codec.h"
#include "datasystem/common/object_cache/urma_fallback_tcp_limiter.h"
#include "datasystem/common/rdma/fast_transport_manager_wrapper.h"
#include "datasystem/common/rpc/api_deadline.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/protos/meta_transport.pb.h"
#if defined(USE_URMA) || defined(USE_URMA_MOCK)
#include "datasystem/common/rdma/urma_manager.h"
#endif

DS_DECLARE_bool(enable_transport_fallback);

namespace datasystem {
namespace client {
namespace {
constexpr std::chrono::seconds PROBE_OBSERVATION_TIMEOUT(3);
constexpr char RECONCILE_AFTER_DEADLINE_CHECK_INJECT[] =
    "TransportLayer.WaitForSnapshotOrStop.afterDeadlineCheck";

template <typename Predicate>
bool WaitUntil(Predicate predicate, std::chrono::milliseconds timeout = PROBE_OBSERVATION_TIMEOUT)
{
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (predicate()) {
            return true;
        }
        std::this_thread::yield();
    }
    return predicate();
}

HostPort MakeAddress(int port)
{
    return HostPort("127.0.0.1", port);
}

std::shared_ptr<Signature> MakeSignature()
{
    return std::make_shared<Signature>();
}

TransportRequestContext MakeRequestContext()
{
    return { "client-1", "token-1", "tenant-1" };
}

TransportCreateParam MakeCreateParam()
{
    TransportCreateParam param;
    param.requestContext = MakeRequestContext();
    return param;
}

TransportSetParam MakeSetParam()
{
    TransportSetParam param;
    param.requestContext = MakeRequestContext();
    return param;
}

master::ObjectLocationInfoPb MakeReplicaLocation(const std::string &key, uint64_t size,
                                                 const std::vector<HostPort> &addresses)
{
    master::ObjectLocationInfoPb location;
    location.set_object_key(key);
    location.set_object_size(size);
    for (const auto &address : addresses) {
        location.add_object_locations(address.ToString());
    }
    return location;
}

class FakeWorkerRpcClient : public WorkerRpcClient {
public:
    explicit FakeWorkerRpcClient(const HostPort &address) : WorkerRpcClient(address, MakeSignature())
    {
    }

    Status Init() override
    {
        alive_ = true;
        return Status::OK();
    }

    bool IsAlive() const override
    {
        return alive_;
    }

    Status ProbeProviderUbRecovery(const std::string &, int32_t, ProviderUbRecoveryProbeRspPb &response) override
    {
        response = providerProbeResponse;
        return providerProbeStatus;
    }

    ProviderUbRecoveryProbeRspPb providerProbeResponse;
    Status providerProbeStatus{ Status::OK() };

protected:
    void Close() override
    {
        alive_ = false;
    }

private:
    bool alive_{ false };
};

class FakeTransporter : public IDataTransporter {
public:
    Status Get(const DataGetRequest &, DataGetResult &output) override
    {
        ++getCount;
        Status status = Status::OK();
        if (!getStatuses.empty()) {
            status = getStatuses.front();
            getStatuses.erase(getStatuses.begin());
        }
        if (status.IsError() && getProviderCqeStatus.has_value()) {
            FillProviderUbFailureDetail(status, "client-receive-endpoint", providerAddress.ToString(),
                                        getProviderCqeStatus, getProviderCqeStatus,
                                        *output.response.mutable_provider_ub_failure_detail());
        }
        return status;
    }

    Status BatchGet(const DataGetBatchRequest &inputs, DataGetBatchResult &outputs) override
    {
        ++batchGetCount;
        outputs.resize(inputs.size());
        for (auto &output : outputs) {
            output.status = Status::OK();
        }
        return Status::OK();
    }

    Status Create(const HostPort &workerAddr, const std::string &key, uint64_t size, const TransportCreateParam &,
                  std::shared_ptr<ObjectBuffer> &buffer) override
    {
        ++createCount;
        auto info = std::make_shared<ObjectBufferInfo>();
        info->objectKey = key;
        info->dataSize = size;
        info->workerAddr = workerAddr;
        info->shmId = ShmKey::Intern("fake-shm-id");
        auto storage = std::make_shared<std::vector<uint8_t>>(size + 1);
        info->pointer = storage->data();
        info->ubGetBufferHandle = std::static_pointer_cast<void>(storage);
        return ObjectBufferInternal::Create(std::move(info), buffer);
    }

    Status Set(ObjectBuffer &buffer, const TransportSetParam &, TransportSetResult *result = nullptr) override
    {
        std::unique_lock<bthread::Mutex> lock(setMutex);
        const int callIndex = ++setCount;
        if (result != nullptr) {
            result->publishAttempted = true;
        }
        setCv.notify_all();
        if (coordinateConcurrentSets) {
            if (callIndex == 1) {
                while (setCount < 2 && !releaseSecondSet) {
                    setCv.wait(lock);
                }
            } else if (callIndex == 2) {
                while (!releaseSecondSet) {
                    setCv.wait(lock);
                }
            }
        }
        if (!setUbFailureReports.empty()) {
            auto &info = ObjectBufferInternal::GetMutableInfo(buffer);
            info.ubFailureReportRc = setUbFailureReports.front();
            setUbFailureReports.erase(setUbFailureReports.begin());
            if (!setUbCqeStatuses.empty()) {
                const auto cqeStatus = setUbCqeStatuses.front();
                info.ubCqeStatus = cqeStatus;
                setUbCqeStatuses.erase(setUbCqeStatuses.begin());
                if (result != nullptr && cqeStatus.has_value()
                    && *cqeStatus == URMA_REMOTE_ACK_TIMEOUT_STATUS) {
                    result->publishAttempted = false;
                }
            }
        }
        if (setStatuses.empty()) {
            return Status::OK();
        }
        Status status = setStatuses.front();
        setStatuses.erase(setStatuses.begin());
        return status;
    }

    bool WaitForSetCount(int expected, std::chrono::milliseconds timeout)
    {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        std::unique_lock<bthread::Mutex> lock(setMutex);
        while (setCount < expected) {
            const auto remaining = std::chrono::duration_cast<std::chrono::microseconds>(
                deadline - std::chrono::steady_clock::now());
            if (remaining <= std::chrono::microseconds::zero()) {
                return false;
            }
            (void)setCv.wait_for(lock, remaining.count());
        }
        return true;
    }

    void ReleaseBlockedSet()
    {
        std::lock_guard<bthread::Mutex> lock(setMutex);
        releaseSecondSet = true;
        setCv.notify_all();
    }

    int GetSetCount()
    {
        std::lock_guard<bthread::Mutex> lock(setMutex);
        return setCount;
    }

    Status MCreate(const HostPort &workerAddr, const std::vector<std::string> &keys, const std::vector<uint64_t> &sizes,
                   const TransportCreateParam &param, std::vector<std::shared_ptr<ObjectBuffer>> &buffers) override
    {
        ++mCreateCount;
        for (size_t i = 0; i < keys.size(); ++i) {
            std::shared_ptr<ObjectBuffer> buffer;
            RETURN_IF_NOT_OK(Create(workerAddr, keys[i], sizes[i], param, buffer));
            buffers.emplace_back(std::move(buffer));
        }
        return Status::OK();
    }

    Status MSet(const std::vector<std::shared_ptr<ObjectBuffer>> &, const TransportSetParam &,
                TransportMSetResult &result) override
    {
        ++mSetCount;
        result.actualKind = kind;
        result.publishAttempted = true;
        result.ubFailureReportRc = mSetUbFailureReportRc;
        result.ubCqeStatus = mSetUbCqeStatus;
        if (mSetUbCqeStatus.has_value() && *mSetUbCqeStatus == URMA_REMOTE_ACK_TIMEOUT_STATUS) {
            result.publishAttempted = false;
        }
        return mSetStatus;
    }

    Status Release(const ShmKey &, const TransportRequestContext &) override
    {
        ++releaseCount;
        return Status::OK();
    }

    AccessTransportKind Kind() const override
    {
        return kind;
    }

    bool IsAlive() const override
    {
        return true;
    }

    AccessTransportKind kind{ AccessTransportKind::TCP };
    HostPort providerAddress;
    int getCount{ 0 };
    int batchGetCount{ 0 };
    int createCount{ 0 };
    int setCount{ 0 };
    int mCreateCount{ 0 };
    int mSetCount{ 0 };
    int releaseCount{ 0 };
    std::vector<Status> setStatuses;
    std::vector<Status> getStatuses;
    std::optional<int> getProviderCqeStatus;
    std::vector<Status> setUbFailureReports;
    std::vector<std::optional<int>> setUbCqeStatuses;
    Status mSetUbFailureReportRc{ Status::OK() };
    std::optional<int> mSetUbCqeStatus;
    Status mSetStatus{ Status::OK() };
    bool coordinateConcurrentSets{ false };
    bool releaseSecondSet{ false };
    bthread::Mutex setMutex;
    bthread::ConditionVariable setCv;
};

class FakeDataPlaneManager : public DataPlaneManager {
public:
    FakeDataPlaneManager() : DataPlaneManager(MakeSignature(), ConnectOptions{}.fastTransportMemSize)
    {
    }

    Status CreateWorkerRpcClient(const HostPort &address, std::shared_ptr<WorkerRpcClient> &output) override
    {
        auto client = std::make_shared<FakeWorkerRpcClient>(address);
        RETURN_IF_NOT_OK(client->Init());
        output = std::move(client);
        return Status::OK();
    }

    Status BuildTransporter(const HostPort &workerAddr, TransportHint hint, const std::shared_ptr<WorkerRpcClient> &,
                            TransportPhaseLatencyRecorder *,
                            std::shared_ptr<IDataTransporter> &output) override
    {
        std::lock_guard<std::mutex> lock(mutex_);
        ++transportBuildCount;
        auto transporter = std::make_shared<FakeTransporter>();
        transporter->providerAddress = workerAddr;
        if (hint == TransportHint::TCP_ONLY) {
            transporter->kind = AccessTransportKind::TCP;
        } else if (hint == TransportHint::SHM_CANDIDATE) {
            transporter->kind = AccessTransportKind::SHM;
        } else {
            transporter->kind = AccessTransportKind::UB;
        }
        auto getStatuses = transporterGetStatuses.find(workerAddr);
        if (getStatuses != transporterGetStatuses.end()) {
            transporter->getStatuses = std::move(getStatuses->second);
            transporterGetStatuses.erase(getStatuses);
        }
        auto getCqeStatus = transporterGetCqeStatuses.find(workerAddr);
        if (getCqeStatus != transporterGetCqeStatuses.end()) {
            transporter->getProviderCqeStatus = getCqeStatus->second;
            transporterGetCqeStatuses.erase(getCqeStatus);
        }
        if (!transporterSetStatuses.empty()) {
            transporter->setStatuses = std::move(transporterSetStatuses.front());
            transporterSetStatuses.erase(transporterSetStatuses.begin());
        }
        if (!transporterMSetUbFailureReports.empty()) {
            transporter->mSetUbFailureReportRc = transporterMSetUbFailureReports.front();
            transporterMSetUbFailureReports.erase(transporterMSetUbFailureReports.begin());
            transporter->mSetUbCqeStatus = transporterMSetUbCqeStatuses.empty()
                                               ? URMA_PORT_UNAVAILABLE_STATUS
                                               : transporterMSetUbCqeStatuses.front();
            if (!transporterMSetUbCqeStatuses.empty()) {
                transporterMSetUbCqeStatuses.erase(transporterMSetUbCqeStatuses.begin());
            }
        }
        builtTransporters.emplace_back(transporter);
        output = std::move(transporter);
        return Status::OK();
    }

    bool WaitForProbeCount(int expected, std::chrono::milliseconds timeout)
    {
        std::unique_lock<std::mutex> lock(mutex_);
        return probeCv.wait_for(lock, timeout, [&] { return probeCount >= expected; });
    }

    bool WaitForProviderProbeCount(int expected, std::chrono::milliseconds timeout)
    {
        std::unique_lock<std::mutex> lock(mutex_);
        return probeCv.wait_for(lock, timeout, [&] { return providerProbeCount >= expected; });
    }

    Status ProbeProviderUbRecovery(const HostPort &workerAddr, const std::string &expectedIncarnation,
                                   int32_t timeoutMs, UbHealthSummary &summary) override
    {
        std::lock_guard<std::mutex> lock(mutex_);
        providerProbedWorkers.emplace_back(workerAddr);
        providerProbeExpectedIncarnations.emplace_back(expectedIncarnation);
        providerProbeTimeouts.emplace_back(timeoutMs);
        ++providerProbeCount;
        summary = providerProbeSummary;
        Status status = providerProbeStatuses.empty() ? Status::OK() : providerProbeStatuses.front();
        if (!providerProbeStatuses.empty()) {
            providerProbeStatuses.erase(providerProbeStatuses.begin());
        }
        probeCv.notify_all();
        return status;
    }

    Status ProbeUbWriteTarget(const HostPort &workerAddr) override
    {
        std::lock_guard<std::mutex> lock(mutex_);
        probedWorkers.emplace_back(workerAddr);
        ++probeCount;
        Status status = probeStatuses.empty() ? Status::OK() : probeStatuses.front();
        if (!probeStatuses.empty()) {
            probeStatuses.erase(probeStatuses.begin());
        }
        probeCv.notify_all();
        return status;
    }

    std::vector<HostPort> GetProbedWorkers()
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return probedWorkers;
    }

    int transportBuildCount{ 0 };
    std::atomic<int> probeCount{ 0 };
    std::atomic<int> providerProbeCount{ 0 };
    std::vector<std::vector<Status>> transporterSetStatuses;
    std::unordered_map<HostPort, std::vector<Status>> transporterGetStatuses;
    std::unordered_map<HostPort, int> transporterGetCqeStatuses;
    std::vector<Status> transporterMSetUbFailureReports;
    std::vector<int> transporterMSetUbCqeStatuses;
    std::vector<Status> probeStatuses;
    std::vector<HostPort> probedWorkers;
    std::vector<HostPort> providerProbedWorkers;
    std::vector<std::string> providerProbeExpectedIncarnations;
    std::vector<int32_t> providerProbeTimeouts;
    UbHealthSummary providerProbeSummary;
    std::vector<Status> providerProbeStatuses;
    std::vector<std::shared_ptr<FakeTransporter>> builtTransporters;
    std::condition_variable probeCv;

protected:
    Status EstablishUbProbe(const HostPort &workerAddr, const std::shared_ptr<WorkerRpcClient> &) override
    {
        std::optional<Status> injectedStatus;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            probedWorkers.emplace_back(workerAddr);
            ++probeCount;
            probeCv.notify_all();
            if (!probeStatuses.empty()) {
                injectedStatus = probeStatuses.front();
                probeStatuses.erase(probeStatuses.begin());
            }
        }
        return injectedStatus.value_or(Status::OK());
    }

private:
    std::mutex mutex_;
};

class TestTransportLayer : public TransportLayer {
public:
    TestTransportLayer(std::shared_ptr<DataPlaneManager> manager, std::shared_ptr<TransportAdvisor> advisor,
                       std::shared_ptr<UbHealthFilter> readSourceFilter = nullptr)
        : TransportLayer(std::move(manager), std::move(advisor), std::move(readSourceFilter))
    {
    }

    bool ReportProviderFailure(const HostPort &provider, const ProviderUbFailureDetailPb &detail)
    {
        return ReportProviderUbFailure(provider, detail);
    }

    Status CheckReadSource(const HostPort &workerAddr, AccessTransportKind &deniedKind) const
    {
        return CheckUbReadSource(workerAddr, deniedKind);
    }
};

class FixedTransportAdvisor : public TransportAdvisor {
public:
    explicit FixedTransportAdvisor(TransportHint hint) : hint_(hint)
    {
    }

    TransportHint GetTransportHint(const HostPort &) const override
    {
        return hint_;
    }

    void SetHint(TransportHint hint)
    {
        hint_ = hint;
    }

private:
    TransportHint hint_;
};

TEST(ReplicaReaderAdmissionTest, SingletonBatchProviderError4CreatesObservationAndNextBatchSwitchesReplica)
{
    ApiDeadlineGuard deadline(1000);
    const auto failedProvider = MakeAddress(28);
    const auto healthyProvider = MakeAddress(29);
    auto manager = std::make_shared<FakeDataPlaneManager>();
    manager->transporterGetStatuses[failedProvider] = { Status(K_URMA_ERROR, "provider write failed") };
    manager->transporterGetCqeStatuses[failedProvider] = 4;
    auto executor = std::make_shared<DataPlaneExecutor>(manager, std::make_shared<TransportAdvisor>());
    auto filter = std::make_shared<UbHealthFilter>();
    ReplicaReader reader(
        executor, std::make_shared<DeadlineRetry>(), std::make_shared<ThreadPool>(1),
        [filter](const HostPort &address, AccessTransportKind &) {
            return filter->IsAvailable(address)
                       ? Status::OK()
                       : Status(K_URMA_DATA_WORKER_UNAVAILABLE, "read source unavailable");
        },
        [filter](const HostPort &provider, const GetObjectRemoteRspPb &response) {
            if (response.has_provider_ub_failure_detail()) {
                (void)filter->ReportProviderFailure(provider, response.provider_ub_failure_detail());
            }
        });
    auto location = MakeReplicaLocation("switch-after-provider-detail", 4, { failedProvider, healthyProvider });
    ObjectReadItemResult result;
    ReplicaReadBatch requests{ { &location, &result } };

    EXPECT_EQ(reader.ReadBatch(requests).GetCode(), K_URMA_ERROR);
    auto observation = filter->GetLocalObservation(failedProvider);
    ASSERT_TRUE(observation.has_value());
    EXPECT_EQ(observation->lastFailureClass, UbFailureClass::PORT_UNAVAILABLE_ERROR4);

    EXPECT_TRUE(reader.ReadBatch(requests).IsOk());
    ASSERT_EQ(manager->builtTransporters.size(), 2u);
    EXPECT_EQ(manager->builtTransporters[0]->providerAddress, failedProvider);
    EXPECT_EQ(manager->builtTransporters[0]->getCount, 1);
    EXPECT_EQ(manager->builtTransporters[1]->providerAddress, healthyProvider);
    EXPECT_EQ(manager->builtTransporters[1]->getCount, 1);
}

TEST(ReplicaReaderAdmissionTest, BatchChecksUnavailableEndpointOnceAndContinuesHealthyGroup)
{
    ApiDeadlineGuard deadline(1000);
    const auto failedProvider = MakeAddress(30);
    const auto healthyProvider = MakeAddress(31);
    auto manager = std::make_shared<FakeDataPlaneManager>();
    auto executor = std::make_shared<DataPlaneExecutor>(manager, std::make_shared<TransportAdvisor>());
    std::unordered_map<HostPort, size_t> admissionChecks;
    ReplicaReader reader(
        executor, std::make_shared<DeadlineRetry>(), std::make_shared<ThreadPool>(2),
        [&admissionChecks, failedProvider](const HostPort &address, AccessTransportKind &) {
            ++admissionChecks[address];
            return address == failedProvider
                       ? Status(K_URMA_DATA_WORKER_UNAVAILABLE, "read source unavailable")
                       : Status::OK();
        });
    std::vector<master::ObjectLocationInfoPb> locations = {
        MakeReplicaLocation("bad-a", 1, { failedProvider }),
        MakeReplicaLocation("bad-b", 1, { failedProvider }),
        MakeReplicaLocation("good", 1, { healthyProvider }),
    };
    std::vector<ObjectReadItemResult> results(locations.size());
    ReplicaReadBatch requests;
    for (size_t i = 0; i < locations.size(); ++i) {
        requests.push_back({ &locations[i], &results[i] });
    }

    EXPECT_TRUE(reader.ReadBatch(requests).IsOk());
    EXPECT_EQ(admissionChecks[failedProvider], 1u);
    EXPECT_EQ(admissionChecks[healthyProvider], 1u);
    EXPECT_EQ(manager->transportBuildCount, 1);
    EXPECT_EQ(results[0].status.GetCode(), K_URMA_DATA_WORKER_UNAVAILABLE);
    EXPECT_EQ(results[1].status.GetCode(), K_URMA_DATA_WORKER_UNAVAILABLE);
    EXPECT_TRUE(results[2].status.IsOk());
}

TEST(ReplicaReaderAdmissionTest, ClientReadSourceDeniedFallsBackToSameProviderTcp)
{
    ApiDeadlineGuard deadline(1000);
    const auto failedProvider = MakeAddress(34);
    const auto healthyProvider = MakeAddress(35);
    auto manager = std::make_shared<FakeDataPlaneManager>();
    auto executor = std::make_shared<DataPlaneExecutor>(manager, std::make_shared<TransportAdvisor>());
    std::unordered_map<HostPort, size_t> admissionChecks;
    ReplicaReader reader(
        executor, std::make_shared<DeadlineRetry>(), std::make_shared<ThreadPool>(2),
        [&admissionChecks, failedProvider](const HostPort &address, AccessTransportKind &) {
            ++admissionChecks[address];
            return address == failedProvider
                       ? Status(K_URMA_READ_SOURCE_DENIED, "client denied read source")
                       : Status::OK();
        });
    std::vector<master::ObjectLocationInfoPb> locations = {
        MakeReplicaLocation("denied-a", 1, { failedProvider }),
        MakeReplicaLocation("denied-b", 1, { failedProvider }),
        MakeReplicaLocation("good", 1, { healthyProvider }),
    };
    std::vector<ObjectReadItemResult> results(locations.size());
    ReplicaReadBatch requests;
    for (size_t i = 0; i < locations.size(); ++i) {
        requests.push_back({ &locations[i], &results[i] });
    }

    EXPECT_TRUE(reader.ReadBatch(requests).IsOk());
    EXPECT_EQ(admissionChecks[failedProvider], 1u);
    EXPECT_EQ(admissionChecks[healthyProvider], 1u);
    EXPECT_EQ(manager->transportBuildCount, 2);
    EXPECT_TRUE(results[0].status.IsOk());
    EXPECT_TRUE(results[1].status.IsOk());
    EXPECT_TRUE(results[2].status.IsOk());
    ASSERT_EQ(manager->builtTransporters.size(), 2u);
    auto failedTransporter = std::find_if(
        manager->builtTransporters.begin(), manager->builtTransporters.end(),
        [failedProvider](const auto &transporter) { return transporter->providerAddress == failedProvider; });
    ASSERT_NE(failedTransporter, manager->builtTransporters.end());
    EXPECT_EQ((*failedTransporter)->kind, AccessTransportKind::TCP);
    EXPECT_EQ((*failedTransporter)->batchGetCount, 1);
    EXPECT_EQ(results[0].attemptedKind, AccessTransportKind::TCP);
    EXPECT_EQ(results[1].attemptedKind, AccessTransportKind::TCP);
}

TEST(ReplicaReaderAdmissionTest, LargeOrDisabledFallbackFailsFastWithoutTcpSubmission)
{
    const bool savedFallback = FLAGS_enable_transport_fallback;
    Raii restoreFallback([savedFallback] { FLAGS_enable_transport_fallback = savedFallback; });
    for (const bool fallback : { false, true }) {
        FLAGS_enable_transport_fallback = fallback;
        for (const bool batch : { false, true }) {
            for (const uint64_t bytes : std::vector<uint64_t>{ UrmaFallbackTcpLimiter::kMaxSinglePayloadBytes,
                                                              8ULL * 1024 * 1024 + 65536 }) {
                ApiDeadlineGuard deadline(1000);
                const auto provider = MakeAddress(36);
                auto manager = std::make_shared<FakeDataPlaneManager>();
                auto executor = std::make_shared<DataPlaneExecutor>(
                    manager, std::make_shared<FixedTransportAdvisor>(TransportHint::UB_CANDIDATE));
                size_t checks = 0;
                ReplicaReader reader(executor, std::make_shared<DeadlineRetry>(), std::make_shared<ThreadPool>(1),
                    [&checks](const HostPort &, AccessTransportKind &kind) {
                        kind = AccessTransportKind::UB;
                        return ++checks == 1 ? Status(K_URMA_READ_SOURCE_DENIED, "waiting for UB probe")
                                             : Status::OK();
                    });
                auto location = MakeReplicaLocation("large-ub-read", bytes, { provider });
                ObjectReadItemResult result;
                auto context = std::make_shared<TransportReadContext>();
                ReplicaReadRequest request{ &location, &result, context };
                auto rc = batch ? reader.ReadBatch({ request }) : reader.Read(location, result, context);
                EXPECT_EQ(rc.GetCode(), K_URMA_READ_SOURCE_DENIED);
                EXPECT_EQ(checks, 1U);
                EXPECT_TRUE(manager->builtTransporters.empty());
                EXPECT_EQ(result.attemptedKind, AccessTransportKind::UB);
                EXPECT_GT(ApiDeadline::Instance().ApiRemainingUs(), 0);
            }
        }
    }
}

TEST(ReplicaReaderAdmissionTest, SmallFallbackAlsoHonorsDisabledSwitch)
{
    const bool savedFallback = FLAGS_enable_transport_fallback;
    Raii restoreFallback([savedFallback] { FLAGS_enable_transport_fallback = savedFallback; });
    FLAGS_enable_transport_fallback = false;
    ApiDeadlineGuard deadline(1000);
    auto manager = std::make_shared<FakeDataPlaneManager>();
    auto executor = std::make_shared<DataPlaneExecutor>(
        manager, std::make_shared<FixedTransportAdvisor>(TransportHint::UB_CANDIDATE));
    ReplicaReader reader(executor, std::make_shared<DeadlineRetry>(), std::make_shared<ThreadPool>(1),
                         [](const HostPort &, AccessTransportKind &kind) {
                             kind = AccessTransportKind::UB;
                             return Status(K_URMA_READ_SOURCE_DENIED, "UB denied");
                         });
    auto location = MakeReplicaLocation("small-no-fallback", 4, { MakeAddress(37) });
    ObjectReadItemResult result;
    EXPECT_EQ(reader.Read(location, result, std::make_shared<TransportReadContext>()).GetCode(),
              K_URMA_READ_SOURCE_DENIED);
    EXPECT_TRUE(manager->builtTransporters.empty());
    EXPECT_GT(ApiDeadline::Instance().ApiRemainingUs(), 0);
}

TEST(ReplicaReaderAdmissionTest, LargeDeniedReadUsesHealthyReplicaWithoutTcp)
{
    const bool savedFallback = FLAGS_enable_transport_fallback;
    Raii restoreFallback([savedFallback] { FLAGS_enable_transport_fallback = savedFallback; });
    FLAGS_enable_transport_fallback = true;
    for (const bool batch : { false, true }) {
        ApiDeadlineGuard deadline(1000);
        const auto denied = MakeAddress(38);
        const auto healthy = MakeAddress(39);
        auto manager = std::make_shared<FakeDataPlaneManager>();
        auto executor = std::make_shared<DataPlaneExecutor>(
            manager, std::make_shared<FixedTransportAdvisor>(TransportHint::UB_CANDIDATE));
        ReplicaReader reader(
            executor, std::make_shared<DeadlineRetry>(), std::make_shared<ThreadPool>(1),
            [denied](const HostPort &address, AccessTransportKind &kind) {
                kind = AccessTransportKind::UB;
                return address == denied ? Status(K_URMA_READ_SOURCE_DENIED, "UB denied") : Status::OK();
            });
        auto location = MakeReplicaLocation("large-replica-read", 8ULL * 1024 * 1024 + 65536, { denied, healthy });
        ObjectReadItemResult result;
        auto context = std::make_shared<TransportReadContext>();
        ReplicaReadRequest request{ &location, &result, context };
        ASSERT_TRUE((batch ? reader.ReadBatch({ request }) : reader.Read(location, result, context)).IsOk());
        ASSERT_EQ(manager->builtTransporters.size(), 1U);
        EXPECT_EQ(manager->builtTransporters.front()->kind, AccessTransportKind::UB);
        EXPECT_EQ(manager->builtTransporters.front()->providerAddress, healthy);
    }
}

TEST(ReplicaReaderAdmissionTest, FallbackQuotaExhaustionHonorsDeadlineAndReleasesPartialBatch)
{
    const bool savedFallback = FLAGS_enable_transport_fallback;
    Raii restoreFallback([savedFallback] { FLAGS_enable_transport_fallback = savedFallback; });
    FLAGS_enable_transport_fallback = true;
    UrmaFallbackTcpLimiter::Ticket occupied;
    ASSERT_TRUE(UrmaFallbackTcpLimiter::TryAcquireProcessScope(
        UrmaFallbackTcpLimiter::kMaxPendingBytes - 6, Status::OK(), "test", occupied, false).IsOk());
    auto manager = std::make_shared<FakeDataPlaneManager>();
    auto executor = std::make_shared<DataPlaneExecutor>(
        manager, std::make_shared<FixedTransportAdvisor>(TransportHint::UB_CANDIDATE));
    ReplicaReader reader(
        executor, std::make_shared<DeadlineRetry>(), std::make_shared<ThreadPool>(1),
        [](const HostPort &, AccessTransportKind &kind) {
            kind = AccessTransportKind::UB;
            return Status(K_URMA_READ_SOURCE_DENIED, "UB denied");
        });
    auto first = MakeReplicaLocation("quota-a", 4, { MakeAddress(39) });
    auto second = MakeReplicaLocation("quota-b", 4, { MakeAddress(39) });
    ObjectReadItemResult firstResult;
    ObjectReadItemResult secondResult;
    ApiDeadlineGuard deadline(20);
    EXPECT_EQ(reader.ReadBatch({ { &first, &firstResult }, { &second, &secondResult } }).GetCode(),
              K_URMA_READ_SOURCE_DENIED);
    EXPECT_LE(ApiDeadline::Instance().ApiRemainingUs(), 0);
    EXPECT_NE(secondResult.status.GetMsg().find("fallback tcp payload rejected by limiter"), std::string::npos);
    EXPECT_EQ(manager->transportBuildCount, 0);
    UrmaFallbackTcpLimiter::Ticket remaining;
    EXPECT_TRUE(UrmaFallbackTcpLimiter::TryAcquireProcessScope(6, Status::OK(), "test", remaining).IsOk());
}

TEST(TransportLayerAdmissionTest, ReadSourceDeniedReportsUbKindWithoutTouchingTracker)
{
    // A quarantined UB read source must surface the denial and report the denied UB medium via the
    // out-param, while the request-scoped tracker stays untouched for caller-thread aggregation.
    AccessTransportTracker::Reset();
    const auto provider = MakeAddress(40);
    auto filter = std::make_shared<UbHealthFilter>();
    ProviderUbFailureDetailPb detail;
    FillProviderUbFailureDetail(Status(K_URMA_ERROR, "provider write failed"), "client-receive-endpoint",
                                provider.ToString(), 4, 4, detail);
    ASSERT_TRUE(filter->ReportProviderFailure(provider, detail));

    TestTransportLayer layer(std::make_shared<FakeDataPlaneManager>(),
                             std::make_shared<FixedTransportAdvisor>(TransportHint::UB_CANDIDATE),
                             filter);

    AccessTransportKind deniedKind = AccessTransportKind::SHM;
    Status rc = layer.CheckReadSource(provider, deniedKind);
    EXPECT_EQ(rc.GetCode(), K_URMA_READ_SOURCE_DENIED);
    EXPECT_EQ(deniedKind, AccessTransportKind::UB);
    EXPECT_EQ(AccessTransportTracker::ToString(), "SHM");

    AccessTransportKind healthyKind = AccessTransportKind::SHM;
    EXPECT_TRUE(layer.CheckReadSource(MakeAddress(41), healthyKind).IsOk());
    EXPECT_EQ(healthyKind, AccessTransportKind::SHM);
    EXPECT_EQ(AccessTransportTracker::ToString(), "SHM");
}

TEST(ReplicaReaderAdmissionTest, DeniedUbSourceThenFailedTcpReplicaReportsTcpAsAttemptedKind)
{
    // Review scenario: the first replica is denied by the UB admission check, the second replica
    // actually executes over TCP and fails. The item's attempted kind must be TCP (the highest medium
    // really tried), not the earlier UB denial, and the request tracker stays untouched.
    ApiDeadlineGuard deadline(1000);
    AccessTransportTracker::Reset();
    const auto deniedProvider = MakeAddress(42);
    const auto tcpProvider = MakeAddress(43);
    auto manager = std::make_shared<FakeDataPlaneManager>();
    manager->transporterGetStatuses[tcpProvider] = { Status(K_INVALID, "tcp attempt failed") };
    auto executor = std::make_shared<DataPlaneExecutor>(
        manager, std::make_shared<FixedTransportAdvisor>(TransportHint::TCP_ONLY));
    ReplicaReader reader(
        executor, std::make_shared<DeadlineRetry>(), std::make_shared<ThreadPool>(1),
        [deniedProvider](const HostPort &address, AccessTransportKind &deniedKind) {
            if (address != deniedProvider) {
                return Status::OK();
            }
            deniedKind = AccessTransportKind::UB;
            return Status(K_URMA_DATA_WORKER_UNAVAILABLE, "authoritative UB source unavailable");
        });
    auto location = MakeReplicaLocation("denied-then-tcp", 4, { deniedProvider, tcpProvider });
    ObjectReadItemResult result;
    ReplicaReadBatch requests{ { &location, &result } };

    EXPECT_EQ(reader.ReadBatch(requests).GetCode(), K_INVALID);
    EXPECT_EQ(result.attemptedKind, AccessTransportKind::TCP);
    EXPECT_EQ(AccessTransportTracker::ToString(), "SHM");
}

TEST(ReplicaReaderAdmissionTest, AuthoritativeUbDenialKeepsAttemptedKindAtUb)
{
    // Regression guard for the original bug: when only the UB admission denial happens (no replica
    // executes), attemptedKind stays UB so the access log reports UB instead of the SHM default.
    ApiDeadlineGuard deadline(1000);
    const auto deniedProvider = MakeAddress(44);
    auto manager = std::make_shared<FakeDataPlaneManager>();
    auto executor = std::make_shared<DataPlaneExecutor>(manager, std::make_shared<TransportAdvisor>());
    ReplicaReader reader(
        executor, std::make_shared<DeadlineRetry>(), std::make_shared<ThreadPool>(1),
        [deniedProvider](const HostPort &address, AccessTransportKind &deniedKind) {
            if (address != deniedProvider) {
                return Status::OK();
            }
            deniedKind = AccessTransportKind::UB;
            return Status(K_URMA_DATA_WORKER_UNAVAILABLE, "authoritative UB source unavailable");
        });
    auto location = MakeReplicaLocation("ub-only-denied", 4, { deniedProvider });
    ObjectReadItemResult result;
    ReplicaReadBatch requests{ { &location, &result } };

    EXPECT_EQ(reader.ReadBatch(requests).GetCode(), K_URMA_DATA_WORKER_UNAVAILABLE);
    EXPECT_EQ(manager->transportBuildCount, 0);
    EXPECT_EQ(result.attemptedKind, AccessTransportKind::UB);
}

TEST(UbHealthFilterTest, NewTopologyIncarnationClearsOldLocalObservation)
{
    const auto provider = MakeAddress(32);
    UbHealthFilter filter;
    ClusterTopologyPb initial;
    (*initial.mutable_members())[provider.ToString()].set_id("incarnation-a");
    filter.ApplyTopologyIncarnations(initial);
    ProviderUbFailureDetailPb detail;
    FillProviderUbFailureDetail(Status(K_URMA_ERROR, "provider write failed"), "client-receive-endpoint",
                                provider.ToString(), 4, 4, detail);

    ASSERT_TRUE(filter.ReportProviderFailure(provider, detail));
    EXPECT_FALSE(filter.IsAvailable(provider));
    filter.ApplyTopologyIncarnations(initial);
    EXPECT_FALSE(filter.IsAvailable(provider));

    ClusterTopologyPb restarted = initial;
    (*restarted.mutable_members())[provider.ToString()].set_id("incarnation-b");
    filter.ApplyTopologyIncarnations(restarted);
    EXPECT_TRUE(filter.IsAvailable(provider));
    EXPECT_FALSE(filter.GetLocalObservation(provider).has_value());
}

TEST(UbHealthFilterTest, Cqe9QuarantinesOnlyWriteTargetAndProbeRecoversIt)
{
    UbHealthFilter filter;
    const auto worker = MakeAddress(54);
    EXPECT_TRUE(filter.ReportWriteTargetFailure(worker, Status(K_URMA_ERROR, "remote ack timeout"),
                                                std::nullopt, URMA_REMOTE_ACK_TIMEOUT_STATUS));
    EXPECT_FALSE(filter.IsWriteTargetAvailable(worker));
    EXPECT_TRUE(filter.IsAvailable(worker));
    ASSERT_EQ(filter.GetUnavailableWriteTargets().size(), 1U);

    auto state = filter.GetWriteTargetObservation(worker);
    ASSERT_TRUE(state.has_value());
    auto candidate = filter.TryBeginWriteTargetRecovery(state->backoffDeadlineMs);
    ASSERT_TRUE(candidate.has_value());
    EXPECT_FALSE(filter.IsWriteTargetAvailable(worker));
    EXPECT_TRUE(filter.CompleteWriteTargetRecovery(*candidate, Status::OK(), state->backoffDeadlineMs));
    EXPECT_TRUE(filter.IsWriteTargetAvailable(worker));
    EXPECT_TRUE(filter.GetUnavailableWriteTargets().empty());
}

TEST(UbHealthFilterTest, LateCqe9UsesPeerGenerationFence)
{
    UbHealthFilter filter;
    const auto worker = MakeAddress(55);
    const uint64_t peerToken = filter.CaptureWriteTargetCompletionGeneration(worker);
    ASSERT_NE(peerToken, 0U);
    filter.ReportLateWriteTargetFailure(
        UrmaLateCompletion{ 5005, URMA_REMOTE_ACK_TIMEOUT_STATUS, worker.ToString(), "worker-incarnation" },
        peerToken);
    EXPECT_FALSE(filter.IsWriteTargetAvailable(worker));

    auto state = filter.GetWriteTargetObservation(worker);
    ASSERT_TRUE(state.has_value());
    auto candidate = filter.TryBeginWriteTargetRecovery(state->backoffDeadlineMs);
    ASSERT_TRUE(candidate.has_value());
    ASSERT_TRUE(filter.CompleteWriteTargetRecovery(*candidate, Status::OK(), state->backoffDeadlineMs));
    filter.ReportLateWriteTargetFailure(
        UrmaLateCompletion{ 5006, URMA_REMOTE_ACK_TIMEOUT_STATUS, worker.ToString(), "old-incarnation" }, peerToken);
    EXPECT_TRUE(filter.IsWriteTargetAvailable(worker));
}

TEST(UbHealthFilterTest, TopologyIncarnationChangeInvalidatesLateWriteTargetCompletion)
{
    UbHealthFilter filter;
    const auto worker = MakeAddress(58);
    ClusterTopologyPb initial;
    (*initial.mutable_members())[worker.ToString()].set_id("incarnation-a");
    filter.ApplyTopologyIncarnations(initial);
    const uint64_t peerToken = filter.CaptureWriteTargetCompletionGeneration(worker);
    ASSERT_NE(peerToken, 0U);

    ClusterTopologyPb restarted = initial;
    (*restarted.mutable_members())[worker.ToString()].set_id("incarnation-b");
    filter.ApplyTopologyIncarnations(restarted);
    filter.ReportLateWriteTargetFailure(
        UrmaLateCompletion{ 5007, URMA_REMOTE_ACK_TIMEOUT_STATUS, worker.ToString(), "incarnation-a" }, peerToken);

    EXPECT_TRUE(filter.IsWriteTargetAvailable(worker));
    EXPECT_TRUE(filter.GetUnavailableWriteTargets().empty());
}

TEST(UbHealthFilterTest, FirstTrustedTopologyIncarnationClearsUnversionedObservation)
{
    const auto provider = MakeAddress(33);
    UbHealthFilter filter;
    ProviderUbFailureDetailPb detail;
    FillProviderUbFailureDetail(Status(K_URMA_ERROR, "provider write failed"), "client-receive-endpoint",
                                provider.ToString(), 4, 4, detail);

    ASSERT_TRUE(filter.ReportProviderFailure(provider, detail));
    EXPECT_FALSE(filter.IsAvailable(provider));

    ClusterTopologyPb admitted;
    (*admitted.mutable_members())[provider.ToString()].set_id("incarnation-a");
    filter.ApplyTopologyIncarnations(admitted);
    EXPECT_TRUE(filter.IsAvailable(provider));
    EXPECT_FALSE(filter.GetLocalObservation(provider).has_value());
}

TEST(UbHealthFilterTest, AuthoritativeTopologyRemovalClearsClientObservation)
{
    const auto provider = MakeAddress(36);
    UbHealthFilter filter;
    ClusterTopologyPb topology;
    (*topology.mutable_members())[provider.ToString()].set_id("incarnation-a");
    filter.ApplyTopologyIncarnations(topology);
    ProviderUbFailureDetailPb detail;
    FillProviderUbFailureDetail(Status(K_URMA_ERROR, "provider write failed"), "client-receive-endpoint",
                                provider.ToString(), 4, 4, detail);
    ASSERT_TRUE(filter.ReportProviderFailure(provider, detail));
    ASSERT_FALSE(filter.IsAvailable(provider));

    filter.ApplyTopologyIncarnations(ClusterTopologyPb{});

    EXPECT_TRUE(filter.IsAvailable(provider));
    EXPECT_FALSE(filter.GetLocalObservation(provider).has_value());
    UbHealthSummary staleSummary;
    staleSummary.worker = provider;
    staleSummary.incarnation = "incarnation-a";
    staleSummary.epoch = 2;
    staleSummary.writable = false;
    EXPECT_FALSE(filter.ApplySummary(staleSummary, staleSummary.incarnation));
    EXPECT_TRUE(filter.IsAvailable(provider));
}

TEST(UbHealthFilterTest, FirstTrustedSummaryIncarnationClearsUnversionedObservation)
{
    const auto provider = MakeAddress(34);
    UbHealthFilter filter;
    ProviderUbFailureDetailPb detail;
    FillProviderUbFailureDetail(Status(K_URMA_ERROR, "provider write failed"), "client-receive-endpoint",
                                provider.ToString(), 4, 4, detail);
    ASSERT_TRUE(filter.ReportProviderFailure(provider, detail));
    UbHealthSummary summary;
    summary.worker = provider;
    summary.incarnation = "incarnation-a";

    EXPECT_FALSE(filter.ApplySummary(summary, "incarnation-b"));
    EXPECT_FALSE(filter.IsAvailable(provider));
    EXPECT_TRUE(filter.ApplySummary(summary, summary.incarnation));
    EXPECT_TRUE(filter.IsAvailable(provider));
    EXPECT_FALSE(filter.GetLocalObservation(provider).has_value());
}

TEST(UbHealthFilterTest, SummaryIncarnationFencesLocalObservation)
{
    const auto provider = MakeAddress(35);
    UbHealthFilter filter;
    UbHealthSummary summary;
    summary.worker = provider;
    summary.incarnation = "incarnation-a";
    ASSERT_TRUE(filter.ApplySummary(summary, summary.incarnation));
    ProviderUbFailureDetailPb detail;
    FillProviderUbFailureDetail(Status(K_URMA_ERROR, "provider write failed"), "client-receive-endpoint",
                                provider.ToString(), 4, 4, detail);

    ASSERT_TRUE(filter.ReportProviderFailure(provider, detail));
    ++summary.epoch;
    EXPECT_TRUE(filter.ApplySummary(summary, summary.incarnation));
    EXPECT_FALSE(filter.IsAvailable(provider));
    EXPECT_TRUE(filter.GetLocalObservation(provider).has_value());

    summary.incarnation = "incarnation-b";
    summary.epoch = 1;
    summary.writable = false;
    EXPECT_TRUE(filter.ApplySummary(summary, summary.incarnation));
    EXPECT_FALSE(filter.GetLocalObservation(provider).has_value());
    EXPECT_FALSE(filter.IsAvailable(provider));

    ++summary.epoch;
    summary.writable = true;
    EXPECT_TRUE(filter.ApplySummary(summary, summary.incarnation));
    EXPECT_TRUE(filter.IsAvailable(provider));
}

TEST(UbHealthFilterTest, SameIncarnationWritableRecoveryClearsClientObservation)
{
    const auto provider = MakeAddress(37);
    UbHealthFilter filter;
    UbHealthSummary summary;
    summary.worker = provider;
    summary.incarnation = "incarnation-a";
    summary.epoch = 1;
    summary.writable = false;
    ASSERT_TRUE(filter.ApplySummary(summary, summary.incarnation));

    ProviderUbFailureDetailPb detail;
    FillProviderUbFailureDetail(Status(K_URMA_ERROR, "provider write failed"), "client-receive-endpoint",
                                provider.ToString(), 4, 4, detail);
    ASSERT_TRUE(filter.ReportProviderFailure(provider, detail));
    ASSERT_TRUE(filter.GetLocalObservation(provider).has_value());
    ASSERT_FALSE(filter.IsAvailable(provider));

    ++summary.epoch;
    summary.writable = true;
    ASSERT_TRUE(filter.ApplySummary(summary, summary.incarnation));
    EXPECT_FALSE(filter.GetLocalObservation(provider).has_value());
    EXPECT_TRUE(filter.IsAvailable(provider));
}

TEST(TransportLayerAdmissionTest, GlobalUnavailableSchedulesProviderRecoveryOnce)
{
    const auto provider = MakeAddress(38);
    auto filter = std::make_shared<UbHealthFilter>();
    ClusterTopologyPb topology;
    (*topology.mutable_members())[provider.ToString()].set_id("incarnation-a");
    filter->ApplyTopologyIncarnations(topology);
    UbHealthSummary summary;
    summary.worker = provider;
    summary.incarnation = "incarnation-a";
    summary.writable = false;
    summary.epoch = 4;
    auto manager = std::make_shared<FakeDataPlaneManager>();
    manager->providerProbeSummary = summary;
    manager->providerProbeSummary.writable = true;
    ++manager->providerProbeSummary.epoch;
    TestTransportLayer layer(manager, std::make_shared<FixedTransportAdvisor>(TransportHint::UB_CANDIDATE), filter);
    ASSERT_TRUE(layer.Init().IsOk());
    ASSERT_TRUE(filter->ApplySummary(summary, summary.incarnation));

    EXPECT_TRUE(layer.ScheduleProviderRecoveryFromGlobalSummary(provider));
    EXPECT_FALSE(layer.ScheduleProviderRecoveryFromGlobalSummary(provider));
    ASSERT_TRUE(manager->WaitForProviderProbeCount(1, PROBE_OBSERVATION_TIMEOUT));
    EXPECT_EQ(manager->providerProbedWorkers, std::vector<HostPort>{ provider });
    // The fake records a probe before TransportLayer applies its successful result.
    const auto deadline = std::chrono::steady_clock::now() + PROBE_OBSERVATION_TIMEOUT;
    while (!filter->IsAvailable(provider) && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    EXPECT_TRUE(filter->IsAvailable(provider));
}

TEST(TransportLayerAdmissionTest, SummaryRecoveryCallbackStopsAtShutdownAndSurvivesDestruction)
{
    const auto provider = MakeAddress(38);
    auto filter = std::make_shared<UbHealthFilter>();
    UbHealthSummary summary;
    summary.worker = provider;
    summary.incarnation = "incarnation-a";
    summary.writable = false;
    summary.epoch = 1;
    ASSERT_TRUE(filter->ApplySummary(summary, summary.incarnation));
    auto manager = std::make_shared<FakeDataPlaneManager>();
    auto layer = std::make_unique<TestTransportLayer>(
        manager, std::make_shared<FixedTransportAdvisor>(TransportHint::UB_CANDIDATE), filter);
    auto callback = layer->MakeProviderRecoveryCallback();
    layer->Shutdown();
    EXPECT_FALSE(layer->ScheduleProviderRecoveryFromGlobalSummary(provider));
    callback(provider);
    EXPECT_FALSE(filter->GetLocalObservation(provider).has_value());
    layer.reset();
    callback(provider);
    EXPECT_FALSE(filter->GetLocalObservation(provider).has_value());
}

TEST(UbHealthFilterTest, OnDemandRecoveryRequiresWritableSummaryAndDirectionalProbe)
{
    const auto provider = MakeAddress(38);
    UbHealthFilter filter;
    ClusterTopologyPb topology;
    (*topology.mutable_members())[provider.ToString()].set_id("incarnation-a");
    filter.ApplyTopologyIncarnations(topology);
    ProviderUbFailureDetailPb detail;
    FillProviderUbFailureDetail(Status(K_URMA_ERROR, "provider write failed"), "client-receive-endpoint",
                                provider.ToString(), 4, 4, detail);
    ASSERT_TRUE(filter.ReportProviderFailure(provider, detail));

    auto firstDeadline = filter.NextProviderRecoveryDeadlineMs();
    ASSERT_TRUE(firstDeadline.has_value());
    auto first = filter.TryBeginProviderRecovery(*firstDeadline);
    ASSERT_TRUE(first.has_value());
    EXPECT_EQ(first->expectedIncarnation, "incarnation-a");
    UbHealthSummary summary;
    summary.worker = provider;
    summary.incarnation = "incarnation-a";
    summary.writable = false;
    EXPECT_FALSE(filter.CompleteProviderRecovery(*first, summary, Status::OK(), *firstDeadline));
    EXPECT_FALSE(filter.IsAvailable(provider));

    auto secondDeadline = filter.NextProviderRecoveryDeadlineMs();
    ASSERT_TRUE(secondDeadline.has_value());
    auto second = filter.TryBeginProviderRecovery(*secondDeadline);
    ASSERT_TRUE(second.has_value());
    summary.writable = true;
    EXPECT_FALSE(filter.CompleteProviderRecovery(*second, summary, Status(K_URMA_ERROR, "probe failed"),
                                                 *secondDeadline));
    EXPECT_FALSE(filter.IsAvailable(provider));

    auto thirdDeadline = filter.NextProviderRecoveryDeadlineMs();
    ASSERT_TRUE(thirdDeadline.has_value());
    auto third = filter.TryBeginProviderRecovery(*thirdDeadline);
    ASSERT_TRUE(third.has_value());
    EXPECT_TRUE(filter.CompleteProviderRecovery(*third, summary, Status::OK(), *thirdDeadline));
    EXPECT_TRUE(filter.IsAvailable(provider));
}

// TransportLayer binds the remote port-health verifier unconditionally, so on the client every
// Provider recovery probe ends in CompleteProviderRecovery's diagnostic branch. It must advance
// the probe backoff; otherwise ReconcileLoop re-arms the same peer every RPC round trip.
TEST(UbHealthFilterTest, VerifierBoundProviderProbeBacksOffAfterDiagnosticCompletion)
{
    const auto provider = MakeAddress(40);
    UbHealthFilter filter;
    uint32_t verifications = 0;
    filter.SetRemotePortHealthVerificationTrigger([&verifications](const HostPort &) { ++verifications; });
    ClusterTopologyPb topology;
    (*topology.mutable_members())[provider.ToString()].set_id("incarnation-a");
    filter.ApplyTopologyIncarnations(topology);
    ProviderUbFailureDetailPb detail;
    FillProviderUbFailureDetail(Status(K_RPC_DEADLINE_EXCEEDED, "provider read timed out"),
                                "client-receive-endpoint", provider.ToString(), std::nullopt, std::nullopt, detail);
    EXPECT_FALSE(filter.ReportProviderFailure(provider, detail));

    auto deadline = filter.NextProviderRecoveryDeadlineMs();
    ASSERT_TRUE(deadline.has_value());
    auto candidate = filter.TryBeginProviderRecovery(*deadline);
    ASSERT_TRUE(candidate.has_value());
    UbHealthSummary summary;
    summary.worker = provider;
    summary.incarnation = "incarnation-a";
    summary.writable = true;
    EXPECT_FALSE(filter.CompleteProviderRecovery(*candidate, summary, Status::OK(), *deadline));
    EXPECT_EQ(verifications, 1U);

    ASSERT_EQ(filter.NextProviderRecoveryDeadlineMs(), std::optional<uint64_t>{ *deadline + 1'000 });
    EXPECT_FALSE(filter.TryBeginProviderRecovery(*deadline + 999).has_value());
}

TEST(UbHealthFilterTest, NewFailureInvalidatesInFlightProviderRecovery)
{
    const auto provider = MakeAddress(39);
    UbHealthFilter filter;
    ClusterTopologyPb topology;
    (*topology.mutable_members())[provider.ToString()].set_id("incarnation-a");
    filter.ApplyTopologyIncarnations(topology);
    ProviderUbFailureDetailPb detail;
    FillProviderUbFailureDetail(Status(K_URMA_ERROR, "provider write failed"), "client-receive-endpoint",
                                provider.ToString(), 4, 4, detail);
    ASSERT_TRUE(filter.ReportProviderFailure(provider, detail));
    auto deadline = filter.NextProviderRecoveryDeadlineMs();
    ASSERT_TRUE(deadline.has_value());
    auto stale = filter.TryBeginProviderRecovery(*deadline);
    ASSERT_TRUE(stale.has_value());
    ASSERT_TRUE(filter.ReportProviderFailure(provider, detail));

    UbHealthSummary summary;
    summary.worker = provider;
    summary.incarnation = "incarnation-a";
    EXPECT_FALSE(filter.CompleteProviderRecovery(*stale, summary, Status::OK(), *deadline));
    EXPECT_FALSE(filter.IsAvailable(provider));
}

TEST(DataPlaneManagerAdmissionTest, ProviderProbeErrorPreservesValidatedSummaryAndTopologyFence)
{
    const auto provider = MakeAddress(43);
    auto manager = std::make_shared<FakeDataPlaneManager>();
    std::shared_ptr<WorkerRpcClient> rpcClient;
    ASSERT_TRUE(manager->GetOrCreateRpcClient(provider, rpcClient).IsOk());
    auto fakeRpcClient = std::dynamic_pointer_cast<FakeWorkerRpcClient>(rpcClient);
    ASSERT_NE(fakeRpcClient, nullptr);

    UbHealthFilter filter;
    ClusterTopologyPb topology;
    (*topology.mutable_members())[provider.ToString()].set_id("incarnation-a");
    filter.ApplyTopologyIncarnations(topology);
    ProviderUbFailureDetailPb detail;
    FillProviderUbFailureDetail(Status(K_URMA_ERROR, "provider write failed"), "client-receive-endpoint",
                                provider.ToString(), 4, 4, detail);
    ASSERT_TRUE(filter.ReportProviderFailure(provider, detail));
    auto deadline = filter.NextProviderRecoveryDeadlineMs();
    ASSERT_TRUE(deadline.has_value());
    auto candidate = filter.TryBeginProviderRecovery(*deadline);
    ASSERT_TRUE(candidate.has_value());

    UbHealthSummary responseSummary;
    responseSummary.worker = provider;
    responseSummary.incarnation = "incarnation-a";
    responseSummary.writable = true;
    responseSummary.state = UbAdmissionState::AVAILABLE;
    responseSummary.reason = UbFailureClass::SUCCESS;
    responseSummary.lastStatusCode = K_OK;
    responseSummary.epoch = 7;
    EncodeUbHealthSummary(responseSummary, *fakeRpcClient->providerProbeResponse.mutable_health_summary());
    fakeRpcClient->providerProbeStatus = Status(K_NOT_READY, "Provider admission is unavailable");

    UbHealthSummary actual;
    Status rc = manager->DataPlaneManager::ProbeProviderUbRecovery(provider, "incarnation-a", 100, actual);

    EXPECT_EQ(rc.GetCode(), K_NOT_READY);
    EXPECT_EQ(actual.worker, provider);
    EXPECT_EQ(actual.incarnation, "incarnation-a");
    EXPECT_TRUE(actual.writable);
    EXPECT_EQ(actual.epoch, 7u);
    EXPECT_FALSE(filter.CompleteProviderRecovery(*candidate, actual, rc, *deadline));
    EXPECT_FALSE(filter.IsAvailable(provider));

    fakeRpcClient->providerProbeResponse.Clear();
    fakeRpcClient->providerProbeStatus = Status(K_RPC_DEADLINE_EXCEEDED, "Provider probe timed out");
    rc = manager->DataPlaneManager::ProbeProviderUbRecovery(provider, "incarnation-a", 100, actual);
    EXPECT_EQ(rc.GetCode(), K_RPC_DEADLINE_EXCEEDED);
    EXPECT_TRUE(actual.worker.Empty());
    EXPECT_TRUE(actual.incarnation.empty());

    responseSummary.incarnation = "incarnation-b";
    ++responseSummary.epoch;
    EncodeUbHealthSummary(responseSummary, *fakeRpcClient->providerProbeResponse.mutable_health_summary());
    fakeRpcClient->providerProbeStatus = Status(K_NOT_READY, "Worker incarnation changed");
    rc = manager->DataPlaneManager::ProbeProviderUbRecovery(provider, "incarnation-a", 100, actual);
    EXPECT_EQ(rc.GetCode(), K_NOT_READY);
    EXPECT_EQ(actual.incarnation, "incarnation-b");

    auto mismatchDeadline = filter.NextProviderRecoveryDeadlineMs();
    ASSERT_TRUE(mismatchDeadline.has_value());
    auto mismatchCandidate = filter.TryBeginProviderRecovery(*mismatchDeadline);
    ASSERT_TRUE(mismatchCandidate.has_value());
    EXPECT_FALSE(filter.CompleteProviderRecovery(*mismatchCandidate, actual, Status::OK(), *mismatchDeadline));
    EXPECT_FALSE(filter.IsAvailable(provider));
}

TEST(TransportLayerAdmissionTest, ProviderRecoveryDoesNotDependOnHeartbeatSummary)
{
    const auto provider = MakeAddress(44);
    auto filter = std::make_shared<UbHealthFilter>();
    ClusterTopologyPb topology;
    (*topology.mutable_members())[provider.ToString()].set_id("incarnation-a");
    filter->ApplyTopologyIncarnations(topology);
    ProviderUbFailureDetailPb detail;
    FillProviderUbFailureDetail(Status(K_URMA_ERROR, "provider write failed"), "client-receive-endpoint",
                                provider.ToString(), 4, 4, detail);
    ASSERT_TRUE(filter->ReportProviderFailure(provider, detail));

    auto manager = std::make_shared<FakeDataPlaneManager>();
    manager->providerProbeSummary.worker = provider;
    manager->providerProbeSummary.incarnation = "incarnation-a";
    manager->providerProbeSummary.writable = true;
    TestTransportLayer layer(manager, std::make_shared<FixedTransportAdvisor>(TransportHint::UB_CANDIDATE),
                             filter);
    ASSERT_TRUE(layer.Init().IsOk());

    ASSERT_TRUE(manager->WaitForProviderProbeCount(1, PROBE_OBSERVATION_TIMEOUT));
    EXPECT_EQ(manager->providerProbedWorkers, std::vector<HostPort>{ provider });
    EXPECT_EQ(manager->providerProbeExpectedIncarnations, std::vector<std::string>{ "incarnation-a" });
    EXPECT_EQ(manager->providerProbeTimeouts, std::vector<int32_t>{ 3'000 });
    EXPECT_TRUE(filter->IsAvailable(provider));
}

// End-to-end rate check through the real ReconcileLoop. With the port-health verifier bound, a
// completed probe carries no verdict, so it must not re-arm the peer before the probe backoff
// elapses. The loop is woken once through the same ApplyWorkerSnapshot path the client uses, then
// runs unattended: the 1s base backoff keeps this under 2 probes, while a missing backoff lets the
// loop spin on the probe RPC and reach the threshold almost immediately after the first probe.
TEST(TransportLayerAdmissionTest, VerifierBoundProviderProbeDoesNotSpinOnReconcileLoop)
{
    constexpr int SPIN_THRESHOLD = 6;
    constexpr std::chrono::seconds OBSERVATION_WINDOW(2);
    const auto provider = MakeAddress(45);
    auto filter = std::make_shared<UbHealthFilter>();
    std::atomic<int> verifications{ 0 };
    filter->SetRemotePortHealthVerificationTrigger(
        [&verifications](const HostPort &) { verifications.fetch_add(1, std::memory_order_acq_rel); });
    ClusterTopologyPb topology;
    (*topology.mutable_members())[provider.ToString()].set_id("incarnation-a");
    filter->ApplyTopologyIncarnations(topology);
    auto manager = std::make_shared<FakeDataPlaneManager>();
    manager->providerProbeSummary.worker = provider;
    manager->providerProbeSummary.incarnation = "incarnation-a";
    manager->providerProbeSummary.writable = true;
    TestTransportLayer layer(manager, std::make_shared<FixedTransportAdvisor>(TransportHint::UB_CANDIDATE),
                             filter);
    ASSERT_TRUE(layer.Init().IsOk());

    ProviderUbFailureDetailPb detail;
    FillProviderUbFailureDetail(Status(K_RPC_DEADLINE_EXCEEDED, "provider read timed out"),
                                "client-receive-endpoint", provider.ToString(), std::nullopt, std::nullopt, detail);
    EXPECT_FALSE(filter->ReportProviderFailure(provider, detail));

    WorkerSnapshot snapshot;
    snapshot.ringVersion = 1;
    snapshot.remoteTransportAddrs = { provider };
    snapshot.workerIncarnations = { { provider, "incarnation-a" } };
    ASSERT_TRUE(layer.ApplyWorkerSnapshot(snapshot).IsOk());

    EXPECT_FALSE(manager->WaitForProviderProbeCount(SPIN_THRESHOLD, OBSERVATION_WINDOW));
    LOG(INFO) << "[UB_PROBE_RATE] probes_in_" << OBSERVATION_WINDOW.count()
              << "s=" << manager->providerProbeCount.load(std::memory_order_acquire)
              << " verifierWakeups=" << verifications.load(std::memory_order_acquire);
}

TEST(TransportLayerAdmissionTest, Cqe4DoesNotDirectlyCloseAdmission)
{
    auto manager = std::make_shared<FakeDataPlaneManager>();
    manager->transporterSetStatuses = { { Status(K_URMA_ERROR, "local sender error 4"), Status::OK() } };
    TestTransportLayer layer(manager, std::make_shared<FixedTransportAdvisor>(TransportHint::UB_CANDIDATE));
    std::shared_ptr<ObjectBuffer> buffer;
    ASSERT_TRUE(layer.Create(MakeAddress(30), "first", 64, MakeCreateParam(), buffer).IsOk());
    manager->builtTransporters.front()->setUbFailureReports = { Status(K_URMA_ERROR, "local sender error 4") };
    manager->builtTransporters.front()->setUbCqeStatuses = { 4 };

    EXPECT_EQ(layer.Set(*buffer, MakeSetParam()).GetCode(), K_URMA_ERROR);
    EXPECT_TRUE(layer.CheckLocalUbSenderAdmission().IsOk());
    ASSERT_EQ(manager->builtTransporters.size(), 2u);
    EXPECT_EQ(manager->builtTransporters.back()->kind, AccessTransportKind::TCP);
    EXPECT_EQ(manager->builtTransporters.back()->releaseCount, 1);
    EXPECT_TRUE(layer.Set(*buffer, MakeSetParam()).IsOk());
    std::shared_ptr<ObjectBuffer> nextBuffer;
    EXPECT_TRUE(layer.Create(MakeAddress(31), "next", 64, MakeCreateParam(), nextBuffer).IsOk());
}

TEST(TransportLayerAdmissionTest, UbAllocationKeepsSetTransportAndExplicitTcpPolicy)
{
    for (const auto hint : { TransportHint::SHM_CANDIDATE, TransportHint::TCP_ONLY }) {
        auto manager = std::make_shared<FakeDataPlaneManager>();
        auto advisor = std::make_shared<FixedTransportAdvisor>(TransportHint::UB_CANDIDATE);
        TestTransportLayer layer(manager, advisor);
        std::shared_ptr<ObjectBuffer> buffer;
        ASSERT_TRUE(layer.Create(MakeAddress(38), "ub-allocation", 64, MakeCreateParam(), buffer).IsOk());
        // Match the allocation metadata installed by UbTransporter::Create after SHM is unavailable.
        ObjectBufferInternal::GetMutableInfo(*buffer).ubUrmaDataInfo = std::make_shared<UrmaRemoteAddrPb>();
        auto ub = manager->builtTransporters.front();
        advisor->SetHint(hint);
        ASSERT_TRUE(layer.Set(*buffer, MakeSetParam()).IsOk());
        EXPECT_EQ(ub->setCount, hint == TransportHint::SHM_CANDIDATE ? 1 : 0);
        if (hint == TransportHint::TCP_ONLY) {
            EXPECT_EQ(manager->builtTransporters.back()->kind, AccessTransportKind::TCP);
            EXPECT_EQ(manager->builtTransporters.back()->setCount, 1);
        }
    }
}

TEST(TransportLayerAdmissionTest, UniformUbMSetKeepsAllocationTransportButMixedBatchKeepsShm)
{
    for (const bool allUb : { true, false }) {
        auto manager = std::make_shared<FakeDataPlaneManager>();
        auto advisor = std::make_shared<FixedTransportAdvisor>(TransportHint::UB_CANDIDATE);
        TestTransportLayer layer(manager, advisor);
        std::vector<std::shared_ptr<ObjectBuffer>> buffers;
        ASSERT_TRUE(layer.MCreate(MakeAddress(39), { "ub-first", "second" }, { 64, 64 },
                                  MakeCreateParam(), buffers).IsOk());
        ObjectBufferInternal::GetMutableInfo(*buffers[0]).ubUrmaDataInfo = std::make_shared<UrmaRemoteAddrPb>();
        if (allUb) {
            ObjectBufferInternal::GetMutableInfo(*buffers[1]).ubUrmaDataInfo = std::make_shared<UrmaRemoteAddrPb>();
        }
        auto ub = manager->builtTransporters.front();
        advisor->SetHint(TransportHint::SHM_CANDIDATE);
        TransportMSetResult result;
        ASSERT_TRUE(layer.MSet(buffers, MakeSetParam(), result).IsOk());
        EXPECT_EQ(result.actualKind, allUb ? AccessTransportKind::UB : AccessTransportKind::SHM);
        EXPECT_EQ(ub->mSetCount, allUb ? 1 : 0);
    }
}

TEST(TransportLayerAdmissionTest, Cqe4DoesNotDirectlyBlockSharedMemoryTransport)
{
    auto manager = std::make_shared<FakeDataPlaneManager>();
    manager->transporterSetStatuses = { { Status(K_URMA_ERROR, "local sender error 4") } };
    auto advisor = std::make_shared<FixedTransportAdvisor>(TransportHint::UB_CANDIDATE);
    TestTransportLayer layer(manager, advisor);
    std::shared_ptr<ObjectBuffer> ubBuffer;
    ASSERT_TRUE(layer.Create(MakeAddress(40), "ub", 64, MakeCreateParam(), ubBuffer).IsOk());
    manager->builtTransporters.front()->setUbFailureReports = { Status(K_URMA_ERROR, "local sender error 4") };
    manager->builtTransporters.front()->setUbCqeStatuses = { 4 };
    ASSERT_EQ(layer.Set(*ubBuffer, MakeSetParam()).GetCode(), K_URMA_ERROR);

    advisor->SetHint(TransportHint::SHM_CANDIDATE);
    std::shared_ptr<ObjectBuffer> shmBuffer;
    EXPECT_TRUE(layer.Create(MakeAddress(41), "shm", 64, MakeCreateParam(), shmBuffer).IsOk());
    ASSERT_NE(shmBuffer, nullptr);
    EXPECT_TRUE(layer.Set(*shmBuffer, MakeSetParam()).IsOk());
    EXPECT_EQ(manager->builtTransporters.back()->kind, AccessTransportKind::SHM);
}

TEST(TransportLayerAdmissionTest, FailureNotificationCannotBeLostAfterDeadlineCheck)
{
    const auto provider = MakeAddress(46);
    auto filter = std::make_shared<UbHealthFilter>();
    ClusterTopologyPb topology;
    (*topology.mutable_members())[provider.ToString()].set_id("incarnation-a");
    filter->ApplyTopologyIncarnations(topology);
    auto manager = std::make_shared<FakeDataPlaneManager>();
    manager->providerProbeSummary.worker = provider;
    manager->providerProbeSummary.incarnation = "incarnation-a";
    manager->providerProbeSummary.writable = true;
    TestTransportLayer layer(manager, std::make_shared<FixedTransportAdvisor>(TransportHint::UB_CANDIDATE),
                             filter);
    ProviderUbFailureDetailPb detail;
    FillProviderUbFailureDetail(Status(K_URMA_ERROR, "provider write failed"), "client-receive-endpoint",
                                provider.ToString(), 4, 4, detail);

    std::future<bool> reportFuture;
    ASSERT_TRUE(inject::Set(RECONCILE_AFTER_DEADLINE_CHECK_INJECT, "1*pause()").IsOk());
    Raii clearInject([] { (void)inject::Clear(RECONCILE_AFTER_DEADLINE_CHECK_INJECT); });
    ASSERT_TRUE(layer.Init().IsOk());
    ASSERT_TRUE(WaitUntil(
        [] { return inject::GetExecuteCount(RECONCILE_AFTER_DEADLINE_CHECK_INJECT) == 1; }));

    reportFuture = std::async(std::launch::async, [&] { return layer.ReportProviderFailure(provider, detail); });
    ASSERT_TRUE(WaitUntil([&] { return !filter->IsAvailable(provider); }));
    EXPECT_EQ(reportFuture.wait_for(std::chrono::milliseconds(50)), std::future_status::timeout);
    ASSERT_TRUE(inject::Clear(RECONCILE_AFTER_DEADLINE_CHECK_INJECT).IsOk());

    EXPECT_TRUE(reportFuture.get());
    EXPECT_TRUE(manager->WaitForProviderProbeCount(1, PROBE_OBSERVATION_TIMEOUT));
}

TEST(DataPlaneManagerAdmissionTest, ProbeRequiresPublishedWorkerSnapshot)
{
    auto manager = std::make_shared<FakeDataPlaneManager>();
    ASSERT_TRUE(manager->Init().IsOk());
    bool committed = false;

    Status rc = manager->ProbeUbConnection(MakeAddress(52), [&] { committed = true; });

    EXPECT_EQ(rc.GetCode(), K_NOT_READY);
    EXPECT_EQ(manager->probeCount, 0);
    EXPECT_FALSE(committed);
}

TEST(UrmaRecoveryProbeBufferTest, ManagerOwnsStableDedicatedSegment)
{
#if defined(USE_URMA) || defined(USE_URMA_MOCK)
    uint64_t sourceAddress = 0;
    uint64_t sourceSize = 0;
    uint64_t sourceDataAddress = 0;
    ASSERT_TRUE(
        UrmaManager::Instance().GetRecoveryProbeSourceInfo(sourceAddress, sourceSize, sourceDataAddress).IsOk());
    UrmaHandshakeReqPb recoveryHandshake;
    UrmaRemoteAddrPb recoveryAddress;
    ASSERT_TRUE(ConstructRecoveryProbeHandshakePb(MakeAddress(52).ToString(), recoveryHandshake, recoveryAddress)
                    .IsOk());
    ASSERT_EQ(recoveryHandshake.seg_infos_size(), 1);
    UrmaSeg recoverySegment;
    ASSERT_TRUE(recoverySegment.FromProto(recoveryHandshake.seg_infos(0).seg()).IsOk());
    const uint64_t segmentAddress = recoveryAddress.seg_va();
    ASSERT_NE(segmentAddress, 0u);
    EXPECT_NE(segmentAddress, sourceAddress);
    EXPECT_EQ(recoverySegment.raw.ubva.va, segmentAddress);
    EXPECT_EQ(recoveryAddress.seg_data_offset(), 0u);
    EXPECT_EQ(recoveryAddress.request_address().host(), recoveryHandshake.address().host());
    EXPECT_EQ(recoveryAddress.request_address().port(), recoveryHandshake.address().port());
    EXPECT_EQ(recoveryAddress.client_id(), recoveryHandshake.client_id());

    uint64_t secondAddress = 0;
    uint64_t secondOffset = 0;
    ASSERT_TRUE(UrmaManager::Instance().GetRecoveryProbeSegmentInfo(secondAddress, secondOffset).IsOk());
    EXPECT_EQ(secondAddress, segmentAddress);
    EXPECT_EQ(secondOffset, 0u);

    auto containsSegment = [segmentAddress](const UrmaHandshakeReqPb &handshake) {
        for (const auto &info : handshake.seg_infos()) {
            UrmaSeg segment;
            if (segment.FromProto(info.seg()).IsOk() && segment.raw.ubva.va == segmentAddress) {
                return true;
            }
        }
        return false;
    };
    UrmaHandshakeReqPb handshake;
    ASSERT_TRUE(UrmaManager::Instance().GetSegmentInfo(handshake).IsOk());
    EXPECT_TRUE(containsSegment(handshake));
    EXPECT_GT(handshake.seg_infos_size(), recoveryHandshake.seg_infos_size());
#else
    GTEST_SKIP() << "URMA recovery probe segment is only available in URMA or URMA mock builds.";
#endif
}

TEST(UrmaRecoveryProbeBufferTest, ManagerOwnsSmallStableSourceSegment)
{
#if defined(USE_URMA) || defined(USE_URMA_MOCK)
    uint64_t sourceAddress = 0;
    uint64_t sourceSize = 0;
    uint64_t sourceDataAddress = 0;
    ASSERT_TRUE(
        UrmaManager::Instance().GetRecoveryProbeSourceInfo(sourceAddress, sourceSize, sourceDataAddress).IsOk());
    EXPECT_NE(sourceAddress, 0u);
    EXPECT_EQ(sourceSize, 4'096u);
    EXPECT_EQ(sourceDataAddress, sourceAddress);

    uint64_t secondAddress = 0;
    ASSERT_TRUE(
        UrmaManager::Instance().GetRecoveryProbeSourceInfo(secondAddress, sourceSize, sourceDataAddress).IsOk());
    EXPECT_EQ(secondAddress, sourceAddress);
    uint64_t destinationAddress = 0;
    uint64_t destinationOffset = 0;
    ASSERT_TRUE(
        UrmaManager::Instance().GetRecoveryProbeSegmentInfo(destinationAddress, destinationOffset).IsOk());
    EXPECT_NE(destinationAddress, sourceAddress);
#else
    GTEST_SKIP() << "URMA recovery probe segment is only available in URMA or URMA mock builds.";
#endif
}

TEST(DataPlaneManagerAdmissionTest, ProbeRejectsMembershipWorkerDeniedByGlobalFact)
{
    auto manager = std::make_shared<FakeDataPlaneManager>();
    ASSERT_TRUE(manager->Init().IsOk());
    WorkerSnapshot denied;
    denied.ringVersion = 1;
    denied.remoteTransportAddrs = { MakeAddress(53) };
    ASSERT_TRUE(manager->UpdateWorkerSnapshot(denied).IsOk());
    bool committed = false;

    Status rc = manager->ProbeUbConnection(MakeAddress(53), [&] { committed = true; });

    EXPECT_EQ(rc.GetCode(), K_NOT_FOUND);
    EXPECT_EQ(manager->probeCount, 0);
    EXPECT_FALSE(committed);
}

TEST(DataPlaneManagerAdmissionTest, ProbeCommitAndSnapshotPostCheckAreAtomic)
{
    auto manager = std::make_shared<FakeDataPlaneManager>();
    ASSERT_TRUE(manager->Init().IsOk());
    const auto workerAddr = MakeAddress(47);
    WorkerSnapshot admitted;
    admitted.ringVersion = 1;
    admitted.remoteTransportAddrs = { workerAddr };
    admitted.writeProbeAddrs = admitted.remoteTransportAddrs;
    ASSERT_TRUE(manager->UpdateWorkerSnapshot(admitted).IsOk());
    std::promise<void> commitStarted;
    auto commitStartedFuture = commitStarted.get_future();
    std::promise<void> allowCommit;
    auto allowCommitFuture = allowCommit.get_future().share();

    auto probe = std::async(std::launch::async, [&] {
        return manager->ProbeUbConnection(workerAddr, [&] {
            commitStarted.set_value();
            allowCommitFuture.wait();
        });
    });
    ASSERT_EQ(commitStartedFuture.wait_for(std::chrono::seconds(1)), std::future_status::ready);
    WorkerSnapshot denied;
    denied.ringVersion = 2;
    auto update = std::async(std::launch::async, [&] { return manager->UpdateWorkerSnapshot(denied); });
    EXPECT_EQ(update.wait_for(std::chrono::milliseconds(50)), std::future_status::timeout);
    allowCommit.set_value();

    EXPECT_TRUE(probe.get().IsOk());
    EXPECT_TRUE(update.get().IsOk());
}

TEST(TransportLayerAdmissionTest, ShutdownWaitsForAdmittedUbOperationsBeforeClosingSender)
{
    auto manager = std::make_shared<FakeDataPlaneManager>();
    manager->transporterSetStatuses = { { Status::OK(), Status::OK() } };
    TestTransportLayer layer(manager, std::make_shared<FixedTransportAdvisor>(TransportHint::UB_CANDIDATE));
    std::shared_ptr<ObjectBuffer> first;
    std::shared_ptr<ObjectBuffer> second;
    std::shared_ptr<ObjectBuffer> rejected;
    ASSERT_TRUE(layer.Create(MakeAddress(54), "first", 4, MakeCreateParam(), first).IsOk());
    ASSERT_TRUE(layer.Create(MakeAddress(54), "second", 4, MakeCreateParam(), second).IsOk());
    ASSERT_TRUE(layer.Create(MakeAddress(54), "rejected", 4, MakeCreateParam(), rejected).IsOk());
    auto transporter = manager->builtTransporters.front();
    transporter->coordinateConcurrentSets = true;

    struct SetCall {
        TestTransportLayer *layer;
        ObjectBuffer *buffer;
        Status result;
    } firstCall{ &layer, first.get(), Status::OK() }, secondCall{ &layer, second.get(), Status::OK() };
    auto runSet = [](void *arg) -> void * {
        auto &call = *static_cast<SetCall *>(arg);
        call.result = call.layer->Set(*call.buffer, MakeSetParam());
        return nullptr;
    };
    bthread_t firstSet;
    bthread_t secondSet;
    ASSERT_EQ(bthread_start_background(&firstSet, nullptr, runSet, &firstCall), 0);
    if (bthread_start_background(&secondSet, nullptr, runSet, &secondCall) != 0) {
        transporter->ReleaseBlockedSet();
        EXPECT_EQ(bthread_join(firstSet, nullptr), 0);
        FAIL() << "Failed to start the second Set bthread";
    }
    if (!transporter->WaitForSetCount(2, std::chrono::seconds(1))) {
        transporter->ReleaseBlockedSet();
        EXPECT_EQ(bthread_join(firstSet, nullptr), 0);
        EXPECT_EQ(bthread_join(secondSet, nullptr), 0);
        FAIL() << "Both Set bthreads were not admitted";
    }

    struct ShutdownCall {
        TestTransportLayer *layer;
        bthread::CountdownEvent done{ 1 };
    } shutdownCall{ &layer };
    auto runShutdown = [](void *arg) -> void * {
        auto &call = *static_cast<ShutdownCall *>(arg);
        call.layer->Shutdown();
        call.done.signal();
        return nullptr;
    };
    bthread_t shutdown;
    if (bthread_start_background(&shutdown, nullptr, runShutdown, &shutdownCall) != 0) {
        transporter->ReleaseBlockedSet();
        EXPECT_EQ(bthread_join(firstSet, nullptr), 0);
        EXPECT_EQ(bthread_join(secondSet, nullptr), 0);
        FAIL() << "Failed to start the Shutdown bthread";
    }
    if (!WaitUntil([&] { return layer.CheckLocalUbSenderAdmission().GetCode() == K_SHUTTING_DOWN; },
                   std::chrono::seconds(1))) {
        transporter->ReleaseBlockedSet();
        EXPECT_EQ(bthread_join(firstSet, nullptr), 0);
        EXPECT_EQ(bthread_join(secondSet, nullptr), 0);
        EXPECT_EQ(bthread_join(shutdown, nullptr), 0);
        FAIL() << "Shutdown did not close sender admission";
    }
    EXPECT_NE(shutdownCall.done.timed_wait(butil::milliseconds_from_now(50)), 0);
    EXPECT_EQ(layer.Set(*rejected, MakeSetParam()).GetCode(), K_SHUTTING_DOWN);
    EXPECT_EQ(transporter->GetSetCount(), 2);

    transporter->ReleaseBlockedSet();
    EXPECT_EQ(bthread_join(firstSet, nullptr), 0);
    EXPECT_EQ(bthread_join(secondSet, nullptr), 0);
    EXPECT_TRUE(firstCall.result.IsOk());
    EXPECT_TRUE(secondCall.result.IsOk());
    EXPECT_EQ(shutdownCall.done.timed_wait(butil::seconds_from_now(1)), 0);
    EXPECT_EQ(bthread_join(shutdown, nullptr), 0);
}

TEST(TransportLayerAdmissionTest, LateCqe4DoesNotDirectlyCloseAdmission)
{
    const auto worker = MakeAddress(40);
    auto manager = std::make_shared<FakeDataPlaneManager>();
    manager->transporterSetStatuses = { { Status(K_URMA_WAIT_TIMEOUT, "write timed out before CQE") } };
    TestTransportLayer layer(manager, std::make_shared<FixedTransportAdvisor>(TransportHint::UB_CANDIDATE));
    std::shared_ptr<ObjectBuffer> buffer;
    ASSERT_TRUE(layer.Create(worker, "late-cqe", 4, MakeCreateParam(), buffer).IsOk());

    EXPECT_EQ(layer.Set(*buffer, MakeSetParam()).GetCode(), K_URMA_WAIT_TIMEOUT);
    ASSERT_EQ(manager->builtTransporters.size(), 1u);
    ASSERT_EQ(manager->builtTransporters.front()->setCount, 1);
    const auto lateContext = ObjectBufferInternal::GetInfo(*buffer).ubLateCompletionContext;
    ASSERT_TRUE(lateContext.has_value());
    auto observer = lateContext->observer.lock();
    ASSERT_NE(observer, nullptr);

    observer->OnLateUrmaCompletion(
        UrmaLateCompletion{ 4001, URMA_PORT_UNAVAILABLE_STATUS, worker.ToString(), "worker-incarnation" },
        lateContext->ownerToken, lateContext->peerToken);

    EXPECT_TRUE(layer.CheckLocalUbSenderAdmission().IsOk());
    EXPECT_TRUE(layer.Set(*buffer, MakeSetParam()).IsOk());
    EXPECT_EQ(manager->builtTransporters.front()->setCount, 2);
}

TEST(TransportLayerAdmissionTest, LateCqe9DoesNotCloseClientLocalSender)
{
    const auto worker = MakeAddress(41);
    auto manager = std::make_shared<FakeDataPlaneManager>();
    auto filter = std::make_shared<UbHealthFilter>();
    TestTransportLayer layer(manager, std::make_shared<FixedTransportAdvisor>(TransportHint::UB_CANDIDATE),
                             filter);
    std::shared_ptr<ObjectBuffer> buffer;
    ASSERT_TRUE(layer.Create(worker, "late-cqe9", 4, MakeCreateParam(), buffer).IsOk());
    ASSERT_TRUE(layer.Set(*buffer, MakeSetParam()).IsOk());
    const auto lateContext = ObjectBufferInternal::GetInfo(*buffer).ubLateCompletionContext;
    ASSERT_TRUE(lateContext.has_value());
    auto observer = lateContext->observer.lock();
    ASSERT_NE(observer, nullptr);

    observer->OnLateUrmaCompletion(
        UrmaLateCompletion{ 4002, URMA_REMOTE_ACK_TIMEOUT_STATUS, worker.ToString(), "worker-incarnation" },
        lateContext->ownerToken, lateContext->peerToken);

    EXPECT_TRUE(layer.CheckLocalUbSenderAdmission().IsOk());
    ASSERT_TRUE(WaitUntil([&] { return !filter->IsWriteTargetAvailable(worker); }));
}

TEST(TransportLayerAdmissionTest, SynchronousCqe9ReportsSafeWriteTargetReplay)
{
    const auto worker = MakeAddress(56);
    auto manager = std::make_shared<FakeDataPlaneManager>();
    auto filter = std::make_shared<UbHealthFilter>();
    TestTransportLayer layer(manager, std::make_shared<FixedTransportAdvisor>(TransportHint::UB_CANDIDATE),
                             filter);
    std::shared_ptr<ObjectBuffer> buffer;
    ASSERT_TRUE(layer.Create(worker, "sync-cqe9", 4, MakeCreateParam(), buffer).IsOk());
    auto transporter = manager->builtTransporters.front();
    transporter->setUbFailureReports = { Status(K_URMA_ERROR, "remote ack timeout") };
    transporter->setUbCqeStatuses = { URMA_REMOTE_ACK_TIMEOUT_STATUS };
    transporter->setStatuses = { Status(K_URMA_ERROR, "fallback was not sent") };

    TransportSetResult result;
    EXPECT_EQ(layer.Set(*buffer, MakeSetParam(), result).GetCode(), K_URMA_ERROR);
    EXPECT_TRUE(result.writeTargetQuarantined);
    EXPECT_FALSE(result.publishAttempted);
    EXPECT_FALSE(filter->IsWriteTargetAvailable(worker));
    EXPECT_TRUE(layer.CheckLocalUbSenderAdmission().IsOk());
}

TEST(TransportLayerAdmissionTest, MSetCqe9ReportsSafeWriteTargetReplay)
{
    const auto worker = MakeAddress(57);
    auto manager = std::make_shared<FakeDataPlaneManager>();
    manager->transporterMSetUbFailureReports = { Status(K_URMA_ERROR, "remote ack timeout") };
    manager->transporterMSetUbCqeStatuses = { URMA_REMOTE_ACK_TIMEOUT_STATUS };
    auto filter = std::make_shared<UbHealthFilter>();
    TestTransportLayer layer(manager, std::make_shared<FixedTransportAdvisor>(TransportHint::UB_CANDIDATE),
                             filter);
    std::vector<std::shared_ptr<ObjectBuffer>> buffers;
    ASSERT_TRUE(layer.MCreate(worker, { "mset-cqe9" }, { 4 }, MakeCreateParam(), buffers).IsOk());

    TransportMSetResult result;
    EXPECT_TRUE(layer.MSet(buffers, MakeSetParam(), result).IsOk());
    EXPECT_TRUE(result.writeTargetQuarantined);
    EXPECT_FALSE(result.publishAttempted);
    EXPECT_FALSE(filter->IsWriteTargetAvailable(worker));
    EXPECT_TRUE(layer.CheckLocalUbSenderAdmission().IsOk());
}

TEST(TransportLayerAdmissionTest, TcpFailureDoesNotTripUbSenderAdmission)
{
    auto manager = std::make_shared<FakeDataPlaneManager>();
    manager->transporterSetStatuses = { { Status(K_URMA_ERROR, "tcp response error"), Status::OK() } };
    TestTransportLayer layer(manager, std::make_shared<FixedTransportAdvisor>(TransportHint::TCP_ONLY));
    std::shared_ptr<ObjectBuffer> firstBuffer;
    ASSERT_TRUE(layer.Create(MakeAddress(35), "first", 4, MakeCreateParam(), firstBuffer).IsOk());

    EXPECT_EQ(layer.Set(*firstBuffer, MakeSetParam()).GetCode(), K_URMA_ERROR);
    EXPECT_TRUE(layer.Set(*firstBuffer, MakeSetParam()).IsOk());
    std::shared_ptr<ObjectBuffer> secondBuffer;
    EXPECT_TRUE(layer.Create(MakeAddress(36), "second", 4, MakeCreateParam(), secondBuffer).IsOk());
}

TEST(TransportLayerAdmissionTest, UbTransporterBusinessErrorWithoutLocalWriteEvidenceDoesNotTripSenderAdmission)
{
    auto manager = std::make_shared<FakeDataPlaneManager>();
    manager->transporterSetStatuses = { { Status(K_URMA_ERROR, "worker response error"), Status::OK() } };
    TestTransportLayer layer(manager, std::make_shared<FixedTransportAdvisor>(TransportHint::UB_CANDIDATE));
    std::shared_ptr<ObjectBuffer> buffer;
    ASSERT_TRUE(layer.Create(MakeAddress(37), "first", 4, MakeCreateParam(), buffer).IsOk());

    EXPECT_EQ(layer.Set(*buffer, MakeSetParam()).GetCode(), K_URMA_ERROR);
    EXPECT_TRUE(layer.Set(*buffer, MakeSetParam()).IsOk());
}

}  // namespace

class DataPlaneManagerAdmissionTestPeer {
public:
    static void SetLastConfirmedPublishMs(DataPlaneManager &manager, int64_t ms)
    {
        auto current = std::atomic_load(&manager.endpointAdmissionSnapshot_);
        auto replacement = std::make_shared<const DataPlaneManager::EndpointAdmissionSnapshot>(
            current->ringVersion, current->liveWorkers, current->provisional, ms);
        std::atomic_store(&manager.endpointAdmissionSnapshot_, std::move(replacement));
    }

    static void SetDegradedAdmissionDeadlineMs(DataPlaneManager &manager, int64_t ms)
    {
        std::atomic_load(&manager.endpointAdmissionSnapshot_)->degradedDeadlineMs.store(ms);
    }
};

TEST(DataPlaneManagerAdmissionTest, ProvisionalSnapshotDoesNotRejectAbsentWorker)
{
    auto manager = std::make_shared<FakeDataPlaneManager>();
    ASSERT_TRUE(manager->Init().IsOk());
    WorkerSnapshot snapshot;
    snapshot.remoteTransportAddrs = { MakeAddress(60) };
    snapshot.provisional = true;
    ASSERT_TRUE(manager->UpdateWorkerSnapshot(snapshot).IsOk());

    std::shared_ptr<IDataTransporter> out;
    EXPECT_TRUE(manager->GetOrCreate(MakeAddress(61), TransportHint::TCP_ONLY, out).IsOk());
}

TEST(DataPlaneManagerAdmissionTest, HealthyRingRejectsAbsentWorker)
{
    auto manager = std::make_shared<FakeDataPlaneManager>();
    ASSERT_TRUE(manager->Init().IsOk());
    WorkerSnapshot snapshot;
    snapshot.ringVersion = 1;
    snapshot.remoteTransportAddrs = { MakeAddress(62) };
    ASSERT_TRUE(manager->UpdateWorkerSnapshot(snapshot).IsOk());

    std::shared_ptr<IDataTransporter> out;
    EXPECT_EQ(manager->GetOrCreate(MakeAddress(63), TransportHint::TCP_ONLY, out).GetCode(), K_NOT_READY);
}

TEST(DataPlaneManagerAdmissionTest, LostRingDegradesAdmissionThenRestoresAfterTtl)
{
    auto manager = std::make_shared<FakeDataPlaneManager>();
    ASSERT_TRUE(manager->Init().IsOk());
    WorkerSnapshot snapshot;
    snapshot.ringVersion = 1;
    snapshot.remoteTransportAddrs = { MakeAddress(64) };
    ASSERT_TRUE(manager->UpdateWorkerSnapshot(snapshot).IsOk());
    const auto nowMs = std::chrono::duration_cast<std::chrono::milliseconds>(
                           std::chrono::steady_clock::now().time_since_epoch())
                           .count();
    DataPlaneManagerAdmissionTestPeer::SetLastConfirmedPublishMs(*manager, nowMs - 65'000);

    std::shared_ptr<IDataTransporter> out;
    EXPECT_TRUE(manager->GetOrCreate(MakeAddress(65), TransportHint::TCP_ONLY, out).IsOk());

    DataPlaneManagerAdmissionTestPeer::SetDegradedAdmissionDeadlineMs(*manager, nowMs - 1);
    out.reset();
    EXPECT_EQ(manager->GetOrCreate(MakeAddress(65), TransportHint::TCP_ONLY, out).GetCode(), K_NOT_READY);
}

TEST(DataPlaneManagerAdmissionTest, MatchingRefreshRestoresStrictAdmissionAndRearmsOnlyAfterAnotherOutage)
{
    auto manager = std::make_shared<FakeDataPlaneManager>();
    ASSERT_TRUE(manager->Init().IsOk());
    WorkerSnapshot snapshot;
    snapshot.ringVersion = 2;
    snapshot.remoteTransportAddrs = { MakeAddress(66) };
    ASSERT_TRUE(manager->UpdateWorkerSnapshot(snapshot).IsOk());
    const auto nowMs = std::chrono::duration_cast<std::chrono::milliseconds>(
                           std::chrono::steady_clock::now().time_since_epoch()).count();
    DataPlaneManagerAdmissionTestPeer::SetLastConfirmedPublishMs(*manager, nowMs - 65'000);
    std::shared_ptr<IDataTransporter> out;
    EXPECT_TRUE(manager->GetOrCreate(MakeAddress(67), TransportHint::TCP_ONLY, out).IsOk());

    manager->RecordRoutingRefresh(1);
    EXPECT_TRUE(manager->GetOrCreate(MakeAddress(67), TransportHint::TCP_ONLY, out).IsOk());
    manager->RecordRoutingRefresh(2);
    EXPECT_EQ(manager->GetOrCreate(MakeAddress(67), TransportHint::TCP_ONLY, out).GetCode(), K_NOT_READY);

    DataPlaneManagerAdmissionTestPeer::SetLastConfirmedPublishMs(*manager, nowMs - 65'000);
    EXPECT_TRUE(manager->GetOrCreate(MakeAddress(67), TransportHint::TCP_ONLY, out).IsOk());
    DataPlaneManagerAdmissionTestPeer::SetDegradedAdmissionDeadlineMs(*manager, nowMs - 1);
    EXPECT_EQ(manager->GetOrCreate(MakeAddress(67), TransportHint::TCP_ONLY, out).GetCode(), K_NOT_READY);
    EXPECT_EQ(manager->GetOrCreate(MakeAddress(68), TransportHint::TCP_ONLY, out).GetCode(), K_NOT_READY);
}

TEST(DataPlaneManagerAdmissionTest, ConfirmedPublicationRevokesAnInFlightGraceDecision)
{
    auto manager = std::make_shared<FakeDataPlaneManager>();
    ASSERT_TRUE(manager->Init().IsOk());
    WorkerSnapshot snapshot;
    snapshot.ringVersion = 1;
    snapshot.remoteTransportAddrs = { MakeAddress(69) };
    ASSERT_TRUE(manager->UpdateWorkerSnapshot(snapshot).IsOk());
    const auto nowMs = std::chrono::duration_cast<std::chrono::milliseconds>(
                           std::chrono::steady_clock::now().time_since_epoch()).count();
    DataPlaneManagerAdmissionTestPeer::SetLastConfirmedPublishMs(*manager, nowMs - 65'000);
    constexpr char POINT[] = "DataPlaneManager.GetOrCreateEntry.afterDegradedAdmission";
    ASSERT_TRUE(inject::Set(POINT, "1*pause()").IsOk());
    Raii clearInject([&] { (void)inject::Clear(POINT); });
    auto reader = std::async(std::launch::async, [&] {
        std::shared_ptr<IDataTransporter> out;
        return manager->GetOrCreate(MakeAddress(70), TransportHint::TCP_ONLY, out);
    });
    for (size_t retry = 0; retry < 2'000 && inject::GetExecuteCount(POINT) == 0; ++retry) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    EXPECT_GT(inject::GetExecuteCount(POINT), 0);
    snapshot.ringVersion = 2;
    EXPECT_TRUE(manager->UpdateWorkerSnapshot(snapshot).IsOk());
    EXPECT_TRUE(inject::Clear(POINT).IsOk());
    EXPECT_EQ(reader.get().GetCode(), K_NOT_READY);
}

}  // namespace client
}  // namespace datasystem
