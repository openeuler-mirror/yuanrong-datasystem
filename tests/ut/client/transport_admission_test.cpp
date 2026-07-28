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

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <future>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "datasystem/client/transport/object_buffer_internal.h"
#include "datasystem/client/transport/transport_layer.h"
#if defined(USE_URMA) || defined(USE_URMA_MOCK)
#include "datasystem/common/rdma/urma_manager.h"
#endif

namespace datasystem {
namespace client {
namespace {
constexpr std::chrono::seconds PROBE_OBSERVATION_TIMEOUT(3);

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
    Status Get(const DataGetRequest &, DataGetResult &) override
    {
        ++getCount;
        return Status::OK();
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

    Status Set(ObjectBuffer &buffer, const TransportSetParam &) override
    {
        std::unique_lock<std::mutex> lock(setMutex);
        const int callIndex = ++setCount;
        setCv.notify_all();
        if (coordinateConcurrentSets) {
            if (callIndex == 1) {
                setCv.wait(lock, [this]() { return setCount >= 2; });
            } else if (callIndex == 2) {
                setCv.wait(lock, [this]() { return releaseSecondSet; });
            }
        }
        if (!setUbFailureReports.empty()) {
            auto &info = ObjectBufferInternal::GetMutableInfo(buffer);
            info.ubFailureReportRc = setUbFailureReports.front();
            setUbFailureReports.erase(setUbFailureReports.begin());
            if (!setUbCqeStatuses.empty()) {
                info.ubCqeStatus = setUbCqeStatuses.front();
                setUbCqeStatuses.erase(setUbCqeStatuses.begin());
            }
        }
        if (setStatuses.empty()) {
            return Status::OK();
        }
        Status status = setStatuses.front();
        setStatuses.erase(setStatuses.begin());
        return status;
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
    int getCount{ 0 };
    int batchGetCount{ 0 };
    int createCount{ 0 };
    int setCount{ 0 };
    int mCreateCount{ 0 };
    int mSetCount{ 0 };
    int releaseCount{ 0 };
    std::vector<Status> setStatuses;
    std::vector<Status> setUbFailureReports;
    std::vector<std::optional<int>> setUbCqeStatuses;
    Status mSetUbFailureReportRc{ Status::OK() };
    std::optional<int> mSetUbCqeStatus;
    Status mSetStatus{ Status::OK() };
    bool coordinateConcurrentSets{ false };
    bool releaseSecondSet{ false };
    std::mutex setMutex;
    std::condition_variable setCv;
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

    Status BuildTransporter(const HostPort &, TransportHint hint, const std::shared_ptr<WorkerRpcClient> &,
                            std::shared_ptr<IDataTransporter> &output) override
    {
        std::lock_guard<std::mutex> lock(mutex_);
        ++transportBuildCount;
        auto transporter = std::make_shared<FakeTransporter>();
        if (hint == TransportHint::TCP_ONLY) {
            transporter->kind = AccessTransportKind::TCP;
        } else if (hint == TransportHint::SHM_CANDIDATE) {
            transporter->kind = AccessTransportKind::SHM;
        } else {
            transporter->kind = AccessTransportKind::UB;
        }
        if (!transporterSetStatuses.empty()) {
            transporter->setStatuses = std::move(transporterSetStatuses.front());
            transporterSetStatuses.erase(transporterSetStatuses.begin());
        }
        if (!transporterMSetUbFailureReports.empty()) {
            transporter->mSetUbFailureReportRc = transporterMSetUbFailureReports.front();
            transporterMSetUbFailureReports.erase(transporterMSetUbFailureReports.begin());
            transporter->mSetUbCqeStatus = 4;
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

    std::vector<HostPort> GetProbedWorkers()
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return probedWorkers;
    }

    int transportBuildCount{ 0 };
    std::atomic<int> probeCount{ 0 };
    std::vector<std::vector<Status>> transporterSetStatuses;
    std::vector<Status> transporterMSetUbFailureReports;
    std::vector<Status> probeStatuses;
    std::vector<HostPort> probedWorkers;
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
                       std::chrono::milliseconds localUbProbeBaseDelay = std::chrono::seconds(1))
        : TransportLayer(std::move(manager), std::move(advisor), localUbProbeBaseDelay)
    {
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

TEST(TransportLayerAdmissionTest, HardUbSetFailureBlocksLaterSetAndAllocationBeforeTransport)
{
    auto manager = std::make_shared<FakeDataPlaneManager>();
    manager->transporterSetStatuses = { { Status(K_URMA_ERROR, "local sender error 4"), Status::OK() } };
    TestTransportLayer layer(manager, std::make_shared<FixedTransportAdvisor>(TransportHint::UB_CANDIDATE));
    std::shared_ptr<ObjectBuffer> buffer;
    ASSERT_TRUE(layer.Create(MakeAddress(30), "first", 64, MakeCreateParam(), buffer).IsOk());
    manager->builtTransporters.front()->setUbFailureReports = { Status(K_URMA_ERROR, "local sender error 4") };
    manager->builtTransporters.front()->setUbCqeStatuses = { 4 };

    EXPECT_EQ(layer.Set(*buffer, MakeSetParam()).GetCode(), K_URMA_ERROR);
    EXPECT_EQ(layer.Set(*buffer, MakeSetParam()).GetCode(), K_URMA_WORKER_UNAVAILABLE);
    std::shared_ptr<ObjectBuffer> blockedBuffer;
    EXPECT_EQ(layer.Create(MakeAddress(31), "blocked", 64, MakeCreateParam(), blockedBuffer).GetCode(),
              K_URMA_WORKER_UNAVAILABLE);
    ASSERT_EQ(manager->builtTransporters.size(), 2u);
    EXPECT_EQ(manager->builtTransporters.front()->kind, AccessTransportKind::UB);
    EXPECT_EQ(manager->builtTransporters.back()->kind, AccessTransportKind::TCP);
    EXPECT_EQ(manager->builtTransporters.front()->setCount, 1);
    EXPECT_EQ(manager->builtTransporters.front()->createCount, 1);
    EXPECT_EQ(manager->builtTransporters.back()->releaseCount, 1);
}

TEST(TransportLayerAdmissionTest, HardUbSenderFailureDoesNotBlockSharedMemoryTransport)
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

TEST(TransportLayerAdmissionTest, DedicatedProbeRestoresClientLocalSenderWithoutBusinessRetry)
{
    auto manager = std::make_shared<FakeDataPlaneManager>();
    manager->transporterSetStatuses = { { Status(K_URMA_ERROR, "local sender error 4"), Status::OK() } };
    manager->probeStatuses = { Status::OK() };
    TestTransportLayer layer(manager, std::make_shared<FixedTransportAdvisor>(TransportHint::UB_CANDIDATE),
                             std::chrono::milliseconds(100));
    ASSERT_TRUE(layer.Init().IsOk());
    WorkerSnapshot admitted;
    admitted.ringVersion = 1;
    admitted.otherAddrs = { MakeAddress(45) };
    admitted.writeProbeAddrs = admitted.otherAddrs;
    ASSERT_TRUE(manager->UpdateWorkerSnapshot(admitted).IsOk());
    std::shared_ptr<ObjectBuffer> buffer;
    ASSERT_TRUE(layer.Create(MakeAddress(45), "recover", 64, MakeCreateParam(), buffer).IsOk());
    manager->builtTransporters.front()->setUbFailureReports = { Status(K_URMA_ERROR, "local sender error 4") };
    manager->builtTransporters.front()->setUbCqeStatuses = { 4 };

    EXPECT_EQ(layer.Set(*buffer, MakeSetParam()).GetCode(), K_URMA_ERROR);
    EXPECT_EQ(layer.Set(*buffer, MakeSetParam()).GetCode(), K_URMA_WORKER_UNAVAILABLE);
    EXPECT_EQ(manager->builtTransporters.front()->setCount, 1);
    EXPECT_EQ(manager->probeCount, 0);
    ASSERT_EQ(manager->builtTransporters.size(), 2u);
    EXPECT_EQ(manager->builtTransporters[0]->kind, AccessTransportKind::UB);
    EXPECT_EQ(manager->builtTransporters[1]->kind, AccessTransportKind::TCP);
    ASSERT_TRUE(manager->WaitForProbeCount(1, PROBE_OBSERVATION_TIMEOUT));
    EXPECT_EQ(manager->builtTransporters.size(), 2u);
    EXPECT_TRUE(layer.Set(*buffer, MakeSetParam()).IsOk());
    EXPECT_TRUE(layer.Set(*buffer, MakeSetParam()).IsOk());
    ASSERT_GE(manager->builtTransporters.size(), 3u);
    EXPECT_EQ(manager->builtTransporters[0]->kind, AccessTransportKind::UB);
    EXPECT_EQ(manager->builtTransporters[1]->kind, AccessTransportKind::TCP);
    EXPECT_EQ(manager->builtTransporters.back()->kind, AccessTransportKind::UB);
    EXPECT_EQ(manager->builtTransporters[0]->setCount, 1);
    int recoveredSetCount = 0;
    for (size_t i = 2; i < manager->builtTransporters.size(); ++i) {
        EXPECT_EQ(manager->builtTransporters[i]->kind, AccessTransportKind::UB);
        recoveredSetCount += manager->builtTransporters[i]->setCount;
    }
    EXPECT_EQ(recoveredSetCount, 2);
}

TEST(TransportLayerAdmissionTest, GlobalSnapshotDenyKeepsClientLocalSenderQuarantinedUntilReadmitted)
{
    auto manager = std::make_shared<FakeDataPlaneManager>();
    manager->transporterSetStatuses = { { Status(K_URMA_ERROR, "local sender error 4") } };
    TestTransportLayer layer(manager, std::make_shared<FixedTransportAdvisor>(TransportHint::UB_CANDIDATE),
                             std::chrono::milliseconds(100));
    ASSERT_TRUE(layer.Init().IsOk());
    const auto workerAddr = MakeAddress(46);
    std::shared_ptr<ObjectBuffer> buffer;
    ASSERT_TRUE(layer.Create(workerAddr, "global-gate", 64, MakeCreateParam(), buffer).IsOk());
    manager->builtTransporters.front()->setUbFailureReports = { Status(K_URMA_ERROR, "local sender error 4") };
    manager->builtTransporters.front()->setUbCqeStatuses = { 4 };
    ASSERT_EQ(layer.Set(*buffer, MakeSetParam()).GetCode(), K_URMA_ERROR);

    WorkerSnapshot denied;
    denied.ringVersion = 1;
    ASSERT_TRUE(manager->UpdateWorkerSnapshot(denied).IsOk());
    std::this_thread::sleep_for(std::chrono::milliseconds(150));
    EXPECT_EQ(manager->probeCount, 0);
    EXPECT_EQ(layer.Set(*buffer, MakeSetParam()).GetCode(), K_URMA_WORKER_UNAVAILABLE);

    WorkerSnapshot readmitted;
    readmitted.ringVersion = 2;
    readmitted.otherAddrs = { workerAddr };
    readmitted.writeProbeAddrs = readmitted.otherAddrs;
    ASSERT_TRUE(manager->UpdateWorkerSnapshot(readmitted).IsOk());
    ASSERT_TRUE(manager->WaitForProbeCount(1, PROBE_OBSERVATION_TIMEOUT));
    Status recovered = Status(K_URMA_WORKER_UNAVAILABLE, "waiting for probe commit");
    for (int attempt = 0; attempt < 50 && recovered.IsError(); ++attempt) {
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
        recovered = layer.Set(*buffer, MakeSetParam());
    }
    EXPECT_TRUE(recovered.IsOk()) << recovered.ToString();
    EXPECT_EQ(manager->probeCount, 1);
}

TEST(TransportLayerAdmissionTest, RemovedFailureEndpointRecoversThroughAnotherAdmittedWorker)
{
    auto manager = std::make_shared<FakeDataPlaneManager>();
    manager->transporterSetStatuses = { { Status(K_URMA_ERROR, "local sender error 4") } };
    TestTransportLayer layer(manager, std::make_shared<FixedTransportAdvisor>(TransportHint::UB_CANDIDATE),
                             std::chrono::milliseconds(100));
    ASSERT_TRUE(layer.Init().IsOk());
    const auto removedWorker = MakeAddress(48);
    const auto replacementWorker = MakeAddress(49);
    WorkerSnapshot initial;
    initial.ringVersion = 1;
    initial.otherAddrs = { removedWorker, replacementWorker };
    initial.writeProbeAddrs = initial.otherAddrs;
    ASSERT_TRUE(manager->UpdateWorkerSnapshot(initial).IsOk());

    std::shared_ptr<ObjectBuffer> failedBuffer;
    ASSERT_TRUE(layer.Create(removedWorker, "removed", 64, MakeCreateParam(), failedBuffer).IsOk());
    manager->builtTransporters.front()->setUbFailureReports = { Status(K_URMA_ERROR, "local sender error 4") };
    manager->builtTransporters.front()->setUbCqeStatuses = { 4 };
    ASSERT_EQ(layer.Set(*failedBuffer, MakeSetParam()).GetCode(), K_URMA_ERROR);

    WorkerSnapshot replacementOnly;
    replacementOnly.ringVersion = 2;
    replacementOnly.otherAddrs = { replacementWorker };
    replacementOnly.writeProbeAddrs = replacementOnly.otherAddrs;
    ASSERT_TRUE(manager->UpdateWorkerSnapshot(replacementOnly).IsOk());
    ASSERT_TRUE(manager->WaitForProbeCount(1, PROBE_OBSERVATION_TIMEOUT));
    EXPECT_EQ(manager->GetProbedWorkers(), std::vector<HostPort>{ replacementWorker });

    std::shared_ptr<ObjectBuffer> recoveredBuffer;
    ASSERT_TRUE(layer.Create(replacementWorker, "replacement", 64, MakeCreateParam(), recoveredBuffer).IsOk());
    EXPECT_TRUE(layer.Set(*recoveredBuffer, MakeSetParam()).IsOk());
}

TEST(TransportLayerAdmissionTest, FailedRecoveryProbeRotatesAcrossAdmittedWorkers)
{
    auto manager = std::make_shared<FakeDataPlaneManager>();
    manager->transporterSetStatuses = { { Status(K_URMA_ERROR, "local sender error 4") } };
    manager->probeStatuses = { Status(K_NOT_SUPPORTED, "old worker has no write probe"), Status::OK() };
    TestTransportLayer layer(manager, std::make_shared<FixedTransportAdvisor>(TransportHint::UB_CANDIDATE),
                             std::chrono::milliseconds(20));
    ASSERT_TRUE(layer.Init().IsOk());
    const auto failedWorker = MakeAddress(50);
    const auto replacementWorker = MakeAddress(51);
    WorkerSnapshot admitted;
    admitted.ringVersion = 1;
    admitted.otherAddrs = { failedWorker, replacementWorker };
    admitted.writeProbeAddrs = admitted.otherAddrs;
    ASSERT_TRUE(manager->UpdateWorkerSnapshot(admitted).IsOk());

    std::shared_ptr<ObjectBuffer> failedBuffer;
    ASSERT_TRUE(layer.Create(failedWorker, "rotate", 64, MakeCreateParam(), failedBuffer).IsOk());
    manager->builtTransporters.front()->setUbFailureReports = { Status(K_URMA_ERROR, "local sender error 4") };
    manager->builtTransporters.front()->setUbCqeStatuses = { 4 };
    ASSERT_EQ(layer.Set(*failedBuffer, MakeSetParam()).GetCode(), K_URMA_ERROR);

    ASSERT_TRUE(manager->WaitForProbeCount(2, PROBE_OBSERVATION_TIMEOUT));
    const auto probedWorkers = manager->GetProbedWorkers();
    ASSERT_GE(probedWorkers.size(), 2u);
    EXPECT_EQ(probedWorkers[0], failedWorker);
    EXPECT_EQ(probedWorkers[1], replacementWorker);

    std::shared_ptr<ObjectBuffer> recoveredBuffer;
    ASSERT_TRUE(layer.Create(replacementWorker, "replacement", 64, MakeCreateParam(), recoveredBuffer).IsOk());
    EXPECT_TRUE(layer.Set(*recoveredBuffer, MakeSetParam()).IsOk());
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
    uint64_t segmentAddress = 0;
    uint64_t dataOffset = 0;
    ASSERT_TRUE(UrmaManager::Instance().GetRecoveryProbeSegmentInfo(segmentAddress, dataOffset).IsOk());
    ASSERT_NE(segmentAddress, 0u);
    EXPECT_EQ(dataOffset, 0u);

    uint64_t secondAddress = 0;
    uint64_t secondOffset = 0;
    ASSERT_TRUE(UrmaManager::Instance().GetRecoveryProbeSegmentInfo(secondAddress, secondOffset).IsOk());
    EXPECT_EQ(secondAddress, segmentAddress);
    EXPECT_EQ(secondOffset, dataOffset);

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
    denied.otherAddrs = { MakeAddress(53) };
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
    admitted.otherAddrs = { workerAddr };
    admitted.writeProbeAddrs = admitted.otherAddrs;
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

TEST(TransportLayerAdmissionTest, ConcurrentAdmittedSetCompletesBeforeSenderQuarantineBecomesVisible)
{
    auto manager = std::make_shared<FakeDataPlaneManager>();
    manager->transporterSetStatuses = { { Status(K_URMA_ERROR, "local sender error 4"), Status::OK() } };
    TestTransportLayer layer(manager, std::make_shared<FixedTransportAdvisor>(TransportHint::UB_CANDIDATE));
    std::shared_ptr<ObjectBuffer> first;
    std::shared_ptr<ObjectBuffer> second;
    ASSERT_TRUE(layer.Create(MakeAddress(31), "first", 4, MakeCreateParam(), first).IsOk());
    ASSERT_TRUE(layer.Create(MakeAddress(31), "second", 4, MakeCreateParam(), second).IsOk());
    auto transporter = manager->builtTransporters.front();
    transporter->setUbFailureReports = { Status(K_URMA_ERROR, "local sender error 4") };
    transporter->setUbCqeStatuses = { 4 };
    transporter->coordinateConcurrentSets = true;

    auto firstSet = std::async(std::launch::async, [&]() { return layer.Set(*first, MakeSetParam()); });
    auto secondSet = std::async(std::launch::async, [&]() { return layer.Set(*second, MakeSetParam()); });
    {
        std::unique_lock<std::mutex> lock(transporter->setMutex);
        ASSERT_TRUE(
            transporter->setCv.wait_for(lock, std::chrono::seconds(1), [&]() { return transporter->setCount >= 2; }));
    }
    EXPECT_EQ(firstSet.wait_for(std::chrono::milliseconds(50)), std::future_status::timeout);
    {
        std::lock_guard<std::mutex> lock(transporter->setMutex);
        transporter->releaseSecondSet = true;
    }
    transporter->setCv.notify_all();

    Status firstRc = firstSet.get();
    Status secondRc = secondSet.get();
    EXPECT_TRUE((firstRc.GetCode() == K_URMA_ERROR && secondRc.IsOk())
                || (firstRc.IsOk() && secondRc.GetCode() == K_URMA_ERROR));
    EXPECT_EQ(layer.Set(*second, MakeSetParam()).GetCode(), K_URMA_WORKER_UNAVAILABLE);
}

TEST(TransportLayerAdmissionTest, ShutdownWaitsForAdmittedUbOperationsBeforeClosingSender)
{
    auto manager = std::make_shared<FakeDataPlaneManager>();
    manager->transporterSetStatuses = { { Status::OK(), Status::OK() } };
    TestTransportLayer layer(manager, std::make_shared<FixedTransportAdvisor>(TransportHint::UB_CANDIDATE));
    std::shared_ptr<ObjectBuffer> first;
    std::shared_ptr<ObjectBuffer> second;
    ASSERT_TRUE(layer.Create(MakeAddress(54), "first", 4, MakeCreateParam(), first).IsOk());
    ASSERT_TRUE(layer.Create(MakeAddress(54), "second", 4, MakeCreateParam(), second).IsOk());
    auto transporter = manager->builtTransporters.front();
    transporter->coordinateConcurrentSets = true;

    auto firstSet = std::async(std::launch::async, [&] { return layer.Set(*first, MakeSetParam()); });
    auto secondSet = std::async(std::launch::async, [&] { return layer.Set(*second, MakeSetParam()); });
    {
        std::unique_lock<std::mutex> lock(transporter->setMutex);
        ASSERT_TRUE(
            transporter->setCv.wait_for(lock, std::chrono::seconds(1), [&] { return transporter->setCount >= 2; }));
    }
    auto shutdown = std::async(std::launch::async, [&] { layer.Shutdown(); });
    EXPECT_EQ(shutdown.wait_for(std::chrono::milliseconds(50)), std::future_status::timeout);

    {
        std::lock_guard<std::mutex> lock(transporter->setMutex);
        transporter->releaseSecondSet = true;
    }
    transporter->setCv.notify_all();
    EXPECT_TRUE(firstSet.get().IsOk());
    EXPECT_TRUE(secondSet.get().IsOk());
    EXPECT_EQ(shutdown.wait_for(std::chrono::seconds(1)), std::future_status::ready);
}

TEST(TransportLayerAdmissionTest, RetryAdmissionFailureStillReleasesSetAllocation)
{
    auto manager = std::make_shared<FakeDataPlaneManager>();
    manager->transporterSetStatuses = { { Status(K_URMA_NEED_CONNECT, "reconnect") } };
    TestTransportLayer layer(manager, std::make_shared<FixedTransportAdvisor>(TransportHint::UB_CANDIDATE));
    std::shared_ptr<ObjectBuffer> buffer;
    ASSERT_TRUE(layer.Create(MakeAddress(38), "set", 4, MakeCreateParam(), buffer).IsOk());
    manager->builtTransporters.front()->setUbFailureReports = { Status(K_URMA_ERROR, "local sender error 4") };
    manager->builtTransporters.front()->setUbCqeStatuses = { 4 };

    EXPECT_EQ(layer.Set(*buffer, MakeSetParam()).GetCode(), K_URMA_ERROR);
    ASSERT_EQ(manager->builtTransporters.size(), 2u);
    EXPECT_EQ(manager->builtTransporters.front()->kind, AccessTransportKind::UB);
    EXPECT_EQ(manager->builtTransporters.back()->kind, AccessTransportKind::TCP);
    EXPECT_EQ(manager->builtTransporters.back()->releaseCount, 1);
}

TEST(TransportLayerAdmissionTest, HardUbMSetFailureBlocksLaterMSetAndRemoteAllocation)
{
    auto manager = std::make_shared<FakeDataPlaneManager>();
    manager->transporterMSetUbFailureReports = { Status(K_URMA_ERROR, "local batch sender error 4") };
    TestTransportLayer layer(manager, std::make_shared<FixedTransportAdvisor>(TransportHint::UB_CANDIDATE));
    std::vector<std::shared_ptr<ObjectBuffer>> buffers;
    ASSERT_TRUE(layer.MCreate(MakeAddress(32), { "first-a", "first-b" }, { 4, 4 }, MakeCreateParam(), buffers).IsOk());
    TransportMSetResult result;

    ASSERT_TRUE(layer.MSet(buffers, MakeSetParam(), result).IsOk());
    EXPECT_EQ(layer.MSet(buffers, MakeSetParam(), result).GetCode(), K_URMA_WORKER_UNAVAILABLE);
    std::vector<std::shared_ptr<ObjectBuffer>> blockedBuffers;
    EXPECT_EQ(layer.MCreate(MakeAddress(33), { "blocked-a", "blocked-b" }, { 4, 4 }, MakeCreateParam(), blockedBuffers)
                  .GetCode(),
              K_URMA_WORKER_UNAVAILABLE);
    ASSERT_EQ(manager->builtTransporters.size(), 2u);
    EXPECT_EQ(manager->builtTransporters.front()->kind, AccessTransportKind::UB);
    EXPECT_EQ(manager->builtTransporters.back()->kind, AccessTransportKind::TCP);
    EXPECT_EQ(manager->builtTransporters.front()->mSetCount, 1);
    EXPECT_EQ(manager->builtTransporters.front()->mCreateCount, 1);
    EXPECT_EQ(manager->builtTransporters.back()->releaseCount, 2);
}

TEST(TransportLayerAdmissionTest, RetryAdmissionFailureStillReleasesEveryMSetAllocation)
{
    auto manager = std::make_shared<FakeDataPlaneManager>();
    TestTransportLayer layer(manager, std::make_shared<FixedTransportAdvisor>(TransportHint::UB_CANDIDATE));
    std::vector<std::shared_ptr<ObjectBuffer>> buffers;
    ASSERT_TRUE(layer.MCreate(MakeAddress(39), { "a", "b" }, { 4, 4 }, MakeCreateParam(), buffers).IsOk());
    manager->builtTransporters.front()->mSetStatus = Status(K_URMA_NEED_CONNECT, "reconnect");
    manager->builtTransporters.front()->mSetUbFailureReportRc = Status(K_URMA_ERROR, "local sender error 4");
    manager->builtTransporters.front()->mSetUbCqeStatus = 4;
    TransportMSetResult result;

    EXPECT_EQ(layer.MSet(buffers, MakeSetParam(), result).GetCode(), K_URMA_ERROR);
    ASSERT_EQ(manager->builtTransporters.size(), 2u);
    EXPECT_EQ(manager->builtTransporters.front()->kind, AccessTransportKind::UB);
    EXPECT_EQ(manager->builtTransporters.back()->kind, AccessTransportKind::TCP);
    EXPECT_EQ(manager->builtTransporters.back()->releaseCount, 2);
}

TEST(TransportLayerAdmissionTest, ClientLocalSenderFailureDoesNotAffectAnotherClient)
{
    auto failedManager = std::make_shared<FakeDataPlaneManager>();
    failedManager->transporterSetStatuses = { { Status(K_URMA_ERROR, "client one error 4") } };
    TestTransportLayer failedClient(failedManager,
                                    std::make_shared<FixedTransportAdvisor>(TransportHint::UB_CANDIDATE));
    std::shared_ptr<ObjectBuffer> failedBuffer;
    ASSERT_TRUE(failedClient.Create(MakeAddress(34), "failed", 4, MakeCreateParam(), failedBuffer).IsOk());
    failedManager->builtTransporters.front()->setUbFailureReports = { Status(K_URMA_ERROR, "client one error 4") };
    failedManager->builtTransporters.front()->setUbCqeStatuses = { 4 };
    EXPECT_EQ(failedClient.Set(*failedBuffer, MakeSetParam()).GetCode(), K_URMA_ERROR);
    EXPECT_EQ(failedClient.Set(*failedBuffer, MakeSetParam()).GetCode(), K_URMA_WORKER_UNAVAILABLE);

    auto healthyManager = std::make_shared<FakeDataPlaneManager>();
    TestTransportLayer healthyClient(healthyManager,
                                     std::make_shared<FixedTransportAdvisor>(TransportHint::UB_CANDIDATE));
    std::shared_ptr<ObjectBuffer> healthyBuffer;
    EXPECT_TRUE(healthyClient.Create(MakeAddress(34), "healthy", 4, MakeCreateParam(), healthyBuffer).IsOk());
    EXPECT_TRUE(healthyClient.Set(*healthyBuffer, MakeSetParam()).IsOk());
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
}  // namespace client
}  // namespace datasystem
