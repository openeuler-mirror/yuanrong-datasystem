/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

#include <chrono>
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "common/stream_cache/stream_common.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/stream/consumer.h"
#include "datasystem/stream/producer.h"
#include "datasystem/stream_client.h"
#include "sc_client_common.h"

namespace datasystem {
namespace st {
namespace {
constexpr int kEventuallyWaitTimeoutMs = 15'000;
constexpr int kEventuallyPollIntervalMs = 100;
constexpr const char *kKeepAliveFailureInject = "EtcdKeepAlive.SendKeepAliveMessage";
constexpr const char *kKeepAliveQuickLoopInject = "EtcdStore.LaunchKeepAliveThreads.loopQuickly";
constexpr const char *kLeaseExpiredInject = "GetLeaseExpiredMs";
constexpr const char *kLocalIsolatedInject = "WorkerOCServer.AfterMarkLocalIsolated";
constexpr const char *kBeforeMarkRunningInject = "WorkerRecoveryController.BeforeMarkRunning";
constexpr const char *kWorkerFlags =
    " -client_reconnect_wait_s=1 -ipc_through_shared_memory=true -node_timeout_s=2 -node_dead_timeout_s=3"
    " -heartbeat_interval_ms=1000 -auto_del_dead_node=false";

void AssertEventuallyOk(const std::function<Status()> &operation, const std::string &operationName)
{
    Status rc;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(kEventuallyWaitTimeoutMs);
    do {
        rc = operation();
        if (rc.IsOk()) {
            return;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(kEventuallyPollIntervalMs));
    } while (std::chrono::steady_clock::now() < deadline);
    ASSERT_TRUE(rc.IsOk()) << operationName << " stayed non-OK for " << kEventuallyWaitTimeoutMs
                           << " ms, last status: " << rc.ToString();
}

void ExpectAdmissionRejected(const Status &rc, const std::string &operation)
{
    ASSERT_EQ(rc.GetCode(), K_NOT_READY) << operation << " status: " << rc.ToString();
}

std::string NewStreamName(const std::string &prefix)
{
    return prefix + "_" + GetStringUuid();
}
}  // namespace

class StreamClientAdmissionTest : public SCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numEtcd = 1;
        opts.numWorkers = 2;
        opts.enableDistributedMaster = "true";
        opts.workerGflagParams = kWorkerFlags;
        SCClientCommon::SetClusterSetupOptions(opts);
    }

protected:
    void SetUp() override
    {
        ExternalClusterTest::SetUp();
        InitStreamClient(0, client_);
        producerConf_.maxStreamSize = TEST_STREAM_SIZE;
    }

    void TearDown() override
    {
        client_.reset();
        ExternalClusterTest::TearDown();
    }

    std::shared_ptr<StreamClient> client_;
    ProducerConf producerConf_;
};

TEST_F(StreamClientAdmissionTest, LEVEL1_StreamClientRejectsReadWriteDuringIsolationAndRecovering)
{
    constexpr int workerIndex = 0;
    const std::string readyStream = NewStreamName("stream_admission_ready");
    std::shared_ptr<Producer> readyProducer;
    DS_ASSERT_OK(client_->CreateProducer(readyStream, readyProducer, producerConf_));
    std::shared_ptr<Consumer> readyConsumer;
    DS_ASSERT_OK(
        client_->Subscribe(readyStream, SubscriptionConfig("sub_ready", SubscriptionType::STREAM), readyConsumer));

    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, workerIndex, kLocalIsolatedInject, "call()"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, workerIndex, kKeepAliveFailureInject, "return(K_RPC_UNAVAILABLE)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, workerIndex, kLeaseExpiredInject, "call(1000)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, workerIndex, kKeepAliveQuickLoopInject, "call(0)"));
    bool keepAliveFailureActive = true;
    bool recoveryPauseActive = false;
    Raii clearFaults([&]() {
        if (recoveryPauseActive) {
            LOG_IF_ERROR(cluster_->ClearInjectAction(WORKER, workerIndex, kBeforeMarkRunningInject),
                         "clear stream recovery pause");
        }
        if (keepAliveFailureActive) {
            LOG_IF_ERROR(cluster_->ClearInjectAction(WORKER, workerIndex, kKeepAliveFailureInject),
                         "clear stream keepalive failure");
            LOG_IF_ERROR(cluster_->ClearInjectAction(WORKER, workerIndex, kLeaseExpiredInject),
                         "clear stream lease override");
            LOG_IF_ERROR(cluster_->ClearInjectAction(WORKER, workerIndex, kKeepAliveQuickLoopInject),
                         "clear stream quick keepalive loop");
        }
    });

    AssertEventuallyOk(
        [&]() {
            uint64_t count = 0;
            RETURN_IF_NOT_OK(cluster_->GetInjectActionExecuteCount(WORKER, workerIndex, kLocalIsolatedInject, count));
            CHECK_FAIL_RETURN_STATUS(count > 0, K_NOT_READY, "worker has not entered local isolation");
            return Status::OK();
        },
        "stream client target enters local isolation");

    std::shared_ptr<Producer> rejectedProducer;
    ExpectAdmissionRejected(
        client_->CreateProducer(NewStreamName("stream_admission_isolated"), rejectedProducer, producerConf_),
        "stream create producer during local isolation");
    std::shared_ptr<Consumer> rejectedConsumer;
    ExpectAdmissionRejected(
        client_->Subscribe(readyStream, SubscriptionConfig("sub_isolated", SubscriptionType::STREAM), rejectedConsumer),
        "stream subscribe during local isolation");
    std::vector<Element> outElements;
    ExpectAdmissionRejected(readyConsumer->Receive(1, 1'000, outElements), "stream receive during local isolation");

    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, workerIndex, kBeforeMarkRunningInject, "1*pause"));
    recoveryPauseActive = true;
    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, workerIndex, kKeepAliveFailureInject));
    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, workerIndex, kLeaseExpiredInject));
    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, workerIndex, kKeepAliveQuickLoopInject));
    keepAliveFailureActive = false;
    AssertEventuallyOk(
        [&]() {
            uint64_t count = 0;
            RETURN_IF_NOT_OK(
                cluster_->GetInjectActionExecuteCount(WORKER, workerIndex, kBeforeMarkRunningInject, count));
            CHECK_FAIL_RETURN_STATUS(count > 0, K_NOT_READY, "worker has not entered the recovery evidence gate");
            return Status::OK();
        },
        "stream client target enters recovering mode");

    rejectedProducer.reset();
    ExpectAdmissionRejected(
        client_->CreateProducer(NewStreamName("stream_admission_recovering"), rejectedProducer, producerConf_),
        "stream create producer during recovery");
    rejectedConsumer.reset();
    ExpectAdmissionRejected(
        client_->Subscribe(readyStream, SubscriptionConfig("sub_recovering", SubscriptionType::STREAM),
                           rejectedConsumer),
        "stream subscribe during recovery");
    outElements.clear();
    ExpectAdmissionRejected(readyConsumer->Receive(1, 1'000, outElements), "stream receive during recovery");

    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, workerIndex, kBeforeMarkRunningInject));
    recoveryPauseActive = false;
    AssertEventuallyOk(
        [&]() {
            const std::string recoveredStream = NewStreamName("stream_admission_recovered");
            std::shared_ptr<Producer> producer;
            RETURN_IF_NOT_OK(client_->CreateProducer(recoveredStream, producer, producerConf_));
            std::shared_ptr<Consumer> consumer;
            RETURN_IF_NOT_OK(client_->Subscribe(
                recoveredStream, SubscriptionConfig("sub_recovered", SubscriptionType::STREAM), consumer));
            const std::string payload = "stream-recovered";
            RETURN_IF_NOT_OK(producer->Send(
                Element(reinterpret_cast<uint8_t *>(const_cast<char *>(payload.data())), payload.size())));
            std::vector<Element> recoveredElements;
            RETURN_IF_NOT_OK(consumer->Receive(1, 5'000, recoveredElements));
            CHECK_FAIL_RETURN_STATUS(recoveredElements.size() == 1, K_NOT_READY,
                                     "stream receive returned no element after recovery");
            return Status::OK();
        },
        "stream client reopens after recovery evidence completes");
}
}  // namespace st
}  // namespace datasystem
