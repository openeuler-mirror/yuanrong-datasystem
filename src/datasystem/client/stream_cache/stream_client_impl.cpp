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
 * Description: Implement stream cache client.
 */
#include "datasystem/client/stream_cache/stream_client_impl.h"

#include <memory>
#include <mutex>

#include "datasystem/client/client_flags_monitor.h"
#include "datasystem/client/stream_cache/client_worker_api.h"
#include "datasystem/client/stream_cache/consumer_impl.h"
#include "datasystem/client/stream_cache/producer_impl.h"
#include "datasystem/client/stream_cache/producer_consumer_worker_api.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/log/logging.h"
#include "datasystem/common/log/spdlog/provider.h"
#include "datasystem/common/util/container_util.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/rpc_util.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/thread_local.h"
#include "datasystem/common/util/uuid_generator.h"
#include "datasystem/common/util/validator.h"
#include "datasystem/stream/consumer.h"
#include "datasystem/stream/producer.h"

const std::string LOG_FILENAME = "ds_client";

namespace datasystem {
namespace client {
namespace stream_cache {

StreamClientImpl::StreamClientImpl(const std::string &clientPublicKey, const SensitiveValue &clientPrivateKey,
                                   const std::string &serverPublicKey, const std::string &accessKey,
                                   const SensitiveValue &secretKey)
    : signature_(std::make_unique<Signature>(accessKey, secretKey))
{
    (void)Provider::Instance();
    clientStateManager_ = std::make_unique<ClientStateManager>();
    authKeys_.SetClientPublicKey(clientPublicKey);
    authKeys_.SetClientPrivateKey(clientPrivateKey);
    authKeys_.SetServerKey(WORKER_SERVER_NAME, serverPublicKey);
}

StreamClientImpl::StreamClientImpl(const ConnectOptions &connectOptions)
{
    (void)Provider::Instance();
    clientStateManager_ = std::make_unique<ClientStateManager>();
    token_ = connectOptions.token;
    signature_ = std::make_unique<Signature>(connectOptions.accessKey, connectOptions.secretKey);
    tenantId_ = connectOptions.tenantId;
    connectTimeoutMs_ = connectOptions.connectTimeoutMs;
    requestTimeoutMs_ = connectOptions.requestTimeoutMs > 0 ? connectOptions.requestTimeoutMs : connectTimeoutMs_;
    authKeys_.SetClientPublicKey(connectOptions.clientPublicKey);
    authKeys_.SetClientPrivateKey(connectOptions.clientPrivateKey);
    authKeys_.SetServerKey(WORKER_SERVER_NAME, connectOptions.serverPublicKey);
}

StreamClientImpl::~StreamClientImpl()
{
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    LOG(INFO) << "Destroy StreamClientImpl";
    auto shutdownFunc = std::bind(&StreamClientImpl::ShutDown, this, true, true);
    clientStateManager_->ProcessDestruct(shutdownFunc);
}

Status StreamClientImpl::ShutDown(bool &needRollbackState, bool isDestruct)
{
    INJECT_POINT("StreamClient.ShutDown.skip");
    // Step0: Check client's status to determine whether it meets the conditions for executing shutdown.
    auto rc = clientStateManager_->ProcessShutdown(needRollbackState, isDestruct);
    if (!needRollbackState) {
        return rc;
    }

    // Step1: Clear existing producers and consumers.
    ClearProducerAndConsumer();

    // Step2: Shutdown heartbeat.
    if (listenWorker_ != nullptr) {
        listenWorker_->RemoveCallBackFunc(this);
        listenWorker_->StopListenWorker(true);
    }

    // Step3: Send notice to worker before disconnection.
    if (clientWorkerApi_ != nullptr) {
        RETURN_IF_NOT_OK(clientWorkerApi_->Disconnect(isDestruct));
    }
    return Status::OK();
}

Status StreamClientImpl::Init(const std::string &ip, const int &port, bool &needRollbackState, bool reportWorkerLost)
{
    Logging::GetInstance()->Start(LOG_FILENAME, true);
    FlagsMonitor::GetInstance()->Start();
    auto rc = clientStateManager_->ProcessInit(needRollbackState);
    if (!needRollbackState) {
        return rc;
    }

    HostPort hostPort(ip, port);
    CHECK_FAIL_RETURN_STATUS(Validator::ValidateHostPortString("StreamClient", hostPort.ToString()), K_INVALID,
                             FormatString("Invalid host or port: %s : %d", ip, port));

    RpcCredential cred;
    RETURN_IF_NOT_OK(RpcAuthKeyManager::CreateClientCredentials(authKeys_, WORKER_SERVER_NAME, cred));

    clientWorkerApi_ = std::make_shared<ClientWorkerApi>(hostPort, cred, token_, signature_.get(), tenantId_);
    RETURN_IF_NOT_OK(clientWorkerApi_->Init(requestTimeoutMs_, connectTimeoutMs_));
    VLOG(SC_NORMAL_LOG_LEVEL) << "clientWorkerApi_ init success";
    mmapManager_ = std::make_unique<datasystem::client::MmapManager>(
        std::dynamic_pointer_cast<IClientWorkerCommonApi>(clientWorkerApi_), false);
    listenWorker_ = std::make_shared<ListenWorker>(clientWorkerApi_, HeartbeatType::RPC_HEARTBEAT);
    callBack_ = [this]() {
        LOG(INFO) << "Disconnected from worker, clear mmap and try to reconnect...";
        if (reportWorkerLost_) {
            workerWasLost_ = true;
        }
        ClearProducerAndConsumer();
        mmapManager_->CleanInvalidMmapTable();
        Status reconnectStatus = clientWorkerApi_->Reconnect();
        if (reconnectStatus.IsError()) {
            LOG(ERROR) << "Reconnect to worker failed, please check network and worker status and restart client."
                       << reconnectStatus.ToString();
            return;
        }
        listenWorker_->SetWorkerAvailable(true);
        LOG(INFO) << "Reconnect to worker success";
    };
    listenWorker_->StartListenWorker();
    listenWorker_->AddCallBackFunc(this, callBack_);
    listenWorker_->SetReleaseFdCallBack(
        [this](const std::vector<int64_t> &fds) { mmapManager_->ClearExpiredFds(fds); });
    reportWorkerLost_ = reportWorkerLost;
    isInit_ = true;
    return Status::OK();
}

Status StreamClientImpl::CreatePrefetchPoolIfNotExist()
{
    const int numPrefetchThreads = 8;
    std::lock_guard<std::mutex> lock(initMutex_);
    if (!prefetchThdPool_) {
        RETURN_IF_EXCEPTION_OCCURS(prefetchThdPool_ = std::make_unique<ThreadPool>(numPrefetchThreads));
    }
    return Status::OK();
}

Status StreamClientImpl::UpdateToken(SensitiveValue &token)
{
    return clientWorkerApi_->UpdateToken(token);
}

Status StreamClientImpl::UpdateAkSk(const std::string &accessKey, SensitiveValue &secretKey)
{
    return clientWorkerApi_->UpdateAkSk(accessKey, secretKey);
}

uint32_t StreamClientImpl::GetLockId() const
{
    return clientWorkerApi_->lockId_;
}

Status StreamClientImpl::VerifyProducerConfig(const ProducerConf &producerConf)
{
    const static int64_t smallestPageSize = 4 * 1024;  // 4KB
    // The maxPageSize is determined by the maximum offset supported in a page slot.
    const static int64_t maxPageSize = SLOT_VALUE_MASK + 1;  // 16MB max limit
    const static uint64_t minStreamSize = 64 * 1024;         // 64K
    const static uint64_t maxRetainForNumConsumers = 16;
    const static uint64_t minNumPages = 2;
    CHECK_FAIL_RETURN_STATUS(producerConf.pageSize > 0 && producerConf.pageSize % smallestPageSize == 0, K_INVALID,
                             FormatString("Page size not multiple of %d.", smallestPageSize));
    CHECK_FAIL_RETURN_STATUS(producerConf.pageSize <= maxPageSize, K_INVALID,
                             FormatString("Page size exceeds the maximum. [page size, max size] : [ %zu, %zu ]",
                                          producerConf.pageSize, maxPageSize));
    CHECK_FAIL_RETURN_STATUS(
        producerConf.retainForNumConsumers <= maxRetainForNumConsumers, K_INVALID,
        FormatString("retainForNumConsumers exceeds the maximum. [retain value, max limit] : [ %zu, %zu ]",
                     producerConf.retainForNumConsumers, maxRetainForNumConsumers));
    CHECK_FAIL_RETURN_STATUS(
        producerConf.maxStreamSize >= minStreamSize, K_INVALID,
        FormatString("Stream size must be at least the minimum size. [stream size, min size] : [ %zu, %zu ]",
                     producerConf.maxStreamSize, minStreamSize));
    CHECK_FAIL_RETURN_STATUS(
        static_cast<uint64_t>(producerConf.pageSize) <= producerConf.maxStreamSize, K_INVALID,
        FormatString("Page size exceeds the maximum stream size. [page size, max stream size] : [ %zu, %zu ]",
                     producerConf.pageSize, producerConf.maxStreamSize));
    CHECK_FAIL_RETURN_STATUS(
        producerConf.reserveSize <= producerConf.maxStreamSize, K_INVALID,
        FormatString("Reserve size exceeds the maximum stream size. [reserve size, max stream size] : [ %zu, %zu ]",
                     producerConf.reserveSize, producerConf.maxStreamSize));
    CHECK_FAIL_RETURN_STATUS(
        producerConf.reserveSize % producerConf.pageSize == 0, K_INVALID,
        FormatString("Reserve size not a multiple of page size. [page size, reserve size] : [ %zu, %zu ]",
                     producerConf.pageSize, producerConf.reserveSize));
    CHECK_FAIL_RETURN_STATUS(
        producerConf.maxStreamSize / producerConf.pageSize >= minNumPages, K_INVALID,
        FormatString("Stream size must be at least twice the page size. [page size, max stream size] : [ %zu, %zu ]",
                     producerConf.pageSize, producerConf.maxStreamSize));
    return Status::OK();
}

Status StreamClientImpl::CreateProducer(const std::string &streamName, std::shared_ptr<Producer> &outProducer,
                                        const ProducerConf &producerConf)
{
    CHECK_FAIL_RETURN_STATUS(Validator::IsRegexMatch(idRe_, streamName), K_INVALID,
                             "The streamName contains illegal char(s).");
    RETURN_IF_NOT_OK(VerifyProducerConfig(producerConf));
    RETURN_IF_NOT_OK(CheckConnectByUds());
    RETURN_IF_NOT_OK(IsClientReady());
    RETURN_IF_NOT_OK(CheckWorkerLost());
    RETURN_IF_NOT_OK(listenWorker_->CheckWorkerAvailable());
    std::string producerId = GetStringUuid();
    ShmView pageView, streamMetaView;
    DataVerificationHeader::SenderProducerNo senderProducerNo;
    DataVerificationHeader::Address address;
    inet_pton(clientWorkerApi_->GetWorkHostPortINETFamily(), clientWorkerApi_->hostPort_.Host().c_str(), &address);
    DataVerificationHeader::Port port = static_cast<uint16_t>(clientWorkerApi_->hostPort_.Port());
    bool enableStreamDataVerification;
    uint64_t streamNo;
    bool enableSharedPage;
    uint64_t sharedPageSize;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
        clientWorkerApi_->CreateProducer(streamName, producerId, producerConf, pageView, senderProducerNo,
                                         enableStreamDataVerification, streamNo, enableSharedPage, sharedPageSize,
                                         streamMetaView),
        "CreateProducer request error");
    INJECT_POINT("Mimic.Producer.Old.Version", [&enableStreamDataVerification, &senderProducerNo]() {
        enableStreamDataVerification = false;
        senderProducerNo = 0;
        return Status::OK();
    });
    std::string tenantId = g_ContextTenantId.empty() ? tenantId_ : g_ContextTenantId;
    std::shared_ptr<ProducerConsumerWorkerApi> clientWorkerApi =
        std::make_shared<ProducerConsumerWorkerApi>(tenantId, clientWorkerApi_);
    auto impl = std::make_shared<ProducerImpl>(
        streamName, tenantId, producerId, producerConf.delayFlushTime, producerConf.pageSize, clientWorkerApi,
        shared_from_this(), mmapManager_.get(), listenWorker_, pageView, producerConf.maxStreamSize, senderProducerNo,
        enableStreamDataVerification, address, port, producerConf.streamMode, streamNo, enableSharedPage,
        sharedPageSize, streamMetaView);

    Status rc = impl->Init();
    class ProducerHelper : public Producer {
    public:
        explicit ProducerHelper(std::shared_ptr<client::stream_cache::ProducerImpl> impl) : Producer(std::move(impl))
        {
        }
    };
    outProducer = std::make_shared<ProducerHelper>(std::move(impl));
    if (rc.IsError()) {
        RETURN_IF_NOT_OK(outProducer->Close());
        RETURN_STATUS(
            StatusCode::K_RUNTIME_ERROR,
            FormatString("Fail to init mmap memory for producer:<%s> with status: %s", producerId, rc.GetMsg()));
    }
    std::lock_guard<std::shared_timed_mutex> lk(clearMutex_);
    producers_.emplace(producerId, outProducer);
    LOG(INFO) << FormatString(
        "[%s] Create producer success. AutoCleanup is %s, senderProducerNo = %lu, "
        "enableStreamDataVerification = %s, workerArea = %s, streamNo = %llu, enableSharedPage = %s, sharedPageSize = "
        "%lu",
        outProducer->impl_->LogPrefix(), producerConf.autoCleanup ? "true" : "false", senderProducerNo,
        (enableStreamDataVerification ? "true" : "false"), (outProducer->impl_->WorkAreaIsV2() ? "V2" : "V1"), streamNo,
        (enableSharedPage ? "true" : "false"), sharedPageSize);
    return Status::OK();
}

Status StreamClientImpl::Subscribe(const std::string &streamName, const struct SubscriptionConfig &config,
                                   std::shared_ptr<Consumer> &outConsumer, bool autoAck)
{
    RETURN_IF_NOT_OK(CheckConnectByUds());
    RETURN_IF_NOT_OK(IsClientReady());
    CHECK_FAIL_RETURN_STATUS(Validator::IsRegexMatch(idRe_, streamName), K_INVALID,
                             "The streamName contains illegal char(s).");
    CHECK_FAIL_RETURN_STATUS(Validator::IsRegexMatch(idRe_, config.subscriptionName), K_INVALID,
                             "The subscriptionName contains illegal char(s).");
    RETURN_IF_NOT_OK(CheckWorkerLost());
    RETURN_IF_NOT_OK(listenWorker_->CheckWorkerAvailable());
    std::string consumerId = GetStringUuid();
    SubscribeRspPb rsp;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(clientWorkerApi_->Subscribe(streamName, consumerId, config, rsp),
                                     "Subscribe request error");
    std::string tenantId = g_ContextTenantId.empty() ? tenantId_ : g_ContextTenantId;
    std::shared_ptr<ProducerConsumerWorkerApi> clientWorkerApi =
        std::make_shared<ProducerConsumerWorkerApi>(tenantId, clientWorkerApi_);
    auto consumerImpl = std::make_unique<ConsumerImpl>(streamName, tenantId, config, consumerId, rsp, clientWorkerApi,
                                                       shared_from_this(), mmapManager_.get(), listenWorker_, autoAck);
    Status rc = consumerImpl->Init();
    if (rc.GetCode() == K_OUT_OF_RANGE) {
        // This is a special return code indicating that the worker may have restarted.
        // The call back function ClearProducerAndConsumer may set each producer/consumer to CLOSE state
        // but, our consumer is not yet in the consumers_ map yet. So we need to check the state again
        RETURN_IF_NOT_OK(CheckWorkerLost());
        RETURN_IF_NOT_OK(listenWorker_->CheckWorkerAvailable());
    }
    RETURN_IF_NOT_OK(rc);

    // When initializing Consumer, the first element to receive.
    class ConsumerHelper : public Consumer {
    public:
        explicit ConsumerHelper(std::unique_ptr<client::stream_cache::ConsumerImpl> impl) : Consumer(std::move(impl))
        {
        }
    };
    outConsumer = std::make_shared<ConsumerHelper>(std::move(consumerImpl));

    std::lock_guard<std::shared_timed_mutex> lk(clearMutex_);
    consumers_.emplace(consumerId, outConsumer);
    LOG(INFO) << FormatString("[%s] Create consumer success. AutoAck is %s, workerArea is %s",
                              outConsumer->impl_->LogPrefix(), autoAck ? "true" : "false",
                              (outConsumer->impl_->WorkAreaIsV2() ? "V2" : "V1"));
    return Status::OK();
}

Status StreamClientImpl::DeleteStream(const std::string &streamName)
{
    RETURN_IF_NOT_OK(IsClientReady());
    CHECK_FAIL_RETURN_STATUS(Validator::IsRegexMatch(idRe_, streamName), K_INVALID,
                             "The streamName contains illegal char(s).");
    RETURN_IF_NOT_OK(listenWorker_->CheckWorkerAvailable());
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(clientWorkerApi_->DeleteStream(streamName),
                                     FormatString("[S:%s] Delete stream failed.", streamName));
    LOG(INFO) << FormatString("[S:%s] Delete stream success.", streamName);
    return Status::OK();
}

Status StreamClientImpl::QueryGlobalProducersNum(const std::string &streamName, uint64_t &gProducerNum)
{
    RETURN_IF_NOT_OK(IsClientReady());
    CHECK_FAIL_RETURN_STATUS(Validator::IsRegexMatch(idRe_, streamName), K_INVALID,
                             "The streamName contains illegal char(s).");
    gProducerNum = 0;
    RETURN_IF_NOT_OK(CheckWorkerLost());
    RETURN_IF_NOT_OK(listenWorker_->CheckWorkerAvailable());
    return clientWorkerApi_->QueryGlobalProducersNum(streamName, gProducerNum);
}

Status StreamClientImpl::QueryGlobalConsumersNum(const std::string &streamName, uint64_t &gConsumerNum)
{
    RETURN_IF_NOT_OK(IsClientReady());
    CHECK_FAIL_RETURN_STATUS(Validator::IsRegexMatch(idRe_, streamName), K_INVALID,
                             "The streamName contains illegal char(s).");
    gConsumerNum = 0;
    RETURN_IF_NOT_OK(CheckWorkerLost());
    RETURN_IF_NOT_OK(listenWorker_->CheckWorkerAvailable());
    return clientWorkerApi_->QueryGlobalConsumersNum(streamName, gConsumerNum);
}

void StreamClientImpl::CleanupProdsCons(std::vector<std::weak_ptr<Producer>> &resetProducers,
                                        std::vector<std::weak_ptr<Consumer>> &resetConsumers)
{
    for (auto &producer : resetProducers) {
        if (auto ptr = producer.lock()) {
            ptr->impl_->Reset();
        }
    }
    for (auto &consumer : resetConsumers) {
        if (auto ptr = consumer.lock()) {
            ptr->impl_->Reset();
        }
    }
}

void StreamClientImpl::ClearProducer(const std::string &producerId)
{
    std::lock_guard<std::shared_timed_mutex> lk(clearMutex_);
    auto num = producers_.erase(producerId);
    LOG_IF(WARNING, num == 0) << "Producer " << producerId << " not found in client.";
}

void StreamClientImpl::ClearConsumer(const std::string &consumerId)
{
    std::lock_guard<std::shared_timed_mutex> lk(clearMutex_);
    auto num = consumers_.erase(consumerId);
    LOG_IF(WARNING, num == 0) << "Consumer " << consumerId << " not found in client.";
}

void StreamClientImpl::ClearProducerAndConsumer()
{
    auto func = [](auto &user) {
        // Ensure that this consumer or producer used is not destroyed.
        if (auto ptr = user.second.lock()) {
            ptr->impl_->SetInactive();
        }
    };
    std::lock_guard<std::shared_timed_mutex> lk(clearMutex_);
    LOG(INFO) << FormatString("Begin to clear %zu producers and %zu consumers", producers_.size(), consumers_.size());
    if (!producers_.empty()) {
        std::for_each(producers_.begin(), producers_.end(), func);
        producers_.clear();
    }
    if (!consumers_.empty()) {
        std::for_each(consumers_.begin(), consumers_.end(), func);
        consumers_.clear();
    }
    LOG(INFO) << "Clear producer and consumer success";
}

inline Status StreamClientImpl::IsClientReady()
{
    uint16_t clientState = clientStateManager_->GetState();
    CHECK_FAIL_RETURN_STATUS(clientState == (uint16_t)ClientState::INITIALIZED, StatusCode::K_NOT_READY,
                             clientStateManager_->ToStringForUser(clientState));
    return Status::OK();
}

Status StreamClientImpl::CheckConnectByUds()
{
    RETURN_OK_IF_TRUE(clientWorkerApi_->shmEnabled_);
    RETURN_STATUS(
        StatusCode::K_RUNTIME_ERROR,
        "Connection to worker not using unix domain socket, please check if the connection is to a local worker.");
}
}  // namespace stream_cache
}  // namespace client
}  // namespace datasystem
