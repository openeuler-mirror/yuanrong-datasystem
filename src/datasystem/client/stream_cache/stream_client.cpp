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
 * Description: Define stream cache client.
 */
#include "datasystem/stream_client.h"

#include "datasystem/common/flags/flags.h"
#include "datasystem/client/stream_cache/stream_client_impl.h"
#include "datasystem/common/log/access_recorder.h"
#include "datasystem/common/perf/perf_manager.h"

namespace datasystem {
StreamClient::StreamClient(ConnectOptions connectOptions)
    : ip_(std::move(connectOptions.host)), port_(connectOptions.port)
{
    impl_ = std::make_shared<client::stream_cache::StreamClientImpl>(connectOptions);
}

StreamClient::~StreamClient()
{
    LOG(INFO) << "Destroy StreamClient";
    if (impl_) {
        impl_.reset();
    }
}

Status StreamClient::ShutDown()
{
    TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
    if (impl_) {
        bool needRollbackState;
        auto rc = impl_->ShutDown(needRollbackState);
        impl_->CompleteHandler(rc.IsError(), needRollbackState);
        return rc;
    }
    return Status::OK();
}

Status StreamClient::Init(bool reportWorkerLost)
{
    TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
    bool needRollbackState;
    auto rc = impl_->Init(ip_, port_, needRollbackState, reportWorkerLost);
    impl_->CompleteHandler(rc.IsError(), needRollbackState);
    return rc;
}

Status StreamClient::UpdateToken(SensitiveValue token)
{
    TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
    return impl_->UpdateToken(token);
}

Status StreamClient::UpdateAkSk(const std::string accessKey, SensitiveValue secretKey)
{
    TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
    return impl_->UpdateAkSk(accessKey, secretKey);
}

Status StreamClient::CreateProducer(const std::string &streamName, std::shared_ptr<Producer> &outProducer,
                                    ProducerConf producerConf)
{
    TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
    PerfPoint point(PerfKey::CLIENT_CREATE_PRODUCER_ALL);
    auto access = AccessRecorder::Stream(AccessRecorderKey::DS_STREAM_CREATE_PRODUCER);
    auto rc = impl_->CreateProducer(streamName, outProducer, producerConf);
    access.StreamName(streamName).DelayFlushTime(producerConf.delayFlushTime).PageSize(producerConf.pageSize)
        .MaxStreamSize(producerConf.maxStreamSize).AutoCleanup(producerConf.autoCleanup)
        .RetainForNumConsumers(producerConf.retainForNumConsumers).EncryptStream(producerConf.encryptStream)
        .StreamMode(producerConf.streamMode).Result(rc).Record();
    return rc;
}

Status StreamClient::Subscribe(const std::string &streamName, const struct SubscriptionConfig &config,
                               std::shared_ptr<Consumer> &outConsumer, bool autoAck)
{
    TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
    PerfPoint point(PerfKey::CLIENT_CREATE_SUB_ALL);
    auto access = AccessRecorder::Stream(AccessRecorderKey::DS_STREAM_SUBSCRIBE);
    std::string consumerId;
    auto rc = impl_->Subscribe(streamName, config, outConsumer, autoAck);
    access.StreamName(streamName).SubscriptionName(config.subscriptionName).AutoAck(autoAck).Result(rc).Record();
    return rc;
}

Status StreamClient::DeleteStream(const std::string &streamName)
{
    TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
    PerfPoint point(PerfKey::CLIENT_DELETE_STREAM_ALL);
    auto access = AccessRecorder::Stream(AccessRecorderKey::DS_STREAM_DELETE_STREAM);
    auto rc = impl_->DeleteStream(streamName);
    access.StreamName(streamName).Result(rc).Record();
    return rc;
}

Status StreamClient::QueryGlobalProducersNum(const std::string &streamName, uint64_t &gProducerNum)
{
    TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
    auto access = AccessRecorder::Stream(AccessRecorderKey::DS_STREAM_QUERY_PRODUCERS_NUM);
    auto rc = impl_->QueryGlobalProducersNum(streamName, gProducerNum);
    access.StreamName(streamName).Count(gProducerNum).Result(rc).Record();
    return rc;
}

Status StreamClient::QueryGlobalConsumersNum(const std::string &streamName, uint64_t &gConsumerNum)
{
    TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
    auto access = AccessRecorder::Stream(AccessRecorderKey::DS_STREAM_QUERY_CONSUMERS_NUM);
    auto rc = impl_->QueryGlobalConsumersNum(streamName, gConsumerNum);
    access.StreamName(streamName).Count(gConsumerNum).Result(rc).Record();
    return rc;
}
}  // namespace datasystem
