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
#include "datasystem/utils/optional.h"

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
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
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
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    bool needRollbackState;
    auto rc = impl_->Init(ip_, port_, needRollbackState, reportWorkerLost);
    impl_->CompleteHandler(rc.IsError(), needRollbackState);
    return rc;
}

Status StreamClient::CreateProducer(const std::string &streamName, std::shared_ptr<Producer> &outProducer,
                                    ProducerConf producerConf)
{
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    PerfPoint point(PerfKey::CLIENT_CREATE_PRODUCER_ALL);
    AccessRecorder recorder(AccessRecorderKey::DS_STREAM_CREATE_PRODUCER);
    auto rc = impl_->CreateProducer(streamName, outProducer, producerConf);
    StreamRequestParam reqParam;
    reqParam.streamName = streamName;
    reqParam.delayFlushTime = Optional<int64_t>(producerConf.delayFlushTime);
    reqParam.pageSize = Optional<int64_t>(producerConf.pageSize);
    reqParam.maxStreamSize = Optional<uint64_t>(producerConf.maxStreamSize);
    reqParam.autoCleanup = Optional<bool>(producerConf.autoCleanup);
    reqParam.retainForNumConsumers = Optional<uint64_t>(producerConf.retainForNumConsumers);
    reqParam.encryptStream = Optional<bool>(producerConf.encryptStream);
    reqParam.streamMode = Optional<int32_t>(producerConf.streamMode);
    StreamResponseParam rspParam;
    rspParam.msg = rc.GetMsg();
    recorder.Record(rc.GetCode(), reqParam, rspParam);
    return rc;
}

Status StreamClient::Subscribe(const std::string &streamName, const struct SubscriptionConfig &config,
                               std::shared_ptr<Consumer> &outConsumer, bool autoAck)
{
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    PerfPoint point(PerfKey::CLIENT_CREATE_SUB_ALL);
    AccessRecorder recorder(AccessRecorderKey::DS_STREAM_SUBSCRIBE);
    std::string consumerId;
    auto rc = impl_->Subscribe(streamName, config, outConsumer, autoAck);
    StreamRequestParam reqParam;
    reqParam.streamName = streamName;
    reqParam.subscriptionName = config.subscriptionName;
    reqParam.autoAck = Optional<bool>(autoAck);
    StreamResponseParam rspParam;
    rspParam.msg = rc.GetMsg();
    recorder.Record(rc.GetCode(), reqParam, rspParam);
    return rc;
}

Status StreamClient::DeleteStream(const std::string &streamName)
{
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    PerfPoint point(PerfKey::CLIENT_DELETE_STREAM_ALL);
    AccessRecorder recorder(AccessRecorderKey::DS_STREAM_DELETE_STREAM);
    auto rc = impl_->DeleteStream(streamName);
    StreamRequestParam reqParam;
    reqParam.streamName = streamName;
    StreamResponseParam rspParam;
    rspParam.msg = rc.GetMsg();
    recorder.Record(rc.GetCode(), reqParam, rspParam);
    return rc;
}

Status StreamClient::QueryGlobalProducersNum(const std::string &streamName, uint64_t &gProducerNum)
{
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    AccessRecorder recorder(AccessRecorderKey::DS_STREAM_QUERY_PRODUCERS_NUM);
    auto rc = impl_->QueryGlobalProducersNum(streamName, gProducerNum);
    StreamRequestParam reqParam;
    reqParam.streamName = streamName;
    StreamResponseParam rspParam;
    rspParam.msg = rc.GetMsg();
    rspParam.count = Optional<uint64_t>(gProducerNum);
    recorder.Record(rc.GetCode(), reqParam, rspParam);
    return rc;
}

Status StreamClient::QueryGlobalConsumersNum(const std::string &streamName, uint64_t &gConsumerNum)
{
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    AccessRecorder recorder(AccessRecorderKey::DS_STREAM_QUERY_CONSUMERS_NUM);
    auto rc = impl_->QueryGlobalConsumersNum(streamName, gConsumerNum);
    StreamRequestParam reqParam;
    reqParam.streamName = streamName;
    StreamResponseParam rspParam;
    rspParam.msg = rc.GetMsg();
    rspParam.count = Optional<uint64_t>(gConsumerNum);
    recorder.Record(rc.GetCode(), reqParam, rspParam);
    return rc;
}
}  // namespace datasystem
