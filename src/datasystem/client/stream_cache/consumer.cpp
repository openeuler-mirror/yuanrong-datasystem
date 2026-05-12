/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2023. All rights reserved.
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
 * Description: Define api of stream cache consumer.
 */
#include "datasystem/stream/consumer.h"

#include "datasystem/client/stream_cache/consumer_impl.h"
#include "datasystem/common/log/access_recorder.h"
#include "datasystem/common/log/trace.h"
#include "datasystem/utils/optional.h"

namespace datasystem {

Consumer::~Consumer()
{
    if (impl_->IsActive()) {
        LOG(INFO) << FormatString("[%s] Implicit close consumer", impl_->LogPrefix());
        Status rc = Close();
        if (rc.IsError()) {
            LOG(ERROR) << FormatString("[%s] Implicit close consumer failed %s.", impl_->LogPrefix(), rc.GetMsg());
        }
    }
}

Status Consumer::Receive(uint32_t expectNum, uint32_t timeoutMs, std::vector<Element> &outElements)
{
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(impl_->CheckAndSetInUse(), "Receive");
    Raii unsetRaii([this]() { impl_->UnsetInUse(); });
    TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
    Optional<uint32_t> expectedNumber(expectNum);
    return impl_->Receive(expectedNumber, timeoutMs, outElements);
}

Status Consumer::Receive(uint32_t timeoutMs, std::vector<Element> &outElements)
{
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(impl_->CheckAndSetInUse(), "Receive");
    Raii unsetRaii([this]() { impl_->UnsetInUse(); });
    TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
    Optional<uint32_t> expectedNumber;
    return impl_->Receive(expectedNumber, timeoutMs, outElements);
}

Status Consumer::Ack(uint64_t elementId)
{
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(impl_->CheckAndSetInUse(), "Ack");
    Raii unsetRaii([this]() { impl_->UnsetInUse(); });
    TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
    return impl_->Ack(elementId);
}

Status Consumer::Close()
{
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(impl_->CheckAndSetInUse(), "Close");
    Raii unsetRaii([this]() { impl_->UnsetInUse(); });
    TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
    AccessRecorder recorder(AccessRecorderKey::DS_STREAM_CLOSE_CONSUMER);
    auto rc = impl_->Close();
    StreamRequestParam reqParam;
    reqParam.streamName = impl_->GetStreamName();
    reqParam.consumerId = impl_->GetConsumerId();
    StreamResponseParam rspParam;
    rspParam.msg = rc.GetMsg();
    recorder.Record(rc.GetCode(), reqParam, rspParam);
    return rc;
}

Consumer::Consumer(std::unique_ptr<client::stream_cache::ConsumerImpl> impl) : impl_(std::move(impl))
{
}

void Consumer::GetStatisticsMessage(uint64_t &totalElements, uint64_t &notProcessedElements)
{
    impl_->GetStatisticsMessage(totalElements, notProcessedElements);
}
}  // namespace datasystem
