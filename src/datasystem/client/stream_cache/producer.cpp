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
 * Description: Define stream cache producer.
 */
#include "datasystem/stream/producer.h"

#include "datasystem/client/stream_cache/producer_impl.h"
#include "datasystem/common/log/access_recorder.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/log/trace.h"
#include "datasystem/common/rpc/rpc_constants.h"
#include "datasystem/common/util/strings_util.h"

namespace datasystem {
Producer::Producer(std::shared_ptr<client::stream_cache::ProducerImpl> impl) : impl_(std::move(impl))
{
}

Producer::~Producer()
{
    if (impl_->IsActive()) {
        LOG(INFO) << FormatString("[%s] Implicit close producer", impl_->LogPrefix());
        Status rc = Close();
        if (rc.IsError()) {
            LOG(ERROR) << FormatString("[%s] Implicit close producer failed %s.", impl_->LogPrefix(), rc.GetMsg());
        }
    }
}

Status Producer::Send(const Element &element)
{
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(impl_->CheckAndSetInUse(), "Send");
    Raii unsetRaii([this]() { impl_->UnsetInUse(); });
    return impl_->Send(element, Optional<int64_t>());
}

Status Producer::Send(const Element &element, int64_t timeoutMs)
{
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(impl_->CheckAndSetInUse(), "Send");
    Raii unsetRaii([this]() { impl_->UnsetInUse(); });
    return impl_->Send(element, Optional<int64_t>(timeoutMs));
}

Status Producer::Close()
{
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(impl_->CheckAndSetInUse(), "Close");
    Raii unsetRaii([this]() { impl_->UnsetInUse(); });
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    AccessRecorder recorder(AccessRecorderKey::DS_STREAM_CLOSE_PRODUCER);
    auto rc = impl_->Close();
    StreamRequestParam reqParam;
    reqParam.streamName = impl_->GetStreamName();
    reqParam.producerId = impl_->GetProducerId();
    StreamResponseParam rspParam;
    rspParam.msg = rc.GetMsg();
    recorder.Record(rc.GetCode(), reqParam, rspParam);
    return rc;
}
}  // namespace datasystem
