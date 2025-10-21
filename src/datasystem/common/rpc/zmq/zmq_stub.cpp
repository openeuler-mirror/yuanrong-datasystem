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
 * Description: Zmq Stub.
 */
#include "datasystem/common/rpc/zmq/zmq_stub.h"

#include "datasystem/common/rpc/zmq/zmq_stub_impl.h"

namespace datasystem {

ZmqStub::ZmqStub(const std::shared_ptr<RpcChannel> &channel, int32_t timeoutMs)
    : channelNo_(0), pimpl_(std::make_unique<ZmqStubImpl>(channel, timeoutMs))
{
}

ZmqStub::~ZmqStub()
{
    pimpl_->CleanUp();
}

void ZmqStub::ForgetRequest(int64_t tag)
{
    pimpl_->ForgetRequest(tag);
}

bool ZmqStub::IsPeerAlive(uint32_t threshold)
{
    return pimpl_->IsPeerAlive(threshold);
}

void ZmqStub::CacheSession(bool cache)
{
    pimpl_->CacheSession(cache);
}

Status ZmqStub::InitConn()
{
    return pimpl_->InitConn(this);
}

Status ZmqStub::GetInitStatus()
{
    return pimpl_->GetInitStatus();
}
}  // namespace datasystem
