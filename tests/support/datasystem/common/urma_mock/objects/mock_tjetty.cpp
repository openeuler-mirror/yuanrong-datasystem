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

#include "datasystem/common/urma_mock/objects/mock_tjetty.h"

namespace datasystem {
namespace urma_mock {

MockTjetty::MockTjetty(uint64_t id, MockContext *ctx, MockSeg *remoteSeg, MockJfc *remoteRecvJfc,
                       const std::string &token)
    : id_(id), ctx_(ctx), remoteSeg_(remoteSeg), remoteRecvJfc_(remoteRecvJfc), token_(token)
{
}

MockTjetty::~MockTjetty() = default;

uint64_t MockTjetty::GetId() const
{
    return id_;
}

MockContext *MockTjetty::GetContext() const
{
    return ctx_;
}

MockSeg *MockTjetty::GetRemoteSeg() const
{
    return remoteSeg_;
}

MockJfc *MockTjetty::GetRemoteRecvJfc() const
{
    return remoteRecvJfc_;
}

const std::string &MockTjetty::GetToken() const
{
    return token_;
}

}  // namespace urma_mock
}  // namespace datasystem
