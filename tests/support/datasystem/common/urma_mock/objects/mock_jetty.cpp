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

#include "datasystem/common/urma_mock/objects/mock_jetty.h"

#include <atomic>

namespace datasystem {
namespace urma_mock {

MockJetty::MockJetty(uint64_t id, MockContext *ctx, MockJfc *sendJfc) : id_(id), ctx_(ctx), sendJfc_(sendJfc)
{
}

uint64_t MockJetty::GetId() const
{
    return id_;
}

MockContext *MockJetty::GetContext() const
{
    return ctx_;
}

MockJfc *MockJetty::GetSendJfc() const
{
    return sendJfc_;
}

void MockJetty::SetDstTjetty(MockTjetty *t)
{
    dstTjetty_.store(t, std::memory_order_release);
}

MockTjetty *MockJetty::GetDstTjetty() const
{
    return dstTjetty_.load(std::memory_order_acquire);
}

}  // namespace urma_mock
}  // namespace datasystem
