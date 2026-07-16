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

#include "datasystem/common/urma_mock/objects/mock_context.h"

#include <utility>

#include "datasystem/common/urma_mock/objects/mock_jfc.h"
#include "datasystem/common/urma_mock/objects/mock_jetty.h"
#include "datasystem/common/urma_mock/objects/mock_seg.h"
#include "datasystem/common/urma_mock/objects/mock_tjetty.h"

namespace datasystem {
namespace urma_mock {
MockContext::MockContext(uint64_t id, MockDevice *dev) : id_(id), dev_(dev)
{
}

MockContext::~MockContext() = default;

uint64_t MockContext::GetId() const
{
    return id_;
}

MockDevice *MockContext::GetDevice() const
{
    return dev_;
}

MockJfc *MockContext::FindJfc(uint64_t id) const
{
    auto it = jfcMap_.find(id);
    return it != jfcMap_.end() ? it->second.get() : nullptr;
}

void MockContext::RegisterJfc(std::shared_ptr<MockJfc> jfc)
{
    jfcMap_[jfc->GetId()] = std::move(jfc);
}

void MockContext::RegisterSeg(std::shared_ptr<MockSeg> seg)
{
    segMap_[seg->GetId()] = std::move(seg);
}

void MockContext::RegisterJetty(std::shared_ptr<MockJetty> jetty)
{
    jettyMap_[jetty->GetId()] = std::move(jetty);
}

void MockContext::RegisterTjetty(std::shared_ptr<MockTjetty> tjetty)
{
    tjettyMap_[tjetty->GetId()] = std::move(tjetty);
}

void MockContext::UnregisterJfc(uint64_t id)
{
    jfcMap_.erase(id);
}

void MockContext::UnregisterSeg(uint64_t id)
{
    segMap_.erase(id);
}

void MockContext::UnregisterJetty(uint64_t id)
{
    jettyMap_.erase(id);
}

void MockContext::UnregisterTjetty(uint64_t id)
{
    tjettyMap_.erase(id);
}

void MockContext::ForEachTjetty(const std::function<void(MockTjetty *)> &cb)
{
    for (auto &kv : tjettyMap_) {
        cb(kv.second.get());
    }
}

}  // namespace urma_mock
}  // namespace datasystem
