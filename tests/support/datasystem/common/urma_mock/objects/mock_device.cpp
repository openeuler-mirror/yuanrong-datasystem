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

#include "datasystem/common/urma_mock/objects/mock_device.h"

#include <utility>

#include "datasystem/common/urma_mock/objects/mock_context.h"

namespace datasystem {
namespace urma_mock {
urma_eid_t StableMockDeviceEid()
{
    urma_eid_t eid{};
    const uint8_t raw[URMA_EID_SIZE] = {
        0x64, 0x73, 0x2d, 0x75, 0x72, 0x6d, 0x61, 0x2d, 0x6d, 0x6f, 0x63, 0x6b, 0x2d, 0x65, 0x69, 0x64,
    };
    for (size_t i = 0; i < URMA_EID_SIZE; ++i) {
        eid.raw[i] = raw[i];
    }
    return eid;
}

MockDevice::MockDevice(uint64_t id, const std::string &name) : id_(id), name_(name)
{
}

MockDevice::~MockDevice() = default;

uint64_t MockDevice::GetId() const
{
    return id_;
}

const std::string &MockDevice::GetName() const
{
    return name_;
}

void MockDevice::SetPrivRawDev(urma_device_t *raw)
{
    privRawDev_ = raw;
}

urma_device_t *MockDevice::GetPrivRawDev() const
{
    return privRawDev_;
}

MockContext *MockDevice::FindContext(uint64_t id) const
{
    auto it = contextMap_.find(id);
    return it != contextMap_.end() ? it->second.get() : nullptr;
}

void MockDevice::RegisterContext(std::shared_ptr<MockContext> ctx)
{
    contextMap_[ctx->GetId()] = std::move(ctx);
}

void MockDevice::UnregisterContext(uint64_t id)
{
    contextMap_.erase(id);
}

void MockDevice::ForEachContext(const std::function<void(MockContext *)> &cb)
{
    for (auto &kv : contextMap_) {
        cb(kv.second.get());
    }
}

}  // namespace urma_mock
}  // namespace datasystem
