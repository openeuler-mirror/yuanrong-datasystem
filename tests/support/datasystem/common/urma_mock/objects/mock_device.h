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

#ifndef DATASYSTEM_COMMON_URMA_MOCK_OBJECTS_MOCK_DEVICE_H
#define DATASYSTEM_COMMON_URMA_MOCK_OBJECTS_MOCK_DEVICE_H

#include "datasystem/common/urma_mock/abi/urma_abi_compat.h"
#include "datasystem/common/urma_mock/objects/mock_context.h"

#include <functional>
#include <memory>
#include <string>
#include <unordered_map>

namespace datasystem {
namespace urma_mock {

/**
 * @brief Get the deterministic EID exposed by the mock device.
 * @return Stable mock device EID.
 */
urma_eid_t StableMockDeviceEid();

/**
 * @brief Per-device mock URMA state.
 * The current mock backend creates one device with a bonding prefix so the normal URMA device selection path can use
 * it.
 */
class MockDevice {
public:
    /**
     * @brief Create a mock URMA device.
     * @param[in] id Device id.
     * @param[in] name Device name reported to URMA selection code.
     */
    MockDevice(uint64_t id, const std::string &name);
    ~MockDevice();

    /**
     * @brief Get the device id.
     * @return Device id.
     */
    uint64_t GetId() const;

    /**
     * @brief Get the device name.
     * @return Device name.
     */
    const std::string &GetName() const;

    /**
     * @brief Store the ABI device handle associated with this mock device.
     * @param[in] raw ABI device handle.
     */
    void SetPrivRawDev(urma_device_t *raw);

    /**
     * @brief Get the ABI device handle associated with this mock device.
     * @return ABI device handle.
     */
    urma_device_t *GetPrivRawDev() const;

    /**
     * @brief Find a context owned by this device.
     * @param[in] id Context id.
     * @return Mock context, or nullptr if no context matches.
     */
    MockContext *FindContext(uint64_t id) const;

    /**
     * @brief Visit all contexts before they are removed.
     * @param[in] cb Callback invoked for each context.
     */
    void ForEachContext(const std::function<void(MockContext *)> &cb);

    /**
     * @brief Register a context owned by this device.
     * @param[in] ctx Context to register.
     */
    void RegisterContext(std::shared_ptr<MockContext> ctx);

    /**
     * @brief Remove a context from this device.
     * @param[in] id Context id.
     */
    void UnregisterContext(uint64_t id);

private:
    uint64_t id_;
    std::string name_;
    urma_device_t *privRawDev_ = nullptr;
    std::unordered_map<uint64_t, std::shared_ptr<MockContext>> contextMap_;
};

}  // namespace urma_mock
}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_URMA_MOCK_OBJECTS_MOCK_DEVICE_H
