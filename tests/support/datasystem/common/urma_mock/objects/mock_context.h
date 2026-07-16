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

#ifndef DATASYSTEM_COMMON_URMA_MOCK_OBJECTS_MOCK_CONTEXT_H
#define DATASYSTEM_COMMON_URMA_MOCK_OBJECTS_MOCK_CONTEXT_H

#include "datasystem/common/urma_mock/objects/mock_jfc.h"
#include "datasystem/common/urma_mock/objects/mock_jetty.h"
#include "datasystem/common/urma_mock/objects/mock_seg.h"
#include "datasystem/common/urma_mock/objects/mock_tjetty.h"

#include <cstdint>
#include <functional>
#include <memory>
#include <unordered_map>

namespace datasystem {
namespace urma_mock {
class MockDevice;

/**
 * @brief Mock URMA context that owns JFC, segment, jetty, and target jetty objects.
 */
class MockContext {
public:
    /**
     * @brief Create a mock context owned by a mock device.
     * @param[in] id Context id.
     * @param[in] dev Owning mock device.
     */
    MockContext(uint64_t id, MockDevice *dev);
    ~MockContext();

    /**
     * @brief Get the context id.
     * @return Context id.
     */
    uint64_t GetId() const;

    /**
     * @brief Get the owning mock device.
     * @return Owning mock device.
     */
    MockDevice *GetDevice() const;

    /**
     * @brief Find a completion queue owned by this context.
     * @param[in] id JFC id.
     * @return Mock JFC, or nullptr if no queue matches.
     */
    MockJfc *FindJfc(uint64_t id) const;

    /**
     * @brief Register objects owned by this context.
     * @param[in] jfc Mock completion queue to register.
     */
    void RegisterJfc(std::shared_ptr<MockJfc> jfc);

    /**
     * @brief Register a segment owned by this context.
     * @param[in] seg Mock segment to register.
     */
    void RegisterSeg(std::shared_ptr<MockSeg> seg);

    /**
     * @brief Register a jetty owned by this context.
     * @param[in] jetty Mock jetty to register.
     */
    void RegisterJetty(std::shared_ptr<MockJetty> jetty);

    /**
     * @brief Register a target jetty imported by this context.
     * @param[in] tjetty Mock target jetty to register.
     */
    void RegisterTjetty(std::shared_ptr<MockTjetty> tjetty);

    /**
     * @brief Visit each registered target jetty.
     * @param[in] cb Callback invoked for each target jetty.
     */
    void ForEachTjetty(const std::function<void(MockTjetty *)> &cb);

    /**
     * @brief Remove a JFC from this context.
     * @param[in] id JFC id.
     */
    void UnregisterJfc(uint64_t id);

    /**
     * @brief Remove a segment from this context.
     * @param[in] id Segment id.
     */
    void UnregisterSeg(uint64_t id);

    /**
     * @brief Remove a jetty from this context.
     * @param[in] id Jetty id.
     */
    void UnregisterJetty(uint64_t id);

    /**
     * @brief Remove a target jetty from this context.
     * @param[in] id Target jetty id.
     */
    void UnregisterTjetty(uint64_t id);

private:
    // Access to these maps must hold MockUrmaBackend::mu_ first and then
    // SideTables::mu. Do not call ds_urma_mock_* while holding either lock.
    uint64_t id_;
    MockDevice *dev_;
    std::unordered_map<uint64_t, std::shared_ptr<MockJfc>> jfcMap_;
    std::unordered_map<uint64_t, std::shared_ptr<MockSeg>> segMap_;
    std::unordered_map<uint64_t, std::shared_ptr<MockJetty>> jettyMap_;
    std::unordered_map<uint64_t, std::shared_ptr<MockTjetty>> tjettyMap_;
};

}  // namespace urma_mock
}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_URMA_MOCK_OBJECTS_MOCK_CONTEXT_H
