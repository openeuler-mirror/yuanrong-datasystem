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

#ifndef DATASYSTEM_COMMON_URMA_MOCK_OBJECTS_MOCK_JETTY_H
#define DATASYSTEM_COMMON_URMA_MOCK_OBJECTS_MOCK_JETTY_H

#include "datasystem/common/urma_mock/objects/mock_jfc.h"
#include "datasystem/common/urma_mock/objects/mock_tjetty.h"

#include <atomic>
#include <cstdint>

namespace datasystem {
namespace urma_mock {
class MockContext;

/**
 * @brief Mock URMA jetty used as the local endpoint for post-send operations.
 */
class MockJetty {
public:
    /**
     * @brief Create a local mock jetty.
     * @param[in] id Jetty id.
     * @param[in] ctx Owning context.
     * @param[in] sendJfc Send completion queue.
     */
    MockJetty(uint64_t id, MockContext *ctx, MockJfc *sendJfc);
    ~MockJetty() = default;

    /**
     * @brief Get the jetty id.
     * @return Jetty id.
     */
    uint64_t GetId() const;

    /**
     * @brief Get the owning context.
     * @return Owning context.
     */
    MockContext *GetContext() const;

    /**
     * @brief Get the send completion queue.
     * @return Send JFC.
     */
    MockJfc *GetSendJfc() const;

    /**
     * @brief Set the imported target jetty used by subsequent post-send requests.
     * @param[in] t Target jetty.
     */
    void SetDstTjetty(MockTjetty *t);

    /**
     * @brief Get the imported target jetty.
     * @return Target jetty, or nullptr if it has not been assigned.
     */
    MockTjetty *GetDstTjetty() const;

private:
    uint64_t id_;
    MockContext *ctx_;
    MockJfc *sendJfc_;
    std::atomic<MockTjetty *> dstTjetty_{ nullptr };
};

}  // namespace urma_mock
}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_URMA_MOCK_OBJECTS_MOCK_JETTY_H
