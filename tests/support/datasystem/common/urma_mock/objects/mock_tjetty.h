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

#ifndef DATASYSTEM_COMMON_URMA_MOCK_OBJECTS_MOCK_TJETTY_H
#define DATASYSTEM_COMMON_URMA_MOCK_OBJECTS_MOCK_TJETTY_H

#include "datasystem/common/urma_mock/objects/mock_jfc.h"
#include "datasystem/common/urma_mock/objects/mock_seg.h"

#include <cstdint>
#include <string>

namespace datasystem {
namespace urma_mock {
class MockContext;

/**
 * @brief Mock imported target jetty used as the destination endpoint for post-send operations.
 */
class MockTjetty {
public:
    /**
     * @brief Create an imported target jetty.
     * @param[in] id Target jetty id.
     * @param[in] ctx Owning context.
     * @param[in] remoteSeg Segment associated with the remote peer.
     * @param[in] remoteRecvJfc Remote receive completion queue.
     * @param[in] token Import token string.
     */
    MockTjetty(uint64_t id, MockContext *ctx, MockSeg *remoteSeg, MockJfc *remoteRecvJfc, const std::string &token);
    ~MockTjetty();

    /**
     * @brief Get the target jetty id.
     * @return Target jetty id.
     */
    uint64_t GetId() const;

    /**
     * @brief Get the owning context.
     * @return Owning context.
     */
    MockContext *GetContext() const;

    /**
     * @brief Get the remote segment associated with the target jetty.
     * @return Remote segment.
     */
    MockSeg *GetRemoteSeg() const;

    /**
     * @brief Get the remote receive completion queue.
     * @return Remote receive JFC.
     */
    MockJfc *GetRemoteRecvJfc() const;

    /**
     * @brief Get the import token associated with this target jetty.
     * @return Import token string.
     */
    const std::string &GetToken() const;

private:
    uint64_t id_;
    MockContext *ctx_;
    MockSeg *remoteSeg_;
    MockJfc *remoteRecvJfc_;
    std::string token_;
};

}  // namespace urma_mock
}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_URMA_MOCK_OBJECTS_MOCK_TJETTY_H
