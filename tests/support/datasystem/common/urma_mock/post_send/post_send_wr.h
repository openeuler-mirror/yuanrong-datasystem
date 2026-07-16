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

#ifndef DATASYSTEM_COMMON_URMA_MOCK_POST_SEND_POST_SEND_WR_H
#define DATASYSTEM_COMMON_URMA_MOCK_POST_SEND_POST_SEND_WR_H

#include "datasystem/common/urma_mock/abi/urma_abi_compat.h"
#include "datasystem/common/urma_mock/objects/mock_jetty.h"

namespace datasystem {
namespace urma_mock {

/**
 * @brief Simulate URMA post-send by copying SGEs and posting a completion record.
 * The function resolves the target jetty, segment, and receive JFC from side tables. It follows the lock order defined
 * in mock_registry.h and never submits worker tasks while holding registry locks.
 *
 * @param[in] jetty Source jetty that submits the work request.
 * @param[in] wr Work request to execute.
 * @param[out] badWr First invalid work request on failure.
 * @return URMA_SUCCESS on success; otherwise a mock URMA error code.
 */
urma_status_t MockPostSendWr(MockJetty *jetty, const urma_jfs_wr_t *wr, urma_jfs_wr_t **badWr);

}  // namespace urma_mock
}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_URMA_MOCK_POST_SEND_POST_SEND_WR_H
