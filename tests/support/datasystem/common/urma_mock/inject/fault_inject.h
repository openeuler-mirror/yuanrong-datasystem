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

#ifndef DATASYSTEM_COMMON_URMA_MOCK_INJECT_FAULT_INJECT_H
#define DATASYSTEM_COMMON_URMA_MOCK_INJECT_FAULT_INJECT_H

#include <cstdint>

#include "datasystem/common/urma_mock/abi/urma_abi_compat.h"

namespace datasystem {
namespace urma_mock {
/**
 * @brief Reset all mock error and delay injection state.
 */
void ResetMockInject();

/**
 * @brief Configure the CQE status returned by mock completion records.
 * @param[in] mode CQE injection mode.
 */
void SetMockInjectCqeError(uint32_t mode);

/**
 * @brief Get configured CQE injection mode.
 * @return CQE injection mode value.
 */
uint32_t GetMockInjectCqeErrorMode();

/**
 * @brief Configure handshake delay injection.
 * @param[in] delayMs Delay before handshake completes.
 * @param[in] timeoutMode Whether the delay should be treated as timeout-like behavior.
 */
void SetMockInjectHandshakeDelay(uint32_t delayMs, uint32_t timeoutMode);

/**
 * @brief Get configured handshake delay.
 * @return Delay in milliseconds.
 */
uint32_t GetMockInjectHandshakeDelayMs();

/**
 * @brief Get configured handshake timeout mode.
 * @return Timeout mode value.
 */
uint32_t GetMockInjectHandshakeTimeoutMode();

/**
 * @brief Configure async event injection.
 * @param[in] delayMs Delay before the event becomes visible.
 * @param[in] eventType Mock event type.
 */
void SetMockInjectEventFire(uint32_t delayMs, uint32_t eventType);

/**
 * @brief Configure the object pointer returned in injected async events.
 * @param[in] object Object pointer encoded as integer.
 */
void SetMockInjectEventObject(uintptr_t object);

/**
 * @brief Get configured async event delay.
 * @return Delay in milliseconds.
 */
uint32_t GetMockInjectEventDelayMs();

/**
 * @brief Get configured async event type.
 * @return Event type, or -1 when no event is configured.
 */
int GetMockInjectEventType();

/**
 * @brief Get the object pointer configured for injected async events.
 * @return Object pointer encoded as integer.
 */
uintptr_t GetMockInjectEventObject();

/**
 * @brief Convert the configured mock CQE injection mode to a URMA completion status.
 * @return Completion status used by mock CR generation.
 */
urma_cr_status_t GetMockInjectCrStatus();

/**
 * @brief Get the jetty object configured for injected async events.
 * @return URMA jetty handle, or nullptr when no jetty is configured.
 */
urma_jetty_t *GetMockInjectEventJetty();

}  // namespace urma_mock
}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_URMA_MOCK_INJECT_FAULT_INJECT_H
