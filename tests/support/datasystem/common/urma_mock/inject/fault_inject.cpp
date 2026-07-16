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

#include "datasystem/common/urma_mock/inject/fault_inject.h"

#include <atomic>

#include "datasystem/common/inject/inject_point.h"

namespace datasystem {
namespace urma_mock {
namespace {

constexpr uint32_t K_EVENT_NONE = 0;
constexpr uint32_t K_REMOTE_ACCESS_ABORT_MODE = 1;
constexpr uint32_t K_LOCAL_ACCESS_ERROR_MODE = 2;
constexpr uint32_t K_GENERAL_ERROR_MODE = 3;
constexpr uint32_t K_WR_FLUSH_DONE_MODE = 4;

std::atomic<uint32_t> g_cqeErrorMode{ 0 };
std::atomic<uint32_t> g_handshakeDelayMs{ 0 };
std::atomic<uint32_t> g_handshakeTimeoutMode{ 0 };
std::atomic<uint32_t> g_eventDelayMs{ 0 };
std::atomic<uint32_t> g_eventType{ K_EVENT_NONE };
std::atomic<uintptr_t> g_eventObject{ 0 };

}  // namespace

void ResetMockInject()
{
    g_cqeErrorMode.store(0, std::memory_order_seq_cst);
    g_handshakeDelayMs.store(0, std::memory_order_seq_cst);
    g_handshakeTimeoutMode.store(0, std::memory_order_seq_cst);
    g_eventDelayMs.store(0, std::memory_order_seq_cst);
    g_eventType.store(K_EVENT_NONE, std::memory_order_seq_cst);
    g_eventObject.store(0, std::memory_order_seq_cst);
}

void SetMockInjectCqeError(uint32_t mode)
{
    g_cqeErrorMode.store(mode, std::memory_order_seq_cst);
}

uint32_t GetMockInjectCqeErrorMode()
{
    return g_cqeErrorMode.load(std::memory_order_seq_cst);
}

void SetMockInjectHandshakeDelay(uint32_t delayMs, uint32_t timeoutMode)
{
    g_handshakeDelayMs.store(delayMs, std::memory_order_seq_cst);
    g_handshakeTimeoutMode.store(timeoutMode, std::memory_order_seq_cst);
}

uint32_t GetMockInjectHandshakeDelayMs()
{
    return g_handshakeDelayMs.load(std::memory_order_seq_cst);
}

uint32_t GetMockInjectHandshakeTimeoutMode()
{
    return g_handshakeTimeoutMode.load(std::memory_order_seq_cst);
}

void SetMockInjectEventFire(uint32_t delayMs, uint32_t eventType)
{
    g_eventDelayMs.store(delayMs, std::memory_order_seq_cst);
    g_eventType.store(eventType, std::memory_order_seq_cst);
}

void SetMockInjectEventObject(uintptr_t object)
{
    g_eventObject.store(object, std::memory_order_seq_cst);
}

uint32_t GetMockInjectEventDelayMs()
{
    return g_eventDelayMs.load(std::memory_order_seq_cst);
}

int GetMockInjectEventType()
{
    return static_cast<int>(g_eventType.load(std::memory_order_seq_cst));
}

uintptr_t GetMockInjectEventObject()
{
    return g_eventObject.load(std::memory_order_seq_cst);
}

urma_cr_status_t GetMockInjectCrStatus()
{
    urma_cr_status_t status = URMA_CR_SUCCESS;
    switch (GetMockInjectCqeErrorMode()) {
        case K_REMOTE_ACCESS_ABORT_MODE:
            status = URMA_CR_REM_ACCESS_ABORT_ERR;
            break;
        case K_LOCAL_ACCESS_ERROR_MODE:
            status = URMA_CR_LOC_ACCESS_ERR;
            break;
        case K_GENERAL_ERROR_MODE:
            status = URMA_CR_GENERAL_ERR;
            break;
        case K_WR_FLUSH_DONE_MODE:
            status = URMA_CR_WR_FLUSH_ERR_DONE;
            break;
        default:
            break;
    }
#ifdef WITH_TESTS
    INJECT_POINT_NO_RETURN("UrmaManager.CheckCompletionRecordStatus", [&status](int64_t index, int64_t injectedStatus) {
        if (index == 0 && status != URMA_CR_WR_FLUSH_ERR_DONE) {
            status = static_cast<urma_cr_status_t>(injectedStatus);
        }
    });
    INJECT_POINT_NO_RETURN("UrmaMock.CheckCompletionRecordStatus", [&status](int64_t index, int64_t injectedStatus) {
        if (index == 0 && status != URMA_CR_WR_FLUSH_ERR_DONE) {
            status = static_cast<urma_cr_status_t>(injectedStatus);
        }
    });
#endif
    return status;
}

urma_jetty_t *GetMockInjectEventJetty()
{
    return reinterpret_cast<urma_jetty_t *>(GetMockInjectEventObject());
}

}  // namespace urma_mock
}  // namespace datasystem
