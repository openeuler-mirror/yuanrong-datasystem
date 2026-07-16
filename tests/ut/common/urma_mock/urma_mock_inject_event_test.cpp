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

#include <gtest/gtest.h>

#include <chrono>

#include "datasystem/common/urma_mock/abi/mock_abi.h"
#include "datasystem/common/urma_mock/inject/fault_inject.h"
#include "datasystem/common/urma_mock/urma_mock_backend.h"

using namespace datasystem::urma_mock;

namespace {

class UrmaMockInjectEventTest : public testing::Test {
protected:
    void TearDown() override
    {
        ResetMockInject();
    }
};

}  // namespace

TEST_F(UrmaMockInjectEventTest, NoInjectionReturnsNoEvent)
{
    urma_async_event_t event{};
    ASSERT_EQ(MockUrmaBackend::Instance().GetAsyncEvent(&event), URMA_SUCCESS);
    EXPECT_EQ(event.event_type, 0);
}

TEST_F(UrmaMockInjectEventTest, FiresInjectedEventAfterDelay)
{
    SetMockInjectEventFire(30, URMA_EVENT_JFC_ERR);
    urma_async_event_t event{};
    auto start = std::chrono::steady_clock::now();
    ASSERT_EQ(MockUrmaBackend::Instance().GetAsyncEvent(&event), URMA_SUCCESS);
    auto elapsedMs =
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start).count();
    EXPECT_GE(elapsedMs, 25);
    EXPECT_EQ(event.event_type, URMA_EVENT_JFC_ERR);
}

TEST_F(UrmaMockInjectEventTest, AbiPathReturnsInjectedEvent)
{
    SetMockInjectEventFire(0, URMA_EVENT_JETTY_ERR);
    urma_async_event_t event{};
    ASSERT_EQ(ds_urma_mock_get_async_event(nullptr, &event), URMA_SUCCESS);
    EXPECT_EQ(event.event_type, URMA_EVENT_JETTY_ERR);
}
