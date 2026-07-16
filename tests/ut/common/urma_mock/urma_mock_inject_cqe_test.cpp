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

#include "datasystem/common/urma_mock/inject/fault_inject.h"
#include "datasystem/common/urma_mock/objects/mock_jfc.h"

using namespace datasystem::urma_mock;

namespace {

class UrmaMockInjectCqeTest : public testing::Test {
protected:
    void TearDown() override
    {
        ResetMockInject();
    }
};

void ExpectInjectedStatus(uint32_t mode, urma_cr_status_t expected)
{
    MockJfc jfc(1, nullptr);
    MockCr cr;
    cr.status = URMA_SUCCESS;
    cr.byteCnt = 128;
    cr.localId = 7;
    SetMockInjectCqeError(mode);
    jfc.PushCr(cr);
    urma_cr_t out{};
    ASSERT_EQ(jfc.Poll(1, &out), 1);
    EXPECT_EQ(out.status, expected);
    EXPECT_EQ(out.byte_cnt, expected == URMA_CR_SUCCESS ? 128u : 0u);
    EXPECT_EQ(out.local_id, 7u);
}

}  // namespace

TEST_F(UrmaMockInjectCqeTest, ModeZeroPreservesSuccess)
{
    ExpectInjectedStatus(0, URMA_CR_SUCCESS);
}

TEST_F(UrmaMockInjectCqeTest, RemoteError)
{
    ExpectInjectedStatus(1, URMA_CR_REM_ACCESS_ABORT_ERR);
}

TEST_F(UrmaMockInjectCqeTest, LocalError)
{
    ExpectInjectedStatus(2, URMA_CR_LOC_ACCESS_ERR);
}

TEST_F(UrmaMockInjectCqeTest, TimeoutLikeGeneralError)
{
    ExpectInjectedStatus(3, URMA_CR_GENERAL_ERR);
}

TEST_F(UrmaMockInjectCqeTest, PermanentError)
{
    ExpectInjectedStatus(4, URMA_CR_WR_FLUSH_ERR_DONE);
}
