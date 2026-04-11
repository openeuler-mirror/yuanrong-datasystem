/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2022. All rights reserved.
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

#include "datasystem/common/util/fd_manager.h"
#include "ut/common.h"

namespace datasystem {
namespace ut {
class FdManagerTest : public CommonTest {};

TEST_F(FdManagerTest, TestFdManager)
{
    DS_ASSERT_OK(FdManager::Instance()->AddFd(0));
    ASSERT_EQ(FdManager::Instance()->AddFd(-1).GetCode(), StatusCode::K_RUNTIME_ERROR);    // invalid input
    ASSERT_EQ(FdManager::Instance()->AddFd(0).GetCode(), StatusCode::K_RUNTIME_ERROR);     // Fd exists
    ASSERT_EQ(FdManager::Instance()->CloseFd(-1).GetCode(), StatusCode::K_RUNTIME_ERROR);  // invalid input
    DS_ASSERT_OK(FdManager::Instance()->CloseFd(0));
    ASSERT_EQ(FdManager::Instance()->CloseFd(0).GetCode(), StatusCode::K_RUNTIME_ERROR);  // Fd not exist
}

}  // namespace ut
}  // namespace datasystem
