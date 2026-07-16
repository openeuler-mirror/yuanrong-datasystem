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

#include "datasystem/common/urma_mock/abi/urma_abi_compat.h"
#include "datasystem/common/rdma/urma_dlopen_util.h"

namespace datasystem {
namespace common {
namespace rdma {
namespace {

#ifdef USE_URMA_MOCK

TEST(UrmaMockDispatchTest, InitSucceedsAndIsAvailableReturnsTrue)
{
    // Cleanup first in case other tests left it initialized.
    datasystem::urma_dlopen::Cleanup();
    EXPECT_TRUE(datasystem::urma_dlopen::Init());
    EXPECT_TRUE(datasystem::urma_dlopen::IsAvailable());
    datasystem::urma_dlopen::Cleanup();
    EXPECT_FALSE(datasystem::urma_dlopen::IsAvailable());
}

TEST(UrmaMockDispatchTest, DsUrmaInitReturnsSuccess)
{
    datasystem::urma_dlopen::Cleanup();
    ASSERT_TRUE(datasystem::urma_dlopen::Init());
    urma_init_attr_t attr{};
    EXPECT_EQ(ds_urma_init(&attr), URMA_SUCCESS);
    EXPECT_EQ(ds_urma_uninit(), URMA_SUCCESS);
    datasystem::urma_dlopen::Cleanup();
}

TEST(UrmaMockDispatchTest, DsUrmaDeviceListReturnsMockDevice)
{
    datasystem::urma_dlopen::Cleanup();
    ASSERT_TRUE(datasystem::urma_dlopen::Init());
    int devNum = -1;
    urma_device_t **list = ds_urma_get_device_list(&devNum);
    ASSERT_NE(list, nullptr);
    ASSERT_EQ(devNum, 1);
    ASSERT_NE(list[0], nullptr);
    EXPECT_STREQ(reinterpret_cast<const char *>(list[0]->name), "bonding_mock0");
    EXPECT_EQ(list[1], nullptr);
    delete[] list;
    datasystem::urma_dlopen::Cleanup();
}

TEST(UrmaMockDispatchTest, DsUrmaCreateContextReturnsNull)
{
    datasystem::urma_dlopen::Cleanup();
    ASSERT_TRUE(datasystem::urma_dlopen::Init());
    EXPECT_EQ(ds_urma_create_context(nullptr, 0), nullptr);
    datasystem::urma_dlopen::Cleanup();
}

TEST(UrmaMockDispatchTest, DsUrmaImportSegReturnsNull)
{
    datasystem::urma_dlopen::Cleanup();
    ASSERT_TRUE(datasystem::urma_dlopen::Init());
    urma_import_seg_flag_t flag{};
    flag.value = URMA_IMPORT_SEG_FLAG_NONE;
    EXPECT_EQ(ds_urma_import_seg(nullptr, nullptr, nullptr, 0, flag), nullptr);
    datasystem::urma_dlopen::Cleanup();
}

#else  // USE_URMA_MOCK

TEST(UrmaMockDispatchTest, DisabledWhenUseUrmaMockOff)
{
    GTEST_SKIP() << "URMA mock backend not compiled in this build (USE_URMA_MOCK undefined)";
}

#endif  // USE_URMA_MOCK

}  // namespace
}  // namespace rdma
}  // namespace common
}  // namespace datasystem
