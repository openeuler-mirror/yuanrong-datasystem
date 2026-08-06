/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
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

#include "datasystem/common/rdma/npu/hixl_plugin_api.h"

#include <cstddef>
#include <type_traits>

#include <gtest/gtest.h>

namespace datasystem {
namespace ut {
namespace {

DsHixlResult CreateFakeEngine(DsHixlEngineHandle *engine)
{
    if (engine == nullptr) {
        return DS_HIXL_INVALID_ARGUMENT;
    }
    *engine = reinterpret_cast<DsHixlEngineHandle>(0x1);
    return DS_HIXL_OK;
}

TEST(HixlPluginApiTest, V1LayoutIsStable)
{
    static_assert(sizeof(DsHixlResult) == sizeof(int32_t), "result ABI changed");
    static_assert(std::is_standard_layout<DsHixlStringView>::value, "string view must be standard-layout");
    static_assert(std::is_standard_layout<DsHixlApi>::value, "function table must be standard-layout");
    EXPECT_EQ(sizeof(DsHixlStringView), 16U);
    EXPECT_EQ(sizeof(DsHixlOption), 32U);
    EXPECT_EQ(sizeof(DsHixlTransferDesc), 24U);
    EXPECT_EQ(sizeof(DsHixlRegisterMemoryRequest), 24U);
    EXPECT_EQ(sizeof(DsHixlTransferRequest), 40U);
    EXPECT_EQ(offsetof(DsHixlApi, create_engine), 8U);
    EXPECT_EQ(sizeof(DsHixlApi), 80U);
}

TEST(HixlPluginApiTest, ConsumerDispatchesThroughOpaqueFunctionTable)
{
    DsHixlApi api{};
    api.abiVersion = DS_HIXL_ABI_VERSION_1;
    api.structSize = sizeof(api);
    api.create_engine = &CreateFakeEngine;

    DsHixlEngineHandle engine = nullptr;
    ASSERT_EQ(api.create_engine(&engine), DS_HIXL_OK);
    EXPECT_NE(engine, nullptr);
    EXPECT_EQ(api.create_engine(nullptr), DS_HIXL_INVALID_ARGUMENT);
}

TEST(HixlPluginApiTest, StableResultAndOperationValuesDoNotOverlap)
{
    EXPECT_EQ(DS_HIXL_OK, 0);
    EXPECT_NE(DS_HIXL_INVALID_ARGUMENT, DS_HIXL_NOT_SUPPORTED);
    EXPECT_NE(DS_HIXL_NOT_SUPPORTED, DS_HIXL_RUNTIME_ERROR);
    EXPECT_NE(DS_HIXL_MEMORY_DEVICE, DS_HIXL_MEMORY_HOST);
    EXPECT_NE(DS_HIXL_TRANSFER_READ, DS_HIXL_TRANSFER_WRITE);
}

}  // namespace
}  // namespace ut
}  // namespace datasystem
