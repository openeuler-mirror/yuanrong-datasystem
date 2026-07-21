/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2024. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Description: Unit tests for centralized metadata endpoint resolver.
 */
#include "datasystem/worker/object_cache/central_metadata_address_resolver.h"

#include <gtest/gtest.h>

#include "datasystem/common/kvstore/coordination_keys.h"
#include "tests/ut/cluster/testing/fake_coordination_backend.h"
#include "tests/ut/common.h"

namespace datasystem {
namespace object_cache {

TEST(CentralMetadataAddressResolverTest, FirstWorkerClaimsLocalAddress)
{
    cluster::FakeCoordinationBackend backend;
    CentralMetadataAddressResolver resolver(backend);

    DS_ASSERT_OK(resolver.EnsureTable());
    std::string address;
    DS_ASSERT_OK(resolver.ClaimOrRead("127.0.0.1:18480", address));

    EXPECT_EQ(address, "127.0.0.1:18480");
    std::string storedAddress;
    DS_ASSERT_OK(backend.Get(COORDINATION_MASTER_ADDRESS_TABLE, COORDINATION_MASTER_ADDRESS_KEY, storedAddress));
    EXPECT_EQ(storedAddress, "127.0.0.1:18480");
}

TEST(CentralMetadataAddressResolverTest, ExistingWorkerReadsCommittedAddress)
{
    cluster::FakeCoordinationBackend backend;
    CentralMetadataAddressResolver resolver(backend);

    DS_ASSERT_OK(backend.Put(COORDINATION_MASTER_ADDRESS_TABLE, COORDINATION_MASTER_ADDRESS_KEY, "127.0.0.1:18480"));
    std::string address;
    DS_ASSERT_OK(resolver.ClaimOrRead("127.0.0.1:18481", address));

    EXPECT_EQ(address, "127.0.0.1:18480");
}

}  // namespace object_cache
}  // namespace datasystem
