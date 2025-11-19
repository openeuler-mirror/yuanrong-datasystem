/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
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

#include <unistd.h>
#include <cstdint>
#include <list>
#include <memory>
#include <string>
#include <unordered_map>

#include "common.h"
#include "datasystem/master/object_cache/device/master_dev_dead_lock_manager.h"
#include "gtest/gtest.h"

using namespace datasystem::master;
namespace datasystem {
namespace ut {

class MasterDevDeadLockManagerTest : public CommonTest {
public:
    void SetUp()
    {
        masterDevDeadLockManager_ = std::make_shared<MasterDevDeadLockManager>();
    }

    void TearDown()
    {
        masterDevDeadLockManager_.reset();
    }

    std::shared_ptr<MasterDevDeadLockManager> masterDevDeadLockManager_{ nullptr };
};

TEST_F(MasterDevDeadLockManagerTest, TestIsExistDeadlock_Ring)
{
    masterDevDeadLockManager_->AddDependencyEdge("client1", "client2");
    masterDevDeadLockManager_->AddDependencyEdge("client2", "client3");
    masterDevDeadLockManager_->AddDependencyEdge("client1", "client3");
    ASSERT_TRUE(masterDevDeadLockManager_->IsExistDeadlock());

    masterDevDeadLockManager_->RemoveDependencyEdge("client1", "client3");
    ASSERT_FALSE(masterDevDeadLockManager_->IsExistDeadlock());
}

TEST_F(MasterDevDeadLockManagerTest, TestIsExistDeadlock_MutualDependence)
{
    masterDevDeadLockManager_->AddDependencyEdge("client1", "client2");
    ASSERT_FALSE(masterDevDeadLockManager_->IsExistDeadlock());

    masterDevDeadLockManager_->AddDependencyEdge("client2", "client1");
    ASSERT_TRUE(masterDevDeadLockManager_->IsExistDeadlock());

    masterDevDeadLockManager_->RemoveDependencyEdge("client2", "client1");
    ASSERT_FALSE(masterDevDeadLockManager_->IsExistDeadlock());
}
}  // namespace ut
}  // namespace datasystem
