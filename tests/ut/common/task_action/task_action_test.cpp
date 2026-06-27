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

#include "datasystem/common/task_action/task_action_registry.h"

#include <string>
#include <vector>

#include <gtest/gtest.h>

namespace datasystem {
namespace ut {
namespace {

class TaskActionTest : public testing::Test {
protected:
    void SetUp() override
    {
        RemoveTestSubscribers();
    }

    void TearDown() override
    {
        RemoveTestSubscribers();
    }

    TransferTask BuildTask(TransferTaskType type)
    {
        TransferTask task;
        task.type = type;
        task.localWorkerAddr = "127.0.0.1:12345";
        task.placementScope.ranges.emplace_back(1, 2);
        return task;
    }

    void RemoveTestSubscribers()
    {
        auto &registry = TaskActionRegistry::GetInstance();
        for (const auto type : types_) {
            for (const auto &name : subscriberNames_) {
                registry.RemoveSubscriber(type, name);
            }
        }
    }

    const std::vector<TransferTaskType> types_{
        TransferTaskType::MIGRATE_SCALE_UP_METADATA,
        TransferTaskType::CHECK_VOLUNTARY_READY,
        TransferTaskType::CLEANUP_DEVICE_CLIENT_META,
    };
    const std::vector<std::string> subscriberNames_{ "task_action_ut_first", "task_action_ut_second",
                                                     "task_action_ut_third" };
};

TEST_F(TaskActionTest, DispatchStopsOnFirstFailure)
{
    auto &registry = TaskActionRegistry::GetInstance();
    bool secondExecuted = false;
    registry.AddSubscriber(TransferTaskType::CHECK_VOLUNTARY_READY, "task_action_ut_first",
                           [](const TransferTask &) {
                               return Status(K_NOT_READY, "not ready");
                           });
    registry.AddSubscriber(TransferTaskType::CHECK_VOLUNTARY_READY, "task_action_ut_second",
                           [&secondExecuted](const TransferTask &) {
                               secondExecuted = true;
                               return Status::OK();
                           });

    auto rc = registry.Dispatch(BuildTask(TransferTaskType::CHECK_VOLUNTARY_READY));
    ASSERT_EQ(rc.GetCode(), K_NOT_READY);
    ASSERT_FALSE(secondExecuted);
    ASSERT_NE(rc.ToString().find("task_action_ut_first"), std::string::npos);
}

TEST_F(TaskActionTest, DispatchPropagatesFailureCodes)
{
    auto &registry = TaskActionRegistry::GetInstance();
    const std::vector<StatusCode> retryableCodes{ K_TRY_AGAIN, K_RPC_UNAVAILABLE, K_INVALID };
    for (auto code : retryableCodes) {
        registry.AddSubscriber(TransferTaskType::CHECK_VOLUNTARY_READY, "task_action_ut_first",
                               [code](const TransferTask &) {
                                   return Status(code, "expected failure");
                               });

        auto rc = registry.Dispatch(BuildTask(TransferTaskType::CHECK_VOLUNTARY_READY));
        ASSERT_EQ(rc.GetCode(), code);
        ASSERT_NE(rc.ToString().find("task_action_ut_first"), std::string::npos);
    }
}

TEST_F(TaskActionTest, RemoveSubscriberSkipsCallback)
{
    auto &registry = TaskActionRegistry::GetInstance();
    registry.AddSubscriber(TransferTaskType::CHECK_VOLUNTARY_READY, "task_action_ut_first",
                           [](const TransferTask &) {
                               return Status::OK();
                           });
    registry.RemoveSubscriber(TransferTaskType::CHECK_VOLUNTARY_READY, "task_action_ut_first");

    auto rc = registry.Dispatch(BuildTask(TransferTaskType::CHECK_VOLUNTARY_READY));
    ASSERT_EQ(rc.GetCode(), K_NOT_READY);
}

TEST_F(TaskActionTest, AddSubscriberReplacesSameName)
{
    auto &registry = TaskActionRegistry::GetInstance();
    std::vector<std::string> executionOrder;
    registry.AddSubscriber(TransferTaskType::MIGRATE_SCALE_UP_METADATA, "task_action_ut_first",
                           [&executionOrder](const TransferTask &) {
                               executionOrder.emplace_back("old");
                               return Status::OK();
                           });
    registry.AddSubscriber(TransferTaskType::MIGRATE_SCALE_UP_METADATA, "task_action_ut_first",
                           [&executionOrder](const TransferTask &) {
                               executionOrder.emplace_back("new");
                               return Status::OK();
                           });

    ASSERT_TRUE(registry.Dispatch(BuildTask(TransferTaskType::MIGRATE_SCALE_UP_METADATA)).IsOk());
    ASSERT_EQ(executionOrder, (std::vector<std::string>{ "new" }));
}

TEST_F(TaskActionTest, DispatchExecutesOutsideRegistryLock)
{
    auto &registry = TaskActionRegistry::GetInstance();
    bool registeredDuringDispatch = false;
    registry.AddSubscriber(TransferTaskType::MIGRATE_SCALE_UP_METADATA, "task_action_ut_first",
                           [&registry, &registeredDuringDispatch](const TransferTask &) {
                               registry.AddSubscriber(TransferTaskType::CHECK_VOLUNTARY_READY,
                                                      "task_action_ut_second",
                                                      [](const TransferTask &) {
                                                          return Status::OK();
                                                      });
                               registeredDuringDispatch = true;
                               return Status::OK();
                           });

    ASSERT_TRUE(registry.Dispatch(BuildTask(TransferTaskType::MIGRATE_SCALE_UP_METADATA)).IsOk());
    ASSERT_TRUE(registeredDuringDispatch);
}

}  // namespace
}  // namespace ut
}  // namespace datasystem
