// Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <vector>

#include "datasystem/utils/coordinator_discovery.h"
#include "datasystem/utils/embedded_config.h"
#include "datasystem/utils/status.h"
#define private public
#include "datasystem/data_worker.h"
#undef private
#include "ut/common.h"

namespace datasystem {
namespace ut {
namespace {

class FakeCoordinatorDiscovery final : public ICoordinatorDiscovery {
public:
    Status GetCoordinators(std::vector<std::string> &serviceList) override
    {
        serviceList = { "127.0.0.1:31501" };
        return Status::OK();
    }
};

void ExpectInvalidWithMessage(const Status &status, const std::string &message)
{
    EXPECT_EQ(status.GetCode(), K_INVALID);
    EXPECT_NE(status.ToString().find(message), std::string::npos) << status.ToString();
}

}  // namespace

TEST(DataWorkerOptionsTest, RejectsNullDiscoveryBeforeReadingConfig)
{
    DataWorkerOptions options;
    options.configFilePath = "missing-worker-config.json";

    auto status = DataWorker::GetInstance()->InitAndRun(options);

    ExpectInvalidWithMessage(status, "coordinatorDiscovery");
}

TEST(DataWorkerOptionsTest, RejectsUnpairedCallbacksBeforeReadingConfig)
{
    DataWorkerOptions options;
    options.configFilePath = "missing-worker-config.json";
    options.coordinatorDiscovery = std::make_shared<FakeCoordinatorDiscovery>();
    options.onStop = [] { return Status::OK(); };

    auto status = DataWorker::GetInstance()->InitAndRun(options);

    ExpectInvalidWithMessage(status, "onStart and onStop");
}

TEST(DataWorkerOptionsTest, ThrownStopExceptionDoesNotReplaceStartError)
{
    auto *worker = DataWorker::GetInstance();
    worker->onStop_ = []() -> Status { throw std::runtime_error("stop threw"); };
    worker->callbackState_ = DataWorker::LifecycleCallbackState::START_ATTEMPTED;

    Status status;
    EXPECT_NO_THROW(status = worker->FinishShutdown(Status(K_INVALID, "start failed")));
    EXPECT_EQ(status.GetCode(), K_INVALID);

    DS_ASSERT_OK(worker->FinishShutdown(Status::OK()));
}

TEST(DataWorkerOptionsTest, KeepsStartErrorWhenStopAlsoFails)
{
    auto *worker = DataWorker::GetInstance();
    int startCount = 0;
    int stopCount = 0;
    worker->onStart_ = [&startCount] {
        ++startCount;
        return Status(K_INVALID, "start failed");
    };
    worker->onStop_ = [&stopCount] {
        ++stopCount;
        return Status(K_RUNTIME_ERROR, "stop failed");
    };
    worker->callbackState_ = DataWorker::LifecycleCallbackState::READY;

    auto startStatus = worker->InvokeOnStart();
    auto status = worker->FinishShutdown(startStatus);

    EXPECT_EQ(status.GetCode(), K_INVALID);
    EXPECT_EQ(startCount, 1);
    EXPECT_EQ(stopCount, 1);
    DS_ASSERT_OK(worker->FinishShutdown(Status::OK()));
    EXPECT_EQ(stopCount, 1);
}

}  // namespace ut
}  // namespace datasystem
