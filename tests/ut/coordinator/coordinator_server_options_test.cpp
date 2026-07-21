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
#include "datasystem/utils/status.h"
#define private public
#include "datasystem/coordinator_server.h"
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

TEST(CoordinatorServerOptionsTest, RejectsNullDiscoveryBeforeReadingConfig)
{
    CoordinatorOptions options;
    options.configFilePath = "missing-coordinator-config.json";
    options.expectedMemberCount = 1;

    auto status = CoordinatorServer::GetInstance()->InitAndRun(options);

    ExpectInvalidWithMessage(status, "coordinatorDiscovery");
}

TEST(CoordinatorServerOptionsTest, RejectsNonPositiveExpectedMemberCountBeforeReadingConfig)
{
    CoordinatorOptions options;
    options.configFilePath = "missing-coordinator-config.json";
    options.coordinatorDiscovery = std::make_shared<FakeCoordinatorDiscovery>();

    for (const int expectedMemberCount : { 0, -1 }) {
        options.expectedMemberCount = expectedMemberCount;
        auto status = CoordinatorServer::GetInstance()->InitAndRun(options);
        ExpectInvalidWithMessage(status, "expectedMemberCount");
    }
}

TEST(CoordinatorServerOptionsTest, RejectsUnpairedCallbacksBeforeReadingConfig)
{
    CoordinatorOptions options;
    options.configFilePath = "missing-coordinator-config.json";
    options.coordinatorDiscovery = std::make_shared<FakeCoordinatorDiscovery>();
    options.expectedMemberCount = 1;
    options.onStart = [] { return Status::OK(); };

    auto status = CoordinatorServer::GetInstance()->InitAndRun(options);

    ExpectInvalidWithMessage(status, "onStart and onStop");
}

TEST(CoordinatorServerOptionsTest, ThrownStartExceptionStillInvokesStopExactlyOnce)
{
    auto *server = CoordinatorServer::GetInstance();
    int stopCount = 0;
    server->onStart_ = []() -> Status { throw std::runtime_error("start threw"); };
    server->onStop_ = [&stopCount] {
        ++stopCount;
        return Status::OK();
    };
    server->callbackState_ = CoordinatorServer::LifecycleCallbackState::READY;

    Status startStatus;
    EXPECT_NO_THROW(startStatus = server->InvokeOnStart());
    auto firstStopStatus = server->InvokeOnStop();
    auto secondStopStatus = server->InvokeOnStop();
    auto shutdownStatus = server->Shutdown();

    EXPECT_EQ(startStatus.GetCode(), K_RUNTIME_ERROR);
    EXPECT_TRUE(firstStopStatus.IsOk()) << firstStopStatus.ToString();
    EXPECT_TRUE(secondStopStatus.IsOk()) << secondStopStatus.ToString();
    EXPECT_EQ(stopCount, 1);
    EXPECT_TRUE(shutdownStatus.IsOk()) << shutdownStatus.ToString();
}

TEST(CoordinatorServerOptionsTest, StartFailureStillInvokesStopExactlyOnce)
{
    auto *server = CoordinatorServer::GetInstance();
    int startCount = 0;
    int stopCount = 0;
    server->onStart_ = [&startCount] {
        ++startCount;
        return Status(K_RUNTIME_ERROR, "start failed");
    };
    server->onStop_ = [&stopCount] {
        ++stopCount;
        return Status::OK();
    };
    server->callbackState_ = CoordinatorServer::LifecycleCallbackState::READY;

    auto startStatus = server->InvokeOnStart();
    auto firstStopStatus = server->InvokeOnStop();
    auto secondStopStatus = server->InvokeOnStop();
    auto shutdownStatus = server->Shutdown();

    EXPECT_EQ(startStatus.GetCode(), K_RUNTIME_ERROR);
    EXPECT_TRUE(firstStopStatus.IsOk()) << firstStopStatus.ToString();
    EXPECT_TRUE(secondStopStatus.IsOk()) << secondStopStatus.ToString();
    EXPECT_EQ(startCount, 1);
    EXPECT_EQ(stopCount, 1);
    EXPECT_TRUE(shutdownStatus.IsOk()) << shutdownStatus.ToString();
}

}  // namespace ut
}  // namespace datasystem
