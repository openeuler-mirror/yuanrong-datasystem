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

#include <memory>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "datasystem/common/coordinator/coordinator_service_proxy.h"

#include "datasystem/utils/coordinator_discovery.h"
#include "datasystem/utils/status.h"
#include "ut/common.h"

namespace datasystem {
namespace ut {
namespace {

using CoordinatorDiscoveryPtr = std::shared_ptr<ICoordinatorDiscovery>;

static_assert(std::is_constructible_v<CoordinatorServiceProxyZmqImpl, CoordinatorDiscoveryPtr>
              && std::is_constructible_v<CoordinatorServiceProxyBrpcImpl, CoordinatorDiscoveryPtr>);

constexpr char ADDRESS_A[] = "127.0.0.1:31501";
constexpr char ADDRESS_B[] = "127.0.0.1:31502";
constexpr char ADDRESS_C[] = "127.0.0.1:31503";
constexpr char RANGE_KEY[] = "/range";

struct DiscoveryReply {
    Status status;
    std::vector<std::string> coordinators;
    bool throws{ false };
    std::string exceptionMessage;
};

class ScriptedCoordinatorDiscovery final : public ICoordinatorDiscovery {
public:
    explicit ScriptedCoordinatorDiscovery(std::vector<DiscoveryReply> replies) : replies_(std::move(replies))
    {
    }

    Status GetCoordinators(std::vector<std::string> &serviceList) override
    {
        ++callCount_;
        if (nextReply_ >= replies_.size()) {
            return Status(K_RUNTIME_ERROR, "scripted Coordinator Discovery exhausted");
        }

        const auto &reply = replies_[nextReply_++];
        if (reply.throws) {
            throw std::runtime_error(reply.exceptionMessage);
        }
        serviceList = reply.coordinators;
        return reply.status;
    }

    size_t GetCallCount() const
    {
        return callCount_;
    }

private:
    std::vector<DiscoveryReply> replies_;
    size_t nextReply_{ 0 };
    size_t callCount_{ 0 };
};

std::shared_ptr<ScriptedCoordinatorDiscovery> MakeDiscovery(Status status, std::vector<std::string> coordinators)
{
    std::vector<DiscoveryReply> replies;
    replies.emplace_back(DiscoveryReply{ std::move(status), std::move(coordinators), false, "" });
    return std::make_shared<ScriptedCoordinatorDiscovery>(std::move(replies));
}

Status RangeOnce(ICoordinatorServiceProxy &proxy)
{
    std::vector<KeyValueEntry> kvs;
    int64_t revision = 0;
    return proxy.Range(RANGE_KEY, "", kvs, revision, DEFAULT_COORDINATOR_RPC_TIMEOUT_MS, nullptr);
}

}  // namespace

TEST(CoordinatorServiceProxyTest, RejectsNullDiscovery)
{
    std::shared_ptr<ICoordinatorDiscovery> discovery;
    CoordinatorServiceProxyZmqImpl proxy(discovery);

    Status status = proxy.Init();

    EXPECT_EQ(status.GetCode(), K_INVALID) << status.ToString();
}

TEST(CoordinatorServiceProxyTest, PropagatesDiscoveryErrorExactly)
{
    const Status discoveryError(K_RUNTIME_ERROR, "scripted discovery failure");
    auto discovery = MakeDiscovery(discoveryError, { ADDRESS_A });
    CoordinatorServiceProxyZmqImpl proxy(discovery);

    Status status = proxy.Init();

    EXPECT_EQ(status.GetCode(), discoveryError.GetCode());
    EXPECT_EQ(status.GetMsg(), discoveryError.GetMsg());
    EXPECT_EQ(discovery->GetCallCount(), 1UL);
}

TEST(CoordinatorServiceProxyTest, ConvertsDiscoveryExceptionToGenericRuntimeError)
{
    const std::string exceptionMessage = "private scripted exception detail";
    auto discovery = std::make_shared<ScriptedCoordinatorDiscovery>(std::vector<DiscoveryReply>{
        { Status::OK(), {}, true, exceptionMessage },
    });
    CoordinatorServiceProxyZmqImpl proxy(discovery);

    Status status = proxy.Init();

    EXPECT_EQ(status.GetCode(), K_RUNTIME_ERROR) << status.ToString();
    EXPECT_EQ(status.GetMsg().find(exceptionMessage), std::string::npos);
    EXPECT_EQ(discovery->GetCallCount(), 1UL);
}

TEST(CoordinatorServiceProxyTest, RejectsEmptyDiscoveryResult)
{
    auto discovery = MakeDiscovery(Status::OK(), {});
    CoordinatorServiceProxyZmqImpl proxy(discovery);

    Status status = proxy.Init();

    EXPECT_EQ(status.GetCode(), K_INVALID) << status.ToString();
    EXPECT_EQ(discovery->GetCallCount(), 1UL);
}

TEST(CoordinatorServiceProxyTest, InitializesFromOneCoordinator)
{
    auto discovery = MakeDiscovery(Status::OK(), { ADDRESS_A });
    CoordinatorServiceProxyZmqImpl proxy(discovery);

    Status status = proxy.Init();

    EXPECT_TRUE(status.IsOk()) << status.ToString();
    EXPECT_EQ(discovery->GetCallCount(), 1UL);
}

TEST(CoordinatorServiceProxyTest, AcceptsMultipleCoordinatorCandidates)
{
    auto discovery = MakeDiscovery(Status::OK(), { ADDRESS_A, ADDRESS_B, ADDRESS_C });
    CoordinatorServiceProxyZmqImpl proxy(discovery);

    Status status = proxy.Init();

    EXPECT_TRUE(status.IsOk()) << status.ToString();
    EXPECT_EQ(discovery->GetCallCount(), 1UL);
}

TEST(CoordinatorServiceProxyTest, RejectsMalformedFrontEvenWhenLaterCoordinatorIsValid)
{
    auto discovery = MakeDiscovery(Status::OK(), { "malformed-address", ADDRESS_A });
    CoordinatorServiceProxyZmqImpl proxy(discovery);

    Status status = proxy.Init();

    EXPECT_EQ(status.GetCode(), K_INVALID) << status.ToString();
    EXPECT_EQ(discovery->GetCallCount(), 1UL);
}

TEST(CoordinatorServiceProxyTest, SuccessfulInitIsIdempotent)
{
    auto discovery = MakeDiscovery(Status::OK(), { ADDRESS_A });
    CoordinatorServiceProxyZmqImpl proxy(discovery);

    DS_ASSERT_OK(proxy.Init());
    DS_ASSERT_OK(proxy.Init());

    EXPECT_EQ(discovery->GetCallCount(), 1UL);
}

TEST(CoordinatorServiceProxyTest, FailedInitCanRetryDiscovery)
{
    const Status discoveryError(K_RUNTIME_ERROR, "first discovery failed");
    auto discovery = std::make_shared<ScriptedCoordinatorDiscovery>(std::vector<DiscoveryReply>{
        { discoveryError, {}, false, "" },
        { Status::OK(), { ADDRESS_A }, false, "" },
    });
    CoordinatorServiceProxyZmqImpl proxy(discovery);

    Status firstStatus = proxy.Init();
    Status secondStatus = proxy.Init();

    EXPECT_EQ(firstStatus.GetCode(), discoveryError.GetCode());
    EXPECT_EQ(firstStatus.GetMsg(), discoveryError.GetMsg());
    EXPECT_TRUE(secondStatus.IsOk()) << secondStatus.ToString();
    EXPECT_EQ(discovery->GetCallCount(), 2UL);
}

TEST(CoordinatorServiceProxyTest, RpcBeforeInitDoesNotDiscover)
{
    auto discovery = MakeDiscovery(Status::OK(), { ADDRESS_A });
    CoordinatorServiceProxyZmqImpl proxy(discovery);

    Status status = RangeOnce(proxy);

    EXPECT_EQ(status.GetCode(), K_NOT_READY) << status.ToString();
    EXPECT_EQ(discovery->GetCallCount(), 0UL);
}

}  // namespace ut
}  // namespace datasystem
