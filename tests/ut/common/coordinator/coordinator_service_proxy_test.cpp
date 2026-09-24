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

#include <chrono>
#include <condition_variable>
#include <future>
#include <memory>
#include <mutex>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#define private public
#include "datasystem/common/coordinator/coordinator_service_proxy.h"
#undef private

#include "datasystem/utils/coordinator_discovery.h"
#include "datasystem/utils/status.h"
#include "ut/common.h"
#include "ut/bthread_test_helper.h"

namespace datasystem {
namespace ut {
namespace {

using CoordinatorDiscoveryPtr = std::shared_ptr<ICoordinatorDiscovery>;

static_assert(std::is_constructible_v<CoordinatorServiceProxyBrpcImpl, CoordinatorDiscoveryPtr>);

constexpr char ADDRESS_A[] = "127.0.0.1:31501";
constexpr char ADDRESS_B[] = "127.0.0.1:31502";
constexpr char ADDRESS_C[] = "127.0.0.1:31503";
constexpr char RANGE_KEY[] = "/range";
constexpr char COORDINATOR_ID[] = "0123456789abcdef";

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

TEST(CoordinatorServiceProxyTest, IdentityRefreshContentionPreservesBthreadProgress)
{
    CoordinatorServiceProxyBrpcImpl proxy(MakeDiscovery(Status::OK(), { ADDRESS_A }));
    proxy.currentCoordinatorId_ = COORDINATOR_ID;
    proxy.identityRefreshMutex_.lock();
    ExpectBthreadProgressWhileBlocked(
        [&](size_t) {
            DS_EXPECT_OK(proxy.ConfirmResponseIdentity(COORDINATOR_ID, DEFAULT_COORDINATOR_RPC_TIMEOUT_MS, false));
        },
        [&] { proxy.identityRefreshMutex_.unlock(); });
}

TEST(CoordinatorServiceProxyTest, ExplicitIdentityProbeContentionPreservesBthreadProgress)
{
    CoordinatorServiceProxyBrpcImpl proxy(MakeDiscovery(Status::OK(), { ADDRESS_A }));
    proxy.identityRefreshMutex_.lock();
    ExpectBthreadProgressWhileBlocked(
        [&](size_t) {
            std::string id;
            EXPECT_EQ(proxy.GetCoordinatorId(id, DEFAULT_COORDINATOR_RPC_TIMEOUT_MS).GetCode(), K_NOT_READY);
        },
        [&] { proxy.identityRefreshMutex_.unlock(); });
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
    CoordinatorServiceProxyBrpcImpl proxy(discovery);

    Status status = proxy.Init();

    EXPECT_EQ(status.GetCode(), K_INVALID) << status.ToString();
}

TEST(CoordinatorServiceProxyTest, PropagatesDiscoveryErrorExactly)
{
    const Status discoveryError(K_RUNTIME_ERROR, "scripted discovery failure");
    auto discovery = MakeDiscovery(discoveryError, { ADDRESS_A });
    CoordinatorServiceProxyBrpcImpl proxy(discovery);

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
    CoordinatorServiceProxyBrpcImpl proxy(discovery);

    Status status = proxy.Init();

    EXPECT_EQ(status.GetCode(), K_RUNTIME_ERROR) << status.ToString();
    EXPECT_EQ(status.GetMsg().find(exceptionMessage), std::string::npos);
    EXPECT_EQ(discovery->GetCallCount(), 1UL);
}

TEST(CoordinatorServiceProxyTest, RejectsEmptyDiscoveryResult)
{
    auto discovery = MakeDiscovery(Status::OK(), {});
    CoordinatorServiceProxyBrpcImpl proxy(discovery);

    Status status = proxy.Init();

    EXPECT_EQ(status.GetCode(), K_INVALID) << status.ToString();
    EXPECT_EQ(discovery->GetCallCount(), 1UL);
}

TEST(CoordinatorServiceProxyTest, InitializesFromOneCoordinator)
{
    auto discovery = MakeDiscovery(Status::OK(), { ADDRESS_A });
    CoordinatorServiceProxyBrpcImpl proxy(discovery);

    Status status = proxy.Init();

    EXPECT_TRUE(status.IsOk()) << status.ToString();
    EXPECT_EQ(discovery->GetCallCount(), 1UL);
}

TEST(CoordinatorServiceProxyTest, AcceptsMultipleCoordinatorCandidates)
{
    auto discovery = MakeDiscovery(Status::OK(), { ADDRESS_A, ADDRESS_B, ADDRESS_C });
    CoordinatorServiceProxyBrpcImpl proxy(discovery);

    Status status = proxy.Init();

    EXPECT_TRUE(status.IsOk()) << status.ToString();
    EXPECT_EQ(discovery->GetCallCount(), 1UL);
}

TEST(CoordinatorServiceProxyTest, SkipsMalformedCandidateWhenLaterCoordinatorIsValid)
{
    auto discovery = MakeDiscovery(Status::OK(), { "malformed-address", ADDRESS_A });
    CoordinatorServiceProxyBrpcImpl proxy(discovery);

    Status status = proxy.Init();

    EXPECT_TRUE(status.IsOk()) << status.ToString();
    EXPECT_EQ(discovery->GetCallCount(), 1UL);
}

TEST(CoordinatorServiceProxyTest, SuccessfulInitIsIdempotent)
{
    auto discovery = MakeDiscovery(Status::OK(), { ADDRESS_A });
    CoordinatorServiceProxyBrpcImpl proxy(discovery);

    DS_ASSERT_OK(proxy.Init());
    DS_ASSERT_OK(proxy.Init());

    EXPECT_EQ(discovery->GetCallCount(), 1UL);
}

TEST(CoordinatorServiceProxyTest, GetRouterRequiresSuccessfulInit)
{
    auto discovery = MakeDiscovery(Status::OK(), { ADDRESS_A });
    CoordinatorServiceProxyBrpcImpl proxy(discovery);
    CoordinatorLeaderRouter *router = nullptr;

    EXPECT_EQ(proxy.GetRouter(router).GetCode(), K_NOT_READY);
    EXPECT_EQ(router, nullptr);

    DS_ASSERT_OK(proxy.Init());
    DS_ASSERT_OK(proxy.GetRouter(router));
    EXPECT_NE(router, nullptr);
}

TEST(CoordinatorServiceProxyTest, ClearingLeaderChangeHandlerWaitsForInflightCallback)
{
    auto discovery = MakeDiscovery(Status::OK(), { ADDRESS_A });
    CoordinatorServiceProxyBrpcImpl proxy(discovery);
    DS_ASSERT_OK(proxy.Init());
    std::mutex gateMutex;
    std::condition_variable gateCv;
    bool entered = false;
    bool resume = false;
    DS_ASSERT_OK(proxy.SetLeaderChangeHandler([&](const CoordinatorLeaderIdentity &) {
        std::unique_lock<std::mutex> lock(gateMutex);
        entered = true;
        gateCv.notify_all();
        gateCv.wait(lock, [&resume] { return resume; });
    }));
    auto publisher = std::async(std::launch::async, [&proxy] {
        proxy.PublishLeaderIdentity(CoordinatorLeaderIdentity{ HostPort(), "", 0, 0 });
    });
    {
        std::unique_lock<std::mutex> lock(gateMutex);
        ASSERT_TRUE(gateCv.wait_for(lock, std::chrono::seconds(2), [&entered] { return entered; }));
    }

    auto clear = std::async(std::launch::async, [&proxy] { return proxy.SetLeaderChangeHandler({}); });
    EXPECT_EQ(clear.wait_for(std::chrono::milliseconds(20)), std::future_status::timeout);
    {
        std::lock_guard<std::mutex> lock(gateMutex);
        resume = true;
    }
    gateCv.notify_all();

    publisher.get();
    ASSERT_EQ(clear.wait_for(std::chrono::seconds(2)), std::future_status::ready);
    EXPECT_TRUE(clear.get().IsOk());
}

TEST(CoordinatorServiceProxyTest, FailedInitCanRetryDiscovery)
{
    const Status discoveryError(K_RUNTIME_ERROR, "first discovery failed");
    auto discovery = std::make_shared<ScriptedCoordinatorDiscovery>(std::vector<DiscoveryReply>{
        { discoveryError, {}, false, "" },
        { Status::OK(), { ADDRESS_A }, false, "" },
    });
    CoordinatorServiceProxyBrpcImpl proxy(discovery);

    Status firstStatus = proxy.Init();
    Status secondStatus = proxy.Init();

    EXPECT_EQ(firstStatus.GetCode(), discoveryError.GetCode());
    EXPECT_EQ(firstStatus.GetMsg(), discoveryError.GetMsg());
    EXPECT_TRUE(secondStatus.IsOk()) << secondStatus.ToString();
    EXPECT_EQ(discovery->GetCallCount(), 2UL);
}

TEST(CoordinatorServiceProxyTest, RecoveringResponseRequiresExplicitAcceptance)
{
    auto discovery = MakeDiscovery(Status::OK(), { ADDRESS_A });
    CoordinatorServiceProxyBrpcImpl proxy(discovery);
    coordinator::ResponseHeader header;
    header.set_coordinator_id(COORDINATOR_ID);
    header.set_leader_term(7);
    header.set_state(coordinator::ResponseHeader::RECOVERING);

    EXPECT_EQ(proxy.AcceptResponse(header, DEFAULT_COORDINATOR_RPC_TIMEOUT_MS, nullptr, false).GetCode(), K_NOT_READY);
    DS_ASSERT_OK(proxy.AcceptResponse(header, DEFAULT_COORDINATOR_RPC_TIMEOUT_MS, nullptr, true));
    std::string observedCoordinatorId;
    proxy.GetObservedCoordinatorId(observedCoordinatorId);
    EXPECT_EQ(observedCoordinatorId, COORDINATOR_ID);
}

TEST(CoordinatorServiceProxyTest, UnchangedRangeRequiresStableCoordinatorIdentity)
{
    EXPECT_FALSE(CoordinatorServiceProxyBase::CanAcceptUnchangedRange("", COORDINATOR_ID));
    EXPECT_FALSE(CoordinatorServiceProxyBase::CanAcceptUnchangedRange(COORDINATOR_ID, "fedcba9876543210"));
    EXPECT_TRUE(CoordinatorServiceProxyBase::CanAcceptUnchangedRange(COORDINATOR_ID, COORDINATOR_ID));
}

TEST(CoordinatorServiceProxyTest, RpcBeforeInitDoesNotDiscover)
{
    auto discovery = MakeDiscovery(Status::OK(), { ADDRESS_A });
    CoordinatorServiceProxyBrpcImpl proxy(discovery);

    Status status = RangeOnce(proxy);

    EXPECT_EQ(status.GetCode(), K_NOT_READY) << status.ToString();
    EXPECT_EQ(discovery->GetCallCount(), 0UL);
}

}  // namespace ut
}  // namespace datasystem
