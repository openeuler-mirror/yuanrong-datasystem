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

/** Description: Unit tests for ObjectClientImpl::Exist routing/transport orchestration. */

#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

#include "common.h"
#include "datasystem/client/object_cache/exist_handler.h"
#include "datasystem/common/util/thread_pool.h"

namespace datasystem {
namespace object_cache {
namespace {

HostPort MakeWorker(int port)
{
    return HostPort("127.0.0.1", port);
}

class FakeExistRouting : public IExistRouting {
public:
    Status SelectWorkers(const std::vector<std::string> &, client::SelectStrategy strategy,
                         std::unordered_map<HostPort, std::vector<std::string>> &output) override
    {
        ++selectWorkersCount;
        selectedStrategy = strategy;
        if (!groupSequence.empty()) {
            output = groupSequence.front();
            groupSequence.erase(groupSequence.begin());
        } else {
            output = groups;
        }
        return selectStatus;
    }

    void UpdateState(const HostPort &addr, StatusCode status) override
    {
        updatedWorkers.emplace_back(addr);
        updatedStatuses.emplace_back(status);
    }

    Status selectStatus = Status::OK();
    client::SelectStrategy selectedStrategy = client::SelectStrategy::SAME_NODE_PREFERRED;
    std::unordered_map<HostPort, std::vector<std::string>> groups;
    std::vector<std::unordered_map<HostPort, std::vector<std::string>>> groupSequence;
    std::vector<HostPort> updatedWorkers;
    std::vector<StatusCode> updatedStatuses;
    int selectWorkersCount = 0;
};

class FakeExistTransport : public IExistTransport {
public:
    struct CallResult {
        Status status;
        std::vector<bool> exists;
    };

    Status Exist(const HostPort &workerAddr, const client::TransportExistRequest &input,
                 client::TransportExistResult &output) override
    {
        std::lock_guard<std::mutex> lock(mutex);
        workers.emplace_back(workerAddr);
        keys.emplace_back(input.objectKeys.begin(), input.objectKeys.end());
        queryL2Cache.emplace_back(input.queryL2Cache);
        isLocal.emplace_back(input.isLocal);
        subTimeouts.emplace_back(input.subTimeoutMs);
        auto byWorkerIt = resultsByWorker.find(workerAddr);
        if (byWorkerIt != resultsByWorker.end()) {
            if (byWorkerIt->second.empty()) {
                return Status(K_RUNTIME_ERROR, "no fake result for worker");
            }
            CallResult result = byWorkerIt->second.front();
            byWorkerIt->second.erase(byWorkerIt->second.begin());
            if (result.status.IsError()) {
                return result.status;
            }
            output.exists = result.exists;
            return Status::OK();
        }
        if (results.empty()) {
            return Status(K_RUNTIME_ERROR, "no fake result");
        }
        CallResult result = results.front();
        results.erase(results.begin());
        if (result.status.IsError()) {
            return result.status;
        }
        output.exists = result.exists;
        return Status::OK();
    }

    std::vector<CallResult> results;
    std::unordered_map<HostPort, std::vector<CallResult>> resultsByWorker;
    std::vector<HostPort> workers;
    std::vector<std::vector<std::string>> keys;
    std::vector<bool> queryL2Cache;
    std::vector<bool> isLocal;
    std::vector<int64_t> subTimeouts;
    std::mutex mutex;
};

class ExistHandlerTest : public ut::CommonTest {
public:
    void SetUp() override
    {
        ut::CommonTest::SetUp();
        taskPool_ = std::make_shared<ThreadPool>(0, 2, "exist_test_rpc");
        routing_ = std::make_shared<FakeExistRouting>();
        transport_ = std::make_shared<FakeExistTransport>();
    }

    void TearDown() override
    {
        taskPool_.reset();
        ut::CommonTest::TearDown();
    }

    Status RunFlow(const std::vector<std::string> &keys, std::vector<bool> &exists, bool queryL2Cache = true,
                   bool isLocal = false, const std::string &tenantId = "")
    {
        ExistHandler flow(routing_, transport_, taskPool_);
        ExistHandlerRequest request{ keys, queryL2Cache, isLocal, requestTimeoutMs_, clientId_, tenantId,
                                  SensitiveValue() };
        return flow.Run(request, exists);
    }

protected:
    std::shared_ptr<FakeExistRouting> routing_;
    std::shared_ptr<FakeExistTransport> transport_;
    std::shared_ptr<ThreadPool> taskPool_;
    int32_t requestTimeoutMs_ = 1000;
    std::string clientId_ = "exist-test-client";
};

TEST_F(ExistHandlerTest, ExistUsesRoutingAndTransportAndKeepsInputOrder)
{
    routing_->groups[MakeWorker(18481)] = { "k2" };
    routing_->groups[MakeWorker(18482)] = { "k1" };
    transport_->results = {
        { Status::OK(), { false } },
        { Status::OK(), { true } },
    };
    std::vector<bool> exists;

    Status rc = RunFlow({ "k1", "k2" }, exists);

    ASSERT_TRUE(rc.IsOk());
    EXPECT_EQ(exists, std::vector<bool>({ true, false }));
    EXPECT_EQ(routing_->selectWorkersCount, 1);
    EXPECT_EQ(routing_->selectedStrategy, client::SelectStrategy::HASH_RING_AFFINITY);
    EXPECT_TRUE(transport_->queryL2Cache[0]);
    EXPECT_FALSE(transport_->isLocal[0]);
}

TEST_F(ExistHandlerTest, ExistFallsBackToSerialOwnerCallsWithoutTaskPool)
{
    HostPort workerA = MakeWorker(18481);
    HostPort workerB = MakeWorker(18482);
    routing_->groups[workerA] = { "k1" };
    routing_->groups[workerB] = { "k2" };
    transport_->resultsByWorker[workerA] = { { Status::OK(), { true } } };
    transport_->resultsByWorker[workerB] = { { Status::OK(), { false } } };
    taskPool_.reset();
    std::vector<bool> exists;

    Status rc = RunFlow({ "k1", "k2" }, exists);

    ASSERT_TRUE(rc.IsOk());
    EXPECT_EQ(exists, std::vector<bool>({ true, false }));
    EXPECT_EQ(transport_->workers, std::vector<HostPort>({ workerA, workerB }));
}

TEST_F(ExistHandlerTest, ExistRetriesNotOwnerExtraWithoutSelectingAgain)
{
    HostPort workerA = MakeWorker(18481);
    HostPort workerB = MakeWorker(18482);
    routing_->groups[workerA] = { "k1" };
    transport_->results = {
        { Status(K_NOT_OWNER, "moved").WithExtra(workerB.ToString()), {} },
        { Status::OK(), { true } },
    };
    std::vector<bool> exists;

    Status rc = RunFlow({ "k1" }, exists);

    ASSERT_TRUE(rc.IsOk());
    EXPECT_EQ(exists, std::vector<bool>({ true }));
    ASSERT_EQ(transport_->workers.size(), 2ul);
    EXPECT_EQ(transport_->workers[0], workerA);
    EXPECT_EQ(transport_->workers[1], workerB);
    EXPECT_EQ(routing_->selectWorkersCount, 1);
    EXPECT_TRUE(routing_->updatedWorkers.empty());
}

TEST_F(ExistHandlerTest, ExistUpdatesRoutingAfterConnectionFailureRetry)
{
    HostPort workerA = MakeWorker(18481);
    routing_->groups[workerA] = { "k1" };
    transport_->results = {
        { Status(K_RPC_UNAVAILABLE, "first"), {} },
        { Status(K_RPC_UNAVAILABLE, "second"), {} },
        { Status(K_RPC_UNAVAILABLE, "third"), {} },
        { Status(K_RPC_UNAVAILABLE, "fourth"), {} },
    };
    std::vector<bool> exists;

    Status rc = RunFlow({ "k1" }, exists);

    EXPECT_EQ(rc.GetCode(), K_RPC_UNAVAILABLE);
    ASSERT_EQ(transport_->workers.size(), 4ul);
    EXPECT_EQ(transport_->workers[0], workerA);
    EXPECT_EQ(transport_->workers[1], workerA);
    EXPECT_EQ(transport_->workers[2], workerA);
    EXPECT_EQ(transport_->workers[3], workerA);
    ASSERT_EQ(routing_->updatedWorkers.size(), 2ul);
    EXPECT_EQ(routing_->updatedWorkers[0], workerA);
    EXPECT_EQ(routing_->updatedWorkers[1], workerA);
    EXPECT_EQ(routing_->updatedStatuses[0], K_CLIENT_WORKER_DISCONNECT);
    EXPECT_EQ(routing_->updatedStatuses[1], K_CLIENT_WORKER_DISCONNECT);
    EXPECT_EQ(routing_->selectWorkersCount, 2);
}

TEST_F(ExistHandlerTest, ExistReroutesAfterDeadlineExceeded)
{
    HostPort workerA = MakeWorker(18481);
    HostPort workerB = MakeWorker(18482);
    routing_->groupSequence = {
        { { workerA, { "k1" } } },
        { { workerB, { "k1" } } },
    };
    transport_->resultsByWorker[workerA] = {
        { Status(K_RPC_DEADLINE_EXCEEDED, "first"), {} },
        { Status(K_RPC_DEADLINE_EXCEEDED, "second"), {} },
    };
    transport_->resultsByWorker[workerB] = {
        { Status::OK(), { true } },
    };
    std::vector<bool> exists;

    Status rc = RunFlow({ "k1" }, exists);

    ASSERT_TRUE(rc.IsOk());
    EXPECT_EQ(exists, std::vector<bool>({ true }));
    ASSERT_EQ(routing_->updatedWorkers.size(), 1ul);
    EXPECT_EQ(routing_->updatedWorkers[0], workerA);
    EXPECT_EQ(routing_->updatedStatuses[0], K_CLIENT_WORKER_DISCONNECT);
    ASSERT_EQ(transport_->workers.size(), 3ul);
    EXPECT_EQ(transport_->workers[0], workerA);
    EXPECT_EQ(transport_->workers[1], workerA);
    EXPECT_EQ(transport_->workers[2], workerB);
    EXPECT_EQ(transport_->subTimeouts, std::vector<int64_t>({ 1000, 1000, 1000 }));
    EXPECT_EQ(routing_->selectWorkersCount, 2);
}

TEST_F(ExistHandlerTest, ExistSplitsRedirectedKeysByOwnerAndKeepsInputOrder)
{
    HostPort oldWorker = MakeWorker(18481);
    HostPort workerB = MakeWorker(18482);
    HostPort workerC = MakeWorker(18483);
    routing_->groups[oldWorker] = { "k1", "k2", "k3" };
    transport_->resultsByWorker[oldWorker] = {
        { Status(K_NOT_OWNER, "moved")
              .WithExtra("{\"exist_redirects\":[{\"address\":\"" + workerB.ToString()
                         + "\",\"keys\":[\"k1\",\"k3\"]},{\"address\":\"" + workerC.ToString()
                         + "\",\"keys\":[\"k2\"]}]}"),
          {} },
    };
    transport_->resultsByWorker[workerB] = {
        { Status::OK(), { true, false } },
    };
    transport_->resultsByWorker[workerC] = {
        { Status::OK(), { true } },
    };
    std::vector<bool> exists;

    Status rc = RunFlow({ "k1", "k2", "k3" }, exists);

    ASSERT_TRUE(rc.IsOk());
    EXPECT_EQ(exists, std::vector<bool>({ true, true, false }));
    EXPECT_EQ(routing_->selectWorkersCount, 1);
    ASSERT_EQ(transport_->workers.size(), 3ul);
    std::unordered_map<std::string, std::vector<std::string>> keysByWorker;
    for (size_t i = 0; i < transport_->workers.size(); ++i) {
        keysByWorker[transport_->workers[i].ToString()] = transport_->keys[i];
    }
    EXPECT_EQ(keysByWorker[oldWorker.ToString()], std::vector<std::string>({ "k1", "k2", "k3" }));
    EXPECT_EQ(keysByWorker[workerB.ToString()], std::vector<std::string>({ "k1", "k3" }));
    EXPECT_EQ(keysByWorker[workerC.ToString()], std::vector<std::string>({ "k2" }));
}

TEST_F(ExistHandlerTest, ExistStructuredRedirectMapsTenantNamespaceKeysToRawKeys)
{
    HostPort oldWorker = MakeWorker(18481);
    HostPort newWorker = MakeWorker(18482);
    const std::string tenantId = "tenant-a";
    routing_->groups[oldWorker] = { "k1" };
    const std::string namespaceKey = tenantId + "$k1";
    transport_->resultsByWorker[oldWorker] = {
        { Status(K_NOT_OWNER, "moved")
              .WithExtra("{\"exist_redirects\":[{\"address\":\"" + newWorker.ToString() + "\",\"keys\":[\""
                         + namespaceKey + "\"]}]}"),
          {} },
    };
    transport_->resultsByWorker[newWorker] = {
        { Status::OK(), { true } },
    };
    std::vector<bool> exists;

    Status rc = RunFlow({ "k1" }, exists, true, false, tenantId);

    ASSERT_TRUE(rc.IsOk());
    EXPECT_EQ(exists, std::vector<bool>({ true }));
    ASSERT_EQ(transport_->workers.size(), 2ul);
    std::unordered_map<std::string, std::vector<std::string>> keysByWorker;
    for (size_t i = 0; i < transport_->workers.size(); ++i) {
        keysByWorker[transport_->workers[i].ToString()] = transport_->keys[i];
    }
    EXPECT_EQ(keysByWorker[newWorker.ToString()], std::vector<std::string>({ "k1" }));
}

TEST_F(ExistHandlerTest, ExistRetriesStructuredRedirectMissingKeysOnOriginalWorker)
{
    HostPort oldWorker = MakeWorker(18481);
    HostPort newWorker = MakeWorker(18482);
    routing_->groups[oldWorker] = { "k1", "k2" };
    transport_->resultsByWorker[oldWorker] = {
        { Status(K_NOT_OWNER, "moved")
              .WithExtra("{\"exist_redirects\":[{\"address\":\"" + newWorker.ToString()
                         + "\",\"keys\":[\"k1\"]}]}"),
          {} },
        { Status::OK(), { true } },
    };
    transport_->resultsByWorker[newWorker] = {
        { Status::OK(), { true } },
    };
    std::vector<bool> exists;

    Status rc = RunFlow({ "k1", "k2" }, exists);

    ASSERT_TRUE(rc.IsOk());
    EXPECT_EQ(exists, std::vector<bool>({ true, true }));
    ASSERT_EQ(transport_->workers.size(), 3ul);
    EXPECT_EQ(transport_->workers[0], oldWorker);
    std::unordered_map<std::string, std::vector<std::vector<std::string>>> callsByWorker;
    for (size_t i = 0; i < transport_->workers.size(); ++i) {
        callsByWorker[transport_->workers[i].ToString()].emplace_back(transport_->keys[i]);
    }
    EXPECT_EQ(callsByWorker[newWorker.ToString()], std::vector<std::vector<std::string>>({ { "k1" } }));
    ASSERT_EQ(callsByWorker[oldWorker.ToString()].size(), 2ul);
    EXPECT_EQ(callsByWorker[oldWorker.ToString()][0], std::vector<std::string>({ "k1", "k2" }));
    EXPECT_EQ(callsByWorker[oldWorker.ToString()][1], std::vector<std::string>({ "k2" }));
}

TEST_F(ExistHandlerTest, ExistFallsBackToFirstStructuredRedirectAddressWhenNoKeysMatch)
{
    HostPort oldWorker = MakeWorker(18481);
    HostPort newWorker = MakeWorker(18482);
    routing_->groups[oldWorker] = { "k1", "k2" };
    transport_->resultsByWorker[oldWorker] = {
        { Status(K_NOT_OWNER, "moved")
              .WithExtra("{\"exist_redirects\":[{\"address\":\"" + newWorker.ToString()
                         + "\",\"keys\":[\"unknown\"]}]}"),
          {} },
    };
    transport_->resultsByWorker[newWorker] = {
        { Status::OK(), { true, false } },
    };
    std::vector<bool> exists;

    Status rc = RunFlow({ "k1", "k2" }, exists);

    ASSERT_TRUE(rc.IsOk());
    EXPECT_EQ(exists, std::vector<bool>({ true, false }));
    ASSERT_EQ(transport_->workers.size(), 2ul);
    EXPECT_EQ(transport_->workers[0], oldWorker);
    EXPECT_EQ(transport_->workers[1], newWorker);
    EXPECT_EQ(transport_->keys[1], std::vector<std::string>({ "k1", "k2" }));
}

TEST_F(ExistHandlerTest, ExistHandlerCanRunWithoutObjectClientImpl)
{
    HostPort workerA = MakeWorker(18481);
    HostPort workerB = MakeWorker(18482);
    routing_->groups[workerA] = { "k1" };
    transport_->results = {
        { Status(K_NOT_OWNER, "moved").WithExtra(workerB.ToString()), {} },
        { Status::OK(), { true } },
    };
    ExistHandler flow(routing_, transport_, taskPool_);
    std::vector<bool> exists;
    ExistHandlerRequest request{ { "k1" }, true, false, 1000, "exist-test-client", "", SensitiveValue() };

    Status rc = flow.Run(request, exists);

    ASSERT_TRUE(rc.IsOk());
    EXPECT_EQ(exists, std::vector<bool>({ true }));
    ASSERT_EQ(transport_->workers.size(), 2ul);
    EXPECT_EQ(transport_->workers[0], workerA);
    EXPECT_EQ(transport_->workers[1], workerB);
    EXPECT_EQ(routing_->selectWorkersCount, 1);
}

TEST_F(ExistHandlerTest, ExistReturnsEmptyForEmptyKeyVector)
{
    routing_->groups.clear();
    transport_->results = { { Status::OK(), {} } };
    std::vector<bool> exists;
    Status rc = RunFlow({}, exists);
    ASSERT_TRUE(rc.IsOk());
    EXPECT_TRUE(exists.empty());
}

TEST_F(ExistHandlerTest, ExistHandlesDuplicateKeysCorrectly)
{
    HostPort worker = MakeWorker(18481);
    routing_->groups[worker] = { "k1", "k1" };
    transport_->resultsByWorker[worker] = { { Status::OK(), { true, true } } };
    std::vector<bool> exists;
    Status rc = RunFlow({ "k1", "k1" }, exists);
    ASSERT_TRUE(rc.IsOk());
    EXPECT_EQ(exists, std::vector<bool>({ true, true }));
}

TEST_F(ExistHandlerTest, ExistReturnsErrorOnCorruptedRedirectExtra)
{
    HostPort worker = MakeWorker(18481);
    routing_->groups[worker] = { "k1" };
    transport_->results = {
        { Status(K_NOT_OWNER, "moved").WithExtra("not valid json"), {} },
    };
    std::vector<bool> exists;
    Status rc = RunFlow({ "k1" }, exists);
    EXPECT_TRUE(rc.IsError());
}

}  // namespace
}  // namespace object_cache
}  // namespace datasystem
