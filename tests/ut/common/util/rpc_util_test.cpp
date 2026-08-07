/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2022. All rights reserved.
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

/**
 * Description: rpc util test.
 */
#include <chrono>
#include <vector>
#include <thread>
#include <unordered_set>

#include "ut/common.h"
#include "datasystem/common/util/rpc_util.h"
#include "datasystem/common/log/log.h"

namespace datasystem {
namespace ut {
class RpcUtilTest : public CommonTest {
public:
    void ExecRpcRetryOneError(int32_t timeoutMs, int32_t expectTime)
    {
        auto func = [](int32_t rpcTimeout) {
            int32_t minTime = 300;
            if (rpcTimeout < minTime) {
                std::this_thread::sleep_for(std::chrono::milliseconds(rpcTimeout));
            }
            return Status(StatusCode::K_RPC_UNAVAILABLE, "test");
        };
        auto startTime = std::chrono::steady_clock::now();
        Status status = RetryOnError(timeoutMs, func, []() { return Status::OK(); }, { StatusCode::K_RPC_UNAVAILABLE });
        auto endTime = std::chrono::steady_clock::now();
        auto execTime = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime).count();
        LOG(INFO) << "RetryOnError exec time: " << execTime << ", timeoutMs: " << timeoutMs
                  << ", expectTime: " << expectTime;
        int32_t range = 50;
        ASSERT_TRUE(execTime >= expectTime);
        ASSERT_TRUE(execTime <= expectTime + range);
    }
};

TEST_F(RpcUtilTest, TestRetryOnErrorOnce)
{
    auto func = [](int32_t realRpcTimeout) {
        std::this_thread::sleep_for(std::chrono::milliseconds(realRpcTimeout));
        return Status::OK();
    };

    auto startTime = std::chrono::steady_clock::now();
    int32_t timeoutMs = 1000;
    Status status = RetryOnError(timeoutMs, func, []() { return Status::OK(); }, {});
    auto endTime = std::chrono::steady_clock::now();
    auto execTime = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime).count();
    LOG(INFO) << "exec Time: " << execTime;
    ASSERT_TRUE(status.IsOk()) << status.ToString();
    ASSERT_GE(execTime, timeoutMs);
    ASSERT_LE(execTime, timeoutMs + 50);
}

TEST_F(RpcUtilTest, TestRemainTime)
{
    FLAGS_v = 1;
    inject::Set("rpc_util.retry_on_error_before_func", "2*sleep(300)");
    int32_t timeoutMs = 1000;
    // exec time: 50 and 200 is interval time, 150 is the last remaining time
    int32_t expectTime = 300 + 50 + 300 + 200 + 150;
    ExecRpcRetryOneError(timeoutMs, expectTime);
}

TEST_F(RpcUtilTest, TestMinOnceRpcTimeout)
{
    FLAGS_v = 1;
    inject::Set("rpc_util.retry_on_error_before_func", "2*sleep(300)");
    int32_t timeoutMs = 882;
    // exec time: 50 and 200 is interval time, 50 is the last remaining time
    int32_t expectTime = 300 + 50 + 300 + 200 + 32;
    ExecRpcRetryOneError(timeoutMs, expectTime);
}

TEST_F(RpcUtilTest, TestLastRun)
{
    FLAGS_v = 1;
    inject::Set("rpc_util.retry_on_error_before_func", "3*sleep(300)");
    int32_t timeoutMs = 1300;
    // // exec time: 50 and 200 is interval time, the last 100 is the interval time
    int32_t expectTime = 300 + 50 + 300 + 200 + 300 + 100 + 50;
    ExecRpcRetryOneError(timeoutMs, expectTime);
}

TEST_F(RpcUtilTest, RpcErrorClassificationUsesRetryableAndNonRetryableBuckets)
{
    ASSERT_TRUE(IsRetryableRpcError(Status(StatusCode::K_RPC_CANCELLED, "cancelled")));
    ASSERT_TRUE(IsRetryableRpcError(Status(StatusCode::K_RPC_DEADLINE_EXCEEDED, "deadline")));
    ASSERT_TRUE(IsRetryableRpcError(Status(StatusCode::K_RPC_UNAVAILABLE, "unavailable")));
    ASSERT_TRUE(IsRetryableRpcError(Status(StatusCode::K_RPC_NETWORK_BLIP, "network blip")));
    ASSERT_TRUE(IsRetryableRpcError(Status(StatusCode::K_URMA_WAIT_TIMEOUT, "urma wait")));

    ASSERT_TRUE(IsNonRetryableRpcError(Status(StatusCode::K_RPC_PEER_DEAD, "peer dead")));
    ASSERT_FALSE(IsRetryableRpcError(Status(StatusCode::K_RPC_PEER_DEAD, "peer dead")));

    ASSERT_FALSE(IsRetryableRpcError(Status(StatusCode::K_TRY_AGAIN, "try again")));
    ASSERT_FALSE(IsNonRetryableRpcError(Status(StatusCode::K_TRY_AGAIN, "try again")));
    ASSERT_FALSE(IsRetryableRpcError(Status::OK()));
    ASSERT_FALSE(IsNonRetryableRpcError(Status::OK()));
}

TEST_F(RpcUtilTest, RetryPolicyKeepsPeerDeadOutOfLegacyUnavailablePolicy)
{
    std::unordered_set<StatusCode> legacyUnavailablePolicy{ StatusCode::K_RPC_UNAVAILABLE };
    std::unordered_set<StatusCode> badExplicitPolicy{ StatusCode::K_RPC_PEER_DEAD };

    ASSERT_TRUE(ShouldRetryOnStatusCode(StatusCode::K_RPC_UNAVAILABLE, legacyUnavailablePolicy));
    ASSERT_TRUE(ShouldRetryOnStatusCode(StatusCode::K_RPC_NETWORK_BLIP, legacyUnavailablePolicy));
    ASSERT_FALSE(ShouldRetryOnStatusCode(StatusCode::K_RPC_PEER_DEAD, legacyUnavailablePolicy));
    ASSERT_FALSE(ShouldRetryOnStatusCode(StatusCode::K_RPC_PEER_DEAD, badExplicitPolicy));
}

TEST_F(RpcUtilTest, RetryOnErrorDoesNotReplayUrmaBackpressureAsGenericTryAgain)
{
    int calls = 0;
    Status status = RetryOnError(
        1000,
        [&calls](int32_t) {
            ++calls;
            return Status(StatusCode::K_URMA_TRY_AGAIN, "send lane pool exhausted");
        },
        []() { return Status::OK(); }, { StatusCode::K_TRY_AGAIN });

    ASSERT_EQ(status.GetCode(), StatusCode::K_URMA_TRY_AGAIN);
    ASSERT_EQ(calls, 1);
}

TEST_F(RpcUtilTest, RetryOnErrorDoesNotRetryPeerDeadForLegacyUnavailablePolicy)
{
    int calls = 0;
    int cleanupCalls = 0;
    auto func = [&calls](int32_t) {
        ++calls;
        return Status(StatusCode::K_RPC_PEER_DEAD, "peer dead");
    };

    auto startTime = std::chrono::steady_clock::now();
    Status status = RetryOnError(5000, func, [&cleanupCalls]() {
        ++cleanupCalls;
        return Status::OK();
    }, { StatusCode::K_RPC_UNAVAILABLE, StatusCode::K_RPC_PEER_DEAD });
    auto execTime = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - startTime).count();

    ASSERT_EQ(status.GetCode(), StatusCode::K_RPC_PEER_DEAD);
    ASSERT_EQ(calls, 1);
    ASSERT_EQ(cleanupCalls, 1);
    ASSERT_LT(execTime, 100) << "Non-retryable peer-dead must not sleep in RetryOnError.";
}

TEST_F(RpcUtilTest, RetryOnErrorDoesNotTreatPeerDeadAsExceptionSuccess)
{
    int calls = 0;
    auto func = [&calls](int32_t) {
        ++calls;
        if (calls == 1) {
            return Status(StatusCode::K_RPC_UNAVAILABLE, "unavailable");
        }
        return Status(StatusCode::K_RPC_PEER_DEAD, "peer dead");
    };

    Status status = RetryOnError(100, func, []() { return Status::OK(); }, { StatusCode::K_RPC_UNAVAILABLE },
                                 MAX_RPC_TIMEOUT_MS, { StatusCode::K_RPC_PEER_DEAD });

    ASSERT_EQ(status.GetCode(), StatusCode::K_RPC_PEER_DEAD);
    ASSERT_EQ(calls, 2);
}

TEST_F(RpcUtilTest, RetryOnErrorRetriesNetworkBlipForLegacyUnavailablePolicy)
{
    int calls = 0;
    auto func = [&calls](int32_t) {
        ++calls;
        if (calls == 1) {
            return Status(StatusCode::K_RPC_NETWORK_BLIP, "network blip");
        }
        return Status::OK();
    };

    Status status = RetryOnError(1000, func, []() { return Status::OK(); }, { StatusCode::K_RPC_UNAVAILABLE });

    ASSERT_TRUE(status.IsOk()) << status.ToString();
    ASSERT_EQ(calls, 2);
}
}  // namespace ut
}  // namespace datasystem
