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
#include <vector>
#include <thread>

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
        int32_t range = 10;
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
    ASSERT_TRUE(execTime == timeoutMs);
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

TEST_F(RpcUtilTest, IsRpcTimeoutIncludesUrmaWaitTimeout)
{
    ASSERT_TRUE(IsRpcTimeout(Status(StatusCode::K_URMA_WAIT_TIMEOUT, "urma wait")));
    ASSERT_TRUE(IsRpcTimeoutOrTryAgain(Status(StatusCode::K_URMA_WAIT_TIMEOUT, "urma wait")));
    ASSERT_FALSE(IsRpcTimeout(Status::OK()));
}
}  // namespace ut
}  // namespace datasystem