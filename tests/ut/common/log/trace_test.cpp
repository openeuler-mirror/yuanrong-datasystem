/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2023. All rights reserved.
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
 * Description: This is used to test the Trace and TraceGuard class.
 */
#include "datasystem/common/log/trace.h"

#include <atomic>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "ut/common.h"
#include "datasystem/context/context.h"

namespace datasystem {
namespace ut {
class TraceTest : public CommonTest {};

void TraceGuardDestructor(std::string traceID)
{
    TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceID);
}

TEST_F(TraceTest, TestTraceGuardDestructor)
{
    std::string traceID = "abc";
    TraceGuardDestructor(traceID);
    auto traceID1 = Trace::Instance().GetTraceID();
    EXPECT_NE(traceID1, traceID);
}

TEST_F(TraceTest, TestTraceGuardMoveDestructor)
{
    std::string traceID = "move-trace";
    {
        TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceID);
        TraceGuard movedTraceGuard = std::move(traceGuard);
        EXPECT_EQ(Trace::Instance().GetTraceID(), traceID);
    }
    EXPECT_NE(Trace::Instance().GetTraceID(), traceID);
}

TEST_F(TraceTest, TestRequestTraceRestoresExistingNonRequestContext)
{
    std::string traceID = "background-trace";
    TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceID);
    ASSERT_FALSE(Trace::Instance().IsRequestLogTrace());

    {
        TraceGuard requestTraceGuard = Trace::Instance().SetRequestTraceUUID();
        EXPECT_EQ(Trace::Instance().GetTraceID(), traceID);
        EXPECT_TRUE(Trace::Instance().IsRequestLogTrace());
    }
    EXPECT_EQ(Trace::Instance().GetTraceID(), traceID);
    EXPECT_FALSE(Trace::Instance().IsRequestLogTrace());
}

TEST_F(TraceTest, TestTraceContextPropagatesRequestDecision)
{
    TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
    Trace::Instance().SetRequestSampleDecision(true, false);
    auto context = Trace::Instance().GetContext();

    std::thread t([context]() {
        TraceGuard childTraceGuard = Trace::Instance().SetTraceContext(context);
        EXPECT_EQ(Trace::Instance().GetTraceID(), context.traceID);
        EXPECT_TRUE(Trace::Instance().IsRequestLogTrace());

        bool admitted = true;
        EXPECT_TRUE(Trace::Instance().GetRequestSampleDecision(admitted));
        EXPECT_FALSE(admitted);
    });
    t.join();
}

TEST_F(TraceTest, TestSetTraceContextKeepTrueRestoreRequestContext)
{
    Trace::Instance().Invalidate();
    TraceGuard oldGuard = Trace::Instance().SetTraceNewID("old-trace");
    Trace::Instance().SetRequestLogTrace(false);
    Trace::Instance().SetRequestSampleDecision(true, true);

    TraceContext incoming;
    incoming.traceID = "new-trace";
    incoming.requestLogTrace = true;
    incoming.requestSampleDecisionValid = true;
    incoming.requestSampleDecisionAdmitted = false;

    {
        TraceGuard guard = Trace::Instance().SetTraceContext(incoming, true);
        EXPECT_EQ(Trace::Instance().GetTraceID(), "new-trace");
        EXPECT_TRUE(Trace::Instance().IsRequestLogTrace());
        bool admitted = true;
        EXPECT_TRUE(Trace::Instance().GetRequestSampleDecision(admitted));
        EXPECT_FALSE(admitted);
    }

    EXPECT_EQ(Trace::Instance().GetTraceID(), "new-trace");
    EXPECT_FALSE(Trace::Instance().IsRequestLogTrace());
    bool admitted = false;
    EXPECT_TRUE(Trace::Instance().GetRequestSampleDecision(admitted));
    EXPECT_TRUE(admitted);
}

TEST_F(TraceTest, LEVEL1_ConcurrentSetTraceContextKeepsRequestFields)
{
    Trace::Instance().Invalidate();
    TraceGuard rootGuard = Trace::Instance().SetRequestTraceUUID();
    Trace::Instance().SetRequestSampleDecision(true, false);
    TraceContext context = Trace::Instance().GetContext();

    constexpr int kThreads = 32;
    constexpr int kRounds = 4000;
    std::atomic<int> mismatch{ 0 };

    std::vector<std::thread> threads;
    threads.reserve(kThreads);
    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back([&]() {
            for (int i = 0; i < kRounds; ++i) {
                TraceGuard guard = Trace::Instance().SetTraceContext(context);
                bool admitted = true;
                bool valid = Trace::Instance().GetRequestSampleDecision(admitted);
                if (Trace::Instance().GetTraceID() != context.traceID || !Trace::Instance().IsRequestLogTrace()
                    || !valid || admitted) {
                    mismatch.fetch_add(1, std::memory_order_relaxed);
                }
            }
        });
    }

    for (auto &thread : threads) {
        thread.join();
    }

    EXPECT_EQ(mismatch.load(std::memory_order_relaxed), 0);
}

TEST_F(TraceTest, TestMultiThreadsIsolation)
{
    std::string traceID1 = "";
    std::thread t1([&traceID1]() {
        TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
        traceID1 = Trace::Instance().GetTraceID();
    });
    t1.join();

    std::thread t2([&traceID1]() {
        auto traceID2 = Trace::Instance().GetTraceID();
        EXPECT_NE(traceID1, traceID2);
    });
    t2.join();
}

TEST_F(TraceTest, TestMultiThreadsDelivery)
{
    // Test set trace with id when strcpy_ does not return 0
    std::string str(40, 'a');
    TraceGuard traceGuard0 = Trace::Instance().SetTraceNewID(str);
    std::string traceID = "abc";
    TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceID);
    auto traceID1 = Trace::Instance().GetTraceID();
    std::thread t1([&traceID1]() {
        TraceGuard traceGuard1 = Trace::Instance().SetTraceNewID(traceID1);
        auto traceID2 = Trace::Instance().GetTraceID();
        EXPECT_EQ(traceID2, traceID1);
    });
    t1.join();
}

TEST_F(TraceTest, TestInvalidate)
{
    std::string traceID = "abc";
    TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceID);

    auto traceID1 = Trace::Instance().GetTraceID();
    EXPECT_EQ(traceID1, traceID);

    Trace::Instance().Invalidate();
    auto traceID2 = Trace::Instance().GetTraceID();
    EXPECT_NE(traceID2, traceID);
}

TEST_F(TraceTest, TestPrefix)
{
    const int loopCount = 3;
    std::vector<std::string> prefixList = { "TestPrefix", std::string(Trace::TRACEID_PREFIX_SIZE, 'a'),
                                            std::string(Trace::TRACEID_PREFIX_SIZE - 1, 'b'),
                                            std::string(Trace::TRACEID_PREFIX_SIZE + 1, 'c'), "" };
    for (const auto &prefix : prefixList) {
        Trace::Instance().SetPrefix(prefix);
        std::string realPrefix = prefix.empty() ? "" : prefix.substr(0, Trace::TRACEID_PREFIX_SIZE) + ";";
        auto traceIdLen = prefix.empty() ? Trace::TRACEID_PREFIX_SIZE : realPrefix.length() + Trace::SHORT_UUID_SIZE;
        for (int i = 0; i < loopCount; i++) {
            TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
            LOG(INFO) << "trace id:" << Trace::Instance().GetTraceID();
            EXPECT_TRUE(Trace::Instance().GetTraceID().find(realPrefix) == 0);
            EXPECT_TRUE(Trace::Instance().GetTraceID().length() == traceIdLen);

            Trace::Instance().SetTraceNewID(Trace::Instance().GetTraceID());
        }
    }

    DS_ASSERT_OK(Context::SetTraceId("traceid"));

    std::vector<std::string> traceIds = { "",
                                          "a",
                                          "123",
                                          "~!@#%^&*()_-=+",
                                          std::string(Trace::TRACEID_PREFIX_SIZE - 1, 'd'),
                                          std::string(Trace::TRACEID_PREFIX_SIZE, 'e') };

    for (const auto &id : traceIds) {
        DS_EXPECT_OK(Context::SetTraceId(id));
    }
    std::vector<std::string> invalidTraceIds = { "|", std::string(Trace::TRACEID_PREFIX_SIZE + 1, 'f') };

    for (const auto &id : invalidTraceIds) {
        DS_EXPECT_NOT_OK(Context::SetTraceId(id));
    }
}

TEST_F(TraceTest, TestSubTraceID)
{
    std::string traceID = "abc";
    TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceID);
    EXPECT_EQ(traceID, Trace::Instance().GetTraceID());

    {
        TraceGuard traceGuard = Trace::Instance().SetSubTraceID("123");
        EXPECT_EQ(traceID + "123", Trace::Instance().GetTraceID());
    }
    EXPECT_EQ(traceID, Trace::Instance().GetTraceID());

    std::string subTraceID = "456";
    TraceGuard traceGuard2 = Trace::Instance().SetSubTraceID("456");
    EXPECT_EQ(traceID + "456", Trace::Instance().GetTraceID());
    Trace::Instance().InvalidateSubTraceID();
    EXPECT_EQ(traceID, Trace::Instance().GetTraceID());

    int traceIdSize = 3;
    {
        TraceGuard traceGuard =
            Trace::Instance().SetSubTraceID(std::string(Trace::TRACEID_MAX_SIZE - traceIdSize, 's'));
        EXPECT_EQ(traceID + std::string(Trace::TRACEID_MAX_SIZE - traceIdSize, 's'), Trace::Instance().GetTraceID());
    }
    EXPECT_EQ(traceID, Trace::Instance().GetTraceID());

    std::string subTraceId = std::string(Trace::TRACEID_MAX_SIZE - traceIdSize + 1, 's');
    {
        TraceGuard traceGuard =
            Trace::Instance().SetSubTraceID(subTraceId);
        EXPECT_EQ(traceID + std::string(Trace::TRACEID_MAX_SIZE - traceIdSize, 's'), Trace::Instance().GetTraceID());
    }
    EXPECT_EQ(traceID, Trace::Instance().GetTraceID());

    Trace::Instance().Invalidate();
    EXPECT_EQ("", Trace::Instance().GetTraceID());

    {
        TraceGuard traceGuard = Trace::Instance().SetSubTraceID("123");
        EXPECT_EQ("123", Trace::Instance().GetTraceID());
    }
    EXPECT_EQ("", Trace::Instance().GetTraceID());
}
}  // namespace ut
}  // namespace datasystem
