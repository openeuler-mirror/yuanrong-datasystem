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

#include <string>
#include <thread>

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