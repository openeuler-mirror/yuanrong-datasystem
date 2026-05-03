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
 * Description: injection test.
 */
#include <string>
#include <thread>
#include <vector>

#include "ut/common.h"
#include "datasystem/common/inject/inject_point.h"

namespace datasystem {
namespace ut {
using namespace datasystem::inject;
class InjectPointTest : public CommonTest {};

TEST_F(InjectPointTest, TestParseTask)
{
    std::unique_ptr<Task> task;
    DS_EXPECT_OK(Task::ParseFromStr("off", task));
    DS_EXPECT_OK(Task::ParseFromStr("return(1)", task));
    DS_EXPECT_OK(Task::ParseFromStr("print(hi)", task));
    DS_EXPECT_OK(Task::ParseFromStr("sleep(1000)", task));
    DS_EXPECT_OK(Task::ParseFromStr("yield", task));
    DS_EXPECT_OK(Task::ParseFromStr("busy(1000)", task));
    DS_EXPECT_OK(Task::ParseFromStr("call", task));
    DS_EXPECT_OK(Task::ParseFromStr("pause", task));
    DS_EXPECT_OK(Task::ParseFromStr("50%100*return(1)", task));
    DS_EXPECT_OK(Task::ParseFromStr("50%return(1)", task));
    DS_EXPECT_OK(Task::ParseFromStr("100*return(1)", task));
    DS_EXPECT_OK(Task::ParseFromStr("return()", task));
    DS_EXPECT_OK(Task::ParseFromStr("return", task));
    DS_EXPECT_OK(Task::ParseFromStr("abort()", task));
    DS_EXPECT_OK(Task::ParseFromStr("abort", task));
    DS_EXPECT_NOT_OK(Task::ParseFromStr("return(", task));
    DS_EXPECT_NOT_OK(Task::ParseFromStr("abc%return(1)", task));
    DS_EXPECT_NOT_OK(Task::ParseFromStr("abc*return(1)", task));
    DS_EXPECT_NOT_OK(Task::ParseFromStr("sleep(abc)", task));
    DS_EXPECT_NOT_OK(Task::ParseFromStr("busy(abc)", task));
}

TEST_F(InjectPointTest, TestParseAction)
{
    std::shared_ptr<InjectPoint> injectPoint;
    DS_EXPECT_OK(InjectPoint::ParseFromStr("10*sleep(1000)->return(1)", injectPoint));
    ASSERT_EQ(injectPoint->GetTaskCount(), 2ul);
}

TEST_F(InjectPointTest, TestTaskOff)
{
    std::string value;
    std::shared_ptr<InjectPoint> injectPoint;
    DS_EXPECT_OK(InjectPoint::ParseFromStr("off", injectPoint));
    EXPECT_EQ(injectPoint->Execute("TestTaskOff", value), TaskType::OFF);
    EXPECT_EQ(injectPoint->GetExecuteCount(), 1ul);
    EXPECT_TRUE(value.empty());
}

TEST_F(InjectPointTest, TestTaskReturn)
{
    std::string value;
    std::shared_ptr<InjectPoint> injectPoint;
    DS_EXPECT_OK(InjectPoint::ParseFromStr("return(hello)", injectPoint));
    EXPECT_EQ(injectPoint->Execute("TestTaskReturn", value), TaskType::RETURN);
    EXPECT_EQ(injectPoint->GetExecuteCount(), 1ul);
    EXPECT_EQ(value, "hello");

    DS_EXPECT_OK(InjectPoint::ParseFromStr("1*return(hello)", injectPoint));
    value.clear();
    EXPECT_EQ(injectPoint->Execute("TestTaskReturn", value), TaskType::RETURN);
    EXPECT_EQ(injectPoint->GetExecuteCount(), 1ul);
    EXPECT_EQ(value, "hello");
    value.clear();
    EXPECT_EQ(injectPoint->Execute("TestTaskReturn", value), TaskType::NONE);
    EXPECT_EQ(injectPoint->GetExecuteCount(), 1ul);
    EXPECT_TRUE(value.empty());
}

TEST_F(InjectPointTest, TestTaskPrint)
{
    std::string value;
    std::shared_ptr<InjectPoint> injectPoint;
    DS_EXPECT_OK(InjectPoint::ParseFromStr("print(hello)", injectPoint));
    EXPECT_EQ(injectPoint->Execute("TestTaskPrint", value), TaskType::PRINT);
    EXPECT_EQ(injectPoint->GetExecuteCount(), 1ul);
    EXPECT_TRUE(value.empty());
}

TEST_F(InjectPointTest, TestTaskSleep)
{
    std::string value;
    std::shared_ptr<InjectPoint> injectPoint;
    auto t1 = std::chrono::system_clock::now();
    DS_EXPECT_OK(InjectPoint::ParseFromStr("1*sleep(1000)", injectPoint));
    EXPECT_EQ(injectPoint->Execute("TestTaskSleep", value), TaskType::SLEEP);
    EXPECT_EQ(injectPoint->GetExecuteCount(), 1ul);
    EXPECT_TRUE(value.empty());
    auto t2 = std::chrono::system_clock::now();
    EXPECT_GE(t2, t1 + std::chrono::milliseconds(1000));

    EXPECT_EQ(injectPoint->Execute("TestTaskSleep", value), TaskType::NONE);
    EXPECT_EQ(injectPoint->GetExecuteCount(), 1ul);
    EXPECT_TRUE(value.empty());
    auto t3 = std::chrono::system_clock::now();
    EXPECT_LT(t3, t2 + std::chrono::milliseconds(500));
}

TEST_F(InjectPointTest, TestTaskYield)
{
    std::string value;
    std::shared_ptr<InjectPoint> injectPoint;
    DS_EXPECT_OK(InjectPoint::ParseFromStr("yield", injectPoint));
    EXPECT_EQ(injectPoint->Execute("TestTaskYield", value), TaskType::YIELD);
    EXPECT_EQ(injectPoint->GetExecuteCount(), 1ul);
    EXPECT_TRUE(value.empty());
}

TEST_F(InjectPointTest, TestTaskBusy)
{
    std::string value;
    std::shared_ptr<InjectPoint> injectPoint;
    auto t1 = std::chrono::system_clock::now();
    DS_EXPECT_OK(InjectPoint::ParseFromStr("1*busy(1000)", injectPoint));
    EXPECT_EQ(injectPoint->Execute("TestTaskBusy", value), TaskType::BUSY);
    EXPECT_EQ(injectPoint->GetExecuteCount(), 1ul);
    EXPECT_TRUE(value.empty());
    auto t2 = std::chrono::system_clock::now();
    EXPECT_GE(t2, t1 + std::chrono::milliseconds(1000));

    EXPECT_EQ(injectPoint->Execute("TestTaskBusy", value), TaskType::NONE);
    EXPECT_EQ(injectPoint->GetExecuteCount(), 1ul);
    EXPECT_TRUE(value.empty());
    auto t3 = std::chrono::system_clock::now();
    EXPECT_LT(t3, t2 + std::chrono::milliseconds(500));
}

TEST_F(InjectPointTest, TestTaskCall)
{
    std::string value;
    std::shared_ptr<InjectPoint> injectPoint;
    DS_EXPECT_OK(InjectPoint::ParseFromStr("call(hello)", injectPoint));
    EXPECT_EQ(injectPoint->Execute("TestTaskCall", value), TaskType::CALL);
    EXPECT_EQ(injectPoint->GetExecuteCount(), 1ul);
    EXPECT_EQ(value, "hello");
}

TEST_F(InjectPointTest, TestTaskPause)
{
    std::shared_ptr<InjectPoint> injectPoint;
    DS_EXPECT_OK(InjectPoint::ParseFromStr("10*pause", injectPoint));

    const int thread_num = 10;
    std::vector<std::thread> threads;
    threads.reserve(thread_num);

    for (int i = 0; i < thread_num; i++) {
        threads.emplace_back([&injectPoint] {
            std::string value;
            EXPECT_EQ(injectPoint->Execute("TestTaskPause", value), TaskType::PAUSE);
            EXPECT_TRUE(value.empty());
        });
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    injectPoint->Clear();

    for (auto &thread : threads) {
        thread.join();
    }
    EXPECT_EQ(injectPoint->GetExecuteCount(), thread_num);
}

TEST_F(InjectPointTest, TestTaskAbort)
{
    std::string value;
    std::shared_ptr<InjectPoint> injectPoint;
    DS_EXPECT_OK(InjectPoint::ParseFromStr("abort(TestAbort)", injectPoint));
    EXPECT_DEATH(injectPoint->Execute("TestTaskAbort", value), "TestAbort");
}

TEST_F(InjectPointTest, TestInjectPoint)
{
    auto func1 = [] {
        INJECT_POINT("TestInjectPoint1");
        return Status::OK();
    };

    DS_EXPECT_OK(func1());
    EXPECT_EQ(inject::GetExecuteCount("TestInjectPoint1"), 0);
    inject::Set("TestInjectPoint1", "return(K_INVALID)");
    EXPECT_EQ(func1().GetCode(), StatusCode::K_INVALID);
    EXPECT_EQ(inject::GetExecuteCount("TestInjectPoint1"), 1);
    const int injectRetVal = 10;
    auto func2 = [] {
        INJECT_POINT("TestInjectPoint2", [] { return injectRetVal; });
        return 0;
    };

    EXPECT_EQ(func2(), 0);
    EXPECT_EQ(inject::GetExecuteCount("TestInjectPoint2"), 0);

    inject::Set("TestInjectPoint2", "return()");
    EXPECT_EQ(func2(), injectRetVal);
    EXPECT_EQ(inject::GetExecuteCount("TestInjectPoint2"), 1);
    EXPECT_EQ(func2(), injectRetVal);
    EXPECT_EQ(inject::GetExecuteCount("TestInjectPoint2"), 2);  // execute count 2
}

TEST_F(InjectPointTest, TestMutilTask1)
{
    auto func = [] {
        INJECT_POINT("TestInjectPoint");
        return Status::OK();
    };

    DS_EXPECT_OK(func());
    DS_EXPECT_OK(inject::Set("TestInjectPoint", "1*return(K_INVALID)->1*return(K_NOT_FOUND)"));
    EXPECT_EQ(func().GetCode(), StatusCode::K_INVALID);
    EXPECT_EQ(inject::GetExecuteCount("TestInjectPoint"), 1);
    EXPECT_EQ(func().GetCode(), StatusCode::K_NOT_FOUND);
    EXPECT_EQ(inject::GetExecuteCount("TestInjectPoint"), 2);  // execute count 2
    EXPECT_EQ(func().GetCode(), StatusCode::K_OK);
    EXPECT_EQ(inject::GetExecuteCount("TestInjectPoint"), 2);  // execute count 2
}

TEST_F(InjectPointTest, TestMutilTask2)
{
    auto func = [] {
        INJECT_POINT("TestInjectPoint1", [] { return inject::Set("TestInjectPoint2", "1*return(K_NOT_FOUND)"); });
        INJECT_POINT("TestInjectPoint2");
        return Status::OK();
    };

    DS_EXPECT_OK(func());
    DS_EXPECT_OK(inject::Set("TestInjectPoint1", "2*off->1*call()"));
    EXPECT_EQ(func().GetCode(), StatusCode::K_OK);
    EXPECT_EQ(inject::GetExecuteCount("TestInjectPoint1"), 1);
    EXPECT_EQ(inject::GetExecuteCount("TestInjectPoint2"), 0);
    EXPECT_EQ(func().GetCode(), StatusCode::K_OK);
    EXPECT_EQ(inject::GetExecuteCount("TestInjectPoint1"), 2);  // execute count 2
    EXPECT_EQ(inject::GetExecuteCount("TestInjectPoint2"), 0);
    EXPECT_EQ(func().GetCode(), StatusCode::K_NOT_FOUND);
    EXPECT_EQ(inject::GetExecuteCount("TestInjectPoint1"), 3);  // execute count 3
    EXPECT_EQ(inject::GetExecuteCount("TestInjectPoint2"), 1);
}

TEST_F(InjectPointTest, TestReturnVoidFunction)
{
    int val = 1;
    auto func = [&val] {
        INJECT_POINT("TestInjectPoint", [&val] { val += 10; });
        val += 100;
    };

    DS_EXPECT_OK(inject::Set("TestInjectPoint", "call()"));
    func();
    ASSERT_EQ(val, 111);

    val = 1;
    DS_EXPECT_OK(inject::Set("TestInjectPoint", "return()"));
    func();
    ASSERT_EQ(val, 11);
}

TEST_F(InjectPointTest, TestInjectParameters)
{
    auto func = [] {
        INJECT_POINT("TestInjectPoint1", [] { return std::string(""); });

        INJECT_POINT("TestInjectPoint2", [](const std::string &s) { return s; });

        INJECT_POINT("TestInjectPoint3", [](int64_t num) { return std::to_string(num); });

        INJECT_POINT("TestInjectPoint4",
                     [](int64_t num, const std::string &s) { return std::to_string(num) + ":" + s; });

        INJECT_POINT("TestInjectPoint5",
                     [](const std::string &s, int64_t num) { return s + ":" + std::to_string(num); });

        INJECT_POINT("TestInjectPoint6", [](int64_t a, int64_t b) { return std::to_string(a + b); });

        INJECT_POINT("TestInjectPoint7", [](const std::string &s1, const std::string &s2) { return s1 + ":" + s2; });
        return std::string("ok");
    };

    DS_EXPECT_OK(inject::Set("TestInjectPoint1", "1*return()"));
    EXPECT_EQ(func(), "");
    DS_EXPECT_OK(inject::Set("TestInjectPoint2", "1*return(hello)"));
    EXPECT_EQ(func(), "hello");
    DS_EXPECT_OK(inject::Set("TestInjectPoint3", "1*return(12345)"));
    EXPECT_EQ(func(), "12345");
    DS_EXPECT_OK(inject::Set("TestInjectPoint4", "1*return(12345,hello)"));
    EXPECT_EQ(func(), "12345:hello");
    DS_EXPECT_OK(inject::Set("TestInjectPoint5", "1*return(hello,12345)"));
    EXPECT_EQ(func(), "hello:12345");
    DS_EXPECT_OK(inject::Set("TestInjectPoint6", "1*return(11111,22222)"));
    EXPECT_EQ(func(), "33333");
    DS_EXPECT_OK(inject::Set("TestInjectPoint7", "1*return(hello,world)"));
    EXPECT_EQ(func(), "hello:world");
}

TEST_F(InjectPointTest, TestInjectPointByString)
{
    DS_EXPECT_OK(inject::SetByString(""));

    auto func = [] {
        INJECT_POINT("point1");
        INJECT_POINT("point2");
        return Status::OK();
    };

    DS_EXPECT_OK(func());

    inject::SetByString("point1:1*return(K_INVALID);point2:1*return(K_NOT_FOUND)");
    EXPECT_EQ(func().GetCode(), StatusCode::K_INVALID);
    EXPECT_EQ(func().GetCode(), StatusCode::K_NOT_FOUND);
}
}  // namespace ut
}  // namespace datasystem
