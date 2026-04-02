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
 * Description: Status basic function test.
 */
#include "datasystem/common/log/trace.h"
#include "datasystem/common/util/status_helper.h"

#include "ut/common.h"

namespace datasystem {
namespace ut {
class StatusTest : public CommonTest {
public:
    static Status ReturnUnexpectedError(const std::string &msg)
    {
        RETURN_STATUS(StatusCode::K_UNKNOWN_ERROR, msg);
    }

    static Status ReturnNullError(const int *ptr)
    {
        RETURN_RUNTIME_ERROR_IF_NULL(ptr);
        return Status::OK();
    }

    static Status ReturnIfNotOk(const Status &status)
    {
        RETURN_IF_NOT_OK(status);
        return Status::OK();
    }

    static Status ReturnIfFail(bool condition, const std::string &msg)
    {
        CHECK_FAIL_RETURN_STATUS(condition, StatusCode::K_RUNTIME_ERROR, msg);
        return Status::OK();
    }

    static Status ReturnIfFailPrintError(bool condition, const StatusCode &code, const std::string &msg)
    {
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(condition, code, msg);
        return Status::OK();
    }
};

TEST_F(StatusTest, TestMarco)
{
    LOG(INFO) << "Test Status Marco.";
    {
        Status status = ReturnUnexpectedError("Hello World!");
        ASSERT_EQ(status.GetCode(), StatusCode::K_UNKNOWN_ERROR);
        ASSERT_TRUE(status.GetMsg().find("Hello World!") != std::string::npos);
    }
    {
        int *integerPtr = nullptr;
        Status status = ReturnNullError(integerPtr);
        ASSERT_EQ(status.GetCode(), StatusCode::K_RUNTIME_ERROR);
        ASSERT_TRUE(status.GetMsg().find("The pointer [ptr] is null.") != std::string::npos);
        int i = 1;
        integerPtr = &i;
        status = ReturnNullError(integerPtr);
        ASSERT_EQ(status.GetCode(), StatusCode::K_OK);
    }
    {
        Status s(StatusCode::K_UNKNOWN_ERROR, "Welcome to Wild Rift!");
        Status status = ReturnIfNotOk(s);
        ASSERT_EQ(status.GetCode(), StatusCode::K_UNKNOWN_ERROR);
        ASSERT_TRUE(status.GetMsg().find("Welcome to Wild Rift!") != std::string::npos);
        s = Status::OK();
        status = ReturnIfNotOk(s);
        ASSERT_EQ(status.GetCode(), StatusCode::K_OK);
    }
    {
        Status status = ReturnIfFail(true, "The world is not black or white, but a delicious shade of grey.");
        ASSERT_EQ(status.GetCode(), StatusCode::K_OK);
        status = ReturnIfFail(false, "The world is not black or white, but a delicious shade of grey.");
        ASSERT_EQ(status.GetCode(), StatusCode::K_RUNTIME_ERROR);
        ASSERT_TRUE(status.GetMsg().find("The world is not black or white, but a delicious shade of grey.")
                    != std::string::npos);
    }
    {
        Status status = ReturnIfFailPrintError(true, K_OK, "It's cool.");
        ASSERT_EQ(status.GetCode(), StatusCode::K_OK);
        status = ReturnIfFailPrintError(false, K_INVALID, "It's cool.");
        ASSERT_EQ(status.GetCode(), StatusCode::K_INVALID);
        ASSERT_TRUE(status.GetMsg().find("It's cool.") != std::string::npos);
    }
}

TEST_F(StatusTest, TestMoveConstructor)
{
    LOG(INFO) << "Test Status move constructor";
    {
        Status one = Status::OK();
        Status another = std::move(one);
        ASSERT_TRUE(one.IsOk());
        ASSERT_TRUE(another.IsOk());
    }
    {
        Status one = Status(StatusCode::K_UNKNOWN_ERROR, "It's not lies that cut but the sharpness of the truth.");
        Status another = std::move(one);
        ASSERT_TRUE(one.IsOk());
        ASSERT_FALSE(another.IsOk());
    }
}

TEST_F(StatusTest, TestMoveAssignment)
{
    LOG(INFO) << "Test Status move assignment";
    {
        Status one = Status::OK();
        Status another(StatusCode::K_UNKNOWN_ERROR, "Morality is a beautiful servant and a dangerous master.");
        another = one;
        ASSERT_TRUE(one.IsOk());
        ASSERT_TRUE(another.IsOk());
    }
    {
        Status one(StatusCode::K_UNKNOWN_ERROR, "Morality is a beautiful servant and a dangerous master.");
        Status another = Status::OK();
        another = std::move(one);
        ASSERT_TRUE(one.IsOk());
        ASSERT_FALSE(another.IsOk());
        ASSERT_EQ(another.GetMsg(), "Morality is a beautiful servant and a dangerous master.");
    }
}

TEST_F(StatusTest, TestOperatorEqual)
{
    LOG(INFO) << "Test Status operator==";
    {
        Status one = Status::OK();
        Status another = Status::OK();
        ASSERT_TRUE(one == another);
    }
    {
        Status one = Status::OK();
        Status another(StatusCode::K_RUNTIME_ERROR, "Results are all that matters.");
        ASSERT_FALSE(one == another);
    }
    {
        Status one(StatusCode::K_RUNTIME_ERROR, "I will not be misled.");
        Status another(StatusCode::K_RUNTIME_ERROR, "Results are all that matters.");
        ASSERT_TRUE(one == another);
    }
    {
        Status one(StatusCode::K_UNKNOWN_ERROR, "I will not be misled.");
        Status another(StatusCode::K_RUNTIME_ERROR, "Results are all that matters.");
        ASSERT_FALSE(one == another);
    }
}

TEST_F(StatusTest, TestOperatorNotEqual)
{
    LOG(INFO) << "Test Status operator!=";
    {
        Status one = Status::OK();
        Status another = Status::OK();
        ASSERT_FALSE(one != another);
    }
    {
        Status one = Status::OK();
        Status another(StatusCode::K_RUNTIME_ERROR, "Results are all that matters.");
        ASSERT_TRUE(one != another);
    }
    {
        Status one(StatusCode::K_RUNTIME_ERROR, "I will not be misled.");
        Status another(StatusCode::K_RUNTIME_ERROR, "Results are all that matters.");
        ASSERT_FALSE(one != another);
    }
    {
        Status one(StatusCode::K_UNKNOWN_ERROR, "I will not be misled.");
        Status another(StatusCode::K_RUNTIME_ERROR, "Results are all that matters.");
        ASSERT_TRUE(one != another);
    }
}

TEST_F(StatusTest, TestStreamOperator)
{
    LOG(INFO) << "Test Status operator<<";
    Status status(StatusCode::K_OUT_OF_MEMORY, "Fight for the First Lands!");
    std::stringstream ss;
    ss << status;
    ASSERT_TRUE(ss.str().find("Fight for the First Lands!") != std::string::npos);
}

TEST_F(StatusTest, TestStatusLogForMat)
{
    Status status(StatusCode::K_RUNTIME_ERROR, "This is a msg.");
    status.AppendMsg("This is appended msg.");
    ASSERT_EQ(status.GetMsg(), "This is a msg. This is appended msg.");

    Status status1(StatusCode::K_RUNTIME_ERROR, "This is a msg");
    status1.AppendMsg("This is appended msg.");
    ASSERT_EQ(status1.GetMsg(), "This is a msg. This is appended msg.");

    Status status3(StatusCode::K_RUNTIME_ERROR, "");
    status3.AppendMsg("This is appended msg.");
    ASSERT_EQ(status3.GetMsg(), " This is appended msg.");

    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    Status status4(StatusCode::K_RUNTIME_ERROR, "This is a msg.");
    ASSERT_TRUE(status4.GetMsg().find("This is a msg, traceId: ") != std::string::npos);

    Status status5(StatusCode::K_RUNTIME_ERROR, "This is a msg");
    ASSERT_TRUE(status5.GetMsg().find("This is a msg, traceId: ") != std::string::npos);

    Status status6(StatusCode::K_RUNTIME_ERROR, status5.GetMsg());
    auto traceIdTagPos = status6.GetMsg().find(", traceId: ");
    ASSERT_NE(traceIdTagPos, std::string::npos);
    ASSERT_EQ(status6.GetMsg().find(", traceId: ", traceIdTagPos + 1), std::string::npos);
}
}  // namespace ut
}  // namespace datasystem
