/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2024. All rights reserved.
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

#include <gmock/gmock.h>
#include "binmock.h"
#include "gmock/gmock.h"

namespace testing {
int CFunc(int i)
{
    return i;
}

class BaseClass {
public:
    int MemberFunc(int i)
    {
        return i;
    }
};

class SampleClass {
public:
    int CallCFunc(int i)
    {
        return CFunc(i);
    }

    int CallInternalMemberFunc(int i)
    {
        return CallCFunc(i);
    }

    int CallOutMemberFunc(int i)
    {
        return base_.MemberFunc(i);
    }

    int CallConstFunc(int i) const
    {
        return CFunc(i);
    }

    static int CallStatiCFunc(int i)
    {
        return i;
    }

private:
    BaseClass base_;
};

class BinMockSpec : public BinTest {
};

TEST_F(BinMockSpec, TestCallCFunc)
{
    BINEXPECT_CALL(&CFunc, (1)).Times(1).WillOnce(Return(0));

    SampleClass subject;
    EXPECT_EQ(0, subject.CallCFunc(1));
}

TEST_F(BinMockSpec, TestCallInternalMemberFunc)
{
    BINEXPECT_CALL(&SampleClass::CallCFunc, (1)).Times(1).WillOnce(Return(0));

    SampleClass subject;
    EXPECT_EQ(0, subject.CallInternalMemberFunc(1));
}

TEST_F(BinMockSpec, TestCallMemberFunc)
{
    BINEXPECT_CALL(&BaseClass::MemberFunc, (1)).Times(1).WillOnce(Return(0));

    SampleClass subject;
    EXPECT_EQ(0, subject.CallOutMemberFunc(1));
}

TEST_F(BinMockSpec, TestCallConstFunc)
{
    BINEXPECT_CALL((&SampleClass::CallConstFunc), (1)).Times(1).WillOnce(Return(0));

    SampleClass subject;
    EXPECT_EQ(0, subject.CallConstFunc(1));
}

TEST_F(BinMockSpec, TestCallStatiCFunc)
{
    BINEXPECT_CALL(&SampleClass::CallStatiCFunc, (1)).Times(1).WillOnce(Return(0));

    SampleClass subject;
    EXPECT_EQ(0, subject.CallStatiCFunc(1));
}

TEST_F(BinMockSpec, TestRelaseStubs)
{
    BINEXPECT_CALL(&CFunc, (1)).Times(1).WillOnce(Return(0));
    EXPECT_EQ(0, CFunc(1));

    RELEASE_STUBS
    EXPECT_EQ(1, CFunc(1));
}

TEST_F(BinMockSpec, TestSameStubMultipleExpectations)
{
    auto &mocker = BINMOCKER(&CFunc);
    EXPECT_CALL(mocker, stub(1)).Times(1).WillOnce(Return(0));
    EXPECT_CALL(mocker, stub(2)).Times(1).WillOnce(Return(10));
    EXPECT_EQ(0, CFunc(1));
    EXPECT_EQ(10, CFunc(2));

    BINEXPECT_CALL(&CFunc, (3)).Times(1).WillOnce(Return(100));
    BINEXPECT_CALL(&CFunc, (4)).Times(1).WillOnce(Return(1000));
    EXPECT_EQ(100, CFunc(3));
    EXPECT_EQ(1000, CFunc(4));
}

class template_class {
public:
    template <typename T>
    int templateFunc(T a)
    {
        (void)a;
        return -1;
    }
};

TEST_F(BinMockSpec, TestCallTemplateFunc)
{
    BINEXPECT_CALL((int (template_class::*)(int))(&template_class::templateFunc), (1)).Times(1).WillOnce(Return(0));

    template_class subject;
    EXPECT_EQ(subject.templateFunc(1), 0);
}

class OverloadClass {
public:
    int OverloadFunc(int i)
    {
        (void)i;
        return -1;
    }
    int OverloadFunc(double i)
    {
        (void)i;
        return -1;
    }
};

TEST_F(BinMockSpec, TestCallOverloadFunc)
{
    BINEXPECT_CALL((int (OverloadClass::*)(int))(&OverloadClass::OverloadFunc), (1)).Times(1).WillOnce(Return(0));

    OverloadClass subject;
    EXPECT_EQ(subject.OverloadFunc(1), 0);
}

int CFunc1()
{
    return 1;
}

int CFunc2()
{
    return 2;
}

TEST_F(BinMockSpec, TestCallSameTypeFunc)
{
    BINEXPECT_CALL((&CFunc1), ()).Times(1).WillOnce(Return(0));
    EXPECT_EQ(CFunc1(), 0);

    BINEXPECT_CALL((&CFunc2), ()).Times(1).WillOnce(Return(1));
    EXPECT_EQ(CFunc2(), 1);
}

int PassRvalue(int &&v)
{
    return v;
}

TEST_F(BinMockSpec, TestPassRvalue)
{
    BINEXPECT_CALL((&PassRvalue), (_)).Times(1).WillOnce(Return(0));
    EXPECT_EQ(PassRvalue(2), 0);
}

class InsClass {
public:
    static InsClass &instance()
    {
        static InsClass intance;
        return intance;
    }
    virtual std::string test()
    {
        return "instance binmock fail";
    }

protected:
    virtual ~InsClass() noexcept
    {
    }
    InsClass() noexcept
    {
    }
};
class InsClassMock : public InsClass {
public:
    virtual ~InsClassMock() noexcept
    {
    }
    InsClassMock() noexcept : InsClass::InsClass()
    {
    }
    std::string test() override
    {
        return "instance binmock success";
    }
};

TEST_F(BinMockSpec, TestWhenNormalThenSuccess)
{
    std::string expectResult("instance binmock success");
    InsClassMock mockIns;
    BINEXPECT_CALL(&InsClass::instance, ()).WillRepeatedly(ReturnRef(mockIns));
    ASSERT_EQ(InsClass::instance().test(), expectResult);
}

struct Param {
    std::string value;
};

void SetParam(std::string value, Param &param) {
    param.value = value;
}

TEST_F(BinMockSpec, TestChangeFuncParameter)
{
    std::string realValue = "test1";
    Param mockParam { .value = "test7" };
    BINEXPECT_CALL(&SetParam, (_, _)).Times(1).WillOnce(SetArgReferee<1>(mockParam));

    Param realParam;
    SetParam(realValue, realParam);
    ASSERT_EQ(realParam.value, "test7");
}
}  // namespace testing