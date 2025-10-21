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

#ifndef __BINMOCK_HPP__
#define __BINMOCK_HPP__

#include <gmock/gmock.h>
#include <memory>
#include <map>
#include "function_stub.h"

namespace testing {
/*
 * The mocker provides stubs and gmock_stubs, integrating with the gmock framework, where the function name "stub" must
 * be consistent with the "stub" defined in the BINEXPECT_CALL macro.
 */
template <typename R, typename... P>
class BinFunctionMocker {
public:
    BinFunctionMocker(const std::string &name) : name_(name)
    {
    }

    R stub(P... p)
    {
        gmocker_.SetOwnerAndName(this, name_.c_str());
        return gmocker_.Invoke(std::forward<P>(p)...);
    }

    MockSpec<R(P...)> gmock_stub(const Matcher<P> &...p)
    {
        gmocker_.RegisterOwner(this);
        return gmocker_.With(p...);
    }

    FunctionMocker<R(P...)> gmocker_;
    const std::string name_;
};

class BaseStub {
public:
    virtual ~BaseStub() = default;
};

/*
 * StubMgr manager all stubs to search and clear resource.
 */
class StubMgr final {
public:
    static StubMgr &Instance();
    void Add(size_t id, std::shared_ptr<BaseStub> stub);
    std::shared_ptr<BaseStub> GetStub(size_t id);
    void Clear();

private:
    StubMgr() = default;
    StubMgr(const StubMgr &) = delete;
    StubMgr(StubMgr &&) = delete;
    StubMgr &operator=(const StubMgr &) = delete;
    StubMgr &operator=(StubMgr &&) = delete;

private:
    using stubs = std::map<size_t, std::shared_ptr<BaseStub>>;
    stubs stubs_;
};

/*
 * The BinFunctionMockerStub series of classes serve as a bridge between the stubbed functions and the static mocker
 * objects. As it involves binary replacement, the entry point is static, hence the mocker object is also static.
 * Functions that are stubbed generate corresponding static member functions and static mocker objects. During
 * construction, they are replaced for binary substitution, calling the static member functions finally.
 *
 * Declare template class BinFunctionMockerStub<U, R(P...)>,
 * Template parameter U is used to generate unique type, in order to address the issue of conflicts that arise when
 * function parameters and return values have the same type, leading to the generation of identical stubs.
 */
template <size_t U, typename T>
class BinFunctionMockerStub {
};

/*
 * Difference with
 * template <typename R, typename ... P> 与 template <typename R, typename C，typename ... P>
 */
template <size_t U, typename R, typename... P>
class BinFunctionMockerStub<U, R(P...)> : public BaseStub {
public:
    using MockerType = BinFunctionMocker<R, P...>;
    using MockerPtr = std::shared_ptr<MockerType>;

    BinFunctionMockerStub(void *funticon, const std::string &name)
    {
        patch_ = std::make_unique<FunctionStub>(funticon, AddrOf(&BinFunctionMockerStub::StaticFuncStub));
        MockerPtr &stub_ptr = GetStubPtr();
        stub_ptr = std::make_shared<MockerType>(name);
    }

    ~BinFunctionMockerStub()
    {
        GetStubPtr().reset();
    }

    MockerType &GetStub()
    {
        return *GetStubPtr();
    }

private:
    static R StaticFuncStub(P... p)
    {
        return GetStubPtr()->stub(std::forward<P>(p)...);
    }

    static MockerPtr &GetStubPtr()
    {
        static MockerPtr staticFunc;
        return staticFunc;
    }

private:
    std::unique_ptr<FunctionStub> patch_;
};

template <size_t U, typename R, typename C, typename... P>
class BinFunctionMockerStub<U, R (C::*)(P...)> : public BaseStub {
public:
    using MockerType = BinFunctionMocker<R, P...>;
    using MockerPtr = std::shared_ptr<MockerType>;

    BinFunctionMockerStub(void *funticon, const std::string &name)
    {
        patch_ = std::make_unique<FunctionStub>(funticon, AddrOf(&BinFunctionMockerStub::MemberFuncStub));
        MockerPtr &stub_ptr = GetStubPtr();
        stub_ptr = std::make_shared<MockerType>(name);
    }

    ~BinFunctionMockerStub()
    {
        GetStubPtr().reset();
    }

    MockerType &GetStub()
    {
        return *GetStubPtr();
    }

private:
    R MemberFuncStub(P... p)
    {
        return GetStubPtr()->stub(std::forward<P>(p)...);
    }

    static MockerPtr &GetStubPtr()
    {
        static MockerPtr staticFunc;
        return staticFunc;
    }

private:
    std::unique_ptr<FunctionStub> patch_;
    MockerPtr menber_func_;
};

/*
 * The stub factory class.
 */
class StubFactory final {
public:
    template <size_t U, typename R, typename... P>
    static decltype(auto) Create(R (*function)(P...), const std::string &name)
    {
        using StubType = BinFunctionMockerStub<U, R(P...)>;
        auto func_addr = AddrOf(function);
        return CommCreate<StubType>(U, func_addr, name);
    }

#define CREATE_FUNCTION_MOCKER_STUB(const_flag)                                              \
    template <size_t U, typename R, typename C, typename... P>                               \
    static decltype(auto) Create(R (C::*function)(P...) const_flag, const std::string &name) \
    {                                                                                        \
        using StubType = BinFunctionMockerStub<U, R (C::*)(P...)>;                           \
        auto func_addr = AddrOf(function);                                                   \
        return CommCreate<StubType>(U, func_addr, name);                                     \
    }

    CREATE_FUNCTION_MOCKER_STUB(const);
    CREATE_FUNCTION_MOCKER_STUB();

private:
    template <typename T>
    static T *CommCreate(size_t id, void *func_addr, const std::string &name)
    {
        auto stub = StubMgr::Instance().GetStub(id);
        if (stub != nullptr) {
            return (T *)stub.get();
        }
        stub = std::make_shared<T>(func_addr, name);
        StubMgr::Instance().Add(id, stub);
        return (T *)stub.get();
    }

    StubFactory() = delete;
};

/*
 * https://en.cppreference.com/w/cpp/language/template_parameters
 * Raw strings cannot be used as template arguments and must be converted
 * into numerical values, evaluated at compile time.
 * S2<"fail"> s2;        // error: string literal cannot be used
 * char okay[] = "okay"; // static object with linkage
 * S2<okay> s4;          // works
 */
constexpr size_t StrHash(const char *str)
{
    size_t result{ 0 };
    constexpr size_t prime{ 31 };
    while (*str) {
        result = *str + (result * prime);
        ++str;
    }
    return result;
}

#define BINMOCKER(method) testing::StubFactory::Create<testing::StrHash(#method)>((method), #method)->GetStub()
// use gmock first if you need to mock virtual function.
#define BINEXPECT_CALL(method, ...) EXPECT_CALL(BINMOCKER(method), stub __VA_ARGS__)
#define BINON_CALL(method, ...) ON_CALL(BINMOCKER(method), stub __VA_ARGS__)
#define RELEASE_STUBS testing::StubMgr::Instance().Clear();

class BinTest : public Test {
public:
    ~BinTest()
    {
        RELEASE_STUBS
    }
};
}  // namespace testing
#endif