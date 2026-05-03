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

/**
 * Description: RpcStubCacheMgr class test.
 */

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "ut/common.h"
#include "datasystem/common/rpc/rpc_stub_base.h"
#include "datasystem/common/rpc/rpc_stub_cache_mgr.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/random_data.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/utils/status.h"
#include "common/rpc_stub_cache_mgr/rpc_stub_cache_mgr_test_helper.h"

namespace datasystem {
namespace ut {
class RpcStubCacheMgrTest : public CommonTest {
public:
    void BasicTest()
    {
        RandomData rand;
        int loopNum = 10000;
        int rangeMax = 10;
        int typNum = 2;
        int removeInterval = 10;
        for (auto i = 0; i < loopNum; i++) {
            int startNum = rand.GetRandomUint64(0, rangeMax - 1);
            int endNum = rand.GetRandomUint64(startNum + 1, rangeMax);
            int typeId = rand.GetRandomIndex(typNum);
            StubType type;
            if (typeId == 0) {
                type = StubType::TEST_TYPE_1;
            } else {
                type = StubType::TEST_TYPE_2;
            }
            TestHelper(startNum, endNum, type);
            if (i != 0 && i % removeInterval == 0) {
                int removeIndex = rand.GetRandomIndex(rangeMax);
                HostPort hostPort("127.0.0.1", removeIndex);
                auto status1 = RpcStubCacheMgrForTest::Instance().Remove(hostPort, StubType::TEST_TYPE_1);
                auto status2 = RpcStubCacheMgrForTest::Instance().Remove(hostPort, StubType::TEST_TYPE_2);
                ASSERT_TRUE(status1.GetCode() == K_OK || status1.GetCode() == K_NOT_FOUND);
                ASSERT_TRUE(status2.GetCode() == K_OK || status2.GetCode() == K_NOT_FOUND);
            }
        }
    }
   
private:
    void TestHelper(int startNum, int endNum, StubType type)
    {
        for (auto i = startNum; i < endNum; i++) {
            HostPort hostPort("127.0.0.1", i);
            std::shared_ptr<RpcStubBase> rpcStub;
            DS_ASSERT_OK(RpcStubCacheMgrForTest::Instance().GetStub(hostPort, type, rpcStub));
            auto ptr = std::dynamic_pointer_cast<RpcStubForTest>(rpcStub);
            ASSERT_NE(ptr, nullptr);
            ASSERT_EQ(ptr->GetMsg(), hostPort.ToString() + std::to_string(static_cast<int>(type)));
        }
    }
};

TEST_F(RpcStubCacheMgrTest, BasicTest)
{
    int cacheNum = 5;
    DS_ASSERT_OK(RpcStubCacheMgrForTest::Instance().InitForTest(cacheNum));
    BasicTest();
}

TEST_F(RpcStubCacheMgrTest, BoundaryTest)
{
    int cacheNum = 1;
    DS_ASSERT_OK(RpcStubCacheMgrForTest::Instance().InitForTest(cacheNum));
    BasicTest();
}

TEST_F(RpcStubCacheMgrTest, TestSomeStubsAlwaysHeldByUser)
{
    int cacheNum = 5;
    DS_ASSERT_OK(RpcStubCacheMgrForTest::Instance().InitForTest(cacheNum));
    HostPort hostPort("127.0.0.2", 0);
    std::shared_ptr<RpcStubBase> rpcStub;
    DS_ASSERT_OK(RpcStubCacheMgrForTest::Instance().GetStub(hostPort, StubType::TEST_TYPE_1, rpcStub));
    BasicTest();
}

TEST_F(RpcStubCacheMgrTest, TestCacheFullAndCannotEvict)
{
    int cacheNum = 2;
    int keyIndex0 = 0, keyIndex1 = 1, keyIndex2 = 2;
    DS_ASSERT_OK(RpcStubCacheMgrForTest::Instance().InitForTest(cacheNum));
    HostPort hostPort0("127.0.0.1", keyIndex0);
    std::shared_ptr<RpcStubBase> rpcStub0;
    DS_ASSERT_OK(RpcStubCacheMgrForTest::Instance().GetStub(hostPort0, StubType::TEST_TYPE_1, rpcStub0));
    HostPort hostPort1("127.0.0.1", keyIndex1);
    std::shared_ptr<RpcStubBase> rpcStub1;
    DS_ASSERT_OK(RpcStubCacheMgrForTest::Instance().GetStub(hostPort1, StubType::TEST_TYPE_1, rpcStub1));
    HostPort hostPort2("127.0.0.1", keyIndex2);
    std::shared_ptr<RpcStubBase> rpcStub2;
    auto rc = RpcStubCacheMgrForTest::Instance().GetStub(hostPort2, StubType::TEST_TYPE_1, rpcStub2);
    ASSERT_EQ(rc.GetCode(), K_RUNTIME_ERROR);
}

TEST_F(RpcStubCacheMgrTest, TestParallelGetStub)
{
    int cacheNum = 10, maxFdNum = 10, loopNum = 10000;
    DS_ASSERT_OK(RpcStubCacheMgrForTest::Instance().InitForTest(cacheNum, maxFdNum));

    auto func = [&loopNum](int threadNum) {
        for (auto i = 0; i < loopNum; i++) {
            HostPort hostPort("127.0.0." + std::to_string(threadNum), i);
            std::shared_ptr<RpcStubBase> rpcStub;
            DS_ASSERT_OK(RpcStubCacheMgrForTest::Instance().GetStub(hostPort, StubType::TEST_TYPE_1, rpcStub));
            auto ptr = std::dynamic_pointer_cast<RpcStubForTest>(rpcStub);
            ASSERT_NE(ptr, nullptr);
            ASSERT_EQ(ptr->GetMsg(), hostPort.ToString() + std::to_string(static_cast<int>(StubType::TEST_TYPE_1)));
        }
    };

    std::vector<std::thread> threads;
    int threadNum = 20;
    for (int i = 0; i < threadNum; i++) {
        threads.push_back(std::thread(func, i));
    }
    for (auto &t : threads) {
        t.join();
    }
}

TEST_F(RpcStubCacheMgrTest, TestCreateStubFailed)
{
    int cacheNum = 10;
    DS_ASSERT_OK(RpcStubCacheMgrForTest::Instance().InitForTest(cacheNum));
    HostPort hostPort("127.0.0.1", 0);
    std::shared_ptr<RpcStubBase> rpcStub;
    DS_ASSERT_NOT_OK(RpcStubCacheMgrForTest::Instance().GetStub(hostPort, StubType::TEST_TYPE_3, rpcStub));
    ASSERT_EQ(0, RpcStubCacheMgrForTest::Instance().Size());
}

TEST_F(RpcStubCacheMgrTest, TestParallelCreateStubFailedAndGetStub)
{
    int cacheNum = 10;
    DS_ASSERT_OK(RpcStubCacheMgrForTest::Instance().InitForTest(cacheNum));
    auto func = []() {
        int loopNum = 10000;
        for (int i = 0; i < loopNum; ++i) {
            HostPort hostPort("127.0.0.1", 0);
            std::shared_ptr<RpcStubBase> rpcStub;
            DS_ASSERT_NOT_OK(RpcStubCacheMgrForTest::Instance().GetStub(hostPort, StubType::TEST_TYPE_3, rpcStub));
        }
    };

    std::vector<std::thread> threads;
    int threadNum = 2;
    for (int i = 0; i < threadNum; i++) {
        threads.push_back(std::thread(func));
    }
    for (auto &t : threads) {
        t.join();
    }

    ASSERT_EQ(0, RpcStubCacheMgrForTest::Instance().Size());
}
}  // namespace ut
}  // namespace datasystem
