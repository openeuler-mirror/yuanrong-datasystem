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
#ifndef DATASYSTEM_TEST_UT_COMMON_RPC_STUB_CACHE_MGR_RPC_STUB_CACHE_MGR_TEST_HELPER_H
#define DATASYSTEM_TEST_UT_COMMON_RPC_STUB_CACHE_MGR_RPC_STUB_CACHE_MGR_TEST_HELPER_H

#include <gtest/gtest.h>

#include <memory>
#include <string>

#include "common.h"
#include "datasystem/common/rpc/rpc_stub_base.h"
#include "datasystem/common/rpc/rpc_stub_cache_mgr.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace ut {
class RpcStubForTest : public RpcStubBase {
public:
    RpcStubForTest(std::string msg) : msg_(msg){};

    ~RpcStubForTest();

    std::string GetMsg()
    {
        return msg_;
    }

    Status GetInitStatus() override
    {
        return Status::OK();
    }

private:
    std::string msg_;
};

class RpcStubCacheMgrForTest : public RpcStubCacheMgr {
public:
    static RpcStubCacheMgrForTest &Instance()
    {
        static RpcStubCacheMgrForTest instance;
        return instance;
    };

    Status InitForTest(uint64_t maxStubCount, int maxFdNum = 100)
    {
        RETURN_IF_NOT_OK(Init(maxStubCount));
        creators_.emplace(
            StubType::TEST_TYPE_1, [this](const HostPort &hostPort, std::shared_ptr<RpcStubBase> &rpcStub) -> Status {
                (void)hostPort;
                rpcStub = std::make_shared<RpcStubForTest>(hostPort.ToString()
                                                           + std::to_string(static_cast<int>(StubType::TEST_TYPE_1)));
                CHECK_FAIL_RETURN_STATUS(BorrowFd(), K_RUNTIME_ERROR, "fd is exhausted");
                return Status::OK();
            });
        creators_.emplace(
            StubType::TEST_TYPE_2, [this](const HostPort &hostPort, std::shared_ptr<RpcStubBase> &rpcStub) -> Status {
                (void)hostPort;
                rpcStub = std::make_shared<RpcStubForTest>(hostPort.ToString()
                                                           + std::to_string(static_cast<int>(StubType::TEST_TYPE_2)));
                CHECK_FAIL_RETURN_STATUS(BorrowFd(), K_RUNTIME_ERROR, "fd is exhausted");
                return Status::OK();
            });
        creators_.emplace(
            StubType::TEST_TYPE_3, [](const HostPort &hostPort, std::shared_ptr<RpcStubBase> &rpcStub) -> Status {
                (void)hostPort;
                (void)rpcStub;
                RETURN_STATUS(K_RUNTIME_ERROR, "test create failed");
            });
        remainingFd_ = maxFdNum;
        return Status::OK();
    };

    std::string ToString()
    {
        return lruCache_->ToString(true, true);
    }

    Status Remove(const HostPort &hostPort, StubType type)
    {
        RETURN_IF_NOT_OK(RpcStubCacheMgr::Remove(hostPort, type));
        ReturnFd();
        return Status::OK();
    }

    bool BorrowFd()
    {
        return remainingFd_.fetch_sub(1) >= 0;
    }

    void ReturnFd()
    {
        remainingFd_.fetch_add(1);
    }

    void SetMaxFdNum(int maxFdNum)
    {
        remainingFd_ = maxFdNum;
    }

private:
    RpcStubCacheMgrForTest() = default;

    std::atomic<int> remainingFd_{ 0 };
};
}  // namespace ut
}  // namespace datasystem
#endif  // DATASYSTEM_TEST_UT_COMMON_RPC_STUB_CACHE_MGR_RPC_STUB_CACHE_MGR_TEST_HELPER_H
