/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
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
 * Description: UcpEndpoint test.
 */

#include <cstdlib>
#include <cstdint>
#include <gtest/gtest.h>
#include <memory>
#include <type_traits>

#include "ucp/api/ucp.h"
#include "ucp/api/ucp_def.h"

#include "common/rdma/create_ucp_context.h"
#include "common/rdma/mimic_remote_server.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/rdma/ucp_dlopen_util.h"
#define private public
#include "datasystem/common/rdma/ucp_endpoint.h"
#undef private
#include "datasystem/common/rdma/ucp_manager.h"

namespace datasystem {
namespace ut {

class UcpEndpointTest : public ::testing::Test {
protected:
    std::unique_ptr<UcpEndpoint> ucpEp_;
    std::unique_ptr<MimicRemoteServer> localServer_;
    std::unique_ptr<MimicRemoteServer> remoteServer_;

    UcpManager *ucpManager_{ nullptr };
    std::unique_ptr<CreateUcpContext> cUcpContext_;

    ucp_context_h context_;

    void SetUp() override
    {
        bool dlopenInitResult = datasystem::ucp_dlopen::Init();
        EXPECT_EQ(dlopenInitResult, true);
        ucpManager_ = &UcpManager::Instance();
        cUcpContext_ = std::make_unique<CreateUcpContext>();
        context_ = cUcpContext_->GetContext();

        localServer_ = std::make_unique<MimicRemoteServer>(context_);
        remoteServer_ = std::make_unique<MimicRemoteServer>(context_);

        localServer_->InitUcpWorker();
        remoteServer_->InitUcpWorker();
        remoteServer_->InitUcpSegment();

        VLOG(INFO) << FormatString("Remote Address: %d", (uintptr_t)remoteServer_->GetLocalSegAddr());

        ucpEp_ = std::make_unique<UcpEndpoint>(localServer_->GetWorker(), remoteServer_->GetWorkerAddr());
        Status status = ucpEp_->Init();
        ASSERT_EQ(status, Status::OK());
    }

    void TearDown() override
    {
        ucpEp_.reset();
        localServer_.reset();
        remoteServer_.reset();
        ucpManager_ = nullptr;
        cUcpContext_.reset();
    }

    void WaitForCompletion(ucs_status_ptr_t status_ptr)
    {
        if (UCS_PTR_IS_PTR(status_ptr)) {
            ucs_status_t status;
            do {
                ucp_worker_progress(localServer_->GetWorker());  
                status = ucp_request_check_status(status_ptr);
            } while (status == UCS_INPROGRESS);

            EXPECT_EQ(UCS_OK, status);  
            ucp_request_free(status_ptr);
        } else {
            EXPECT_EQ(UCS_OK, UCS_PTR_STATUS(status_ptr));
        }
    }
};

TEST_F(UcpEndpointTest, TestEpGenerated)
{
    auto call1 = ucpEp_->GetEp();
    auto call2 = ucpEp_->GetEp();

    EXPECT_EQ(call1, call2);
    EXPECT_NE(std::addressof(call1), std::addressof(call2));
}

TEST_F(UcpEndpointTest, TestUnpackRkeyWithEmptyRkey)
{
    std::string emptyRkey = "";
    ucp_rkey_h rkey0 = ucpEp_->GetOrUnpackRkey(emptyRkey);
    ucp_rkey_h rkey1 = ucpEp_->GetOrUnpackRkey(emptyRkey);
    EXPECT_EQ(rkey0, rkey1);
}

TEST_F(UcpEndpointTest, TestUnpackRkey)
{
    ucp_rkey_h rkey = ucpEp_->GetOrUnpackRkey(remoteServer_->GetPackedRkey());
    EXPECT_NE(rkey, nullptr);
}
}  // namespace ut
}  // namespace datasystem