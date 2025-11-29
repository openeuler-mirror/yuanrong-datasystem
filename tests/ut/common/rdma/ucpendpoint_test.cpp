#include <gtest/gtest.h>
#include <cstdlib>
#include <cstdint>
#include <type_traits>
#include <memory>

#include "ucp/api/ucp.h"
#include "ucp/api/ucp_def.h"

#include "datasystem/common/log/log.h"
#include "datasystem/common/util/format.h"
#define private public
#include "datasystem/common/rdma/ucp_endpoint.h"
#undef private
#include "common/rdma/mimic_remote_server.h"
#include "common/rdma/mock_ucp_manager.h"
#include "common/rdma/create_ucp_context.h"

namespace datasystem {
namespace ut {

class UcpEndpointTest : public ::testing::Test {
protected:
    std::unique_ptr<UcpEndpoint> ucpEp_;
    std::unique_ptr<MimicRemoteServer> localServer_;
    std::unique_ptr<MimicRemoteServer> remoteServer_;

    std::unique_ptr<MockUcpManager> ucpManager_;
    std::unique_ptr<CreateUcpContext> cUcpContext_;

    ucp_context_h context_;

    void SetUp() override
    {
        ucpManager_ = std::make_unique<MockUcpManager>();
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
        ucpManager_.reset();
        cUcpContext_.reset();
    }

    void WaitForCompletion(ucs_status_ptr_t status_ptr)
    {
        if (UCS_PTR_IS_PTR(status_ptr)) {
            ucs_status_t status;
            do {
                ucp_worker_progress(localServer_->GetWorker());  // 推进worker
                status = ucp_request_check_status(status_ptr);
            } while (status == UCS_INPROGRESS);

            EXPECT_EQ(UCS_OK, status);  // 确保操作成功
            ucp_request_free(status_ptr);
        } else {
            // 立即完成的情况
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
    EXPECT_EQ(ucpEp_->UnpackRkey(emptyRkey), Status::OK());
}

TEST_F(UcpEndpointTest, TestUnpackRkey)
{
    EXPECT_EQ(ucpEp_->UnpackRkey(remoteServer_->GetPackedRkey()), Status::OK());
    EXPECT_NE(ucpEp_->GetUnpackedRkey(), nullptr);
}
}  // namespace ut
}  // namespace datasystem