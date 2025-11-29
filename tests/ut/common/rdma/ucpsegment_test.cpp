#include <gtest/gtest.h>
#include <cstdlib>
#include <type_traits>
#include <memory>
#include <cstdint>

#include "ucp/api/ucp.h"
#include "ucp/api/ucp_def.h"

#define private public
#include "datasystem/common/rdma/ucp_segment.h"
#undef private
#include "common/rdma/mock_ucp_manager.h"
#include "common/rdma/create_ucp_context.h"

namespace datasystem {
namespace ut {

class UcpSegmentTest : public ::testing::Test {
protected:
    ucp_context_h ucpContext_;
    std::unique_ptr<MockUcpManager> ucpManager_;
    std::unique_ptr<CreateUcpContext> cUcpContext_;
    std::unique_ptr<UcpSegment> ucpSegment_;

    void *buffer_;
    size_t buf_size_ = 1024;

    void SetUp() override
    {
        ucpManager_ = std::make_unique<MockUcpManager>();
        cUcpContext_ = std::make_unique<CreateUcpContext>();
        ucpContext_ = cUcpContext_->GetContext();

        buffer_ = malloc(buf_size_);
        uintptr_t bufferInt = reinterpret_cast<uintptr_t>(buffer_);

        ucpSegment_ = std::make_unique<UcpSegment>(bufferInt, buf_size_, ucpContext_);
        Status status = ucpSegment_->Init();
        ASSERT_EQ(status, Status::OK());
    }

    void TearDown() override
    {
        ucpSegment_.reset();
        ucpManager_.reset();
        cUcpContext_.reset();
        if (buffer_ != nullptr) {
            free(buffer_);
            buffer_ = nullptr;
        }
    }
};

TEST_F(UcpSegmentTest, TestGetPackedRkey)
{
    auto rkey = ucpSegment_->GetPackedRkey();
    EXPECT_EQ(rkey, ucpSegment_->packedRkey_);
}

TEST_F(UcpSegmentTest, TestGetLocalSegAddr)
{
    auto localSegAddr = ucpSegment_->GetLocalSegAddr();
    EXPECT_EQ(localSegAddr, buffer_);
    EXPECT_EQ(localSegAddr, ucpSegment_->memBuffer_);
}

TEST_F(UcpSegmentTest, TestGetLocalSegSize)
{
    auto localSegSize = ucpSegment_->GetLocalSegSize();
    EXPECT_EQ(localSegSize, buf_size_);
    EXPECT_EQ(ucpSegment_->memSize_, buf_size_);
}

}  // namespace ut
}  // namespace datasystem