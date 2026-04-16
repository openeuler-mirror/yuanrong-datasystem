#include <cerrno>

#include <gtest/gtest.h>

#include "datasystem/common/rpc/unix_sock_fd.h"

namespace datasystem {

TEST(UnixSockFdStatusTest, ConnectResetMapsToRpcUnavailableWithPrefix)
{
    constexpr int kFakeFd = 42;
    auto status = UnixSockFd::ErrnoToStatus(ECONNRESET, kFakeFd);
    EXPECT_EQ(status.GetCode(), StatusCode::K_RPC_UNAVAILABLE);
    EXPECT_NE(status.GetMsg().find("[TCP_CONNECT_RESET]"), std::string::npos);
}

TEST(UnixSockFdStatusTest, EagainMapsToTryAgain)
{
    constexpr int kFakeFd = 42;
    auto status = UnixSockFd::ErrnoToStatus(EAGAIN, kFakeFd);
    EXPECT_EQ(status.GetCode(), StatusCode::K_TRY_AGAIN);
}

TEST(UnixSockFdStatusTest, EpipeMapsToRpcUnavailableWithPrefix)
{
    constexpr int kFakeFd = 42;
    auto status = UnixSockFd::ErrnoToStatus(EPIPE, kFakeFd);
    EXPECT_EQ(status.GetCode(), StatusCode::K_RPC_UNAVAILABLE);
    EXPECT_NE(status.GetMsg().find("[TCP_CONNECT_RESET]"), std::string::npos);
}

}  // namespace datasystem
