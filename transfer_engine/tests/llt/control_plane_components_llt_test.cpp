#include <gtest/gtest.h>

#include <sys/socket.h>
#include <unistd.h>

#include <memory>
#include <string>
#include <vector>

#include "internal/control_plane/control_plane.h"
#include "internal/control_plane/control_plane_codec.h"
#include "internal/control_plane/socket_rpc_transport.h"
#include "internal/control_plane/transfer_control_dispatcher.h"

namespace datasystem {
namespace {

class DispatcherFakeService final : public ITransferControlService {
public:
    Result ExchangeRootInfo(const ExchangeRootInfoRequest &req, ExchangeRootInfoResponse *rsp) override
    {
        if (rsp == nullptr) {
            return Result(ErrorCode::kRuntimeError, "rsp is null");
        }
        rsp->code = 0;
        rsp->msg = "exchange_ok_" + req.requesterHost;
        rsp->ownerDeviceId = 9;
        return Result::OK();
    }

    Result QueryConnReady(const QueryConnReadyRequest &, QueryConnReadyResponse *rsp) override
    {
        if (rsp == nullptr) {
            return Result(ErrorCode::kRuntimeError, "rsp is null");
        }
        rsp->code = 0;
        rsp->msg = "query_ok";
        rsp->ready = true;
        return Result::OK();
    }

    Result ReadTrigger(const ReadTriggerRequest &, ReadTriggerResponse *rsp) override
    {
        if (rsp == nullptr) {
            return Result(ErrorCode::kRuntimeError, "rsp is null");
        }
        rsp->code = 0;
        rsp->msg = "read_ok";
        return Result::OK();
    }

    Result BatchReadTrigger(const BatchReadTriggerRequest &, BatchReadTriggerResponse *rsp) override
    {
        if (rsp == nullptr) {
            return Result(ErrorCode::kRuntimeError, "rsp is null");
        }
        rsp->code = 0;
        rsp->msg = "batch_read_ok";
        rsp->failedItemIndex = -1;
        return Result::OK();
    }
};

// 中文说明：验证 control_plane_codec 对 Exchange 请求的编解码能保持字段一致。
TEST(ControlPlaneCodecLltTest, ExchangeReqRoundTrip)
{
    ExchangeRootInfoRequest in;
    in.requesterHost = "127.0.0.1";
    in.requesterPort = 12345;
    in.requesterDeviceId = 3;
    in.ownerDeviceId = -1;
    in.rootInfo = std::string("ab\0cd", 5);

    const auto payload = EncodeExchangeReq(in);
    ExchangeRootInfoRequest out;
    ASSERT_TRUE(DecodeExchangeReq(payload, &out));
    EXPECT_EQ(out.requesterHost, in.requesterHost);
    EXPECT_EQ(out.requesterPort, in.requesterPort);
    EXPECT_EQ(out.requesterDeviceId, in.requesterDeviceId);
    EXPECT_EQ(out.ownerDeviceId, in.ownerDeviceId);
    EXPECT_EQ(out.rootInfo, in.rootInfo);
}

// 中文说明：验证 socket_rpc_transport 的帧收发，确保方法号和 payload 能正确传输。
TEST(SocketRpcTransportLltTest, SocketPairSendRecv)
{
    int fd[2] = {-1, -1};
    ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, fd), 0);

    std::vector<uint8_t> sendPayload {1, 2, 3, 4, 5};
    ASSERT_TRUE(SendFrame(fd[0], RpcMethod::kQueryConnReady, sendPayload).IsOk());

    RpcMethod method;
    std::vector<uint8_t> recvPayload;
    ASSERT_TRUE(RecvFrame(fd[1], &method, &recvPayload).IsOk());
    EXPECT_EQ(method, RpcMethod::kQueryConnReady);
    EXPECT_EQ(recvPayload, sendPayload);

    close(fd[0]);
    close(fd[1]);
}

// 中文说明：验证 dispatcher 能正确路由 Exchange 请求并生成对应响应 payload。
TEST(TransferControlDispatcherLltTest, ExchangeDispatchEncodeResp)
{
    auto service = std::make_shared<DispatcherFakeService>();

    ExchangeRootInfoRequest req;
    req.requesterHost = "host_a";
    req.requesterPort = 20001;
    req.requesterDeviceId = 1;
    req.ownerDeviceId = -1;
    req.rootInfo = "root";

    RpcMethod rspMethod;
    std::vector<uint8_t> rspPayload;
    ASSERT_TRUE(DispatchControlRequest(service, RpcMethod::kExchangeRootInfo, EncodeExchangeReq(req), &rspMethod,
                                       &rspPayload)
                    .IsOk());
    EXPECT_EQ(rspMethod, RpcMethod::kExchangeRootInfo);

    ExchangeRootInfoResponse rsp;
    ASSERT_TRUE(DecodeExchangeRsp(rspPayload, &rsp));
    EXPECT_EQ(rsp.code, 0);
    EXPECT_EQ(rsp.msg, "exchange_ok_host_a");
    EXPECT_EQ(rsp.ownerDeviceId, 9);
}

// 中文说明：验证 dispatcher 在请求 payload 非法时会返回 kInvalid 错误码。
TEST(TransferControlDispatcherLltTest, InvalidPayloadReturnsInvalid)
{
    auto service = std::make_shared<DispatcherFakeService>();

    RpcMethod rspMethod;
    std::vector<uint8_t> rspPayload;
    std::vector<uint8_t> badPayload {0x01, 0x02};
    ASSERT_TRUE(DispatchControlRequest(service, RpcMethod::kReadTrigger, badPayload, &rspMethod, &rspPayload).IsOk());
    EXPECT_EQ(rspMethod, RpcMethod::kReadTrigger);

    ReadTriggerResponse rsp;
    ASSERT_TRUE(DecodeReadRsp(rspPayload, &rsp));
    EXPECT_EQ(rsp.code, static_cast<int32_t>(ErrorCode::kInvalid));
}

}  // namespace
}  // namespace datasystem
