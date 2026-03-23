#include <gtest/gtest.h>

#include <cstdint>
#include <memory>
#include <vector>

#include "internal/connection/connection_manager.h"
#include "internal/backend/mock_data_plane_backend.h"
#include "internal/memory/registered_memory_table.h"

namespace datasystem {
namespace {

// 中文说明：验证 ConnectionManager 初始状态下，链路未就绪且状态字段为默认值。
TEST(ConnectionManagerLltTest, DefaultNotReady)
{
    ConnectionManager mgr;
    ConnectionKey key { 0, "127.0.0.1", 50001, 1 };

    EXPECT_FALSE(mgr.HasReadyConnection(key));
    ConnectionState state = mgr.GetState(key);
    EXPECT_FALSE(state.requesterRecvReady);
    EXPECT_FALSE(state.ownerSendReady);
    EXPECT_FALSE(state.stale);
}

// 中文说明：验证 ConnectionManager 在双方就绪、标记 stale、再次建链时状态转换正确。
TEST(ConnectionManagerLltTest, ReadyStaleRecover)
{
    ConnectionManager mgr;
    ConnectionKey key { 2, "127.0.0.1", 50002, 3 };

    mgr.MarkRequesterRecvReady(key);
    EXPECT_FALSE(mgr.HasReadyConnection(key));

    mgr.MarkOwnerSendReady(key);
    EXPECT_TRUE(mgr.HasReadyConnection(key));

    mgr.MarkStale(key);
    EXPECT_FALSE(mgr.HasReadyConnection(key));

    // 中文说明：MarkStale 现在会清空该 key 状态，因此仅补一侧 ready 仍应 not ready。
    mgr.MarkRequesterRecvReady(key);
    EXPECT_FALSE(mgr.HasReadyConnection(key));
    mgr.MarkOwnerSendReady(key);
    EXPECT_TRUE(mgr.HasReadyConnection(key));
}

// 中文说明：验证 RegisteredMemoryTable 对已注册范围、跨范围访问和错误设备号的判定逻辑。
TEST(RegisteredMemoryTableLltTest, RangeDeviceValidation)
{
    RegisteredMemoryTable table;
    RegisteredRegion region { 0x1000, 0x100, 0 };
    ASSERT_TRUE(table.AddRegion(region));

    EXPECT_TRUE(table.IsRegistered(0x1000, 0x40, 0));
    EXPECT_TRUE(table.IsRegistered(0x1080, 0x20, 0));
    EXPECT_FALSE(table.IsRegistered(0x0FF0, 0x20, 0));
    EXPECT_FALSE(table.IsRegistered(0x10F0, 0x40, 0));
    EXPECT_FALSE(table.IsRegistered(0x1000, 0x10, 1));
}

// 中文说明：验证 RegisteredMemoryTable 能拒绝非法区域（长度为0、地址溢出）并支持删除。
TEST(RegisteredMemoryTableLltTest, AddInvalidRemove)
{
    RegisteredMemoryTable table;

    EXPECT_FALSE(table.AddRegion(RegisteredRegion{ 0x2000, 0, 0 }));
    EXPECT_FALSE(table.AddRegion(RegisteredRegion{ UINT64_MAX - 7, 16, 0 }));

    RegisteredRegion valid { 0x3000, 0x80, 1 };
    ASSERT_TRUE(table.AddRegion(valid));
    EXPECT_TRUE(table.IsRegistered(0x3010, 0x10, 1));

    EXPECT_TRUE(table.RemoveRegion(valid));
    EXPECT_FALSE(table.IsRegistered(0x3010, 0x10, 1));
    EXPECT_FALSE(table.RemoveRegion(valid));
}

// 中文说明：验证按基地址反注册与按区间反查 device_id 的行为，且跨设备歧义会返回 false。
TEST(RegisteredMemoryTableLltTest, RemoveByBaseFindDevice)
{
    RegisteredMemoryTable table;
    ASSERT_TRUE(table.AddRegion(RegisteredRegion{ 0x4000, 0x80, 2 }));
    ASSERT_TRUE(table.AddRegion(RegisteredRegion{ 0x5000, 0x80, 3 }));
    ASSERT_TRUE(table.AddRegion(RegisteredRegion{ 0x4000, 0x80, 4 }));

    int32_t deviceId = -1;
    EXPECT_FALSE(table.FindDeviceIdByRange(0x4010, 0x10, &deviceId));
    EXPECT_TRUE(table.FindDeviceIdByRange(0x5010, 0x10, &deviceId));
    EXPECT_EQ(deviceId, 3);

    EXPECT_TRUE(table.RemoveByBaseAddr(0x5000));
    EXPECT_FALSE(table.FindDeviceIdByRange(0x5010, 0x10, &deviceId));
    EXPECT_FALSE(table.RemoveByBaseAddr(0x5000));
}

// 中文说明：验证 MockDataPlaneBackend 在双端建链后，可通过 PostRecv/PostSend/WaitRecv 完成数据拷贝。
TEST(MockDataPlaneBackendLltTest, SendRecvRoundTrip)
{
    auto sharedState = std::make_shared<MockDataPlaneBackend::SharedState>();
    MockDataPlaneBackend owner(sharedState);
    MockDataPlaneBackend requester(sharedState);

    ConnectionSpec recvSpec;
    recvSpec.localHost = "127.0.0.1";
    recvSpec.localPort = 51052;
    recvSpec.localDeviceId = 0;
    recvSpec.peerHost = "127.0.0.1";
    recvSpec.peerPort = 51051;
    recvSpec.peerDeviceId = 0;

    ConnectionSpec sendSpec;
    sendSpec.localHost = "127.0.0.1";
    sendSpec.localPort = 51051;
    sendSpec.localDeviceId = 0;
    sendSpec.peerHost = "127.0.0.1";
    sendSpec.peerPort = 51052;
    sendSpec.peerDeviceId = 0;

    ASSERT_TRUE(requester.InitRecv(recvSpec, "mock_root_info").IsOk());
    ASSERT_TRUE(owner.InitSend(sendSpec, "mock_root_info").IsOk());

    std::vector<uint8_t> src {1, 2, 3, 4, 5, 6, 7, 8};
    std::vector<uint8_t> dst(src.size(), 0);

    ASSERT_TRUE(requester.PostRecv(recvSpec, reinterpret_cast<uint64_t>(dst.data()), dst.size()).IsOk());
    ASSERT_TRUE(owner.PostSend(sendSpec, reinterpret_cast<uint64_t>(src.data()), src.size()).IsOk());
    ASSERT_TRUE(requester.WaitRecv(recvSpec, 1000).IsOk());
    EXPECT_EQ(src, dst);
}

// 中文说明：验证 MockDataPlaneBackend 在未收到对应 Send 时，WaitRecv 会超时返回 NotReady。
TEST(MockDataPlaneBackendLltTest, WaitRecvTimeout)
{
    auto sharedState = std::make_shared<MockDataPlaneBackend::SharedState>();
    MockDataPlaneBackend requester(sharedState);

    ConnectionSpec recvSpec;
    recvSpec.localHost = "127.0.0.1";
    recvSpec.localPort = 52052;
    recvSpec.localDeviceId = 0;
    recvSpec.peerHost = "127.0.0.1";
    recvSpec.peerPort = 52051;
    recvSpec.peerDeviceId = 0;

    std::vector<uint8_t> dst(16, 0);
    ASSERT_TRUE(requester.PostRecv(recvSpec, reinterpret_cast<uint64_t>(dst.data()), dst.size()).IsOk());
    Status rc = requester.WaitRecv(recvSpec, 10);
    EXPECT_EQ(rc.GetCode(), StatusCode::kNotReady);
}

}  // namespace
}  // namespace datasystem
