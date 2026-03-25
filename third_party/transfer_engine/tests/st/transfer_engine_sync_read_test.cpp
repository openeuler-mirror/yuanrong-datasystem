#include <gtest/gtest.h>

#include <atomic>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "internal/backend/mock_data_plane_backend.h"
#include "datasystem/transfer_engine/transfer_engine.h"

namespace datasystem {
namespace {

Status BatchReadOne(TransferEngine *engine, const std::string &peerHost, uint16_t peerPort, uint64_t remoteAddr,
                    uint64_t localAddr, uint64_t length)
{
    if (engine == nullptr) {
        return Status(StatusCode::kRuntimeError, "engine is null");
    }
    const std::string targetHostname = peerHost + ":" + std::to_string(peerPort);
    return engine->BatchTransferSyncRead(
        targetHostname,
        {static_cast<uintptr_t>(localAddr)},
        {static_cast<uintptr_t>(remoteAddr)},
        {static_cast<size_t>(length)});
}

// 中文说明：验证使用 MockDataPlaneBackend 时，单项 BatchTransferSyncRead 能正确完成端到端内存拷贝。
TEST(TransferEngineSyncReadTest, SyncReadCopyMockOk)
{
    auto sharedState = std::make_shared<MockDataPlaneBackend::SharedState>();
    auto ownerBackend = std::make_shared<MockDataPlaneBackend>(sharedState);
    auto requesterBackend = std::make_shared<MockDataPlaneBackend>(sharedState);

    TransferEngine owner(ownerBackend);
    TransferEngine requester(requesterBackend);

    ASSERT_TRUE(owner.Initialize("127.0.0.1:61051", "ascend", "npu:0").IsOk());
    ASSERT_TRUE(requester.Initialize("127.0.0.1:61052", "ascend", "npu:1").IsOk());

    std::vector<uint8_t> src(256);
    std::vector<uint8_t> dst(256, 0);
    for (size_t i = 0; i < src.size(); ++i) {
        src[i] = static_cast<uint8_t>(i & 0xFF);
    }

    ASSERT_TRUE(owner.RegisterMemory(reinterpret_cast<uintptr_t>(src.data()), src.size()).IsOk());

    Status rc = BatchReadOne(&requester, "127.0.0.1", 61051, reinterpret_cast<uint64_t>(src.data()),
                             reinterpret_cast<uint64_t>(dst.data()), dst.size());
    ASSERT_TRUE(rc.IsOk()) << rc.GetMsg();
    EXPECT_EQ(src, dst);
}

// 中文说明：验证未注册内存时，单项 BatchTransferSyncRead 会被控制面授权检查拒绝。
TEST(TransferEngineSyncReadTest, SyncReadRejectUnregistered)
{
    auto sharedState = std::make_shared<MockDataPlaneBackend::SharedState>();
    auto ownerBackend = std::make_shared<MockDataPlaneBackend>(sharedState);
    auto requesterBackend = std::make_shared<MockDataPlaneBackend>(sharedState);

    TransferEngine owner(ownerBackend);
    TransferEngine requester(requesterBackend);

    ASSERT_TRUE(owner.Initialize("127.0.0.1:62051", "ascend", "npu:0").IsOk());
    ASSERT_TRUE(requester.Initialize("127.0.0.1:62052", "ascend", "npu:1").IsOk());

    std::vector<uint8_t> src(64, 1);
    std::vector<uint8_t> dst(64, 0);

    Status rc = BatchReadOne(&requester, "127.0.0.1", 62051, reinterpret_cast<uint64_t>(src.data()),
                             reinterpret_cast<uint64_t>(dst.data()), dst.size());
    EXPECT_EQ(rc.GetCode(), StatusCode::kNotAuthorized);
}

// 中文说明：验证单机并发场景下，多个 requester(不同 device_id) 同时从同一 owner 读取时都能成功。
TEST(TransferEngineSyncReadTest, ConcurrentRequestersMockOk)
{
    constexpr int kRequesterCount = 8;
    constexpr size_t kPayloadSize = 128;

    auto sharedState = std::make_shared<MockDataPlaneBackend::SharedState>();
    auto ownerBackend = std::make_shared<MockDataPlaneBackend>(sharedState);
    TransferEngine owner(ownerBackend);
    ASSERT_TRUE(owner.Initialize("127.0.0.1:63051", "ascend", "npu:0").IsOk());

    std::vector<std::vector<uint8_t>> srcBuffers(kRequesterCount, std::vector<uint8_t>(kPayloadSize, 0));
    for (int i = 0; i < kRequesterCount; ++i) {
        std::fill(srcBuffers[i].begin(), srcBuffers[i].end(), static_cast<uint8_t>(i + 1));
        ASSERT_TRUE(owner.RegisterMemory(reinterpret_cast<uintptr_t>(srcBuffers[i].data()), srcBuffers[i].size()).IsOk());
    }

    std::atomic<int> okCount{0};
    std::vector<std::thread> workers;
    workers.reserve(kRequesterCount);
    for (int i = 0; i < kRequesterCount; ++i) {
        workers.emplace_back([&, i]() {
            auto requesterBackend = std::make_shared<MockDataPlaneBackend>(sharedState);
            TransferEngine requester(requesterBackend);
            const uint16_t requesterPort = static_cast<uint16_t>(63052 + i);
            const int32_t requesterDeviceId = 1 + i;
            if (!requester.Initialize("127.0.0.1:" + std::to_string(requesterPort), "ascend",
                                      "npu:" + std::to_string(requesterDeviceId)).IsOk()) {
                return;
            }

            std::vector<uint8_t> dst(kPayloadSize, 0);
            Status rc = BatchReadOne(&requester, "127.0.0.1", 63051, reinterpret_cast<uint64_t>(srcBuffers[i].data()),
                                     reinterpret_cast<uint64_t>(dst.data()), dst.size());
            if (rc.IsOk() && dst == srcBuffers[i]) {
                okCount.fetch_add(1);
            }
            (void)requester.Finalize();
        });
    }

    for (auto &worker : workers) {
        worker.join();
    }

    EXPECT_EQ(okCount.load(), kRequesterCount);
    (void)owner.Finalize();
}

// 中文说明：验证单节点下，owner 进程可注册多个 npu_id 的内存区域，requester 可分别读取成功。
TEST(TransferEngineSyncReadTest, MultiNpuRegisteredReadOk)
{
    auto sharedState = std::make_shared<MockDataPlaneBackend::SharedState>();
    auto ownerBackend = std::make_shared<MockDataPlaneBackend>(sharedState);
    auto requesterBackend = std::make_shared<MockDataPlaneBackend>(sharedState);

    TransferEngine owner(ownerBackend);
    TransferEngine requester(requesterBackend);

    ASSERT_TRUE(owner.Initialize("127.0.0.1:64051", "ascend", "npu:0").IsOk());
    ASSERT_TRUE(requester.Initialize("127.0.0.1:64052", "ascend", "npu:1").IsOk());

    std::vector<uint8_t> srcA(128, 0x1A);
    std::vector<uint8_t> srcB(128, 0x2B);
    std::vector<uint8_t> dstA(128, 0);
    std::vector<uint8_t> dstB(128, 0);

    ASSERT_TRUE(owner.RegisterMemory(reinterpret_cast<uintptr_t>(srcA.data()), srcA.size()).IsOk());
    ASSERT_TRUE(owner.RegisterMemory(reinterpret_cast<uintptr_t>(srcB.data()), srcB.size()).IsOk());

    Status rcA = BatchReadOne(&requester, "127.0.0.1", 64051, reinterpret_cast<uint64_t>(srcA.data()),
                              reinterpret_cast<uint64_t>(dstA.data()), dstA.size());
    Status rcB = BatchReadOne(&requester, "127.0.0.1", 64051, reinterpret_cast<uint64_t>(srcB.data()),
                              reinterpret_cast<uint64_t>(dstB.data()), dstB.size());
    ASSERT_TRUE(rcA.IsOk()) << rcA.ToString();
    ASSERT_TRUE(rcB.IsOk()) << rcB.ToString();
    EXPECT_EQ(dstA, srcA);
    EXPECT_EQ(dstB, srcB);
}

// 中文说明：验证 BatchTransferSyncRead 可通过单次批量触发完成多个读取任务并校验数据。
TEST(TransferEngineSyncReadTest, BatchMultiRangeMockOk)
{
    auto sharedState = std::make_shared<MockDataPlaneBackend::SharedState>();
    auto ownerBackend = std::make_shared<MockDataPlaneBackend>(sharedState);
    auto requesterBackend = std::make_shared<MockDataPlaneBackend>(sharedState);

    TransferEngine owner(ownerBackend);
    TransferEngine requester(requesterBackend);

    ASSERT_TRUE(owner.Initialize("127.0.0.1:64151", "ascend", "npu:0").IsOk());
    ASSERT_TRUE(requester.Initialize("127.0.0.1:64152", "ascend", "npu:1").IsOk());

    std::vector<uint8_t> src0(64, 0x31);
    std::vector<uint8_t> src1(64, 0x42);
    std::vector<uint8_t> dst0(64, 0);
    std::vector<uint8_t> dst1(64, 0);
    ASSERT_TRUE(owner.RegisterMemory(reinterpret_cast<uintptr_t>(src0.data()), src0.size()).IsOk());
    ASSERT_TRUE(owner.RegisterMemory(reinterpret_cast<uintptr_t>(src1.data()), src1.size()).IsOk());

    Status rc = requester.BatchTransferSyncRead(
        "127.0.0.1:64151",
        {reinterpret_cast<uintptr_t>(dst0.data()), reinterpret_cast<uintptr_t>(dst1.data())},
        {reinterpret_cast<uintptr_t>(src0.data()), reinterpret_cast<uintptr_t>(src1.data())},
        {dst0.size(), dst1.size()});
    ASSERT_TRUE(rc.IsOk()) << rc.ToString();
    EXPECT_EQ(dst0, src0);
    EXPECT_EQ(dst1, src1);
}

}  // namespace
}  // namespace datasystem
