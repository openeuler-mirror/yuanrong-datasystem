#include <gtest/gtest.h>

#include <cstdint>
#include <chrono>
#include <string>
#include <vector>

#include "internal/connection/connection_manager.h"
#include "internal/memory/registered_memory_table.h"

namespace datasystem {
namespace {

ReadLeaseRequester DefaultRequester()
{
    return ReadLeaseRequester{ "requester", 12345, 1 };
}

// 中文说明：验证 ConnectionManager 初始状态下，链路未就绪且状态字段为默认值。
TEST(ConnectionManagerLltTest, DefaultNotReady)
{
    ConnectionManager mgr;
    ConnectionKey key{ 0, "127.0.0.1", 50001, 1 };

    EXPECT_FALSE(mgr.HasReadyConnection(key));
    ConnectionState state = mgr.GetState(key);
    EXPECT_FALSE(state.requesterRecvReady);
    EXPECT_FALSE(state.ownerSendReady);
}

// 中文说明：验证 ConnectionManager 在双方就绪、移除链路、再次建链时状态转换正确。
TEST(ConnectionManagerLltTest, ReadyRemoveRecover)
{
    ConnectionManager mgr;
    ConnectionKey key{ 2, "127.0.0.1", 50002, 3 };

    mgr.MarkRequesterRecvReady(key);
    EXPECT_FALSE(mgr.HasReadyConnection(key));

    mgr.MarkOwnerSendReady(key);
    EXPECT_TRUE(mgr.HasReadyConnection(key));

    mgr.Remove(key);
    EXPECT_FALSE(mgr.HasReadyConnection(key));

    // 中文说明：Remove 会清空该 key 状态，因此仅补一侧 ready 仍应 not ready。
    mgr.MarkRequesterRecvReady(key);
    EXPECT_FALSE(mgr.HasReadyConnection(key));
    mgr.MarkOwnerSendReady(key);
    EXPECT_TRUE(mgr.HasReadyConnection(key));
}

TEST(ConnectionManagerLltTest, BoundsAndClearsConnectionStates)
{
    ConnectionManager mgr;
    for (uint32_t i = 0; i < 5000; ++i) {
        mgr.MarkRequesterRecvReady(ConnectionKey{ 0, "peer-" + std::to_string(i), static_cast<uint16_t>(i), 1 });
    }
    EXPECT_LE(mgr.Size(), 4096U);
    mgr.Clear();
    EXPECT_EQ(mgr.Size(), 0U);
}

TEST(ConnectionManagerLltTest, CapacityPreservesReadyStateAndRecoversAfterRemove)
{
    constexpr size_t kConnectionStateCapacity = 4096;
    ConnectionManager mgr;
    const ConnectionKey oldKey{ 0, "old-peer", 50000, 1 };
    ASSERT_TRUE(mgr.MarkRequesterRecvReady(oldKey));
    ASSERT_TRUE(mgr.MarkOwnerSendReady(oldKey));

    for (size_t i = 0; i < kConnectionStateCapacity - 1; ++i) {
        const ConnectionKey key{ 0, "peer-" + std::to_string(i), static_cast<uint16_t>(i), 1 };
        ASSERT_TRUE(mgr.MarkRequesterRecvReady(key));
    }
    EXPECT_EQ(mgr.Size(), kConnectionStateCapacity);

    const ConnectionKey rejectedKey{ 0, "rejected-peer", 50001, 1 };
    EXPECT_FALSE(mgr.MarkRequesterRecvReady(rejectedKey));
    EXPECT_FALSE(mgr.MarkOwnerSendReady(rejectedKey));
    EXPECT_TRUE(mgr.HasReadyConnection(oldKey));
    EXPECT_FALSE(mgr.GetState(rejectedKey).requesterRecvReady);
    EXPECT_FALSE(mgr.GetState(rejectedKey).ownerSendReady);

    const ConnectionKey removedKey{ 0, "peer-0", 0, 1 };
    mgr.Remove(removedKey);
    EXPECT_EQ(mgr.Size(), kConnectionStateCapacity - 1);
    ASSERT_TRUE(mgr.MarkRequesterRecvReady(rejectedKey));
    ASSERT_TRUE(mgr.MarkOwnerSendReady(rejectedKey));
    EXPECT_TRUE(mgr.HasReadyConnection(rejectedKey));
    EXPECT_TRUE(mgr.HasReadyConnection(oldKey));
}

TEST(ConnectionManagerLltTest, IncomingOwnerStateEvictsOldestAfterSequentialRequesters)
{
    constexpr size_t kConnectionStateCapacity = 4096;
    ConnectionManager mgr;
    const auto keyFor = [](size_t i) { return ConnectionKey{ 0, "requester-" + std::to_string(i), 50000, 1 }; };
    for (size_t i = 0; i < kConnectionStateCapacity; ++i) {
        ASSERT_TRUE(mgr.MarkOwnerSendReadyWithOldestEviction(keyFor(i)));
    }
    ASSERT_TRUE(mgr.MarkRequesterRecvReady(keyFor(0)));
    ASSERT_TRUE(mgr.MarkOwnerSendReadyWithOldestEviction(keyFor(2)));
    ASSERT_TRUE(mgr.MarkOwnerSendReadyWithOldestEviction(keyFor(kConnectionStateCapacity)));

    EXPECT_EQ(mgr.Size(), kConnectionStateCapacity);
    EXPECT_TRUE(mgr.HasReadyConnection(keyFor(0)));
    EXPECT_FALSE(mgr.GetState(keyFor(1)).ownerSendReady);
    EXPECT_TRUE(mgr.GetState(keyFor(2)).ownerSendReady);
    EXPECT_TRUE(mgr.GetState(keyFor(kConnectionStateCapacity)).ownerSendReady);
}

TEST(ConnectionManagerLltTest, IncomingOwnerStatePreservesRequesterStatesAtCapacity)
{
    constexpr size_t kConnectionStateCapacity = 4096;
    ConnectionManager mgr;
    for (size_t i = 0; i < kConnectionStateCapacity; ++i) {
        ASSERT_TRUE(mgr.MarkRequesterRecvReady(ConnectionKey{ 0, "requester-" + std::to_string(i), 50000, 1 }));
    }
    const ConnectionKey newKey{ 0, "new-requester", 50000, 1 };
    EXPECT_FALSE(mgr.MarkOwnerSendReadyWithOldestEviction(newKey));
    EXPECT_EQ(mgr.Size(), kConnectionStateCapacity);
    EXPECT_TRUE(mgr.GetState(ConnectionKey{ 0, "requester-0", 50000, 1 }).requesterRecvReady);
}

// 中文说明：验证 RegisteredMemoryTable 对已注册范围、跨范围访问和错误设备号的判定逻辑。
TEST(RegisteredMemoryTableLltTest, RangeDeviceValidation)
{
    RegisteredMemoryTable table;
    RegisteredRegion region{ 0x1000, 0x100, 0 };
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

    RegisteredRegion valid{ 0x3000, 0x80, 1 };
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

// 中文说明：验证 HIXL read lease 会阻止 owner 侧提前反注册，释放 lease 后可正常反注册。
TEST(RegisteredMemoryTableLltTest, ReadLeaseBlocksRemoveUntilRelease)
{
    RegisteredMemoryTable table;
    ASSERT_TRUE(table.AddRegion(RegisteredRegion{ 0x6000, 0x100, 5 }));

    uint64_t leaseId = 0;
    ASSERT_TRUE(
        table.AcquireReadLease({ TransferMemoryRegion{ 0x6040, 0x20 } }, 5, DefaultRequester(), 30000, &leaseId)
            .IsOk());
    EXPECT_NE(leaseId, 0);
    EXPECT_EQ(table.RemoveByBaseAddrIfNoActiveLease(0x6000), RegisteredMemoryTable::RemoveResult::K_BUSY);

    EXPECT_FALSE(table.ReleaseReadLease(leaseId, ReadLeaseRequester{ "other", 12345, 1 }));
    EXPECT_EQ(table.RemoveByBaseAddrIfNoActiveLease(0x6000), RegisteredMemoryTable::RemoveResult::K_BUSY);
    EXPECT_TRUE(table.ReleaseReadLease(leaseId, DefaultRequester()));
    EXPECT_EQ(table.RemoveByBaseAddrIfNoActiveLease(0x6000), RegisteredMemoryTable::RemoveResult::K_REMOVED);
    EXPECT_EQ(table.RemoveByBaseAddrIfNoActiveLease(0x6000), RegisteredMemoryTable::RemoveResult::K_NOT_FOUND);
}

// 中文说明：验证 read lease 不允许授权未注册或跨界的远端地址。
TEST(RegisteredMemoryTableLltTest, ReadLeaseRejectsUnregisteredRange)
{
    RegisteredMemoryTable table;
    ASSERT_TRUE(table.AddRegion(RegisteredRegion{ 0x7000, 0x80, 6 }));

    uint64_t leaseId = 0;
    Result rc =
        table.AcquireReadLease({ TransferMemoryRegion{ 0x7070, 0x20 } }, 6, DefaultRequester(), 30000, &leaseId);
    EXPECT_EQ(rc.GetCode(), ErrorCode::kNotAuthorized);
    EXPECT_EQ(leaseId, 0);
}

TEST(RegisteredMemoryTableLltTest, BatchRemoveIsAtomicWhenOneRegionIsBusy)
{
    RegisteredMemoryTable table;
    ASSERT_TRUE(table.AddRegions({ RegisteredRegion{ 0x8000, 0x80, 7 }, RegisteredRegion{ 0x9000, 0x80, 7 } }));

    uint64_t leaseId = 0;
    ASSERT_TRUE(
        table.AcquireReadLease({ TransferMemoryRegion{ 0x9010, 0x10 } }, 7, DefaultRequester(), 30000, &leaseId)
            .IsOk());
    std::vector<RegisteredRegion> removedRegions;
    EXPECT_EQ(table.RemoveByBaseAddrsIfNoActiveLease({ 0x8000, 0x9000 }, &removedRegions),
              RegisteredMemoryTable::RemoveResult::K_BUSY);
    EXPECT_TRUE(removedRegions.empty());
    EXPECT_TRUE(table.IsRegistered(0x8010, 0x10, 7));
    EXPECT_TRUE(table.IsRegistered(0x9010, 0x10, 7));

    EXPECT_TRUE(table.ReleaseReadLease(leaseId, DefaultRequester()));
    EXPECT_EQ(table.RemoveByBaseAddrsIfNoActiveLease({ 0x8000, 0x9000 }, &removedRegions),
              RegisteredMemoryTable::RemoveResult::K_REMOVED);
    EXPECT_EQ(removedRegions.size(), 2U);
}

TEST(RegisteredMemoryTableLltTest, BatchRemoveRejectsDuplicateBaseAddress)
{
    RegisteredMemoryTable table;
    ASSERT_TRUE(table.AddRegions({ RegisteredRegion{ 0xe000, 0x80, 7 }, RegisteredRegion{ 0xf000, 0x80, 7 } }));

    std::vector<RegisteredRegion> removedRegions;
    EXPECT_EQ(table.RemoveByBaseAddrsIfNoActiveLease({ 0xe000, 0xe000 }, &removedRegions),
              RegisteredMemoryTable::RemoveResult::K_NOT_FOUND);
    EXPECT_TRUE(removedRegions.empty());
    EXPECT_TRUE(table.IsRegistered(0xe010, 0x10, 7));
    EXPECT_TRUE(table.IsRegistered(0xf010, 0x10, 7));
}

TEST(RegisteredMemoryTableLltTest, BatchRemoveRemovesAllRequestedRegions)
{
    RegisteredMemoryTable table;
    ASSERT_TRUE(table.AddRegions({ RegisteredRegion{ 0x10000, 0x80, 7 }, RegisteredRegion{ 0x11000, 0x80, 7 },
                                   RegisteredRegion{ 0x12000, 0x80, 7 } }));

    std::vector<RegisteredRegion> removedRegions;
    EXPECT_EQ(table.RemoveByBaseAddrsIfNoActiveLease({ 0x10000, 0x12000 }, &removedRegions),
              RegisteredMemoryTable::RemoveResult::K_REMOVED);
    EXPECT_EQ(removedRegions.size(), 2U);
    EXPECT_FALSE(table.IsRegistered(0x10010, 0x10, 7));
    EXPECT_TRUE(table.IsRegistered(0x11010, 0x10, 7));
    EXPECT_FALSE(table.IsRegistered(0x12010, 0x10, 7));
}

TEST(RegisteredMemoryTableLltTest, BatchAddAllowsAdjacentRegionsAndCrossDeviceSameAddress)
{
    RegisteredMemoryTable table;
    EXPECT_TRUE(table.AddRegions({ RegisteredRegion{ 0x13000, 0x100, 7 }, RegisteredRegion{ 0x13100, 0x100, 7 },
                                   RegisteredRegion{ 0x13000, 0x100, 8 } }));
    EXPECT_TRUE(table.IsRegistered(0x13010, 0x10, 7));
    EXPECT_TRUE(table.IsRegistered(0x13110, 0x10, 7));
    EXPECT_TRUE(table.IsRegistered(0x13010, 0x10, 8));
}

// 中文说明：跨设备同 baseAddr 的批量注销必须返回 K_AMBIGUOUS，且两个区域都原样保留，不得多删少报。
TEST(RegisteredMemoryTableLltTest, BatchRemoveRejectsCrossDeviceSameBaseAddrAsAmbiguous)
{
    RegisteredMemoryTable table;
    ASSERT_TRUE(table.AddRegions({ RegisteredRegion{ 0x13000, 0x100, 7 }, RegisteredRegion{ 0x13000, 0x100, 8 } }));

    std::vector<RegisteredRegion> removedRegions;
    EXPECT_EQ(table.RemoveByBaseAddrsIfNoActiveLease({ 0x13000 }, &removedRegions),
              RegisteredMemoryTable::RemoveResult::K_AMBIGUOUS);
    EXPECT_TRUE(removedRegions.empty());
    EXPECT_TRUE(table.IsRegistered(0x13010, 0x10, 7));
    EXPECT_TRUE(table.IsRegistered(0x13010, 0x10, 8));

    EXPECT_EQ(table.RemoveByBaseAddrIfNoActiveLease(0x13000), RegisteredMemoryTable::RemoveResult::K_AMBIGUOUS);
    EXPECT_TRUE(table.IsRegistered(0x13010, 0x10, 7));
    EXPECT_TRUE(table.IsRegistered(0x13010, 0x10, 8));
}

TEST(RegisteredMemoryTableLltTest, BatchAddRejectsOverlapsRegardlessOfInputOrder)
{
    RegisteredMemoryTable table;
    EXPECT_FALSE(table.AddRegions({ RegisteredRegion{ 0x14000, 0x40, 7 }, RegisteredRegion{ 0x14200, 0x40, 7 },
                                   RegisteredRegion{ 0x14020, 0x20, 7 } }));
    EXPECT_FALSE(table.IsRegistered(0x14010, 0x10, 7));

    ASSERT_TRUE(table.AddRegions({ RegisteredRegion{ 0x15000, 0x100, 7 }, RegisteredRegion{ 0x15200, 0x100, 7 } }));
    EXPECT_FALSE(table.AddRegions({ RegisteredRegion{ 0x15100, 0x120, 7 } }));
    EXPECT_TRUE(table.IsRegistered(0x15010, 0x10, 7));
    EXPECT_TRUE(table.IsRegistered(0x15210, 0x10, 7));
}

TEST(RegisteredMemoryTableLltTest, LeaseAdmissionClosesAndReopens)
{
    RegisteredMemoryTable table;
    ASSERT_TRUE(table.AddRegion(RegisteredRegion{ 0xa000, 0x80, 8 }));
    table.CloseReadLeaseAdmission();
    uint64_t leaseId = 0;
    Result closedRc =
        table.AcquireReadLease({ TransferMemoryRegion{ 0xa010, 0x10 } }, 8, DefaultRequester(), 30000, &leaseId);
    EXPECT_EQ(closedRc.GetCode(), ErrorCode::kNotReady);
    table.OpenReadLeaseAdmission();
    ASSERT_TRUE(
        table.AcquireReadLease({ TransferMemoryRegion{ 0xa010, 0x10 } }, 8, DefaultRequester(), 30000, &leaseId)
            .IsOk());
    EXPECT_TRUE(table.ReleaseReadLease(leaseId, DefaultRequester()));
    EXPECT_TRUE(table.WaitForNoActiveReadLeases(1));
}

TEST(RegisteredMemoryTableLltTest, LeaseWaitWakesAtExpiry)
{
    RegisteredMemoryTable table;
    ASSERT_TRUE(table.AddRegion(RegisteredRegion{ 0xb000, 0x80, 9 }));
    uint64_t leaseId = 0;
    ASSERT_TRUE(table.AcquireReadLease({ TransferMemoryRegion{ 0xb010, 0x10 } }, 9, DefaultRequester(), 20, &leaseId)
                    .IsOk());
    const auto start = std::chrono::steady_clock::now();
    EXPECT_TRUE(table.WaitForNoActiveReadLeases(500));
    EXPECT_LT(std::chrono::steady_clock::now() - start, std::chrono::milliseconds(400));
}

TEST(RegisteredMemoryTableLltTest, ReadLeaseRejectsTooManyRanges)
{
    RegisteredMemoryTable table;
    std::vector<TransferMemoryRegion> ranges(4097, TransferMemoryRegion{ 0xc000, 1 });
    uint64_t leaseId = 0;
    Result rc = table.AcquireReadLease(ranges, 10, DefaultRequester(), 30000, &leaseId);
    EXPECT_EQ(rc.GetCode(), ErrorCode::kInvalid);
    EXPECT_EQ(leaseId, 0);
}

TEST(RegisteredMemoryTableLltTest, ReadLeaseBoundsTotalActiveRanges)
{
    RegisteredMemoryTable table;
    ASSERT_TRUE(table.AddRegion(RegisteredRegion{ 0xd000, 0x100, 11 }));
    std::vector<TransferMemoryRegion> ranges(4096, TransferMemoryRegion{ 0xd000, 1 });
    std::vector<uint64_t> leaseIds;
    for (int i = 0; i < 16; ++i) {
        uint64_t leaseId = 0;
        ASSERT_TRUE(table.AcquireReadLease(ranges, 11, DefaultRequester(), 30000, &leaseId).IsOk());
        leaseIds.push_back(leaseId);
    }
    uint64_t rejectedLeaseId = 0;
    Result rejected = table.AcquireReadLease(ranges, 11, DefaultRequester(), 30000, &rejectedLeaseId);
    EXPECT_EQ(rejected.GetCode(), ErrorCode::kNotReady);
    for (const uint64_t leaseId : leaseIds) {
        EXPECT_TRUE(table.ReleaseReadLease(leaseId, DefaultRequester()));
    }
}

}  // namespace
}  // namespace datasystem
