#include "internal/memory/registered_memory_table.h"

#include <algorithm>
#include <limits>
#include <utility>

#include "datasystem/transfer_engine/status_helper.h"

namespace datasystem {

bool RegisteredMemoryTable::AddRegion(const RegisteredRegion &region)
{
    if (region.length == 0 || region.deviceId < 0 ||
        region.baseAddr > (std::numeric_limits<uint64_t>::max() - region.length)) {
        return false;
    }
    std::lock_guard<std::mutex> lock(mutex_);
    for (const auto &existing : regions_) {
        if (existing.deviceId == region.deviceId && IsOverlap(existing, region)) {
            return false;
        }
    }
    regions_.push_back(region);
    return true;
}

bool RegisteredMemoryTable::RemoveRegion(const RegisteredRegion &region)
{
    std::lock_guard<std::mutex> lock(mutex_);
    const auto oldSize = regions_.size();
    regions_.erase(
        std::remove_if(regions_.begin(), regions_.end(), [&](const RegisteredRegion &item) {
            return IsSameRegion(item, region);
        }),
        regions_.end());
    return regions_.size() != oldSize;
}

bool RegisteredMemoryTable::RemoveByBaseAddr(uint64_t baseAddr)
{
    return RemoveByBaseAddrIfNoActiveLease(baseAddr) == RemoveResult::K_REMOVED;
}

RegisteredMemoryTable::RemoveResult RegisteredMemoryTable::RemoveByBaseAddrIfNoActiveLease(uint64_t baseAddr)
{
    const auto now = std::chrono::steady_clock::now();
    std::lock_guard<std::mutex> lock(mutex_);
    PruneExpiredLeasesLocked(now);

    bool found = false;
    for (const auto &region : regions_) {
        if (region.baseAddr != baseAddr) {
            continue;
        }
        found = true;
        if (HasActiveLeaseForRegionLocked(region, now)) {
            return RemoveResult::K_BUSY;
        }
    }
    if (!found) {
        return RemoveResult::K_NOT_FOUND;
    }

    regions_.erase(
        std::remove_if(regions_.begin(), regions_.end(),
                       [&](const RegisteredRegion &item) { return item.baseAddr == baseAddr; }),
        regions_.end());
    return RemoveResult::K_REMOVED;
}

bool RegisteredMemoryTable::IsRegistered(uint64_t baseAddr, uint64_t length, int32_t deviceId) const
{
    if (length == 0 || deviceId < 0 || baseAddr > (std::numeric_limits<uint64_t>::max() - length)) {
        return false;
    }
    std::lock_guard<std::mutex> lock(mutex_);
    for (const auto &region : regions_) {
        if (region.deviceId == deviceId && IsRangeInside(baseAddr, length, region)) {
            return true;
        }
    }
    return false;
}

bool RegisteredMemoryTable::FindDeviceIdByRange(uint64_t baseAddr, uint64_t length, int32_t *deviceId) const
{
    if (deviceId == nullptr || length == 0 || baseAddr > (std::numeric_limits<uint64_t>::max() - length)) {
        return false;
    }
    std::lock_guard<std::mutex> lock(mutex_);
    int32_t matchedDeviceId = -1;
    for (const auto &region : regions_) {
        if (!IsRangeInside(baseAddr, length, region)) {
            continue;
        }
        if (matchedDeviceId < 0) {
            matchedDeviceId = region.deviceId;
            continue;
        }
        if (matchedDeviceId != region.deviceId) {
            return false;
        }
    }
    if (matchedDeviceId < 0) {
        return false;
    }
    *deviceId = matchedDeviceId;
    return true;
}

bool RegisteredMemoryTable::FindRegionByBaseAddr(uint64_t baseAddr, RegisteredRegion *region) const
{
    if (region == nullptr) {
        return false;
    }
    std::lock_guard<std::mutex> lock(mutex_);
    for (const auto &item : regions_) {
        if (item.baseAddr == baseAddr) {
            *region = item;
            return true;
        }
    }
    return false;
}

Result RegisteredMemoryTable::AcquireReadLease(const std::vector<TransferMemoryRegion> &ranges, int32_t deviceId,
                                               uint64_t ttlMs, uint64_t *leaseId)
{
    TE_CHECK_PTR_OR_RETURN(leaseId);
    TE_CHECK_OR_RETURN(!ranges.empty(), ErrorCode::kInvalid, "lease ranges is empty");
    TE_CHECK_OR_RETURN(deviceId >= 0, ErrorCode::kInvalid, "lease device_id is invalid");
    TE_CHECK_OR_RETURN(ttlMs > 0, ErrorCode::kInvalid, "lease ttl should be positive");

    const auto now = std::chrono::steady_clock::now();
    std::lock_guard<std::mutex> lock(mutex_);
    PruneExpiredLeasesLocked(now);

    for (const auto &range : ranges) {
        TE_CHECK_OR_RETURN(range.addr > 0 && range.length > 0, ErrorCode::kInvalid, "invalid lease range");
        TE_CHECK_OR_RETURN(range.addr <= std::numeric_limits<uint64_t>::max() - range.length,
                           ErrorCode::kInvalid, "lease range overflow");
        bool found = false;
        for (const auto &region : regions_) {
            if (region.deviceId == deviceId && IsRangeInside(range.addr, range.length, region)) {
                found = true;
                break;
            }
        }
        TE_CHECK_OR_RETURN(found, ErrorCode::kNotAuthorized, "remote range is not registered");
    }

    uint64_t id = nextLeaseId_++;
    if (id == 0) {
        id = nextLeaseId_++;
    }
    ReadLease lease;
    lease.ranges = ranges;
    lease.deviceId = deviceId;
    lease.expireTime = now + std::chrono::milliseconds(ttlMs);
    readLeases_[id] = std::move(lease);
    *leaseId = id;
    return Result::OK();
}

void RegisteredMemoryTable::ReleaseReadLease(uint64_t leaseId)
{
    std::lock_guard<std::mutex> lock(mutex_);
    readLeases_.erase(leaseId);
}

bool RegisteredMemoryTable::IsSameRegion(const RegisteredRegion &left, const RegisteredRegion &right)
{
    return left.baseAddr == right.baseAddr && left.length == right.length && left.deviceId == right.deviceId;
}

bool RegisteredMemoryTable::IsOverlap(const RegisteredRegion &left, const RegisteredRegion &right)
{
    const uint64_t leftEnd = left.baseAddr + left.length;
    const uint64_t rightEnd = right.baseAddr + right.length;
    return !(leftEnd <= right.baseAddr || rightEnd <= left.baseAddr);
}

bool RegisteredMemoryTable::IsRangeInside(uint64_t baseAddr, uint64_t length, const RegisteredRegion &region)
{
    const uint64_t endAddr = baseAddr + length;
    const uint64_t regionEnd = region.baseAddr + region.length;
    return baseAddr >= region.baseAddr && endAddr <= regionEnd;
}

bool RegisteredMemoryTable::IsRangeOverlap(uint64_t baseAddr, uint64_t length, const TransferMemoryRegion &range)
{
    const uint64_t endAddr = baseAddr + length;
    const uint64_t rangeEnd = range.addr + range.length;
    return !(endAddr <= range.addr || rangeEnd <= baseAddr);
}

void RegisteredMemoryTable::PruneExpiredLeasesLocked(std::chrono::steady_clock::time_point now)
{
    for (auto iter = readLeases_.begin(); iter != readLeases_.end();) {
        if (iter->second.expireTime <= now) {
            iter = readLeases_.erase(iter);
        } else {
            ++iter;
        }
    }
}

bool RegisteredMemoryTable::HasActiveLeaseForRegionLocked(const RegisteredRegion &region,
                                                          std::chrono::steady_clock::time_point now)
{
    for (const auto &leaseEntry : readLeases_) {
        const auto &lease = leaseEntry.second;
        if (lease.deviceId != region.deviceId || lease.expireTime <= now) {
            continue;
        }
        for (const auto &range : lease.ranges) {
            if (IsRangeOverlap(region.baseAddr, region.length, range)) {
                return true;
            }
        }
    }
    return false;
}

}  // namespace datasystem
