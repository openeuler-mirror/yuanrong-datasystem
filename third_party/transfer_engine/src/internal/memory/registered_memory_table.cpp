#include "internal/memory/registered_memory_table.h"

#include <algorithm>
#include <limits>

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
    std::lock_guard<std::mutex> lock(mutex_);
    const auto oldSize = regions_.size();
    regions_.erase(
        std::remove_if(regions_.begin(), regions_.end(),
                       [&](const RegisteredRegion &item) { return item.baseAddr == baseAddr; }),
        regions_.end());
    return regions_.size() != oldSize;
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

}  // namespace datasystem
