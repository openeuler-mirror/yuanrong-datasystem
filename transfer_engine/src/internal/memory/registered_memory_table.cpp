#include "internal/memory/registered_memory_table.h"

#include <sys/random.h>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <functional>
#include <limits>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include "datasystem/transfer_engine/status_helper.h"

namespace datasystem {
namespace {

constexpr size_t K_MAX_RANGES_PER_READ_LEASE = 4096;
constexpr size_t K_MAX_ACTIVE_READ_LEASE_RANGES = 65536;

uint64_t GenerateLeaseToken()
{
    uint64_t token = 0;
    ssize_t randomBytes = -1;
    do {
        randomBytes = getrandom(&token, sizeof(token), GRND_NONBLOCK);
    } while (randomBytes < 0 && errno == EINTR);
    if (randomBytes == static_cast<ssize_t>(sizeof(token)) && token != 0) {
        return token;
    }

    static std::atomic<uint64_t> fallbackCounter{ 1 };
    const uint64_t now = static_cast<uint64_t>(std::chrono::steady_clock::now().time_since_epoch().count());
    const uint64_t instanceSalt =
        static_cast<uint64_t>(std::hash<std::atomic<uint64_t> *>{}(&fallbackCounter));
    token = now ^ fallbackCounter.fetch_add(1) ^ instanceSalt;
    return token == 0 ? 1 : token;
}

}  // namespace

bool RegisteredMemoryTable::AddRegion(const RegisteredRegion &region)
{
    return AddRegions({ region });
}

bool RegisteredMemoryTable::AddRegions(const std::vector<RegisteredRegion> &regions)
{
    std::lock_guard<std::mutex> lock(mutex_);
    if (!CanAddRegionsLocked(regions_, regions)) {
        return false;
    }
    regions_.insert(regions_.end(), regions.begin(), regions.end());
    return true;
}

bool RegisteredMemoryTable::CanAddRegions(const std::vector<RegisteredRegion> &regions) const
{
    std::lock_guard<std::mutex> lock(mutex_);
    return CanAddRegionsLocked(regions_, regions);
}

bool RegisteredMemoryTable::RemoveRegion(const RegisteredRegion &region)
{
    std::lock_guard<std::mutex> lock(mutex_);
    const auto oldSize = regions_.size();
    const auto isSameRegion = [&region](const RegisteredRegion &item) { return IsSameRegion(item, region); };
    regions_.erase(std::remove_if(regions_.begin(), regions_.end(), isSameRegion), regions_.end());
    return regions_.size() != oldSize;
}

bool RegisteredMemoryTable::RemoveByBaseAddr(uint64_t baseAddr)
{
    return RemoveByBaseAddrIfNoActiveLease(baseAddr) == RemoveResult::K_REMOVED;
}

RegisteredMemoryTable::RemoveResult RegisteredMemoryTable::RemoveByBaseAddrIfNoActiveLease(uint64_t baseAddr)
{
    std::vector<RegisteredRegion> removedRegions;
    return RemoveByBaseAddrsIfNoActiveLease({ baseAddr }, &removedRegions);
}

RegisteredMemoryTable::RemoveResult RegisteredMemoryTable::RemoveByBaseAddrsIfNoActiveLease(
    const std::vector<uint64_t> &baseAddrs, std::vector<RegisteredRegion> *removedRegions)
{
    if (baseAddrs.empty() || removedRegions == nullptr) {
        return RemoveResult::K_NOT_FOUND;
    }
    std::unordered_set<uint64_t> requestedBaseAddrs;
    requestedBaseAddrs.reserve(baseAddrs.size());
    for (const auto baseAddr : baseAddrs) {
        if (!requestedBaseAddrs.insert(baseAddr).second) {
            return RemoveResult::K_NOT_FOUND;
        }
    }

    const auto now = std::chrono::steady_clock::now();
    std::lock_guard<std::mutex> lock(mutex_);
    PruneExpiredLeasesLocked(now);

    std::unordered_map<uint64_t, std::vector<const RegisteredRegion *>> regionsByBaseAddr;
    regionsByBaseAddr.reserve(regions_.size());
    for (const auto &region : regions_) {
        regionsByBaseAddr[region.baseAddr].push_back(&region);
    }

    std::vector<RegisteredRegion> matches;
    matches.reserve(baseAddrs.size());
    for (const auto baseAddr : baseAddrs) {
        const auto iter = regionsByBaseAddr.find(baseAddr);
        if (iter == regionsByBaseAddr.end() || iter->second.empty()) {
            return RemoveResult::K_NOT_FOUND;
        }
        if (iter->second.size() > 1) {
            // Same baseAddr registered on multiple devices: removal by baseAddr alone cannot tell
            // which region is intended, so reject instead of silently dropping all but the first.
            return RemoveResult::K_AMBIGUOUS;
        }
        if (HasActiveLeaseForRegionLocked(*iter->second.front(), now)) {
            return RemoveResult::K_BUSY;
        }
        matches.push_back(*iter->second.front());
    }

    const auto isRemovedBaseAddr = [&requestedBaseAddrs](const RegisteredRegion &item) {
        return requestedBaseAddrs.find(item.baseAddr) != requestedBaseAddrs.end();
    };
    regions_.erase(std::remove_if(regions_.begin(), regions_.end(), isRemovedBaseAddr), regions_.end());
    *removedRegions = std::move(matches);
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
                                               const ReadLeaseRequester &requester, uint64_t ttlMs, uint64_t *leaseId)
{
    TE_CHECK_PTR_OR_RETURN(leaseId);
    TE_CHECK_OR_RETURN(!ranges.empty(), ErrorCode::kInvalid, "lease ranges is empty");
    TE_CHECK_OR_RETURN(ranges.size() <= K_MAX_RANGES_PER_READ_LEASE, ErrorCode::kInvalid,
                       "lease range count exceeds limit");
    TE_CHECK_OR_RETURN(deviceId >= 0, ErrorCode::kInvalid, "lease device_id is invalid");
    TE_CHECK_OR_RETURN(!requester.host.empty() && requester.port > 0 && requester.deviceId >= 0, ErrorCode::kInvalid,
                       "lease requester identity is invalid");
    TE_CHECK_OR_RETURN(ttlMs > 0, ErrorCode::kInvalid, "lease ttl should be positive");

    const auto now = std::chrono::steady_clock::now();
    std::lock_guard<std::mutex> lock(mutex_);
    PruneExpiredLeasesLocked(now);
    TE_CHECK_OR_RETURN(readLeaseAdmissionOpen_, ErrorCode::kNotReady, "read lease admission is closed");
    TE_CHECK_OR_RETURN(activeReadLeaseRanges_ <= K_MAX_ACTIVE_READ_LEASE_RANGES - ranges.size(),
                       ErrorCode::kNotReady, "active read lease ranges exceed limit");

    for (const auto &range : ranges) {
        TE_CHECK_OR_RETURN(range.addr > 0 && range.length > 0, ErrorCode::kInvalid, "invalid lease range");
        TE_CHECK_OR_RETURN(range.addr <= std::numeric_limits<uint64_t>::max() - range.length, ErrorCode::kInvalid,
                           "lease range overflow");
        bool found = false;
        for (const auto &region : regions_) {
            if (region.deviceId == deviceId && IsRangeInside(range.addr, range.length, region)) {
                found = true;
                break;
            }
        }
        TE_CHECK_OR_RETURN(found, ErrorCode::kNotAuthorized, "remote range is not registered");
    }

    uint64_t id = 0;
    do {
        id = GenerateLeaseToken();
    } while (id == 0 || readLeases_.find(id) != readLeases_.end());
    ReadLease lease;
    lease.ranges = ranges;
    lease.deviceId = deviceId;
    lease.requester = requester;
    lease.expireTime = now + std::chrono::milliseconds(ttlMs);
    readLeases_[id] = std::move(lease);
    activeReadLeaseRanges_ += ranges.size();
    *leaseId = id;
    return Result::OK();
}

bool RegisteredMemoryTable::ReleaseReadLease(uint64_t leaseId, const ReadLeaseRequester &requester)
{
    std::lock_guard<std::mutex> lock(mutex_);
    const auto iter = readLeases_.find(leaseId);
    if (iter == readLeases_.end() || iter->second.requester.host != requester.host
        || iter->second.requester.port != requester.port
        || iter->second.requester.deviceId != requester.deviceId) {
        return false;
    }
    activeReadLeaseRanges_ -= iter->second.ranges.size();
    readLeases_.erase(iter);
    leaseCv_.notify_all();
    return true;
}

void RegisteredMemoryTable::OpenReadLeaseAdmission()
{
    std::lock_guard<std::mutex> lock(mutex_);
    readLeaseAdmissionOpen_ = true;
}

void RegisteredMemoryTable::CloseReadLeaseAdmission()
{
    std::lock_guard<std::mutex> lock(mutex_);
    readLeaseAdmissionOpen_ = false;
}

bool RegisteredMemoryTable::WaitForNoActiveReadLeases(uint64_t timeoutMs)
{
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeoutMs);
    std::unique_lock<std::mutex> lock(mutex_);
    auto now = std::chrono::steady_clock::now();
    PruneExpiredLeasesLocked(now);
    while (!readLeases_.empty()) {
        if (now >= deadline) {
            return false;
        }
        auto wakeTime = deadline;
        for (const auto &entry : readLeases_) {
            wakeTime = std::min(wakeTime, entry.second.expireTime);
        }
        (void)leaseCv_.wait_until(lock, wakeTime);
        now = std::chrono::steady_clock::now();
        PruneExpiredLeasesLocked(now);
    }
    return true;
}

void RegisteredMemoryTable::Clear()
{
    std::lock_guard<std::mutex> lock(mutex_);
    regions_.clear();
    readLeases_.clear();
    activeReadLeaseRanges_ = 0;
    leaseCv_.notify_all();
}

bool RegisteredMemoryTable::IsSameRegion(const RegisteredRegion &left, const RegisteredRegion &right)
{
    return left.baseAddr == right.baseAddr && left.length == right.length && left.deviceId == right.deviceId
           && left.backingBaseAddr == right.backingBaseAddr && left.backingLength == right.backingLength;
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

bool RegisteredMemoryTable::IsValidRegion(const RegisteredRegion &region)
{
    const bool implicitBacking = region.backingBaseAddr == 0 && region.backingLength == 0;
    const uint64_t backingBaseAddr = implicitBacking ? region.baseAddr : region.backingBaseAddr;
    const uint64_t backingLength = implicitBacking ? region.length : region.backingLength;
    return region.baseAddr > 0 && region.length > 0 && region.deviceId >= 0 && backingBaseAddr > 0 && backingLength > 0
           && region.baseAddr <= std::numeric_limits<uint64_t>::max() - region.length
           && backingBaseAddr <= std::numeric_limits<uint64_t>::max() - backingLength
           && region.baseAddr >= backingBaseAddr && region.baseAddr + region.length <= backingBaseAddr + backingLength;
}

bool RegisteredMemoryTable::CanAddRegionsLocked(const std::vector<RegisteredRegion> &existing,
    const std::vector<RegisteredRegion> &regions)
{
    if (regions.empty()) {
        return false;
    }
    for (const auto &region : regions) {
        if (!IsValidRegion(region)) {
            return false;
        }
    }

    std::vector<const RegisteredRegion *> sortedRegions;
    sortedRegions.reserve(regions.size());
    for (const auto &region : regions) {
        for (const auto &item : existing) {
            if (item.deviceId == region.deviceId && IsOverlap(item, region)) {
                return false;
            }
        }
        sortedRegions.push_back(&region);
    }
    std::sort(sortedRegions.begin(), sortedRegions.end(),
        [](const RegisteredRegion *left, const RegisteredRegion *right) {
            if (left->deviceId != right->deviceId) {
                return left->deviceId < right->deviceId;
            }
            return left->baseAddr < right->baseAddr;
        });

    const RegisteredRegion *previous = nullptr;
    for (const auto *region : sortedRegions) {
        if (previous == nullptr || region->deviceId != previous->deviceId) {
            previous = region;
            continue;
        }
        if (IsOverlap(*previous, *region)) {
            return false;
        }
        previous = region;
    }
    return true;
}

void RegisteredMemoryTable::PruneExpiredLeasesLocked(std::chrono::steady_clock::time_point now)
{
    for (auto iter = readLeases_.begin(); iter != readLeases_.end();) {
        if (iter->second.expireTime <= now) {
            activeReadLeaseRanges_ -= iter->second.ranges.size();
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
