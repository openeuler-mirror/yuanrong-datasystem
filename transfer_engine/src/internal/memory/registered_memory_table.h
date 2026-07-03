#ifndef TRANSFER_ENGINE_INTERNAL_REGISTERED_MEMORY_TABLE_H
#define TRANSFER_ENGINE_INTERNAL_REGISTERED_MEMORY_TABLE_H

#include <chrono>
#include <cstdint>
#include <mutex>
#include <unordered_map>
#include <vector>

#include "datasystem/transfer_engine/data_plane_backend.h"
#include "datasystem/transfer_engine/status.h"

namespace datasystem {

struct RegisteredRegion {
    uint64_t baseAddr = 0;
    uint64_t length = 0;
    int32_t deviceId = -1;
};

class RegisteredMemoryTable {
public:
    enum class RemoveResult {
        K_REMOVED,
        K_NOT_FOUND,
        K_BUSY,
    };

    bool AddRegion(const RegisteredRegion &region);
    bool RemoveRegion(const RegisteredRegion &region);
    bool RemoveByBaseAddr(uint64_t baseAddr);
    RemoveResult RemoveByBaseAddrIfNoActiveLease(uint64_t baseAddr);
    bool IsRegistered(uint64_t baseAddr, uint64_t length, int32_t deviceId) const;
    bool FindDeviceIdByRange(uint64_t baseAddr, uint64_t length, int32_t *deviceId) const;
    bool FindRegionByBaseAddr(uint64_t baseAddr, RegisteredRegion *region) const;
    Result AcquireReadLease(const std::vector<TransferMemoryRegion> &ranges, int32_t deviceId, uint64_t ttlMs,
                            uint64_t *leaseId);
    void ReleaseReadLease(uint64_t leaseId);

private:
    struct ReadLease {
        std::vector<TransferMemoryRegion> ranges;
        int32_t deviceId = -1;
        std::chrono::steady_clock::time_point expireTime;
    };

    static bool IsSameRegion(const RegisteredRegion &left, const RegisteredRegion &right);
    static bool IsOverlap(const RegisteredRegion &left, const RegisteredRegion &right);
    static bool IsRangeInside(uint64_t baseAddr, uint64_t length, const RegisteredRegion &region);
    static bool IsRangeOverlap(uint64_t baseAddr, uint64_t length, const TransferMemoryRegion &range);
    void PruneExpiredLeasesLocked(std::chrono::steady_clock::time_point now);
    bool HasActiveLeaseForRegionLocked(const RegisteredRegion &region,
                                       std::chrono::steady_clock::time_point now);

    mutable std::mutex mutex_;
    std::vector<RegisteredRegion> regions_;
    std::unordered_map<uint64_t, ReadLease> readLeases_;
    uint64_t nextLeaseId_ = 1;
};

}  // namespace datasystem

#endif  // TRANSFER_ENGINE_INTERNAL_REGISTERED_MEMORY_TABLE_H
