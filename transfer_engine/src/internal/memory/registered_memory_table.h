#ifndef TRANSFER_ENGINE_INTERNAL_REGISTERED_MEMORY_TABLE_H
#define TRANSFER_ENGINE_INTERNAL_REGISTERED_MEMORY_TABLE_H

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "datasystem/transfer_engine/data_plane_backend.h"
#include "datasystem/transfer_engine/status.h"

namespace datasystem {

struct RegisteredRegion {
    uint64_t baseAddr = 0;
    uint64_t length = 0;
    int32_t deviceId = -1;
    uint64_t backingBaseAddr = 0;
    uint64_t backingLength = 0;
};

struct ReadLeaseRequester {
    std::string host;
    uint16_t port = 0;
    int32_t deviceId = -1;
};

class RegisteredMemoryTable {
public:
    enum class RemoveResult {
        K_REMOVED,
        K_NOT_FOUND,
        K_BUSY,
        K_AMBIGUOUS,
    };

    bool AddRegion(const RegisteredRegion &region);
    bool AddRegions(const std::vector<RegisteredRegion> &regions);
    bool CanAddRegions(const std::vector<RegisteredRegion> &regions) const;
    bool RemoveRegion(const RegisteredRegion &region);
    bool RemoveByBaseAddr(uint64_t baseAddr);
    RemoveResult RemoveByBaseAddrIfNoActiveLease(uint64_t baseAddr);
    bool IsRegistered(uint64_t baseAddr, uint64_t length, int32_t deviceId) const;
    bool FindDeviceIdByRange(uint64_t baseAddr, uint64_t length, int32_t *deviceId) const;
    bool FindRegionByBaseAddr(uint64_t baseAddr, RegisteredRegion *region) const;
    RemoveResult RemoveByBaseAddrsIfNoActiveLease(const std::vector<uint64_t> &baseAddrs,
                                                  std::vector<RegisteredRegion> *removedRegions);
    Result AcquireReadLease(const std::vector<TransferMemoryRegion> &ranges, int32_t deviceId,
                            const ReadLeaseRequester &requester, uint64_t ttlMs, uint64_t *leaseId);
    bool ReleaseReadLease(uint64_t leaseId, const ReadLeaseRequester &requester);
    void OpenReadLeaseAdmission();
    void CloseReadLeaseAdmission();
    bool WaitForNoActiveReadLeases(uint64_t timeoutMs);
    void Clear();

private:
    struct ReadLease {
        std::vector<TransferMemoryRegion> ranges;
        int32_t deviceId = -1;
        ReadLeaseRequester requester;
        std::chrono::steady_clock::time_point expireTime;
    };

    static bool IsSameRegion(const RegisteredRegion &left, const RegisteredRegion &right);
    static bool IsOverlap(const RegisteredRegion &left, const RegisteredRegion &right);
    static bool IsRangeInside(uint64_t baseAddr, uint64_t length, const RegisteredRegion &region);
    static bool IsRangeOverlap(uint64_t baseAddr, uint64_t length, const TransferMemoryRegion &range);
    static bool IsValidRegion(const RegisteredRegion &region);
    static bool CanAddRegionsLocked(const std::vector<RegisteredRegion> &existing,
                                    const std::vector<RegisteredRegion> &regions);
    void PruneExpiredLeasesLocked(std::chrono::steady_clock::time_point now);
    bool HasActiveLeaseForRegionLocked(const RegisteredRegion &region, std::chrono::steady_clock::time_point now);

    mutable std::mutex mutex_;
    std::condition_variable leaseCv_;
    std::vector<RegisteredRegion> regions_;
    std::unordered_map<uint64_t, ReadLease> readLeases_;
    size_t activeReadLeaseRanges_ = 0;
    bool readLeaseAdmissionOpen_ = true;
};

}  // namespace datasystem

#endif  // TRANSFER_ENGINE_INTERNAL_REGISTERED_MEMORY_TABLE_H
