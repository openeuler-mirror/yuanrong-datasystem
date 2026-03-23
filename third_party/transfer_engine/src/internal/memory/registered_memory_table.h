#ifndef TRANSFER_ENGINE_INTERNAL_REGISTERED_MEMORY_TABLE_H
#define TRANSFER_ENGINE_INTERNAL_REGISTERED_MEMORY_TABLE_H

#include <cstdint>
#include <mutex>
#include <vector>

namespace datasystem {

struct RegisteredRegion {
    uint64_t baseAddr = 0;
    uint64_t length = 0;
    int32_t deviceId = -1;
};

class RegisteredMemoryTable {
public:
    bool AddRegion(const RegisteredRegion &region);
    bool RemoveRegion(const RegisteredRegion &region);
    bool RemoveByBaseAddr(uint64_t baseAddr);
    bool IsRegistered(uint64_t baseAddr, uint64_t length, int32_t deviceId) const;
    bool FindDeviceIdByRange(uint64_t baseAddr, uint64_t length, int32_t *deviceId) const;

private:
    static bool IsSameRegion(const RegisteredRegion &left, const RegisteredRegion &right);
    static bool IsOverlap(const RegisteredRegion &left, const RegisteredRegion &right);
    static bool IsRangeInside(uint64_t baseAddr, uint64_t length, const RegisteredRegion &region);

    mutable std::mutex mutex_;
    std::vector<RegisteredRegion> regions_;
};

}  // namespace datasystem

#endif  // TRANSFER_ENGINE_INTERNAL_REGISTERED_MEMORY_TABLE_H
