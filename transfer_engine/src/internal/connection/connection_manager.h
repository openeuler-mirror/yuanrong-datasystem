#ifndef TRANSFER_ENGINE_INTERNAL_CONNECTION_MANAGER_H
#define TRANSFER_ENGINE_INTERNAL_CONNECTION_MANAGER_H

#include <cstdint>
#include <cstddef>
#include <mutex>
#include <string>
#include <unordered_map>

namespace datasystem {

struct ConnectionKey {
    int32_t localDeviceId = -1;
    std::string peerHost;
    uint16_t peerPort = 0;
    int32_t peerDeviceId = -1;
};

struct ConnectionState {
    bool requesterRecvReady = false;
    bool ownerSendReady = false;
    uint64_t ownerReadySequence = 0;
};

class ConnectionManager {
public:
    bool HasReadyConnection(const ConnectionKey &key) const;
    ConnectionState GetState(const ConnectionKey &key) const;
    void Remove(const ConnectionKey &key);
    bool MarkRequesterRecvReady(const ConnectionKey &key);
    bool MarkOwnerSendReady(const ConnectionKey &key);
    bool MarkOwnerSendReadyWithOldestEviction(const ConnectionKey &key);
    void Clear();
    size_t Size() const;

private:
    static std::string ToMapKey(const ConnectionKey &key);
    ConnectionState *GetOrCreateStateLocked(const std::string &mapKey);

    mutable std::mutex mutex_;
    std::unordered_map<std::string, ConnectionState> states_;
    uint64_t nextOwnerReadySequence_ = 1;
};

}  // namespace datasystem

#endif  // TRANSFER_ENGINE_INTERNAL_CONNECTION_MANAGER_H
