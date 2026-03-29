#ifndef TRANSFER_ENGINE_INTERNAL_CONNECTION_MANAGER_H
#define TRANSFER_ENGINE_INTERNAL_CONNECTION_MANAGER_H

#include <cstdint>
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
    bool stale = false;
};

class ConnectionManager {
public:
    bool HasReadyConnection(const ConnectionKey &key) const;
    ConnectionState GetState(const ConnectionKey &key) const;
    void MarkStale(const ConnectionKey &key);
    void MarkRequesterRecvReady(const ConnectionKey &key);
    void MarkOwnerSendReady(const ConnectionKey &key);

private:
    static std::string ToMapKey(const ConnectionKey &key);

    mutable std::mutex mutex_;
    std::unordered_map<std::string, ConnectionState> states_;
};

}  // namespace datasystem

#endif  // TRANSFER_ENGINE_INTERNAL_CONNECTION_MANAGER_H
