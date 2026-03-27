#include "internal/connection/connection_manager.h"

#include <sstream>

namespace datasystem {

std::string ConnectionManager::ToMapKey(const ConnectionKey &key)
{
    std::ostringstream oss;
    oss << key.localDeviceId << "|" << key.peerHost << "|" << key.peerPort << "|" << key.peerDeviceId;
    return oss.str();
}

bool ConnectionManager::HasReadyConnection(const ConnectionKey &key) const
{
    std::lock_guard<std::mutex> lock(mutex_);
    const auto iter = states_.find(ToMapKey(key));
    if (iter == states_.end()) {
        return false;
    }
    return iter->second.requesterRecvReady && iter->second.ownerSendReady && !iter->second.stale;
}

ConnectionState ConnectionManager::GetState(const ConnectionKey &key) const
{
    std::lock_guard<std::mutex> lock(mutex_);
    const auto iter = states_.find(ToMapKey(key));
    return iter == states_.end() ? ConnectionState{} : iter->second;
}

void ConnectionManager::MarkStale(const ConnectionKey &key)
{
    std::lock_guard<std::mutex> lock(mutex_);
    states_.erase(ToMapKey(key));
}

void ConnectionManager::MarkRequesterRecvReady(const ConnectionKey &key)
{
    std::lock_guard<std::mutex> lock(mutex_);
    auto &state = states_[ToMapKey(key)];
    state.requesterRecvReady = true;
    state.stale = false;
}

void ConnectionManager::MarkOwnerSendReady(const ConnectionKey &key)
{
    std::lock_guard<std::mutex> lock(mutex_);
    auto &state = states_[ToMapKey(key)];
    state.ownerSendReady = true;
    state.stale = false;
}

}  // namespace datasystem
