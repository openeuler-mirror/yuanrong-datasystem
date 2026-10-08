#include "internal/connection/connection_manager.h"

#include <sstream>

namespace datasystem {
namespace {

constexpr size_t K_MAX_CONNECTION_STATES = 4096;

}  // namespace

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
    return iter->second.requesterRecvReady && iter->second.ownerSendReady;
}

ConnectionState ConnectionManager::GetState(const ConnectionKey &key) const
{
    std::lock_guard<std::mutex> lock(mutex_);
    const auto iter = states_.find(ToMapKey(key));
    return iter == states_.end() ? ConnectionState{} : iter->second;
}

void ConnectionManager::Remove(const ConnectionKey &key)
{
    std::lock_guard<std::mutex> lock(mutex_);
    states_.erase(ToMapKey(key));
}

bool ConnectionManager::MarkRequesterRecvReady(const ConnectionKey &key)
{
    std::lock_guard<std::mutex> lock(mutex_);
    auto *state = GetOrCreateStateLocked(ToMapKey(key));
    if (state == nullptr) {
        return false;
    }
    state->requesterRecvReady = true;
    return true;
}

bool ConnectionManager::MarkOwnerSendReady(const ConnectionKey &key)
{
    std::lock_guard<std::mutex> lock(mutex_);
    auto *state = GetOrCreateStateLocked(ToMapKey(key));
    if (state == nullptr) {
        return false;
    }
    state->ownerSendReady = true;
    return true;
}

bool ConnectionManager::MarkOwnerSendReadyWithOldestEviction(const ConnectionKey &key)
{
    std::lock_guard<std::mutex> lock(mutex_);
    const std::string mapKey = ToMapKey(key);
    auto state = states_.find(mapKey);
    if (state == states_.end() && states_.size() >= K_MAX_CONNECTION_STATES) {
        // Owner readiness does not own a receiver-driven read lease; an evicted peer
        // re-establishes readiness on its next query without ending an active read.
        auto oldest = states_.end();
        for (auto iter = states_.begin(); iter != states_.end(); ++iter) {
            const auto &candidate = iter->second;
            if (candidate.ownerReadySequence == 0 || candidate.requesterRecvReady) {
                continue;
            }
            if (oldest == states_.end() || candidate.ownerReadySequence < oldest->second.ownerReadySequence) {
                oldest = iter;
            }
        }
        if (oldest == states_.end()) {
            return false;
        }
        states_.erase(oldest);
        state = states_.emplace(mapKey, ConnectionState{}).first;
    } else if (state == states_.end()) {
        state = states_.emplace(mapKey, ConnectionState{}).first;
    }
    state->second.ownerSendReady = true;
    state->second.ownerReadySequence = nextOwnerReadySequence_++;
    return true;
}

void ConnectionManager::Clear()
{
    std::lock_guard<std::mutex> lock(mutex_);
    states_.clear();
    nextOwnerReadySequence_ = 1;
}

size_t ConnectionManager::Size() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    return states_.size();
}

ConnectionState *ConnectionManager::GetOrCreateStateLocked(const std::string &mapKey)
{
    const auto existing = states_.find(mapKey);
    if (existing != states_.end()) {
        return &existing->second;
    }
    if (states_.size() >= K_MAX_CONNECTION_STATES) {
        return nullptr;
    }
    const auto inserted = states_.emplace(mapKey, ConnectionState{});
    return &inserted.first->second;
}

}  // namespace datasystem
