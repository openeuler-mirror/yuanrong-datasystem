
/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Description: Defines client info class and client manager class.
 */

#include "datasystem/worker/client_manager/client_info.h"

#include "datasystem/common/flags/flags.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/shared_memory/allocator.h"
#include "datasystem/common/string_intern/string_ref.h"
#include "datasystem/common/util/uuid_generator.h"

DS_DEFINE_uint64(client_dead_timeout_s, 120,
                 "Maximum time interval for the worker to determine client death, value range: [15, UINT64_MAX/1000)");
static bool ValidClientDeadTimeoutSecs(const char *flagName, uint64_t value)
{
#ifdef WITH_TESTS
    (void)flagName;
    (void)value;
    return true;
#else
    const uint32_t minTimeoutSecs = 3;
    const uint64_t s2ms = 1000;
    if (value < minTimeoutSecs || value > UINT64_MAX / s2ms) {
        // LCOV_EXCL_START
        LOG(ERROR) << "Flag: " << flagName << " is invalid: " << value << ", valid range: [" << minTimeoutSecs << ", "
                   << UINT64_MAX / s2ms << "]";
        return false;
        // LCOV_EXCL_END
    }
    return true;
#endif
}
DS_DEFINE_validator(client_dead_timeout_s, &ValidClientDeadTimeoutSecs);

namespace datasystem {
namespace worker {

const std::string &ClientInfo::GetDeviceId() const
{
    return deviceId_;
}

void ClientInfo::SetUniqueCount(bool uniqueCount)
{
    uniqueCount_ = uniqueCount;
}

int32_t ClientInfo::GetSocketFd() const
{
    return socketFd_;
}

const std::string &ClientInfo::GetClientId() const
{
    return clientId_;
}

void ClientInfo::UpdateLastHeartbeat()
{
    lastHeartbeat_.Reset();
}

void ClientInfo::LostHandler()
{
    if (lostHandler_) {
        lostHandler_();
    }
}

void ClientInfo::SetLostHandler(std::function<void()> lostHandler, HeartbeatType heartbeatType)
{
    heartbeatType_ = heartbeatType;
    lostHandler_ = std::move(lostHandler);
}

bool ClientInfo::IsClientLost()
{
    uint32_t heartbeatThreshold = FLAGS_client_dead_timeout_s;
    INJECT_POINT("ClientManager.IsClientLost.heartbeatThreshold", [&heartbeatThreshold](int time) {
        heartbeatThreshold = time;
        return true;
    });
    return (lastHeartbeat_.ElapsedSecond() > heartbeatThreshold) && heartbeatType_ == HeartbeatType::RPC_HEARTBEAT;
}

bool ClientInfo::ShmEnabled()
{
    return shmEnabled_;
}

bool ClientInfo::AddShmUnit(const std::shared_ptr<ShmUnit> &shmUnit)
{
    if (shmUnit == nullptr) {
        LOG(ERROR) << "Add shmunit failed, shmunit is null";
        return false;
    }
    std::lock_guard<std::mutex> lck(mutex_);
    fds_.emplace(shmUnit->fd);
    auto itr = shmUnitIds_.find(shmUnit->id);
    if (itr == shmUnitIds_.end()) {
        shmUnitIds_.emplace(shmUnit->id, 1);
        if (shmUnit->refCount == 0) {
            datasystem::memory::Allocator::Instance()->ChangeNoRefPageCount(-1);
            datasystem::memory::Allocator::Instance()->ChangeRefPageCount(1);
        }
        shmUnit->refCount++;
        return true;
    }
    if (uniqueCount_) {
        return false;
    }
    itr->second += 1;
    if (shmUnit->refCount == 0) {
        datasystem::memory::Allocator::Instance()->ChangeNoRefPageCount(-1);
        datasystem::memory::Allocator::Instance()->ChangeRefPageCount(1);
    }
    shmUnit->refCount++;
    return true;
}

bool ClientInfo::RemoveShmUnit(const std::shared_ptr<ShmUnit> &shmUnit)
{
    if (shmUnit == nullptr) {
        LOG(ERROR) << "remove shmunit failed, shmunit is null";
        return false;
    }
    std::lock_guard<std::mutex> lck(mutex_);
    if (uniqueCount_) {
        if (shmUnitIds_.erase(shmUnit->id) <= 0) {
            return false;
        }
        if (shmUnit->refCount > 0) {
            shmUnit->refCount--;
            if (shmUnit->refCount == 0) {
                datasystem::memory::Allocator::Instance()->ChangeNoRefPageCount(1);
                datasystem::memory::Allocator::Instance()->ChangeRefPageCount(-1);
            }
        } else {
            LOG(WARNING) << "RemoveShmUnit: The value of refCount is 0 and cannot be decreased. id:"
                         << BytesUuidToString(shmUnit->id.ToString());
        }
        return true;
    }
    auto itr = shmUnitIds_.find(shmUnit->id);
    if (itr == shmUnitIds_.end()) {
        LOG(WARNING) << "RemoveShmUnit: The ID does not exist. id:" << BytesUuidToString(shmUnit->id.ToString());
        return false;
    }
    if (shmUnit->refCount > 0) {
        shmUnit->refCount--;
        if (shmUnit->refCount == 0) {
            datasystem::memory::Allocator::Instance()->ChangeRefPageCount(-1);
            datasystem::memory::Allocator::Instance()->ChangeNoRefPageCount(1);
        }
    } else {
        LOG(WARNING) << "RemoveShmUnit: The value of refCount is 0 and cannot be decreased. id:"
                     << BytesUuidToString(shmUnit->id.ToString());
    }
    itr->second -= 1;
    if (itr->second == 0) {
        auto result = shmUnitIds_.erase(shmUnit->id);
        if (result <= 0) {
            return false;
        }
    }
    return true;
}

bool ClientInfo::Contains(const ShmKey &shmId) const
{
    return shmUnitIds_.find(shmId) != shmUnitIds_.end();
}

#ifdef WITH_TESTS
void ClientInfo::GetShmUnitIds(std::unordered_map<ShmKey, uint32_t> &shmUnitIds)
{
    shmUnitIds = shmUnitIds_;
}
#endif

void ClientInfo::GetReaderSessionIds(std::unordered_set<std::string> &sessionIds) const
{
    sessionIds = readerSessionTable_;
}

void ClientInfo::GetWriterSessionIds(std::unordered_set<std::string> &sessionIds) const
{
    sessionIds = writerSessionTable_;
}

bool ClientInfo::AddReaderSessionId(const std::string &sessionId)
{
    std::lock_guard<std::mutex> lck(mutex_);
    return readerSessionTable_.emplace(sessionId).second;
}

bool ClientInfo::AddWriterSessionId(const std::string &sessionId)
{
    std::lock_guard<std::mutex> lck(mutex_);
    return writerSessionTable_.emplace(sessionId).second;
}

Status ClientInfo::RemoveReaderSessionId(const std::string &sessionId)
{
    std::lock_guard<std::mutex> lck(mutex_);
    CHECK_FAIL_RETURN_STATUS(readerSessionTable_.erase(sessionId), StatusCode::K_RUNTIME_ERROR,
                             "ClientInfo remove reader session failed. sessionId: " + BytesUuidToString(sessionId));
    return Status::OK();
}

Status ClientInfo::RemoveWriterSessionId(const std::string &sessionId)
{
    std::lock_guard<std::mutex> lck(mutex_);
    CHECK_FAIL_RETURN_STATUS(writerSessionTable_.erase(sessionId), StatusCode::K_RUNTIME_ERROR,
                             "ClientInfo remove writer session failed. sessionId: " + BytesUuidToString(sessionId));
    return Status::OK();
}

void ClientInfo::SetLockId(uint32_t lockId)
{
    lockId_ = lockId;
}

Status ClientInfo::GetLockId(uint32_t &lockId) const
{
    if (lockId_ == UINT32_MAX) {
        RETURN_STATUS(K_NOT_READY, "No available lock ID.");
    }
    lockId = lockId_;
    return Status::OK();
}
}  // namespace worker
}  // namespace datasystem