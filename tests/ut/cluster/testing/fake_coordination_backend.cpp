/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

/**
 * Description: In-memory single-key coordination backend for cluster module tests.
 */
#include "ut/cluster/testing/fake_coordination_backend.h"

#include <utility>

#include "datasystem/cluster/repository/topology_repository_codec.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem::cluster {
namespace {
constexpr size_t SINGLE_NOT_READY_GET_ATTEMPT = 1;
}

std::string FakeCoordinationBackend::FullKey(const std::string &table, const std::string &key) const
{
    return table + "/" + key;
}

Status FakeCoordinationBackend::GetAll(const std::string &table,
                                       std::vector<std::pair<std::string, std::string>> &values)
{
    std::lock_guard<std::mutex> lock(mutex_);
    values.clear();
    const std::string prefix = table + "/";
    for (const auto &[key, value] : values_) {
        if (key.rfind(prefix, 0) == 0) {
            values.emplace_back(key.substr(prefix.size()), value.first);
        }
    }
    return Status::OK();
}

Status FakeCoordinationBackend::Get(const std::string &table, const std::string &key, std::string &value)
{
    RangeSearchResult result;
    RETURN_IF_NOT_OK(Get(table, key, result));
    value = result.value;
    return Status::OK();
}

Status FakeCoordinationBackend::Get(const std::string &table, const std::string &key, RangeSearchResult &result,
                                    int32_t timeoutMs)
{
    CHECK_FAIL_RETURN_STATUS(timeoutMs > 0, K_INVALID, "invalid timeout");
    std::unique_lock<std::mutex> lock(mutex_);
    ++getAttempts_;
    getCv_.notify_all();
    if (failNextGet_) {
        failNextGet_ = false;
        return Status(K_RPC_UNAVAILABLE, "injected exact-read backend failure");
    }
    if (notReadyGetAttempts_ > 0) {
        --notReadyGetAttempts_;
        return Status(K_NOT_READY, "injected recovering backend");
    }
    if (blockNextGet_) {
        blockNextGet_ = false;
        getBlocked_ = true;
        releaseGet_ = false;
        getCv_.notify_all();
        getCv_.wait(lock, [this] { return releaseGet_; });
    }
    auto iter = values_.find(FullKey(table, key));
    CHECK_FAIL_RETURN_STATUS(iter != values_.end(), K_NOT_FOUND, "fake key not found");
    result.key = key;
    result.value = iter->second.first;
    result.version = 1;
    result.modRevision = iter->second.second;
    return Status::OK();
}

Status FakeCoordinationBackend::CreateTable(const std::string &table, const std::string &tablePrefix)
{
    (void)table;
    (void)tablePrefix;
    return Status::OK();
}

Status FakeCoordinationBackend::CreateTableWithExactPrefix(const std::string &table, const std::string &tablePrefix)
{
    return CreateTable(table, tablePrefix);
}

Status FakeCoordinationBackend::Put(const std::string &table, const std::string &key, const std::string &value)
{
    PutBytes(table, key, value);
    return Status::OK();
}

Status FakeCoordinationBackend::CAS(const std::string &table, const std::string &key, const ProcessFunction &process,
                                    RangeSearchResult &result)
{
    std::function<void()> beforeCas;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        beforeCas = std::move(beforeCasHandler_);
    }
    if (beforeCas != nullptr) {
        beforeCas();
    }
    std::lock_guard<std::mutex> lock(mutex_);
    auto fullKey = FullKey(table, key);
    auto iter = values_.find(fullKey);
    const std::string oldValue = iter == values_.end() ? "" : iter->second.first;
    if (failNextCasBeforeCommit_) {
        failNextCasBeforeCommit_ = false;
        return Status(K_RPC_UNAVAILABLE, "injected pre-commit unknown");
    }
    std::unique_ptr<std::string> newValue;
    bool retry = false;
    RETURN_IF_NOT_OK(process(oldValue, newValue, retry));
    const bool wrote = newValue != nullptr;
    if (wrote) {
        values_[fullKey] = { *newValue, ++revision_ };
    }
    auto current = values_.find(fullKey);
    result.value = current == values_.end() ? "" : current->second.first;
    result.modRevision = current == values_.end() ? revision_ : current->second.second;
    if (failNextCasAfterCommit_ && wrote) {
        failNextCasAfterCommit_ = false;
        return Status(K_RPC_UNAVAILABLE, "injected post-commit unknown");
    }
    return Status::OK();
}

Status FakeCoordinationBackend::CAS(const std::string &table, const std::string &key, const ProcessFunction &process)
{
    RangeSearchResult result;
    return CAS(table, key, process, result);
}

Status FakeCoordinationBackend::CAS(const std::string &table, const std::string &key, const std::string &oldValue,
                                    const std::string &newValue)
{
    ProcessFunction process = [&oldValue, &newValue](const std::string &current, std::unique_ptr<std::string> &next,
                                                     bool &retry) {
        retry = false;
        CHECK_FAIL_RETURN_STATUS(current == oldValue, K_TRY_AGAIN, "fake CAS conflict");
        next = std::make_unique<std::string>(newValue);
        return Status::OK();
    };
    return CAS(table, key, process);
}

Status FakeCoordinationBackend::Delete(const std::string &table, const std::string &key)
{
    std::function<void()> beforeDelete;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        beforeDelete = std::move(beforeDeleteHandler_);
    }
    if (beforeDelete != nullptr) {
        beforeDelete();
    }
    {
        std::lock_guard<std::mutex> lock(mutex_);
        ++deleteAttempts_;
        values_.erase(FullKey(table, key));
        ++revision_;
    }
    return Status::OK();
}

Status FakeCoordinationBackend::Delete(const std::string &table, const std::string &key, int)
{
    return Delete(table, key);
}

Status FakeCoordinationBackend::WatchEvents(const std::vector<WatchKey> &watchKeys)
{
    std::lock_guard<std::mutex> lock(mutex_);
    lifecycleCalls_.emplace_back("watch");
    if (failNextWatch_) {
        failNextWatch_ = false;
        return Status(K_RPC_UNAVAILABLE, "injected watch failure");
    }
    watchKeys_ = watchKeys;
    return Status::OK();
}

Status FakeCoordinationBackend::InitKeepAlive(const std::string &, const std::string &, bool, bool)
{
    std::lock_guard<std::mutex> lock(mutex_);
    lifecycleCalls_.emplace_back("membership");
    return Status::OK();
}

Status FakeCoordinationBackend::ShutdownEventSources()
{
    return Status::OK();
}

Status FakeCoordinationBackend::Shutdown()
{
    return Status::OK();
}

Status FakeCoordinationBackend::UpdateNodeState(MemberLifecycleState)
{
    return Status::OK();
}

Status FakeCoordinationBackend::GetStorePrefix(const std::string &table, std::string &prefix)
{
    prefix = table;
    return Status::OK();
}

Status FakeCoordinationBackend::InformReconciliationDone(const HostPort &)
{
    return Status::OK();
}

bool FakeCoordinationBackend::IsKeepAliveTimeout()
{
    return false;
}

bool FakeCoordinationBackend::IsFirstKeepAliveSent()
{
    return true;
}

void FakeCoordinationBackend::SetEventHandler(EventHandler &&handler)
{
    std::lock_guard<std::mutex> lock(mutex_);
    handler_ = std::move(handler);
}

void FakeCoordinationBackend::SetLocalIsolationHandler(LocalIsolationHandler handler)
{
    std::lock_guard<std::mutex> lock(mutex_);
    localIsolationHandler_ = std::move(handler);
}

void FakeCoordinationBackend::SetLocalRecoveryHandler(LocalRecoveryHandler handler)
{
    std::lock_guard<std::mutex> lock(mutex_);
    localRecoveryHandler_ = std::move(handler);
}

void FakeCoordinationBackend::SetCheckStoreStateWhenNetworkFailedHandler(std::function<bool()>)
{
}

void FakeCoordinationBackend::PutBytes(const std::string &table, const std::string &key, std::string value)
{
    std::lock_guard<std::mutex> lock(mutex_);
    values_[FullKey(table, key)] = { std::move(value), ++revision_ };
}

void FakeCoordinationBackend::PutRaw(const std::string &table, const std::string &key, const TopologyState &state)
{
    std::string bytes;
    if (TopologyRepositoryCodec::EncodeTopology(state, bytes).IsOk()) {
        PutBytes(table, key, std::move(bytes));
    }
}

void FakeCoordinationBackend::FailNextCasAfterCommit()
{
    failNextCasAfterCommit_ = true;
}

void FakeCoordinationBackend::FailNextCasBeforeCommit()
{
    failNextCasBeforeCommit_ = true;
}

void FakeCoordinationBackend::FailNextWatch()
{
    failNextWatch_ = true;
}

void FakeCoordinationBackend::FailNextGet()
{
    std::lock_guard<std::mutex> lock(mutex_);
    failNextGet_ = true;
}

void FakeCoordinationBackend::ReturnNotReadyOnNextGet()
{
    ReturnNotReadyOnNextGets(SINGLE_NOT_READY_GET_ATTEMPT);
}

void FakeCoordinationBackend::ReturnNotReadyOnNextGets(size_t count)
{
    std::lock_guard<std::mutex> lock(mutex_);
    notReadyGetAttempts_ = count;
}

void FakeCoordinationBackend::EmitEvent(CoordinationEvent event)
{
    EventHandler handler;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        handler = handler_;
    }
    if (handler != nullptr) {
        handler(std::move(event));
    }
}

void FakeCoordinationBackend::EmitLocalIsolation(const Status &status)
{
    LocalIsolationHandler handler;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        handler = localIsolationHandler_;
    }
    if (handler != nullptr) {
        handler(status);
    }
}

void FakeCoordinationBackend::EmitLocalRecovery()
{
    LocalRecoveryHandler handler;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        handler = localRecoveryHandler_;
    }
    if (handler != nullptr) {
        handler();
    }
}

std::vector<WatchKey> FakeCoordinationBackend::WatchKeys() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    return watchKeys_;
}

std::vector<std::string> FakeCoordinationBackend::LifecycleCalls() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    return lifecycleCalls_;
}

bool FakeCoordinationBackend::HasEventHandler() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    return handler_ != nullptr;
}

void FakeCoordinationBackend::BlockNextGet()
{
    std::lock_guard<std::mutex> lock(mutex_);
    blockNextGet_ = true;
    getBlocked_ = false;
    releaseGet_ = false;
}

bool FakeCoordinationBackend::WaitUntilGetBlocked(std::chrono::steady_clock::time_point deadline)
{
    std::unique_lock<std::mutex> lock(mutex_);
    return getCv_.wait_until(lock, deadline, [this] { return getBlocked_; });
}

size_t FakeCoordinationBackend::GetAttemptCount() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    return getAttempts_;
}

bool FakeCoordinationBackend::WaitForGetAttempts(size_t expected, std::chrono::steady_clock::time_point deadline)
{
    std::unique_lock<std::mutex> lock(mutex_);
    return getCv_.wait_until(lock, deadline, [this, expected] { return getAttempts_ >= expected; });
}

void FakeCoordinationBackend::ReleaseBlockedGet()
{
    std::lock_guard<std::mutex> lock(mutex_);
    releaseGet_ = true;
    getCv_.notify_all();
}

void FakeCoordinationBackend::SetBeforeCasHandler(std::function<void()> handler)
{
    std::lock_guard<std::mutex> lock(mutex_);
    beforeCasHandler_ = std::move(handler);
}

void FakeCoordinationBackend::SetBeforeDeleteHandler(std::function<void()> handler)
{
    std::lock_guard<std::mutex> lock(mutex_);
    beforeDeleteHandler_ = std::move(handler);
}

size_t FakeCoordinationBackend::DeleteAttemptCount() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    return deleteAttempts_;
}

}  // namespace datasystem::cluster
