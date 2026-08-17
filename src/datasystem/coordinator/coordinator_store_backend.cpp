/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Description: Process-local Coordinator Store adapter for cluster topology control.
 */
#include "datasystem/coordinator/coordinator_store_backend.h"

#include <cstddef>
#include <utility>

#include "datasystem/common/coordinator/coordinator_store.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"

namespace datasystem::coordinator {
namespace {
constexpr size_t MAX_CAS_ATTEMPTS = 16;

std::string BuildPhysicalKey(const std::string &tableName, const std::string &key)
{
    return tableName + "/" + key;
}

std::string RemoveTablePrefix(const std::string &physicalKey, const std::string &tableName)
{
    return physicalKey.substr(tableName.size() + 1);
}

bool IsRetryableCasConflict(const Status &status)
{
    return status.GetCode() == K_INVALID || status.GetCode() == K_TRY_AGAIN || status.GetCode() == K_NOT_FOUND;
}

RangeSearchResult BuildResult(const KeyValueEntry &entry)
{
    RangeSearchResult result;
    result.key = entry.key;
    result.value = entry.value;
    result.version = entry.version;
    result.modRevision = entry.modRevision;
    return result;
}
}  // namespace

CoordinatorStoreBackend::CoordinatorStoreBackend(CoordinatorStore &store) : store_(store)
{
}

CoordinatorStoreBackend::~CoordinatorStoreBackend() = default;

Status CoordinatorStoreBackend::GetAll(
    const std::string &tableName, std::vector<std::pair<std::string, std::string>> &outKeyValues)
{
    int64_t responseRevision = 0;
    return GetAll(tableName, outKeyValues, responseRevision);
}

Status CoordinatorStoreBackend::GetAll(
    const std::string &tableName, std::vector<std::pair<std::string, std::string>> &outKeyValues,
    int64_t &responseRevision)
{
    std::string prefix;
    RETURN_IF_NOT_OK(GetStorePrefix(tableName, prefix));
    const std::string rangeKey = prefix + "/";
    std::vector<KeyValueEntry> entries;
    RETURN_IF_NOT_OK(store_.Range(rangeKey, StringPlusOne(rangeKey), entries, responseRevision));
    outKeyValues.reserve(outKeyValues.size() + entries.size());
    for (auto &entry : entries) {
        outKeyValues.emplace_back(RemoveTablePrefix(entry.key, prefix), std::move(entry.value));
    }
    return Status::OK();
}

Status CoordinatorStoreBackend::Get(const std::string &tableName, const std::string &key, std::string &value)
{
    RangeSearchResult result;
    RETURN_IF_NOT_OK(Get(tableName, key, result));
    value = std::move(result.value);
    return Status::OK();
}

Status CoordinatorStoreBackend::Get(const std::string &tableName, const std::string &key, RangeSearchResult &result,
                                    int32_t timeoutMs)
{
    static_cast<void>(timeoutMs);
    std::string prefix;
    RETURN_IF_NOT_OK(GetStorePrefix(tableName, prefix));
    KeyValueEntry entry;
    RETURN_IF_NOT_OK(ReadExact(BuildPhysicalKey(prefix, key), entry));
    result = BuildResult(entry);
    return Status::OK();
}

Status CoordinatorStoreBackend::CAS(const std::string &tableName, const std::string &key,
                                    const ProcessFunction &process, RangeSearchResult &result)
{
    CHECK_FAIL_RETURN_STATUS(process != nullptr, K_INVALID, "Coordinator process function is null");
    std::string prefix;
    RETURN_IF_NOT_OK(GetStorePrefix(tableName, prefix));
    return RunCas(BuildPhysicalKey(prefix, key), process, result);
}

Status CoordinatorStoreBackend::CASAtRevision(const std::string &tableName, const std::string &key,
                                              const ProcessFunction &process, int64_t expectedRevision,
                                              RangeSearchResult &result)
{
    CHECK_FAIL_RETURN_STATUS(process != nullptr, K_INVALID, "Coordinator process function is null");
    CHECK_FAIL_RETURN_STATUS(expectedRevision > 0, K_INVALID, "Coordinator expected revision is invalid");
    std::string prefix;
    RETURN_IF_NOT_OK(GetStorePrefix(tableName, prefix));
    return RunCas(BuildPhysicalKey(prefix, key), process, result, expectedRevision);
}

Status CoordinatorStoreBackend::CAS(const std::string &tableName, const std::string &key,
                                    const ProcessFunction &process)
{
    RangeSearchResult result;
    return CAS(tableName, key, process, result);
}

Status CoordinatorStoreBackend::CAS(const std::string &tableName, const std::string &key,
                                    const std::string &oldValue, const std::string &newValue)
{
    std::string prefix;
    RETURN_IF_NOT_OK(GetStorePrefix(tableName, prefix));
    const std::string physicalKey = BuildPhysicalKey(prefix, key);
    KeyValueEntry current;
    auto rc = ReadExact(physicalKey, current);
    int64_t expectedVersion = COORDINATOR_KEY_NOT_EXISTS_VERSION;
    if (rc.IsOk()) {
        CHECK_FAIL_RETURN_STATUS(current.value == oldValue, K_TRY_AGAIN, "Coordinator compare value failed");
        expectedVersion = current.version;
    } else if (rc.GetCode() != K_NOT_FOUND) {
        return rc;
    }
    int64_t version = 0;
    int64_t revision = 0;
    return store_.Put(physicalKey, newValue, 0, expectedVersion, version, revision);
}

Status CoordinatorStoreBackend::Delete(const std::string &tableName, const std::string &key)
{
    return Delete(tableName, key, cluster::DEFAULT_COORDINATION_DELETE_TIMEOUT_MS);
}

Status CoordinatorStoreBackend::Delete(const std::string &tableName, const std::string &key, int timeoutMs)
{
    static_cast<void>(timeoutMs);
    std::string prefix;
    RETURN_IF_NOT_OK(GetStorePrefix(tableName, prefix));
    int64_t deleted = 0;
    int64_t revision = 0;
    return store_.DeleteRange(BuildPhysicalKey(prefix, key), "", deleted, revision);
}

Status CoordinatorStoreBackend::WatchEvents(const std::vector<cluster::WatchKey> &watchKeys)
{
    static_cast<void>(watchKeys);
    RETURN_STATUS(K_INVALID, "local topology Store adapter does not own watches");
}

Status CoordinatorStoreBackend::PutWithKeepAliveLease(const std::string &tableName, const std::string &key,
                                                      const std::string &value)
{
    static_cast<void>(tableName);
    static_cast<void>(key);
    static_cast<void>(value);
    RETURN_STATUS(K_INVALID, "local topology Store adapter does not own a Worker membership lease");
}

Status CoordinatorStoreBackend::InitKeepAlive(const std::string &tableName, const std::string &key, bool isRestart,
                                              bool isStoreAvailableWhenStart)
{
    static_cast<void>(tableName);
    static_cast<void>(key);
    static_cast<void>(isRestart);
    static_cast<void>(isStoreAvailableWhenStart);
    RETURN_STATUS(K_INVALID, "local topology Store adapter does not own membership keepalive");
}

Status CoordinatorStoreBackend::ShutdownEventSources()
{
    return Status::OK();
}

Status CoordinatorStoreBackend::Shutdown()
{
    return Status::OK();
}

Status CoordinatorStoreBackend::UpdateNodeState(cluster::MemberLifecycleState state)
{
    static_cast<void>(state);
    RETURN_STATUS(K_INVALID, "local topology Store adapter cannot update Worker lifecycle");
}

Status CoordinatorStoreBackend::GetStorePrefix(const std::string &tableName, std::string &prefix)
{
    CHECK_FAIL_RETURN_STATUS(!tableName.empty(), K_INVALID, "Coordinator table name is empty");
    prefix = tableName;
    return Status::OK();
}

Status CoordinatorStoreBackend::InformReconciliationDone(const HostPort &workerAddr)
{
    static_cast<void>(workerAddr);
    RETURN_STATUS(K_INVALID, "local topology Store adapter cannot complete Worker reconciliation");
}

bool CoordinatorStoreBackend::IsKeepAliveTimeout()
{
    return false;
}

bool CoordinatorStoreBackend::IsFirstKeepAliveSent()
{
    return false;
}

void CoordinatorStoreBackend::SetEventHandler(EventHandler &&eventHandler)
{
    static_cast<void>(eventHandler);
}

void CoordinatorStoreBackend::SetCheckStoreStateWhenNetworkFailedHandler(std::function<bool()> handler)
{
    static_cast<void>(handler);
}

Status CoordinatorStoreBackend::ReadExact(const std::string &physicalKey, KeyValueEntry &entry) const
{
    std::vector<KeyValueEntry> entries;
    int64_t revision = 0;
    RETURN_IF_NOT_OK(store_.Range(physicalKey, "", entries, revision));
    CHECK_FAIL_RETURN_STATUS(!entries.empty(), K_NOT_FOUND, "Coordinator key does not exist");
    CHECK_FAIL_RETURN_STATUS(entries.size() == 1, K_KVSTORE_ERROR, "Coordinator exact key is not unique");
    entry = std::move(entries.front());
    return Status::OK();
}

Status CoordinatorStoreBackend::RunCas(const std::string &physicalKey, const ProcessFunction &process,
                                       RangeSearchResult &result, int64_t expectedRevision)
{
    Status lastError(K_TRY_AGAIN, "Coordinator CAS exceeded retry limit");
    for (size_t attempt = 0; attempt < MAX_CAS_ATTEMPTS; ++attempt) {
        KeyValueEntry current{ physicalKey, "", COORDINATOR_KEY_NOT_EXISTS_VERSION, 0 };
        const auto readStatus = ReadExact(physicalKey, current);
        if (readStatus.IsError() && readStatus.GetCode() != K_NOT_FOUND) {
            return readStatus;
        }
        std::unique_ptr<std::string> next;
        bool retry = true;
        const auto processStatus = process(current.value, next, retry);
        if (processStatus.IsError()) {
            if (!retry) {
                return processStatus;
            }
            lastError = processStatus;
            continue;
        }
        if (next == nullptr) {
            result = BuildResult(current);
            return Status::OK();
        }
        int64_t version = 0;
        int64_t revision = 0;
        const auto putStatus = store_.Put(physicalKey, *next, 0, current.version, version, revision,
                                          COORDINATOR_NO_MOD_REVISION_CHECK, expectedRevision);
        if (putStatus.IsOk()) {
            RangeSearchResult committed;
            committed.key = physicalKey;
            committed.value = std::move(*next);
            committed.version = version;
            committed.modRevision = revision;
            result = std::move(committed);
            return Status::OK();
        }
        if (expectedRevision != COORDINATOR_NO_GLOBAL_REVISION_CHECK && putStatus.GetCode() == K_TRY_AGAIN) {
            return putStatus;
        }
        if (!IsRetryableCasConflict(putStatus)) {
            return putStatus;
        }
        lastError = putStatus;
    }
    LOG(WARNING) << "CLUSTER_COORDINATOR_STORE_CAS action=retry_exhausted"
                 << " key=" << physicalKey << " attempts=" << MAX_CAS_ATTEMPTS
                 << " status=" << lastError.ToString();
    return lastError;
}

}  // namespace datasystem::coordinator
