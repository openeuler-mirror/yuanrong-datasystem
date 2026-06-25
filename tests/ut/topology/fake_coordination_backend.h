/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
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

#ifndef DATASYSTEM_TESTS_UT_COMMON_TOPOLOGY_FAKE_CLUSTER_STORE_H
#define DATASYSTEM_TESTS_UT_COMMON_TOPOLOGY_FAKE_CLUSTER_STORE_H

#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "datasystem/common/kvstore/etcd/etcd_constants.h"
#include "datasystem/common/kvstore/etcd/etcd_store.h"
#include "datasystem/topology/coordination_backend/i_coordination_backend.h"

namespace datasystem {
namespace topology {

class FakeCoordinationBackend final : public ICoordinationBackend {
public:
    Status PutForTest(const std::string &tableName, const std::string &key, const std::string &value)
    {
        auto &entry = entries_[{ tableName, key }];
        entry.value = value;
        ++entry.version;
        entry.revision = ++revision_;
        Emit(CoordinationEventType::PUT, tableName, key, entry);
        return Status::OK();
    }

    Status DeleteForTest(const std::string &tableName, const std::string &key)
    {
        auto iter = entries_.find({ tableName, key });
        if (iter == entries_.end()) {
            RETURN_STATUS(K_NOT_FOUND, "key not found");
        }
        auto entry = iter->second;
        entries_.erase(iter);
        entry.revision = ++revision_;
        Emit(CoordinationEventType::DELETE, tableName, key, entry);
        return Status::OK();
    }

    Status GetAll(const std::string &tableName, std::vector<std::pair<std::string, std::string>> &outKeyValues) override
    {
        outKeyValues.clear();
        for (const auto &entry : entries_) {
            if (entry.first.first == tableName) {
                outKeyValues.emplace_back(entry.first.second, entry.second.value);
            }
        }
        return Status::OK();
    }

    Status Get(const std::string &tableName, const std::string &key, std::string &value) override
    {
        value.clear();
        auto iter = entries_.find({ tableName, key });
        if (iter == entries_.end()) {
            RETURN_STATUS(K_NOT_FOUND, "key not found");
        }
        value = iter->second.value;
        return Status::OK();
    }

    Status Get(const std::string &tableName, const std::string &key, RangeSearchResult &res,
               int32_t timeoutMs = SEND_RPC_TIMEOUT_MS_DEFAULT) override
    {
        (void)timeoutMs;
        res = {};
        auto iter = entries_.find({ tableName, key });
        if (iter == entries_.end()) {
            RETURN_STATUS(K_NOT_FOUND, "key not found");
        }
        res.key = key;
        res.value = iter->second.value;
        res.version = iter->second.version;
        res.modRevision = iter->second.revision;
        return Status::OK();
    }

    Status CAS(const std::string &tableName, const std::string &key, const ProcessFunction &processFunc,
               RangeSearchResult &res) override
    {
        constexpr uint32_t maxRetry = 10;
        Status lastErr;
        for (uint32_t retryNum = 0; retryNum < maxRetry; ++retryNum) {
            RETURN_IF_NOT_OK(Get(tableName, key, res));
            std::unique_ptr<std::string> newValue;
            bool retry = true;
            auto rc = processFunc(res.value, newValue, retry);
            if (rc.IsError()) {
                if (!retry) {
                    return rc;
                }
                lastErr = rc;
                continue;
            }
            if (newValue == nullptr) {
                return Status::OK();
            }
            InjectCasConflictIfNeeded(tableName, key);
            auto iter = entries_.find({ tableName, key });
            if (iter == entries_.end()) {
                RETURN_STATUS(K_TRY_AGAIN, "key was deleted during CAS");
            }
            if (iter->second.revision != res.modRevision) {
                if (!retry) {
                    RETURN_STATUS(K_TRY_AGAIN, "CAS revision mismatch");
                }
                lastErr = Status(K_TRY_AGAIN, "CAS revision mismatch");
                continue;
            }
            return PutForTest(tableName, key, *newValue);
        }
        return lastErr.IsError() ? lastErr : Status(K_TRY_AGAIN, "CAS retry limit exceeded");
    }

    Status CAS(const std::string &tableName, const std::string &key, const ProcessFunction &processFunc) override
    {
        RangeSearchResult res;
        return CAS(tableName, key, processFunc, res);
    }

    Status CAS(const std::string &tableName, const std::string &key, const std::string &oldValue,
               const std::string &newValue) override
    {
        std::string value;
        auto rc = Get(tableName, key, value);
        if (rc.GetCode() == K_NOT_FOUND && oldValue.empty()) {
            return PutForTest(tableName, key, newValue);
        }
        RETURN_IF_NOT_OK(rc);
        CHECK_FAIL_RETURN_STATUS(value == oldValue, K_TRY_AGAIN, "value mismatch");
        return PutForTest(tableName, key, newValue);
    }

    Status Delete(const std::string &tableName, const std::string &key) override
    {
        return DeleteForTest(tableName, key);
    }

    Status WatchEvents(const std::vector<WatchKey> &watchKeys) override
    {
        watchKeys_ = watchKeys;
        return Status::OK();
    }

    Status InitKeepAlive(const std::string &tableName, const std::string &key, bool isRestart,
                         bool isStoreAvailableWhenStart) override
    {
        KeepAliveValue value;
        value.timestamp = "1";
        value.state = isStoreAvailableWhenStart ? (isRestart ? "restart" : "start") : ETCD_NODE_DOWNGRADE_RESTART;
        return PutForTest(tableName, key, value.ToString());
    }

    Status UpdateNodeState(const std::string &state) override
    {
        nodeState_ = state;
        return Status::OK();
    }

    Status GetStorePrefix(const std::string &tableName, std::string &prefix) override
    {
        prefix = tableName.empty() || tableName.front() == '/' ? tableName : "/" + tableName;
        return Status::OK();
    }

    Status InformReconciliationDone(const HostPort &workerAddr) override
    {
        (void)workerAddr;
        return Status::OK();
    }

    bool IsKeepAliveTimeout() override
    {
        return false;
    }

    bool IsCreateFirstLease() override
    {
        return false;
    }

    void SetEventHandler(EventHandler &&eventHandler) override
    {
        eventHandler_ = std::move(eventHandler);
    }

    void SetCheckStoreStateWhenNetworkFailedHandler(std::function<bool()> handler) override
    {
        checkStoreStateHandler_ = std::move(handler);
    }

    void InjectCasConflictsForTest(uint32_t conflictCount)
    {
        casConflictCount_ = conflictCount;
    }

private:
    struct Entry {
        std::string value;
        int64_t version{ 0 };
        int64_t revision{ 0 };
    };

    void Emit(CoordinationEventType type, const std::string &tableName, const std::string &key, const Entry &entry)
    {
        if (eventHandler_ == nullptr) {
            return;
        }
        std::string prefix;
        (void)GetStorePrefix(tableName, prefix);
        CoordinationEvent event;
        event.type = type;
        event.key = key.empty() ? prefix + "/" : prefix + "/" + key;
        event.value = type == CoordinationEventType::DELETE ? "" : entry.value;
        event.version = entry.version;
        event.revision = entry.revision;
        eventHandler_(std::move(event));
    }

    void InjectCasConflictIfNeeded(const std::string &tableName, const std::string &key)
    {
        if (casConflictCount_ == 0) {
            return;
        }
        --casConflictCount_;
        auto iter = entries_.find({ tableName, key });
        if (iter == entries_.end()) {
            return;
        }
        (void)PutForTest(tableName, key, iter->second.value);
    }

    std::map<std::pair<std::string, std::string>, Entry> entries_;
    int64_t revision_{ 0 };
    EventHandler eventHandler_;
    std::function<bool()> checkStoreStateHandler_;
    std::vector<WatchKey> watchKeys_;
    std::string nodeState_;
    uint32_t casConflictCount_{ 0 };
};

}  // namespace topology
}  // namespace datasystem

#endif  // DATASYSTEM_TESTS_UT_COMMON_TOPOLOGY_FAKE_CLUSTER_STORE_H
