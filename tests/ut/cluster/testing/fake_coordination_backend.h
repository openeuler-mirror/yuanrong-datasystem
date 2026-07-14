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
#ifndef DATASYSTEM_TEST_UT_CLUSTER_TESTING_FAKE_COORDINATION_BACKEND_H
#define DATASYSTEM_TEST_UT_CLUSTER_TESTING_FAKE_COORDINATION_BACKEND_H

#include <chrono>
#include <condition_variable>
#include <map>
#include <mutex>

#include "datasystem/cluster/coordination_backend/coordination_backend.h"
#include "datasystem/cluster/model/topology_types.h"

namespace datasystem::cluster {

class FakeCoordinationBackend final : public ICoordinationBackend {
public:
    FakeCoordinationBackend() = default;
    ~FakeCoordinationBackend() override = default;
    Status GetAll(const std::string &table, std::vector<std::pair<std::string, std::string>> &values) override;
    Status Get(const std::string &table, const std::string &key, std::string &value) override;
    Status Get(const std::string &table, const std::string &key, RangeSearchResult &result,
               int32_t timeoutMs = SEND_RPC_TIMEOUT_MS_DEFAULT) override;
    Status CAS(const std::string &table, const std::string &key, const ProcessFunction &process,
               RangeSearchResult &result) override;
    Status CAS(const std::string &table, const std::string &key, const ProcessFunction &process) override;
    Status CAS(const std::string &table, const std::string &key, const std::string &oldValue,
               const std::string &newValue) override;
    Status Delete(const std::string &table, const std::string &key) override;
    Status Delete(const std::string &table, const std::string &key, int timeoutMs) override;
    Status WatchEvents(const std::vector<WatchKey> &watchKeys) override;
    Status InitKeepAlive(const std::string &, const std::string &, bool, bool) override;
    Status ShutdownEventSources() override;
    Status Shutdown() override;
    Status UpdateNodeState(MemberLifecycleState) override;
    Status GetStorePrefix(const std::string &table, std::string &prefix) override;
    Status InformReconciliationDone(const HostPort &) override;
    bool IsKeepAliveTimeout() override;
    bool IsFirstKeepAliveSent() override;
    void SetEventHandler(EventHandler &&handler) override;
    void SetCheckStoreStateWhenNetworkFailedHandler(std::function<bool()> handler) override;

    void PutBytes(const std::string &table, const std::string &key, std::string value);
    void PutRaw(const std::string &table, const std::string &key, const TopologyState &state);
    void FailNextCasAfterCommit();
    void FailNextCasBeforeCommit();
    void FailNextWatch();
    void FailNextGet();
    void EmitEvent(CoordinationEvent event);
    std::vector<WatchKey> WatchKeys() const;
    bool HasEventHandler() const;
    void BlockNextGet();
    bool WaitUntilGetBlocked(std::chrono::steady_clock::time_point deadline);
    void ReleaseBlockedGet();
    void SetBeforeDeleteHandler(std::function<void()> handler);

private:
    std::string FullKey(const std::string &table, const std::string &key) const;
    // Protects all mutable fake-backend values, revisions, handlers, watch state, fault-injection flags, and Get gates.
    mutable std::mutex mutex_;
    std::map<std::string, std::pair<std::string, int64_t>> values_;
    int64_t revision_{ 0 };
    EventHandler handler_;
    std::vector<WatchKey> watchKeys_;
    bool failNextCasAfterCommit_{ false };
    bool failNextCasBeforeCommit_{ false };
    bool failNextWatch_{ false };
    bool failNextGet_{ false };
    // Uses mutex_ to signal changes to getBlocked_ and releaseGet_.
    std::condition_variable getCv_;
    bool blockNextGet_{ false };
    bool getBlocked_{ false };
    bool releaseGet_{ false };
    std::function<void()> beforeDeleteHandler_;
};

}  // namespace datasystem::cluster

#endif  // DATASYSTEM_TEST_UT_CLUSTER_TESTING_FAKE_COORDINATION_BACKEND_H
