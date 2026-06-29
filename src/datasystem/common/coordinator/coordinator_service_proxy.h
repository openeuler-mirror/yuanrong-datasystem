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

/**
 * Description: Coordinator service proxy interface and default implementation skeleton.
 */
#ifndef DATASYSTEM_COMMON_COORDINATOR_COORDINATOR_SERVICE_PROXY_H
#define DATASYSTEM_COMMON_COORDINATOR_COORDINATOR_SERVICE_PROXY_H

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "datasystem/common/coordinator/key_value_entry.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/utils/status.h"

namespace datasystem {
constexpr int32_t DEFAULT_COORDINATOR_RPC_TIMEOUT_MS = 3'000;
class ICoordinatorServiceProxy {
public:
    using CasProcessFunc =
        std::function<Status(const std::string &oldValue, std::unique_ptr<std::string> &newValue, bool &retry)>;

    virtual ~ICoordinatorServiceProxy() = default;

    /**
     * @brief Put a key-value pair.
     * @param[in] expectedVersion COORDINATOR_NO_VERSION_CHECK means no check.
     * COORDINATOR_KEY_NOT_EXISTS_VERSION means the key must not exist. Positive values require exact version match.
     */
    virtual Status Put(const std::string &key, const std::string &value, int64_t ttlMs, int64_t expectedVersion,
                       int64_t &version, int64_t &revision, int32_t timeoutMs = DEFAULT_COORDINATOR_RPC_TIMEOUT_MS) = 0;
    virtual Status Range(const std::string &key, const std::string &rangeEnd, std::vector<KeyValueEntry> &kvs,
                         int64_t &revision, int32_t timeoutMs = DEFAULT_COORDINATOR_RPC_TIMEOUT_MS) = 0;
    virtual Status DeleteRange(const std::string &key, const std::string &rangeEnd, int64_t &deleted, int64_t &revision,
                               int32_t timeoutMs = DEFAULT_COORDINATOR_RPC_TIMEOUT_MS) = 0;
    virtual Status WatchRange(const std::string &key, const std::string &rangeEnd, const std::string &watcherAddr,
                              int64_t &watchId, std::vector<KeyValueEntry> &initialKvs,
                              int32_t timeoutMs = DEFAULT_COORDINATOR_RPC_TIMEOUT_MS) = 0;
    virtual Status CancelWatch(const std::string &watcherAddr, const std::vector<int64_t> &watchIds,
                               int32_t timeoutMs = DEFAULT_COORDINATOR_RPC_TIMEOUT_MS) = 0;
    virtual Status KeepAlive(const std::string &key, int64_t &ttlMs, int64_t &remainingTtlMs,
                             int32_t timeoutMs = DEFAULT_COORDINATOR_RPC_TIMEOUT_MS) = 0;
    virtual Status CAS(const std::string &key, const CasProcessFunc &processFunc, int64_t &version,
                       int64_t &revision) = 0;
};

class CoordinatorServiceProxyBase : public ICoordinatorServiceProxy {
public:
    CoordinatorServiceProxyBase() = default;
    explicit CoordinatorServiceProxyBase(HostPort coordinatorAddr) : coordinatorAddr_(std::move(coordinatorAddr))
    {
    }
    ~CoordinatorServiceProxyBase() override = default;

    Status CAS(const std::string &key, const CasProcessFunc &processFunc, int64_t &version, int64_t &revision) override;

protected:
    HostPort coordinatorAddr_;
};

class CoordinatorServiceProxyZmqImpl : public CoordinatorServiceProxyBase {
public:
    using CoordinatorServiceProxyBase::CoordinatorServiceProxyBase;
    ~CoordinatorServiceProxyZmqImpl() override = default;

    Status Put(const std::string &key, const std::string &value, int64_t ttlMs, int64_t expectedVersion,
               int64_t &version, int64_t &revision, int32_t timeoutMs) override;
    Status Range(const std::string &key, const std::string &rangeEnd, std::vector<KeyValueEntry> &kvs,
                 int64_t &revision, int32_t timeoutMs) override;
    Status DeleteRange(const std::string &key, const std::string &rangeEnd, int64_t &deleted, int64_t &revision,
                       int32_t timeoutMs) override;
    Status WatchRange(const std::string &key, const std::string &rangeEnd, const std::string &watcherAddr,
                      int64_t &watchId, std::vector<KeyValueEntry> &initialKvs, int32_t timeoutMs) override;
    Status CancelWatch(const std::string &watcherAddr, const std::vector<int64_t> &watchIds,
                       int32_t timeoutMs) override;
    Status KeepAlive(const std::string &key, int64_t &ttlMs, int64_t &remainingTtlMs, int32_t timeoutMs) override;
};

class CoordinatorServiceProxyBrpcImpl : public CoordinatorServiceProxyBase {
public:
    using CoordinatorServiceProxyBase::CoordinatorServiceProxyBase;
    ~CoordinatorServiceProxyBrpcImpl() override = default;

    Status Put(const std::string &key, const std::string &value, int64_t ttlMs, int64_t expectedVersion,
               int64_t &version, int64_t &revision, int32_t timeoutMs) override;
    Status Range(const std::string &key, const std::string &rangeEnd, std::vector<KeyValueEntry> &kvs,
                 int64_t &revision, int32_t timeoutMs) override;
    Status DeleteRange(const std::string &key, const std::string &rangeEnd, int64_t &deleted, int64_t &revision,
                       int32_t timeoutMs) override;
    Status WatchRange(const std::string &key, const std::string &rangeEnd, const std::string &watcherAddr,
                      int64_t &watchId, std::vector<KeyValueEntry> &initialKvs, int32_t timeoutMs) override;
    Status CancelWatch(const std::string &watcherAddr, const std::vector<int64_t> &watchIds,
                       int32_t timeoutMs) override;
    Status KeepAlive(const std::string &key, int64_t &ttlMs, int64_t &remainingTtlMs, int32_t timeoutMs) override;
};
}  // namespace datasystem
#endif  // DATASYSTEM_COMMON_COORDINATOR_COORDINATOR_SERVICE_PROXY_H
