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

#include <functional>
#include <string>
#include <vector>

#include "datasystem/common/coordinator/key_value_entry.h"
#include "datasystem/utils/status.h"

namespace datasystem {
class ICoordinatorServiceProxy {
public:
    using CasProcessFunc = std::function<Status(const std::string &oldValue, std::string &newValue, bool &retry)>;

    virtual ~ICoordinatorServiceProxy() = default;

    virtual Status Put(const std::string &key, const std::string &value, int64_t ttlMs, int64_t expectedVersion,
                       int64_t &version, int64_t &revision) = 0;
    virtual Status Range(const std::string &key, const std::string &rangeEnd, std::vector<KeyValueEntry> &kvs,
                         int64_t &revision) = 0;
    virtual Status DeleteRange(const std::string &key, const std::string &rangeEnd, int64_t &deleted,
                               int64_t &revision) = 0;
    virtual Status WatchRange(const std::string &key, const std::string &rangeEnd, const std::string &watcherAddr,
                              int64_t &watchId, std::vector<KeyValueEntry> &initialKvs) = 0;
    virtual Status KeepAlive(const std::string &key, int64_t &ttlMs, int64_t &remainingTtlMs) = 0;
    virtual Status CAS(const std::string &key, const CasProcessFunc &processFunc, int64_t &version,
                       int64_t &revision) = 0;
};

class CoordinatorServiceProxyImpl : public ICoordinatorServiceProxy {
public:
    CoordinatorServiceProxyImpl() = default;
    ~CoordinatorServiceProxyImpl() override = default;

    Status Put(const std::string &key, const std::string &value, int64_t ttlMs, int64_t expectedVersion,
               int64_t &version, int64_t &revision) override;
    Status Range(const std::string &key, const std::string &rangeEnd, std::vector<KeyValueEntry> &kvs,
                 int64_t &revision) override;
    Status DeleteRange(const std::string &key, const std::string &rangeEnd, int64_t &deleted,
                       int64_t &revision) override;
    Status WatchRange(const std::string &key, const std::string &rangeEnd, const std::string &watcherAddr,
                      int64_t &watchId, std::vector<KeyValueEntry> &initialKvs) override;
    Status KeepAlive(const std::string &key, int64_t &ttlMs, int64_t &remainingTtlMs) override;
    Status CAS(const std::string &key, const CasProcessFunc &processFunc, int64_t &version, int64_t &revision) override;
};
}  // namespace datasystem
#endif  // DATASYSTEM_COMMON_COORDINATOR_COORDINATOR_SERVICE_PROXY_H
