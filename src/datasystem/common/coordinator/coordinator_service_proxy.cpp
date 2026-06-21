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

#include "datasystem/common/coordinator/coordinator_service_proxy.h"

namespace datasystem {
namespace {
Status CoordinatorServiceProxyNotImplemented()
{
    return Status(StatusCode::K_NOT_READY, "coordinator service proxy is not implemented");
}
}  // namespace

Status CoordinatorServiceProxyImpl::Put(const std::string &key, const std::string &value, int64_t ttlMs,
                                        int64_t expectedVersion, int64_t &version, int64_t &revision)
{
    (void)key;
    (void)value;
    (void)ttlMs;
    (void)expectedVersion;
    (void)version;
    (void)revision;
    return CoordinatorServiceProxyNotImplemented();
}

Status CoordinatorServiceProxyImpl::Range(const std::string &key, const std::string &rangeEnd,
                                          std::vector<KeyValueEntry> &kvs, int64_t &revision)
{
    (void)key;
    (void)rangeEnd;
    (void)kvs;
    (void)revision;
    return CoordinatorServiceProxyNotImplemented();
}

Status CoordinatorServiceProxyImpl::DeleteRange(const std::string &key, const std::string &rangeEnd, int64_t &deleted,
                                                int64_t &revision)
{
    (void)key;
    (void)rangeEnd;
    (void)deleted;
    (void)revision;
    return CoordinatorServiceProxyNotImplemented();
}

Status CoordinatorServiceProxyImpl::WatchRange(const std::string &key, const std::string &rangeEnd,
                                               const std::string &watcherAddr, int64_t &watchId,
                                               std::vector<KeyValueEntry> &initialKvs)
{
    (void)key;
    (void)rangeEnd;
    (void)watcherAddr;
    (void)watchId;
    (void)initialKvs;
    return CoordinatorServiceProxyNotImplemented();
}

Status CoordinatorServiceProxyImpl::KeepAlive(const std::string &key, int64_t &ttlMs, int64_t &remainingTtlMs)
{
    (void)key;
    (void)ttlMs;
    (void)remainingTtlMs;
    return CoordinatorServiceProxyNotImplemented();
}

Status CoordinatorServiceProxyImpl::CAS(const std::string &key, const CasProcessFunc &processFunc, int64_t &version,
                                        int64_t &revision)
{
    (void)key;
    (void)processFunc;
    (void)version;
    (void)revision;
    return CoordinatorServiceProxyNotImplemented();
}
}  // namespace datasystem
