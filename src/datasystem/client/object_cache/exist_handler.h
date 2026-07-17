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

/** Description: Defines routed Exist orchestration for the object client. */
#ifndef DATASYSTEM_CLIENT_OBJECT_CACHE_EXIST_FLOW_H
#define DATASYSTEM_CLIENT_OBJECT_CACHE_EXIST_FLOW_H

#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "datasystem/client/routing/routing.h"
#include "datasystem/client/transport/data_plane/i_data_transporter.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/thread_pool.h"
#include "datasystem/utils/sensitive_value.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace client {
class TransportLayer;
}  // namespace client

namespace object_cache {

class IExistRouting {
public:
    virtual ~IExistRouting() = default;
    virtual Status SelectWorkers(const std::vector<std::string> &keys, client::SelectStrategy strategy,
                                 std::unordered_map<HostPort, std::vector<std::string>> &groups) = 0;
    virtual void UpdateState(const HostPort &addr, StatusCode status) = 0;
};

class IExistTransport {
public:
    virtual ~IExistTransport() = default;
    virtual Status Exist(const HostPort &workerAddr, const client::TransportExistRequest &input,
                         client::TransportExistResult &output) = 0;
};

struct ExistHandlerRequest {
    std::vector<std::string> keys;
    bool queryL2Cache;
    bool isLocal;
    int32_t requestTimeoutMs;
    std::string clientId;
    std::string tenantId;
    SensitiveValue token;
};

struct ExistRedirectGroup {
    HostPort worker;
    std::vector<std::string> keys;
};

struct ExistRedirectResolution {
    bool parsedStructuredRedirect = false;
    std::vector<ExistRedirectGroup> redirectGroups;
    std::vector<std::string> missingKeys;
};

class ExistRedirectResolver {
public:
    ~ExistRedirectResolver() = default;

    Status ResolveStructuredRedirect(const Status &rc, const std::vector<std::string> &requestKeys,
                                     const std::string &tenantId, ExistRedirectResolution &resolution) const;
    Status ParseAddressOnlyRedirect(const Status &rc, HostPort &worker) const;

private:
    Status ParseRedirectJson(const std::string &extra, const std::vector<std::string> &requestKeys,
                             const std::string &tenantId, ExistRedirectResolution &resolution) const;
    Status BuildRedirectGroups(const std::vector<std::string> &requestKeys, const std::string &tenantId,
                               const std::string &redirectsJson, ExistRedirectResolution &resolution) const;
    Status AssembleGroups(const std::vector<std::string> &requestKeys,
                          const std::unordered_map<std::string, HostPort> &redirectByKey,
                          const std::unordered_set<std::string> &coveredKeys, bool hasFallbackWorker,
                          const HostPort &fallbackWorker, ExistRedirectResolution &resolution) const;
};

class ExistHandler {
public:
    ExistHandler(std::shared_ptr<client::Routing> routing, client::TransportLayer *transport,
                 std::shared_ptr<ThreadPool> taskPool);

    ExistHandler(std::shared_ptr<IExistRouting> routing, std::shared_ptr<IExistTransport> transport,
                 std::shared_ptr<ThreadPool> taskPool);

    ~ExistHandler() = default;

    Status Run(const ExistHandlerRequest &request, std::vector<bool> &exists);

private:
    Status SelectWorkers(const std::vector<std::string> &keys, client::SelectStrategy strategy,
                         std::unordered_map<HostPort, std::vector<std::string>> &groups);

    void UpdateRoutingState(const HostPort &addr, StatusCode status);

    Status RunSelectedWorkers(const ExistHandlerRequest &request,
                              const std::unordered_map<std::string, std::vector<size_t>> &keyIndexes,
                              std::vector<bool> &exists);

    Status HandleRedirect(const Status &rc, HostPort &worker, const std::vector<std::string> &workerKeys,
                          const ExistHandlerRequest &request,
                          const std::unordered_map<std::string, std::vector<size_t>> &keyIndexes,
                          std::vector<bool> &exists, int32_t &redirectRetries, bool &redirected);

    std::shared_ptr<client::Routing> clientRouting_;
    std::shared_ptr<IExistRouting> testRouting_;
    client::TransportLayer *transportLayer_;
    std::shared_ptr<IExistTransport> testTransport_;
    std::shared_ptr<ThreadPool> taskPool_;
    ExistRedirectResolver redirectResolver_;
};

}  // namespace object_cache
}  // namespace datasystem

#endif  // DATASYSTEM_CLIENT_OBJECT_CACHE_EXIST_FLOW_H
