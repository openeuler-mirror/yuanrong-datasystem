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

#include "datasystem/common/urma_mock/registry/import_endpoint_registry.h"

#include <mutex>
#include <unordered_map>

namespace datasystem {
namespace urma_mock {
namespace {
struct ImportEndpointMap {
    std::mutex mu;
    std::unordered_map<uint64_t, ImportEndpoint> endpoints;
    std::unordered_map<std::string, ImportEndpoint> endpointsByClient;
    std::unordered_map<std::string, ImportEndpoint> endpointsByVa;
};

ImportEndpointMap &ImportEndpointMapInstance()
{
    static ImportEndpointMap *s = new ImportEndpointMap();
    return *s;
}

std::string BuildImportEndpointClientKey(uint64_t token, const std::string &clientId)
{
    return std::to_string(token) + ":" + clientId;
}

std::string BuildImportEndpointVaKey(uint64_t token, uint64_t remoteVa)
{
    return std::to_string(token) + ":" + std::to_string(remoteVa);
}

}  // namespace

ImportEndpointRegistry &ImportEndpointRegistry::Instance()
{
    static ImportEndpointRegistry *s = new ImportEndpointRegistry();
    return *s;
}

void ImportEndpointRegistry::Register(uint64_t token, const ImportEndpoint &ep)
{
    auto &r = ImportEndpointMapInstance();
    std::lock_guard<std::mutex> lk(r.mu);
    r.endpoints[token] = ep;
    if (ep.va != 0) {
        r.endpointsByVa[BuildImportEndpointVaKey(token, ep.va)] = ep;
    }
}

void ImportEndpointRegistry::Register(uint64_t token, const std::string &clientId, const ImportEndpoint &ep)
{
    if (clientId.empty()) {
        Register(token, ep);
        return;
    }
    auto &r = ImportEndpointMapInstance();
    std::lock_guard<std::mutex> lk(r.mu);
    auto newEp = ep;
    newEp.clientId = clientId;
    r.endpointsByClient[BuildImportEndpointClientKey(token, clientId)] = newEp;
    if (newEp.va != 0) {
        r.endpointsByVa[BuildImportEndpointVaKey(token, newEp.va)] = newEp;
    }
    r.endpoints[token] = newEp;
}

ImportEndpoint ImportEndpointRegistry::Lookup(uint64_t token) const
{
    auto &r = ImportEndpointMapInstance();
    std::lock_guard<std::mutex> lk(r.mu);
    auto it = r.endpoints.find(token);
    if (it == r.endpoints.end()) {
        return {};
    }
    return it->second;
}

ImportEndpoint ImportEndpointRegistry::Lookup(uint64_t token, uint64_t remoteVa) const
{
    if (remoteVa == 0) {
        return Lookup(token);
    }
    auto &r = ImportEndpointMapInstance();
    std::lock_guard<std::mutex> lk(r.mu);
    auto it = r.endpointsByVa.find(BuildImportEndpointVaKey(token, remoteVa));
    return it == r.endpointsByVa.end() ? ImportEndpoint{} : it->second;
}

ImportEndpoint ImportEndpointRegistry::Lookup(uint64_t token, const std::string &clientId) const
{
    if (clientId.empty()) {
        return Lookup(token);
    }
    auto &r = ImportEndpointMapInstance();
    std::lock_guard<std::mutex> lk(r.mu);
    auto it = r.endpointsByClient.find(BuildImportEndpointClientKey(token, clientId));
    if (it == r.endpointsByClient.end()) {
        return {};
    }
    return it->second;
}

void ImportEndpointRegistry::Clear()
{
    auto &r = ImportEndpointMapInstance();
    std::lock_guard<std::mutex> lk(r.mu);
    r.endpoints.clear();
    r.endpointsByClient.clear();
    r.endpointsByVa.clear();
}

}  // namespace urma_mock
}  // namespace datasystem
