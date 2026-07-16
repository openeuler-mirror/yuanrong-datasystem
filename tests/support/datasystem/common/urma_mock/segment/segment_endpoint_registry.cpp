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

#include "datasystem/common/urma_mock/segment/segment_endpoint_registry.h"

#include <dirent.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <functional>

#include "datasystem/common/urma_mock/transport/uds_transport.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/strings_util.h"

namespace datasystem {
namespace urma_mock {
namespace {

constexpr mode_t K_UDS_DIR_MODE = 0700;
constexpr const char *K_ENDPOINT_SUFFIX = ".endpoint";

struct SegmentEndpointRecord {
    std::string key;
    ImportEndpoint ep;
};

std::string SegmentRegistryDir()
{
    auto registrySock = ResolveUdsPathForInstance("__segment_registry__");
    auto slash = registrySock.find_last_of('/');
    return slash == std::string::npos ? std::string{} : registrySock.substr(0, slash);
}

bool MkdirAll(const std::string &dir)
{
    if (dir.empty()) {
        return true;
    }
    size_t pos = dir[0] == '/' ? 1 : 0;
    while (pos <= dir.size()) {
        auto next = dir.find('/', pos);
        auto current = dir.substr(0, next);
        if (!current.empty() && ::mkdir(current.c_str(), K_UDS_DIR_MODE) != 0 && errno != EEXIST) {
            LOG(WARNING) << "[MockUrma-UDS] mkdir " << current << " failed: " << StrErr(errno);
            return false;
        }
        if (next == std::string::npos) {
            break;
        }
        pos = next + 1;
    }
    return true;
}

std::string SegmentEndpointFilePath(const std::string &key)
{
    std::hash<std::string> hasher;
    return SegmentRegistryDir() + "/seg_" + std::to_string(hasher(key)) + ".endpoint";
}

bool IsEndpointFile(const std::string &name)
{
    return name.size() > std::strlen(K_ENDPOINT_SUFFIX)
           && name.compare(name.size() - std::strlen(K_ENDPOINT_SUFFIX), std::strlen(K_ENDPOINT_SUFFIX),
                           K_ENDPOINT_SUFFIX)
                  == 0;
}

bool ReadSegmentEndpointFile(const std::string &filePath, SegmentEndpointRecord &record)
{
    std::ifstream in(filePath);
    if (!in.is_open()) {
        return false;
    }
    if (!std::getline(in, record.key) || !std::getline(in, record.ep.instanceId)) {
        return false;
    }
    in >> record.ep.va >> record.ep.len;
    return !record.key.empty() && !record.ep.instanceId.empty() && record.ep.va != 0 && record.ep.len != 0;
}

bool EndpointSocketExists(const ImportEndpoint &ep)
{
    std::string path;
    if (!ep.host.empty() && ep.port >= 0) {
        path = ResolveUdsPathForHost(ep.host, ep.port);
    } else {
        path = ResolveUdsPathForInstance(ep.instanceId);
    }
    struct stat st {};
    return !path.empty() && ::stat(path.c_str(), &st) == 0 && S_ISSOCK(st.st_mode);
}

bool EndpointContainsVa(const ImportEndpoint &ep, uint64_t requestedVa)
{
    return requestedVa >= ep.va && requestedVa - ep.va < ep.len;
}

void UpdateBestEndpoint(const ImportEndpoint &candidate, uint64_t requestedVa, ImportEndpoint &best)
{
    if (!EndpointContainsVa(candidate, requestedVa)) {
        return;
    }
    if (best.instanceId.empty() || candidate.va > best.va || (candidate.va == best.va && candidate.len < best.len)) {
        best = candidate;
    }
}

ImportEndpoint LookupEndpointRecord(const std::function<bool(const std::string &)> &matchKey, uint64_t requestedVa)
{
    auto dirPath = SegmentRegistryDir();
    DIR *dir = ::opendir(dirPath.c_str());
    if (dir == nullptr) {
        return {};
    }
    Raii dirGuard([dir]() { ::closedir(dir); });
    ImportEndpoint best;
    while (auto *entry = ::readdir(dir)) {
        std::string name = entry->d_name;
        if (name == "." || name == "..") {
            continue;
        }
        SegmentEndpointRecord record;
        std::string recordPath = dirPath;
        recordPath.append("/").append(name);
        if (!ReadSegmentEndpointFile(recordPath, record) || !matchKey(record.key)) {
            continue;
        }
        if (!EndpointSocketExists(record.ep)) {
            static_cast<void>(::unlink(recordPath.c_str()));
            continue;
        }
        UpdateBestEndpoint(record.ep, requestedVa, best);
    }
    return best;
}

ImportEndpoint LookupContainingSegmentEndpoint(const std::string &prefix, uint64_t requestedVa)
{
    return LookupEndpointRecord([&prefix](const std::string &key) { return key.rfind(prefix, 0) == 0; }, requestedVa);
}

ImportEndpoint LookupTokenEndpoint(uint64_t token, uint64_t requestedVa)
{
    const auto tokenPart = ":" + std::to_string(token) + ":";
    return LookupEndpointRecord(
        [&tokenPart](const std::string &key) { return key.find(tokenPart) != std::string::npos; }, requestedVa);
}

void RemoveEndpointFiles(const std::string &instanceId)
{
    auto dirPath = SegmentRegistryDir();
    DIR *dir = ::opendir(dirPath.c_str());
    if (dir == nullptr) {
        return;
    }
    Raii dirGuard([dir]() { ::closedir(dir); });
    while (auto *entry = ::readdir(dir)) {
        std::string name = entry->d_name;
        if (!IsEndpointFile(name)) {
            continue;
        }
        std::string recordPath = dirPath;
        recordPath.append("/").append(name);
        SegmentEndpointRecord record;
        if (ReadSegmentEndpointFile(recordPath, record) && record.ep.instanceId == instanceId) {
            static_cast<void>(::unlink(recordPath.c_str()));
        }
    }
}

}  // namespace

const SegmentEndpointRegistry &SegmentEndpointRegistry::Instance()
{
    static SegmentEndpointRegistry *s = new SegmentEndpointRegistry();
    return *s;
}

bool SegmentEndpointRegistry::Register(const std::string &key, const ImportEndpoint &ep) const
{
    if (key.empty() || ep.instanceId.empty() || ep.va == 0 || ep.len == 0) {
        return false;
    }
    auto registryDir = SegmentRegistryDir();
    if (!MkdirAll(registryDir)) {
        return false;
    }
    auto filePath = SegmentEndpointFilePath(key);
    auto tmpPath = filePath + ".tmp." + std::to_string(static_cast<long long>(::getpid()));
    std::ofstream out(tmpPath, std::ios::trunc);
    if (!out.is_open()) {
        LOG(WARNING) << "[MockUrma-UDS] open segment endpoint registry " << tmpPath << " failed";
        return false;
    }
    out << key << '\n' << ep.instanceId << '\n' << ep.va << '\n' << ep.len << '\n';
    out.close();
    if (::rename(tmpPath.c_str(), filePath.c_str()) != 0) {
        LOG(WARNING) << "[MockUrma-UDS] publish segment endpoint registry " << filePath << " failed: " << StrErr(errno);
        static_cast<void>(::unlink(tmpPath.c_str()));
        return false;
    }
    return true;
}

void SegmentEndpointRegistry::Unregister(const std::string &key) const
{
    if (!key.empty()) {
        static_cast<void>(::unlink(SegmentEndpointFilePath(key).c_str()));
    }
}

void SegmentEndpointRegistry::Clear(const std::string &instanceId) const
{
    if (!instanceId.empty()) {
        RemoveEndpointFiles(instanceId);
    }
}

ImportEndpoint SegmentEndpointRegistry::Lookup(const std::string &key, const std::string &containingPrefix,
                                               uint64_t requestedVa) const
{
    if (key.empty() || containingPrefix.empty() || requestedVa == 0) {
        return {};
    }
    SegmentEndpointRecord record;
    auto filePath = SegmentEndpointFilePath(key);
    if (ReadSegmentEndpointFile(filePath, record) && EndpointSocketExists(record.ep)) {
        return record.ep;
    }
    static_cast<void>(::unlink(filePath.c_str()));
    return LookupContainingSegmentEndpoint(containingPrefix, requestedVa);
}

ImportEndpoint SegmentEndpointRegistry::Lookup(uint64_t token, uint64_t requestedVa) const
{
    if (token == 0 || requestedVa == 0) {
        return {};
    }
    return LookupTokenEndpoint(token, requestedVa);
}

}  // namespace urma_mock
}  // namespace datasystem
