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

#ifndef DATASYSTEM_COMMON_UTIL_COMPATIBILITY_MANAGER_H
#define DATASYSTEM_COMMON_UTIL_COMPATIBILITY_MANAGER_H

#include <cstdint>
#include <functional>
#include <map>
#include <mutex>
#include <string>
#include <unordered_map>

#include "datasystem/utils/status.h"

namespace datasystem {
// NONE is a sentinel value that is never supported by any version.
// Use it to represent "no feature" rather than as a real capability flag.
enum class FeatureFlag : uint32_t {
    NONE = 0,
};

class CompatibilityVersion {
public:
    CompatibilityVersion();
    CompatibilityVersion(uint32_t major, uint32_t minor, uint32_t patch);
    ~CompatibilityVersion() = default;

    uint32_t Major() const;
    uint32_t Minor() const;
    uint32_t Patch() const;
    bool IsValid() const;
    std::string ToString() const;
    static Status FromString(const std::string &str, CompatibilityVersion &version);

    bool operator==(const CompatibilityVersion &other) const;
    bool operator!=(const CompatibilityVersion &other) const;
    bool operator<(const CompatibilityVersion &other) const;
    bool operator<=(const CompatibilityVersion &other) const;
    bool operator>(const CompatibilityVersion &other) const;
    bool operator>=(const CompatibilityVersion &other) const;

private:
    uint32_t major_;
    uint32_t minor_;
    uint32_t patch_;
};

struct CompatibilityVersionHash {
    size_t operator()(const CompatibilityVersion &v) const;
};
}  // namespace datasystem

// Inject std::hash specialization so CompatibilityVersion works as unordered_map key
// without requiring a custom hasher template argument.
// Must appear before CompatibilityManager because featureMaps_ uses
// unordered_map<CompatibilityVersion, ...> in the class body.
namespace std {
template <>
struct hash<datasystem::CompatibilityVersion> {
    size_t operator()(const datasystem::CompatibilityVersion &v) const
    {
        return datasystem::CompatibilityVersionHash()(v);
    }
};
}  // namespace std

namespace datasystem {

class CompatibilityManager {
public:
    ~CompatibilityManager() = default;
    static CompatibilityManager &Instance();

    bool Supports(const CompatibilityVersion &compatibilityVersion, FeatureFlag feature) const;
    void UpdateClusterCompatibilityVersion(const CompatibilityVersion &compatibilityVersion);
    const CompatibilityVersion &GetBaselineCompatibilityVersion() const;
    const CompatibilityVersion &GetCurrentCompatibilityVersion() const;
    CompatibilityVersion GetClusterCompatibilityVersion() const;

private:
    CompatibilityManager();

    // Maps compatibility versions to their supported feature flags.
    // Initialized in the constructor and never updated afterwards.
    std::unordered_map<CompatibilityVersion, std::map<FeatureFlag, bool>> featureMaps_;
    // Fixed lower-bound version for missing/invalid records from older binaries. Do not raise this when current
    // changes.
    const CompatibilityVersion baselineCompatibilityVersion_{ 1, 0, 0 };
    const CompatibilityVersion currentCompatibilityVersion_{ baselineCompatibilityVersion_ };
    mutable std::mutex mutex_;
    // Protected by mutex_. Tracks the lowest READY member version and falls back to baselineCompatibilityVersion_
    // when no READY member exists.
    CompatibilityVersion clusterCompatibilityVersion_;
};

}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_UTIL_COMPATIBILITY_MANAGER_H
