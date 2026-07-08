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

#include "datasystem/common/util/compatibility_manager.h"

#include "datasystem/common/log/log.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {

CompatibilityVersion::CompatibilityVersion() : major_(0), minor_(0), patch_(0)
{
}

CompatibilityVersion::CompatibilityVersion(uint32_t major, uint32_t minor, uint32_t patch)
    : major_(major), minor_(minor), patch_(patch)
{
}

uint32_t CompatibilityVersion::Major() const
{
    return major_;
}

uint32_t CompatibilityVersion::Minor() const
{
    return minor_;
}

uint32_t CompatibilityVersion::Patch() const
{
    return patch_;
}

bool CompatibilityVersion::IsValid() const
{
    return major_ != 0 || minor_ != 0 || patch_ != 0;
}

std::string CompatibilityVersion::ToString() const
{
    return std::to_string(major_) + "." + std::to_string(minor_) + "." + std::to_string(patch_);
}

namespace {
bool ParseVersionComponent(const std::string &s, uint32_t &out)
{
    if (s.empty()) {
        return false;
    }
    // Reject leading zeros (e.g. "01") to keep SemVer semantics.
    // "0" by itself is valid.
    if (s.size() > 1 && s[0] == '0') {
        return false;
    }
    for (auto c : s) {
        if (c < '0' || c > '9') {
            return false;
        }
    }
    try {
        auto val = std::stoul(s);
        if (val > UINT32_MAX) {
            return false;
        }
        out = static_cast<uint32_t>(val);
    } catch (const std::exception &) {
        return false;
    }
    return true;
}
}  // namespace

Status CompatibilityVersion::FromString(const std::string &str, CompatibilityVersion &version)
{
    if (str.empty()) {
        RETURN_STATUS(K_INVALID, "compatibility version string is empty");
    }
    // Expected format: major.minor.patch (e.g. "1.0.0")
    auto firstDot = str.find('.');
    if (firstDot == std::string::npos || firstDot == 0 || firstDot + 1 >= str.size()) {
        RETURN_STATUS(K_INVALID, FormatString("invalid compatibility version format: %s", str));
    }
    auto secondDot = str.find('.', firstDot + 1);
    if (secondDot == std::string::npos || secondDot == firstDot + 1 || secondDot + 1 >= str.size()) {
        RETURN_STATUS(K_INVALID, FormatString("invalid compatibility version format: %s", str));
    }
    // Reject more than three components.
    auto thirdDot = str.find('.', secondDot + 1);
    if (thirdDot != std::string::npos) {
        RETURN_STATUS(K_INVALID, FormatString("invalid compatibility version format: %s", str));
    }
    std::string majorStr = str.substr(0, firstDot);
    std::string minorStr = str.substr(firstDot + 1, secondDot - firstDot - 1);
    std::string patchStr = str.substr(secondDot + 1);
    uint32_t major = 0;
    uint32_t minor = 0;
    uint32_t patch = 0;
    if (!ParseVersionComponent(majorStr, major) || !ParseVersionComponent(minorStr, minor)
        || !ParseVersionComponent(patchStr, patch)) {
        RETURN_STATUS(K_INVALID, FormatString("invalid compatibility version format: %s", str));
    }
    version = CompatibilityVersion(major, minor, patch);
    return Status::OK();
}

bool CompatibilityVersion::operator==(const CompatibilityVersion &other) const
{
    return major_ == other.major_ && minor_ == other.minor_ && patch_ == other.patch_;
}

bool CompatibilityVersion::operator!=(const CompatibilityVersion &other) const
{
    return !(*this == other);
}

bool CompatibilityVersion::operator<(const CompatibilityVersion &other) const
{
    if (major_ != other.major_) {
        return major_ < other.major_;
    }
    if (minor_ != other.minor_) {
        return minor_ < other.minor_;
    }
    return patch_ < other.patch_;
}

bool CompatibilityVersion::operator<=(const CompatibilityVersion &other) const
{
    return !(other < *this);
}

bool CompatibilityVersion::operator>(const CompatibilityVersion &other) const
{
    return other < *this;
}

bool CompatibilityVersion::operator>=(const CompatibilityVersion &other) const
{
    return !(*this < other);
}

size_t CompatibilityVersionHash::operator()(const CompatibilityVersion &v) const
{
    // Golden ratio constants used for hash combining.
    // This is the same constant used by Boost::hash_combine
    constexpr uint32_t goldenRatio32 = 0x9e3779b9U;
    constexpr int leftShift = 6;
    constexpr int rightShift = 2;
    size_t h = std::hash<uint32_t>{}(v.Major());
    h ^= std::hash<uint32_t>{}(v.Minor()) + goldenRatio32 + (h << leftShift) + (h >> rightShift);
    h ^= std::hash<uint32_t>{}(v.Patch()) + goldenRatio32 + (h << leftShift) + (h >> rightShift);
    return h;
}

CompatibilityManager &CompatibilityManager::Instance()
{
    static CompatibilityManager instance;
    return instance;
}

CompatibilityManager::CompatibilityManager() : clusterCompatibilityVersion_{ baselineCompatibilityVersion_ }
{
    // Pre-populate the baseline version entry. NONE is a sentinel that is
    // never supported (no entry in the inner map), in line with the
    // FeatureFlag::NONE documentation.
    featureMaps_.emplace(baselineCompatibilityVersion_, std::map<FeatureFlag, bool>());
}

bool CompatibilityManager::Supports(const CompatibilityVersion &compatibilityVersion, FeatureFlag feature) const
{
    auto it = featureMaps_.find(compatibilityVersion);
    if (it == featureMaps_.end()) {
        // Version not declared in featureMaps_ defaults to not supported.
        return false;
    }
    auto featureIt = it->second.find(feature);
    if (featureIt == it->second.end()) {
        // Feature not declared for this version defaults to not supported.
        return false;
    }
    return featureIt->second;
}

void CompatibilityManager::UpdateClusterCompatibilityVersion(const CompatibilityVersion &compatibilityVersion)
{
    std::lock_guard<std::mutex> lock(mutex_);
    if (compatibilityVersion != clusterCompatibilityVersion_) {
        clusterCompatibilityVersion_ = compatibilityVersion;
        LOG(INFO) << "Cluster compatibility version updated to: " << compatibilityVersion.ToString();
    }
}

const CompatibilityVersion &CompatibilityManager::GetBaselineCompatibilityVersion() const
{
    return baselineCompatibilityVersion_;
}

const CompatibilityVersion &CompatibilityManager::GetCurrentCompatibilityVersion() const
{
    return currentCompatibilityVersion_;
}

CompatibilityVersion CompatibilityManager::GetClusterCompatibilityVersion() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    return clusterCompatibilityVersion_;
}

}  // namespace datasystem
