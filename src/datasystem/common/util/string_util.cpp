/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
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
 * Description: Strings util function.
 */
#ifndef DATASYSTEM_COMMON_UTIL_STRINGS_UTIL_H
#define DATASYSTEM_COMMON_UTIL_STRINGS_UTIL_H

#include "datasystem/common/util/strings_util.h"

#include <set>

#include "re2/re2.h"

namespace datasystem {
namespace {
constexpr std::string_view SRC_LOG_PREFIX = ", src=";
constexpr std::string_view DST_LOG_PREFIX = ", dst=";

void AppendSrcDstFields(std::string &result, std::string_view srcAddr, std::string_view dstAddr)
{
    if (!srcAddr.empty()) {
        result.append(SRC_LOG_PREFIX.data(), SRC_LOG_PREFIX.size());
        result.append(srcAddr.data(), srcAddr.size());
    }
    if (!dstAddr.empty()) {
        result.append(DST_LOG_PREFIX.data(), DST_LOG_PREFIX.size());
        result.append(dstAddr.data(), dstAddr.size());
    }
}

size_t CalcSrcDstLogSize(std::string_view srcAddr, std::string_view dstAddr)
{
    return (srcAddr.empty() ? 0 : SRC_LOG_PREFIX.size() + srcAddr.size())
        + (dstAddr.empty() ? 0 : DST_LOG_PREFIX.size() + dstAddr.size());
}
}  // namespace

std::vector<std::string> SplitToUniqueStr(const std::string &typeStr, const std::string &pattern)
{
    if (typeStr.empty()) {
        return {};
    }
    re2::StringPiece text(typeStr);
    re2::RE2 re2Pattern(pattern);
    std::vector<std::string> types;
    size_t last_pos = 0;

    re2::StringPiece match;
    while (re2Pattern.Match(text, last_pos, text.size(), RE2::UNANCHORED, &match, 1)) {
        uint64_t splitIndex = match.data() - text.data();
        if (match.data() - (text.data() + last_pos) > 0) {
            types.push_back(std::string(text.data() + last_pos, match.data() - (text.data() + last_pos)));
        }
        last_pos = splitIndex + match.size();
    }

    if (last_pos < text.size()) {
        types.push_back(std::string(text.data() + last_pos));
    }
    std::set<std::string> typeSet(types.begin(), types.end());
    types.assign(typeSet.begin(), typeSet.end());
    return types;
}

bool IsValidNumber(const std::string &str)
{
    if (str.empty()) {
        return false;
    }

    re2::RE2 pattern(R"(^([-+])?([1-9][0-9]*|[0])(\.[0-9]+)?$)");
    if (re2::RE2::FullMatch(str, pattern)) {
        return true;
    }
    return false;
}

std::string AppendSrcDstForLog(std::string_view srcAddr, std::string_view dstAddr)
{
    std::string result;
    result.reserve(CalcSrcDstLogSize(srcAddr, dstAddr));
    AppendSrcDstFields(result, srcAddr, dstAddr);
    return result;
}

std::string AppendSrcDstForLog(std::string_view prefix, std::string_view srcAddr, std::string_view dstAddr)
{
    std::string result;
    result.reserve(prefix.size() + CalcSrcDstLogSize(srcAddr, dstAddr));
    result.append(prefix.data(), prefix.size());
    AppendSrcDstFields(result, srcAddr, dstAddr);
    return result;
}
}  // namespace datasystem
#endif
