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
constexpr char SRC_LOG_PREFIX[] = ", src=";
constexpr char DST_LOG_PREFIX[] = ", dst=";
constexpr size_t SRC_LOG_PREFIX_SIZE = sizeof(SRC_LOG_PREFIX) - 1;
constexpr size_t DST_LOG_PREFIX_SIZE = sizeof(DST_LOG_PREFIX) - 1;

void AppendSrcDstFields(std::string &result, const std::string &srcAddr, const std::string &dstAddr)
{
    if (!srcAddr.empty()) {
        result.append(SRC_LOG_PREFIX, SRC_LOG_PREFIX_SIZE);
        result.append(srcAddr);
    }
    if (!dstAddr.empty()) {
        result.append(DST_LOG_PREFIX, DST_LOG_PREFIX_SIZE);
        result.append(dstAddr);
    }
}

size_t CalcSrcDstLogSize(const std::string &srcAddr, const std::string &dstAddr)
{
    return (srcAddr.empty() ? 0 : SRC_LOG_PREFIX_SIZE + srcAddr.size())
        + (dstAddr.empty() ? 0 : DST_LOG_PREFIX_SIZE + dstAddr.size());
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

std::string AppendSrcDstForLog(const std::string &srcAddr, const std::string &dstAddr)
{
    std::string result;
    result.reserve(CalcSrcDstLogSize(srcAddr, dstAddr));
    AppendSrcDstFields(result, srcAddr, dstAddr);
    return result;
}

std::string AppendSrcDstForLog(const std::string &prefix, const std::string &srcAddr, const std::string &dstAddr)
{
    std::string result;
    result.reserve(prefix.size() + CalcSrcDstLogSize(srcAddr, dstAddr));
    result.append(prefix);
    AppendSrcDstFields(result, srcAddr, dstAddr);
    return result;
}
}  // namespace datasystem
#endif
