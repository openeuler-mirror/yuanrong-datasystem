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
#include "datasystem/common/urma_mock/segment/memfd_resolver.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <charconv>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <dirent.h>
#include <string>
#include <system_error>

#include "datasystem/common/log/log.h"

namespace datasystem {
namespace urma_mock {

namespace {

constexpr int K_MAPS_SCAN_FIELD_COUNT = 5;
constexpr size_t K_PROC_LINE_SIZE = 1024;
constexpr size_t K_PROC_PERMS_SIZE = 8;
constexpr size_t K_PROC_TOKEN_SIZE = 32;
constexpr size_t K_PROC_FD_LINK_SIZE = 256;
constexpr int K_HEX_BASE = 16;
constexpr int K_DECIMAL_BASE = 10;

// /proc/self/maps line format (canonical):
//   address           perms offset  dev   inode   pathname
//   7f60463fd000-7f60463fe000 rw-s 00000000 00:01 597965 /memfd:datasystem (deleted)
// Columns are whitespace-separated; pathname starts at column 5 (zero-
// indexed at 5) and may contain spaces (e.g. "(deleted)").
struct MapsLine {
    uint64_t start = 0;
    uint64_t end = 0;
    uint64_t offset = 0;
    uint64_t inode = 0;
    std::string pathname;
};

bool ParseHexValue(const std::string &value, uint64_t *out)
{
    if (out == nullptr || value.empty()) {
        return false;
    }
    uint64_t parsed = 0;
    auto [ptr, ec] = std::from_chars(value.data(), value.data() + value.size(), parsed, K_HEX_BASE);
    if (ec != std::errc() || ptr != value.data() + value.size()) {
        return false;
    }
    *out = parsed;
    return true;
}

bool ParseDecimalValue(const std::string &value, uint64_t *out)
{
    if (out == nullptr || value.empty()) {
        return false;
    }
    uint64_t parsed = 0;
    auto [ptr, ec] = std::from_chars(value.data(), value.data() + value.size(), parsed, K_DECIMAL_BASE);
    if (ec != std::errc() || ptr != value.data() + value.size()) {
        return false;
    }
    *out = parsed;
    return true;
}

bool ParseMapsLine(const char *line, MapsLine *out)
{
    if (line == nullptr || out == nullptr) {
        return false;
    }
    char range[K_PROC_LINE_SIZE] = { 0 };
    char perms[K_PROC_PERMS_SIZE] = { 0 };
    char offset[K_PROC_TOKEN_SIZE] = { 0 };
    char dev[K_PROC_PERMS_SIZE] = { 0 };
    char inode[K_PROC_TOKEN_SIZE] = { 0 };
    int pathOffset = 0;
    int matched = std::sscanf(line, "%1023s %7s %31s %7s %31s %n", range, perms, offset, dev, inode, &pathOffset);
    if (matched != K_MAPS_SCAN_FIELD_COUNT || pathOffset < 0) {
        return false;
    }
    std::string rangeStr(range);
    auto dashPos = rangeStr.find('-');
    if (dashPos == std::string::npos || !ParseHexValue(rangeStr.substr(0, dashPos), &out->start)
        || !ParseHexValue(rangeStr.substr(dashPos + 1), &out->end) || !ParseHexValue(offset, &out->offset)
        || !ParseDecimalValue(inode, &out->inode)) {
        return false;
    }
    // pathname: find first '/' from the line.
    const char *slash = std::strchr(line + pathOffset, '/');
    if (slash == nullptr) {
        out->pathname.clear();
    } else {
        out->pathname.assign(slash);
        // Trim trailing whitespace.
        while (!out->pathname.empty() && (out->pathname.back() == '\n' || out->pathname.back() == ' ')) {
            out->pathname.pop_back();
        }
    }
    return true;
}

bool VaRangeContains(uint64_t va, uint64_t len, const MapsLine &line)
{
    // Requested range [va, va+len) must fit inside the maps line range.
    if (len == 0) {
        return va >= line.start && va < line.end;
    }
    uint64_t reqEnd = va + len;
    return va >= line.start && reqEnd <= line.end;
}

MemfdMapping ParseMapsMemfdMappingForVa(uint64_t va, uint64_t len, const std::string &memfdName)
{
    FILE *fp = std::fopen("/proc/self/maps", "r");
    if (fp == nullptr) {
        return {};
    }
    char line[K_PROC_LINE_SIZE];
    MemfdMapping mapping;
    while (std::fgets(line, sizeof(line), fp) != nullptr) {
        MapsLine ml;
        if (!ParseMapsLine(line, &ml)) {
            continue;
        }
        // Pathname must match "/memfd:<name> (deleted)".
        std::string expected = "/memfd:" + memfdName + " (deleted)";
        if (ml.pathname != expected) {
            continue;
        }
        if (!VaRangeContains(va, len, ml)) {
            continue;
        }
        mapping.inode = ml.inode;
        mapping.offset = ml.offset + (va - ml.start);
        break;
    }
    std::fclose(fp);
    return mapping;
}

}  // namespace

int LookupMemfdFd(uint64_t va, uint64_t len, const std::string &memfdName)
{
    return LookupMemfdFd(va, len, memfdName, nullptr);
}

int LookupMemfdFd(uint64_t va, uint64_t len, const std::string &memfdName, MemfdMapping *mapping)
{
    auto targetMapping = ParseMapsMemfdMappingForVa(va, len, memfdName);
    if (targetMapping.inode == 0) {
        LOG(INFO) << "[LookupMemfdFd] no /proc/self/maps line for va=0x" << std::hex << va << " len=" << std::dec << len
                  << " name=" << memfdName;
        return -1;
    }
    DIR *dir = ::opendir("/proc/self/fd");
    if (dir == nullptr) {
        LOG(ERROR) << "[LookupMemfdFd] opendir /proc/self/fd failed";
        return -1;
    }
    int result = -1;
    struct dirent *ent;
    while ((ent = ::readdir(dir)) != nullptr) {
        if (ent->d_name[0] == '.') {
            continue;
        }
        std::string fdPath = std::string("/proc/self/fd/") + ent->d_name;
        char linkTarget[K_PROC_FD_LINK_SIZE] = { 0 };
        ssize_t linkLen = ::readlink(fdPath.c_str(), linkTarget, sizeof(linkTarget) - 1);
        if (linkLen < 0) {
            continue;
        }
        linkTarget[linkLen] = '\0';
        // Quick filter: only memfd entries can match.
        if (std::strstr(linkTarget, "memfd:") == nullptr) {
            continue;
        }
        struct stat st {};
        if (::stat(fdPath.c_str(), &st) != 0) {
            continue;
        }
        if (static_cast<uint64_t>(st.st_ino) != targetMapping.inode) {
            continue;
        }
        int fd = ::open(fdPath.c_str(), O_RDWR);
        if (fd < 0) {
            continue;
        }
        result = fd;
        break;
    }
    ::closedir(dir);
    if (result < 0) {
        LOG(INFO) << "[LookupMemfdFd] inode=" << targetMapping.inode << " for va=0x" << std::hex << va
                  << " not found in /proc/self/fd";
    } else if (mapping != nullptr) {
        *mapping = targetMapping;
    }
    return result;
}

}  // namespace urma_mock
}  // namespace datasystem
