/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2022. All rights reserved.
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
 * Description: Validate Gflag.
 */
#ifndef DATASYSTEM_COMMON_UTIL_FLAG_VALIDATOR_H
#define DATASYSTEM_COMMON_UTIL_FLAG_VALIDATOR_H

#include <sys/vfs.h>
#include <algorithm>
#include <cstdint>
#include <string>
#include <string_view>
#include <thread>
#include <vector>
#include <arpa/inet.h>

#include <linux/limits.h>
#include <re2/re2.h>

#include "datasystem/common/log/log.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/util/file_util.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/uri.h"

constexpr int32_t THREAD_POOL_SIZE_LIMIT = 4096;
constexpr int32_t OBJECT_KEYS_MAX_SIZE_LIMIT = 10000;
constexpr int32_t NODE_TIMEOUT_LIMIT = 5;
constexpr int32_t MS_PER_SECOND = 1000;

static std::string ENCRYPT_KIT_PLAINTEXT = "plaintext";

using namespace datasystem;

class Validator {
public:
    Validator() = default;

    ~Validator() = default;

    static const int threadMultiplier = 512;
    static constexpr uint64_t MB_TO_BYTES = 1024 * 1024;  // mb to bytes;
    static constexpr uint64_t MB_LIMIT = std::numeric_limits<uint64_t>::max() / MB_TO_BYTES;
    inline static const std::string idFormat = "^[a-zA-Z0-9\\~\\.\\-\\/_!@#%\\^\\&\\*\\(\\)\\+\\=\\:;]*$";
    inline static const std::string objKeyFormat = "^[a-zA-Z0-9\\-_!@#%\\^\\*\\(\\)\\+\\=\\:;]*$";
    /**
     * @brief Validate a int32 is greater than 0.
     * @param[in] flagName The positive int flag.
     * @param[in] value The value to be checked.
     * @return True if valid.
     */
    static bool ValidateInt32(const char *flagName, int32_t value)
    {
        if (value <= 0) {
            LOG(ERROR) << FormatString("The value of %s flag is %d, which must be greater than 0.", flagName, value);
            return false;
        }
        return true;
    }

    /**
     * @brief Validate a uint32_t is greater than 0.
     * @param[in] flagName The positive int flag.
     * @param[in] value The value to be checked.
     * @return True if valid.
     */
    static bool ValidateUint32(const char *flagName, uint32_t value)
    {
        if (value <= 0) {
            LOG(ERROR) << FormatString("The value of %s flag is %u, which must be greater than 0.", flagName, value);
            return false;
        }
        return true;
    }

    /**
     * @brief Validate eviction_reserve_mem_threshold_mb which is a value between 100MB and 100GB.
     * @param[in] flagName The positive int flag.
     * @param[in] value The value to be checked.
     * @return True if valid.
     */
    static bool ValidateEvictReserveMemThreshold(const char *flagName, uint32_t value)
    {
        if (!ValidateUint32(flagName, value)) {
            return false;
        }
        auto low = 100u, high = 100 * 1024u;
        if (low > value || high < value) {
            LOG(ERROR) << FormatString(
                "The %s flag is currently set to %u MB. It is recommended to adjust it to a value between 100 MB and "
                "100 GB.",
                flagName, value);
            return false;
        }
        return true;
    }

    /**
     * @brief Validate a int32 is non-negative.
     * @param[in] value The value to be checked.
     * @return True if valid.
     */
    static bool ValidateInt32(int32_t value)
    {
        if (value < 0) {
            LOG(ERROR) << FormatString("The value is %d, which must be non-negative.", value);
            return false;
        }
        return true;
    }

    /**
     * @brief Validate a string is a valid ipv4 address.
     * @param[in] flagName IP address flag.
     * @param[in] value The string to be checked.
     * @return True if valid.
     */
    static bool ValidateHostIPv4(const char *flagName, const std::string &value)
    {
        in_addr  addr{};
        if (inet_pton(AF_INET, value.c_str(), &addr) == 1) {
            return true;
        }
        LOG(ERROR) << FormatString("The value of %s flag is %s, which is a illegal IPv4 address format.", flagName,
                                   value);
        return false;
    }

    /**
     * @brief Validate a string is a valid ipv6 address.
     * @param[in] flagName IP address flag.
     * @param[in] value The string to be checked.
     * @return True if valid.
     */
    static bool ValidateHostIPv6(const char *flagName, const std::string &value)
    {
        in6_addr addr{};
        if (inet_pton(AF_INET6, value.c_str(), &addr) == 1) {
            return true;
        }
        LOG(ERROR) << FormatString("The value of %s flag is %s, which is a illegal IPv6 address format.", flagName,
                                   value);
        return false;
    }

    /**
     * @brief Validate a string is a a valid set of addresses in the format "ip:port,ip:port".
     * IPv6 formats are also support, like "[ipv6]:port,[ipv6]:port
     * @param[in] flagName IP address flag.
     * @param[in] value The string to be checked.
     * @return Returns true if the value is empty or all are valid ip:port addresses, false otherwise.
     */
    static bool ValidateEtcdAddresses(const char *flagName, const std::string &value)
    {
        // Regex to match the address for etcd with port from 0.0.0.0:0 to 255.255.255.255:65535
        char delimiter = ',';
        std::stringstream sstream(value);
        std::string word;
        while (std::getline(sstream, word, delimiter)) {
            // Regex to match ipv4 address with port from 0.0.0.0:0 to 255.255.255.255:65535
            if (ValidateHostPortString("etcd_address", word) || ValidateDomainNamePort("etcd_address", word)) {
                continue;
            } else {
                LOG(ERROR) << FormatString(
                    "The value of %s flag is %s, which contains some illegal IP address or domain name formats.",
                    flagName, value);
                return false;
            }
        }
        return true;
    }

    /**
     * @brief Validate a string is a valid ip address with port as the format ipv4_format:port.
     * ipv6 formats are also allowed, with format: [ipv6_format]:port
     * @param[in] flagName IP address with port flag.
     * @param[in] value The string to be checked.
     * @return True if valid.
     */
    static bool ValidateHostPortString(const char *flagName, const std::string &value)
    {
        // If the first character is not a [, then validate this as an IPv4 address. Otherwise, it must be IPv6
        if (value[0] != '[') {
            return IsValidIPv4HostPortString(flagName, value);
        } else {
            return IsValidIPv6HostPortString(flagName, value);
        }
    }

    /**
     * @brief Validate a string is a valid IPv4 address with port as the format ipv4_format:port.
     * @param[in] flagName IP address with port flag.
     * @param[in] value The string to be checked.
     * @return True if valid and its an IPv6
     */
    static bool IsValidIPv4HostPortString(const char *flagName, const std::string &value)
    {
        // Regex to match ipv4 address with port from 0.0.0.0:0 to 255.255.255.255:65535
        // or localhost:<port>.
        static const re2::RE2 re(
            "^(((25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[0-9]{1,2})(\\.(25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[0-9]{1,2})){3})|("
            "localhost))(:((6553[0-5])|(655[0-2][0-9])|(65[0-4][0-9]{2})|"
            "(6[0-4][0-9]{3})|([1-5][0-9]{4})|([0-5]{1,5})|([0-9]{1,4})))$");

        // Allow value size 0 due to certain circumstances that indicates default or function disabled.
        if (re2::RE2::FullMatch(value, re) || value.empty()) {
            return true;
        }
        LOG(INFO) << FormatString("Value of %s flag is %s. Illegal [IPv6]:port or IPv4:Port address format.",
                                  flagName, value);
        return false;
    }
    
    /**
     * @brief Validate a string is a valid IPv6 address with port as the format [ipv6_format]:port.
     * @param[in] flagName IP address with port flag.
     * @param[in] value The string to be checked.
     * @return True if valid and its an IPv6
     */
    static bool IsValidIPv6HostPortString(const char *flagName, const std::string &value)
    {
        // Future: A regex can be created to make a more comprehensive IPv6 validator
        // For now, do some simpler condition checks that catch most of the cases.
        // First character must be [
        if (value[0] != '[') {
            return false;
        }
        // Quick solution for now. A full regex for IPv6 would be complex. Just make sure the address has form:
        // [IPv6_addr]:port
        // Don't actually dive into the IPv6_addr structure itself for validation.
        auto pos = value.find_last_of(':');
        if (pos == std::string::npos) {
            LOG(INFO) << FormatString("The value of %s flag is %s, which is an illegal [IPv6]:Port address format.",
                                      flagName, value);
            return false;
        }
        if (value[pos - 1] != ']') {
            LOG(INFO) << FormatString("The value of %s flag is %s, which is an illegal [IPv6]:Port address format.",
                                      flagName, value);
            return false;
        }
        const int addrPrefixLen = 4;
        std::string addrPrefix = value.substr(1, addrPrefixLen);
        if ((addrPrefix == "fe80" || addrPrefix == "FE80") && value.find('%') == std::string::npos) {
            LOG(INFO) << "The value of " << flagName << " flag is " << value
                      << ", is an illegal [IPv6]:Port address format. Link local address require %interface_name.";
            return false;
        }

        // extract the port piece and validate it.
        std::string portNumStr = value.substr(pos + 1);
        if (!IsInPortRange(portNumStr, false)) {
            return false;
        }

        // made through all the checks. format looks good.
        return true;
    }

    /**
     * @brief Validate the given uint32 num is in a range of [0,65535].
     * @param[in] flagName The port flag.
     * @param[in] value The int value to be checked.
     * @return True if valid.
     */
    static bool ValidatePort(const char *flagName, const uint32_t value)
    {
        if (value > UINT16_MAX) {
            LOG(ERROR) << FormatString("The value of %s flag is %u, which must be in [0, 65535].", flagName, value);
            return false;
        }
        return true;
    }

    /**
     * @brief Validate the given string matches the cache type supported.
     * @param[in] flagName Cache type flag.
     * @param[in] value The string to be checked.
     * @return True if valid.
     */
    static bool ValidateL2CacheType(const char *flagName, const std::string &value)
    {
        if (value == "obs" || value == "sfs" || value == "none") {
            return true;
        }
        LOG(ERROR) << datasystem::FormatString(
            "The value of %s flag is %s, which must be 'sfs'/'none.", flagName, value);
        return false;
    }

    /**
     * @brief Validate the given string matches the cache type supported.
     * @param[in] flagName Cache type flag.
     * @param[in] value The string to be checked.
     * @return True if valid.
     */
    static bool ValidateArenaPerTenant(const char *flagName, const uint32_t value)
    {
        const uint32_t minValueLimit = 1;
        const uint32_t maxValueLimit = 32;
        if (minValueLimit <= value && value <= maxValueLimit) {
            return true;
        }
        LOG(ERROR) << FormatString("The value of the flag %s is %u, which exceeds the limit [1,32].", flagName, value);
        return false;
    }

    /**
     * @brief Validate the given string matches the shared disk arena per tenant supported.
     * @param[in] flagName The shared disk arena per tenant flag.
     * @param[in] value The string to be checked.
     * @return True if valid.
     */
    static bool ValidateSharedDiskArenaPerTenant(const char *flagName, const uint32_t value)
    {
        const uint32_t maxValueLimit = 32;
        if (value <= maxValueLimit) {
            return true;
        }
        LOG(ERROR) << FormatString("The value of the flag %s is %u, which exceeds the limit [0,32].", flagName, value);
        return false;
    }

    /**
     * @brief Validate the given string contains all legal characters.
     * @param[in] flagName The characters flag.
     * @param[in] value The string to be checked.
     * @return True if valid.
     */
    static bool ValidateEligibleChar(const char *flagName, const std::string &value)
    {
        // Regex to match a string that all chars are:
        // a-zA-Z
        // 0-9
        // .-_/~
        // Chinese chars
        static const re2::RE2 re("^[a-zA-Z0-9\\~\\.\\-\\:\\/_!@#$%\\^\\&\\*\\(\\)\\+\\=\u4e00-\u9fa5]*$");
        if (value.empty() || re2::RE2::FullMatch(value, re)) {
            return true;
        }
        LOG(ERROR) << FormatString("The value of %s flag is %s, which is a illegal path format.", flagName, value);
        return false;
    }

    /**
     * @brief Validate the given string is a valid path format.
     * @param[in] flagName The path flag.
     * @param[in] value The string to be checked.
     * @return True if valid.
     */
    static bool ValidatePathString(const char *flagName, const std::string &value)
    {
        // Regex to match a string of path, for instance:
        // /
        // ./
        // ~/
        // path/to/file
        // /path/to/file
        // ./path/to/file
        // ../path/to/file
        // .././path/to.file
        // ~/../path/to/file
        // ~/path/../file

        static const RE2 re("^\\/$|(^\\/[^\\/\\0]+|^\\.\\.?|^\\~|[^/\\0]+)(\\/[^\\/\\0]+)*\\/?$");

        // Allow value size 0 due to certain circumstances that indicate default path or function disabled.
        if ((ValidateStringLenPathMax(flagName, value)
             && re2::RE2::FullMatch(value, re) && ValidateEligibleChar(flagName, value) && IsSafePath(value))
            || value.size() == 0) {
            return true;
        }
        LOG(ERROR) << FormatString("The value of %s flag is %s, which is a illegal path.", flagName, value);
        return false;
    }

    /**
     * @brief Validate the node timeout flag.
     * @param[in] flagName The path flag.
     * @param[in] nodeTimeoutS The value to be checked.
     * @return True if valid.
     */
    static bool ValidateNodeTimeout(const char *flagName, const uint32_t nodeTimeoutS)
    {
#ifdef WITH_TESTS
        return true;
#endif
        if (nodeTimeoutS < NODE_TIMEOUT_LIMIT || nodeTimeoutS > UINT32_MAX / MS_PER_SECOND) {
            LOG(ERROR) << FormatString("The value of %s flag is %u, which must be greater than or equal to 5.",
                                       flagName, nodeTimeoutS);
            return false;
        }
        return true;
    }

    /**
     * @brief Validate the given string is a real path.
     * @param[in] flagName The path flag.
     * @param[in] value The string to be checked.
     * @return True if valid.
     */
    static bool ValidateRealPath(const char *flagName, const std::string &value)
    {
        char realPath[PATH_MAX + 1] = { 0 };
        // Allow value size 0 due to certain circumstances that indicate default path or function disabled.
        if ((ValidateStringLenPathMax(flagName, value) && realpath(value.c_str(), realPath) != nullptr
             && IsSafePath(value))
            || value.size() == 0) {
            return true;
        }
        LOG(ERROR) << FormatString("The value of %s flag is %s, which is not a real path.", flagName, value);
        return false;
    }

    /**
     * @brief Validate the given string is no longer than NAME_MAX(255) chars.
     * @param[in] flagName NAME_MAX flag.
     * @param[in] value The string to be checked.
     * @return True if valid.
     */
    static bool ValidateStringLenNameMax(const char *flagName, const std::string &value)
    {
        if (value.size() <= NAME_MAX) {
            return true;
        }
        LOG(ERROR) << FormatString("The value of %s flag is %s, which should be no longer than %s.", flagName, value,
                                   NAME_MAX);
        return false;
    }

    /**
     * @brief Validate the given string is no longer than PATH_MAX(4096) chars.
     * @param[in] flagName PATH_MAX flag.
     * @param[in] value The string to be checked.
     * @return True if valid.
     */
    static bool ValidateStringLenPathMax(const char *flagName, const std::string &value)
    {
        // Null terminator has 1 byte so we subtract it.
        if (value.size() <= PATH_MAX - 1) {
            return true;
        }
        LOG(ERROR) << FormatString("The value of %s flag is %s, which should be no longer than %s.", flagName, value,
                                   PATH_MAX - 1);
        return false;
    }

    /**
     * @brief Validate the given string is a valid uuid format, regardless of the uuid version.
     * @param[in] flagName Uuid number flag.
     * @param[in] value The string to be checked.
     * @return True if valid.
     */
    static bool ValidateUuid(const char *flagName, const std::string &value)
    {
        static const re2::RE2 re(
            "^[0-9a-fA-F]{8}\\-[0-9a-fA-F]{4}\\-[0-9a-fA-F]{4}\\-[0-9a-fA-F]{4}\\-[0-9a-fA-F]{12}$");
        if (RE2::FullMatch(value, re)) {
            return true;
        }
        LOG(ERROR) << FormatString("The value of %s flag is %s, which is not a legal UUID format.", flagName, value);
        return false;
    }

    /**
     * @brief Validate the value for initializing a rpc thread pool.
     * @param[in] threadNum Thread number flag.
     * @param[in] value Gflags value.
     * @return True if valid.
     */
    static bool ValidateRpcThreadNum(const char *threadNum, const int32_t value)
    {
        if (value >= 0 && value <= THREAD_POOL_SIZE_LIMIT) {
            return true;
        }
        LOG(ERROR) << FormatString("The value of %s flag is %d, which must be in (0, %d].", threadNum, value,
                                   THREAD_POOL_SIZE_LIMIT);
        return false;
    }

    /**
     * @brief Validate the value for initializing a thread pool.
     * @param[in] flagName Thread number flag.
     * @param[in] value Gflags value.
     * @return True if valid.
     */
    static bool ValidateThreadNum(const char *flagName, const int32_t value)
    {
        if (value > 0 && value <= THREAD_POOL_SIZE_LIMIT) {
            return true;
        }
        LOG(ERROR) << FormatString("The value of %s flag is %d, which must be in (0, %d].", flagName, value,
                                   THREAD_POOL_SIZE_LIMIT);
        return false;
    }

    /**
     * @brief Validate the value for initializing a thread pool.
     * @param[in] flagName Thread number flag.
     * @param[in] value Gflags value.
     * @return True if valid.
     */
    static bool ValidateThreadNum(const char *flagName, const uint32_t value)
    {
        if (value <= THREAD_POOL_SIZE_LIMIT) {
            return true;
        }
        LOG(ERROR) << FormatString("The value of %s flag is %u, which must be in [0, %d].", flagName, value,
                                   THREAD_POOL_SIZE_LIMIT);
        return false;
    }

    /**
     * @brief Validate the value for initializing a thread pool.
     * @param[in] clientNum Client number flag.
     * @param[in] value Gflags value.
     * @return True if valid.
     */
    static bool ValidateClientNum(const char *clientNum, const uint32_t value)
    {
        constexpr uint32_t maxClientNum = 10000;
        if (value > 0 && value <= maxClientNum) {
            return true;
        }
        LOG(ERROR) << FormatString("The value of %s flag is %u, which must be in (0, %u].", clientNum, value,
                                   maxClientNum);
        return false;
    }

    /**
     * @brief Validate the value for IAMKit.
     * @param[in] IAMKit IAM type.
     * @param[in] value Gflags value.
     * @return True if valid.
     */
    static bool ValidateIAMKit(const char *IAMKit, const std::string &value)
    {
        if (value == "none" || value == "yuanrong_iam") {
            return true;
        } else {
            LOG(ERROR) << FormatString("The value of %s flags is %s, which must be in [none, yuanrong_iam]",
                                       IAMKit, value);
            return false;
        }
    }

    // The following functions are not compatible with gflag validator.

    /**
     * @brief Check passed in string is a valid ID format. The ID format refers to
     *        any IDs like objectKey, streamName, key, etc. used in the datasystem.
     * @param[in] value The string to be checked.
     * @return True if param is a valid ID format. Otherwise false.
     */
    static bool IsTraceIdFormat(const std::string &value)
    {
        static const re2::RE2 re(idFormat);

        if (value.size() <= UINT8_MAX && (re2::RE2::FullMatch(value, re) || value.empty())) {
            return true;
        }
        LOG(ERROR) << value << " is not a legal ID format, allowed regex format: " << idFormat;
        return false;
    }

    /**
     * @brief Check passed in string is a valid ID format. The ID format refers to
     *        any IDs like objectKey, streamName, key, etc. used in the datasystem.
     * @param[in] value The string to be checked.
     * @return True if param is a valid ID format. Otherwise false.
     */
    static bool IsIdFormat(const std::string &value)
    {
        static const re2::RE2 re(objKeyFormat);

        if (value.size() <= UINT8_MAX && (re2::RE2::FullMatch(value, re) || value.empty())) {
            return true;
        }
        LOG(ERROR) << value << " is not a legal ID format, allowed format: " << objKeyFormat;
        return false;
    }

    /**
     * @brief Check value is regex match or not.
     * @param[in] re Regex rule.
     * @param[in] value The string to be checked.
     * @return True if param is a valid format. Otherwise false.
     */
    static bool IsRegexMatch(const re2::RE2 &re, const std::string &value)
    {
        if (value.size() <= UINT8_MAX && (re2::RE2::FullMatch(value, re) || value.empty())) {
            return true;
        }
        LOG(ERROR) << value << " is miss match";
        return false;
    }

    /**
     * @brief Check passed in string is a valid IPv4 address with or without a valid port.
     * @param[in] value The string to be checked.
     * @return True if param is a valid ipv4 address. Otherwise false.
     */
    static bool IsIpv4OrUrl(const std::string &value, bool allowEmpty = true)
    {
        // Regex to match ipv4 address with port from 0.0.0.0:0 to 255.255.255.255:65535
        // or localhost:<port>
        // or just a ip address or localhost
        // or any url based on RFC 1738
        static const re2::RE2 re(
            "^(((25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[0-9]{1,2})(\\.(25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[0-9]{1,2})){3})|("
            "localhost)|([a-zA-Z0-9\\$\\.\\-_\\+\\!\\*'\\(\\),]+))(:((6553[0-5])|(655[0-2][0-9])|(65[0-4][0-9]{2})|(6["
            "0-4][0-9]{3})|([1-5][0-9]{4})|([0-5]{1,5})|"
            "([0-9]{1,4})))?$|^$");

        // Allow value size 0 due to certain circumstances that indicates default or function disabled.
        if (allowEmpty == false && value.size() == 0) {
            LOG(ERROR) << value << " empty is not a IPv4 address format.";
            return false;
        }
        if (re2::RE2::FullMatch(value, re)) {
            return true;
        }
        LOG(ERROR) << value << " is an illegal IPv4 address format.";
        return false;
    }

    /**
     * @brief Check passed in string is a valid port in range.
     * @param[in] value The string to be checked.
     * @return True if param is a valid port number. Otherwise false.
     */
    static bool IsInPortRange(const std::string &value, bool allowEmpty = true)
    {
        // Regex to match port from 0 to 65535.
        static const re2::RE2 re(
            "^((6553[0-5])|(655[0-2][0-9])|(65[0-4][0-9]{2})|(6[0-4][0-9]{3})|([1-5][0-9]{4})|([0-5]{1,5})|([0-9]{1,4})"
            ")$");
        // Allow value size 0 due to certain circumstances that indicates default or function disabled.
        if (allowEmpty == false && value.empty()) {
            LOG(ERROR) << value << " empty is not a valid port.";
            return false;
        }
        if (re2::RE2::FullMatch(value, re)) {
            return true;
        }
        LOG(ERROR) << value << " is not a valid port.";
        return false;
    }

    /**
     * @brief Check passed in int is a valid port in range.
     * @param[in] value The string to be checked.
     * @return True if param is a valid port number. Otherwise false.
     */
    static bool IsInPortRange(const int value, bool allowEmpty = true)
    {
        // Regex to match port from 0 to 65535.
        // Allow value size 0 due to certain circumstances that indicates default or function disabled.
        if (allowEmpty == false && value == 0) {
            LOG(ERROR) << value << " is out of standard port range";
            return false;
        }
        if (value >= 0 && value <= UINT16_MAX) {
            return true;
        }
        LOG(ERROR) << value << " is out of standard port range";
        return false;
    }

    /**
     * @brief Check passed in int is a non-negative int32.
     * @param[in] value The data to be checked.
     * @return True if param is a valid number. Otherwise false.
     */
    static bool IsInNonNegativeInt32(const int64_t value)
    {
        if (value >= 0 && value <= INT32_MAX) {
            return true;
        }
        LOG(ERROR) << value << " is out of standard range [0, " << INT32_MAX << "].";
        return false;
    }

    /**
     * @brief Check passed in string is valid to convert to int.
     * @param[in] value The data to be checked.
     * @param[out] dest The integer data convert from string.
     * @return True if param is a valid type. Otherwise false.
     */
    static bool IsStringOfTypeInt(const std::string value, int &dest)
    {
        try {
            dest = std::stoi(value);
        } catch (std::invalid_argument &invalidArgument) {
            LOG(ERROR) << std::string(invalidArgument.what()) << ", invalid argument.";
            return false;
        } catch (std::out_of_range &outOfRange) {
            LOG(ERROR) << std::string(outOfRange.what()) << ", out of int range.";
            return false;
        } catch (std::exception &e) {
            LOG(ERROR) << std::string(e.what()) << ", convert error.";
            return false;
        }
        return true;
    }

    static bool ValidateSharedMemSize(const char *flagname, uint64_t value)
    {
        (void)flagname;
        if (value == 0) {
            LOG(ERROR) << "The shared_memory_size_mb must be greater than 0.";
            return false;
        }
        if (value > MB_LIMIT) {
            LOG(ERROR) << "The shared_memory_size_mb must be smaller than " << MB_LIMIT << ".";
            return false;
        }
        return true;
    }

    static bool ValidateBatchGetThreshold(const char *flagname, int64_t value)
    {
        (void)flagname;
        const int64_t KB = 1024;
        value = value * KB * KB;
        const int64_t maxValue = KB * KB * KB;
        if (value >= 0 && value <= maxValue) {
            return true;
        }
        LOG(ERROR) << FormatString("The value of %s flag is %zu MB, which must be in [0, %zu].", flagname, value,
                                   maxValue);
        return false;
    }

    /**
     * @brief Validate the given value matches the shared disk size supported.
     * @param[in] flagName The shared disk size flag.
     * @param[in] value The value to be checked.
     * @return True if valid.
     */
    static bool ValidateSharedDiskSize(const char *flagname, uint64_t value)
    {
        (void)flagname;
        if (value > MB_LIMIT) {
            LOG(ERROR) << "The " << flagname << " must be smaller than " << MB_LIMIT << ".";
            return false;
        }
        return true;
    }

    static bool ValidateLocalCacheMemSize(const char *flagname, uint64_t value)
    {
        (void)flagname;
        if (value == 0) {
            LOG(ERROR) << "The stream_local_cache_memory_size_mb must be greater than 0.";
            return false;
        }
        if (value > MB_LIMIT) {
            LOG(ERROR) << "The stream_local_cache_memory_size_mb must be smaller than " << MB_LIMIT << ".";
            return false;
        }
        return true;
    }

    static bool ValidatePageSize(const char *flagname, uint32_t value)
    {
        (void)flagname;
        const uint32_t minValue = 4 * 1024;
        const uint32_t maxValue = 1024 * 1024 * 1024;
        if ((value < minValue) || (value > maxValue)) {
            LOG(ERROR) << FormatString("The value of %s flag is %u Byte, which must be in [%u, %u].", flagname, value,
                                       minValue, maxValue);
            return false;
        }
        return true;
    }

    /**
     * @brief whether the value is http url
     * @param[in] value the value of the flag
     * @return true if is http url, otherwise false
     */
    static bool IsHttpUrl(const std::string &value)
    {
        std::string::size_type pos = value.find("https://");
        constexpr int HTTP_INDEX = 7, HTTPS_INDEX = 8;
        if (pos != std::string::npos && pos == 0) {
            return IsIpv4OrUrl(value.substr(HTTPS_INDEX));
        }
        pos = value.find("http://");
        if (pos != std::string::npos && pos == 0) {
            return IsIpv4OrUrl(value.substr(HTTP_INDEX));
        }
        return false;
    }

    /**
     * @brief check whether value is empty
     * @param[in] value the value of the flag
     * @return true if the value is not empty, otherwise false
     */
    static bool IsNotEmpty(const std::string &value)
    {
        if (!value.empty()) {
            return true;
        }
        return false;
    }

    /**
     * @brief Validate the given size is valid.
     * @param[in] flagname File max size flag.
     * @param[in] value Gflags value.
     * @return True if param is a valid number. Otherwise false.
     */
    static bool ValidateSpillFileMaxSize(const char *flagname, uint64_t value)
    {
        (void)flagname;
        const uint64_t minMB = 200;
        const uint64_t maxMB = 10 * 1024;
        if (value >= minMB && value <= maxMB) {
            return true;
        }
        LOG(ERROR) << FormatString("The value of %s flag is %zu MB, which must be in [%zu, %zu].", flagname, value,
                                   minMB, maxMB);
        return false;
    }

    /**
     * @brief Validate the limit of open file.
     * @param[in] flagname Open file limit flag.
     * @param[in] value Gflags value.
     * @return True if param is a valid number. Otherwise false.
     */
    static bool ValidateSpillOpenFileLimit(const char *flagname, uint64_t value)
    {
        (void)flagname;
        const uint64_t minValue = 8;
        if (value >= minValue) {
            return true;
        }
        LOG(ERROR) << FormatString("The value of %s flag is %zu, which  must be >= 8.", flagname, value);
        return false;
    }

    template<typename T>
    static bool IsUuid(const T &value)
    {
        static const re2::RE2 re("^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$");
        if (re2::RE2::FullMatch(value, re)) {
            return true;
        }
        LOG(ERROR) << value << " is not a legal ID format";
        return false;
    }

    static bool ValidateEncryptKit(const char *flagname, const std::string &value)
    {
        (void)flagname;
        if (value == ENCRYPT_KIT_PLAINTEXT) {
            return true;
        }

        LOG(ERROR) << "The encrypt kit is only support plaintext";
        return false;
    }

    static bool IsBatchSizeUnderLimit(size_t objectSize)
    {
        return objectSize <= OBJECT_KEYS_MAX_SIZE_LIMIT;
    }

    /**
     * @brief Validate a string is a valid domain name with port as the format domain_name:port.
     * @param[in] flagName Domain name with port flag.
     * @param[in] value The string to be checked.
     * @return True if valid.
     */
    static bool ValidateDomainNamePort(const char *flagName, const std::string &value)
    {
        // Regex to match domain name with port.
        // The rule of domain name:
        // Domain name include a-z, A-Z, 0-9 and -
        // The length of domain name is between 1 and 63
        // The domain name cannot start with -
        // "^(?=^.{3,255}$)[a-zA-Z0-9][-a-zA-Z0-9]{0,62}(\\.[a-zA-Z0-9][-a-zA-Z0-9]{0,62})+(:((6553[0-5])|(655[0-2][0-"
        // "9])|(65[0-4][0-9]{2})|(6[0-4][0-9]{3})|([1-5][0-9]{4})|([0-5]{1,5})|([0-9]{1,4})))$"
        static RE2 re(
            "^[a-zA-Z0-9][-a-zA-Z0-9]{0,62}(\\.[a-zA-Z0-9][-a-zA-Z0-9]{0,62})+(:((6553[0-5])|(655[0-2][0-"
            "9])|(65[0-4][0-9]{2})|(6[0-4][0-9]{3})|([1-5][0-9]{4})|([0-5]{1,5})|([0-9]{1,4})))$");
        // Allow value size 0 due to certain circumstances that indicates default or function disabled.
        // min size is 3, max size is 255
        if ((re2::RE2::FullMatch(value, re) && value.size() >= 3 && value.size() <= 255) || value.empty()) {
            return true;
        }
        LOG(ERROR) << FormatString("The value of %s flag is %s, which is a illegal DomainName:Port address format.",
                                   flagName, value);
        return false;
    }

    /**
     * @brief Validate a string is a valid encrypted key.
     * @param flagName[in] ScEncryptSecretKey flags.
     * @param value[in] ScEncryptSecretKey value.
     * @return True if valid.
     */
    static bool ValidateScEncryptSecretKey(const char *flagName, const std::string &value)
    {
        const uint32_t MAX_LEN = 1024;
        if (value.empty() || value.size() <= MAX_LEN) {
            return true;
        }

        LOG(ERROR) << FormatString("The value of %s flag is %s, which must be in [0, %u].", flagName, value, MAX_LEN);
        return false;
    }

    /**
     * @brief Validate MaxRpcSessionNum
     * @param flagName[in] MaxRpcSessionNum flags.
     * @param value[in] MaxRpcSessionNum value.
     * @return True if valid.
     */
    static bool ValidateMaxRpcSessionNum(const char *flagName, int32_t value)
    {
        const int32_t MIN_NUM = 512;
        const int32_t MAX_NUM = 10000;
        if (value >= MIN_NUM && value <= MAX_NUM) {
            return true;
        }

        LOG(ERROR) << FormatString("The value of %s flag is %d, which must be in [%d, %d].", flagName, value, MIN_NUM,
                                   MAX_NUM);
        return false;
    }

    /**
     * @brief Validate unix domain socket dir.
     * @param[in] flagName The path flag.
     * @param[in] value The string to be checked.
     * @return True if valid.
     */
    static bool ValidateUnixDomainSocketDir(const char *flagName, const std::string &value)
    {
        if (value.empty()) {
            LOG(ERROR) << "unix domain socket dir can not br empty";
            return false;
        }
        const int MAX_LEN = 80;
        auto valueToModify = value;
        auto rc = Uri::NormalizePathWithUserHomeDir(valueToModify, "~/datasystem/unix_domain_socket_dir", "");
        if (rc.IsError()) {
            LOG(ERROR) << "Invalid unix domain socket dir: " << value << ", errMsg: " << rc.ToString();
            return false;
        }
        if (valueToModify.size() > MAX_LEN) {
            LOG(ERROR) << "Invalid unix domain socket dir lenth: " << valueToModify.size()
                       << ", max lenth: " << MAX_LEN;
            return false;
        }
        return ValidatePathString(flagName, value);
    }

    /**
     * @brief Validate OtherAzNames
     * @param flagName[in] OtherAzNames flags.
     * @param value[in] OtherAzNames value.
     * @return True if valid.
     */
    static bool ValidateOtherAzNames(const char *flagName, const std::string &value)
    {
        if (std::find(value.begin(), value.end(), ' ') != value.end()) {
            LOG(ERROR) << FormatString("The value of %s flag is %s, which must not contain spaces.", flagName, value);
            return false;
        }
        return true;
    }

    /**
     * @brief Validate the given string matches the cache type supported.
     * @param[in] flagName Cache type flag.
     * @param[in] value The string to be checked.
     * @return True if valid.
     */
    static bool ValidateRocksdbModeType(const char *flagName, const std::string &value)
    {
        if (value == "none" || value == "sync" || value == "async") {
            return true;
        }
        LOG(ERROR) << FormatString(
            "The value of %s flag is %s, which must be 'none'/'sync'/'async'.", flagName, value);
        return false;
    }

    /**
     * @brief Validate memory alignment.
     * @param[in] flagName The flag name.
     * @param[in] value The value to be checked.
     * @return True if valid.
     */
    static bool ValidateMemoryAlignment(const char *flagName, uint32_t value)
    {
        auto maxAlignment = 512U;
        auto divisor = 2;
        auto isEven = (value % divisor) == 0;
        if (value > 0U && value <= maxAlignment && isEven) {
            return true;
        }
        LOG(ERROR) << FormatString("The value of %s(%u) should be even in (0 , %u].", flagName, value, maxAlignment);
        return false;
    }
};
#endif  // DATASYSTEM_COMMON_UTIL_FLAG_VALIDATOR_H
