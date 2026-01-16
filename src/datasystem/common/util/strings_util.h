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
 * Description: Strings util function.
 */
#ifndef DATASYSTEM_COMMON_UTIL_STRINGS_UTIL_H
#define DATASYSTEM_COMMON_UTIL_STRINGS_UTIL_H

#include <cstdint>
#include <cstring>
#include <memory>
#include <random>
#include <set>
#include <sstream>
#include <string>
#include <vector>

#include <securec.h>

#include "datasystem/common/util/format.h"
#include "datasystem/utils/sensitive_value.h"
#include "datasystem/common/log/log.h"

namespace datasystem {
const uint32_t LOG_MAX_SIZE_LIMIT = 25000;  // log max size is 30000.
constexpr size_t MIN_TOKEN_SIZE_TO_STRING = 16;
const std::string STR_WHITESPACE = " \t\n\r";
/**
 * @brief Format error num to error string.
 * @param[in] errNum Linux error number.
 * @return Return error string.
 */
inline std::string StrErr(int errNum)
{
    char errBuf[256];
    errBuf[0] = '\0';
    return strerror_r(errNum, errBuf, sizeof errBuf);
}

inline std::streampos GetSize(std::iostream *ss)
{
    if (!ss) {
        return 0;
    }
    auto currentPos = ss->tellg();
    ss->seekg(0, ss->end);
    auto size = ss->tellg();
    ss->seekg(currentPos, ss->beg);
    return size;
}

/**
 * @brief Print vector.
 * @param[in] vec Vector to print.
 * @return Return string.
 */
template <typename Vec>
std::string VectorToString(const Vec &vec, bool allowCut = true)
{
    std::stringstream out;
    auto totalCount = vec.size();
    decltype(totalCount) count = 0;
    bool first = true;
    for (auto &item : vec) {
        if (!first) {
            out << ", ";
        }
        out << item;
        first = false;
        auto length = GetSize(&out);
        count++;
        if (length > static_cast<decltype(length)>(LOG_MAX_SIZE_LIMIT) && allowCut) {
            out << "...(" << (totalCount - count) << ")";
            break;
        }
    }
    return out.str();
}

/**
 * @brief Print map.
 * @param[in] map Map to print.
 * @return Return string.
 */
template <typename Map>
std::string MapToString(const Map &map)
{
    std::stringstream out;
    for (auto &item : map) {
        out << "{" << item.first << ": " << item.second << "} ";
    }
    return out.str();
}

/**
 * @brief Adds the value at the end of the string by one.
 * @param[in] value String to be process.
 * @return String after processing.
 */
inline std::string StringPlusOne(const std::string &value)
{
    for (auto it = value.rbegin(); it != value.rend(); ++it) {
        if (static_cast<unsigned char>(*it) < 0xff) {
            std::string result(value.begin(), it.base());
            result[result.size() - 1] = *it + 1;
            return result;
        }
    }
    return {};
}

/**
 * @brief Convert string to int.
 * @param[in] str String to be converted.
 * @param[out] num out number according to string.
 * @return successful or not.
 */
inline bool StringToInt(const std::string &str, int &num)
{
    try {
        num = std::stoi(str);
    } catch (std::invalid_argument &invalidArgument) {
        LOG(ERROR) << "String to int failed, error: " << invalidArgument.what() << "\n";
        return false;
    } catch (std::out_of_range &outOfRange) {
        LOG(ERROR) << "String to int failed, error: " << outOfRange.what() << "\n";
        return false;
    }
    return true;
}

/**
 * @brief Check if the string contains the negative sign.
 * @param[in] str string to be checked.
 * @return true if has negative sign, else false.
 */
inline bool IsNegative(const std::string &str)
{
    size_t idx = 0;
    while (idx < str.size() && std::isspace(str[idx])) {
        ++idx;
    }
    if (idx < str.size() && str[idx] == '-') {
        return true;
    }
    return false;
}

/**
 * @brief Convert string to unsigned long, using stoull directly will interpret negative number as extremely large
 * positive values, therefore it’s necessary to check if the string contains a negative sign.
 * @param[in] str string to be interpreted.
 * @return Converted number.
 */
inline unsigned long StrToUnsignedLong(const std::string &str)
{
    if (IsNegative(str)) {
        throw std::out_of_range("The string " + str + " is out of range for unsigned long type.");
    }
    return std::stoul(str);
}

/**
 * @brief Convert string to unsigned long long, using stoull directly will interpret negative number as extremely
 * large positive values, therefore it’s necessary to check if the string contains a negative sign.
 * @param[in] str string to be interpreted.
 * @return Converted number.
 */
inline unsigned long long StrToUnsignedLongLong(const std::string &str)
{
    if (IsNegative(str)) {
        throw std::out_of_range("The string " + str + " is out of range for unsigned long long type.");
    }
    return std::stoull(str);
}

/**
 * @brief Copy string data, including the null terminator, to automatically managed memory space.
 * @param[in] src original data pointer.
 * @param[in] dest automatically managed memory space pointer.
 * @return Bytes successfully copied.
 */
inline size_t StringToUniquePtrChar(const char *src, std::unique_ptr<char[]> &dest)
{
    if (src == nullptr) {
        return 0;
    };
    auto srcLen = std::strlen(src) + 1;
    dest = std::make_unique<char[]>(srcLen);
    int ret = strcpy_s(dest.get(), srcLen, src);
    if (ret != EOK) {
        return 0;
    }
    return srcLen;
}

/**
 * @brief Use the regular expression of pattern to separate strings.
 * @param[in] typeStr Inputted type string.
 * @param[in] pattern Regular expression of split.
 * @return Vector of strings.
 */
std::vector<std::string> SplitToUniqueStr(const std::string &typeStr, const std::string &pattern);

/**
 * @brief Clear the string information in the memory.
 * @param[in/out] str The str needs to clear.
 */
inline void ClearStr(std::string &str)
{
    while (!str.empty()) {
        str.pop_back();
    }
}

/**
 * @brief Clear the unique char information in the memory.
 * @param[in/out] text The text char to clear.
 * @param[in] textLen The text length.
 */
template <typename T>
inline void ClearUniqueChar(std::unique_ptr<T[]> &text, size_t textLen)
{
    if (text != nullptr) {
        auto ret = memset_s(text.get(), textLen, 0, textLen);
        if (ret != EOK) {
            LOG(WARNING) << FormatString("memset failed, ret = %d", ret);
        }
        text = nullptr;
    }
}

/**
 * @brief Clear the string information in the memory.
 * @param[in/out] str The str needs to clear.
 */
inline void ClearStream(std::stringstream *ptr)
{
    if (ptr == nullptr) {
        LOG(WARNING) << "Failed to clear stream with nullptr";
        return;
    }
    size_t size = ptr->str().size();
    ptr->rdbuf()->pubseekpos(0);
    // set the sensitive char in memory to '\0'
    std::fill_n(std::ostreambuf_iterator<char>(ptr->rdbuf()), size, 0);
    // release the string memory
    ptr->str("");
}

/**
 * @brief Concatenates all elements in a vector into a string based on the connectStr.
 * @param[in] parts Elements vector.
 * @param[in] connectStr The connect string.
 * @return A complete connection string.
 */
inline std::string Join(const std::vector<std::string> &parts, const std::string &connectStr)
{
    if (parts.empty()) {
        LOG(ERROR) << "Vector is empty.";
        return "";
    }
    std::string result(*parts.begin());
    for (auto it = ++parts.begin(); it != parts.end(); ++it) {  // Skip first element.
        result.append(connectStr).append(*it);
    }
    return result;
}

/**
 * @brief Deletes all newline characters at the end of str and replaces them with null characters.
 * @param[in] str The string needs to be processed.
 * @return The string with newline characters removed.
 */
inline std::string RemoveNewlineOfStr(const std::string &str)
{
    std::string newStr = str;
    auto pos = newStr.find_last_not_of("\n\r");
    if (pos != std::string::npos) {
        newStr.erase(pos + 1);
    } else {
        newStr.clear();
    }
    return newStr;
}

/**
 * @brief Strip the suffix from a file name.
 * @param[in,out] filename Name of the file.
 * @param[in] suffix Suffix to be stripped.
 * @return True if successful.
 */
inline bool StripSuffix(std::string &filename, const std::string &suffix)
{
    auto n = filename.find_last_of(suffix);
    if (n != std::string::npos && (n + 1) == filename.length()) {
        filename.resize(filename.size() - suffix.size());
        return true;
    }
    return false;
}

/**
 * @brief Turn string to uppercase.
 * @param[in] str The string the transform.
 * @return String in uppercase.
 */
inline std::string StringToUpper(std::string str)
{
    std::transform(str.begin(), str.end(), str.begin(), ::toupper);
    return str;
}

/**
 * @brief Shuffle string with delimiter.
 * @param[in] str The string to shuffle.
 * @param[in] pattern Regular expression of split.
 * @return Shuffled string.
 */
inline std::string ShuffleStringWithDelimiter(const std::string &str, const std::string &pattern)
{
    auto strVec = SplitToUniqueStr(str, pattern);
    std::shuffle(strVec.begin(), strVec.end(), std::mt19937{ std::random_device{}() });
    std::string strShuffled;
    for (const auto &tmpStr : strVec) {
        strShuffled += tmpStr + pattern;
    }
    strShuffled.pop_back();
    return strShuffled;
}

inline std::string Trim(const std::string &str, const std::string &chars = STR_WHITESPACE)
{
    size_t first = str.find_first_not_of(chars);
    if (first == std::string::npos) {
        return "";
    }
    size_t last = str.find_last_not_of(chars);
    return str.substr(first, last - first + 1);
}

inline std::string GetSubStringAfterField(const std::string &input, const std::string &field)
{
    size_t pos = input.find(field);
    if (pos != std::string::npos) {
        return input.substr(pos + field.length());
    }
    return "";
}

inline std::string GetSubStringBeforeField(const std::string &input, const std::string &field)
{
    size_t pos = input.find(field);
    if (pos != std::string::npos) {
        return input.substr(0, pos);
    }
    return "";
}

/**
 * @brief Check whether the string is a valid integer or float number.
 * @param str The string to be checked.
 * @return True if the string is a valid integer or float number, false otherwise.
 */
bool IsValidNumber(const std::string &str);

inline const char *BoolToString(bool val)
{
    return val ? "true" : "false";
}

/**
 * @brief Get the truncated string of an input value.
 * @param input The input string.
 * @param retainDigits The retained digits.
 * @return std::string Truncated string.
 */
inline std::string GetTruncatedStr(const std::string &input, size_t retainDigits = 6)
{
    const size_t minDigits = 10;
    retainDigits = (input.length() > minDigits) ? retainDigits : 2;  // At least retain 2 digits
    retainDigits = std::min(retainDigits, input.length());

    return std::string(input.length() - retainDigits, '*') + input.substr(input.length() - retainDigits);
}

/**
 * Formats a string for logging output.
 *
 * @param str The original string to format,
 * @param maxDisplayLength Maximum characters to display before truncation (default: 255)
 * @return Formatted string.
 */
inline std::string FormatStringForLog(const std::string &str, size_t maxDisplayLength = 255)
{
    // Check if truncation is needed
    if (str.size() <= maxDisplayLength) {
        return str;
    }

    // Simple truncation: take the beginning portion and add ellipsis with size info
    return FormatString("%s... [total: %zu]", str.substr(0, maxDisplayLength).c_str(), str.size());
}
}  // namespace datasystem
#endif
