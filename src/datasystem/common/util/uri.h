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
 * Description: The Uri management.
 */
#ifndef DATASYSTEM_CLIENT_FILE_CACHE_URI_H
#define DATASYSTEM_CLIENT_FILE_CACHE_URI_H

#include <climits>
#include <functional>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "datasystem/utils/status.h"

namespace datasystem {
class Uri {
public:
    /**
     * @brief Create a new URI in the input path.
     */
    explicit Uri(const std::string &pathStr);

    Uri() = default;

    ~Uri() = default;

    Uri(const Uri &other);

    Uri(Uri &&other) noexcept;

    Uri &operator=(const Uri &other);

    Uri &operator=(Uri &&other) noexcept;

    /**
     * @brief Return the path of string type.
     * @return The path string.
     */
    std::string Path() const;

    /**
     * @brief Return the paths separated by '/'.
     * @return The path name vector.
     */
    std::vector<std::string> GetSeparatePath() const;

    /**
     * @brief Return the filename.
     * @return The file name string.
     */
    std::string GetFileName() const;

    /**
     * @brief Return whether the path is root.
     * @return True for root, false for non-root.
     */
    bool IsRoot() const;

    /**
     * @brief Normalize the path, by replacing all "//" with "/".
     */
    void NormalizePath();

    /**
     * @brief Get the parent of this Uri or null if at root.
     * @return The parent path.
     */
    std::string GetParent() const;

    /**
     * @brief Validate is file uri.
     * @return OK if validate, otherwise return INVALID_PARAMETER.
     */
    Status ValidateFile() const;

    /**
     * @ref https://www.boost.org/doc/libs/1_76_0/libs/filesystem/doc/portability_guide.htm#name_check_functions
     * @brief Validate is file or directory uri.
     * @return OK if validate, otherwise return INVALID_PARAMETER.
     */
    Status Validate() const;

    /**
     * @brief Indicate the uri is sub-path of parent.
     * @param[in] parent Parnet path.
     * @return True if uri is sub-path of parnet.
     */
    bool IsSubPathOf(const std::string &parent) const;

    /**
     * @brief Get user's home dir.
     * @param[out] userPath Get user's home path.
     * @return K_OK on success; K_RUNTIME_ERROR if get nullptr.
     */
    static Status GetHomeDir(std::string &userPath);

    /**
    * @brief Resolve the given path.
    * @param[in] path The path to resolve.
    * @param[out] resolvedPath The resolved path.
    * @return K_OK on success; K_RUNTIME_ERROR if realpath returns nullptr.
    */
    static Status Realpath(const std::string &path, std::string &resolvedPath);

    /**
    * @brief Resolve the given file path.
    * @param[in] rawFilePath The raw file path to resolve.
    * @param[out] realPath The resolved path.
    * @return K_OK on success; K_RUNTIME_ERROR if resolve raw path failed.
    */
    static Status GetRealfile(const std::string &rawFilePath, std::string &realPath);

    /**
    * @brief Normalize the path with user's homeDir.
    * @param[in] defaultDir The default value for the corresponding parameter.
    * @param[in] additionalPath Set it if dir equals with defaultDir and need to add additional path.
    * @param[out] dir The normalized path.
    * @return OK if success, otherwise return K_RUNTIME_ERROR.
     */
    static Status NormalizePathWithUserHomeDir(std::string &dir, const std::string &defaultDir,
                                        const std::string &additionalPath);

    /**
    * @brief Delete the append path.
    * @param[out] dir The path's additions have been removed.
    * @return OK if success, otherwise return K_RUNTIME_ERROR.
     */
    static Status DeleteAppendPath(std::string &dir);

    /**
    * @brief Modify the files in an input directory.
    * @param[in] inputDir The input directory.
    * @param[in] mode The permission want to change to.
    * @return OK if success, otherwise return K_RUNTIME_ERROR.
     */
    static Status ModifyFilesInInputDir(const std::string &inputDir, const mode_t permission);

    /**
     * @brief Convert a string to an integer.
     * @note Since the data type of integer is too complex(such as [short int long int32 int64 uint16 uint32 ...]),
     * and the size of some types of data depends on the machine environment, this interface is provided
     * to realize the following scenarios:
     * Achieve more data conversion with fewer functions, and reuse more functions in the standard library.
     * @param[in] str The string to be converted.
     * @param[in] func The function of converting a string to an integer of NumType2.
     * @param[in] type1Max The max num of NumType1.
     * @param[in] type1Min The min num of NumType1.
     * @param[in] type2Max The max num of NumType2.
     * @param[in] type2Min The min num of NumType2.
     * @param[out] res The integer being converted by a string.
     * @return True if res is available, false otherwise.
     */
    template <typename NumType1, typename NumType2>
    static bool StrToInteger(const char* str, std::function<NumType2(const char*, char**, int)> fun, NumType1 type1Max,
                            NumType1 type1Min, NumType2 type2Max, NumType2 type2Min, NumType1 &res);

    static bool StrToInt32(const char* str, int32_t &res);
    static bool StrToInt(const char* str, int &res);
    static bool StrToLong(const char* str, long &res);
    static bool StrToUint64(const char* str, uint64_t &res);

private:
    bool IsWindowsName(const std::string &name) const;

    std::string origPath_;
    std::string formatPath_;
    const std::string separator_ = "/";
    static std::mutex mutex_;
};

/**
 * @brief Checks if the string ends with the given suffix.
 * @param[in] fullStr The full string.
 * @param[in] ending The ending given suffix.
 * @return True for yes, false for no.
 */
inline bool endsWith(std::string const &fullStr, std::string const &ending)
{
    if (fullStr.length() >= ending.length()) {
        return (fullStr.compare(fullStr.length() - ending.length(), ending.length(), ending) == 0);
    } else {
        return false;
    }
}

/**
 * @brief Replaces all occurrences of a substring in a string.
 * @param[in] str The string in which replacements are performed.
 * @param[in] oldSubstr The substring to be replaced.
 * @param[in] newSubstr The substring that will replace `oldSubstr`.
 */
inline void ReplaceRecursively(std::string &str, const std::string &oldSubstr, const std::string &newSubstr)
{
    while (true) {
        std::string::size_type pos(0);
        if ((pos = str.find(oldSubstr)) == std::string::npos) {
            break;
        }
        str.replace(pos, oldSubstr.length(), newSubstr);
    }
}
}  // namespace datasystem
#endif  // DATASYSTEM_CLIENT_FILE_CACHE_URI_H