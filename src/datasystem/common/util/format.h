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
 * Description: A type-safed simple format tool.
 */
#ifndef DATASYSTEM_COMMON_UTIL_FORMAT_H
#define DATASYSTEM_COMMON_UTIL_FORMAT_H

#include <vector>
#include <sstream>

namespace datasystem {
struct SplitUnit {
    int splitBegin;
    int splitEnd;
    bool isSpecialNeed;
};

class Format {
public:
    explicit Format(const std::string &fmt);

    Format(const std::string &fmt, int size);

    /**
     * @brief Parsing escape characters in strings.
     * @param[in] arg The string to be filled.
     * @return return to self.
     */
    template <typename T>
    Format &operator%(const T &arg)
    {
        if (currCount_ >= expectCount_) {
            throw std::invalid_argument("too much args");
        }

        if (!IsSpecialNeed()) {
            result_ << arg;
        } else {
            ParseSpecialNeed(arg);
        }

        result_ << fmt_.substr(units_[currCount_].splitEnd,
                               currCount_ + 1 >= expectCount_
                                   ? std::string::npos
                                   : (units_[currCount_ + 1].splitBegin - units_[currCount_].splitEnd));

        currCount_++;
        return *this;
    }

    std::string Str() const;

private:
    /**
     * @brief Records the location of an escape character and whether special parsing is required
     *        (such as scientific counting).
     * @param[in/out] pos Current position of the string.
     * @param[out] unit Information about the escape character, including the start position, end position,
     *        and whether special parsing is required.
     */
    void Parse(size_t &pos, SplitUnit &unit);

    /**
     * @brief Parses the entire string.
     * @param[in] needParseNum Number of escape characters to be parsed. If the value is 0,
     *        the entire character string is automatically parsed.
     */
    void ParseAllParas(size_t needParseNum = 0);

    bool IsSpecialNeed() const
    {
        return units_[currCount_].isSpecialNeed;
    }

    std::string GetExpression() const
    {
        return std::string(fmt_.data() + units_[currCount_].splitBegin + 1,
                           units_[currCount_].splitEnd - units_[currCount_].splitBegin);
    }

    void SetFlags(bool &revertFill);
    void ResetFlags(bool revertFill);
    void SetFlagsAndWidth(const std::string &expression, size_t flagEnd, bool &revertFill);
    void SetPrecisionAndArgType(const std::string &expression, size_t flagEnd);

    template <typename T>
    void ParseSpecialNeed(const T &arg)
    {
        bool revertFill{ false };
        SetFlags(revertFill);
        result_ << arg;
        ResetFlags(revertFill);
    }

    int expectCount_{ 0 };
    int currCount_{ 0 };
    std::stringstream result_;
    std::string fmt_;
    std::vector<SplitUnit> units_;
};

/**
 * @brief Format string with specific pattern.
 * @param[in] format The string format pattern.
 * @param[in] args The strings to be filled.
 * @return Return formatted string.
 */
template <class... Args>
std::string FormatString(const std::string &format, Args&&... args)
{
    int paraSize = static_cast<int>(sizeof...(Args));
    Format fmt(format, paraSize);
    (void)std::initializer_list<int>{ (fmt % args, 0)... };
    return fmt.Str();
}
}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_UTIL_FORMAT_H