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

#include "datasystem/common/util/format.h"

#include <cstring>
#include <iomanip>

namespace {
bool IsIn(const char *str, char c)
{
    if (str == nullptr) {
        return false;
    }
    // excluding \0
    for (size_t pos = 0; pos < strlen(str); ++pos) {
        if (str[pos] == c) {
            return true;
        }
    }

    return false;
}
}  // namespace

namespace datasystem {
Format::Format(const std::string &fmt) : fmt_(fmt)
{
    ParseAllParas();
}

Format::Format(const std::string &fmt, int size) : fmt_(fmt)
{
    if (size == 0) {
        result_ << fmt_;
        return;
    }
    units_.reserve(size);
    ParseAllParas(size);
}

std::string Format::Str() const
{
    if (currCount_ != expectCount_) {
        throw std::invalid_argument("args not match");
    }

    return result_.str();
}

void Format::Parse(size_t &pos, SplitUnit &unit)
{
    unit.splitBegin = pos;
    bool isSpecial = false;
    auto tmpPos = pos + 1;

    // check valid: format specifier should be followed.
    for (; tmpPos < fmt_.size(); tmpPos++) {
        if (IsIn("sdcSioxXufFeEaAgGp", fmt_[tmpPos])) {  // type indicators
            unit.splitEnd = static_cast<int>(tmpPos + 1);
            if (IsIn("oxXeEpaAfF", fmt_[tmpPos])) {
                isSpecial = true;
            }
            unit.isSpecialNeed = isSpecial;
            break;
        } else if (IsIn("hljztL0123456789-+.# ", fmt_[tmpPos])) {  // format indicators and length decorators
            if (IsIn("0123456789-+.# ", fmt_[tmpPos])) {           // format indicators
                isSpecial = true;
            }
            continue;
        } else {
            throw std::invalid_argument("invalid format" + fmt_);
        }
    }

    pos = tmpPos;
    expectCount_++;
}

void Format::ParseAllParas(size_t needParseNum)
{
    // find %
    std::string::size_type pos = 0;
    while ((pos = fmt_.find('%', pos)) != std::string::npos) {
        // transform escape %%
        if (pos < fmt_.size() - 1 && fmt_[pos + 1] == '%') {
            fmt_.erase(pos, 1);
            pos++;
            continue;
        }

        SplitUnit unit{ 0, 0, false };
        Parse(pos, unit);
        if (needParseNum > 0) {
            units_[expectCount_ - 1] = unit;
        } else {
            units_.push_back(std::move(unit));
        }

        if (units_[expectCount_ - 1].splitEnd == 0) {
            throw std::invalid_argument("invalid format terminal");
        }
    }

    result_ << fmt_.substr(0, (expectCount_ == 0) ? std::string::npos : units_[0].splitBegin);
}

void Format::SetFlagsAndWidth(const std::string &expression, size_t flagEnd, bool &revertFill)
{
    auto width = atoi(expression.data() + (flagEnd == std::string::npos ? 0 : flagEnd));

    for (size_t i = 0; i < flagEnd; i++) {
        switch (expression[i]) {
            case '-':
                result_ << std::left;
                break;
            case '+':
                result_ << std::showpos;
                break;
            case '#':
                result_ << std::showbase << std::showpos;
                break;
            case ' ':
                break;
            case '0':
                result_ << std::setfill('0');
                revertFill = true;
                break;
            default:
                break;
        }
    }

    result_ << std::setw(width);
}

void Format::SetPrecisionAndArgType(const std::string &expression, size_t flagEnd)
{
    auto precisionPos = expression.find('.', flagEnd);
    int precision = 6;  // default precision of printf is 6
    if (precisionPos != std::string::npos) {
        precision = atoi(expression.data() + precisionPos + 1);
    }

    switch (fmt_[units_[currCount_].splitEnd - 1]) {
        case 'x':
            result_ << std::hex;
            break;
        case 'X':
            result_ << std::uppercase << std::hex;
            break;
        case 'o':
            result_ << std::oct;
            break;
        case 'e':
            result_ << std::scientific;
            break;
        case 'E':
            result_ << std::uppercase << std::scientific;
            break;
        case 'p':
            result_ << std::hex;
            break;
        case 'a':
            result_ << std::hexfloat;
            break;
        case 'A':
            result_ << std::uppercase << std::hexfloat;
            break;
        case 'f':
            result_ << std::setprecision(precision) << std::fixed;
            break;
        case 'F':
            result_ << std::uppercase << std::setprecision(precision) << std::fixed;
            break;
        default:
            break;
    }
}

void Format::SetFlags(bool &revertFill)
{
    // expression: [flags][width][.precision][argType]
    auto expression = GetExpression();
    auto flagEnd = expression.find_first_not_of("-+# 0");

    SetFlagsAndWidth(expression, flagEnd, revertFill);
    SetPrecisionAndArgType(expression, flagEnd);
}

void Format::ResetFlags(bool revertFill)
{
    result_ << resetiosflags(result_.flags());

    if (revertFill) {
        result_ << std::setfill(' ');
    }
}
}  // namespace datasystem