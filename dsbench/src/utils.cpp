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

#include "utils.h"
#include <fstream>
#include <iostream>
#include <libgen.h>
#include <map>
#include <numeric>
#include <stdexcept>
#include <sstream>
#include <thread>

namespace datasystem {
namespace bench {

Status StrToInt(const std::string &str, uint64_t &num)
{
    try {
        num = std::stoul(str);
    } catch (std::invalid_argument &invalidArgument) {
        return Status(StatusCode::K_INVALID, "Failed to parse '" + str + "' as number: invalid argument");
    } catch (std::out_of_range &outOfRange) {
        return Status(StatusCode::K_INVALID, "Failed to parse '" + str + "' as number: value out of range");
    }
    return Status::OK();
}

Status StrToInt(const std::string &str, int32_t &num)
{
    try {
        num = std::stol(str);
    } catch (std::invalid_argument &invalidArgument) {
        return Status(StatusCode::K_INVALID, "Failed to parse '" + str + "' as number: invalid argument");
    } catch (std::out_of_range &outOfRange) {
        return Status(StatusCode::K_INVALID, "Failed to parse '" + str + "' as number: value out of range");
    }
    return Status::OK();
}

Status StrToHostPort(const std::string &str, std::string &host, int32_t &port)
{
    auto index = str.find(":");
    if (index == std::string::npos) {
        return Status(K_INVALID, "Invalid host-port format: '" + str + "'. Expected 'host:port'");
    }
    host = str.substr(0, index);
    auto portStr = str.substr(index + 1);
    return StrToInt(portStr, port);
}

Status StringToBytes(const std::string &sizeStr, uint64_t &byteSize)
{
    if (sizeStr.empty()) {
        return Status(K_INVALID, "Empty size string is invalid");
    }
    size_t unitPos = 0;
    while (unitPos < sizeStr.size() && (std::isdigit(sizeStr[unitPos]) || sizeStr[unitPos] == '.')) {
        unitPos++;
    }
    if (unitPos == 0) {
        return Status(K_INVALID, "Invalid size format: '" + sizeStr + "'");
    }
    std::string numStr = sizeStr.substr(0, unitPos);
    double number = std::stod(numStr);
    std::string unitStr = sizeStr.substr(unitPos);
    double factor = 1;
    if (!unitStr.empty()) {
        for (char &c : unitStr) {
            c = std::toupper(c);
        }
        static std::map<std::string, double> unitFactors = { { "B", 1 },
                                                             { "KB", 1024 },
                                                             { "MB", 1024 * 1024 },
                                                             { "GB", 1024ul * 1024 * 1024 },
                                                             { "TB", 1024ul * 1024 * 1024 * 1024.0 },
                                                             { "PB", 1024ul * 1024 * 1024 * 1024 * 1024.0 } };
        if (unitFactors.find(unitStr) != unitFactors.end()) {
            factor = unitFactors[unitStr];
        } else if (!unitStr.empty()) {
            return Status(K_INVALID, "Unsupported size unit in '" + sizeStr + "'");
        }
    }
    byteSize = number * factor;
    return Status::OK();
}
}  // namespace bench
}  // namespace datasystem
