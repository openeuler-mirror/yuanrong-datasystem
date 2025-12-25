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

#ifndef BENCH_UTILS_H
#define BENCH_UTILS_H
#include "datasystem/utils/status.h"

namespace datasystem {
namespace bench {
Status StrToInt(const std::string &str, uint64_t &num);
Status StrToInt(const std::string &str, int32_t &num);
Status StrToHostPort(const std::string &str, std::string &host, int32_t &port);
Status StringToBytes(const std::string &sizeStr, uint64_t &byteSize);
}  // namespace bench
}  // namespace datasystem
#endif
