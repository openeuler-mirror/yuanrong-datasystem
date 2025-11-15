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

#ifndef BENCH_ARGS_H
#define BENCH_ARGS_H
#include <string>

#include "args_base.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace bench {
struct KVArgs : public ArgsBase {
    KVArgs(const std::string &command);
    Status Parse(int argc, char *argv[]) override;
    std::string Usage(const std::string &argv0);
    std::string ToString();
    std::string ownerWorker;
    std::string ownerId;
    std::string keyPrefix;
    uint64_t keyNum;
    std::string keySize;
    uint64_t batchNum;
    int workerNum;
};
}  // namespace bench
}  // namespace datasystem
#endif
