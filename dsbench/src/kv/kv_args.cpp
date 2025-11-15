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

#include "kv_args.h"

#include <getopt.h>
#include <iostream>
#include <re2/re2.h>
#include <sstream>

#include "datasystem/utils/status.h"
#include "utils.h"

namespace datasystem {

std::string GetGitHash()
{
    std::string gitHash = GIT_HASH;
    static re2::RE2 re("(\\[[a-zA-Z0-9]+\\]) (\\[[0-9\\-\\s\\:\\+]+\\])");

    std::string hash;
    std::string time;

    if (RE2::FullMatch(gitHash, re, &hash, &time)) {
        // Do not return the name for privacy and security purpose.
        return hash + time;
    }
    return "[UNKNOWN VERSION]";
}

namespace bench {
KVArgs::KVArgs(const std::string &command)
    : ArgsBase(command), keyPrefix("Bench"), keyNum(1), keySize("1KB"), batchNum(1), workerNum(0)
{
}

std::string KVArgs::Usage(const std::string &argv0)
{
    std::stringstream ss;
    ss << "Usage:" << argv0 << " kv [options]\n";
    ss << "Options:\n";
    ss << "  -a --action          action set/get/del\n";
    ss << "  -w --worker_address  worker address\n";
    ss << "  -o --owner_worker    meta owner worker address\n";
    ss << "  -p --prefix          the key prefix\n";
    ss << "  -n --num             Key number per thread, default 1\n";
    ss << "  -s --size            default key size 1024, support n/nB/nKB/nMB/nGB\n";
    ss << "  -t --thread_num      Thread number, default 1\n";
    ss << "  -b --batch_num       Batch number per request, default 1\n";
    ss << "  -f --perf_path       The perf point path\n";
    ss << "  -P --perf_workers    Get or reset perf point for those workers\n";
    ss << "  -k --access_key      Access key for authentication\n";
    ss << "  -K --secret_key      Secret key for authentication\n";
    ss << "  -W --worker_num      Worker number, used for key generation and range fetching (default: 0)\n";
    ss << "  -h                   Show help\n";
    return ss.str();
}

std::string KVArgs::ToString()
{
    std::stringstream ss;
    ss << "  -a --action:         " << action << "\n";
    ss << "  -w --worker_address: " << workerAddress << "\n";
    ss << "  -o --owner_worker:   " << ownerWorker << "\n";
    ss << "  -p --prefix:         " << keyPrefix << "\n";
    ss << "  -n --num:            " << keyNum << "\n";
    ss << "  -s --size:           " << keySize << "\n";
    ss << "  -t --thread_num:     " << threadNum << "\n";
    ss << "  -b --batch_num:      " << batchNum << "\n";
    ss << "  -f --perf_path:      " << perfPath << "\n";
    ss << "  -P --perf_workers:   " << perfWorkers << "\n";
    ss << "  -k --access_key:     " << accessKey << "\n";
    ss << "  -K --secret_key:     " << secretKey << "\n";
    ss << "  -W --worker_num:     " << workerNum;
    return ss.str();
}

Status KVArgs::Parse(int argc, char *argv[])
{
    static const struct option longOptions[] = {
        { "action", required_argument, nullptr, 'a' },       { "worker_address", required_argument, nullptr, 'w' },
        { "owner_worker", required_argument, nullptr, 'o' }, { "prefix", required_argument, nullptr, 'p' },
        { "num", required_argument, nullptr, 'n' },          { "size", required_argument, nullptr, 's' },
        { "thread_num", required_argument, nullptr, 't' },   { "batch_num", required_argument, nullptr, 'b' },
        { "show_args", required_argument, nullptr, 'S' },    { "perf_path", required_argument, nullptr, 'f' },
        { "perf_workers", required_argument, nullptr, 'P' }, { "access_key", required_argument, nullptr, 'k' },
        { "secret_key", required_argument, nullptr, 'K' },   { "version", no_argument, nullptr, 'v' },
        { "worker_num", required_argument, nullptr, 'W' },   { "help", no_argument, nullptr, 'h' }
    };

    while (true) {
        auto c = getopt_long(argc - 1, argv + 1, "ha:w:o:p:n:t:b:s:P:f:k:K:W:", longOptions, NULL);
        if (c == -1) {
            break;
        }
        Status rc;
        switch (c) {
            case 'a':
                action = optarg;
                break;
            case 'w':
                workerAddress = optarg;
                break;
            case 'o':
                ownerWorker = optarg;
                break;
            case 'p':
                keyPrefix = optarg;
                break;
            case 's':
                keySize = optarg;
                break;
            case 'f':
                perfPath = optarg;
                break;
            case 'P':
                perfWorkers = optarg;
                break;
            case 'k':
                accessKey = optarg;
                break;
            case 'K':
                secretKey = optarg;
                break;
            case 'n':
                rc = StrToInt(optarg, keyNum);
                break;
            case 't':
                rc = StrToInt(optarg, threadNum);
                break;
            case 'b':
                rc = StrToInt(optarg, batchNum);
                break;
            case 'W':
                rc = StrToInt(optarg, workerNum);
                break;
            default:
                std::cout << Usage(argv[0]);
                return Status(K_INVALID, "");
        }
        if (rc.IsError()) {
            std::cerr << "Error: Invalid argument value - " << rc.GetMsg() << "\n";
            std::cerr << "Please refer to the usage below:\n";
            std::cerr << Usage(argv[0]);
            return Status(K_INVALID, "");
        }
    }

    if (action.empty()) {
        std::cerr << "Error: action cannot be empty\n";
        std::cerr << "Please refer to the usage below:\n";
        std::cerr << Usage(argv[0]);
        return Status(K_INVALID, "");
    }

    if (workerAddress.empty()) {
        std::cerr << "Error: workerAddress cannot be empty\n";
        std::cerr << "Please refer to the usage below:\n";
        std::cerr << Usage(argv[0]);
        return Status(K_INVALID, "");
    }
    return Status::OK();
}
}  // namespace bench
}  // namespace datasystem
