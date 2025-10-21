/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2024. All rights reserved.
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
 * Description: The device object cache example.
 */

#include <iostream>

#include "datasystem/perf_client.h"

using datasystem::ConnectOptions;
using datasystem::Status;

static std::shared_ptr<datasystem::PerfClient> client;

static std::string DEFAULT_IP = "127.0.0.1";
static constexpr int DEFAULT_PORT = 9088;
static constexpr int PARAMETERS_NUM = 3;

bool Start()
{
    std::cerr << "Start to get perf log" << std::endl;
    std::unordered_map<std::string, std::unordered_map<std::string, uint64_t>> clientPerfLog;
    Status status = client->GetPerfLog("client", clientPerfLog);
    if (status.IsError()) {
        std::cerr << "Run device exampled failed, detail: " << status.ToString() << std::endl;
    }

    return status.IsOk();
}

int main(int argc, char *argv[])
{
    std::string ip;
    int port = 0;
    int index = 0;
    std::string clientPublicKey, clientPrivateKey, serverPublicKey;

    if (argc == 1) {
        ip = DEFAULT_IP;
        port = DEFAULT_PORT;
    } else if (argc == PARAMETERS_NUM) {
        ip = argv[++index];
        port = atoi(argv[++index]);
    } else {
        std::cerr << "Invalid input parameters.";
        return -1;
    }

    std::cout << "Start to run perf example" << std::endl;

    // Get the client
    std::string workerAddr = ip + ':' + std::to_string(port);
    std::cout << "The client workerAddr:" << workerAddr << std::endl;
    ConnectOptions connectOpts{ .host = ip, .port = port, .connectTimeoutMs = 5 * 1000 };
    client = std::make_shared<datasystem::PerfClient>(connectOpts);
    Status status = client->Init();
    if (status.IsError()) {
        std::cerr << "[device]Failed to init object client : " << status.ToString() << std::endl;
        return -1;
    }
    if (!Start()) {
        return -1;
    }
    return 0;
}
