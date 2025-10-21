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

#include "datasystem/client/client_flags_monitor.h"

#include <cstdlib>
#include <cstring>

#include "datasystem/common/log/log.h"
#include "datasystem/common/util/validator.h"

DS_DECLARE_string(monitor_config_file);

const int LISTENGING_FILE_TIME_INTERVAL = 1;

namespace datasystem {
FlagsMonitor *FlagsMonitor::GetInstance()
{
    static FlagsMonitor instance;
    return &instance;
}

FlagsMonitor::FlagsMonitor() : flags_(), monitorThread_(), stop_(false), isStarted_(false)
{
}

FlagsMonitor::~FlagsMonitor()
{
    {
        std::unique_lock<std::mutex> lock(mutex_);
        condition_.notify_all();
        stop_ = true;
    }
    if (monitorThread_.joinable()) {
        monitorThread_.join();
    }
}

void FlagsMonitor::Start()
{
    LOG(INFO) << "Start the thread for listening to the log configuration file.";
    bool expected = false;
    if (isStarted_.compare_exchange_strong(expected, true)) {
        monitorThread_ = std::thread(&FlagsMonitor::ListenConfigFile, this);
    }
}

void FlagsMonitor::ListenConfigFile()
{
    std::unique_lock<std::mutex> lock(mutex_);
    std::string configFilePath = GetStringFromEnv("DATASYSTEM_CLIENT_CONFIG_PATH", FLAGS_monitor_config_file);
    Status rc = Uri::NormalizePathWithUserHomeDir(configFilePath, "", "");
    LOG_IF_ERROR(rc, FormatString("Failed to normalize the path (%s) with user home directory", configFilePath));
    if (rc.IsError()) {
        return;
    }
    LOG(INFO) << "The path of the configuration file is:" << configFilePath;
    while (!stop_) {
        flags_.StartConfigFileHandle(configFilePath, std::chrono::steady_clock::now());
        condition_.wait_for(lock, std::chrono::seconds(LISTENGING_FILE_TIME_INTERVAL));
    }
}
}  // namespace datasystem
