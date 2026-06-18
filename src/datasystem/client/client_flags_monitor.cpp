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
#include "datasystem/common/util/gflag/config_monitor_state.h"
#include "datasystem/common/util/uri.h"
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

bool FlagsMonitor::IsMonitorThreadRunning() const
{
    return monitorThread_.joinable();
}

Flags &FlagsMonitor::GetFlags()
{
    return flags_;
}

void FlagsMonitor::Start()
{
    bool expected = false;
    if (!isStarted_.compare_exchange_strong(expected, true)) {
        return;
    }
    std::string configFilePath = GetStringFromEnv("DATASYSTEM_CLIENT_CONFIG_PATH", FLAGS_monitor_config_file);
    Status rc = Uri::NormalizePathWithUserHomeDir(configFilePath, "", "");
    if (rc.IsError() || configFilePath.empty()) {
        LOG(INFO) << "Skip config file monitor: empty monitor path";
        ConfigMonitorState::Instance().SetFileMonitorEnabled(false);
        isStarted_ = false;
        return;
    }
    configFilePath_ = configFilePath;
    ConfigMonitorState::Instance().SetFileMonitorEnabled(true);
    LOG(INFO) << "Start the thread for listening to the log configuration file.";
    monitorThread_ = std::thread(&FlagsMonitor::ListenConfigFile, this);
}

void FlagsMonitor::ListenConfigFile()
{
    std::unique_lock<std::mutex> lock(mutex_);
    LOG(INFO) << "The path of the configuration file is:" << configFilePath_;
    while (!stop_) {
        flags_.StartConfigFileHandle(configFilePath_, std::chrono::steady_clock::now());
        condition_.wait_for(lock, std::chrono::seconds(LISTENGING_FILE_TIME_INTERVAL));
    }
}
}  // namespace datasystem
