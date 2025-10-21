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

/**
 * Description: Provider.
 */
#include "datasystem/common/log/spdlog/provider.h"

namespace datasystem {

static Provider g_ProviderInstance;

bool g_ProviderAlive = false;

Provider::Provider()
{
    // Ensure spdlog is initialized early to avoid static initialization order issues.
    // This forces the construction of internal singletons (e.g., registry) via a
    // harmless public API call, securing proper lifetime ordering.
    LoggerContext::GetDefaultLogger();
    g_ProviderAlive = true;
}

Provider::~Provider()
{
    g_ProviderAlive = false;
}

Provider &Provider::Instance()
{
    return g_ProviderInstance;
}

bool Provider::IsAlive()
{
    return g_ProviderAlive;
}

std::string Provider::GetPodName()
{
    const char *podName = (std::getenv("POD_NAME") == nullptr) ? std::getenv("HOSTNAME") : std::getenv("POD_NAME");
    if (podName == nullptr) {
        podName = " ";
    }

    return podName;
}

void Provider::FlushLogs()
{
    std::shared_lock<std::shared_mutex> mutex(mutex_);
    if (provider_) {
        provider_->ForceFlush();
    }
}

std::shared_ptr<LoggerProvider> Provider::GetLoggerProvider() noexcept
{
    std::shared_lock<std::shared_mutex> mutex(mutex_);
    return provider_;
}

void Provider::SetLoggerProvider(const std::shared_ptr<LoggerProvider> &tp) noexcept
{
    std::unique_lock<std::shared_mutex> mutex(mutex_);
    provider_ = tp;
}

}  // namespace datasystem
