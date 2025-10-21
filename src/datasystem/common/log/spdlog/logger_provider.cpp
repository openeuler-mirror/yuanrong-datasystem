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
 * Description: Logger provider.
 */
#include "datasystem/common/log/spdlog/logger_provider.h"

namespace datasystem {

LoggerProvider::LoggerProvider() noexcept : context_{ std::make_shared<LoggerContext>() }
{
}

LoggerProvider::LoggerProvider(const GlobalLogParam &globalLogParam) noexcept
    : context_{ std::make_shared<LoggerContext>(globalLogParam) }
{
}

LoggerProvider::~LoggerProvider()
{
    if (context_) {
        (void)context_->ForceFlush();
    }
}

DsLogger LoggerProvider::GetDsLogger() noexcept
{
    if (context_ == nullptr) {
        return nullptr;
    }

    return context_->GetLogger(DS_LOGGER_NAME);
}

DsLogger LoggerProvider::InitDsLogger(const LogParam &logParam) noexcept
{
    if (context_ == nullptr) {
        return nullptr;
    }

    auto logger = context_->GetLogger(DS_LOGGER_NAME);
    if (logger) {
        return logger;
    }

    return context_->CreateLogger(logParam);
}

void LoggerProvider::DropDsLogger() noexcept
{
    if (context_) {
        (void)context_->DropLogger(DS_LOGGER_NAME);
    }
}

bool LoggerProvider::ForceFlush(std::chrono::microseconds) noexcept
{
    if (context_ == nullptr) {
        return false;
    }

    return context_->ForceFlush();
}

}  // namespace datasystem