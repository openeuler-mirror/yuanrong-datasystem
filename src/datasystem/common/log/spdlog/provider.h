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
#ifndef DATASYSTEM_COMMON_LOG_SPDLOG_PROVIDER_H
#define DATASYSTEM_COMMON_LOG_SPDLOG_PROVIDER_H

#include <shared_mutex>

#include "datasystem/common/log/spdlog/logger_provider.h"

namespace datasystem {

class Provider {
public:
    Provider();

    Provider(const Provider &other) = delete;

    Provider(Provider &&other) = delete;

    Provider &operator=(const Provider &) = delete;

    Provider &operator=(Provider &&) = delete;

    ~Provider();

    /**
     * @brief Obtain the instance of this singleton class.
     * @return The pointer of Provider.
     */
    static Provider &Instance();

    /**
     * @brief Check if the Provider instance is alive.
     */
    static bool IsAlive();

    /**
     * @brief Retrieve the pod identifier from environment variables.
     *
     * The priority is POD_IP, POD_NAME, HOSTNAME, then a blank placeholder.
     */
    static std::string GetPodName();

    /**
     * @brief Flush all pending log messages.
     */
    void FlushLogs();

    std::shared_ptr<LoggerProvider> GetLoggerProvider() noexcept;

    void SetLoggerProvider(const std::shared_ptr<LoggerProvider> &tp) noexcept;

private:
    std::shared_mutex mutex_; // Protects access to provider_ for thread-safe read/write operations.
    std::shared_ptr<LoggerProvider> provider_ = nullptr;
};

}  // namespace datasystem

#endif
