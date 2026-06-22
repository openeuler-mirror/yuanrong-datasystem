/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
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
 * Description: Operation audit logger for config lifecycle events.
 */
#ifndef DATASYSTEM_COMMON_LOG_OPERATION_LOGGER_H
#define DATASYSTEM_COMMON_LOG_OPERATION_LOGGER_H

#include <memory>
#include <mutex>
#include <string>

#include <spdlog/spdlog.h>

namespace datasystem {

class OperationLogger {
public:
    static OperationLogger &Instance();

    /**
     * @brief Initialize operation audit logger.
     * @param[in] role Process role, e.g. "worker" or "client".
     * @return True if initialization succeeds.
     */
    bool Init(const std::string &role);

    /**
     * @brief Shutdown operation audit logger and flush pending messages.
     */
    void Shutdown();

    /**
     * @brief Record operation logger startup in audit log.
     */
    void LogOperationStart();

    /**
     * @brief Record operation logger shutdown in audit log.
     */
    void LogOperationStop();

    /**
     * @brief Record initial configuration snapshot in audit log.
     * @param[in] flagsSnapshot Serialized non-default flags at startup.
     */
    void LogConfigInit(const std::string &flagsSnapshot);

    /**
     * @brief Record configuration initialization failure in audit log.
     * @param[in] detail Failure detail message.
     */
    void LogConfigInitFailed(const std::string &detail);

    /**
     * @brief Record a successful dynamic configuration change in audit log.
     * @param[in] name Flag name.
     * @param[in] oldVal Previous value string.
     * @param[in] newVal New value string.
     */
    void LogConfigChanged(const std::string &name, const std::string &oldVal, const std::string &newVal);

    /**
     * @brief Record a failed dynamic configuration change in audit log.
     * @param[in] name Flag name.
     * @param[in] reason Failure reason.
     */
    void LogConfigFailed(const std::string &name, const std::string &reason);

    /**
     * @brief Get operation audit log file path.
     * @return ${log_dir}/${log_filename}_operation.log
     */
    std::string OperationLogPath() const;

private:
    OperationLogger() = default;
    ~OperationLogger() = default;
    OperationLogger(const OperationLogger &) = delete;
    OperationLogger &operator=(const OperationLogger &) = delete;
    OperationLogger(OperationLogger &&) = delete;
    OperationLogger &operator=(OperationLogger &&) = delete;

    void WriteLog(const std::string &message);

    std::shared_ptr<ds_spdlog::logger> logger_;
    std::string role_;
    std::string logPath_;
    mutable std::mutex mutex_;
};

}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_LOG_OPERATION_LOGGER_H
