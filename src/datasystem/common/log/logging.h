/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2025. All rights reserved.
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
 * Description: Logging util for spdlog.
 */
#ifndef DATASYSTEM_COMMON_LOG_LOGGING_H
#define DATASYSTEM_COMMON_LOG_LOGGING_H

#include <chrono>
#include <memory>

#include "datasystem/common/flags/flags.h"
#include "datasystem/common/util/locks.h"
#include "datasystem/utils/status.h"

DS_DECLARE_int32(v);

namespace datasystem {
static constexpr uint32_t LOG_ROLLING_COMPRESS_SECS = 30;  // Log rolling or compress interval seconds.
static const std::string LOG_NAME_ENV = "DATASYSTEM_CLIENT_LOG_NAME";
static const std::string ACCESS_LOG_NAME_ENV = "DATASYSTEM_CLIENT_ACCESS_LOG_NAME";
static const std::string LOG_DIR_ENV = "DATASYSTEM_CLIENT_LOG_DIR";
static const std::string MAX_LOG_SIZE_ENV = "DATASYSTEM_CLIENT_MAX_LOG_SIZE";
static const std::string MAX_LOG_FILE_NUM_ENV = "DATASYSTEM_MAX_LOG_FILE_NUM";
static const std::string LOG_COMPRESS_ENV = "DATASYSTEM_LOG_COMPRESS";
static const std::string LOG_RETENTION_DAY_ENV = "DATASYSTEM_LOG_RETENTION_DAY";
static const std::string DEFAULT_LOG_DIR = "~/datasystem/logs";
static const std::string LOG_TO_STDERR_ENV = "DATASYSTEM_LOG_TO_STDERR";
static const std::string ALSO_LOG_TO_STDERR_ENV = "DATASYSTEM_ALSO_LOG_TO_STDERR";
static const std::string STDERR_THRESHOLD_ENV = "DATASYSTEM_STD_THRESHOLD";
static const std::string LOG_ASYNC_ENABLE = "DATASYSTEM_LOG_ASYNC_ENABLE";
static const std::string LOG_ASYNC_QUEUE_SIZE = "DATASYSTEM_LOG_ASYNC_QUEUE_SIZE";
static const std::string LOG_V = "DATASYSTEM_LOG_V";
static const std::string MIN_LOG_LEVEL = "DATASYSTEM_MIN_LOG_LEVEL";
static const std::string LOG_MONITOR_ENABLE = "DATASYSTEM_LOG_MONITOR_ENABLE";
static const std::string LOG_RATE_LIMIT_ENV = "DATASYSTEM_LOG_RATE_LIMIT";

constexpr uint32_t DEFAULT_LOG_ASYNC_QUEUE_SIZE = 65536;
constexpr uint32_t DEFAULT_MAX_LOG_SIZE_MB = 400;

class AccessRecorderManager;

class LogManager;

class Logging {
public:
    Logging(const Logging &other) = delete;

    Logging(Logging &&other) = delete;

    Logging &operator=(const Logging &) = delete;

    Logging &operator=(Logging &&) = delete;

    ~Logging();

    static Logging *GetInstance();

    /**
     * @brief  Start log for singleton mode.
     * @param[in] logFilename The name of log file.
     * @param[in] isClient The client sets log parameters through environment variables. So we need this flag to
     * indicate the client.
     * @param[in] logProcessInterval Log rolling or compress interval seconds.
     */
    void Start(const std::string logFilename = "", bool isClient = false,
               uint32_t logProcessInterval = LOG_ROLLING_COMPRESS_SECS, bool isEmbeddedClient = false);

    /**
     * @brief  Check and create the log dir.
     * @return True if success.
     */
    static bool CreateLogDir();

    /**
     * @brief Get the Singleton TimeCostLogger instance.
     * @return TimeCostLogger instance.
     */
    static AccessRecorderManager *AccessRecorderManagerInstance();

    /**
     * @brief direct write log to file
     *
     * @param[in] lineOfCode The line no.
     * @param[in] fileNameOfCode The file name.
     * @param[in] logFileName The log file name.
     * @param[in] level The log level in I/W/E.
     * @param[in] message The log message.
     * @return Status of this call.
     */
    static Status WriteLogToFile(int lineOfCode, const std::string &fileNameOfCode, const std::string &logFileName,
                                 char level, const std::string &message);

    static std::string PodName()
    {
        return podName_;
    }

    static bool ValidateLogName(const std::string &logName);

protected:
    /**
     * @brief Safely get max_log_size, overriding to 1 if it somehow gets defined as 0.
     */
    static uint32_t MaxLogSize();

    /**
     * @brief Initialize log wrapper.
     * @param[in] logProcessInterval Log rolling or compress interval seconds.
     * @return True if success.
     */
    bool InitLoggingWrapper(uint32_t logProcessInterval);

    /**
     * @brief Shutdown log wrapper.
     */
    void ShutdownLoggingWrapper();

    /**
     * @brief Init client basic log parameters from environment.
     */
    void InitClientBasicConfig();

    /**
     * @brief Init client advanced log parameters from environment.
     */
    void InitClientAdvancedConfig();

    /**
     * @brief Init client log parameters from environment, and this function will abort if failed to init log parameters
     * from the environment. NOTE: for booleans, for true use 't' or 'T' or 'true' or '1', for false 'f' or 'F' or
     * 'false' or '0'.
     */
    void InitClientConfig();

    bool IsLoggingInitialized() const
    {
        return isLoggingInitialized_;
    }

    void SetLoggingInitialized(bool status)
    {
        isLoggingInitialized_ = status;
    }

private:
    explicit Logging();

    std::unique_ptr<AccessRecorderManager> accessRecorderManagerInstance_;

    // For make_unique to access private constructor.
    friend std::unique_ptr<Logging> std::make_unique<Logging>();

    // Log manager, it will start threads for log rolling, log compress and log flush_.
    std::unique_ptr<LogManager> logManager_;
    WriterPrefRWLock mux_;  // protects the initialization process of log.
    std::atomic<bool> init_{ false };
    static std::string podName_;
    bool isClient_;
    bool isEmbeddedClient_ = false;
    bool isLoggingInitialized_ = false;
};

}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_LOG_LOGGING_H
