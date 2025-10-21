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
 * Description: LogManager will run threads and periodically do log rolling and compress.
 */
#ifndef DATASYSTEM_COMMON_LOG_LOG_MANAGER_H
#define DATASYSTEM_COMMON_LOG_LOG_MANAGER_H

#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <vector>
#include <atomic>

#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/thread.h"

namespace datasystem {
class LogManager {
public:
    /**
     * @brief Construct LogManager.
     * @param[in] logProcessInterval Log rolling or compress interval seconds.
     */
    explicit LogManager(uint32_t logProcessInterval) : logProcessInterval_(logProcessInterval) {};

    ~LogManager();

    /**
     * @brief Start log rolling thread.
     * @note Failure if not in the inited state.
     * @return Status of call.
     */
    Status Start();

    /**
     * @brief Stop log rolling thread.
     * @return Status of call.
     */
    Status Stop();

    /**
     * @brief Do log file rolling, use regular expression to get the log files and rolling them.
     * @return Status::OK() if success.
     */
    static Status DoLogFileRolling();

    /**
     * @brief Do log file compress, use regular expression to get the log files and compress them in '.gz' format.
     * @param[in/out] isCompress Indicates whether to perform compression.
     * @return Status::OK() if success.
     */
    static Status DoLogFileCompress(bool &isCompressed);

    /**
     * @brief Do log log monitor write , the interval depends on logProcessInterval_.
     * @return Status::OK() if success.
     */
    static Status DoLogMonitorWrite();

    /**
     * @brief Do log background task, include log file rolling and compress.
     * @return Status::OK() if success.
     */
    static Status DoLogBackgroundTask();

    /**
     * @brief Fetch log files need to compress and rolling.
     * @param[in/out] files Set of filenames matching the regex pattern.
     * @return Status::OK() if success.
     */
    static Status FetchLogWithPattern(std::vector<std::string> &files, bool isRolling = false);

private:
    struct FileUnit {
        FileUnit(std::string fileName, size_t size) : fileName_(std::move(fileName)), size_(size)
        {
        }

        // filename.
        std::string fileName_;

        // file size.
        size_t size_;
    };

    /**
     * @brief Launch a thread to periodically execute functor in waitSeconds intervals.
     * @param[in] func A callable.
     * @param[in] waitSeconds Wait interval, we execute func very waitSeconds seconds.
     */
    void RunTimerTask(const std::function<Status(void)> &func, int64_t waitSeconds);
    // Log rolling or compress interval seconds.
    uint32_t logProcessInterval_;
    // Thread for log background task.
    Thread backgroundThread_;

    enum State { INITED = 0, RUNNING, STOPPED };
    std::atomic<State> state_ = INITED;
};
}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_LOG_LOG_MANAGER_H
