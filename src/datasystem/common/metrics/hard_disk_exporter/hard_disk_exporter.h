/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2023. All rights reserved.
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
 * Description: Monitor logger, flush current node's res information and operation time cost
 * information, there is two Bounded-buffer to support async write log messages.
 */

#ifndef DATASYSTEM_WORKER_HARD_DISK_EXPORTER_H
#define DATASYSTEM_WORKER_HARD_DISK_EXPORTER_H

#include <string>

#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/metrics/metrics_exporter.h"

namespace datasystem {
constexpr int K_INVALID_FD = -1;

class HardDiskExporter : public MetricsExporter {
public:
    /**
     * @brief  Initialize sink and start some threads and create log file by absolute path.
     * @param[in] filePath Absolute path to the log file.
     * @return Status of the call.
     */
    Status Init(const std::string &filePath);

    ~HardDiskExporter();

    /**
     * @brief Write a message to the active buffer.
     * @param[in] message the message of for writing to the active buffer
     * @param[in] uri file uri
     * @param[in] line line of code.
     */
    void Send(const std::string &message, Uri &uri, int line) override;

private:
    /**
     * @brief Write message to specified file.
     */
    void FlushThread();

    /**
     * @brief Rename old file and create new file.
     */
    void ChangeLogFile();

    /**
     * @brief Get new file path.
     * @param timestamp[in] The timestamp required to create the file.
     * @param filePath[out] The file path need to create.
     */
    void GetLogFilePath(uint64_t &timestamp, std::string &filePath);

    /**
     * @brief Create log file by absolute path.
     * @param[in] filePath Absolute path to the log file.
     * @return Status of the call.
     */
    Status CreateFileByPath(const std::string &filePath);

    /**
     * @brief Collect rotated log files.
     * @param[in] filePath The active log file path.
     * @param[out] files Rotated log files matching the pattern.
     */
    void CollectRotatedLogFiles(const std::string &filePath, std::vector<std::string> &files);

    /**
     * @brief Prune oldest rotated logs to satisfy max_log_file_num.
     * @param[in] files Rotated log files.
     */
    void PruneOldLogFiles(const std::vector<std::string> &files);

    int fd_ = K_INVALID_FD;
    std::string filePath_;
    size_t fileSize_ = 0;
    const char *podName_;
};
}  // namespace datasystem
#endif
