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
 * Description: exporter of metrics.
 */

#ifndef DATASYSTEM_WORKER_METRICS_EXPORTER_H
#define DATASYSTEM_WORKER_METRICS_EXPORTER_H

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <vector>
#include <sstream>

#include "datasystem/common/util/gflag/common_gflags.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/uri.h"
#include "datasystem/common/util/wait_post.h"
#include "datasystem/common/log/trace.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/util/thread.h"

namespace datasystem {
class MetricsExporter {
public:
    virtual ~MetricsExporter()
    {
    }

    /**
     * @brief Submit write messages in the active buffer.
     */
    void SubmitWriteMessage();

    /**
     * @brief Write a message to the active buffer.
     * @param[in] message the message of for writing to the active buffer
     * @param[in] uri file uri
     * @param[in] line line of code.
     */
    virtual void Send(const std::string &message, Uri &uri, int line) = 0;

protected:
    /**
     * @brief  Initialize sink and start some threads and create log file by absolute path.
     * @return Status of the call.
     */
    Status Init();

    /**
     * @brief Stop flusher thread.
     */
    void Stop();

    /**
     * @brief Write a message to the active buffer.
     * @param[in] message the message of for writing to the active buffer
     */
    void WriteMessage(const std::string &message);

    /**
     * @brief Get flush buffer from buffer pool and notify writer the pool is not full.
     * @param[in] flushBuffer The buffer that stores the message.
     */
    void GetFlushBufferFromQueue(std::unique_ptr<std::vector<std::string>> &flushBuffer);

    // Protects 'activeBuffer_' and 'bufferSize_'.
    mutable std::mutex mtx_;
    WaitPost cvLock_;
    std::condition_variable notFull_;
    std::condition_variable notEmpty_;

    std::atomic<bool> exitFlag_{ false };
    std::unique_ptr<Thread> flushThread_{ nullptr };

    const int poolSize_ = 8;
    size_t bufferSize_ = 0;
    std::unique_ptr<std::vector<std::string>> activeBuffer_;
    std::queue<std::unique_ptr<std::vector<std::string>>> bufferPool_;

private:
    /**
     * @brief Flusher thread, we will flush the log from buffer to log file in this thread.
     */
    void StartFlushThread();

    /**
     * @brief Write message to specified file.
     */
    virtual void FlushThread() = 0;

    /**
     * @brief Set active buffer to buffer pool and notify flusher to write message.
     * @param[in] activeBuffer The buffer that stores the message.
     */
    void SetActiveBufferToQueue(std::unique_ptr<std::vector<std::string>> &activeBuffer);
};
}  // namespace datasystem
#endif