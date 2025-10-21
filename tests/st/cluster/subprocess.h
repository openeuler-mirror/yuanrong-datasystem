/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2022. All rights reserved.
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
 * Description: Subprocess, it's api just like thread.
 */
#ifndef DATASYSTEM_TEST_ST_CLUSTER_SUBPROCESS_H
#define DATASYSTEM_TEST_ST_CLUSTER_SUBPROCESS_H

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "datasystem/common/util/status_helper.h"

namespace datasystem {
namespace st {
class Subprocess {
public:
    explicit Subprocess(std::string cmd);

    virtual ~Subprocess() = default;

    /**
     * @brief Start the subprocess.
     * @return Status::OK() if success.
     */
    virtual Status Start();

    /**
     * @brief Set this process as running process.
     * @return Status::OK() if success.
     */
    virtual Status SetRunningMode();

    /**
     * @brief Shutdown the process
     * @return Status::OK() if success.
     */
    virtual Status Shutdown();

    /**
     * @brief Kill the process
     * @return Status::OK() if success.
     */
    virtual Status ShutdownByKill();

    /**
     * @brief Join the subprocess.
     */
    void Join() const;

    /**
     * @brief Set specified environment variables for subprocess.
     * @param[in] env Environment variable list.
     */
    void SetEnv(std::vector<std::pair<std::string, std::string>> env);

    /**
     * @brief send signal to subprocess via kill.
     * @param[in] signum signal number.
     * @return Status::OK() if success.
     */
    Status Kill(int signum) const;

    /**
     * @brief send signal to subprocess via kill and wait for subprocess exit.
     * @param[in] signum signal number.
     * @param[in] timeoutSecs timeout seconds.
     * @return Status::OK() if success.
     */
    Status KillAndWait(int signum, int timeoutSecs = TERMINATE_TIMEOUT_SECS);

    /**
     * @brief Return true if subprocess is still alive.
     * @return true if subprocess is still alive.
     */
    bool IsProcessAlive();

    /**
     * @brief Return true if subprocess exit normally.
     * @return true if subprocess exit normally.
     */
    bool IsProcessExitNormally();

    /**
     * @brief Return the pid of running subprocess.
     * @return pid.
     */
    pid_t Pid() const;

    /**
     * @brief Get subprocess exit code.
     * @return exit code.
     */
    int ExitCode() const;

    /**
     * @brief Append parameters for process command, should invoke before start.
     * @param[in] params New Append parameter.
     */
    void AppendCmdParams(const std::string &params);

    /**
     * @brief inject abort() to subprocess
     */
    void SetInjectAbort();

protected:
    enum ProcessState { K_INITIALIZE = 0, K_RUNNING = 1, K_TERMINATE = 2 };

    // Subprocess state.
    ProcessState state_;

    // command pass via user.
    std::string cmd_;

    // Environment variables for subprocess.
    std::vector<std::pair<std::string, std::string>> env_;

    // Parse the command and get the argv list.
    std::vector<std::string> args_;

    // Exec filename.
    std::string exe_;

    // Subprocess pid.
    pid_t pid_;

    // Subprocess exit code.
    int exitCode_;

    // Whether subprocess have inject abort()
    bool injectAbort_;

private:
    /**
     * @brief Parse exec command.
     * @return Status::OK() if success, the error otherwise.
     */
    Status ParseCommand();

    void DeleteHealthCheckFile();

    // Terminated default timeout seconds.
    static const int TERMINATE_TIMEOUT_SECS = 10;
};
}  // namespace st
}  // namespace datasystem
#endif  // DATASYSTEM_TEST_ST_CLUSTER_SUBPROCESS_H